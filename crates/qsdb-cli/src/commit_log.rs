//! 提交事件的**环形缓冲** —— `GET /api/v1/watch` 的后端（T2）。
//!
//! 设计见 `提交事件订阅设计稿` §1.3/§1.4。消费方是 qsdb-sync：
//! 他们今天靠**轮询** `partition_stats` 发现增量，接上这个端点就能换成 push
//! （RPO 秒级 → 毫秒级）。
//!
//! # 三条口径（都是"宁可报错，也不给假象"那条纪律的落地）
//!
//! 1. **只留最近 N 条**（进程内存）。游标比最旧的还早 ⇒ 报 `CURSOR_TOO_OLD`，
//!    **绝不**假装连续 —— 消费方据此走一次 `generation` 对账（那本来就是它今天在做的）。
//! 2. 游标是**单调递增的序号**，服务端发放、客户端原样回传；`next` 是"下次从这儿要"。
//! 3. **服务端重启即清空**。此时任何"上一代的游标"都会落进 `CURSOR_TOO_OLD`
//!    （判据见 `try_take` 里的 `since > next`），而不是**静默地永远等下去** ——
//!    后者是这条通路唯一真正危险的失败模式。
//!
//! ⚠️ 与内核钩子的关系：钩子**可能丢事件**（回调 panic 会被吞、远端写路径暂不透出
//! generation）。所以这里是**加速器**，不是真相来源；消费方该保留兜底轮询。

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use qsdb::commit::{CommitEvent, CommitHook};

/// 单条已序列化的事件。
#[derive(Clone)]
pub struct Entry {
    pub seq: u64,
    pub json: serde_json::Value,
}

/// 一次取件的返回。
pub struct Batch {
    pub events: Vec<serde_json::Value>,
    /// 下一次请求该带的游标
    pub next: u64,
    /// 还有积压（客户端**立刻**用新游标再拉，别等 `wait`）
    pub more: bool,
}

/// 游标太旧（或被缓冲区/重启甩下）。
pub struct TooOld;

struct Inner {
    events: VecDeque<Entry>,
    /// 下一个要发放的序号（= 已发事件数）
    next: u64,
    /// 缓冲里最旧那条的序号（空 = `next`）
    oldest: u64,
}

/// 环大小：够消费方在一次短抖动/重连里补齐，又不至于把内存当日志用。
/// 1 万条分区级事件对同步工具是充足的回补窗口。
const CAP: usize = 10_000;

/// 一次返回最多多少条：避免一个积压很久的订阅方一口气拉爆内存。
const MAX_BATCH: usize = 1_000;

/// 长轮询的默认/上限（秒）。默认 25 是为了**低于常见网关的 30s 空闲超时**。
pub const DEFAULT_WAIT_SECS: u64 = 25;
pub const MAX_WAIT_SECS: u64 = 60;

pub struct CommitLog {
    inner: Mutex<Inner>,
    notify: tokio::sync::Notify,
}

impl CommitLog {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(Inner {
                events: VecDeque::new(),
                next: 0,
                oldest: 0,
            }),
            notify: tokio::sync::Notify::new(),
        })
    }

    /// 同上，但事件带上**挂载库名**（M-B 多库：同一个 CommitLog 收所有挂载的
    /// 事件，消费方靠 `database` 字段区分）。字段只增——旧消费方多收一个键。
    pub fn hook_for(self: &Arc<Self>, database: &str) -> CommitHook {
        let me = Arc::clone(self);
        let db = database.to_string();
        CommitHook::new(move |ev: &CommitEvent| me.push(&db, ev))
    }

    /// 写一条（由内核钩子调用；**任意线程**，可能是阻塞线程池）。
    pub fn push(&self, database: &str, ev: &CommitEvent) {
        let json = serde_json::json!({
            "database": database,
            "table": ev.table,
            "partition": ev
                .partition_values
                .iter()
                .map(qsdb::partition::partition_value_string)
                .collect::<Vec<_>>(),
            "generation": ev.generation,
            "kind": match ev.kind {
                qsdb::commit::CommitKind::Append => "Append",
                qsdb::commit::CommitKind::Overwrite => "Overwrite",
                qsdb::commit::CommitKind::Clear => "Clear",
                // `CommitKind` 是 `non_exhaustive`：将来加变体时这里**编译期就红**
                // （不留通配臂），逼着人把新 kind 的协议名一并想清楚
                ref other => unreachable!("未登记的 CommitKind: {other:?}"),
            },
        });
        {
            let mut g = self.inner.lock().expect("commit log poisoned");
            let seq = g.next;
            g.next += 1;
            g.events.push_back(Entry { seq, json });
            while g.events.len() > CAP {
                g.events.pop_front();
            }
            g.oldest = g.events.front().map_or(g.next, |e| e.seq);
        }
        // **叫醒所有在等的订阅方**（`notify_waiters` 只叫醒已注册的）。
        // 万一漏叫，`wait` 里的安全超时兜底（见下）——只是慢一点，不会卡死。
        self.notify.notify_waiters();
    }

    /// 当前游标（"现在从这儿开始要"）。
    pub fn latest(&self) -> u64 {
        self.inner.lock().expect("commit log poisoned").next
    }

    /// 取一次。`Ok(None)` = 暂时没有新事件（继续等）；`Err(TooOld)` = 游标没法服务。
    ///
    /// **判据（两条缺一不可）**：
    /// - `since > next` ⇒ 客户端**比服务端还新** —— 只可能是服务端重启过（缓冲清零、
    ///   `next` 归 0）或换了实例。**必须报太旧**：否则它会永远等一个不会到来的序号。
    /// - `since + 1 < oldest` ⇒ 事件已被环挤掉，接不上了。
    fn try_take(&self, since: u64) -> Result<Option<Batch>, TooOld> {
        let g = self.inner.lock().expect("commit log poisoned");
        if since > g.next || since.saturating_add(1) < g.oldest {
            return Err(TooOld);
        }
        if since == g.next {
            return Ok(None); // 追平了，没有新的
        }
        let evs: Vec<serde_json::Value> = g
            .events
            .iter()
            .filter(|e| e.seq >= since)
            .take(MAX_BATCH)
            .map(|e| e.json.clone())
            .collect();
        let next = since + evs.len() as u64;
        let more = next < g.next;
        Ok(Some(Batch {
            events: evs,
            next,
            more,
        }))
    }

    /// 长轮询：有事件立刻回；没有就等到 `max_wait`，再回一个空批次。
    ///
    /// `since = None` ⇒ **从现在开始**（首次订阅不该收到历史全量）。
    pub async fn wait(&self, since: Option<u64>, max_wait: Duration) -> Result<Batch, TooOld> {
        let since = since.unwrap_or_else(|| self.latest());
        let deadline = std::time::Instant::now() + max_wait;
        loop {
            if let Some(b) = self.try_take(since)? {
                return Ok(b);
            }
            let left = deadline.saturating_duration_since(std::time::Instant::now());
            if left.is_zero() {
                // 超时：回空批次，**带上当前游标**（客户端下次用它接着等）
                return Ok(Batch {
                    events: Vec::new(),
                    next: since,
                    more: false,
                });
            }
            // 等通知；**同时加一个安全上限**：万一唤醒丢了（`notify_waiters` 的
            // 注册时序），也只是最多晚 200ms —— 不会永久卡住，延迟仍是毫秒级。
            let _ =
                tokio::time::timeout(left.min(Duration::from_millis(200)), self.notify.notified())
                    .await;
        }
    }
}
