//! 全局内存账本：**一个预算，三类租户**。
//!
//! 设计（`docs/内存预算与溢出设计.md`）：
//!
//! ```text
//! ① 每查询配额  超 → QueryMemoryLimit   (400)  「你的查询太大」
//! ② 挤兑缓存    令 cache 让位（可到 0）
//! ③ 全局不足    → 算子溢出；无溢出能力 → MemoryPressure (503, 可退避重试)
//! ```
//!
//! 两条不变量：
//! - **C1**：`exec`（物化算子保留字节）≤ `budget`——恒成立，因为准入与释放同源；
//! - **C2**：缓存是**可挤兑**的纯缓存（丢了大不了重算），压力下先丢它。
//!
//! 与被删掉的 `charge_memory` / `release_memory` 手工配对相比，这里只有**一个入口**
//! [`QueryContext::try_reserve`]，归还由 [`Reservation`] 的 `Drop` 负责——「忘还」
//! 在类型上不可能（这也是审计出的真实缺陷：6 处手工配对里 `HashAggExec` 就只记不还）。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::cache::SegmentCache;
use crate::error::{QsdbError, Result};
use crate::query::QueryContext;

/// 可被挤兑的内存持有者（enum 派发，零虚调用——贴合项目惯例）。
pub enum Yielder {
    /// 解码后批的 LRU 缓存（`scan_cache_bytes`）。
    SegmentCache(SegmentCache),
}

impl Yielder {
    /// 请求释放至多 `want` 字节，返回**实际可释放**的字节数。
    ///
    /// 「可释放」= 索引侧摘除量：已被查询持有（在途 `Arc`）的批摘了也不还内存，
    /// 故与 `cache` 记账口径一致（记账记的也是索引侧持有量）。
    pub fn yield_bytes(&self, want: u64) -> u64 {
        match self {
            Yielder::SegmentCache(c) => c.evict_bytes(want as usize) as u64,
        }
    }

    /// 当前持有量（观测用）。
    pub fn used_bytes(&self) -> u64 {
        match self {
            Yielder::SegmentCache(c) => c.stats().1 as u64,
        }
    }
}

/// `RecordBatch` 的**切片感知**字节估算——记账与攒批阈值都用它。
///
/// **不要**用 `RecordBatch::get_array_memory_size()` 做这两件事：它按**底层缓冲区
/// 容量**算，而扫描出来的每一批几乎都是大缓冲的**切片**——父缓冲多大就报多大。
/// 实测：500k 行父批切成 8192 行，`get_array_memory_size()` 仍报 4,000,096 字节，
/// 逻辑只有 65,536（**虚高 61 倍**）。后果两类，都是实打实的：
///
/// 1. **记账虚高** → 配额下过早落盘 / 过早 `MEMORY_PRESSURE`（"明明装得下"）；
/// 2. **阈值虚高** → 攒批逻辑退化：`INSERT … SELECT` 曾因此**每批提交一次**
///    （5M 行 610 次提交），每次提交 3 次 fsync ≈ 112ms → 导入慢到 60-100s。
///
/// 取 arrow 的切片口径 `ArrayData::get_slice_memory_size()`（缓存的容量按切片折算）；
/// 个别类型算不出来时退回按**逻辑长度**估（行数 × 列宽），再兜底老口径。
pub fn batch_bytes(b: &arrow::record_batch::RecordBatch) -> u64 {
    let mut total = 0u64;
    for c in b.columns() {
        match c.to_data().get_slice_memory_size() {
            Ok(n) => total += n as u64,
            Err(_) => {
                // 兜底：按行数 × 该列的逻辑宽度（不碰父缓冲）
                total += logical_width(c.data_type()) * b.num_rows() as u64;
            }
        }
    }
    total.max(1)
}

/// 单行的逻辑宽度下界（string/binary 只算 offset，数据段另计；够记账用）。
fn logical_width(dt: &arrow::datatypes::DataType) -> u64 {
    use arrow::datatypes::DataType as D;
    match dt {
        D::Boolean | D::Int8 | D::UInt8 => 1,
        D::Int16 | D::UInt16 | D::Float16 => 2,
        D::Int32 | D::UInt32 | D::Float32 | D::Date32 | D::Time32(_) => 4,
        D::Int64 | D::UInt64 | D::Float64 | D::Date64 | D::Time64(_) | D::Timestamp(..) => 8,
        D::Decimal128(..) | D::Interval(..) => 16,
        D::Utf8 | D::Binary => 8,
        D::LargeUtf8 | D::LargeBinary => 16,
        _ => 16,
    }
}

/// 进程内（严格说：一个 `Qsdb` 句柄内）共享的内存预算。
pub struct MemoryPool {
    /// 0 = 不限（嵌入方自控；server 会给兜底默认值）。
    budget: u64,
    /// 执行侧已保留字节（物化算子）。
    exec: AtomicU64,
    /// 缓存侧已用字节（可被挤兑；由 [`SegmentCache`] 自分片同步过来）。
    cache: AtomicU64,
    /// 可挤兑者（只在**慢路径**加锁——快路径只读两个原子）。
    yielders: Mutex<Vec<Yielder>>,
}

impl MemoryPool {
    /// `budget_bytes = 0` = 不限。
    pub fn new(budget_bytes: u64) -> Self {
        Self {
            budget: budget_bytes,
            exec: AtomicU64::new(0),
            cache: AtomicU64::new(0),
            yielders: Mutex::new(Vec::new()),
        }
    }

    pub fn budget(&self) -> u64 {
        self.budget
    }

    pub fn exec_used(&self) -> u64 {
        self.exec.load(Ordering::Relaxed)
    }

    pub fn cache_used(&self) -> u64 {
        self.cache.load(Ordering::Relaxed)
    }

    /// 注册可挤兑者（`Qsdb::open` 时挂上解码缓存）。
    pub fn register_yielder(&self, y: Yielder) {
        if let Ok(mut v) = self.yielders.lock() {
            v.push(y);
        }
    }

    /// 缓存侧记账同步（由 `SegmentCache` 在自己的增删后调用；非热路径）。
    pub fn set_cache_used(&self, bytes: u64) {
        self.cache.store(bytes, Ordering::Relaxed);
    }

    /// 准入：保留 `bytes`；不足则先挤兑缓存，仍不足 → [`QsdbError::MemoryPressure`]。
    ///
    /// 调用方在**拿不到额度时**有两个选择：有溢出能力就溢出（如 `SortExec`），
    /// 没有就把它当终止错误上抛（如 `WindowExec`）。
    pub fn acquire(&self, bytes: u64) -> Result<()> {
        if self.budget == 0 {
            return Ok(()); // 不限
        }
        if self.try_add(bytes) {
            return Ok(());
        }
        // 慢路径：压力下先丢缓存（纯缓存，丢了大不了重算），再试一次
        let want = self
            .exec
            .load(Ordering::Relaxed)
            .saturating_add(bytes)
            .saturating_add(self.cache.load(Ordering::Relaxed))
            .saturating_sub(self.budget);
        self.evict(want);
        if self.try_add(bytes) {
            return Ok(());
        }
        // 用 KB 而不是 MB：小预算（+小请求）下 MB 会把三个数全抹成 0，
        // 排障时看不出"到底要了多少、还占着多少"（踩过一次）。
        Err(QsdbError::MemoryPressure(format!(
            "memory budget {}KB exhausted: {}KB in use ({}KB cache, {}KB evictable), {}KB requested (retry later)",
            self.budget / 1024,
            self.exec.load(Ordering::Relaxed) / 1024,
            self.cache.load(Ordering::Relaxed) / 1024,
            self.yieldable_bytes() / 1024,
            bytes / 1024
        )))
    }

    /// 可挤兑者当前占用（**只读观测**：走 `used_bytes` 而不是 `yield_bytes`——
    /// 后者会真去挤兑，拼错误信息的路上顺手清空缓存是荒谬的副作用）。
    fn yieldable_bytes(&self) -> u64 {
        let Ok(v) = self.yielders.lock() else {
            return 0;
        };
        v.iter().map(|y| y.used_bytes()).sum()
    }

    /// 无条件尝试入账（CAS 循环，防并发超额）。
    fn try_add(&self, bytes: u64) -> bool {
        let mut cur = self.exec.load(Ordering::Relaxed);
        loop {
            let total = cur
                .saturating_add(bytes)
                .saturating_add(self.cache.load(Ordering::Relaxed));
            if total > self.budget {
                return false;
            }
            match self.exec.compare_exchange_weak(
                cur,
                cur.saturating_add(bytes),
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(actual) => cur = actual,
            }
        }
    }

    /// 请所有可挤兑者让位（最多 `want` 字节）。
    fn evict(&self, want: u64) {
        if want == 0 {
            return;
        }
        let Ok(v) = self.yielders.lock() else { return };
        let mut remaining = want;
        for y in v.iter() {
            if remaining == 0 {
                break;
            }
            remaining -= remaining.min(y.yield_bytes(remaining));
        }
    }

    /// 归还（只由 [`Reservation::drop`] 调用）。
    ///
    /// `fetch_update` 在 rustc **1.99** 起被标 deprecated（改名 `try_update`），
    /// 但 `try_update` **1.95 才有**，而本工作区声明的 MSRV 是 **1.85**
    /// （`Cargo.toml` 的 `rust-version`，clippy 的 `incompatible_msrv` 会拦住）
    /// ⇒ **现在还换不了**，只能先 allow 掉这个提示、留待 MSRV 提到 1.95 时改。
    /// （实测：`.14` 是 1.99、本地是 1.97，两边都要能编。）
    #[allow(deprecated)]
    pub(crate) fn release_bytes(&self, bytes: u64) {
        let _ = self
            .exec
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
                Some(cur.saturating_sub(bytes))
            });
    }
}

/// 内存预留（RAII）：`Drop` 即归还。算子把它作为字段持有，
/// 结构消失（查询结束 / panic 展开）时额度自动回到账上。
#[must_use = "预留必须被持有，否则立即归还（等于没预留）"]
pub struct Reservation {
    ctx: Arc<QueryContext>,
    bytes: u64,
    /// 是否受**每查询配额**约束。溢出缓冲走 `false`——见
    /// [`QueryContext::reserve_spill`]：逃生通道不能自己也吃配额，否则
    /// 小配额下溢出永远启动不了（`grow` 会再撞配额，实测踩过）。
    quota: bool,
}

impl std::fmt::Debug for Reservation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // 不打印 ctx（它会牵出 query_id 与整条链路）
        f.debug_struct("Reservation")
            .field("query_id", &self.ctx.query_id)
            .field("bytes", &self.bytes)
            .finish()
    }
}

impl Reservation {
    /// 由 [`QueryContext::try_reserve`] 构造（账已入，此处只移交所有权）。
    pub(crate) fn from_parts(ctx: Arc<QueryContext>, bytes: u64) -> Self {
        Self {
            ctx,
            bytes,
            quota: true,
        }
    }

    /// 溢出缓冲用：只受全局预算约束（不受每查询配额）。
    pub(crate) fn from_parts_global(ctx: Arc<QueryContext>, bytes: u64) -> Self {
        Self {
            ctx,
            bytes,
            quota: false,
        }
    }

    /// 已预留字节。
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    /// 追加预留（用于**单调增长**的状态，如聚合组表）。
    ///
    /// 失败时**不改动本预留**——调用方据此决定溢出还是终止。
    pub fn grow(&mut self, more: u64) -> Result<()> {
        if more == 0 {
            return Ok(());
        }
        let n = if self.quota {
            self.ctx.reserve_plain(more)?
        } else {
            self.ctx.reserve_global(more)?
        };
        self.bytes += n;
        Ok(())
    }

    /// 部分归还（用于**可收缩**的场景，如排序 chunk 落盘后）。
    pub fn shrink(&mut self, less: u64) {
        let less = less.min(self.bytes);
        self.ctx.release_plain(less);
        self.bytes -= less;
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.ctx.release_plain(self.bytes);
    }
}
