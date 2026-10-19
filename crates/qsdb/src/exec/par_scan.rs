//! ParScanExec：scan 层并行（对标 morsel 并行的读侧）。
//!
//! Scan 分片（文件级）→ rayon 并行拉批 → 有界 channel 汇聚（背压流式）。
//! 输出无序（分析场景；排序由上层算子负责）。分片失败（单文件等）自动
//! 退化为串行 ScanExec。

use std::sync::Arc;
use std::sync::mpsc::sync_channel;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use crate::error::{QsdbError, Result};
use crate::scan::Scan;

use super::{ExecNode, ScanExec};

/// **扫描生产端专用线程池**——绝不能跑在全局 rayon 池上。
///
/// 生产端往有界 channel 里 `send`，满即**阻塞**。此前它跑在全局池上，于是：
/// 两个并行扫描同时存在时（UNION 左右侧、JOIN build/probe、多 Scan 计划），
/// 消费者一次只抽一边——另一边已填满 channel 的分片任务把池线程**全占住**
/// 阻塞在 `send`，被抽的那一边分片排不上线程 → **互等死锁**。
///
/// 实测（2026-10-11，HEAD 2162b16）：`SELECT k, v FROM p UNION ALL SELECT k, v FROM p`
/// 在 200 段的非分区表上**必挂**（插桩改变时序后同查询 16ms 通过，即竞态）。
///
/// 独立池让「扫描生产」与「查询自身的并行」（聚合分片、UDAF 组求值）互不占位，
/// 从结构上消除该类死锁，而不是靠调大 channel 容量去躲。池**懒创建**
/// （首次并行扫描才起线程，空闲即挂起），符合"无后台常驻线程"的既有约定。
/// **懒启动 + 专用池的已知代价**（2026-10-11 gate 实测，A/B 交替、最小口径）：
/// 纯扫描用例 B 反而快 2~7%；`join_count` ±2%；但**宽物化 join + LIMIT**
/// （`SELECT t.v, d.name FROM t JOIN dim d ON t.g=d.g LIMIT 1000`）慢 **5~15%**。
///
/// 原因不是池、是**预取窗口**：改动前两侧生产端都在计划构建期启动 —— 探测侧在
/// 构建阶段**期间**就把通道填满了（重叠预热）；现在懒启动 + 在构建**之后**才
/// `prefetch`（见 [`ExecNode::prefetch`]），少掉这段重叠。
///
/// 为什么不干脆在构建**之前**预取探测侧：那正是**互锁的成因**——未被抽的那一侧
/// 占满池线程阻塞在 `send`，被抽的一侧排不上线程。本设计的正确性就建立在
/// "一条流同时最多一个活跃生产端"上，预取只能保守地放在"当前生产端已退出"之后。
/// 要拿回全部预取又不引入互锁，得让生产端**阻塞时不占池线程**（独立线程池/每分片
/// 独立线程/可让出的任务），那是另一个量级的设计（P3 记录，未做）。
/// 测量口径另见 `target/bench-release` 的 `fair_vs`（本机负载波动可达 ±50%，
/// 绝对值不可比，只看同一窗口内的 A/B 比）。
fn scan_pool() -> &'static rayon::ThreadPool {
    static POOL: std::sync::OnceLock<rayon::ThreadPool> = std::sync::OnceLock::new();
    POOL.get_or_init(|| {
        rayon::ThreadPoolBuilder::new()
            .num_threads(rayon::current_num_threads().clamp(1, 16))
            .thread_name(|i| format!("qsdb-scan-{i}"))
            .build()
            .expect("build scan pool")
    })
}

/// 未启动的生产端（懒启动，见 [`ParScanExec::start`]）。
struct Pending {
    shards: Vec<Scan>,
    tx: std::sync::mpsc::SyncSender<Result<RecordBatch>>,
    done: Arc<std::sync::atomic::AtomicBool>,
}

/// 并行扫描节点。
///
/// 生产端**懒启动**：分片任务在首次 [`ExecNode::next`] 才提交。原因是死锁——
/// 计划构建期会同时建出多个扫描节点（UNION 左右、JOIN build/probe），若都立刻
/// 起生产端，**没被抽的那一侧**会先把自己的有界 channel 填满、全部分片任务阻塞
/// 在 `send`，把池线程占住 → 被抽的那一侧分片排不上线程 → 消费端 `recv`
/// 永不返回。懒启动让"当前正在被抽的扫描"独占池线程，从结构上消除该类互锁。
pub struct ParScanExec {
    rx: std::sync::mpsc::Receiver<Result<RecordBatch>>,
    schema: SchemaRef,
    pending: Option<Pending>,
    /// 生产端全部结束的信号
    done: Arc<std::sync::atomic::AtomicBool>,
    started: bool,
    ctx: Arc<crate::query::QueryContext>,
}

impl ParScanExec {
    /// 从 Scan 构造并行执行；不可分片（文件 < 2）返回 None。借用 scan。
    pub fn new(scan: &Scan, threads: usize, ctx: Arc<crate::query::QueryContext>) -> Option<Self> {
        let schema = Arc::clone(scan.schema());
        let shards = scan.shard(threads, true)?;
        // 有界 channel = 背压，也是**内存上界**（在途批 ≤ 容量 × 批大小）
        let (tx, rx) = sync_channel::<Result<RecordBatch>>(threads.clamp(2, 16) * 2);
        let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
        Some(Self {
            rx,
            schema,
            pending: Some(Pending {
                shards,
                tx,
                done: Arc::clone(&done),
            }),
            done,
            started: false,
            ctx,
        })
    }

    /// 预取：提前提交分片任务（父算子确认"另一侧已不再需要池"时调，
    /// 见 [`ExecNode::prefetch`]——懒启动会丢预取，这里把它补回来）。
    pub fn prefetch(&mut self) {
        self.start();
    }

    /// 首次取数才提交分片任务。
    fn start(&mut self) {
        let Some(Pending { shards, tx, done }) = self.pending.take() else {
            return;
        };
        self.started = true;
        spawn_producer(shards, tx, done, Arc::clone(&self.ctx));
    }
}

impl ExecNode for ParScanExec {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
    fn next(&mut self) -> Option<Result<RecordBatch>> {
        if let Err(e) = self.ctx.check() {
            return Some(Err(e));
        }
        self.start();
        self.rx.recv().ok()
    }

    fn prefetch(&mut self) {
        self.start();
    }
}

impl Drop for ParScanExec {
    fn drop(&mut self) {
        // 消费端提前退出：丢弃剩余批，让生产端 send 失败自然终止
        if self.started {
            while !self.done.load(std::sync::atomic::Ordering::Acquire) {
                match self.rx.try_recv() {
                    Ok(_) => {}
                    Err(std::sync::mpsc::TryRecvError::Empty) => {
                        // 生产者仍在填；短暂让出
                        std::thread::yield_now();
                        if self.rx.try_recv().is_err() {
                            // 生产者可能阻塞在 send 上——继续让出直到它观察到我方退出
                            std::thread::sleep(std::time::Duration::from_micros(100));
                        }
                    }
                    Err(std::sync::mpsc::TryRecvError::Disconnected) => break,
                }
            }
        }
    }
}

/// 并行读的接收端（`Scan::next` 用它，见 [`spawn_reader`]）。
pub struct ParReader {
    rx: std::sync::mpsc::Receiver<Result<RecordBatch>>,
    /// 生产端全部结束（仅为诊断/观测，不强等）
    pub done: Arc<std::sync::atomic::AtomicBool>,
}

impl ParReader {
    /// 取下一批（`None` = 生产端结束）。
    pub fn recv(&mut self) -> Option<Result<RecordBatch>> {
        self.rx.recv().ok()
    }
}

/// 起并行生产端：分片 → 专用池 → 有界 channel（背压即内存上界）。
///
/// 与 [`ParScanExec`] 同一套机制，抽出来给**公开 scan API** 用——那才是导出工具、
/// 嵌入方走的路；此前只有 SQL 路径（`make_scan`）享受并行，`db.scan()` 是串行的。
pub fn spawn_reader(shards: Vec<Scan>, ctx: Arc<crate::query::QueryContext>) -> ParReader {
    let (tx, rx) = sync_channel::<Result<RecordBatch>>(shards.len().clamp(2, 16) * 2);
    let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    spawn_producer(shards, tx, Arc::clone(&done), ctx);
    ParReader { rx, done }
}

/// 是否禁用扫描并行（`QSDB_SCAN_SERIAL=1`）——**诊断/对照开关**，
/// 也用于"并行会不会改变行为"的 A/B。
pub fn scan_parallel_disabled() -> bool {
    std::env::var("QSDB_SCAN_SERIAL").is_ok()
}

/// 把分片喂进 channel（专用池，不占全局 rayon 池——避免与查询自身并行互锁）。
fn spawn_producer(
    shards: Vec<Scan>,
    tx: std::sync::mpsc::SyncSender<Result<RecordBatch>>,
    done: Arc<std::sync::atomic::AtomicBool>,
    ctx: Arc<crate::query::QueryContext>,
) {
    std::thread::spawn(move || {
        use rayon::prelude::*;
        let items: Vec<_> = shards.into_iter().map(|s| (s, tx.clone())).collect();
        scan_pool().install(|| {
            items.into_par_iter().for_each(|(mut shard, tx)| {
                for item in shard.by_ref() {
                    // 透传真实原因（超时/取消各归各位）——此前一律改报
                    // QueryCancelled，把超时伪装成取消，误导按码分支的调用方。
                    if let Err(e) = ctx.check() {
                        let _ = tx.send(Err(e));
                        break;
                    }
                    if tx.send(item).is_err() {
                        break; // 消费端退出（LIMIT 截断 / 提前 drop）
                    }
                }
            });
        });
        drop(tx);
        done.store(true, std::sync::atomic::Ordering::Release);
    });
}

/// 构造扫描节点：可分片走并行，否则串行。
pub fn make_scan(scan: Scan, ctx: &Arc<crate::query::QueryContext>) -> Box<dyn ExecNode> {
    let threads = rayon::current_num_threads().clamp(1, 16);
    // ★ 诊断开关在这条路上**此前是死的**（2026-10-17 实测）：`QSDB_SCAN_SERIAL=1`
    //   只在**公开 `scan()` API** 那条路被认（`Scan::next` 里），而 SQL 路径走的是
    //   这里 ⇒ 设了它照样起 16 个扫描线程。文档两处都把它写成"诊断/对照开关"，
    //   于是**任何人做这个 A/B 都会得到"并行没有影响"的假结论**（我就先踩了一次）。
    //   收口在 `make_scan`：SQL 的两个扫描叶（`Scan` / `ExternalScan`）都走它。
    #[allow(clippy::collapsible_if)] // MSRV 1.85：还不能用 let-chains
    if !scan_parallel_disabled() {
        if let Some(p) = ParScanExec::new(&scan, threads, Arc::clone(ctx)) {
            return Box::new(p);
        }
    }
    Box::new(ScanExec::new(scan))
}

// 消除未用 import 警戒
#[allow(unused)]
fn _err_hint(_: QsdbError) {}
