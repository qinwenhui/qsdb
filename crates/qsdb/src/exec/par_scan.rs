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
/// 实测（2026-10-13，HEAD 2162b16）：`SELECT k, v FROM p UNION ALL SELECT k, v FROM p`
/// 在 200 段的非分区表上**必挂**（插桩改变时序后同查询 16ms 通过，即竞态）。
///
/// 独立池让「扫描生产」与「查询自身的并行」（聚合分片、UDAF 组求值）互不占位，
/// 从结构上消除该类死锁，而不是靠调大 channel 容量去躲。池**懒创建**
/// （首次并行扫描才起线程，空闲即挂起），符合"无后台常驻线程"的既有约定。
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
        Some(Self {
            rx,
            schema,
            pending: Some(Pending { shards, tx }),
            done: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            started: false,
            ctx,
        })
    }

    /// 首次取数才提交分片任务。
    fn start(&mut self) {
        let Some(Pending { shards, tx }) = self.pending.take() else {
            return;
        };
        self.started = true;
        let done = Arc::clone(&self.done);
        let worker_ctx = Arc::clone(&self.ctx);
        std::thread::spawn(move || {
            use rayon::prelude::*;
            let items: Vec<_> = shards.into_iter().map(|s| (s, tx.clone())).collect();
            // **专用池**（不占全局 rayon 池，避免与查询自身的并行互锁）
            scan_pool().install(|| {
                items.into_par_iter().for_each(|(mut shard, tx)| {
                    for item in shard.by_ref() {
                        if worker_ctx.check().is_err() {
                            // 取消/超时：把错误发给消费端并停止本分片
                            let _ = tx.send(Err(crate::error::QsdbError::QueryCancelled(
                                worker_ctx.query_id.clone(),
                            )));
                            break;
                        }
                        if tx.send(item).is_err() {
                            break; // 消费端提前退出（如 LIMIT 截断）
                        }
                    }
                });
            });
            drop(tx); // 关闭 channel：消费端 recv → None
            done.store(true, std::sync::atomic::Ordering::Release);
        });
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

/// 构造扫描节点：可分片走并行，否则串行。
pub fn make_scan(scan: Scan, ctx: &Arc<crate::query::QueryContext>) -> Box<dyn ExecNode> {
    let threads = rayon::current_num_threads().clamp(1, 16);
    if let Some(p) = ParScanExec::new(&scan, threads, Arc::clone(ctx)) {
        return Box::new(p);
    }
    Box::new(ScanExec::new(scan))
}

// 消除未用 import 警戒
#[allow(unused)]
fn _err_hint(_: QsdbError) {}
