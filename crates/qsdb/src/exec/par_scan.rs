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

/// 并行扫描节点。
pub struct ParScanExec {
    rx: std::sync::mpsc::Receiver<Result<RecordBatch>>,
    schema: SchemaRef,
    /// join 句柄的完成信号（rayon scope 无法跨结构体存活，用 detached 线程）
    done: Option<std::sync::Arc<std::sync::atomic::AtomicBool>>,
    ctx: Arc<crate::query::QueryContext>,
}

impl ParScanExec {
    /// 从 Scan 构造并行执行；不可分片（文件 < 2）返回 None。借用 scan。
    pub fn new(scan: &Scan, threads: usize, ctx: Arc<crate::query::QueryContext>) -> Option<Self> {
        let schema = Arc::clone(scan.schema());
        let shards = scan.shard(threads, true)?;
        let (tx, rx) = sync_channel::<Result<RecordBatch>>(threads.clamp(2, 16) * 2);
        let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let done_flag = Arc::clone(&done);
        // 调度线程：rayon 分片并行；结束时 drop tx 关闭 channel
        let worker_ctx = Arc::clone(&ctx);
        std::thread::spawn(move || {
            use rayon::prelude::*;
            let items: Vec<_> = shards.into_iter().map(|s| (s, tx.clone())).collect();
            let results: Vec<()> = items
                .into_par_iter()
                .map(|(mut shard, tx)| {
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
                })
                .collect();
            let _ = results;
            drop(tx);
            done_flag.store(true, std::sync::atomic::Ordering::Release);
        });
        Some(Self {
            rx,
            schema,
            done: Some(done),
            ctx,
        })
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
        self.rx.recv().ok()
    }
}

impl Drop for ParScanExec {
    fn drop(&mut self) {
        // 消费端提前退出：丢弃剩余批，让生产端 send 失败自然终止
        if let Some(done) = &self.done {
            while !done.load(std::sync::atomic::Ordering::Acquire) {
                match self.rx.try_recv() {
                    Ok(_) => {}
                    Err(std::sync::mpsc::TryRecvError::Empty) => {
                        // 生产者仍在填；短暂让出
                        std::thread::yield_now();
                        // 防死锁：channel 满则生产者阻塞在 send——try_recv 已排空，
                        // 下一轮 send 会成功或 Err（我们已 drop 接收端之后）
                        if self.rx.try_recv().is_err() {
                            // 极小概率竞态：继续让出直到生产者观察到我方退出
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
