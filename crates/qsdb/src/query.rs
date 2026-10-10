//! QueryContext：单查询生命周期（超时 / 取消 / 内存记账）。
//!
//! 一个查询一份上下文，rayon 分片并行时各线程共享（全部原子/只读字段）。
//! 算子在批边界协作检查（[`QueryContext::check`]），取消/超时/内存超限
//! 返回错误而不是拖垮进程——会话隔离的根基。

use std::any::Any;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::error::{QsdbError, Result};

/// 单查询上下文。
pub struct QueryContext {
    pub query_id: String,
    started_at: Instant,
    deadline: Option<Instant>,
    cancelled: AtomicBool,
    memory_used: AtomicU64,
    /// `None` = 不限
    pub memory_limit: Option<u64>,
}

impl QueryContext {
    /// `timeout_ms` 为 None/0 表示不超时；`memory_limit` None 不限。
    pub fn new(
        query_id: impl Into<String>,
        timeout_ms: Option<u64>,
        memory_limit: Option<u64>,
    ) -> Arc<Self> {
        Arc::new(Self {
            query_id: query_id.into(),
            started_at: Instant::now(),
            deadline: timeout_ms
                .filter(|t| *t > 0)
                .map(|t| Instant::now() + Duration::from_millis(t)),
            cancelled: AtomicBool::new(false),
            memory_used: AtomicU64::new(0),
            memory_limit,
        })
    }

    /// 良性上下文：不超时、不可取消、不限内存（内部操作 / 嵌入式直连扫描）。
    pub fn benign() -> Arc<Self> {
        Self::new("benign", None, None)
    }

    /// 批边界协作检查：取消 / 超时 → Err（查询终止，进程无恙）。
    #[inline]
    pub fn check(&self) -> Result<()> {
        if self.cancelled.load(Ordering::Relaxed) {
            return Err(QsdbError::QueryCancelled(self.query_id.clone()));
        }
        if let Some(d) = self.deadline {
            if Instant::now() >= d {
                return Err(QsdbError::QueryTimeout(format!(
                    "query {} exceeded {}ms",
                    self.query_id,
                    self.elapsed().as_millis()
                )));
            }
        }
        Ok(())
    }

    /// 取消（幂等；并行分片即刻可见）。
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Relaxed);
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Relaxed)
    }

    /// 记账并检查上限；超限 → Err（而非 OOM 全库）。
    pub fn charge_memory(&self, bytes: u64) -> Result<()> {
        let used = self.memory_used.fetch_add(bytes, Ordering::Relaxed) + bytes;
        if let Some(limit) = self.memory_limit {
            if used > limit {
                return Err(QsdbError::QueryMemoryLimit(format!(
                    "query {} at {}MB exceeds {}MB limit",
                    self.query_id,
                    used / 1_048_576,
                    limit / 1_048_576
                )));
            }
        }
        Ok(())
    }

    pub fn release_memory(&self, bytes: u64) {
        let _ = self
            .memory_used
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
                Some(cur.saturating_sub(bytes))
            });
    }

    pub fn memory_used(&self) -> u64 {
        self.memory_used.load(Ordering::Relaxed)
    }

    pub fn elapsed(&self) -> Duration {
        self.started_at.elapsed()
    }
}

/// panic → 错误的隔离边界（`execute_sql` 顶层调用）。
pub fn run_isolated<F, T>(f: F) -> Result<T>
where
    F: FnOnce() -> Result<T>,
{
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(r) => r,
        // 注意 &*payload：先解 Box 再转 dyn，否则 Box 自身被当作 Any（downcast 恒失败）
        Err(payload) => Err(QsdbError::QueryPanic(panic_message(&*payload))),
    }
}

fn panic_message(p: &(dyn Any + Send)) -> String {
    if let Some(s) = p.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = p.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic payload".into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn benign_check_passes() {
        let ctx = QueryContext::benign();
        assert!(ctx.check().is_ok());
    }

    #[test]
    fn cancel_takes_effect() {
        let ctx = QueryContext::new("q1", None, None);
        assert!(ctx.check().is_ok());
        ctx.cancel();
        let err = ctx.check().unwrap_err();
        assert!(matches!(err, QsdbError::QueryCancelled(_)));
        assert!(ctx.is_cancelled());
    }

    #[test]
    fn timeout_fires_after_deadline() {
        let ctx = QueryContext::new("q2", Some(1), None); // 1ms
        std::thread::sleep(Duration::from_millis(5));
        assert!(matches!(
            ctx.check().unwrap_err(),
            QsdbError::QueryTimeout(_)
        ));
    }

    #[test]
    fn memory_charge_and_limit() {
        let ctx = QueryContext::new("q3", None, Some(1000));
        assert!(ctx.charge_memory(600).is_ok());
        assert_eq!(ctx.memory_used(), 600);
        assert!(matches!(
            ctx.charge_memory(600).unwrap_err(),
            QsdbError::QueryMemoryLimit(_)
        ));
        ctx.release_memory(600);
        assert!(ctx.charge_memory(400).is_ok());
    }

    #[test]
    fn run_isolated_converts_panic() {
        let r: Result<()> = run_isolated(|| panic!("boom 42"));
        let err = r.unwrap_err();
        match err {
            QsdbError::QueryPanic(msg) => assert!(msg.contains("boom"), "{msg}"),
            other => panic!("expected QueryPanic, got {other}"),
        }
    }

    #[test]
    fn run_isolated_passes_ok_through() {
        let r: Result<u32> = run_isolated(|| Ok(7));
        assert_eq!(r.unwrap(), 7);
    }
}
