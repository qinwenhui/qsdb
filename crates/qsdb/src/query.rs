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
use crate::memory::{MemoryPool, Reservation};

/// 单查询上下文。
pub struct QueryContext {
    pub query_id: String,
    started_at: Instant,
    deadline: Option<Instant>,
    cancelled: AtomicBool,
    memory_used: AtomicU64,
    /// `None` = 不限
    pub memory_limit: Option<u64>,
    /// 全局预算（`None` = 无池：仅每查询配额生效，嵌入方自控场景）。
    pool: Option<Arc<MemoryPool>>,
    /// 溢出目录根（`None` = 系统临时目录）。见 `QsdbOptions::spill_dir`。
    spill_base: Option<std::path::PathBuf>,
}

impl QueryContext {
    /// `timeout_ms` 为 None/0 表示不超时；`memory_limit` None 不限。
    pub fn new(
        query_id: impl Into<String>,
        timeout_ms: Option<u64>,
        memory_limit: Option<u64>,
    ) -> Arc<Self> {
        Self::new_with_pool(query_id, timeout_ms, memory_limit, None)
    }

    /// 同 [`Self::new`]，但挂到全局内存池上（准入 + 挤兑缓存）。
    pub fn new_with_pool(
        query_id: impl Into<String>,
        timeout_ms: Option<u64>,
        memory_limit: Option<u64>,
        pool: Option<Arc<MemoryPool>>,
    ) -> Arc<Self> {
        Self::new_full(query_id, timeout_ms, memory_limit, pool, None)
    }

    /// 全参构造：内存池 + **溢出目录根**（见 [`Self::spill_base`]）。
    pub fn new_full(
        query_id: impl Into<String>,
        timeout_ms: Option<u64>,
        memory_limit: Option<u64>,
        pool: Option<Arc<MemoryPool>>,
        spill_base: Option<std::path::PathBuf>,
    ) -> Arc<Self> {
        Arc::new(Self {
            spill_base,
            query_id: query_id.into(),
            started_at: Instant::now(),
            deadline: timeout_ms
                .filter(|t| *t > 0)
                .map(|t| Instant::now() + Duration::from_millis(t)),
            cancelled: AtomicBool::new(false),
            memory_used: AtomicU64::new(0),
            memory_limit,
            pool,
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

    /// **内存准入**：保留 `bytes`，返回 RAII 预留（`Drop` 即归还）。
    ///
    /// 顺序（见 `docs/内存预算与溢出设计.md` §3.2）：
    /// ① 每查询配额（报错最精确，且不动全局账）→ ② 挤兑缓存 → ③ 全局不足则拒。
    ///
    /// 失败**不入账**（旧实现「先加后比、失败不回滚」会把超限额永久留在
    /// `memory_used` 里，而它正是 `GET /api/v1/queries` 对外回显的字段）。
    pub fn try_reserve(self: &Arc<Self>, bytes: u64) -> Result<Reservation> {
        let n = self.reserve_plain(bytes)?;
        Ok(Reservation::from_parts(Arc::clone(self), n))
    }

    /// **溢出缓冲专用准入**：只受全局预算约束、**不受每查询配额约束**。
    ///
    /// 为什么单开一条：配额管的是「这个查询的物化规模」（用户承诺的上限），
    /// 而溢出是配额不够时的**逃生通道**——若逃生通道自己也吃配额，小配额下
    /// 溢出永远启动不了（实测：64KB 配额的 GROUP BY 直接失败而不是落盘完成，
    /// 那 P1 就等于没做）。上界由调用方按配额缩放单分区缓冲来兜
    /// （`AggSpill::part_cap_for`），真正的硬上界是全局预算。
    pub fn reserve_spill(self: &Arc<Self>, bytes: u64) -> Result<Reservation> {
        let n = self.reserve_global(bytes)?;
        Ok(Reservation::from_parts_global(Arc::clone(self), n))
    }

    /// 只受全局预算约束的准入（溢出缓冲 / 其增长用）。
    pub(crate) fn reserve_global(&self, bytes: u64) -> Result<u64> {
        if bytes == 0 {
            return Ok(0);
        }
        if let Some(pool) = &self.pool {
            pool.acquire(bytes)?;
        }
        self.memory_used.fetch_add(bytes, Ordering::Relaxed);
        Ok(bytes)
    }

    /// 准入但不构造预留（[`Reservation::grow`] 用）：返回实际保留字节。
    pub(crate) fn reserve_plain(&self, bytes: u64) -> Result<u64> {
        if bytes == 0 {
            return Ok(0);
        }
        // ① 每查询配额
        if let Some(limit) = self.memory_limit {
            let used = self.memory_used.load(Ordering::Relaxed);
            if used.saturating_add(bytes) > limit {
                if std::env::var("QSDB_TRACE_QUOTA").is_ok() {
                    eprintln!(
                        "QUOTA FAIL used={used} bytes={bytes} limit={limit}\n{}",
                        std::backtrace::Backtrace::force_capture()
                    );
                }
                return Err(QsdbError::QueryMemoryLimit(format!(
                    "query {} at {}MB would exceed its {}MB limit",
                    self.query_id,
                    used.saturating_add(bytes) / 1_048_576,
                    limit / 1_048_576
                )));
            }
        }
        // ②③ 全局预算（内含缓存挤兑）
        if let Some(pool) = &self.pool {
            pool.acquire(bytes)?;
        }
        self.memory_used.fetch_add(bytes, Ordering::Relaxed);
        Ok(bytes)
    }

    /// 归还（[`Reservation`] 私有）。
    pub(crate) fn release_plain(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        if let Some(pool) = &self.pool {
            pool.release_bytes(bytes);
        }
        let _ = self
            .memory_used
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
                Some(cur.saturating_sub(bytes))
            });
    }

    /// 溢出目录根（`None` = 系统临时目录）。
    pub fn spill_base(&self) -> Option<&std::path::Path> {
        self.spill_base.as_deref()
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
    fn memory_reserve_and_limit() {
        let ctx = QueryContext::new("q3", None, Some(1000));
        let a = ctx.try_reserve(600).unwrap();
        assert_eq!(ctx.memory_used(), 600);
        // 超配额：Err，且**不上账**（旧实现先加后比会把超限额留在账上）
        assert!(matches!(
            ctx.try_reserve(600).unwrap_err(),
            QsdbError::QueryMemoryLimit(_)
        ));
        assert_eq!(ctx.memory_used(), 600, "失败的预留不得入账");
        // RAII：Drop 即归还
        drop(a);
        assert_eq!(ctx.memory_used(), 0);
        let _b = ctx.try_reserve(400).unwrap();
    }

    #[test]
    fn reservation_grow_shrink_and_drop() {
        let ctx = QueryContext::new("q4", None, Some(1000));
        let mut r = ctx.try_reserve(100).unwrap();
        r.grow(300).unwrap();
        assert_eq!(ctx.memory_used(), 400);
        assert_eq!(r.bytes(), 400);
        // 增长失败不改动本预留
        assert!(r.grow(10_000).is_err());
        assert_eq!(r.bytes(), 400);
        r.shrink(150);
        assert_eq!(ctx.memory_used(), 250);
        r.shrink(10_000); // 过度归还 saturating，不为负
        assert_eq!(ctx.memory_used(), 0);
    }

    #[test]
    fn global_pool_rejects_over_budget() {
        use crate::memory::MemoryPool;
        let pool = Arc::new(MemoryPool::new(1000));
        let ctx = QueryContext::new_with_pool("q5", None, None, Some(Arc::clone(&pool)));
        let _a = ctx.try_reserve(800).unwrap();
        assert_eq!(pool.exec_used(), 800);
        // 全局不够（无缓存可挤兑）→ MemoryPressure（503 可重试），而非配额错
        let other = QueryContext::new_with_pool("q6", None, None, Some(Arc::clone(&pool)));
        assert!(matches!(
            other.try_reserve(400).unwrap_err(),
            QsdbError::MemoryPressure(_)
        ));
        assert_eq!(pool.exec_used(), 800, "被拒的预留不入账");
        // 释放后立刻可再用（闸门不是熔断）
        drop(_a);
        assert_eq!(pool.exec_used(), 0);
        let _c = other.try_reserve(400).unwrap();
        assert_eq!(pool.exec_used(), 400);
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
