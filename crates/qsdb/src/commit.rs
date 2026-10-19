//! 提交事件钩子（**T2**）：写入提交之后、**锁外**通知订阅方。
//!
//! 设计见 `提交事件订阅设计稿` §1。消费方是 qsdb-sync ——
//! 他们今天靠**轮询** `partition_stats` 发现增量，有了这个钩子就能换成 push
//! （RPO 秒级 → 毫秒级）。

use std::sync::Arc;

use crate::types::Literal;

/// 提交的种类。与 `write` / `compact` 的实际动作一一对应。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum CommitKind {
    /// 追加段（`append`）
    Append,
    /// 整分区替换（`overwrite_partition` / `…_cas`）
    Overwrite,
    // ⚠️ `Compact` **暂不在本枚举里**：它要触发得把钩子穿进 `compact.rs` 的
    // 逐分区提交点（那两层的参数表里现在没有它）。**故意先不加** —— 定义一个
    // 永远不触发的 kind，等于给消费方一个"我会收到 Compaction"的假承诺。
    // 补它时再加（枚举是 `non_exhaustive`，加了不打断任何消费方）。
    /// 清空（`truncate_table` / 整表 `DELETE`）
    Clear,
}

/// 一次**分区级**提交。
///
/// ⚠️ **粒度是分区**：`partition_values` 与 `generation` 本来就是分区级的；
/// 一次跨 N 个分区的 `append` 会发 **N 个**事件 —— 订阅方据此可以**按分区**记进度，
/// 不必整表重来。
///
/// ⚠️ **不带 `new_segments`**（`主备与同步设计稿` 的旧稿里有这一项）：
/// ① 要把它带出来得让 `Vec<SegmentEntry>` 穿过 `WriteResult`，而后者现在是 `Copy`
/// —— 加 `Vec` 就把 `Copy` 破了（嵌入方会跟着炸）；② 更要紧的是语义：事件里的段清单
/// **可能已经过期**（紧接着又有一次提交），订阅方拿到通知后**本来就要读 manifest**
/// 才是它的一致性来源。省掉的只是一次目录 list，不值得把事件做胖、把 API 做脆。
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct CommitEvent {
    pub table: String,
    /// 分区键值（无分区表 = 空）
    pub partition_values: Vec<Literal>,
    /// 提交**之后**的分区 generation（订阅方据此判"我落后了没有"）
    pub generation: u64,
    pub kind: CommitKind,
}

/// 提交钩子。
///
/// # ★ 它是加速器，**不是真相来源**
///
/// 这条比接口本身重要，三条都是从它推出来的：
///
/// 1. **回调 panic 会被捕获并丢弃。** 反过来做（让它传播）**更坏**：数据已经
///    **提交成功了**，调用方却收到一个错误 ⇒ 它会去重试或回滚 ⇒ 一次"静默改语义"。
///    **提交成功就必须报成功**，这是不可让步的。
/// 2. **回调慢会拖慢写入**（同步触发；虽在锁外，但仍在这条写路径上）。要做 IO
///    请在自己的回调里丢队列 —— 那是调用方的事，内核不替你缓冲。
/// 3. 因此**订阅端必须带游标**、消费方**应当保留一个慢轮询/对账兜底**。
///    **任何"正确性依赖钩子必达"的设计都是错的。**
///
/// `None`（默认）= 一次 `Option` 判断，零开销。
#[derive(Clone)]
pub struct CommitHook(Arc<dyn Fn(&CommitEvent) + Send + Sync>);

impl CommitHook {
    pub fn new(f: impl Fn(&CommitEvent) + Send + Sync + 'static) -> Self {
        Self(Arc::new(f))
    }

    /// 触发。**捕获 panic**（理由见类型文档第 1 条）——调用方拿到的是"通知发过了"，
    /// 不是"通知成功了"；内核不向上报回调的错，因为它**没有第二条路可走**：
    /// 报了就等于把已提交的写说成失败。
    pub fn fire(&self, ev: &CommitEvent) {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| (self.0)(ev)));
    }
}

/// 手写 `Debug`：`dyn Fn` 不是 `Debug`，而 `QsdbOptions` 是 `Debug + Clone`
/// （嵌入方在用的公开类型，不能因为加个钩子就把它的 derive 拆了）。
impl std::fmt::Debug for CommitHook {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("CommitHook(<fn>)")
    }
}
