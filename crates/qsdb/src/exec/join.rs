//! HashJoin：build 侧（右表）建哈希表 → probe 侧（左表）流式探测。
//!
//! - 等值条件（多键 AND）；INNER / LEFT OUTER
//! - NULL 键不匹配（SQL 语义：NULL = NULL 非 TRUE）
//! - build 侧全量物化（ctx 内存记账）；probe 侧流式
//! - 输出列序：左表列 ++ 右表列

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, StringArray, TimestampNanosecondArray,
    UInt32Array,
};
use arrow::compute::{concat, take};
use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::{RecordBatch, RecordBatchOptions};

use crate::error::{QsdbError, Result};
use crate::expr::PhysExpr;
use crate::expr::eval;
use crate::query::QueryContext;

use super::ExecNode;

/// 连接类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    LeftOuter,
    /// **FULL OUTER**：匹配的对 + 左未匹配（左段有值、右段补 NULL）
    /// + **右未匹配**（右段有值、左段补 NULL）。
    ///
    /// 与前两者的关键差别：probe 阶段要**记录哪些 build 行匹配过**
    /// （`HashJoinExec::matched`），probe 跑完后补一轮"没匹配上的 build 行"。
    FullOuter,
    /// Semi（IN / EXISTS）：左行命中 build 键集 ≥1 次则发射（至多一次，
    /// 不重复）；NULL 键不命中（SQL：NULL IN set = UNKNOWN）
    Semi,
    /// Anti（NOT EXISTS）：左行零命中则发射；NULL 键零命中 → 发射
    Anti,
    /// AntiIn（NOT IN，三值语义）：build 含 NULL 键 → 结果恒空；
    /// probe NULL 键 → 该行丢弃（NOT IN 对 NULL 的 SQL 语义）
    AntiIn,
}

impl JoinType {
    /// semi/anti 族：输出 = 左 schema；build 侧只需键存在集。
    pub fn is_semi_family(self) -> bool {
        matches!(self, Self::Semi | Self::Anti | Self::AntiIn)
    }
}

/// 笛卡尔积（非等值 INNER JOIN 的底座）：右表首拉时物化，左批逐批 × 右表展开。
/// 一次 `next()` 最多吐多少行。
///
/// ⚠️ **这个常量同时管两件事，别改小聪明**：
/// ① **取消/超时的生效延迟**：展开了却不发出去的行，中间没有任何 `ctx.check()`
///    机会（2026-10-15 实测：`n_left × n_right` 一次性展开时，7000×7000 的
///    取消要 1.9s 才生效、基线 5.8s —— 取消形同摆设）；
/// ② **峰值内存**：25M 行 × 列宽，一次 `take` 出来就是几百 MB 的瞬时物化。
/// 64K 行 ≈ 一次 `take` 的量级（同 `batch_size` 一个数量级），两头都够用。
const CROSS_CHUNK_ROWS: usize = 64 * 1024;

/// 块内**再**查一次取消的间隔（块本身也不许长到"点不动"）。
///
/// 只靠"每块一次"在极窄的列上有风险（64K 行的 `take` 是 µs 级，但若将来有人
/// 把块调大、或 `build` 侧宽到每行几百字节，块内就可能几十毫秒起步）。
const CROSS_CHECK_EVERY: usize = 16 * 1024;

/// 当前 probe 批的展开进度（跨 `next()` 调用保持）。
struct CrossCursor {
    /// 左（probe）批。
    batch: RecordBatch,
    /// 右表行数（展开前定下，免得每块重算）。
    n_right: usize,
    /// 已发射的 pair 数。
    cursor: usize,
    /// 本批总 pair 数 = `batch.num_rows() × n_right`。
    total: usize,
}

pub struct CrossJoinExec {
    probe: Box<dyn ExecNode>,
    build: Option<Box<dyn ExecNode>>,
    build_state: Option<RecordBatch>,
    /// 物化表的内存预留（随 `build_state` 存活到节点销毁）
    build_res: Option<crate::memory::Reservation>,
    schema: SchemaRef,
    ctx: Arc<QueryContext>,
    /// 在飞的 probe 批（`None` = 该去取下一批）。见 [`CROSS_CHUNK_ROWS`]。
    cur: Option<CrossCursor>,
}

impl CrossJoinExec {
    pub fn new(probe: Box<dyn ExecNode>, build: Box<dyn ExecNode>, ctx: Arc<QueryContext>) -> Self {
        let fields: Vec<arrow::datatypes::FieldRef> = probe
            .schema()
            .fields()
            .iter()
            .cloned()
            .chain(build.schema().fields().iter().cloned())
            .collect();
        let schema = Arc::new(arrow::datatypes::Schema::new(fields));
        Self {
            probe,
            build: Some(build),
            build_state: None,
            build_res: None,
            schema,
            ctx,
            cur: None,
        }
    }

    fn materialize_build(&mut self) -> Result<()> {
        if self.build_state.is_some() {
            return Ok(());
        }
        let Some(mut build) = self.build.take() else {
            return Ok(());
        };
        // 右 schema 从 build 节点自身（首批出现前用它 schema 的空批次形状不可行——
        // 直接收集批，再以首批 schema concat；无批则空表）
        //
        // 内存：预留随数据**存活**（RAII）——物化表常驻 `build_state`，
        // 故预留也常驻 `build_res`，节点销毁即归还（旧实现收完立刻 release，
        // 等于物化的表白占内存不记账）。
        let mut res = self.ctx.try_reserve(0)?; // 空预留，随增长扩容
        let mut batches = Vec::new();
        while let Some(b) = build.next() {
            self.ctx.check()?;
            let x = b?;
            res.grow(crate::memory::batch_bytes(&x))?;
            batches.push(x);
        }
        let total = res.bytes();
        res.grow(total)?; // concat 期间瞬时双份（峰值真实存在，就该记账）
        let table = match batches.split_first() {
            Some((first, _)) => crate::exec::spill::concat_batches(&batches, &first.schema())?,
            None => RecordBatch::new_empty(build.schema()),
        };
        res.shrink(total); // 源批已随 Vec 释放
        self.build_res = Some(res);
        self.build_state = Some(table);
        // build 侧生产端已退出 → 此刻预取 probe 侧是安全的（不会互锁），
        // 且把懒启动丢掉的"重叠预取"补回来（实测宽 join 物化 ~10%）
        self.probe.prefetch();
        Ok(())
    }
}

impl ExecNode for CrossJoinExec {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn next(&mut self) -> Option<Result<RecordBatch>> {
        if let Err(e) = self.ctx.check() {
            return Some(Err(e));
        }
        if let Err(e) = self.materialize_build() {
            return Some(Err(e));
        }
        loop {
            // 上一批还有没发完的 → 接着发（**不重算索引**，只推游标）
            if let Some(c) = &self.cur {
                if c.cursor < c.total {
                    return Some(self.emit_chunk());
                }
                self.cur = None;
            }
            let batch = match self.probe.next()? {
                Ok(b) => b,
                Err(e) => return Some(Err(e)),
            };
            if batch.num_rows() == 0 {
                continue;
            }
            let n_right = self.build_state.as_ref().map(|b| b.num_rows()).unwrap_or(0);
            self.cur = Some(CrossCursor {
                total: batch.num_rows() * n_right,
                batch,
                n_right,
                cursor: 0,
            });
            // 右表为空 ⇒ `total = 0` ⇒ 下一轮直接丢掉游标、取下一批 probe
            // （cross join 与空集连接恒为空，与原实现同义）
        }
    }
}

impl CrossJoinExec {
    /// 展开**一块**（最多 [`CROSS_CHUNK_ROWS`] 行）并射出。
    ///
    /// 索引按 `(l, r) = (k / n_right, k % n_right)` 递增推进，不预先物化整批的
    /// 索引向量（那本身就是 `2 × 4 字节 × 总对数` 的瞬时内存）。
    fn emit_chunk(&mut self) -> Result<RecordBatch> {
        let Self {
            cur,
            build_state,
            schema,
            ctx,
            ..
        } = self;
        let c = cur.as_mut().expect("emit_chunk 只在有待发批时调用");
        let n_right = c.n_right;
        debug_assert!(n_right > 0, "空右表不该走到展开（total = 0）");
        let start = c.cursor;
        let end = (start + CROSS_CHUNK_ROWS).min(c.total);
        let n = end - start;
        let mut li: Vec<u32> = Vec::with_capacity(n);
        let mut ri: Vec<u32> = Vec::with_capacity(n);
        let mut l = start / n_right;
        let mut r = start % n_right;
        for k in 0..n {
            if k % CROSS_CHECK_EVERY == 0 {
                // 块内也不许长到"点不动"：见 `CROSS_CHECK_EVERY`
                ctx.check()?;
            }
            li.push(l as u32);
            ri.push(r as u32);
            r += 1;
            if r == n_right {
                r = 0;
                l += 1;
            }
        }
        c.cursor = end;
        let li = UInt32Array::from(li);
        let ri = UInt32Array::from(ri);
        let mut cols: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
        for i in 0..c.batch.num_columns() {
            cols.push(
                take(c.batch.column(i), &li, None)
                    .map_err(|e| QsdbError::invalid(format!("cross take: {e}")))?,
            );
        }
        match build_state {
            Some(build) => {
                for i in 0..build.num_columns() {
                    cols.push(
                        take(build.column(i), &ri, None)
                            .map_err(|e| QsdbError::invalid(format!("cross take: {e}")))?,
                    );
                }
            }
            // 右表为空批：补全空列（本分支实际到不了，`total = 0`；留着是给
            // "schema 有右列但 build 为空"的形状一个不 panic 的兜底）
            None => {
                for f in schema.fields().iter().skip(c.batch.num_columns()) {
                    cols.push(arrow::array::new_null_array(f.data_type(), n));
                }
            }
        }
        RecordBatch::try_new_with_options(
            Arc::clone(schema),
            cols,
            &RecordBatchOptions::new().with_row_count(Some(n)),
        )
        .map_err(|e| QsdbError::invalid(format!("cross output: {e}")))
    }
}

/// 哈希连接执行节点。
pub struct HashJoinExec {
    probe: Box<dyn ExecNode>,
    build: Box<dyn ExecNode>,
    probe_keys: Vec<PhysExpr>,
    build_keys: Vec<PhysExpr>,
    join_type: JoinType,
    schema: SchemaRef,
    ctx: Arc<QueryContext>,
    /// build 阶段产物（首拉时构建）
    build_state: Option<BuildState>,
    /// build 侧物化表 + 键索引的内存预留（随 `build_state` 存活到节点销毁）
    build_res: Option<crate::memory::Reservation>,
    /// semi/anti 族 build 产物（与 build_state 互斥，按 join_type 选）
    exists_state: Option<ExistsState>,
    probe_done: bool,
    // ── 溢出态（build 侧要不到额度时启用）：**分区块 hash join** ──
    /// build 侧按 join key 分区落盘
    spill_build: Option<crate::exec::agg_spill::PartSpill>,
    /// probe 侧按 join key 分区落盘（同一键必然同分区 → 分区内连接即完整）
    spill_probe: Option<crate::exec::agg_spill::PartSpill>,
    /// 当前正在处理的分区号
    spill_part: usize,
    /// 当前分区的 build 索引（每分区重建一次）
    cur_build: Option<BuildState>,
    /// 当前分区待探测的行（从磁盘读回，逐批流式）
    cur_probe: Option<std::vec::IntoIter<RecordBatch>>,
    /// **只需物化这些输出位置**（`None` = 全部）。见 `LogicalPlan::Join::needed`：
    /// 不在表里的位置**填 NULL**（列还在、schema 不变，只是没人读）。
    needed: Option<Arc<Vec<u32>>>,
    /// 键之外的 ON 条件（对**匹配对**求值）。见 [`HashJoinExec::with_residual`]。
    residual: Option<PhysExpr>,
    /// **FULL OUTER 专用**：build 侧每行是否匹配过（长度 = build 行数）。
    ///
    /// `None` = 非 FULL（不必记）。probe 阶段由 `join_batch` 置位，
    /// probe 跑完后据此补发"没匹配上的 build 行"（见 [`HashJoinExec::full_tail`]）。
    matched: Option<Vec<bool>>,
}

/// **build 侧物化数据：保留分批，不 concat**。
///
/// ## 为什么不做成一张大表（2026-10-15 改）
///
/// 原实现把所有批 `concat_batches` 成一张大表 —— 那一刻**源批与新表同时活着**，
/// 瞬时 **2×**。后果实测过：右表 32MB 配 64MB 配额（**逻辑上绰绰有余**）也会因为
/// 这 2× 而触发溢出（INNER/LEFT 落盘白跑一趟）或**直接报错**（FULL 没有溢出通道）。
///
/// 改成"保留分批 + 跨批全局行号"后，常驻 = **1×**，那笔峰值**根本不存在**。
/// 取数走 `arrow::compute::interleave`（每种类型都有特化核，不是慢路径）；
/// 单批时仍走原来的 `take` 快路径。
///
/// ⚠️ **行号口径**：整个 join 内部用的行号一律是**跨批全局行号**
/// （索引、`join_batch`、`full_tail` 的匹配位图三处必须一致）。
struct BuildSide {
    batches: Vec<RecordBatch>,
    /// `base[i]` = 第 i 批第一行的全局行号；末项 = 总行数（长度 = 批数 + 1）
    base: Vec<usize>,
    n_rows: usize,
    /// **build 输入的 schema**（不是批的）：build 侧可能**一批都没有**，
    /// 那时仍要能回答"这一列是什么类型"（`column` / `null_like` 用）。
    schema: SchemaRef,
}

impl BuildSide {
    fn new(batches: Vec<RecordBatch>, schema: SchemaRef) -> Self {
        let mut base = Vec::with_capacity(batches.len() + 1);
        let mut acc = 0usize;
        base.push(0);
        for b in &batches {
            acc += b.num_rows();
            base.push(acc);
        }
        Self {
            batches,
            base,
            n_rows: acc,
            schema,
        }
    }

    fn num_rows(&self) -> usize {
        self.n_rows
    }

    fn num_columns(&self) -> usize {
        self.schema.fields().len()
    }

    /// 物化字节数（记账用；与 `crate::memory::batch_bytes` 同口径，逐批求和）。
    fn bytes(&self) -> u64 {
        self.batches.iter().map(crate::memory::batch_bytes).sum()
    }

    /// 该列的**一个代表数组**（只用来取类型造 NULL；空 build 侧也能回答）。
    fn column(&self, c: usize) -> ArrayRef {
        match self.batches.first() {
            Some(b) => b.column(c).clone(),
            None => arrow::array::new_null_array(self.schema.field(c).data_type(), 0),
        }
    }

    /// 全局行号 → `(批号, 批内行号)`。`base` 单调 ⇒ 二分。
    fn row_of(&self, global: usize) -> (usize, usize) {
        let upto = &self.base[..self.base.len() - 1];
        let b = match upto.binary_search(&global) {
            Ok(i) => i,
            Err(i) => i - 1, // 落在 base[i-1]..base[i] 之间
        };
        (b, global - self.base[b])
    }

    /// 按**全局行号**取列（`interleave` 一次成型，不 concat 中间结果）。
    fn take(&self, c: usize, idx: &UInt32Array) -> Result<ArrayRef> {
        if self.batches.is_empty() {
            // **一批都没有**（右表为空）：这里只可能是"取 0 行"（行号来自空索引），
            // 按该列的**类型**造等长 NULL 数组即可 —— 直接走 `interleave` 会报
            // "requires input of at least one array"（FULL + 空右表那次实测踩到）。
            return Ok(arrow::array::new_null_array(
                self.schema.field(c).data_type(),
                idx.len(),
            ));
        }
        if self.batches.len() == 1 {
            // 单批：走原来的 `take` 快路径（不构造 (批号,行号) 对）
            return take(self.batches[0].column(c), idx, None)
                .map_err(|e| QsdbError::invalid(format!("join take: {e}")));
        }
        let arrays: Vec<&dyn Array> = self.batches.iter().map(|b| b.column(c).as_ref()).collect();
        let pairs: Vec<(usize, usize)> = (0..idx.len())
            .map(|i| self.row_of(idx.value(i) as usize))
            .collect();
        arrow::compute::interleave(&arrays, &pairs)
            .map_err(|e| QsdbError::invalid(format!("join interleave: {e}")))
    }
}

struct BuildState {
    /// build 侧数据（**分批**，见 [`BuildSide`]）
    table: BuildSide,
    /// 键索引：单整数键走 i64 免编码（probe 每行省一次字节编码 + 字节哈希）
    index: HashIndex,
}

/// 键 → build 行号列表。单整数键（Int/Date/Boolean/Timestamp）用 i64 直查。
enum HashIndex {
    I64(I64Map<Vec<u32>>),
    Bytes(BytesMap<Vec<u32>>),
}

impl HashIndex {
    /// 单整数键判定（与 semi-join 快路径同款）。
    fn int_ok(keys: &[PhysExpr]) -> bool {
        keys.len() == 1
            && matches!(
                keys[0].data_type(),
                crate::types::LogicalType::Int8
                    | crate::types::LogicalType::Int16
                    | crate::types::LogicalType::Int32
                    | crate::types::LogicalType::Int64
                    | crate::types::LogicalType::Date
            )
    }
}

/// semi/anti 族 build 产物：存在性三形态（按键形状自动选优——「别人不做
/// 的偏门」：IN/EXISTS 的键 90% 是单个整数列，走免编码快路径；稠密整数
/// 域再退化为位图探测）。
pub struct ExistsState {
    keys: ExistsKeys,
    /// build 侧出现过 NULL 键（NOT IN 三值语义用）
    has_null: bool,
    /// 存在集内存预留（**此前只有 `ctx.check()`：不记账 → 无界增长**）。
    /// 随状态存活——状态被 Arc 共享给并行分片，故预留也跟着 `Arc<ExistsState>` 走。
    #[allow(dead_code)] // 只为 Drop（RAII 归还），不直接读
    res: crate::memory::Reservation,
}

enum ExistsKeys {
    /// 通用：多键/字符串/浮点 → 键字节编码存在集
    Bytes(BytesSet),
    /// 单整数键（Int8..64/Date32/Boolean）→ 免编码 i64 存在集（快哈希）
    I64(std::collections::HashSet<i64, BuildI64Hasher>),
    /// 单整数键且域稠密（max−min ≤ 16M 位 = 2MB）→ 位图探测：
    /// probe = 单 bit 测试（无哈希/无分配/缓存友好；分区键 IN 的标准形态）
    Bitmap { min: i64, bits: Vec<u64> },
}

/// 单整数键取值（Int8..64 / Date32 / Boolean → i64）；其他类型 None。
fn int_key_value(a: &ArrayRef, row: usize) -> Option<i64> {
    use arrow::datatypes::DataType;
    macro_rules! as_i64 {
        ($t:ident) => {{ a.as_any().downcast_ref::<$t>().unwrap().value(row) as i64 }};
    }
    Some(match a.data_type() {
        DataType::Int8 => as_i64!(Int8Array),
        DataType::Int16 => as_i64!(Int16Array),
        DataType::Int32 => as_i64!(Int32Array),
        DataType::Int64 => as_i64!(Int64Array),
        // Date32/Timestamp 物理上 i32/i64 但 arrow 数组类型不同——各自 downcast
        DataType::Date32 => a.as_any().downcast_ref::<Date32Array>().unwrap().value(row) as i64,
        DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, _) => a
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap()
            .value(row),
        DataType::Boolean => i64::from(
            a.as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(row),
        ),
        _ => return None,
    })
}

impl ExistsState {
    fn contains(&self, v: i64) -> bool {
        match &self.keys {
            ExistsKeys::I64(set) => set.contains(&v),
            ExistsKeys::Bitmap { min, bits } => {
                let b = v.wrapping_sub(*min);
                if b < 0 || b as u64 >= bits.len() as u64 * 64 {
                    return false;
                }
                bits[(b >> 6) as usize] & (1u64 << (b & 63)) != 0
            }
            ExistsKeys::Bytes(_) => unreachable!("int path on bytes state"),
        }
    }

    fn contains_bytes(&self, k: &[u8]) -> bool {
        match &self.keys {
            ExistsKeys::Bytes(set) => set.contains(k),
            _ => unreachable!("bytes path on int state"),
        }
    }
}

impl HashJoinExec {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        probe: Box<dyn ExecNode>,
        build: Box<dyn ExecNode>,
        probe_keys: Vec<PhysExpr>,
        build_keys: Vec<PhysExpr>,
        join_type: JoinType,
        ctx: Arc<QueryContext>,
    ) -> Self {
        // 输出 schema：semi/anti 族 = 仅左（probe）；INNER/LEFT = 左 ++ 右
        let fields: Vec<arrow::datatypes::FieldRef> = if join_type.is_semi_family() {
            probe.schema().fields().iter().cloned().collect()
        } else {
            probe
                .schema()
                .fields()
                .iter()
                .cloned()
                .chain(build.schema().fields().iter().cloned())
                .collect()
        };
        let schema = Arc::new(arrow::datatypes::Schema::new(fields));
        Self {
            probe,
            build,
            probe_keys,
            build_keys,
            join_type,
            schema,
            ctx,
            build_state: None,
            build_res: None,
            exists_state: None,
            probe_done: false,
            spill_build: None,
            spill_probe: None,
            spill_part: 0,
            cur_build: None,
            cur_probe: None,
            needed: None,
            residual: None,
            matched: None,
        }
    }

    /// 键之外的 ON 条件（对**匹配对**求值）。见 `LogicalPlan::Join::residual`。
    ///
    /// ⚠️ 必须在 join 内部求值，**不能**由调用方提到 `WHERE`：LEFT 的未匹配行
    /// 要补 NULL 保留，提到 WHERE 会把它们整体丢掉（静默少行）。
    pub fn with_residual(mut self, residual: Option<PhysExpr>) -> Self {
        self.residual = residual;
        self
    }

    /// 只物化这些输出位置（不在表里的填 NULL）。见 `LogicalPlan::Join::needed`。
    pub fn with_needed(mut self, needed: Option<Arc<Vec<u32>>>) -> Self {
        self.needed = needed;
        self
    }

    fn build_phase(&mut self) -> Result<()> {
        if self.join_type.is_semi_family() {
            return self.build_phase_exists();
        }
        if self.build_state.is_some() {
            return Ok(());
        }
        // 诊断：**建侧到底值多少**（分片路径里它每片各来一次）。
        // 值是"分片优化该不该做"的唯一依据——没有它就只能猜（猜错过一次）。
        let __t0 = std::time::Instant::now();
        // 物化右表：预留随表存活（RAII），节点销毁即归还
        let mut res = self.ctx.try_reserve(0)?;
        let mut batches = Vec::new();
        while let Some(b) = self.build.next() {
            self.ctx.check()?;
            let x = b?;
            if res.grow(crate::memory::batch_bytes(&x)).is_err() {
                // **要不到额度 → 转分区块 hash join**（而不是把查询打死）。
                // 注意：**本批也要进分区**——它就是压垮内存的那一批，
                // 只交已收集的 `batches` 会静默少一个批的行（实测丢 2000 行）。
                batches.push(x);
                self.enter_spill(batches)?;
                return Ok(());
            }
            batches.push(x);
        }
        // **不 concat**：分批保留 + 跨批全局行号。原先这里 `concat_batches` 出
        // 一张大表，那一刻源批与新表**同时活着**（瞬时 2×）—— 右表 32MB 配 64MB
        // 配额（逻辑上绰绰有余）也会被这 2× 顶掉，白跑一趟溢出（INNER/LEFT）
        // 或直接报错（FULL 无溢出通道）。见 [`BuildSide`]。
        // **能 concat 就 concat**（2026-10-15 A/B 实测的结论）：
        // 保留分批会让 probe 取数走 `interleave` —— 那是**热路径**，.14 上
        // `complex_three_way` 0.321s → 0.350s（**+9%**，三次一致、不是噪声），
        // 换来的只是内存。所以：
        //   - 额度够付这份峰值 ⇒ 照旧 concat（单批 ⇒ `take` 快路径，性能与改前一致）；
        //   - 付不起 ⇒ **保留分批**（正是原先会白跑一趟溢出/直接报错的那种情形）。
        // 于是"性能不变、只是不再因为 2× 峰值而失败"——两头都占到。
        let schema = self.build.schema();
        let peak = res.bytes();
        let table = if res.grow(peak).is_ok() {
            let t = concat_batches(&batches, &schema)?;
            res.shrink(peak); // 源批随 `batches` 释放，峰值回落
            BuildSide::new(vec![t], schema)
        } else {
            BuildSide::new(batches, schema)
        };

        // 键哈希索引（**逐批构建，行号加全局偏移** —— 与 `BuildSide` 同一口径）
        let index = build_index(&table, &self.build_keys)?;
        // 键索引（键值 + 行号列表）同样常驻，一并记账：粗估每行 16B
        // （键拷贝 + 行号），量级正确即可——目的是让预算覆盖它，不追求逐字节。
        //
        // **要不到这份额度不是终止理由**（同上面的 concat 峰值）：索引是 build 侧
        // 常驻的第二块，装不下说明"单分区 build 侧太大"，正解是分得更细——交
        // `enter_spill` 让每个分区各自物化（分区小了，行与索引一起变小）。
        // 回归：本行原为 `res.grow(...)?`，小全局预算下 join 被直接打死。
        if res.grow(table.num_rows() as u64 * 16).is_err() {
            self.enter_spill(table.batches)?; // `res` 是局部量，随本次返回归还
            return Ok(());
        }
        if std::env::var("QSDB_JOIN_PROF").is_ok() {
            eprintln!(
                "[join-build] {} 行 / {} 列，耗时 {:.1}ms",
                table.num_rows(),
                table.num_columns(),
                __t0.elapsed().as_secs_f64() * 1000.0
            );
        }
        self.build_res = Some(res);
        self.build_state = Some(BuildState { table, index });
        self.probe.prefetch(); // 见 CrossJoinExec：build 侧已退出，预取 probe
        Ok(())
    }
}

impl HashJoinExec {
    /// semi/anti 族 build：流式消费右表逐批求键 → 存在集。
    fn build_phase_exists(&mut self) -> Result<()> {
        if self.exists_state.is_some() {
            return Ok(());
        }
        let schema = self.build.schema();
        let node = std::mem::replace(&mut self.build, Box::new(EmptyExec::new(schema)));
        let keys = std::mem::take(&mut self.build_keys);
        let state = prepare_exists_state(node, &keys, &self.ctx)?;
        self.build_keys = keys;
        self.exists_state = Some(Arc::try_unwrap(state).unwrap_or_else(|arc| {
            let _ = arc;
            unreachable!("hash join owns the only ref")
        }));
        Ok(())
    }
}

/// 空源（build 节点被 move 走后的占位）。
struct EmptyExec {
    schema: SchemaRef,
}
impl EmptyExec {
    fn new(schema: SchemaRef) -> Self {
        Self { schema }
    }
}
impl ExecNode for EmptyExec {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
    fn next(&mut self) -> Option<Result<RecordBatch>> {
        None
    }
}

/// 独立准备存在集（聚合融合路径共享：build 一次、多分片探测）。
/// 右表不物化、不 concat；单整数键先走 i64 累积，扫完按域稠密度
/// 选 HashSet 或位图（2MB 位图 = 稠密整数域的 O(1) 无哈希探测）。
pub fn prepare_exists_state(
    mut build: Box<dyn ExecNode>,
    build_keys: &[PhysExpr],
    ctx: &Arc<QueryContext>,
) -> Result<Arc<ExistsState>> {
    let mut has_null = false;
    // 单整数键快路径累积（min/max 供位图判定）
    let mut ints: Option<Vec<i64>> = if build_keys.len() == 1 {
        Some(Vec::new())
    } else {
        None
    };
    let mut bytes: BytesSet = BytesSet::default();
    let mut buf: Vec<u8> = Vec::new();
    while let Some(b) = build.next() {
        ctx.check()?;
        let batch = b?;
        let mut key_cols: Vec<ArrayRef> = Vec::with_capacity(build_keys.len());
        for k in build_keys {
            key_cols.push(eval::eval(k, &batch)?);
        }
        if let (Some(acc), [only]) = (&mut ints, key_cols.as_slice()) {
            let mut fallback = false;
            for row in 0..batch.num_rows() {
                if only.is_null(row) {
                    has_null = true;
                    continue;
                }
                match int_key_value(only, row) {
                    Some(v) => acc.push(v),
                    // 非整数键（字符串/浮点等）→ 整体退回通用字节路径
                    None => {
                        fallback = true;
                        break;
                    }
                }
            }
            if fallback {
                ints = None;
                for r in 0..batch.num_rows() {
                    encode_join_key(&key_cols, r, &mut buf);
                    if buf.first() == Some(&0) {
                        has_null = true;
                    } else {
                        bytes.insert(buf.clone());
                    }
                }
            }
        } else {
            for row in 0..batch.num_rows() {
                encode_join_key(&key_cols, row, &mut buf);
                if buf.first() == Some(&0) {
                    has_null = true;
                } else {
                    bytes.insert(buf.clone());
                }
            }
        }
    }
    // 存在集记账（容器容量口径；Bitmap 的 bits 是实打实的分配）
    let mut res = ctx.try_reserve(0)?;
    let keys = match ints {
        Some(vals) if !vals.is_empty() => {
            let min = *vals.iter().min().unwrap();
            let max = *vals.iter().max().unwrap();
            let range = (max as i128 - min as i128) as u128;
            if range <= 16 * 1024 * 1024 {
                // 稠密域 → 位图（2MB 上限）
                let nbits = range as usize + 1;
                let mut bits = vec![0u64; nbits.div_ceil(64)];
                for v in vals {
                    let b = (v - min) as usize;
                    bits[b / 64] |= 1u64 << (b % 64);
                }
                res.grow(bits.capacity() as u64 * 8)?;
                ExistsKeys::Bitmap { min, bits }
            } else {
                let est = vals.capacity() as u64 * 24;
                res.grow(est)?;
                ExistsKeys::I64(vals.into_iter().collect())
            }
        }
        Some(_) => ExistsKeys::I64(Default::default()), // 空集
        None => {
            let est = bytes.iter().map(|k| k.capacity() as u64 + 40).sum::<u64>()
                + bytes.capacity() as u64 * 8;
            res.grow(est)?;
            ExistsKeys::Bytes(bytes)
        }
    };
    Ok(Arc::new(ExistsState {
        keys,
        has_null,
        res,
    }))
}

/// 掩码探测（聚合融合路径）：probe 批 → 半连接布尔掩码（true = 行入选）。
/// 与 join_batch_semi 同语义：Semi 命中入选；Anti 零命中入选（NULL 键入选）；
/// AntiIn 零命中入选（NULL 键剔除；build 含 NULL → 全 false）。
pub fn semi_mask(
    state: &ExistsState,
    probe_keys: &[PhysExpr],
    batch: &RecordBatch,
    join_type: JoinType,
    ctx: &Arc<QueryContext>,
) -> Result<BooleanArray> {
    let _ = ctx;
    if join_type == JoinType::AntiIn && state.has_null {
        return Ok(BooleanArray::from(vec![Some(false); batch.num_rows()]));
    }
    let mut key_cols: Vec<ArrayRef> = Vec::with_capacity(probe_keys.len());
    for k in probe_keys {
        key_cols.push(eval::eval(k, batch)?);
    }
    let n = batch.num_rows();
    let mut mask: Vec<bool> = Vec::with_capacity(n);
    if let (ExistsKeys::I64(_) | ExistsKeys::Bitmap { .. }, [only]) =
        (&state.keys, key_cols.as_slice())
    {
        // 零拷贝：类型分派一次（downcast 出循环——逐行 downcast 在 40M 行
        // 级 probe 上是主要成本，同 P3 教训）
        macro_rules! probe_int {
            ($arr:ident) => {{
                let v = only.as_any().downcast_ref::<$arr>().unwrap();
                for row in 0..n {
                    let selected = if v.is_null(row) {
                        join_type == JoinType::Anti
                    } else {
                        let hit = state.contains(v.value(row) as i64);
                        match join_type {
                            JoinType::Semi => hit,
                            JoinType::Anti | JoinType::AntiIn => !hit,
                            _ => false,
                        }
                    };
                    mask.push(selected);
                }
            }};
        }
        match only.data_type() {
            arrow::datatypes::DataType::Int8 => probe_int!(Int8Array),
            arrow::datatypes::DataType::Int16 => probe_int!(Int16Array),
            arrow::datatypes::DataType::Int32 => probe_int!(Int32Array),
            arrow::datatypes::DataType::Int64 => probe_int!(Int64Array),
            arrow::datatypes::DataType::Date32 => probe_int!(Date32Array),
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, _) => {
                probe_int!(TimestampNanosecondArray)
            }
            arrow::datatypes::DataType::Boolean => probe_int!(BooleanArray),
            _ => {
                return Err(QsdbError::meta("semi mask: mixed key types"));
            }
        }
    } else {
        let mut buf: Vec<u8> = Vec::new();
        for row in 0..n {
            encode_join_key(&key_cols, row, &mut buf);
            let selected = if buf.first() == Some(&0) {
                join_type == JoinType::Anti
            } else {
                let hit = state.contains_bytes(buf.as_slice());
                match join_type {
                    JoinType::Semi => hit,
                    JoinType::Anti | JoinType::AntiIn => !hit,
                    _ => false,
                }
            };
            mask.push(selected);
        }
    }
    Ok(BooleanArray::from(mask))
}

impl HashJoinExec {
    /// 转入**分区块 hash join**：两侧都按 join key 分区落盘，再逐分区连接。
    ///
    /// 为什么两侧都要分区：同一键必须落同一分区（`route_by_join_key` 与 join
    /// 索引同用 `encode_join_key`），故分区内连接即完整——但这也意味着探测侧要
    /// 能**按分区重放**，而它是流式的、只能落盘一次（每行写一次读一次）。
    ///
    /// 覆盖范围：**INNER / LEFT OUTER**（build 侧要物化整行的大头就在这两类）。
    /// 不覆盖：semi/anti 族（build 侧只存**键集**、体量小一个量级，另走
    /// `build_phase_exists`，要不到额度仍报 `MEMORY_PRESSURE`）；非等值 join
    /// （`CrossJoinExec` 无键可分区）。
    fn enter_spill(&mut self, collected: Vec<RecordBatch>) -> Result<()> {
        // **FULL OUTER 不支持溢出**（本版限制，明确报错而不是假装能跑）：
        // 分区块 hash join 按**键**分区两侧，逐分区连接；但 FULL 的"右孤儿"要
        // 看**全量** build 侧的匹配标记，分区各发各的会**重复发射**同一个右孤儿
        // （每片都认为它没配上）。要支持得把标记做成跨分区的全局位图 +
        // 收尾阶段单独扫一遍所有分区 —— 那是另一件事，别在这里半吊子地做。
        if self.join_type == JoinType::FullOuter {
            return Err(QsdbError::MemoryPressure(
                "FULL OUTER JOIN 在内存不足时不支持落盘溢出（本版限制）——                  请抬高内存配额、或先减少 join 两侧的行数"
                    .to_string(),
            ));
        }
        let build_schema = self.build.schema();
        let probe_schema = self.probe.schema();
        let ctx = Arc::clone(&self.ctx);
        let (nparts, cap) = crate::exec::agg_spill::PartSpill::plan(&ctx);
        let mut bs = crate::exec::agg_spill::PartSpill::new(build_schema, &ctx, nparts, cap)?;
        let mut ps = crate::exec::agg_spill::PartSpill::new(probe_schema, &ctx, nparts, cap)?;
        let n = bs.nparts();
        // ① 已收集的 build 行 + 剩余 build 输入
        for b in collected {
            let parts = crate::exec::agg_spill::route_by_join_key(&b, &self.build_keys, n)?;
            bs.push_routed(&b, &parts)?;
        }
        while let Some(b) = self.build.next() {
            self.ctx.check()?;
            let b = b?;
            let parts = crate::exec::agg_spill::route_by_join_key(&b, &self.build_keys, n)?;
            bs.push_routed(&b, &parts)?;
        }
        // ② probe 侧全量（它原本是流式的，现在必须先落盘才能按分区重放）
        while let Some(b) = self.probe.next() {
            self.ctx.check()?;
            let b = b?;
            let parts = crate::exec::agg_spill::route_by_join_key(&b, &self.probe_keys, n)?;
            ps.push_routed(&b, &parts)?;
        }
        self.spill_build = Some(bs);
        self.spill_probe = Some(ps);
        self.spill_part = 0;
        Ok(())
    }

    /// 逐分区产出：读该分区的 build 行 → 建索引 → 流式探测该分区的 probe 行。
    fn spill_next(&mut self) -> Option<Result<RecordBatch>> {
        loop {
            // 进入下一分区：建索引 + 取回该分区的 probe 行
            if self.cur_build.is_none() {
                let nparts = self.spill_build.as_ref()?.nparts();
                if self.spill_part >= nparts {
                    self.probe_done = true;
                    return None;
                }
                let pi = self.spill_part;
                self.spill_part += 1;
                let bs = self.spill_build.as_mut()?;
                let rows = match bs.take_partition(pi) {
                    Ok(r) => r,
                    Err(e) => return Some(Err(e)),
                };
                if rows.is_empty() {
                    continue; // 该分区 build 侧为空 → 无匹配（LEFT 也无需发射）
                }
                // 同 `build_phase`：**能 concat 就 concat**（保 probe 侧快路径）。
                // 分区路径走 `reserve_spill`，额度口径不同 —— 这里先记账再决定。
                let schema = self.build.schema();
                let table = BuildSide::new(rows, schema);
                // 分区表也记账：正常情况 ≈ build 总量 / P，但**键倾斜**时可能
                // 仍很大——那时要不到额度就如实报错（不再往下切分），
                // 而不是让它无界涨上去。
                //
                // 走 `reserve_spill`（**只受全局预算约束**）：溢出路径的记账不
                // 吃每查询配额——否则小配额下"逃生通道"自己先被配额掐死
                // （P1 同样的坑，实测复现）。真正的硬上界是全局预算。
                let mut r = match self.ctx.reserve_spill(0) {
                    Ok(r) => r,
                    Err(e) => return Some(Err(e)),
                };
                let est = table.bytes() + table.num_rows() as u64 * 16;
                if let Err(e) = r.grow(est) {
                    return Some(Err(e));
                }
                let index = match build_index(&table, &self.build_keys) {
                    Ok(i) => i,
                    Err(e) => return Some(Err(e)),
                };
                self.build_res = Some(r);
                self.cur_build = Some(BuildState { table, index });
                let ps = self.spill_probe.as_mut()?;
                let rows = match ps.take_partition(pi) {
                    Ok(r) => r,
                    Err(e) => return Some(Err(e)),
                };
                self.cur_probe = Some(rows.into_iter());
            }
            // 当前分区还有探测行 → 连接
            let batch = match self.cur_probe.as_mut().and_then(|it| it.next()) {
                Some(b) => b,
                None => {
                    self.cur_build = None;
                    self.cur_probe = None;
                    continue; // 下一分区（build 索引与预留随之释放）
                }
            };
            if batch.num_rows() == 0 {
                continue;
            }
            let out_schema = Arc::clone(&self.schema);
            let probe_keys = self.probe_keys.clone();
            let needed = self.needed.clone();
            let residual = self.residual.clone();
            let join_type = self.join_type;
            let build = self.cur_build.as_ref()?;
            match join_batch(
                &batch,
                build,
                &probe_keys,
                &out_schema,
                join_type,
                needed.as_ref().map(|v| v.as_slice()),
                residual.as_ref(),
                // 溢出分区路径不传掩码：FULL 在这里被 `enter_spill` 挡下（见那里的说明），
                // 所以走到这一步的 `matched` 必然是 None —— 传 None 是**如实**的
                None,
            ) {
                Ok(Some(b)) => return Some(Ok(b)),
                Ok(None) => continue,
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

impl ExecNode for HashJoinExec {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn next(&mut self) -> Option<Result<RecordBatch>> {
        if self.probe_done {
            return None;
        }
        // 溢出态：逐分区"建索引 → 探测"，两侧的行都已落盘
        if self.spill_build.is_some() {
            return self.spill_next();
        }
        if let Err(e) = self.build_phase() {
            return Some(Err(e));
        }
        // build_phase 可能刚转入溢出态（本次 build 要不到额度）
        if self.spill_build.is_some() {
            return self.spill_next();
        }
        let join_type = self.join_type;
        let out_schema = Arc::clone(&self.schema);
        let probe_keys = self.probe_keys.clone();
        let needed = self.needed.clone();
        if join_type.is_semi_family() {
            let exists = self.exists_state.as_ref().unwrap();
            loop {
                if let Err(e) = self.ctx.check() {
                    return Some(Err(e));
                }
                let batch = self.probe.next()?;
                let batch = match batch {
                    Ok(b) => b,
                    Err(e) => return Some(Err(e)),
                };
                if batch.num_rows() == 0 {
                    continue;
                }
                match join_batch_semi(&batch, exists, &probe_keys, &out_schema, join_type) {
                    Ok(Some(b)) => return Some(Ok(b)),
                    Ok(None) => continue,
                    Err(e) => return Some(Err(e)),
                }
            }
        }
        // FULL OUTER：建 build 侧匹配标记（probe 阶段置位、跑完据此补发未匹配行）
        if join_type == JoinType::FullOuter && self.matched.is_none() {
            let n = self
                .build_state
                .as_ref()
                .map(|b| b.table.num_rows())
                .unwrap_or(0);
            self.matched = Some(vec![false; n]);
        }
        loop {
            if let Err(e) = self.ctx.check() {
                return Some(Err(e));
            }
            let batch = match self.probe.next() {
                // probe 跑完 → FULL 还要补一轮"没匹配上的 build 行"（其余类型就此结束）
                None => return self.full_tail(),
                Some(Ok(b)) => b,
                Some(Err(e)) => return Some(Err(e)),
            };
            if batch.num_rows() == 0 {
                continue;
            }
            // 分字段借用：`build_state` 只读、`matched` 只写（二者不重叠）
            let out = {
                let Self {
                    build_state,
                    matched,
                    residual,
                    ..
                } = self;
                let build = build_state.as_ref().expect("build 阶段已过");
                join_batch(
                    &batch,
                    build,
                    &probe_keys,
                    &out_schema,
                    join_type,
                    needed.as_ref().map(|v| v.as_slice()),
                    residual.as_ref(),
                    matched.as_mut().map(|m| m.as_mut_slice()),
                )
            };
            match out {
                Ok(Some(b)) => return Some(Ok(b)),
                Ok(None) => continue, // 无匹配且非 LEFT/FULL
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

impl HashJoinExec {
    /// FULL OUTER 的**收尾轮**：把 build 侧"一个 probe 行都没配上"的行发射一次，
    /// 左段整片补 NULL。（其余 join 类型直接结束。）
    ///
    /// 为什么必须有这一轮：FULL = 匹配对 ∪ 左孤儿 ∪ **右孤儿**。前两者 probe
    /// 阶段就发完了，右孤儿只有等 probe 跑完、看过 `matched` 标记才知道是谁。
    /// 漏了它 = 静默**少行**（实测口径：4 行变 3 行）。
    ///
    /// 发完把标记全置真，保证只发一次（`next()` 之后还可能被再调）。
    fn full_tail(&mut self) -> Option<Result<RecordBatch>> {
        if self.join_type != JoinType::FullOuter {
            return None;
        }
        let Self {
            matched,
            build_state,
            probe,
            schema,
            needed,
            ..
        } = self;
        let (Some(mask), Some(build)) = (matched.as_mut(), build_state.as_ref()) else {
            return None;
        };
        let out = unmatched_build_batch(
            &build.table,
            mask,
            &probe.schema(),
            schema,
            needed.as_ref().map(|v| v.as_slice()),
        );
        if mask.iter().any(|m| !*m) {
            mask.iter_mut().for_each(|m| *m = true);
        }
        match out {
            Ok(Some(b)) => Some(Ok(b)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

/// 把 `mask` 里**没匹配上**的 build 行挑出来，左段补 NULL 发射。
///
/// 左段列一律 `new_null_array`（这些行在左表里没有对应物）；右段列照 `needed`
/// 掩码决定是 `take` 还是填 NULL（与 `join_batch` 同一口径，别让它俩分叉）。
fn unmatched_build_batch(
    build: &BuildSide,
    mask: &[bool],
    probe_schema: &SchemaRef,
    out_schema: &SchemaRef,
    needed: Option<&[u32]>,
) -> Result<Option<RecordBatch>> {
    let n_left = probe_schema.fields().len();
    let idx: Vec<u32> = mask
        .iter()
        .enumerate()
        .filter(|(_, m)| !**m)
        .map(|(i, _)| i as u32)
        .collect();
    if idx.is_empty() {
        return Ok(None);
    }
    let n = idx.len();
    let want = |pos: usize| needed.is_none_or(|nn| nn.contains(&(pos as u32)));
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(out_schema.fields().len());
    for f in probe_schema.fields() {
        cols.push(arrow::array::new_null_array(f.data_type(), n));
    }
    let take_idx = UInt32Array::from(idx);
    for (c, f) in build.schema.fields().iter().enumerate() {
        if !want(n_left + c) {
            cols.push(arrow::array::new_null_array(f.data_type(), n));
            continue;
        }
        cols.push(build.take(c, &take_idx)?);
    }
    RecordBatch::try_new_with_options(
        Arc::clone(out_schema),
        cols,
        &RecordBatchOptions::new().with_row_count(Some(n)),
    )
    .map(Some)
    .map_err(|e| QsdbError::invalid(format!("full join output: {e}")))
}

/// 把若干批拼成一张表（**只在峰值额度付得起时**调用，见 `build_phase` 的说明）。
fn concat_batches(batches: &[RecordBatch], schema: &SchemaRef) -> Result<RecordBatch> {
    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::clone(schema)));
    }
    if batches.len() == 1 {
        return Ok(batches[0].clone());
    }
    let n_cols = schema.fields().len();
    let mut cols = Vec::with_capacity(n_cols);
    for c in 0..n_cols {
        let parts: Vec<&dyn Array> = batches.iter().map(|b| b.column(c).as_ref()).collect();
        cols.push(concat(&parts).map_err(|e| QsdbError::invalid(format!("concat: {e}")))?);
    }
    let n: usize = batches.iter().map(|b| b.num_rows()).sum();
    RecordBatch::try_new_with_options(
        Arc::clone(schema),
        cols,
        &RecordBatchOptions::new().with_row_count(Some(n)),
    )
    .map_err(|e| QsdbError::invalid(format!("concat batch: {e}")))
}

/// build 侧键索引（**逐批**构建，行号是**跨批全局行号**）。
///
/// 逐批复用 `build_bytes_index` / i64 快路径，再把每批的结果按 `base` 偏移
/// 并进总表 —— 不重写编码与"NULL 键不入索引"的规则，那两处一旦分叉就是静默错值。
fn build_index(side: &BuildSide, build_keys: &[PhysExpr]) -> Result<HashIndex> {
    let int_ok = HashIndex::int_ok(build_keys);
    if int_ok {
        let mut m: I64Map<Vec<u32>> = I64Map::default();
        let mut ok = true;
        for (bi, b) in side.batches.iter().enumerate() {
            let mut key_cols: Vec<ArrayRef> = Vec::with_capacity(build_keys.len());
            for k in build_keys {
                key_cols.push(eval::eval(k, b)?);
            }
            match int_key_column(&key_cols[0]) {
                Some(vals) => {
                    let off = side.base[bi];
                    for row in 0..b.num_rows() {
                        if let Some(v) = vals(row) {
                            m.entry(v).or_default().push((off + row) as u32);
                        }
                    }
                }
                None => {
                    ok = false; // 类型不符 ⇒ 整体退字节路径（已经建的部分丢弃）
                    break;
                }
            }
        }
        if ok {
            return Ok(HashIndex::I64(m));
        }
    }
    let mut m: BytesMap<Vec<u32>> = BytesMap::default();
    for (bi, b) in side.batches.iter().enumerate() {
        let mut key_cols: Vec<ArrayRef> = Vec::with_capacity(build_keys.len());
        for k in build_keys {
            key_cols.push(eval::eval(k, b)?);
        }
        let sub = build_bytes_index(&key_cols, b.num_rows());
        let off = side.base[bi] as u32;
        for (k, rows) in sub {
            let e = m.entry(k).or_default();
            e.extend(rows.into_iter().map(|r| r + off));
        }
    }
    Ok(HashIndex::Bytes(m))
}

/// 字节键索引构建（多键/字符串/浮点路径）。
fn build_bytes_index(key_cols: &[ArrayRef], nrows: usize) -> BytesMap<Vec<u32>> {
    let mut index: BytesMap<Vec<u32>> = BytesMap::default();
    let mut buf: Vec<u8> = Vec::new();
    for row in 0..nrows {
        encode_join_key(key_cols, row, &mut buf);
        if buf.first() == Some(&0) {
            continue; // 任一键 NULL → 不入索引（SQL 语义）
        }
        index.entry(buf.clone()).or_default().push(row as u32);
    }
    index
}

/// 单个 probe 批的连接。
/// 残留谓词对**每一对 (probe 行, build 行)** 的判定结果。
///
/// 把匹配对儿两侧的列 `take` 出来拼成一个"合并批"（布局 = 左 ++ 右，与 join 输出
/// 同构），再交给通用表达式求值——这样残留里写的 `r.v > 250` 与用户看到的
/// 列序完全一致，不需要为它单独造一套求值器。
///
/// 代价是每对多一次列拷贝（输出侧还要再 take 一次）。这是**残留 join 才付**的
/// 成本；为它把求值器做进 take 里，省下的没有"语义只有一处"值钱。
fn residual_keep_mask(
    probe_batch: &RecordBatch,
    build_table: &BuildSide,
    probe_idx: &[u32],
    build_idx: &[u32],
    residual: &PhysExpr,
) -> Result<BooleanArray> {
    let pi = UInt32Array::from(probe_idx.to_vec());
    let bi = UInt32Array::from(build_idx.to_vec());
    let mut fields: Vec<arrow::datatypes::FieldRef> =
        probe_batch.schema().fields().iter().cloned().collect();
    fields.extend(build_table.schema.fields().iter().cloned());
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(fields.len());
    for c in 0..probe_batch.num_columns() {
        cols.push(
            take(probe_batch.column(c), &pi, None)
                .map_err(|e| QsdbError::invalid(format!("join residual take: {e}")))?,
        );
    }
    for c in 0..build_table.num_columns() {
        cols.push(build_table.take(c, &bi)?);
    }
    let combined = RecordBatch::try_new(Arc::new(arrow::datatypes::Schema::new(fields)), cols)
        .map_err(|e| QsdbError::invalid(format!("join residual batch: {e}")))?;
    let v = eval::eval(residual, &combined)?;
    // 常量残留（`ON a.k = b.k AND true` 里那个 `true`，以及 `ON TRUE` 的叉积）
    // 产**长度 1** 的数组，而下游按**匹配对**索引它 ⇒ 不广播就
    // `Trying to access an element at index 1 from a BooleanArray of length 1`。
    // 与 `ProjectExec` / 聚合键 / 聚合参数同一个坑，共用一份 `broadcast_const`。
    let v = super::broadcast_const(&v, probe_idx.len())?;
    let mask = v
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| QsdbError::type_err("JOIN residual must evaluate to boolean"))?
        .clone();
    Ok(mask)
}

/// `residual` = 键之外的 ON 条件（**对匹配对**求值；判否只丢弃那个"对"）。
/// LEFT 下"全部对都被否掉"的左行**仍要补 NULL 保留** —— 这正是它不能被提到
/// `WHERE` 的原因（提上去就整体丢行）。见 `LogicalPlan::Join::residual`。
/// `matched` = FULL OUTER 的 build 侧匹配标记（长度 = build 行数）；
/// 发射**存活**的匹配对时置位，probe 跑完后据此补发没匹配上的 build 行。
/// 其它 join 类型传 `None`。
#[allow(clippy::too_many_arguments)] // 多两个语义参数，比把 9 个参数包成结构体更好读
fn join_batch(
    probe_batch: &RecordBatch,
    state: &BuildState,
    probe_keys: &[PhysExpr],
    out_schema: &SchemaRef,
    join_type: JoinType,
    needed: Option<&[u32]>,
    residual: Option<&PhysExpr>,
    matched: Option<&mut [bool]>,
) -> Result<Option<RecordBatch>> {
    let n = probe_batch.num_rows();
    // probe 键求值
    let mut key_cols: Vec<ArrayRef> = Vec::with_capacity(probe_keys.len());
    for k in probe_keys {
        key_cols.push(eval::eval(k, probe_batch)?);
    }
    // 每行：build 行号列表（NULL 键 → 空）
    let mut probe_idx: Vec<u32> = Vec::new();
    let mut build_idx: Vec<u32> = Vec::new();
    let mut unmatched: Vec<u32> = Vec::new();
    match &state.index {
        // i64 快路径：免编码免字节哈希
        HashIndex::I64(map) => {
            let vals = int_key_column(&key_cols[0]);
            for row in 0..n {
                let hit = vals.as_ref().and_then(|f| f(row));
                match hit.and_then(|v| map.get(&v)) {
                    Some(rows) => {
                        for &b in rows {
                            probe_idx.push(row as u32);
                            build_idx.push(b);
                        }
                    }
                    None => unmatched.push(row as u32),
                }
            }
        }
        HashIndex::Bytes(map) => {
            let mut buf: Vec<u8> = Vec::new();
            for row in 0..n {
                encode_join_key(&key_cols, row, &mut buf);
                if buf.first() == Some(&0) {
                    unmatched.push(row as u32);
                    continue;
                }
                match map.get(buf.as_slice()) {
                    Some(rows) => {
                        for &b in rows {
                            probe_idx.push(row as u32);
                            build_idx.push(b);
                        }
                    }
                    None => unmatched.push(row as u32),
                }
            }
        }
    }
    // ── 残留谓词：对**匹配对**求值（键之外的条件）──────────────────────────
    //
    // ⚠️ 位置很关键：**在"未匹配"定案之前**。LEFT 的语义是"判否只丢弃这个对"，
    // 若一个左行的所有匹配对都被否掉，它就该**算未匹配、补 NULL 保留**。
    // 反过来（先定案再判残留，或把残留提到 WHERE）会**整体丢掉那些行** ——
    // 那是一次静默少行（实测口径：3 行变 1 行）。
    if let Some(r) = residual {
        if !probe_idx.is_empty() {
            let keep = residual_keep_mask(probe_batch, &state.table, &probe_idx, &build_idx, r)?;
            let mut np: Vec<u32> = Vec::with_capacity(probe_idx.len());
            let mut nb: Vec<u32> = Vec::with_capacity(build_idx.len());
            for i in 0..probe_idx.len() {
                // NULL 结果按"不通过"处理（SQL 三值：UNKNOWN 不是 TRUE）
                if !keep.is_null(i) && keep.value(i) {
                    np.push(probe_idx[i]);
                    nb.push(build_idx[i]);
                }
            }
            if matches!(join_type, JoinType::LeftOuter | JoinType::FullOuter) {
                // 匹配过、但**一对都没活下来**的左行 → 并入未匹配（补 NULL 保留）。
                // FULL 同样要（它的左孤儿定义与 LEFT 一致）。
                // 三个集合都是 O(1) 查：残留路径上 probe_idx 可能有几十万项，
                // 用 `Vec::contains` 就是 O(n²)（一次误判够毁掉整条查询）。
                let survived: std::collections::HashSet<u32> = np.iter().copied().collect();
                let already: std::collections::HashSet<u32> = unmatched.iter().copied().collect();
                let mut seen: std::collections::HashSet<u32> = std::collections::HashSet::new();
                let mut add: Vec<u32> = Vec::new();
                for &row in &probe_idx {
                    if !survived.contains(&row) && !already.contains(&row) && seen.insert(row) {
                        add.push(row);
                    }
                }
                unmatched.extend(add);
            }
            probe_idx = np;
            build_idx = nb;
        }
    }

    // FULL：记录**存活**的匹配对用到了哪些 build 行（判否的对不算匹配）
    if let Some(mask) = matched {
        for &b in &build_idx {
            mask[b as usize] = true;
        }
    }

    // LEFT / FULL：未匹配行补 NULL build 侧
    let emit_unmatched =
        matches!(join_type, JoinType::LeftOuter | JoinType::FullOuter) && !unmatched.is_empty();
    if probe_idx.is_empty() && !emit_unmatched {
        return Ok(None);
    }
    let n_probe_cols = probe_batch.num_columns();
    let n_build_cols = state.table.num_columns();
    // 声明行数 = 实际输出行数：INNER 丢弃未匹配行（M6-2 修复——
    // 原先恒计入 unmatched，未匹配行存在时与列长不符触发 batch 校验失败）
    let total_rows = probe_idx.len() + usize::from(emit_unmatched) * unmatched.len();
    // 左（probe）列索引 = 匹配行 ++ 未匹配行（保留左值）
    let mut all_probe = probe_idx.clone();
    if emit_unmatched {
        all_probe.extend_from_slice(&unmatched);
    }
    let indices = UInt32Array::from(all_probe);
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(n_probe_cols + n_build_cols);
    // **只需这些输出位置**（`None` = 全部）：不在表里的列**填 NULL**，不 take。
    // 省的是"整列 × 全部输出行"的拷贝——实测 join 输出 10 列 → 5 列时间减半。
    // 判错的后果是静默错值，所以标注侧（`annotate_join_needs`）只在
    // "读取集合可枚举"时才动手；这里只负责照做。
    let want = |pos: usize| needed.is_none_or(|n| n.contains(&(pos as u32)));
    if !probe_idx.is_empty() || emit_unmatched {
        for c in 0..n_probe_cols {
            if !want(c) {
                cols.push(new_null_like(probe_batch.column(c).as_ref(), total_rows));
                continue;
            }
            let taken = take(probe_batch.column(c), &indices, None)
                .map_err(|e| QsdbError::invalid(format!("join take: {e}")))?;
            cols.push(taken);
        }
    } else {
        for c in 0..n_probe_cols {
            cols.push(new_null_like(probe_batch.column(c).as_ref(), 0));
        }
    }
    // 右（build）列
    if total_rows > 0 {
        let build_indices = UInt32Array::from(build_idx);
        for c in 0..n_build_cols {
            if !want(n_probe_cols + c) {
                cols.push(new_null_like(state.table.column(c).as_ref(), total_rows));
                continue;
            }
            let taken = state.table.take(c, &build_indices)?;
            if emit_unmatched {
                // 未匹配段补 NULL：先取 build 段，再拼接 NULL 段
                let nulls = new_null_like(taken.as_ref(), unmatched.len());
                let parts: Vec<&dyn Array> = vec![taken.as_ref(), nulls.as_ref()];
                cols.push(
                    concat(&parts).map_err(|e| QsdbError::invalid(format!("join concat: {e}")))?,
                );
            } else {
                cols.push(taken);
            }
        }
    } else {
        for c in 0..n_build_cols {
            cols.push(new_null_like(state.table.column(c).as_ref(), 0));
        }
    }
    let _ = n_probe_cols;
    RecordBatch::try_new_with_options(
        Arc::clone(out_schema),
        cols,
        &RecordBatchOptions::new().with_row_count(Some(total_rows)),
    )
    .map(Some)
    .map_err(|e| QsdbError::invalid(format!("join output: {e}")))
}

/// semi/anti 族单 probe 批：存在集探测 → 选中左行 take（仅左列，
/// 无右列展开、无重复——semi 语义天然免 distinct）。
fn join_batch_semi(
    probe_batch: &RecordBatch,
    state: &ExistsState,
    probe_keys: &[PhysExpr],
    out_schema: &SchemaRef,
    join_type: JoinType,
) -> Result<Option<RecordBatch>> {
    // NOT IN 三值语义：build 含 NULL → 全查询恒空
    if join_type == JoinType::AntiIn && state.has_null {
        return Ok(None);
    }
    let n = probe_batch.num_rows();
    let mut key_cols: Vec<ArrayRef> = Vec::with_capacity(probe_keys.len());
    for k in probe_keys {
        key_cols.push(eval::eval(k, probe_batch)?);
    }
    let mut sel: Vec<u32> = Vec::with_capacity(n);
    // 单整数键快路径（与 build 侧形态一致才有效）
    if let (ExistsKeys::I64(_) | ExistsKeys::Bitmap { .. }, [only]) =
        (&state.keys, key_cols.as_slice())
    {
        for row in 0..n {
            if only.is_null(row) {
                // probe NULL：Semi 不发射；Anti 发射（零命中）；AntiIn 不发射
                if join_type == JoinType::Anti {
                    sel.push(row as u32);
                }
                continue;
            }
            if let Some(v) = int_key_value(only, row) {
                let hit = state.contains(v);
                match (join_type, hit) {
                    (JoinType::Semi, true)
                    | (JoinType::Anti, false)
                    | (JoinType::AntiIn, false) => sel.push(row as u32),
                    _ => {}
                }
            } else {
                return Err(QsdbError::meta("semi join: mixed key types"));
            }
        }
    } else {
        let mut buf: Vec<u8> = Vec::new();
        for row in 0..n {
            encode_join_key(&key_cols, row, &mut buf);
            let hit = buf.first() != Some(&0) && state.contains_bytes(buf.as_slice());
            if buf.first() == Some(&0) && join_type == JoinType::Anti {
                sel.push(row as u32); // NULL 键零命中 → NOT EXISTS 发射
                continue;
            }
            match (join_type, hit) {
                (JoinType::Semi, true) | (JoinType::Anti, false) | (JoinType::AntiIn, false) => {
                    sel.push(row as u32)
                }
                _ => {}
            }
        }
    }
    if sel.is_empty() {
        return Ok(None);
    }
    let indices = UInt32Array::from(sel);
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(probe_batch.num_columns());
    for c in 0..probe_batch.num_columns() {
        let taken = take(probe_batch.column(c), &indices, None)
            .map_err(|e| QsdbError::invalid(format!("semi take: {e}")))?;
        cols.push(taken);
    }
    let n_out = cols.first().map(|c| c.len()).unwrap_or(0);
    RecordBatch::try_new_with_options(
        Arc::clone(out_schema),
        cols,
        &RecordBatchOptions::new().with_row_count(Some(n_out)),
    )
    .map(Some)
    .map_err(|e| QsdbError::invalid(format!("semi output: {e}")))
}

fn new_null_like(arr: &dyn Array, n: usize) -> ArrayRef {
    arrow::array::new_null_array(arr.data_type(), n)
}

/// 连接键编码：与 agg 的 encode_key_row 同构（null 前缀 tag + 类型化值）。
pub fn encode_join_key(arrays: &[ArrayRef], row: usize, buf: &mut Vec<u8>) {
    buf.clear();
    for a in arrays {
        if a.is_null(row) {
            buf.push(0);
        } else {
            buf.push(1);
            encode_value(a, row, buf);
        }
    }
}

fn encode_value(a: &dyn Array, i: usize, buf: &mut Vec<u8>) {
    use arrow::datatypes::*;
    match a.data_type() {
        DataType::Int8 => buf.extend_from_slice(
            &a.as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .value(i)
                .to_le_bytes(),
        ),
        DataType::Int16 => buf.extend_from_slice(
            &a.as_any()
                .downcast_ref::<Int16Array>()
                .unwrap()
                .value(i)
                .to_le_bytes(),
        ),
        DataType::Int32 => buf.extend_from_slice(
            &a.as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(i)
                .to_le_bytes(),
        ),
        DataType::Int64 => buf.extend_from_slice(
            &a.as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(i)
                .to_le_bytes(),
        ),
        DataType::Float32 => buf.extend_from_slice(
            &a.as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(i)
                .to_le_bytes(),
        ),
        DataType::Float64 => buf.extend_from_slice(
            &a.as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(i)
                .to_le_bytes(),
        ),
        DataType::Date32 => buf.extend_from_slice(
            &a.as_any()
                .downcast_ref::<Date32Array>()
                .unwrap()
                .value(i)
                .to_le_bytes(),
        ),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => buf.extend_from_slice(
            &a.as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap()
                .value(i)
                .to_le_bytes(),
        ),
        DataType::Boolean => buf.push(u8::from(
            a.as_any().downcast_ref::<BooleanArray>().unwrap().value(i),
        )),
        DataType::Decimal128(_, _) => buf.extend_from_slice(
            &a.as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .value(i)
                .to_le_bytes(),
        ),
        DataType::Utf8 => {
            let s = a.as_any().downcast_ref::<StringArray>().unwrap().value(i);
            buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
            buf.extend_from_slice(s.as_bytes());
        }
        other => {
            let s = format!("{other:?}");
            buf.extend_from_slice(s.as_bytes());
        }
    }
}

// ---------------------------------------------------------------------------
// i64 键快速哈希（Fibonacci hashing）：std 默认 SipHash 对整数键是浪费
//（实测 join probe 57ns/行，哈希占大头）。乘法哈希 ~2ns 且分布良好。
// ---------------------------------------------------------------------------
#[derive(Default, Clone, Copy)]
pub struct I64Hasher(u64);

impl std::hash::Hasher for I64Hasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        for b in bytes {
            self.0 = self.0.rotate_left(5) ^ u64::from(*b);
        }
    }
    #[inline]
    fn write_i64(&mut self, i: i64) {
        self.0 = (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    }
    #[inline]
    fn write_u64(&mut self, i: u64) {
        self.0 = i.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    }
}

#[derive(Default, Clone, Copy)]
pub struct BuildI64Hasher;

impl std::hash::BuildHasher for BuildI64Hasher {
    type Hasher = I64Hasher;
    #[inline]
    fn build_hasher(&self) -> I64Hasher {
        I64Hasher(0)
    }
}

/// i64 键 map（快哈希）。
pub type I64Map<V> = std::collections::HashMap<i64, V, BuildI64Hasher>;

/// `Vec<u8>` 键的 **8 字节块** FxHash。
///
/// 为什么要它：`HashMap<Vec<u8>, _>` 默认用 `RandomState` = **SipHash**。
/// `perf` 实测（2026-10-14，`complex_three_way` 0.363s / 2586 采样）：
/// `Sip13Round` **4.64%** + `RandomState` **4.13%** = **8.77%**，且旁边就是
/// `encode_join_key`（3.77%）—— 全在 join 建侧索引与去重集合上。
///
/// ⚠️ 不复用 `I64Hasher`：它的 `write` 是**逐字节** rotate-mix，
/// 对字节序列等于每字节一次运算；这里按 8 字节一块走（与 `agg::fxhash_bytes` 同构）。
pub struct ByteHasher(u64);

impl std::hash::Hasher for ByteHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }
    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        const SEED: u64 = 0x51_7c_c1_b7_27_22_0a_95;
        let mut h = if self.0 == 0 { SEED } else { self.0 };
        let mut chunks = bytes.chunks_exact(8);
        for c in &mut chunks {
            let v = u64::from_le_bytes(c.try_into().unwrap());
            h = (h.rotate_left(5) ^ v).wrapping_mul(SEED);
        }
        let rem = chunks.remainder();
        if !rem.is_empty() {
            let mut buf = [0u8; 8];
            buf[..rem.len()].copy_from_slice(rem);
            h = (h.rotate_left(5) ^ u64::from_le_bytes(buf)).wrapping_mul(SEED);
        }
        self.0 = h;
    }
}

#[derive(Default, Clone, Copy)]
pub struct BuildByteHasher;

impl std::hash::BuildHasher for BuildByteHasher {
    type Hasher = ByteHasher;
    #[inline]
    fn build_hasher(&self) -> ByteHasher {
        ByteHasher(0)
    }
}

/// `Vec<u8>` 键的 map / set（快哈希）。
pub type BytesMap<V> = std::collections::HashMap<Vec<u8>, V, BuildByteHasher>;
pub type BytesSet = std::collections::HashSet<Vec<u8>, BuildByteHasher>;

// ---------------------------------------------------------------------------
// count(*) 下推（T3 优化）：JOIN 输出的行数 = Σ(左键匹配的右键行数)，
// 恒等变换为「右键计数 + 左键累加」——不物化 JOIN 输出。
// 低基数键下物化输出是 n²/keys 行（POC T3：10M×10M/1e4 键 → 10^7 倍放大），
// 本路径将其压回 O(n)。仅 INNER 等值 JOIN + 单一 count(*) + 无 GROUP BY。
// ---------------------------------------------------------------------------
pub struct JoinCountExec {
    probe: Box<dyn ExecNode>,
    build: Box<dyn ExecNode>,
    probe_keys: Vec<PhysExpr>,
    build_keys: Vec<PhysExpr>,
    /// LEFT：未匹配行也计入 1（Σ max(匹配数, 1)）——INNER 为 false
    left_outer: bool,
    ctx: Arc<QueryContext>,
    /// 键 → 右表匹配行数（单整数键走 i64 快路径）
    counts: Option<CountMap>,
    /// 计数 map 的内存预留（此前只有 `ctx.check()`，无记账 → 无界增长）
    res: crate::memory::Reservation,
    emitted: bool,
    schema: SchemaRef,
}

enum CountMap {
    /// 稠密整数键域 → 直接索引计数数组（免哈希、顺序访问 cache 友好）。
    /// 稀疏域退回哈希路径（跨度 > 16M 或非整数键）。
    Direct {
        min: i64,
        counts: Vec<u32>,
        miss: u64,
    },
    I64(I64Map<u64>),
    Bytes(BytesMap<u64>),
}

impl JoinCountExec {
    pub fn new(
        probe: Box<dyn ExecNode>,
        build: Box<dyn ExecNode>,
        probe_keys: Vec<PhysExpr>,
        build_keys: Vec<PhysExpr>,
        left_outer: bool,
        ctx: Arc<QueryContext>,
    ) -> crate::error::Result<Self> {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("count(*)", DataType::Int64, false),
        ]));
        let res = ctx.try_reserve(0)?;
        Ok(Self {
            probe,
            build,
            probe_keys,
            build_keys,
            left_outer,
            ctx,
            counts: None,
            res,
            emitted: false,
            schema,
        })
    }

    /// 单整数键快路径判定（与 semi-join 同款：免键编码）。
    fn int_key_ok(keys: &[PhysExpr]) -> bool {
        keys.len() == 1
            && matches!(
                keys[0].data_type(),
                crate::types::LogicalType::Int8
                    | crate::types::LogicalType::Int16
                    | crate::types::LogicalType::Int32
                    | crate::types::LogicalType::Int64
            )
    }

    fn build_counts(&mut self) -> Result<()> {
        let t_prof = std::env::var("QSDB_JOIN_PROF").is_ok();
        let t0 = std::time::Instant::now();
        let use_i64 = Self::int_key_ok(&self.build_keys);
        let mut i64_map: I64Map<u64> = I64Map::default();
        let mut bytes_map: BytesMap<u64> = BytesMap::default();
        // 稠密直索引候选：单整数键时先收集键值（build 侧通常远小于 probe）
        let mut dense_keys: Vec<i64> = Vec::new();
        let mut buf: Vec<u8> = Vec::new();
        let mut rows: u64 = 0;
        while let Some(b) = self.build.next() {
            self.ctx.check()?;
            let batch = b?;
            rows += batch.num_rows() as u64;
            let mut key_cols: Vec<ArrayRef> = Vec::with_capacity(self.build_keys.len());
            for k in &self.build_keys {
                key_cols.push(eval::eval(k, &batch)?);
            }
            if use_i64 {
                if let Some(vals) = int_key_column(&key_cols[0]) {
                    for row in 0..batch.num_rows() {
                        if let Some(v) = vals(row) {
                            dense_keys.push(v);
                        }
                    }
                    continue;
                }
            }
            for row in 0..batch.num_rows() {
                encode_join_key(&key_cols, row, &mut buf);
                if buf.first() == Some(&0) {
                    continue; // NULL 键不匹配（SQL 语义）
                }
                *bytes_map.entry(buf.clone()).or_insert(0) += 1;
            }
        }
        // 稠密判定：span ≤ 16M 用直索引（16M × 4B = 64MB 上界）
        if use_i64 {
            if let (Some(&mn), Some(&mx)) = (dense_keys.iter().min(), dense_keys.iter().max()) {
                let span = (mx as i128 - mn as i128 + 1) as u128;
                if span <= 16_000_000 {
                    let mut counts = vec![0u32; span as usize];
                    for k in &dense_keys {
                        counts[(*k - mn) as usize] += 1;
                    }
                    self.res.grow(counts.capacity() as u64 * 4)?;
                    self.counts = Some(CountMap::Direct {
                        min: mn,
                        counts,
                        miss: 0,
                    });
                    return Ok(());
                }
            }
            for k in dense_keys {
                *i64_map.entry(k).or_insert(0) += 1;
            }
        }
        // 右表为空 → 结果 0（用空 i64 map 表示）
        let est = i64_map.capacity() as u64 * 24
            + bytes_map
                .keys()
                .map(|k| k.capacity() as u64 + 40)
                .sum::<u64>()
            + bytes_map.capacity() as u64 * 8;
        self.res.grow(est)?;
        self.counts = Some(if use_i64 && bytes_map.is_empty() {
            CountMap::I64(i64_map)
        } else {
            CountMap::Bytes(bytes_map)
        });
        if t_prof {
            eprintln!(
                "[join-prof] build: {:.3}s rows={rows} i64={use_i64}",
                t0.elapsed().as_secs_f64()
            );
        }
        let _ = rows;
        Ok(())
    }
}

/// 单整数列 → i64 取值闭包（NULL → None；非整数返回 None 走通用路径）。
fn int_key_column(col: &ArrayRef) -> Option<Box<dyn Fn(usize) -> Option<i64> + '_>> {
    macro_rules! prim {
        ($ty:ty) => {{
            let arr = col.as_any().downcast_ref::<$ty>()?;
            Some(Box::new(move |i: usize| {
                if arr.is_null(i) {
                    None
                } else {
                    Some(arr.value(i) as i64)
                }
            }) as Box<dyn Fn(usize) -> Option<i64> + '_>)
        }};
    }
    match col.data_type() {
        DataType::Int8 => prim!(arrow::array::Int8Array),
        DataType::Int16 => prim!(arrow::array::Int16Array),
        DataType::Int32 => prim!(arrow::array::Int32Array),
        DataType::Int64 => prim!(arrow::array::Int64Array),
        _ => None,
    }
}

impl ExecNode for JoinCountExec {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
    fn next(&mut self) -> Option<Result<RecordBatch>> {
        if self.emitted {
            return None;
        }
        if self.counts.is_none() {
            if let Err(e) = self.build_counts() {
                return Some(Err(e));
            }
        }
        let t_prof = std::env::var("QSDB_JOIN_PROF").is_ok();
        let t1 = std::time::Instant::now();
        let counts = self.counts.as_ref().unwrap();
        let use_i64 = matches!(counts, CountMap::I64(_) | CountMap::Direct { .. });
        let mut total: u64 = 0;
        let mut buf: Vec<u8> = Vec::new();
        loop {
            if let Err(e) = self.ctx.check() {
                return Some(Err(e));
            }
            let Some(b) = self.probe.next() else { break };
            let batch = match b {
                Ok(x) => x,
                Err(e) => return Some(Err(e)),
            };
            let mut key_cols: Vec<ArrayRef> = Vec::with_capacity(self.probe_keys.len());
            for k in &self.probe_keys {
                match eval::eval(k, &batch) {
                    Ok(a) => key_cols.push(a),
                    Err(e) => return Some(Err(e)),
                }
            }
            let miss = if self.left_outer { 1u64 } else { 0 };
            if let CountMap::Direct {
                min,
                counts: dc,
                miss: dm,
            } = counts
            {
                let _ = dm;
                if let Some(vals) = int_key_column(&key_cols[0]) {
                    for row in 0..batch.num_rows() {
                        if let Some(v) = vals(row) {
                            let i = v - *min;
                            if i >= 0 && (i as usize) < dc.len() {
                                total += u64::from(dc[i as usize]);
                                continue;
                            }
                        }
                        total += miss;
                    }
                    continue;
                }
            }
            if use_i64 {
                if let Some(vals) = int_key_column(&key_cols[0]) {
                    if let CountMap::I64(m) = counts {
                        for row in 0..batch.num_rows() {
                            let hit = vals(row).and_then(|v| m.get(&v).copied());
                            total += hit.unwrap_or(miss);
                        }
                    }
                    continue;
                }
            }
            if let CountMap::Bytes(m) = counts {
                for row in 0..batch.num_rows() {
                    encode_join_key(&key_cols, row, &mut buf);
                    let hit = if buf.first() == Some(&0) {
                        None
                    } else {
                        m.get(&buf).copied()
                    };
                    total += hit.unwrap_or(miss);
                }
            }
        }
        if t_prof {
            eprintln!(
                "[join-prof] probe: {:.3}s total={total}",
                t1.elapsed().as_secs_f64()
            );
        }
        self.emitted = true;
        let arr = arrow::array::Int64Array::from(vec![total as i64]);
        let out = RecordBatch::try_new(Arc::clone(&self.schema), vec![Arc::new(arr)]);
        Some(out.map_err(|e| QsdbError::invalid(format!("join count output: {e}"))))
    }
}
