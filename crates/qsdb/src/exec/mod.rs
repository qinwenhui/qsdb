//! 执行算子：拉式迭代（`next() -> Option<Result<RecordBatch>>`）。
//!
//! M2 算子：Filter / Project / Limit / Sort(TopN) / Distinct / HashAgg。
//! Scan 由 `crate::scan::Scan` 包装。并行聚合见 `agg`（文件级 rayon 展开）。

pub mod agg;
pub mod agg_spill;
pub mod join;
pub mod par_scan;
pub mod spill;
pub mod udaf_agg;
pub mod window;

use std::sync::Arc;

use arrow::array::Array;

use arrow::array::ArrayRef;
use arrow::compute::kernels::sort::{SortColumn, lexsort_to_indices};
use arrow::compute::{concat, filter_record_batch, take};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use crate::error::{QsdbError, Result};
use crate::expr::eval;
use crate::expr::{AggExpr, PhysExpr};

/// 拉式执行节点。
pub trait ExecNode: Send {
    /// 输出 schema
    fn schema(&self) -> SchemaRef;
    /// 拉取下一批；None = 结束
    fn next(&mut self) -> Option<Result<RecordBatch>>;

    /// 预取钩子（默认无操作）：让**后面才被抽到的**子节点提前开跑生产端。
    ///
    /// 这是**性能**钩子不是正确性钩子：`ParScanExec` 的生产端是**懒启动**的
    /// （早启动会与另一侧扫描互锁——见 `exec/par_scan.rs`），代价是丢掉
    /// "计划构建期就开始填通道"的重叠预取（实测宽 join 物化因此慢 ~10%）。
    /// 父算子在自己**不再需要另一侧**的时刻（如 hash join 建完 build 侧）调它，
    /// 就能把预取拿回来，且此时对方的生产端已退出、不会互锁。
    fn prefetch(&mut self) {}
}

/// 布尔掩码过滤。
pub struct FilterExec {
    pub input: Box<dyn ExecNode>,
    pub predicate: PhysExpr,
    schema: SchemaRef,
    ctx: Arc<crate::query::QueryContext>,
}

impl FilterExec {
    pub fn new(
        input: Box<dyn ExecNode>,
        predicate: PhysExpr,
        ctx: Arc<crate::query::QueryContext>,
    ) -> Self {
        let schema = input.schema();
        Self {
            input,
            predicate,
            schema,
            ctx,
        }
    }
}

impl ExecNode for FilterExec {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    /// 透传预取（见 [`ExecNode::prefetch`]）：本节点 1:1 转发输入，
    /// 预取请求要一路传到真正的数据源（`ParScanExec`）才有效。
    fn prefetch(&mut self) {
        self.input.prefetch();
    }

    fn next(&mut self) -> Option<Result<RecordBatch>> {
        loop {
            if let Err(e) = self.ctx.check() {
                return Some(Err(e));
            }
            let batch = self.input.next()?;
            let batch = match batch {
                Ok(b) => b,
                Err(e) => return Some(Err(e)),
            };
            match eval::eval_mask(&self.predicate, &batch) {
                Ok(mask) => {
                    let out = filter_record_batch(&batch, &mask)
                        .map_err(|e| QsdbError::invalid(format!("filter failed: {e}")));
                    match out {
                        Ok(filtered) if filtered.num_rows() > 0 => return Some(Ok(filtered)),
                        Ok(_) => continue,
                        Err(e) => return Some(Err(e)),
                    }
                }
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

/// 表达式投影（输出列 = 各表达式结果）。
pub struct ProjectExec {
    pub input: Box<dyn ExecNode>,
    pub exprs: Vec<PhysExpr>,
    schema: SchemaRef,
}

impl ProjectExec {
    pub fn new(input: Box<dyn ExecNode>, exprs: Vec<PhysExpr>, names: Vec<String>) -> Self {
        let fields: Vec<arrow::datatypes::Field> = exprs
            .iter()
            .zip(&names)
            .map(|(e, n)| arrow::datatypes::Field::new(n, e.data_type().arrow_type(), true))
            .collect();
        Self {
            input,
            exprs,
            schema: Arc::new(arrow::datatypes::Schema::new(fields)),
        }
    }
}

impl ExecNode for ProjectExec {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    /// 透传预取（见 [`ExecNode::prefetch`]）：本节点 1:1 转发输入，
    /// 预取请求要一路传到真正的数据源（`ParScanExec`）才有效。
    fn prefetch(&mut self) {
        self.input.prefetch();
    }

    fn next(&mut self) -> Option<Result<RecordBatch>> {
        let batch = self.input.next()?;
        let batch = match batch {
            Ok(b) => b,
            Err(e) => return Some(Err(e)),
        };
        let n = batch.num_rows();
        let mut cols: Vec<ArrayRef> = Vec::with_capacity(self.exprs.len());
        for e in &self.exprs {
            match eval::eval(e, &batch) {
                // 常量表达式产**长度 1** 的数组 ⇒ 就地广播到批长。
                //
                // ⚠️ 必须在 `try_new` **之前**：各列长度不一致时 `try_new` 直接失败，
                // 而"常量 + 列"的混合投影（`SELECT 1, g FROM t`）是最普通的写法 ——
                // 此前它**必炸**，报的还是 Arrow 的内部话术（`all columns in a
                // record batch must have the same length`）；全常量
                // （`SELECT 1 FROM t`）靠事后那段广播侥幸过得去，于是这个洞
                // **只对"混合"这一种形态显形**，一直没被撞见。
                // `n == 0` 也走这里（广播到 0 列长），否则空结果同样对不齐。
                Ok(a) if a.len() == 1 && n != 1 => match broadcast_const(&a, n) {
                    Ok(b) => cols.push(b),
                    Err(err) => return Some(Err(err)),
                },
                Ok(a) => cols.push(a),
                Err(err) => return Some(Err(err)),
            }
        }
        Some(
            // `cols.clone()`：`Vec<Arc>` 的克隆只是引用计数 +1（每列一次原子加），
            // 为的是**报错路径**还能拿到实际产出类型 —— 热路径上这点开销
            // 相对每个 batch 的表达式求值可以忽略。
            RecordBatch::try_new(self.schema(), cols.clone()).map_err(|e| {
                // **schema 声明的类型 ≠ 实际产出的类型** —— 这种错不给"是哪一列"
                // 就没法定位：表达式类型是绑定期推的，37 层 CTE 的语料里
                // 每一层的类型都是推导出来的（L4-01 三块卡了半天，就是因为
                // 报错里没有表达式，只能一层层数）。
                let bad: Vec<String> = self
                    .exprs
                    .iter()
                    .zip(cols.iter())
                    .enumerate()
                    .filter(|(_, (e, a))| e.data_type().arrow_type() != *a.data_type())
                    .map(|(i, (e, a))| {
                        format!(
                            "col[{}] {:?}: schema={} actual={} expr={}",
                            i,
                            self.schema().fields()[i].name(),
                            e.data_type(),
                            a.data_type(),
                            e.display()
                        )
                    })
                    .collect();
                if bad.is_empty() {
                    QsdbError::invalid(format!("project failed: {e}"))
                } else {
                    QsdbError::invalid(format!(
                        "project failed: {e}; mismatched columns: {}",
                        bad.join("; ")
                    ))
                }
            }),
        )
    }
}

/// 常量数组（长度 1）广播到 `n` 行（同一元素取 `n` 次；`n == 0` 得空数组）。
///
/// **为什么值得一个公共函数**：`eval` 对常量表达式返回**长度 1** 的数组，而下游
/// 处处按"批内行号"索引它 ⇒ 不广播就有两种下场，都很难看：
/// `RecordBatch::try_new` 报各列长度不一致（`SELECT 1, g FROM t`），
/// 或者**直接越界 panic**（`GROUP BY 'k'` 这类常量分组键）。
/// 这是同一族坑，收口到一处，别再各写各的。
pub(crate) fn broadcast_const(a: &ArrayRef, n: usize) -> Result<ArrayRef> {
    if a.len() == n {
        return Ok(Arc::clone(a));
    }
    let idx: Vec<(usize, usize)> = vec![(0, 0); n];
    arrow::compute::interleave(&[a.as_ref()], &idx)
        .map_err(|e| QsdbError::invalid(format!("const broadcast: {e}")))
}

/// 内存批源（CTE 物化体；批由外部注入）。
pub struct ValuesExec {
    batches: std::vec::IntoIter<RecordBatch>,
    schema: SchemaRef,
}

impl ValuesExec {
    pub fn new(schema: SchemaRef, batches: Vec<RecordBatch>) -> Self {
        Self {
            batches: batches.into_iter(),
            schema,
        }
    }

    pub fn empty(schema: SchemaRef) -> Self {
        Self::new(schema, Vec::new())
    }
}

impl ExecNode for ValuesExec {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
    fn next(&mut self) -> Option<Result<RecordBatch>> {
        self.batches.next().map(Ok)
    }
}

/// 单行零列源（无 FROM SELECT：`SELECT 1+1` / `SELECT CURRENT_DATE`）。
pub struct OneRowExec {
    schema: SchemaRef,
    done: bool,
}

impl OneRowExec {
    pub fn new() -> Self {
        Self {
            schema: Arc::new(arrow::datatypes::Schema::empty()),
            done: false,
        }
    }
}

impl Default for OneRowExec {
    fn default() -> Self {
        Self::new()
    }
}

impl ExecNode for OneRowExec {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
    fn next(&mut self) -> Option<Result<RecordBatch>> {
        if self.done {
            return None;
        }
        self.done = true;
        let opts = arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(1));
        Some(
            RecordBatch::try_new_with_options(Arc::clone(&self.schema), vec![], &opts)
                .map_err(|e| QsdbError::invalid(format!("one-row batch: {e}"))),
        )
    }
}

/// 集合操作类型（LogicalPlan::SetOp 携带）。
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SetOpKind {
    Union,
    Intersect,
    Except,
}

/// 右表行键集合：单整数域列（Int/Date/Boolean/Timestamp 等）免编码
/// i64 快路径（semi-join 同款偏门），其余走字节编码。
enum SetKeys {
    I64 {
        set: std::collections::HashSet<i64>,
        has_null: bool,
    },
    Bytes(std::collections::HashSet<Vec<u8>>),
}

/// 单列 → i64（null → None）。非整数域列返回 None（走 Bytes 路径）。
fn single_col_i64(batch: &RecordBatch) -> Option<Box<dyn Fn(usize) -> Option<i64> + '_>> {
    if batch.num_columns() != 1 {
        return None;
    }
    let col = batch.column(0);
    macro_rules! prim {
        ($ty:ty, $f:ident) => {{
            let arr = col.as_any().downcast_ref::<$ty>()?;
            let f = move |i: usize| {
                if arr.is_null(i) {
                    None
                } else {
                    Some(arr.value(i) as i64)
                }
            };
            Some(Box::new(f) as Box<dyn Fn(usize) -> Option<i64> + '_>)
        }};
    }
    use arrow::datatypes::DataType as D;
    match col.data_type() {
        D::Int8 => prim!(arrow::array::Int8Array, v8),
        D::Int16 => prim!(arrow::array::Int16Array, v16),
        D::Int32 | D::Date32 => prim!(arrow::array::Int32Array, v32),
        D::Int64 | D::Timestamp(_, _) | D::Time64(_) => prim!(arrow::array::Int64Array, v64),
        D::Boolean => {
            let arr = col.as_any().downcast_ref::<arrow::array::BooleanArray>()?;
            let f = move |i: usize| {
                if arr.is_null(i) {
                    None
                } else {
                    Some(arr.value(i) as i64)
                }
            };
            Some(Box::new(f))
        }
        _ => None,
    }
}

/// 集合操作执行：UNION 左右直连（去重由上层 DistinctExec 包装）；
/// INTERSECT/EXCEPT = 右表行键存在集探测左表 + 内联去重（DISTINCT 语义；
/// NULL 在集合操作中视为相等）。单整数列走 i64 免编码快路径。
pub struct SetOpExec {
    left: Box<dyn ExecNode>,
    right: Box<dyn ExecNode>,
    op: SetOpKind,
    schema: SchemaRef,
    /// None = 右表集合未构建（首条前完成）
    right_set: Option<SetKeys>,
    emitted: std::collections::HashSet<Vec<u8>>,
    emitted_i64: std::collections::HashSet<i64>,
    emitted_null: bool,
    buf: Vec<u8>,
    ctx: std::sync::Arc<crate::query::QueryContext>,
    /// 右表集合 + emitted 去重集合的内存预留（此前完全无记账 → 无界增长）
    res: crate::memory::Reservation,
    accounted: u64,
}

impl SetOpExec {
    pub fn new(
        left: Box<dyn ExecNode>,
        right: Box<dyn ExecNode>,
        op: SetOpKind,
        ctx: &std::sync::Arc<crate::query::QueryContext>,
    ) -> Result<Self> {
        let schema = left.schema();
        let res = ctx.try_reserve(0)?;
        Ok(Self {
            left,
            right,
            op,
            schema,
            right_set: None,
            emitted: std::collections::HashSet::new(),
            emitted_i64: std::collections::HashSet::new(),
            emitted_null: false,
            buf: Vec::new(),
            ctx: Arc::clone(ctx),
            res,
            accounted: 0,
        })
    }

    /// 集合增长 → 增量预留（集合常驻 `right_set`，账跟着它走）。
    fn grow_set(&mut self, est: u64) -> Result<()> {
        if est > self.accounted {
            self.res.grow(est - self.accounted)?;
            self.accounted = est;
        }
        Ok(())
    }

    fn row_key(batch: &RecordBatch, row: usize, buf: &mut Vec<u8>) -> Vec<u8> {
        buf.clear();
        join::encode_join_key(batch.columns(), row, buf);
        buf.clone()
    }

    /// 右表全量构建行键集合（首个 next 前调用）。
    fn build_right_set(&mut self) -> Result<()> {
        // schema 判定 i64/Bytes 路径（两侧已归一，批间形态一致）
        let use_i64 = {
            // 借右表 schema 判定（schema 与批同构）
            let rs = self.right.schema();
            let fields = rs.fields();
            fields.len() == 1 && {
                use arrow::datatypes::DataType as D;
                matches!(
                    fields[0].data_type(),
                    D::Int8
                        | D::Int16
                        | D::Int32
                        | D::Int64
                        | D::Date32
                        | D::Boolean
                        | D::Timestamp(_, _)
                        | D::Time64(_)
                )
            }
        };
        if use_i64 {
            let mut set = std::collections::HashSet::new();
            let mut has_null = false;
            let mut feed = |b: RecordBatch| -> Result<()> {
                self.ctx.check()?;
                let col = b.column(0);
                let mut visit = |v: Option<i64>| match v {
                    Some(v) => {
                        set.insert(v);
                    }
                    None => has_null = true,
                };
                macro_rules! prim {
                    ($ty:ty) => {{
                        let arr = col
                            .as_any()
                            .downcast_ref::<$ty>()
                            .ok_or_else(|| QsdbError::type_err("set op: bad i64 column"))?;
                        for i in 0..arr.len() {
                            visit(if arr.is_null(i) {
                                None
                            } else {
                                Some(arr.value(i) as i64)
                            });
                        }
                    }};
                }
                use arrow::datatypes::DataType as D;
                match col.data_type() {
                    D::Int8 => prim!(arrow::array::Int8Array),
                    D::Int16 => prim!(arrow::array::Int16Array),
                    D::Int32 | D::Date32 => prim!(arrow::array::Int32Array),
                    D::Int64 | D::Timestamp(_, _) | D::Time64(_) => prim!(arrow::array::Int64Array),
                    D::Boolean => {
                        let arr = col
                            .as_any()
                            .downcast_ref::<arrow::array::BooleanArray>()
                            .ok_or_else(|| QsdbError::type_err("set op: bad boolean"))?;
                        for i in 0..arr.len() {
                            visit(if arr.is_null(i) {
                                None
                            } else {
                                Some(arr.value(i) as i64)
                            });
                        }
                    }
                    _ => return Err(QsdbError::type_err("set op: unsupported i64 column")),
                }
                Ok(())
            };
            while let Some(nb) = self.right.next() {
                feed(nb?)?;
            }
            let est = set.capacity() as u64 * 24; // 桶 + i64 键
            self.grow_set(est)?;
            self.right_set = Some(SetKeys::I64 { set, has_null });
            return Ok(());
        }
        let mut set = std::collections::HashSet::new();
        let mut feed = |b: &RecordBatch, buf: &mut Vec<u8>| -> Result<()> {
            self.ctx.check()?;
            for row in 0..b.num_rows() {
                buf.clear();
                join::encode_join_key(b.columns(), row, buf);
                set.insert(buf.clone());
            }
            Ok(())
        };
        while let Some(nb) = self.right.next() {
            let b = nb?;
            feed(&b, &mut self.buf)?;
        }
        let est =
            set.iter().map(|k| k.capacity() as u64 + 40).sum::<u64>() + set.capacity() as u64 * 8;
        self.grow_set(est)?;
        self.right_set = Some(SetKeys::Bytes(set));
        Ok(())
    }
}

impl ExecNode for SetOpExec {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
    fn next(&mut self) -> Option<Result<RecordBatch>> {
        if self.op == SetOpKind::Union {
            // 左流尽接右（拉式：每次先取左，None 后转右）
            if let Some(b) = self.left.next() {
                return Some(b);
            }
            return self.right.next();
        }
        // INTERSECT / EXCEPT：首轮构建右表行键集合
        if self.right_set.is_none() {
            if let Err(e) = self.build_right_set() {
                return Some(Err(e));
            }
        }
        // 流式探测左表。INTERSECT 的去重与存在集合一：命中即移除
        //（set.remove 一个哈希操作替代 contains+去重 insert 两个，天然 emit-once）；
        // EXCEPT 保留独立 emitted 集。
        while let Some(b) = self.left.next() {
            let batch = match b {
                Ok(x) => x,
                Err(e) => return Some(Err(e)),
            };
            if let Err(e) = self.ctx.check() {
                return Some(Err(e));
            }
            let mut mask = arrow::array::BooleanBuilder::with_capacity(batch.num_rows());
            let mut all_true = true;
            match self.right_set.as_mut().unwrap() {
                // i64 快路径：单整数列免编码探测
                SetKeys::I64 { set, has_null } => {
                    if let Some(val) = single_col_i64(&batch) {
                        for row in 0..batch.num_rows() {
                            let keep = match val(row) {
                                Some(v) => match self.op {
                                    // 命中即移除：去重与探测合一
                                    SetOpKind::Intersect => set.remove(&v),
                                    _ => !set.contains(&v) && self.emitted_i64.insert(v),
                                },
                                None => match self.op {
                                    SetOpKind::Intersect => std::mem::replace(has_null, false),
                                    _ => {
                                        !*has_null
                                            && !std::mem::replace(&mut self.emitted_null, true)
                                    }
                                },
                            };
                            mask.append_value(keep);
                            all_true &= keep;
                        }
                    } else {
                        return Some(Err(QsdbError::type_err(
                            "set op: left/right column shape mismatch",
                        )));
                    }
                }
                SetKeys::Bytes(set) => {
                    for row in 0..batch.num_rows() {
                        let key = Self::row_key(&batch, row, &mut self.buf);
                        let keep = match self.op {
                            SetOpKind::Intersect => set.remove(&key),
                            _ => !set.contains(&key) && self.emitted.insert(key),
                        };
                        mask.append_value(keep);
                        all_true &= keep;
                    }
                }
            }
            if all_true {
                // 全真：零拷贝直通
                return Some(Ok(batch));
            }
            let mask = mask.finish();
            if mask.false_count() == batch.num_rows() {
                continue; // 全假：跳批
            }
            let out = match filter_record_batch(&batch, &mask) {
                Ok(o) => o,
                Err(e) => return Some(Err(QsdbError::invalid(format!("set op filter: {e}")))),
            };
            return Some(Ok(out));
        }
        None
    }
}

/// LIMIT / OFFSET。
///
/// `remaining = None` = 不限行数（`OFFSET n` 不带 `LIMIT` 的形态）；
/// `skip` = 先丢掉的起始行数（`OFFSET n`）。
pub struct LimitExec {
    pub input: Box<dyn ExecNode>,
    pub remaining: Option<usize>,
    pub skip: usize,
}

impl ExecNode for LimitExec {
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    /// 透传预取（见 [`ExecNode::prefetch`]）：本节点 1:1 转发输入，
    /// 预取请求要一路传到真正的数据源（`ParScanExec`）才有效。
    fn prefetch(&mut self) {
        self.input.prefetch();
    }

    fn next(&mut self) -> Option<Result<RecordBatch>> {
        loop {
            if self.remaining == Some(0) {
                return None;
            }
            let batch = self.input.next()?;
            let mut batch = match batch {
                Ok(b) => b,
                Err(e) => return Some(Err(e)),
            };
            // **先跳 OFFSET**：整批落在跳过区就丢掉继续取；部分落进则切片。
            if self.skip > 0 {
                let n = batch.num_rows();
                if self.skip >= n {
                    self.skip -= n;
                    continue;
                }
                batch = batch.slice(self.skip, n - self.skip);
                self.skip = 0;
            }
            let Some(lim) = self.remaining else {
                return Some(Ok(batch)); // 无 LIMIT：跳过之后全给
            };
            if batch.num_rows() > lim {
                let sliced = batch.slice(0, lim);
                self.remaining = Some(0);
                return Some(Ok(sliced));
            }
            self.remaining = Some(lim - batch.num_rows());
            return Some(Ok(batch));
        }
    }
}

/// 排序键。
#[derive(Debug, Clone)]
pub struct SortKey {
    pub expr: PhysExpr,
    pub descending: bool,
    /// `NULLS FIRST/LAST`。`None` = 未写（用默认放置：`ASC ⇒ NULLS LAST`、
    /// `DESC ⇒ NULLS FIRST`，与 PG 一致）。
    ///
    /// **为什么是 Option 而不是 bool**：要区分"没写"与"写了默认值"——前者是
    /// 未指定语义，后者在更早的版本里曾**被静默忽略**（见 `docs/SQL支持面.md`）。
    pub nulls_first: Option<bool>,
}

impl SortKey {
    /// 该键的 NULL 放置：显式值优先，否则按方向取默认。
    #[inline]
    pub fn nulls_first_or_default(&self) -> bool {
        self.nulls_first.unwrap_or(self.descending)
    }
}

/// 排序 / TopN（物化 + lexsort；limit=Some 即 TopN）。
pub struct SortExec {
    pub input: Box<dyn ExecNode>,
    pub keys: Vec<SortKey>,
    pub limit: Option<usize>,
    schema: SchemaRef,
    ctx: Arc<crate::query::QueryContext>,
}

impl SortExec {
    pub fn new(
        input: Box<dyn ExecNode>,
        keys: Vec<SortKey>,
        limit: Option<usize>,
        ctx: Arc<crate::query::QueryContext>,
    ) -> Self {
        let schema = input.schema();
        Self {
            input,
            keys,
            limit,
            schema,
            ctx,
        }
    }

    fn collect_all(&mut self) -> Result<RecordBatch> {
        // 内存超限 → 落盘（chunk 排序后写临时 parquet run）而非终止查询
        let mut spill_dir: Option<spill::SpillDir> = None;
        let mut run_paths: Vec<std::path::PathBuf> = Vec::new();
        let mut chunk: Vec<RecordBatch> = Vec::new();
        let mut chunk_bytes = 0u64;
        // 内存预留：覆盖当前驻留 chunk（落盘即 shrink 归还）；RAII，函数返回即清零
        let mut res = self.ctx.try_reserve(0)?;

        let flush_chunk = |chunk: &mut Vec<RecordBatch>,
                           chunk_bytes: &mut u64,
                           spill_dir: &mut Option<spill::SpillDir>,
                           run_paths: &mut Vec<std::path::PathBuf>|
         -> Result<()> {
            if chunk.is_empty() {
                return Ok(());
            }
            // 走**配置的溢出目录**（`QsdbOptions::spill_dir`）；此处曾是
            // `SpillDir::new()`（系统临时目录）——全仓最后一个没走配置目录的溢出点。
            let base = self.ctx.spill_base().map(|p| p.to_path_buf());
            let dir = spill_dir.get_or_insert_with(|| spill::SpillDir::new_in(base.as_deref()));
            let path = spill::write_sorted_run(dir, chunk, &self.keys, &self.schema)?;
            run_paths.push(path);
            chunk.clear();
            *chunk_bytes = 0;
            Ok(())
        };

        // TopN 的**有界缓冲**：带 limit 时缓冲没必要超过 n 行——攒到 `8n + 4096` 行就
        // 排序截断到前 n 行（几何增长 → 总工作量仍是 O(N log n)，比分区内重复排序省），
        // 内存从 O(输入) 降到 O(n)。`ORDER BY … LIMIT 10` 排全表是这类查询的常态。
        let topn_keep = self
            .limit
            .map(|n| (n.saturating_mul(8)).saturating_add(4096));
        let mut buffered_rows = 0usize;

        while let Some(b) = self.input.next() {
            self.ctx.check()?;
            let x = b?;
            let bytes = crate::memory::batch_bytes(&x);
            // 要不到额度（自己的配额满 **或** 全库吃紧）→ 当前 chunk 落盘，
            // 之后受磁盘而非内存约束（**溢出只改完成 vs 拒绝的边界**）
            if res.grow(bytes).is_err() {
                let back = chunk_bytes;
                flush_chunk(&mut chunk, &mut chunk_bytes, &mut spill_dir, &mut run_paths)?;
                res.shrink(back); // chunk 已写盘并从内存释放
                if res.grow(bytes).is_ok() {
                    // 腾空后单批放得下：继续攒
                    chunk_bytes = bytes;
                    chunk.push(x);
                } else {
                    // 单批仍超全额：独立成 run（不驻留内存）
                    let mut single = Vec::new();
                    single.push(x);
                    flush_chunk(&mut single, &mut 0, &mut spill_dir, &mut run_paths)?;
                }
                continue;
            }
            chunk_bytes += bytes;
            buffered_rows += x.num_rows();
            chunk.push(x);

            // TopN 有界缓冲（见 `topn_keep` 的注释）：攒够了就**就地截断**到前 n 行。
            // 正确性：全体前 n = 「已见的全体前 n」的前 n，逐次截断因此不丢答案。
            if let Some(keep) = topn_keep {
                if buffered_rows > keep {
                    let merged = sort_in_memory(std::mem::take(&mut chunk), &self.schema())?;
                    let cut = sort_with_limit(&merged, &self.keys, self.limit, &self.schema)?;
                    let back = chunk_bytes;
                    res.shrink(back);
                    chunk_bytes = crate::memory::batch_bytes(&cut);
                    buffered_rows = cut.num_rows();
                    // 截断后必然更小（≤ 前 n 行）；拿不回额度也不影响正确性，
                    // 只影响记账精度——同溢出路径的口径（不因额度打死可完成的查询）
                    let _ = res.grow(chunk_bytes);
                    chunk.clear();
                    if cut.num_rows() > 0 {
                        chunk.push(cut);
                    }
                }
            }
        }

        if run_paths.is_empty() {
            // 纯内存路径（原逻辑）。concat + take 期间瞬时约 2x——峰值真实存在，
            // 就该记账；但**要不到这份峰值额度不是终止理由**：本算子有溢出能力
            // （P1 承诺「额度不够就落盘完成，而不是把查询打死」），尾块落盘后
            // 内存即时归还、改走下面的归并即可。
            //
            // 回归（本行原为 `res.grow(res.bytes())?`）：`res` 此刻已覆盖驻留的
            // chunk（= 全量输入），再 grow 同样的字节是**在已有预留之上**要第二份，
            // 于是「输入 ≤ 配额」的查询只要输入 > 配额/2（2x 峰值越过配额）就被
            // 直接上抛 QueryMemoryLimit——小配额下（如 64MB 配额排 39MB 数据）
            // 明明装得下、且溢出路径可用，却报「at 78MB would exceed its 64MB limit」。
            if res.grow(res.bytes()).is_ok() {
                return sort_in_memory(chunk, &self.schema());
            }
            let back = chunk_bytes;
            flush_chunk(&mut chunk, &mut chunk_bytes, &mut spill_dir, &mut run_paths)?;
            res.shrink(back); // 尾块已写盘并从内存释放
        }
        // 有 run：尾块排序成内存 run，k 路归并（一次性产出合并批；
        // 流式化留待后续——排序输出本来就要被 limit/take 整体消费）
        let mut runs: Vec<Vec<RecordBatch>> = Vec::new();
        for p in &run_paths {
            runs.push(spill::read_run(p)?);
        }
        if !chunk.is_empty() {
            let sorted = sort_in_memory(chunk, &self.schema)?;
            if sorted.num_rows() > 0 {
                runs.push(vec![sorted]);
            }
        }
        let mut merger = spill::RunMerger::new(runs, &self.keys)?;
        let mut outs = Vec::new();
        while let Some(b) = merger.next_batch(&self.keys)? {
            outs.push(b);
        }
        drop(spill_dir); // 清理临时文件
        spill::concat_batches(&outs, &self.schema)
    }
}

/// 纯内存排序：concat → lexsort → take。
fn sort_in_memory(batches: Vec<RecordBatch>, schema: &SchemaRef) -> Result<RecordBatch> {
    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::clone(schema)));
    }
    if batches.len() == 1 {
        return Ok(batches.into_iter().next().unwrap());
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
        &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(n)),
    )
    .map_err(|e| QsdbError::invalid(format!("concat batch: {e}")))
}

impl ExecNode for SortExec {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
    fn next(&mut self) -> Option<Result<RecordBatch>> {
        let data = match self.collect_all() {
            Ok(d) => d,
            Err(e) => return Some(Err(e)),
        };
        if data.num_rows() == 0 {
            return None;
        }
        Some(sort_with_limit(
            &data,
            &self.keys,
            self.limit,
            &self.schema(),
        ))
    }
}

/// 按 `keys` 排序 `data` 并（可选）截断到前 `limit` 行。
///
/// **两种情形共用同一份比较口径**（`SortOptions`：pg 语义 ASC NULLS LAST /
/// DESC NULLS FIRST）：最终输出，以及 TopN 有界缓冲的**中间截断**——中间截断若
/// 用了另一套比较规则，就会悄悄丢答案，所以这里是同一个函数。
fn sort_with_limit(
    data: &RecordBatch,
    keys: &[SortKey],
    limit: Option<usize>,
    schema: &SchemaRef,
) -> Result<RecordBatch> {
    let mut sort_cols: Vec<SortColumn> = Vec::with_capacity(keys.len());
    for k in keys {
        let arr = eval::eval(&k.expr, data)?;
        sort_cols.push(SortColumn {
            values: arr,
            options: Some(arrow::compute::SortOptions {
                descending: k.descending,
                nulls_first: k.nulls_first_or_default(),
            }),
        });
    }
    let indices = lexsort_to_indices(&sort_cols, limit)
        .map_err(|e| QsdbError::invalid(format!("sort failed: {e}")))?;
    let mut cols = Vec::with_capacity(data.num_columns());
    for c in data.columns() {
        cols.push(take(c, &indices, None).map_err(|e| QsdbError::invalid(format!("take: {e}")))?);
    }
    RecordBatch::try_new_with_options(
        Arc::clone(schema),
        cols,
        &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(indices.len())),
    )
    .map_err(|e| QsdbError::invalid(format!("sort output: {e}")))
}

/// 去重（哈希；group = 全部列）。
pub struct DistinctExec {
    pub input: Box<dyn ExecNode>,
    state: agg::GroupTable,
    schema: SchemaRef,
    emitted: bool,
    ctx: Arc<crate::query::QueryContext>,
    /// 去重表内存预留（**此前 Distinct 连 ctx 都没有**：不记账、不检查取消、
    /// 不吃超时——隔离承诺在这里是空的）。RAII，节点销毁即归还。
    res: crate::memory::Reservation,
    accounted: u64,
    /// 溢出态（与 HashAggExec 同机制：分区落盘 → 逐分区聚合）
    spill: Option<crate::exec::agg_spill::AggSpill>,
    spill_part: usize,
}

impl DistinctExec {
    pub fn new(input: Box<dyn ExecNode>, ctx: Arc<crate::query::QueryContext>) -> Result<Self> {
        let schema = input.schema();
        let res = ctx.try_reserve(0)?;
        Ok(Self {
            input,
            state: agg::GroupTable::new(),
            schema,
            emitted: false,
            ctx,
            res,
            accounted: 0,
            spill: None,
            spill_part: 0,
        })
    }
}

impl ExecNode for DistinctExec {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
    fn next(&mut self) -> Option<Result<RecordBatch>> {
        if self.emitted {
            return None;
        }
        // group by 全部输入列（列表达式 0..n）
        let n = self.schema().fields().len();
        let group_exprs: Vec<PhysExpr> = (0..n)
            .map(|i| PhysExpr::Column {
                idx: i,
                name: self.schema().field(i).name().clone(),
                ty: crate::types::LogicalType::try_from_arrow(self.schema().field(i).data_type())
                    .unwrap_or(crate::types::LogicalType::Int64),
            })
            .collect();
        // 已在溢出态：逐分区产出
        if let Some(sp) = self.spill.take() {
            if self.spill_part >= sp.nparts() {
                self.emitted = true;
                self.spill = Some(sp);
                return None;
            }
            let pi = self.spill_part;
            self.spill_part += 1;
            let mut t = agg::GroupTable::new();
            let ogs: Vec<usize> = {
                let lits = self.state.group_lits();
                let n = self.state.len();
                (0..n)
                    .filter(|&g| {
                        let l: Vec<crate::types::Literal> =
                            lits.iter().map(|c| c[g].clone()).collect();
                        crate::exec::agg_spill::partition_of_lits(&l, sp.nparts()) == pi
                    })
                    .collect()
            };
            if !ogs.is_empty() {
                t.merge_subset(&self.state, ogs, 0);
            }
            // **流式**吃该分区（峰值 = 一个 run）：整个分区的行一起物化等于要求
            // "单分区放得下"，小预算下做不到也没必要（聚合是逐批可吃的）。
            // `take` 出 `sp` 是为了让闭包能借 `self` 的其它字段（借用冲突）。
            let mut sp = sp;
            let r = sp.for_each_partition(pi, |b| t.ingest(&group_exprs, &[], b));
            self.spill = Some(sp);
            if let Err(e) = r {
                return Some(Err(e));
            }
            if t.is_empty() {
                return self.next(); // 空分区：继续下一个
            }
            // **列式**输出：组表本来就是 SoA，别再转置成行式（见 `build_output_batch_cols`）
            return Some(agg::build_output_batch_cols(&t, &self.schema()));
        }
        while let Some(b) = self.input.next() {
            if let Err(e) = self.ctx.check() {
                return Some(Err(e));
            }
            let batch = match b {
                Ok(x) => x,
                Err(e) => return Some(Err(e)),
            };
            if let Err(e) = self.state.ingest(&group_exprs, &[], &batch) {
                return Some(Err(e));
            }
            let est = self.state.estimated_bytes();
            if est > self.accounted {
                if self.res.grow(est - self.accounted).is_err() {
                    // 要不到额度 → 转分区溢出（去重与聚合同机制：组 = 全部列）
                    let schema = self.input.schema();
                    match crate::exec::agg_spill::AggSpill::new(schema, &self.ctx) {
                        Ok(sp) => self.spill = Some(sp),
                        Err(e) => return Some(Err(e)),
                    }
                    break;
                }
                self.accounted = est;
            }
        }
        if let Some(sp) = self.spill.as_mut() {
            while let Some(b) = self.input.next() {
                if let Err(e) = self.ctx.check() {
                    return Some(Err(e));
                }
                let batch = match b {
                    Ok(x) => x,
                    Err(e) => return Some(Err(e)),
                };
                if let Err(e) = sp.push(&batch, &group_exprs) {
                    return Some(Err(e));
                }
            }
            self.spill_part = 0;
            return self.next();
        }
        self.emitted = true;
        if std::env::var("QSDB_AGG_PROF").is_ok() {
            let mut parts = self.state.profile_bytes();
            parts.push(("estimated_bytes", self.state.estimated_bytes()));
            parts.push(("groups", self.state.len() as u64));
            eprintln!(
                "[agg-prof] distinct rows={} {}",
                self.accounted,
                parts
                    .iter()
                    .map(|(k, v)| format!("{k}={v}"))
                    .collect::<Vec<_>>()
                    .join(" ")
            );
        }
        if self.state.is_empty() {
            return None;
        }
        // **列式**输出：组表本来就是 SoA，别再转置成行式（见 `build_output_batch_cols`）
        match agg::build_output_batch_cols(&self.state, &self.schema()) {
            Ok(o) => Some(Ok(o)),
            Err(e) => Some(Err(e)),
        }
    }
}

// ---------------------------------------------------------------------------
// `count(*) over DISTINCT` 的**融合计数**：只数"不同的行"，不物化去重结果。
//
// 认出的形状（`build_aggregate`）：
//     Aggregate{ group: [], aggs: [count(*)] } ← Distinct ← X
// 语义 = X 的不同行数。基准那句 `SELECT count(*) FROM (SELECT DISTINCT gs FROM t)`
// 与绑定层从 `count(DISTINCT x)` 改写出来的计划都长这样。
//
// 相比"先物化 250 万行、再数一遍"省掉三件事：
// 1. 中间批（250 万次 `literals_to_array` 拼字符串数组）；
// 2. 第二遍聚合（250 万行再入一次组表）；
// 3. 组表里的 literal（每组一次 `String` 克隆 + `Literal` 槽）——
//    见 `agg::GroupTable::count_only`。
//
// ⚠️ 集合**必须带真值**（`key_arena` 存完整键）：只存 64 位哈希是有碰撞概率的
// **静默错值**，不能为了省内存那么干。
// ---------------------------------------------------------------------------
pub struct DistinctCountExec {
    input: Box<dyn ExecNode>,
    /// 去重键 = 输入的全部列（`LogicalPlan::Distinct` 语义就是"整行去重"）
    keys: Vec<PhysExpr>,
    state: agg::GroupTable,
    ctx: Arc<crate::query::QueryContext>,
    res: crate::memory::Reservation,
    accounted: u64,
    spill: Option<agg_spill::PartSpill>,
    emitted: bool,
    schema: SchemaRef,
}

/// 键字节 → 溢出分区号。**内存侧（`key_of`）与批侧（`encode_key_row`）必须走同一个**
/// —— 两套口径会让同一逻辑键落到不同分区，两边各算一次 ⇒ **静默多数**。
fn route_key_bytes(key: &[u8], nparts: usize) -> usize {
    // FNV-1a（与 `agg::fxhash_bytes` 无关，只要求本文件内自洽）
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in key {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x100_0000_01b3);
    }
    (h % nparts as u64) as usize
}

impl DistinctCountExec {
    /// `out_schema` 是**聚合节点的输出 schema**（`count(*)` 那一列），不是输入的
    /// —— 本节点替代的是整个 `Aggregate ← Distinct` 两层，上层按聚合的 schema 认列。
    pub fn new(
        input: Box<dyn ExecNode>,
        out_schema: SchemaRef,
        ctx: Arc<crate::query::QueryContext>,
    ) -> Result<Self> {
        let in_schema = input.schema();
        // 键 = 全部输入列（与 `DistinctExec` 同款：那里也是"整行做组键"）
        let keys: Vec<PhysExpr> = in_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| PhysExpr::Column {
                idx: i,
                name: f.name().clone(),
                ty: crate::types::LogicalType::try_from_arrow(f.data_type())
                    .unwrap_or(crate::types::LogicalType::Int64),
            })
            .collect();
        let res = ctx.try_reserve(0)?;
        Ok(Self {
            input,
            keys,
            state: agg::GroupTable::new_count_only(),
            ctx,
            res,
            accounted: 0,
            spill: None,
            emitted: false,
            schema: out_schema,
        })
    }

    /// 溢出：把**剩下的**输入批按键字节分区落盘。
    fn spill_rest(&mut self) -> Result<()> {
        if std::env::var("QSDB_DISTINCT_PROF").is_ok() {
            eprintln!(
                "[distinct-count] 转溢出重分区：内存中已建组 {}",
                self.state.len()
            );
        }
        let schema = self.input.schema();
        let (nparts, cap) = agg_spill::PartSpill::plan(&self.ctx);
        let mut sp = agg_spill::PartSpill::new(schema, &self.ctx, nparts, cap)?;
        let mut buf: Vec<u8> = Vec::with_capacity(64);
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(self.keys.len());
        while let Some(b) = self.input.next() {
            self.ctx.check()?;
            let batch = b?;
            arrays.clear();
            for k in &self.keys {
                arrays.push(eval::eval(k, &batch)?);
            }
            let mut parts: Vec<u32> = Vec::with_capacity(batch.num_rows());
            for row in 0..batch.num_rows() {
                agg::encode_key_row(&arrays, row, &mut buf);
                parts.push(route_key_bytes(&buf, sp.nparts()) as u32);
            }
            sp.push_routed(&batch, &parts)?;
        }
        self.spill = Some(sp);
        Ok(())
    }

    /// 逐分区取回：每个分区一张新表，**先灌入内存里已归到本分区的组**（同一个键
    /// 字节口径），再流式吃本分区的落盘行。各分区互不相交 ⇒ 组数直接相加。
    fn drain_partitions(&mut self) -> Result<u64> {
        let mut sp = self.spill.take().expect("溢出态");
        let nparts = sp.nparts();
        let mut by_part: Vec<Vec<u32>> = vec![Vec::new(); nparts];
        for gid in 0..self.state.len() as u32 {
            let p = route_key_bytes(self.state.key_of(gid as usize), nparts);
            by_part[p].push(gid);
        }
        let keys = self.keys.clone();
        let mut total: u64 = 0;
        for (p, gids) in by_part.iter().enumerate() {
            let mut t = agg::GroupTable::new_count_only();
            for &gid in gids {
                t.insert_raw_key(self.state.key_of(gid as usize));
            }
            sp.for_each_partition(p, |b| t.ingest(&keys, &[], b))?;
            total += t.len() as u64;
        }
        Ok(total)
    }

    fn run(&mut self) -> Result<u64> {
        while let Some(b) = self.input.next() {
            self.ctx.check()?;
            let batch = b?;
            self.state.ingest(&self.keys, &[], &batch)?;
            let est = self.state.estimated_bytes();
            if est > self.accounted {
                if self.res.grow(est - self.accounted).is_err() {
                    // 额度不够 → 转溢出（与聚合/去重同机制）
                    self.spill_rest()?;
                    return self.drain_partitions();
                }
                self.accounted = est;
            }
        }
        Ok(self.state.len() as u64)
    }

    fn one_row(&self, total: u64) -> Result<RecordBatch> {
        Self::one_row_static(total, &self.schema)
    }

    pub(crate) fn one_row_static(total: u64, schema: &SchemaRef) -> Result<RecordBatch> {
        let arr = arrow::array::Int64Array::from(vec![total as i64]);
        RecordBatch::try_new(Arc::clone(schema), vec![Arc::new(arr)])
            .map_err(|e| QsdbError::unsupported(format!("count() 输出构造失败: {e}")))
    }
}

impl ExecNode for DistinctCountExec {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
    fn next(&mut self) -> Option<Result<RecordBatch>> {
        if self.emitted {
            return None;
        }
        self.emitted = true;
        match self.run() {
            Ok(total) => Some(self.one_row(total)),
            Err(e) => Some(Err(e)),
        }
    }
}

// ---------------------------------------------------------------------------
// `count(*) over DISTINCT` 的**哈希分区并行**（P1⑪）。
//
// 与"行分片 + 键并集"（**实测否决**）的本质区别：这里按**键**切。
// 同一个键的字节相同 ⇒ 哈希相同 ⇒ **必落同一分区** ⇒ 各分区的键集**不相交**
// ⇒ **各分区的组数可以直接相加**，不需要求并集。
// （旧方案按行切，片间键集相交，并集要串行重灌 500 万个键 —— 那正是它输掉的地方。）
//
// 两相（刻意选最简形态，不为并发引入新原语）：
//   相 1（顺序）：逐行算 `part = fxhash(编码键) % N`，把**键字节**追加进该分区的
//                arena（`Vec<u8>` + `(offset,len)` spans）。**只搬键，不搬整行**。
//   相 2（并行）：`into_par_iter` 各分区各建一张 count-only 表
//                （用**已写好并测过**的 `insert_raw_key`），`len()` **相加**。
//
// ⚠️ 内存：相 1 的缓冲是这批唯一的额外内存（5M 行 ≈ 135MB）。调用方**必须**
// 在动手前拿到行数上界并 `try_reserve` 成功才能选这条路 —— 一旦开读，行数据就
// 变成了分区缓冲，**回不去**（这是它与 [`DistinctCountExec`] 溢出路径最大的不同）。
pub struct PartitionedDistinctCountExec {
    input: Box<dyn ExecNode>,
    /// 去重键 = 输入的全部列（与 `DistinctCountExec` 同口径）
    keys: Vec<PhysExpr>,
    nparts: usize,
    ctx: Arc<crate::query::QueryContext>,
    /// 相 1 缓冲的内存预留（活到本节点销毁）
    res: crate::memory::Reservation,
    accounted: u64,
    emitted: bool,
    schema: SchemaRef,
}

/// 一个分区的键字节：`arena` 里连续放，`spans` 是每个键的 `(offset, len)`。
#[derive(Default)]
struct PartBuf {
    arena: Vec<u8>,
    spans: Vec<(u32, u32)>,
}

impl PartBuf {
    #[inline]
    fn push(&mut self, key: &[u8]) {
        let off = self.arena.len() as u32;
        self.arena.extend_from_slice(key);
        self.spans.push((off, key.len() as u32));
    }

    fn bytes(&self) -> u64 {
        self.arena.capacity() as u64 + self.spans.capacity() as u64 * 8
    }
}

impl PartitionedDistinctCountExec {
    pub fn new(
        input: Box<dyn ExecNode>,
        out_schema: SchemaRef,
        nparts: usize,
        res: crate::memory::Reservation,
        ctx: Arc<crate::query::QueryContext>,
    ) -> Result<Self> {
        let in_schema = input.schema();
        let keys: Vec<PhysExpr> = in_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| PhysExpr::Column {
                idx: i,
                name: f.name().clone(),
                ty: crate::types::LogicalType::try_from_arrow(f.data_type())
                    .unwrap_or(crate::types::LogicalType::Int64),
            })
            .collect();
        Ok(Self {
            input,
            keys,
            nparts: nparts.max(1),
            ctx,
            res,
            accounted: 0,
            emitted: false,
            schema: out_schema,
        })
    }

    fn run(&mut self) -> Result<u64> {
        let n = self.nparts;
        let mut parts: Vec<PartBuf> = (0..n).map(|_| PartBuf::default()).collect();
        let mut buf: Vec<u8> = Vec::with_capacity(64);
        let prof = std::env::var("QSDB_DISTINCT_PROF").is_ok();
        let t0 = std::time::Instant::now();
        while let Some(b) = self.input.next() {
            self.ctx.check()?;
            let batch = b?;
            let arrays: Vec<ArrayRef> = self
                .keys
                .iter()
                .map(|k| eval::eval(k, &batch))
                .collect::<Result<_>>()?;
            for row in 0..batch.num_rows() {
                agg::encode_key_row(&arrays, row, &mut buf);
                let p = (agg::fxhash_bytes(&buf) % n as u64) as usize;
                parts[p].push(&buf);
            }
            // 逐批记账；**要不到上限也继续**（见类型注释：缓冲已经吃掉行数据，
            // 回不去 —— 此时拒绝只是把已提交的内存算成没算）。总量由调用方的
            // 预检（`行数 × 32B`）兜住。
            let est: u64 = parts.iter().map(PartBuf::bytes).sum();
            if est > self.accounted && self.res.grow(est - self.accounted).is_ok() {
                self.accounted = est;
            }
        }
        if prof {
            eprintln!(
                "[distinct-count] 哈希分区并行：{n} 分区，缓冲 {}MB，相1（顺序路由）{:?}",
                parts.iter().map(PartBuf::bytes).sum::<u64>() / (1024 * 1024),
                t0.elapsed()
            );
        }
        // 相 2：各分区独立去重 —— **键集不相交 ⇒ 组数直接相加**
        use rayon::prelude::*;
        let t1 = std::time::Instant::now();
        let total: u64 = parts
            .into_par_iter()
            .map(|p| {
                let mut t = agg::GroupTable::new_count_only();
                for &(off, len) in &p.spans {
                    t.insert_raw_key(&p.arena[off as usize..(off + len) as usize]);
                }
                t.len() as u64
            })
            .sum();
        if prof {
            eprintln!("[distinct-count] 相2（并行建组）{:?}，共 {total} 组", t1.elapsed());
        }
        Ok(total)
    }
}

impl ExecNode for PartitionedDistinctCountExec {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
    fn next(&mut self) -> Option<Result<RecordBatch>> {
        if self.emitted {
            return None;
        }
        self.emitted = true;
        match self.run() {
            Ok(total) => Some(DistinctCountExec::one_row_static(total, &self.schema)),
            Err(e) => Some(Err(e)),
        }
    }
}

// ---------------------------------------------------------------------------
// `count(*) over DISTINCT` 的**排序去重**路（v2，第十三批）—— 见
// `docs/去重分区路v2设计.md`。与 v1（[`PartitionedDistinctCountExec`]）的
// 三点结构差异（都来自 .14 的相拆账实测：相 1 顺序 228ms、相 2 并行 420ms）：
//
//   1. **条目零拷贝**：相 1 不再"编码 → 拷进分区 arena"，只存
//      `(哈希, 行引用)` 16 字节定长条目 —— 键字节留在箭头缓冲（Arc 保活），
//      v1 的"三拷"省掉两拷；
//   2. **哈希只算一次**：相 1 算完存进条目，路由/排序/分桶全用存下的值 ——
//      v1 的 route/insert 两套哈希口径问题在这里结构性不存在；
//   3. **相 2 排序去重**：条目按哈希排序后线性扫 —— 同哈希必相邻，把
//      250 万次随机哈希表插入（内存延迟墙，实测 ~1.3µs/行）换成
//      L3 内排序 + 顺序扫（随机访问只在"留样比真字节"的首次触碰发生）。
//
// 输入可分片（剥 `Project*`/`Filter*` 到 Scan，gate 在 `exec_builder`）时
// `inputs` 是 N 个行不相交分片节点，相 1 并行路由；否则 1 个节点顺序跑 ——
// 两种形态走同一套代码。适用面：**单 Utf8 键**才进这条路（多键/混合键留 v1）。

/// 一个去重条目：`h` 是键哈希（NULL/非空用 tag 混进哈希），`br` 打包
/// `(batch_idx << 32) | row` —— 行字节在 `cols[batch_idx]` 的箭头缓冲里。
#[derive(Clone, Copy)]
struct DistEntry {
    h: u64,
    br: u64,
}

/// 相 1 单分片的产出：各分区条目 + 本片各批的键列（条目引用的字节在这里）。
struct DistShardOut {
    parts: Vec<Vec<DistEntry>>,
    cols: Vec<arrow::array::StringArray>,
}

/// 行字节比较：都 NULL ⇒ 相等；一方 NULL ⇒ 不等；否则比字符串字节。
/// **哈希碰撞必须落到这条** —— 只信哈希 = 静默错值。
fn dist_entry_eq(cols: &[arrow::array::StringArray], a: u64, b: u64) -> bool {
    let x = &cols[(a >> 32) as usize];
    let y = &cols[(b >> 32) as usize];
    let (ar, br) = (a as u32 as usize, b as u32 as usize);
    match (x.is_null(ar), y.is_null(br)) {
        (true, true) => true,
        (true, false) | (false, true) => false,
        (false, false) => x.value(ar).as_bytes() == y.value(br).as_bytes(),
    }
}

/// 排序后的线性去重：返回**不同值的个数**。
///
/// 同哈希 run 内逐个与**全部留样**比真字节 —— 碰撞时留样长到 2+，
/// 不同值永不被吞。`kept` 跨 run 复用（免每 run 一次堆分配）。
fn dist_dedup_sorted(cols: &[arrow::array::StringArray], es: &mut [DistEntry]) -> u64 {
    es.sort_unstable_by_key(|e| e.h);
    let mut count = 0u64;
    let mut kept: Vec<u64> = Vec::with_capacity(4);
    let mut i = 0;
    while i < es.len() {
        count += 1;
        let h = es[i].h;
        kept.clear();
        kept.push(es[i].br);
        let mut j = i + 1;
        while j < es.len() && es[j].h == h {
            let br = es[j].br;
            if !kept.iter().any(|&k| dist_entry_eq(cols, k, br)) {
                kept.push(br);
                count += 1;
            }
            j += 1;
        }
        i = j;
    }
    count
}

pub struct SortedDistinctCountExec {
    /// 1 个 = 顺序相 1；N 个 = 行不相交分片（相 1 并行路由）
    inputs: Vec<Box<dyn ExecNode>>,
    nparts: usize,
    ctx: Arc<crate::query::QueryContext>,
    /// 相 1 缓冲的内存预留（活到本节点销毁；预检由调用方按 行数×32B 给足）
    res: crate::memory::Reservation,
    accounted: u64,
    emitted: bool,
    schema: SchemaRef,
}

impl SortedDistinctCountExec {
    pub fn new(
        inputs: Vec<Box<dyn ExecNode>>,
        out_schema: SchemaRef,
        nparts: usize,
        res: crate::memory::Reservation,
        ctx: Arc<crate::query::QueryContext>,
    ) -> Self {
        Self {
            inputs,
            nparts: nparts.max(1),
            ctx,
            res,
            accounted: 0,
            emitted: false,
            schema: out_schema,
        }
    }

    /// 相 1：单分片顺序路由。**零拷贝**：每行只哈希 `value(i)` 的字节切片
    /// 并落一个 16B 条目；键列以 Arc 克隆保活（实到不是 Utf8 —— 比如字典
    /// 编码 —— 就逐批 cast 一次，罕见路；schema 闸门已判过 Utf8）。
    fn route_one(
        mut input: Box<dyn ExecNode>,
        nparts: usize,
        ctx: &Arc<crate::query::QueryContext>,
    ) -> Result<DistShardOut> {
        let n = nparts;
        let mut parts: Vec<Vec<DistEntry>> = (0..n).map(|_| Vec::new()).collect();
        let mut cols: Vec<arrow::array::StringArray> = Vec::new();
        let key = PhysExpr::Column {
            idx: 0,
            name: input.schema().field(0).name().clone(),
            ty: crate::types::LogicalType::try_from_arrow(input.schema().field(0).data_type())
                .unwrap_or(crate::types::LogicalType::Int64),
        };
        while let Some(b) = input.next() {
            ctx.check()?;
            let batch = b?;
            let arr = eval::eval(&key, &batch)?;
            let col = match arr.as_any().downcast_ref::<arrow::array::StringArray>() {
                Some(c) => c.clone(),
                None => {
                    let casted = arrow::compute::cast(&arr, &arrow::datatypes::DataType::Utf8)
                        .map_err(|e| {
                            QsdbError::unsupported(format!("去重键列转 Utf8 失败: {e}"))
                        })?;
                    casted
                        .as_any()
                        .downcast_ref::<arrow::array::StringArray>()
                        .ok_or_else(|| {
                            QsdbError::unsupported("去重键列 cast 后仍不是 Utf8（内部错误）")
                        })?
                        .clone()
                }
            };
            let base = cols.len() as u64;
            for row in 0..col.len() {
                // tag 混进哈希：NULL 与空串哈希不同值（正确性靠字节比对，
                // 这只是少一次无谓比较）；NULL 不产生字节。
                let (tag, bytes): (u64, &[u8]) = if col.is_null(row) {
                    (0, &[])
                } else {
                    (1, col.value(row).as_bytes())
                };
                let h = agg::fxhash_bytes(bytes) ^ tag.wrapping_mul(0x9e37_79b9_7f4a_7c15);
                // Lemire 整数桶：免除法；路由/排序/分桶全用这个存下的 h
                let p = ((h as u128 * n as u128) >> 64) as usize;
                parts[p].push(DistEntry {
                    h,
                    br: (base << 32) | row as u64,
                });
            }
            cols.push(col);
        }
        Ok(DistShardOut { parts, cols })
    }

    fn run(&mut self) -> Result<u64> {
        use rayon::prelude::*;
        let prof = std::env::var("QSDB_DISTINCT_PROF").is_ok();
        let nshard = self.inputs.len();
        let t0 = std::time::Instant::now();
        // ── 相 1：分片并行路由 ──
        let outs: Vec<DistShardOut> = std::mem::take(&mut self.inputs)
            .into_par_iter()
            .map(|node| Self::route_one(node, self.nparts, &self.ctx))
            .collect::<Result<_>>()?;
        // ── 拼接 + 批号全局重编号（一次线性扫；条目 16B/条）──
        let mut parts: Vec<Vec<DistEntry>> = (0..self.nparts).map(|_| Vec::new()).collect();
        let mut cols: Vec<arrow::array::StringArray> = Vec::new();
        let mut rows = 0u64;
        for out in outs {
            let base = cols.len() as u64;
            rows += out.parts.iter().map(|v| v.len() as u64).sum::<u64>();
            cols.extend(out.cols);
            for (p, v) in out.parts.into_iter().enumerate() {
                if base != 0 {
                    parts[p]
                        .extend(v.into_iter().map(|mut e| {
                            e.br += base << 32;
                            e
                        }));
                } else {
                    parts[p].extend(v);
                }
            }
        }
        // 记账（预检已在调用方按 行数×32B 给足；这里按实补差额，尽力而为）
        let est: u64 = parts.iter().map(|v| v.capacity() as u64 * 16).sum::<u64>()
            + cols
                .iter()
                .map(|c| c.get_array_memory_size() as u64)
                .sum::<u64>();
        if est > self.accounted {
            let _ = self.res.grow(est - self.accounted);
            self.accounted = est;
        }
        if prof {
            eprintln!(
                "[distinct-v2] 分区 {}，分片 {}，条目 {}（{}MB），键列 {}MB，相1 {:?}",
                self.nparts,
                nshard,
                rows,
                parts.iter().map(|v| v.capacity() as u64 * 16).sum::<u64>()
                    / (1024 * 1024),
                cols.iter().map(|c| c.get_array_memory_size() as u64).sum::<u64>()
                    / (1024 * 1024),
                t0.elapsed()
            );
        }
        // ── 相 2：排序去重 —— 键集不相交 ⇒ 组数直接相加 ──
        let t1 = std::time::Instant::now();
        let cols_ref = &cols;
        let total: u64 = parts
            .into_par_iter()
            .map(|mut es| dist_dedup_sorted(cols_ref, &mut es))
            .sum();
        if prof {
            eprintln!("[distinct-v2] 相2 {:?}，共 {total} 组", t1.elapsed());
        }
        Ok(total)
    }
}

impl ExecNode for SortedDistinctCountExec {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
    fn next(&mut self) -> Option<Result<RecordBatch>> {
        if self.emitted {
            return None;
        }
        self.emitted = true;
        match self.run() {
            Ok(total) => Some(DistinctCountExec::one_row_static(total, &self.schema)),
            Err(e) => Some(Err(e)),
        }
    }
}

/// 包装 M1 Scan。
pub struct ScanExec {
    pub scan: crate::scan::Scan,
    schema: SchemaRef,
}

impl ScanExec {
    pub fn new(scan: crate::scan::Scan) -> Self {
        let schema = Arc::clone(scan.schema());
        Self { scan, schema }
    }
}

impl ExecNode for ScanExec {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
    fn next(&mut self) -> Option<Result<RecordBatch>> {
        self.scan.next()
    }
}

/// 聚合执行（串行：单输入流式 partial+finalize）。
pub struct HashAggExec {
    pub input: Box<dyn ExecNode>,
    pub group: Vec<PhysExpr>,
    pub aggs: Vec<AggExpr>,
    pub(crate) state: agg::GroupTable,
    schema: SchemaRef,
    emitted: bool,
    ctx: Arc<crate::query::QueryContext>,
    /// 溢出态（内存要不到额度时启用；见 `exec/agg_spill.rs`）
    spill: Option<crate::exec::agg_spill::AggSpill>,
    /// 溢出模式下正在产出的分区游标
    spill_part: usize,
    /// 由**调用方**接管溢出（分片并行路径：结果要走 merge，不能在这里 emit）
    defer_spill: bool,
    /// 组表内存预留（随组表增长；节点销毁即归还）。
    ///
    /// 旧实现按「输入批 / 2」记一次且**永不 release**——既没算分片放大
    /// （并行分片各建一份完整组表），账又只增不减。现按组表实际容量的
    /// 增量预留，并把预留交给 `into_state` 转移给调用方（分片表在 merge
    /// 期间仍活着，账不能提前放）。
    res: crate::memory::Reservation,
    /// 已入账的组表估算字节（增量预留用）
    accounted: u64,
}

impl HashAggExec {
    pub fn new(
        input: Box<dyn ExecNode>,
        group: Vec<PhysExpr>,
        aggs: Vec<AggExpr>,
        ctx: Arc<crate::query::QueryContext>,
    ) -> Result<Self> {
        let mut fields: Vec<arrow::datatypes::Field> = group
            .iter()
            .map(|g| {
                arrow::datatypes::Field::new(g.display_name(), g.data_type().arrow_type(), true)
            })
            .collect();
        for a in &aggs {
            fields.push(arrow::datatypes::Field::new(
                &a.out_name,
                a.out_ty.arrow_type(),
                true,
            ));
        }
        let mut state = agg::GroupTable::new();
        state.set_arity(aggs.len());
        let res = ctx.try_reserve(0)?;
        Ok(Self {
            input,
            group,
            aggs,
            state,
            schema: Arc::new(arrow::datatypes::Schema::new(fields)),
            emitted: false,
            ctx,
            spill: None,
            spill_part: 0,
            defer_spill: false,
            res,
            accounted: 0,
        })
    }

    /// 分片并行路径：溢出由调用方接管（它要把各分片的结果 merge 起来，
    /// 不能在这里把分区结果当最终行 emit 掉——那样 merge 只会拿到保留表，
    /// **结果静默丢一半**）。
    pub fn set_defer_spill(&mut self, v: bool) {
        self.defer_spill = v;
    }

    /// 取走溢出器（defer 模式下调用方 merge 时用；`None` = 本分片没溢出）。
    pub fn take_spill(&mut self) -> Option<crate::exec::agg_spill::AggSpill> {
        self.spill.take()
    }

    /// 单分区产出：保留表里属于该分区的组 + 该分区落盘的行 → 一次聚合。
    fn spill_next(&mut self) -> Option<Result<RecordBatch>> {
        let mut sp = self.spill.take()?;
        if self.spill_part >= sp.nparts() {
            self.emitted = true;
            self.spill = Some(sp);
            return None;
        }
        let pi = self.spill_part;
        self.spill_part += 1;
        let mut t = agg::GroupTable::new();
        let ogs: Vec<usize> = {
            let lits = self.state.group_lits();
            let n = self.state.len();
            (0..n)
                .filter(|&g| {
                    let l: Vec<crate::types::Literal> = lits.iter().map(|c| c[g].clone()).collect();
                    crate::exec::agg_spill::partition_of_lits(&l, sp.nparts()) == pi
                })
                .collect()
        };
        if !ogs.is_empty() {
            t.merge_subset(&self.state, ogs, self.aggs.len());
        }
        // 流式吃该分区（同 `DistinctExec`：峰值 = 一个 run）
        let agg_list = self.aggs.clone();
        let group = self.group.clone();
        let r = sp.for_each_partition(pi, |b| t.ingest(&group, &agg_list, b));
        self.spill = Some(sp);
        if let Err(e) = r {
            return Some(Err(e));
        }
        Some(agg::finalize_agg(&t, &self.aggs, &self.group, &self.schema))
    }

    /// 取走组表 + 它的内存预留（分片路径用：表要交给 merge，预留也得跟着走，
    /// 否则 merge 期间「N 张分片表同时在内存」这个峰值就不在账上）。
    pub fn into_parts(
        self,
    ) -> (
        agg::GroupTable,
        crate::memory::Reservation,
        Option<crate::exec::agg_spill::AggSpill>,
    ) {
        let Self {
            mut state,
            res,
            spill,
            ..
        } = self;
        (std::mem::take(&mut state), res, spill)
    }
}

impl PhysExpr {
    fn display_name(&self) -> String {
        self.display()
    }
}

impl ExecNode for HashAggExec {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
    fn next(&mut self) -> Option<Result<RecordBatch>> {
        if self.emitted {
            return None;
        }
        // 已在溢出态：逐分区产出（defer 模式下由调用方取走，不在此 emit）
        if self.spill.is_some() {
            if self.defer_spill {
                return None; // 调用方：表 + spill 都归它，走 merge
            }
            return self.spill_next();
        }
        while let Some(b) = self.input.next() {
            if let Err(e) = self.ctx.check() {
                return Some(Err(e));
            }
            let batch = match b {
                Ok(x) => x,
                Err(e) => return Some(Err(e)),
            };
            if let Err(e) = self.state.ingest(&self.group, &self.aggs, &batch) {
                return Some(Err(e));
            }
            // 组表增长 → 增量预留（按实际容器容量，含分片放大）。
            // 一批的滞后可接受：增长上界就是刚摄入这批的规模。
            let est = self.state.estimated_bytes();
            if est > self.accounted {
                if self.res.grow(est - self.accounted).is_err() {
                    // **要不到额度 → 转分区溢出**（而不是把查询打死）。
                    // 已聚合的组保留在 `self.state`（它就是"分区 0 的部分结果"），
                    // 本批也已摄入，故**不重复路由本批**；后续行全部落盘。
                    let schema = self.input.schema();
                    match crate::exec::agg_spill::AggSpill::new(schema, &self.ctx) {
                        Ok(sp) => self.spill = Some(sp),
                        Err(e) => return Some(Err(e)),
                    }
                    break;
                }
                self.accounted = est;
            }
        }
        if let Some(sp) = self.spill.as_mut() {
            // 剩余输入 → 按分组键路由到各分区（超界落盘）
            let group = self.group.clone();
            while let Some(b) = self.input.next() {
                if let Err(e) = self.ctx.check() {
                    return Some(Err(e));
                }
                let batch = match b {
                    Ok(x) => x,
                    Err(e) => return Some(Err(e)),
                };
                if let Err(e) = sp.push(&batch, &group) {
                    return Some(Err(e));
                }
            }
            if self.defer_spill {
                self.emitted = true;
                return None;
            }
            self.spill_part = 0;
            return self.spill_next();
        }
        self.emitted = true;
        // 有 GROUP BY 且零组 = 真·零行（`SELECT a, count(*) FROM t WHERE false GROUP BY a`）。
        // 无 GROUP BY 则**必须回一行**——零输入时由 `finalize_agg` 补的「空组」产出。
        if self.state.is_empty() && !self.group.is_empty() {
            return None;
        }
        let out = match agg::finalize_agg(&self.state, &self.aggs, &self.group, &self.schema()) {
            Ok(o) => o,
            Err(e) => return Some(Err(e)),
        };
        Some(Ok(out))
    }
}

/// v2 排序去重的**碰撞路径**单元测试（`docs/去重分区路v2设计.md` §三）。
/// 随机数据撞不出 64 位哈希碰撞 ⇒ 伪造同哈希条目，确定性地走到
/// "同哈希 run 内比真字节"那条分支 —— 这类优化的失败模式是静默少数/多数，
/// 只看"跑得通"完全看不出来。
#[cfg(test)]
mod distinct_v2_tests {
    use super::*;
    use arrow::array::StringArray;

    fn col(vals: &[Option<&str>]) -> StringArray {
        StringArray::from(vals.to_vec())
    }

    fn e(h: u64, batch: u32, row: u32) -> DistEntry {
        DistEntry {
            h,
            br: ((batch as u64) << 32) | row as u64,
        }
    }

    /// 同哈希 run 里 "a"/"b"/"a" 是 **2** 个值 —— 碰撞不能吞值，重复不能多数。
    #[test]
    fn 排序去重_同哈希不同值必须都算() {
        let cols = vec![col(&[Some("a"), Some("b"), Some("a"), Some("c")])];
        let mut es = vec![
            e(7, 0, 0), // a
            e(7, 0, 1), // b ← 与 a 伪造同哈希（碰撞）
            e(7, 0, 2), // a ← 重复
            e(9, 0, 3), // c
        ];
        assert_eq!(dist_dedup_sorted(&cols, &mut es), 3, "碰撞算 2 + 独立值 1");
    }

    /// NULL 是一个独立值；NULL 与空串不相等（tag 与字节双判，最坏同哈希 run）。
    #[test]
    fn 排序去重_null与空串是两个值() {
        let cols = vec![col(&[None, Some(""), None, Some("")])];
        let mut es = vec![e(5, 0, 0), e(5, 0, 1), e(5, 0, 2), e(5, 0, 3)];
        assert_eq!(dist_dedup_sorted(&cols, &mut es), 2, "NULL 与 '' 各算一个");
    }

    /// 跨批引用（br 高 32 位是批号）+ 乱序输入（排序归位）。
    #[test]
    fn 排序去重_跨批与乱序() {
        let cols = vec![col(&[Some("x")]), col(&[Some("y"), Some("x")])];
        let mut es = vec![e(42, 1, 1), e(42, 0, 0), e(42, 1, 0)];
        // x(批0行0) 与 x(批1行1) 跨批同值；y(批1行0) 不同 ⇒ 2 个值
        assert_eq!(dist_dedup_sorted(&cols, &mut es), 2);
    }
}
