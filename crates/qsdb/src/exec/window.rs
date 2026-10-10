//! 窗口函数算子：物化输入 → (PARTITION BY, ORDER BY) 排序 → 逐分区计算 → 附加列输出。
//!
//! 支持的窗口函数：
//! - 排名类：ROW_NUMBER / RANK / DENSE_RANK
//! - 聚合类（默认帧 = SQL 标准 RANGE 语义）：SUM / COUNT / MIN / MAX / AVG
//!   - 无 ORDER BY → 帧为整个分区，每行取分区总值
//!   - 有 ORDER BY → 无界前驱到当前行（含并列 peer），并列行共享段尾值
//! - 偏移类：LAG / LEAD(arg, offset[, default])——offset 为正整数常量，越出分区取 NULL/default
//!
//! 输出列序：输入列 ++ 窗口列（按 SELECT 中出现顺序）。
//! 输出行序：(PARTITION BY, ORDER BY) 排序后（窗口函数语义允许）。
//!
//! 已知后置：全 NULL 分区的 SUM/MIN/MAX 返回 0、AVG 返回 NaN（应为 NULL）；
//! SUM/AVG 不支持 Decimal 参数（显式报错）。

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray, UInt32Array,
    UInt32Builder,
};
use arrow::compute::kernels::sort::{SortColumn, lexsort_to_indices};
use arrow::compute::{concat, interleave, take};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};

use crate::error::{QsdbError, Result};
use crate::expr::PhysExpr;
use crate::expr::eval;
use crate::query::QueryContext;

use super::ExecNode;

/// 窗口规格：一个 OVER 子句的 PARTITION BY / ORDER BY（绑定后的物理表达式）。
/// 多个窗口函数 spec 相同（signature 相等）时可共享同一 Window 节点。
#[derive(Debug, Clone, Default)]
pub struct WindowSpec {
    pub partition_by: Vec<PhysExpr>,
    pub order_by: Vec<(PhysExpr, bool)>,
}

impl WindowSpec {
    /// 绑定签名（表达式 display 串）：分组共享节点的判据。
    pub fn signature(&self) -> String {
        let p: Vec<String> = self.partition_by.iter().map(|e| e.display()).collect();
        let o: Vec<String> = self
            .order_by
            .iter()
            .map(|(e, d)| format!("{}:{d}", e.display()))
            .collect();
        format!("{}|{}", p.join(","), o.join(","))
    }
}

/// 窗口函数定义（binder 产出）。
#[derive(Debug, Clone)]
pub enum WindowFn {
    RowNumber,
    Rank,
    DenseRank,
    Sum(PhysExpr),
    Count(PhysExpr),
    Min(PhysExpr),
    Max(PhysExpr),
    Avg(PhysExpr),
    /// lag(arg, offset[, default])：分区内在排序序上取前 offset 行的值
    Lag(PhysExpr, i64, Option<Box<PhysExpr>>),
    /// lead(arg, offset[, default])：取后 offset 行的值
    Lead(PhysExpr, i64, Option<Box<PhysExpr>>),
}

impl WindowFn {
    pub fn output_name(&self) -> String {
        match self {
            Self::RowNumber => "row_number()".into(),
            Self::Rank => "rank()".into(),
            Self::DenseRank => "dense_rank()".into(),
            Self::Sum(e) => format!("sum({})", e.display()),
            Self::Count(e) => format!("count({})", e.display()),
            Self::Min(e) => format!("min({})", e.display()),
            Self::Max(e) => format!("max({})", e.display()),
            Self::Avg(e) => format!("avg({})", e.display()),
            Self::Lag(e, off, None) => format!("lag({}, {off})", e.display()),
            Self::Lag(e, off, Some(d)) => format!("lag({}, {off}, {})", e.display(), d.display()),
            Self::Lead(e, off, None) => format!("lead({}, {off})", e.display()),
            Self::Lead(e, off, Some(d)) => {
                format!("lead({}, {off}, {})", e.display(), d.display())
            }
        }
    }

    pub fn output_type(&self) -> crate::types::LogicalType {
        use crate::types::LogicalType;
        match self {
            Self::RowNumber | Self::Rank | Self::DenseRank | Self::Count(_) => LogicalType::Int64,
            Self::Sum(e) => match e.data_type() {
                LogicalType::Float32 | LogicalType::Float64 => LogicalType::Float64,
                t if t.is_numeric() => LogicalType::Int64,
                LogicalType::Decimal(m) => {
                    LogicalType::Decimal(crate::types::DecimalMeta::sum_result(m))
                }
                _ => LogicalType::Int64,
            },
            Self::Avg(_) => LogicalType::Float64,
            Self::Min(e) | Self::Max(e) => e.data_type(),
            Self::Lag(e, ..) | Self::Lead(e, ..) => e.data_type(),
        }
    }
}

/// 窗口执行节点。
pub struct WindowExec {
    input: Box<dyn ExecNode>,
    /// PARTITION BY 表达式
    partition_by: Vec<PhysExpr>,
    /// ORDER BY 表达式 + 方向
    order_by: Vec<(PhysExpr, bool)>, // (expr, descending)
    /// 窗口函数（与输出列一一对应）
    window_fns: Vec<WindowFn>,
    schema: SchemaRef,
    ctx: Arc<QueryContext>,
    output: Option<RecordBatch>,
}

impl WindowExec {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        input: Box<dyn ExecNode>,
        partition_by: Vec<PhysExpr>,
        order_by: Vec<(PhysExpr, bool)>,
        window_fns: Vec<WindowFn>,
        ctx: Arc<QueryContext>,
    ) -> Self {
        // 输出 schema：输入列 ++ 窗口列
        let input_schema = input.schema();
        let mut fields: Vec<arrow::datatypes::FieldRef> =
            input_schema.fields().iter().cloned().collect();
        for f in &window_fns {
            let t = f.output_type();
            fields.push(Arc::new(arrow::datatypes::Field::new(
                f.output_name(),
                t.arrow_type(),
                true,
            )));
        }
        let schema = Arc::new(arrow::datatypes::Schema::new(fields));
        Self {
            input,
            partition_by,
            order_by,
            window_fns,
            schema,
            ctx,
            output: None,
        }
    }

    fn execute_all(&mut self) -> Result<RecordBatch> {
        // 物化输入
        let mut batches = Vec::new();
        let mut charged = 0u64;
        while let Some(b) = self.input.next() {
            self.ctx.check()?;
            let x = b?;
            let bytes = x.get_array_memory_size() as u64;
            self.ctx.charge_memory(bytes)?;
            charged += bytes;
            batches.push(x);
        }
        self.ctx.release_memory(charged);

        if batches.is_empty() {
            return Ok(RecordBatch::new_empty(Arc::clone(&self.schema)));
        }
        // concat
        let input_schema = batches[0].schema();
        let mut cols: Vec<ArrayRef> = Vec::with_capacity(input_schema.fields().len());
        for c in 0..input_schema.fields().len() {
            let parts: Vec<&dyn Array> = batches.iter().map(|b| b.column(c).as_ref()).collect();
            cols.push(
                concat(&parts).map_err(|e| QsdbError::invalid(format!("window concat: {e}")))?,
            );
        }
        let n: usize = batches.iter().map(|b| b.num_rows()).sum();
        let input_batch = RecordBatch::try_new_with_options(
            Arc::clone(&input_schema),
            cols,
            &RecordBatchOptions::new().with_row_count(Some(n)),
        )
        .map_err(|e| QsdbError::invalid(format!("window batch: {e}")))?;

        if n == 0 {
            return self.assemble_empty();
        }

        // 排序键：PARTITION BY ++ ORDER BY（全部 ASC——分区边界检测不需要方向）
        let mut sort_cols: Vec<SortColumn> = Vec::new();
        for p in &self.partition_by {
            let arr = eval::eval(p, &input_batch)?;
            sort_cols.push(SortColumn {
                values: arr,
                options: Some(arrow::compute::SortOptions::default()),
            });
        }
        let mut order_arrs: Vec<ArrayRef> = Vec::new();
        for (e, desc) in &self.order_by {
            let arr = eval::eval(e, &input_batch)?;
            order_arrs.push(Arc::clone(&arr));
            sort_cols.push(SortColumn {
                values: arr,
                options: Some(arrow::compute::SortOptions {
                    descending: *desc,
                    nulls_first: *desc,
                }),
            });
        }
        // 空 spec（OVER ()：无分区无序）→ 恒等序（保持输入行序）
        let indices = if sort_cols.is_empty() {
            (0..n as u32).collect::<UInt32Array>()
        } else {
            lexsort_to_indices(&sort_cols, None)
                .map_err(|e| QsdbError::invalid(format!("window sort: {e}")))?
        };

        // 取排序后的输入列
        let sorted_cols: Vec<ArrayRef> = (0..input_batch.num_columns())
            .map(|c| {
                take(input_batch.column(c), &indices, None)
                    .map_err(|e| QsdbError::invalid(format!("window take: {e}")))
            })
            .collect::<Result<_>>()?;
        let sorted_batch = RecordBatch::try_new_with_options(
            Arc::clone(&input_schema),
            sorted_cols,
            &RecordBatchOptions::new().with_row_count(Some(n)),
        )
        .map_err(|e| QsdbError::invalid(format!("window sorted batch: {e}")))?;

        // 排序后的分区键数组（用于边界检测）
        let part_arrs: Vec<ArrayRef> = self
            .partition_by
            .iter()
            .map(|p| eval::eval(p, &sorted_batch))
            .collect::<Result<_>>()?;
        let sorted_order_arrs: Vec<ArrayRef> = order_arrs
            .iter()
            .map(|a| {
                take(a, &indices, None)
                    .map_err(|e| QsdbError::invalid(format!("window order take: {e}")))
            })
            .collect::<Result<_>>()?;

        // 逐分区计算窗口函数
        let mut window_cols: Vec<ArrayRef> = Vec::with_capacity(self.window_fns.len());
        for (fi, f) in self.window_fns.iter().enumerate() {
            let col = compute_window_fn(
                f,
                fi,
                &self.window_fns,
                &part_arrs,
                &sorted_order_arrs,
                &sorted_batch,
                n,
            )?;
            window_cols.push(col);
        }

        // 组装输出：sorted input cols ++ window cols
        let mut out_cols = Vec::with_capacity(self.schema.fields().len());
        for c in 0..sorted_batch.num_columns() {
            out_cols.push(Arc::clone(sorted_batch.column(c)));
        }
        out_cols.extend(window_cols);
        RecordBatch::try_new_with_options(
            Arc::clone(&self.schema),
            out_cols,
            &RecordBatchOptions::new().with_row_count(Some(n)),
        )
        .map_err(|e| QsdbError::invalid(format!("window output: {e}")))
    }

    fn assemble_empty(&self) -> Result<RecordBatch> {
        Ok(RecordBatch::new_empty(Arc::clone(&self.schema)))
    }
}

/// 计算单个窗口函数列。
fn compute_window_fn(
    f: &WindowFn,
    _fn_idx: usize,
    _all_fns: &[WindowFn],
    part_arrs: &[ArrayRef],
    order_arrs: &[ArrayRef],
    sorted_batch: &RecordBatch,
    n: usize,
) -> Result<ArrayRef> {
    // 分区边界：任一分区键与前一行不同 → 新分区开始。
    // 无 PARTITION BY → part_arrs 为空 → 全表一个分区（仅首行为边界）。
    let mut partition_start = vec![false; n];
    partition_start[0] = true;
    for (i, start) in partition_start.iter_mut().enumerate().skip(1) {
        *start = part_arrs
            .iter()
            .any(|arr| !array_row_eq(arr, i - 1, arr, i));
    }
    // ORDER BY 值是否与前一行不同（用于 RANK/DENSE_RANK）
    let order_changed = |i: usize| -> bool {
        if i == 0 {
            return true;
        }
        for arr in order_arrs {
            if !array_row_eq(arr, i - 1, arr, i) {
                return true;
            }
        }
        false
    };
    // 聚合默认帧（RANGE）的取值段起点：分区边界 ∪ ORDER BY 值变化。
    // 无 ORDER BY 时 order_changed 恒 false → 段 = 整个分区。
    let mut seg_start = partition_start.clone();
    for (i, start) in seg_start.iter_mut().enumerate().skip(1) {
        if !*start && order_changed(i) {
            *start = true;
        }
    }

    match f {
        WindowFn::RowNumber => {
            let mut rn = vec![0i64; n];
            let mut cnt = 0i64;
            for i in 0..n {
                if partition_start[i] {
                    cnt = 0;
                }
                cnt += 1;
                rn[i] = cnt;
            }
            Ok(Arc::new(Int64Array::from(rn)))
        }
        WindowFn::Rank => {
            let mut rk = vec![0i64; n];
            let mut cur_rank = 0i64;
            for i in 0..n {
                if partition_start[i] {
                    cur_rank = 1;
                } else if order_changed(i) {
                    cur_rank = i as i64 + 1;
                }
                rk[i] = cur_rank;
            }
            Ok(Arc::new(Int64Array::from(rk)))
        }
        WindowFn::DenseRank => {
            let mut dr = vec![0i64; n];
            let mut cur = 0i64;
            for i in 0..n {
                if partition_start[i] {
                    cur = 1;
                } else if order_changed(i) {
                    cur += 1;
                }
                dr[i] = cur;
            }
            Ok(Arc::new(Int64Array::from(dr)))
        }
        WindowFn::Sum(arg) | WindowFn::Avg(arg) | WindowFn::Min(arg) | WindowFn::Max(arg) => {
            let arg_arr = eval::eval(arg, sorted_batch)?;
            let is_avg = matches!(f, WindowFn::Avg(_));
            let is_min = matches!(f, WindowFn::Min(_));
            let is_max = matches!(f, WindowFn::Max(_));
            let fname = match f {
                WindowFn::Sum(_) => "sum",
                WindowFn::Avg(_) => "avg",
                WindowFn::Min(_) => "min",
                _ => "max",
            };
            match arg.data_type() {
                t if t.is_integer() => {
                    // 整型路径：i64 运行值（sum/min/max → Int64，avg → Float64）
                    let mut acc = 0i64; // sum 累加
                    let mut ext: Option<i64> = None; // min/max 当前值
                    let mut cnt = 0i64; // 非 null 计数（avg 除数）
                    let mut out_i = vec![0i64; n];
                    let mut out_f = vec![0f64; n];
                    for i in 0..n {
                        if partition_start[i] {
                            acc = 0;
                            ext = None;
                            cnt = 0;
                        }
                        if let Some(v) = arr_i64(&arg_arr, i) {
                            acc += v;
                            cnt += 1;
                            ext = Some(match ext {
                                None => v,
                                Some(e) if is_min => e.min(v),
                                Some(e) if is_max => e.max(v),
                                Some(e) => e,
                            });
                        }
                        if is_avg {
                            out_f[i] = acc as f64 / cnt as f64;
                        } else if is_min || is_max {
                            out_i[i] = ext.unwrap_or(0);
                        } else {
                            out_i[i] = acc;
                        }
                    }
                    if is_avg {
                        backfill_segments(&mut out_f, &seg_start);
                        Ok(Arc::new(Float64Array::from(out_f)))
                    } else {
                        backfill_segments(&mut out_i, &seg_start);
                        Ok(Arc::new(Int64Array::from(out_i)))
                    }
                }
                t if t.is_floating() => {
                    // 浮点路径：f64 运行值
                    let mut acc = 0f64;
                    let mut ext: Option<f64> = None;
                    let mut cnt = 0i64;
                    let mut out = vec![0f64; n];
                    for i in 0..n {
                        if partition_start[i] {
                            acc = 0.0;
                            ext = None;
                            cnt = 0;
                        }
                        if let Some(v) = arr_f64(&arg_arr, i) {
                            acc += v;
                            cnt += 1;
                            ext = Some(match ext {
                                None => v,
                                Some(e) if is_min => e.min(v),
                                Some(e) if is_max => e.max(v),
                                Some(e) => e,
                            });
                        }
                        out[i] = if is_min || is_max {
                            ext.unwrap_or(0.0)
                        } else if is_avg {
                            acc / cnt as f64
                        } else {
                            acc
                        };
                    }
                    backfill_segments(&mut out, &seg_start);
                    Ok(Arc::new(Float64Array::from(out)))
                }
                other => Err(QsdbError::unsupported(format!(
                    "window {fname} over {other:?} (support: integer/float args)"
                ))),
            }
        }
        WindowFn::Count(arg) => {
            let arg_arr = eval::eval(arg, sorted_batch)?;
            let mut out = vec![0i64; n];
            let mut cnt = 0i64;
            for i in 0..n {
                if partition_start[i] {
                    cnt = 0;
                }
                if !arg_arr.is_null(i) {
                    cnt += 1;
                }
                out[i] = cnt;
            }
            backfill_segments(&mut out, &seg_start);
            Ok(Arc::new(Int64Array::from(out)))
        }
        WindowFn::Lag(arg, offset, default) | WindowFn::Lead(arg, offset, default) => {
            let lag = matches!(f, WindowFn::Lag(..));
            let arg_arr = broadcast_const(eval::eval(arg, sorted_batch)?, n)?;
            // 分区行区间（行已按分区+序排序）：每行所属分区的首/末行号
            let mut part_first = vec![0usize; n];
            let mut part_last = vec![0usize; n];
            let mut seg = 0usize;
            for (i, is_start) in partition_start
                .iter()
                .enumerate()
                .chain(std::iter::once((n, &true)))
            {
                if *is_start {
                    for r in seg..i {
                        part_first[r] = seg;
                        part_last[r] = i - 1;
                    }
                    seg = i;
                }
            }
            // 目标行号：越出分区 → 无（NULL / default）
            let off = *offset as usize;
            let mut idx_b = UInt32Builder::with_capacity(n);
            for i in 0..n {
                let j = if lag {
                    i.checked_sub(off)
                } else {
                    i.checked_add(off)
                };
                match j {
                    Some(j) if j >= part_first[i] && j <= part_last[i] => {
                        idx_b.append_value(j as u32);
                    }
                    _ => idx_b.append_null(),
                }
            }
            let idx = idx_b.finish();
            let taken = take(&arg_arr, &idx, None)
                .map_err(|e| QsdbError::invalid(format!("window lag/lead take: {e}")))?;
            match default {
                None => Ok(taken),
                Some(d) => {
                    // 越界行取 default（索引为 null 的位置）：0=taken，1=default
                    let d_arr = broadcast_const(eval::eval(d, sorted_batch)?, n)?;
                    let pick: Vec<(usize, usize)> = (0..n)
                        .map(|i| if idx.is_null(i) { (1, i) } else { (0, i) })
                        .collect();
                    interleave(&[taken.as_ref(), d_arr.as_ref()], &pick)
                        .map_err(|e| QsdbError::invalid(format!("window lag/lead default: {e}")))
                }
            }
        }
    }
}

/// RANGE 默认帧段回填：同一取值段内所有行取段尾运行值。
/// （无 ORDER BY → 段 = 整分区 → 分区总值；有 ORDER BY → 并列行共享组尾值）
fn backfill_segments<T: Copy>(out: &mut [T], seg_start: &[bool]) {
    let n = out.len();
    let mut i = 0;
    while i < n {
        let mut j = i + 1;
        while j < n && !seg_start[j] {
            j += 1;
        }
        let last = out[j - 1];
        out[i..j].fill(last);
        i = j;
    }
}

/// 常量（长度 1）数组广播到 n 行（lag/lead 的字面量参数/default）。
fn broadcast_const(arr: ArrayRef, n: usize) -> Result<ArrayRef> {
    if arr.len() == n || n == 0 {
        return Ok(arr);
    }
    let pick: Vec<(usize, usize)> = vec![(0, 0); n];
    interleave(&[arr.as_ref()], &pick)
        .map_err(|e| QsdbError::invalid(format!("window const broadcast: {e}")))
}

/// 两行在同一数组上是否相等（null == null 为 true 用于分区边界）。
fn array_row_eq(a: &dyn Array, i: usize, b: &dyn Array, j: usize) -> bool {
    use arrow::datatypes::*;
    if a.is_null(i) && b.is_null(j) {
        return true;
    }
    if a.is_null(i) || b.is_null(j) {
        return false;
    }
    match a.data_type() {
        DataType::Int8 => {
            a.as_any()
                .downcast_ref::<arrow::array::Int8Array>()
                .unwrap()
                .value(i)
                == b.as_any()
                    .downcast_ref::<arrow::array::Int8Array>()
                    .unwrap()
                    .value(j)
        }
        DataType::Int16 => {
            a.as_any()
                .downcast_ref::<arrow::array::Int16Array>()
                .unwrap()
                .value(i)
                == b.as_any()
                    .downcast_ref::<arrow::array::Int16Array>()
                    .unwrap()
                    .value(j)
        }
        DataType::Int32 => {
            a.as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .unwrap()
                .value(i)
                == b.as_any()
                    .downcast_ref::<arrow::array::Int32Array>()
                    .unwrap()
                    .value(j)
        }
        DataType::Date32 => {
            a.as_any()
                .downcast_ref::<arrow::array::Date32Array>()
                .unwrap()
                .value(i)
                == b.as_any()
                    .downcast_ref::<arrow::array::Date32Array>()
                    .unwrap()
                    .value(j)
        }
        DataType::Int64 | DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            a.as_any().downcast_ref::<Int64Array>().unwrap().value(i)
                == b.as_any().downcast_ref::<Int64Array>().unwrap().value(j)
        }
        DataType::Float64 => {
            a.as_any().downcast_ref::<Float64Array>().unwrap().value(i)
                == b.as_any().downcast_ref::<Float64Array>().unwrap().value(j)
        }
        DataType::Utf8 => {
            a.as_any().downcast_ref::<StringArray>().unwrap().value(i)
                == b.as_any().downcast_ref::<StringArray>().unwrap().value(j)
        }
        DataType::Boolean => {
            a.as_any().downcast_ref::<BooleanArray>().unwrap().value(i)
                == b.as_any().downcast_ref::<BooleanArray>().unwrap().value(j)
        }
        _ => false,
    }
}

/// 数组第 i 行 → Option<i64>（null → None）。
fn arr_i64(a: &dyn Array, i: usize) -> Option<i64> {
    use arrow::datatypes::*;
    if a.is_null(i) {
        return None;
    }
    Some(match a.data_type() {
        DataType::Int8 => i64::from(
            a.as_any()
                .downcast_ref::<arrow::array::Int8Array>()
                .unwrap()
                .value(i),
        ),
        DataType::Int16 => i64::from(
            a.as_any()
                .downcast_ref::<arrow::array::Int16Array>()
                .unwrap()
                .value(i),
        ),
        DataType::Int32 => i64::from(
            a.as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .unwrap()
                .value(i),
        ),
        DataType::Int64 => a.as_any().downcast_ref::<Int64Array>().unwrap().value(i),
        DataType::Float64 => a.as_any().downcast_ref::<Float64Array>().unwrap().value(i) as i64,
        _ => 0,
    })
}

/// 数组第 i 行 → Option<f64>。
fn arr_f64(a: &dyn Array, i: usize) -> Option<f64> {
    use arrow::datatypes::*;
    if a.is_null(i) {
        return None;
    }
    Some(match a.data_type() {
        DataType::Float64 => a.as_any().downcast_ref::<Float64Array>().unwrap().value(i),
        DataType::Float32 => f64::from(
            a.as_any()
                .downcast_ref::<arrow::array::Float32Array>()
                .unwrap()
                .value(i),
        ),
        DataType::Int64 => a.as_any().downcast_ref::<Int64Array>().unwrap().value(i) as f64,
        DataType::Int32 => f64::from(
            a.as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .unwrap()
                .value(i),
        ),
        _ => 0.0,
    })
}

impl ExecNode for WindowExec {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn next(&mut self) -> Option<Result<RecordBatch>> {
        if self.output.is_some() {
            return None;
        }
        let batch = match self.execute_all() {
            Ok(b) => b,
            Err(e) => return Some(Err(e)),
        };
        self.output = Some(RecordBatch::new_empty(Arc::clone(&self.schema)));
        if batch.num_rows() == 0 {
            return None;
        }
        Some(Ok(batch))
    }
}
