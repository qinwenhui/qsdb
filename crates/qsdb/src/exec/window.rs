//! 窗口函数算子：物化输入 → (PARTITION BY, ORDER BY) 排序 → 逐分区计算 → 附加列输出。
//!
//! 支持的窗口函数：
//! - 排名类：ROW_NUMBER / RANK / DENSE_RANK
//! - 聚合类（默认帧 = SQL 标准 RANGE 语义）：SUM / COUNT / MIN / MAX / AVG
//!   - 无 ORDER BY → 帧为整个分区，每行取分区总值
//!   - 有 ORDER BY → 无界前驱到当前行（含并列 peer），并列行共享段尾值
//! - **显式行帧** `ROWS BETWEEN a AND b`（[`RowFrame`]）：按**行数**分帧，
//!   与默认帧是**两条路径**（默认帧 = `None` ⇒ 原快路径一字不改）。
//!   语义按 SQL 标准 / PG / DuckDB：帧 = `{j | i-a ≤ j ≤ i+b}` ∩ 本分区，
//!   **帧可能为空**（边界落在当前行之外时不回夹到分区边）⇒ 除 `count` 外
//!   空帧一律 NULL（`count` 空帧 = 0）。
//! - 偏移类：LAG / LEAD(arg, offset[, default])——offset 为正整数常量，越出分区取 NULL/default
//!
//! 输出列序：输入列 ++ 窗口列（按 SELECT 中出现顺序）。
//! 输出行序：(PARTITION BY, ORDER BY) 排序后（窗口函数语义允许）。
//!
//! **溢出（P3）**：物化输入要不到额度时，按 `PARTITION BY` 键分区落盘
//! （`enter_spill`）→ 逐分区取回 → 分区内排序 + 窗口计算 → emit 一个批。
//!
//! 为什么按分区键路由就够：窗口函数**只在同一个 `PARTITION BY` 分区内**计算
//! （`compute_window_fn` 里的 `partition_start` 判边界；无 `PARTITION BY` 时
//! 全表就是一个分区），故按分区键路由后每个分区可**独立**算——分区内的排序与
//! 窗口逻辑与内存路径**逐字相同**（都走 `run_window` 那段代码，溢出只改
//! "什么时候读"，不改"怎么算"）。路由口径 = `agg_spill::route_by_lits`
//! （`Literal` 规范形态）：窗口这边两侧都是原始行批（没有聚合那样的"已聚合表"），
//! 故全程用同一个函数即天然一致（同一逻辑键必然算出同一个分区）。
//!
//! **明确不覆盖**：`PARTITION BY` 为空的查询（`OVER ()` / `OVER (ORDER BY ...)`）
//! ——整个输入就是一个分区，**无键可分区**，故保持现状：要不到额度如实报错
//! （`QUERY_MEMORY_LIMIT` / `MEMORY_PRESSURE`），不硬做。
//! 这是设计边界，见 `docs/内存预算与溢出设计.md` §四 4.2。
//!
//! **DECIMAL 支持（2026-10-16）**：`sum/avg/min/max` 均可用于 `DECIMAL` 参数
//! （电量/电费/电价这类核心列几乎全是它）。精度口径与非窗口聚合**逐字一致**：
//! `sum` ⇒ `min(p+10,38), s`；`avg` ⇒ `min(p+10,38), max(s,4)`（HALF_UP）；
//! `min/max` ⇒ 参数类型。**累加用 arrow 自带的 `i256`**（零新依赖），
//! 装不回声明精度时**显式报错，不回 NULL**。
//!
//! 取值口径（两条路径**同一套**）：帧内**一个非 NULL 都没有**（含空帧）⇒
//! SUM/MIN/MAX/AVG 输出 **NULL**（此前输出 0 / NaN —— 那是错值，2026-10-15 修）；
//! COUNT 输出 **0**（`count` 数的是行，空帧就是 0）。默认帧与显式帧因此
//! **逐值一致**：`sum(x) OVER (ORDER BY i)` 与 `… ROWS BETWEEN UNBOUNDED
//! PRECEDING AND CURRENT ROW` 在无并列时结果相同。

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Decimal128Array, Float64Array, Int64Array, StringArray,
    UInt32Array, UInt32Builder,
};
use arrow::compute::kernels::sort::{SortColumn, lexsort_to_indices};
use arrow::compute::{concat, interleave, take};
use arrow::datatypes::{DataType, SchemaRef, i256};
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
    pub order_by: Vec<crate::exec::SortKey>,
}

impl WindowSpec {
    /// 绑定签名（表达式 display 串）：分组共享节点的判据。
    pub fn signature(&self) -> String {
        let p: Vec<String> = self.partition_by.iter().map(|e| e.display()).collect();
        let o: Vec<String> = self
            .order_by
            .iter()
            .map(|k| {
                format!(
                    "{}:{}:{}",
                    k.expr.display(),
                    k.descending,
                    k.nulls_first.map_or('d', |b| if b { 'f' } else { 'l' })
                )
            })
            .collect();
        format!("{}|{}", p.join(","), o.join(","))
    }
}

/// 行帧的一段边界（`ROWS BETWEEN a AND b` 里的 a 或 b）。
///
/// `Unbounded` 的**方向由位置决定**：作起点是 `UNBOUNDED PRECEDING`、作终点是
/// `UNBOUNDED FOLLOWING`。非法组合（起点 = `UNBOUNDED FOLLOWING`、终点 =
/// `UNBOUNDED PRECEDING`、起点在终点之后）在**绑定期**就被拒，见
/// `translate::parse_window_fn_frame` —— 这里不再重复校验。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameBound {
    /// 起点 `UNBOUNDED PRECEDING` / 终点 `UNBOUNDED FOLLOWING`
    Unbounded,
    /// `n PRECEDING`（n ≥ 0）
    Preceding(u64),
    /// `CURRENT ROW`
    Current,
    /// `n FOLLOWING`（n ≥ 0）
    Following(u64),
}

/// 显式行帧 `ROWS BETWEEN start AND end`：**按行数**分帧（不看值）。
///
/// 只表示"用户显式写了帧"。**默认帧不走这里**（`None` = 默认 ⇒ 原快路径），
/// 因为默认帧是 RANGE 语义（按**值**分段、并列行共享段尾值），与行帧是两回事。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowFrame {
    pub start: FrameBound,
    pub end: FrameBound,
    /// **RANGE 语义标志**（十九批 S3）：起点 `CURRENT ROW` 指**并列组**起点
    /// （按值分帧，键相等的一组整组进帧），不是当前行。绑定层只把
    /// `RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING` 映射成
    /// `range_peer_start = true`；另两个 RANGE 形态在绑定层就改写掉了
    /// （`UP..CR` ≡ 默认帧 → `None`；`UP..UF` ≡ 整分区 → 普通行帧）。
    /// 空帧语义（帧内无值 ⇒ NULL/0）与 ROWS 路径同一套。
    pub range_peer_start: bool,
}

/// 窗口函数定义（binder 产出）。
///
/// 聚合变体带 `Option<RowFrame>`：`Some` = 用户显式写了 `ROWS BETWEEN`（走滑窗
/// 路径），`None` = 默认帧（走原来的运行累加器）。排名/偏移类**不带帧**
/// —— 按标准它们忽略帧，而本仓纪律是"不静默忽略"，故绑定期直接拒（见
/// `translate::audit_window_spec`）。
#[derive(Debug, Clone)]
pub enum WindowFn {
    RowNumber,
    Rank,
    DenseRank,
    Sum(PhysExpr, Option<RowFrame>),
    /// `count(x)`（`Some`）或 `count(*)`（`None`，数**行**）—— 与聚合那边同一套
    /// 表示；两者在含 NULL 的列上结果不同，所以必须分开。
    Count(Option<PhysExpr>, Option<RowFrame>),
    Min(PhysExpr, Option<RowFrame>),
    Max(PhysExpr, Option<RowFrame>),
    Avg(PhysExpr, Option<RowFrame>),
    /// lag(arg, offset[, default])：分区内在排序序上取前 offset 行的值
    Lag(PhysExpr, i64, Option<Box<PhysExpr>>),
    /// lead(arg, offset[, default])：取后 offset 行的值
    Lead(PhysExpr, i64, Option<Box<PhysExpr>>),
}

impl WindowFn {
    /// 显式行帧：`Some` ⇒ 走滑窗路径；`None` ⇒ 默认帧（原快路径）。
    pub fn frame(&self) -> Option<RowFrame> {
        match self {
            Self::Sum(_, fr) | Self::Min(_, fr) | Self::Max(_, fr) | Self::Avg(_, fr) => *fr,
            Self::Count(.., fr) => *fr,
            _ => None,
        }
    }

    pub fn output_name(&self) -> String {
        match self {
            Self::RowNumber => "row_number()".into(),
            Self::Rank => "rank()".into(),
            Self::DenseRank => "dense_rank()".into(),
            Self::Sum(e, _) => format!("sum({})", e.display()),
            Self::Count(None, _) => "count(*)".into(),
            Self::Count(Some(e), _) => format!("count({})", e.display()),
            Self::Min(e, _) => format!("min({})", e.display()),
            Self::Max(e, _) => format!("max({})", e.display()),
            Self::Avg(e, _) => format!("avg({})", e.display()),
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
            Self::RowNumber | Self::Rank | Self::DenseRank | Self::Count(..) => LogicalType::Int64,
            // ⚠️ **`Decimal` 必须排在 `t if t.is_numeric()` 前面**：
            //   `Decimal::is_numeric()` 为真 ⇒ 顺序反了这条臂就是**死代码**，
            //   `sum(d) OVER ()` 的输出类型会变成 **BIGINT**（值被当整数显示）。
            //   实测（2026-10-16）：这条一直是错的，只是此前 `sum(decimal) OVER`
            //   直接报"不支持"、**从没走到取类型那一步**，所以从没暴露。
            //   非窗口聚合那边用的是严格 `match input`（Decimal 显式一条），没这个坑。
            Self::Sum(e, _) => match e.data_type() {
                LogicalType::Decimal(m) => {
                    LogicalType::Decimal(crate::types::DecimalMeta::sum_result(m))
                }
                LogicalType::Float32 | LogicalType::Float64 => LogicalType::Float64,
                t if t.is_numeric() => LogicalType::Int64,
                _ => LogicalType::Int64,
            },
            // ⚠️ **与非窗口聚合对齐**：那边 `avg(DECIMAL)` 走 HALF_UP 精确路径、
            // 输出 `Decimal`；窗口此前一律 Float64 ⇒ **同一个函数两条路给出不同类型**，
            // 而窗口那条对电量/电费这种列是**有损**的（Float64 只有 ~15 位有效数字，
            // DECIMAL(30,8) 有 30 位）。金融/结算场景这条不能含糊。
            Self::Avg(e, _) => match e.data_type() {
                LogicalType::Decimal(m) => {
                    LogicalType::Decimal(crate::types::DecimalMeta::avg_result(m))
                }
                _ => LogicalType::Float64,
            },
            Self::Min(e, _) | Self::Max(e, _) => e.data_type(),
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
    order_by: Vec<crate::exec::SortKey>,
    /// 窗口函数（与输出列一一对应）
    window_fns: Vec<WindowFn>,
    schema: SchemaRef,
    ctx: Arc<QueryContext>,
    /// `Some` = 已产出（内存路径一批；溢出路径逐分区到最后一批）
    output: Option<RecordBatch>,
    // ── 溢出态（物化输入要不到额度时启用）：按 PARTITION BY 键分区落盘 → 逐分区产出 ──
    /// 输入行按 `PARTITION BY` 键分区落盘（内存 → 缓冲，超界 → 临时 parquet）。
    /// **缓冲区为 `None` 直到真正转入溢出**（内存路径不付任何代价）。
    spill: Option<crate::exec::agg_spill::PartSpill>,
    /// 溢出模式下正在产出的分区游标
    spill_part: usize,
}

impl WindowExec {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        input: Box<dyn ExecNode>,
        partition_by: Vec<PhysExpr>,
        order_by: Vec<crate::exec::SortKey>,
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
            spill: None,
            spill_part: 0,
        }
    }

    /// 转入**分区溢出**：已物化的批 + 剩余输入全部按 `PARTITION BY` 键路由落盘，
    /// 之后由 `spill_next` 逐分区取回、分区内计算。
    ///
    /// 调用前提（`execute_all` 保证）：`PARTITION BY` 非空——否则无键可分区。
    ///
    /// 内存：缓冲与分区表走 `PartSpill` 的 `reserve_spill`（**只受全局预算约束**，
    /// 不吃每查询配额）——溢出是配额不够时的逃生通道，若逃生通道自己也吃配额，
    /// 小配额下溢出永远启动不了（P1/P2 同一个坑，实测复现）。硬上界是全局预算。
    fn enter_spill(&mut self, collected: Vec<RecordBatch>) -> Result<()> {
        let schema = self.input.schema();
        let ctx = Arc::clone(&self.ctx);
        let (nparts, cap) = crate::exec::agg_spill::PartSpill::plan(&ctx);
        let mut sp = crate::exec::agg_spill::PartSpill::new(schema, &ctx, nparts, cap)?;
        let n = sp.nparts();
        // ① 已物化的批：**含触发失败的那一批**（坑见 `execute_all`）
        for b in collected {
            let parts = crate::exec::agg_spill::route_by_lits(&b, &self.partition_by, n)?;
            sp.push_routed(&b, &parts)?;
        }
        // ② 剩余输入
        while let Some(b) = self.input.next() {
            self.ctx.check()?;
            let b = b?;
            let parts = crate::exec::agg_spill::route_by_lits(&b, &self.partition_by, n)?;
            sp.push_routed(&b, &parts)?;
        }
        self.spill = Some(sp);
        self.spill_part = 0;
        Ok(())
    }

    /// 物化输入 → [`Self::run_window`]。
    ///
    /// **要不到额度 → 转分区溢出**（`enter_spill`）：返回空批，由 `spill_next`
    /// 逐分区产出（调用方据 `self.spill.is_some()` 分辨）。
    fn execute_all(&mut self) -> Result<RecordBatch> {
        // 物化输入：预留随函数内数据存活（RAII），函数返回即归还
        let mut res = self.ctx.try_reserve(0)?;
        let mut batches = Vec::new();
        while let Some(b) = self.input.next() {
            self.ctx.check()?;
            let x = b?;
            if let Err(e) = res.grow(crate::memory::batch_bytes(&x)) {
                // **要不到额度 → 按 PARTITION BY 键分区落盘**（而不是把查询打死）。
                if self.partition_by.is_empty() {
                    // 边界：无 PARTITION BY → 整个输入就是一个分区，**无键可分区**
                    // → 保持现状（如实报错：QUERY_MEMORY_LIMIT / MEMORY_PRESSURE）。
                    return Err(e);
                }
                // 注意：**本批也要进分区**——它就是压垮内存的那一批，只交已收集的
                // `batches` 会静默少一批的行（P2 踩过：INNER 少 2000 行正好一个批）。
                batches.push(x);
                // 让出内存路径的账：溢出缓冲的额度来自**全局预算**，两份同时挂着
                // 会把预算占满、把逃生通道自己掐死。已收集的批随后会被 take 出
                // 副本进分区缓冲（那一份由溢出预留自己记）。
                drop(res);
                self.enter_spill(batches)?;
                return Ok(RecordBatch::new_empty(Arc::clone(&self.schema)));
            }
            batches.push(x);
        }

        if batches.is_empty() {
            return Ok(RecordBatch::new_empty(Arc::clone(&self.schema)));
        }
        // concat 期间瞬时双份（峰值真实存在，就该记账）
        if let Err(e) = res.grow(res.bytes()) {
            if self.partition_by.is_empty() {
                return Err(e); // 同上：无键可分区，保持现状
            }
            drop(res);
            self.enter_spill(batches)?;
            return Ok(RecordBatch::new_empty(Arc::clone(&self.schema)));
        }
        self.run_window(&batches)
    }

    /// 排序 + 窗口计算 + 组装输出——**内存路径与溢出路径共用的那一段**。
    ///
    /// `batches` = 要算的全部输入行：内存路径是**全部输入**；溢出路径是
    /// **一个 `PARTITION BY` 分区的全部行**。窗口函数只在同一分区内计算，
    /// 故分区可独立算，两条路径的排序与窗口逻辑因此逐字相同。
    fn run_window(&self, batches: &[RecordBatch]) -> Result<RecordBatch> {
        if batches.is_empty() {
            return self.assemble_empty();
        }
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
        for k in &self.order_by {
            let arr = eval::eval(&k.expr, &input_batch)?;
            order_arrs.push(Arc::clone(&arr));
            sort_cols.push(SortColumn {
                values: arr,
                options: Some(arrow::compute::SortOptions {
                    descending: k.descending,
                    // 窗口内 ORDER BY 的 NULL 放置与外部 ORDER BY 同一套规则
                    nulls_first: k.nulls_first_or_default(),
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

    /// 溢出态：逐分区取回 → 分区内 [`Self::run_window`] → 产出一个批。
    ///
    /// 记账随分区走：分区的缓冲账在取回时归还（`PartSpill::take_partition` 会把
    /// 上一分区的行账一并归还），本分区 concat 的瞬时双份由 `part_res` 记，
    /// 出作用域即还；整个溢出器在最后一个分区后 `Drop`（溢出目录一并删除）。
    fn spill_next(&mut self) -> Option<Result<RecordBatch>> {
        loop {
            let nparts = self.spill.as_ref()?.nparts();
            if self.spill_part >= nparts {
                self.spill = None; // 落盘的 run 与缓冲的账随 Drop 释放
                self.output = Some(RecordBatch::new_empty(Arc::clone(&self.schema)));
                return None;
            }
            let pi = self.spill_part;
            self.spill_part += 1;
            let rows = match self.spill.as_mut().unwrap().take_partition(pi) {
                Ok(r) => r,
                Err(e) => return Some(Err(e)),
            };
            if rows.is_empty() {
                continue; // 该分区没有行 → 无输出
            }
            // 本分区 concat 的瞬时双份记账。走 `reserve_spill`（**只受全局预算
            // 约束**）：溢出路径的记账不吃每查询配额——否则小配额下逃生通道
            // 自己先被配额掐死（P1/P2 同一个坑，实测复现）。
            let bytes: u64 = rows.iter().map(crate::memory::batch_bytes).sum();
            let mut part_res = match self.ctx.reserve_spill(0) {
                Ok(r) => r,
                Err(e) => return Some(Err(e)),
            };
            if let Err(e) = part_res.grow(bytes) {
                return Some(Err(e));
            }
            let out = match self.run_window(&rows) {
                Ok(b) => b,
                Err(e) => return Some(Err(e)),
            };
            drop(part_res); // 本分区的记账到此为止
            if out.num_rows() == 0 {
                continue;
            }
            return Some(Ok(out));
        }
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
            // 名次的分母是**分区内位置**，不是全局行号：`rank()` 定义 = 分区内
            // 严格排在其前面的行数 + 1，并列者同名次且跳号。故必须自己数
            // 分区内位置 `pos`——早先用全局 `i + 1` 会让**第二个分区起跳号**
            // （6 行两分区 → `1,2,3,1,5,6`）。
            let mut rk = vec![0i64; n];
            let mut cur_rank = 0i64;
            let mut pos = 0i64;
            for i in 0..n {
                if partition_start[i] {
                    pos = 0;
                }
                pos += 1;
                if pos == 1 || order_changed(i) {
                    // 分区首行 / 新并列组首行 → 名次 = 该行在分区内的位置
                    cur_rank = pos;
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
        WindowFn::Sum(arg, fr)
        | WindowFn::Avg(arg, fr)
        | WindowFn::Min(arg, fr)
        | WindowFn::Max(arg, fr) => {
            let arg_arr = eval::eval(arg, sorted_batch)?;
            let is_avg = matches!(f, WindowFn::Avg(..));
            let is_min = matches!(f, WindowFn::Min(..));
            let is_max = matches!(f, WindowFn::Max(..));
            let fname = match f {
                WindowFn::Sum(..) => "sum",
                WindowFn::Avg(..) => "avg",
                WindowFn::Min(..) => "min",
                _ => "max",
            };
            // **显式帧走另一条路**（滑窗）；默认帧（None）**不进这里**，
            // 下面原代码一字不改 —— 这是"零回归"的落点。
            if let Some(frame) = *fr {
                return framed_agg(
                    f,
                    &arg_arr,
                    arg.data_type(),
                    frame,
                    &partition_start,
                    Some(&seg_start),
                    n,
                );
            }
            let out = match arg.data_type() {
                t if t.is_integer() => {
                    // 整型路径：i64 运行值（sum/min/max → Int64，avg → Float64）
                    let mut acc = 0i64; // sum 累加
                    let mut ext: Option<i64> = None; // min/max 当前值
                    let mut cnt = 0i64; // 非 null 计数（avg 除数）
                    let mut out_i = vec![0i64; n];
                    let mut out_f = vec![0f64; n];
                    // 帧内一个非 NULL 都没有的行（只可能是分区开头的一段）⇒ 输出 NULL。
                    // **无 NULL 数据时这个 Vec 一次都不 push**（零额外代价）。
                    let mut empty_rows: Vec<u32> = Vec::new();
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
                        } else if cnt == 0 {
                            empty_rows.push(i as u32);
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
                        finish_float(out_f, &empty_rows, &seg_start)
                    } else {
                        finish_int(out_i, &empty_rows, &seg_start)
                    }
                }
                t if t.is_floating() => {
                    // 浮点路径：f64 运行值
                    let mut acc = 0f64;
                    let mut ext: Option<f64> = None;
                    let mut cnt = 0i64;
                    let mut out = vec![0f64; n];
                    let mut empty_rows: Vec<u32> = Vec::new();
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
                        } else if cnt == 0 {
                            empty_rows.push(i as u32);
                        }
                        out[i] = if is_min || is_max {
                            ext.unwrap_or(0.0)
                        } else if is_avg {
                            acc / cnt as f64
                        } else {
                            acc
                        };
                    }
                    finish_float(out, &empty_rows, &seg_start)
                }
                crate::types::LogicalType::Decimal(_) => {
                    // DECIMAL：sum/avg 走 i256 **精确**累加（与聚合那边同一套），
                    // min/max 取 i128 极值（不做算术，值本身就装得下）
                    let dec = arg_arr
                        .as_any()
                        .downcast_ref::<Decimal128Array>()
                        .ok_or_else(|| {
                            QsdbError::type_err("decimal window arg downcast failed (internal)")
                        })?;
                    let mut acc = i256::from_i128(0);
                    let mut ext: Option<i128> = None;
                    let mut cnt = 0i64;
                    let mut out_d = vec![0i128; n];
                    let mut empty_rows: Vec<u32> = Vec::new();
                    for i in 0..n {
                        if partition_start[i] {
                            acc = i256::from_i128(0);
                            ext = None;
                            cnt = 0;
                        }
                        if !dec.is_null(i) {
                            let v = dec.value(i);
                            acc += i256::from_i128(v);
                            cnt += 1;
                            ext = Some(match ext {
                                None => v,
                                Some(e) if is_min => e.min(v),
                                Some(e) if is_max => e.max(v),
                                Some(e) => e,
                            });
                        } else if cnt == 0 {
                            empty_rows.push(i as u32);
                        }
                        out_d[i] = if is_avg {
                            // cnt == 0 的行由 `empty_rows` 统一置 NULL（见 finish_dec）
                            if cnt == 0 {
                                0
                            } else {
                                dec_avg(
                                    acc,
                                    cnt,
                                    dec_meta(&arg.data_type())?.scale,
                                    out_meta(f, &arg.data_type())?.scale,
                                )?
                            }
                        } else if is_min || is_max {
                            ext.unwrap_or(0)
                        } else {
                            dec_fit(acc, "sum")?
                        };
                    }
                    finish_dec(
                        out_d,
                        &empty_rows,
                        &seg_start,
                        out_meta(f, &arg.data_type())?,
                    )
                }
                other => {
                    return Err(QsdbError::unsupported(format!(
                        "window {fname} over {other:?} (support: integer/float/decimal args)"
                    )));
                }
            };
            align_output(f, out)
        }
        WindowFn::Count(arg, fr) => {
            // `count(*)`（arg = None）逐行都计 ⇒ 没有"非 NULL"这回事。
            let arg_arr = match arg {
                Some(e) => Some(eval::eval(e, sorted_batch)?),
                None => None,
            };
            let counted = |i: usize| arg_arr.as_ref().is_none_or(|a| !a.is_null(i));
            if let Some(frame) = *fr {
                return Ok(framed_count(
                    arg_arr.as_deref(),
                    frame,
                    &partition_start,
                    Some(&seg_start),
                    n,
                ));
            }
            let mut out = vec![0i64; n];
            let mut cnt = 0i64;
            for i in 0..n {
                if partition_start[i] {
                    cnt = 0;
                }
                if counted(i) {
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

/// 聚合窗口函数名（报错文案用）。
fn agg_name(f: &WindowFn) -> &'static str {
    match f {
        WindowFn::Sum(..) => "sum",
        WindowFn::Count(..) => "count",
        WindowFn::Min(..) => "min",
        WindowFn::Max(..) => "max",
        _ => "avg",
    }
}

/// 默认帧的输出收尾（整型/浮点各一份：arrow 的 `From<Vec<_>>` 只对具体类型实现，
/// 泛型函数写不出来）：`empty_rows`（帧内一个非 NULL 都没有）→ NULL，然后段内回填。
///
/// **`empty_rows` 为空时走原快路径**（`Vec<T>` 直出、零额外分配）—— 没有 NULL
/// 的数据（含基准里的）一分钱都不多付；有 NULL 时才付一次 `Vec<Option<T>>`。
/// DECIMAL 版收敛（形状与 [`finish_int`] 一致：**只有真有空帧时才付 `Option` 的代价**）。
fn finish_dec(
    vals: Vec<i128>,
    empty_rows: &[u32],
    seg_start: &[bool],
    m: crate::types::DecimalMeta,
) -> ArrayRef {
    let dt = DataType::Decimal128(m.precision, m.scale);
    if empty_rows.is_empty() {
        let mut v = vals;
        backfill_segments(&mut v, seg_start);
        return Arc::new(Decimal128Array::from(v).with_data_type(dt));
    }
    let mut opts: Vec<Option<i128>> = vals.into_iter().map(Some).collect();
    for &i in empty_rows {
        opts[i as usize] = None;
    }
    backfill_segments(&mut opts, seg_start);
    Arc::new(Decimal128Array::from(opts).with_data_type(dt))
}

fn finish_int(vals: Vec<i64>, empty_rows: &[u32], seg_start: &[bool]) -> ArrayRef {
    if empty_rows.is_empty() {
        let mut v = vals;
        backfill_segments(&mut v, seg_start);
        return Arc::new(Int64Array::from(v));
    }
    let mut opts: Vec<Option<i64>> = vals.into_iter().map(Some).collect();
    for &i in empty_rows {
        opts[i as usize] = None;
    }
    // 段内回填照做：段（并列组）的尾部无值 ⇒ 整段 NULL
    backfill_segments(&mut opts, seg_start);
    Arc::new(Int64Array::from(opts))
}

/// 见 [`finish_int`]（浮点的一份）。
fn finish_float(vals: Vec<f64>, empty_rows: &[u32], seg_start: &[bool]) -> ArrayRef {
    if empty_rows.is_empty() {
        let mut v = vals;
        backfill_segments(&mut v, seg_start);
        return Arc::new(Float64Array::from(v));
    }
    let mut opts: Vec<Option<f64>> = vals.into_iter().map(Some).collect();
    for &i in empty_rows {
        opts[i as usize] = None;
    }
    backfill_segments(&mut opts, seg_start);
    Arc::new(Float64Array::from(opts))
}

/// 显式行帧的滑窗求值（`ROWS BETWEEN …`）。
///
/// **取值口径与默认帧同一套**（见模块头）：帧内一个非 NULL 都没有 ⇒ SUM/MIN/
/// MAX/AVG 为 NULL、COUNT 为 0。两条路径的差别**只在帧怎么划**：默认帧是 RANGE
/// （按值分段、并列行共享段尾值），这里是 ROWS（按行号，帧可能为空）。
fn framed_agg(
    f: &WindowFn,
    arg_arr: &ArrayRef,
    arg_type: crate::types::LogicalType,
    frame: RowFrame,
    partition_start: &[bool],
    seg_start: Option<&[bool]>,
    n: usize,
) -> Result<ArrayRef> {
    let fname = agg_name(f);
    let is_avg = matches!(f, WindowFn::Avg(..));
    let is_extreme = matches!(f, WindowFn::Min(..) | WindowFn::Max(..));
    let want_min = matches!(f, WindowFn::Min(..));
    let peer = peer_starts(n, seg_start);

    let arr: ArrayRef = match arg_type {
        t if t.is_integer() => {
            let vals: Vec<Option<i64>> = (0..n).map(|i| arr_i64(arg_arr, i)).collect();
            if is_extreme {
                Arc::new(Int64Array::from(framed_extreme(
                    &vals,
                    frame,
                    partition_start,
                    &peer,
                    n,
                    want_min,
                )))
            } else {
                let (sums, cnts) = framed_prefix(n, partition_start, frame, &peer, &vals);
                if is_avg {
                    Arc::new(Float64Array::from(avg_of(&sums, &cnts, |v| v as f64)))
                } else {
                    Arc::new(Int64Array::from(sums))
                }
            }
        }
        t if t.is_floating() => {
            let vals: Vec<Option<f64>> = (0..n).map(|i| arr_f64(arg_arr, i)).collect();
            if is_extreme {
                Arc::new(Float64Array::from(framed_extreme(
                    &vals,
                    frame,
                    partition_start,
                    &peer,
                    n,
                    want_min,
                )))
            } else {
                let (sums, cnts) = framed_prefix(n, partition_start, frame, &peer, &vals);
                if is_avg {
                    Arc::new(Float64Array::from(avg_of(&sums, &cnts, |v| v)))
                } else {
                    Arc::new(Float64Array::from(sums))
                }
            }
        }
        crate::types::LogicalType::Decimal(_) => {
            let Some(vals) = dec_vals(arg_arr, n) else {
                return Err(QsdbError::type_err("decimal window arg downcast failed"));
            };
            if is_extreme {
                let out = framed_extreme(&vals, frame, partition_start, &peer, n, want_min);
                Arc::new(Decimal128Array::from(out).with_data_type({
                    let m = dec_meta(&arg_type)?;
                    DataType::Decimal128(m.precision, m.scale)
                }))
            } else {
                let big: Vec<Option<i256>> = vals.iter().map(|v| v.map(i256::from_i128)).collect();
                let (sums, cnts) = framed_prefix(n, partition_start, frame, &peer, &big);
                let in_scale = dec_meta(&arg_type)?.scale;
                let out_sc = out_meta(f, &arg_type)?.scale;
                let out: Vec<Option<i128>> = if is_avg {
                    sums.iter()
                        .zip(cnts.iter())
                        .map(|(s, c)| match s {
                            Some(v) if *c > 0 => dec_avg(*v, *c, in_scale, out_sc).map(Some),
                            _ => Ok(None),
                        })
                        .collect::<Result<_>>()?
                } else {
                    sums.iter()
                        .map(|s| match s {
                            Some(v) => dec_fit(*v, "sum").map(Some),
                            None => Ok(None),
                        })
                        .collect::<Result<_>>()?
                };
                let m = out_meta(f, &arg_type)?;
                Arc::new(
                    Decimal128Array::from(out)
                        .with_data_type(DataType::Decimal128(m.precision, m.scale)),
                )
            }
        }
        other => {
            return Err(QsdbError::unsupported(format!(
                "window {fname} over {other:?} (support: integer/float/decimal args)"
            )));
        }
    };
    align_output(f, arr)
}

// ---------------------------------------------------------------------------
// DECIMAL 的窗口聚合（2026-10-16）
//
// 起因：`sum(d) OVER (…)` 此前报 `support: integer/float args` —— 而**电量/电费/
// 电价这类核心列几乎全是 `DECIMAL(30,8)`**，窗口累计是日清最常用的形状之一。
//
// 三条精度口径**逐字对齐非窗口聚合**（不另立一套，否则同一个函数两条路给不同值）：
// - `sum`  → `i256` 精确累加，输出 `Decimal(sum_result(m))` ＝ `min(p+10, 38), s`
// - `avg`  → `HALF_UP(sum / count)`，输出 `Decimal(avg_result(m))` ＝ `min(p+10,38), max(s,4)`
// - `min/max` → 输出**参数类型**（值本身就是 i128，不做算术）
// ---------------------------------------------------------------------------

/// 把 `i256` 累加值装回 `i128`。**装不下就报错，不许静默变 NULL。**
///
/// 输出类型声明的是 `min(p+10, 38)`，而 `i256` 能装 76 位 —— 两者都可能不够。
/// 非窗口聚合那边此前是 `.to_i128()` 直接 collect 成 `Option` ⇒ **溢出变 NULL**：
/// 「求和装不下」被说成「这组全空」，两个含义天差地别，而调用方无从分辨。
/// 本仓纪律是**宁可报错，不给错值** —— 这里照办。
fn dec_fit(v: i256, what: &str) -> Result<i128> {
    v.to_i128().ok_or_else(|| {
        QsdbError::invalid(format!(
            "{what} 超出 DECIMAL 能表示的范围（38 位十进制）——              结果装不进声明的输出精度。请缩窄精度（如 `CAST(… AS DECIMAL(30,8))`）             或分批计算；**本仓不做静默截断、也不回 NULL**"
        ))
    })
}

/// `avg(decimal)`：`HALF_UP(sum / count)`，与非窗口聚合逐字一致。
fn dec_avg(sum: i256, cnt: i64, in_scale: i8, out_scale: i8) -> Result<i128> {
    if cnt <= 0 {
        return Err(QsdbError::type_err("avg with zero count (internal)"));
    }
    let d = i256::from_i128(cnt as i128);
    // ⚠️ **输出 scale 可能 ≠ 输入 scale**（Spark 的 `avg` 规则是 `s+4`）⇒ 输出小数位
    // 更多时把**分子**（unscaled 和）放大 `10^(s_out - s_in)`。
    //
    // 漏掉这一步不会报错，只会**把数算小**：2026-10-16 实测 `avg(DECIMAL(30,8)) OVER ()`
    // 回 `0.000020833333` 而不是 `0.208333333333` —— 小了 1 万倍。
    // **同一处病在聚合那条路（`exec::agg`）也有一份，必须一起改**，
    // 否则"窗口 avg"与"聚合 avg"会给出不同的数（本文件那条逐值对撞测试会当场抓到）。
    let shift = i32::from(out_scale) - i32::from(in_scale);
    let (num, den) = if shift >= 0 {
        (
            sum * i256::from_i128(crate::types::decimal::pow10(shift as u32)),
            d,
        )
    } else {
        (
            sum,
            d * i256::from_i128(crate::types::decimal::pow10((-shift) as u32)),
        )
    };
    let q = num / den;
    let r = num % den;
    let round_up =
        r != i256::from_i128(0) && r.wrapping_mul(i256::from_i128(2)).wrapping_abs() >= den;
    let adj = if !round_up {
        q
    } else if num >= i256::from_i128(0) {
        q + i256::from_i128(1)
    } else {
        q - i256::from_i128(1)
    };
    dec_fit(adj, "avg")
}

/// 一批 `Decimal128` 值（按行取；NULL → `None`）。
fn dec_vals(arr: &ArrayRef, n: usize) -> Option<Vec<Option<i128>>> {
    let d = arr.as_any().downcast_ref::<Decimal128Array>()?;
    Some(
        (0..n)
            .map(|i| if d.is_null(i) { None } else { Some(d.value(i)) })
            .collect(),
    )
}

/// 窗口 DECIMAL 聚合的输出精度（与非窗口聚合同一套规则）。
fn dec_meta(t: &crate::types::LogicalType) -> Result<crate::types::DecimalMeta> {
    match t {
        crate::types::LogicalType::Decimal(m) => Ok(*m),
        other => Err(QsdbError::type_err(format!(
            "窗口 DECIMAL 聚合：参数类型应为 Decimal，实得 {other:?}（内部错误）"
        ))),
    }
}

fn out_meta(
    f: &WindowFn,
    arg_type: &crate::types::LogicalType,
) -> Result<crate::types::DecimalMeta> {
    let m = dec_meta(arg_type)?;
    Ok(match f {
        WindowFn::Sum(..) => crate::types::DecimalMeta::sum_result(m),
        WindowFn::Avg(..) => crate::types::DecimalMeta::avg_result(m),
        _ => m,
    })
}

/// 把聚合窗口的输出**对齐到声明类型**。
///
/// 为什么需要：`min/max` 的输出类型 = **参数类型**（`INT` 列 ⇒ `Int32`），而两条
/// 取值路径都按 `i64`/`f64` 算 ⇒ 类型对不上时 `RecordBatch::try_new` 直接抛
/// `column types must match schema types, expected Int32 but found Int64`。
/// **默认帧路径此前就是这样**：`min(x) OVER (ORDER BY i)` 在 `INT` 列上必炸
/// （报的是内部错误、不是人话），`FLOAT` 列上其实也对不上（只是没人写过）。
/// 类型相同 ⇒ 零拷贝直通，所以热路径不付代价。
fn align_output(f: &WindowFn, arr: ArrayRef) -> Result<ArrayRef> {
    let want = f.output_type().arrow_type();
    if arr.data_type() == &want {
        return Ok(arr);
    }
    arrow::compute::cast(&arr, &want)
        .map_err(|e| QsdbError::unsupported(format!("window {} output type: {e}", agg_name(f))))
}

/// `count` 的帧求值：数**行**（`count(x)` 数的是非 NULL 的 x）。
///
/// `has_arg = None` 即 `count(*)` —— 逐行都计。与 sum/min/max/avg 的关键差别：
/// **空帧给 0 而不是 NULL**（它数的是行的个数，0 是正确答案，不是"没有值"）。
fn framed_count(
    has_arg: Option<&dyn Array>,
    frame: RowFrame,
    partition_start: &[bool],
    seg_start: Option<&[bool]>,
    n: usize,
) -> ArrayRef {
    // 非 NULL 记 1、NULL 记 0，前缀和即区间计数（`count(*)` 全是 1）
    let marks: Vec<Option<i64>> = (0..n)
        .map(|i| match has_arg {
            None => Some(1i64),
            Some(a) if a.is_null(i) => None,
            Some(_) => Some(1i64),
        })
        .collect();
    let peer = peer_starts(n, seg_start);
    let (_, cnts) = framed_prefix(n, partition_start, frame, &peer, &marks);
    Arc::new(Int64Array::from(cnts))
}

/// `sum/count → avg`：`counts[i] == 0`（空帧或帧内全 NULL）⇒ NULL。
fn avg_of<T: Copy>(
    sums: &[Option<T>],
    cnts: &[i64],
    to_f64: impl Fn(T) -> f64,
) -> Vec<Option<f64>> {
    sums.iter()
        .zip(cnts)
        .map(|(s, &c)| s.map(|v| to_f64(v) / c as f64))
        .collect()
}

/// 分区区间 `[ps, pe)`（输入已按 `PARTITION BY` 排好，`partition_start[0] == true`）。
fn partition_ranges(n: usize, partition_start: &[bool]) -> Vec<(usize, usize)> {
    let mut out = Vec::new();
    let mut s = 0usize;
    for (i, is_start) in partition_start.iter().enumerate().take(n).skip(1) {
        if *is_start {
            out.push((s, i));
            s = i;
        }
    }
    if n > 0 {
        out.push((s, n)); // 末分区的右端（开区间）
    }
    out
}

/// 行 `i` 的帧区间 `[a, b]`（闭区间，行号在本分区内；`a > b` = 空帧）。
///
/// **只夹"该在分区内"的那一侧**：越出分区的那一侧**不**回夹到分区边 ——
/// 回夹会把不该进帧的行算进来。`ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING` 在
/// 分区首行是**空帧**（PG / DuckDB 都给 NULL），回夹成 `[ps, ps]` 就成"取首行"了。
fn frame_bounds(frame: RowFrame, i: usize, ps: usize, pe: usize, peer: &[usize]) -> (i64, i64) {
    let i = i as i64;
    let a = match frame.start {
        FrameBound::Unbounded => ps as i64,
        // RANGE `CURRENT ROW` 起点 = **并列组**起点（十九批 S3：按值分帧，
        // 键相等的一组整组进帧）。组号随 i 单调不减 —— 滑窗前缀与单调队列的
        // "左界单调"前提不破。`peer` 为空 = 该形态不该出现（绑定层只在有
        // ORDER BY 时放行），防御性回退当前行。
        FrameBound::Current if frame.range_peer_start => {
            peer.get(i as usize).copied().unwrap_or(i as usize) as i64
        }
        FrameBound::Current => i,
        FrameBound::Preceding(k) => (i - k as i64).max(ps as i64),
        FrameBound::Following(k) => i + k as i64,
    };
    let b = match frame.end {
        FrameBound::Unbounded => pe as i64 - 1,
        FrameBound::Current => i,
        FrameBound::Preceding(k) => i - k as i64,
        FrameBound::Following(k) => (i + k as i64).min(pe as i64 - 1),
    };
    (a, b)
}

/// 并列组起点数组：`peer[i]` = 行 i 所在并列组的首行号（`seg_start` 缺省 → 空，
/// 调用方保证此帧不用它）。
fn peer_starts(n: usize, seg_start: Option<&[bool]>) -> Vec<usize> {
    let Some(ss) = seg_start else {
        return Vec::new();
    };
    let mut peer = vec![0usize; n];
    let mut s = 0usize;
    for (i, is_start) in ss.iter().enumerate().take(n) {
        if *is_start {
            s = i;
        }
        peer[i] = s;
    }
    peer
}

/// 滑窗求和/计数：分区内一次前缀和 ⇒ 每行 O(1)。
///
/// 返回 `(sums, counts)`。`counts[i] == 0`（空帧 **或** 帧内全 NULL）时
/// `sums[i] = None` —— 这就是"帧内无值 ⇒ NULL"的判据，与默认帧路径同一套口径。
/// ⚠️ `counts` 的口径是**非 NULL 计数**（`count(x)`），不是行数。
fn framed_prefix<T>(
    n: usize,
    partition_start: &[bool],
    frame: RowFrame,
    peer: &[usize],
    vals: &[Option<T>],
) -> (Vec<Option<T>>, Vec<i64>)
where
    T: Copy + Default + std::ops::Add<Output = T> + std::ops::Sub<Output = T>,
{
    let mut sums: Vec<Option<T>> = vec![None; n];
    let mut cnts = vec![0i64; n];
    for (ps, pe) in partition_ranges(n, partition_start) {
        let len = pe - ps;
        let mut psum = vec![T::default(); len + 1];
        let mut pcnt = vec![0i64; len + 1];
        for k in 0..len {
            let v = vals[ps + k];
            psum[k + 1] = psum[k] + v.unwrap_or_default();
            pcnt[k + 1] = pcnt[k] + i64::from(v.is_some());
        }
        for i in ps..pe {
            let (a, b) = frame_bounds(frame, i, ps, pe, peer);
            if a > b {
                continue; // 空帧 ⇒ sums 保持 None、cnts 保持 0
            }
            let lo = (a - ps as i64) as usize;
            let hi = (b - ps as i64 + 1) as usize;
            cnts[i] = pcnt[hi] - pcnt[lo];
            sums[i] = if cnts[i] == 0 {
                None
            } else {
                Some(psum[hi] - psum[lo])
            };
        }
    }
    (sums, cnts)
}

/// 滑窗 min/max：单调队列，摊还 O(1)/行。
///
/// 能用队列的前提是**帧两端都随 i 单调不减**（`Preceding` / `Current` /
/// `Following` / `Unbounded` 四种边界都满足）⇒ 出队（滑出左界、或被更优值压掉）
/// 的元素此后再不会被任何一帧需要。分区开头重置队列，否则会把上一分区的行推
/// 进来。NULL **不入队**（不参与极值），帧内全 NULL ⇒ 空队 ⇒ NULL。
fn framed_extreme<T: PartialOrd + Copy>(
    vals: &[Option<T>],
    frame: RowFrame,
    partition_start: &[bool],
    peer: &[usize],
    n: usize,
    want_min: bool,
) -> Vec<Option<T>> {
    let mut out: Vec<Option<T>> = vec![None; n];
    for (ps, pe) in partition_ranges(n, partition_start) {
        let mut dq: std::collections::VecDeque<usize> = std::collections::VecDeque::new();
        let mut last = ps as i64 - 1; // 已入过队的最大行号
        for (i, slot) in out.iter_mut().enumerate().take(pe).skip(ps) {
            let (a, b) = frame_bounds(frame, i, ps, pe, peer);
            while last < b {
                last += 1;
                let j = last as usize;
                let Some(v) = vals[j] else { continue };
                // 队尾不比新值更优 ⇒ 此后永远不可能是答案，直接丢弃
                while let Some(&k) = dq.back() {
                    match vals[k] {
                        Some(kv) if (want_min && kv >= v) || (!want_min && kv <= v) => {
                            dq.pop_back();
                        }
                        _ => break,
                    }
                }
                dq.push_back(j);
            }
            // 队首滑出帧左界 ⇒ 出队（左界单调不减 ⇒ 出去就不会再回来）
            while let Some(&k) = dq.front() {
                if (k as i64) < a {
                    dq.pop_front();
                } else {
                    break;
                }
            }
            *slot = dq.front().and_then(|&k| vals[k]);
        }
    }
    out
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
///
/// 与 `ProjectExec` / 聚合分组键**共用一份实现**（[`super::broadcast_const`]）——
/// 这族坑（"常量数组按行号索引"）已经在三处各咬过一次，不再各写各的。
fn broadcast_const(arr: ArrayRef, n: usize) -> Result<ArrayRef> {
    super::broadcast_const(&arr, n)
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
        // 溢出态：逐分区产出（每分区一个批）
        if self.spill.is_some() {
            return self.spill_next();
        }
        if self.output.is_some() {
            return None;
        }
        let batch = match self.execute_all() {
            Ok(b) => b,
            Err(e) => return Some(Err(e)),
        };
        // `execute_all` 可能刚转入溢出（物化时就要不到额度）→ 逐分区产出
        if self.spill.is_some() {
            return self.spill_next();
        }
        self.output = Some(RecordBatch::new_empty(Arc::clone(&self.schema)));
        if batch.num_rows() == 0 {
            return None;
        }
        Some(Ok(batch))
    }
}
