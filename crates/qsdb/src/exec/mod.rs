//! 执行算子：拉式迭代（`next() -> Option<Result<RecordBatch>>`）。
//!
//! M2 算子：Filter / Project / Limit / Sort(TopN) / Distinct / HashAgg。
//! Scan 由 `crate::scan::Scan` 包装。并行聚合见 `agg`（文件级 rayon 展开）。

pub mod agg;
pub mod join;
pub mod par_scan;
pub mod spill;

use std::sync::Arc;

use arrow::array::Array;

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
    fn next(&mut self) -> Option<Result<RecordBatch>> {
        let batch = self.input.next()?;
        let batch = match batch {
            Ok(b) => b,
            Err(e) => return Some(Err(e)),
        };
        let mut cols = Vec::with_capacity(self.exprs.len());
        for e in &self.exprs {
            match eval::eval(e, &batch) {
                Ok(a) => cols.push(a),
                Err(err) => return Some(Err(err)),
            }
        }
        let n = batch.num_rows();
        let out = RecordBatch::try_new(self.schema(), cols)
            .map_err(|e| QsdbError::invalid(format!("project failed: {e}")))
            .and_then(|b| {
                // 常量表达式产 1 行 → 广播到批长
                if b.num_rows() == 1 && n > 1 {
                    let mut cols = Vec::with_capacity(b.num_columns());
                    for c in b.columns() {
                        let idx: Vec<(usize, usize)> = vec![(0, 0); n];
                        cols.push(
                            arrow::compute::interleave(&[c.as_ref()], &idx)
                                .map_err(|e| QsdbError::invalid(format!("broadcast: {e}")))?,
                        );
                    }
                    RecordBatch::try_new(self.schema(), cols)
                        .map_err(|e| QsdbError::invalid(format!("project failed: {e}")))
                } else {
                    Ok(b)
                }
            });
        Some(out)
    }
}

/// LIMIT。
pub struct LimitExec {
    pub input: Box<dyn ExecNode>,
    pub remaining: usize,
}

impl ExecNode for LimitExec {
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }
    fn next(&mut self) -> Option<Result<RecordBatch>> {
        if self.remaining == 0 {
            return None;
        }
        let batch = self.input.next()?;
        let batch = match batch {
            Ok(b) => b,
            Err(e) => return Some(Err(e)),
        };
        if batch.num_rows() > self.remaining {
            let sliced = batch.slice(0, self.remaining);
            self.remaining = 0;
            return Some(Ok(sliced));
        }
        self.remaining -= batch.num_rows();
        Some(Ok(batch))
    }
}

/// 排序键。
#[derive(Debug, Clone)]
pub struct SortKey {
    pub expr: PhysExpr,
    pub descending: bool,
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
        let mut charged = 0u64;

        let flush_chunk = |chunk: &mut Vec<RecordBatch>,
                           chunk_bytes: &mut u64,
                           spill_dir: &mut Option<spill::SpillDir>,
                           run_paths: &mut Vec<std::path::PathBuf>|
         -> Result<()> {
            if chunk.is_empty() {
                return Ok(());
            }
            let dir = spill_dir.get_or_insert_with(spill::SpillDir::new);
            let path = spill::write_sorted_run(dir, chunk, &self.keys, &self.schema)?;
            run_paths.push(path);
            chunk.clear();
            *chunk_bytes = 0;
            Ok(())
        };

        while let Some(b) = self.input.next() {
            self.ctx.check()?;
            let x = b?;
            let bytes = x.get_array_memory_size() as u64;
            // 记账超限 → 当前 chunk 落盘，继续（总量受磁盘而非内存约束）
            if self.ctx.charge_memory(bytes).is_err() {
                self.ctx.release_memory(chunk_bytes);
                flush_chunk(&mut chunk, &mut chunk_bytes, &mut spill_dir, &mut run_paths)?;
                // 单批超过全额限制：该批独立成 run（不再记账）
                let mut single = Vec::new();
                single.push(x);
                flush_chunk(&mut single, &mut 0, &mut spill_dir, &mut run_paths)?;
                continue;
            }
            charged += bytes;
            chunk_bytes += bytes;
            chunk.push(x);
        }
        self.ctx.release_memory(charged);

        if run_paths.is_empty() {
            // 纯内存路径（原逻辑）
            return sort_in_memory(chunk, &self.schema());
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
        // 排序列（表达式求值）
        let mut sort_cols: Vec<SortColumn> = Vec::with_capacity(self.keys.len());
        for k in &self.keys {
            let arr = match eval::eval(&k.expr, &data) {
                Ok(a) => a,
                Err(e) => return Some(Err(e)),
            };
            // pg 语义：ASC NULLS LAST / DESC NULLS FIRST
            sort_cols.push(SortColumn {
                values: arr,
                options: Some(arrow::compute::SortOptions {
                    descending: k.descending,
                    nulls_first: k.descending,
                }),
            });
        }
        let indices = lexsort_to_indices(&sort_cols, self.limit)
            .map_err(|e| QsdbError::invalid(format!("sort failed: {e}")));
        let indices = match indices {
            Ok(i) => i,
            Err(e) => return Some(Err(e)),
        };
        let mut cols = Vec::with_capacity(data.num_columns());
        for c in data.columns() {
            match take(c, &indices, None) {
                Ok(t) => cols.push(t),
                Err(e) => return Some(Err(QsdbError::invalid(format!("take: {e}")))),
            }
        }
        let out = RecordBatch::try_new_with_options(
            self.schema(),
            cols,
            &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(indices.len())),
        )
        .map_err(|e| QsdbError::invalid(format!("sort output: {e}")));
        Some(out)
    }
}

/// 去重（哈希；group = 全部列）。
pub struct DistinctExec {
    pub input: Box<dyn ExecNode>,
    state: agg::GroupTable,
    schema: SchemaRef,
    emitted: bool,
}

impl DistinctExec {
    pub fn new(input: Box<dyn ExecNode>) -> Self {
        let schema = input.schema();
        Self {
            input,
            state: agg::GroupTable::new(),
            schema,
            emitted: false,
        }
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
        while let Some(b) = self.input.next() {
            let batch = match b {
                Ok(x) => x,
                Err(e) => return Some(Err(e)),
            };
            if let Err(e) = self.state.ingest(&group_exprs, &[], &batch) {
                return Some(Err(e));
            }
        }
        self.emitted = true;
        if self.state.is_empty() {
            return None;
        }
        let keys = self.state.take_group_keys();
        let out = match agg::build_output_batch(&keys, &[], &self.schema()) {
            Ok(o) => o,
            Err(e) => return Some(Err(e)),
        };
        Some(Ok(out))
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
        Ok(Self {
            input,
            group,
            aggs,
            state,
            schema: Arc::new(arrow::datatypes::Schema::new(fields)),
            emitted: false,
            ctx,
        })
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
        while let Some(b) = self.input.next() {
            if let Err(e) = self.ctx.check() {
                return Some(Err(e));
            }
            let batch = match b {
                Ok(x) => x,
                Err(e) => return Some(Err(e)),
            };
            // 聚合状态（分组表）随输入增长：按输入批记账近似
            if let Err(e) = self
                .ctx
                .charge_memory(batch.get_array_memory_size() as u64 / 2)
            {
                return Some(Err(e));
            }
            if let Err(e) = self.state.ingest(&self.group, &self.aggs, &batch) {
                return Some(Err(e));
            }
        }
        self.emitted = true;
        if self.state.is_empty() {
            return None;
        }
        let keys = self.state.take_group_keys();
        let states = self.state.take_states();
        let out = match agg::finalize(&keys, &states, &self.aggs, &self.group, &self.schema()) {
            Ok(o) => o,
            Err(e) => return Some(Err(e)),
        };
        Some(Ok(out))
    }
}
