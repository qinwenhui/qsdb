//! HashJoin：build 侧（右表）建哈希表 → probe 侧（左表）流式探测。
//!
//! - 等值条件（多键 AND）；INNER / LEFT OUTER
//! - NULL 键不匹配（SQL 语义：NULL = NULL 非 TRUE）
//! - build 侧全量物化（ctx 内存记账）；probe 侧流式
//! - 输出列序：左表列 ++ 右表列

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, StringArray, TimestampNanosecondArray,
    UInt32Array,
};
use arrow::compute::{concat, take};
use arrow::datatypes::SchemaRef;
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
}

/// 笛卡尔积（非等值 INNER JOIN 的底座）：右表首拉时物化，左批逐批 × 右表展开。
pub struct CrossJoinExec {
    probe: Box<dyn ExecNode>,
    build: Option<Box<dyn ExecNode>>,
    build_state: Option<RecordBatch>,
    schema: SchemaRef,
    ctx: Arc<QueryContext>,
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
            schema,
            ctx,
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
        let mut batches = Vec::new();
        let mut charged = 0u64;
        while let Some(b) = build.next() {
            self.ctx.check()?;
            let x = b?;
            let bytes = x.get_array_memory_size() as u64;
            self.ctx.charge_memory(bytes)?;
            charged += bytes;
            batches.push(x);
        }
        self.ctx.release_memory(charged);
        let table = match batches.split_first() {
            Some((first, _)) => crate::exec::spill::concat_batches(&batches, &first.schema())?,
            None => RecordBatch::new_empty(build.schema()),
        };
        self.build_state = Some(table);
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
        let batch = self.probe.next()?;
        let batch = match batch {
            Ok(b) => b,
            Err(e) => return Some(Err(e)),
        };
        if batch.num_rows() == 0 {
            return self.next();
        }
        // 展开：左批每行 × 右表每行
        let n_left = batch.num_rows();
        let n_right = self.build_state.as_ref().map(|b| b.num_rows()).unwrap_or(0);
        let total = n_left * n_right;
        let mut left_indices: Vec<u32> = Vec::with_capacity(total);
        let mut right_indices: Vec<u32> = Vec::with_capacity(total);
        for l in 0..n_left {
            for r in 0..n_right {
                left_indices.push(l as u32);
                right_indices.push(r as u32);
            }
        }
        let li = UInt32Array::from(left_indices);
        let ri = UInt32Array::from(right_indices);
        let mut cols: Vec<ArrayRef> = Vec::with_capacity(self.schema.fields().len());
        for c in 0..batch.num_columns() {
            let taken = take(batch.column(c), &li, None)
                .map_err(|e| QsdbError::invalid(format!("cross take: {e}")));
            match taken {
                Ok(a) => cols.push(a),
                Err(e) => return Some(Err(e)),
            }
        }
        if let Some(build) = &self.build_state {
            for c in 0..build.num_columns() {
                let taken = take(build.column(c), &ri, None)
                    .map_err(|e| QsdbError::invalid(format!("cross take: {e}")));
                match taken {
                    Ok(a) => cols.push(a),
                    Err(e) => return Some(Err(e)),
                }
            }
        } else {
            // 右表为空批：补全空列
            for f in self.schema.fields().iter().skip(batch.num_columns()) {
                cols.push(arrow::array::new_null_array(f.data_type(), total));
            }
        }
        let out = RecordBatch::try_new_with_options(
            Arc::clone(&self.schema),
            cols,
            &RecordBatchOptions::new().with_row_count(Some(total)),
        )
        .map_err(|e| QsdbError::invalid(format!("cross output: {e}")));
        Some(out)
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
    probe_done: bool,
}

struct BuildState {
    /// 右表物化批（单批 concat 后）
    table: RecordBatch,
    /// key bytes → 行号列表
    index: HashMap<Vec<u8>, Vec<u32>>,
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
        // 输出 schema：左（probe）++ 右（build）
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
            build,
            probe_keys,
            build_keys,
            join_type,
            schema,
            ctx,
            build_state: None,
            probe_done: false,
        }
    }

    fn build_phase(&mut self) -> Result<()> {
        if self.build_state.is_some() {
            return Ok(());
        }
        // 物化右表
        let mut batches = Vec::new();
        let mut charged = 0u64;
        while let Some(b) = self.build.next() {
            self.ctx.check()?;
            let x = b?;
            let bytes = x.get_array_memory_size() as u64;
            self.ctx.charge_memory(bytes)?;
            charged += bytes;
            batches.push(x);
        }
        self.ctx.release_memory(charged);
        let table = concat_batches(&batches, &self.build.schema())?;

        // 键哈希索引
        let mut key_cols: Vec<ArrayRef> = Vec::with_capacity(self.build_keys.len());
        for k in &self.build_keys {
            key_cols.push(eval::eval(k, &table)?);
        }
        let mut index: HashMap<Vec<u8>, Vec<u32>> = HashMap::new();
        let mut buf: Vec<u8> = Vec::new();
        for row in 0..table.num_rows() {
            encode_join_key(&key_cols, row, &mut buf);
            if buf.first() == Some(&0) {
                continue; // 任一键 NULL → 不入索引（SQL 语义）
            }
            index.entry(buf.clone()).or_default().push(row as u32);
        }
        self.build_state = Some(BuildState { table, index });
        Ok(())
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
        if let Err(e) = self.build_phase() {
            return Some(Err(e));
        }
        let join_type = self.join_type;
        let out_schema = Arc::clone(&self.schema);
        let probe_keys = self.probe_keys.clone();
        let build = self.build_state.as_ref().unwrap();
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
            let out = match join_batch(&batch, build, &probe_keys, &out_schema, join_type) {
                Ok(Some(b)) => b,
                Ok(None) => continue, // 无匹配且非 LEFT
                Err(e) => return Some(Err(e)),
            };
            return Some(Ok(out));
        }
    }
}

/// 单个 probe 批的连接。
fn join_batch(
    probe_batch: &RecordBatch,
    state: &BuildState,
    probe_keys: &[PhysExpr],
    out_schema: &SchemaRef,
    join_type: JoinType,
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
    let mut buf: Vec<u8> = Vec::new();
    for row in 0..n {
        encode_join_key(&key_cols, row, &mut buf);
        if buf.first() == Some(&0) {
            unmatched.push(row as u32);
            continue;
        }
        match state.index.get(buf.as_slice()) {
            Some(rows) => {
                for &b in rows {
                    probe_idx.push(row as u32);
                    build_idx.push(b);
                }
            }
            None => unmatched.push(row as u32),
        }
    }
    // LEFT：未匹配行补 NULL build 侧
    let emit_unmatched = join_type == JoinType::LeftOuter && !unmatched.is_empty();
    if probe_idx.is_empty() && !emit_unmatched {
        return Ok(None);
    }
    let n_probe_cols = probe_batch.num_columns();
    let n_build_cols = state.table.num_columns();
    let total_rows = probe_idx.len() + unmatched.len();
    // 左（probe）列索引 = 匹配行 ++ 未匹配行（保留左值）
    let mut all_probe = probe_idx.clone();
    if emit_unmatched {
        all_probe.extend_from_slice(&unmatched);
    }
    let indices = UInt32Array::from(all_probe);
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(n_probe_cols + n_build_cols);
    if !probe_idx.is_empty() || emit_unmatched {
        for c in 0..n_probe_cols {
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
            let taken = take(state.table.column(c), &build_indices, None)
                .map_err(|e| QsdbError::invalid(format!("join take: {e}")))?;
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

fn new_null_like(arr: &dyn Array, n: usize) -> ArrayRef {
    arrow::array::new_null_array(arr.data_type(), n)
}

/// 连接键编码：与 agg 的 encode_key_row 同构（null 前缀 tag + 类型化值）。
fn encode_join_key(arrays: &[ArrayRef], row: usize, buf: &mut Vec<u8>) {
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
