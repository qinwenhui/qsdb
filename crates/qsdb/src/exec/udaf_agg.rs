//! UDAF 串行物化聚合（M5-A4）。
//!
//! 查询的聚合集含任何 UDAF 时走本路径：物化输入 → 按 group key 编码分组
//! （首见序）→ 每组 take 出参数列 → UDAF 整组求值 / 内置聚合按组重算。
//! 不参与并行 partial/merge——核心聚合热路径（GroupTable 状态机）零改动。
//!
//! 混合查询中内置聚合的按组重算语义：count 精确；sum/avg 数值路径
//! （float 求和次序与状态机路径不同——浮点和本无结合律）；min/max 经
//! cmp_literals 精确；avg(decimal) 混用显式报错（状态机 decimal 精度路径不可复用）。

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, UInt32Array};
use arrow::compute::{concat, take};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};

use crate::error::{QsdbError, Result};
use crate::expr::{AggExpr, AggFunc, PhysExpr, eval};
use crate::query::QueryContext;
use crate::types::{Literal, LogicalType};

use super::ExecNode;
use super::agg::{array_value_to_literal, encode_key_row, literals_to_array};

/// UDAF（或含 UDAF 的混合）聚合执行节点。
pub struct UdafAggExec {
    input: Box<dyn ExecNode>,
    group: Vec<PhysExpr>,
    aggs: Vec<AggExpr>,
    schema: SchemaRef,
    ctx: Arc<QueryContext>,
    output: Option<RecordBatch>,
}

impl UdafAggExec {
    pub fn new(
        input: Box<dyn ExecNode>,
        group: Vec<PhysExpr>,
        aggs: Vec<AggExpr>,
        ctx: Arc<QueryContext>,
    ) -> Self {
        let mut fields: Vec<arrow::datatypes::Field> = group
            .iter()
            .map(|g| arrow::datatypes::Field::new(g.display(), g.data_type().arrow_type(), true))
            .collect();
        for a in &aggs {
            fields.push(arrow::datatypes::Field::new(
                &a.out_name,
                a.out_ty.arrow_type(),
                true,
            ));
        }
        let schema = Arc::new(arrow::datatypes::Schema::new(fields));
        Self {
            input,
            group,
            aggs,
            schema,
            ctx,
            output: None,
        }
    }

    fn execute_all(&mut self) -> Result<RecordBatch> {
        // 物化输入（批边界协作检查 + 内存记账）
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
        let input_schema = batches[0].schema();
        let mut cols: Vec<ArrayRef> = Vec::with_capacity(input_schema.fields().len());
        for c in 0..input_schema.fields().len() {
            let parts: Vec<&dyn Array> = batches.iter().map(|b| b.column(c).as_ref()).collect();
            cols.push(concat(&parts).map_err(|e| QsdbError::invalid(format!("udaf concat: {e}")))?);
        }
        let n: usize = batches.iter().map(|b| b.num_rows()).sum();
        let input_batch = RecordBatch::try_new_with_options(
            Arc::clone(&input_schema),
            cols,
            &RecordBatchOptions::new().with_row_count(Some(n)),
        )
        .map_err(|e| QsdbError::invalid(format!("udaf batch: {e}")))?;
        if n == 0 {
            return Ok(RecordBatch::new_empty(Arc::clone(&self.schema)));
        }

        // 分组：key 编码 → gid（首见序）+ 组行索引 + 组键 literal
        let key_arrs: Vec<ArrayRef> = self
            .group
            .iter()
            .map(|g| eval::eval(g, &input_batch))
            .collect::<Result<_>>()?;
        let mut key_buf = Vec::new();
        let mut gid_of_key: HashMap<Vec<u8>, u32> = HashMap::new();
        let mut group_keys: Vec<Vec<Literal>> = Vec::new();
        let mut group_rows: Vec<Vec<u32>> = Vec::new();
        for row in 0..n {
            encode_key_row(&key_arrs, row, &mut key_buf);
            let gid = *gid_of_key.entry(key_buf.clone()).or_insert_with(|| {
                let gid = group_keys.len() as u32;
                group_keys.push(
                    key_arrs
                        .iter()
                        .map(|a| array_value_to_literal(a, row))
                        .collect(),
                );
                group_rows.push(Vec::new());
                gid
            });
            group_rows[gid as usize].push(row as u32);
        }

        // 输出：group 列 ++ 聚合列
        let mut out_cols: Vec<ArrayRef> = Vec::with_capacity(self.group.len() + self.aggs.len());
        for (gi, g) in self.group.iter().enumerate() {
            let lits: Vec<&Literal> = group_keys.iter().map(|k| &k[gi]).collect();
            out_cols.push(literals_to_array(&lits, &g.data_type())?);
        }
        for agg in &self.aggs {
            let arr = self.finalize_column(agg, &group_rows, &input_batch)?;
            out_cols.push(arr);
        }
        RecordBatch::try_new(Arc::clone(&self.schema), out_cols)
            .map_err(|e| QsdbError::invalid(format!("udaf output: {e}")))
    }

    /// 单个聚合列：每组 take 参数值后求值（UDAF / 内置重算）。
    fn finalize_column(
        &self,
        agg: &AggExpr,
        group_rows: &[Vec<u32>],
        input_batch: &RecordBatch,
    ) -> Result<ArrayRef> {
        let arg_arr = match &agg.arg {
            Some(a) => Some(eval::eval(a, input_batch)?),
            None => None,
        };
        let mut lits: Vec<Literal> = Vec::with_capacity(group_rows.len());
        for rows in group_rows {
            let taken: Option<ArrayRef> = match &arg_arr {
                Some(a) => Some(
                    take(a, &UInt32Array::from(rows.clone()), None)
                        .map_err(|e| QsdbError::invalid(format!("udaf take: {e}")))?,
                ),
                None => None,
            };
            lits.push(match agg.func {
                AggFunc::Udaf => {
                    let def = agg
                        .udaf
                        .as_ref()
                        .ok_or_else(|| QsdbError::invalid("UDAF agg without UdafDef (internal)"))?;
                    let out = (def.eval)(&[taken.expect("udaf requires an argument")])?;
                    if out.len() != 1 {
                        return Err(QsdbError::invalid(
                            "UDAF eval must return exactly one value",
                        ));
                    }
                    if out.is_null(0) {
                        Literal::Null
                    } else {
                        array_value_to_literal(out.as_ref(), 0)
                    }
                }
                AggFunc::Count => match &taken {
                    None => Literal::Int64(rows.len() as i64), // COUNT(*)
                    Some(t) => Literal::Int64((t.len() - t.null_count()) as i64),
                },
                AggFunc::Sum | AggFunc::Avg => sum_or_avg(agg, taken.as_deref())?,
                AggFunc::Min | AggFunc::Max => min_max(taken.as_deref(), agg.func == AggFunc::Min)?,
            });
        }
        let refs: Vec<&Literal> = lits.iter().collect();
        literals_to_array(&refs, &agg.out_ty)
    }
}

fn sum_or_avg(agg: &AggExpr, taken: Option<&dyn Array>) -> Result<Literal> {
    let a = taken
        .ok_or_else(|| QsdbError::invalid(format!("{} requires an argument", agg.func.name())))?;
    let is_avg = agg.func == AggFunc::Avg;
    if is_avg
        && matches!(
            agg.arg.as_ref().map(|x| x.data_type()),
            Some(LogicalType::Decimal(_))
        )
    {
        return Err(QsdbError::unsupported(
            "avg(decimal) 与 UDAF 混用走后置（decimal 精度路径在状态机内）",
        ));
    }
    let mut sum_i: i128 = 0;
    let mut sum_f: f64 = 0.0;
    let mut is_float = false;
    let mut count = 0i64;
    for i in 0..a.len() {
        if a.is_null(i) {
            continue;
        }
        let v = array_value_to_literal(a, i);
        match v {
            Literal::Int8(x) => sum_i += i128::from(x),
            Literal::Int16(x) => sum_i += i128::from(x),
            Literal::Int32(x) => sum_i += i128::from(x),
            Literal::Int64(x) => sum_i += i128::from(x),
            Literal::Float32(x) => {
                is_float = true;
                sum_f += f64::from(x);
            }
            Literal::Float64(x) => {
                is_float = true;
                sum_f += x;
            }
            other => {
                return Err(QsdbError::type_err(format!(
                    "sum/avg over {other:?} not supported in UDAF mixed path"
                )));
            }
        }
        count += 1;
    }
    if count == 0 {
        return Ok(Literal::Null);
    }
    if is_avg {
        let s = if is_float {
            sum_f + sum_i as f64
        } else {
            sum_i as f64
        };
        return Ok(Literal::Float64(s / count as f64));
    }
    if is_float {
        Ok(Literal::Float64(sum_f + sum_i as f64))
    } else {
        Ok(Literal::Int64(i64::try_from(sum_i).unwrap_or(i64::MAX)))
    }
}

fn min_max(taken: Option<&dyn Array>, is_min: bool) -> Result<Literal> {
    let a = taken.ok_or_else(|| QsdbError::invalid("min/max requires an argument"))?;
    let mut best: Option<Literal> = None;
    for i in 0..a.len() {
        if a.is_null(i) {
            continue;
        }
        let v = array_value_to_literal(a, i);
        let take = match &best {
            None => true,
            Some(b) => {
                let ord = crate::types::compare::cmp_literals(&v, b)
                    .map_err(|e| QsdbError::invalid(format!("udaf group compare: {e}")))?
                    .unwrap_or(std::cmp::Ordering::Equal);
                if is_min {
                    ord == std::cmp::Ordering::Less
                } else {
                    ord == std::cmp::Ordering::Greater
                }
            }
        };
        if take {
            best = Some(v);
        }
    }
    Ok(best.unwrap_or(Literal::Null))
}

impl ExecNode for UdafAggExec {
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
