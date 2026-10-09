//! 两阶段哈希聚合：GroupTable（分组 + 状态）→ partial / merge / finalize。
//!
//! 性能要点（1 亿行 gate）：
//! - group key 编码复用 buffer（零分配），FxHash 8 字节块哈希
//! - 行→组 id 一次遍历；各聚合列独立数组序更新（缓存友好）
//! - DECIMAL sum 走 i256；float sum Kahan 补偿
//!
//! partial 产出即 GroupTable 本身（内存态），merge 直接合并表——
//! 并行路径（文件分片 × rayon）在 exec_builder 层展开。

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int64Array,
    StringArray, Time64NanosecondArray, TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, i256};
use arrow::record_batch::RecordBatch;

use crate::error::{QsdbError, Result};
use crate::expr::{AggExpr, PhysExpr, eval};
use crate::types::{DecimalMeta, Literal, LogicalType};

// ---------------------------------------------------------------------------
// FxHash（简版，8 字节块）
// ---------------------------------------------------------------------------

const FX_SEED: u64 = 0x51_7c_c1_b7_27_22_0a_95;

#[inline]
fn fxhash_bytes(bytes: &[u8]) -> u64 {
    let mut h = FX_SEED;
    let mut chunks = bytes.chunks_exact(8);
    for c in &mut chunks {
        let v = u64::from_le_bytes(c.try_into().unwrap());
        h = (h.rotate_left(5) ^ v).wrapping_mul(0x517c_c1b7_2722_0a95);
    }
    let rem = chunks.remainder();
    if !rem.is_empty() {
        let mut buf = [0u8; 8];
        buf[..rem.len()].copy_from_slice(rem);
        h = (h.rotate_left(5) ^ u64::from_le_bytes(buf)).wrapping_mul(0x517c_c1b7_2722_0a95);
    }
    h
}

// ---------------------------------------------------------------------------
// group key 编码（复用 buffer）
// ---------------------------------------------------------------------------

/// 把一行各 group 列编码进 buf（tag + bytes）；失败列按 raw 字符串兜底。
fn encode_key_row(arrays: &[ArrayRef], row: usize, buf: &mut Vec<u8>) {
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
                .downcast_ref::<arrow::array::Int8Array>()
                .unwrap()
                .value(i)
                .to_le_bytes(),
        ),
        DataType::Int16 => buf.extend_from_slice(
            &a.as_any()
                .downcast_ref::<arrow::array::Int16Array>()
                .unwrap()
                .value(i)
                .to_le_bytes(),
        ),
        DataType::Int32 => buf.extend_from_slice(
            &a.as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .unwrap()
                .value(i)
                .to_le_bytes(),
        ),
        DataType::Int64
        | DataType::Timestamp(TimeUnit::Nanosecond, _)
        | DataType::Time64(TimeUnit::Nanosecond) => {
            let v = if matches!(a.data_type(), DataType::Int64) {
                a.as_any().downcast_ref::<Int64Array>().unwrap().value(i)
            } else if matches!(a.data_type(), DataType::Timestamp(_, _)) {
                a.as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap()
                    .value(i)
            } else {
                a.as_any()
                    .downcast_ref::<Time64NanosecondArray>()
                    .unwrap()
                    .value(i)
            };
            buf.extend_from_slice(&v.to_le_bytes());
        }
        DataType::Float32 => buf.extend_from_slice(
            &a.as_any()
                .downcast_ref::<arrow::array::Float32Array>()
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
            // 兜底：显示文本
            let s = format!("{other:?}:{i}");
            buf.extend_from_slice(s.as_bytes());
        }
    }
}

/// 数组取值 → Literal（全部类型；NULL → Literal::Null）。
pub fn array_value_to_literal(a: &dyn Array, i: usize) -> Literal {
    use arrow::datatypes::*;
    if a.is_null(i) {
        return Literal::Null;
    }
    match a.data_type() {
        DataType::Int8 => Literal::Int8(
            a.as_any()
                .downcast_ref::<arrow::array::Int8Array>()
                .unwrap()
                .value(i),
        ),
        DataType::Int16 => Literal::Int16(
            a.as_any()
                .downcast_ref::<arrow::array::Int16Array>()
                .unwrap()
                .value(i),
        ),
        DataType::Int32 => Literal::Int32(
            a.as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .unwrap()
                .value(i),
        ),
        DataType::Int64 => {
            Literal::Int64(a.as_any().downcast_ref::<Int64Array>().unwrap().value(i))
        }
        DataType::Float32 => Literal::Float32(
            a.as_any()
                .downcast_ref::<arrow::array::Float32Array>()
                .unwrap()
                .value(i),
        ),
        DataType::Float64 => {
            Literal::Float64(a.as_any().downcast_ref::<Float64Array>().unwrap().value(i))
        }
        DataType::Date32 => {
            Literal::Date(a.as_any().downcast_ref::<Date32Array>().unwrap().value(i))
        }
        DataType::Time64(TimeUnit::Nanosecond) => Literal::Time(
            a.as_any()
                .downcast_ref::<Time64NanosecondArray>()
                .unwrap()
                .value(i),
        ),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => Literal::Timestamp(
            a.as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap()
                .value(i),
        ),
        DataType::Boolean => {
            Literal::Boolean(a.as_any().downcast_ref::<BooleanArray>().unwrap().value(i))
        }
        DataType::Decimal128(p, s) => Literal::Decimal {
            unscaled: a
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .value(i),
            meta: DecimalMeta::try_new(*p, *s).unwrap_or(DecimalMeta::new(38, 0)),
        },
        DataType::Utf8 => Literal::Utf8(
            a.as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(i)
                .to_string(),
        ),
        _ => Literal::Null,
    }
}

// ---------------------------------------------------------------------------
// 聚合状态
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
enum SumState {
    #[default]
    None,
    Int(i128),
    Dec(i256),
    /// (sum, kahan c)
    Flt(f64, f64),
}

#[derive(Debug, Clone, Default)]
pub struct AggState {
    pub count: u64,
    sum: SumState,
    min: Option<Literal>,
    max: Option<Literal>,
}

impl AggState {
    fn merge(&mut self, other: &AggState) {
        self.count += other.count;
        merge_sum(&mut self.sum, &other.sum);
        self.min = combine_min(self.min.take(), other.min.clone());
        self.max = combine_max(self.max.take(), other.max.clone());
    }
}

fn merge_sum(a: &mut SumState, b: &SumState) {
    use SumState::*;
    match (&mut *a, b) {
        (None, x) => *a = x.clone(),
        (Int(x), Int(y)) => *x += y,
        (Dec(x), Dec(y)) => *x += *y,
        (Flt(x, c), Flt(y, c2)) => {
            // Kahan 合并
            let t = *x + *y;
            *c += c2 - (t - *x - *y);
            *x = t;
        }
        _ => {}
    }
}

fn combine_min(a: Option<Literal>, b: Option<Literal>) -> Option<Literal> {
    pick(a, b, true)
}
fn combine_max(a: Option<Literal>, b: Option<Literal>) -> Option<Literal> {
    pick(a, b, false)
}

fn pick(a: Option<Literal>, b: Option<Literal>, keep_min: bool) -> Option<Literal> {
    match (a, b) {
        (None, x) | (x, None) => x,
        (Some(x), Some(y)) => {
            if let Ok(Some(o)) = x.partial_cmp_literal(&y) {
                let better = if keep_min {
                    o == std::cmp::Ordering::Less
                } else {
                    o == std::cmp::Ordering::Greater
                };
                if better { Some(x) } else { Some(y) }
            } else {
                Some(x)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// GroupTable
// ---------------------------------------------------------------------------

struct GroupRow {
    key: Vec<u8>,
    literals: Vec<Literal>,
    states: Vec<AggState>,
}

#[derive(Default)]
pub struct GroupTable {
    by_hash: HashMap<u64, Vec<u32>>,
    groups: Vec<GroupRow>,
    pending_arity: Option<usize>,
}

impl GroupTable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.groups.is_empty()
    }

    /// 单批摄入：group 表达式求值 → 行分组 → 各聚合更新。
    pub fn ingest(
        &mut self,
        group: &[PhysExpr],
        aggs: &[AggExpr],
        batch: &RecordBatch,
    ) -> Result<()> {
        let n = batch.num_rows();
        if n == 0 {
            return Ok(());
        }
        let group_arrays: Vec<ArrayRef> = group
            .iter()
            .map(|g| eval::eval(g, batch))
            .collect::<Result<_>>()?;
        let agg_arrays: Vec<Option<ArrayRef>> = aggs
            .iter()
            .map(|a| match &a.arg {
                Some(e) => eval::eval(e, batch).map(Some),
                None => Ok(None),
            })
            .collect::<Result<_>>()?;

        // 行 → 组 id
        // fast path：分组列全为非空 Int64 —— 内联 key 拼接 + 开放寻址表
        let mut group_ids: Vec<u32> = Vec::with_capacity(n);
        if let Some(cols) = int64_group_columns(&group_arrays) {
            self.assign_groups_fast(&cols, &mut group_ids);
        } else {
            let mut key_buf: Vec<u8> = Vec::with_capacity(64);
            for row in 0..n {
                encode_key_row(&group_arrays, row, &mut key_buf);
                let gid = self.group_id(&key_buf, &group_arrays, row);
                group_ids.push(gid);
            }
        }

        // 各聚合列更新（数组序）
        for (ai, agg) in aggs.iter().enumerate() {
            self.update_aggs(ai, agg, agg_arrays[ai].as_deref(), &group_ids)?;
        }
        Ok(())
    }

    /// Int64 分组列特化：key = 列值序列直接内联，开放寻址探测。
    ///
    /// 槽 (hash, gid)；gid == u32::MAX 为空。命中需 hash 与 key bytes 双重校验。
    fn assign_groups_fast(&mut self, cols: &[Vec<i64>], out: &mut Vec<u32>) {
        const EMPTY: u32 = u32::MAX;
        let arity = self.agg_arity_hint();
        let n = cols.first().map(|c| c.len()).unwrap_or(0);
        let mut cap: usize = 16;
        while cap < (n + 1) * 2 {
            cap <<= 1;
        }
        let mut slots: Vec<(u64, u32)> = vec![(0, EMPTY); cap];
        let mut used = 0usize;
        let mut key_bytes: Vec<u8> = Vec::with_capacity(cols.len() * 8);

        let insert_slot = |slots: &mut Vec<(u64, u32)>, h: u64, gid: u32| {
            let mask = slots.len() - 1;
            let mut idx = (h as usize) & mask;
            while slots[idx].1 != EMPTY {
                idx = (idx + 1) & mask;
            }
            slots[idx] = (h, gid);
        };

        for row in 0..n {
            key_bytes.clear();
            for c in cols {
                key_bytes.extend_from_slice(&c[row].to_le_bytes());
            }
            let h = fxhash_bytes(&key_bytes) | 1;
            let mask = slots.len() - 1;
            let mut idx = (h as usize) & mask;
            let gid;
            loop {
                let slot = slots[idx];
                if slot.1 == EMPTY {
                    gid = self.groups.len() as u32;
                    let literals: Vec<Literal> =
                        cols.iter().map(|c| Literal::Int64(c[row])).collect();
                    self.groups.push(GroupRow {
                        key: key_bytes.clone(),
                        literals,
                        states: vec![AggState::default(); arity],
                    });
                    slots[idx] = (h, gid);
                    used += 1;
                    if used * 2 >= slots.len() {
                        let old = std::mem::take(&mut slots);
                        slots = vec![(0, EMPTY); old.len() * 2];
                        for (oh, og) in old {
                            if og != EMPTY {
                                insert_slot(&mut slots, oh, og);
                            }
                        }
                    }
                    break;
                }
                if slot.0 == h && self.groups[slot.1 as usize].key == key_bytes.as_slice() {
                    gid = slot.1;
                    break;
                }
                idx = (idx + 1) & mask;
            }
            out.push(gid);
        }
    }

    fn group_id(&mut self, key: &[u8], arrays: &[ArrayRef], row: usize) -> u32 {
        let h = fxhash_bytes(key);
        if let Some(cands) = self.by_hash.get(&h) {
            for &gid in cands {
                if self.groups[gid as usize].key == key {
                    return gid;
                }
            }
        }
        // 新组
        let gid = self.groups.len() as u32;
        let literals: Vec<Literal> = arrays
            .iter()
            .map(|a| array_value_to_literal(a, row))
            .collect();
        let states = vec![AggState::default(); self.agg_arity_hint()];
        self.groups.push(GroupRow {
            key: key.to_vec(),
            literals,
            states,
        });
        self.by_hash.entry(h).or_default().push(gid);
        gid
    }

    /// 聚合列数（新组初始化用；与 ingest 的 aggs 长度一致——由首次调用注入）。
    fn agg_arity_hint(&self) -> usize {
        self.pending_arity.unwrap_or(0)
    }

    /// merge 另一张 partial 表（两表结构一致：同 group/aggs 语义）。
    pub fn merge(&mut self, other: GroupTable, agg_count: usize) {
        for row in other.groups {
            let h = fxhash_bytes(&row.key);
            let gid = match self.by_hash.get(&h).and_then(|cands| {
                cands
                    .iter()
                    .copied()
                    .find(|&g| self.groups[g as usize].key == row.key)
            }) {
                Some(g) => g,
                None => {
                    let gid = self.groups.len() as u32;
                    self.groups.push(GroupRow {
                        key: row.key.clone(),
                        literals: row.literals.clone(),
                        states: vec![AggState::default(); agg_count],
                    });
                    self.by_hash.entry(h).or_default().push(gid);
                    gid
                }
            };
            let dst = &mut self.groups[gid as usize];
            debug_assert_eq!(dst.states.len(), row.states.len());
            for (d, s) in dst.states.iter_mut().zip(&row.states) {
                d.merge(s);
            }
        }
    }

    /// 输出用组键。
    pub fn take_group_keys(&mut self) -> Vec<Vec<Literal>> {
        self.groups.iter().map(|g| g.literals.clone()).collect()
    }

    pub fn take_states(&mut self) -> Vec<Vec<AggState>> {
        self.groups.iter().map(|g| g.states.clone()).collect()
    }

    fn update_aggs(
        &mut self,
        ai: usize,
        agg: &AggExpr,
        arr: Option<&dyn Array>,
        group_ids: &[u32],
    ) -> Result<()> {
        use crate::expr::AggFunc::*;
        match agg.func {
            Count => {
                match arr {
                    None => {
                        // COUNT(*)：全部行
                        for &g in group_ids {
                            self.groups[g as usize].states[ai].count += 1;
                        }
                    }
                    Some(a) => {
                        for (row, &g) in group_ids.iter().enumerate() {
                            if !a.is_null(row) {
                                self.groups[g as usize].states[ai].count += 1;
                            }
                        }
                    }
                }
                Ok(())
            }
            Sum | Avg => {
                let a = arr.ok_or_else(|| {
                    QsdbError::invalid(format!("{} requires an argument", agg.func.name()))
                })?;
                self.update_sum(ai, a, group_ids)
            }
            Min | Max => {
                let a = arr.ok_or_else(|| {
                    QsdbError::invalid(format!("{} requires an argument", agg.func.name()))
                })?;
                self.update_min_max(ai, agg.func == Min, a, group_ids)
            }
        }
    }

    fn update_sum(&mut self, ai: usize, a: &dyn Array, group_ids: &[u32]) -> Result<()> {
        let dt = a.data_type().clone();
        match dt {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                let vals: Vec<i64> = downcast_ints(a);
                for (row, &g) in group_ids.iter().enumerate() {
                    // NULL 行不计入 sum/avg（SQL 语义；downcast_ints 以 0 占位丢失 null）
                    if a.is_null(row) {
                        continue;
                    }
                    let v = vals[row];
                    let st = &mut self.groups[g as usize].states[ai];
                    match &mut st.sum {
                        SumState::Int(acc) => *acc += i128::from(v),
                        s => *s = SumState::Int(i128::from(v)),
                    }
                    st.count += 1;
                }
            }
            DataType::Float64 => {
                let f = a.as_any().downcast_ref::<Float64Array>().unwrap();
                for (row, &g) in group_ids.iter().enumerate() {
                    if !f.is_null(row) {
                        let v = f.value(row);
                        let st = &mut self.groups[g as usize].states[ai];
                        match &mut st.sum {
                            SumState::Flt(acc, c) => {
                                let y = v - *c;
                                let t = *acc + y;
                                *c = (t - *acc) - y;
                                *acc = t;
                            }
                            s => *s = SumState::Flt(v, 0.0),
                        }
                        st.count += 1;
                    }
                }
            }
            DataType::Decimal128(_, _) => {
                let d = a.as_any().downcast_ref::<Decimal128Array>().unwrap();
                for (row, &g) in group_ids.iter().enumerate() {
                    if !d.is_null(row) {
                        let v = i256::from_i128(d.value(row));
                        let st = &mut self.groups[g as usize].states[ai];
                        match &mut st.sum {
                            SumState::Dec(acc) => *acc += v,
                            s => *s = SumState::Dec(v),
                        }
                        st.count += 1;
                    }
                }
            }
            other => {
                return Err(QsdbError::unsupported(format!(
                    "sum/avg on {other:?} not supported"
                )));
            }
        }
        Ok(())
    }

    fn update_min_max(
        &mut self,
        ai: usize,
        is_min: bool,
        a: &dyn Array,
        group_ids: &[u32],
    ) -> Result<()> {
        // int64/date/ts 快路径
        let dt = a.data_type().clone();
        let fast_i64: Option<Vec<i64>> = match dt {
            DataType::Int64 => Some(
                a.as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .iter()
                    .map(|v| v.unwrap_or(0))
                    .collect(),
            ),
            _ => None,
        };
        if let Some(vals) = fast_i64 {
            for (row, &g) in group_ids.iter().enumerate() {
                if a.is_null(row) {
                    continue;
                }
                let v = Literal::Int64(vals[row]);
                let st = &mut self.groups[g as usize].states[ai];
                let cur = if is_min { st.min.take() } else { st.max.take() };
                let merged = if is_min {
                    combine_min(cur, Some(v))
                } else {
                    combine_max(cur, Some(v))
                };
                if is_min {
                    st.min = merged;
                } else {
                    st.max = merged;
                }
            }
            return Ok(());
        }
        // 通用路径
        for (row, &g) in group_ids.iter().enumerate() {
            if a.is_null(row) {
                continue;
            }
            let v = array_value_to_literal(a, row);
            let st = &mut self.groups[g as usize].states[ai];
            let cur = if is_min { st.min.take() } else { st.max.take() };
            let merged = if is_min {
                combine_min(cur, Some(v))
            } else {
                combine_max(cur, Some(v))
            };
            if is_min {
                st.min = merged;
            } else {
                st.max = merged;
            }
        }
        Ok(())
    }

    /// partial 分片执行辅助：聚合列数注册（首批前调用）。
    pub fn set_arity(&mut self, n: usize) {
        self.pending_arity = Some(n);
        for g in &mut self.groups {
            g.states = vec![AggState::default(); n];
        }
    }

    pub fn arity(&self) -> usize {
        self.pending_arity.unwrap_or(0)
    }

    pub fn len(&self) -> usize {
        self.groups.len()
    }
}

/// 全部分组列均为非空 Int64 时返回其值副本（否则 None）。
/// 空分组列表（无 GROUP BY）返回 None——走通用路径（单一全局组）。
fn int64_group_columns(arrays: &[ArrayRef]) -> Option<Vec<Vec<i64>>> {
    if arrays.is_empty() {
        return None;
    }
    let mut out = Vec::with_capacity(arrays.len());
    for a in arrays {
        let arr = a.as_any().downcast_ref::<Int64Array>()?;
        if arr.null_count() > 0 {
            return None;
        }
        out.push(arr.values().to_vec());
    }
    Some(out)
}

fn downcast_ints(a: &dyn Array) -> Vec<i64> {
    use arrow::datatypes::*;
    match a.data_type() {
        DataType::Int8 => a
            .as_any()
            .downcast_ref::<arrow::array::Int8Array>()
            .unwrap()
            .iter()
            .map(|v| i64::from(v.unwrap_or(0)))
            .collect(),
        DataType::Int16 => a
            .as_any()
            .downcast_ref::<arrow::array::Int16Array>()
            .unwrap()
            .iter()
            .map(|v| i64::from(v.unwrap_or(0)))
            .collect(),
        DataType::Int32 => a
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap()
            .iter()
            .map(|v| i64::from(v.unwrap_or(0)))
            .collect(),
        _ => a
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .map(|v| v.unwrap_or(0))
            .collect(),
    }
}

// ---------------------------------------------------------------------------
// finalize
// ---------------------------------------------------------------------------

/// 组键 + 状态 → 输出批。
pub fn finalize(
    keys: &[Vec<Literal>],
    states: &[Vec<AggState>],
    aggs: &[AggExpr],
    group: &[PhysExpr],
    schema: &arrow::datatypes::SchemaRef,
) -> Result<RecordBatch> {
    let _n = keys.len();
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(group.len() + aggs.len());
    // group 列
    for (gi, g) in group.iter().enumerate() {
        let lits: Vec<&Literal> = keys.iter().map(|k| &k[gi]).collect();
        cols.push(literals_to_array(&lits, &g.data_type())?);
    }
    // agg 列
    for (ai, agg) in aggs.iter().enumerate() {
        let arr = finalize_agg(states.iter().map(|s| &s[ai]), agg)?;
        cols.push(arr);
    }
    RecordBatch::try_new(Arc::clone(schema), cols)
        .map_err(|e| QsdbError::invalid(format!("agg output: {e}")))
}

fn finalize_agg<'a>(states: impl Iterator<Item = &'a AggState>, agg: &AggExpr) -> Result<ArrayRef> {
    use crate::expr::AggFunc::*;
    match agg.func {
        Count => {
            let out: arrow::array::Int64Array = states.map(|s| Some(s.count as i64)).collect();
            Ok(Arc::new(out))
        }
        Sum => match agg.out_ty {
            LogicalType::Int64 => {
                let out: arrow::array::Int64Array = states
                    .map(|s| match s.sum {
                        SumState::Int(v) => i64::try_from(v).ok(),
                        _ => None,
                    })
                    .collect();
                Ok(Arc::new(out))
            }
            LogicalType::Float64 => {
                let out: Float64Array = states
                    .map(|s| match s.sum {
                        SumState::Flt(v, _) => Some(v),
                        _ => None,
                    })
                    .collect();
                Ok(Arc::new(out))
            }
            LogicalType::Decimal(m) => {
                let out: Decimal128Array = states
                    .map(|s| match s.sum {
                        SumState::Dec(v) => v.to_i128(),
                        _ => None,
                    })
                    .collect();
                Ok(Arc::new(out.with_data_type(DataType::Decimal128(
                    m.precision,
                    m.scale,
                ))))
            }
            _ => Err(QsdbError::type_err("bad sum output type")),
        },
        Avg => match agg.out_ty {
            LogicalType::Float64 => {
                let out: Float64Array = states
                    .map(|s| match s.sum {
                        SumState::Flt(v, _) if s.count > 0 => Some(v / s.count as f64),
                        SumState::Int(v) if s.count > 0 => Some(v as f64 / s.count as f64),
                        _ => None,
                    })
                    .collect();
                Ok(Arc::new(out))
            }
            LogicalType::Decimal(m) => {
                // HALF_UP(sum / count) 到 avg_result scale
                let out: Decimal128Array = states
                    .map(|s| match s.sum {
                        SumState::Dec(v) if s.count > 0 => {
                            let cnt = i256::from_i128(s.count as i128);
                            let q = v / cnt;
                            let rem = v % cnt;
                            // HALF_UP
                            let two = i256::from_i128(2);
                            let round_up = rem != i256::from_i128(0)
                                && rem.wrapping_mul(two).wrapping_abs() >= cnt;
                            let adj = if round_up {
                                if v >= i256::from_i128(0) {
                                    q + i256::from_i128(1)
                                } else {
                                    q - i256::from_i128(1)
                                }
                            } else {
                                q
                            };
                            adj.to_i128()
                        }
                        _ => None,
                    })
                    .collect();
                Ok(Arc::new(out.with_data_type(DataType::Decimal128(
                    m.precision,
                    m.scale,
                ))))
            }
            _ => Err(QsdbError::type_err("bad avg output type")),
        },
        Min | Max => {
            let is_min = agg.func == Min;
            let lits: Vec<&Literal> = states
                .map(|s| {
                    if is_min {
                        s.min.as_ref()
                    } else {
                        s.max.as_ref()
                    }
                    .unwrap_or(&Literal::Null)
                })
                .collect();
            let ty = agg
                .arg
                .as_ref()
                .map(|e| e.data_type())
                .unwrap_or(LogicalType::Int64);
            literals_to_array(&lits, &ty)
        }
    }
}

/// Literal 列表 → arrow 数组。
pub fn literals_to_array(lits: &[&Literal], ty: &LogicalType) -> Result<ArrayRef> {
    let dt = ty.arrow_type();
    let mut one_by_one: Vec<ArrayRef> = Vec::with_capacity(lits.len());
    for l in lits {
        let one = match l {
            Literal::Null => arrow::array::new_null_array(&dt, 1),
            lit => {
                let arr = crate::expr::eval::literal_array(lit)?;
                // 与目标类型一致化（decimal/tz 差异）
                if arr.data_type() != &dt {
                    arrow::compute::cast_with_options(
                        arr.as_ref(),
                        &dt,
                        &arrow::compute::CastOptions::default(),
                    )?
                } else {
                    arr
                }
            }
        };
        one_by_one.push(one);
    }
    if one_by_one.is_empty() {
        return Ok(arrow::array::new_empty_array(&dt));
    }
    // interleave 拼接
    let refs: Vec<&dyn Array> = one_by_one.iter().map(|a| a.as_ref()).collect();
    let indices: Vec<(usize, usize)> = (0..lits.len()).map(|i| (i, 0)).collect();
    arrow::compute::interleave(&refs, &indices)
        .map_err(|e| QsdbError::invalid(format!("interleave literals: {e}")))
}

/// Distinct 输出（group keys → 全列批）。
pub fn build_output_batch(
    keys: &[Vec<Literal>],
    _aggs: &[AggExpr],
    schema: &arrow::datatypes::SchemaRef,
) -> Result<RecordBatch> {
    let n = schema.fields().len();
    let mut cols = Vec::with_capacity(n);
    for c in 0..n {
        let lits: Vec<&Literal> = keys.iter().map(|k| &k[c]).collect();
        let ty =
            LogicalType::try_from_arrow(schema.field(c).data_type()).unwrap_or(LogicalType::Int64);
        cols.push(literals_to_array(&lits, &ty)?);
    }
    RecordBatch::try_new(Arc::clone(schema), cols)
        .map_err(|e| QsdbError::invalid(format!("distinct output: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fxhash_basic() {
        assert_ne!(fxhash_bytes(b"abc"), fxhash_bytes(b"abd"));
        assert_eq!(fxhash_bytes(b"abc"), fxhash_bytes(b"abc"));
        // 非 8 倍长
        assert_ne!(fxhash_bytes(b"abcdefghijklm"), 0);
    }
}
