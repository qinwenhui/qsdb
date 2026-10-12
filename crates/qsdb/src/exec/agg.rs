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
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, StringArray, Time64NanosecondArray,
    TimestampNanosecondArray,
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
pub(crate) fn encode_key_row(arrays: &[ArrayRef], row: usize, buf: &mut Vec<u8>) {
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

/// 快路径比较：现有 Literal（同型）与新原生值比较。
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

fn combine_min(a: Option<Literal>, b: Option<Literal>) -> Option<Literal> {
    pick(a, b, true)
}
fn combine_max(a: Option<Literal>, b: Option<Literal>) -> Option<Literal> {
    pick(a, b, false)
}

/// 类型化切片求和进 i128 累加器：null_count=0 时 `values()` 切片直加
/// （免逐行判空，LLVM 自动向量化）；含 NULL 退回判空循环。
fn sum_slice<T: arrow::datatypes::ArrowPrimitiveType>(
    acc: &mut [i128],
    arr: &arrow::array::PrimitiveArray<T>,
) where
    i128: From<T::Native>,
{
    let a = &mut acc[0];
    if arr.null_count() == 0 {
        for v in arr.values() {
            *a += i128::from(*v);
        }
    } else {
        for i in 0..arr.len() {
            if !arr.is_null(i) {
                *a += i128::from(arr.value(i));
            }
        }
    }
}

/// Kahan 补偿求和（迭代器形态；f32 上游转 f64）。
fn kahan_slice(it: impl Iterator<Item = f64>, out: &mut (f64, f64)) {
    for x in it {
        let y = x - out.1;
        let t = out.0 + y;
        out.1 = (t - out.0) - y;
        out.0 = t;
    }
}

/// typed 数组 SIMD 最小值 → Literal（忽略 NULL；形态由调用方验证）。
fn typed_scalar_min(a: &dyn arrow::array::Array) -> Option<crate::types::Literal> {
    use crate::types::Literal;
    use arrow::compute::min;
    macro_rules! m {
        ($t:ident, $lit:expr) => {{ min(a.as_any().downcast_ref::<$t>().unwrap()).map($lit) }};
    }
    match a.data_type() {
        DataType::Int8 => m!(Int8Array, Literal::Int8),
        DataType::Int16 => m!(Int16Array, Literal::Int16),
        DataType::Int32 => m!(Int32Array, Literal::Int32),
        DataType::Int64 => m!(Int64Array, Literal::Int64),
        DataType::Float32 => m!(Float32Array, Literal::Float32),
        DataType::Float64 => m!(Float64Array, Literal::Float64),
        DataType::Date32 => m!(Date32Array, Literal::Date),
        _ => None,
    }
}

/// typed 数组 SIMD 最大值 → Literal。
fn typed_scalar_max(a: &dyn arrow::array::Array) -> Option<crate::types::Literal> {
    use crate::types::Literal;
    use arrow::compute::max;
    macro_rules! m {
        ($t:ident, $lit:expr) => {{ max(a.as_any().downcast_ref::<$t>().unwrap()).map($lit) }};
    }
    match a.data_type() {
        DataType::Int8 => m!(Int8Array, Literal::Int8),
        DataType::Int16 => m!(Int16Array, Literal::Int16),
        DataType::Int32 => m!(Int32Array, Literal::Int32),
        DataType::Int64 => m!(Int64Array, Literal::Int64),
        DataType::Float32 => m!(Float32Array, Literal::Float32),
        DataType::Float64 => m!(Float64Array, Literal::Float64),
        DataType::Date32 => m!(Date32Array, Literal::Date),
        _ => None,
    }
}
/// 数值累加列（sum/avg 的 sum 部分；按输入类型选一种，SoA 稠密向量）。
enum SumCol {
    /// 非数值聚合列（count-only / min-max）不占状态
    None,
    /// 整型族 i128 累加（16B/组）
    Int(Vec<i128>),
    /// 浮点 Kahan 补偿 (sum, c)（16B/组）
    Flt(Vec<(f64, f64)>),
    /// Decimal i256 精确（32B/组）
    Dec(Vec<i256>),
}

/// 分组聚合表（SoA：key 单一 arena + 偏移，聚合状态按列稠密——
/// 百万级组数时散布步长 8~40B，不再逐组堆分配；P3 性能重做）。
#[derive(Default)]

pub struct GroupTable {
    by_hash: HashMap<u64, Vec<u32>, crate::exec::join::BuildI64Hasher>,
    pending_arity: Option<usize>,
    /// Int64 fast path 的持久化开放寻址索引（(hash, gid)；跨批存活）
    fast_slots: Vec<(u64, u32)>,
    /// 单 Int64 分组列专用：槽内直存键值 (key, gid)——免 key_arena 二次
    /// 随机访问（高基数下每次命中省一次 cache miss）与每行 key_bytes 构造。
    /// 与 fast_slots 互斥（按列数选用），merge 后一并清空。
    fast_slots_inline: Vec<(i64, u32)>,
    // —— 组数据（SoA）——
    /// 全部组 key 的单一 arena（fast 格式定长内联 / 慢路径 tag+bytes 变长）
    key_arena: Vec<u8>,
    /// key 偏移（len = 组数+1；组数 = len-1）
    key_off: Vec<u32>,
    /// 组键 literal（[分组列][gid]，建组时按 gid 顺序追加）
    group_lits: Vec<Vec<Literal>>,
    /// 聚合列状态种类（首批 ingest 初始化）
    kinds: Vec<crate::expr::AggFunc>,
    /// Count 值 / Sum·Avg 的非空计数
    counts: Vec<Vec<u64>>,
    sums: Vec<SumCol>,
    /// Min/Max 值（仅 Min/Max 列分配）
    minmax: Vec<Vec<Option<Literal>>>,
}

impl GroupTable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.key_off.is_empty()
    }

    pub fn len(&self) -> usize {
        self.key_off.len().saturating_sub(1)
    }

    /// 组 g 的 key 字节。
    #[inline]
    fn key_of(&self, g: usize) -> &[u8] {
        &self.key_arena[self.key_off[g] as usize..self.key_off[g + 1] as usize]
    }

    /// 追加一个新组（key 入 arena，键 literal 与各聚合列状态按 gid 对齐扩展）。
    /// 新建组。`lits` 按引用传入——调用方用栈数组（避免每组一次 Vec 堆分配：
    /// 高基数 89 万组时实测是主要开销）。
    fn push_group(&mut self, key: &[u8], lits: &[Literal]) -> u32 {
        if self.key_off.is_empty() {
            self.key_off.push(0); // 首组前压起点哨兵
        }
        let gid = self.len() as u32;
        self.key_off.push((self.key_arena.len() + key.len()) as u32);
        self.key_arena.extend_from_slice(key);
        for (c, l) in self.group_lits.iter_mut().zip(lits.iter()) {
            c.push(l.clone());
        }
        for c in &mut self.counts {
            c.push(0);
        }
        for s in &mut self.sums {
            match s {
                SumCol::None => {}
                SumCol::Int(v) => v.push(0),
                SumCol::Flt(v) => v.push((0.0, 0.0)),
                SumCol::Dec(v) => v.push(i256::from_i128(0)),
            }
        }
        for m in &mut self.minmax {
            m.push(None);
        }
        gid
    }

    /// 首批初始化：分组列数 + 各聚合列状态种类。
    fn init_cols(&mut self, group: &[PhysExpr], aggs: &[AggExpr], arrays: &[Option<ArrayRef>]) {
        if !self.kinds.is_empty() {
            return;
        }
        if self.group_lits.is_empty() {
            self.group_lits = vec![Vec::new(); group.len()];
        }
        self.kinds = aggs.iter().map(|a| a.func).collect();
        for (ai, agg) in aggs.iter().enumerate() {
            let dt = arrays[ai]
                .as_ref()
                .map(|a| a.data_type().clone())
                .unwrap_or(DataType::Int64);
            use crate::expr::AggFunc::*;
            match agg.func {
                Count | Udaf => {
                    self.counts.push(Vec::new());
                    self.sums.push(SumCol::None);
                    self.minmax.push(Vec::new());
                }
                Sum | Avg => {
                    self.counts.push(Vec::new());
                    self.sums.push(match dt {
                        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                            SumCol::Int(Vec::new())
                        }
                        DataType::Float32 | DataType::Float64 => SumCol::Flt(Vec::new()),
                        DataType::Decimal128(_, _) => SumCol::Dec(Vec::new()),
                        _ => SumCol::None,
                    });
                    self.minmax.push(Vec::new());
                }
                Min | Max => {
                    self.counts.push(Vec::new());
                    self.sums.push(SumCol::None);
                    self.minmax.push(Vec::new());
                }
            }
        }
    }

    /// 全局聚合（无 GROUP BY）零迭代快路径——「缓存聚合 0.04x」的底座修复：
    /// `count(*)` = `num_rows`（**O(1)**）、`count(col)` = `num_rows − null_count`
    /// （**O(1)**）、整型 sum = 类型化 `values()` 切片直加（null_count=0 免逐行
    /// 判空，自动向量化；i128 累加无溢出）、float sum = 切片 Kahan、
    /// min/max = typed 数组 SIMD `min()`/`max()`。
    /// 逐批成本从 ~6ns/行 降到 ~0.3ns/行 + O(1) 计数。
    /// 返回 Ok(false) = 形态不支持（零状态变更，调用方回退原路径）。
    fn try_global_fast(
        &mut self,
        aggs: &[AggExpr],
        arrays: &[Option<ArrayRef>],
        n: usize,
    ) -> Result<bool> {
        use crate::expr::AggFunc::*;
        // 先验证全形态可加速（不变更状态）
        for (ai, agg) in aggs.iter().enumerate() {
            match agg.func {
                Count => {}
                Sum | Avg => {
                    let Some(a) = &arrays[ai] else {
                        return Ok(false);
                    };
                    let ok = matches!(
                        (&self.sums[ai], a.data_type()),
                        (
                            SumCol::Int(_),
                            DataType::Int8
                                | DataType::Int16
                                | DataType::Int32
                                | DataType::Int64
                                | DataType::Date32
                        ) | (SumCol::Flt(_), DataType::Float32 | DataType::Float64)
                            | (SumCol::Dec(_), DataType::Decimal128(_, _))
                    );
                    if !ok {
                        return Ok(false);
                    }
                }
                Min | Max => {
                    let Some(a) = &arrays[ai] else {
                        return Ok(false);
                    };
                    let ok = matches!(
                        a.data_type(),
                        DataType::Int8
                            | DataType::Int16
                            | DataType::Int32
                            | DataType::Int64
                            | DataType::Float32
                            | DataType::Float64
                            | DataType::Date32
                    );
                    if !ok {
                        return Ok(false);
                    }
                }
                Udaf => return Ok(false),
            }
        }
        // 应用（全局唯一组 gid 0；首批建空键组）
        if self.is_empty() {
            self.push_group(&[], &[]);
        }
        for (ai, agg) in aggs.iter().enumerate() {
            let arr = arrays[ai].as_deref();
            match agg.func {
                Count => {
                    let add = match arr {
                        None => n,
                        Some(a) => n - a.null_count(),
                    };
                    self.counts[ai][0] += add as u64;
                }
                Sum | Avg => {
                    let a = arr.unwrap();
                    self.counts[ai][0] += (n - a.null_count()) as u64;
                    match (&mut self.sums[ai], a.data_type()) {
                        (SumCol::Int(acc), DataType::Int8) => {
                            sum_slice(acc, a.as_any().downcast_ref::<Int8Array>().unwrap())
                        }
                        (SumCol::Int(acc), DataType::Int16) => {
                            sum_slice(acc, a.as_any().downcast_ref::<Int16Array>().unwrap())
                        }
                        (SumCol::Int(acc), DataType::Int32) => {
                            sum_slice(acc, a.as_any().downcast_ref::<Int32Array>().unwrap())
                        }
                        (SumCol::Int(acc), DataType::Int64) => {
                            sum_slice(acc, a.as_any().downcast_ref::<Int64Array>().unwrap())
                        }
                        (SumCol::Int(acc), DataType::Date32) => {
                            sum_slice(acc, a.as_any().downcast_ref::<Date32Array>().unwrap())
                        }
                        (SumCol::Flt(acc), DataType::Float64) => {
                            let f = a.as_any().downcast_ref::<Float64Array>().unwrap();
                            if f.null_count() == 0 {
                                kahan_slice(f.values().iter().copied(), &mut acc[0])
                            } else {
                                kahan_slice(
                                    (0..n).filter(|&i| !f.is_null(i)).map(|i| f.value(i)),
                                    &mut acc[0],
                                )
                            }
                        }
                        (SumCol::Flt(acc), DataType::Float32) => {
                            let f = a.as_any().downcast_ref::<Float32Array>().unwrap();
                            kahan_slice(
                                (0..n)
                                    .filter(|&i| !f.is_null(i))
                                    .map(|i| f64::from(f.value(i))),
                                &mut acc[0],
                            )
                        }
                        (SumCol::Dec(acc), DataType::Decimal128(_, _)) => {
                            let d = a.as_any().downcast_ref::<Decimal128Array>().unwrap();
                            for i in 0..n {
                                if !d.is_null(i) {
                                    acc[0] += i256::from_i128(d.value(i));
                                }
                            }
                        }
                        _ => unreachable!("validated above"),
                    }
                }
                Min | Max => {
                    let a = arr.unwrap();
                    let slot = &mut self.minmax[ai][0];
                    // 单一槽位只存该聚合自己的极值（min 聚合取 min、max 取 max）
                    let v = if agg.func == Min {
                        typed_scalar_min(a)
                    } else {
                        typed_scalar_max(a)
                    };
                    if let Some(v) = v {
                        *slot = crate::write::combine_lit(slot.take(), Some(v), agg.func == Min);
                    }
                }
                Udaf => unreachable!("validated above"),
            }
        }
        Ok(true)
    }

    /// 掩码摄入（semi/anti + 聚合融合路径）：只处理 `sel` 列出的行，
    /// 全程零中间物化——semi 条件直接变行选择喂聚合内核。
    pub fn ingest_with_sel(
        &mut self,
        group: &[PhysExpr],
        aggs: &[AggExpr],
        batch: &RecordBatch,
        sel: &[u32],
    ) -> Result<()> {
        if sel.is_empty() {
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
        self.init_cols(group, aggs, &agg_arrays);

        // 全选 + 全局聚合 → 零迭代快路径（O(1) 计数 + SIMD 列和）
        if group.is_empty()
            && sel.len() == batch.num_rows()
            && self.try_global_fast(aggs, &agg_arrays, batch.num_rows())?
        {
            return Ok(());
        }

        // 行 → 组 id（仅选中行；group_id 走通用路径——fast 槽表在其内部
        // 维护，与 ingest 全量路径共用同一张探测表，跨批一致）
        let mut group_ids: Vec<u32> = Vec::with_capacity(sel.len());
        {
            let mut key_buf: Vec<u8> = Vec::with_capacity(64);
            for &s in sel {
                encode_key_row(&group_arrays, s as usize, &mut key_buf);
                let gid = self.group_id(&key_buf, &group_arrays, s as usize);
                group_ids.push(gid);
            }
        }
        for (ai, agg) in aggs.iter().enumerate() {
            self.update_aggs_sel(ai, agg, agg_arrays[ai].as_deref(), &group_ids, sel)?;
        }
        Ok(())
    }

    /// update_aggs 的选择向量变体（数组取值按 sel 间接；group_ids 与 sel 等长）。
    fn update_aggs_sel(
        &mut self,
        ai: usize,
        agg: &AggExpr,
        arr: Option<&dyn Array>,
        group_ids: &[u32],
        sel: &[u32],
    ) -> Result<()> {
        use crate::expr::AggFunc::*;
        match agg.func {
            Count => {
                let col = &mut self.counts[ai];
                match arr {
                    None => {
                        for &g in group_ids {
                            col[g as usize] += 1;
                        }
                    }
                    Some(a) => {
                        for (&s, &g) in sel.iter().zip(group_ids.iter()) {
                            if !a.is_null(s as usize) {
                                col[g as usize] += 1;
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
                self.update_sum_sel(ai, a, group_ids, sel)
            }
            Min | Max => {
                let a = arr.ok_or_else(|| {
                    QsdbError::invalid(format!("{} requires an argument", agg.func.name()))
                })?;
                self.update_min_max_sel(ai, agg.func == Min, a, group_ids, sel)
            }
            Udaf => Ok(()),
        }
    }

    /// update_sum 的选择向量变体：零拷贝类型化迭代 + sel 行间接。
    fn update_sum_sel(
        &mut self,
        ai: usize,
        a: &dyn Array,
        group_ids: &[u32],
        sel: &[u32],
    ) -> Result<()> {
        let dt = a.data_type().clone();
        match dt {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                let cnt = &mut self.counts[ai];
                let SumCol::Int(acc) = &mut self.sums[ai] else {
                    return Err(QsdbError::type_err("sum column kind mismatch (internal)"));
                };
                let v8 = a.as_any().downcast_ref::<arrow::array::Int8Array>();
                let v16 = a.as_any().downcast_ref::<arrow::array::Int16Array>();
                let v32 = a.as_any().downcast_ref::<arrow::array::Int32Array>();
                let v64 = a.as_any().downcast_ref::<Int64Array>();
                for (&s, &g) in sel.iter().zip(group_ids.iter()) {
                    let row = s as usize;
                    let val = if let Some(x) = v64 {
                        (!x.is_null(row)).then(|| i128::from(x.value(row)))
                    } else if let Some(x) = v32 {
                        (!x.is_null(row)).then(|| i128::from(x.value(row)))
                    } else if let Some(x) = v16 {
                        (!x.is_null(row)).then(|| i128::from(x.value(row)))
                    } else if let Some(x) = v8 {
                        (!x.is_null(row)).then(|| i128::from(x.value(row)))
                    } else {
                        None
                    };
                    if let Some(v) = val {
                        let g = g as usize;
                        acc[g] += v;
                        cnt[g] += 1;
                    }
                }
            }
            DataType::Float64 => {
                let f = a.as_any().downcast_ref::<Float64Array>().unwrap();
                let cnt = &mut self.counts[ai];
                let SumCol::Flt(acc) = &mut self.sums[ai] else {
                    return Err(QsdbError::type_err("sum column kind mismatch (internal)"));
                };
                for (&s, &g) in sel.iter().zip(group_ids.iter()) {
                    let row = s as usize;
                    if !f.is_null(row) {
                        let v = f.value(row);
                        let g = g as usize;
                        let sm = &mut acc[g];
                        let y = v - sm.1;
                        let t = sm.0 + y;
                        sm.1 = (t - sm.0) - y;
                        sm.0 = t;
                        cnt[g] += 1;
                    }
                }
            }
            DataType::Decimal128(_, _) => {
                let d = a.as_any().downcast_ref::<Decimal128Array>().unwrap();
                let cnt = &mut self.counts[ai];
                let SumCol::Dec(acc) = &mut self.sums[ai] else {
                    return Err(QsdbError::type_err("sum column kind mismatch (internal)"));
                };
                for (&s, &g) in sel.iter().zip(group_ids.iter()) {
                    let row = s as usize;
                    if !d.is_null(row) {
                        let g = g as usize;
                        acc[g] += i256::from_i128(d.value(row));
                        cnt[g] += 1;
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

    /// update_min_max 的选择向量变体（typed 快路径同款，行取值按 sel）。
    fn update_min_max_sel(
        &mut self,
        ai: usize,
        is_min: bool,
        a: &dyn Array,
        group_ids: &[u32],
        sel: &[u32],
    ) -> Result<()> {
        use arrow::datatypes::TimeUnit;
        let col = &mut self.minmax[ai];
        macro_rules! fast {
            ($ty:ty, $make:expr) => {{
                let arr = a.as_any().downcast_ref::<$ty>().unwrap();
                for (&s, &g) in sel.iter().zip(group_ids.iter()) {
                    let row = s as usize;
                    if arr.is_null(row) {
                        continue;
                    }
                    let v = arr.value(row);
                    let slot = &mut col[g as usize];
                    match slot {
                        None => *slot = Some($make(v)),
                        Some(cur) => {
                            let cmp = cur.partial_cmp_literal(&$make(v));
                            if let Ok(Some(o)) = cmp {
                                let replace = if is_min {
                                    o == std::cmp::Ordering::Greater
                                } else {
                                    o == std::cmp::Ordering::Less
                                };
                                if replace {
                                    *slot = Some($make(v));
                                }
                            }
                        }
                    }
                }
            }};
        }
        match a.data_type() {
            arrow::datatypes::DataType::Int64 => fast!(Int64Array, Literal::Int64),
            arrow::datatypes::DataType::Float64 => fast!(Float64Array, Literal::Float64),
            arrow::datatypes::DataType::Date32 => fast!(Date32Array, Literal::Date),
            arrow::datatypes::DataType::Utf8 => {
                fast!(StringArray, |v: &str| { Literal::Utf8(v.to_string()) })
            }
            arrow::datatypes::DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                fast!(TimestampNanosecondArray, |v: i64| Literal::Timestamp(v))
            }
            _ => {
                return Err(QsdbError::unsupported(format!(
                    "min/max on {:?} in fused path not supported",
                    a.data_type()
                )));
            }
        }
        Ok(())
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
        static BATCHES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        static ROWS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        static MAXB: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        static MINB: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(u64::MAX);
        BATCHES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        ROWS.fetch_add(n as u64, std::sync::atomic::Ordering::Relaxed);
        MAXB.fetch_max(n as u64, std::sync::atomic::Ordering::Relaxed);
        MINB.fetch_min(n as u64, std::sync::atomic::Ordering::Relaxed);
        if std::env::var("QSDB_AGG_TIMING").is_ok()
            && BATCHES.load(std::sync::atomic::Ordering::Relaxed) % 500 == 0
        {
            eprintln!(
                "[agg-timing] batches={} rows={} min={} max={}",
                BATCHES.load(std::sync::atomic::Ordering::Relaxed),
                ROWS.load(std::sync::atomic::Ordering::Relaxed),
                MINB.load(std::sync::atomic::Ordering::Relaxed),
                MAXB.load(std::sync::atomic::Ordering::Relaxed)
            );
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
        let fast_cols = int64_group_columns(&group_arrays);

        self.init_cols(group, aggs, &agg_arrays);

        // 全局聚合零迭代快路径（O(1) 计数 + SIMD 列和；形态不中则原路径）
        if group.is_empty() && self.try_global_fast(aggs, &agg_arrays, n)? {
            return Ok(());
        }

        // 行 → 组 id
        // fast path：分组列全为非空 Int64 —— 内联 key 拼接 + 开放寻址表
        let mut group_ids: Vec<u32> = Vec::with_capacity(n);
        if let Some(cols) = fast_cols {
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

    /// 单 Int64 键内联槽探测：槽 = (key, gid)，命中判定只比槽内键，
    /// 无 key_arena 访问、无每行字节缓冲构造。
    fn assign_groups_fast_inline(&mut self, col: &Int64Array, out: &mut Vec<u32>) {
        const EMPTY: u32 = u32::MAX;
        let n = col.len();
        if self.fast_slots_inline.is_empty() {
            let mut cap: usize = 16;
            while cap < (self.len() + n + 1) * 2 {
                cap <<= 1;
            }
            self.fast_slots_inline = vec![(0, EMPTY); cap];
            // 已有组回填（merge 后继续 ingest 的组键完整性——M4-1 教训）
            for gid in 0..self.len() {
                let k = match self.group_lits.first().and_then(|c| c.get(gid)) {
                    Some(crate::types::Literal::Int64(v)) => *v,
                    _ => continue,
                };
                let h = crate::exec::agg::fxhash_bytes(&k.to_le_bytes()) | 1;
                Self::insert_slot_inline(&mut self.fast_slots_inline, h, k, gid as u32);
            }
        }
        for row in 0..n {
            let k = col.value(row);
            let h = crate::exec::agg::fxhash_bytes(&k.to_le_bytes()) | 1;
            let mask = self.fast_slots_inline.len() - 1;
            let mut idx = (h as usize) & mask;
            loop {
                let (sk, sg) = self.fast_slots_inline[idx];
                if sg == EMPTY {
                    let gid = self.push_group(&k.to_le_bytes(), &[crate::types::Literal::Int64(k)]);
                    self.fast_slots_inline[idx] = (k, gid);
                    if self.len() * 2 >= self.fast_slots_inline.len() {
                        let mut cap = self.fast_slots_inline.len() * 2;
                        while cap < (self.len() + 1) * 2 {
                            cap <<= 1;
                        }
                        let old = std::mem::take(&mut self.fast_slots_inline);
                        self.fast_slots_inline = vec![(0, EMPTY); cap];
                        for (ok, og) in old {
                            if og != EMPTY {
                                let oh = crate::exec::agg::fxhash_bytes(&ok.to_le_bytes()) | 1;
                                Self::insert_slot_inline(&mut self.fast_slots_inline, oh, ok, og);
                            }
                        }
                    }
                    out.push(gid);
                    break;
                }
                if sk == k {
                    out.push(sg);
                    break;
                }
                idx = (idx + 1) & mask;
            }
        }
    }

    fn insert_slot_inline(slots: &mut [(i64, u32)], h: u64, k: i64, gid: u32) {
        let mask = slots.len() - 1;
        let mut idx = (h as usize) & mask;
        while slots[idx].1 != u32::MAX {
            idx = (idx + 1) & mask;
        }
        slots[idx] = (k, gid);
    }

    /// Int64 分组列特化：key = 列值序列直接内联，开放寻址探测。
    ///
    /// 探测表（fast_slots）是 GroupTable 的持久化字段——跨批存活，
    /// 否则每批重建会丢失已建组索引导致组键分裂（M4-1 bug）。
    fn assign_groups_fast(&mut self, cols: &[&Int64Array], out: &mut Vec<u32>) {
        // 单列 → 内联槽快路径（高基数 group by 的主形态）
        if cols.len() == 1 {
            self.assign_groups_fast_inline(cols[0], out);
            return;
        }
        const EMPTY: u32 = u32::MAX;
        let n = cols.first().map(|c| c.len()).unwrap_or(0);
        if self.fast_slots.is_empty() {
            // 初始容量：2×当前组数与合理下限取大（留扩容余量）
            let mut cap: usize = 16;
            while cap < (self.len() + n + 1) * 2 {
                cap <<= 1;
            }
            self.fast_slots = vec![(0, EMPTY); cap];
            // 已有组（如 merge 后继续 ingest）回填索引
            for gid in 0..self.len() {
                let h = fxhash_bytes(self.key_of(gid)) | 1;
                Self::insert_slot(&mut self.fast_slots, h, gid as u32);
            }
        }
        let mut key_bytes: Vec<u8> = Vec::with_capacity(cols.len() * 8);

        for row in 0..n {
            key_bytes.clear();
            for c in cols {
                key_bytes.extend_from_slice(&c.value(row).to_le_bytes());
            }
            let h = fxhash_bytes(&key_bytes) | 1;
            let mut idx = (h as usize) & (self.fast_slots.len() - 1);
            let gid;
            loop {
                let slot = self.fast_slots[idx];
                if slot.1 == EMPTY {
                    let lits: Vec<Literal> =
                        cols.iter().map(|c| Literal::Int64(c.value(row))).collect();
                    gid = self.push_group(&key_bytes, &lits);
                    self.fast_slots[idx] = (h, gid);
                    // 负载因子 >= 50% 扩容重排
                    if self.len() * 2 >= self.fast_slots.len() {
                        let mut cap = self.fast_slots.len() * 2;
                        while cap < (self.len() + 1) * 2 {
                            cap <<= 1;
                        }
                        let old = std::mem::take(&mut self.fast_slots);
                        self.fast_slots = vec![(0, EMPTY); cap];
                        for (oh, og) in old {
                            if og != EMPTY {
                                Self::insert_slot(&mut self.fast_slots, oh, og);
                            }
                        }
                    }
                    break;
                }
                if slot.0 == h && self.key_of(slot.1 as usize) == key_bytes.as_slice() {
                    gid = slot.1;
                    break;
                }
                idx = (idx + 1) & (self.fast_slots.len() - 1);
            }
            out.push(gid);
        }
    }

    #[inline]
    fn insert_slot(slots: &mut [(u64, u32)], h: u64, gid: u32) {
        let mask = slots.len() - 1;
        let mut idx = (h as usize) & mask;
        while slots[idx].1 != u32::MAX {
            idx = (idx + 1) & mask;
        }
        slots[idx] = (h, gid);
    }

    fn group_id(&mut self, key: &[u8], arrays: &[ArrayRef], row: usize) -> u32 {
        let h = fxhash_bytes(key);
        if let Some(cands) = self.by_hash.get(&h) {
            for &gid in cands {
                if self.key_of(gid as usize) == key {
                    return gid;
                }
            }
        }
        // 新组
        let lits: Vec<Literal> = arrays
            .iter()
            .map(|a| array_value_to_literal(a, row))
            .collect();
        let gid = self.push_group(key, &lits);
        self.by_hash.entry(h).or_default().push(gid);
        gid
    }

    /// merge 另一张 partial 表（两表结构一致：同 group/aggs 语义）。
    /// merge 后 fast 索引置空（下次 fast ingest 时按 groups 惰性回填）。
    /// 空表首并直接整体接管（move，零拷贝）。
    pub fn merge(&mut self, other: GroupTable, agg_count: usize) {
        if self.is_empty() {
            *self = other;
            self.fast_slots.clear();
            self.fast_slots_inline.clear();
            return;
        }
        let other_len = other.len();
        self.merge_subset(&other, (0..other_len).collect(), agg_count);
    }

    /// merge 另一张表的指定组子集（并行分区 merge 用：调用方按 hash 把
    /// shard 组分桶，各桶独立合并）。
    pub fn merge_subset(&mut self, other: &GroupTable, ogs: Vec<usize>, agg_count: usize) {
        self.fast_slots.clear();
        self.fast_slots_inline.clear();
        // 高基数下本循环是热点（组数 × 分片数）：key/lits 全部缓冲复用，
        // 免每次跨表合并的两次堆分配（原实现 14M 次 malloc 是主要开销）
        let mut key_buf: Vec<u8> = Vec::with_capacity(64);
        let mut lits_buf: Vec<Literal> = Vec::new();
        for og in ogs {
            key_buf.clear();
            key_buf.extend_from_slice(other.key_of(og));
            let key: &[u8] = &key_buf;
            let h = fxhash_bytes(key);
            let gid = match self.by_hash.get(&h).and_then(|cands| {
                cands
                    .iter()
                    .copied()
                    .find(|&g| self.key_of(g as usize) == key)
            }) {
                Some(g) => g,
                None => {
                    // 列状态种类未初始化（空表首组）：从 other 继承结构
                    if self.kinds.is_empty() {
                        self.kinds = other.kinds.clone();
                        self.group_lits = other
                            .group_lits
                            .iter()
                            .map(|c| Vec::with_capacity(c.len()))
                            .collect();
                        for (ai, _k) in self.kinds.iter().enumerate() {
                            self.counts.push(Vec::new());
                            self.sums.push(match &other.sums[ai] {
                                SumCol::None => SumCol::None,
                                SumCol::Int(_) => SumCol::Int(Vec::new()),
                                SumCol::Flt(_) => SumCol::Flt(Vec::new()),
                                SumCol::Dec(_) => SumCol::Dec(Vec::new()),
                            });
                            self.minmax.push(Vec::new());
                        }
                    }
                    lits_buf.clear();
                    lits_buf.extend(other.group_lits.iter().map(|c| c[og].clone()));
                    let gid = self.push_group(key, &lits_buf);
                    self.by_hash.entry(h).or_default().push(gid);
                    gid
                }
            };
            let g = gid as usize;
            for ai in 0..agg_count {
                if let (Some(dst), Some(src)) = (self.counts.get_mut(ai), other.counts.get(ai)) {
                    dst[g] += src[og];
                }
                if let (Some(d), Some(s)) = (self.sums.get_mut(ai), other.sums.get(ai)) {
                    match (d, s) {
                        (SumCol::Int(d), SumCol::Int(s)) => d[g] += s[og],
                        (SumCol::Flt(d), SumCol::Flt(s)) => {
                            let t = d[g].0 + s[og].0;
                            d[g].1 += s[og].1 - (t - d[g].0 - s[og].0);
                            d[g].0 = t;
                        }
                        (SumCol::Dec(d), SumCol::Dec(s)) => d[g] += s[og],
                        _ => {}
                    }
                }
                if let (Some(dst), Some(src)) = (self.minmax.get_mut(ai), other.minmax.get(ai)) {
                    let cur = dst[g].take();
                    let merged = if self.kinds.get(ai) == Some(&crate::expr::AggFunc::Min) {
                        combine_min(cur, src[og].clone())
                    } else {
                        combine_max(cur, src[og].clone())
                    };
                    dst[g] = merged;
                }
            }
        }
    }

    /// 组 key 的 hash（分区 merge 分桶用）。
    pub fn group_hash(&self, g: usize) -> u64 {
        fxhash_bytes(self.key_of(g))
    }

    /// partial 分片执行辅助：聚合列数注册（首批前调用；列状态首批 ingest 落地）。
    pub fn set_arity(&mut self, n: usize) {
        self.pending_arity = Some(n);
    }

    pub fn arity(&self) -> usize {
        self.pending_arity.unwrap_or(0)
    }

    /// 输出用组键（[gid][分组列]）。
    pub fn group_keys(&self) -> Vec<Vec<Literal>> {
        let n = self.len();
        let mut by_row = vec![Vec::with_capacity(self.group_lits.len()); n];
        for gid in 0..n {
            for col in &self.group_lits {
                by_row[gid].push(col[gid].clone());
            }
        }
        by_row
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
                let col = &mut self.counts[ai];
                match arr {
                    None => {
                        // COUNT(*)：全部行
                        for &g in group_ids {
                            col[g as usize] += 1;
                        }
                    }
                    Some(a) => {
                        for (row, &g) in group_ids.iter().enumerate() {
                            if !a.is_null(row) {
                                col[g as usize] += 1;
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
            // UDAF 不走 GroupTable 状态路径（UdafAggExec 串行物化分组）
            Udaf => Ok(()),
        }
    }

    fn update_sum(&mut self, ai: usize, a: &dyn Array, group_ids: &[u32]) -> Result<()> {
        let dt = a.data_type().clone();
        match dt {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                // 直接迭代类型化数组（零拷贝；整列拷贝在此上下文实测病理级慢）
                let cnt = &mut self.counts[ai];
                let SumCol::Int(acc) = &mut self.sums[ai] else {
                    return Err(QsdbError::type_err("sum column kind mismatch (internal)"));
                };
                let v8 = a.as_any().downcast_ref::<arrow::array::Int8Array>();
                let v16 = a.as_any().downcast_ref::<arrow::array::Int16Array>();
                let v32 = a.as_any().downcast_ref::<arrow::array::Int32Array>();
                let v64 = a.as_any().downcast_ref::<Int64Array>();
                for (row, &g) in group_ids.iter().enumerate() {
                    let val = if let Some(x) = v64 {
                        if x.is_null(row) {
                            None
                        } else {
                            Some(i128::from(x.value(row)))
                        }
                    } else if let Some(x) = v32 {
                        if x.is_null(row) {
                            None
                        } else {
                            Some(i128::from(x.value(row)))
                        }
                    } else if let Some(x) = v16 {
                        if x.is_null(row) {
                            None
                        } else {
                            Some(i128::from(x.value(row)))
                        }
                    } else if let Some(x) = v8 {
                        if x.is_null(row) {
                            None
                        } else {
                            Some(i128::from(x.value(row)))
                        }
                    } else {
                        None
                    };
                    if let Some(v) = val {
                        let g = g as usize;
                        acc[g] += v;
                        cnt[g] += 1;
                    }
                }
            }
            DataType::Float64 => {
                let f = a.as_any().downcast_ref::<Float64Array>().unwrap();
                let cnt = &mut self.counts[ai];
                let SumCol::Flt(acc) = &mut self.sums[ai] else {
                    return Err(QsdbError::type_err("sum column kind mismatch (internal)"));
                };
                for (row, &g) in group_ids.iter().enumerate() {
                    if !f.is_null(row) {
                        let v = f.value(row);
                        let g = g as usize;
                        let s = &mut acc[g];
                        let y = v - s.1;
                        let t = s.0 + y;
                        s.1 = (t - s.0) - y;
                        s.0 = t;
                        cnt[g] += 1;
                    }
                }
            }
            DataType::Decimal128(_, _) => {
                let d = a.as_any().downcast_ref::<Decimal128Array>().unwrap();
                let cnt = &mut self.counts[ai];
                let SumCol::Dec(acc) = &mut self.sums[ai] else {
                    return Err(QsdbError::type_err("sum column kind mismatch (internal)"));
                };
                for (row, &g) in group_ids.iter().enumerate() {
                    if !d.is_null(row) {
                        let g = g as usize;
                        acc[g] += i256::from_i128(d.value(row));
                        cnt[g] += 1;
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
        use arrow::datatypes::TimeUnit;
        let col = &mut self.minmax[ai];
        // typed 快路径：直接以原生值比较，跳过逐行 Literal 构造
        macro_rules! fast {
            ($ty:ty, $make:expr) => {{
                let arr = a.as_any().downcast_ref::<$ty>().unwrap();
                for (row, &g) in group_ids.iter().enumerate() {
                    if arr.is_null(row) {
                        continue;
                    }
                    let v = arr.value(row);
                    let slot = &mut col[g as usize];
                    match slot {
                        None => *slot = Some($make(v)),
                        Some(cur) => match cur {
                            crate::types::Literal::Null => *slot = Some($make(v)),
                            _ => match cur.partial_cmp_literal(&$make(v)) {
                                Ok(Some(o)) => {
                                    let better = if is_min {
                                        o == std::cmp::Ordering::Greater
                                    } else {
                                        o == std::cmp::Ordering::Less
                                    };
                                    if better {
                                        *slot = Some($make(v));
                                    }
                                }
                                _ => {}
                            },
                        },
                    }
                }
                return Ok(());
            }};
        }
        match a.data_type() {
            DataType::Int64 => fast!(Int64Array, Literal::Int64),
            DataType::Float64 => fast!(Float64Array, Literal::Float64),
            DataType::Date32 => fast!(Date32Array, Literal::Date),
            DataType::Utf8 => fast!(StringArray, |v: &str| Literal::Utf8(v.to_string())),
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                fast!(TimestampNanosecondArray, Literal::Timestamp)
            }
            _ => {}
        }
        // 通用路径（其余类型）
        for (row, &g) in group_ids.iter().enumerate() {
            if a.is_null(row) {
                continue;
            }
            let v = array_value_to_literal(a, row);
            let cur = col[g as usize].take();
            col[g as usize] = if is_min {
                combine_min(cur, Some(v))
            } else {
                combine_max(cur, Some(v))
            };
        }
        Ok(())
    }
}

/// 全部分组列均为非空 Int64 时返回其值副本（否则 None）。
/// 空分组列表（无 GROUP BY）返回 None——走通用路径（单一全局组）。
/// 全部分组列为非空 Int64 时返回数组引用（零拷贝；value(i) 带 offset 语义）。
fn int64_group_columns(arrays: &[ArrayRef]) -> Option<Vec<&Int64Array>> {
    if arrays.is_empty() {
        return None;
    }
    let mut out = Vec::with_capacity(arrays.len());
    for a in arrays {
        let arr = a.as_any().downcast_ref::<Int64Array>()?;
        if arr.null_count() > 0 {
            return None;
        }
        out.push(arr);
    }
    Some(out)
}

// ---------------------------------------------------------------------------
// finalize
// ---------------------------------------------------------------------------

/// 组键 + 状态 → 输出批。
pub fn finalize_table(
    table: &GroupTable,
    aggs: &[AggExpr],
    group: &[PhysExpr],
    schema: &arrow::datatypes::SchemaRef,
) -> Result<RecordBatch> {
    let n = table.len();
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(group.len() + aggs.len());
    // group 列
    for (gi, g) in group.iter().enumerate() {
        let lits: Vec<&Literal> = (0..n).map(|gid| &table.group_lits[gi][gid]).collect();
        cols.push(literals_to_array(&lits, &g.data_type())?);
    }
    // agg 列
    for (ai, agg) in aggs.iter().enumerate() {
        cols.push(finalize_col(table, ai, agg, n)?);
    }
    RecordBatch::try_new(Arc::clone(schema), cols)
        .map_err(|e| QsdbError::invalid(format!("agg output: {e}")))
}

/// 单聚合列：SoA 状态 → 输出数组（不做行式 AggState 物化）。
fn finalize_col(table: &GroupTable, ai: usize, agg: &AggExpr, n: usize) -> Result<ArrayRef> {
    use crate::expr::AggFunc::*;
    match agg.func {
        Udaf => Err(QsdbError::unsupported(
            "UDAF 走 UdafAggExec 串行物化路径（不应到达 finalize）",
        )),
        Count => {
            let c = &table.counts[ai];
            let out: Int64Array = c.iter().take(n).map(|v| Some(*v as i64)).collect();
            Ok(Arc::new(out))
        }
        Sum => {
            // count=0（未见任何非空值）→ NULL（全 NULL 组语义）
            let cnt = &table.counts[ai];
            match agg.out_ty {
                LogicalType::Int64 => {
                    let SumCol::Int(v) = &table.sums[ai] else {
                        return Err(QsdbError::type_err("sum column kind mismatch (internal)"));
                    };
                    let out: Int64Array = (0..n)
                        .map(|g| {
                            if cnt[g] == 0 {
                                None
                            } else {
                                i64::try_from(v[g]).ok()
                            }
                        })
                        .collect();
                    Ok(Arc::new(out))
                }
                LogicalType::Float64 => {
                    let SumCol::Flt(v) = &table.sums[ai] else {
                        return Err(QsdbError::type_err("sum column kind mismatch (internal)"));
                    };
                    let out: Float64Array = (0..n)
                        .map(|g| if cnt[g] == 0 { None } else { Some(v[g].0) })
                        .collect();
                    Ok(Arc::new(out))
                }
                LogicalType::Decimal(m) => {
                    let SumCol::Dec(v) = &table.sums[ai] else {
                        return Err(QsdbError::type_err("sum column kind mismatch (internal)"));
                    };
                    let out: Decimal128Array = (0..n)
                        .map(|g| if cnt[g] == 0 { None } else { v[g].to_i128() })
                        .collect();
                    Ok(Arc::new(out.with_data_type(DataType::Decimal128(
                        m.precision,
                        m.scale,
                    ))))
                }
                _ => Err(QsdbError::type_err("bad sum output type")),
            }
        }
        Avg => match agg.out_ty {
            LogicalType::Float64 => {
                let cnt = &table.counts[ai];
                let out: Float64Array = match &table.sums[ai] {
                    SumCol::Flt(v) => v
                        .iter()
                        .take(n)
                        .zip(cnt.iter())
                        .map(|(x, c)| Some(x.0 / *c as f64))
                        .collect(),
                    SumCol::Int(v) => v
                        .iter()
                        .take(n)
                        .zip(cnt.iter())
                        .map(|(x, c)| Some(*x as f64 / *c as f64))
                        .collect(),
                    SumCol::Dec(v) => v
                        .iter()
                        .take(n)
                        .zip(cnt.iter())
                        .map(|(x, c)| Some(x.to_i128().unwrap_or(0) as f64 / *c as f64))
                        .collect(),
                    SumCol::None => (0..n).map(|_| None).collect(),
                };
                // count = 0 → NULL（空组不产生：组只在见到行时创建，但 merge 后可能）
                let out: Float64Array = out
                    .iter()
                    .zip(cnt.iter())
                    .map(|(v, c)| if *c == 0 { None } else { v })
                    .collect();
                Ok(Arc::new(out))
            }
            LogicalType::Decimal(m) => {
                let cnt = &table.counts[ai];
                let SumCol::Dec(v) = &table.sums[ai] else {
                    return Err(QsdbError::type_err("sum column kind mismatch (internal)"));
                };
                // HALF_UP(sum / count) 到 avg_result scale
                let out: Decimal128Array = v
                    .iter()
                    .take(n)
                    .zip(cnt.iter())
                    .map(|(x, c)| {
                        if *c == 0 {
                            return None;
                        }
                        let cnt_v = i256::from_i128(*c as i128);
                        let q = *x / cnt_v;
                        let rem = *x % cnt_v;
                        let two = i256::from_i128(2);
                        let round_up = rem != i256::from_i128(0)
                            && rem.wrapping_mul(two).wrapping_abs() >= cnt_v;
                        let adj = if round_up {
                            if *x >= i256::from_i128(0) {
                                q + i256::from_i128(1)
                            } else {
                                q - i256::from_i128(1)
                            }
                        } else {
                            q
                        };
                        adj.to_i128()
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
            let col = &table.minmax[ai];
            let lits: Vec<&Literal> = (0..n)
                .map(|g| col[g].as_ref().unwrap_or(&Literal::Null))
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
