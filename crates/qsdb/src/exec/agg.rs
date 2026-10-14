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
    /// `by_hash` 各桶的**载荷字节**（`capacity()*4 + Vec 头`）**增量累计值**。
    ///
    /// 为什么要单独存：`estimated_bytes` 原先每调一次就
    /// `by_hash.values().map(...).sum()` 遍历**全部条目**——**每批调一次**，
    /// 于是成本是 `组数 × 批数`。实测（2026-10-14）：250 万字符串组 × 610 批
    /// ≈ 每次流过 **80MB**、合计 **48GB 内存流量 ≈ 5 秒**，
    /// 这正是 `SELECT DISTINCT gs`（5M 行）6.0s 里的绝大部分
    /// （对照组：同样 5M 行、500 万**整数**组的 `DISTINCT v` 只要 0.16s
    /// —— 整数键走 `fast_slots_inline`，`by_hash` 基本是空的，所以没被这条拖累）。
    ///
    /// 其余项本来都是 O(1) 的 `capacity()`（或 O(列数)），只有这一项是 O(组数)。
    payload_bytes: u64,
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

    /// 组表**内存占用的量级估算**（字节）——供内存预算记账。
    ///
    /// 口径是「分配的容器容量」而非「元素个数」：账要覆盖**已保留的内存**，
    /// 而不是逻辑数据量。不追求逐字节精确（HashMap 桶开销、分配器对齐、
    /// 容量翻倍余量都是估的），但**不系统性低估**——高基数下组表是内存大头，
    /// 估错一个量级就等于预算失效。
    ///
    /// 用它的地方：`HashAggExec` 按本值增量预留（见 `exec/mod.rs`），
    /// 从而把「每分片各建一份完整组表」的**分片放大**如实记进全局预算。
    /// **组件级占用剖析**（`QSDB_AGG_PROF=1` 时由执行器打印）。
    ///
    /// 为什么要有它：`estimated_bytes` 只给一个总数，而"组表胖"有好几种完全不同的
    /// 成因（键 arena 涨 / 每组的 `Vec` 头 / 每组一个 `Literal` 堆对象 /
    /// 哈希桶失衡），总数看不出是哪一种，改起来就只能猜。
    /// 实测（2026-10-14）`SELECT DISTINCT gs` 250 万字符串组占 4.4GB，
    /// 而 500 万 int64 组只占 569MB —— 差的就是这张表要指出的东西。
    pub fn profile_bytes(&self) -> Vec<(&'static str, u64)> {
        let vec_hdr = std::mem::size_of::<Vec<u32>>() as u64;
        vec![
            ("key_arena", self.key_arena.len() as u64),
            ("key_arena_cap", self.key_arena.capacity() as u64),
            ("key_off", self.key_off.len() as u64 * 4),
            ("key_off_cap", self.key_off.capacity() as u64 * 4),
            ("by_hash_buckets", self.by_hash.capacity() as u64 * 33),
            ("by_hash_len", self.by_hash.len() as u64),
            (
                "by_hash_payload",
                self.by_hash
                    .values()
                    .map(|v| v.capacity() as u64 * 4 + vec_hdr)
                    .sum(),
            ),
            ("fast_slots_cap", self.fast_slots.capacity() as u64 * 16),
            (
                "fast_slots_inline_cap",
                self.fast_slots_inline.capacity() as u64 * 16,
            ),
            (
                "group_lits_cap",
                self.group_lits
                    .iter()
                    .map(|v| v.capacity() as u64 * std::mem::size_of::<Literal>() as u64)
                    .sum(),
            ),
            // **堆内字符串**（`Literal::Utf8` 的 String 体）——`estimated_bytes`
            // **没有**把这块算进去（它只数容器容量）。这一行就是为了让那笔账可见。
            (
                "group_lits_heap_str",
                self.group_lits
                    .iter()
                    .flat_map(|v| v.iter())
                    .map(|l| match l {
                        Literal::Utf8(s) => s.capacity() as u64 + 32,
                        _ => 0,
                    })
                    .sum(),
            ),
        ]
    }

    /// **载荷项的全量重算**（仅测试用）：与增量维护的 `payload_bytes` 对拍。
    ///
    /// ⚠️ 只重算**载荷那一项**——`estimated_bytes` 还有 `by_hash.capacity()*33`、
    /// `group_lits` 容量等**本来就 O(1)** 的项，拿整条估计值来比会误判成"不一致"。
    #[cfg(test)]
    pub fn payload_bytes_full(&self) -> u64 {
        let vec_hdr = std::mem::size_of::<Vec<u32>>() as u64;
        self.by_hash
            .values()
            .map(|v| v.capacity() as u64 * 4 + vec_hdr)
            .sum()
    }

    pub fn estimated_bytes(&self) -> u64 {
        let mut b: u64 = 0;
        b += self.key_arena.capacity() as u64;
        b += self.key_off.capacity() as u64 * 4;
        b += self.fast_slots.capacity() as u64 * 16; // (u64, u32) + padding
        b += self.fast_slots_inline.capacity() as u64 * 16; // (i64, u32) + padding
        // HashMap<u64, Vec<u32>>：桶 + 每组一个 Vec 头
        let vec_hdr = std::mem::size_of::<Vec<u32>>() as u64;
        b += self.by_hash.capacity() as u64 * (std::mem::size_of::<(u64, Vec<u32>)>() as u64 + 1);
        // **增量维护**（见 `payload_bytes` 的注释）：这里以前是全量遍历，
        // 每批一次 ⇒ 组数 × 批数，是 `SELECT DISTINCT` 高基数下的平方级热点。
        b += self.payload_bytes;
        let lit = std::mem::size_of::<Literal>() as u64;
        b += self.group_lits.capacity() as u64 * vec_hdr;
        b += self
            .group_lits
            .iter()
            .map(|v| v.capacity() as u64 * lit)
            .sum::<u64>();
        b += self.counts.capacity() as u64 * vec_hdr;
        b += self
            .counts
            .iter()
            .map(|v| v.capacity() as u64 * 8)
            .sum::<u64>();
        for s in &self.sums {
            let per = match s {
                SumCol::None => 0,
                SumCol::Int(v) => v.capacity() as u64 * 16,
                SumCol::Flt(v) => v.capacity() as u64 * 16,
                SumCol::Dec(v) => v.capacity() as u64 * 32,
            };
            b += per;
        }
        b += self.minmax.capacity() as u64 * vec_hdr;
        b += self
            .minmax
            .iter()
            .map(|v| v.capacity() as u64 * (lit + 8))
            .sum::<u64>();
        b
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

    /// **补「空组」：无 `GROUP BY` 的聚合必须恰好一行**（SQL 契约）。
    ///
    /// 零输入时组表是空的——`ingest` 对零行批直接 early-return，`try_global_fast`
    /// 也只在**有行**时 `push_group`，于是 `init_cols`/`push_group` 一次都没跑过。
    /// 调用方看到 `is_empty()` 就吐零个批 ⇒ `SELECT sum(v) FROM t WHERE false`
    /// 返回**空结果集**而不是"一行 NULL"。
    ///
    /// 类型信息必须从**计划**取（`agg.arg` 的 Arrow 类型）：空的实参批拿不到
    /// 类型，而 `init_cols` 就靠它分派 `SumCol::Int/Flt/Dec`。
    fn seed_global_group(&mut self, aggs: &[AggExpr]) {
        let args: Vec<Option<ArrayRef>> = aggs
            .iter()
            .map(|a| {
                a.arg
                    .as_ref()
                    .map(|e| arrow::array::new_empty_array(&e.data_type().arrow_type()))
            })
            .collect();
        self.init_cols(&[], aggs, &args);
        if self.key_off.is_empty() {
            self.push_group(&[], &[]);
        }
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
                    // 负载因子 0.5（实测 0.75 更慢：探测链变长的代价 > 表变小的收益）
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

    /// 往 `by_hash` 的桶里追加一个 gid，并**增量**维护 [`GroupTable::payload_bytes`]。
    ///
    /// 见该字段的注释：这一项以前是"每次 `estimated_bytes` 全量重算"，是平方级开销。
    fn bucket_push(&mut self, h: u64, gid: u32) {
        const VEC_HDR: u64 = std::mem::size_of::<Vec<u32>>() as u64;
        let v = self.by_hash.entry(h).or_default();
        if v.capacity() == 0 {
            self.payload_bytes += VEC_HDR;
        }
        let before = v.capacity() as u64;
        v.push(gid);
        self.payload_bytes += (v.capacity() as u64 - before) * 4;
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
        self.bucket_push(h, gid);
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
                    // 列状态种类未初始化（**还没有任何组**）：从 other 继承结构。
                    //
                    // 判据是"还没有组"而不是 `kinds.is_empty()`：DISTINCT/无聚合列
                    // 的表 `kinds` **恒为空**，用它当代理会导致每新增一组就重置一次
                    // `group_lits` → 组键列表与 `key_off` 长度错位 → 输出时越界
                    // （实测：DISTINCT 溢出路径 panic "len is 126 but index is 126"）。
                    if self.key_off.is_empty() {
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
                    self.bucket_push(h, gid);
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

    /// 各组的分组键 Literal（`[列][组]`）。溢出路由要用它算出"这一组属于哪个分区"。
    pub fn group_lits(&self) -> &[Vec<Literal>] {
        &self.group_lits
    }

    /// partial 分片执行辅助：聚合列数注册（首批前调用；列状态首批 ingest 落地）。
    pub fn set_arity(&mut self, n: usize) {
        self.pending_arity = Some(n);
    }

    pub fn arity(&self) -> usize {
        self.pending_arity.unwrap_or(0)
    }

    // `group_keys()` **已删**（2026-10-14）：它把列式的 `group_lits` 转置成
    // `Vec<Vec<Literal>>`（每组一个 Vec 堆对象 + 一次 Literal 克隆），调用方再转回
    // 列式——一进一出纯白扔。唯一调用方 `DistinctExec` 改走列式的
    // [`build_output_batch_cols`]。留着它就是留着一个"顺手拿行式键"的陷阱。

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
/// **无 `GROUP BY` 且整条输入为空**时的那唯一一行（`count`→0，其余→NULL）。
///
/// 分片并行收尾必须用这个函数在**外层补一次**，不能在每个分片里各补一次：
/// 全局聚合只有一个组，它按 `group_hash` 只落在一个分片上，其余分片是空的——
/// 各补一行就会拼出 P 行（P = 分片数）。
pub fn finalize_global_empty(
    aggs: &[AggExpr],
    schema: &arrow::datatypes::SchemaRef,
) -> Result<RecordBatch> {
    let mut t = GroupTable::new();
    t.seed_global_group(aggs);
    finalize_table(&t, aggs, &[], schema)
}

/// 聚合收尾统一入口：**无 `GROUP BY` 恰好一行**（SQL 契约）。
///
/// 空输入时组表里一个组都没有，但无 `GROUP BY` 的聚合仍必须回一行
/// （`count`→0、`sum`/`avg`/`min`/`max`→NULL）——由 [`finalize_global_empty`]
/// 补的那个「空组」产出。**所有聚合收尾点都该走这里**，别直接调
/// [`finalize_table`]：直接调会把"零输入"变成零行、再被调用方当成"没有结果"丢掉。
pub fn finalize_agg(
    table: &GroupTable,
    aggs: &[AggExpr],
    group: &[PhysExpr],
    schema: &arrow::datatypes::SchemaRef,
) -> Result<RecordBatch> {
    if table.is_empty() && group.is_empty() {
        return finalize_global_empty(aggs, schema);
    }
    finalize_table(table, aggs, group, schema)
}

/// 分片/分区收尾：`None` = 该片没有输出。
///
/// 只判"**有 `GROUP BY`** 且零组"（那才是真·零行，如
/// `SELECT a, count(*) FROM t WHERE false GROUP BY a`）；无 `GROUP BY` 的
/// 空输入**不能在这里各补一行**，见 [`finalize_global_empty`]。
pub fn finalize_or_none(
    table: &GroupTable,
    aggs: &[AggExpr],
    group: &[PhysExpr],
    schema: &arrow::datatypes::SchemaRef,
) -> Result<Option<RecordBatch>> {
    if table.is_empty() {
        return Ok(None);
    }
    finalize_agg(table, aggs, group, schema).map(Some)
}

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
///
/// **快路径 = 一次遍历的 typed builder**（常见类型）；不认识的类型退回
/// [`literals_to_array_slow`]。
///
/// 为什么要有快路径（2026-10-14 实测）：慢路径是"**每行建一个单元素 Arrow 数组
/// 再 `interleave` 拼起来**"——n 行 = **n 个数组**，每个自带 `ArrayData` +
/// buffers + `Arc` 链。`SELECT DISTINCT gs`（250 万字符串组）因此吃满 **4.4GB / 9s**
/// （组表本身账上只有 449MB），对照 `count(DISTINCT gs)` 的 DuckDB 0.20s/400MB
/// 差 87×。builder 是 O(n) 一趟、增量扩容，没有逐行的数组对象。
///
/// 快路径**只在字面量变体与目标 arrow 类型精确对应时**生效；需要隐式转换
/// （如 `Int32` → `Int64`、带时区的 timestamp、decimal 精度调整）的一律退回慢路径——
/// 那里有无条件 cast，语义完全一致。**宁可慢，不许悄悄换语义。**
pub fn literals_to_array(lits: &[&Literal], ty: &LogicalType) -> Result<ArrayRef> {
    use arrow::array::{BooleanBuilder, StringBuilder};
    use arrow::datatypes::{DataType, Int8Type, Int16Type, Int32Type, Int64Type, TimeUnit};

    if lits.is_empty() {
        return Ok(arrow::array::new_empty_array(&ty.arrow_type()));
    }
    let dt = ty.arrow_type();

    /// 数值/时间类共用的 builder 循环：`f` 做不到就是"类型不精确对应" → 退慢路径。
    fn prim<T, F>(lits: &[&Literal], f: F) -> Option<ArrayRef>
    where
        T: arrow::datatypes::ArrowPrimitiveType,
        F: Fn(&Literal) -> Option<T::Native>,
    {
        use arrow::array::PrimitiveBuilder;
        let mut b = PrimitiveBuilder::<T>::with_capacity(lits.len());
        for l in lits {
            match l {
                Literal::Null => b.append_null(),
                other => b.append_value(f(other)?),
            }
        }
        Some(std::sync::Arc::new(b.finish()) as ArrayRef)
    }

    let fast: Option<ArrayRef> = match &dt {
        DataType::Int64 => prim::<Int64Type, _>(lits, |l| match l {
            Literal::Int64(v) => Some(*v),
            _ => None,
        }),
        DataType::Int32 => prim::<Int32Type, _>(lits, |l| match l {
            Literal::Int32(v) => Some(*v),
            _ => None,
        }),
        DataType::Int16 => prim::<Int16Type, _>(lits, |l| match l {
            Literal::Int16(v) => Some(*v),
            _ => None,
        }),
        DataType::Int8 => prim::<Int8Type, _>(lits, |l| match l {
            Literal::Int8(v) => Some(*v),
            _ => None,
        }),
        DataType::Float64 => prim::<arrow::datatypes::Float64Type, _>(lits, |l| match l {
            Literal::Float64(v) => Some(*v),
            _ => None,
        }),
        DataType::Float32 => prim::<arrow::datatypes::Float32Type, _>(lits, |l| match l {
            Literal::Float32(v) => Some(*v),
            _ => None,
        }),
        DataType::Date32 => prim::<arrow::datatypes::Date32Type, _>(lits, |l| match l {
            Literal::Date(v) => Some(*v),
            _ => None,
        }),
        // 只在**无时区**时走快路径：带 tz 的目标类型要 cast，交给慢路径。
        DataType::Timestamp(TimeUnit::Nanosecond, None) => {
            prim::<arrow::datatypes::TimestampNanosecondType, _>(lits, |l| match l {
                Literal::Timestamp(v) => Some(*v),
                _ => None,
            })
        }
        DataType::Boolean => {
            let mut b = BooleanBuilder::with_capacity(lits.len());
            let mut ok = true;
            for l in lits {
                match l {
                    Literal::Null => b.append_null(),
                    Literal::Boolean(v) => b.append_value(*v),
                    _ => {
                        ok = false;
                        break;
                    }
                }
            }
            ok.then(|| std::sync::Arc::new(b.finish()) as ArrayRef)
        }
        DataType::Utf8 => {
            let bytes: usize = lits
                .iter()
                .map(|l| match l {
                    Literal::Utf8(s) => s.len(),
                    _ => 0,
                })
                .sum();
            let mut b = StringBuilder::with_capacity(lits.len(), bytes);
            let mut ok = true;
            for l in lits {
                match l {
                    Literal::Null => b.append_null(),
                    Literal::Utf8(s) => b.append_value(s),
                    _ => {
                        ok = false;
                        break;
                    }
                }
            }
            ok.then(|| std::sync::Arc::new(b.finish()) as ArrayRef)
        }
        _ => None,
    };
    if let Some(a) = fast {
        return Ok(a);
    }
    literals_to_array_slow(lits, ty)
}

/// 慢路径：每行一个单元素数组 + `interleave`。
///
/// **只用于快路径覆盖不到的类型**（decimal / Binary / Interval / Time /
/// LargeUtf8 / 带时区 ts / 需要隐式 cast 的混合字面量）。它正确但沉重——
/// 别把它当默认路径用，见 [`literals_to_array`] 的实测数字。
fn literals_to_array_slow(lits: &[&Literal], ty: &LogicalType) -> Result<ArrayRef> {
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

/// Distinct 输出（**列式**：直接从组表的 SoA 取键，不做行式转置）。
///
/// 早先 `DistinctExec` 走的是 `group_keys()`——把列式的 `group_lits` 转置成
/// `Vec<Vec<Literal>>`（**每组一个 Vec 堆对象**：250 万组 = 250 万次分配），
/// 交给 `build_output_batch` 再转回列式。**这一进一出纯属白扔**：
/// `group_lits` 本来就是列式的（`finalize_table` 里的循环是同款）。
pub fn build_output_batch_cols(
    table: &GroupTable,
    schema: &arrow::datatypes::SchemaRef,
) -> Result<RecordBatch> {
    let n = table.len();
    let ncol = schema.fields().len();
    // 组键列数必须与 schema 对齐：错位会静默把 A 列的值写进 B 列
    if ncol != table.group_lits.len() {
        return Err(QsdbError::invalid(format!(
            "distinct 输出：schema {} 列 vs 组键 {} 列",
            ncol,
            table.group_lits.len()
        )));
    }
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(ncol);
    for (gi, f) in schema.fields().iter().enumerate() {
        let lits: Vec<&Literal> = (0..n).map(|gid| &table.group_lits[gi][gid]).collect();
        let ty = LogicalType::try_from_arrow(f.data_type()).unwrap_or(LogicalType::Int64);
        cols.push(literals_to_array(&lits, &ty)?);
    }
    RecordBatch::try_new(Arc::clone(schema), cols)
        .map_err(|e| QsdbError::invalid(format!("distinct out: {e}")))
}

// `build_output_batch(keys: &[Vec<Literal>], …)` **已删**（2026-10-14）：行式键的唯一
// 来源是上面删掉的 `group_keys()`，两个一起走。列式入口见
// [`build_output_batch_cols`]（`GROUP BY` 侧一直用的是同款列式循环 `finalize_table`）。

#[cfg(test)]
mod tests {
    use super::*;

    /// **增量记账必须恒等于全量重算**——它驱动溢出决策，算错就是"该落盘没落盘"。
    ///
    /// 背景：`payload_bytes` 是为了干掉 `estimated_bytes` 里的平方级全量遍历
    /// （250 万组 × 610 批 ≈ 48GB 内存流量）才改的增量维护。
    /// 这条测试把它钉在"与重算逐字节相等"上。
    #[test]
    fn incremental_payload_matches_full_recompute() {
        use std::sync::Arc;
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("k", arrow::datatypes::DataType::Utf8, true),
        ]));
        let ks: Vec<Option<String>> = (0..5000)
            .map(|i| {
                if i % 97 == 0 {
                    None
                } else {
                    Some(format!("k-{}", i % 700))
                }
            })
            .collect();
        let arr = StringArray::from(ks);
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(arr)]).unwrap();
        let g = vec![PhysExpr::Column {
            idx: 0,
            name: "k".into(),
            ty: LogicalType::Varchar { max_chars: None },
        }];

        let mut a = GroupTable::default();
        a.ingest(&g, &[], &batch).unwrap();
        assert_eq!(a.len(), 701, "700 个 k-* 组 + 1 个 NULL 组");
        assert_eq!(a.payload_bytes, a.payload_bytes_full(), "单表 ingest 后");
        // 整条估计值也得自洽（载荷只是其中一项，其余是 O(1) 的 capacity 项）
        assert!(a.estimated_bytes() >= a.payload_bytes);

        // merge_subset 也往 by_hash 里插 ⇒ 同样要一致
        let mut b = GroupTable::default();
        b.ingest(&g, &[], &batch).unwrap();
        a.merge_subset(&b, (0..b.len()).collect(), 0);
        assert_eq!(a.payload_bytes, a.payload_bytes_full(), "merge 后");

        // 空表整体接管（`*self = other`）也不许漏
        let mut c = GroupTable::default();
        c.merge(b, 0);
        assert_eq!(c.payload_bytes, c.payload_bytes_full(), "整体接管后");
    }

    #[test]
    fn fxhash_basic() {
        assert_ne!(fxhash_bytes(b"abc"), fxhash_bytes(b"abd"));
        assert_eq!(fxhash_bytes(b"abc"), fxhash_bytes(b"abc"));
        // 非 8 倍长
        assert_ne!(fxhash_bytes(b"abcdefghijklm"), 0);
    }
}
