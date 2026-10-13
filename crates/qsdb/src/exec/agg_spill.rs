//! 聚合/去重的**分区溢出**：按 group key 把输入行分到 P 个分区（各自有界缓冲，
//! 超界即落盘），收尾时**逐分区**读回、分区内聚合/去重。
//!
//! 为什么这样是对的：同一个逻辑组键必然算出同一个路由值 → **同一个分区**；
//! 于是每个分区可独立聚合出该组的部分结果，最后把各分区输出简单拼接即可
//! （同一组不会跨分区，不存在需要跨分区合并同组的情形）。
//!
//! 路由的**一致性**是这里的命门：内存里已经聚合出的那张表（`GroupTable`）的组，
//! 与后续落盘的原始行，必须算出同一个分区。两者可用的信息不同——
//! 表里只有组的 `Literal`（`group_lits`），行上是 Arrow 数组。故统一走
//! `array_value_to_literal`（组表的键正是由它构造）+ `Literal::to_sql_string()`
//! 这条**两边同源**的规范形态。不能用 `GroupTable` 内部的 arena 字节：
//! 那是快/慢路径各写各的（单 Int64 键只存 8 字节 LE、其余带 tag），照抄会随
//! 内部实现漂移。
//!
//! 为什么不是"溢出中间聚合状态"：那要给 `GroupTable` 的 SoA 状态定义一套可合并的
//! 序列化格式（AVG 之类还得拆成 sum/count 才可合并），正确性代价高——本仓
//! `GroupTable` 有过三次因正确性回滚的改动（见 `docs/分组聚合性能专项方案.md`）。
//! P1 取"落原始行"这条**易验证**的路径：分区内聚合走的是与内存路径**完全相同**的
//! `GroupTable::ingest`，溢出只改"什么时候读"，不改"怎么算"。
//!
//! 内存口径：在途 = 各分区缓冲合计（**计入全局预算**：push 时预留、落盘时归还）
//! + 收尾时**单个分区**的行。峰值与分区数、单分区上界成正比，**与输入规模无关**。

use std::path::PathBuf;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

use crate::error::{QsdbError, Result};
use crate::expr::PhysExpr;
use crate::memory::Reservation;
use crate::types::Literal;

use super::agg::array_value_to_literal;
use super::spill::{self, SpillDir};

/// 单个分区的内存缓冲上界缺省值（超界即落盘）。刻意取小：在途缓冲吃的是
/// **全局预算**，缓冲越大越接近"用内存换 I/O"的老路。
const PART_CAP_MAX: u64 = 4 * 1024 * 1024;
const PART_CAP_MIN: u64 = 64 * 1024;

/// 分区数：与常见并行度同量级即可——分区越多单分区越小，但文件数与收尾开销随之涨。
/// （自适应分区留到 P3；先固定值把正确性与内存上界立住。）
pub fn default_nparts() -> usize {
    rayon::current_num_threads().clamp(2, 16)
}

/// 一组分组键 → 分区号。**规范形态**：各列 `Literal` 的 SQL 显示串拼接。
///
/// 不同键撞到同一分区是允许的（只是分区大小不均）；**同一个键必须落同一分区**——
/// 这条由"两侧都经 `array_value_to_literal` → `to_sql_string`"保证。
pub fn partition_of_lits(lits: &[Literal], nparts: usize) -> usize {
    let mut h: u64 = 1_469_598_103_934_665_603;
    for l in lits {
        let s = l.to_sql_string();
        for b in s.as_bytes() {
            h = (h ^ u64::from(*b)).wrapping_mul(1_099_511_628_211);
        }
        h = h.wrapping_mul(0x100_0000_01b3); // 列分隔
    }
    (h % nparts as u64) as usize
}

// ---------------------------------------------------------------------------
// 通用分区溢出内核：**路由由调用方算**（`part_of_row`），本结构只管
// 有界缓冲 → 落盘 → 逐分区取回 → 内存记账。
//
// 两个使用者、两套路由口径（都必须与各自**内存路径**的口径一致，否则同一逻辑键
// 会被拆到不同分区、两边各算出半个结果）：
// - 聚合/去重：`route_by_lits`（Literal 规范形态——组表里只有 Literal）
// - hash join：`route_by_join_key`（`encode_join_key`——join 索引用的就是它）
// ---------------------------------------------------------------------------

/// 分区溢出内核。
pub struct PartSpill {
    dir: SpillDir,
    nparts: usize,
    bufs: Vec<Vec<RecordBatch>>,
    buf_bytes: Vec<u64>,
    runs: Vec<Vec<PathBuf>>,
    schema: SchemaRef,
    part_cap: u64,
    res: Reservation,
    live: u64,
}

impl PartSpill {
    /// 单分区缓冲上界的自适应（见 `AggSpill::part_cap_for` 的注释）。
    pub fn part_cap_for(ctx: &crate::query::QueryContext, nparts: usize) -> u64 {
        match ctx.memory_limit {
            Some(l) => (l / (4 * nparts.max(1) as u64)).clamp(PART_CAP_MIN, PART_CAP_MAX),
            None => PART_CAP_MAX,
        }
    }

    pub fn new(
        schema: SchemaRef,
        ctx: &std::sync::Arc<crate::query::QueryContext>,
        part_cap: u64,
    ) -> Result<Self> {
        let nparts = default_nparts();
        let res = ctx.reserve_spill(0)?;
        Ok(Self {
            dir: SpillDir::new(),
            nparts,
            bufs: (0..nparts).map(|_| Vec::new()).collect(),
            buf_bytes: vec![0; nparts],
            runs: (0..nparts).map(|_| Vec::new()).collect(),
            schema,
            part_cap,
            res,
            live: 0,
        })
    }

    pub fn nparts(&self) -> usize {
        self.nparts
    }

    /// 按**调用方算好的分区号**分发一批（分组 take → 追加缓冲 → 超界落盘）。
    pub fn push_routed(&mut self, batch: &RecordBatch, part_of_row: &[u32]) -> Result<()> {
        let n = batch.num_rows();
        if n == 0 {
            return Ok(());
        }
        debug_assert_eq!(n, part_of_row.len());
        let mut idx_by_part: Vec<Vec<u32>> = vec![Vec::new(); self.nparts];
        for (row, &p) in part_of_row.iter().enumerate() {
            idx_by_part[p as usize].push(row as u32);
        }
        for (p, idx) in idx_by_part.into_iter().enumerate() {
            if idx.is_empty() {
                continue;
            }
            let take_idx = arrow::array::UInt32Array::from(idx);
            let cols: Vec<arrow::array::ArrayRef> = (0..batch.num_columns())
                .map(|c| {
                    arrow::compute::take(batch.column(c), &take_idx, None)
                        .map_err(|e| QsdbError::invalid(format!("part spill take: {e}")))
                })
                .collect::<Result<_>>()?;
            let sub = RecordBatch::try_new(Arc::clone(&self.schema), cols)
                .map_err(|e| QsdbError::invalid(format!("part spill batch: {e}")))?;
            let bytes = sub.get_array_memory_size() as u64;
            self.res.grow(bytes)?;
            self.buf_bytes[p] += bytes;
            self.bufs[p].push(sub);
            if self.buf_bytes[p] >= self.part_cap {
                let freed = self.buf_bytes[p];
                let path = spill::write_run(&mut self.dir, &self.bufs[p], &self.schema)?;
                self.runs[p].push(path);
                self.bufs[p].clear();
                self.buf_bytes[p] = 0;
                self.res.shrink(freed);
            }
        }
        Ok(())
    }

    /// 取出一个分区**全部行**（落盘 run + 内存尾批）；顺带释放上一分区的记账。
    pub fn take_partition(&mut self, p: usize) -> Result<Vec<RecordBatch>> {
        self.res.shrink(self.live);
        self.live = 0;
        let mut out: Vec<RecordBatch> = Vec::new();
        for path in self.runs[p].drain(..) {
            out.extend(spill::read_run(&path)?);
        }
        let freed = self.buf_bytes[p];
        out.append(&mut self.bufs[p]);
        self.buf_bytes[p] = 0;
        self.res.shrink(freed);
        let bytes: u64 = out.iter().map(|b| b.get_array_memory_size() as u64).sum();
        self.res.grow(bytes)?;
        self.live = bytes;
        Ok(out)
    }

    pub fn spilled_runs(&self) -> usize {
        self.runs.iter().map(|v| v.len()).sum()
    }
}

/// 路由口径①：聚合/去重——分组键的 `Literal` 规范形态（**与组表的键同源**：
/// 组表的键就是 `array_value_to_literal` 造出来的）。
pub fn route_by_lits(batch: &RecordBatch, group: &[PhysExpr], nparts: usize) -> Result<Vec<u32>> {
    let group_arrays: Vec<arrow::array::ArrayRef> = group
        .iter()
        .map(|g| crate::expr::eval::eval(g, batch))
        .collect::<Result<_>>()?;
    let mut out = Vec::with_capacity(batch.num_rows());
    let mut lits: Vec<Literal> = Vec::with_capacity(group_arrays.len());
    for row in 0..batch.num_rows() {
        lits.clear();
        lits.extend(
            group_arrays
                .iter()
                .map(|a| array_value_to_literal(a.as_ref(), row)),
        );
        out.push(partition_of_lits(&lits, nparts) as u32);
    }
    Ok(out)
}

/// 路由口径②：hash join——`encode_join_key` 的字节（**与 join 索引同源**：
/// 索引也是拿它建键的；NULL 键带 0 前缀，两侧一致即可）。
pub fn route_by_join_key(
    batch: &RecordBatch,
    keys: &[PhysExpr],
    nparts: usize,
) -> Result<Vec<u32>> {
    let key_cols: Vec<arrow::array::ArrayRef> = keys
        .iter()
        .map(|k| crate::expr::eval::eval(k, batch))
        .collect::<Result<_>>()?;
    let mut out = Vec::with_capacity(batch.num_rows());
    let mut buf: Vec<u8> = Vec::with_capacity(64);
    for row in 0..batch.num_rows() {
        crate::exec::join::encode_join_key(&key_cols, row, &mut buf);
        out.push((fxhash_bytes(&buf) % nparts as u64) as u32);
    }
    Ok(out)
}

fn fxhash_bytes(bytes: &[u8]) -> u64 {
    const K: u64 = 0x51_7c_c1_b7_27_22_0a_95;
    let mut h: u64 = 0;
    for &b in bytes {
        h = (h.rotate_left(5) ^ u64::from(b)).wrapping_mul(K);
    }
    h
}

/// 聚合/去重的分区溢出器：路由口径 = 分组键的 `Literal` 规范形态
/// （内核见 [`PartSpill`]，`push` 即"算路由 + 交给内核"）。
pub struct AggSpill {
    core: PartSpill,
}

impl AggSpill {
    /// 单分区缓冲上界（转发 [`PartSpill::part_cap_for`]）。
    pub fn part_cap_for(ctx: &crate::query::QueryContext, nparts: usize) -> u64 {
        PartSpill::part_cap_for(ctx, nparts)
    }

    pub fn new(
        schema: SchemaRef,
        ctx: &std::sync::Arc<crate::query::QueryContext>,
    ) -> Result<Self> {
        let nparts = default_nparts();
        let part_cap = PartSpill::part_cap_for(ctx, nparts);
        Ok(Self {
            core: PartSpill::new(schema, ctx, part_cap)?,
        })
    }

    pub fn nparts(&self) -> usize {
        self.core.nparts()
    }

    /// 路由一批（口径见 [`route_by_lits`]）→ 内核。
    pub fn push(&mut self, batch: &RecordBatch, group: &[PhysExpr]) -> Result<()> {
        let nparts = self.core.nparts();
        let parts = route_by_lits(batch, group, nparts)?;
        self.core.push_routed(batch, &parts)
    }

    pub fn take_partition(&mut self, p: usize) -> Result<Vec<RecordBatch>> {
        self.core.take_partition(p)
    }

    pub fn spilled_runs(&self) -> usize {
        self.core.spilled_runs()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("g", DataType::Int64, true),
            Field::new("v", DataType::Int64, true),
        ]))
    }

    fn batch(g: &[i64], v: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from(g.to_vec())),
                Arc::new(Int64Array::from(v.to_vec())),
            ],
        )
        .unwrap()
    }

    /// **路由一致性**：同一逻辑键（跨批、跨 NULL）必须落同一分区，且行数守恒。
    /// 这是溢出正确性的命门——破了就会出现"同组被拆到两处、各自聚合出半个结果"。
    #[test]
    fn same_key_lands_in_same_partition() {
        let ctx = crate::query::QueryContext::benign();
        let group = vec![PhysExpr::Column {
            idx: 0,
            name: "g".into(),
            ty: crate::types::LogicalType::Int64,
        }];
        let mut sp = AggSpill::new(schema(), &ctx).unwrap();
        sp.push(&batch(&[1, 2, 3, 1, 2], &[10, 20, 30, 11, 21]), &group)
            .unwrap();
        sp.push(&batch(&[3, 1, 2], &[31, 12, 22]), &group).unwrap();
        let mut seen: std::collections::HashMap<i64, usize> = std::collections::HashMap::new();
        let mut total = 0usize;
        for p in 0..sp.nparts() {
            for b in sp.take_partition(p).unwrap() {
                total += b.num_rows();
                let gs = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
                for i in 0..b.num_rows() {
                    let g = gs.value(i);
                    assert_eq!(*seen.entry(g).or_insert(p), p, "组 {g} 跨分区：路由不稳定");
                }
            }
        }
        assert_eq!(total, 8, "行数守恒");
    }

    /// 规范形态对 `Literal` 的等价性（组表侧与行侧同源）。
    #[test]
    fn partition_of_lits_is_stable() {
        let a = [Literal::Int64(7), Literal::Utf8("x".into())];
        let b = [Literal::Int64(7), Literal::Utf8("x".into())];
        assert_eq!(partition_of_lits(&a, 8), partition_of_lits(&b, 8));
        // NULL 键不 panic 且稳定
        let c = [Literal::Null];
        assert_eq!(partition_of_lits(&c, 8), partition_of_lits(&c, 8));
    }
}
