//! 维表链聚合融合（第十四批刀 C，`docs/join发射与维表链聚合融合设计.md`）。
//!
//! `SELECT d2.cat, count(*) FROM t ⋈ d ⋈ d2 GROUP BY d2.cat` 这类
//! "事实表 → 维表 → 维表"链 + 简单聚合，**一行都不物化**：
//!
//! - TOP join 的 probe 键 = BOT join 的 **build 侧列** ⇒ "BOT 的每个 build 行
//!   能配到哪些 TOP build 行"是 build 侧的纯函数，**一次性预计算**
//!   （`matches`），probe 阶段按行号直查——不再逐对取值、哈希、匹配；
//! - 分组键 = TOP build 侧裸列 ⇒ 每个 TOP build 行的组号**懒种子化**
//!   （首个配对触达才建组，见 [`GroupTable::seed_group`]）——组表里只有
//!   真有输入行的组，零计数组根本不存在；
//! - probe 阶段每对配对只剩 `counts[gid] += 1` / `sums[gid] += v[p]`。
//!
//! 剖面依据（2026-10-17，.14）：`complex_three_way` 发射段的 79% 在探测/配对
//! 循环、22% 在发射 take——融合把这两块连同第二次 join 的逐对匹配一起拿掉。
//!
//! ⚠️ 语义对齐纪律：join 语义（NULL 不匹配 / 碰撞比真字节）唯一定义在
//! [`HashIndex::for_each_match`]；组表种子/直写口径逐位对齐 `ingest`/
//! `update_aggs`（回归：`tests/fused_dim_agg.rs` 对撞）。

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

use crate::error::Result;
use crate::expr::{AggExpr, AggFunc, PhysExpr, eval};
use crate::query::QueryContext;
use crate::types::Literal;

use super::agg::{GroupTable, array_value_to_literal, encode_key_row, finalize_or_none};
use super::join::{BuildSide, HashIndex, SmallRows, build_index, concat_batches};
use super::ExecNode;

/// 列号形状（`exec_builder::fused_dim_shape` 判定通过后交进来）。
/// 全部是**裸列**（门保证），键表达式带好各自的局部空间重映射。
pub(crate) struct FusedIdx {
    /// TOP build 批局部键表达式（`build_index` 用）
    pub top_build_keys: Vec<PhysExpr>,
    /// BOT build 批局部键表达式（`build_index` 用）；深度 1 = 空
    pub bot_build_keys: Vec<PhysExpr>,
    /// probe 批局部键表达式（深度 1 = TOP probe 键；深度 2 = BOT probe 键）
    pub probe_keys: Vec<PhysExpr>,
    /// TOP 的 probe 键在 **BOT build 批**的局部列号（预计算 matches 的查找键）
    pub top_key_in_bot_build: Vec<usize>,
    /// 组键 = TOP build 批局部列号
    pub group_cols: Vec<usize>,
    /// 聚合取数列（probe 批局部列号；`None` = count(*)）
    pub agg_cols: Vec<Option<usize>>,
}

/// 已建好的三个输入（exec_builder 在 `ShardScope` 下用 `build_inner` 造出）。
pub(crate) struct FusedParts {
    pub bot_build: Option<Box<dyn ExecNode>>,
    pub top_build: Box<dyn ExecNode>,
    pub probe: Box<dyn ExecNode>,
}

/// 融合执行核心：build 物化 → 预计算 → probe 直累计。返回 GroupTable partial
/// （分片路交给 `finish_partitioned_merge`，非分片路直接 finalize）。
pub(crate) fn run_fused_dim_agg(
    plans: FusedParts,
    idx: &FusedIdx,
    group_phys: &[PhysExpr],
    aggs: &[AggExpr],
    ctx: &Arc<QueryContext>,
) -> Result<(GroupTable, crate::memory::Reservation)> {
    let prof = std::env::var("QSDB_JOIN_PROF").is_ok();
    let t0 = prof.then(std::time::Instant::now);
    let mut res = ctx.try_reserve(0)?;
    let bot_side = match plans.bot_build {
        Some(n) => Some(materialize(n, ctx, &mut res)?),
        None => None,
    };
    let top_side = materialize(plans.top_build, ctx, &mut res)?;
    let top_index = build_index(&top_side, &idx.top_build_keys)?;
    // matches / gid_of 的常驻粗估（12B/BOT build 行 + 4B/TOP build 行）
    let bot_rows = bot_side.as_ref().map(|s| s.num_rows()).unwrap_or(0);
    res.grow(bot_rows as u64 * 12 + top_side.num_rows() as u64 * 4)
        .map_err(|e| crate::error::QsdbError::invalid(format!("fused-dim precompute: {e}")))?;

    // ── 深度 2 预计算：matches[b] = BOT build 行 b 能配到的 TOP build 行 ──
    // 查找键 = TOP 的 probe 键列，值就在 BOT build 批上（门保证裸列）。
    let (bot_index, matches): (Option<HashIndex>, Vec<SmallRows>) =
        if !idx.top_key_in_bot_build.is_empty() {
            let side = bot_side.as_ref().expect("深度 2 必有 BOT build 侧");
            let bidx = build_index(side, &idx.bot_build_keys)?;
            let mut ms: Vec<SmallRows> = Vec::with_capacity(side.num_rows());
            let mut hit: Vec<u32> = Vec::new();
            for batch in side.batches().iter() {
                ctx.check()?;
                let kcols: Vec<ArrayRef> = idx
                    .top_key_in_bot_build
                    .iter()
                    .map(|&c| batch.column(c).clone())
                    .collect();
                let mut probe = top_index.prepare_probe(&kcols);
                for row in 0..batch.num_rows() {
                    hit.clear();
                    probe.match_row(row, &mut |m| hit.push(m));
                    ms.push(match hit.as_slice() {
                        [] => SmallRows::One(u32::MAX), // 未匹配（含 NULL 键）哨兵
                        [one] => SmallRows::One(*one),
                        many => SmallRows::Many(many.to_vec()),
                    });
                }
            }
            (Some(bidx), ms)
        } else {
            (None, Vec::new())
        };

    // ── probe：逐批流式，配对直累计（组懒种子化）────────────────────────
    let kinds: Vec<AggFunc> = aggs.iter().map(|a| a.func).collect();
    let sum_types: Vec<DataType> = aggs
        .iter()
        .map(|a| match &a.arg {
            Some(e) => e.data_type().arrow_type(),
            None => DataType::Int64,
        })
        .collect();
    let mut table = GroupTable::default();
    let mut gid_of: Vec<u32> = vec![u32::MAX; top_side.num_rows()];
    let mut key_buf: Vec<u8> = Vec::with_capacity(64);
    let mut probe = plans.probe;
    let mut probe_rows: u64 = 0;
    while let Some(b) = probe.next() {
        ctx.check()?;
        let batch = b?;
        let n = batch.num_rows();
        if n == 0 {
            continue;
        }
        probe_rows += n as u64;
        let key_cols: Vec<ArrayRef> = idx
            .probe_keys
            .iter()
            .map(|k| eval::eval(k, &batch))
            .collect::<Result<Vec<_>>>()?;
        let agg_arrs: Vec<Option<ArrayRef>> = idx
            .agg_cols
            .iter()
            .map(|c| c.map(|p| batch.column(p).clone()))
            .collect();
        // 每批一次预备态（downcast/类型化视图在 prepare_probe，行内零预备）
        let mut rp = match &bot_index {
            Some(bidx) => bidx.prepare_probe(&key_cols),
            None => top_index.prepare_probe(&key_cols),
        };
        if bot_index.is_some() {
            for row in 0..n {
                rp.match_row(row, &mut |b| {
                    for m in matches[b as usize].as_slice() {
                        if *m == u32::MAX {
                            continue; // 该 BOT build 行没配上任何 TOP build 行
                        }
                        accumulate(
                            &mut table,
                            &mut gid_of,
                            &top_side,
                            &mut key_buf,
                            &idx.group_cols,
                            group_phys.len(),
                            &kinds,
                            &sum_types,
                            &idx.agg_cols,
                            &agg_arrs,
                            row,
                            *m as usize,
                        );
                    }
                });
            }
        } else {
            for row in 0..n {
                rp.match_row(row, &mut |b| {
                    accumulate(
                        &mut table,
                        &mut gid_of,
                        &top_side,
                        &mut key_buf,
                        &idx.group_cols,
                        group_phys.len(),
                        &kinds,
                        &sum_types,
                        &idx.agg_cols,
                        &agg_arrs,
                        row,
                        b as usize,
                    );
                });
            }
        }
    }
    if prof {
        eprintln!(
            "[fused-dim] bot={}行 top={}行，probe {probe_rows} 行 → {} 组，耗时 {:.1}ms",
            bot_rows,
            top_side.num_rows(),
            table.len(),
            t0.map(|t| t.elapsed().as_secs_f64() * 1000.0).unwrap_or(0.0),
        );
    }
    Ok((table, res))
}

/// 一对配对（probe 行 × TOP build 行）的累计：懒种子组 + 直写计数/求和。
///
/// 门保证聚合只有 Count/Sum 且 sum 是整型族/Float64 裸列 —— 不匹配的形状
/// 走不到这里（`unreachable!` 是内部不变量，不是用户输入处理）。
#[inline]
#[allow(clippy::too_many_arguments)]
fn accumulate(
    table: &mut GroupTable,
    gid_of: &mut [u32],
    top_side: &BuildSide,
    key_buf: &mut Vec<u8>,
    group_cols: &[usize],
    group_width: usize,
    kinds: &[AggFunc],
    sum_types: &[DataType],
    agg_cols: &[Option<usize>],
    agg_arrs: &[Option<ArrayRef>],
    probe_row: usize,
    top_row: usize,
) {
    let g = match gid_of[top_row] {
        v if v != u32::MAX => v as usize,
        _ => {
            // 懒种子：首个配对触达该 TOP build 行
            let (bi, row) = top_side.row_of(top_row);
            let batch = &top_side.batches()[bi];
            let cols: Vec<ArrayRef> =
                group_cols.iter().map(|&c| batch.column(c).clone()).collect();
            key_buf.clear();
            encode_key_row(&cols, row, key_buf);
            let lits: Vec<Literal> = cols
                .iter()
                .map(|a| array_value_to_literal(a.as_ref(), row))
                .collect();
            let g = table.seed_group(key_buf, lits, group_width, kinds, sum_types) as usize;
            gid_of[top_row] = g as u32;
            g
        }
    };
    for (ai, k) in kinds.iter().enumerate() {
        match (k, &agg_cols[ai]) {
            (AggFunc::Count, None) => table.fused_add_count(ai, g, 1),
            (AggFunc::Count, Some(_)) => {
                let a = agg_arrs[ai].as_ref().expect("取数列必有数组");
                if !a.is_null(probe_row) {
                    table.fused_add_count(ai, g, 1);
                }
            }
            (AggFunc::Sum, Some(_)) => {
                let a = agg_arrs[ai].as_ref().expect("取数列必有数组");
                if a.is_null(probe_row) {
                    continue; // sum 口径：NULL 不计入（与 update_sum 一致）
                }
                match a.data_type() {
                    DataType::Int8 => {
                        let x = a.as_any().downcast_ref::<Int8Array>().expect("int8");
                        table.fused_add_sum_int(ai, g, i128::from(x.value(probe_row)));
                    }
                    DataType::Int16 => {
                        let x = a.as_any().downcast_ref::<Int16Array>().expect("int16");
                        table.fused_add_sum_int(ai, g, i128::from(x.value(probe_row)));
                    }
                    DataType::Int32 => {
                        let x = a.as_any().downcast_ref::<Int32Array>().expect("int32");
                        table.fused_add_sum_int(ai, g, i128::from(x.value(probe_row)));
                    }
                    DataType::Int64 => {
                        let x = a.as_any().downcast_ref::<Int64Array>().expect("int64");
                        table.fused_add_sum_int(ai, g, i128::from(x.value(probe_row)));
                    }
                    DataType::Float64 => {
                        let x = a.as_any().downcast_ref::<Float64Array>().expect("f64");
                        table.fused_add_sum_f64(ai, g, x.value(probe_row));
                    }
                    _ => unreachable!("fused 门只放整型族/Float64 的 sum"),
                }
            }
            _ => unreachable!("fused 门只放 Count/Sum"),
        }
    }
}

/// 把一条腿的 build 侧物化成 [`BuildSide`]（记账语义同 `HashJoinExec::build_phase`：
/// 逐批 grow、能 concat 就 concat 单批化、索引每行 16B 粗估）。
/// **没有溢出通道**——融合路需要的内存严格少于现有路，现有路装不下时这里同样装不下。
fn materialize(
    node: Box<dyn ExecNode>,
    ctx: &Arc<QueryContext>,
    res: &mut crate::memory::Reservation,
) -> Result<BuildSide> {
    let mut node = node;
    let schema = node.schema();
    let mut batches = Vec::new();
    while let Some(b) = node.next() {
        ctx.check()?;
        let x = b?;
        res.grow(crate::memory::batch_bytes(&x))
            .map_err(|e| crate::error::QsdbError::invalid(format!("fused-dim build: {e}")))?;
        batches.push(x);
    }
    let peak = res.bytes();
    let side = if res.grow(peak).is_ok() {
        let t = concat_batches(&batches, &schema)?;
        res.shrink(peak); // 源批随 `batches` 释放，峰值回落
        BuildSide::new(vec![t], schema)
    } else {
        BuildSide::new(batches, schema)
    };
    res.grow(side.num_rows() as u64 * 16)
        .map_err(|e| crate::error::QsdbError::invalid(format!("fused-dim index: {e}")))?;
    Ok(side)
}

/// 非分片路入口：核心 fn 在计划期一口气跑完，finalize 后经 [`super::OneShotExec`-like]
/// 的批源吐出（exec_builder 持有 `OneShotExec`，这里只回批）。
pub(crate) fn run_fused_dim_agg_final(
    plans: FusedParts,
    idx: &FusedIdx,
    group: &[PhysExpr],
    aggs: &[AggExpr],
    schema: &arrow::datatypes::SchemaRef,
    ctx: &Arc<QueryContext>,
) -> Result<Option<RecordBatch>> {
    let (table, _res) = run_fused_dim_agg(plans, idx, group, aggs, ctx)?;
    finalize_or_none(&table, aggs, group, schema)
}
