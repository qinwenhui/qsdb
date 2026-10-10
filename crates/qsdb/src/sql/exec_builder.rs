//! LogicalPlan → ExecNode。聚合遇「Scan(/Filter) 无破坏算子」时做
//! 文件级 rayon 并行（分片 partial agg → merge → finalize）。

use std::sync::Arc;

use arrow::record_batch::RecordBatch;

use crate::api::{Qsdb, ScanRequest};
use crate::error::Result;
use crate::exec::agg::GroupTable;
use crate::exec::{ExecNode, FilterExec, HashAggExec, LimitExec, ProjectExec, ScanExec, SortExec};
use crate::expr::{AggExpr, PhysExpr};

use super::translate::LogicalPlan;

// CTE 批缓存（execute_sql 物化后注入；build 期间读取）。
// thread_local：查询独立，无跨查询泄漏。
thread_local! {
    static CTE_BATCHES: std::cell::RefCell<std::collections::HashMap<u32, Vec<RecordBatch>>> =
        std::cell::RefCell::new(std::collections::HashMap::new());
}

/// 注入 CTE 批缓存（execute_sql 调用；build 前设置）。
pub fn set_cte_batches(map: std::collections::HashMap<u32, Vec<RecordBatch>>) {
    CTE_BATCHES.with(|c| {
        *c.borrow_mut() = map;
    });
}

/// 增量写入单个 CTE 批（链式 CTE：后物化者读取先物化者的缓存）。
pub fn add_cte_batches(cte_id: u32, batches: Vec<RecordBatch>) {
    CTE_BATCHES.with(|c| {
        c.borrow_mut().insert(cte_id, batches);
    });
}

/// 一次性批执行节点（并行聚合结果）。
struct OneShotExec {
    batch: Option<RecordBatch>,
    schema: Arc<arrow::datatypes::Schema>,
}

impl ExecNode for OneShotExec {
    fn schema(&self) -> Arc<arrow::datatypes::Schema> {
        Arc::clone(&self.schema)
    }
    fn next(&mut self) -> Option<Result<RecordBatch>> {
        self.batch.take().map(Ok)
    }
}

pub fn build(
    db: &Qsdb,
    plan: &LogicalPlan,
    ctx: &Arc<crate::query::QueryContext>,
) -> Result<Box<dyn ExecNode>> {
    // M4-3 列裁剪下推：收集全计划引用列，Scan 只读所需列（含谓词列）。
    // Window 计划的窗口列 idx 假设全列序——下推重排会破坏索引映射，跳过。
    if has_window_node(plan) {
        return build_inner(db, plan, None, ctx);
    }
    let mut used: Vec<String> = Vec::new();
    collect_plan_columns(plan, &mut used);
    if let Some(def) = plan_scan_def(plan) {
        let all: Vec<String> = def.columns.iter().map(|c| c.name.clone()).collect();
        let mut proj: Vec<String> = all
            .iter()
            .filter(|n| used.iter().any(|u| u.eq_ignore_ascii_case(n.as_str())))
            .cloned()
            .collect();
        if proj.is_empty() {
            proj = all.first().cloned().into_iter().collect();
        }
        if proj.len() < all.len() {
            let mapping = build_mapping(&all, &proj);
            let rewritten = rewrite_plan(plan, &mapping, &all);
            return build_inner(db, &rewritten, Some(proj), ctx);
        }
    }
    build_inner(db, plan, None, ctx)
}

/// 全计划引用列名（Column 节点）。
fn collect_plan_columns(plan: &LogicalPlan, out: &mut Vec<String>) {
    match plan {
        LogicalPlan::Scan { .. } => {}
        LogicalPlan::Filter { input, predicate } => {
            collect_expr_columns(predicate, out);
            collect_plan_columns(input, out);
        }
        LogicalPlan::Project { input, exprs, .. } => {
            for e in exprs {
                collect_expr_columns(e, out);
            }
            collect_plan_columns(input, out);
        }
        LogicalPlan::Aggregate { input, group, aggs } => {
            for g in group {
                collect_expr_columns(g, out);
            }
            for a in aggs {
                if let Some(arg) = &a.arg {
                    collect_expr_columns(arg, out);
                }
            }
            collect_plan_columns(input, out);
        }
        LogicalPlan::Sort { input, keys } => {
            for k in keys {
                collect_expr_columns(&k.expr, out);
            }
            collect_plan_columns(input, out);
        }
        LogicalPlan::Limit { input, .. } | LogicalPlan::Distinct { input } => {
            collect_plan_columns(input, out);
        }
        LogicalPlan::Join { .. } | LogicalPlan::CrossJoinFilter { .. } => {
            // 多表计划不走列裁剪下推（build 的 count_scans guard 兜底）
        }
        LogicalPlan::Values { .. } | LogicalPlan::Window { .. } => {} // 内存/窗口源无需裁剪
    }
}

fn collect_expr_columns(e: &PhysExpr, out: &mut Vec<String>) {
    match e {
        PhysExpr::Column { name, .. } => {
            if !out.iter().any(|n| n.eq_ignore_ascii_case(name.as_str())) {
                out.push(name.clone());
            }
        }
        PhysExpr::Lit(_) => {}
        PhysExpr::Arith { l, r, .. } | PhysExpr::Cmp { l, r, .. } => {
            collect_expr_columns(l, out);
            collect_expr_columns(r, out);
        }
        PhysExpr::And(a, b) | PhysExpr::Or(a, b) => {
            collect_expr_columns(a, out);
            collect_expr_columns(b, out);
        }
        PhysExpr::Not(a) | PhysExpr::IsNull(a, _) => collect_expr_columns(a, out),
        PhysExpr::Cast { e, .. } => collect_expr_columns(e, out),
        PhysExpr::Fn { args, .. } => args.iter().for_each(|a| collect_expr_columns(a, out)),
    }
}

/// 计划内是否含 Window 节点（列裁剪跳过：窗口列 idx 假设全列序）。
fn has_window_node(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Window { .. } => true,
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Distinct { input } => has_window_node(input),
        _ => false,
    }
}

/// 计划根到叶的 Scan 表定义。
fn plan_scan_def(plan: &LogicalPlan) -> Option<Arc<crate::catalog::TableDef>> {
    match plan {
        LogicalPlan::Scan { def, .. } => Some(Arc::clone(def)),
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Distinct { input } => plan_scan_def(input),
        LogicalPlan::Join { .. }
        | LogicalPlan::CrossJoinFilter { .. }
        | LogicalPlan::Values { .. } => None,
        LogicalPlan::Window { input, .. } => plan_scan_def(input),
    }
}

/// 全列名 → 投影列名 的 idx 映射（大小写不敏感）。
fn build_mapping(all: &[String], proj: &[String]) -> Vec<Option<usize>> {
    all.iter()
        .map(|a| proj.iter().position(|p| p.eq_ignore_ascii_case(a.as_str())))
        .collect()
}

/// 重写计划中所有 Column.idx（全表列序 → 投影序）。
/// `all`：表全列名——非表列（聚合输出列如 sum(x)）保持原 idx。
fn rewrite_plan(plan: &LogicalPlan, mapping: &[Option<usize>], all: &[String]) -> LogicalPlan {
    match plan {
        LogicalPlan::Scan { def, predicate } => LogicalPlan::Scan {
            def: Arc::clone(def),
            predicate: predicate.clone(),
        },
        LogicalPlan::Filter { input, predicate } => LogicalPlan::Filter {
            input: Box::new(rewrite_plan(input, mapping, all)),
            predicate: rewrite_expr(predicate, mapping, all),
        },
        LogicalPlan::Project {
            input,
            exprs,
            names,
        } => LogicalPlan::Project {
            input: Box::new(rewrite_plan(input, mapping, all)),
            exprs: exprs
                .iter()
                .map(|e| rewrite_expr(e, mapping, all))
                .collect(),
            names: names.clone(),
        },
        LogicalPlan::Aggregate { input, group, aggs } => LogicalPlan::Aggregate {
            input: Box::new(rewrite_plan(input, mapping, all)),
            group: group
                .iter()
                .map(|g| rewrite_expr(g, mapping, all))
                .collect(),
            aggs: aggs
                .iter()
                .map(|a| crate::expr::AggExpr {
                    func: a.func,
                    arg: a.arg.as_ref().map(|e| rewrite_expr(e, mapping, all)),
                    out_name: a.out_name.clone(),
                    out_ty: a.out_ty.clone(),
                    udaf: a.udaf.clone(),
                })
                .collect(),
        },
        LogicalPlan::Sort { input, keys } => LogicalPlan::Sort {
            input: Box::new(rewrite_plan(input, mapping, all)),
            keys: keys
                .iter()
                .map(|k| crate::exec::SortKey {
                    expr: rewrite_expr(&k.expr, mapping, all),
                    descending: k.descending,
                })
                .collect(),
        },
        LogicalPlan::Limit { input, n } => LogicalPlan::Limit {
            input: Box::new(rewrite_plan(input, mapping, all)),
            n: *n,
        },
        LogicalPlan::Distinct { input } => LogicalPlan::Distinct {
            input: Box::new(rewrite_plan(input, mapping, all)),
        },
        LogicalPlan::Join { .. }
        | LogicalPlan::CrossJoinFilter { .. }
        | LogicalPlan::Values { .. } => plan.clone(),
        LogicalPlan::Window {
            input,
            partition_by,
            order_by,
            window_fns,
        } => LogicalPlan::Window {
            input: Box::new(rewrite_plan(input, mapping, all)),
            partition_by: partition_by
                .iter()
                .map(|p| rewrite_expr(p, mapping, all))
                .collect(),
            order_by: order_by
                .iter()
                .map(|(e, d)| (rewrite_expr(e, mapping, all), *d))
                .collect(),
            window_fns: window_fns.clone(),
        },
    }
}

fn rewrite_expr(e: &PhysExpr, mapping: &[Option<usize>], all: &[String]) -> PhysExpr {
    match e {
        PhysExpr::Column { idx, name, ty } => {
            // 非表列（聚合输出列等）：idx 语义属上层算子，保持不变
            let is_table_col = all.iter().any(|a| a.eq_ignore_ascii_case(name.as_str()));
            PhysExpr::Column {
                idx: if is_table_col {
                    mapping[*idx].unwrap_or(*idx)
                } else {
                    *idx
                },
                name: name.clone(),
                ty: ty.clone(),
            }
        }
        PhysExpr::Lit(l) => PhysExpr::Lit(l.clone()),
        PhysExpr::Arith { op, l, r, ty } => PhysExpr::Arith {
            op: *op,
            l: Box::new(rewrite_expr(l, mapping, all)),
            r: Box::new(rewrite_expr(r, mapping, all)),
            ty: ty.clone(),
        },
        PhysExpr::Cmp { op, l, r } => PhysExpr::Cmp {
            op: *op,
            l: Box::new(rewrite_expr(l, mapping, all)),
            r: Box::new(rewrite_expr(r, mapping, all)),
        },
        PhysExpr::And(a, b) => PhysExpr::And(
            Box::new(rewrite_expr(a, mapping, all)),
            Box::new(rewrite_expr(b, mapping, all)),
        ),
        PhysExpr::Or(a, b) => PhysExpr::Or(
            Box::new(rewrite_expr(a, mapping, all)),
            Box::new(rewrite_expr(b, mapping, all)),
        ),
        PhysExpr::Not(a) => PhysExpr::Not(Box::new(rewrite_expr(a, mapping, all))),
        PhysExpr::IsNull(a, neg) => PhysExpr::IsNull(Box::new(rewrite_expr(a, mapping, all)), *neg),
        PhysExpr::Cast { e, ty } => PhysExpr::Cast {
            e: Box::new(rewrite_expr(e, mapping, all)),
            ty: ty.clone(),
        },
        PhysExpr::Fn { name, args, ty } => PhysExpr::Fn {
            name: name.clone(),
            args: args.iter().map(|a| rewrite_expr(a, mapping, all)).collect(),
            ty: ty.clone(),
        },
    }
}

fn build_inner(
    db: &Qsdb,
    plan: &LogicalPlan,
    scan_proj: Option<Vec<String>>,
    ctx: &Arc<crate::query::QueryContext>,
) -> Result<Box<dyn ExecNode>> {
    match plan {
        LogicalPlan::Scan { def, predicate } => {
            let scan = db.scan(ScanRequest {
                table: def.name.clone(),
                predicate: predicate.clone(),
                projection: scan_proj,
                ..Default::default()
            })?;
            Ok(crate::exec::par_scan::make_scan(scan, ctx))
        }
        LogicalPlan::Filter { input, predicate } => Ok(Box::new(FilterExec::new(
            build_inner(db, input, scan_proj.clone(), ctx)?,
            predicate.clone(),
            Arc::clone(ctx),
        ))),
        LogicalPlan::Project {
            input,
            exprs,
            names,
        } => Ok(Box::new(ProjectExec::new(
            build_inner(db, input, scan_proj.clone(), ctx)?,
            exprs.clone(),
            names.clone(),
        ))),
        LogicalPlan::Aggregate { input, group, aggs } => {
            build_aggregate(db, input, group, aggs, &scan_proj, ctx)
        }
        LogicalPlan::Sort { input, keys } => Ok(Box::new(SortExec::new(
            build_inner(db, input, scan_proj.clone(), ctx)?,
            keys.clone(),
            None,
            Arc::clone(ctx),
        ))),
        LogicalPlan::Limit { input, n } => Ok(Box::new(LimitExec {
            input: build_inner(db, input, scan_proj.clone(), ctx)?,
            remaining: *n,
        })),
        LogicalPlan::Distinct { input } => Ok(Box::new(crate::exec::DistinctExec::new(
            build_inner(db, input, scan_proj.clone(), ctx)?,
        ))),
        LogicalPlan::Window {
            input,
            partition_by,
            order_by,
            window_fns,
        } => {
            let input_exec = build_inner(db, input, None, ctx)?;
            Ok(Box::new(crate::exec::window::WindowExec::new(
                input_exec,
                partition_by.clone(),
                order_by.clone(),
                window_fns.clone(),
                Arc::clone(ctx),
            )))
        }
        LogicalPlan::Values { columns, cte_id } => {
            let schema = Arc::new(arrow::datatypes::Schema::new(
                columns
                    .iter()
                    .map(|(n, t)| arrow::datatypes::Field::new(n, t.arrow_type(), true))
                    .collect::<Vec<_>>(),
            ));
            let batches = CTE_BATCHES
                .with(|c| c.borrow().get(cte_id).cloned())
                .unwrap_or_default();
            Ok(Box::new(crate::exec::ValuesExec::new(schema, batches)))
        }
        LogicalPlan::CrossJoinFilter {
            left,
            right,
            predicate,
        } => {
            let probe = build_inner(db, left, None, ctx)?;
            let build = build_inner(db, right, None, ctx)?;
            // cross：probe 批 × build 全表，再按谓词过滤
            // 实现为：FilterExec { input: CrossJoinExec } —— CrossJoinExec 内联在此
            let cross = crate::exec::join::CrossJoinExec::new(probe, build, Arc::clone(ctx));
            Ok(Box::new(FilterExec::new(
                Box::new(cross),
                predicate.clone(),
                Arc::clone(ctx),
            )))
        }
        LogicalPlan::Join {
            left,
            right,
            left_keys,
            right_keys,
            join_type,
        } => {
            let probe = build_inner(db, left, None, ctx)?;
            let build = build_inner(db, right, None, ctx)?;
            // 右键 idx 为全局 ctx 序（左宽 + 局部）→ 减 probe 宽度得右扫描局部序
            let left_width = probe.schema().fields().len();
            let local_keys: Vec<PhysExpr> = right_keys
                .iter()
                .map(|k| match k {
                    PhysExpr::Column { idx, name, ty } => PhysExpr::Column {
                        idx: idx.saturating_sub(left_width),
                        name: name.clone(),
                        ty: ty.clone(),
                    },
                    other => other.clone(),
                })
                .collect();
            Ok(Box::new(crate::exec::join::HashJoinExec::new(
                probe,
                build,
                left_keys.clone(),
                local_keys,
                *join_type,
                Arc::clone(ctx),
            )))
        }
    }
}

/// 聚合构建：含 UDAF → 串行物化路径；可分片源 → 并行 partial/merge；否则串行。
fn build_aggregate(
    db: &Qsdb,
    input: &LogicalPlan,
    group: &[PhysExpr],
    aggs: &[AggExpr],
    scan_proj: &Option<Vec<String>>,
    ctx: &Arc<crate::query::QueryContext>,
) -> Result<Box<dyn ExecNode>> {
    if aggs
        .iter()
        .any(|a| matches!(a.func, crate::expr::AggFunc::Udaf))
    {
        return Ok(Box::new(crate::exec::udaf_agg::UdafAggExec::new(
            build_inner(db, input, scan_proj.clone(), ctx)?,
            group.to_vec(),
            aggs.to_vec(),
            Arc::clone(ctx),
        )));
    }
    let Some((shards, filters)) = try_shard_source(db, input, scan_proj, ctx) else {
        return Ok(Box::new(HashAggExec::new(
            build_inner(db, input, scan_proj.clone(), ctx)?,
            group.to_vec(),
            aggs.to_vec(),
            Arc::clone(ctx),
        )?));
    };

    let mut fields: Vec<arrow::datatypes::Field> = group
        .iter()
        .map(|g| arrow::datatypes::Field::new(g.display(), g.data_type().arrow_type(), true))
        .collect();
    for a in aggs {
        fields.push(arrow::datatypes::Field::new(
            &a.out_name,
            a.out_ty.arrow_type(),
            true,
        ));
    }
    let schema = Arc::new(arrow::datatypes::Schema::new(fields));

    let group = group.to_vec();
    let aggs = aggs.to_vec();

    use rayon::prelude::*;
    let tables: Vec<GroupTable> = shards
        .into_par_iter()
        .map(|s| run_partial(s, &filters, &group, &aggs, ctx))
        .collect::<Result<Vec<_>>>()?;

    // merge（单线程）
    let mut merged = GroupTable::new();
    merged.set_arity(aggs.len());
    for t in tables {
        merged.merge(t, aggs.len());
    }
    if merged.is_empty() {
        return Ok(Box::new(OneShotExec {
            batch: None,
            schema,
        }));
    }
    let keys = merged.take_group_keys();
    let states = merged.take_states();
    let batch = crate::exec::agg::finalize(&keys, &states, &aggs, &group, &schema)?;
    Ok(Box::new(OneShotExec {
        batch: Some(batch),
        schema,
    }))
}

/// 单分片 partial 聚合（当前线程执行；由 rayon 并行调度调用方决定）。
fn run_partial(
    scan: crate::scan::Scan,
    filters: &[PhysExpr],
    group: &[PhysExpr],
    aggs: &[AggExpr],
    ctx: &Arc<crate::query::QueryContext>,
) -> Result<GroupTable> {
    let mut node: Box<dyn ExecNode> = Box::new(ScanExec::new(scan));
    for f in filters {
        node = Box::new(FilterExec::new(node, f.clone(), Arc::clone(ctx)));
    }
    let mut exec = HashAggExec::new(node, group.to_vec(), aggs.to_vec(), Arc::clone(ctx))?;
    while let Some(b) = exec.next() {
        b?; // 取消/超时/内存超限在此冒泡
    }
    Ok(std::mem::take(&mut exec.state))
}

/// 源分片：`(Scan | Filter)+` 链且 scan 可切多片 → Some((分片, 逆序 filters))。
#[allow(clippy::too_many_arguments)]
fn try_shard_source(
    db: &Qsdb,
    plan: &LogicalPlan,
    scan_proj: &Option<Vec<String>>,
    ctx: &Arc<crate::query::QueryContext>,
) -> Option<(Vec<crate::scan::Scan>, Vec<PhysExpr>)> {
    let mut filters: Vec<PhysExpr> = Vec::new();
    let mut cur = plan;
    loop {
        match cur {
            LogicalPlan::Filter { input, predicate } => {
                filters.push(predicate.clone());
                cur = input;
            }
            LogicalPlan::Scan { def, predicate } => {
                let scan = db
                    .scan_with_context(
                        ScanRequest {
                            table: def.name.clone(),
                            predicate: predicate.clone(),
                            projection: scan_proj.clone(),
                            ..Default::default()
                        },
                        Arc::clone(ctx),
                    )
                    .ok()?;
                let threads = rayon::current_num_threads().clamp(1, 16);
                let shards = scan.shard(threads)?;
                return Some((shards, filters));
            }
            _ => return None,
        }
    }
}
