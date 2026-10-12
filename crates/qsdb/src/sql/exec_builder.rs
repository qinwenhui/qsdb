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
        // 下推进 Scan 的谓词也引用数据列——必须收集，否则投影被裁到只剩分区列
        // （M4 期 bug：数据列谓词静默失效——WHERE a>0 读不到 a 列按恒真处理）
        LogicalPlan::Scan { predicate, .. } | LogicalPlan::ExternalScan { predicate, .. } => {
            if let Some(p) = predicate {
                let mut cols = std::collections::HashSet::new();
                p.referenced_columns(&mut cols);
                for c in cols {
                    if !out.iter().any(|u| u.eq_ignore_ascii_case(&c)) {
                        out.push(c);
                    }
                }
            }
        }
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
        LogicalPlan::Join {
            left,
            left_keys,
            join_type,
            ..
        } if join_type.is_semi_family() => {
            // semi 族输出 = 左 schema：可安全裁剪左扫描；连接键列必须保留
            for k in left_keys {
                collect_expr_columns(k, out);
            }
            collect_plan_columns(left, out);
        }
        LogicalPlan::Join { .. } | LogicalPlan::CrossJoinFilter { .. } => {
            // INNER/LEFT 多表计划不走列裁剪下推（build 的 count_scans guard 兜底）
        }
        LogicalPlan::Values { .. } | LogicalPlan::Window { .. } | LogicalPlan::OneRow => {} // 内存/窗口源无需裁剪
        LogicalPlan::SetOp { left, right, .. } => {
            collect_plan_columns(left, out);
            collect_plan_columns(right, out);
        }
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
        PhysExpr::Case {
            conds, else_expr, ..
        } => {
            for (c, r) in conds {
                collect_expr_columns(c, out);
                collect_expr_columns(r, out);
            }
            collect_expr_columns(else_expr, out);
        }
        PhysExpr::Coalesce(exprs, _) => exprs.iter().for_each(|e| collect_expr_columns(e, out)),
        PhysExpr::Concat(l, r) => {
            collect_expr_columns(l, out);
            collect_expr_columns(r, out);
        }
        PhysExpr::Like { e, .. } => collect_expr_columns(e, out),
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
        LogicalPlan::Scan { def, .. } | LogicalPlan::ExternalScan { def, .. } => {
            Some(Arc::clone(def))
        }
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Distinct { input } => plan_scan_def(input),
        LogicalPlan::Join {
            left, join_type, ..
        } if join_type.is_semi_family() => plan_scan_def(left),
        LogicalPlan::Join { .. }
        | LogicalPlan::CrossJoinFilter { .. }
        | LogicalPlan::Values { .. }
        | LogicalPlan::OneRow
        | LogicalPlan::SetOp { .. } => None,
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
        LogicalPlan::Join {
            left,
            right,
            left_keys,
            right_keys,
            join_type,
        } if join_type.is_semi_family() => LogicalPlan::Join {
            left: Box::new(rewrite_plan(left, mapping, all)),
            // 右键为右子计划局部 idx（绑定期约定），裁剪不触碰
            right: Box::new((**right).clone()),
            left_keys: left_keys
                .iter()
                .map(|k| rewrite_expr(k, mapping, all))
                .collect(),
            right_keys: right_keys.clone(),
            join_type: *join_type,
        },
        LogicalPlan::ExternalScan {
            def,
            files,
            predicate,
        } => LogicalPlan::ExternalScan {
            def: Arc::clone(def),
            files: Arc::clone(files),
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
        | LogicalPlan::Values { .. }
        | LogicalPlan::OneRow => plan.clone(),
        LogicalPlan::SetOp {
            left,
            right,
            op,
            all: all_flag,
        } => LogicalPlan::SetOp {
            left: Box::new(rewrite_plan(left, mapping, all)),
            right: Box::new(rewrite_plan(right, mapping, all)),
            op: *op,
            all: *all_flag,
        },
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
        PhysExpr::Case {
            conds,
            else_expr,
            ty,
        } => PhysExpr::Case {
            conds: conds
                .iter()
                .map(|(c, r)| (rewrite_expr(c, mapping, all), rewrite_expr(r, mapping, all)))
                .collect(),
            else_expr: Box::new(rewrite_expr(else_expr, mapping, all)),
            ty: ty.clone(),
        },
        PhysExpr::Coalesce(exprs, ty) => PhysExpr::Coalesce(
            exprs
                .iter()
                .map(|e| rewrite_expr(e, mapping, all))
                .collect(),
            ty.clone(),
        ),
        PhysExpr::Concat(l, r) => PhysExpr::Concat(
            Box::new(rewrite_expr(l, mapping, all)),
            Box::new(rewrite_expr(r, mapping, all)),
        ),
        PhysExpr::Like {
            e,
            pattern,
            negated,
            ci,
        } => PhysExpr::Like {
            e: Box::new(rewrite_expr(e, mapping, all)),
            pattern: pattern.clone(),
            negated: *negated,
            ci: *ci,
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
        LogicalPlan::ExternalScan {
            def,
            files,
            predicate,
        } => {
            let scan = db.external_scan(
                Arc::clone(def),
                files.as_ref().clone(),
                scan_proj,
                predicate.clone(),
                Arc::clone(ctx),
            )?;
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
        LogicalPlan::OneRow => Ok(Box::new(crate::exec::OneRowExec::new())),
        LogicalPlan::SetOp {
            left,
            right,
            op,
            all,
        } => {
            // 集合操作内含多 Scan：不走列裁剪（build 侧 scan_proj=None 传全列）
            let l = build_inner(db, left, None, ctx)?;
            let r = build_inner(db, right, None, ctx)?;
            let exec = crate::exec::SetOpExec::new(l, r, *op, ctx);
            match (*op, *all) {
                // UNION distinct → DistinctExec 包装去重
                (crate::exec::SetOpKind::Union, false) => {
                    Ok(Box::new(crate::exec::DistinctExec::new(Box::new(exec))))
                }
                _ => Ok(Box::new(exec)),
            }
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
            // semi 族：左子继承裁剪投影（build() 对 semi 计划做了列裁剪——
            // 传 None 会读全列使左键 idx 错位；INNER/LEFT 无上游裁剪，None 等价）
            let probe = build_inner(
                db,
                left,
                if join_type.is_semi_family() {
                    scan_proj.clone()
                } else {
                    None
                },
                ctx,
            )?;
            let build = build_inner(db, right, None, ctx)?;
            // 右键 idx：INNER/LEFT 为全局 ctx 序（减 probe 宽度得右局部序）；
            // semi 族绑定期即用右子计划局部序（列裁剪改写左宽后仍正确）
            let local_keys: Vec<PhysExpr> = if join_type.is_semi_family() {
                right_keys.clone()
            } else {
                let left_width = probe.schema().fields().len();
                right_keys
                    .iter()
                    .map(|k| match k {
                        PhysExpr::Column { idx, name, ty } => PhysExpr::Column {
                            idx: idx.saturating_sub(left_width),
                            name: name.clone(),
                            ty: ty.clone(),
                        },
                        other => other.clone(),
                    })
                    .collect()
            };
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
    // ── count(*) over INNER 等值 JOIN：计数下推（T3 优化）──
    // 低基数键下 JOIN 输出行数 = Σ左键匹配数，物化代价 n²/keys。
    // 恒等变换为「右键计数 + 左键累加」→ O(n)、build 侧只物化键列。
    if group.is_empty()
        && aggs.len() == 1
        && matches!(aggs[0].func, crate::expr::AggFunc::Count)
        && aggs[0].arg.is_none()
    {
        if std::env::var("QSDB_JOIN_PROF").is_ok() {
            eprintln!(
                "[join-prof] count(*) agg, input = {}",
                match input {
                    LogicalPlan::Join { .. } => "Join",
                    LogicalPlan::Project { .. } => "Project",
                    LogicalPlan::Filter { .. } => "Filter",
                    LogicalPlan::Values { .. } => "Values",
                    LogicalPlan::Scan { .. } => "Scan",
                    _ => "other",
                }
            );
        }
        if let LogicalPlan::Join {
            left,
            right,
            left_keys,
            right_keys,
            join_type,
        } = input
        {
            // INNER：Σ匹配数；LEFT：Σ max(匹配数, 1)（未匹配行补 NULL 仍计 1）
            let left_outer = matches!(join_type, crate::exec::join::JoinType::LeftOuter);
            if !left_outer && !matches!(join_type, crate::exec::join::JoinType::Inner) {
                // semi/anti 族等不适用（输出语义不同）——交给常规路径
            } else {
                // 键列投影：join 只需键列（扫描量 5 列 → 1 列）。
                // idx 两层转换：① right_keys 全局序 → 右表序（减左表**全**宽）
                // ② 两侧全表序 → 投影序（rewrite_expr，仅纯 Scan 无谓词时生效）。
                // 左表**原始**输出宽度（不限 Scan——CTE/派生表/Join 左侧同样适用）
                let l_full = crate::sql::translate::plan_output_columns(left)?.len();
                let r_local: Vec<PhysExpr> = right_keys
                    .iter()
                    .map(|k| match k {
                        PhysExpr::Column { idx, name, ty } => PhysExpr::Column {
                            idx: idx.saturating_sub(l_full),
                            name: name.clone(),
                            ty: ty.clone(),
                        },
                        other => other.clone(),
                    })
                    .collect();
                // 纯 Scan 无谓词 → 键列投影（否则回退全列）
                let project =
                    |pl: &LogicalPlan,
                     keys: &[PhysExpr]|
                     -> (LogicalPlan, Vec<PhysExpr>, Option<Vec<String>>) {
                        let LogicalPlan::Scan {
                            def,
                            predicate: None,
                        } = pl
                        else {
                            return (pl.clone(), keys.to_vec(), None);
                        };
                        let all: Vec<String> = def.columns.iter().map(|c| c.name.clone()).collect();
                        let mut cols: Vec<String> = Vec::new();
                        for k in keys {
                            let mut raw: Vec<String> = Vec::new();
                            collect_expr_columns(k, &mut raw);
                            for n in raw {
                                // 去表限定（`rr.k` → `k`）：def.columns 存非限定名
                                let unq = n.rsplit('.').next().unwrap_or(&n).to_string();
                                if !cols.iter().any(|c| c.eq_ignore_ascii_case(&unq)) {
                                    cols.push(unq);
                                }
                            }
                        }
                        // 分区列必须保留（Scan 从目录重建）
                        for pk in &def.partition_keys {
                            if !cols.iter().any(|c| c.eq_ignore_ascii_case(pk)) {
                                cols.push(pk.clone());
                            }
                        }
                        if std::env::var("QSDB_JOIN_PROF").is_ok() {
                            eprintln!(
                                "[join-prof] scan all={} keycols={:?} project={}",
                                all.len(),
                                cols,
                                cols.len() < all.len()
                            );
                        }
                        if cols.len() >= all.len() {
                            return (pl.clone(), keys.to_vec(), None);
                        }
                        let mapping = build_mapping(&all, &cols);
                        let lp = rewrite_plan(pl, &mapping, &all);
                        let k2: Vec<PhysExpr> = keys
                            .iter()
                            .map(|k| rewrite_expr(k, &mapping, &all))
                            .collect();
                        (lp, k2, Some(cols))
                    };
                let (lp, lkeys2, lproj) = project(left, left_keys);
                let (rp, rkeys2, rproj) = project(right, &r_local);
                let probe = build_inner(db, &lp, lproj, ctx)?;
                let build = build_inner(db, &rp, rproj, ctx)?;
                let left_width = probe.schema().fields().len();
                let _ = left_width; // 投影已含 idx 转换，此处不再减宽
                return Ok(Box::new(crate::exec::join::JoinCountExec::new(
                    probe,
                    build,
                    lkeys2,
                    rkeys2,
                    left_outer,
                    Arc::clone(ctx),
                )));
            }
        }
    }
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
    let Some((shards, semis, filters)) = try_shard_source(db, input, scan_proj, ctx) else {
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

    // ── semi/anti 融合路径：存在集 build 一次全分片共享；probe 批上
    // 直接求掩码 → 行选择 → 掩码摄入聚合（零中间物化、零 take）──
    if !semis.is_empty() {
        let probes: Vec<SemiProbe> = semis
            .iter()
            .map(|st| {
                let build = build_inner(db, &st.right, None, ctx)?;
                // semi 右键 = 右子计划局部 idx（绑定期约定）
                let state = crate::exec::join::prepare_exists_state(build, &st.right_keys, ctx)?;
                Ok(SemiProbe {
                    left_keys: st.left_keys.clone(),
                    state,
                    join_type: st.join_type,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let tables: Vec<GroupTable> = shards
            .into_par_iter()
            .map(|s| run_partial_fused(s, &probes, &filters, &group, &aggs, ctx))
            .collect::<Result<Vec<_>>>()?;
        return finish_partitioned_merge(tables, group, aggs, schema);
    }

    use rayon::prelude::*;
    let tables: Vec<GroupTable> = shards
        .into_par_iter()
        .map(|s| run_partial(db, s, &semis, &filters, &group, &aggs, ctx))
        .collect::<Result<Vec<_>>>()?;

    // merge：按组 key hash 分区并行（每分区独立合并全部 shard 的对应组——
    // 分区表小缓存友好；原本单线程串行 merge 是高组数下的瓶颈），
    // 各分区各自 finalize 后按列 concat。
    let p = rayon::current_num_threads().clamp(1, 32);
    // 分区主序桶：parts[pi][ti] = 第 ti 张表中 hash 落 pi 分区的组下标
    let mut parts: Vec<Vec<Vec<usize>>> = vec![vec![Vec::new(); tables.len()]; p];
    for (ti, t) in tables.iter().enumerate() {
        for g in 0..t.len() {
            parts[(t.group_hash(g) as usize) % p][ti].push(g);
        }
    }
    let group_ref = &group;
    let aggs_ref = &aggs;
    let schema_ref = &schema;
    let batches: Vec<Option<RecordBatch>> = parts
        .into_par_iter()
        .map(|mut table_buckets| {
            let mut merged = GroupTable::new();
            for (ti, t) in tables.iter().enumerate() {
                let ogs = std::mem::take(&mut table_buckets[ti]);
                if !ogs.is_empty() {
                    merged.merge_subset(t, ogs, aggs_ref.len());
                }
            }
            if merged.is_empty() {
                return Ok(None);
            }
            crate::exec::agg::finalize_table(&merged, aggs_ref, group_ref, schema_ref).map(Some)
        })
        .collect::<Result<Vec<_>>>()?;
    let _ = &tables;
    let some_batches: Vec<RecordBatch> = batches.into_iter().flatten().collect();
    if some_batches.is_empty() {
        return Ok(Box::new(OneShotExec {
            batch: None,
            schema,
        }));
    }
    let batch = if some_batches.len() == 1 {
        some_batches.into_iter().next().expect("len checked")
    } else {
        let cols = some_batches[0].num_columns();
        let mut out_cols = Vec::with_capacity(cols);
        for c in 0..cols {
            let slices: Vec<&dyn arrow::array::Array> =
                some_batches.iter().map(|b| b.column(c).as_ref()).collect();
            out_cols.push(
                arrow::compute::concat(&slices)
                    .map_err(|e| crate::error::QsdbError::invalid(format!("agg concat: {e}")))?,
            );
        }
        let n: usize = some_batches.iter().map(|b| b.num_rows()).sum();
        arrow::record_batch::RecordBatch::try_new_with_options(
            Arc::clone(&schema),
            out_cols,
            &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(n)),
        )
        .map_err(|e| crate::error::QsdbError::invalid(format!("agg output concat: {e}")))?
    };
    Ok(Box::new(OneShotExec {
        batch: Some(batch),
        schema,
    }))
}

/// 单分片 partial 聚合（当前线程执行；由 rayon 并行调度调用方决定）。
/// 预构建的存在集探针（融合路径全分片共享 Arc）。
struct SemiProbe {
    left_keys: Vec<PhysExpr>,
    state: std::sync::Arc<crate::exec::join::ExistsState>,
    join_type: crate::exec::join::JoinType,
}

/// 融合 partial：scan 批 → semi 掩码（AND 组合）→ 过滤谓词掩码 →
/// 掩码摄入聚合。无中间批物化——与 DuckDB 的 filter-into-scan 聚合
/// 融合同构，且存在集探测是免哈希的（i64 集或位图）。
fn run_partial_fused(
    scan: crate::scan::Scan,
    probes: &[SemiProbe],
    filters: &[PhysExpr],
    group: &[PhysExpr],
    aggs: &[AggExpr],
    ctx: &Arc<crate::query::QueryContext>,
) -> Result<GroupTable> {
    use crate::exec::ExecNode;
    use arrow::array::Array;
    // 分片任务内串行拉取（ScanExec）——不用 ParScanExec：嵌套并行会与
    // 分片任务互锁同一 rayon 池（M4 教训；mask+ingest 实测仅 0.07s，
    // 时间全耗在嵌套并行等待）
    let mut node: Box<dyn ExecNode> = Box::new(crate::exec::ScanExec::new(scan));
    let mut table = GroupTable::default();
    while let Some(b) = node.next() {
        ctx.check()?;
        let batch = b?;
        if batch.num_rows() == 0 {
            continue;
        }
        // 行选择 = semi 掩码 ∧ 过滤谓词（**每批一次求值**再逐行组合——
        // 掩码求值是整列向量化，逐行重算整列 = O(n²) 灾难）
        let mut masks: Vec<arrow::array::BooleanArray> =
            Vec::with_capacity(probes.len() + filters.len());
        for p in probes {
            masks.push(crate::exec::join::semi_mask(
                &p.state,
                &p.left_keys,
                &batch,
                p.join_type,
                ctx,
            )?);
        }
        for f in filters {
            let m = crate::expr::eval::eval(f, &batch)?;
            let ba = m
                .as_any()
                .downcast_ref::<arrow::array::BooleanArray>()
                .ok_or_else(|| crate::error::QsdbError::type_err("filter must be boolean"))?;
            masks.push(ba.clone());
        }
        let mut sel: Vec<u32> = Vec::with_capacity(batch.num_rows());
        'outer: for row in 0..batch.num_rows() {
            for m in &masks {
                if m.is_null(row) || !m.value(row) {
                    continue 'outer;
                }
            }
            sel.push(row as u32);
        }
        table.ingest_with_sel(group, aggs, &batch, &sel)?;
    }
    Ok(table)
}

/// 分片合并 + 分区并行 finalize（融合路径）：与主路径 merge 块同款。
fn finish_partitioned_merge(
    tables: Vec<GroupTable>,
    group: Vec<PhysExpr>,
    aggs: Vec<AggExpr>,
    schema: Arc<arrow::datatypes::Schema>,
) -> Result<Box<dyn ExecNode>> {
    use rayon::prelude::*;
    let p = rayon::current_num_threads().clamp(1, 32);
    let mut parts: Vec<Vec<Vec<usize>>> = vec![vec![Vec::new(); tables.len()]; p];
    for (ti, t) in tables.iter().enumerate() {
        for g in 0..t.len() {
            parts[(t.group_hash(g) as usize) % p][ti].push(g);
        }
    }
    let aggs_ref = &aggs;
    let group_ref = &group;
    let schema_ref = &schema;
    let batches: Vec<Option<RecordBatch>> = parts
        .into_par_iter()
        .map(|mut table_buckets| {
            let mut merged = GroupTable::new();
            for (ti, t) in tables.iter().enumerate() {
                let ogs = std::mem::take(&mut table_buckets[ti]);
                if !ogs.is_empty() {
                    merged.merge_subset(t, ogs, aggs_ref.len());
                }
            }
            if merged.is_empty() {
                return Ok(None);
            }
            crate::exec::agg::finalize_table(&merged, aggs_ref, group_ref, schema_ref).map(Some)
        })
        .collect::<Result<Vec<_>>>()?;
    let some_batches: Vec<RecordBatch> = batches.into_iter().flatten().collect();
    if some_batches.is_empty() {
        return Ok(Box::new(OneShotExec {
            batch: None,
            schema,
        }));
    }
    let batch = if some_batches.len() == 1 {
        some_batches.into_iter().next().expect("len checked")
    } else {
        crate::exec::spill::concat_batches(&some_batches, &schema)?
    };
    Ok(Box::new(OneShotExec {
        batch: Some(batch),
        schema,
    }))
}

/// 分片管线中的 semi/anti 阶段（build 侧计划持引用，每分片重建 ExecNode
/// ——dims 级小表重建成本可忽略）。
struct SemiStage {
    left_keys: Vec<PhysExpr>,
    right: std::sync::Arc<LogicalPlan>,
    /// 全局 ctx 序（构建时减 probe 宽度）
    right_keys: Vec<PhysExpr>,
    join_type: crate::exec::join::JoinType,
}

fn run_partial(
    db: &Qsdb,
    scan: crate::scan::Scan,
    semis: &[SemiStage],
    filters: &[PhysExpr],
    group: &[PhysExpr],
    aggs: &[AggExpr],
    ctx: &Arc<crate::query::QueryContext>,
) -> Result<GroupTable> {
    let mut node: Box<dyn ExecNode> = Box::new(ScanExec::new(scan));
    // semi 阶段按「自底向上」应用（try_shard_source 剥离序为自顶向下）
    for st in semis.iter().rev() {
        let build = build_inner(db, &st.right, None, ctx)?;
        let left_width = node.schema().fields().len();
        let local_keys: Vec<PhysExpr> = st
            .right_keys
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
        node = Box::new(crate::exec::join::HashJoinExec::new(
            node,
            build,
            st.left_keys.clone(),
            local_keys,
            st.join_type,
            Arc::clone(ctx),
        ));
    }
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
) -> Option<(Vec<crate::scan::Scan>, Vec<SemiStage>, Vec<PhysExpr>)> {
    let mut filters: Vec<PhysExpr> = Vec::new();
    let mut semis: Vec<SemiStage> = Vec::new();
    let mut cur = plan;
    loop {
        match cur {
            LogicalPlan::Filter { input, predicate } => {
                filters.push(predicate.clone());
                cur = input;
            }
            // semi/anti 族穿透：左链继续分片，右侧计划记为 build 阶段
            LogicalPlan::Join {
                left,
                right,
                left_keys,
                right_keys,
                join_type,
            } if join_type.is_semi_family() => {
                semis.push(SemiStage {
                    left_keys: left_keys.clone(),
                    right: std::sync::Arc::new((**right).clone()),
                    right_keys: right_keys.clone(),
                    join_type: *join_type,
                });
                cur = left;
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
                let shards = scan.shard(threads, false)?;
                return Some((shards, semis, filters));
            }
            LogicalPlan::ExternalScan {
                def,
                files,
                predicate,
            } => {
                let scan = db
                    .external_scan(
                        Arc::clone(def),
                        files.as_ref().clone(),
                        scan_proj.clone(),
                        predicate.clone(),
                        Arc::clone(ctx),
                    )
                    .ok()?;
                let threads = rayon::current_num_threads().clamp(1, 16);
                let shards = scan.shard(threads, false)?;
                return Some((shards, semis, filters));
            }
            _ => return None,
        }
    }
}
