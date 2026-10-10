# -*- coding: utf-8 -*-
import io

p = 'crates/qsdb/src/sql/translate.rs'
s = io.open(p, encoding='utf-8').read()

# 1) LogicalPlan::Join
s = s.replace('''    Limit { input: Box<LogicalPlan>, n: usize },
    Distinct { input: Box<LogicalPlan> },
}''', '''    Limit { input: Box<LogicalPlan>, n: usize },
    Distinct { input: Box<LogicalPlan> },
    /// 等值连接（left=probe right=build）；输出列 = 左 ++ 右
    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        left_keys: Vec<PhysExpr>,
        right_keys: Vec<PhysExpr>,
        join_type: crate::exec::join::JoinType,
    },
}''')

# 2) FROM + JOIN 链（含移除旧 guard）
old_from = '''    if select.from.len() != 1 || !select.from[0].joins.is_empty() {
        return Err(QsdbError::unsupported("JOIN/multi-FROM lands in M4"));
    }
    let table = match &select.from[0].relation {
        TableFactor::Table { name, .. } => object_name_single(name.clone())?,
        other => {
            return Err(QsdbError::unsupported(format!(
                "FROM {other:?} not supported"
            )));
        }
    };
    let def = (resolver.resolve)(&table)?;
    let ctx = SchemaCtx::from_table(&def);'''
new_from = '''    if select.from.len() != 1 {
        return Err(QsdbError::unsupported("multi-FROM (comma join) not supported"));
    }
    let table = match &select.from[0].relation {
        TableFactor::Table { name, .. } => object_name_single(name.clone())?,
        other => {
            return Err(QsdbError::unsupported(format!(
                "FROM {other:?} not supported"
            )));
        }
    };
    let def = (resolver.resolve)(&table)?;
    let mut ctx = SchemaCtx::from_table(&def);'''
assert old_from in s
s = s.replace(old_from, new_from)

# JOIN 链处理：插在「let mut plan = LogicalPlan::Scan」之后、WHERE 之前
old_plan = '''    let mut plan = LogicalPlan::Scan { def: Arc::clone(&def), predicate: None };

    // WHERE'''
new_plan = '''    let mut plan = LogicalPlan::Scan { def: Arc::clone(&def), predicate: None };

    // JOIN 链（SQL-2：等值 INNER/LEFT）
    for join in &select.from[0].joins {
        let (jt, constraint) = match &join.join_operator {
            sqlparser::ast::JoinOperator::Inner(c) => (crate::exec::join::JoinType::Inner, c),
            sqlparser::ast::JoinOperator::LeftOuter(c) => {
                (crate::exec::join::JoinType::LeftOuter, c)
            }
            other => {
                return Err(QsdbError::unsupported(format!(
                    "JOIN type {other:?} not supported (INNER/LEFT only)"
                )))
            }
        };
        // 右表（别名优先）
        let right_name = match &join.relation {
            TableFactor::Table { name, alias, .. } => {
                match alias {
                    Some(a) => a.name.value.clone(),
                    None => object_name_single(name.clone())?,
                }
            }
            other => return Err(QsdbError::unsupported(format!("JOIN target {other:?}"))),
        };
        let right_def = (resolver.resolve)(&right_name)?;
        let sqlparser::ast::JoinConstraint::On(on_expr) = constraint else {
            return Err(QsdbError::unsupported("JOIN ... ON <equi-cond> required"));
        };
        // 扩展 ctx：右表列（限定名 + 唯一非限定名），先记 n_left
        let n_left = ctx.cols.iter().filter(|(n, _)| !n.contains('.')).count();
        for c in &right_def.columns {
            ctx.cols
                .push((format!("{right_name}.{}", c.name), c.logical_type.clone()));
        }
        // 非限定名：仅当全局唯一时补注册；歧义则此前注册也作废
        for c in &right_def.columns {
            let left_plain = plan_left_plain_count(&ctx, &right_name, &c.name);
            if left_plain == 0 && ctx_plain_count(&ctx, &c.name) == 0 {
                ctx.cols.push((c.name.clone(), c.logical_type.clone()));
            }
        }
        let (lkey, rkey) = split_equi_condition(on_expr, n_left, &ctx)?;
        let right_plan = LogicalPlan::Scan {
            def: Arc::clone(&right_def),
            predicate: None,
        };
        plan = LogicalPlan::Join {
            left: Box::new(plan),
            right: Box::new(right_plan),
            left_keys: vec![lkey],
            right_keys: vec![rkey],
            join_type: jt,
        };
    }

    // WHERE'''
assert old_plan in s
s = s.replace(old_plan, new_plan)

# 辅助：列名歧义计数 + 等值拆分
s = s.replace('''fn expr_display(e: &sqlparser::ast::Expr) -> String {''', '''/// 左表（非限定注册）中与 `col` 同名的列数。
fn plan_left_plain_count(ctx: &SchemaCtx, right_name: &str, col: &str) -> usize {
    // 左表列即「既非 right 限定又非限定重复」——精确做法：扫限定名归组
    // 简化：ctx 中不含 right_name 前缀的同名非限定/限定注册
    ctx.cols
        .iter()
        .filter(|(n, _)| {
            !n.starts_with(&format!("{right_name}."))
                && !n.contains('.')
                && n.eq_ignore_ascii_case(col)
        })
        .count()
}

/// ctx 中非限定名注册数。
fn ctx_plain_count(ctx: &SchemaCtx, col: &str) -> usize {
    ctx.cols
        .iter()
        .filter(|(n, _)| !n.contains('.') && n.eq_ignore_ascii_case(col))
        .count()
}

/// ON expr：等值条件 a.k = b.k 拆为（左键, 右键）。
/// idx < n_left 属左表。
fn split_equi_condition(
    e: &sqlparser::ast::Expr,
    n_left: usize,
    ctx: &SchemaCtx,
) -> Result<(PhysExpr, PhysExpr)> {
    use sqlparser::ast::Expr as E;
    let E::BinaryOp { left, op, right } = e else {
        return Err(QsdbError::unsupported("JOIN ON must be an equality"));
    };
    if !matches!(op, sqlparser::ast::BinaryOperator::Eq) {
        return Err(QsdbError::unsupported(
            "JOIN ON supports only equality (non-equi lands in M5)",
        ));
    }
    let lb = bind_expr(left, ctx)?;
    let rb = bind_expr(right, ctx)?;
    let side = |x: &PhysExpr| -> Option<bool> {
        match x {
            PhysExpr::Column { idx, .. } => Some(*idx < n_left),
            _ => None,
        }
    };
    match (side(&lb), side(&rb)) {
        (Some(true), Some(false)) => Ok((lb, rb)),
        (Some(false), Some(true)) => Ok((rb, lb)),
        _ => Err(QsdbError::invalid(
            "JOIN ON keys must be column references, one from each side",
        )),
    }
}

fn expr_display(e: &sqlparser::ast::Expr) -> String {''')

# 3) HAVING
old_having = '''    if select.having.is_some() {
        return Err(QsdbError::unsupported("HAVING lands in M4 (SQL-2)"));
    }'''
new_having = '''    // HAVING（SQL-2）：对聚合输出过滤
    let mut having_pred: Option<PhysExpr> = None;
    if let Some(h) = &select.having {
        if group_exprs.is_empty() && aggs.is_empty() {
            return Err(QsdbError::invalid("HAVING requires aggregation"));
        }
        let mut agg_ctx_cols: Vec<(String, LogicalType)> = group_exprs
            .iter()
            .map(|g| (g.display(), g.data_type()))
            .collect();
        for a in &aggs {
            agg_ctx_cols.push((a.out_name.clone(), a.out_ty.clone()));
        }
        let agg_ctx = SchemaCtx { cols: agg_ctx_cols };
        having_pred = Some(rebind_over_agg(h, &ctx, &group_exprs, &aggs, &agg_ctx)?);
        if !having_pred.as_ref().unwrap().is_boolean() {
            return Err(QsdbError::type_err("HAVING must be boolean"));
        }
    }'''
assert old_having in s
s = s.replace(old_having, new_having)

# HAVING Filter 插 Aggregate 后
old_agg = '''        plan = LogicalPlan::Aggregate {
            input: Box::new(plan),
            group: group_exprs,
            aggs,
        };
        plan = LogicalPlan::Project { input: Box::new(plan), exprs: proj_exprs, names: proj_names };'''
new_agg = '''        plan = LogicalPlan::Aggregate {
            input: Box::new(plan),
            group: group_exprs,
            aggs,
        };
        if let Some(hp) = having_pred {
            plan = LogicalPlan::Filter { input: Box::new(plan), predicate: hp };
        }
        plan = LogicalPlan::Project { input: Box::new(plan), exprs: proj_exprs, names: proj_names };'''
assert old_agg in s
s = s.replace(old_agg, new_agg)

io.open(p, 'w', encoding='utf-8').write(s)
print('ok')
