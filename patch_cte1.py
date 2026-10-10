# -*- coding: utf-8 -*-
import io

p = 'crates/qsdb/src/sql/translate.rs'
s = io.open(p, encoding='utf-8').read()

# 1) Values plan 变体
s = s.replace('''    /// 非等值 INNER：cross join（左 × 右）后在合并 schema 上过滤
    CrossJoinFilter {''','''    /// 内存批源（CTE 物化；执行层持有批数据，binder 只带 schema 描述）
    Values {
        columns: Vec<(String, LogicalType)>,
        /// 唯一 id（执行层按 id 从 CTE 批缓存取数）
        cte_id: u32,
    },
    /// 非等值 INNER：cross join（左 × 右）后在合并 schema 上过滤
    CrossJoinFilter {''')

# 2) WITH 处理 + FROM 主表 CTE 检查
old = '''    let def = (resolver.resolve)(&table)?;
    let ctx = SchemaCtx::from_table(&def);'''
new = '''    // CTE（非递归）：WITH name AS (...) 内联展开为子计划
    let mut cte_plans: std::collections::HashMap<String, (LogicalPlan, Vec<(String, LogicalType)>)> =
        std::collections::HashMap::new();
    if let Some(with) = &q.with {
        for cte in &with.ctes {
            let name = cte.alias.name.value.clone();
            if cte_plans.contains_key(&name) || (resolver.resolve)(&name).is_ok() {
                return Err(QsdbError::unsupported("recursive or duplicate CTE"));
            }
            let sub = translate_select_full(*cte.query.clone(), resolver)?;
            let cols = plan_output_columns(&sub)?;
            cte_plans.insert(name, (sub, cols));
        }
    }

    // FROM 主表：CTE 名优先
    let (plan, ctx): (LogicalPlan, SchemaCtx) =
        if let Some((sub, cols)) = cte_plans.get(&table) {
            (
                LogicalPlan::Values {
                    columns: cols.clone(),
                    cte_id: next_cte_id(),
                },
                SchemaCtx { cols: cols.clone() },
            )
        } else {
            let def = (resolver.resolve)(&table)?;
            (
                LogicalPlan::Scan { def: Arc::clone(&def), predicate: None },
                SchemaCtx::from_table(&def),
            )
        };'''
assert old in s, 'cte main'
s = s.replace(old, new, 1)

# 旧 Scan 创建行删除（被上面的合并替代）
s = s.replace('''    let mut plan = LogicalPlan::Scan {
        def: Arc::clone(&def),
        predicate: None,
    };

    // JOIN 链''', '''    let mut plan = plan;

    // JOIN 链''', 1)

# JOIN 右表也检查 CTE
s = s.replace('''        let right_def = (resolver.resolve)(&right_name)?;
        let sqlparser::ast::JoinConstraint::On(on_expr) = constraint else {''','''        // JOIN 右表 CTE 检查
        if cte_plans.contains_key(&right_name) {
            return Err(QsdbError::unsupported(
                "CTE as JOIN right side lands later",
            ));
        }
        let right_def = (resolver.resolve)(&right_name)?;
        let sqlparser::ast::JoinConstraint::On(on_expr) = constraint else {''')

# 3) 辅助：plan 输出列推导
s = s.replace('''pub struct TableResolver<'a> {''','''static CTE_ID: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
fn next_cte_id() -> u32 {
    CTE_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

/// 计划输出列（名字 + 类型）——CTE 内联绑定的列序来源。
pub fn plan_output_columns(plan: &LogicalPlan) -> Result<Vec<(String, LogicalType)>> {
    Ok(match plan {
        LogicalPlan::Scan { def, .. } => def
            .columns
            .iter()
            .map(|c| (c.name.clone(), c.logical_type.clone()))
            .collect(),
        LogicalPlan::Project { names, exprs, .. } => names
            .iter()
            .zip(exprs.iter())
            .map(|(n, e)| (n.clone(), e.data_type()))
            .collect(),
        LogicalPlan::Values { columns, .. } => columns.clone(),
        LogicalPlan::Aggregate { group, aggs, .. } => {
            let mut cols: Vec<(String, LogicalType)> = group
                .iter()
                .map(|g| (g.display(), g.data_type()))
                .collect();
            for a in aggs {
                cols.push((a.out_name.clone(), a.out_ty.clone()));
            }
            cols
        }
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Distinct { input } => plan_output_columns(input)?,
        LogicalPlan::Join { .. } | LogicalPlan::CrossJoinFilter { .. } => {
            return Err(QsdbError::unsupported(
                "CTE output from JOIN not inferable in binder",
            ))
        }
    })
}

pub struct TableResolver<'a> {''')
io.open(p, 'w', encoding='utf-8').write(s)
print('translate ok')
