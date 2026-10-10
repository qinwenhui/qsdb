# -*- coding: utf-8 -*-
"""CTE 执行层：WITH 子句在 translate 期间生成 cte_plans 副本，
execute_sql 在构建主计划前逐个执行 CTE 子计划并注入 ValuesExec。

简化策略：translate_select_full 返回的 plan 中 CTE FROM 已绑定为
Values(cte_id) 占位——但 binder 无法执行。真实做法：
execute_sql 在收到 SELECT 前，手工提取 WITH 子句，逐个执行内层
SELECT 物化为批，再构造一个不带 WITH 的主查询。

这里采用最可靠的方案：sql/mod.rs 的 Select 分支解析 q.with，
先把每个 CTE 执行成 Vec<RecordBatch>（递归调用 execute_inner），
然后把 CTE 批注入 Qsdb 的 CTE 缓存（线程局部 HashMap<cte_id, 批>），
builder 用 cte_id 取批构造 ValuesExec。
"""
import io

# translate.rs：cte_plans 提取到 pub 返回（伴随主计划）
p = 'crates/qsdb/src/sql/translate.rs'
s = io.open(p, encoding='utf-8').read()

# translate_select_full 改为返回 (plan, cte_defs)——CTE 定义随主计划返回
s = s.replace('''pub fn translate_select_full(
    q: sqlparser::ast::Query,
    resolver: &TableResolver,
) -> Result<LogicalPlan> {''','''/// CTE 物化定义（执行层按 id 填充批数据）。
#[derive(Debug, Clone)]
pub struct CteDef {
    pub cte_id: u32,
    pub columns: Vec<(String, LogicalType)>,
    pub plan: LogicalPlan,
}

pub fn translate_select_full(
    q: sqlparser::ast::Query,
    resolver: &TableResolver,
) -> Result<LogicalPlan> {
    translate_select_full_with_ctes(q, resolver).map(|(p, _)| p)
}

pub fn translate_select_full_with_ctes(
    q: sqlparser::ast::Query,
    resolver: &TableResolver,
) -> Result<(LogicalPlan, Vec<CteDef>)> {''')

# 函数体：CTE 收集改为带 id 的 CteDef，返回
s = s.replace('''    // CTE（非递归）：WITH name AS (...) 内联展开为子计划
    let mut cte_plans: std::collections::HashMap<String, (LogicalPlan, Vec<(String, LogicalType)>)> =
        std::collections::HashMap::new();
    if let Some(with) = &q.with {
        if with.recursive {
            return Err(QsdbError::unsupported("recursive CTE"));
        }
        for cte in &with.cte_tables {
            let name = cte.alias.name.value.clone();
            if cte_plans.contains_key(&name) || (resolver.resolve)(&name).is_ok() {
                return Err(QsdbError::unsupported("recursive or duplicate CTE"));
            }
            let sub = translate_select_full(*cte.query.clone(), resolver)?;
            let cols = plan_output_columns(&sub)?;
            cte_plans.insert(name, (sub, cols));
        }
    }''','''    // CTE（非递归）：WITH name AS (...) 内联展开为子计划
    let mut cte_plans: std::collections::HashMap<String, (LogicalPlan, Vec<(String, LogicalType)>)> =
        std::collections::HashMap::new();
    let mut cte_defs: Vec<CteDef> = Vec::new();
    if let Some(with) = &q.with {
        if with.recursive {
            return Err(QsdbError::unsupported("recursive CTE"));
        }
        for cte in &with.cte_tables {
            let name = cte.alias.name.value.clone();
            if cte_plans.contains_key(&name) || (resolver.resolve)(&name).is_ok() {
                return Err(QsdbError::unsupported("recursive or duplicate CTE"));
            }
            let sub = translate_select_full(*cte.query.clone(), resolver)?;
            let cols = plan_output_columns(&sub)?;
            cte_plans.insert(name, (sub.clone(), cols.clone()));
            cte_defs.push(CteDef {
                cte_id: 0, // 由 Values 占位的 id 填——在主查询 FROM 检查时才知 id
                columns: cols,
                plan: sub,
            });
        }
    }''')

# FROM 主表 CTE 分支：cte_def.cte_id 同步
s = s.replace('''    let (mut plan, mut ctx): (LogicalPlan, SchemaCtx) =
        if let Some((sub, cols)) = cte_plans.get(&table) {
            (
                LogicalPlan::Values {
                    columns: cols.clone(),
                    cte_id: next_cte_id(),
                },
                SchemaCtx { cols: cols.clone() },
            )
        } else {''','''    let (mut plan, mut ctx): (LogicalPlan, SchemaCtx) =
        if let Some((sub, cols)) = cte_plans.get(&table) {
            let id = next_cte_id();
            // 同步 CteDef 的 id（执行层按 id 匹配）
            if let Some(d) = cte_defs.iter_mut().find(|d| Arc::ptr_eq(&d.plan, sub)) {
                d.cte_id = id;
            }
            (
                LogicalPlan::Values {
                    columns: cols.clone(),
                    cte_id: id,
                },
                SchemaCtx { cols: cols.clone() },
            )
        } else {''')

# 函数末尾返回 (plan, cte_defs)
s = s.replace('''    Ok(plan)
}

/// DELETE/TRUNCATE 辅助''','''    Ok((plan, cte_defs))
}

/// DELETE/TRUNCATE 辅助''')
io.open(p, 'w', encoding='utf-8').write(s)
print('translate ok')
