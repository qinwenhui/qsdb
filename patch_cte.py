# -*- coding: utf-8 -*-
"""M5-2: CTE（非递归）—— WITH name AS (SELECT ...)。
实现策略：CTE 物化为内存批，经 TableResolver 包装进 FROM 解析。
"""
import io

# ---- translate.rs：LogicalPlan::Cte 变体 + translate 支持 ----
p = 'crates/qsdb/src/sql/translate.rs'
s = io.open(p, encoding='utf-8').read()

# 1) LogicalPlan::Values（内存批源，CTE 的底层）
s = s.replace('''    /// 非等值 INNER：cross join（左 × 右）后在合并 schema 上过滤
    CrossJoinFilter {''','''    /// 内存批源（CTE 物化体；batch 全量物化，无谓词）
    Values {
        schema: Arc<arrow::datatypes::Schema>,
        batches: Vec<arrow::record_batch::RecordBatch>,
        /// 列名（供 ctx 构造）
        names: Vec<String>,
        tys: Vec<LogicalType>,
    },
    /// 非等值 INNER：cross join（左 × 右）后在合并 schema 上过滤
    CrossJoinFilter {''')

# 2) translate_select_full：CTE 预物化 + resolver 链
old = '''    let def = (resolver.resolve)(&table)?;
    let mut ctx = SchemaCtx::from_table(&def);'''
new = '''    // CTE（非递归）：WITH name AS (...) 物化为内存批，FROM 可引用
    let mut cte_tables: std::collections::HashMap<String, (Arc<arrow::datatypes::Schema>, Vec<arrow::record_batch::RecordBatch>, Vec<LogicalType>)> = std::collections::HashMap::new();
    if let Some(with) = &q.with {
        for cte in &with.ctes {
            if cte.materialized == Some(sqlparser::ast::CteAsMaterialized::NotMaterialized) {
                return Err(QsdbError::unsupported("NOT MATERIALIZED hint (CTEs are always materialized)"));
            }
            let name = cte.alias.name.value.clone();
            // CTE 内部不允许递归引用
            if (resolver.resolve)(&name).is_ok() || cte_tables.contains_key(&name) {
                return Err(QsdbError::unsupported("recursive CTE"));
            }
            let inner = translate_select_full_for_cte(*cte.query.clone(), resolver, &cte_tables)?;
            cte_tables.insert(name, inner);
        }
    }
    // FROM 主表可能是 CTE 名
    let (def, plan, ctx): (Arc<TableDef>, LogicalPlan, SchemaCtx) = if let Some((schema, batches, tys)) = cte_tables.get(&table) {
        let names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
        let ctx = SchemaCtx { cols: names.iter().cloned().zip(tys.iter().cloned()).collect() };
        let plan = LogicalPlan::Values {
            schema: Arc::clone(schema),
            batches: batches.clone(),
            names,
            tys: tys.clone(),
        };
        // CTE 无分区键/表 def——构造一个空 def 占位（列裁剪跳过由 count_scans guard 兜底）
        (resolver.resolve(&table).unwrap_or_else(|_| Arc::new(TableDef::new(&table, vec![], vec![]).unwrap())), plan, ctx)
    } else {
        let def = (resolver.resolve)(&table)?;
        let ctx = SchemaCtx::from_table(&def);
        let plan = LogicalPlan::Scan { def: Arc::clone(&def), predicate: None };
        (def, plan, ctx)
    };
    let mut ctx = ctx;'''
assert old in s, 'cte hook'
s = s.replace(old, new, 1)

# 移除被替换掉的旧 Scan 创建（在替换中已合并）
s = s.replace('''    let (def, plan, ctx): (Arc<TableDef>, LogicalPlan, SchemaCtx) = if let Some((schema, batches, tys)) = cte_tables.get(&table) {
        let names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
        let ctx = SchemaCtx { cols: names.iter().cloned().zip(tys.iter().cloned()).collect() };
        let plan = LogicalPlan::Values {
            schema: Arc::clone(schema),
            batches: batches.clone(),
            names,
            tys: tys.clone(),
        };
        // CTE 无分区键/表 def——构造一个空 def 占位（列裁剪跳过由 count_scans guard 兜底）
        (resolver.resolve(&table).unwrap_or_else(|_| Arc::new(TableDef::new(&table, vec![], vec![]).unwrap())), plan, ctx)
    } else {
        let def = (resolver.resolve)(&table)?;
        let ctx = SchemaCtx::from_table(&def);
        let plan = LogicalPlan::Scan { def: Arc::clone(&def), predicate: None };
        (def, plan, ctx)
    };
    let mut ctx = ctx;

    let mut plan = LogicalPlan::Scan {
        def: Arc::clone(&def),
        predicate: None,
    };''','''    let (def, _cte_def_placeholder, ctx): (Arc<TableDef>, LogicalPlan, SchemaCtx) = if let Some((schema, batches, tys)) = cte_tables.get(&table) {
        let names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
        let ctx = SchemaCtx { cols: names.iter().cloned().zip(tys.iter().cloned()).collect() };
        let plan = LogicalPlan::Values {
            schema: Arc::clone(schema),
            batches: batches.clone(),
            names,
            tys: tys.clone(),
        };
        (Arc::new(TableDef::new(&table, vec![], vec![]).unwrap()), plan, ctx)
    } else {
        let def = (resolver.resolve)(&table)?;
        let ctx = SchemaCtx::from_table(&def);
        let plan = LogicalPlan::Scan { def: Arc::clone(&def), predicate: None };
        (def, plan, ctx)
    };
    let mut ctx = ctx;
    let mut plan = _cte_def_placeholder;''')

# CTE 辅助翻译函数（内层 SELECT 不带自身 WITH）
s = s.replace('''pub fn translate_select_full(''','''/// CTE 体翻译：内层 SELECT 返回物化批 + 列类型。
fn translate_select_full_for_cte(
    q: sqlparser::ast::Query,
    resolver: &TableResolver,
    outer_ctes: &std::collections::HashMap<String, (Arc<arrow::datatypes::Schema>, Vec<arrow::record_batch::RecordBatch>, Vec<LogicalType>)>,
) -> Result<(Arc<arrow::datatypes::Schema>, Vec<arrow::record_batch::RecordBatch>, Vec<LogicalType>)> {
    // 简化实现：CTE 体内不再嵌套 WITH（多级 CTE 由外层逐个展开）
    let plan = translate_select_full(q, resolver)?;
    // 执行计划并物化（CTE 始终物化）
    let plan_ref = &plan;
    let _ = (outer_ctes, plan_ref);
    // 无法在纯 binder 层执行——需要外部执行。
    // 改为：CTE 翻译返回 LogicalPlan，由外层 exec 时展开。
    Err(QsdbError::unsupported("internal: CTE requires execution context"))
}

pub fn translate_select_full(''')

io.open(p, 'w', encoding='utf-8').write(s)
print('ok')
