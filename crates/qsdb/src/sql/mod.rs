//! SQL 前端：sqlparser-rs（只解析）→ Binder → LogicalPlan → ExecNode。
//!
//! SQL-0：DDL（CREATE/DROP/DESCRIBE/SHOW）+ INSERT VALUES +
//!        SELECT 投影/过滤/字面量/LIMIT
//! SQL-1：GROUP BY + count/sum/avg/min/max、ORDER BY、DISTINCT、
//!        CAST、IS [NOT] NULL、标量函数 v1 集、别名

pub mod exec_builder;
pub mod translate;

use std::sync::Arc;

use arrow::record_batch::RecordBatch;

use crate::api::{Qsdb, ScanRequest};
use crate::catalog::ColumnDef;
use crate::error::{QsdbError, Result};
use crate::types::cast::cast_literal;
use crate::types::{Literal, LogicalType};
use crate::write::WriteResult;

/// SQL 执行结果。
pub enum SqlResult {
    /// 查询结果批
    Query {
        schema: Arc<arrow::datatypes::Schema>,
        batches: Vec<RecordBatch>,
    },
    /// DDL/DML 文本回执
    Ok { message: String },
}

/// 执行一条 SQL（panic 隔离边界 + 良性上下文）。
pub fn execute_sql(db: &Qsdb, sql: &str) -> Result<SqlResult> {
    execute_sql_ctx(db, sql, &crate::query::QueryContext::benign())
}

/// 带查询上下文执行（隔离边界：panic → 错误；ctx 贯穿全部算子）。
pub fn execute_sql_ctx(
    db: &Qsdb,
    sql: &str,
    ctx: &Arc<crate::query::QueryContext>,
) -> Result<SqlResult> {
    crate::query::run_isolated(|| execute_inner(db, sql, ctx))
}

/// 角色权限检查（server 传入；嵌入方可选使用 sql_with_role）。
/// 语句类型 × 角色矩阵——拒绝在 parse 后、执行前。
pub fn check_role_permission(
    stmt: &translate::ParsedStatement,
    role: crate::users::Role,
) -> Result<()> {
    use translate::ParsedStatement as PS;
    let deny = |what: &str| {
        Err(QsdbError::invalid(format!(
            "role '{}' cannot execute {} (requires {})",
            role.name(),
            what,
            match what {
                "write operations" => "writer",
                _ => "admin",
            }
        )))
    };
    match stmt {
        // 只读：任意角色
        PS::Select(_)
        | PS::Describe { .. }
        | PS::ShowTables
        | PS::ShowPartitions { .. }
        | PS::ShowUsers => Ok(()),
        // 写：Writer 及以上
        PS::Insert { .. } | PS::Delete { .. } => {
            if role.can_write() {
                Ok(())
            } else {
                deny("write operations")
            }
        }
        // DDL + 运维 + 用户管理：Admin
        PS::CreateTable { .. }
        | PS::DropTable { .. }
        | PS::AlterAddColumn { .. }
        | PS::CreateView { .. }
        | PS::DropView { .. }
        | PS::Truncate { .. }
        | PS::CreateUser { .. }
        | PS::DropUser { .. }
        | PS::AlterUser { .. } => {
            if role.is_admin() {
                Ok(())
            } else {
                deny("DDL/admin operations")
            }
        }
    }
}

/// 带角色闸门的 SQL 执行（server 认证后传入用户角色；嵌入方宿主可选使用）。
pub fn execute_sql_with_role(
    db: &Qsdb,
    sql: &str,
    role: crate::users::Role,
    ctx: &Arc<crate::query::QueryContext>,
) -> Result<SqlResult> {
    crate::query::run_isolated(|| {
        let mut stmts = translate::parse_statements(sql)?;
        if stmts.len() != 1 {
            return Err(QsdbError::invalid(
                "multiple statements in one call are not supported; execute one by one",
            ));
        }
        let stmt = stmts.pop().unwrap();
        check_role_permission(&stmt, role)?;
        // 复用主执行器：重新序列化回单条再走 execute（避免复制整个 match）
        drop(stmt);
        execute_inner(db, sql, ctx)
    })
}

fn execute_inner(db: &Qsdb, sql: &str, ctx: &Arc<crate::query::QueryContext>) -> Result<SqlResult> {
    ctx.check()?;
    let mut stmts = translate::parse_statements(sql)?;
    if stmts.is_empty() {
        return Err(QsdbError::invalid("empty statement"));
    }
    if stmts.len() > 1 {
        return Err(QsdbError::invalid(
            "multiple statements in one call are not supported; execute one by one",
        ));
    }
    let stmt = stmts.pop().unwrap();
    ctx.check()?;
    match stmt {
        translate::ParsedStatement::CreateTable { def } => {
            db.create_table(def)?;
            Ok(SqlResult::Ok {
                message: "table created".into(),
            })
        }
        translate::ParsedStatement::AlterAddColumn { table, column } => {
            let name = column.name.clone();
            db.alter_add_column(&table, column)?;
            Ok(SqlResult::Ok {
                message: format!("column '{name}' added"),
            })
        }
        translate::ParsedStatement::DropTable { name, if_exists } => match db.drop_table(&name) {
            Ok(()) => Ok(SqlResult::Ok {
                message: "table dropped".into(),
            }),
            Err(e) if if_exists && matches!(e, QsdbError::NotFound { .. }) => Ok(SqlResult::Ok {
                message: "table dropped".into(),
            }),
            Err(e) => Err(e),
        },
        translate::ParsedStatement::Describe { table } => {
            let def = db.table_def(&table)?;
            let mut names: Vec<&str> = Vec::new();
            let mut types: Vec<String> = Vec::new();
            let mut nulls: Vec<String> = Vec::new();
            for c in &def.columns {
                names.push(&c.name);
                types.push(c.logical_type.to_string());
                nulls.push(if c.nullable {
                    "YES".into()
                } else {
                    "NO".into()
                });
            }
            let parts: Vec<bool> = def
                .columns
                .iter()
                .map(|c| def.partition_keys.contains(&c.name))
                .collect();
            let schema = Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("column", arrow::datatypes::DataType::Utf8, true),
                arrow::datatypes::Field::new("type", arrow::datatypes::DataType::Utf8, true),
                arrow::datatypes::Field::new("nullable", arrow::datatypes::DataType::Utf8, true),
                arrow::datatypes::Field::new(
                    "partition",
                    arrow::datatypes::DataType::Boolean,
                    true,
                ),
            ]));
            use arrow::array::{BooleanArray, StringArray};
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(StringArray::from(
                        names.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(types)),
                    Arc::new(StringArray::from(nulls)),
                    Arc::new(BooleanArray::from(parts)),
                ],
            )
            .map_err(|e| QsdbError::invalid(format!("describe output: {e}")))?;
            Ok(SqlResult::Query {
                schema,
                batches: vec![batch],
            })
        }
        translate::ParsedStatement::ShowTables => {
            let tables = db.tables()?;
            let schema = Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("table", arrow::datatypes::DataType::Utf8, true),
            ]));
            use arrow::array::StringArray;
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(StringArray::from(tables))],
            )
            .map_err(|e| QsdbError::invalid(format!("show output: {e}")))?;
            Ok(SqlResult::Query {
                schema,
                batches: vec![batch],
            })
        }
        translate::ParsedStatement::ShowUsers => {
            let users = crate::users::UserStore::list(db.store())?;
            let schema = Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("user", arrow::datatypes::DataType::Utf8, true),
                arrow::datatypes::Field::new("role", arrow::datatypes::DataType::Utf8, true),
            ]));
            use arrow::array::StringArray;
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(StringArray::from(
                        users.iter().map(|(n, _)| n.clone()).collect::<Vec<_>>(),
                    )),
                    Arc::new(StringArray::from(
                        users
                            .iter()
                            .map(|(_, r)| r.name().to_string())
                            .collect::<Vec<_>>(),
                    )),
                ],
            )
            .map_err(|e| QsdbError::invalid(format!("show users output: {e}")))?;
            Ok(SqlResult::Query {
                schema,
                batches: vec![batch],
            })
        }
        translate::ParsedStatement::CreateUser {
            name,
            password,
            role,
        } => {
            crate::users::UserStore::add(db.store(), &name, &password, role)?;
            Ok(SqlResult::Ok {
                message: format!("user '{name}' created ({})", role.name()),
            })
        }
        translate::ParsedStatement::DropUser { name, if_exists } => {
            match crate::users::UserStore::drop_user(db.store(), &name) {
                Ok(()) => Ok(SqlResult::Ok {
                    message: format!("user '{name}' dropped"),
                }),
                Err(e) if if_exists && matches!(e, QsdbError::NotFound { .. }) => {
                    Ok(SqlResult::Ok {
                        message: format!("user '{name}' dropped"),
                    })
                }
                Err(e) => Err(e),
            }
        }
        translate::ParsedStatement::AlterUser { name, action } => {
            use translate::UserAlterAction;
            match action {
                UserAlterAction::SetPassword(pw) => {
                    crate::users::UserStore::set_password(db.store(), &name, &pw)?;
                    Ok(SqlResult::Ok {
                        message: format!("password updated for '{name}'"),
                    })
                }
                UserAlterAction::SetRole(r) => {
                    crate::users::UserStore::set_role(db.store(), &name, r)?;
                    Ok(SqlResult::Ok {
                        message: format!("role updated to {} for '{name}'", r.name()),
                    })
                }
            }
        }
        translate::ParsedStatement::ShowPartitions { table } => {
            let def = db.table_def(&table)?;
            let tdir = crate::catalog::table_dir(db.root(), crate::catalog::DEFAULT_DB, &table);
            let parts = crate::scan::list_partitions(db.store(), &tdir, def.partition_keys.len());
            let schema = Arc::new(arrow::datatypes::Schema::new(vec![
                arrow::datatypes::Field::new("partition", arrow::datatypes::DataType::Utf8, true),
            ]));
            use arrow::array::StringArray;
            let names: Vec<String> = parts
                .iter()
                .map(|(p, _)| {
                    p.strip_prefix(&tdir)
                        .unwrap_or(p)
                        .to_string_lossy()
                        .trim_start_matches(['\\', '/'])
                        .to_string()
                })
                .collect();
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(StringArray::from(names))],
            )
            .map_err(|e| QsdbError::invalid(format!("show output: {e}")))?;
            Ok(SqlResult::Query {
                schema,
                batches: vec![batch],
            })
        }
        translate::ParsedStatement::Insert {
            table,
            source,
            column_order,
            overwrite,
        } => {
            let def = db.table_def(&table)?;
            match source {
                translate::InsertSource::Values(rows) => {
                    // VALUES 源：列清单必须覆盖全部表列（既有契约）
                    let perm = insert_column_perm(&def, column_order.as_deref(), true)?;
                    insert_values(db, &table, &def, &perm, rows, overwrite, ctx)
                }
                translate::InsertSource::Select(q) => {
                    // SELECT 源：允许只给部分列（未列出的列补 NULL）
                    let perm = insert_column_perm(&def, column_order.as_deref(), false)?;
                    insert_select(db, &table, &def, &perm, *q, overwrite, ctx)
                }
            }
        }
        translate::ParsedStatement::Delete { table, selection } => {
            let rows = db.delete_partitions_where(&table, selection.as_deref())?;
            Ok(SqlResult::Ok {
                message: format!("{rows} rows deleted"),
            })
        }
        translate::ParsedStatement::CreateView {
            name,
            sql,
            or_replace,
        } => {
            // 定义期校验：必须可解析为单条 SELECT（防存入垃圾 SQL）
            match translate::parse_statements(&sql)?.into_iter().next() {
                Some(translate::ParsedStatement::Select(_)) => {}
                _ => {
                    return Err(QsdbError::invalid(
                        "view definition must be a single SELECT statement",
                    ));
                }
            }
            crate::views::ViewStore::create(db.store(), &name, &sql, or_replace)?;
            Ok(SqlResult::Ok {
                message: format!("view '{name}' created"),
            })
        }
        translate::ParsedStatement::DropView { name, if_exists } => {
            crate::views::ViewStore::drop_view(db.store(), &name, if_exists)?;
            Ok(SqlResult::Ok {
                message: format!("view '{name}' dropped"),
            })
        }
        translate::ParsedStatement::Truncate { table } => {
            let rows = db.truncate_table(&table)?;
            Ok(SqlResult::Ok {
                message: format!("{rows} rows truncated"),
            })
        }
        translate::ParsedStatement::Select(q) => {
            let resolver = translate::TableResolver {
                resolve: Box::new(|name: &str| db.table_def(name)),
                views: crate::views::ViewStore::read_all(db.store())?,
            };
            let (plan, cte_defs) = translate::translate_select_full_with_ctes(*q, &resolver)?;
            let _cte_scope = exec_builder::CteScope::new();
            materialize_ctes(db, &cte_defs, ctx)?;
            ctx.check()?;
            // M4-4 元数据短路：count(*) + 纯分区列谓词 → manifest O(1) 应答
            if let Some(n) = metadata_shortcut_count(db, &plan)? {
                let schema = Arc::new(arrow::datatypes::Schema::new(vec![
                    arrow::datatypes::Field::new(
                        "count(*)",
                        arrow::datatypes::DataType::Int64,
                        true,
                    ),
                ]));
                let batch = RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(arrow::array::Int64Array::from(vec![Some(
                        n as i64,
                    )]))],
                )
                .map_err(|e| QsdbError::invalid(format!("shortcut batch: {e}")))?;
                return Ok(SqlResult::Query {
                    schema,
                    batches: vec![batch],
                });
            }
            let mut exec = exec_builder::build(db, &plan, ctx)?;
            let schema = exec.schema();
            let mut batches = Vec::new();
            while let Some(b) = exec.next() {
                batches.push(b?);
            }
            Ok(SqlResult::Query { schema, batches })
        }
    }
}

/// CTE 物化：按定义序执行每个 CTE 子计划 → 批缓存（cte_id → Vec<RecordBatch>）。
/// 增量装入：链式 CTE（引用先定义者）物化时缓存已可读。
///
/// 两处硬伤（2026-10-13 修）：
/// ① 子计划此前用 `QueryContext::benign()` 执行——**内存闸/超时/取消
///    三条隔离全绕过**；现用外层真 ctx。
/// ② 物化批此前只在"下一条 WITH 查询开始时"才被替换 → **跨查询驻留**
///    在这条线程上（注释自称"无跨查询泄漏"并不成立）。现由 `CteScope`
///    在查询结束（含提前 return / panic 展开）时清空并归还预留。
///
/// SELECT 与 `INSERT … SELECT` 共用——两条路的 CTE 语义必须一致，否则
/// `INSERT INTO t WITH x AS (…) SELECT …` 会与查询版走不同实现。
/// 调用方负责持住 `CteScope`（物化批活到查询结束）。
fn materialize_ctes(
    db: &Qsdb,
    cte_defs: &[translate::CteDef],
    ctx: &Arc<crate::query::QueryContext>,
) -> Result<()> {
    for cte in cte_defs {
        if cte.cte_id == u32::MAX {
            continue; // 防御：translate 已过滤 dead CTE
        }
        let mut exec = exec_builder::build(db, &cte.plan, ctx)?;
        let mut batches = Vec::new();
        let mut res = ctx.try_reserve(0)?;
        while let Some(b) = exec.next() {
            let b = b?;
            res.grow(b.get_array_memory_size() as u64)?;
            batches.push(b);
        }
        exec_builder::add_cte_batches(cte.cte_id, batches, res);
    }
    Ok(())
}

/// 检测可元数据应答的形态：Project? → Aggregate(count(*), 无 group) → Scan(predicate 仅分区列)。
/// 返回 Some(count) 表示短路命中（None 走正常执行）。
fn metadata_shortcut_count(
    db: &Qsdb,
    plan: &crate::sql::translate::LogicalPlan,
) -> Result<Option<u64>> {
    use crate::sql::translate::LogicalPlan;
    // 剥 Project
    let mut cur = plan;
    if let LogicalPlan::Project { input, .. } = cur {
        cur = input;
    }
    // Aggregate：无 group、唯一聚合 count(*)
    let LogicalPlan::Aggregate { input, group, aggs } = cur else {
        return Ok(None);
    };
    let input: &LogicalPlan = input;
    if !group.is_empty() || aggs.len() != 1 {
        return Ok(None);
    }
    let agg = &aggs[0];
    if agg.func != crate::expr::AggFunc::Count || agg.arg.is_some() {
        return Ok(None);
    }
    // Scan：谓词仅引用分区列
    let LogicalPlan::Scan { def, predicate } = input else {
        return Ok(None);
    };
    // 无分区表：无谓词 → footer 元数据（parquet num_rows，零解码，DuckDB 同款）；
    // 有谓词需数据 → 交给常规扫描。
    if def.partition_keys.is_empty() {
        if predicate.is_some() {
            return Ok(None);
        }
        return db.scan_metadata_count(&def.name, None);
    }
    if let Some(pred) = predicate {
        let mut cols = std::collections::HashSet::new();
        pred.referenced_columns(&mut cols);
        let only_partition = cols.iter().all(|c| {
            def.partition_keys
                .iter()
                .any(|p| p.eq_ignore_ascii_case(c.as_str()))
        });
        if !only_partition {
            return Ok(None);
        }
    }
    db.scan_metadata_count(&def.name, predicate.as_ref())
}

// ---------------------------------------------------------------------------
// INSERT（VALUES / SELECT 两条源，共用列映射与写入端）
// ---------------------------------------------------------------------------

/// 目标列映射：源第 `i` 列 → 表第 `perm[i]` 列。
///
/// `column_order` 是**位置语义**（标准 SQL：INSERT…VALUES 与 INSERT…SELECT 都按
/// 位置对齐列清单，不按名匹配），所以这里只把列名翻译成表列下标。
///
/// `require_full`：列清单是否必须覆盖全部表列——
/// - VALUES 源传 `true`：SQL-0 起的既有契约（`INSERT INTO events (dt, tenant)
///   VALUES …` 必须报错，而不是补 NULL 写进半行），已被 e2e 测试锁定；
/// - SELECT 源传 `false`：导入场景常只给部分列（缺的列补 NULL，真正的
///   NOT NULL/CHECK 判定是写入端的事，见 `write::check_constraints`）。
fn insert_column_perm(
    def: &crate::catalog::TableDef,
    column_order: Option<&[String]>,
    require_full: bool,
) -> Result<Vec<usize>> {
    let Some(order) = column_order else {
        return Ok((0..def.columns.len()).collect());
    };
    if require_full && order.len() != def.columns.len() {
        return Err(QsdbError::invalid(format!(
            "INSERT column list has {} entries, table {} has {}",
            order.len(),
            def.name,
            def.columns.len()
        )));
    }
    let perm: Vec<usize> = order
        .iter()
        .map(|n| {
            def.columns
                .iter()
                .position(|c| c.name.eq_ignore_ascii_case(n))
                .ok_or_else(|| QsdbError::column_not_found(format!("column '{n}'")))
        })
        .collect::<Result<_>>()?;
    let mut seen = vec![false; def.columns.len()];
    for &p in &perm {
        if seen[p] {
            return Err(QsdbError::invalid("duplicate column in INSERT list"));
        }
        seen[p] = true;
    }
    Ok(perm)
}

/// `perm` 的逆映射：表第 j 列取自源第 `src_of[j]` 列（`usize::MAX` = 未提供 → NULL）。
fn invert_perm(perm: &[usize], width: usize) -> Vec<usize> {
    let mut src_of = vec![usize::MAX; width];
    for (i, &p) in perm.iter().enumerate() {
        src_of[p] = i;
    }
    src_of
}

/// VALUES 行集 → 表列序（列清单未覆盖的列补 NULL——VALUES 源要求列清单覆盖
/// 全表，故实际不会有这种列；SELECT 源的部分列由 `align_insert_batch` 同样处理）。
/// 恒等映射（无列清单）直接返回原行集，不做拷贝。
fn reorder_rows(
    def: &crate::catalog::TableDef,
    rows: Vec<Vec<Literal>>,
    perm: &[usize],
) -> Vec<Vec<Literal>> {
    if perm.iter().enumerate().all(|(i, &p)| i == p) {
        return rows;
    }
    let src_of = invert_perm(perm, def.columns.len());
    rows.into_iter()
        .map(|row| {
            src_of
                .iter()
                .map(|&s| {
                    if s == usize::MAX || s >= row.len() {
                        Literal::Null
                    } else {
                        row[s].clone()
                    }
                })
                .collect()
        })
        .collect()
}

/// VALUES 行 → RecordBatch（按表列序 cast）。行宽已由调用方按列清单校验，
/// 这里再判一次是廉价防线（宽度不符时下面的列取用会错位）。
fn build_insert_batch(
    def: &crate::catalog::TableDef,
    rows: &[Vec<Literal>],
) -> Result<RecordBatch> {
    let schema = Arc::new(arrow::datatypes::Schema::new(
        def.columns
            .iter()
            .map(|c| arrow::datatypes::Field::new(&c.name, c.logical_type.arrow_type(), true))
            .collect::<Vec<_>>(),
    ));
    let n = rows.len();
    if let Some(first) = rows.first() {
        if first.len() != def.columns.len() {
            return Err(QsdbError::invalid(format!(
                "VALUES rows have {} entries, table {} has {} columns",
                first.len(),
                def.name,
                def.columns.len()
            )));
        }
    }
    let mut cols = Vec::with_capacity(def.columns.len());
    for (ci, col) in def.columns.iter().enumerate() {
        let lits: Vec<&Literal> = rows.iter().map(|r| &r[ci]).collect();
        // 每值 cast 到列类型
        let casted: Vec<Literal> = lits
            .iter()
            .map(|l| {
                if l.is_null() {
                    Ok(Literal::Null)
                } else if l.logical_type().as_ref() == Some(&col.logical_type) {
                    Ok((*l).clone())
                } else {
                    cast_literal(l, &col.logical_type)
                }
            })
            .collect::<Result<_>>()?;
        let refs: Vec<&Literal> = casted.iter().collect();
        cols.push(crate::exec::agg::literals_to_array(
            &refs,
            &col.logical_type,
        )?);
    }
    RecordBatch::try_new_with_options(
        schema,
        cols,
        &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(n)),
    )
    .map_err(|e| QsdbError::invalid(format!("insert batch: {e}")))
}

/// SELECT 结果批 → 表列序全宽批：按 `perm` 重排列 + cast 到目标列类型 +
/// 未列出的列补 NULL。
///
/// 类型校验走**同一张** `cast_rule` 矩阵（VALUES 路径的 `cast_literal` 也用它）：
/// `Impossible` 在这里就报错（如 BOOLEAN → BIGINT），其余交给 arrow 的数组 cast
/// 执行——窄化溢出、字符串转数字失败等运行时问题由 arrow 报错，语义与逐值
/// cast 一致（不会静默截断）。
fn align_insert_batch(
    def: &crate::catalog::TableDef,
    perm: &[usize],
    batch: &RecordBatch,
) -> Result<RecordBatch> {
    if batch.num_columns() != perm.len() {
        return Err(QsdbError::invalid(format!(
            "INSERT ... SELECT produces {} columns, target column list has {}",
            batch.num_columns(),
            perm.len()
        )));
    }
    let n = batch.num_rows();
    let src_of = invert_perm(perm, def.columns.len());
    let mut fields: Vec<arrow::datatypes::FieldRef> = Vec::with_capacity(def.columns.len());
    let mut cols: Vec<arrow::array::ArrayRef> = Vec::with_capacity(def.columns.len());
    for (j, col) in def.columns.iter().enumerate() {
        let want = col.logical_type.arrow_type();
        fields.push(Arc::new(arrow::datatypes::Field::new(
            &col.name,
            want.clone(),
            true,
        )));
        let s = src_of[j];
        if s == usize::MAX {
            cols.push(arrow::array::new_null_array(&want, n));
            continue;
        }
        let src = batch.column(s);
        if src.data_type() == &want {
            cols.push(Arc::clone(src)); // 同类型（含 DECIMAL 元数据一致）零拷贝
            continue;
        }
        check_insert_cast(src.data_type(), &col.logical_type, &col.name)?;
        let casted = arrow::compute::cast(src.as_ref(), &want).map_err(|e| {
            QsdbError::type_err(format!(
                "INSERT column '{}': cast {:#?} → {} failed: {e}",
                col.name,
                src.data_type(),
                col.logical_type
            ))
        })?;
        // arrow 的 cast 默认 safe=true：转换不了的值（'abc' → BIGINT、溢出……）
        // **静默变 NULL**。对 INSERT 而言那是静默写脏数据——宁可报错。
        // 源本来就 NULL 的行不在此列（null 数不变即合法）。
        if casted.null_count() > src.null_count() {
            return Err(QsdbError::type_err(format!(
                "INSERT column '{}': {} 个非 NULL 值无法转换为 {}（源类型 {:#?}）",
                col.name,
                casted.null_count() - src.null_count(),
                col.logical_type,
                src.data_type()
            )));
        }
        cols.push(casted);
    }
    RecordBatch::try_new_with_options(
        Arc::new(arrow::datatypes::Schema::new(fields)),
        cols,
        &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(n)),
    )
    .map_err(|e| QsdbError::invalid(format!("INSERT ... SELECT batch: {e}")))
}

/// 数组 cast 的类型规则检查（与 `cast_literal` 同源）。
/// arrow 层没有对应 QSDB 逻辑类型的表示（Utf8View / LargeUtf8 / Date64…）时
/// 放行给 arrow 尝试——它们不是 QSDB 类型系统的成员，规则矩阵无话可说。
fn check_insert_cast(from: &arrow::datatypes::DataType, to: &LogicalType, col: &str) -> Result<()> {
    let Ok(from_lt) = LogicalType::try_from_arrow(from) else {
        return Ok(());
    };
    if crate::types::cast::cast_rule(&from_lt, to) == crate::types::cast::CastRule::Impossible {
        return Err(QsdbError::type_err(format!(
            "INSERT column '{col}': cannot cast {from_lt} to {to}"
        )));
    }
    Ok(())
}

/// APPEND 攒批阈值（批数 / 字节）。
///
/// 与 `write_buffer` 同一动机：逐批 `db.append` 会一批一个 segment，万级小批下
/// manifest/索引重写放大到 O(n²)。阈值取 write_buffer 默认档的 1/4——这里是
/// 单次导入的临时缓冲，没必要按常驻写缓冲的预算驻留。
const INSERT_FLUSH_BATCHES: usize = 16;
const INSERT_FLUSH_BYTES: u64 = 32 * 1024 * 1024;

/// INSERT 写入端（两条源共用）。
///
/// - APPEND：攒够阈值即 `db.append`（自动分区路由），**流式**不物化全量；
/// - OVERWRITE：按分区值聚齐后逐分区 `overwrite_partition`（整分区原子替换）。
///
/// OVERWRITE 为什么必须先聚齐：替换语义要求"整分区一次提交"，而 SELECT 源的
/// 同一分区可能跨多批出现（甚至跨文件），不聚齐就会互相覆盖成"最后一批赢"。
/// 代价是该路径把数据留在内存——与 VALUES 路径同口径（那条路本来就全量在
/// 内存）；大数据集导入走 APPEND，或先落表再 `DELETE` + 插入。
struct InsertSink<'a> {
    db: &'a Qsdb,
    table: &'a str,
    def: &'a crate::catalog::TableDef,
    perm: &'a [usize],
    overwrite: bool,
    /// 缓冲批的内存预留（引擎内存闸/配额对这条路径同样生效）
    res: crate::memory::Reservation,
    pending: Vec<RecordBatch>,
    pending_bytes: u64,
    /// OVERWRITE：分区值 → 该分区全宽批
    buckets: Vec<(Vec<Literal>, Vec<RecordBatch>)>,
    total: u64,
}

impl<'a> InsertSink<'a> {
    fn new(
        db: &'a Qsdb,
        table: &'a str,
        def: &'a crate::catalog::TableDef,
        perm: &'a [usize],
        overwrite: bool,
        ctx: &Arc<crate::query::QueryContext>,
    ) -> Result<Self> {
        Ok(Self {
            db,
            table,
            def,
            perm,
            overwrite,
            res: ctx.try_reserve(0)?,
            pending: Vec::new(),
            pending_bytes: 0,
            buckets: Vec::new(),
            total: 0,
        })
    }

    /// 推入一批**源列序**的批（SELECT 结果），内部先按 `perm` 对齐到表列序。
    fn push(&mut self, batch: &RecordBatch) -> Result<()> {
        let aligned = align_insert_batch(self.def, self.perm, batch)?;
        self.push_aligned(aligned)
    }

    /// 推入一批**已对齐到表列序、且类型已就位**的批。
    ///
    /// VALUES 路径走这里而不是 `push`：`build_insert_batch` 产出的是表列序批，
    /// 再经一次"源列序 → 表列序"的对齐就会被按列清单语义**再排一次**（列序错乱
    /// 后 cast 才报错，报的还是"无法转换"这种二手信息）。
    fn push_aligned(&mut self, aligned: RecordBatch) -> Result<()> {
        if aligned.num_rows() == 0 {
            return Ok(());
        }
        let bytes = aligned.get_array_memory_size() as u64;
        self.res.grow(bytes)?;
        if self.overwrite {
            for (vals, mask) in partition_row_masks(self.def, &aligned)? {
                let sub = arrow::compute::filter_record_batch(&aligned, &mask)
                    .map_err(|e| QsdbError::invalid(format!("INSERT 分区切分失败: {e}")))?;
                match self
                    .buckets
                    .iter_mut()
                    .find(|(v, _)| same_partition(v, &vals))
                {
                    Some((_, list)) => list.push(sub),
                    None => self.buckets.push((vals, vec![sub])),
                }
            }
        } else {
            self.pending.push(aligned);
            self.pending_bytes += bytes;
            if self.pending.len() >= INSERT_FLUSH_BATCHES
                || self.pending_bytes >= INSERT_FLUSH_BYTES
            {
                self.flush_pending()?;
            }
        }
        Ok(())
    }

    fn flush_pending(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let r: WriteResult = self.db.append(self.table, &self.pending)?;
        self.total += r.rows;
        self.pending.clear();
        self.res.shrink(self.pending_bytes);
        self.pending_bytes = 0;
        Ok(())
    }

    fn finish(mut self) -> Result<u64> {
        if self.overwrite {
            let (db, table, def) = (self.db, self.table, self.def);
            for (vals, batches) in std::mem::take(&mut self.buckets) {
                let names: Vec<(&str, Literal)> = def
                    .partition_keys
                    .iter()
                    .zip(vals.iter())
                    .map(|(k, v)| (k.as_str(), v.clone()))
                    .collect();
                let r = db.overwrite_partition(table, &names, &batches)?;
                self.total += r.rows;
            }
        } else {
            self.flush_pending()?;
        }
        Ok(self.total)
    }
}

/// 分区值序列等价（比较用写入端同一套规范化：`partition_value_string`）。
fn same_partition(a: &[Literal], b: &[Literal]) -> bool {
    a.len() == b.len()
        && a.iter().zip(b.iter()).all(|(x, y)| {
            crate::partition::partition_value_string(x)
                == crate::partition::partition_value_string(y)
        })
}

/// 批 → (分区值, 行掩码)。OVERWRITE 要的是"每个分区的行子集"，
/// 而 `partition::split_by_partition` 返回的是**剥掉分区列**的批（replace 原语
/// 要全宽批，内部自己剥）——所以这里只借它的取值原语（value_at /
/// partition_value_string 保证与写入端的划分口径一致），子集由 arrow 的
/// `filter_record_batch` 切。
fn partition_row_masks(
    def: &crate::catalog::TableDef,
    batch: &RecordBatch,
) -> Result<Vec<(Vec<Literal>, arrow::array::BooleanArray)>> {
    if def.partition_keys.is_empty() {
        // 无分区键：整批一个"桶"（与 split_by_partition 的空键组对应）
        return Ok(vec![(
            Vec::new(),
            arrow::array::BooleanArray::from(vec![true; batch.num_rows()]),
        )]);
    }
    let pidx: Vec<usize> = def
        .partition_keys
        .iter()
        .map(|k| {
            batch.schema().index_of(k).map_err(|_| {
                // 报出**实际收到的列**——只说"缺列"没法排障（实践：客户端列名/列序
                // 不对时，这句话是唯一线索）
                let sch = batch.schema();
                let have: Vec<&str> = sch.fields().iter().map(|f| f.name().as_str()).collect();
                QsdbError::invalid(format!(
                    "batch lacks partition column '{k}' (batch columns: {have:?})"
                ))
            })
        })
        .collect::<Result<_>>()?;
    let mut out: Vec<(Vec<Literal>, Vec<bool>)> = Vec::new();
    let mut index: std::collections::HashMap<Vec<String>, usize> = std::collections::HashMap::new();
    for row in 0..batch.num_rows() {
        let vals: Vec<Literal> = pidx
            .iter()
            .map(|&c| crate::partition::value_at(batch.column(c).as_ref(), row))
            .collect::<Result<_>>()?;
        let key: Vec<String> = vals
            .iter()
            .map(crate::partition::partition_value_string)
            .collect();
        let id = match index.get(&key) {
            Some(&id) => id,
            None => {
                let id = out.len();
                out.push((vals, vec![false; batch.num_rows()]));
                index.insert(key, id);
                id
            }
        };
        out[id].1[row] = true;
    }
    Ok(out
        .into_iter()
        .map(|(v, m)| (v, arrow::array::BooleanArray::from(m)))
        .collect())
}

/// `INSERT [OVERWRITE] INTO t [(cols)] VALUES …`。
fn insert_values(
    db: &Qsdb,
    table: &str,
    def: &crate::catalog::TableDef,
    perm: &[usize],
    rows: Vec<Vec<Literal>>,
    overwrite: bool,
    ctx: &Arc<crate::query::QueryContext>,
) -> Result<SqlResult> {
    // 行宽按**列清单**宽度校验（不是表列数）：列清单是位置语义的基准
    if let Some(w) = rows.first().map(Vec::len) {
        if w != perm.len() {
            return Err(QsdbError::invalid(format!(
                "VALUES rows have {w} entries, INSERT column list has {}",
                perm.len()
            )));
        }
    }
    let rows = reorder_rows(def, rows, perm);
    let mut sink = InsertSink::new(db, table, def, perm, overwrite, ctx)?;
    if !rows.is_empty() {
        sink.push_aligned(build_insert_batch(def, &rows)?)?;
    }
    let total = sink.finish()?;
    Ok(SqlResult::Ok {
        message: if overwrite {
            format!("{total} rows overwritten")
        } else {
            format!("{total} rows inserted")
        },
    })
}

/// `INSERT [OVERWRITE] INTO t [(cols)] SELECT …`：源走**同一条** SELECT 链路
/// （translate → exec_builder::build），再逐批对齐写入。
///
/// 两条源的列映射/类型校验/约束/分区路由完全共用（`InsertSink`），差别只有
/// "批从哪来"。反过来说：**不**为 INSERT 单独走一套"简化版"执行——那样
/// CTE / 子查询 / 集合操作 / 窗口会一个个漏掉。
///
/// 已知边界：SELECT 返回 0 行时 OVERWRITE 是**无操作**（没有行 → 推不出要替换
/// 哪个分区）；`INSERT OVERWRITE t SELECT … WHERE false` 不会清空目标分区。
fn insert_select(
    db: &Qsdb,
    table: &str,
    def: &crate::catalog::TableDef,
    perm: &[usize],
    q: sqlparser::ast::Query,
    overwrite: bool,
    ctx: &Arc<crate::query::QueryContext>,
) -> Result<SqlResult> {
    let resolver = translate::TableResolver {
        resolve: Box::new(|name: &str| db.table_def(name)),
        views: crate::views::ViewStore::read_all(db.store())?,
    };
    let (plan, cte_defs) = translate::translate_select_full_with_ctes(q, &resolver)?;
    let _cte_scope = exec_builder::CteScope::new();
    materialize_ctes(db, &cte_defs, ctx)?;
    let mut exec = exec_builder::build(db, &plan, ctx)?;
    // 列数在第一批之前就能判：报错比"写了一半才炸"干净
    let src_width = exec.schema().fields().len();
    if src_width != perm.len() {
        return Err(QsdbError::invalid(format!(
            "INSERT ... SELECT produces {src_width} columns, target column list has {}",
            perm.len()
        )));
    }
    let mut sink = InsertSink::new(db, table, def, perm, overwrite, ctx)?;
    while let Some(b) = exec.next() {
        ctx.check()?;
        sink.push(&b?)?;
    }
    let total = sink.finish()?;
    Ok(SqlResult::Ok {
        message: if overwrite {
            format!("{total} rows overwritten")
        } else {
            format!("{total} rows inserted")
        },
    })
}

/// ScanRequest 便捷构造（测试用）。
#[allow(dead_code)]
pub(crate) fn scan_all(table: &str) -> ScanRequest {
    ScanRequest {
        table: table.into(),
        ..Default::default()
    }
}

/// 建列定义便捷函数（translate 用）。
pub(crate) fn make_column(
    name: &str,
    ty: LogicalType,
    nullable: bool,
    check: Option<String>,
    comment: Option<String>,
) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        logical_type: ty,
        nullable,
        check,
        default: None,
        comment,
    }
}
