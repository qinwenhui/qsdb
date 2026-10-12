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
            rows,
            column_order,
            overwrite,
        } => {
            let def = db.table_def(&table)?;
            let rows = reorder_rows(&def, rows, column_order)?;
            if overwrite {
                // INSERT OVERWRITE：按分区列值在行层面分桶，逐分区原子替换
                // （overwrite_partition 的契约是全列批，内部自行剥离分区列）
                let pidx: Vec<usize> = def
                    .partition_keys
                    .iter()
                    .map(|k| def.columns.iter().position(|c| &c.name == k).unwrap())
                    .collect();
                let mut buckets: Vec<(Vec<Literal>, Vec<Vec<Literal>>)> = Vec::new();
                for row in rows {
                    let key: Vec<Literal> = pidx
                        .iter()
                        .map(|&p| {
                            let lit = &row[p];
                            let col = &def.columns[p];
                            if lit.is_null() {
                                return Err(QsdbError::invalid("partition value cannot be NULL"));
                            }
                            if lit.logical_type().as_ref() == Some(&col.logical_type) {
                                Ok(lit.clone())
                            } else {
                                cast_literal(lit, &col.logical_type)
                            }
                        })
                        .collect::<Result<_>>()?;
                    match buckets.iter_mut().find(|(k, _)| {
                        k.iter().zip(key.iter()).all(|(a, b)| {
                            matches!(
                                a.partial_cmp_literal(b),
                                Ok(Some(std::cmp::Ordering::Equal))
                            )
                        })
                    }) {
                        Some((_, rows)) => rows.push(row),
                        None => buckets.push((key, vec![row])),
                    }
                }
                let mut total = 0u64;
                for (vals, sub_rows) in buckets {
                    let mut names: Vec<(&str, Literal)> = Vec::new();
                    for (k, v) in def.partition_keys.iter().zip(vals.iter()) {
                        names.push((k.as_str(), v.clone()));
                    }
                    let batch = build_insert_batch(&def, &sub_rows)?;
                    let r = db.overwrite_partition(&table, &names, &[batch])?;
                    total += r.rows;
                }
                Ok(SqlResult::Ok {
                    message: format!("{total} rows overwritten"),
                })
            } else {
                let batch = build_insert_batch(&def, &rows)?;
                let r: WriteResult = db.append(&table, &[batch])?;
                Ok(SqlResult::Ok {
                    message: format!("{} rows inserted", r.rows),
                })
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
            // CTE 物化：按定义序执行每个 CTE 子计划 → 批缓存（cte_id → Vec<RecordBatch>）。
            // 增量装入：链式 CTE（引用先定义者）物化时缓存已可读。
            exec_builder::set_cte_batches(std::collections::HashMap::new());
            for cte in &cte_defs {
                if cte.cte_id == u32::MAX {
                    continue; // 防御：translate 已过滤 dead CTE
                }
                let ctx = crate::query::QueryContext::benign();
                let mut exec = exec_builder::build(db, &cte.plan, &ctx)?;
                let mut batches = Vec::new();
                while let Some(b) = exec.next() {
                    batches.push(b?);
                }
                exec_builder::add_cte_batches(cte.cte_id, batches);
            }
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

/// INSERT 行 → RecordBatch（按表列序 cast）。
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

/// INSERT 列序重排/校验：Some(order) 必须覆盖全部列（可乱序）。
fn reorder_rows(
    def: &crate::catalog::TableDef,
    rows: Vec<Vec<Literal>>,
    column_order: Option<Vec<String>>,
) -> Result<Vec<Vec<Literal>>> {
    let Some(order) = column_order else {
        return Ok(rows);
    };
    if order.len() != def.columns.len() {
        return Err(QsdbError::invalid(format!(
            "INSERT column list has {} entries, table has {}",
            order.len(),
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
    if !seen.iter().all(|&x| x) {
        return Err(QsdbError::invalid(
            "INSERT column list must cover all table columns",
        ));
    }
    Ok(rows
        .into_iter()
        .map(|row| {
            let mut out = vec![Literal::Null; def.columns.len()];
            for (i, &p) in perm.iter().enumerate() {
                out[p] = row[i].clone();
            }
            out
        })
        .collect())
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
