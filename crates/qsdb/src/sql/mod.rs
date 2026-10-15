//! SQL 前端：sqlparser-rs（只解析）→ Binder → LogicalPlan → ExecNode。
//!
//! SQL-0：DDL（CREATE/DROP/DESCRIBE/SHOW）+ INSERT VALUES +
//!        SELECT 投影/过滤/字面量/LIMIT
//! SQL-1：GROUP BY + count/sum/avg/min/max、ORDER BY、DISTINCT、
//!        CAST、IS [NOT] NULL、标量函数 v1 集、别名

pub mod copy_options;
pub mod exec_builder;
pub mod params;
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
///
/// **`#[non_exhaustive]` 是刻意标的**（qsdb-sync F27）——这是个**内核产出、
/// 嵌入方只消费**的类型，J12 给 `Ok` 加两个字段就把首个嵌入方的编译打断了
/// （穷尽匹配 → E0027）。规则（2026-10-13 与 F27 一并定）：
/// **结果/错误类类型标 `non_exhaustive`；嵌入方要构造或按字段取值的
/// 输入/格式类型（`Literal` / `SegmentEntry` / `PartitionManifest` …）不标。**
///
/// ⚠️ **枚举级标注不足以防"加字段"，必须同时标在变体上**（实测）：
/// 枚举级只要求外部加通配臂（E0004）；外部若把变体字段列全了，加字段照样
/// 报 E0027。变体级才会强制外部写 `..`（E0638）。**别把变体上这两行删了。**
/// `Debug` 是**附加式**补的（2026-10-13）：先前没实现，导致调用方连
/// `{result:?}` 都用不了——内核自己的 `server.rs` / `flight.rs` 都在这里栽过
/// （只能手写静态文案）。补上纯属易用性，不影响任何已有代码。
#[derive(Debug)]
#[non_exhaustive]
pub enum SqlResult {
    /// 查询结果批
    #[non_exhaustive]
    Query {
        schema: Arc<arrow::datatypes::Schema>,
        batches: Vec<RecordBatch>,
    },
    /// DDL/DML 回执。
    ///
    /// `message` 是**给人看的文案**（码表文档明确"文案不做兼容承诺"），
    /// 机器消费请用 `rows_affected` / `command`（J12：qsdb-jdbc 的
    /// `executeUpdate` 此前只能解析文案拿行数，是契约面上的脆弱点）。
    #[non_exhaustive]
    Ok {
        message: String,
        /// DML 影响行数；DDL 无意义（`None`）
        rows_affected: Option<u64>,
        /// 机器可读命令名（`INSERT` / `DELETE` / `CREATE_TABLE` …）；未归类为 `None`
        command: Option<&'static str>,
    },
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
        // CTAS 也是 DDL（建表）——虽然它顺带写数据，但**建表**才是它的身份；
        // 不因为"它写了数据"就降级到 writer（那会让 writer 能建表）。
        PS::CreateTable { .. }
        | PS::CreateTableAs { .. }
        | PS::DropTable { .. }
        | PS::AlterAddColumn { .. }
        | PS::CreateView { .. }
        | PS::DropView { .. }
        | PS::Truncate { .. }
        | PS::CreateUser { .. }
        | PS::DropUser { .. }
        | PS::AlterUser { .. }
        // `COPY … TO` 往**任意文件系统路径**写东西（目标不由我们约束）⇒ 归 admin。
        // 判据照 PG：`COPY TO '<文件>'` 要 superuser（非超级用户只能走 STDOUT）。
        // 让 writer 角色能写任意路径，等于给网线上的 SQL 一个文件写入原语。
        | PS::CopyTo { .. } => {
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
    execute_sql_full(db, sql, Vec::new(), Some(role), ctx)
}

/// 带**参数**执行（`docs/参数化查询设计.md`）：`params` 是已类型化的字面量，
/// 绑定在解析期完成（值→AST 字面量节点，**从不进入 SQL 文本**）。
pub fn execute_sql_params_ctx(
    db: &Qsdb,
    sql: &str,
    params: Vec<Literal>,
    ctx: &Arc<crate::query::QueryContext>,
) -> Result<SqlResult> {
    execute_sql_full(db, sql, params, None, ctx)
}

/// 带**参数** + 角色闸门（server 侧同时需要两者时用）。
pub fn execute_sql_with_role_and_params(
    db: &Qsdb,
    sql: &str,
    params: Vec<Literal>,
    role: crate::users::Role,
    ctx: &Arc<crate::query::QueryContext>,
) -> Result<SqlResult> {
    execute_sql_full(db, sql, params, Some(role), ctx)
}

fn execute_inner(db: &Qsdb, sql: &str, ctx: &Arc<crate::query::QueryContext>) -> Result<SqlResult> {
    execute_sql_full(db, sql, Vec::new(), None, ctx)
}

/// **唯一的执行入口**（参数 / 角色 / 上下文三件事在这里汇合）。
///
/// 顺带修掉一处既有浪费：角色那条路此前**解析两遍**（先 `parse_statements` 查权限、
/// 丢掉、再进 `execute_inner` 重新解析一遍）。现在解析一次、`ParsedStatement`
/// 直接交给下面的 match。
fn execute_sql_full(
    db: &Qsdb,
    sql: &str,
    params: Vec<Literal>,
    role: Option<crate::users::Role>,
    ctx: &Arc<crate::query::QueryContext>,
) -> Result<SqlResult> {
    crate::query::run_isolated(|| {
        ctx.check()?;
        // **参数个数先对账，再翻译**（见 `params::normalize_placeholders` 的说明）：
        // 在**文本**上数出这条 SQL 用几个占位符，与传来的个数比 —— 对不上就地拒。
        // 放在这里而不是绑定期，是为了**任何副作用之前**就报错（`INSERT` 尤其）。
        let (_, expected) = params::normalize_placeholders(sql)?;
        params::check_param_count(expected, params.len())?;
        // 参数作用域**必须在解析/绑定之前装上**（绑定器从这里取值）
        let _scope = params::ParamScope::new(params);
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
        if let Some(r) = role {
            check_role_permission(&stmt, r)?;
        }
        ctx.check()?;
        execute_stmt(db, stmt, ctx)
    })
}

/// 单条已翻译语句的执行（参数/角色都已处理完）。
fn execute_stmt(
    db: &Qsdb,
    stmt: translate::ParsedStatement,
    ctx: &Arc<crate::query::QueryContext>,
) -> Result<SqlResult> {
    match stmt {
        translate::ParsedStatement::CreateTable { def } => {
            db.create_table(def)?;
            Ok(SqlResult::Ok {
                rows_affected: None,
                command: None,
                message: "table created".into(),
            })
        }
        translate::ParsedStatement::CreateTableAs {
            name,
            columns,
            partition_keys,
            query,
        } => create_table_as(db, &name, &columns, &partition_keys, *query, ctx),
        translate::ParsedStatement::AlterAddColumn { table, column } => {
            let name = column.name.clone();
            db.alter_add_column(&table, column)?;
            Ok(SqlResult::Ok {
                rows_affected: None,
                command: None,
                message: format!("column '{name}' added"),
            })
        }
        translate::ParsedStatement::DropTable { name, if_exists } => match db.drop_table(&name) {
            Ok(()) => Ok(SqlResult::Ok {
                rows_affected: None,
                command: None,
                message: "table dropped".into(),
            }),
            Err(e) if if_exists && matches!(e, QsdbError::NotFound { .. }) => Ok(SqlResult::Ok {
                rows_affected: None,
                command: None,
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
                rows_affected: None,
                command: None,
                message: format!("user '{name}' created ({})", role.name()),
            })
        }
        translate::ParsedStatement::DropUser { name, if_exists } => {
            match crate::users::UserStore::drop_user(db.store(), &name) {
                Ok(()) => Ok(SqlResult::Ok {
                    rows_affected: None,
                    command: None,
                    message: format!("user '{name}' dropped"),
                }),
                Err(e) if if_exists && matches!(e, QsdbError::NotFound { .. }) => {
                    Ok(SqlResult::Ok {
                        rows_affected: None,
                        command: None,
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
                        rows_affected: None,
                        command: None,
                        message: format!("password updated for '{name}'"),
                    })
                }
                UserAlterAction::SetRole(r) => {
                    crate::users::UserStore::set_role(db.store(), &name, r)?;
                    Ok(SqlResult::Ok {
                        rows_affected: None,
                        command: None,
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
                    // VALUES 源：列清单可只给部分列，未列出的列补 NULL
                    // （2026-10-13 放宽：此前要求"必须覆盖全部表列"，理由是"宁可报错
                    //  也不要补 NULL 写进半行"——但 INSERT … SELECT 一直允许部分列，
                    //  同一条理由并不成立；而且写端本就有 NOT NULL/CHECK 兜底
                    //  （`write::check_constraints`），缺列补 NULL 后照样会被拦下来。
                    //  放宽后与标准 SQL（MySQL/PG 都允许省略列）及本仓 SELECT 源一致。）
                    let perm = insert_column_perm(&def, column_order.as_deref(), false)?;
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
                rows_affected: Some(rows),
                command: Some("DELETE"),
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
                rows_affected: None,
                command: None,
                message: format!("view '{name}' created"),
            })
        }
        translate::ParsedStatement::DropView { name, if_exists } => {
            crate::views::ViewStore::drop_view(db.store(), &name, if_exists)?;
            Ok(SqlResult::Ok {
                rows_affected: None,
                command: None,
                message: format!("view '{name}' dropped"),
            })
        }
        translate::ParsedStatement::CopyTo {
            query,
            target,
            partition_by,
        } => copy_to(db, *query, &target, &partition_by, ctx),
        translate::ParsedStatement::Truncate { table } => {
            let rows = db.truncate_table(&table)?;
            Ok(SqlResult::Ok {
                rows_affected: None,
                command: None,
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
            // （输出列名随客户端别名走——J13）
            if let Some((n, out_name)) = metadata_shortcut_count(db, &plan)? {
                let schema = Arc::new(arrow::datatypes::Schema::new(vec![
                    arrow::datatypes::Field::new(out_name, arrow::datatypes::DataType::Int64, true),
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
            // **零行结果也给一个空批 + 完整 schema**（`对接契约.md` §二 的
            // schema 协商承诺，`LIMIT 0` 是它的标准姿势——BI 工具/DBeaver
            // 探测元数据就这么写）。
            //
            // 此前零行 = **零个批**：schema 只在 `SqlResult::Query.schema` 里，
            // 逐批消费的调用方（含网线各格式）拿不到列名，得靠 server 侧特判
            // 补一个空批（J15 那批就是这么修的）。收到这里，库层即兑现契约。
            //
            // 注意**不掩盖聚合**：无 `GROUP BY` 的聚合恰好一行由 exec 保证
            // （`agg::finalize_agg` 补"空组"，见 `tests/agg.rs` 回归）；
            // 走到这里还空说明该查询本来就零行。
            if batches.is_empty() {
                batches.push(arrow::record_batch::RecordBatch::new_empty(Arc::clone(
                    &schema,
                )));
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
            res.grow(crate::memory::batch_bytes(&b))?;
            batches.push(b);
        }
        exec_builder::add_cte_batches(cte.cte_id, batches, res);
    }
    Ok(())
}

/// 检测可元数据应答的形态：Project? → Aggregate(count(*), 无 group) → Scan(predicate 仅分区列)。
/// 返回 Some(count) 表示短路命中（None 走正常执行）。
/// `count(*)` 的 **O(1) 元数据短路**：返回 `(行数, 输出列名)`。
///
/// 返回列名是必须的：调用方要拿它建结果 schema（J13——此前硬编码 `"count(*)")`，
/// 把 `SELECT COUNT(*) AS n` 的别名吞了）。
fn metadata_shortcut_count(
    db: &Qsdb,
    plan: &crate::sql::translate::LogicalPlan,
) -> Result<Option<(u64, String)>> {
    use crate::sql::translate::LogicalPlan;
    // 剥 Project —— 但只认「**恰好一个裸列引用**」的投影：
    //   · 多个输出列（`SELECT COUNT(*), 1 AS k FROM t`）→ 短路只回一列，**静默吞列**
    //   · 表达式投影（`SELECT COUNT(*) + 1 AS n FROM t`）→ 短路拿原始 count 顶上，
    //     **值直接算错**（实测 SQL 里的 `+1` 被无声忽略）
    // 两条都是"用户拿到错东西还不报错"，必须让开走常规路径（慢一点但正确）。
    let mut out_name = "count(*)".to_string();
    let mut cur = plan;
    if let LogicalPlan::Project {
        input,
        exprs,
        names,
    } = cur
    {
        let plain_single_col =
            exprs.len() == 1 && matches!(exprs[0], crate::expr::PhysExpr::Column { .. });
        if !plain_single_col {
            return Ok(None);
        }
        if let Some(n) = names.first() {
            out_name = n.clone(); // 别名透出（J13）
        }
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
    // ⚠️ **写了 `FILTER` 就不能走元数据短路**（2026-10-15）：这条短路直接回
    // parquet footer 的行数，压根不看行 —— `count(*) FILTER (WHERE v>15)` 会被
    // 当成 `count(*)` 回**全量行数**（实测 5 而非 2），而且**一声不响**。
    // 新加任何"整表行数"式捷径时，都要问一句"这个聚合还带什么逐行条件"。
    if agg.filter.is_some() {
        return Ok(None);
    }
    // Scan：谓词仅引用分区列
    let LogicalPlan::Scan {
        def,
        predicate,
        read: _,
    } = input
    else {
        return Ok(None);
    };
    // 无分区表：无谓词 → footer 元数据（parquet num_rows，零解码，DuckDB 同款）；
    // 有谓词需数据 → 交给常规扫描。
    if def.partition_keys.is_empty() {
        if predicate.is_some() {
            return Ok(None);
        }
        return Ok(db
            .scan_metadata_count(&def.name, None)?
            .map(|n| (n, out_name)));
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
    Ok(db
        .scan_metadata_count(&def.name, predicate.as_ref())?
        .map(|n| (n, out_name)))
}

// ---------------------------------------------------------------------------
// INSERT（VALUES / SELECT 两条源，共用列映射与写入端）
// ---------------------------------------------------------------------------

/// 目标列映射：源第 `i` 列 → 表第 `perm[i]` 列。
///
/// `column_order` 是**位置语义**（标准 SQL：INSERT…VALUES 与 INSERT…SELECT 都按
/// 位置对齐列清单，不按名匹配），所以这里只把列名翻译成表列下标。
///
/// `require_full`：列清单是否必须覆盖全部表列。**两个源现在都传 `false`**
/// （未列出的列补 NULL，NOT NULL/CHECK 由写入端兜底，见 `write::check_constraints`），
/// 保留参数是为了让"要不要收紧"这件事在调用点可见。
///
/// 历史上 VALUES 源传 `true`（"宁可报错也不要补 NULL 写进半行"）——与 SELECT 源
/// 口径不一致，2026-10-13 统一放宽。
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
    // 只有**全宽恒等**才是 no-op。`perm = [0]`（"表列前缀"的部分列清单）看着
    // 像恒等（第 0 位对第 0 列），其实是一次**投影**——直接返回会让行宽停在
    // 列清单宽度上，下面 `build_insert_batch` 就报"行宽 ≠ 表列数"。
    // 症状：`INSERT INTO t (a) VALUES (…)` 400，而 `(a, c)` 这种非前缀清单正常。
    if perm.len() == def.columns.len() && perm.iter().enumerate().all(|(i, &p)| i == p) {
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

/// **`Literal` 行 → 表列序 RecordBatch**（公开：服务端的 CSV/bulk 写入路径也要它）。
/// 与 `INSERT … VALUES` 走的是同一份构造逻辑，故列序/类型/空值语义天然一致。
pub fn rows_to_batch(
    def: &crate::catalog::TableDef,
    rows: &[Vec<crate::types::Literal>],
) -> Result<RecordBatch> {
    build_insert_batch(def, rows)
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
const INSERT_FLUSH_BYTES: u64 = 32 * 1024 * 1024;
/// **行数**阈值：字节阈值在切片批上曾经失灵（见 `memory::batch_bytes` 的注释——
/// `get_array_memory_size()` 报父缓冲大小，8192 行切片报 4MB，于是"每批都超 32MB"
/// → 每批提交一次 → 5M 行 610 次提交 × 3 次 fsync = 60s）。行数不受切片影响，
/// 作为主判据；字节判据（切片感知口径）只用于"宽行"兜底。
const INSERT_FLUSH_ROWS: u64 = 512 * 1024;

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
    pending_rows: u64,
    /// OVERWRITE：分区值 → 该分区全宽批
    buckets: Vec<(Vec<Literal>, Vec<RecordBatch>)>,
    total: u64,
    /// 分段计时（`QSDB_INSERT_PROF=1` 时打印）：对齐 / 落段 / 批数。
    /// 动机：`INSERT … SELECT` 曾比 `db.append` 慢 6 倍而同为"扫 + 写"，
    /// 没有分段数字只能猜（见 `docs/真机基准对账.md`）。
    prof_align_ns: u128,
    prof_append_ns: u128,
    prof_batches: u64,
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
            pending_rows: 0,
            buckets: Vec::new(),
            total: 0,
            prof_align_ns: 0,
            prof_append_ns: 0,
            prof_batches: 0,
        })
    }

    /// 推入一批**源列序**的批（SELECT 结果），内部先按 `perm` 对齐到表列序。
    fn push(&mut self, batch: &RecordBatch) -> Result<()> {
        let t0 = std::time::Instant::now();
        let aligned = align_insert_batch(self.def, self.perm, batch)?;
        self.prof_align_ns += t0.elapsed().as_nanos();
        self.prof_batches += 1;
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
        let bytes = crate::memory::batch_bytes(&aligned);
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
            self.pending_rows += aligned.num_rows() as u64;
            self.pending.push(aligned);
            self.pending_bytes += bytes;
            if self.pending_rows >= INSERT_FLUSH_ROWS || self.pending_bytes >= INSERT_FLUSH_BYTES {
                // 行数为主（切片无关），字节为辅

                self.flush_pending()?;
            }
        }
        Ok(())
    }

    fn flush_pending(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        if std::env::var("QSDB_INSERT_PROF").is_ok() {
            eprintln!(
                "[insert-flush] batches={} rows={} bytes={}",
                self.pending.len(),
                self.pending_rows,
                self.pending_bytes
            );
        }
        let t0 = std::time::Instant::now();
        let r: WriteResult = self.db.append(self.table, &self.pending)?;
        self.prof_append_ns += t0.elapsed().as_nanos();
        self.total += r.rows;
        self.pending.clear();
        self.res.shrink(self.pending_bytes);
        self.pending_bytes = 0;
        self.pending_rows = 0;
        Ok(())
    }

    fn finish(mut self) -> Result<u64> {
        if std::env::var("QSDB_INSERT_PROF").is_ok() {
            eprintln!(
                "[insert-prof] batches={} align={:.3}s append={:.3}s total_rows={}",
                self.prof_batches,
                self.prof_align_ns as f64 / 1e9,
                self.prof_append_ns as f64 / 1e9,
                self.total
            );
        }
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

/// 分区值序列等价——转发 [`crate::partition::same_partition_values`]
/// （与写入端划分口径同源；`api.rs` 的 append 归并也用同一个）。
fn same_partition(a: &[Literal], b: &[Literal]) -> bool {
    crate::partition::same_partition_values(a, b)
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
        rows_affected: Some(total),
        command: Some("INSERT"),
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
/// CTAS 推导列名时**去掉表限定**：`a.g` ⇒ `g`（与 PG 一致）。
///
/// 为什么必须剥：JOIN 场景下本仓的列名是**限定名**（`a.g` —— 裸名恒解析到左表，
/// 右表列必须限定，见 `SQL支持面.md` §二），而表列名不允许 `.`
/// ⇒ 不剥的话 `CREATE TABLE j AS SELECT a.g, b.v FROM t a JOIN t b …` 必被拒，
/// 而那是最普通的写法之一。
///
/// 只在**形如 `标识符.标识符`** 时剥：表列名限 `[A-Za-z_][A-Za-z0-9_]*`，
/// 所以带 `.` 的推导名**只可能**是限定引用（不可能是"某个列本来就叫 a.b"）。
/// 剥完若重名（`SELECT a.g, b.g`），下游的重复检查会报出来。
fn strip_table_qualifier(n: &str) -> &str {
    match n.rsplit_once('.') {
        Some((q, c)) if crate::catalog::ident_ok(q) && crate::catalog::ident_ok(c) => c,
        _ => n,
    }
}

/// `CREATE TABLE … AS SELECT …`：**派 schema → 建表 → 跑查询落表**，失败则回滚。
///
/// 与"手工两步"（`CREATE TABLE` + `INSERT INTO … SELECT …`）的差别只有两条：
/// ① 列名/类型由 SELECT 的**输出**推导（没写列清单时）；
/// ② 失败时把刚建的表**删掉** —— PG 的 CTAS 是全有全无的，留一张空表让人以为
///    "建好了、只是没数据"比报错更坏。
/// `COPY (SELECT …) TO '<目录>' (FORMAT PARQUET [, PARTITION_BY (…)])`（F9 / A3）。
///
/// 与 `create_table_as` 同构：绑计划 → 建 exec → 拉批。区别只在批的去处 ——
/// 那边是 `InsertSink`（落表），这边是 `crate::export`（hive 目录 + parquet 文件）。
///
/// 分工：**这里只管绑计划与拉批**，分区切分、目录布局、并行编码全在
/// `crate::export` 里（那部分不依赖执行器，可单测）。
fn copy_to(
    db: &Qsdb,
    query: sqlparser::ast::Query,
    target: &str,
    partition_by: &[String],
    ctx: &Arc<crate::query::QueryContext>,
) -> Result<SqlResult> {
    let resolver = translate::TableResolver {
        resolve: Box::new(|n: &str| db.table_def(n)),
        views: crate::views::ViewStore::read_all(db.store())?,
    };
    let (plan, cte_defs) = translate::translate_select_full_with_ctes(query, &resolver)?;
    let _cte_scope = exec_builder::CteScope::new();
    materialize_ctes(db, &cte_defs, ctx)?;
    ctx.check()?;
    let mut exec = exec_builder::build(db, &plan, ctx)?;
    let schema = exec.schema();
    let stats = crate::export::write_dataset(
        db.store(),
        target,
        &schema,
        partition_by,
        // 拉批时**每次都查 ctx** —— 大导出是长时间跑的，取消/超时/内存压力
        // 得能中断它（`cancel_latency` 那批的教训：只在入口查一次等于没有）。
        &mut || {
            ctx.check()?;
            match exec.next() {
                Some(b) => Ok(Some(b?)),
                None => Ok(None),
            }
        },
    )?;
    Ok(SqlResult::Ok {
        message: format!(
            "exported {} rows → {} ({} files, {} partitions, {:.1} MB)",
            stats.rows,
            target,
            stats.files,
            stats.partitions,
            stats.bytes as f64 / (1024.0 * 1024.0)
        ),
        rows_affected: Some(stats.rows),
        command: Some("COPY"),
    })
}

fn create_table_as(
    db: &Qsdb,
    name: &str,
    columns: &[(String, crate::types::LogicalType)],
    partition_keys: &[String],
    query: sqlparser::ast::Query,
    ctx: &Arc<crate::query::QueryContext>,
) -> Result<SqlResult> {
    // ① 先绑计划：schema 要从它推出来，**建表之前**就得有。
    let resolver = translate::TableResolver {
        resolve: Box::new(|n: &str| db.table_def(n)),
        views: crate::views::ViewStore::read_all(db.store())?,
    };
    let (plan, cte_defs) = translate::translate_select_full_with_ctes(query, &resolver)?;
    let out = translate::plan_output_columns(&plan)?;
    if out.is_empty() {
        return Err(QsdbError::invalid(
            "CREATE TABLE … AS SELECT 的查询产出了 0 列 —— 建不出表",
        ));
    }

    // ② 定列名/列类型
    let cols: Vec<(String, crate::types::LogicalType)> = if columns.is_empty() {
        // 没写列清单 ⇒ 用 SELECT 的输出名。**输出名可以是表达式文本**
        // （`count(*)` / `a + b`），那种名字建不成表列（`validate_ident` 只认
        // `[A-Za-z_][A-Za-z0-9_]*`）—— 建表前就拦住并**指路**。
        // 为什么不替用户改名（PG 会把它们改成 `count` / `?column?`）：那是**另一种
        // 静默** —— 表里会出现一个他从没写过的列名，而"改名"这件事本身还可能撞车。
        let out: Vec<(String, crate::types::LogicalType)> = out
            .into_iter()
            .map(|(n, t)| (strip_table_qualifier(&n).to_string(), t))
            .collect();
        for (i, (n, _)) in out.iter().enumerate() {
            if !crate::catalog::ident_ok(n) {
                return Err(QsdbError::invalid(format!(
                    "CTAS 推导出的第 {} 列名 `{n}` 不是合法表列名（限 [A-Za-z_][A-Za-z0-9_]*、\
                     ≤64 字符）—— 给表达式起个别名（`… AS col{i}`），或显式写列清单\
                     （`CREATE TABLE {name} (c1 BIGINT, …) AS SELECT …`）",
                    i + 1,
                )));
            }
        }
        let mut seen = std::collections::HashSet::new();
        if let Some(dup) = out.iter().find(|(n, _)| !seen.insert(n.clone())) {
            return Err(QsdbError::invalid(format!(
                "CTAS 推导出的列名有重复：`{}` 出现两次 —— 请给其中一个起别名",
                dup.0
            )));
        }
        out
    } else {
        // 写了列清单 ⇒ 名字与类型都用用户写的。**列数必须一致**：不一致就是
        // "你建的表和你要塞的数据对不上"，在建表前说清楚（否则建完才炸，还得回滚）。
        if columns.len() != out.len() {
            return Err(QsdbError::invalid(format!(
                "CREATE TABLE … AS SELECT：列清单 {} 列，而查询产出 {} 列",
                columns.len(),
                out.len()
            )));
        }
        columns.to_vec()
    };

    // ③ 建表（`TableDef::validate` 顺手把分区列的规则也校验了：必须是列、必须
    //    nullable、类型受限、至少要有一个非分区列）
    let defs: Vec<crate::catalog::ColumnDef> = cols
        .iter()
        .map(|(n, t)| crate::sql::make_column(n, t.clone(), true, None, None))
        .collect();
    let def = crate::catalog::TableDef::new(name.to_string(), defs, partition_keys.to_vec())?;
    db.create_table(def.clone())?;

    // ④ 跑计划 → 落表。**失败就回滚**（表刚建、里面不可能有别人写的数据）。
    let perm: Vec<usize> = (0..cols.len()).collect();
    let run = || -> Result<u64> {
        let _cte_scope = exec_builder::CteScope::new();
        materialize_ctes(db, &cte_defs, ctx)?;
        let mut exec = exec_builder::build(db, &plan, ctx)?;
        let mut sink = InsertSink::new(db, name, &def, &perm, false, ctx)?;
        while let Some(b) = exec.next() {
            ctx.check()?;
            sink.push(&b?)?;
        }
        sink.finish()
    };
    match run() {
        Ok(n) => Ok(SqlResult::Ok {
            rows_affected: Some(n),
            command: Some("CREATE TABLE AS"),
            message: format!("table '{name}' created, {n} rows inserted"),
        }),
        Err(e) => {
            // 回滚：删掉刚建的表。删失败也不掩盖**原始**错误（那才是要修的）
            let _ = db.drop_table(name);
            Err(e)
        }
    }
}

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
        rows_affected: Some(total),
        command: Some("INSERT"),
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
