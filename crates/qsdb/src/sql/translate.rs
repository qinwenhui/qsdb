//! AST → LogicalPlan：类型映射、字面量、表达式绑定（binder）、谓词下推拆分。

use std::sync::Arc;

use sqlparser::ast::{SetExpr, Statement as SqlStatement};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::catalog::TableDef;
use crate::error::{QsdbError, Result};
use crate::exec::SortKey;
use crate::expr::{AggExpr, AggFunc, ArithOp, CmpOp, PhysExpr, scalar};
use crate::scan::Predicate;
use crate::types::cast::cast_literal;
use crate::types::{Literal, LogicalType, decimal::DecimalMeta, temporal};

// ---------------------------------------------------------------------------
// LogicalPlan
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub enum LogicalPlan {
    /// 全表列扫描（含分区列常量重建）；predicate = 下推裁剪谓词
    Scan {
        def: Arc<TableDef>,
        predicate: Option<Predicate>,
        /// **只解码这些列**（`None` = 全部）。列还在、列序不变，未列出的产出 NULL
        /// ——见 `Scan::restrict_read`。由 `exec_builder::annotate_join_needs`
        /// 在"能证明下游不读它"时设置（join 之下的聚合链）。
        ///
        /// 收益（2026-10-14 实测）：扫描代价几乎线性于解码列宽 —— 同一张 5M 行表
        /// 只读 2 列 0.18s、读全 7 列 0.63s，而基准那句 join 里扫描占 53%。
        read: Option<Arc<Vec<String>>>,
    },
    /// 外表 parquet 扫描（read_parquet，M6-2）：文件列表绑定期解析，
    /// schema 取自首文件 footer；无分区列
    ExternalScan {
        def: Arc<TableDef>,
        files: Arc<Vec<crate::scan::FileLoc>>,
        predicate: Option<Predicate>,
    },
    /// 外表 CSV 扫描（read_csv）：文件列表 + 表头/类型在绑定期确定（推断一次，
    /// 全批稳定）。刻意不做谓词下推——CSV 无 footer/RG 元数据，谓词留在
    /// FilterExec（少一层"扫描内过滤"的特殊分支）；列裁剪由 exec_builder 的
    /// 通用投影下推覆盖（见 `plan_scan_def`）。
    CsvScan {
        def: Arc<TableDef>,
        files: Arc<Vec<crate::scan::FileLoc>>,
    },
    Filter {
        input: Box<LogicalPlan>,
        predicate: PhysExpr,
    },
    Project {
        input: Box<LogicalPlan>,
        exprs: Vec<PhysExpr>,
        names: Vec<String>,
    },
    Aggregate {
        input: Box<LogicalPlan>,
        group: Vec<PhysExpr>,
        aggs: Vec<AggExpr>,
    },
    Sort {
        input: Box<LogicalPlan>,
        keys: Vec<SortKey>,
    },
    Limit {
        input: Box<LogicalPlan>,
        /// `None` = 不限（只写 `OFFSET n` 的形态）。
        n: Option<usize>,
        /// `OFFSET n` 的起始行数（此前**被静默丢掉** ⇒ 少翻页）。
        offset: usize,
    },
    Distinct {
        input: Box<LogicalPlan>,
    },
    /// 窗口函数：PARTITION BY + ORDER BY + 窗口函数列表
    Window {
        input: Box<LogicalPlan>,
        partition_by: Vec<PhysExpr>,
        /// 窗口内 `ORDER BY`：与外部排序**共用 `SortKey`**（含 `NULLS FIRST/LAST`），
        /// 免得两处各有一套 NULL 语义而慢慢漂移。
        order_by: Vec<crate::exec::SortKey>,
        window_fns: Vec<crate::exec::window::WindowFn>,
    },
    /// 内存批源（CTE 物化；执行层持有批数据，binder 只带 schema 描述）
    Values {
        columns: Vec<(String, LogicalType)>,
        cte_id: u32,
    },
    /// 非等值 INNER：cross join（左 × 右）后在合并 schema 上过滤
    CrossJoinFilter {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        predicate: PhysExpr,
    },
    /// 等值连接（left=probe right=build）；输出列 = 左 ++ 右
    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        left_keys: Vec<PhysExpr>,
        right_keys: Vec<PhysExpr>,
        join_type: crate::exec::join::JoinType,
        /// **只需物化这些输出位置**（`None` = 全部，即今天的行为）。
        ///
        /// 位置序 = 输出序（左全列 ++ 右全列），与 `HashJoinExec::schema()` 一致；
        /// 不在表里的位置**填 NULL**（列还在、schema 不变，只是没人读）。
        ///
        /// **为什么要有它**（2026-10-14 实测）：join 输出宽 = 左全列 + 右全列，
        /// 而下游聚合往往只读其中一两列。基准那句 `t×d + GROUP BY` 产出 **1000 万行
        /// × 10 列**（含 4 个字符串列），聚合只读 2 列 —— 单线程 1.27s 里约
        /// **0.85s 白花在"物化 10 列再丢掉 8 列"**。等价的列宽实验：
        /// 输出 10 列 → 5 列，**时间直接减半**（1.33s → 0.63s，RSS 300MB → 111MB）。
        ///
        /// ⚠️ **它只许"收窄到能证明没人读的列"**：判错一列 = 那一列读出 NULL =
        /// **静默错值**。故标注逻辑（`exec_builder::annotate_join_needs`）只在
        /// "join 输出**唯一**的读者可枚举"时才动手，其余一律 `None`。
        needed: Option<std::sync::Arc<Vec<u32>>>,
    },
    /// 单行零列源（无 FROM SELECT）
    OneRow,
    /// 集合操作（M7-B4）：UNION [ALL] / INTERSECT / EXCEPT（DISTINCT 语义）。
    /// 两侧 schema 已归一（公共超类型 cast）；输出列名取左。
    SetOp {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        op: crate::exec::SetOpKind,
        /// UNION ALL = 不去重直连；其余形态内部去重
        all: bool,
    },
}

// ---------------------------------------------------------------------------
// 语句解析
// ---------------------------------------------------------------------------

pub enum UserAlterAction {
    SetPassword(String),
    SetRole(crate::users::Role),
}

/// INSERT VALUES 行集（每行按给定列序的字面量）
pub type InsertRows = Vec<Vec<Literal>>;

// ---------------------------------------------------------------------------
// 绑定前的「静默丢弃」闸门（2026-10-13 系统扫描）
// ---------------------------------------------------------------------------

/// **凡不实现的子句，一律显式报错——绝不无声丢弃。**
///
/// **为什么需要它**：绑定器是**逐字段读**的——它只读它认识的那些字段，AST 里其余
/// 字段**默认被忽略**。于是"语法合法但没实现"的子句会安静地按"没写"执行。
/// 2026-10-13 逐条实测（`tests/sql_silent_drop.rs` 就是这批的回归），全都不报错：
///
/// | 写了 | 实际执行成 | 后果 |
/// |---|---|---|
/// | `count(*) FILTER (WHERE a>1)` | `count(*)` | **错值**（80 而非 10） |
/// | `sum(x) OVER (ORDER BY y ROWS BETWEEN …)` | 默认帧 | **错值** |
/// | `INSERT … ON CONFLICT DO NOTHING` | 照插 | **重复数据** |
/// | `LIMIT 10 OFFSET 20` / `OFFSET n ROWS` | 只 LIMIT | **错行** |
/// | `ORDER BY x NULLS FIRST`（及窗口内） | 默认 NULLS LAST | **错序** |
/// | `DISTINCT ON (a)` | 普通 `DISTINCT` | **错行** |
/// | `TABLESAMPLE (10 PERCENT)` / `FETCH FIRST 10 ROWS ONLY` | 全表 | 多返回 |
/// | `SELECT * EXCLUDE (b)` / `* REPLACE` / `* RENAME` | `SELECT *` | 多列/列名不对 |
/// | `WITH w(q) AS …` / `… AS d(x)` | 列别名丢掉 | 之后"列不存在" |
/// | `FROM t AT (VERSION => …)` / `PARTITION (…)` | 当普通表读 | **读了错的数据** |
///
/// 这类 bug **不报错、不打日志**，只能靠人撞见（`count(DISTINCT)` 那条就是手滑撞见的）。
/// 所以这里反过来做：**枚举"我们不读的字段"，逐个判处置**。
///
/// **维护约定**：新增 SQL 能力时，要么在绑定器里读那个字段，要么在这里拒绝
/// ——**没有第三条路**。本函数的检查表 = `docs/SQL支持面.md` 的"显式拒绝"列。
/// 例外：**纯建议性**的东西（`optimizer_hints` 这类 hint）忽略掉是正确语义，不列。
///
/// 挂载点是绑定器自己的递归入口（见 `translate_select_inner`），所以**嵌套子查询
/// / 派生表 / CTE 体自动一并被审**，不需要额外遍历表达式树。
fn audit_query(q: &sqlparser::ast::Query) -> Result<()> {
    // —— Query 级：这些子句我们都不读 ——
    let reject = |what: &str| -> Result<()> {
        Err(QsdbError::unsupported(format!(
            "{what} is not supported — refusing rather than silently ignoring it \
             (a silently-dropped clause returns wrong results without any error)"
        )))
    };
    if !q.locks.is_empty() {
        return reject("row locking (FOR UPDATE / FOR SHARE)");
    }
    if q.for_clause.is_some() {
        return reject("FOR XML/JSON clause");
    }
    if q.settings.is_some() {
        return reject("SETTINGS clause");
    }
    if q.format_clause.is_some() {
        return reject("FORMAT clause");
    }
    if !q.pipe_operators.is_empty() {
        return reject("pipe operators (|>)");
    }
    // `LIMIT` / `OFFSET` / `FETCH FIRST n ROWS ONLY`：**已实现**（见
    // `bind_limit_offset`），故不在此拒绝。`FETCH … PERCENT / WITH TIES`
    // 语义不同，在那个助手内单独拒绝。
    // CTE 列别名：`WITH w(q) AS …`
    if let Some(w) = &q.with {
        for cte in &w.cte_tables {
            if !cte.alias.columns.is_empty() {
                return reject("CTE column alias list (WITH w(a,b) AS …)");
            }
            if cte.from.is_some() {
                return reject("CTE FROM clause");
            }
        }
    }
    // ORDER BY 的 NULLS FIRST/LAST 与 WITH FILL
    if let Some(ob) = &q.order_by {
        audit_order_by(&ob.kind)?;
    }
    Ok(())
}

/// `ORDER BY` 的选项位：只读 `sort`（方向），**`nulls_first` 与 `with_fill` 都没读**。
fn audit_order_by(kind: &sqlparser::ast::OrderByKind) -> Result<()> {
    let sqlparser::ast::OrderByKind::Expressions(es) = kind else {
        return Err(QsdbError::unsupported("ORDER BY ALL"));
    };
    for ob in es {
        // `nulls_first` **已实现**：显式值优先，否则默认放置
        // （ASC ⇒ NULLS LAST / DESC ⇒ NULLS FIRST，见 `exec::SortKey::nulls_first_or_default`）。
        // 这里只审仍未实现的 `with_fill`。
        if ob.with_fill.is_some() {
            return Err(QsdbError::unsupported("ORDER BY … WITH FILL"));
        }
    }
    Ok(())
}

/// `SELECT` 级闸门（见 [`audit_query`] 的说明）。
fn audit_select(s: &sqlparser::ast::Select) -> Result<()> {
    use sqlparser::ast::{Distinct, GroupByExpr};
    let reject = |what: &str| -> Result<()> {
        Err(QsdbError::unsupported(format!(
            "{what} is not supported — refusing rather than silently ignoring it"
        )))
    };
    // `DISTINCT ON (…)` ≠ `DISTINCT`：前者是"每键取一行"，静默当普通 DISTINCT
    // 会**多返回行**（实测 `DISTINCT ON (s)` 与 `DISTINCT s, a` 结果一致）。
    if let Some(Distinct::On(_)) = &s.distinct {
        return reject("DISTINCT ON (…)");
    }
    if s.top.is_some() {
        return reject("TOP n");
    }
    if s.into.is_some() {
        return reject("SELECT … INTO");
    }
    if s.exclude.is_some() {
        return reject("SELECT … EXCLUDE");
    }
    if s.qualify.is_some() {
        return reject("QUALIFY");
    }
    if s.prewhere.is_some() {
        return reject("PREWHERE");
    }
    if !s.lateral_views.is_empty() {
        return reject("LATERAL VIEW");
    }
    if !s.connect_by.is_empty() {
        return reject("CONNECT BY (hierarchical query)");
    }
    if !s.cluster_by.is_empty() {
        return reject("CLUSTER BY");
    }
    if !s.distribute_by.is_empty() {
        return reject("DISTRIBUTE BY");
    }
    if !s.sort_by.is_empty() {
        return reject("SORT BY");
    }
    if s.select_modifiers.is_some() {
        return reject("select modifiers");
    }
    if s.value_table_mode.is_some() {
        return reject("value table mode");
    }
    if let GroupByExpr::Expressions(_, mods) = &s.group_by {
        if !mods.is_empty() {
            return reject("GROUP BY ROLLUP/CUBE/GROUPING SETS");
        }
    }
    // 投影：通配符的附加选项（EXCLUDE/EXCEPT/REPLACE/RENAME/ILIKE/别名）
    for item in &s.projection {
        if let sqlparser::ast::SelectItem::Wildcard(opts) = item {
            if opts.opt_exclude.is_some()
                || opts.opt_except.is_some()
                || opts.opt_replace.is_some()
                || opts.opt_rename.is_some()
                || opts.opt_ilike.is_some()
                || opts.opt_alias.is_some()
            {
                return reject("SELECT * EXCLUDE/EXCEPT/REPLACE/RENAME/ILIKE");
            }
        }
    }
    // FROM：表因子的"我们没读的字段"
    for twj in &s.from {
        audit_table_factor(&twj.relation)?;
        for j in &twj.joins {
            audit_table_factor(&j.relation)?;
        }
    }
    Ok(())
}

/// 表因子：派生表**列别名**、以及读错数据的几类表修饰（见 [`audit_query`]）。
fn audit_table_factor(tf: &sqlparser::ast::TableFactor) -> Result<()> {
    use sqlparser::ast::TableFactor;
    let reject = |what: &str| -> Result<()> {
        Err(QsdbError::unsupported(format!(
            "{what} is not supported — refusing rather than silently ignoring it"
        )))
    };
    match tf {
        TableFactor::Table {
            alias,
            sample,
            version,
            partitions,
            with_hints,
            args,
            ..
        } => {
            // **表别名上的列清单**（`FROM t AS x(a,b)`）——同样被静默丢掉。
            // 顺带提醒一个真实的踩坑：`SELECT * FROM t EXCLUDE (b)` 会被解析成
            // 「别名 `exclude` + 列清单 `(b)`」，于是用户以为在写 Snowflake 的
            // EXCLUDE，实际拿到了**全列**（静默多列）。故这里必须拦。
            if let Some(a) = alias {
                if !a.columns.is_empty() {
                    return reject(
                        "column alias list on a table factor (… AS x(a,b)) — note                          `SELECT * FROM t EXCLUDE (b)` parses as an alias `exclude`                          with a dropped column list, not as EXCLUDE",
                    );
                }
            }
            if sample.is_some() {
                return reject("TABLESAMPLE");
            }
            // 时间旅行 / 分区选择：**静默忽略会读到别的数据**（最坏的一类）
            if version.is_some() {
                return reject("table version/time travel (AT VERSION / FOR SYSTEM_TIME)");
            }
            if !partitions.is_empty() {
                return reject("table PARTITION (…) selector");
            }
            if !with_hints.is_empty() {
                return reject("table hints (WITH (…))");
            }
            // 注意 `args` **不在**本闸门内：`read_parquet('…')` 这类表函数正是
            // 经 `TableFactor::Table { args: Some(..) }` 表达，dpc 绑定器有专门分支
            // （未知表函数会显式报错）——它不是"被忽略的字段"。
            let _ = args;
        }
        TableFactor::Derived { alias: Some(a), .. } if !a.columns.is_empty() => {
            return reject("derived-table column alias list (… AS d(a,b))");
        }
        _ => {}
    }
    Ok(())
}

/// 函数调用上的子句：`FILTER (WHERE …)` / `IGNORE NULLS` / 参数内 `ORDER BY` 等。
///
/// 放在**绑定函数的必经点**（`bind_expr` 的 Function 臂与窗口绑定），因为表达式
/// 树里函数可以出现在任何位置（投影 / WHERE / HAVING / ORDER BY），而这些位置
/// 绑定器都会走到——不需要单独遍历表达式树。
fn audit_function(f: &sqlparser::ast::Function) -> Result<()> {
    let reject = |what: &str| -> Result<()> {
        Err(QsdbError::unsupported(format!(
            "{what} is not supported — refusing rather than silently ignoring it \
             (a silently-dropped clause returns wrong results without any error)"
        )))
    };
    if f.filter.is_some() {
        return reject("aggregate FILTER (WHERE …)");
    }
    if f.null_treatment.is_some() {
        return reject("IGNORE/RESPECT NULLS");
    }
    if !f.within_group.is_empty() {
        return reject("WITHIN GROUP (ORDER BY …)");
    }
    if !matches!(f.parameters, sqlparser::ast::FunctionArguments::None) {
        return reject("function parameters");
    }
    if let sqlparser::ast::FunctionArguments::List(l) = &f.args {
        if !l.clauses.is_empty() {
            return reject("clauses inside function arguments (ORDER BY / LIMIT / SEPARATOR)");
        }
    }
    Ok(())
}

/// 窗口说明上的子句：**帧边界**（`ROWS/RANGE BETWEEN …`）与窗口内 `ORDER BY` 选项。
///
/// 帧静默忽略最阴：`ROWS BETWEEN 1 PRECEDING AND CURRENT ROW` 被当成默认帧
/// （累计），结果**换个数字、不报错**。
fn audit_window_spec(spec: &sqlparser::ast::WindowSpec) -> Result<()> {
    if spec.window_frame.is_some() {
        return Err(QsdbError::unsupported(
            "window frame (ROWS/RANGE BETWEEN …) is not supported — refusing rather than \
             silently using the default frame (which returns different numbers, not an error)",
        ));
    }
    audit_order_by(&sqlparser::ast::OrderByKind::Expressions(
        spec.order_by.clone(),
    ))
}

/// INSERT 的源：字面量行集（VALUES）或一条完整 SELECT（含 CTE/集合操作）。
///
/// 为什么把源做成 enum 而不是"两条语句"：解析期已能判别源形态
/// （`SetExpr::Values` vs 其余），此后 VALUES 与 SELECT 除"取值"外
/// 完全同路（列序映射 / 类型校验 / 约束 / 分区路由）——执行侧只需换
/// 取批来源，见 `sql::insert_rows_into_batches` 与 `insert_select_into`。
pub enum InsertSource {
    Values(InsertRows),
    Select(Box<sqlparser::ast::Query>),
}

pub enum ParsedStatement {
    CreateTable {
        def: TableDef,
    },
    DropTable {
        name: String,
        if_exists: bool,
    },
    Describe {
        table: String,
    },
    CreateView {
        name: String,
        sql: String,
        or_replace: bool,
    },
    DropView {
        name: String,
        if_exists: bool,
    },
    ShowTables,
    ShowPartitions {
        table: String,
    },
    Insert {
        table: String,
        source: InsertSource,
        /// Some(列名顺序) = INSERT INTO t (a, b, …)；None = 表全列序
        column_order: Option<Vec<String>>,
        overwrite: bool,
    },
    Select(Box<sqlparser::ast::Query>),
    /// ALTER TABLE t ADD COLUMN name type（只加不删；列必须 nullable）
    AlterAddColumn {
        table: String,
        column: crate::catalog::ColumnDef,
    },
    /// DELETE FROM t WHERE <仅分区键谓词>（分区粒度原子清空）
    Delete {
        table: String,
        selection: Option<String>,
    },
    // -- 用户体系（M7-A；Admin 专属，见 check_role_permission）----------
    /// CREATE USER name WITH PASSWORD 'pw' [ROLE admin|writer|reader]
    CreateUser {
        name: String,
        password: String,
        role: crate::users::Role,
    },
    /// DROP USER name [IF EXISTS]
    DropUser {
        name: String,
        if_exists: bool,
    },
    /// ALTER USER name SET PASSWORD 'pw' / SET ROLE r
    AlterUser {
        name: String,
        action: UserAlterAction,
    },
    ShowUsers,
    Truncate {
        table: String,
    },
}

pub fn parse_statements(sql: &str) -> Result<Vec<ParsedStatement>> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_ascii_uppercase();
    // 轻量语句手解析（SHOW/DESCRIBE 不在通用方言稳定支持范围）
    if upper == "SHOW TABLES" {
        return Ok(vec![ParsedStatement::ShowTables]);
    }
    // -- 用户体系手工解析（sqlparser 无用户语法；Admin 专属）----------
    if upper == "SHOW USERS" {
        return Ok(vec![ParsedStatement::ShowUsers]);
    }
    if let Some(rest) = upper.strip_prefix("CREATE USER ") {
        // CREATE USER name WITH PASSWORD 'pw' [ROLE r]
        if let Some(caps) = parse_create_user(rest, trimmed) {
            return Ok(vec![caps]);
        }
    }
    if let Some(rest) = upper.strip_prefix("DROP USER ") {
        let rest = rest.trim();
        let if_exists = rest.starts_with("IF EXISTS ");
        let name = rest.trim_start_matches("IF EXISTS ").trim();
        if is_ident_str(name) {
            return Ok(vec![ParsedStatement::DropUser {
                name: name.to_ascii_lowercase(),
                if_exists,
            }]);
        }
    }
    if let Some(rest) = upper.strip_prefix("ALTER USER ") {
        // ALTER USER name SET PASSWORD 'pw' | SET ROLE r
        if let Some((name, action)) = parse_alter_user(rest, trimmed) {
            return Ok(vec![ParsedStatement::AlterUser { name, action }]);
        }
    }
    if upper.starts_with("SHOW PARTITIONS ") {
        let rest = &trimmed["SHOW PARTITIONS".len()..].trim_start();
        if !rest.contains(' ') && !rest.is_empty() {
            return Ok(vec![ParsedStatement::ShowPartitions {
                table: rest.to_string(),
            }]);
        }
    }
    if let Some(rest) = trimmed.strip_prefix("DESCRIBE ").or_else(|| {
        trimmed
            .strip_prefix("describe ")
            .or_else(|| trimmed.strip_prefix("DESC "))
            .or_else(|| trimmed.strip_prefix("desc "))
    }) {
        let table = rest.trim();
        if !table.is_empty() && !table.contains(' ') {
            return Ok(vec![ParsedStatement::Describe {
                table: table.to_string(),
            }]);
        }
    }

    let ast = Parser::parse_sql(&GenericDialect {}, trimmed)
        .map_err(|e| QsdbError::parse_err(format!("SQL parse error: {e}")))?;
    ast.into_iter()
        .map(|stmt| translate_statement(stmt, trimmed))
        .collect()
}

/// 解析独立表达式文本（CHECK 约束以字符串存储，写入端重解析绑定）。
pub(crate) fn parse_expr_sql(s: &str) -> Result<sqlparser::ast::Expr> {
    let stmts = Parser::parse_sql(&GenericDialect {}, &format!("SELECT {s}"))
        .map_err(|e| QsdbError::parse_err(format!("check expr '{s}' parse: {e}")))?;
    match stmts.into_iter().next() {
        Some(SqlStatement::Query(q)) => match *q.body {
            SetExpr::Select(s) => match s.projection.into_iter().next() {
                Some(sqlparser::ast::SelectItem::UnnamedExpr(e)) => Ok(e),
                _ => Err(QsdbError::invalid("check expr empty")),
            },
            other => Err(QsdbError::invalid(format!(
                "check expr body {other:?} not supported"
            ))),
        },
        _ => Err(QsdbError::invalid("check expr not a query")),
    }
}

/// 列选项解析（CREATE TABLE / ALTER ADD COLUMN 共用）。
/// allow_not_null=false 时 NOT NULL 报错（ADD COLUMN NOT NULL 需 DEFAULT，后置）。
fn parse_col_options(
    opts: &[sqlparser::ast::ColumnOption],
    allow_not_null: bool,
) -> Result<(bool, Option<String>, Option<String>)> {
    let mut nullable = true;
    let mut check = None;
    let mut comment = None;
    for opt in opts {
        match opt {
            sqlparser::ast::ColumnOption::Null => nullable = true,
            sqlparser::ast::ColumnOption::NotNull => {
                if allow_not_null {
                    nullable = false;
                } else {
                    return Err(QsdbError::unsupported(
                        "ADD COLUMN NOT NULL needs DEFAULT (lands later)",
                    ));
                }
            }
            sqlparser::ast::ColumnOption::Check(cc) => check = Some(cc.expr.to_string()),
            sqlparser::ast::ColumnOption::Comment(s) => comment = Some(s.clone()),
            sqlparser::ast::ColumnOption::Unique(_) => {
                return Err(QsdbError::unsupported(
                    "UNIQUE constraint (needs partition-level dedup against old segments) lands later",
                ));
            }
            sqlparser::ast::ColumnOption::PrimaryKey(_) => {
                return Err(QsdbError::unsupported(
                    "PRIMARY KEY constraint (implies UNIQUE, needs partition-level dedup) lands later",
                ));
            }
            other => {
                return Err(QsdbError::unsupported(format!(
                    "column option {other:?} not supported (allow: NULL / NOT NULL / CHECK / COMMENT)"
                )));
            }
        }
    }
    Ok((nullable, check, comment))
}

/// CHECK 约束 DDL 期校验：仅可引用数据列（分区列不在写入批内）、结果必须布尔。
fn validate_check_exprs(def: &TableDef) -> Result<()> {
    let has_check = def.columns.iter().any(|c| c.check.is_some());
    if !has_check {
        return Ok(());
    }
    let ctx = SchemaCtx {
        cols: def
            .data_columns()
            .iter()
            .map(|c| (c.name.clone(), c.logical_type.clone()))
            .collect(),
    };
    for c in def.columns.iter().filter(|c| c.check.is_some()) {
        let pred = bind_expr(&parse_expr_sql(c.check.as_ref().unwrap())?, &ctx)?;
        if !pred.is_boolean() {
            return Err(QsdbError::type_err(format!(
                "CHECK constraint on '{}' must be boolean",
                c.name
            )));
        }
    }
    Ok(())
}

fn translate_statement(stmt: SqlStatement, raw: &str) -> Result<ParsedStatement> {
    match stmt {
        SqlStatement::CreateTable(ct) => {
            let table = object_name_single(ct.name.clone())?;
            let mut defs = Vec::with_capacity(ct.columns.len());
            for c in &ct.columns {
                let ty = ast_type_to_logical(&c.data_type)?;
                let opts = c
                    .options
                    .iter()
                    .map(|o| o.option.clone())
                    .collect::<Vec<_>>();
                let (nullable, check, comment) = parse_col_options(&opts, true)?;
                defs.push(super::make_column(
                    &c.name.value,
                    ty,
                    nullable,
                    check,
                    comment,
                ));
            }
            // PARTITIONED BY (…) 原生解析；旧语法后备手工解析
            let part_keys = match &ct.hive_distribution {
                sqlparser::ast::HiveDistributionStyle::PARTITIONED { columns } => {
                    columns.iter().map(|c| c.name.value.clone()).collect()
                }
                _ => parse_partitioned_by(raw)?,
            };
            // WITH (k = 'v') / OPTIONS / TBLPROPERTIES → 表属性（compression 等）
            let mut properties = std::collections::HashMap::new();
            if let sqlparser::ast::CreateTableOptions::With(opts)
            | sqlparser::ast::CreateTableOptions::Options(opts)
            | sqlparser::ast::CreateTableOptions::TableProperties(opts) = &ct.table_options
            {
                for opt in opts {
                    if let sqlparser::ast::SqlOption::KeyValue { key, value } = opt {
                        if let Ok(v) = option_value_string(value) {
                            properties.insert(key.value.clone(), v);
                        }
                    }
                }
            }
            let mut def = TableDef::new(table, defs, part_keys)?;
            def.properties = properties;
            // CHECK 约束 DDL 期校验：可绑定（仅数据列）、布尔型——写路径免重复报错歧义
            validate_check_exprs(&def)?;
            let _ = ct.if_not_exists; // 建表冲突按 AlreadyExists 报错
            Ok(ParsedStatement::CreateTable { def })
        }
        SqlStatement::AlterTable(at) => {
            let table = object_name_single(at.name.clone())?;
            let mut add: Option<crate::catalog::ColumnDef> = None;
            for op in &at.operations {
                match op {
                    sqlparser::ast::AlterTableOperation::AddColumn { column_def, .. } => {
                        if add.is_some() {
                            return Err(QsdbError::invalid(
                                "one ADD COLUMN per ALTER statement (多次 ALTER 分开执行)",
                            ));
                        }
                        let ty = ast_type_to_logical(&column_def.data_type)?;
                        let opts = column_def
                            .options
                            .iter()
                            .map(|o| o.option.clone())
                            .collect::<Vec<_>>();
                        let (nullable, check, comment) = parse_col_options(&opts, false)?;
                        add = Some(super::make_column(
                            &column_def.name.value,
                            ty,
                            nullable,
                            check,
                            comment,
                        ));
                    }
                    other => {
                        return Err(QsdbError::unsupported(format!(
                            "ALTER TABLE {other:?} not supported (只支持 ADD COLUMN)"
                        )));
                    }
                }
            }
            let Some(column) = add else {
                return Err(QsdbError::invalid(
                    "ALTER TABLE ... ADD COLUMN <name> <type> expected",
                ));
            };
            Ok(ParsedStatement::AlterAddColumn { table, column })
        }
        SqlStatement::CreateView(v) => {
            if v.materialized {
                return Err(QsdbError::unsupported("MATERIALIZED VIEW"));
            }
            let vname = object_name_single(v.name.clone())?;
            Ok(ParsedStatement::CreateView {
                name: vname,
                sql: v.query.to_string(),
                or_replace: v.or_replace,
            })
        }
        SqlStatement::Drop {
            object_type,
            if_exists,
            names,
            ..
        } => {
            if matches!(object_type, sqlparser::ast::ObjectType::View) {
                let name = names
                    .first()
                    .ok_or_else(|| QsdbError::invalid("DROP VIEW needs a name"))?;
                return Ok(ParsedStatement::DropView {
                    name: object_name_single(name.clone())?,
                    if_exists,
                });
            }
            if !matches!(object_type, sqlparser::ast::ObjectType::Table) {
                return Err(QsdbError::unsupported(format!(
                    "DROP {object_type:?} not supported"
                )));
            }
            let name = names
                .first()
                .ok_or_else(|| QsdbError::invalid("DROP TABLE needs a name"))?;
            Ok(ParsedStatement::DropTable {
                name: object_name_single(name.clone())?,
                if_exists,
            })
        }
        SqlStatement::Insert(ins) => {
            // **静默丢弃闸门（DML 面）**：`ON CONFLICT` 此前被忽略 → 直接插进去
            // （实测 u 表从 1 行变 2 行）。冲突处理不实现，就必须拒绝。
            // 另有 `RETURNING` / `overwrite` / `partitioned` 等同类字段，一并拦。
            if ins.on.is_some() {
                return Err(QsdbError::unsupported(
                    "INSERT … ON CONFLICT is not supported — refusing rather than silently                      inserting a duplicate row",
                ));
            }
            if ins.returning.is_some() {
                return Err(QsdbError::unsupported(
                    "INSERT … RETURNING is not supported",
                ));
            }
            // 注意 `overwrite` **不在**本闸门内：`INSERT OVERWRITE <t> SELECT …`
            // 正是本仓支持的写法（`insert.overwrite == true` 就是它的解析结果）。
            let _ = ins.overwrite;
            if !ins.partitioned.is_none() {
                return Err(QsdbError::unsupported(
                    "INSERT … PARTITION (…) is not supported",
                ));
            }
            if ins.ignore || ins.replace_into {
                return Err(QsdbError::unsupported(
                    "INSERT IGNORE / REPLACE INTO is not supported",
                ));
            }
            let table = match &ins.table {
                sqlparser::ast::TableObject::TableName(name) => object_name_single(name.clone())?,
                other => {
                    return Err(QsdbError::unsupported(format!(
                        "INSERT target {other:?} not supported"
                    )));
                }
            };
            let column_order = if ins.columns.is_empty() {
                None
            } else {
                Some(
                    ins.columns
                        .iter()
                        .map(|c| {
                            c.0.last()
                                .map(|p| match p {
                                    sqlparser::ast::ObjectNamePart::Identifier(i) => {
                                        i.value.clone()
                                    }
                                    sqlparser::ast::ObjectNamePart::Function(_) => String::new(),
                                })
                                .unwrap_or_default()
                        })
                        .collect(),
                )
            };
            let source = extract_insert_source(ins.source)?;
            Ok(ParsedStatement::Insert {
                table,
                source,
                column_order,
                overwrite: ins.overwrite,
            })
        }
        SqlStatement::Delete(d) => {
            use sqlparser::ast::FromTable;
            let from = match &d.from {
                FromTable::WithFromKeyword(t) | FromTable::WithoutKeyword(t) => t,
            };
            let target = from
                .first()
                .map(|twj| &twj.relation)
                .ok_or_else(|| QsdbError::invalid("DELETE needs a target table"))?;
            let table = match target {
                sqlparser::ast::TableFactor::Table { name, .. } => {
                    object_name_single(name.clone())?
                }
                other => return Err(QsdbError::unsupported(format!("DELETE target {other:?}"))),
            };
            let selection = d.selection.as_ref().map(|e| e.to_string());
            Ok(ParsedStatement::Delete { table, selection })
        }
        SqlStatement::Truncate(tr) => {
            let target = tr
                .table_names
                .first()
                .ok_or_else(|| QsdbError::invalid("TRUNCATE needs a table"))?;
            Ok(ParsedStatement::Truncate {
                table: object_name_single(target.name.clone())?,
            })
        }
        SqlStatement::Query(q) => Ok(ParsedStatement::Select(q)),
        other => Err(QsdbError::unsupported(format!(
            "statement {other:?} not supported in this version (SQL-1)"
        ))),
    }
}

fn is_ident_str(s: &str) -> bool {
    !s.is_empty()
        && s.chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// CREATE USER 解析（upper 前缀已剥；原串用于密码提取保留大小写）。
fn parse_create_user(upper_rest: &str, orig: &str) -> Option<ParsedStatement> {
    // upper_rest: "NAME WITH PASSWORD 'pw' [ROLE r]"（密码在 orig 中同位置）
    let mut parts = upper_rest.splitn(2, " WITH PASSWORD ");
    let name = parts.next()?.trim();
    if !is_ident_str(name) || name.contains(' ') {
        return None;
    }
    let after_pw = parts.next()?.trim();
    // 密码：'pw' 到闭合单引号（跳过开头的 '）
    if !after_pw.starts_with('\'') {
        return None;
    }
    let close = after_pw[1..].find('\'').map(|i| i + 1)?;
    let pw_upper = &after_pw[1..close];
    let role_part = after_pw[close + 1..].trim();
    // 密码原样大小写：在原串中找同名段（upper 位置一致）
    let pw = extract_password(orig, pw_upper)?;
    let role = if role_part.is_empty() {
        crate::users::Role::Writer // 默认写角色
    } else {
        let r = role_part.strip_prefix("ROLE ")?;
        crate::users::Role::from_name(r.trim()).ok()?
    };
    Some(ParsedStatement::CreateUser {
        name: name.to_ascii_lowercase(),
        password: pw,
        role,
    })
}

/// 密码从原 SQL 提取（upper 变换保留原大小写）。
fn extract_password(orig: &str, pw_upper: &str) -> Option<String> {
    // 找 upper 中 pw_upper 的位置，取原串同位置（密码含引号内任意字符）
    let orig_upper = orig.to_ascii_uppercase();
    let idx = orig_upper.find(pw_upper)?;
    Some(orig[idx..idx + pw_upper.len()].to_string())
}

/// ALTER USER name SET PASSWORD 'pw' | SET ROLE r。
fn parse_alter_user(upper_rest: &str, orig: &str) -> Option<(String, UserAlterAction)> {
    let rest = upper_rest.trim();
    let (name, action_str) = rest.split_once(" SET ")?;
    let name = name.trim().to_ascii_lowercase();
    if !is_ident_str(&name) {
        return None;
    }
    let action_str = action_str.trim();
    if let Some(pw_part) = action_str.strip_prefix("PASSWORD ") {
        let pw_part = pw_part.trim();
        if pw_part.starts_with('\'') && pw_part.ends_with('\'') && pw_part.len() >= 2 {
            // 密码保留原大小写：从原 SQL 提取（upper 变换会丢失）
            let pw_upper = &pw_part[1..pw_part.len() - 1];
            let pw = extract_password(orig, pw_upper)?;
            return Some((name, UserAlterAction::SetPassword(pw)));
        }
        return None;
    }
    if let Some(r) = action_str.strip_prefix("ROLE ") {
        let role = crate::users::Role::from_name(r.trim()).ok()?;
        return Some((name, UserAlterAction::SetRole(role)));
    }
    None
}

/// `PARTITIONED BY (dt, region)` / `PARTITION BY (...)` 手工提取
/// （sqlparser 对该 Hive 扩展的解析随方言不定）。
fn parse_partitioned_by(raw: &str) -> Result<Vec<String>> {
    let lower = raw.to_ascii_lowercase();
    let idx = lower
        .find("partitioned by")
        .or_else(|| lower.find("partition by"));
    let Some(idx) = idx else {
        return Ok(Vec::new());
    };
    let rest = &raw[idx..];
    let open = rest
        .find('(')
        .ok_or_else(|| QsdbError::parse_err("PARTITIONED BY expects '('"))?;
    let close = rest[open..]
        .find(')')
        .map(|c| open + c)
        .ok_or_else(|| QsdbError::parse_err("PARTITIONED BY expects ')'"))?;
    let inner = &rest[open + 1..close];
    let keys = inner
        .split(',')
        .map(|s| s.trim().trim_matches('`').to_string())
        .filter(|s| !s.is_empty())
        .collect();
    Ok(keys)
}

/// DDL 属性值 → 字符串（支持字面量与单值数字/布尔）。
fn option_value_string(e: &sqlparser::ast::Expr) -> Result<String> {
    use sqlparser::ast::Expr as E;
    match e {
        E::Value(v) => match &v.value {
            sqlparser::ast::Value::SingleQuotedString(s)
            | sqlparser::ast::Value::DoubleQuotedString(s) => Ok(s.clone()),
            sqlparser::ast::Value::Number(n, _) => Ok(n.clone()),
            sqlparser::ast::Value::Boolean(b) => Ok(b.to_string()),
            other => Err(QsdbError::invalid(format!(
                "table property value must be a string/number/bool literal, got {other:?}"
            ))),
        },
        E::UnaryOp {
            op: sqlparser::ast::UnaryOperator::Minus,
            expr,
        } => Ok(format!("-{}", option_value_string(expr)?)),
        other => Err(QsdbError::invalid(format!(
            "table property value must be a literal, got {other:?}"
        ))),
    }
}

fn object_name_single(name: sqlparser::ast::ObjectName) -> Result<String> {
    let mut parts = name.0.into_iter();
    let first = parts
        .next()
        .ok_or_else(|| QsdbError::invalid("empty object name"))?;
    let part = match first {
        sqlparser::ast::ObjectNamePart::Identifier(ident) => ident.value,
        sqlparser::ast::ObjectNamePart::Function(_) => {
            return Err(QsdbError::unsupported("function object name"));
        }
    };
    if parts.next().is_some() {
        return Err(QsdbError::unsupported(
            "qualified names (db.table) not supported yet; single database in use",
        ));
    }
    Ok(part)
}

/// sqlparser DataType → LogicalType
fn ast_type_to_logical(dt: &sqlparser::ast::DataType) -> Result<LogicalType> {
    use sqlparser::ast::DataType as D;
    use sqlparser::ast::{ExactNumberInfo, TimezoneInfo};
    Ok(match dt {
        D::Boolean | D::Bool => LogicalType::Boolean,
        D::TinyInt(_) => LogicalType::Int8,
        D::SmallInt(_) => LogicalType::Int16,
        D::Int(_) | D::Integer(_) => LogicalType::Int32,
        D::BigInt(_) => LogicalType::Int64,
        D::Float(_) | D::Real | D::Float4 => LogicalType::Float32,
        D::Double(_) | D::DoublePrecision | D::Float8 => LogicalType::Float64,
        D::Decimal(info) => match info {
            ExactNumberInfo::PrecisionAndScale(p, s) => {
                LogicalType::try_decimal(*p as u8, *s as i8)?
            }
            ExactNumberInfo::Precision(p) => LogicalType::try_decimal(*p as u8, 0)?,
            ExactNumberInfo::None => LogicalType::try_decimal(38, 0)?,
        },
        D::Varchar(_) => LogicalType::Varchar { max_chars: None },
        D::Char(_) | D::Character(_) => LogicalType::Varchar { max_chars: None },
        D::Text => LogicalType::Varchar { max_chars: None },
        D::String(_) => LogicalType::Varchar { max_chars: None },
        D::Date => LogicalType::Date,
        D::Time(_, tz) => match tz {
            TimezoneInfo::None => LogicalType::Time,
            _ => return Err(QsdbError::unsupported("TIME WITH TIME ZONE")),
        },
        D::Timestamp(_, tz) => match tz {
            TimezoneInfo::None => LogicalType::Timestamp { timezone: None },
            _ => LogicalType::Timestamp {
                timezone: Some("UTC".into()),
            },
        },
        D::Binary(_) | D::Blob(_) | D::Bytea => LogicalType::Binary,
        other => {
            return Err(QsdbError::unsupported(format!(
                "type {other:?} not supported"
            )));
        }
    })
}

/// LogicalType → sqlparser DataType（CAST 目标解析用）。
fn cast_target_to_logical(dt: &sqlparser::ast::DataType) -> Result<LogicalType> {
    ast_type_to_logical(dt)
}

// ---------------------------------------------------------------------------
// INSERT 源（VALUES / SELECT）
// ---------------------------------------------------------------------------

/// INSERT 的源形态判别：`VALUES (…)` 走字面量路径；其余（SELECT / WITH … SELECT /
/// 集合操作）整体交给既有 SELECT 链路。**不在这里展开 SELECT**——解析层只分类，
/// 计划/执行层（`sql::mod`）才有 catalog 与执行器可用。
fn extract_insert_source(query: Option<Box<sqlparser::ast::Query>>) -> Result<InsertSource> {
    let Some(query) = query else {
        return Err(QsdbError::invalid(
            "INSERT requires VALUES or SELECT source",
        ));
    };
    if !matches!(query.body.as_ref(), SetExpr::Values(_)) {
        return Ok(InsertSource::Select(query));
    }
    let SetExpr::Values(values) = *query.body else {
        unreachable!("body 已由上面的 matches! 判定为 Values")
    };
    let mut rows = Vec::with_capacity(values.rows.len());
    let mut width = None;
    for row in values.rows {
        let mut lits = Vec::with_capacity(row.content.len());
        for e in row.content {
            lits.push(constant_expr(&e)?);
        }
        match width {
            None => width = Some(lits.len()),
            Some(w) if w != lits.len() => {
                return Err(QsdbError::invalid("VALUES rows have different widths"));
            }
            Some(_) => {}
        }
        rows.push(lits);
    }
    Ok(InsertSource::Values(rows))
}

/// 常量表达式 → Literal（INSERT/比较常量侧）。
pub(crate) fn constant_expr(e: &sqlparser::ast::Expr) -> Result<Literal> {
    use sqlparser::ast::Expr as E;
    match e {
        E::Value(v) => value_to_literal(v),
        E::TypedString(ts) => {
            let raw = ts.value.to_string();
            let value = raw.trim_matches('\'');
            match &ts.data_type {
                sqlparser::ast::DataType::Date => temporal::parse_date(value).map(Literal::Date),
                sqlparser::ast::DataType::Timestamp(..) => {
                    temporal::parse_timestamp_ns(value).map(Literal::Timestamp)
                }
                sqlparser::ast::DataType::Time(..) => {
                    temporal::parse_time_ns(value).map(Literal::Time)
                }
                other => Err(QsdbError::unsupported(format!(
                    "typed literal {other:?} not supported"
                ))),
            }
        }
        E::UnaryOp { op, expr } => match op {
            sqlparser::ast::UnaryOperator::Minus => {
                let inner = constant_expr(expr)?;
                negate_literal(inner)
            }
            _ => Err(QsdbError::invalid("unsupported unary in constant")),
        },
        // `CAST(常量 AS 类型)`：**在绑定期折成常量**。
        //
        // 为什么必须支持：VALUES 是**无表达式**的字面量通道，而 BINARY 列在 SQL 里
        // 没有任何"裸字面量"形态——`CAST('ab' AS BINARY)` / `X'6162'` 是唯一写法。
        // 不收 CAST 等于"BINARY 列经 SQL 写不进去"（qsdb-jdbc 实测踩到）。
        // 折常量是安全的：CAST 的另一侧也是常量，不引入求值时机问题。
        E::Cast {
            expr, data_type, ..
        } => {
            let lit = constant_expr(expr)?;
            let to = cast_target_to_logical(data_type)?;
            crate::types::cast::cast_literal(&lit, &to)
        }
        E::Nested(inner) => constant_expr(inner),
        other => Err(QsdbError::invalid(format!(
            "constant expression expected, got {other:?}"
        ))),
    }
}

fn negate_literal(l: Literal) -> Result<Literal> {
    Ok(match l {
        Literal::Int64(v) => Literal::Int64(
            v.checked_neg()
                .ok_or_else(|| QsdbError::type_err("integer literal overflow"))?,
        ),
        Literal::Int32(v) => Literal::Int32(v.checked_neg().unwrap_or(v)),
        Literal::Int16(v) => Literal::Int16(v.checked_neg().unwrap_or(v)),
        Literal::Int8(v) => Literal::Int8(v.checked_neg().unwrap_or(v)),
        Literal::Float32(v) => Literal::Float32(-v),
        Literal::Float64(v) => Literal::Float64(-v),
        Literal::Decimal { unscaled, meta } => Literal::Decimal {
            unscaled: unscaled.checked_neg().unwrap_or(unscaled),
            meta,
        },
        other => return Err(QsdbError::invalid(format!("cannot negate {other:?}"))),
    })
}

pub(crate) fn value_to_literal(v: &sqlparser::ast::Value) -> Result<Literal> {
    use sqlparser::ast::Value as V;
    match v {
        V::Number(s, _) => number_literal(s),
        V::SingleQuotedString(s) | V::DoubleQuotedString(s) | V::EscapedStringLiteral(s) => {
            Ok(Literal::Utf8(s.clone()))
        }
        // `X'6162'` = 标准 SQL 的字节串字面量（唯一能"裸写" BINARY 的形态）
        V::HexStringLiteral(s) => Ok(Literal::Binary(hex_bytes(s)?)),
        V::Boolean(b) => Ok(Literal::Boolean(*b)),
        V::Null => Ok(Literal::Null),
        other => Err(QsdbError::unsupported(format!(
            "literal {other:?} not supported"
        ))),
    }
}

/// `X'6162'` → 字节（标准 SQL 字节串；**奇数位/非十六进制字符一律报错**，
/// 不猜、不截断——写进 BINARY 列的值必须与用户写的一模一样）。
fn hex_bytes(s: &str) -> Result<Vec<u8>> {
    let t = s.trim();
    if t.len() % 2 != 0 {
        return Err(QsdbError::invalid(format!(
            "hex literal X'{t}' has an odd number of digits"
        )));
    }
    let mut out = Vec::with_capacity(t.len() / 2);
    let b = t.as_bytes();
    for i in (0..b.len()).step_by(2) {
        let hi = (b[i] as char).to_digit(16);
        let lo = (b[i + 1] as char).to_digit(16);
        match (hi, lo) {
            (Some(h), Some(l)) => out.push((h * 16 + l) as u8),
            _ => {
                return Err(QsdbError::invalid(format!(
                    "hex literal X'{t}' has a non-hex digit"
                )));
            }
        }
    }
    Ok(out)
}

fn number_literal(s: &str) -> Result<Literal> {
    let t = s.trim();
    if t.contains(['e', 'E']) {
        return Ok(Literal::Float64(
            t.parse()
                .map_err(|_| QsdbError::parse_err(format!("bad number '{t}'")))?,
        ));
    }
    if let Some((int_part, frac_part)) = t.split_once('.') {
        let int_digits = int_part.trim_start_matches(['-', '+', '0']);
        let frac_digits = frac_part.trim_end_matches('0');
        let significant = int_digits.len() + frac_digits.len();
        let scale = frac_digits.len() as i8;
        let precision = significant.max(1) as u8;
        if precision > 38 {
            return Ok(Literal::Float64(
                t.parse()
                    .map_err(|_| QsdbError::parse_err(format!("bad number '{t}'")))?,
            ));
        }
        let meta = DecimalMeta::try_new(precision.max(scale as u8), scale)?;
        let unscaled = crate::types::decimal::parse_decimal(t, meta)?;
        return Ok(Literal::Decimal { unscaled, meta });
    }
    // 整数
    match t.parse::<i64>() {
        Ok(v) => Ok(Literal::Int64(v)),
        Err(_) => {
            // 大整数 → decimal(38,0)
            let meta = DecimalMeta::try_new(38, 0)?;
            let unscaled = crate::types::decimal::parse_decimal(t, meta)?;
            Ok(Literal::Decimal { unscaled, meta })
        }
    }
}

// ---------------------------------------------------------------------------
// SELECT 翻译（binder）
// ---------------------------------------------------------------------------

/// 列上下文（大小写不敏感）。
#[derive(Debug, Clone)]
pub struct SchemaCtx {
    pub cols: Vec<(String, LogicalType)>,
}

impl SchemaCtx {
    pub fn from_table(def: &TableDef) -> Self {
        Self {
            cols: def
                .columns
                .iter()
                .map(|c| (c.name.clone(), c.logical_type.clone()))
                .collect(),
        }
    }

    pub fn index_of(&self, name: &str) -> Option<usize> {
        let lower = name.to_ascii_lowercase();
        self.cols
            .iter()
            .position(|(n, _)| n.to_ascii_lowercase() == lower)
    }

    pub fn ty_of(&self, idx: usize) -> &LogicalType {
        &self.cols[idx].1
    }
}

/// 完整 SELECT 翻译（需要 catalog 访问）。
pub type TableResolverFn<'a> = Box<dyn Fn(&str) -> Result<Arc<TableDef>> + 'a>;

/// DELETE/TRUNCATE 辅助：`WHERE` 文本 → 下推谓词（SELECT 包装走 binder）。
pub fn predicate_from_where(
    table: &str,
    where_sql: &str,
    resolve: &dyn Fn(&str) -> Result<Arc<TableDef>>,
) -> Result<Predicate> {
    let wrapped = format!("SELECT 1 FROM {table} WHERE {where_sql}");
    let ast = Parser::parse_sql(&GenericDialect {}, &wrapped)
        .map_err(|e| QsdbError::parse_err(format!("DELETE WHERE parse: {e}")))?;
    let q = ast
        .into_iter()
        .next()
        .ok_or_else(|| QsdbError::invalid("empty DELETE WHERE"))?;
    let sqlparser::ast::Statement::Query(q) = q else {
        return Err(QsdbError::invalid("internal DELETE parse"));
    };
    let select = match *q.body {
        sqlparser::ast::SetExpr::Select(s) => s,
        _ => return Err(QsdbError::invalid("internal DELETE parse")),
    };
    let Some(w) = select.selection else {
        return Err(QsdbError::invalid("DELETE requires WHERE"));
    };
    let def = resolve(table)?;
    let ctx = SchemaCtx::from_table(&def);
    let bound = bind_expr(&w, &ctx)?;
    if !bound.is_boolean() {
        return Err(QsdbError::type_err("DELETE WHERE must be boolean"));
    }
    let (pushdown, _residual) = split_pushdown(&bound);
    pushdown.ok_or_else(|| {
        QsdbError::invalid("DELETE WHERE must reference partition columns for pruning")
    })
}

static CTE_ID: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
fn next_cte_id() -> u32 {
    CTE_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

/// 计划输出列（名字 + 类型）——CTE 内联绑定的列序来源。
pub fn plan_output_columns(plan: &LogicalPlan) -> Result<Vec<(String, LogicalType)>> {
    Ok(match plan {
        LogicalPlan::Scan { def, .. }
        | LogicalPlan::ExternalScan { def, .. }
        | LogicalPlan::CsvScan { def, .. } => def
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
        LogicalPlan::OneRow => Vec::new(),
        LogicalPlan::SetOp { left, .. } => plan_output_columns(left)?,
        LogicalPlan::Aggregate { group, aggs, .. } => {
            let mut cols: Vec<(String, LogicalType)> =
                group.iter().map(|g| (g.display(), g.data_type())).collect();
            for a in aggs {
                cols.push((a.out_name.clone(), a.out_ty.clone()));
            }
            cols
        }
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Distinct { input } => plan_output_columns(input)?,
        // join 输出 = 左段 ++ 右段（与 `translate_select_inner` 里 ctx 的注册顺序同构）。
        // 这里给的是**非限定**列名：本函数的读者（CTE/派生表/`UNION` 两侧/`l_full` 宽度）
        // 要的都是"列"，限定名由 join 层的 ctx 另行提供。
        // 此前这里直接报错，于是**左侧是 join 的 join**（三表以上）只能走叉积分支——
        // 那正是 `complex_three_way` 113× 的另一半原因，两个 bug 恰好互相掩护。
        LogicalPlan::Join { left, right, .. }
        | LogicalPlan::CrossJoinFilter { left, right, .. } => {
            let mut cols = plan_output_columns(left)?;
            cols.extend(plan_output_columns(right)?);
            cols
        }
        LogicalPlan::Window {
            input, window_fns, ..
        } => {
            let mut cols = plan_output_columns(input)?;
            for f in window_fns {
                cols.push((f.output_name(), f.output_type()));
            }
            cols
        }
    })
}

pub struct TableResolver<'a> {
    pub resolve: TableResolverFn<'a>,
    /// 视图（name → SQL；SELECT 期重解析展开）
    pub views: std::collections::HashMap<String, String>,
}

/// CTE 物化定义（执行层按 id 填充批数据）。
#[derive(Debug, Clone)]
pub struct CteDef {
    pub cte_id: u32,
    pub columns: Vec<(String, LogicalType)>,
    pub plan: LogicalPlan,
}

/// 可见 CTE 表：本级已构建项 + 祖先级传递（链式引用）。
/// (cte_id, 输出列)——完整计划在 CteDef 中。
type VisibleCtes = std::collections::HashMap<String, (u32, Vec<(String, LogicalType)>)>;

pub fn translate_select_full(
    q: sqlparser::ast::Query,
    resolver: &TableResolver,
) -> Result<LogicalPlan> {
    translate_select_full_with_ctes(q, resolver).map(|(p, _)| p)
}

pub fn translate_select_full_with_ctes(
    q: sqlparser::ast::Query,
    resolver: &TableResolver,
) -> Result<(LogicalPlan, Vec<CteDef>)> {
    let ancestors: VisibleCtes = std::collections::HashMap::new();
    translate_select_inner(q, resolver, &ancestors)
}

/// 表源解析：CTE 优先（本级 → 祖先），退回实表 Scan。
fn resolve_table_source(
    name: &str,
    visible: &VisibleCtes,
    resolver: &TableResolver,
) -> Result<(LogicalPlan, SchemaCtx)> {
    if let Some((id, cols)) = visible.get(name) {
        return Ok((
            LogicalPlan::Values {
                columns: cols.clone(),
                cte_id: *id,
            },
            SchemaCtx { cols: cols.clone() },
        ));
    }
    // 视图：重解析展开为子计划（递归深度护栏防循环引用）
    if let Some(vsql) = resolver.views.get(name) {
        return resolve_view(vsql, resolver);
    }
    let def = (resolver.resolve)(name)?;
    Ok((
        LogicalPlan::Scan {
            def: Arc::clone(&def),
            predicate: None,
            read: None,
        },
        SchemaCtx::from_table(&def),
    ))
}

/// 视图展开：SQL 重解析 → 单条 SELECT → 子计划（祖先 CTE 透传）。
/// 深度护栏：视图链过深/循环引用报错。
fn resolve_view(vsql: &str, resolver: &TableResolver) -> Result<(LogicalPlan, SchemaCtx)> {
    use std::cell::Cell;
    thread_local! {
        static VIEW_DEPTH: Cell<u32> = const { Cell::new(0) };
    }
    if VIEW_DEPTH.with(|d| d.get()) >= 8 {
        return Err(QsdbError::invalid(
            "view expansion too deep (circular view reference?)",
        ));
    }
    let stmts = crate::sql::translate::parse_statements(vsql)?;
    let q = match stmts.into_iter().next() {
        Some(ParsedStatement::Select(q)) => q,
        _ => {
            return Err(QsdbError::invalid(
                "view definition must be a single SELECT statement",
            ));
        }
    };
    VIEW_DEPTH.with(|d| d.set(d.get() + 1));
    let r = translate_select_inner(*q, resolver, &VisibleCtes::default());
    VIEW_DEPTH.with(|d| d.set(d.get() - 1));
    let (plan, cte_defs) = r?;
    if !cte_defs.is_empty() {
        return Err(QsdbError::invalid(
            "CTE inside view not supported (inline the subquery)",
        ));
    }
    let cols = plan_output_columns(&plan)?;
    Ok((plan, SchemaCtx { cols }))
}

/// 表函数（FROM 子句）：`read_parquet('<路径/glob>')` / `read_csv('<路径/glob>')`。
/// 返回（计划, 列上下文）；文件列表绑定期展开。
///
/// 两个函数刻意共用一个参数解析（都是「恰好一个字符串字面量」）与一个
/// "建表定义" 骨架：差别只在扫描器（footer vs 逐行解析）与 schema 来源
/// （文件元数据 vs 表头+类型推断）——**不做成 table function 注册表**，
/// 因为差异点不可配置化（参数形态、schema 来源、执行器全不同）。
fn resolve_table_function(
    name: &str,
    args: &[sqlparser::ast::FunctionArg],
) -> Result<(LogicalPlan, SchemaCtx)> {
    let path = table_fn_path_arg(name, args)?;
    match name.to_ascii_lowercase().as_str() {
        "read_parquet" => resolve_read_parquet(&path),
        "read_csv" => resolve_read_csv(&path),
        _ => Err(QsdbError::unsupported(format!(
            "table function '{name}' not supported (read_parquet / read_csv only)"
        ))),
    }
}

/// 表函数唯一支持的参数形态：一个字符串字面量路径/glob。
fn table_fn_path_arg(name: &str, args: &[sqlparser::ast::FunctionArg]) -> Result<String> {
    use sqlparser::ast::{Expr as E, FunctionArg, FunctionArgExpr};
    match args {
        [FunctionArg::Unnamed(FunctionArgExpr::Expr(E::Value(v)))] => match &v.value {
            sqlparser::ast::Value::SingleQuotedString(s)
            | sqlparser::ast::Value::DoubleQuotedString(s) => Ok(s.clone()),
            other => Err(QsdbError::invalid(format!(
                "{name} path must be a string literal, got {other:?}"
            ))),
        },
        other => Err(QsdbError::invalid(format!(
            "{name} takes exactly one string literal, got {other:?} args"
        ))),
    }
}

/// `read_parquet`：schema 取首文件 footer（parquet 自带列名/类型）。
fn resolve_read_parquet(path: &str) -> Result<(LogicalPlan, SchemaCtx)> {
    let files = crate::scan::resolve_file_locs(path, "read_parquet")?;
    let Some(first) = files.first() else {
        return Err(QsdbError::file_not_found(format!("read_parquet: {path}")));
    };
    let schema = crate::scan::file_arrow_schema(first)?;
    // F12：hive 目录语义——路径含 k=v 段 → 目录名组装分区列常量
    let hive = crate::scan::extract_hive_partitions(&files);
    let mut columns = Vec::with_capacity(schema.fields().len());
    for f in schema.fields() {
        let logical = LogicalType::try_from_arrow(f.data_type()).map_err(|e| {
            QsdbError::unsupported(format!(
                "read_parquet column '{}' type {:#?} not supported: {e}",
                f.name(),
                f.data_type()
            ))
        })?;
        columns.push(crate::catalog::ColumnDef {
            name: f.name().clone(),
            logical_type: logical,
            nullable: true,
            check: None,
            default: None,
            comment: None,
        });
    }
    // F12：hive 分区列追加为 nullable 列 + partition_keys（类型由首个值推断）
    let mut all_columns = columns;
    let partition_keys: Vec<String> = if let Some(hive) = &hive {
        for (k, vals) in hive {
            if let Some(lit) = vals.first() {
                all_columns.push(crate::catalog::ColumnDef {
                    name: k.clone(),
                    logical_type: lit
                        .logical_type()
                        .unwrap_or(LogicalType::Varchar { max_chars: None }),
                    nullable: true,
                    check: None,
                    default: None,
                    comment: None,
                });
            }
        }
        hive.keys().cloned().collect()
    } else {
        Vec::new()
    };
    let def = Arc::new(TableDef {
        version: 1,
        name: "read_parquet".into(),
        columns: all_columns,
        partition_keys,
        created_at: 0,
        properties: std::collections::HashMap::new(),
    });
    let cols: Vec<(String, LogicalType)> = def
        .columns
        .iter()
        .map(|c| (c.name.clone(), c.logical_type.clone()))
        .collect();
    Ok((
        LogicalPlan::ExternalScan {
            def,
            files: Arc::new(files),
            predicate: None,
        },
        SchemaCtx { cols },
    ))
}

/// `read_csv`：schema = 表头 + 前 N 行类型推断（推断一次，全批稳定）。
/// 列裁剪复用 exec_builder 的通用投影下推；谓词不做扫描级下推（见 CsvScan 注释）。
fn resolve_read_csv(path: &str) -> Result<(LogicalPlan, SchemaCtx)> {
    let (files, def) = crate::csv::resolve_csv_table(path)?;
    let cols: Vec<(String, LogicalType)> = def
        .columns
        .iter()
        .map(|c| (c.name.clone(), c.logical_type.clone()))
        .collect();
    Ok((
        LogicalPlan::CsvScan {
            def,
            files: Arc::new(files),
        },
        SchemaCtx { cols },
    ))
}

// ---------------------------------------------------------------------------
// EXISTS / IN 子查询 → semi/anti-join（v1：WHERE 顶层 AND 合取项级提取）
// ---------------------------------------------------------------------------

/// AST 顶层 AND 合取拆分（借用视图）。
fn split_conjuncts(e: &sqlparser::ast::Expr) -> Vec<&sqlparser::ast::Expr> {
    use sqlparser::ast::{BinaryOperator, Expr};
    match e {
        Expr::BinaryOp {
            op: BinaryOperator::And,
            left,
            right,
        } => {
            let mut v = split_conjuncts(left);
            v.extend(split_conjuncts(right));
            v
        }
        other => vec![other],
    }
}

/// 合取重组（空 → None）。
fn and_conjuncts(mut parts: Vec<sqlparser::ast::Expr>) -> Option<sqlparser::ast::Expr> {
    use sqlparser::ast::{BinaryOperator, Expr};
    let mut acc = parts.pop()?;
    while let Some(p) = parts.pop() {
        acc = Expr::BinaryOp {
            left: Box::new(p),
            right: Box::new(acc),
            op: BinaryOperator::And,
        };
    }
    Some(acc)
}

/// WHERE 合取分类：EXISTS / IN 子查询项 → semi/anti-join 链式叠加到计划；
/// 其余（含 NOT(EXISTS/IN) 显式括号形态）按原样返回走行过滤。
#[allow(clippy::too_many_arguments)]
fn attach_semi_subqueries(
    plan: LogicalPlan,
    ctx: &SchemaCtx,
    resolver: &TableResolver,
    ancestors: &VisibleCtes,
    cte_defs: &mut Vec<CteDef>,
    conjuncts: Vec<&sqlparser::ast::Expr>,
) -> Result<(LogicalPlan, Vec<sqlparser::ast::Expr>)> {
    use sqlparser::ast::Expr;
    let mut plan = plan;
    let mut regular: Vec<Expr> = Vec::new();
    for c in conjuncts {
        match c {
            Expr::Exists { negated, subquery } => {
                plan = build_exists_semi_join(
                    plan,
                    ctx,
                    resolver,
                    ancestors,
                    cte_defs,
                    subquery.as_ref(),
                    *negated,
                )?;
            }
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                plan = build_in_semi_join(
                    plan,
                    ctx,
                    resolver,
                    ancestors,
                    cte_defs,
                    expr,
                    subquery.as_ref(),
                    *negated,
                )?;
            }
            // 显式 NOT(...) 括号形态（0.63：UnaryOp{op: Not}）
            Expr::UnaryOp {
                op: sqlparser::ast::UnaryOperator::Not,
                expr: inner,
            } => match inner.as_ref() {
                Expr::Exists { negated, subquery } => {
                    plan = build_exists_semi_join(
                        plan,
                        ctx,
                        resolver,
                        ancestors,
                        cte_defs,
                        subquery.as_ref(),
                        !*negated,
                    )?;
                }
                Expr::InSubquery {
                    expr,
                    subquery,
                    negated,
                } => {
                    plan = build_in_semi_join(
                        plan,
                        ctx,
                        resolver,
                        ancestors,
                        cte_defs,
                        expr,
                        subquery.as_ref(),
                        !*negated,
                    )?;
                }
                _ => regular.push(sqlparser::ast::Expr::UnaryOp {
                    op: sqlparser::ast::UnaryOperator::Not,
                    expr: Box::new((**inner).clone()),
                }),
            },
            _ => regular.push(c.clone()),
        }
    }
    Ok((plan, regular))
}

/// 子查询 FROM 的表名/别名集合（限定名前缀归属判定用）。
fn subquery_table_names(q: &sqlparser::ast::Query) -> Vec<String> {
    use sqlparser::ast::{SetExpr, TableFactor};
    let mut names = Vec::new();
    let SetExpr::Select(s) = &*q.body else {
        return names;
    };
    fn push_factor(f: &TableFactor, names: &mut Vec<String>) {
        match f {
            TableFactor::Table { name, alias, .. } => {
                if let Some(a) = alias {
                    names.push(a.name.value.to_ascii_lowercase());
                } else if let Some(sqlparser::ast::ObjectNamePart::Identifier(last)) = name.0.last()
                {
                    names.push(last.value.to_ascii_lowercase());
                }
            }
            TableFactor::Derived { alias: Some(a), .. } => {
                names.push(a.name.value.to_ascii_lowercase())
            }
            _ => {}
        }
    }
    for twj in &s.from {
        push_factor(&twj.relation, &mut names);
        for j in &twj.joins {
            push_factor(&j.relation, &mut names);
        }
    }
    names
}

/// 合取项某一侧的归属。
enum Side {
    Inner(PhysExpr),
    Outer(PhysExpr),
}

/// 侧归属：限定名前缀优先（限定符 ∈ 子表 → 子侧；否则先外层后子侧裸名）。
fn classify_side(
    ast: &sqlparser::ast::Expr,
    sub_ctx: &SchemaCtx,
    outer_ctx: &SchemaCtx,
    sub_tables: &[String],
) -> Result<Side> {
    use sqlparser::ast::Expr as A;
    match ast {
        A::CompoundIdentifier(parts) if parts.len() == 2 => {
            let q = parts[0].value.to_ascii_lowercase();
            if sub_tables.contains(&q) {
                Ok(Side::Inner(bind_expr(ast, sub_ctx)?))
            } else {
                match bind_expr(ast, outer_ctx) {
                    Ok(e) => Ok(Side::Outer(e)),
                    // 限定符既非子表也解析不到外层 → 交子侧报真实错误
                    Err(_) => Ok(Side::Inner(bind_expr(ast, sub_ctx)?)),
                }
            }
        }
        A::Identifier(id) => {
            if sub_ctx.index_of(&id.value).is_some() {
                Ok(Side::Inner(bind_expr(ast, sub_ctx)?))
            } else {
                Ok(Side::Outer(bind_expr(ast, outer_ctx)?))
            }
        }
        // 表达式形态：整体先子侧；失败且外层可解析 → 外侧（要求仅外层列）
        other => match bind_expr(other, sub_ctx) {
            Ok(e) => Ok(Side::Inner(e)),
            Err(_) => Ok(Side::Outer(bind_expr(other, outer_ctx)?)),
        },
    }
}

/// 关联 EXISTS → Semi/Anti join。子查询 WHERE 的合取分类：
/// 本地谓词（仅引用子查询列）留子查询内作 Filter；`外列 = 内列` 等值
/// 合取拆为连接键（v1：键必须为简单列、类型一致）；非等值关联不支持。
#[allow(clippy::too_many_arguments)]
fn build_exists_semi_join(
    plan: LogicalPlan,
    ctx: &SchemaCtx,
    resolver: &TableResolver,
    ancestors: &VisibleCtes,
    cte_defs: &mut Vec<CteDef>,
    subquery: &sqlparser::ast::Query,
    negated: bool,
) -> Result<LogicalPlan> {
    use sqlparser::ast::{BinaryOperator, Expr};
    // ① 无 WHERE + 全列投影版本先建：EXISTS 的输出无关紧要，但关联列
    // 不能被 SELECT 1 投影裁掉——改 Wildcard 保留 FROM 全列作 SchemaCtx
    let mut stripped = subquery.clone();
    let mut selection = None;
    if let sqlparser::ast::SetExpr::Select(s) = &mut *stripped.body {
        selection = s.selection.take();
        s.projection = vec![sqlparser::ast::SelectItem::Wildcard(Default::default())];
    }
    let (sub_plan, extra) = translate_select_inner(stripped, resolver, ancestors)?;
    cte_defs.extend(extra);
    let sub_cols = plan_output_columns(&sub_plan)?;
    let sub_ctx = SchemaCtx { cols: sub_cols };
    let left_width = ctx.cols.len();

    // ② WHERE 合取分类。归属判定按**限定名前缀**：限定符 ∈ 子查询 FROM
    // 表名/别名 → 子侧；否则先试外层解析。裸名仅在单侧可见时才归该侧
    // （防「裸名回退静默解析到子查询自身列」——关联引用要求写限定名）
    let sub_tables = subquery_table_names(subquery);
    let mut local: Vec<PhysExpr> = Vec::new();
    let mut left_keys: Vec<PhysExpr> = Vec::new();
    let mut right_keys: Vec<PhysExpr> = Vec::new();
    if let Some(sel) = &selection {
        for conj in split_conjuncts(sel) {
            let Expr::BinaryOp {
                op: BinaryOperator::Eq,
                left,
                right,
            } = conj
            else {
                // 非 Eq 合取：须整体为本地（子侧可解析）
                let b = bind_expr(conj, &sub_ctx).map_err(|_| {
                    QsdbError::unsupported(format!(
                        "correlated EXISTS: only 'inner_col = outer_col' predicates supported (near {conj})"
                    ))
                })?;
                if !b.is_boolean() {
                    return Err(QsdbError::type_err(
                        "EXISTS subquery predicate must be boolean",
                    ));
                }
                local.push(b);
                continue;
            };
            match (
                classify_side(left, &sub_ctx, ctx, &sub_tables)?,
                classify_side(right, &sub_ctx, ctx, &sub_tables)?,
            ) {
                (Side::Inner(_), Side::Inner(_)) => {
                    let b = bind_expr(conj, &sub_ctx)?;
                    if !b.is_boolean() {
                        return Err(QsdbError::type_err(
                            "EXISTS subquery predicate must be boolean",
                        ));
                    }
                    local.push(b);
                }
                (Side::Inner(i), Side::Outer(o)) | (Side::Outer(o), Side::Inner(i)) => {
                    let (PhysExpr::Column { .. }, PhysExpr::Column { .. }) = (&o, &i) else {
                        return Err(QsdbError::unsupported(
                            "correlated EXISTS: join keys must be simple columns (v1)",
                        ));
                    };
                    if o.data_type() != i.data_type() {
                        return Err(QsdbError::unsupported(format!(
                            "EXISTS join key type mismatch: {} vs {} (cast lands later)",
                            o.data_type(),
                            i.data_type()
                        )));
                    }
                    let _ = left_width;
                    left_keys.push(o);
                    right_keys.push(i);
                }
                (Side::Outer(_), Side::Outer(_)) => {
                    return Err(QsdbError::unsupported(
                        "correlated EXISTS: predicate references only outer columns",
                    ));
                }
            }
        }
    }
    if left_keys.is_empty() {
        return Err(QsdbError::unsupported(
            "uncorrelated EXISTS: v1 requires correlation (inner_col = outer_col); rewrite with IN or JOIN",
        ));
    }
    // ③ 本地谓词 → 子计划内 Filter
    let mut sub_plan = sub_plan;
    if let Some(pred) = local
        .into_iter()
        .reduce(|a, b| PhysExpr::And(Box::new(a), Box::new(b)))
    {
        sub_plan = LogicalPlan::Filter {
            input: Box::new(sub_plan),
            predicate: pred,
        };
    }
    let join_type = if negated {
        crate::exec::join::JoinType::Anti
    } else {
        crate::exec::join::JoinType::Semi
    };
    Ok(LogicalPlan::Join {
        left: Box::new(plan),
        right: Box::new(sub_plan),
        left_keys,
        right_keys,
        join_type,
        needed: None,
    })
}

/// IN (SELECT 单列) → Semi；NOT IN → AntiIn（三值 NULL 语义在 exec）。
/// 子查询必须非关联（其谓词仅引用自身列）；左操作数 v1 须为简单列。
#[allow(clippy::too_many_arguments)]
fn build_in_semi_join(
    plan: LogicalPlan,
    ctx: &SchemaCtx,
    resolver: &TableResolver,
    ancestors: &VisibleCtes,
    cte_defs: &mut Vec<CteDef>,
    lhs: &sqlparser::ast::Expr,
    subquery: &sqlparser::ast::Query,
    negated: bool,
) -> Result<LogicalPlan> {
    let (sub_plan, extra) = translate_select_inner(subquery.clone(), resolver, ancestors)?;
    cte_defs.extend(extra);
    let sub_cols = plan_output_columns(&sub_plan)?;
    if sub_cols.len() != 1 {
        return Err(QsdbError::invalid(format!(
            "IN subquery must select exactly one column (got {})",
            sub_cols.len()
        )));
    }
    let outer_key = bind_expr(lhs, ctx)?;
    if !matches!(outer_key, PhysExpr::Column { .. }) {
        return Err(QsdbError::unsupported(
            "IN lhs must be a simple column (v1)",
        ));
    }
    let (name0, ty0) = &sub_cols[0];
    if outer_key.data_type() != *ty0 {
        return Err(QsdbError::unsupported(format!(
            "IN key type mismatch: {} vs {ty0} (cast lands later)",
            outer_key.data_type()
        )));
    }
    // 右键 = 右子计划局部 idx（0 位 = 子查询首列；绑定期约定）
    let right_key = PhysExpr::Column {
        idx: 0,
        name: name0.clone(),
        ty: ty0.clone(),
    };
    let join_type = if negated {
        crate::exec::join::JoinType::AntiIn
    } else {
        crate::exec::join::JoinType::Semi
    };
    Ok(LogicalPlan::Join {
        left: Box::new(plan),
        right: Box::new(sub_plan),
        left_keys: vec![outer_key],
        right_keys: vec![right_key],
        join_type,
        needed: None,
    })
}

/// 提取表达式内的标量子查询并置换为合成 Identifier（__scalar_sub_{n}）。
/// 返回（置换后表达式, (合成列名, 子查询) 列表），出现序 = 收集序。
fn extract_scalar_subqueries(
    e: &sqlparser::ast::Expr,
    counter: &mut usize,
) -> (sqlparser::ast::Expr, Vec<(String, sqlparser::ast::Query)>) {
    fn go(
        e: &sqlparser::ast::Expr,
        counter: &mut usize,
        out: &mut Vec<(String, sqlparser::ast::Query)>,
    ) -> sqlparser::ast::Expr {
        use sqlparser::ast::Expr as E;
        match e {
            E::Subquery(query) => {
                let name = format!("__scalar_sub_{counter}");
                *counter += 1;
                out.push((name.clone(), (**query).clone()));
                E::Identifier(sqlparser::ast::Ident::new(name))
            }
            E::BinaryOp { left, op, right } => E::BinaryOp {
                left: Box::new(go(left, counter, out)),
                op: op.clone(),
                right: Box::new(go(right, counter, out)),
            },
            E::Nested(inner) => E::Nested(Box::new(go(inner, counter, out))),
            E::UnaryOp { expr, .. } => {
                let inner = go(expr, counter, out);
                let mut u = e.clone();
                if let E::UnaryOp { expr: slot, .. } = &mut u {
                    **slot = inner;
                }
                u
            }
            E::Cast { expr, .. } => {
                let inner = go(expr, counter, out);
                let mut c = e.clone();
                if let E::Cast { expr: slot, .. } = &mut c {
                    **slot = inner;
                }
                c
            }
            E::Function(f) if f.over.is_none() => {
                let mut nf = f.clone();
                let new_args: Vec<sqlparser::ast::FunctionArg> = fn_args(f)
                    .iter()
                    .map(|a| match a {
                        sqlparser::ast::FunctionArg::Unnamed(
                            sqlparser::ast::FunctionArgExpr::Expr(inner),
                        ) => sqlparser::ast::FunctionArg::Unnamed(
                            sqlparser::ast::FunctionArgExpr::Expr(go(inner, counter, out)),
                        ),
                        other => other.clone(),
                    })
                    .collect();
                if let sqlparser::ast::FunctionArguments::List(l) = &mut nf.args {
                    l.args = new_args;
                }
                E::Function(nf)
            }
            E::IsNull(inner) => E::IsNull(Box::new(go(inner, counter, out))),
            E::IsNotNull(inner) => E::IsNotNull(Box::new(go(inner, counter, out))),
            other => other.clone(),
        }
    }
    let mut out = Vec::new();
    let replaced = go(e, counter, &mut out);
    (replaced, out)
}

/// 标量子查询接线：单列子计划 → Limit 1 → 常真谓词 cross join 叠到主计划上，
/// 合成列注册进 ctx。子级 CTE defs 并入（先于使用）。
fn attach_scalar_subqueries(
    plan: LogicalPlan,
    ctx: &mut SchemaCtx,
    cte_defs: &mut Vec<CteDef>,
    queries: Vec<(String, sqlparser::ast::Query)>,
    visible: &VisibleCtes,
    resolver: &TableResolver,
) -> Result<LogicalPlan> {
    let mut plan = plan;
    for (name, q) in queries {
        let (sub, child_defs) = translate_select_inner(q, resolver, visible)?;
        cte_defs.extend(child_defs);
        let cols = plan_output_columns(&sub)?;
        let ty = match cols.as_slice() {
            [(_, t)] => t.clone(),
            _ => {
                return Err(QsdbError::invalid(
                    "scalar subquery must return exactly one column",
                ));
            }
        };
        plan = LogicalPlan::CrossJoinFilter {
            left: Box::new(plan),
            right: Box::new(LogicalPlan::Limit {
                input: Box::new(sub),
                n: Some(1),
                offset: 0,
            }),
            predicate: PhysExpr::Lit(Literal::Boolean(true)),
        };
        ctx.cols.push((name, ty));
    }
    Ok(plan)
}

/// 过滤未被引用的 CTE（dead CTE 不物化）：
/// 从主计划收集 Values 引用，沿 CteDef.plan 传递闭包。
fn filter_reachable_ctes(defs: Vec<CteDef>, main: &LogicalPlan) -> Vec<CteDef> {
    fn collect_refs(p: &LogicalPlan, out: &mut Vec<u32>) {
        match p {
            LogicalPlan::Values { cte_id, .. } => out.push(*cte_id),
            LogicalPlan::Filter { input, .. }
            | LogicalPlan::Project { input, .. }
            | LogicalPlan::Aggregate { input, .. }
            | LogicalPlan::Sort { input, .. }
            | LogicalPlan::Limit { input, .. }
            | LogicalPlan::Distinct { input }
            | LogicalPlan::Window { input, .. } => collect_refs(input, out),
            LogicalPlan::Join { left, right, .. }
            | LogicalPlan::CrossJoinFilter { left, right, .. }
            | LogicalPlan::SetOp { left, right, .. } => {
                collect_refs(left, out);
                collect_refs(right, out);
            }
            LogicalPlan::Scan { .. }
            | LogicalPlan::ExternalScan { .. }
            | LogicalPlan::CsvScan { .. }
            | LogicalPlan::OneRow => {}
        }
    }
    let mut keep = std::collections::HashSet::new();
    let mut queue = Vec::new();
    collect_refs(main, &mut queue);
    while let Some(id) = queue.pop() {
        if keep.insert(id) {
            if let Some(d) = defs.iter().find(|d| d.cte_id == id) {
                collect_refs(&d.plan, &mut queue);
            }
        }
    }
    defs.into_iter()
        .filter(|d| keep.contains(&d.cte_id))
        .collect()
}

/// WITH 子句绑定（普通 SELECT 与集合操作两路共用）：
/// CTE 内联展开为子计划，祖先可见（后定义可引用先定义，链式）。
fn bind_with_ctes(
    with: Option<&sqlparser::ast::With>,
    resolver: &TableResolver,
    ancestors: &VisibleCtes,
) -> Result<(VisibleCtes, Vec<CteDef>)> {
    let mut visible: VisibleCtes = ancestors.clone();
    let mut cte_defs: Vec<CteDef> = Vec::new();
    if let Some(with) = with {
        if with.recursive {
            return Err(QsdbError::unsupported("recursive CTE"));
        }
        for cte in &with.cte_tables {
            let name = cte.alias.name.value.clone();
            if visible.contains_key(&name) || (resolver.resolve)(&name).is_ok() {
                return Err(QsdbError::unsupported("recursive or duplicate CTE"));
            }
            let (sub, mut child_defs) =
                translate_select_inner(*cte.query.clone(), resolver, &visible)?;
            let cols = plan_output_columns(&sub)?;
            let id = next_cte_id();
            // 子级 WITH 的 CTE 先入列（执行层按序物化，被引用者在前）
            cte_defs.append(&mut child_defs);
            cte_defs.push(CteDef {
                cte_id: id,
                columns: cols.clone(),
                plan: sub,
            });
            visible.insert(name, (id, cols));
        }
    }
    Ok((visible, cte_defs))
}

/// 集合操作绑定（M7-B4）：UNION [ALL] / INTERSECT / EXCEPT。
/// 两侧递归翻译（外层 WITH 对两侧可见）；列数相等、类型按公共超类型
/// 双侧 cast 归一（列名取左）；INTERSECT/EXCEPT 的 ALL 多重集形态拒绝。
fn translate_set_operation(
    op: sqlparser::ast::SetOperator,
    left: Box<sqlparser::ast::SetExpr>,
    right: Box<sqlparser::ast::SetExpr>,
    quantifier: Option<sqlparser::ast::SetQuantifier>,
    q: &sqlparser::ast::Query,
    resolver: &TableResolver,
    ancestors: &VisibleCtes,
) -> Result<(LogicalPlan, Vec<CteDef>)> {
    use crate::exec::SetOpKind;
    let (visible, mut cte_defs) = bind_with_ctes(q.with.as_ref(), resolver, ancestors)?;
    let body_query = |body: Box<sqlparser::ast::SetExpr>| sqlparser::ast::Query {
        with: None,
        body,
        order_by: None,
        limit_clause: None,
        fetch: None,
        locks: vec![],
        for_clause: None,
        settings: None,
        format_clause: None,
        pipe_operators: vec![],
    };
    let (mut lp, mut dl) = translate_select_inner(body_query(left), resolver, &visible)?;
    let (mut rp, mut dr) = translate_select_inner(body_query(right), resolver, &visible)?;
    cte_defs.append(&mut dl);
    cte_defs.append(&mut dr);

    // schema 归一：列数相等；类型公共超类型双侧 cast（列名取左）
    let lc = plan_output_columns(&lp)?;
    let rc = plan_output_columns(&rp)?;
    if lc.len() != rc.len() {
        return Err(QsdbError::invalid(format!(
            "set operation sides have different column counts ({} vs {})",
            lc.len(),
            rc.len()
        )));
    }
    let common: Vec<LogicalType> = lc
        .iter()
        .zip(&rc)
        .map(|((_, a), (_, b))| {
            if a == b {
                a.clone()
            } else {
                common_supertype(a, b)
            }
        })
        .collect();
    let needs_cast =
        |cols: &[(String, LogicalType)]| cols.iter().zip(&common).any(|((_, t), c)| t != c);
    if needs_cast(&rc) {
        rp = cast_setop_side(rp, &common, &lc);
    }
    if needs_cast(&lc) {
        lp = cast_setop_side(lp, &common, &lc);
    }

    let is_all = matches!(quantifier, Some(sqlparser::ast::SetQuantifier::All));
    let (kind, all) = match op {
        sqlparser::ast::SetOperator::Union => (SetOpKind::Union, is_all),
        sqlparser::ast::SetOperator::Intersect => {
            if is_all {
                return Err(QsdbError::unsupported(
                    "INTERSECT ALL (multiset) not supported",
                ));
            }
            (SetOpKind::Intersect, false)
        }
        sqlparser::ast::SetOperator::Except => {
            if is_all {
                return Err(QsdbError::unsupported(
                    "EXCEPT ALL (multiset) not supported",
                ));
            }
            (SetOpKind::Except, false)
        }
        other => {
            return Err(QsdbError::unsupported(format!(
                "set operation {other:?} not supported"
            )));
        }
    };
    let mut plan = LogicalPlan::SetOp {
        left: Box::new(lp),
        right: Box::new(rp),
        op: kind,
        all,
    };

    // 外层 ORDER BY：输出列名 / 序号（1-based）
    if let Some(order_by) = &q.order_by {
        let exprs = match &order_by.kind {
            sqlparser::ast::OrderByKind::Expressions(es) => es.clone(),
            _ => return Err(QsdbError::unsupported("ORDER BY ALL")),
        };
        let mut keys = Vec::with_capacity(exprs.len());
        for ob in &exprs {
            let desc = matches!(&ob.options.sort, Some(sqlparser::ast::OrderBySort::Desc));
            let nulls_first = ob.options.nulls_first;
            let idx = match &ob.expr {
                sqlparser::ast::Expr::Identifier(id) => lc
                    .iter()
                    .position(|(n, _)| n.eq_ignore_ascii_case(&id.value)),
                sqlparser::ast::Expr::Value(v) => match &v.value {
                    sqlparser::ast::Value::Number(n, _) => n
                        .parse::<usize>()
                        .ok()
                        .and_then(|i| i.checked_sub(1))
                        .filter(|i| *i < lc.len()),
                    _ => None,
                },
                _ => None,
            };
            let Some(i) = idx else {
                return Err(QsdbError::invalid(
                    "ORDER BY over set operations must reference output column name or ordinal",
                ));
            };
            keys.push(crate::exec::SortKey {
                expr: PhysExpr::Column {
                    idx: i,
                    name: lc[i].0.clone(),
                    ty: lc[i].1.clone(),
                },
                descending: desc,
                nulls_first,
            });
        }
        plan = LogicalPlan::Sort {
            input: Box::new(plan),
            keys,
        };
    }
    // LIMIT / OFFSET / FETCH（统一走助手：五种写法一处处理）
    let (n, offset) = bind_limit_offset(q.limit_clause.as_ref(), q.fetch.as_ref())?;
    if n.is_some() || offset > 0 {
        plan = LogicalPlan::Limit {
            input: Box::new(plan),
            n,
            offset,
        };
    }
    Ok((plan, cte_defs))
}

/// 绑定 `LIMIT n [OFFSET m]` / 各变体 → `(limit, offset)`。
///
/// 覆盖五种写法：`LIMIT n` / `LIMIT n OFFSET m` / `OFFSET m`（无 LIMIT）/
/// `LIMIT m, n`（MySQL 形，offset 在前）/ `FETCH FIRST n ROWS ONLY`。
///
/// ⚠️ `FETCH FIRST n PERCENT ROWS` 与 `… WITH TIES` **语义不同**（按比例 / 带并列），
/// **不能当 LIMIT 处理**——显式拒绝。
///
/// 收 `(limit_clause, fetch)` 而不是 `&Query`：调用点在 `*q.body` 被部分移动之后，
/// 那时整借 `&q` 不成立（`limit_clause`/`fetch` 是别的字段，可单独借/克隆）。
fn bind_limit_offset(
    limit_clause: Option<&sqlparser::ast::LimitClause>,
    fetch: Option<&sqlparser::ast::Fetch>,
) -> Result<(Option<usize>, usize)> {
    use sqlparser::ast::LimitClause;
    let mut limit: Option<usize> = None;
    let mut offset = 0usize;
    let bind_const = |e: &sqlparser::ast::Expr, what: &str| -> Result<usize> {
        match bind_expr(e, &SchemaCtx { cols: vec![] })? {
            PhysExpr::Lit(Literal::Int64(n)) if n >= 0 => Ok(n as usize),
            _ => Err(QsdbError::invalid(format!(
                "{what} requires a non-negative integer"
            ))),
        }
    };
    if let Some(lc) = limit_clause {
        match lc {
            LimitClause::LimitOffset {
                limit: l,
                offset: o,
                ..
            } => {
                if let Some(l) = l {
                    limit = Some(bind_const(l, "LIMIT")?);
                }
                if let Some(o) = o {
                    offset = bind_const(&o.value, "OFFSET")?;
                }
            }
            // `LIMIT <offset>, <count>`（MySQL 形——offset 在前）
            LimitClause::OffsetCommaLimit {
                offset: o,
                limit: l,
            } => {
                offset = bind_const(o, "LIMIT offset")?;
                limit = Some(bind_const(l, "LIMIT")?);
            }
        }
    }
    if let Some(f) = fetch {
        if f.percent || f.with_ties {
            return Err(QsdbError::unsupported(
                "FETCH FIRST … PERCENT ROWS / WITH TIES is not supported: semantics differ                  from LIMIT (proportional / ties included), so it must not be executed as LIMIT",
            ));
        }
        if let Some(e) = &f.quantity {
            limit = Some(bind_const(e, "FETCH FIRST")?);
        }
    }
    Ok((limit, offset))
}

/// 集合操作一侧 cast 到公共类型（包 Project；列名沿用）。
fn cast_setop_side(
    plan: LogicalPlan,
    common: &[LogicalType],
    names: &[(String, LogicalType)],
) -> LogicalPlan {
    let exprs: Vec<PhysExpr> = (0..common.len())
        .map(|i| {
            let c = PhysExpr::Column {
                idx: i,
                name: names[i].0.clone(),
                ty: names[i].1.clone(),
            };
            c.cast_to_pub(common[i].clone())
        })
        .collect();
    LogicalPlan::Project {
        input: Box::new(plan),
        exprs,
        names: names.iter().map(|(n, _)| n.clone()).collect(),
    }
}

fn translate_select_inner(
    q: sqlparser::ast::Query,
    resolver: &TableResolver,
    ancestors: &VisibleCtes,
) -> Result<(LogicalPlan, Vec<CteDef>)> {
    use sqlparser::ast::*;

    // **静默丢弃闸门**（见 `audit_query`）：每个 Query 的必经点，嵌套子查询自动被审。
    audit_query(&q)?;
    // LIMIT/OFFSET/FETCH 在 `*q.body` 被部分移动之前取出（都是小 AST）
    let limit_clause = q.limit_clause.clone();
    let fetch = q.fetch.clone();

    // 集合操作分支（借用判别，避免 body 部分移动）
    if let SetExpr::SetOperation {
        op,
        left,
        right,
        set_quantifier,
    } = q.body.as_ref()
    {
        return translate_set_operation(
            *op,
            left.clone(),
            right.clone(),
            Some(*set_quantifier),
            &q,
            resolver,
            ancestors,
        );
    }
    let select = match *q.body {
        SetExpr::Select(s) => {
            audit_select(&s)?;
            s
        }
        other => {
            return Err(QsdbError::unsupported(format!(
                "query body {other:?} not supported (plain SELECT only)"
            )));
        }
    };
    if select.from.len() > 1 {
        return Err(QsdbError::unsupported(
            "multi-FROM (comma join) not supported",
        ));
    }
    // CTE（非递归）：WITH name AS (...) 内联展开为子计划。
    // 定义期即分配 cte_id；祖先 CTE 可见（后定义可引用先定义，链式）。
    let (visible, mut cte_defs) = bind_with_ctes(q.with.as_ref(), resolver, ancestors)?;

    // FROM 主表：实表 / 表函数（read_parquet）/ CTE（本级 → 祖先）/ 派生表；
    // 无 FROM → 单行零列源（SELECT 1+1 / SELECT CURRENT_DATE）
    let (mut plan, mut ctx): (LogicalPlan, SchemaCtx) = match select.from.first() {
        None => (LogicalPlan::OneRow, SchemaCtx { cols: Vec::new() }),
        Some(tw) => match &tw.relation {
            TableFactor::Table {
                name,
                args: Some(tf_args),
                ..
            } => resolve_table_function(&object_name_single(name.clone())?, &tf_args.args)?,
            TableFactor::Function { name, args, .. } => {
                resolve_table_function(&object_name_single(name.clone())?, args)?
            }
            TableFactor::Table { name, .. } => {
                resolve_table_source(&object_name_single(name.clone())?, &visible, resolver)?
            }
            TableFactor::Derived { subquery, .. } => {
                let (sub, child_defs) =
                    translate_select_inner((**subquery).clone(), resolver, &visible)?;
                cte_defs.extend(child_defs);
                let cols = plan_output_columns(&sub)?;
                (sub, SchemaCtx { cols })
            }
            other => {
                return Err(QsdbError::unsupported(format!(
                    "FROM {other:?} not supported"
                )));
            }
        },
    };

    // JOIN 链（SQL-2：等值 INNER/LEFT）
    let no_joins: Vec<sqlparser::ast::Join> = Vec::new();
    let joins = select
        .from
        .first()
        .map(|t| t.joins.as_slice())
        .unwrap_or_else(|| no_joins.as_slice());
    for join in joins {
        let (jt, constraint) = match &join.join_operator {
            sqlparser::ast::JoinOperator::Inner(c) | sqlparser::ast::JoinOperator::Join(c) => {
                (crate::exec::join::JoinType::Inner, c)
            }
            sqlparser::ast::JoinOperator::LeftOuter(c) | sqlparser::ast::JoinOperator::Left(c) => {
                (crate::exec::join::JoinType::LeftOuter, c)
            }
            other => {
                return Err(QsdbError::unsupported(format!(
                    "JOIN type {other:?} not supported (INNER/LEFT only)"
                )));
            }
        };
        // 右侧源：实表 / 表函数（read_parquet）/ CTE（别名 → 原名 → 祖先）/ 派生表
        let (right_name, right_plan, right_cols): (
            String,
            LogicalPlan,
            Vec<(String, LogicalType)>,
        ) = match &join.relation {
            TableFactor::Table {
                name,
                args: Some(tf_args),
                alias,
                ..
            } => {
                let (p, c) =
                    resolve_table_function(&object_name_single(name.clone())?, &tf_args.args)?;
                let rn = alias
                    .as_ref()
                    .map(|a| a.name.value.clone())
                    .unwrap_or_else(|| "read_parquet".into());
                (rn, p, c.cols)
            }
            TableFactor::Table { name, alias, .. } => {
                let (rn, orig) = match alias {
                    Some(a) => (a.name.value.clone(), object_name_single(name.clone())?),
                    None => {
                        let n = object_name_single(name.clone())?;
                        (n.clone(), n)
                    }
                };
                let lookup = if visible.contains_key(&rn) {
                    rn.clone()
                } else {
                    orig
                };
                let (plan, cols) = if let Some((id, cols)) = visible.get(&lookup) {
                    (
                        LogicalPlan::Values {
                            columns: cols.clone(),
                            cte_id: *id,
                        },
                        cols.clone(),
                    )
                } else {
                    // 实表按原名解析（lookup 已含别名→CTE 优先逻辑）；别名仅用于限定引用
                    let right_def = (resolver.resolve)(&lookup)?;
                    (
                        LogicalPlan::Scan {
                            def: Arc::clone(&right_def),
                            predicate: None,
                            read: None,
                        },
                        right_def
                            .columns
                            .iter()
                            .map(|c| (c.name.clone(), c.logical_type.clone()))
                            .collect(),
                    )
                };
                (rn, plan, cols)
            }
            // JOIN 右侧派生表：FROM a JOIN (SELECT …) AS r ON …
            TableFactor::Derived {
                subquery, alias, ..
            } => {
                let (sub, child_defs) =
                    translate_select_inner((**subquery).clone(), resolver, &visible)?;
                cte_defs.extend(child_defs);
                let cols = plan_output_columns(&sub)?;
                let name = alias
                    .as_ref()
                    .map(|a| a.name.value.clone())
                    .unwrap_or_else(|| "subquery".into());
                (name, sub, cols)
            }
            other => return Err(QsdbError::unsupported(format!("JOIN target {other:?}"))),
        };
        let sqlparser::ast::JoinConstraint::On(on_expr) = constraint else {
            return Err(QsdbError::unsupported("JOIN ... ON <equi-cond> required"));
        };
        // ctx 扩展：右表列仅以限定名注册（table.col），顺序 = join 输出右段。
        // 非限定右列引用不支持（SQL-2 约束，歧义保护）。
        //
        // **左侧宽度 = `ctx.cols` 现有长度**，不是"非限定列数"。第二次 join 起，
        // 左侧已经含上一次带进来的**限定**列（`… JOIN d ON … JOIN d2 ON d.c = d2.c` 里的
        // `d.c`），按"非限定"计数会把它算进右侧 ⇒ 两个操作数同侧 ⇒ `split_equi_condition`
        // 报错 ⇒ 落到叉积分支（**静默退化**：结果对，代价 5M×|右表|）。
        // 第一次 join 时左侧恰好全是非限定列，两者相等——所以这个错只从第二张表起暴露。
        let n_left = ctx.cols.len();
        for (cn, ct) in &right_cols {
            ctx.cols.push((format!("{right_name}.{cn}"), ct.clone()));
        }
        match split_equi_condition(on_expr, n_left, &ctx) {
            Ok((lkey, rkey)) => {
                plan = LogicalPlan::Join {
                    left: Box::new(plan),
                    right: Box::new(right_plan),
                    left_keys: vec![lkey],
                    right_keys: vec![rkey],
                    join_type: jt,
                    needed: None,
                };
            }
            Err(_) if jt == crate::exec::join::JoinType::Inner => {
                // 非等值 INNER：cross join + 合并 schema 上过滤原 ON 表达式
                let combined = bind_expr(on_expr, &ctx)?;
                if !combined.is_boolean() {
                    return Err(QsdbError::type_err("JOIN ON must be boolean"));
                }
                plan = LogicalPlan::CrossJoinFilter {
                    left: Box::new(plan),
                    right: Box::new(right_plan),
                    predicate: combined,
                };
            }
            Err(e) => {
                return Err(QsdbError::unsupported(format!(
                    "non-equi LEFT JOIN lands later: {e}"
                )));
            }
        }
    }

    // 标量子查询（WHERE/投影中的 (SELECT …)）：提取 → Limit 1 右侧 cross join
    // （常真谓词）→ AST 置换为合成列引用 __scalar_sub_{n}；>1 行取首行（严格报错后置）。
    // 关联（引用外层列）子查询不支持——子查询独立绑定，列缺失即报错。
    let mut scalar_counter = 0usize;
    let selection_sub;
    if let Some(w) = &select.selection {
        let (replaced, qs) = extract_scalar_subqueries(w, &mut scalar_counter);
        plan = attach_scalar_subqueries(plan, &mut ctx, &mut cte_defs, qs, &visible, resolver)?;
        selection_sub = Some(replaced);
    } else {
        selection_sub = None;
    }
    let mut proj_extracted: Vec<Option<Expr>> = Vec::with_capacity(select.projection.len());
    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
                let (replaced, qs) = extract_scalar_subqueries(e, &mut scalar_counter);
                plan = attach_scalar_subqueries(
                    plan,
                    &mut ctx,
                    &mut cte_defs,
                    qs,
                    &visible,
                    resolver,
                )?;
                proj_extracted.push(Some(replaced));
            }
            _ => proj_extracted.push(None),
        }
    }

    // WHERE
    // ① EXISTS / IN 子查询合取 → semi/anti-join（链式叠加；v1 顶层 AND）
    let selection_rest = if let Some(w) = selection_sub {
        let conjuncts = split_conjuncts(&w);
        let (p, regular) =
            attach_semi_subqueries(plan, &ctx, resolver, &visible, &mut cte_defs, conjuncts)?;
        plan = p;
        and_conjuncts(regular)
    } else {
        None
    };
    // ② 其余谓词照常绑定
    if let Some(w) = selection_rest {
        let bound = bind_expr(&w, &ctx)?;
        if !bound.is_boolean() {
            return Err(QsdbError::type_err("WHERE must be boolean"));
        }
        // 谓词仅当根为单 Scan/ExternalScan 时下推：Join/CrossJoin/Values/派生表根上，
        // split_pushdown 的 Scan 附着模式不中会静默丢谓词 → 全部留作行过滤
        let root_is_scan = matches!(
            plan,
            LogicalPlan::Scan { .. } | LogicalPlan::ExternalScan { .. }
        );
        let (pushdown, residual) = if root_is_scan {
            split_pushdown(&bound)
        } else {
            (None, Some(bound))
        };
        if let Some(p) = pushdown {
            if let LogicalPlan::Scan { predicate, .. }
            | LogicalPlan::ExternalScan { predicate, .. } = &mut plan
            {
                *predicate = Some(p);
            }
        }
        if let Some(r) = residual {
            plan = LogicalPlan::Filter {
                input: Box::new(plan),
                predicate: r,
            };
        }
    }

    // GROUP BY
    let group_exprs: Vec<PhysExpr> = match &select.group_by {
        GroupByExpr::All(_) => return Err(QsdbError::unsupported("GROUP BY ALL")),
        GroupByExpr::Expressions(exprs, _) => exprs
            .iter()
            .map(|e| bind_expr(e, &ctx))
            .collect::<Result<_>>()?,
    };

    // SELECT 投影
    struct ProjItem {
        sql: Expr,
        name: String,
    }
    let mut items = Vec::new();
    for (i, item) in select.projection.iter().enumerate() {
        match item {
            SelectItem::UnnamedExpr(e) => items.push(ProjItem {
                sql: proj_extracted[i].clone().expect("scalar-sub extracted"),
                name: expr_display(e),
            }),
            SelectItem::ExprWithAlias { alias, .. } => items.push(ProjItem {
                sql: proj_extracted[i].clone().expect("scalar-sub extracted"),
                name: alias.value.clone(),
            }),
            SelectItem::Wildcard(_) => {
                if !group_exprs.is_empty() {
                    return Err(QsdbError::invalid(
                        "SELECT * with GROUP BY not supported; list group columns explicitly",
                    ));
                }
                for (name, _) in &ctx.cols {
                    items.push(ProjItem {
                        sql: Expr::Identifier(Ident::new(name.clone())),
                        name: name.clone(),
                    });
                }
            }
            SelectItem::QualifiedWildcard(..) => {
                return Err(QsdbError::unsupported("qualified wildcard"));
            }
            SelectItem::ExprWithAliases { .. } => {
                return Err(QsdbError::unsupported(
                    "multiple aliases (expr AS a, b) not supported",
                ));
            }
        }
    }

    // 聚合检测与收集
    let mut aggs: Vec<AggExpr> = Vec::new();
    for it in &items {
        collect_aggs(&it.sql, &ctx, &mut aggs)?;
    }
    // 窗口函数收集（AST 级去重；绑定在确定输入上下文后进行）
    let mut win_calls: Vec<WindowCall> = Vec::new();
    for it in &items {
        collect_window_calls(&it.sql, &mut win_calls);
    }
    let has_window = !win_calls.is_empty();
    // HAVING（SQL-2）：对聚合输出过滤
    let mut having_pred: Option<PhysExpr> = None;
    if let Some(h) = &select.having {
        // 别名置换后的 HAVING 独有聚合（不在 SELECT 里）也进聚合面（隐藏聚合列）
        let pairs: Vec<(Expr, String)> = items
            .iter()
            .map(|it| (it.sql.clone(), it.name.clone()))
            .collect();
        let substituted = substitute_aliases(h, &pairs);
        collect_aggs(&substituted, &ctx, &mut aggs)?;
        if group_exprs.is_empty() && aggs.is_empty() {
            return Err(QsdbError::invalid("HAVING requires aggregation"));
        }
        let mut hctx: Vec<(String, LogicalType)> = group_exprs
            .iter()
            .map(|g| (g.display(), g.data_type()))
            .collect();
        for a in &aggs {
            hctx.push((a.out_name.clone(), a.out_ty.clone()));
        }

        let agg_ctx = SchemaCtx { cols: hctx };
        having_pred = Some(rebind_over_agg(
            &substituted,
            &ctx,
            &group_exprs,
            &aggs,
            &agg_ctx,
        )?);
        if !having_pred.as_ref().unwrap().is_boolean() {
            return Err(QsdbError::type_err("HAVING must be boolean"));
        }
    }
    let is_agg = !aggs.is_empty() || !group_exprs.is_empty();
    // 窗口（无聚合场景：直接在输入上开窗口，按 spec 分组链式叠加）
    let mut windows: Vec<BoundWindow> = Vec::new();
    let mut win_cols: Vec<(String, crate::types::LogicalType)> = Vec::new();
    if has_window && !is_agg {
        windows = bind_window_calls(&win_calls, &mut |e| bind_expr(e, &ctx))?;
        let (p, wc) = chain_window_nodes(plan, &windows);
        plan = p;
        win_cols = wc;
    }

    if is_agg {
        // 聚合后 schema 上下文（重绑定投影）
        let mut agg_ctx_cols: Vec<(String, LogicalType)> = group_exprs
            .iter()
            .map(|g| (g.display(), g.data_type()))
            .collect();
        for a in &aggs {
            agg_ctx_cols.push((a.out_name.clone(), a.out_ty.clone()));
        }
        let agg_ctx = SchemaCtx { cols: agg_ctx_cols };
        plan = LogicalPlan::Aggregate {
            input: Box::new(plan),
            group: group_exprs.clone(),
            aggs: aggs.clone(),
        };
        if let Some(hp) = having_pred {
            plan = LogicalPlan::Filter {
                input: Box::new(plan),
                predicate: hp,
            };
        }
        // 窗口在聚合输出之上（HAVING 之后）：spec/参数重绑定到聚合输出
        let mut proj_exprs = Vec::with_capacity(items.len());
        let mut proj_names = Vec::with_capacity(items.len());
        let mut win_agg_ctx = SchemaCtx {
            cols: agg_ctx.cols.clone(),
        };
        if has_window {
            windows = bind_window_calls(&win_calls, &mut |e| {
                rebind_over_agg(e, &ctx, &group_exprs, &aggs, &agg_ctx)
            })?;
            let (p, wc) = chain_window_nodes(plan, &windows);
            plan = p;
            win_agg_ctx.cols.extend(wc);
        }
        // 投影：窗口调用置换为合成 Identifier 后重绑定（win_agg_ctx 含窗口列）
        for it in &items {
            let sub = if has_window {
                substitute_windows(&it.sql, &windows)
            } else {
                it.sql.clone()
            };
            let rebound = rebind_over_agg(&sub, &ctx, &group_exprs, &aggs, &win_agg_ctx)?;
            proj_exprs.push(rebound);
            proj_names.push(it.name.clone());
        }
        plan = LogicalPlan::Project {
            input: Box::new(plan),
            exprs: proj_exprs,
            names: proj_names,
        };
    } else {
        let mut proj_exprs = Vec::with_capacity(items.len());
        let mut proj_names = Vec::with_capacity(items.len());
        // 投影上下文 = 输入列 ++ 窗口列（链序）；AST 置换后统一按名字绑定
        let mut proj_ctx = SchemaCtx {
            cols: ctx.cols.clone(),
        };
        proj_ctx.cols.extend(win_cols.iter().cloned());
        for it in &items {
            let sub = if has_window {
                substitute_windows(&it.sql, &windows)
            } else {
                it.sql.clone()
            };
            proj_exprs.push(bind_expr(&sub, &proj_ctx)?);
            proj_names.push(it.name.clone());
        }
        plan = LogicalPlan::Project {
            input: Box::new(plan),
            exprs: proj_exprs,
            names: proj_names,
        };
    }

    // DISTINCT
    if select.distinct.is_some() {
        plan = LogicalPlan::Distinct {
            input: Box::new(plan),
        };
    }

    // ORDER BY（绑定于投影输出；未投影表达式追加隐藏列）
    if let Some(order_by) = &q.order_by {
        let (proj_exprs, proj_names) = match &plan {
            LogicalPlan::Project { exprs, names, .. } => (exprs.clone(), names.clone()),
            LogicalPlan::Distinct { input } => match input.as_ref() {
                LogicalPlan::Project { exprs, names, .. } => (exprs.clone(), names.clone()),
                _ => (Vec::new(), Vec::new()),
            },
            _ => (Vec::new(), Vec::new()),
        };
        let out_ctx = SchemaCtx {
            cols: proj_names
                .iter()
                .cloned()
                .map(|n| {
                    // 输出类型从表达式取
                    let ty = proj_exprs
                        .iter()
                        .find(|_| false)
                        .map(|_| LogicalType::Int64)
                        .unwrap_or(LogicalType::Int64);
                    (n, ty)
                })
                .collect(),
        };
        let mut sort_keys = Vec::new();
        let mut extra_exprs = proj_exprs.clone();
        let mut extra_names = proj_names.clone();
        let order_exprs: Vec<OrderByExpr> = match &order_by.kind {
            sqlparser::ast::OrderByKind::Expressions(exprs) => exprs.clone(),
            sqlparser::ast::OrderByKind::All(_) => {
                return Err(QsdbError::unsupported("ORDER BY ALL"));
            }
        };
        for ob in &order_exprs {
            let (key_expr, descending, nulls_first) = {
                let desc = matches!(&ob.options.sort, Some(sqlparser::ast::OrderBySort::Desc));
                (&ob.expr, desc, ob.options.nulls_first)
            };
            // 别名/输出名优先：Identifier 查输出名；纯数字字面量 = 输出序号
            //（1-based，SQL 标准）；其他表达式按 display 匹配投影
            let by_output = match key_expr {
                Expr::Identifier(id) => out_ctx.index_of(&id.value),
                Expr::Value(v) => match &v.value {
                    sqlparser::ast::Value::Number(n, _) => n
                        .parse::<usize>()
                        .ok()
                        .and_then(|i| i.checked_sub(1).filter(|i| *i < proj_names.len())),
                    _ => None,
                },
                other => {
                    let d = expr_display(other);
                    proj_names.iter().position(|n| n.eq_ignore_ascii_case(&d))
                }
            };
            match by_output {
                Some(idx) => sort_keys.push(SortKey {
                    expr: PhysExpr::Column {
                        idx,
                        name: extra_names[idx].clone(),
                        ty: LogicalType::Int64, // 占位；exec 用实际 schema 列
                    },
                    descending,
                    nulls_first,
                }),
                None => {
                    if is_agg {
                        return Err(QsdbError::unsupported(
                            "ORDER BY on non-projected expressions with GROUP BY: project it explicitly",
                        ));
                    }
                    if matches!(plan, LogicalPlan::Distinct { .. }) {
                        return Err(QsdbError::invalid(
                            "ORDER BY with DISTINCT must reference projected columns",
                        ));
                    }
                    let bound = bind_expr(key_expr, &ctx)?;
                    let idx = extra_exprs.len();
                    extra_exprs.push(bound);
                    extra_names.push(format!("__sort_{idx}"));
                    sort_keys.push(SortKey {
                        expr: PhysExpr::Column {
                            idx,
                            name: format!("__sort_{idx}"),
                            ty: LogicalType::Int64,
                        },
                        descending,
                        nulls_first,
                    });
                }
            }
        }
        // 仅当追加了隐藏排序列时才替换顶层投影（DISTINCT 无隐藏列时保持原样）
        let n_client = proj_names.len();
        let added_hidden = extra_exprs.len() > proj_exprs_len_before(&plan);
        if added_hidden {
            plan = replace_top_project(plan, extra_exprs, extra_names);
        }
        plan = LogicalPlan::Sort {
            input: Box::new(plan),
            keys: sort_keys,
        };
        // 隐藏排序列（`__sort_N`）**只服务于排序，不是查询结果的一部分**：在 Sort 之上
        // 补一层投影，把结果收回到客户端要的那几列。（此前隐藏列直接跟着结果出网——
        // `SELECT v FROM t ORDER BY w` 会回**两列**，多出一个 `__sort_1`；对 JDBC 这类
        // 按列数/列名解析的客户端是"凭据错位"。）
        if added_hidden && n_client > 0 {
            let keep: Vec<PhysExpr> = (0..n_client)
                .map(|i| PhysExpr::Column {
                    idx: i,
                    name: proj_names[i].clone(),
                    // 类型取原投影表达式（`Column{ty}` 会被 `ProjectExec` 拿去做 schema，
                    // 占位 Int64 会让字符串列声明成整数）
                    ty: proj_exprs[i].data_type(),
                })
                .collect();
            plan = LogicalPlan::Project {
                input: Box::new(plan),
                exprs: keep,
                names: proj_names.clone(),
            };
        }
    }

    // LIMIT / OFFSET / FETCH（CTE 物化路径同样走助手）
    let (n, offset) = bind_limit_offset(limit_clause.as_ref(), fetch.as_ref())?;
    if n.is_some() || offset > 0 {
        plan = LogicalPlan::Limit {
            input: Box::new(plan),
            n,
            offset,
        };
    }

    let defs = filter_reachable_ctes(cte_defs, &plan);
    Ok((plan, defs))
}

fn proj_exprs_len_before(plan: &LogicalPlan) -> usize {
    match plan {
        LogicalPlan::Project { exprs, .. } => exprs.len(),
        LogicalPlan::Distinct { input } => proj_exprs_len_before(input),
        _ => 0,
    }
}

fn replace_top_project(plan: LogicalPlan, exprs: Vec<PhysExpr>, names: Vec<String>) -> LogicalPlan {
    match plan {
        LogicalPlan::Project { input, .. } => LogicalPlan::Project {
            input,
            exprs,
            names,
        },
        other => other,
    }
}

/// ON expr：等值条件 a.k = b.k 拆为（左键, 右键）。idx < n_left 属左表。
/// 把 `ON <等值>` 拆成 (左键, 右键)。
///
/// `n_left` = **左侧输入的输出宽度**（列序号 < n_left 即属左侧）。调用方必须传
/// `ctx.cols` 在**追加右表列之前**的长度——见 `translate_select_inner` 里 JOIN 循环处
/// 的注释（那里曾误用"非限定列数"，导致第二次 join 静默退化成叉积）。
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

/// HAVING/ORDER BY 中的 SELECT 别名 → 对应源表达式（AST 级浅替换）。
fn substitute_aliases(
    e: &sqlparser::ast::Expr,
    items: &[(sqlparser::ast::Expr, String)],
) -> sqlparser::ast::Expr {
    use sqlparser::ast::Expr as E;
    match e {
        E::Identifier(id) => {
            for it in items {
                let (sql, name) = it;
                if name.eq_ignore_ascii_case(&id.value) {
                    return sql.clone();
                }
            }
            e.clone()
        }
        E::Nested(inner) => E::Nested(Box::new(substitute_aliases(inner, items))),
        E::UnaryOp { op, expr } => E::UnaryOp {
            op: *op,
            expr: Box::new(substitute_aliases(expr, items)),
        },
        E::BinaryOp { left, op, right } => E::BinaryOp {
            left: Box::new(substitute_aliases(left, items)),
            op: op.clone(),
            right: Box::new(substitute_aliases(right, items)),
        },
        E::Cast {
            kind,
            expr,
            data_type,
            format,
        } => E::Cast {
            kind: kind.clone(),
            expr: Box::new(substitute_aliases(expr, items)),
            data_type: data_type.clone(),
            format: format.clone(),
        },
        E::IsNull(inner) => E::IsNull(Box::new(substitute_aliases(inner, items))),
        E::IsNotNull(inner) => E::IsNotNull(Box::new(substitute_aliases(inner, items))),
        other => other.clone(),
    }
}

// ---------------------------------------------------------------------------
// 窗口函数：AST 收集 → 绑定（普通 ctx / 聚合重绑定）→ 按 spec 分组链式叠加 → 投影置换
// ---------------------------------------------------------------------------

/// AST 级窗口调用（未绑定；绑定上下文由调用方决定）。
#[derive(Clone)]
struct WindowCall {
    /// 归一化调用签名 "name(arg,...)"（同名不同参区分）
    key: String,
    /// OVER spec 的 AST 签名（同名同参不同 spec 区分）
    spec_sig: String,
    f: sqlparser::ast::Function,
}

/// 绑定后的窗口：spec + 函数 + 唯一输出列名（AST 置换的目标标识符）。
struct BoundWindow {
    spec: crate::exec::window::WindowSpec,
    wf: crate::exec::window::WindowFn,
    key: String,
    spec_sig: String,
    /// 输出列名：默认 output_name，重名追加 " #n"
    name: String,
}

/// 收集表达式内的窗口调用（含嵌套标量函数参数内）；同调用签名+spec 签名去重。
fn collect_window_calls(e: &sqlparser::ast::Expr, out: &mut Vec<WindowCall>) {
    use sqlparser::ast::Expr as E;
    match e {
        E::Function(f) => {
            if f.over.is_some() {
                let c = WindowCall {
                    key: window_call_key(f),
                    spec_sig: window_spec_sig(f),
                    f: f.clone(),
                };
                if !out
                    .iter()
                    .any(|x| x.key == c.key && x.spec_sig == c.spec_sig)
                {
                    out.push(c);
                }
                return;
            }
            for a in fn_args(f) {
                if let sqlparser::ast::FunctionArg::Unnamed(
                    sqlparser::ast::FunctionArgExpr::Expr(inner),
                ) = a
                {
                    collect_window_calls(inner, out);
                }
            }
        }
        E::BinaryOp { left, right, .. } => {
            collect_window_calls(left, out);
            collect_window_calls(right, out);
        }
        E::Nested(inner) => collect_window_calls(inner, out),
        E::Cast { expr, .. } => collect_window_calls(expr, out),
        E::UnaryOp { expr, .. } => collect_window_calls(expr, out),
        E::IsNull(inner) | E::IsNotNull(inner) => collect_window_calls(inner, out),
        _ => {}
    }
}

/// 归一化调用签名：name(arg1,arg2)，全小写。
fn window_call_key(f: &sqlparser::ast::Function) -> String {
    let args: Vec<String> = fn_args(f)
        .iter()
        .filter_map(|a| match a {
            sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(e)) => {
                Some(e.to_string().to_ascii_lowercase())
            }
            _ => None,
        })
        .collect();
    format!(
        "{}({})",
        f.name.to_string().to_ascii_lowercase(),
        args.join(",")
    )
}

/// OVER spec 的 AST 签名（小写；named window 后置）。
fn window_spec_sig(f: &sqlparser::ast::Function) -> String {
    if let Some(sqlparser::ast::WindowType::WindowSpec(spec)) = &f.over {
        let p: Vec<String> = spec
            .partition_by
            .iter()
            .map(|e| e.to_string().to_ascii_lowercase())
            .collect();
        let o: Vec<String> = spec
            .order_by
            .iter()
            .map(|ob| {
                let desc = matches!(&ob.options.sort, Some(sqlparser::ast::OrderBySort::Desc));
                format!("{}:{desc}", ob.expr.to_string().to_ascii_lowercase())
            })
            .collect();
        format!("p[{}]o[{}]", p.join(","), o.join(","))
    } else {
        "named-window".into()
    }
}

/// AST 置换：窗口调用 → 合成 Identifier（引用窗口输出列，名字绑定层唯一化）。
/// 覆盖 collect_window_calls 的全部形态；纯窗口项同样置换（统一走名字绑定）。
fn substitute_windows(e: &sqlparser::ast::Expr, windows: &[BoundWindow]) -> sqlparser::ast::Expr {
    use sqlparser::ast::Expr as E;
    match e {
        E::Function(f) => {
            if f.over.is_some() {
                let key = window_call_key(f);
                let sig = window_spec_sig(f);
                if let Some(w) = windows.iter().find(|w| w.key == key && w.spec_sig == sig) {
                    return E::Identifier(sqlparser::ast::Ident::new(w.name.clone()));
                }
                return e.clone();
            }
            // 嵌套在标量函数参数里的窗口（abs(sum(x) OVER (...))）
            let mut nf = f.clone();
            let new_args: Vec<sqlparser::ast::FunctionArg> = fn_args(f)
                .iter()
                .map(|a| match a {
                    sqlparser::ast::FunctionArg::Unnamed(
                        sqlparser::ast::FunctionArgExpr::Expr(inner),
                    ) => sqlparser::ast::FunctionArg::Unnamed(
                        sqlparser::ast::FunctionArgExpr::Expr(substitute_windows(inner, windows)),
                    ),
                    other => other.clone(),
                })
                .collect();
            if let sqlparser::ast::FunctionArguments::List(l) = &mut nf.args {
                l.args = new_args;
            }
            E::Function(nf)
        }
        E::BinaryOp { left, op, right } => E::BinaryOp {
            left: Box::new(substitute_windows(left, windows)),
            op: op.clone(),
            right: Box::new(substitute_windows(right, windows)),
        },
        E::Nested(inner) => E::Nested(Box::new(substitute_windows(inner, windows))),
        E::UnaryOp { expr, .. } => {
            let inner = substitute_windows(expr, windows);
            let mut u = e.clone();
            if let E::UnaryOp { expr: slot, .. } = &mut u {
                **slot = inner;
            }
            u
        }
        E::Cast { expr, .. } => {
            let inner = substitute_windows(expr, windows);
            let mut c = e.clone();
            if let E::Cast { expr: slot, .. } = &mut c {
                **slot = inner;
            }
            c
        }
        E::IsNull(inner) => E::IsNull(Box::new(substitute_windows(inner, windows))),
        E::IsNotNull(inner) => E::IsNotNull(Box::new(substitute_windows(inner, windows))),
        other => other.clone(),
    }
}

/// 绑定全部窗口调用（bind 决定上下文：普通 ctx 或聚合重绑定）+ 输出列名唯一化。
fn bind_window_calls(
    calls: &[WindowCall],
    bind: &mut dyn FnMut(&sqlparser::ast::Expr) -> Result<PhysExpr>,
) -> Result<Vec<BoundWindow>> {
    let mut out = Vec::with_capacity(calls.len());
    for c in calls {
        let spec = parse_window_spec(&c.f, bind)?;
        let wf = parse_window_fn(&c.f, bind)?;
        out.push(BoundWindow {
            spec,
            wf,
            key: c.key.clone(),
            spec_sig: c.spec_sig.clone(),
            name: String::new(),
        });
    }
    for i in 0..out.len() {
        let base = out[i].wf.output_name();
        let dup = out[..i].iter().filter(|w| w.name == base).count();
        out[i].name = if dup == 0 {
            base
        } else {
            format!("{base} #{}", dup + 1)
        };
    }
    Ok(out)
}

/// 解析 OVER spec（PARTITION BY / ORDER BY 经 bind 绑定）。
fn parse_window_spec(
    f: &sqlparser::ast::Function,
    bind: &mut dyn FnMut(&sqlparser::ast::Expr) -> Result<PhysExpr>,
) -> Result<crate::exec::window::WindowSpec> {
    // 窗口函数**绕过 `bind_expr`**（重写遍就把调用换成合成列名了），所以
    // 函数子句闸门（`IGNORE NULLS` 等）与帧闸门都在此处挂。
    audit_function(f)?;
    if let Some(sqlparser::ast::WindowType::WindowSpec(spec)) = &f.over {
        audit_window_spec(spec)?;
        let mut partition_by = Vec::new();
        for p in &spec.partition_by {
            partition_by.push(bind(p)?);
        }
        let mut order_by = Vec::new();
        for ob in &spec.order_by {
            order_by.push(crate::exec::SortKey {
                expr: bind(&ob.expr)?,
                descending: matches!(&ob.options.sort, Some(sqlparser::ast::OrderBySort::Desc)),
                nulls_first: ob.options.nulls_first,
            });
        }
        Ok(crate::exec::window::WindowSpec {
            partition_by,
            order_by,
        })
    } else {
        Err(QsdbError::unsupported("named window (WINDOW 子句)"))
    }
}

/// 解析窗口函数本体（参数经 bind 绑定）。
fn parse_window_fn(
    f: &sqlparser::ast::Function,
    bind: &mut dyn FnMut(&sqlparser::ast::Expr) -> Result<PhysExpr>,
) -> Result<crate::exec::window::WindowFn> {
    use crate::exec::window::WindowFn as W;
    let name = fn_name(f);
    let arg_exprs: Vec<sqlparser::ast::Expr> = fn_args(f)
        .iter()
        .filter_map(|a| match a {
            sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(e)) => {
                Some(e.clone())
            }
            _ => None,
        })
        .collect();
    match name.as_str() {
        "row_number" | "rownum" => Ok(W::RowNumber),
        "rank" => Ok(W::Rank),
        "dense_rank" => Ok(W::DenseRank),
        "sum" | "count" | "min" | "max" | "avg" => {
            let first = arg_exprs
                .first()
                .ok_or_else(|| QsdbError::invalid("window agg needs an argument"))?;
            let arg = bind(first)?;
            Ok(match name.as_str() {
                "sum" => W::Sum(arg),
                "count" => W::Count(arg),
                "min" => W::Min(arg),
                "max" => W::Max(arg),
                _ => W::Avg(arg),
            })
        }
        "lag" | "lead" => {
            let first = arg_exprs
                .first()
                .ok_or_else(|| QsdbError::invalid("lag/lead needs an argument"))?;
            let arg = bind(first)?;
            // offset：正整数常量（默认 1）
            let offset = match arg_exprs.get(1) {
                None => 1i64,
                Some(sqlparser::ast::Expr::Value(sqlparser::ast::ValueWithSpan {
                    value: sqlparser::ast::Value::Number(s, _),
                    ..
                })) => {
                    let v: i64 = s.parse().map_err(|_| {
                        QsdbError::invalid(format!("lag/lead offset '{s}' not an integer"))
                    })?;
                    if v <= 0 {
                        return Err(QsdbError::invalid("lag/lead offset must be positive"));
                    }
                    v
                }
                Some(_) => {
                    return Err(QsdbError::invalid(
                        "lag/lead offset must be an integer literal",
                    ));
                }
            };
            // default：绑定后 cast 到参数类型
            let default = match arg_exprs.get(2) {
                None => None,
                Some(d) => Some(Box::new(bind(d)?.cast_to(arg.data_type()))),
            };
            Ok(if name == "lag" {
                W::Lag(arg, offset, default)
            } else {
                W::Lead(arg, offset, default)
            })
        }
        other => Err(QsdbError::unsupported(format!(
            "window function '{other}' (support: row_number/rank/dense_rank/sum/count/min/max/avg/lag/lead)"
        ))),
    }
}

/// 按 spec 绑定签名分组，链式叠加 Window 节点（spec 共享一节点）；
/// 返回窗口输出列（列名, 类型）按链序（组间首见序、组内收集序）。
fn chain_window_nodes(
    plan: LogicalPlan,
    windows: &[BoundWindow],
) -> (LogicalPlan, Vec<(String, crate::types::LogicalType)>) {
    let mut groups: Vec<(String, Vec<usize>)> = Vec::new();
    for (i, w) in windows.iter().enumerate() {
        let sig = w.spec.signature();
        match groups.iter_mut().find(|(s, _)| *s == sig) {
            Some((_, idxs)) => idxs.push(i),
            None => groups.push((sig, vec![i])),
        }
    }
    let mut plan = plan;
    let mut cols = Vec::new();
    for (_, idxs) in &groups {
        plan = LogicalPlan::Window {
            input: Box::new(plan),
            partition_by: windows[idxs[0]].spec.partition_by.clone(),
            order_by: windows[idxs[0]].spec.order_by.clone(),
            window_fns: idxs.iter().map(|&i| windows[i].wf.clone()).collect(),
        };
        for &i in idxs {
            cols.push((windows[i].name.clone(), windows[i].wf.output_type()));
        }
    }
    (plan, cols)
}

fn expr_display(e: &sqlparser::ast::Expr) -> String {
    e.to_string()
}

// ---------------------------------------------------------------------------
// 表达式绑定（无聚合上下文）
// ---------------------------------------------------------------------------

pub(crate) fn bind_expr(e: &sqlparser::ast::Expr, ctx: &SchemaCtx) -> Result<PhysExpr> {
    // **函数子句闸门**（见 `audit_function`）：函数可以出现在任何表达式位置
    // （投影/WHERE/HAVING/ORDER BY），而 `bind_expr` 是它们共同的门。
    if let sqlparser::ast::Expr::Function(f) = e {
        audit_function(f)?;
    }
    use sqlparser::ast::Expr as E;
    match e {
        E::Identifier(id) => {
            let idx = ctx.index_of(&id.value).ok_or_else(|| {
                // **JOIN 右表的列只以限定名注册**（见 `translate_select_inner` 里
                // ctx 扩展处的注释）：裸名恒解析到左表。所以"没找到"若其实是右侧的列，
                // 直接说清该怎么写——否则用户只会看到"列不存在"而去怀疑列名拼错。
                let hint = ctx
                    .cols
                    .iter()
                    .find(|(n, _)| n.rsplit_once('.').is_some_and(|(_, c)| c == id.value))
                    .map(|(n, _)| {
                        format!(
                            " (a JOIN right-side column with this name exists — qualify it as `{n}`:                              unqualified names resolve to the left input only)"
                        )
                    })
                    .unwrap_or_default();
                QsdbError::column_not_found(format!("column '{}'{hint}", id.value))
            })?;
            Ok(PhysExpr::Column {
                idx,
                name: ctx.cols[idx].0.clone(),
                ty: ctx.cols[idx].1.clone(),
            })
        }
        E::CompoundIdentifier(parts) if parts.len() == 2 => {
            let qual = format!("{}.{}", parts[0].value, parts[1].value);
            // 限定名优先（JOIN 右表列以 table.col 注册）；无则退回裸列名
            let idx = ctx
                .index_of(&qual)
                .or_else(|| ctx.index_of(&parts[1].value))
                .ok_or_else(|| {
                    QsdbError::column_not_found(format!("column '{}'", parts[1].value))
                })?;
            Ok(PhysExpr::Column {
                idx,
                name: ctx.cols[idx].0.clone(),
                ty: ctx.cols[idx].1.clone(),
            })
        }
        E::Value(v) => Ok(PhysExpr::Lit(value_to_literal(v)?)),
        E::TypedString(_) => {
            let lit = constant_expr(e)?;
            // J10 修复（qsdb-jdbc 反馈）：原实现只识别 Date，**其余一律 cast 到
            // Timestamp**——`TIME '08:30:00'` 的 Literal::Time 落进兜底分支后被
            // 请求 Time64→Timestamp 转换，而 arrow 不支持该方向 → 投影即报错。
            // 三种 temporal 字面量各自保持原类型直通。
            let ty = match &lit {
                Literal::Date(_) => LogicalType::Date,
                Literal::Time(_) => LogicalType::Time,
                Literal::Timestamp(_) => LogicalType::Timestamp { timezone: None },
                _ => LogicalType::Timestamp { timezone: None },
            };
            Ok(PhysExpr::Lit(lit).cast_to(ty))
        }
        // ── M7-B 表达式补全（向量化 CASE / coalesce / nullif / ||）────────
        E::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            // 简单形态：CASE x WHEN v1 THEN r1 ... → 转搜索形态 x = v1
            let conds: Vec<(PhysExpr, PhysExpr)> = match operand {
                Some(op_expr) => {
                    let subject = bind_expr(op_expr, ctx)?;
                    let mut out = Vec::with_capacity(conditions.len());
                    for cw in conditions {
                        let cond = bind_cmp_phys(
                            CmpOp::Eq,
                            subject.clone(),
                            bind_expr(&cw.condition, ctx)?,
                        )?;
                        out.push((cond, bind_expr(&cw.result, ctx)?));
                    }
                    out
                }
                None => {
                    let mut out = Vec::with_capacity(conditions.len());
                    for cw in conditions {
                        let cond = bind_expr(&cw.condition, ctx)?;
                        if !cond.is_boolean() {
                            return Err(QsdbError::type_err("CASE WHEN condition must be boolean"));
                        }
                        out.push((cond, bind_expr(&cw.result, ctx)?));
                    }
                    out
                }
            };
            let else_expr = match else_result {
                Some(e) => bind_expr(e, ctx)?,
                None => PhysExpr::Lit(Literal::Null),
            };
            finish_case(conds, else_expr)
        }
        E::Function(f)
            if f.name.to_string().eq_ignore_ascii_case("coalesce") && f.over.is_none() =>
        {
            let args = arg_exprs(f)?;
            if args.len() < 2 {
                return Err(QsdbError::invalid("coalesce requires >= 2 arguments"));
            }
            let bound: Vec<PhysExpr> = args
                .iter()
                .map(|a| bind_expr(a, ctx))
                .collect::<Result<_>>()?;
            // 类型：非 NULL 参数的公共超类型
            let mut ty: Option<LogicalType> = None;
            for b in &bound {
                if matches!(b, PhysExpr::Lit(Literal::Null)) {
                    continue;
                }
                ty = Some(match ty {
                    None => b.data_type(),
                    Some(acc) => common_supertype(&acc, &b.data_type()),
                });
            }
            let ty = ty.unwrap_or(LogicalType::Int64);
            let casted: Vec<PhysExpr> = bound.into_iter().map(|b| b.cast_to(ty.clone())).collect();
            Ok(PhysExpr::Coalesce(casted, ty))
        }
        E::Function(f) if f.name.to_string().eq_ignore_ascii_case("nullif") && f.over.is_none() => {
            let args = arg_exprs(f)?;
            if args.len() != 2 {
                return Err(QsdbError::invalid(
                    "nullif(a, b) requires exactly 2 arguments",
                ));
            }
            let a = bind_expr(args[0], ctx)?;
            let b = bind_expr(args[1], ctx)?;
            let ty = a.data_type();
            // nullif(a, b) → CASE WHEN a = b THEN NULL ELSE a
            let cond = bind_cmp_phys(CmpOp::Eq, a.clone(), b)?;
            Ok(PhysExpr::Case {
                conds: vec![(cond, PhysExpr::Lit(Literal::Null).cast_to(ty.clone()))],
                else_expr: Box::new(a.cast_to(ty.clone())),
                ty,
            })
        }
        // date_diff(unit, a, b)（DuckDB 签名）：unit 字面量 + 字符串日期提升 + Date/Ts 归一
        E::Function(f)
            if f.name.to_string().eq_ignore_ascii_case("date_diff") && f.over.is_none() =>
        {
            let args = arg_exprs(f)?;
            if args.len() != 3 {
                return Err(QsdbError::invalid(
                    "date_diff(unit, a, b) requires exactly 3 arguments",
                ));
            }
            let unit = match bind_expr(args[0], ctx)? {
                PhysExpr::Lit(Literal::Utf8(s)) => s,
                _ => {
                    return Err(QsdbError::invalid(
                        "date_diff unit must be a string literal (year/quarter/month/week/day/hour/minute/second)",
                    ));
                }
            };
            let mut a = bind_expr(args[1], ctx)?;
            let mut b = bind_expr(args[2], ctx)?;
            // 字符串字面量 → 对侧 temporal 类型提升（F16 同款白名单）
            if !matches!(
                a.data_type(),
                LogicalType::Date | LogicalType::Timestamp { .. } | LogicalType::Time
            ) && matches!(
                b.data_type(),
                LogicalType::Date | LogicalType::Timestamp { .. }
            ) {
                if let PhysExpr::Lit(Literal::Utf8(s)) = &a {
                    let lit = parse_temporal_literal(s, &b.data_type())?;
                    a = PhysExpr::Lit(lit);
                }
            }
            if !matches!(
                b.data_type(),
                LogicalType::Date | LogicalType::Timestamp { .. } | LogicalType::Time
            ) && matches!(
                a.data_type(),
                LogicalType::Date | LogicalType::Timestamp { .. }
            ) {
                if let PhysExpr::Lit(Literal::Utf8(s)) = &b {
                    let lit = parse_temporal_literal(s, &a.data_type())?;
                    b = PhysExpr::Lit(lit);
                }
            }
            // a/b 类型归一（Date ↔ Timestamp → Timestamp）
            let (ta, tb) = (a.data_type(), b.data_type());
            if ta != tb {
                let common = cmp_common_type(&ta, &tb)?;
                a = a.cast_to(common.clone());
                b = b.cast_to(common);
            }
            let def = scalar::lookup("date_diff")?;
            let ty = (def.ret_ty)(&[
                LogicalType::Varchar { max_chars: None },
                a.data_type(),
                b.data_type(),
            ])?;
            Ok(PhysExpr::Fn {
                name: "date_diff".into(),
                args: vec![PhysExpr::Lit(Literal::Utf8(unit)), a, b],
                ty,
            })
        }
        E::Nested(inner) => bind_expr(inner, ctx),
        E::UnaryOp { op, expr } => match op {
            sqlparser::ast::UnaryOperator::Not => {
                let inner = bind_expr(expr, ctx)?;
                if !inner.is_boolean() {
                    return Err(QsdbError::type_err("NOT requires boolean operand"));
                }
                Ok(PhysExpr::Not(Box::new(inner)))
            }
            sqlparser::ast::UnaryOperator::Minus => {
                // -x → 0 - x
                let inner = bind_expr(expr, ctx)?;
                bind_binary_arith(
                    ArithOp::Sub,
                    PhysExpr::Lit(zero_of(&inner.data_type())?),
                    inner,
                )
            }
            other => Err(QsdbError::unsupported(format!("unary {other:?}"))),
        },
        E::BinaryOp { left, op, right } => {
            use sqlparser::ast::BinaryOperator as B;
            match op {
                B::Plus => {
                    bind_binary_arith(ArithOp::Add, bind_expr(left, ctx)?, bind_expr(right, ctx)?)
                }
                B::Minus => {
                    bind_binary_arith(ArithOp::Sub, bind_expr(left, ctx)?, bind_expr(right, ctx)?)
                }
                B::Multiply => {
                    bind_binary_arith(ArithOp::Mul, bind_expr(left, ctx)?, bind_expr(right, ctx)?)
                }
                B::Divide => {
                    bind_binary_arith(ArithOp::Div, bind_expr(left, ctx)?, bind_expr(right, ctx)?)
                }
                B::Modulo => {
                    bind_binary_arith(ArithOp::Mod, bind_expr(left, ctx)?, bind_expr(right, ctx)?)
                }
                B::StringConcat => {
                    let l = bind_expr(left, ctx)?.cast_to(LogicalType::Varchar { max_chars: None });
                    let r =
                        bind_expr(right, ctx)?.cast_to(LogicalType::Varchar { max_chars: None });
                    Ok(PhysExpr::Concat(Box::new(l), Box::new(r)))
                }
                B::Eq => bind_cmp(CmpOp::Eq, left, right, ctx),
                B::NotEq => bind_cmp(CmpOp::NotEq, left, right, ctx),
                B::Lt => bind_cmp(CmpOp::Lt, left, right, ctx),
                B::LtEq => bind_cmp(CmpOp::LtEq, left, right, ctx),
                B::Gt => bind_cmp(CmpOp::Gt, left, right, ctx),
                B::GtEq => bind_cmp(CmpOp::GtEq, left, right, ctx),
                B::And => {
                    let l = bind_expr(left, ctx)?;
                    let r = bind_expr(right, ctx)?;
                    if !l.is_boolean() || !r.is_boolean() {
                        return Err(QsdbError::type_err("AND requires boolean operands"));
                    }
                    Ok(PhysExpr::And(Box::new(l), Box::new(r)))
                }
                B::Or => {
                    let l = bind_expr(left, ctx)?;
                    let r = bind_expr(right, ctx)?;
                    if !l.is_boolean() || !r.is_boolean() {
                        return Err(QsdbError::type_err("OR requires boolean operands"));
                    }
                    Ok(PhysExpr::Or(Box::new(l), Box::new(r)))
                }
                other => Err(QsdbError::unsupported(format!(
                    "operator {other:?} not supported"
                ))),
            }
        }
        // IN (v1, v2, …)：展开为等值 OR 链（NOT IN → 取反）；空列表恒 false
        E::InList {
            expr,
            list,
            negated,
        } => {
            let mut acc: Option<PhysExpr> = None;
            for v in list {
                let eq = bind_cmp(CmpOp::Eq, expr, v, ctx)?;
                acc = Some(match acc {
                    None => eq,
                    Some(a) => PhysExpr::Or(Box::new(a), Box::new(eq)),
                });
            }
            let e = acc.unwrap_or(PhysExpr::Lit(Literal::Boolean(false)));
            if *negated {
                Ok(PhysExpr::Not(Box::new(e)))
            } else {
                Ok(e)
            }
        }
        E::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => {
            // SUBSTRING(x FROM a FOR b) / substr(x, a, b) → 标量函数 substr
            let s = bind_expr(expr, ctx)?;
            let from = substring_from
                .as_ref()
                .map(|e| bind_expr(e, ctx))
                .transpose()?
                .unwrap_or(PhysExpr::Lit(Literal::Int64(1)));
            let args = match substring_for {
                Some(f) => vec![s, from, bind_expr(f, ctx)?],
                None => vec![s, from],
            };
            let arg_tys: Vec<LogicalType> = args.iter().map(|a| a.data_type()).collect();
            let def = scalar::lookup("substr")?;
            let ty = (def.ret_ty)(&arg_tys)?;
            Ok(PhysExpr::Fn {
                name: "substr".into(),
                args,
                ty,
            })
        }
        E::Floor { expr, .. } => bind_floor_ceil(expr, ctx, "floor"),
        E::Ceil { expr, .. } => bind_floor_ceil(expr, ctx, "ceil"),
        // EXTRACT(field FROM x) / EXTRACT(field, x) → extract('<field>', x)
        E::Extract { field, expr, .. } => {
            let inner = bind_expr(expr, ctx)?;
            let ty = inner.data_type();
            if !matches!(
                ty,
                LogicalType::Date | LogicalType::Timestamp { .. } | LogicalType::Time
            ) {
                return Err(QsdbError::type_err(format!(
                    "extract on {ty} not supported (DATE/TIMESTAMP/TIME)"
                )));
            }
            Ok(PhysExpr::Fn {
                name: "extract".into(),
                args: vec![
                    PhysExpr::Lit(Literal::Utf8(datetime_field_str(field)?)),
                    inner,
                ],
                ty: LogicalType::Int64,
            })
        }
        E::IsNull(inner) => Ok(PhysExpr::IsNull(Box::new(bind_expr(inner, ctx)?), false)),
        E::IsNotNull(inner) => Ok(PhysExpr::IsNull(Box::new(bind_expr(inner, ctx)?), true)),
        // IS TRUE/FALSE 族：NULL 归 false/true（Coalesce+Not 组合，无新节点）
        // IS TRUE = coalesce(x, false)；IS NOT TRUE = coalesce(NOT x, true)…
        E::IsTrue(inner) => {
            let x = bind_expr(inner, ctx)?;
            require_boolean(&x, "IS TRUE")?;
            Ok(PhysExpr::Coalesce(
                vec![x, PhysExpr::Lit(Literal::Boolean(false))],
                LogicalType::Boolean,
            ))
        }
        E::IsNotTrue(inner) => {
            let x = bind_expr(inner, ctx)?;
            require_boolean(&x, "IS NOT TRUE")?;
            Ok(PhysExpr::Coalesce(
                vec![
                    PhysExpr::Not(Box::new(x)),
                    PhysExpr::Lit(Literal::Boolean(true)),
                ],
                LogicalType::Boolean,
            ))
        }
        E::IsFalse(inner) => {
            let x = bind_expr(inner, ctx)?;
            require_boolean(&x, "IS FALSE")?;
            Ok(PhysExpr::Coalesce(
                vec![
                    PhysExpr::Not(Box::new(x)),
                    PhysExpr::Lit(Literal::Boolean(false)),
                ],
                LogicalType::Boolean,
            ))
        }
        E::IsNotFalse(inner) => {
            let x = bind_expr(inner, ctx)?;
            require_boolean(&x, "IS NOT FALSE")?;
            Ok(PhysExpr::Coalesce(
                vec![x, PhysExpr::Lit(Literal::Boolean(true))],
                LogicalType::Boolean,
            ))
        }
        // IS [NOT] DISTINCT FROM：NULL 安全等值/不等（三值 → 二值）
        // IS NOT DISTINCT FROM ≡ (x = y) OR (x IS NULL AND y IS NULL)
        // IS DISTINCT FROM ≡ NOT 上式
        E::IsNotDistinctFrom(l, r) => {
            let lb = bind_expr(l, ctx)?;
            let rb = bind_expr(r, ctx)?;
            bind_is_not_distinct(lb, rb, false)
        }
        E::IsDistinctFrom(l, r) => {
            let lb = bind_expr(l, ctx)?;
            let rb = bind_expr(r, ctx)?;
            bind_is_not_distinct(lb, rb, true)
        }
        // LIKE / ILIKE（模式须字符串字面量——绑定期预编译；ESCAPE 显式指定）
        E::Like {
            negated,
            any,
            expr,
            pattern,
            escape_char,
        } if !any => {
            let inner = bind_expr(expr, ctx)?;
            let pat = bind_expr(pattern, ctx)?;
            let esc = escape_char_literal(escape_char.as_deref(), ctx)?;
            like_from_phys(inner, pat, esc, *negated, false)
        }
        E::ILike {
            negated,
            any,
            expr,
            pattern,
            escape_char,
        } if !any => {
            let inner = bind_expr(expr, ctx)?;
            let pat = bind_expr(pattern, ctx)?;
            let esc = escape_char_literal(escape_char.as_deref(), ctx)?;
            like_from_phys(inner, pat, esc, *negated, true)
        }
        // x BETWEEN lo AND hi ≡ x >= lo AND x <= hi（NOT 取反；Kleene 三值）
        E::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let x = bind_expr(expr, ctx)?;
            let ge = bind_cmp_phys(CmpOp::GtEq, x.clone(), bind_expr(low, ctx)?)?;
            let le = bind_cmp_phys(CmpOp::LtEq, x, bind_expr(high, ctx)?)?;
            let both = PhysExpr::And(Box::new(ge), Box::new(le));
            Ok(if *negated {
                PhysExpr::Not(Box::new(both))
            } else {
                both
            })
        }
        E::Cast {
            expr,
            data_type,
            kind: _,
            format: _,
        } => {
            let inner = bind_expr(expr, ctx)?;
            let ty = cast_target_to_logical(data_type)?;
            if crate::types::cast::cast_rule(&inner.data_type(), &ty)
                == crate::types::cast::CastRule::Impossible
            {
                return Err(QsdbError::type_err(format!(
                    "cannot cast {} to {ty}",
                    inner.data_type()
                )));
            }
            Ok(PhysExpr::Cast {
                e: Box::new(inner),
                ty,
            })
        }
        E::Function(f) => {
            if let Some(agg) = try_bind_agg(f, ctx)? {
                // 聚合出现在非聚合上下文（binder 外层保证不会到这里）
                let _ = agg;
                return Err(QsdbError::invalid(format!(
                    "aggregate {}() not allowed here",
                    f.name
                )));
            }
            bind_scalar_fn(f, ctx)
        }
        other => Err(QsdbError::unsupported(format!(
            "expression {other:?} not supported"
        ))),
    }
}

fn bind_floor_ceil(e: &sqlparser::ast::Expr, ctx: &SchemaCtx, fname: &str) -> Result<PhysExpr> {
    let inner = bind_expr(e, ctx)?;
    let def = scalar::lookup(fname)?;
    let ty = (def.ret_ty)(&[inner.data_type()])?;
    Ok(PhysExpr::Fn {
        name: fname.to_string(),
        args: vec![inner],
        ty,
    })
}

/// EXTRACT 字段 → 规范名（extract eval 层的 unit 词表）。
fn datetime_field_str(f: &sqlparser::ast::DateTimeField) -> Result<String> {
    use sqlparser::ast::DateTimeField as F;
    Ok(match f {
        F::Year | F::Years => "year",
        F::Quarter => "quarter",
        F::Month | F::Months => "month",
        F::Day | F::Days | F::Date => "day",
        F::DayOfWeek | F::Dow => "dow",
        F::DayOfYear | F::Doy => "doy",
        F::Hour | F::Hours => "hour",
        F::Minute | F::Minutes => "minute",
        F::Second | F::Seconds => "second",
        // 逗号/引号形态（extract('year', x)）→ 词表由 eval 层校验
        F::Custom(id) => return Ok(id.value.to_ascii_lowercase()),
        other => {
            return Err(QsdbError::unsupported(format!(
                "extract field {other:?} not supported"
            )));
        }
    }
    .to_string())
}

fn zero_of(ty: &LogicalType) -> Result<Literal> {
    Ok(match ty {
        LogicalType::Int8 => Literal::Int8(0),
        LogicalType::Int16 => Literal::Int16(0),
        LogicalType::Int32 => Literal::Int32(0),
        LogicalType::Int64 => Literal::Int64(0),
        LogicalType::Float32 => Literal::Float32(0.0),
        LogicalType::Float64 => Literal::Float64(0.0),
        LogicalType::Decimal(m) => Literal::Decimal {
            unscaled: 0,
            meta: *m,
        },
        other => return Err(QsdbError::type_err(format!("cannot negate {other}"))),
    })
}

/// 同型不包 Cast，异型包 Cast 节点（表达式公共转换辅助）。
pub(crate) fn cast_to_impl(e: PhysExpr, ty: LogicalType) -> PhysExpr {
    if e.data_type() == ty {
        e
    } else {
        PhysExpr::Cast { e: Box::new(e), ty }
    }
}

impl PhysExpr {
    fn cast_to(self, ty: LogicalType) -> PhysExpr {
        cast_to_impl(self, ty)
    }
}

// -- 算术绑定 --------------------------------------------------------------

/// CASE 尾段：分支公共超类型统一 + cast（普通/聚合上两路共用）。
fn finish_case(conds: Vec<(PhysExpr, PhysExpr)>, else_expr: PhysExpr) -> Result<PhysExpr> {
    // 类型统一：分支求公共超类型（数值塔向宽处靠 / 字符串收编）
    let mut ty: Option<LogicalType> = None;
    for b in conds
        .iter()
        .map(|(_, r)| r)
        .chain(std::iter::once(&else_expr))
    {
        if matches!(b, PhysExpr::Lit(Literal::Null)) {
            continue; // 无类型 NULL 分支跳过
        }
        ty = Some(match ty {
            None => b.data_type(),
            Some(acc) => common_supertype(&acc, &b.data_type()),
        });
    }
    let ty = ty.unwrap_or(LogicalType::Int64); // 全 NULL 分支
    let conds: Vec<(PhysExpr, PhysExpr)> = conds
        .into_iter()
        .map(|(c, r)| (c, r.cast_to(ty.clone())))
        .collect();
    let else_expr = else_expr.cast_to(ty.clone());
    Ok(PhysExpr::Case {
        conds,
        else_expr: Box::new(else_expr),
        ty,
    })
}

/// 布尔操作数闸门（IS TRUE 族用）。
fn require_boolean(e: &PhysExpr, what: &str) -> Result<()> {
    if !e.is_boolean() {
        return Err(QsdbError::type_err(format!(
            "{what} requires boolean operand, got {}",
            e.data_type()
        )));
    }
    Ok(())
}

/// IS [NOT] DISTINCT FROM（null-safe 等值/不等；negate=true 为 DISTINCT 形态）。
/// NOT DISTINCT ≡ (x = y) OR (x IS NULL AND y IS NULL)。
/// DISTINCT 不能用 NOT 包裹（Kleene 三值：NOT UNKNOWN = UNKNOWN ≠ TRUE）——
/// 显式展开为 (单侧 NULL) OR (x != y)。
fn bind_is_not_distinct(l: PhysExpr, r: PhysExpr, negate: bool) -> Result<PhysExpr> {
    let is_null = |e: PhysExpr, neg: bool| PhysExpr::IsNull(Box::new(e), neg);
    if !negate {
        let eq = bind_cmp_phys(CmpOp::Eq, l.clone(), r.clone())?;
        let both_null = PhysExpr::And(Box::new(is_null(l, false)), Box::new(is_null(r, false)));
        return Ok(PhysExpr::Or(Box::new(eq), Box::new(both_null)));
    }
    // DISTINCT：(x 空且 y 非空) OR (x 非空且 y 空) OR (x != y)
    let t1 = PhysExpr::And(
        Box::new(is_null(l.clone(), false)),
        Box::new(is_null(r.clone(), true)),
    );
    let t2 = PhysExpr::And(
        Box::new(is_null(l.clone(), true)),
        Box::new(is_null(r.clone(), false)),
    );
    let neq = bind_cmp_phys(CmpOp::NotEq, l, r)?;
    Ok(PhysExpr::Or(
        Box::new(PhysExpr::Or(Box::new(t1), Box::new(t2))),
        Box::new(neq),
    ))
}

/// ESCAPE 子句 → 单字符（None = 无转义，`\` 为普通字符——Postgres 默认）。
fn escape_char_literal(e: Option<&sqlparser::ast::Expr>, ctx: &SchemaCtx) -> Result<Option<char>> {
    match e {
        None => Ok(None),
        Some(ee) => match bind_expr(ee, ctx)? {
            PhysExpr::Lit(Literal::Utf8(s)) => {
                let mut cs = s.chars();
                let c = cs
                    .next()
                    .ok_or_else(|| QsdbError::invalid("ESCAPE must be a single character"))?;
                if cs.next().is_some() {
                    return Err(QsdbError::invalid("ESCAPE must be a single character"));
                }
                Ok(Some(c))
            }
            _ => Err(QsdbError::invalid("ESCAPE must be a character literal")),
        },
    }
}

/// 已绑定操作数 + 模式字面量 → LIKE 表达式（普通/聚合上两路共用）。
fn like_from_phys(
    inner: PhysExpr,
    pat: PhysExpr,
    esc: Option<char>,
    negated: bool,
    ci: bool,
) -> Result<PhysExpr> {
    if !inner.data_type().is_string() {
        return Err(QsdbError::type_err(format!(
            "LIKE requires string operand, got {}",
            inner.data_type()
        )));
    }
    let pat_s = match pat {
        PhysExpr::Lit(Literal::Utf8(s)) => s,
        _ => return Err(QsdbError::invalid("LIKE pattern must be a string literal")),
    };
    Ok(PhysExpr::Like {
        e: Box::new(inner),
        pattern: crate::expr::LikePattern::compile(&pat_s, esc, ci)?,
        negated,
        ci,
    })
}

/// 已绑定表达式的比较（CASE 简单形态 / nullif 用）：对齐逻辑同 bind_cmp。
fn bind_cmp_phys(op: CmpOp, lb: PhysExpr, rb: PhysExpr) -> Result<PhysExpr> {
    // NULL 比较短路（x = NULL 恒 UNKNOWN → 条件不取）
    if matches!(lb, PhysExpr::Lit(Literal::Null)) || matches!(rb, PhysExpr::Lit(Literal::Null)) {
        return Ok(PhysExpr::Lit(Literal::Null));
    }
    let (lb, rb) = promote_temporal_lit(lb, rb)?;
    let lt = lb.data_type();
    let rt = rb.data_type();
    let common = cmp_common_type(&lt, &rt)?;
    let lb = if lt == common {
        lb
    } else {
        lb.cast_to(common.clone())
    };
    let rb = if rt == common {
        rb
    } else {
        rb.cast_to(common.clone())
    };
    Ok(PhysExpr::Cmp {
        op,
        l: Box::new(lb),
        r: Box::new(rb),
    })
}

/// CASE/COALESCE 分支公共超类型：数值塔向宽处靠、字符串收编标量、
/// 其余不可合并时取左值（cast 层裁决是否可行）。
fn common_supertype(a: &LogicalType, b: &LogicalType) -> LogicalType {
    use LogicalType as Lt;
    if a == b {
        return a.clone();
    }
    if a.is_string() || b.is_string() {
        return Lt::Varchar { max_chars: None };
    }
    if a.is_numeric() && b.is_numeric() {
        if matches!(a, Lt::Float64) || matches!(b, Lt::Float64) {
            return Lt::Float64;
        }
        if matches!(a, Lt::Float32) || matches!(b, Lt::Float32) {
            return Lt::Float64;
        }
        match (a, b) {
            // 双 decimal：取 scale 大者（精度不丢）
            (Lt::Decimal(x), Lt::Decimal(y)) => {
                Lt::Decimal(if y.scale > x.scale { *y } else { *x })
            }
            // decimal 与整数：decimal 承接
            (Lt::Decimal(d), x) | (x, Lt::Decimal(d)) if x.is_integer() => Lt::Decimal(*d),
            _ => wider_int(a, b),
        }
    } else {
        a.clone()
    }
}

fn bind_binary_arith(op: ArithOp, l: PhysExpr, r: PhysExpr) -> Result<PhysExpr> {
    let lt = l.data_type();
    let rt = r.data_type();
    // ── 日期算术：DATE ± INTEGER（天）/ DATE − DATE（天数）──
    if matches!(lt, Lt::Date) || matches!(rt, Lt::Date) {
        match (op, &lt, &rt) {
            (ArithOp::Sub, Lt::Date, Lt::Date) => {
                return Ok(PhysExpr::Arith {
                    op,
                    l: Box::new(l),
                    r: Box::new(r),
                    ty: Lt::Int64,
                });
            }
            // Add 双侧可换；Sub 仅 Date−Int（int−Date 无日历语义）
            (ArithOp::Add, Lt::Date, x)
            | (ArithOp::Add, x, Lt::Date)
            | (ArithOp::Sub, Lt::Date, x)
                if x.is_integer() =>
            {
                // int 侧归一 Int64，Date 侧原样（保谓词下推）
                let lc = if matches!(lt, Lt::Date) {
                    l
                } else {
                    l.cast_to(Lt::Int64)
                };
                let rc = if matches!(rt, Lt::Date) {
                    r
                } else {
                    r.cast_to(Lt::Int64)
                };
                return Ok(PhysExpr::Arith {
                    op,
                    l: Box::new(lc),
                    r: Box::new(rc),
                    ty: Lt::Date,
                });
            }
            _ => {
                return Err(QsdbError::type_err(format!(
                    "date arithmetic on {lt} and {rt} not supported (DATE ± INTEGER 天 / DATE - DATE 天数)"
                )));
            }
        }
    }
    if !lt.is_numeric() || !rt.is_numeric() {
        return Err(QsdbError::type_err(format!(
            "arithmetic on {} and {} not supported",
            lt, rt
        )));
    }
    use LogicalType as Lt;
    // 结果类型与两侧对齐
    let (result, lc, rc) = match (&lt, &rt) {
        // 任一 NULL literal → 结果按另一侧
        _ if matches!(l, PhysExpr::Lit(Literal::Null))
            || matches!(r, PhysExpr::Lit(Literal::Null)) =>
        {
            let t = if matches!(l, PhysExpr::Lit(Literal::Null)) {
                rt.clone()
            } else {
                lt.clone()
            };
            (t.clone(), l.cast_to(t.clone()), r.cast_to(t))
        }
        (Lt::Float64, _) | (_, Lt::Float64) => {
            (Lt::Float64, l.cast_to(Lt::Float64), r.cast_to(Lt::Float64))
        }
        (Lt::Float32, x) if x.is_integer() || matches!(x, Lt::Float32) => {
            (Lt::Float64, l.cast_to(Lt::Float64), r.cast_to(Lt::Float64))
        }
        (x, Lt::Float32) if x.is_integer() => {
            (Lt::Float64, l.cast_to(Lt::Float64), r.cast_to(Lt::Float64))
        }
        (Lt::Decimal(a), Lt::Decimal(b)) => {
            let m = match op {
                ArithOp::Add | ArithOp::Sub => DecimalMeta::add_result(*a, *b),
                ArithOp::Mul => DecimalMeta::mul_result(*a, *b),
                ArithOp::Div => DecimalMeta::div_result(*a, *b),
                ArithOp::Mod => DecimalMeta::mod_result(*a, *b),
            }?;
            (Lt::Decimal(m), l, r) // decimal 路径 eval 内部对齐 scale
        }
        (Lt::Decimal(a), x) | (x, Lt::Decimal(a)) if x.is_integer() => {
            let b = DecimalMeta::try_new(19, 0)?;
            let m = match op {
                ArithOp::Add | ArithOp::Sub => DecimalMeta::add_result(*a, b),
                ArithOp::Mul => DecimalMeta::mul_result(*a, b),
                ArithOp::Div => DecimalMeta::div_result(*a, b),
                ArithOp::Mod => DecimalMeta::mod_result(*a, b),
            }?;
            let lc = l.cast_to(Lt::Decimal(m));
            let rc = r.cast_to(Lt::Decimal(m));
            (Lt::Decimal(m), lc, rc)
        }
        (Lt::Decimal(_), Lt::Float32) | (Lt::Float32, Lt::Decimal(_)) => {
            (Lt::Float64, l.cast_to(Lt::Float64), r.cast_to(Lt::Float64))
        }
        // 整数族：取宽
        _ => {
            let t = wider_int(&lt, &rt);
            (t.clone(), l.cast_to(t.clone()), r.cast_to(t))
        }
    };
    Ok(PhysExpr::Arith {
        op,
        l: Box::new(lc),
        r: Box::new(rc),
        ty: result,
    })
}

fn wider_int(a: &LogicalType, b: &LogicalType) -> LogicalType {
    use LogicalType as Lt;
    let rank = |t: &LogicalType| match t {
        Lt::Int8 => 1,
        Lt::Int16 => 2,
        Lt::Int32 => 3,
        _ => 4,
    };
    if rank(a) >= rank(b) {
        a.clone()
    } else {
        b.clone()
    }
}

// -- 比较绑定 --------------------------------------------------------------

fn bind_cmp(
    op: CmpOp,
    l: &sqlparser::ast::Expr,
    r: &sqlparser::ast::Expr,
    ctx: &SchemaCtx,
) -> Result<PhysExpr> {
    let lb = bind_expr(l, ctx)?;
    let rb = bind_expr(r, ctx)?;
    // NULL 比较短路（Kleene 语义由 null boolean 常量承载）
    if matches!(lb, PhysExpr::Lit(Literal::Null)) || matches!(rb, PhysExpr::Lit(Literal::Null)) {
        return Ok(PhysExpr::Lit(Literal::Null));
    }
    // F16：temporal 列 × 字符串字面量 → 绑定期字面量提升（白名单隐式转换）。
    // 谓词保持 typed → min/max 裁剪与谓词下推不受影响；解析失败给出字面量指引。
    let (lb, rb) = promote_temporal_lit(lb, rb)?;
    let lt = lb.data_type();
    let rt = rb.data_type();
    let common = cmp_common_type(&lt, &rt)?;
    // 常量侧优先 cast（保留 Column 原样，便于谓词下推）
    let lb = if lt == common {
        lb
    } else if is_lit(&lb) {
        cast_lit_expr(lb, &common)?
    } else {
        lb.cast_to(common.clone())
    };
    let rb = if rt == common {
        rb
    } else if is_lit(&rb) {
        cast_lit_expr(rb, &common)?
    } else {
        rb.cast_to(common.clone())
    };
    Ok(PhysExpr::Cmp {
        op,
        l: Box::new(lb),
        r: Box::new(rb),
    })
}

fn is_lit(e: &PhysExpr) -> bool {
    matches!(e, PhysExpr::Lit(_))
}

/// 一侧 temporal、另一侧字符串**字面量** → 按目标类型解析提升
///（字符串列 × temporal 仍显式报错——诚实语义，仅白名单字面量）。
fn promote_temporal_lit(lb: PhysExpr, rb: PhysExpr) -> Result<(PhysExpr, PhysExpr)> {
    let temporal = |t: &LogicalType| {
        matches!(
            t,
            LogicalType::Date | LogicalType::Timestamp { .. } | LogicalType::Time
        )
    };
    match (&lb, &rb) {
        (a, PhysExpr::Lit(Literal::Utf8(s))) if temporal(&a.data_type()) => {
            let lit = parse_temporal_literal(s, &a.data_type())?;
            Ok((lb, PhysExpr::Lit(lit)))
        }
        (PhysExpr::Lit(Literal::Utf8(s)), b) if temporal(&b.data_type()) => {
            let lit = parse_temporal_literal(s, &b.data_type())?;
            Ok((PhysExpr::Lit(lit), rb))
        }
        _ => Ok((lb, rb)),
    }
}

/// 字符串字面量 → temporal 字面量（失败给 `DATE '...'` 形态指引）。
fn parse_temporal_literal(s: &str, ty: &LogicalType) -> Result<Literal> {
    use crate::types::temporal;
    let hint = |kind: &str| {
        QsdbError::parse_err(format!(
            "cannot parse '{s}' as {kind} (use {kind} '{s}' literal)"
        ))
    };
    Ok(match ty {
        LogicalType::Date => Literal::Date(temporal::parse_date(s).map_err(|_| hint("DATE"))?),
        LogicalType::Timestamp { .. } => {
            Literal::Timestamp(temporal::parse_timestamp_ns(s).map_err(|_| hint("TIMESTAMP"))?)
        }
        LogicalType::Time => Literal::Time(temporal::parse_time_ns(s).map_err(|_| hint("TIME"))?),
        _ => unreachable!("caller 已过滤 temporal"),
    })
}

fn cast_lit_expr(e: PhysExpr, ty: &LogicalType) -> Result<PhysExpr> {
    if let PhysExpr::Lit(l) = e {
        if l.is_null() {
            return Ok(PhysExpr::Lit(l));
        }
        return Ok(PhysExpr::Lit(cast_literal(&l, ty)?));
    }
    Ok(e.cast_to(ty.clone()))
}

fn cmp_common_type(a: &LogicalType, b: &LogicalType) -> Result<LogicalType> {
    use LogicalType as Lt;
    if a == b {
        return Ok(a.clone());
    }
    match (a, b) {
        // 数值塔（decimal 混 int → 公共 decimal）
        (x, y) if x.is_numeric() && y.is_numeric() => {
            if x.is_floating() || y.is_floating() {
                Ok(Lt::Float64)
            } else if let (Lt::Decimal(m1), Lt::Decimal(m2)) = (x, y) {
                let s = m1.scale.max(m2.scale);
                let ip = (i16::from(m1.precision) - i16::from(m1.scale))
                    .max(i16::from(m2.precision) - i16::from(m2.scale));
                let p = ip + i16::from(s);
                if p > 38 {
                    Ok(Lt::Float64)
                } else {
                    Ok(Lt::Decimal(DecimalMeta::try_new(p as u8, s)?))
                }
            } else if x.is_integer() && matches!((x, y), (Lt::Decimal(_), _) | (_, Lt::Decimal(_)))
            {
                let (m, x) = if let Lt::Decimal(m) = x {
                    (*m, y)
                } else if let Lt::Decimal(m) = y {
                    (*m, x)
                } else {
                    unreachable!()
                };
                let xi = int_meta(x);
                let s = m.scale;
                let ip = (i16::from(m.precision) - i16::from(m.scale)).max(i16::from(xi.precision));
                let p = ip + i16::from(s);
                if p > 38 {
                    Ok(Lt::Float64)
                } else {
                    Ok(Lt::Decimal(DecimalMeta::try_new(p as u8, s)?))
                }
            } else {
                // int 混合 → 宽 int
                Ok(wider_int(x, y))
            }
        }
        (Lt::Date, Lt::Timestamp { .. }) | (Lt::Timestamp { .. }, Lt::Date) => {
            Ok(Lt::Timestamp { timezone: None })
        }
        (x, y) if x.is_string() && y.is_string() => Ok(Lt::Varchar { max_chars: None }),
        _ => Err(QsdbError::type_err(format!("cannot compare {a} with {b}"))),
    }
}

fn int_meta(t: &LogicalType) -> DecimalMeta {
    match t {
        LogicalType::Int8 => DecimalMeta::new(3, 0),
        LogicalType::Int16 => DecimalMeta::new(5, 0),
        LogicalType::Int32 => DecimalMeta::new(10, 0),
        _ => DecimalMeta::new(19, 0),
    }
}

// -- 函数绑定 --------------------------------------------------------------

/// 函数参数列表（0.63：FunctionArguments::List(..)）。
fn fn_args(f: &sqlparser::ast::Function) -> &[sqlparser::ast::FunctionArg] {
    match &f.args {
        sqlparser::ast::FunctionArguments::List(list) => &list.args,
        _ => &[],
    }
}

/// 函数参数列表里写了 `DISTINCT`（`count(DISTINCT x)` 这类）。
///
/// **单独成函数是为了"让这件事可见"**：此前绑定聚合时压根没读这个字段，
/// 于是 `DISTINCT` 被静默丢弃成 `count(x)`（见 `try_bind_agg` 的说明）。
/// 任何新加的聚合绑定路径都该先问一句 `has_duplicate_treatment`。
fn has_duplicate_treatment(f: &sqlparser::ast::Function) -> bool {
    matches!(
        &f.args,
        sqlparser::ast::FunctionArguments::List(list)
            if matches!(
                list.duplicate_treatment,
                Some(sqlparser::ast::DuplicateTreatment::Distinct)
            )
    )
}

/// 函数参数解包为表达式（命名参数/通配符不参与普通函数求值）。
fn arg_exprs(f: &sqlparser::ast::Function) -> Result<Vec<&sqlparser::ast::Expr>> {
    fn_args(f)
        .iter()
        .map(|a| match a {
            sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(e)) => Ok(e),
            _ => Err(QsdbError::invalid("unsupported function argument form")),
        })
        .collect()
}

fn fn_name(f: &sqlparser::ast::Function) -> String {
    f.name.to_string().to_ascii_lowercase()
}

/// 聚合函数名。
fn is_agg_name(name: &str) -> bool {
    matches!(name, "count" | "sum" | "min" | "max" | "avg")
}

/// UDAF 聚合绑定：单参数表达式，输出类型由 ret_ty 决定。
fn try_bind_udaf(
    f: &sqlparser::ast::Function,
    name: &str,
    def: std::sync::Arc<crate::expr::udaf::UdafDef>,
    ctx: &SchemaCtx,
) -> Result<AggExpr> {
    let arg = match fn_args(f) {
        [sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(e))] => {
            bind_expr(e, ctx)?
        }
        _ => return Err(QsdbError::invalid(format!("{name}(expr) expected (UDAF)"))),
    };
    let out_name = format!("{name}({})", arg.display());
    AggExpr::new_udaf(def, arg, out_name)
}

fn try_bind_agg(f: &sqlparser::ast::Function, ctx: &SchemaCtx) -> Result<Option<AggExpr>> {
    // 聚合**不经 `bind_expr` 的 Function 臂**（走 `collect_aggs` → 这里），
    // 所以函数子句闸门要在这条路径上也挂一次——`count(*) FILTER (…)` 就是从这里漏的。
    audit_function(f)?;
    let name = fn_name(f);
    if !is_agg_name(&name) {
        // UDAF 注册表（窗口形态的 UDAF 后置）
        if f.over.is_none() {
            if let Some(def) = crate::expr::udaf::lookup(&name) {
                return try_bind_udaf(f, &name, def, ctx).map(Some);
            }
        }
        return Ok(None);
    }
    if f.over.is_some() {
        return Ok(None); // 窗口函数不作为聚合处理（由 collect_window_calls / 投影置换接管）
    }
    // **聚合参数上的 `DISTINCT`：显式拒绝，绝不静默丢弃**。
    //
    // 此前这里从不读 `duplicate_treatment`，于是 `count(DISTINCT a)` 被当成
    // `count(a)` 执行——回的是**错的数字**（`{1,1,2}` 上回 3 而非 2），
    // 而且是"完全合法的 SQL、悄悄给错值"这一类。宁可报错。
    //
    // 等价写法（**今天就能用**，`SELECT DISTINCT` 的去重是对的）：
    //   `SELECT count(DISTINCT a) FROM t`
    // → `SELECT count(*) FROM (SELECT DISTINCT a FROM t)`
    // 带分组：`SELECT g, count(DISTINCT a) FROM t GROUP BY g`
    // → `SELECT g, count(*) FROM (SELECT DISTINCT g, a FROM t) GROUP BY g`
    // 原生实现（真值去重、可与其它聚合混用、走溢出）在路线图上另立。
    if has_duplicate_treatment(f) {
        return Err(QsdbError::unsupported(format!(
            "{name}(DISTINCT …) is not implemented yet — it used to be silently executed as \
             {name}(…), returning a WRONG number, so it is now rejected outright. \
             Equivalent query: `SELECT count(*) FROM (SELECT DISTINCT … FROM …)`"
        )));
    }
    let func = match name.as_str() {
        "count" => AggFunc::Count,
        "sum" => AggFunc::Sum,
        "min" => AggFunc::Min,
        "max" => AggFunc::Max,
        "avg" => AggFunc::Avg,
        _ => unreachable!(),
    };
    let args = fn_args(f);
    let arg = match args.len() {
        1 => match &args[0] {
            sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Wildcard) => None,
            sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(e)) => {
                Some(bind_expr(e, ctx)?)
            }
            other => {
                return Err(QsdbError::unsupported(format!(
                    "aggregate arg {other:?} not supported"
                )));
            }
        },
        0 => None,
        _ => {
            return Err(QsdbError::invalid(format!(
                "{name} takes exactly one argument"
            )));
        }
    };
    if arg.is_none() && func != AggFunc::Count {
        return Err(QsdbError::invalid(format!("{name}(*) is not valid")));
    }
    let out_name = format!(
        "{}({})",
        name,
        arg.as_ref()
            .map(|a| a.display())
            .unwrap_or_else(|| "*".into())
    );
    Ok(Some(AggExpr::new(func, arg, out_name)?))
}

/// 收集 SELECT item 里的全部聚合调用（按发现顺序去重）。
fn collect_aggs(e: &sqlparser::ast::Expr, ctx: &SchemaCtx, aggs: &mut Vec<AggExpr>) -> Result<()> {
    use sqlparser::ast::Expr as E;
    match e {
        E::Function(f) => {
            if let Some(agg) = try_bind_agg(f, ctx)? {
                if !aggs.iter().any(|a| a.out_name == agg.out_name) {
                    aggs.push(agg);
                }
                return Ok(());
            }
            for a in fn_args(f) {
                if let sqlparser::ast::FunctionArg::Unnamed(
                    sqlparser::ast::FunctionArgExpr::Expr(inner),
                ) = a
                {
                    collect_aggs(inner, ctx, aggs)?;
                }
            }
            // OVER spec 内的聚合引用（rank() OVER (ORDER BY count(*))）
            if let Some(sqlparser::ast::WindowType::WindowSpec(spec)) = &f.over {
                for p in &spec.partition_by {
                    collect_aggs(p, ctx, aggs)?;
                }
                for ob in &spec.order_by {
                    collect_aggs(&ob.expr, ctx, aggs)?;
                }
            }
            Ok(())
        }
        E::BinaryOp { left, right, .. } => {
            collect_aggs(left, ctx, aggs)?;
            collect_aggs(right, ctx, aggs)
        }
        E::Nested(inner) => collect_aggs(inner, ctx, aggs),
        E::Cast { expr, .. } => collect_aggs(expr, ctx, aggs),
        E::UnaryOp { expr, .. } => collect_aggs(expr, ctx, aggs),
        E::IsNull(inner) | E::IsNotNull(inner) => collect_aggs(inner, ctx, aggs),
        E::Floor { expr, .. } | E::Ceil { expr, .. } => collect_aggs(expr, ctx, aggs),
        E::Between {
            expr, low, high, ..
        } => {
            collect_aggs(expr, ctx, aggs)?;
            collect_aggs(low, ctx, aggs)?;
            collect_aggs(high, ctx, aggs)
        }
        E::Extract { expr, .. } => collect_aggs(expr, ctx, aggs),
        E::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if let Some(op) = operand {
                collect_aggs(op, ctx, aggs)?;
            }
            for cw in conditions {
                collect_aggs(&cw.condition, ctx, aggs)?;
                collect_aggs(&cw.result, ctx, aggs)?;
            }
            if let Some(e) = else_result {
                collect_aggs(e, ctx, aggs)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

/// 聚合输出上的重绑定：聚合调用 → agg 列；裸列/表达式 → 匹配 group 键。
fn rebind_over_agg(
    e: &sqlparser::ast::Expr,
    ctx: &SchemaCtx,
    group: &[PhysExpr],
    aggs: &[AggExpr],
    agg_ctx: &SchemaCtx,
) -> Result<PhysExpr> {
    use sqlparser::ast::Expr as E;
    match e {
        E::Function(f) => {
            if let Some(agg) = try_bind_agg(f, ctx)? {
                let idx = aggs
                    .iter()
                    .position(|a| a.out_name == agg.out_name)
                    .expect("collected earlier");
                return Ok(PhysExpr::Column {
                    idx: group.len() + idx,
                    name: agg.out_name.clone(),
                    ty: agg.out_ty,
                });
            }
            bind_scalar_fn_over(e, f, ctx, group, aggs, agg_ctx)
        }
        E::Identifier(id) => {
            // 聚合别名优先：agg_ctx 已把别名注册在 group.len() 之后
            if let Some(idx) = agg_ctx
                .cols
                .iter()
                .position(|(n, _)| n.eq_ignore_ascii_case(&id.value))
            {
                if idx >= group.len() {
                    let (name, ty) = &agg_ctx.cols[idx];
                    return Ok(PhysExpr::Column {
                        idx,
                        name: name.clone(),
                        ty: ty.clone(),
                    });
                }
            }
            // 裸列必须出现在 group by（group 为纯列）
            match group
                .iter()
                .position(|g| matches!(g, PhysExpr::Column { name, .. } if name.eq_ignore_ascii_case(&id.value)))
            {
                Some(gi) => Ok(PhysExpr::Column {
                    idx: gi,
                    name: group[gi].display(),
                    ty: group[gi].data_type(),
                }),
                None => Err(QsdbError::invalid(format!(
                    "column '{}' must appear in GROUP BY or inside an aggregate",
                    id.value
                ))),
            }
        }
        // 复合表达式：与 group 表达式 display 匹配
        other => {
            let display = expr_display(other);
            if let Some(gi) = group.iter().position(|g| g.display() == display) {
                return Ok(PhysExpr::Column {
                    idx: gi,
                    name: group[gi].display(),
                    ty: group[gi].data_type(),
                });
            }
            // 递归结构（如 sum(x)+1）
            match other {
                E::BinaryOp { left, op, right } => {
                    use sqlparser::ast::BinaryOperator as B;
                    let l = rebind_over_agg(left, ctx, group, aggs, agg_ctx)?;
                    let r = rebind_over_agg(right, ctx, group, aggs, agg_ctx)?;
                    match op {
                        B::Plus => bind_binary_arith(ArithOp::Add, l, r),
                        B::Minus => bind_binary_arith(ArithOp::Sub, l, r),
                        B::Multiply => bind_binary_arith(ArithOp::Mul, l, r),
                        B::Divide => bind_binary_arith(ArithOp::Div, l, r),
                        B::Modulo => bind_binary_arith(ArithOp::Mod, l, r),
                        B::Eq | B::NotEq | B::Lt | B::LtEq | B::Gt | B::GtEq => {
                            // 直接构造（两侧已对齐聚合输出类型）
                            let cop = match op {
                                B::Eq => CmpOp::Eq,
                                B::NotEq => CmpOp::NotEq,
                                B::Lt => CmpOp::Lt,
                                B::LtEq => CmpOp::LtEq,
                                B::Gt => CmpOp::Gt,
                                _ => CmpOp::GtEq,
                            };
                            Ok(PhysExpr::Cmp {
                                op: cop,
                                l: Box::new(l),
                                r: Box::new(r),
                            })
                        }
                        B::And => Ok(PhysExpr::And(Box::new(l), Box::new(r))),
                        B::Or => Ok(PhysExpr::Or(Box::new(l), Box::new(r))),
                        x => Err(QsdbError::unsupported(format!("operator {x:?}"))),
                    }
                }
                E::Nested(inner) => rebind_over_agg(inner, ctx, group, aggs, agg_ctx),
                E::Case {
                    operand,
                    conditions,
                    else_result,
                    ..
                } => {
                    // 简单形态 CASE x WHEN v → x = v；与 bind_expr 臂同构
                    let subject = match operand {
                        Some(op) => Some(rebind_over_agg(op, ctx, group, aggs, agg_ctx)?),
                        None => None,
                    };
                    let mut out = Vec::with_capacity(conditions.len());
                    for cw in conditions {
                        let cond = match &subject {
                            Some(s) => bind_cmp_phys(
                                CmpOp::Eq,
                                s.clone(),
                                rebind_over_agg(&cw.condition, ctx, group, aggs, agg_ctx)?,
                            )?,
                            None => {
                                let c = rebind_over_agg(&cw.condition, ctx, group, aggs, agg_ctx)?;
                                if !c.is_boolean() {
                                    return Err(QsdbError::type_err(
                                        "CASE WHEN condition must be boolean",
                                    ));
                                }
                                c
                            }
                        };
                        out.push((
                            cond,
                            rebind_over_agg(&cw.result, ctx, group, aggs, agg_ctx)?,
                        ));
                    }
                    let else_expr = match else_result {
                        Some(e) => rebind_over_agg(e, ctx, group, aggs, agg_ctx)?,
                        None => PhysExpr::Lit(Literal::Null),
                    };
                    finish_case(out, else_expr)
                }
                E::Like {
                    negated,
                    any,
                    expr,
                    pattern,
                    escape_char,
                } if !any => {
                    let inner = rebind_over_agg(expr, ctx, group, aggs, agg_ctx)?;
                    let pat = rebind_over_agg(pattern, ctx, group, aggs, agg_ctx)?;
                    let esc = escape_char_literal(escape_char.as_deref(), ctx)?;
                    like_from_phys(inner, pat, esc, *negated, false)
                }
                E::ILike {
                    negated,
                    any,
                    expr,
                    pattern,
                    escape_char,
                } if !any => {
                    let inner = rebind_over_agg(expr, ctx, group, aggs, agg_ctx)?;
                    let pat = rebind_over_agg(pattern, ctx, group, aggs, agg_ctx)?;
                    let esc = escape_char_literal(escape_char.as_deref(), ctx)?;
                    like_from_phys(inner, pat, esc, *negated, true)
                }
                E::Floor { expr, .. } | E::Ceil { expr, .. } => {
                    let fname = match other {
                        E::Floor { .. } => "floor",
                        _ => "ceil",
                    };
                    let inner = rebind_over_agg(expr, ctx, group, aggs, agg_ctx)?;
                    let def = scalar::lookup(fname)?;
                    let ty = (def.ret_ty)(&[inner.data_type()])?;
                    Ok(PhysExpr::Fn {
                        name: fname.to_string(),
                        args: vec![inner],
                        ty,
                    })
                }
                E::Between {
                    expr,
                    negated,
                    low,
                    high,
                } => {
                    // 与 bind_expr 的 Between 臂同构（x >= lo AND x <= hi）
                    let x = rebind_over_agg(expr, ctx, group, aggs, agg_ctx)?;
                    let ge = bind_cmp_phys(
                        CmpOp::GtEq,
                        x.clone(),
                        rebind_over_agg(low, ctx, group, aggs, agg_ctx)?,
                    )?;
                    let le = bind_cmp_phys(
                        CmpOp::LtEq,
                        x,
                        rebind_over_agg(high, ctx, group, aggs, agg_ctx)?,
                    )?;
                    let both = PhysExpr::And(Box::new(ge), Box::new(le));
                    Ok(if *negated {
                        PhysExpr::Not(Box::new(both))
                    } else {
                        both
                    })
                }
                E::Cast {
                    expr, data_type, ..
                } => {
                    let inner = rebind_over_agg(expr, ctx, group, aggs, agg_ctx)?;
                    Ok(PhysExpr::Cast {
                        e: Box::new(inner),
                        ty: cast_target_to_logical(data_type)?,
                    })
                }
                E::Value(v) => Ok(PhysExpr::Lit(value_to_literal(v)?)),
                x => Err(QsdbError::unsupported(format!(
                    "expression {x:?} over aggregates not supported"
                ))),
            }
        }
    }
}

fn bind_scalar_fn(f: &sqlparser::ast::Function, ctx: &SchemaCtx) -> Result<PhysExpr> {
    let name = fn_name(f);
    let fargs = fn_args(f);
    let mut args = Vec::with_capacity(fargs.len());
    for a in fargs {
        match a {
            sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(e)) => {
                args.push(bind_expr(e, ctx)?)
            }
            other => {
                return Err(QsdbError::unsupported(format!(
                    "function arg {other:?} not supported"
                )));
            }
        }
    }
    finish_scalar_fn(name, args)
}

/// 标量函数绑定尾段：ret_ty 校验 + 隐式 cast（int→Int64 / float→Float64）
/// + Fn 构造。普通路径与聚合上路（bind_scalar_fn_over）共用。
fn finish_scalar_fn(name: String, args: Vec<PhysExpr>) -> Result<PhysExpr> {
    let def = scalar::lookup(&name)?;
    let arg_tys: Vec<LogicalType> = args.iter().map(|a| a.data_type()).collect();
    let ty = (def.ret_ty)(&arg_tys)?;
    let args = args
        .into_iter()
        .enumerate()
        .map(|(i, a)| {
            let target = match &arg_tys[i] {
                LogicalType::Int8
                | LogicalType::Int16
                | LogicalType::Int32
                | LogicalType::Int64 => LogicalType::Int64,
                LogicalType::Float32 | LogicalType::Float64 => LogicalType::Float64,
                t => t.clone(),
            };
            a.cast_to(target)
        })
        .collect::<Vec<_>>();
    Ok(PhysExpr::Fn { name, args, ty })
}

/// 聚合输出上的标量函数（round(sum(x)) / upper(tenant) 聚合查询形态）：
/// 参数递归走 rebind_over_agg（聚合调用→agg 列引用），尾段与普通路径共用。
fn bind_scalar_fn_over(
    _e: &sqlparser::ast::Expr,
    f: &sqlparser::ast::Function,
    ctx: &SchemaCtx,
    group: &[PhysExpr],
    _aggs: &[AggExpr],
    agg_ctx: &SchemaCtx,
) -> Result<PhysExpr> {
    let name = fn_name(f);
    let fargs = fn_args(f);
    let mut args = Vec::with_capacity(fargs.len());
    for a in fargs {
        match a {
            sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(e)) => {
                args.push(rebind_over_agg(e, ctx, group, _aggs, agg_ctx)?)
            }
            other => {
                return Err(QsdbError::unsupported(format!(
                    "function arg {other:?} not supported"
                )));
            }
        }
    }
    finish_scalar_fn(name, args)
}

// ---------------------------------------------------------------------------
// 谓词下推拆分（PhysExpr → M1 Predicate + 残余）
// ---------------------------------------------------------------------------

pub(crate) fn split_pushdown(expr: &PhysExpr) -> (Option<Predicate>, Option<PhysExpr>) {
    match expr {
        PhysExpr::Cmp { op, l, r } => {
            let (col, val, flipped) = match (l.as_ref(), r.as_ref()) {
                (PhysExpr::Column { .. }, PhysExpr::Lit(v)) => (l, v, false),
                (PhysExpr::Lit(v), PhysExpr::Column { .. }) => (r, v, true),
                _ => return (None, Some(expr.clone())),
            };
            if val.is_null() {
                return (None, Some(expr.clone()));
            }
            let name = match col.as_ref() {
                PhysExpr::Column { name, .. } => name.clone(),
                _ => unreachable!(),
            };
            let _ = flipped; // 翻转在 op 映射时处理
            let p = match op {
                CmpOp::Eq => Predicate::eq(name, val.clone()),
                CmpOp::NotEq => Predicate::not_eq(name, val.clone()),
                CmpOp::Lt => Predicate::lt(name, val.clone()),
                CmpOp::LtEq => Predicate::lt_eq(name, val.clone()),
                CmpOp::Gt => Predicate::gt(name, val.clone()),
                CmpOp::GtEq => Predicate::gt_eq(name, val.clone()),
            };
            (Some(p), None)
        }
        PhysExpr::And(l, r) => {
            let (pl, rl) = split_pushdown(l);
            let (pr, rr) = split_pushdown(r);
            let l_ok = pl.is_some();
            let r_ok = pr.is_some();
            let p = match (pl, pr) {
                (Some(a), Some(b)) => Some(Predicate::and(a, b)),
                (Some(a), None) | (None, Some(a)) => Some(a),
                (None, None) => None,
            };
            // 不可下推的侧留在行过滤
            let l_keep = (!l_ok).then(|| (**l).clone());
            let r_keep = (!r_ok).then(|| (**r).clone());
            let residual = match (rl.or(l_keep), rr.or(r_keep)) {
                (Some(a), Some(b)) => Some(PhysExpr::And(Box::new(a), Box::new(b))),
                (Some(a), None) | (None, Some(a)) => Some(a),
                (None, None) => None,
            };
            (p, residual)
        }
        _ => (None, Some(expr.clone())),
    }
}
