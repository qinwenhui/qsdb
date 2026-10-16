//! AST → LogicalPlan：类型映射、字面量、表达式绑定（binder）、谓词下推拆分。

use std::sync::Arc;

use sqlparser::ast::{CopySource, CopyTarget, SetExpr, Statement as SqlStatement};
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
        /// **键之外的 ON 条件**（对**已匹配的对**再判；`None` = 没有）。
        ///
        /// 例：`LEFT JOIN r ON l.k = r.k AND r.v > 250` —— 键是 `k`，
        /// 残留是 `r.v > 250`。在**合并 schema**（左 ++ 右）上求值。
        ///
        /// ⚠️ **必须在 join 内部求值，不能提到 `WHERE`**：LEFT 的语义是
        /// "判否 ⇒ 丢弃这个**对**"，未匹配的左行仍要**补 NULL 保留**；
        /// 提到 WHERE 会把那些行**整体丢掉**（实测口径：3 行变 1 行，**静默少行**）。
        /// INNER 上两者等价，但为了一条语义路径，这里同样走 join 内部。
        ///
        /// ⚠️ 它一旦存在，两处优化要**避开**（否则读出 NULL / 少数行）：
        /// `annotate_join_needs` 的列掩码、`JoinCountExec` 的 `count(*)` 下推。
        /// 见各自的注释。
        residual: Option<PhysExpr>,
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
    // `FILTER (WHERE …)` **聚合已实现**（2026-10-15，生态功能批次 3）：谓词只有
    // TRUE 才把行喂给该聚合。这里只拦**标量函数上的 FILTER**（没有意义）。
    // ⚠️ 窗口函数**不走 `try_bind_agg`**，所以窗口 + FILTER 要在
    // `parse_window_fn` 里单独拦 —— 否则它会被静默丢掉（回全量，正是本批要消灭的）。
    if f.filter.is_some() && !is_agg_name(&fn_name(f)) {
        return reject("FILTER (WHERE …) on a scalar function");
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
/// （累计），结果**换个数字、不报错**。现在 `ROWS` 已实现（走滑窗路径），
/// `RANGE`/`GROUPS` 仍**显式拒**。
fn audit_window_spec(spec: &sqlparser::ast::WindowSpec) -> Result<()> {
    if let Some(fr) = &spec.window_frame {
        audit_window_frame(fr)?;
    }
    audit_order_by(&sqlparser::ast::OrderByKind::Expressions(
        spec.order_by.clone(),
    ))
}

/// 帧闸门（**单位**这一层）：`ROWS` 放行（`parse_window_fn_frame` 会把边界解析成
/// [`crate::exec::window::RowFrame`] 交给执行器），其余单位**显式拒**。
///
/// `RANGE` / `GROUPS` 必须拒：按**值**分帧要 `ORDER BY` 恰好一列 + 值域语义，
/// 与 ROWS 是两回事 —— 当 ROWS 跑会**换个数字**（本批只实现 ROWS）。
fn audit_window_frame(fr: &sqlparser::ast::WindowFrame) -> Result<()> {
    use sqlparser::ast::WindowFrameUnits as U;
    match fr.units {
        U::Rows => Ok(()),
        U::Range => Err(QsdbError::unsupported(
            "window frame RANGE BETWEEN … (only ROWS is implemented) — RANGE 按「值」\
             分帧（要 ORDER BY 恰好一列 + 值域语义），按 ROWS 跑会给出不同数字",
        )),
        U::Groups => Err(QsdbError::unsupported(
            "window frame GROUPS BETWEEN … (only ROWS is implemented)",
        )),
    }
}

/// 起点是否不在终点之后（帧非空）。
///
/// 起点侧的 `Unbounded` = −∞、终点侧的 = +∞，故**无界一律放行**；两侧都有界时
/// 才比相对次序（`5 PRECEDING` 在 `3 PRECEDING` 之前 ✓，写反了 ✗）。
fn frame_bounds_order_ok(
    start: crate::exec::window::FrameBound,
    end: crate::exec::window::FrameBound,
) -> bool {
    use crate::exec::window::FrameBound as B;
    let ord = |b: B| -> i128 {
        match b {
            B::Unbounded => 0, // 两侧都在上面拦掉了，这里取不到
            B::Preceding(n) => -(n as i128),
            B::Current => 0,
            B::Following(n) => n as i128,
        }
    };
    if matches!(start, B::Unbounded) || matches!(end, B::Unbounded) {
        return true;
    }
    ord(start) <= ord(end)
}

/// 解析 `ROWS BETWEEN … AND …` 的两个边界（非法组合在 [`parse_window_fn_frame`] 判，
/// 单位在 [`audit_window_frame`] 判）。
fn parse_frame_bounds(
    fr: &sqlparser::ast::WindowFrame,
) -> Result<(
    crate::exec::window::FrameBound,
    crate::exec::window::FrameBound,
)> {
    use crate::exec::window::FrameBound as B;
    let start = match &fr.start_bound {
        sqlparser::ast::WindowFrameBound::CurrentRow => B::Current,
        sqlparser::ast::WindowFrameBound::Preceding(None) => B::Unbounded,
        sqlparser::ast::WindowFrameBound::Preceding(Some(e)) => B::Preceding(frame_offset(e)?),
        sqlparser::ast::WindowFrameBound::Following(None) => {
            return Err(QsdbError::unsupported(
                "window frame `UNBOUNDED FOLLOWING` as the start bound (standard forbids it)",
            ));
        }
        sqlparser::ast::WindowFrameBound::Following(Some(e)) => B::Following(frame_offset(e)?),
    };
    let end = match fr.end_bound.as_ref() {
        None => B::Current,
        Some(sqlparser::ast::WindowFrameBound::CurrentRow) => B::Current,
        Some(sqlparser::ast::WindowFrameBound::Preceding(None)) => {
            return Err(QsdbError::unsupported(
                "window frame `UNBOUNDED PRECEDING` as the end bound (standard forbids it)",
            ));
        }
        Some(sqlparser::ast::WindowFrameBound::Preceding(Some(e))) => {
            B::Preceding(frame_offset(e)?)
        }
        Some(sqlparser::ast::WindowFrameBound::Following(None)) => B::Unbounded,
        Some(sqlparser::ast::WindowFrameBound::Following(Some(e))) => {
            B::Following(frame_offset(e)?)
        }
    };
    Ok((start, end))
}

/// 帧偏移量：**非负整数字面量**（本仓不做表达式求值兜底；上限收到 `i64::MAX`
/// 以内，执行器按 `i64` 算帧区间）。
fn frame_offset(e: &sqlparser::ast::Expr) -> Result<u64> {
    let v = const_nonneg_int(e, "window frame offset")?;
    if v < 0 {
        return Err(QsdbError::unsupported(
            "window frame offset must be non-negative (write it as PRECEDING/FOLLOWING)",
        ));
    }
    Ok(v as u64)
}

/// 从 AST 里取一个**整数常量**：整数字面量，或**一个参数**（`$n` / `?`）。
///
/// 为什么参数要在这里单独认一次：有两处（**窗口帧偏移**、`lag/lead` 偏移）是在
/// **绑定之前**直接从 AST 上读整数的 —— 不认参数的话，用户写
/// `ROWS BETWEEN $1 PRECEDING` 或 `lag(v, ?)` 会收到"必须是整数字面量"，
/// 而他明明给的就是个参数。
///
/// 安全：`params::lookup` **无副作用**（"用到的最大序号"在 tokenizer 那一步就数完了），
/// 所以就算这两处被走两遍也不影响别处。
fn const_nonneg_int(e: &sqlparser::ast::Expr, what: &str) -> Result<i64> {
    use sqlparser::ast::Value as V;
    let sqlparser::ast::Expr::Value(v) = e else {
        return Err(QsdbError::unsupported(format!(
            "{what} must be an integer literal or a parameter, got `{e}`"
        )));
    };
    match &v.value {
        V::Number(s, _) => s.parse::<i64>().map_err(|_| {
            QsdbError::unsupported(format!("{what} '{s}' is not an integer (or exceeds i64)"))
        }),
        V::Placeholder(p) => {
            let idx = crate::sql::params::placeholder_index(p)?;
            match crate::sql::params::lookup(idx)? {
                Literal::Int64(n) => Ok(n),
                other => Err(QsdbError::type_err(format!(
                    "{what} 的参数必须是整数，实得 {:?}",
                    other.logical_type().map(|t| format!("{t:?}"))
                ))),
            }
        }
        _ => Err(QsdbError::unsupported(format!(
            "{what} must be an integer literal or a parameter, got `{e}`"
        ))),
    }
}

/// 解析一条窗口函数的帧（`None` = 没写帧 ⇒ 默认帧）。
///
/// **排名 / lag / lead 带帧 ⇒ 显式拒**：标准说它们忽略帧，而本仓纪律是
/// "不静默忽略"（静默忽略正是这批要消灭的东西）。
fn parse_window_fn_frame(
    f: &sqlparser::ast::Function,
    name: &str,
) -> Result<Option<crate::exec::window::RowFrame>> {
    let Some(sqlparser::ast::WindowType::WindowSpec(spec)) = &f.over else {
        return Ok(None);
    };
    let Some(fr) = &spec.window_frame else {
        return Ok(None);
    };
    let (start, end) = parse_frame_bounds(fr)?;
    if !frame_bounds_order_ok(start, end) {
        return Err(QsdbError::unsupported(
            "window frame start is after its end (e.g. `3 PRECEDING AND 5 PRECEDING`) — \
             the frame would always be empty",
        ));
    }
    let is_agg = matches!(name, "sum" | "count" | "min" | "max" | "avg");
    if !is_agg {
        return Err(QsdbError::unsupported(format!(
            "window function '{name}' does not take a frame (`ROWS BETWEEN …`) — \
             ranking/lag/lead 按定义只吃分区与排序，帧是给聚合函数用的"
        )));
    }
    // 无 ORDER BY 时行序由扫描决定（可能随并行度变化）⇒ 按行号分帧会给出
    // **不确定**的值。整分区那一种（前后都无界）与行序无关，放行。
    let whole_partition = matches!(start, crate::exec::window::FrameBound::Unbounded)
        && matches!(end, crate::exec::window::FrameBound::Unbounded);
    if spec.order_by.is_empty() && !whole_partition {
        return Err(QsdbError::unsupported(
            "window frame `ROWS BETWEEN …` without ORDER BY — 没有 ORDER BY 就没有确定的\
             行序（并行扫描下会变），按行号分帧算出来的值不可复现；请加 ORDER BY，\
             或改用 `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`（整分区）",
        ));
    }
    Ok(Some(crate::exec::window::RowFrame { start, end }))
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
    /// `CREATE TABLE … AS SELECT …`（CTAS）。
    ///
    /// ⚠️ **列名/列类型的来源分两种，别混**：
    /// - 没写列清单（`CREATE TABLE t AS SELECT …`）⇒ 用 **SELECT 的输出名与类型**；
    ///   输出名可能是**表达式文本**（`count(*)` / `a + b`），那不是合法表列名
    ///   ⇒ 在建表前**显式拒**并指路（见 `sql::mod` 的 CTAS 分支）。
    /// - 写了列清单（`CREATE TABLE t (g BIGINT, n BIGINT) AS SELECT …`）⇒ 名字与
    ///   类型都用**用户写的**，列数必须与 SELECT 一致（sqlparser 要求列清单带类型，
    ///   所以不存在"只给名字"的形态）。
    CreateTableAs {
        name: String,
        columns: Vec<(String, crate::types::LogicalType)>,
        partition_keys: Vec<String>,
        query: Box<sqlparser::ast::Query>,
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
    /// `SHOW CREATE TABLE t` —— 把表结构**反渲染**成可重放的 DDL（备份/迁移用）。
    ///
    /// 放在 SQL 面而不是只放 CLI：这样 HTTP / JDBC 的调用方也能拿到它
    /// （`qsdb dump` 只是它的一个薄壳）。
    ShowCreateTable {
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
    /// `COPY (SELECT …) TO '<目录>' (FORMAT PARQUET [, PARTITION_BY (cols)])`（F9 / A3）。
    ///
    /// 目标是**目录**（不是文件）：导出可能产出多个 `part-*.parquet`
    /// （并行分块写，见 `crate::export`）—— 哪怕没写 `PARTITION_BY` 也一样。
    ///
    /// `partition_by` 是**词法层**抽出来的（`sql::copy_options`；sqlparser 的
    /// `CopyOption` 没有这个变体），列名在这里还没校验 —— 它们必须能在查询输出里
    /// 找到，那条在 `crate::export` 里做（要看到计划）。
    CopyTo {
        query: Box<sqlparser::ast::Query>,
        target: String,
        partition_by: Vec<String>,
        format: crate::export::ExportFormat,
    },
}

pub fn parse_statements(sql: &str) -> Result<Vec<ParsedStatement>> {
    // **`?` 必须先固化**（参数化查询设计 §三）：序号写死在文本里，之后所有
    // "把同一个表达式再走一遍"的路径（HAVING/窗口重绑、`group_key_pos` 试探绑定）
    // 都不会错位。没有 `?` 时是借用，零成本。
    let (numbered, _n_params) = crate::sql::params::normalize_placeholders(sql)?;
    let sql: &str = &numbered;
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
    if upper.starts_with("SHOW CREATE TABLE ") {
        let rest = &trimmed["SHOW CREATE TABLE".len()..].trim_start();
        if !rest.is_empty() && !rest.contains(' ') {
            return Ok(vec![ParsedStatement::ShowCreateTable {
                table: rest.to_ascii_lowercase(),
            }]);
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

    // **`COPY … TO` 的 `PARTITION_BY` 桩**：sqlparser 的 `CopyOption` 没有这个
    // 变体，所以先把它从文本里摘出来、再让 sqlparser 解析剩下的（详见
    // `sql::copy_options` 的模块文档）。没有这个子句时是借用、零成本。
    let (for_parse, copy_pb) = super::copy_options::extract_partition_by(trimmed)?;
    let ast = Parser::parse_sql(&GenericDialect {}, &for_parse)
        .map_err(|e| QsdbError::parse_err(format!("SQL parse error: {e}")))?;
    ast.into_iter()
        .map(|stmt| translate_statement(stmt, trimmed, copy_pb.as_deref()))
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

fn translate_statement(
    stmt: SqlStatement,
    raw: &str,
    copy_partition_by: Option<&[String]>,
) -> Result<ParsedStatement> {
    match stmt {
        // `COPY … TO`（F9 / A3）。**只做导出**，且只做 parquet + hive 目录。
        //
        // 每条不支持的形态都**显式拒并指路** —— 这些正是"语法合法但没实现"的
        // 高发区（`COPY t TO 'x'` / `COPY … FROM` / `FORMAT CSV`）。
        SqlStatement::Copy {
            source,
            to,
            target,
            options,
            legacy_options,
            ..
        } => {
            if !to {
                return Err(QsdbError::unsupported(
                    "COPY … FROM（导入）未实现 —— 导入请用 `INSERT INTO t SELECT … FROM \
                     read_csv('…')`，或走 HTTP `/write` 通道（`bulk`/`rows` 批量装载）",
                ));
            }
            let query = match source {
                CopySource::Query(q) => q,
                CopySource::Table { .. } => {
                    return Err(QsdbError::unsupported(
                        "COPY <表> TO … 未实现 —— 写成 `COPY (SELECT * FROM t) TO '<目录>' \
                         (FORMAT PARQUET)`",
                    ));
                }
            };
            let path = match target {
                CopyTarget::File { filename } => filename,
                CopyTarget::Stdin | CopyTarget::Stdout | CopyTarget::Program { .. } => {
                    return Err(QsdbError::unsupported(format!(
                        "COPY … TO {target} 未实现（只支持文件/目录路径）"
                    )));
                }
            };
            // **格式必须显式写**：DuckDB 不写 FORMAT 时默认是 CSV —— 我们按
            // "没写就报错"处理，免得从 DuckDB 过来的人以为自己在导 CSV。
            let mut fmt: Option<crate::export::ExportFormat> = None;
            let mut header = false;
            for opt in &options {
                match opt {
                    sqlparser::ast::CopyOption::Format(f)
                        if f.value.eq_ignore_ascii_case("PARQUET") =>
                    {
                        fmt = Some(crate::export::ExportFormat::Parquet)
                    }
                    sqlparser::ast::CopyOption::Format(f)
                        if f.value.eq_ignore_ascii_case("CSV") =>
                    {
                        fmt = Some(crate::export::ExportFormat::Csv { header: false })
                    }
                    sqlparser::ast::CopyOption::Format(f) => {
                        return Err(QsdbError::unsupported(format!(
                            "COPY … TO 只支持 `FORMAT PARQUET` 与 `FORMAT CSV`（给的是 `{}`）",
                            f.value
                        )));
                    }
                    // `HEADER [true|false]`：sqlparser 本来就有这个变体，不用词法桩。
                    // 默认**不带**表头（与 DuckDB 一致）。
                    sqlparser::ast::CopyOption::Header(h) => header = *h,
                    other => {
                        return Err(QsdbError::unsupported(format!(
                            "COPY … TO 不支持选项 `{other}`\
                             （现在只认 `FORMAT PARQUET|CSV`、`HEADER` 与 `PARTITION_BY (…)`）"
                        )));
                    }
                }
            }
            let format = match fmt {
                Some(crate::export::ExportFormat::Csv { .. }) => {
                    crate::export::ExportFormat::Csv { header }
                }
                Some(p) => p,
                None => {
                    return Err(QsdbError::unsupported(
                        "COPY … TO 要显式写格式：`(FORMAT PARQUET)` 或 `(FORMAT CSV)` —— \
                         不写就等于让调用方猜（DuckDB 不写时默认 CSV，我们**不猜**）",
                    ));
                }
            };
            // ★ CSV × PARTITION_BY：**在绑定计划之前**就拒（执行期那条更贵）。
            //   理由见 `ExportFormat::Csv` 的文档：`read_csv` 没有 hive 语义 ⇒
            //   写得出、读不回。
            if matches!(format, crate::export::ExportFormat::Csv { .. })
                && copy_partition_by.is_some_and(|c| !c.is_empty())
            {
                return Err(QsdbError::unsupported(
                    "CSV 不支持 PARTITION_BY —— 本仓的 `read_csv` **没有 hive 目录语义**\
                     （只有 `read_parquet` 有），分区列写出去就重建不回来，等于导出一份\
                     我们自己读不了的数据。要分区导出请用 `FORMAT PARQUET`（出 + 读闭环）",
                ));
            }
            if !legacy_options.is_empty() {
                return Err(QsdbError::unsupported(
                    "COPY 的旧式（带括号外）选项未实现 —— 选项请都写进括号里：\
                     `(FORMAT PARQUET, PARTITION_BY (…))`",
                ));
            }
            // 词法层抽到 PARTITION_BY、但 AST 里没有对应项 —— 两者必须一致，
            // 不一致说明有人把 `raw` 和 `for_parse` 搞混了（内部错误）。
            Ok(ParsedStatement::CopyTo {
                query,
                target: path,
                partition_by: copy_partition_by.unwrap_or(&[]).to_vec(),
                format,
            })
        }
        SqlStatement::CreateTable(ct) => {
            let table = object_name_single(ct.name.clone())?;
            // `CREATE OR REPLACE TABLE` 一律仍拒（含 CTAS 形态）——**先判它**，
            // 因为 `CREATE OR REPLACE TABLE t AS SELECT …` 是最容易让人误以为
            // "反正要重建，OR REPLACE 无所谓"的写法，得让文案说清为什么不行。
            if ct.or_replace {
                return Err(QsdbError::unsupported(
                    "CREATE OR REPLACE TABLE 未实现 —— 先 `DROP TABLE` 再 `CREATE`（要原子替换分区内容用 `INSERT OVERWRITE`）",
                ));
            }
            // **CTAS**（2026-10-15 实现，生态功能批次 4）。
            // 此前会掉进"零列"检查、报 `table needs at least one column`（把"没实现"
            // 说成"你建表建错了"）；文档一度声称"报错文案已明确"，实测**根本没落地**
            // （与 F29② 同款）。现在按标准实现：schema 由 SELECT 的输出推导。
            if let Some(q) = ct.query.clone() {
                let part_keys = match &ct.hive_distribution {
                    sqlparser::ast::HiveDistributionStyle::PARTITIONED { columns } => {
                        columns.iter().map(|c| c.name.value.clone()).collect()
                    }
                    _ => parse_partitioned_by(raw)?,
                };
                let mut columns = Vec::with_capacity(ct.columns.len());
                for c in &ct.columns {
                    columns.push((c.name.value.clone(), ast_type_to_logical(&c.data_type)?));
                }
                return Ok(ParsedStatement::CreateTableAs {
                    name: table,
                    columns,
                    partition_keys: part_keys,
                    query: q,
                });
            }
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
        other => {
            // **不许 Debug-print 整棵语句树**（`PREPARE` 那条实测 dump 出几百字符，
            // 把整条嵌套查询连 `Span(Location(1,9)..)` 一起打出来）——
            // 与 batch 1 清掉的那几处同源，这处当时漏了。`ast_head` 只留变体名。
            let d = format!("{other:?}");
            Err(QsdbError::unsupported(format!(
                "statement `{}` not supported in this version (SQL-1)",
                ast_head(&d)
            )))
        }
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
pub(crate) fn ast_type_to_logical(dt: &sqlparser::ast::DataType) -> Result<LogicalType> {
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
        // **参数化查询的落点**：`$n` 在这里变成**字面量节点**（不是 SQL 文本）。
        // 放在 `value_to_literal` 而不是 `bind_expr` 里，是为了让**所有**绑定入口
        // 都自动认（`bind_expr` 与 `rebind_over_agg` 那条重绑路径都走这里）。
        // 值从不进入文本 ⇒ 注入无从谈起（见 `docs/参数化查询设计.md` §七）。
        V::Placeholder(p) => {
            let idx = crate::sql::params::placeholder_index(p)?;
            crate::sql::params::lookup(idx)
        }
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

/// 文本 + 声明类型 → 字面量（**参数化查询专用**：值从协议来，不是从 SQL 文本来）。
///
/// 与 `number_literal` / `TypedString` 那两条路刻意分开：那两条是**解析 SQL 文本**
/// 时用的（形状由语法保证），这里要对**任意用户输入**做严格转换 —— 转不过去就报错，
/// 绝不猜。
pub(crate) fn parse_literal_text(text: &str, ty: &crate::types::LogicalType) -> Result<Literal> {
    use crate::types::LogicalType as T;
    use crate::types::temporal;
    let bad = |what: &str| QsdbError::invalid(format!("值 `{text}` 转不成 {what}"));
    Ok(match ty {
        T::Boolean => match text {
            "true" | "TRUE" | "1" => Literal::Boolean(true),
            "false" | "FALSE" | "0" => Literal::Boolean(false),
            _ => return Err(bad("BOOLEAN")),
        },
        T::Int8 => Literal::Int8(text.parse().map_err(|_| bad("Int8"))?),
        T::Int16 => Literal::Int16(text.parse().map_err(|_| bad("Int16"))?),
        T::Int32 => Literal::Int32(text.parse().map_err(|_| bad("Int32"))?),
        T::Int64 => Literal::Int64(text.parse().map_err(|_| bad("BIGINT"))?),
        T::Float32 => Literal::Float32(text.parse().map_err(|_| bad("FLOAT"))?),
        T::Float64 => Literal::Float64(text.parse().map_err(|_| bad("DOUBLE"))?),
        T::Decimal(meta) => {
            // 十进制文本 → unscaled（**按字符串算，别过 f64**：那会丢精度）
            let (neg, body) = match text.strip_prefix('-') {
                Some(r) => (true, r),
                None => (false, text.strip_prefix('+').unwrap_or(text)),
            };
            let (int_part, frac_part) = body.split_once('.').unwrap_or((body, ""));
            if !int_part.chars().all(|c| c.is_ascii_digit())
                || !frac_part.chars().all(|c| c.is_ascii_digit())
            {
                return Err(bad("DECIMAL"));
            }
            let scale = meta.scale as usize;
            // 小数位不足补零、超出**报错**（静默截断 = 改值）
            if frac_part.len() > scale {
                return Err(QsdbError::invalid(format!(
                    "值 `{text}` 的小数位比 DECIMAL 声明的多（scale = {}）—— \
                     静默截断会改值，请显式写更宽的 DECIMAL 或自行取整",
                    meta.scale
                )));
            }
            let digits = format!("{int_part}{frac_part:0<width$}", width = scale);
            let unscaled: i128 = digits.parse().map_err(|_| bad("DECIMAL"))?;
            Literal::Decimal {
                unscaled: if neg { -unscaled } else { unscaled },
                meta: *meta,
            }
        }
        T::Date => Literal::Date(temporal::parse_date(text)?),
        T::Time => Literal::Time(temporal::parse_time_ns(text)?),
        T::Timestamp { .. } => Literal::Timestamp(temporal::parse_timestamp_ns(text)?),
        T::Varchar { .. } => Literal::Utf8(text.to_string()),
        T::Binary => {
            let h = text.strip_prefix("0x").unwrap_or(text);
            if h.len() % 2 != 0 || !h.chars().all(|c| c.is_ascii_hexdigit()) {
                return Err(QsdbError::invalid(format!(
                    "BINARY 参数要写成十六进制串（可带 `0x` 前缀），实得 `{text}`"
                )));
            }
            let bytes = (0..h.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&h[i..i + 2], 16).expect("checked hex"))
                .collect();
            Literal::Binary(bytes)
        }
        other => {
            return Err(QsdbError::unsupported(format!(
                "参数不支持的类型 {other:?}"
            )));
        }
    })
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

    /// 内部探测用：有这一列吗？（**命中多个取第一个**）
    ///
    /// ⚠️ 只给"这个 ctx 里有没有 X"这类判断用。**解析用户写的列名一律走
    /// [`SchemaCtx::resolve_opt`]** —— 它重名会报错，这条是 F30 的核心
    /// （见 `docs/结果集列名口径.md` §三）。
    pub fn index_of(&self, name: &str) -> Option<usize> {
        let lower = name.to_ascii_lowercase();
        self.cols
            .iter()
            .position(|(n, _)| n.to_ascii_lowercase() == lower)
    }

    /// **解析用户写的列名**（F30）。
    ///
    /// - `Ok(Some(i))`：唯一命中。
    /// - `Ok(None)`：没有这一列（调用方自己组织文案，好带上各自的上下文）。
    /// - `Err(…)`：**重名 ⇒ 显式报错**。
    ///
    /// ★ 为什么重名必须报错而不是取第一个：剥掉表限定之后，`SELECT a.v, b.v
    /// FROM a JOIN b` 的结果集列名就是两个 `v` —— 而"JOIN 两侧同名列"是**极常见**
    /// 的形状。取第一个 = 把一个本来能跑的查询变成**静默错值**。
    /// PG 在这里的措辞是 `column reference "v" is ambiguous`。
    ///
    /// ⚠️ 重名只可能来自**派生表 / CTE**（基表与 join 的 ctx 天然无重名：左裸右
    /// 限定，且列名不许带 `.`）⇒ 这条检查不会误伤正常查询。
    pub fn resolve_opt(&self, name: &str) -> Result<Option<usize>> {
        let lower = name.to_ascii_lowercase();
        let mut hit: Option<usize> = None;
        for (i, (n, _)) in self.cols.iter().enumerate() {
            if n.to_ascii_lowercase() != lower {
                continue;
            }
            if hit.is_some() {
                // ⚠️ 这里**不能**建议"用表限定名" —— 重名只发生在派生表/CTE 的
                //    输出上，而那一层的限定名（`a.id`）根本不在 ctx 里，写了也没用。
                //    唯一的出路是在**产生重名的那个子查询**里起别名。
                return Err(QsdbError::invalid(format!(
                    "column reference '{name}' is ambiguous —— 这个查询的输出里有多个\
                     叫 `{name}` 的列。请在**产生它们的那一层**给其中一列起别名\
                     （如 `SELECT a.{name} AS {name}_a, b.{name} … FROM …`）"
                )));
            }
            hit = Some(i);
        }
        Ok(hit)
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
        hive.iter().map(|(k, _)| k.clone()).collect()
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
        residual: None, // semi/anti 族：ON 由 IN/EXISTS 生成，没有"键之外的条件"
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
        // 行值比较 `(a, b) IN (子查询)` 是**标准 SQL、我们没实现**（2026-10-15 的
        // join 支持面探针抓到这条归类错）：此前一律报 400 `INVALID_ARGUMENT`
        // "IN subquery must select exactly one column" —— 那是把"没实现"说成
        // "你写错了"。**驱动按码决定动作**（400 = 修正参数 / 501 = 改写法），
        // 归类错了动作就错。
        if is_row_value(lhs) {
            return Err(QsdbError::unsupported(
                "row-value comparison `(a, b) IN (subquery)` not supported — \
                 write it as EXISTS (SELECT 1 FROM … WHERE … AND …)",
            ));
        }
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
        residual: None, // semi/anti 族：ON 由 IN/EXISTS 生成，没有"键之外的条件"
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

/// `GROUP BY <裸整数>` ⇒ **投射位置序号**（标准 SQL：`GROUP BY 1` ≡ 第一列），
/// 换算成投射里那个表达式再绑定。不是序号的表达式原样借回。
///
/// 为什么必须换算而不是当常量：裸整数当常量会把全表并成一组（**静默错值**），
/// 而当序号才是用户的意思。`ORDER BY 序号` 走的是同一套标准。
///
/// 四类显式拒（都是"照着跑必然给错值/给不出值"）：
/// - 序号越界（`GROUP BY 0` / 超出投射列数）—— 报出来比默默回一个组好；
/// - 数字不是整数（`GROUP BY 1.5`）—— 标准如此（PG 也拒），而它几乎必然是笔误；
/// - 序号指向 `SELECT *` —— 通配没有"第 n 个表达式"，指不出来；
/// - 序号指向聚合（`SELECT count(*) … GROUP BY 1`）—— 聚合不能当分组键。
fn group_key_expr<'a>(
    e: &'a sqlparser::ast::Expr,
    proj: &'a [Option<sqlparser::ast::Expr>],
) -> Result<&'a sqlparser::ast::Expr> {
    let sqlparser::ast::Expr::Value(sqlparser::ast::ValueWithSpan {
        value: sqlparser::ast::Value::Number(n, _),
        ..
    }) = e
    else {
        return Ok(e);
    };
    let pos: i64 = n.parse().map_err(|_| {
        QsdbError::unsupported(format!(
            "GROUP BY `{n}` —— 裸数字按标准是**投射位置序号**，必须写成整数（如 GROUP BY 1）；\
             当常量分组请显式加括号或写表达式"
        ))
    })?;
    if pos < 1 || pos as usize > proj.len() {
        return Err(QsdbError::invalid(format!(
            "GROUP BY 序号 {pos} 超出投射列范围（SELECT 里有 {} 列，序号从 1 起）",
            proj.len()
        )));
    }
    let target = proj[pos as usize - 1].as_ref().ok_or_else(|| {
        QsdbError::unsupported(
            "GROUP BY 序号不能与 `SELECT *` 同用 —— 通配展不成“第 n 个表达式”，\
             请把分组列写出来",
        )
    })?;
    if let sqlparser::ast::Expr::Function(f) = target {
        if is_agg_name(&fn_name(f)) {
            return Err(QsdbError::invalid(format!(
                "GROUP BY 序号 {pos} 指向的是聚合（`{}`）—— 聚合不能当分组键，\
                 分组键要写原始列或表达式",
                expr_display(target)
            )));
        }
    }
    Ok(target)
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
    //
    // ⚠️ 拒绝文案里**不许 `{op:?}`**：那是把 sqlparser 的语法树 Debug-print 给用户，
    // 一条能到几百字符（`Join type Right(On(BinaryOp { left: CompoundIdentifier([Ident
    // { value: "a", span: Span(Location(1,40)..Location(1,41)) }]), …`），人和驱动都
    // 读不出重点——真正有用的信息只是"你不认识哪种写法"。见 `join_op_name`。
    let no_joins: Vec<sqlparser::ast::Join> = Vec::new();
    let joins = select
        .from
        .first()
        .map(|t| t.joins.as_slice())
        .unwrap_or_else(|| no_joins.as_slice());
    for join in joins {
        // `USING (cols)` 的列名（`None` = 不是 USING 写法）
        let mut using_names: Option<Vec<String>> = None;
        // `swap` = 换边规划（RIGHT 用）：右侧当 probe、左侧当 build。
        let (jt, constraint, swap) = match &join.join_operator {
            sqlparser::ast::JoinOperator::Inner(c) | sqlparser::ast::JoinOperator::Join(c) => {
                (crate::exec::join::JoinType::Inner, Some(c), false)
            }
            sqlparser::ast::JoinOperator::LeftOuter(c) | sqlparser::ast::JoinOperator::Left(c) => {
                (crate::exec::join::JoinType::LeftOuter, Some(c), false)
            }
            // **RIGHT = 换边的 LEFT**：`l RIGHT JOIN r ON c` ≡ `r LEFT JOIN l ON c`
            //（"保留哪侧的未匹配行"正是 LEFT/RIGHT 的唯一区别）。换边后输出列序是
            // `r.* ++ l.*`，与用户写的相反 ⇒ 下面补一层投影换回来。
            sqlparser::ast::JoinOperator::RightOuter(c)
            | sqlparser::ast::JoinOperator::Right(c) => {
                (crate::exec::join::JoinType::LeftOuter, Some(c), true)
            }
            // **FULL OUTER**：两侧的孤儿都要保留 —— 不能换边（换边只是把
            // "保留哪一侧"对调），执行层要记 build 侧匹配标记并补发一轮（见
            // `HashJoinExec::full_tail`）。
            sqlparser::ast::JoinOperator::FullOuter(c) => {
                (crate::exec::join::JoinType::FullOuter, Some(c), false)
            }
            // `CROSS JOIN`：约束是 `JoinConstraint::None`（没有 ON），谓词恒真
            sqlparser::ast::JoinOperator::CrossJoin(_) => {
                (crate::exec::join::JoinType::Inner, None, false)
            }
            other => {
                return Err(QsdbError::unsupported(format!(
                    "{} not supported (INNER / LEFT [OUTER] / RIGHT [OUTER] / FULL [OUTER] /                      CROSS JOIN are)",
                    join_op_name(other)
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
        // `CROSS JOIN` 的约束是 `None`（它本来就没有 ON）；`USING`/`NATURAL`
        // 仍是明确拒绝（当叉积会炸行数）——两者都在这条分派里。
        let on_expr = match constraint {
            Some(sqlparser::ast::JoinConstraint::On(e)) => Some(e),
            // `USING (cols)`：键由列名给（两侧同名），**且输出要合并那一列** ——
            // 见 `using_names` 与下面 `build_using_merge_project` 的说明。
            Some(sqlparser::ast::JoinConstraint::Using(names)) => {
                let names: Vec<String> = names
                    .iter()
                    .map(|n| {
                        n.0.last()
                            .and_then(|p| p.as_ident())
                            .map(|i| i.value.clone())
                            .ok_or_else(|| {
                                QsdbError::invalid("USING: column name must be identifiers")
                            })
                    })
                    .collect::<Result<_>>()?;
                using_names = Some(names);
                None
            }
            // `NATURAL JOIN`：**明确不做**（它按"同名同类型列"隐式匹配，两侧
            // 加一列就可能**静默改变连接语义**）—— 与本仓"拼错就报错"的纪律相反。
            Some(sqlparser::ast::JoinConstraint::Natural) => {
                return Err(QsdbError::unsupported(
                    "NATURAL JOIN not supported（隐式按同名列匹配，加一列就可能静默改语义）—                      write the `ON`/`USING` explicitly",
                ));
            }
            Some(sqlparser::ast::JoinConstraint::None) => {
                return Err(QsdbError::unsupported(
                    "JOIN ... ON <equi-cond> required (use CROSS JOIN if you really want a                      cartesian product)",
                ));
            }
            // 只有显式的 `CROSS JOIN` 会走到这里（它在算子匹配里就没有约束对象）
            None => None,
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
        // `ON` 拆成「键 + 残留」：`ON l.k=r.k AND r.v>1` 从"整条退化成叉积"变成
        // **多键/单键哈希 join + 残留**（残留必须在 join 内对匹配对求值，
        // 不能提到 WHERE —— 见 `LogicalPlan::Join::residual`）。
        // ── `USING (cols)`：键由列名给，**且输出要把那一列合并成一份** ──
        //
        // 合并列取值一律 `COALESCE(l.c, r.c)`：等值键下匹配对的 l.c 与 r.c 相等，
        // 四种 join 的差别只在**孤儿行**（LEFT 的孤儿 l.c 有值、RIGHT 的 r.c 有值、
        // FULL 两者各有一半）—— `COALESCE` 正好覆盖全部四种，不必按类型分派。
        if let Some(names) = using_names {
            let mut lkeys: Vec<PhysExpr> = Vec::with_capacity(names.len());
            let mut rkeys: Vec<PhysExpr> = Vec::with_capacity(names.len());
            let mut pairs: Vec<(usize, usize, LogicalType)> = Vec::with_capacity(names.len());
            for nm in &names {
                let li = ctx.cols[..n_left]
                    .iter()
                    .position(|(n, _)| n == nm)
                    .ok_or_else(|| {
                        QsdbError::not_found(format!(
                            "USING column '{nm}' not found in the left input"
                        ))
                    })?;
                let ri = right_cols
                    .iter()
                    .position(|(n, _)| n == nm)
                    .ok_or_else(|| {
                        QsdbError::not_found(format!(
                            "USING column '{nm}' not found in the right input"
                        ))
                    })?;
                let (ln, lt) = ctx.cols[li].clone();
                let (rn, rt) = right_cols[ri].clone();
                lkeys.push(PhysExpr::Column {
                    idx: li,
                    name: ln,
                    ty: lt.clone(),
                });
                rkeys.push(PhysExpr::Column {
                    idx: n_left + ri,
                    name: format!("{right_name}.{rn}"),
                    ty: rt,
                });
                pairs.push((li, ri, lt));
            }
            // RIGHT 要**换边**（`r LEFT JOIN l`）——同非 USING 的路径；换边后
            // join 的输出序是 `[右 ++ 左]`，合并投影的位置基准随之改变。
            let joined = if swap {
                let n_right = right_cols.len();
                let probe_keys: Vec<PhysExpr> = rkeys
                    .iter()
                    .map(|k| shift_col_idx(k, -(n_left as isize)))
                    .collect();
                let build_keys: Vec<PhysExpr> = lkeys
                    .iter()
                    .map(|k| shift_col_idx(k, n_right as isize))
                    .collect();
                LogicalPlan::Join {
                    left: Box::new(right_plan),
                    right: Box::new(plan),
                    left_keys: probe_keys,
                    right_keys: build_keys,
                    join_type: jt,
                    needed: None,
                    residual: None,
                }
            } else {
                LogicalPlan::Join {
                    left: Box::new(plan),
                    right: Box::new(right_plan),
                    left_keys: lkeys,
                    right_keys: rkeys,
                    join_type: jt,
                    needed: None,
                    residual: None,
                }
            };
            // 位置基准：非换边 = [左 0..n_left, 右 n_left..]；换边 = [右 0..n_right, 左 n_right..]
            let (left_base, right_base) = if swap {
                (right_cols.len(), 0usize)
            } else {
                (0usize, n_left)
            };
            let (proj, cols) =
                using_merge_project(joined, &pairs, &ctx, n_left, left_base, right_base)?;
            plan = proj;
            ctx.cols = cols;
            continue;
        }
        let Some(on_expr) = on_expr else {
            // **`CROSS JOIN`**：笛卡尔积 = 谓词恒真的非等值 INNER
            plan = LogicalPlan::CrossJoinFilter {
                left: Box::new(plan),
                right: Box::new(right_plan),
                predicate: PhysExpr::Lit(Literal::Boolean(true)),
            };
            continue;
        };
        let split = split_on_condition(on_expr, n_left, &ctx)?;
        if swap {
            // **RIGHT 换边**：`l RIGHT JOIN r` ≡ `r LEFT JOIN l`（"保留哪侧的未匹配行"
            // 正是 LEFT/RIGHT 的唯一区别）。三件事都要跟着换算，漏一件就是静默错值：
            //   ① 两侧**角色互换**（右表当 probe、左表当 build）；
            //   ② **键的索引空间**：`left_keys` 是 probe 局部序、`right_keys` 是
            //      "从 probe 宽度起算"的全局序（见 `exec_builder::local_keys`），
            //      换边后两者互换角色；
            //   ③ **残留**绑在合并 schema（左 ++ 右）上，换边后合并序变成右 ++ 左；
            // 最后补一层投影把**输出列序换回** `l.* ++ r.*`（用户写的顺序）。
            if split.keys.is_empty() {
                return Err(QsdbError::unsupported(format!(
                    "non-equi {} not supported (only non-equi INNER is) — put the condition \
                     in WHERE for an INNER, or join on an equi key",
                    join_op_name(&join.join_operator)
                )));
            }
            let (lk, rk): (Vec<PhysExpr>, Vec<PhysExpr>) = split.keys.into_iter().unzip();
            let n_right = right_cols.len();
            let probe_keys: Vec<PhysExpr> = rk
                .iter()
                .map(|k| shift_col_idx(k, -(n_left as isize)))
                .collect();
            let build_keys: Vec<PhysExpr> = lk
                .iter()
                .map(|k| shift_col_idx(k, n_right as isize))
                .collect();
            let residual = split
                .residual
                .as_ref()
                .map(|r| swap_sides_expr(r, n_left, n_right));
            let joined = LogicalPlan::Join {
                left: Box::new(right_plan),
                right: Box::new(plan),
                left_keys: probe_keys,
                right_keys: build_keys,
                join_type: jt,
                needed: None,
                residual,
            };
            plan = reorder_after_swap(joined, &right_cols, &ctx, n_left, n_right);
        } else if !split.keys.is_empty() {
            let (lk, rk): (Vec<PhysExpr>, Vec<PhysExpr>) = split.keys.into_iter().unzip();
            plan = LogicalPlan::Join {
                left: Box::new(plan),
                right: Box::new(right_plan),
                left_keys: lk,
                right_keys: rk,
                join_type: jt,
                needed: None,
                residual: split.residual,
            };
        } else if jt == crate::exec::join::JoinType::Inner {
            // 纯非等值 INNER：cross join + 合并 schema 上过滤整个 ON
            let combined = split
                .residual
                .ok_or_else(|| QsdbError::invalid("JOIN ON must be an equality or boolean"))?;
            plan = LogicalPlan::CrossJoinFilter {
                left: Box::new(plan),
                right: Box::new(right_plan),
                predicate: combined,
            };
        } else {
            // 纯非等值 INNER 之外的（LEFT/RIGHT/…）：**不能按 INNER 跑**——
            // 那会静默丢掉未匹配行。文案说清"改怎么写"（别留路线图口吻）。
            return Err(QsdbError::unsupported(format!(
                "non-equi {} not supported (only non-equi INNER is) — put the condition in \
                 WHERE for an INNER, or join on an equi key",
                join_op_name(&join.join_operator)
            )));
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

    // GROUP BY（裸整数先按标准换算成**投射位置序号**，见 `group_key_expr`）
    let group_exprs: Vec<PhysExpr> = match &select.group_by {
        GroupByExpr::All(_) => return Err(QsdbError::unsupported("GROUP BY ALL")),
        GroupByExpr::Expressions(exprs, _) => exprs
            .iter()
            .map(|e| bind_expr(group_key_expr(e, &proj_extracted)?, &ctx))
            .collect::<Result<_>>()?,
    };

    // SELECT 投影
    struct ProjItem {
        sql: Expr,
        name: String,
        /// `SELECT *` 展开出来的项：**按下标绑定**，不走名字解析。
        ///
        /// 理由（F30）：剥掉表限定之后，派生表里会出现重名（`SELECT a.id, b.id …`
        /// 的输出就是两个 `id`），而 `SELECT *` 恰恰要**原样透传**它们 ——
        /// `SELECT * FROM (SELECT a.id, b.id …) x` 在 PG 里就出两个同名列。
        /// 按名字绑会在那里报 "ambiguous"，可那**不是用户在引用一个名字**，
        /// 是通配展开 —— 按下标才是它本来的语义。
        wild_idx: Option<usize>,
    }
    let mut items = Vec::new();
    for (i, item) in select.projection.iter().enumerate() {
        match item {
            SelectItem::UnnamedExpr(e) => items.push(ProjItem {
                sql: proj_extracted[i].clone().expect("scalar-sub extracted"),
                // F30：裸列引用剥表限定（`t.v` ⇒ `v`）；表达式原样
                name: output_name_of(e),
                wild_idx: None,
            }),
            SelectItem::ExprWithAlias { alias, .. } => items.push(ProjItem {
                sql: proj_extracted[i].clone().expect("scalar-sub extracted"),
                name: alias.value.clone(),
                wild_idx: None,
            }),
            SelectItem::Wildcard(_) => {
                if !group_exprs.is_empty() {
                    return Err(QsdbError::invalid(
                        "SELECT * with GROUP BY not supported; list group columns explicitly",
                    ));
                }
                for (wi, (name, _)) in ctx.cols.iter().enumerate() {
                    items.push(ProjItem {
                        sql: Expr::Identifier(Ident::new(name.clone())),
                        name: strip_table_qualifier(name).to_string(),
                        wild_idx: Some(wi),
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

    // 投影表达式的**原文**（F30）：`ORDER BY` 认列时要用它 —— 投影名剥了表限定
    // 之后，`ORDER BY d.label` 按名再也比不中（见 ORDER BY 那段）。
    // ⚠️ 用 AST 的 `expr_display`（`t.v + 1`），**不能**用 `PhysExpr::display()`
    //    （后者带括号 `(t.v + 1)`）—— 两个渲染器永远比不中，本仓栽过一次。
    let proj_display: Vec<String> = items.iter().map(|it| expr_display(&it.sql)).collect();

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
        // 聚合参数上的 `DISTINCT`：等价改写（去重 + 原聚合）。见 `rewrite_distinct_aggs`。
        // ⚠️ 只改**喂给计划节点**的 group/aggs；上面的 `agg_ctx` 与下游重绑一律
        // 仍用原始表达式（列名不变 ⇒ 下游一个字都不用动）。
        let (agg_input, plan_group, plan_aggs) = rewrite_distinct_aggs(plan, &group_exprs, &aggs)?;
        plan = LogicalPlan::Aggregate {
            input: Box::new(agg_input),
            group: plan_group,
            aggs: plan_aggs,
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
            // `SELECT *` 展开的项**按下标绑**（见 `ProjItem::wild_idx` 的说明）
            let bound = match it.wild_idx {
                Some(i) => {
                    let (n, t) = proj_ctx
                        .cols
                        .get(i)
                        .ok_or_else(|| QsdbError::invalid("内部错误：通配展开的下标越界"))?;
                    PhysExpr::Column {
                        idx: i,
                        name: n.clone(),
                        ty: t.clone(),
                    }
                }
                None => bind_expr(&sub, &proj_ctx)?,
            };
            proj_exprs.push(bound);
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
            //
            // ⚠️ 走 `resolve_opt`（重名报错）而不是 `index_of`：`ORDER BY v` 撞上
            //    两个 `v` 时，PG 报的就是 `ORDER BY "v" is ambiguous` —— 静默按第一个
            //    排序等于**排错还没人知道**。
            let by_output = match key_expr {
                Expr::Identifier(id) => out_ctx.resolve_opt(&id.value)?,
                Expr::Value(v) => match &v.value {
                    // 纯数字 = 输出列序号（1-based，SQL 标准）。
                    // **越界/非整数必须显式拒**：悄悄退回"当常量"等于**不排序**
                    //（常量排序键），用户以为排了、其实没有 —— 静默无效。
                    // 此前是靠"常量排序键会让 `RecordBatch` 列长对不齐而报错"侥幸
                    // 拦住的，那是**借了 ProjectExec 那个 bug 的力**；2026-10-15 把
                    // 常量广播修好之后它就漏了（`sql_gaps::order_by_ordinal` 当场
                    // 抓到 —— 那条断言 `ORDER BY 5` 报错的用例一直是"借 bug 通过"）。
                    sqlparser::ast::Value::Number(n, _) => {
                        let i: usize = n.parse().map_err(|_| {
                            QsdbError::unsupported(format!(
                                "ORDER BY `{n}` —— 裸数字是**输出列序号**（从 1 起），必须写成整数"
                            ))
                        })?;
                        if i < 1 || i > proj_names.len() {
                            return Err(QsdbError::invalid(format!(
                                "ORDER BY 序号 {i} 超出输出列范围（SELECT 里有 {} 列）",
                                proj_names.len()
                            )));
                        }
                        Some(i - 1)
                    }
                    _ => None,
                },
                other => {
                    // F30：投影名现在**剥了表限定**（`SELECT d.label` ⇒ `label`），
                    // 而 ORDER BY 写的还是 `d.label` ⇒ 按**名**比对不上。
                    //
                    // ⚠️ 这里匹配的是**投影表达式**（`items[i].sql` 的原文），不是
                    //    "按末段再匹配一次名"。差别很要紧：后者会把 `ORDER BY b.id`
                    //    匹配到碰巧叫 `id` 的**另一个**投影列（`a.id`）—— 值全对、
                    //    **顺序错**，最难发现的一类。按表达式原文比就不会串。
                    let d = expr_display(other);
                    proj_names
                        .iter()
                        .position(|n| n.eq_ignore_ascii_case(&d))
                        .or_else(|| proj_display.iter().position(|p| p.eq_ignore_ascii_case(&d)))
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
/// 列引用整体平移 `delta`（换边时换算索引空间；非列表达式原样）。
fn shift_col_idx(e: &PhysExpr, delta: isize) -> PhysExpr {
    match e {
        PhysExpr::Column { idx, name, ty } => PhysExpr::Column {
            idx: (*idx as isize + delta).max(0) as usize,
            name: name.clone(),
            ty: ty.clone(),
        },
        other => other.clone(),
    }
}

/// 合并 schema 由 `[左 ++ 右]` 变成 `[右 ++ 左]`（换边后**残留谓词**的落点）。
///
/// 递归改写，因为残留通常是 `And` / `Cmp` 的组合而不是单个列。
fn swap_sides_expr(e: &PhysExpr, n_left: usize, n_right: usize) -> PhysExpr {
    match e {
        PhysExpr::Column { idx, name, ty } => {
            // 左列（旧 idx < n_left）整段后移到右侧段之后；右列前移 n_left。
            let new_idx = if *idx < n_left {
                n_right + *idx
            } else {
                *idx - n_left
            };
            PhysExpr::Column {
                idx: new_idx,
                name: name.clone(),
                ty: ty.clone(),
            }
        }
        PhysExpr::And(a, b) => PhysExpr::And(
            Box::new(swap_sides_expr(a, n_left, n_right)),
            Box::new(swap_sides_expr(b, n_left, n_right)),
        ),
        PhysExpr::Or(a, b) => PhysExpr::Or(
            Box::new(swap_sides_expr(a, n_left, n_right)),
            Box::new(swap_sides_expr(b, n_left, n_right)),
        ),
        PhysExpr::Not(a) => PhysExpr::Not(Box::new(swap_sides_expr(a, n_left, n_right))),
        PhysExpr::Cmp { op, l, r } => PhysExpr::Cmp {
            op: *op,
            l: Box::new(swap_sides_expr(l, n_left, n_right)),
            r: Box::new(swap_sides_expr(r, n_left, n_right)),
        },
        PhysExpr::IsNull(a, n) => {
            PhysExpr::IsNull(Box::new(swap_sides_expr(a, n_left, n_right)), *n)
        }
        PhysExpr::Arith { op, l, r, ty } => PhysExpr::Arith {
            op: *op,
            l: Box::new(swap_sides_expr(l, n_left, n_right)),
            r: Box::new(swap_sides_expr(r, n_left, n_right)),
            ty: ty.clone(),
        },
        other => other.clone(),
    }
}

/// 换边后补一层投影：把 join 的输出序 `[右 ++ 左]` 换回 `[左 ++ 右]`。
///
/// ⚠️ 这是"位置属于哪个空间"那个老坑的又一站：`LogicalPlan::Join` 的输出恒为
/// `probe ++ build`，换边后与**用户看到的列序**不同。漏了这层投影，
/// `SELECT l.k, r.k` 会取到**对调的列**（类型还常常兼容 ⇒ 静默错值）。
///
/// 列名取"该列在**未换边**时本该有的名字"：左段用 `ctx.cols` 的前 `n_left` 个
/// （多表链时外层可能已带限定名），右段用右表自己的列名（与
/// `plan_output_columns(Join)` 的口径一致——那里给的也是右表裸名）。
fn reorder_after_swap(
    joined: LogicalPlan,
    right_cols: &[(String, LogicalType)],
    ctx: &SchemaCtx,
    n_left: usize,
    n_right: usize,
) -> LogicalPlan {
    let mut exprs: Vec<PhysExpr> = Vec::with_capacity(n_left + n_right);
    let mut names: Vec<String> = Vec::with_capacity(n_left + n_right);
    // 源序 = [右..., 左...]：左列自 `n_right` 起，右列自 0 起
    for j in 0..n_left {
        let (name, ty) = ctx.cols[j].clone();
        exprs.push(PhysExpr::Column {
            idx: n_right + j,
            name: name.clone(),
            ty,
        });
        names.push(name);
    }
    for (i, (cn, ct)) in right_cols.iter().enumerate() {
        exprs.push(PhysExpr::Column {
            idx: i,
            name: cn.clone(),
            ty: ct.clone(),
        });
        names.push(cn.clone());
    }
    LogicalPlan::Project {
        input: Box::new(joined),
        exprs,
        names,
    }
}

/// `USING (cols)` 的**输出合并投影 + 新可见列集**。
///
/// `USING` 不是纯语法糖：`SELECT *` 里那一列**只出一份**（标准 SQL），
/// 所以 join 的输出（左 ++ 右，那一列出现两次）之上必须再套一层投影，
/// 并且 `ctx.cols`（绑定与 `SELECT *` 展开的唯一依据）要换成新的列序。
///
/// 新列序 = `[合并列…] ++ [左侧其余列…] ++ [右侧其余列…]`；
/// 合并列取 `COALESCE(l.c, r.c)`（等值键下匹配对相等，孤儿行各自有值 ——
/// 四种 join 一套覆盖，见调用点的说明）。
///
/// ⚠️ **代价（如实说明）**：合并之后，`a.c` / `b.c` 这种**限定名不再可解析**
/// （它们已不在可见列集里）—— 与 PostgreSQL 不同（那边两者并存）。本仓选择
/// "宁可不支持，也不静默给错的列"：写 `a.c` 会明确报"列不存在"。
#[allow(clippy::too_many_arguments)]
fn using_merge_project(
    joined: LogicalPlan,
    pairs: &[(usize, usize, LogicalType)],
    ctx: &SchemaCtx,
    n_left: usize,
    left_base: usize,
    right_base: usize,
) -> Result<(LogicalPlan, Vec<(String, LogicalType)>)> {
    let mut exprs: Vec<PhysExpr> = Vec::new();
    let mut cols: Vec<(String, LogicalType)> = Vec::new();
    let merged_left: Vec<usize> = pairs.iter().map(|(li, _, _)| *li).collect();
    let merged_right: Vec<usize> = pairs.iter().map(|(_, ri, _)| *ri).collect();
    // ① 合并列（按 USING 里写的顺序）
    for (li, ri, ty) in pairs {
        let (ln, _) = ctx.cols[*li].clone();
        let rn = ctx.cols[n_left + *ri].0.clone();
        exprs.push(PhysExpr::Coalesce(
            vec![
                PhysExpr::Column {
                    idx: left_base + *li,
                    name: ln.clone(),
                    ty: ty.clone(),
                },
                PhysExpr::Column {
                    idx: right_base + *ri,
                    name: rn,
                    ty: ty.clone(),
                },
            ],
            ty.clone(),
        ));
        cols.push((ln, ty.clone()));
    }
    // ② 左侧其余列（名字沿用左输入的 —— 多表链时可能已带限定名）
    for (i, (n, t)) in ctx.cols[..n_left].iter().enumerate() {
        if merged_left.contains(&i) {
            continue;
        }
        exprs.push(PhysExpr::Column {
            idx: left_base + i,
            name: n.clone(),
            ty: t.clone(),
        });
        cols.push((n.clone(), t.clone()));
    }
    // ③ 右侧其余列（限定名，与本仓"裸名 = 左侧、右表须限定"的规则一致）
    for (i, (n, t)) in ctx.cols[n_left..].iter().enumerate() {
        if merged_right.contains(&i) {
            continue;
        }
        exprs.push(PhysExpr::Column {
            idx: right_base + i,
            name: n.clone(),
            ty: t.clone(),
        });
        cols.push((n.clone(), t.clone()));
    }
    Ok((
        LogicalPlan::Project {
            input: Box::new(joined),
            exprs,
            names: cols.iter().map(|(n, _)| n.clone()).collect(),
        },
        cols,
    ))
}

/// `JoinOperator` → **用户写的那种写法**（用于拒绝文案）。
///
/// **别用 `{op:?}`**：`JoinOperator` 的 Debug 会把整条 `ON` 表达式连同 `Ident`/
/// `Span` 一起打出来（实测一条几百字符），人和驱动都读不出重点；而这里要说的
/// 只是"你不认识哪种写法"。
///
/// 穷尽匹配、**不留通配臂**：sqlparser 升级加了新算子时**编译期就会红**
/// （那时才该决定新写法是支持还是拒），比默默报一个 `"this JOIN form"` 强。
fn join_op_name(op: &sqlparser::ast::JoinOperator) -> &'static str {
    use sqlparser::ast::JoinOperator as J;
    match op {
        J::Join(_) => "JOIN",
        J::Inner(_) => "INNER JOIN",
        J::Left(_) => "LEFT JOIN",
        J::LeftOuter(_) => "LEFT OUTER JOIN",
        J::Right(_) => "RIGHT JOIN",
        J::RightOuter(_) => "RIGHT OUTER JOIN",
        J::FullOuter(_) => "FULL [OUTER] JOIN",
        J::CrossJoin(_) => "CROSS JOIN",
        J::Semi(_) => "SEMI JOIN",
        J::LeftSemi(_) => "LEFT SEMI JOIN",
        J::RightSemi(_) => "RIGHT SEMI JOIN",
        J::Anti(_) => "ANTI JOIN",
        J::LeftAnti(_) => "LEFT ANTI JOIN",
        J::RightAnti(_) => "RIGHT ANTI JOIN",
        J::CrossApply => "CROSS APPLY",
        J::OuterApply => "OUTER APPLY",
        J::AsOf { .. } => "ASOF JOIN",
        J::StraightJoin(_) => "STRAIGHT_JOIN",
        J::ArrayJoin => "ARRAY JOIN",
        J::LeftArrayJoin => "LEFT ARRAY JOIN",
        J::InnerArrayJoin => "INNER ARRAY JOIN",
    }
}

/// 左操作数是不是**行值构造** `(a, b)`（含括号包裹的形态）。
///
/// 用途见 `build_in_semi_join`：行值 `IN` 是合法 SQL 但没实现 ⇒ 该报 `UNSUPPORTED`
/// 而不是"你的子查询列数不对"。
fn is_row_value(e: &sqlparser::ast::Expr) -> bool {
    match e {
        sqlparser::ast::Expr::Tuple { .. } => true,
        sqlparser::ast::Expr::Nested(inner) => is_row_value(inner),
        _ => false,
    }
}

/// 一段 AST 的 `Debug` 输出 → **只留变体名**（兜底用，见 `join_op_name` 的说明）。
///
/// 不可穷尽匹配的地方（如 `Expr` 有上百个变体）用这个：拿 `Debug` 串的**前缀**，
/// 后面的字段全丢掉。至少是短的、能指认的。
fn ast_head(debug: &str) -> &str {
    let end = debug.find(['{', '(', ' ']).unwrap_or(debug.len());
    debug[..end].trim()
}

/// `ON` 的拆分结果：**键**（参与哈希匹配）+ **残留**（对匹配对再判）。
///
/// 键为空即"纯非等值 ON"（`ON l.x < r.y`），此时残留就是整个 ON。
pub(crate) struct OnSplit {
    /// 等值键对 `(左表达式, 右表达式)`。
    pub keys: Vec<(PhysExpr, PhysExpr)>,
    /// 键之外的条件（在**合并 schema** 上求值）。
    pub residual: Option<PhysExpr>,
}

/// 把 `ON` 拆成「键 + 残留」。
///
/// 规则（**每条都有反例钉着**，见 `tests/join_common.rs`）：
///
/// - 顶层按 `AND` 拆开、逐项判定（`ON l.a=r.a AND l.b=r.b AND r.v>1` 是三项）；
/// - 一项是 `=`、且两侧**分属左右**（且都是列引用）⇒ **键**；
/// - 其它一切 ⇒ **残留**：同侧等值（`AND l.m = l.m` 不是键——当键会造出恒真键、
///   还把同侧的列编进哈希）、非等值、表达式键（`l.k + 1 = r.k`）、布尔列……
///
/// ⚠️ 残留**不能提到 `WHERE`**（LEFT 的未匹配行会被整体丢掉），见
/// `LogicalPlan::Join::residual` 的说明。
///
/// 返回 `Err` 只表示**真绑定失败**或 ON 不是布尔式；"认不出是键"一律降级成残留，
/// 不报错——这正是不再把 `ON k=k AND v>1` 整条推向叉积的原因。
pub(crate) fn split_on_condition(
    e: &sqlparser::ast::Expr,
    n_left: usize,
    ctx: &SchemaCtx,
) -> Result<OnSplit> {
    let mut conjuncts = Vec::new();
    flatten_and(e, &mut conjuncts);
    let mut keys: Vec<(PhysExpr, PhysExpr)> = Vec::new();
    let mut residual_parts: Vec<PhysExpr> = Vec::new();
    for c in conjuncts {
        match as_equi_key(c, n_left, ctx) {
            Some(k) => keys.push(k),
            // 绑不动的项在下面 `bind_expr` 里报**确切**的错（列不存在之类）
            None => residual_parts.push(bind_expr(c, ctx)?),
        }
    }
    let residual = match residual_parts.len() {
        0 => None,
        1 => Some(residual_parts.pop().expect("长度已判")),
        _ => Some(and_all(residual_parts)?),
    };
    if let Some(r) = &residual {
        if !r.is_boolean() {
            return Err(QsdbError::type_err("JOIN ON must be boolean"));
        }
    }
    Ok(OnSplit { keys, residual })
}

/// 顶层 `AND` 链摊平（`a AND b AND c` → `[a, b, c]`）。
fn flatten_and<'a>(e: &'a sqlparser::ast::Expr, out: &mut Vec<&'a sqlparser::ast::Expr>) {
    use sqlparser::ast::{BinaryOperator as B, Expr as E};
    match e {
        E::BinaryOp {
            left,
            op: B::And,
            right,
        } => {
            flatten_and(left, out);
            flatten_and(right, out);
        }
        // 括号只是分组，摊平不影响语义（`(a AND b) AND c` 与 `a AND (b AND c)` 同）
        E::Nested(inner) => flatten_and(inner, out),
        _ => out.push(e),
    }
}

/// 一项是不是**跨侧的等值**；是则给出 `(左, 右)`。
fn as_equi_key(
    e: &sqlparser::ast::Expr,
    n_left: usize,
    ctx: &SchemaCtx,
) -> Option<(PhysExpr, PhysExpr)> {
    use sqlparser::ast::{BinaryOperator as B, Expr as E};
    let E::BinaryOp {
        left,
        op: B::Eq,
        right,
    } = e
    else {
        return None;
    };
    let lb = bind_expr(left, ctx).ok()?;
    let rb = bind_expr(right, ctx).ok()?;
    let side = |x: &PhysExpr| -> Option<bool> {
        match x {
            PhysExpr::Column { idx, .. } => Some(*idx < n_left),
            _ => None,
        }
    };
    match (side(&lb), side(&rb)) {
        (Some(true), Some(false)) => Some((lb, rb)),
        (Some(false), Some(true)) => Some((rb, lb)),
        _ => None,
    }
}

/// 把若干布尔式 `AND` 起来（左结合；残留通常很短，不值得为它做平衡树）。
fn and_all(mut parts: Vec<PhysExpr>) -> Result<PhysExpr> {
    let mut acc = parts.remove(0);
    for p in parts {
        if !p.is_boolean() {
            return Err(QsdbError::type_err("JOIN ON must be boolean"));
        }
        acc = PhysExpr::And(Box::new(acc), Box::new(p));
    }
    Ok(acc)
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
///
/// **帧必须进签名**：两个窗口调用只差帧（`sum(x) OVER (ORDER BY i ROWS BETWEEN
/// 1 PRECEDING AND 1 FOLLOWING)` vs `… 2 PRECEDING AND 2 FOLLOWING`）时，若签名
/// 相同就会被 `collect_window_calls` 去重成**同一个调用** ⇒ 两列都拿第一个帧的
/// 结果（静默错值，而且数字只差一点、极难发现）。
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
        // 帧（原样文本即可：同一段源码里的同一个写法必然得到同一个串）
        let fr = match &spec.window_frame {
            None => String::new(),
            Some(fr) => format!(
                "{}:{}:{}",
                fr.units,
                fr.start_bound,
                fr.end_bound
                    .as_ref()
                    .map(|b| b.to_string())
                    .unwrap_or_default()
            ),
        };
        format!("p[{}]o[{}]f[{}]", p.join(","), o.join(","), fr)
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

/// 参数里写了 `*`（`count(*)` 这种）。
fn has_star_arg(f: &sqlparser::ast::Function) -> bool {
    fn_args(f).iter().any(|a| {
        matches!(
            a,
            sqlparser::ast::FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Wildcard)
        )
    })
}

/// 解析窗口函数本体（参数经 bind 绑定）。
fn parse_window_fn(
    f: &sqlparser::ast::Function,
    bind: &mut dyn FnMut(&sqlparser::ast::Expr) -> Result<PhysExpr>,
) -> Result<crate::exec::window::WindowFn> {
    use crate::exec::window::WindowFn as W;
    let name = fn_name(f);
    // 窗口函数**不走 `try_bind_agg`** ⇒ `audit_function` 放行的聚合 FILTER 会从
    // 这里漏进来被静默丢掉（回全量聚合 = 错值）。窗口 FILTER 本批没做 ⇒ 显式拒。
    if f.filter.is_some() {
        return Err(QsdbError::unsupported(
            "窗口函数上的 FILTER (WHERE …) 暂未实现 —— 显式拒绝而不是静默丢掉它\
             （丢掉就等于把 `sum(x) FILTER (WHERE c)` 当成 `sum(x)` 跑，回错值）；\
             改写：`sum(CASE WHEN c THEN x END) OVER (…)`",
        ));
    }
    // 帧先解析（`None` = 没写帧 ⇒ 默认帧）：排名/偏移类带帧在这里就被拒。
    let frame = parse_window_fn_frame(f, &name)?;
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
        // `count(*)`（数**行**）与 `count(x)`（数非 NULL 的 x）是两件事，
        // 用 `Option<PhysExpr>` 区分（`None` = `*`）—— 与聚合那边同一套表示。
        // 此前窗口这边**没有** `*` 的表示，于是 `count(*) OVER (…)` 报
        // "needs an argument"（把"没实现"说成"你写错了"），而
        // `docs/SQL支持面.md` 声称窗口 `count` 可用 —— F29 那一类。
        "count" => {
            let arg = match arg_exprs.first() {
                Some(e) => Some(bind(e)?),
                None if has_star_arg(f) => None,
                None => {
                    return Err(QsdbError::invalid(
                        "window count needs an argument (write count(*) or count(x))",
                    ));
                }
            };
            Ok(W::Count(arg, frame))
        }
        "sum" | "min" | "max" | "avg" => {
            if has_star_arg(f) {
                return Err(QsdbError::unsupported(format!(
                    "window {name}(*) —— `*` 只有 count 能用（{name} 要一列或一个表达式）"
                )));
            }
            let first = arg_exprs
                .first()
                .ok_or_else(|| QsdbError::invalid(format!("window {name} needs an argument")))?;
            let arg = bind(first)?;
            Ok(match name.as_str() {
                "sum" => W::Sum(arg, frame),
                "min" => W::Min(arg, frame),
                "max" => W::Max(arg, frame),
                _ => W::Avg(arg, frame),
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
                // 字面量或**参数**（`lag(v, ?)` —— 见 `const_nonneg_int` 的说明）
                Some(e) => {
                    let v = const_nonneg_int(e, "lag/lead offset")?;
                    if v <= 0 {
                        return Err(QsdbError::invalid("lag/lead offset must be positive"));
                    }
                    v
                }
                #[allow(unreachable_patterns)]
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

/// 剥掉表限定：`t.v` ⇒ `v`（**段数不匹配就原样返回**，不猜）。
///
/// F30（`docs/结果集列名口径.md`）：结果集的列名与 PG 对齐 —— **只剥限定符**。
/// 分界线是「**剥掉限定符 ≠ 编造名字**」：
/// - `t.v` ⇒ `v`：`v` 就是用户写的列名，去掉的只是**消歧用的表限定** —— 该做；
/// - `count(*)` ⇒ `count`：`count` 用户从没写过 —— **不做**（那正是本仓一路在
///   消灭的"静默"，CTAS 里也据此明确拒过）。
///
/// 只在 `q`/`c` 都是合法标识符时才剥，避免把真的含点的名字（本仓列名不许带
/// `.`，所以这是防御）剥坏。
pub(crate) fn strip_table_qualifier(n: &str) -> &str {
    match n.rsplit_once('.') {
        Some((q, c)) if crate::catalog::ident_ok(q) && crate::catalog::ident_ok(c) => c,
        _ => n,
    }
}

/// **投影输出名**：裸列引用剥限定符，其余（表达式）原样。
///
/// ⚠️ 只用于**输出名**。绑定用的 `SchemaCtx::cols` 里仍然保留限定名
/// （`t.v` 是它消歧的手段），两条路必须分开 —— 混了会让 `t.v` 绑不上。
fn output_name_of(e: &sqlparser::ast::Expr) -> String {
    use sqlparser::ast::Expr as E;
    match e {
        // `SELECT v` / `SELECT t.v` / `SELECT db.t.v` —— 都是"取这一列"
        E::Identifier(id) => id.value.clone(),
        E::CompoundIdentifier(parts) => match parts.last() {
            Some(last) => last.value.clone(),
            None => expr_display(e),
        },
        // `count(*)` / `t.v + 1` / `sum(v)` —— **原样**（见本函数的文档）
        _ => expr_display(e),
    }
}

/// 列名没解析到时的报错。
///
/// **JOIN 右表的列只以限定名注册**（见 `translate_select_inner` 里 ctx 扩展处的
/// 注释）：裸名恒解析到左表。所以"没找到"若其实是右侧的列，直接说清该怎么写 ——
/// 否则用户只会看到"列不存在"而去怀疑列名拼错。
///
/// ⚠️ 判据必须要求那名**带限定符**（`n.rsplit_once('.')`）。F30 实测踩过：
/// 这条提示原本只看"尾名相同"，于是对着**派生表**（输出名恰好叫 `t.v` 那种历史
/// 形态）也会说 "a JOIN right-side column exists" —— **那句里根本没有 JOIN**，
/// 用户会往完全错误的方向排查。剥限定符之后这种 ctx 不存在了，但仍保留判据，
/// 因为"尾名相同但不是限定名"还能来自别处。
fn not_found_with_hint(ctx: &SchemaCtx, name: &str) -> QsdbError {
    let hint = ctx
        .cols
        .iter()
        .find(|(n, _)| n.rsplit_once('.').is_some_and(|(_, c)| c == name))
        .map(|(n, _)| {
            format!(
                " (a JOIN right-side column with this name exists — qualify it as `{n}`: \
                 unqualified names resolve to the left input only)"
            )
        })
        .unwrap_or_default();
    QsdbError::column_not_found(format!("column '{name}'{hint}"))
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
            // `resolve_opt`：**重名会 Err**（F30），不再静默取第一个
            let idx = ctx
                .resolve_opt(&id.value)?
                .ok_or_else(|| not_found_with_hint(ctx, &id.value))?;
            Ok(PhysExpr::Column {
                idx,
                name: ctx.cols[idx].0.clone(),
                ty: ctx.cols[idx].1.clone(),
            })
        }
        E::CompoundIdentifier(parts) if parts.len() == 2 => {
            let qual = format!("{}.{}", parts[0].value, parts[1].value);
            // 限定名优先（JOIN 右表列以 table.col 注册）；**只有"根本没有"才**退回
            // 裸列名 —— 重名（Err）必须原样上抛，别被这条退路吞掉。
            let idx = match ctx.resolve_opt(&qual)? {
                Some(i) => i,
                None => ctx
                    .resolve_opt(&parts[1].value)?
                    .ok_or_else(|| not_found_with_hint(ctx, &parts[1].value))?,
            };
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
        // `NVL(a, b)` 是 **Spark SQL 的内置函数**（= 两参的 coalesce，
        // 实测真 Spark 3.5.2：`SELECT nvl(0.2, 1)` ⇒ `0.2`）。日清那批结算 SQL 在用。
        // 与 `coalesce` 走同一条绑定路，只是**参数个数固定 2**。
        E::Function(f)
            if (f.name.to_string().eq_ignore_ascii_case("coalesce")
                || f.name.to_string().eq_ignore_ascii_case("nvl"))
                && f.over.is_none() =>
        {
            let is_nvl = f.name.to_string().eq_ignore_ascii_case("nvl");
            let args = arg_exprs(f)?;
            if is_nvl && args.len() != 2 {
                return Err(QsdbError::invalid("nvl(a, b) requires exactly 2 arguments"));
            }
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
        // TRIM(x) → 标量函数 `trim`。**只认无修饰的那一种。**
        //
        // ⚠️ 此前**根本没有这条臂**：sqlparser 把 `trim` 解析成**专用节点**
        // （`Expr::Trim`，不是 `Function`）⇒ `trim(x)` 掉进兜底、报一句泛化的
        // unsupported。而 `expr::scalar` 的注册表里**有** `trim`、模块头也写着它是内置的
        // ⇒ **"文档说 ✅、实测不 ✅"**（本仓第四次栽在同一个形态上；前三次见
        // `SQL支持面.md` 的 `GROUP BY` 行、F29 两处）。
        //
        // ⚠️ 其余形态（`LEADING`/`TRAILING`、`TRIM(chars FROM x)`、`TRIM(x, chars)`）
        // **显式拒**——绝不许落到"只去空白"的那条路上**假装支持**（那正是本仓
        // 一路在消灭的静默）。
        E::Trim {
            trim_where,
            trim_what,
            expr,
            trim_characters,
        } => {
            if trim_where.is_some() || trim_what.is_some() || trim_characters.is_some() {
                return Err(QsdbError::unsupported(
                    "only TRIM(<string>) is supported; LEADING/TRAILING and custom \
                     character sets are not implemented yet",
                ));
            }
            let inner = bind_expr(expr, ctx)?;
            let ty = (scalar::lookup("trim")?.ret_ty)(&[inner.data_type()])?;
            Ok(PhysExpr::Fn {
                name: "trim".into(),
                args: vec![inner],
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
        // 两条常见的"语法认识、但没实现"——单独给话，**别落到兜底去 Debug-print**
        sqlparser::ast::Expr::AnyOp { .. } => Err(QsdbError::unsupported(
            "`= ANY/ALL (subquery)` not supported — use IN / EXISTS, or an explicit OR/AND chain",
        )),
        sqlparser::ast::Expr::Tuple { .. } => Err(QsdbError::unsupported(
            "row value `(a, b)` not supported here",
        )),
        // 兜底：**只报变体名**，不 dump 整棵语法树（`ast_head` 的说明）
        other => {
            let d = format!("{other:?}");
            Err(QsdbError::unsupported(format!(
                "expression `{}` not supported (list: docs/SQL支持面.md §三)",
                ast_head(&d)
            )))
        }
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
            // UDAF 不走等价改写（改写要重绑参数表达式，UDAF 输出类型由 `ret_ty` 定），
            // 仍然显式拒绝——**绝不静默丢弃**。
            if has_duplicate_treatment(f) && crate::expr::udaf::lookup(&name).is_some() {
                return Err(QsdbError::unsupported(format!(
                    "{name}(DISTINCT …) is not supported for user-defined aggregates"
                )));
            }
            if let Some(def) = crate::expr::udaf::lookup(&name) {
                return try_bind_udaf(f, &name, def, ctx).map(Some);
            }
        }
        return Ok(None);
    }
    if f.over.is_some() {
        return Ok(None); // 窗口函数不作为聚合处理（由 collect_window_calls / 投影置换接管）
    }
    // **聚合参数上的 `DISTINCT`**：这里只打标记，真正的等价改写落在计划构造处
    // （`rewrite_distinct_aggs`：去重 + 普通聚合）。
    //
    // 历史：2026-10-13 之前这里压根不读 `duplicate_treatment`，于是
    // `count(DISTINCT a)` 被当成 `count(a)` 执行——回的是**错的数字**
    // （`{1,1,2}` 上回 3 而非 2），"完全合法的 SQL、悄悄给错值"这一类。
    // 当时改成显式拒绝；2026-10-14 补上原生语义，拒绝撤掉。
    //
    let distinct = has_duplicate_treatment(f);
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
    if distinct && arg.is_none() {
        return Err(QsdbError::invalid(format!(
            "{name}(DISTINCT *) is not valid"
        )));
    }
    // `FILTER (WHERE …)`：谓词必须绑成**布尔**表达式（非布尔直接拒，别让它在
    // 执行层变成"永远不为 TRUE"而静默回空值）。
    let filter = match &f.filter {
        None => None,
        Some(pred) => {
            if matches!(
                pred.as_ref(),
                sqlparser::ast::Expr::Value(sqlparser::ast::ValueWithSpan {
                    value: sqlparser::ast::Value::Null,
                    ..
                })
            ) {
                return Err(QsdbError::invalid(
                    "FILTER (WHERE NULL) —— 谓词恒 UNKNOWN，聚合会永远回空",
                ));
            }
            let p = bind_expr(pred, ctx)?;
            if !p.is_boolean() {
                return Err(QsdbError::type_err(format!(
                    "FILTER (WHERE …) 的谓词必须是布尔，实得 {:?}",
                    p.data_type()
                )));
            }
            Some(p)
        }
    };
    // `count(DISTINCT x)` 的**显示名**必须与 `count(x)` 不同：`collect_aggs` 按
    // `out_name` 去重，同名会把语义不同的两个聚合合并成一个（少算一列）。
    // PG/DuckDB 对这类列名同样回 `count(DISTINCT x)`。
    //
    // ⚠️ **FILTER 同理、且更险**：`count(*) FILTER (WHERE a>1)` 与
    // `count(*) FILTER (WHERE a<5)` 若都叫 `count(*)`，会被去重成**同一个聚合**
    // ⇒ 两列拿到**同一个数字**，而那个数字只对其中一个谓词成立。
    // 与窗口帧那次的 `window_spec_sig` 是同一类坑（去重键漏了语义维度）。
    let mut out_name = format!(
        "{}({}{})",
        name,
        if distinct { "DISTINCT " } else { "" },
        arg.as_ref()
            .map(|a| a.display())
            .unwrap_or_else(|| "*".into())
    );
    if let Some(p) = &filter {
        out_name.push_str(&format!(" FILTER (WHERE {})", p.display()));
    }
    let mut agg = AggExpr::new(func, arg, out_name)?;
    agg.distinct = distinct;
    agg.filter = filter;
    Ok(Some(agg))
}

/// 聚合参数上的 `DISTINCT` → 等价改写：**先去重，再跑原聚合**。
///
/// ```text
/// count(DISTINCT x)              → aggregate{ aggs: [count(*)],  input: Distinct{[x]} }
/// count(DISTINCT x) GROUP BY g   → aggregate{ group:[g], aggs:[count(x)],
///                                             input: Distinct{[g, x]} }
/// ```
///
/// ⚠️ **NULL 语义——这里是最容易写错的地方**，两处必须分开处理：
///
/// * **无分组**：`count(DISTINCT x)` 跳过 NULL，`count(*)` 不跳，两者差一个
///   "NULL 组"。所以在去重**之前**先滤掉 `x IS NULL`，此时 `count(*)` = 不同非空
///   x 的个数 ✓（顺带把 `count(x)` 收紧成 `count(*)`，让执行层的融合计数节点
///   能认出来）。`sum/avg/min/max` 同样忽略 NULL，滤掉不影响 ✓。
/// * **有分组**：**绝不能过滤** —— 某个组可能只有 NULL 的 x，滤掉会让**整组消失**
///   （正确结果是该组回 0，而不是没有这一行；实测 `GROUP BY` 全空组会漏行）。
///   这里保留 `count(x)`：Distinct 把 NULL 当独立值，所以每组的 NULL 自成一行，
///   `count(x)` 数不到它 —— 逐值等价 ✓。
///
/// 只支持「**全部**聚合都带 DISTINCT 且参数表达式相同」：混用时一次去重会同时
/// 改掉那些**不带** DISTINCT 的聚合的输入（`sum(y)` 会少算行），所以显式拒绝而不是硬猜。
fn rewrite_distinct_aggs(
    input: LogicalPlan,
    group: &[PhysExpr],
    aggs: &[AggExpr],
) -> Result<(LogicalPlan, Vec<PhysExpr>, Vec<AggExpr>)> {
    if !aggs.iter().any(|a| a.distinct) {
        return Ok((input, group.to_vec(), aggs.to_vec()));
    }
    // **`DISTINCT` + `FILTER` 同时写**：去重改写会把输入换成 `Distinct{[分组键…, 参数]}`，
    // 那时 FILTER 谓词引用的列**已经不在输入里了**（谓词可以引用任何列，不只是参数）。
    // 硬做需要把谓词引用的列也塞进去重键 —— 那会**改变去重语义**（多一个列就多一组，
    // `count(DISTINCT x)` 的个数会变多）。所以显式拒，绝不静默丢弃谓词。
    if let Some(bad) = aggs.iter().find(|a| a.distinct && a.filter.is_some()) {
        return Err(QsdbError::unsupported(format!(
            "`{}` —— `DISTINCT` 与 `FILTER` 同时写暂不支持：去重改写会换掉输入，\
             FILTER 谓词引用的列就不在了。请拆成子查询（先 WHERE 再 DISTINCT）",
            bad.out_name
        )));
    }
    // ① 组合校验（宁可报错，也不猜语义）。
    if let Some(bad) = aggs.iter().find(|a| !a.distinct) {
        return Err(QsdbError::unsupported(format!(
            "mixing DISTINCT and non-DISTINCT aggregates is not supported yet: a single \
             de-duplication would also change the input of `{}`",
            bad.out_name
        )));
    }
    let key = aggs[0]
        .arg
        .as_ref()
        .ok_or_else(|| QsdbError::invalid("DISTINCT aggregate requires an argument"))?;
    if let Some(other) = aggs.iter().find(|a| {
        !a.arg
            .as_ref()
            .is_some_and(|e| e.display() == key.display() && e.data_type() == key.data_type())
    }) {
        return Err(QsdbError::unsupported(format!(
            "DISTINCT aggregates over different arguments are not supported yet \
             (`{}` vs `{}`)",
            aggs[0].out_name, other.out_name
        )));
    }
    // ② 去重输入 = [分组键…, 去重参数]（列序即位置，上层按位置引用）。
    let n_g = group.len();
    let mut src = input;
    if n_g == 0 {
        // 见上面 NULL 语义：无分组才允许过滤
        src = LogicalPlan::Filter {
            input: Box::new(src),
            predicate: PhysExpr::IsNull(Box::new(key.clone()), true),
        };
    }
    let mut exprs: Vec<PhysExpr> = group.to_vec();
    let mut names: Vec<String> = group.iter().map(|g| g.display()).collect();
    exprs.push(key.clone());
    names.push(key.display());
    let src = LogicalPlan::Distinct {
        input: Box::new(LogicalPlan::Project {
            input: Box::new(src),
            exprs,
            names,
        }),
    };
    // ③ 聚合改写成对「去重输出列」的位置引用。列名沿用原 `display()` ——
    // 聚合输出 schema 与下游重绑（`rebind_over_agg`）全都按**名字**对齐，
    // 名字一改，下游的 HAVING/投影就找不到列了。
    let plan_group: Vec<PhysExpr> = group
        .iter()
        .enumerate()
        .map(|(i, g)| PhysExpr::Column {
            idx: i,
            name: g.display(),
            ty: g.data_type(),
        })
        .collect();
    let plan_aggs: Vec<AggExpr> = aggs
        .iter()
        .map(|a| AggExpr {
            // 无分组时去重前已滤掉 NULL ⇒ `count(x)` 与 `count(*)` 等价，
            // 收成 `count(*)` 让执行层的融合计数节点认得出来（见 `DistinctCountExec`）。
            func: a.func,
            arg: if n_g == 0 && a.func == AggFunc::Count {
                None
            } else {
                Some(PhysExpr::Column {
                    idx: n_g,
                    name: key.display(),
                    ty: key.data_type(),
                })
            },
            out_name: a.out_name.clone(),
            out_ty: a.out_ty.clone(),
            udaf: a.udaf.clone(),
            distinct: false,
            // DISTINCT + FILTER 已在上面显式拒 ⇒ 走到这里必然无 FILTER
            filter: None,
        })
        .collect();
    Ok((src, plan_group, plan_aggs))
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
/// 这个（子）表达式**就是**某个 GROUP BY 键吗？是则给出它在键里的位置。
///
/// **判据必须两边都绑定成 `PhysExpr` 再比 `display()`**（同一个渲染器）。
/// 2026-10-15 修：此前拿 AST 的 `to_string()`（`expr_display`）去比
/// `PhysExpr::display()` —— 后者带括号（`(g + 1)` vs `g + 1`）⇒ **永远比不中**，
/// 于是**所有表达式分组键**（`GROUP BY upper(s)` / `GROUP BY v % 2` /
/// `GROUP BY CAST(v AS …)` / `GROUP BY CASE WHEN …` …）都掉进递归、最后报
/// "列 's' 不在 GROUP BY 里" —— 把"没实现"说成了"你写错了"（F29 同一族）。
///
/// 绑定失败（例如子树里含聚合）⇒ `None`，交给调用方的递归继续走。
fn group_key_pos(e: &sqlparser::ast::Expr, ctx: &SchemaCtx, group: &[PhysExpr]) -> Option<usize> {
    let bound = bind_expr(e, ctx).ok()?;
    let d = bound.display();
    group.iter().position(|g| g.display() == d)
}

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
            // 整个函数调用**就是**某个分组键（`GROUP BY upper(s)`）⇒ 直接引用键。
            // 不先判这一下就会递归进参数，最后报"列 's' 不在 GROUP BY 里"。
            if let Some(gi) = group_key_pos(e, ctx, group) {
                return Ok(PhysExpr::Column {
                    idx: gi,
                    name: group[gi].display(),
                    ty: group[gi].data_type(),
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
                // **F29②（qsdb-sync 实测）** 的旧痕已消：`GROUP BY 1` 当时报的是
                // "列不在 GROUP BY 里"（把"没实现"说成"你写错了"）。序号现在**已实现**
                //（`group_key_expr`，绑定前就换算成投射表达式）⇒ 走到这里的裸整数
                // 只可能是"投射那一列本身就是常量"（如 `SELECT 1, x … GROUP BY 1`），
                // 此时"x 不在 GROUP BY 里"是**准确**的报错，直接给通用文案。
                None => Err(QsdbError::invalid(format!(
                    "column '{}' must appear in GROUP BY or inside an aggregate",
                    id.value
                ))),
            }
        }
        // 复合表达式：先按**绑定结果**比分组键，再退回**源文本**比。
        other => {
            if let Some(gi) = group_key_pos(other, ctx, group) {
                return Ok(PhysExpr::Column {
                    idx: gi,
                    name: group[gi].display(),
                    ty: group[gi].data_type(),
                });
            }
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
