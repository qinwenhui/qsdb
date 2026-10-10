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
        n: usize,
    },
    Distinct {
        input: Box<LogicalPlan>,
    },
    /// 窗口函数：PARTITION BY + ORDER BY + 窗口函数列表
    Window {
        input: Box<LogicalPlan>,
        partition_by: Vec<PhysExpr>,
        order_by: Vec<(PhysExpr, bool)>,
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
    },
}

// ---------------------------------------------------------------------------
// 语句解析
// ---------------------------------------------------------------------------

/// INSERT VALUES 行集（每行按给定列序的字面量）
pub type InsertRows = Vec<Vec<Literal>>;

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
    ShowTables,
    ShowPartitions {
        table: String,
    },
    Insert {
        table: String,
        rows: Vec<Vec<Literal>>,
        /// Some(列名顺序) = INSERT INTO t (a, b, …)；None = 表全列序
        column_order: Option<Vec<String>>,
        overwrite: bool,
    },
    Select(Box<sqlparser::ast::Query>),
    /// DELETE FROM t WHERE <仅分区键谓词>（分区粒度原子清空）
    Delete {
        table: String,
        selection: Option<String>,
    },
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

fn translate_statement(stmt: SqlStatement, raw: &str) -> Result<ParsedStatement> {
    match stmt {
        SqlStatement::CreateTable(ct) => {
            let table = object_name_single(ct.name.clone())?;
            let mut defs = Vec::with_capacity(ct.columns.len());
            for c in &ct.columns {
                let ty = ast_type_to_logical(&c.data_type)?;
                let mut nullable = true;
                for opt in &c.options {
                    if matches!(opt.option, sqlparser::ast::ColumnOption::NotNull) {
                        nullable = false;
                    }
                }
                defs.push(super::make_column(&c.name.value, ty, nullable));
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
            let _ = ct.if_not_exists; // 建表冲突按 AlreadyExists 报错
            Ok(ParsedStatement::CreateTable { def })
        }
        SqlStatement::Drop {
            object_type,
            if_exists,
            names,
            ..
        } => {
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
            let (rows, _) = extract_insert_rows(ins.source)?;
            Ok(ParsedStatement::Insert {
                table,
                rows,
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
// INSERT VALUES
// ---------------------------------------------------------------------------

fn extract_insert_rows(
    query: Option<Box<sqlparser::ast::Query>>,
) -> Result<(InsertRows, Option<Vec<String>>)> {
    let Some(query) = query else {
        return Err(QsdbError::invalid("INSERT requires VALUES source"));
    };
    match *query.body {
        SetExpr::Values(values) => {
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
            Ok((rows, None))
        }
        other => Err(QsdbError::unsupported(format!(
            "INSERT source {other:?} not supported (use VALUES)"
        ))),
    }
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
        V::Boolean(b) => Ok(Literal::Boolean(*b)),
        V::Null => Ok(Literal::Null),
        other => Err(QsdbError::unsupported(format!(
            "literal {other:?} not supported"
        ))),
    }
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
        LogicalPlan::Join { .. } | LogicalPlan::CrossJoinFilter { .. } => {
            return Err(QsdbError::unsupported(
                "CTE output from JOIN not inferable in binder",
            ));
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
}

/// CTE 物化定义（执行层按 id 填充批数据）。
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
) -> Result<(LogicalPlan, Vec<CteDef>)> {
    use sqlparser::ast::*;

    let select = match *q.body {
        SetExpr::Select(s) => s,
        other => {
            return Err(QsdbError::unsupported(format!(
                "query body {other:?} not supported (plain SELECT only)"
            )));
        }
    };
    if select.from.len() != 1 {
        return Err(QsdbError::unsupported(
            "multi-FROM (comma join) not supported",
        ));
    }
    let table = match &select.from[0].relation {
        TableFactor::Table { name, .. } => object_name_single(name.clone())?,
        other => {
            return Err(QsdbError::unsupported(format!(
                "FROM {other:?} not supported"
            )));
        }
    };
    // CTE（非递归）：WITH name AS (...) 内联展开为子计划
    let mut cte_plans: std::collections::HashMap<
        String,
        (LogicalPlan, Vec<(String, LogicalType)>),
    > = std::collections::HashMap::new();
    let mut cte_defs: Vec<CteDef> = Vec::new();
    if let Some(with) = &q.with {
        if with.recursive {
            return Err(QsdbError::unsupported("recursive CTE"));
        }
        let mut defs: Vec<CteDef> = Vec::new();
        for cte in &with.cte_tables {
            let name = cte.alias.name.value.clone();
            if cte_plans.contains_key(&name) || (resolver.resolve)(&name).is_ok() {
                return Err(QsdbError::unsupported("recursive or duplicate CTE"));
            }
            let sub = translate_select_full(*cte.query.clone(), resolver)?;
            let cols = plan_output_columns(&sub)?;
            cte_plans.insert(name, (sub.clone(), cols.clone()));
            defs.push(CteDef {
                cte_id: u32::MAX, // 哨兵：主查询 FROM 未引用时保持 MAX
                columns: cols,
                plan: sub,
            });
        }
        cte_defs = defs;
    }

    // FROM 主表：CTE 名优先
    let (mut plan, mut ctx): (LogicalPlan, SchemaCtx) =
        if let Some((_sub, cols)) = cte_plans.get(&table) {
            let id = next_cte_id();
            // 同步 CteDef 的 id（执行层按 id 匹配；用列名匹配定位）
            if let Some(d) = cte_defs
                .iter_mut()
                .find(|d| d.columns.as_slice() == cols.as_slice())
            {
                d.cte_id = id;
            }
            (
                LogicalPlan::Values {
                    columns: cols.clone(),
                    cte_id: id,
                },
                SchemaCtx { cols: cols.clone() },
            )
        } else {
            let def = (resolver.resolve)(&table)?;
            (
                LogicalPlan::Scan {
                    def: Arc::clone(&def),
                    predicate: None,
                },
                SchemaCtx::from_table(&def),
            )
        };

    // JOIN 链（SQL-2：等值 INNER/LEFT）
    for join in &select.from[0].joins {
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
        let right_name = match &join.relation {
            TableFactor::Table { name, alias, .. } => match alias {
                Some(a) => a.name.value.clone(),
                None => object_name_single(name.clone())?,
            },
            other => return Err(QsdbError::unsupported(format!("JOIN target {other:?}"))),
        };
        let right_def = (resolver.resolve)(&right_name)?;
        let sqlparser::ast::JoinConstraint::On(on_expr) = constraint else {
            return Err(QsdbError::unsupported("JOIN ... ON <equi-cond> required"));
        };
        // ctx 扩展：右表列仅以限定名注册（table.col），顺序 = join 输出右段。
        // 非限定右列引用不支持（SQL-2 约束，歧义保护）。
        let n_left_plain = ctx.cols.iter().filter(|(n, _)| !n.contains('.')).count();
        for c in &right_def.columns {
            ctx.cols
                .push((format!("{right_name}.{}", c.name), c.logical_type.clone()));
        }
        match split_equi_condition(on_expr, n_left_plain, &ctx) {
            Ok((lkey, rkey)) => {
                plan = LogicalPlan::Join {
                    left: Box::new(plan),
                    right: Box::new(LogicalPlan::Scan {
                        def: Arc::clone(&right_def),
                        predicate: None,
                    }),
                    left_keys: vec![lkey],
                    right_keys: vec![rkey],
                    join_type: jt,
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
                    right: Box::new(LogicalPlan::Scan {
                        def: Arc::clone(&right_def),
                        predicate: None,
                    }),
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

    // WHERE
    if let Some(w) = select.selection {
        let bound = bind_expr(&w, &ctx)?;
        if !bound.is_boolean() {
            return Err(QsdbError::type_err("WHERE must be boolean"));
        }
        // Values（CTE）不可下推（无 Scan 节点）→ 全部留作行过滤
        let is_values = matches!(plan, LogicalPlan::Values { .. });
        let (pushdown, residual) = if is_values {
            (None, Some(bound))
        } else {
            split_pushdown(&bound)
        };
        if let Some(p) = pushdown {
            if let LogicalPlan::Scan { predicate, .. } = &mut plan {
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
    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(e) => items.push(ProjItem {
                sql: e.clone(),
                name: expr_display(e),
            }),
            SelectItem::ExprWithAlias { expr, alias } => items.push(ProjItem {
                sql: expr.clone(),
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
        let pairs: Vec<(Expr, String)> = items
            .iter()
            .map(|it| (it.sql.clone(), it.name.clone()))
            .collect();
        let substituted = substitute_aliases(h, &pairs);
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
            let (key_expr, descending) = {
                let desc = matches!(&ob.options.sort, Some(sqlparser::ast::OrderBySort::Desc));
                (&ob.expr, desc)
            };
            // 别名/输出名优先：Identifier 查输出名；其他表达式按 display 匹配投影
            let by_output = match key_expr {
                Expr::Identifier(id) => out_ctx.index_of(&id.value),
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
                    });
                }
            }
        }
        // 仅当追加了隐藏排序列时才替换顶层投影（DISTINCT 无隐藏列时保持原样）
        if extra_exprs.len() > proj_exprs_len_before(&plan) {
            plan = replace_top_project(plan, extra_exprs, extra_names);
        }
        plan = LogicalPlan::Sort {
            input: Box::new(plan),
            keys: sort_keys,
        };
    }

    // LIMIT
    if let Some(limit_clause) = &q.limit_clause {
        let limit_expr: Option<&sqlparser::ast::Expr> = match limit_clause {
            sqlparser::ast::LimitClause::LimitOffset { limit, .. } => limit.as_ref(),
            sqlparser::ast::LimitClause::OffsetCommaLimit { limit, .. } => Some(limit),
        };
        if let Some(limit_expr) = limit_expr {
            let lit = constant_expr(limit_expr)?;
            let n = match lit {
                Literal::Int64(v) if v >= 0 => v as usize,
                _ => return Err(QsdbError::invalid("LIMIT expects a non-negative integer")),
            };
            plan = LogicalPlan::Limit {
                input: Box::new(plan),
                n,
            };
        }
    }

    Ok((plan, cte_defs))
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
    if let Some(sqlparser::ast::WindowType::WindowSpec(spec)) = &f.over {
        let mut partition_by = Vec::new();
        for p in &spec.partition_by {
            partition_by.push(bind(p)?);
        }
        let mut order_by = Vec::new();
        for ob in &spec.order_by {
            let desc = matches!(&ob.options.sort, Some(sqlparser::ast::OrderBySort::Desc));
            order_by.push((bind(&ob.expr)?, desc));
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
    use sqlparser::ast::Expr as E;
    match e {
        E::Identifier(id) => {
            let idx = ctx
                .index_of(&id.value)
                .ok_or_else(|| QsdbError::not_found(format!("column '{}'", id.value)))?;
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
                .ok_or_else(|| QsdbError::not_found(format!("column '{}'", parts[1].value)))?;
            Ok(PhysExpr::Column {
                idx,
                name: ctx.cols[idx].0.clone(),
                ty: ctx.cols[idx].1.clone(),
            })
        }
        E::Value(v) => Ok(PhysExpr::Lit(value_to_literal(v)?)),
        E::TypedString(_) => {
            let lit = constant_expr(e)?;
            let ty = match &lit {
                Literal::Date(_) => LogicalType::Date,
                _ => LogicalType::Timestamp { timezone: None },
            };
            Ok(PhysExpr::Lit(lit).cast_to(ty))
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
        E::IsNull(inner) => Ok(PhysExpr::IsNull(Box::new(bind_expr(inner, ctx)?), false)),
        E::IsNotNull(inner) => Ok(PhysExpr::IsNull(Box::new(bind_expr(inner, ctx)?), true)),
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

fn bind_binary_arith(op: ArithOp, l: PhysExpr, r: PhysExpr) -> Result<PhysExpr> {
    let lt = l.data_type();
    let rt = r.data_type();
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
                ArithOp::Mod => return Err(QsdbError::unsupported("decimal % operator")),
            }?;
            (Lt::Decimal(m), l, r) // decimal 路径 eval 内部对齐 scale
        }
        (Lt::Decimal(a), x) | (x, Lt::Decimal(a)) if x.is_integer() => {
            let b = DecimalMeta::try_new(19, 0)?;
            let m = match op {
                ArithOp::Add | ArithOp::Sub => DecimalMeta::add_result(*a, b),
                ArithOp::Mul => DecimalMeta::mul_result(*a, b),
                ArithOp::Div => DecimalMeta::div_result(*a, b),
                ArithOp::Mod => return Err(QsdbError::unsupported("decimal % operator")),
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

fn fn_name(f: &sqlparser::ast::Function) -> String {
    f.name.to_string().to_ascii_lowercase()
}

/// 聚合函数名。
fn is_agg_name(name: &str) -> bool {
    matches!(name, "count" | "sum" | "min" | "max" | "avg")
}

fn try_bind_agg(f: &sqlparser::ast::Function, ctx: &SchemaCtx) -> Result<Option<AggExpr>> {
    let name = fn_name(f);
    if !is_agg_name(&name) {
        return Ok(None);
    }
    if f.over.is_some() {
        return Ok(None); // 窗口函数不作为聚合处理（由 collect_window_calls / 投影置换接管）
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
    let def = scalar::lookup(&name)?;
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
    let arg_tys: Vec<LogicalType> = args.iter().map(|a| a.data_type()).collect();
    let ty = (def.ret_ty)(&arg_tys)?;
    // 参数隐式 cast（int→Int64 / float→Float64）
    let args = args
        .into_iter()
        .enumerate()
        .map(|(i, a)| {
            let want = arg_tys.get(i).cloned().unwrap_or(LogicalType::Int64);
            let _ = want;
            let target = match &arg_tys[i] {
                LogicalType::Int8
                | LogicalType::Int16
                | LogicalType::Int32
                | LogicalType::Int64 => LogicalType::Int64,
                LogicalType::Float32 | LogicalType::Float64 => LogicalType::Float64,
                t => t.clone(),
            };
            Ok(a.cast_to(target))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(PhysExpr::Fn { name, args, ty })
}

fn bind_scalar_fn_over(
    _e: &sqlparser::ast::Expr,
    _f: &sqlparser::ast::Function,
    _ctx: &SchemaCtx,
    _group: &[PhysExpr],
    _aggs: &[AggExpr],
    _agg_ctx: &SchemaCtx,
) -> Result<PhysExpr> {
    Err(QsdbError::unsupported(
        "scalar functions over aggregates (e.g. upper(sum(x))) land in M4",
    ))
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
