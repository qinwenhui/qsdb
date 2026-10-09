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

/// 执行一条 SQL（含多条语句时只执行第一条并提示）。
pub fn execute_sql(db: &Qsdb, sql: &str) -> Result<SqlResult> {
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
    match stmt {
        translate::ParsedStatement::CreateTable { def } => {
            db.create_table(def)?;
            Ok(SqlResult::Ok {
                message: "table created".into(),
            })
        }
        translate::ParsedStatement::DropTable { name, if_exists } => match db.drop_table(&name) {
            Ok(()) => Ok(SqlResult::Ok {
                message: "table dropped".into(),
            }),
            Err(e) if if_exists && matches!(e, QsdbError::NotFound(_)) => Ok(SqlResult::Ok {
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
        translate::ParsedStatement::ShowPartitions { table } => {
            let def = db.table_def(&table)?;
            let tdir = crate::catalog::table_dir(db.root(), crate::catalog::DEFAULT_DB, &table);
            let parts = crate::scan::list_partitions(&tdir, def.partition_keys.len());
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
        translate::ParsedStatement::Select(q) => {
            let resolver = translate::TableResolver {
                resolve: Box::new(|name: &str| db.table_def(name)),
            };
            let plan = translate::translate_select_full(*q, &resolver)?;
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
            let mut exec = exec_builder::build(db, &plan)?;
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
    if def.partition_keys.is_empty() {
        return Ok(None);
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
                .ok_or_else(|| QsdbError::not_found(format!("column '{n}'")))
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
pub(crate) fn make_column(name: &str, ty: LogicalType, nullable: bool) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        logical_type: ty,
        nullable,
        default: None,
        comment: None,
    }
}
