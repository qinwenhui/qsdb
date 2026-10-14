//! **常用 join 形态**的现状探针（一次性摸底，摸完把结论写进设计文档）。
//!
//! 为什么不能只看值：**叉积与 hash join 结果逐行相同**。`ON k = k AND v > 250`
//! 这种"等值 + 残留谓词"若被拆错、整条退化成叉积，**结果一个不差**，只是从
//! 毫秒变分钟（`complex_three_way` 113× 就是这个家族）。所以每条形态同时看
//! **值**与**计划形状**（`Join` 节点数 vs `CrossJoinFilter` 节点数）。
//!
//! 本文件是**摸底**用的：它把现状原样打出来，最后统一报告缺口。

use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use qsdb::sql::translate::{self, LogicalPlan};
use qsdb::{Qsdb, QsdbOptions};

fn tmp_root(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join("qsdb_join_common").join(format!(
        "{tag}_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&d).unwrap();
    d
}

/// `l(k, m)` = (1,10) (2,20) (3,30)；`r(k, m, v)` = (2,20,200) (3,35,300) (4,40,400)。
///
/// 刻意让**复合键**与**单键**给出不同答案（`k=3` 的 m 两侧不等）——
/// 否则"复合键只用了第一个"这种错会与正确答案重合。
fn seeded(tag: &str) -> (Qsdb, std::path::PathBuf) {
    let root = tmp_root(tag);
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    db.sql("CREATE TABLE l (k BIGINT, m BIGINT)").unwrap();
    db.sql("CREATE TABLE r (k BIGINT, m BIGINT, v BIGINT)")
        .unwrap();
    let sl = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("m", DataType::Int64, true),
    ]));
    let sr = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("m", DataType::Int64, true),
        Field::new("v", DataType::Int64, true),
    ]));
    db.append(
        "l",
        &[RecordBatch::try_new(
            Arc::clone(&sl),
            vec![
                Arc::new(Int64Array::from(vec![1i64, 2, 3])),
                Arc::new(Int64Array::from(vec![10i64, 20, 30])),
            ],
        )
        .unwrap()],
    )
    .unwrap();
    db.append(
        "r",
        &[RecordBatch::try_new(
            Arc::clone(&sr),
            vec![
                Arc::new(Int64Array::from(vec![2i64, 3, 4])),
                Arc::new(Int64Array::from(vec![20i64, 35, 40])),
                Arc::new(Int64Array::from(vec![200i64, 300, 400])),
            ],
        )
        .unwrap()],
    )
    .unwrap();
    (db, root)
}

fn plan_of(db: &Qsdb, sql: &str) -> Result<LogicalPlan, String> {
    let mut stmts = translate::parse_statements(sql).map_err(|e| e.to_string())?;
    let translate::ParsedStatement::Select(q) = stmts.remove(0) else {
        return Err("不是 SELECT".into());
    };
    let resolver = translate::TableResolver {
        resolve: Box::new(|n: &str| db.table_def(n)),
        views: Default::default(),
    };
    translate::translate_select_full(*q, &resolver).map_err(|e| e.to_string())
}

fn count_nodes(p: &LogicalPlan, join: &mut usize, cross: &mut usize) {
    match p {
        LogicalPlan::Join { left, right, .. } => {
            *join += 1;
            count_nodes(left, join, cross);
            count_nodes(right, join, cross);
        }
        LogicalPlan::CrossJoinFilter { left, right, .. } => {
            *cross += 1;
            count_nodes(left, join, cross);
            count_nodes(right, join, cross);
        }
        LogicalPlan::Project { input, .. }
        | LogicalPlan::Filter { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Distinct { input }
        | LogicalPlan::Window { input, .. }
        | LogicalPlan::Aggregate { input, .. } => count_nodes(input, join, cross),
        LogicalPlan::SetOp { left, right, .. } => {
            count_nodes(left, join, cross);
            count_nodes(right, join, cross);
        }
        _ => {}
    }
}

fn value_of(db: &Qsdb, sql: &str) -> Result<i64, String> {
    match db.sql(sql) {
        Ok(qsdb::sql::SqlResult::Query { batches, .. }) => batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("count 是 Int64")
                    .values()
                    .to_vec()
            })
            .next()
            .ok_or_else(|| "零行".to_string()),
        Ok(other) => Err(format!("非查询结果 {other:?}")),
        Err(e) => Err(format!("{e}")),
    }
}

/// 一条形态的摸底记录：值（或错误）+ 计划形状。
struct Row {
    name: &'static str,
    sql: &'static str,
    expect: i64,
    /// 期望的哈希 join 节点数（`None` = 不改计划形状要求）
    want_hash: Option<usize>,
}

#[test]
fn 常用_join_形态摸底() {
    let (db, root) = seeded("probe");
    let cases = [
        Row {
            name: "INNER 单等值",
            sql: "SELECT count(*) FROM l JOIN r ON l.k = r.k",
            expect: 2,
            want_hash: Some(1),
        },
        Row {
            name: "INNER 复合等值(k,m)",
            sql: "SELECT count(*) FROM l JOIN r ON l.k = r.k AND l.m = r.m",
            expect: 1,
            want_hash: Some(1),
        },
        Row {
            name: "INNER 等值+残留谓词",
            sql: "SELECT count(*) FROM l JOIN r ON l.k = r.k AND r.v > 250",
            expect: 1,
            want_hash: Some(1),
        },
        Row {
            name: "LEFT 单等值",
            sql: "SELECT count(*) FROM l LEFT JOIN r ON l.k = r.k",
            expect: 3,
            want_hash: Some(1),
        },
        Row {
            name: "LEFT 等值+残留谓词",
            sql: "SELECT count(*) FROM l LEFT JOIN r ON l.k = r.k AND r.v > 250",
            expect: 3, // 残留只作用于**匹配对**：k=1 无匹配、k=2 被残留否掉 → 两行补 NULL
            want_hash: Some(1),
        },
        Row {
            name: "LEFT 复合等值",
            sql: "SELECT count(*) FROM l LEFT JOIN r ON l.k = r.k AND l.m = r.m",
            expect: 3,
            want_hash: Some(1),
        },
        Row {
            name: "RIGHT 单等值",
            sql: "SELECT count(*) FROM l RIGHT JOIN r ON l.k = r.k",
            expect: 3, // r 的 k=4 未匹配 → 补 NULL 保留
            want_hash: None,
        },
        Row {
            name: "FULL 单等值",
            sql: "SELECT count(*) FROM l FULL OUTER JOIN r ON l.k = r.k",
            expect: 4, // 两侧孤儿各一行
            want_hash: None,
        },
        Row {
            name: "CROSS JOIN",
            sql: "SELECT count(*) FROM l CROSS JOIN r",
            expect: 9,
            want_hash: None,
        },
        Row {
            name: "USING(k)",
            sql: "SELECT count(*) FROM l JOIN r USING (k)",
            expect: 2,
            want_hash: Some(1),
        },
    ];

    let mut gaps = Vec::new();
    eprintln!("\n{:<26} {:>6} {:>6}   计划(Join/Cross)", "形态", "期望", "实测");
    for c in &cases {
        let v = value_of(&db, c.sql);
        let shape = match plan_of(&db, c.sql) {
            Ok(p) => {
                let (mut j, mut x) = (0, 0);
                count_nodes(&p, &mut j, &mut x);
                format!("{j}/{x}")
            }
            Err(_) => "-".into(),
        };
        let got = match &v {
            Ok(n) => format!("{n}"),
            Err(_) => "ERR".into(),
        };
        eprintln!(
            "{:<26} {:>6} {:>6}   {}",
            c.name, c.expect, got, shape
        );
        // 值对不上 / 报错 / 该走哈希却退化成叉积 —— 三类都算缺口
        let ok_value = matches!(v, Ok(n) if n == c.expect);
        let ok_shape = match c.want_hash {
            Some(want) => shape == format!("{want}/0"),
            None => true,
        };
        if !ok_value || !ok_shape {
            gaps.push(format!(
                "{}：期望 {}，实测 {}（{}{}）",
                c.name,
                c.expect,
                got,
                match &v {
                    Err(e) => e.clone(),
                    Ok(_) => String::new(),
                },
                if ok_shape {
                    String::new()
                } else {
                    format!("；计划形状 {shape}，期望 {}/0", c.want_hash.unwrap())
                }
            ));
        }
    }
    eprintln!("\n=== 缺口 {} 条 ===", gaps.len());
    for g in &gaps {
        eprintln!("- {g}");
    }
    let _ = std::fs::remove_dir_all(&root);
    // 摸底阶段不断言（缺口是**产出**，不是失败）；跑完把结论写进设计文档
}
