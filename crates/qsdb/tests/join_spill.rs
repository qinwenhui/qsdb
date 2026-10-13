//! hash join 的 build 侧溢出（P2）验收：**分区块 hash join**。
//!
//! 右侧（build 侧）要物化整行建哈希索引——大维表 join 是分析场景里最经典的
//! OOM 形态。要不到额度时不能把查询打死，而应两侧按 join key 分区落盘、
//! 逐分区连接（同一键必然同分区，故分区内连接即完整）。
//!
//! 验收口径与 P1 一致：**极小配额 vs 不限内存，结果逐值相同**（含 LEFT OUTER
//! 的未匹配行）。

use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use qsdb::query::QueryContext;
use qsdb::{Qsdb, QsdbOptions};

fn tmp_root(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir()
        .join("qsdb_joinspill_test")
        .join(format!(
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

/// 建两表：`a`（探测侧）与 `b`（**build 侧**，右侧）。
///
/// `b` 刻意做成"大 + 多键"：build 侧要物化它建索引，小配额下必然撑不住。
/// `a` 里留一部分**匹配不到**的键，用来验证 LEFT OUTER 的未匹配行没丢。
fn seeded(tag: &str, rows: i64) -> (Qsdb, std::path::PathBuf) {
    let root = tmp_root(tag);
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    db.sql("CREATE TABLE a (g BIGINT, v BIGINT)").unwrap();
    db.sql("CREATE TABLE b (g BIGINT, name STRING)").unwrap();
    let sa = Arc::new(Schema::new(vec![
        Field::new("g", DataType::Int64, true),
        Field::new("v", DataType::Int64, true),
    ]));
    let sb = Arc::new(Schema::new(vec![
        Field::new("g", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
    ]));
    let per = 2000i64;
    let mut done = 0i64;
    while done < rows {
        let n = per.min(rows - done);
        // a 的键取**偶数**且域是 b 的两倍 → 约一半落在 b 的域外（LEFT OUTER 的
        // 未匹配行就靠它们；早先写成 `% (rows*2)` 时行数只有 rows、键全落在
        // b 的域内，一个未匹配行都没有，那条断言等于没测）
        let ga: Vec<i64> = (0..n).map(|i| ((done + i) * 2) % (rows * 2)).collect();
        let va: Vec<i64> = (0..n).map(|i| done + i).collect();
        db.append(
            "a",
            &[RecordBatch::try_new(
                Arc::clone(&sa),
                vec![
                    Arc::new(Int64Array::from(ga)),
                    Arc::new(Int64Array::from(va)),
                ],
            )
            .unwrap()],
        )
        .unwrap();
        let gb: Vec<i64> = (0..n).map(|i| (done + i) % rows).collect();
        let name: Vec<String> = (0..n).map(|i| format!("n{}", done + i)).collect();
        db.append(
            "b",
            &[RecordBatch::try_new(
                Arc::clone(&sb),
                vec![
                    Arc::new(Int64Array::from(gb)),
                    Arc::new(StringArray::from(name)),
                ],
            )
            .unwrap()],
        )
        .unwrap();
        done += n;
    }
    (db, root)
}

fn rows_of(db: &Qsdb, sql: &str, limit: Option<u64>) -> Vec<String> {
    let ctx = match limit {
        Some(l) => QueryContext::new("js", None, Some(l)),
        None => QueryContext::new("js-free", None, None),
    };
    let out = db
        .sql_with_context(sql, &ctx)
        .unwrap_or_else(|e| panic!("查询失败（limit={limit:?}）: {e}\nSQL: {sql}"));
    let batches = match out {
        qsdb::sql::SqlResult::Query { batches, .. } => batches,
        qsdb::sql::SqlResult::Ok { .. } => panic!("expected query"),
    };
    let mut v: Vec<String> = batches
        .iter()
        .flat_map(|b| {
            (0..b.num_rows())
                .map(|i| {
                    (0..b.num_columns())
                        .map(|c| {
                            arrow::util::display::array_value_to_string(b.column(c).as_ref(), i)
                                .unwrap_or_default()
                        })
                        .collect::<Vec<_>>()
                        .join("|")
                })
                .collect::<Vec<_>>()
        })
        .collect();
    v.sort();
    v
}

/// INNER：极小配额（必然走分区块溢出）结果必须与内存路径逐值相同。
#[test]
fn join_inner_results_identical_with_and_without_spill() {
    let (db, root) = seeded("inner", 20_000);
    let sql = "SELECT a.v, b.name FROM a JOIN b ON a.g = b.g";
    let free = rows_of(&db, sql, None);
    assert!(!free.is_empty(), "不限内存应有结果");
    let spilled = rows_of(&db, sql, Some(64 * 1024));
    assert_eq!(free.len(), spilled.len(), "行数必须一致（INNER）");
    assert_eq!(free, spilled, "INNER 溢出结果必须与内存路径逐值相同");
    let _ = std::fs::remove_dir_all(&root);
}

/// LEFT OUTER：未匹配的左行（补 NULL）也必须一条不少——这是分区块最容易漏的语义。
#[test]
fn join_left_outer_results_identical_with_and_without_spill() {
    let (db, root) = seeded("left", 20_000);
    let sql = "SELECT a.v, b.name FROM a LEFT JOIN b ON a.g = b.g";
    let free = rows_of(&db, sql, None);
    let spilled = rows_of(&db, sql, Some(64 * 1024));
    assert_eq!(free.len(), spilled.len(), "行数必须一致（LEFT）");
    assert_eq!(free, spilled, "LEFT OUTER 溢出结果必须与内存路径逐值相同");
    // 确有未匹配行（否则这个用例证明不了 LEFT 语义）
    assert!(
        free.iter().any(|r| r.ends_with("|")),
        "样本里应有未匹配的左行（补 NULL）"
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// 全局预算（而非每查询配额）触发时同样成立。
#[test]
fn join_spills_under_global_budget_too() {
    let root = tmp_root("global");
    let db = Qsdb::open(
        &root,
        QsdbOptions {
            memory_budget_bytes: Some(256 * 1024),
            scan_cache_bytes: 0,
            ..Default::default()
        },
    )
    .unwrap();
    db.sql("CREATE TABLE a (g BIGINT, v BIGINT)").unwrap();
    db.sql("CREATE TABLE b (g BIGINT, name STRING)").unwrap();
    let sa = Arc::new(Schema::new(vec![
        Field::new("g", DataType::Int64, true),
        Field::new("v", DataType::Int64, true),
    ]));
    let sb = Arc::new(Schema::new(vec![
        Field::new("g", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
    ]));
    for chunk in 0..10i64 {
        let n = 2000i64;
        db.append(
            "a",
            &[RecordBatch::try_new(
                Arc::clone(&sa),
                vec![
                    Arc::new(Int64Array::from(
                        (0..n)
                            .map(|i| (chunk * n + i) % 10_000)
                            .collect::<Vec<i64>>(),
                    )),
                    Arc::new(Int64Array::from(
                        (0..n).map(|i| chunk * n + i).collect::<Vec<i64>>(),
                    )),
                ],
            )
            .unwrap()],
        )
        .unwrap();
        db.append(
            "b",
            &[RecordBatch::try_new(
                Arc::clone(&sb),
                vec![
                    Arc::new(Int64Array::from(
                        (0..n)
                            .map(|i| (chunk * n + i) % 10_000)
                            .collect::<Vec<i64>>(),
                    )),
                    Arc::new(StringArray::from(
                        (0..n)
                            .map(|i| format!("n{}", chunk * n + i))
                            .collect::<Vec<String>>(),
                    )),
                ],
            )
            .unwrap()],
        )
        .unwrap();
    }
    // 256KB 预算远小于 build 侧需求 → 必须走分区块溢出完成
    let out = db
        .sql_collect("SELECT count(*) FROM a JOIN b ON a.g = b.g")
        .unwrap_or_else(|e| panic!("预算压力下应溢出完成，而不是失败: {e}"));
    let n: usize = out.iter().map(|b| b.num_rows()).sum();
    assert_eq!(n, 1, "应返回一行计数");
    let _ = std::fs::remove_dir_all(&root);
}
