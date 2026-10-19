//! **列别名清单**（十九批 S2）的值级验收：`WITH c(a,b) AS …` /
//! `FROM (…) AS d(a,b)` / `FROM t AS x(a,b)` / JOIN 右侧同款。
//!
//! 语义 = **按位置重命名**（第 i 个别名接管第 i 个输出列的名字，值/类型不变），
//! 在**定义处**做一次。边界显式拒：数不符、重名、带类型。

use std::sync::Arc;

use arrow::array::{Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use qsdb::{Qsdb, QsdbOptions};

/// 测试目录唯一序号：pid 挡跨进程碰撞，序号挡同进程内并发（Windows 时钟
/// 粒度 ~100ns，纳秒后缀在同 tick 内会重复——CI 上真炸过，见 constraints）。
fn _uniq_seq() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    SEQ.fetch_add(1, Ordering::Relaxed)
}

fn tmp_root(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join("qsdb_cte_alias").join(format!(
        "{tag}_{:x}_{}",
        std::process::id(),
        _uniq_seq()
    ));
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn seeded(tag: &str) -> (Qsdb, std::path::PathBuf) {
    let root = tmp_root(tag);
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    db.sql("CREATE TABLE t (g VARCHAR, v BIGINT)").unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("g", DataType::Utf8, true),
        Field::new("v", DataType::Int64, true),
    ]));
    db.append(
        "t",
        &[RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("a"), Some("b")])),
                Arc::new(Int64Array::from(vec![1i64, 2])),
            ],
        )
        .unwrap()],
    )
    .unwrap();
    (db, root)
}

/// 查询结果的 `(列名, 整数首列值)`。
fn names_and_first_i64(db: &Qsdb, sql: &str) -> (Vec<String>, Vec<Option<i64>>) {
    let r = db
        .sql(sql)
        .unwrap_or_else(|e| panic!("`{sql}` 应当成功：{e}"));
    let qsdb::sql::SqlResult::Query {
        schema, batches, ..
    } = r
    else {
        panic!("`{sql}` 应当是查询结果");
    };
    let names = schema.fields().iter().map(|f| f.name().clone()).collect();
    let mut vals = Vec::new();
    for b in &batches {
        let a = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..a.len() {
            vals.push(if a.is_null(i) { None } else { Some(a.value(i)) });
        }
    }
    (names, vals)
}

#[test]
fn cte_列别名_名字换值不变() {
    let (db, root) = seeded("cte");
    let (names, v) = names_and_first_i64(
        &db,
        "WITH c (gg, vv) AS (SELECT g, v FROM t) SELECT vv, gg FROM c ORDER BY vv",
    );
    assert_eq!(names, vec!["vv", "gg"], "输出名 = 别名");
    assert_eq!(v, vec![Some(1), Some(2)], "值不变");
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn cte_列别名_where与二次引用() {
    let (db, root) = seeded("cte2");
    let (_, v) = names_and_first_i64(
        &db,
        "WITH c (gg) AS (SELECT v FROM t) SELECT gg FROM c WHERE gg = 2",
    );
    assert_eq!(v, vec![Some(2)], "别名在 WHERE 里可见");
    // 同一 CTE 引用两次 —— 重命名做一次，处处生效
    let (names, _) = names_and_first_i64(
        &db,
        "WITH c (x, y) AS (SELECT g, v FROM t) SELECT a.y FROM c a JOIN c b ON a.x = b.x",
    );
    assert_eq!(names, vec!["y"]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn 派生表列别名() {
    let (db, root) = seeded("derived");
    let (names, v) = names_and_first_i64(
        &db,
        "SELECT x.b FROM (SELECT g, v FROM t) AS x (a, b) ORDER BY x.b",
    );
    assert_eq!(names, vec!["b"]);
    assert_eq!(v, vec![Some(1), Some(2)]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn 实表列别名() {
    let (db, root) = seeded("table");
    let (names, v) = names_and_first_i64(&db, "SELECT x.vv FROM t AS x (gg, vv) ORDER BY vv");
    assert_eq!(names, vec!["vv"]);
    assert_eq!(v, vec![Some(1), Some(2)]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn join_右侧派生表列别名() {
    let (db, root) = seeded("jr");
    let (names, v) = names_and_first_i64(
        &db,
        "SELECT d.b FROM t JOIN (SELECT g, v FROM t) d (a, b) ON t.g = d.a",
    );
    assert_eq!(names, vec!["b"]);
    assert_eq!(v, vec![Some(1), Some(2)], "join 右侧的别名列可裸名引用");
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn 边界_数不符_重复_带类型() {
    let (db, root) = seeded("edge");
    let e = db
        .sql("WITH c (gg) AS (SELECT g, v FROM t) SELECT gg FROM c")
        .unwrap_err();
    assert!(
        e.to_string().contains("1 个名字") && e.to_string().contains("2 列"),
        "数不符要两侧数字都给：{e}"
    );
    let e = db
        .sql("WITH c (gg, gg) AS (SELECT g, v FROM t) SELECT gg FROM c")
        .unwrap_err();
    assert!(e.to_string().contains("重复"), "重名要报：{e}");
    let e = db
        .sql("WITH c (gg VARCHAR) AS (SELECT g FROM t) SELECT gg FROM c")
        .unwrap_err();
    assert!(
        e.to_string().contains("类型"),
        "带类型的别名要拒（类型由查询输出决定）：{e}"
    );
    // 实表别名同口径
    let e = db.sql("SELECT x.gg FROM t AS x (gg)").unwrap_err();
    assert!(e.to_string().contains("1 个名字"), "实表别名数不符：{e}");
    let _ = std::fs::remove_dir_all(&root);
}
