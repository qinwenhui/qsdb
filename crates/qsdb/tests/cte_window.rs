//! CTE + 窗口函数测试（M5）。

use qsdb::{Qsdb, QsdbOptions};

fn setup(tag: &str) -> (Qsdb, std::path::PathBuf) {
    let root = std::env::temp_dir().join("qsdb_m5_test").join(format!(
        "{tag}_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    (db, root)
}

fn q(db: &Qsdb, sql: &str) -> Vec<String> {
    let batches = db.sql_collect(sql).unwrap();
    let mut out = Vec::new();
    for b in &batches {
        for i in 0..b.num_rows() {
            let mut cells = Vec::new();
            for c in 0..b.num_columns() {
                let arr = b.column(c);
                if arr.is_null(i) {
                    cells.push("NULL".into());
                } else {
                    cells.push(
                        arrow::util::display::array_value_to_string(arr.as_ref(), i).unwrap(),
                    );
                }
            }
            out.push(cells.join("|"));
        }
    }
    out.sort();
    out
}

#[test]
fn basic_cte() {
    let (db, root) = setup("cte");
    db.sql("CREATE TABLE sales (dt DATE, tenant STRING, amt BIGINT) PARTITIONED BY (dt)")
        .unwrap();
    db.sql(
        "INSERT INTO sales VALUES
        (DATE '2026-01-01', 'a', 10), (DATE '2026-01-01', 'b', 20),
        (DATE '2026-01-02', 'a', 30), (DATE '2026-01-02', 'c', 40)",
    )
    .unwrap();

    // 基础 CTE + 主查询过滤
    let r = q(
        &db,
        "WITH big AS (SELECT tenant, amt FROM sales WHERE amt > 15) SELECT tenant FROM big ORDER BY tenant",
    );
    // amt>15: b(20), a(30), c(40)
    assert_eq!(r.len(), 3);

    // CTE 内聚合 + 主查询二次聚合
    let r = q(
        &db,
        "WITH by_t AS (SELECT tenant, sum(amt) AS total FROM sales GROUP BY tenant) SELECT count(*) FROM by_t WHERE total > 25",
    );
    // tenant sums: a=40, b=20, c=40 → >25: a, c → 2
    assert_eq!(r, vec!["2"]);

    // 多 CTE（引用前面的 CTE 是后置项——当前版本子 CTE 不支持引用兄弟 CTE）
    let r = q(
        &db,
        "
        WITH s1 AS (SELECT amt FROM sales WHERE tenant = 'a'),
             s2 AS (SELECT sum(amt) AS t FROM sales WHERE tenant = 'a')
        SELECT t FROM s2",
    );
    assert_eq!(r, vec!["40"]);

    // 递归 CTE 拒绝
    assert!(
        db.sql("WITH RECURSIVE t(n) AS (SELECT 1) SELECT * FROM t")
            .is_err()
    );
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn cte_dead_and_shadow() {
    let (db, root) = setup("cte2");
    db.sql("CREATE TABLE t (a BIGINT) PARTITIONED BY (a)")
        .unwrap();
    db.sql("INSERT INTO t VALUES (1),(2),(3)").unwrap();
    // 未引用的 CTE（dead）不报错
    let r = q(
        &db,
        "WITH unused AS (SELECT a FROM t WHERE a > 1) SELECT count(*) FROM t",
    );
    assert_eq!(r, vec!["3"]);
    // 与表同名 → 拒绝（防止歧义）
    assert!(db.sql("WITH t AS (SELECT 1) SELECT * FROM t").is_err());
    std::fs::remove_dir_all(&root).ok();
}
