//! 嵌套子查询（M5-A3）：FROM 派生表 / JOIN 右派生表 / 标量子查询 / IN 值列表。

use qsdb::{Qsdb, QsdbOptions};

fn setup(tag: &str) -> (Qsdb, std::path::PathBuf) {
    let root = std::env::temp_dir().join("qsdb_m5_subq").join(format!(
        "{tag}_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    db.sql("CREATE TABLE sales (dt DATE, tenant STRING, amt BIGINT) PARTITIONED BY (dt)")
        .unwrap();
    db.sql(
        "INSERT INTO sales VALUES
        (DATE '2026-01-01', 'a', 10), (DATE '2026-01-01', 'b', 20),
        (DATE '2026-01-02', 'a', 30), (DATE '2026-01-02', 'c', 40)",
    )
    .unwrap();
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
fn derived_table_from() {
    let (db, root) = setup("derived");
    // 派生表聚合 + 外层排序
    let r = q(
        &db,
        "SELECT tenant, total FROM
            (SELECT tenant, sum(amt) AS total FROM sales GROUP BY tenant) AS t
         ORDER BY tenant",
    );
    assert_eq!(r, vec!["a|40", "b|20", "c|40"]);

    // 外层 WHERE 过滤派生表输出
    let r = q(
        &db,
        "SELECT tenant FROM
            (SELECT tenant, sum(amt) AS total FROM sales GROUP BY tenant) t
         WHERE total > 25 ORDER BY tenant",
    );
    assert_eq!(r, vec!["a", "c"]);

    // 派生表嵌套两层
    let r = q(
        &db,
        "SELECT max(total) AS m FROM
            (SELECT tenant, sum(amt) AS total FROM
                (SELECT * FROM sales WHERE amt > 15) s
             GROUP BY tenant) t",
    );
    // amt>15: b(20),a(30),c(40) → totals a=30,b=20,c=40 → max 40
    assert_eq!(r, vec!["40"]);

    // CTE + 派生表混用（派生表可见外层 CTE）
    let r = q(
        &db,
        "WITH big AS (SELECT amt FROM sales WHERE amt > 15)
         SELECT sum(amt) FROM (SELECT amt FROM big) x",
    );
    assert_eq!(r, vec!["90"]);
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn derived_join_right() {
    let (db, root) = setup("djoin");
    let r = q(
        &db,
        "SELECT sales.tenant, t.total FROM sales
         JOIN (SELECT tenant, sum(amt) AS total FROM sales GROUP BY tenant) AS t
              ON sales.tenant = t.tenant
         ORDER BY sales.tenant, t.total",
    );
    assert_eq!(r, vec!["a|40", "a|40", "b|20", "c|40"]);
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn scalar_subquery() {
    let (db, root) = setup("scalar");
    // 投影中的标量子查询
    let r = q(
        &db,
        "SELECT tenant, amt, (SELECT max(amt) FROM sales) AS mx
         FROM sales WHERE tenant = 'a' ORDER BY amt",
    );
    assert_eq!(r, vec!["a|10|40", "a|30|40"]);

    // WHERE 中的标量子查询
    let r = q(
        &db,
        "SELECT tenant, amt FROM sales WHERE amt = (SELECT max(amt) FROM sales)",
    );
    assert_eq!(r, vec!["c|40"]);

    // 表达式内（算术 + 比较）：avg=25 → amt > 15 → b/a(30)/c
    let r = q(
        &db,
        "SELECT tenant FROM sales WHERE amt > (SELECT avg(amt) FROM sales) - 10 ORDER BY tenant",
    );
    assert_eq!(r, vec!["a", "b", "c"]);

    // 必须恰好一列
    assert!(
        db.sql("SELECT (SELECT tenant, amt FROM sales) FROM sales")
            .is_err()
    );

    // 关联子查询不支持：子查询独立绑定，限定名经裸名回退解析到子查询自己的列
    // （同名表自引用恰好语义等价；异表关联引用会静默解析错位——已知限制）
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn in_value_list() {
    let (db, root) = setup("inlist");
    let r = q(
        &db,
        "SELECT amt FROM sales WHERE tenant IN ('a', 'c') ORDER BY amt",
    );
    assert_eq!(r, vec!["10", "30", "40"]);

    let r = q(
        &db,
        "SELECT amt FROM sales WHERE tenant NOT IN ('a') ORDER BY amt",
    );
    assert_eq!(r, vec!["20", "40"]);

    // IN 与其他谓词组合
    let r = q(
        &db,
        "SELECT amt FROM sales WHERE tenant IN ('a', 'b') AND amt > 15 ORDER BY amt",
    );
    assert_eq!(r, vec!["20", "30"]);
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn limit_zero_schema_contract() {
    // 同步工具 schema 协商契约：SELECT ... LIMIT 0 → 零行仍携带完整 schema
    let (db, root) = setup("limit0");
    match db.sql("SELECT tenant, amt FROM sales LIMIT 0") {
        Ok(qsdb::sql::SqlResult::Query { schema, batches }) => {
            assert_eq!(schema.fields().len(), 2);
            assert_eq!(schema.field(0).name(), "tenant");
            assert_eq!(schema.field(1).name(), "amt");
            assert!(batches.iter().all(|b| b.num_rows() == 0));
        }
        Err(e) => panic!("LIMIT 0 should succeed: {e}"),
        _ => panic!("expected Query result"),
    }
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn predicate_column_not_in_projection() {
    // 回归：谓词列不在投影中（count(*) WHERE v>15）——下推进 Scan 的谓词
    // 引用列必须进列裁剪收集，否则投影被裁到只剩分区列、谓词静默失效
    let (db, root) = setup("predcol");
    let r = q(&db, "SELECT count(*) FROM sales WHERE amt > 15");
    // amt>15: b(20), a(30), c(40) → 3（失效则 4）
    assert_eq!(r, vec!["3"]);

    // 谓词列在投影中（原本就正确）对照
    let r = q(&db, "SELECT amt FROM sales WHERE amt > 15 ORDER BY amt");
    assert_eq!(r, vec!["20", "30", "40"]);
    std::fs::remove_dir_all(&root).ok();
}
