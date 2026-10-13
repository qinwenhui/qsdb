//! CTE + 窗口函数 + UDF 测试（M5）。

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
    // 注意：**分区表必须至少有一个非分区列**（2026-10-13 起 DDL 层拒绝
    // "所有列都是分区列"——那会产出零列数据文件，见 `catalog::validate`）。
    // 本用例不关心分区语义，加一列数据列即可。
    db.sql("CREATE TABLE t (a BIGINT, v BIGINT) PARTITIONED BY (a)")
        .unwrap();
    db.sql("INSERT INTO t VALUES (1,1),(2,1),(3,1)").unwrap();
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

#[test]
fn cte_chained_and_join_right() {
    let (db, root) = setup("cte3");
    db.sql("CREATE TABLE sales (dt DATE, tenant STRING, amt BIGINT) PARTITIONED BY (dt)")
        .unwrap();
    db.sql(
        "INSERT INTO sales VALUES
        (DATE '2026-01-01', 'a', 10), (DATE '2026-01-01', 'b', 20),
        (DATE '2026-01-02', 'a', 30), (DATE '2026-01-02', 'c', 40)",
    )
    .unwrap();

    // 链式引用：s2 引用 s1（后定义引用先定义）；s1: b/a/c → 20+30+40 = 90
    let r = q(
        &db,
        "WITH s1 AS (SELECT tenant, amt FROM sales WHERE amt > 15),
              s2 AS (SELECT sum(amt) AS t FROM s1)
         SELECT t FROM s2",
    );
    assert_eq!(r, vec!["90"]);

    // 三级链：s3 引用 s2 引用 s1；s1: a 行 10+30=40 → ×2 = 80
    let r = q(
        &db,
        "WITH s1 AS (SELECT amt FROM sales WHERE tenant = 'a'),
              s2 AS (SELECT sum(amt) AS t FROM s1),
              s3 AS (SELECT t * 2 AS t2 FROM s2)
         SELECT t2 FROM s3",
    );
    assert_eq!(r, vec!["80"]);

    // CTE 内自带 WITH（嵌套）：inner1: 30+40 = 70
    let r = q(
        &db,
        "WITH outer1 AS (
             WITH inner1 AS (SELECT amt FROM sales WHERE amt > 25)
             SELECT sum(amt) AS t FROM inner1)
         SELECT t FROM outer1",
    );
    assert_eq!(r, vec!["70"]);

    // CTE 做 JOIN 右侧（等值）：totals a=40, b=20, c=40
    let r = q(
        &db,
        "WITH totals AS (SELECT tenant, sum(amt) AS total FROM sales GROUP BY tenant)
         SELECT sales.tenant, totals.total FROM sales JOIN totals ON sales.tenant = totals.tenant
         ORDER BY sales.tenant, totals.total",
    );
    assert_eq!(r, vec!["a|40", "a|40", "b|20", "c|40"]);

    // 同一 CTE 主 FROM + JOIN 右侧共享一次物化
    let r = q(
        &db,
        "WITH s1 AS (SELECT tenant, amt FROM sales)
         SELECT count(*) FROM s1 JOIN s1 AS other ON s1.amt = other.amt",
    );
    // amt 各不相同 → 等值自连接每行恰匹配 1 行 → 4
    assert_eq!(r, vec!["4"]);
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn window_row_number() {
    let (db, root) = setup("win1");
    db.sql("CREATE TABLE t (dt DATE, uid BIGINT, amt BIGINT) PARTITIONED BY (dt)")
        .unwrap();
    // amt=100 出现两次（uid 1 和 uid 3）→ 测 RANK 并列
    db.sql(
        "INSERT INTO t VALUES
        (DATE '2026-01-01', 1, 100), (DATE '2026-01-01', 2, 300),
        (DATE '2026-01-01', 3, 200),
        (DATE '2026-01-02', 1, 50), (DATE '2026-01-02', 2, 80),
        (DATE '2026-01-02', 3, 100)",
    )
    .unwrap();

    // ROW_NUMBER：uid 分区内按 amt 降序
    let r = q(
        &db,
        "SELECT uid, row_number() OVER (PARTITION BY uid ORDER BY amt DESC) AS rn FROM t",
    );
    assert_eq!(r, vec!["1|1", "1|2", "2|1", "2|2", "3|1", "3|2"]);

    // RANK：全局 amt 降序，两个 100 并列 rank=3，之后跳到 5
    // 300→1, 200→2, 100→3, 100→3, 80→5, 50→6
    let r = q(
        &db,
        "SELECT uid, rank() OVER (ORDER BY amt DESC) AS rk FROM t",
    );
    assert_eq!(r, vec!["1|3", "1|6", "2|1", "2|5", "3|2", "3|3"]);

    // DENSE_RANK：并列不跳号 300→1, 200→2, 100→3, 100→3, 80→4, 50→5
    let r = q(
        &db,
        "SELECT uid, dense_rank() OVER (ORDER BY amt DESC) AS dr FROM t",
    );
    assert_eq!(r, vec!["1|3", "1|5", "2|1", "2|4", "3|2", "3|3"]);

    // SUM OVER 有 ORDER BY：运行总和（RANGE 帧含并列）
    let r = q(
        &db,
        "SELECT uid, sum(amt) OVER (PARTITION BY uid ORDER BY amt DESC) AS run_total FROM t",
    );
    assert_eq!(
        r,
        vec!["1|100", "1|150", "2|300", "2|380", "3|200", "3|300"]
    );

    // SUM OVER 无 ORDER BY：整分区总值（每行相同）
    let r = q(
        &db,
        "SELECT uid, sum(amt) OVER (PARTITION BY uid) AS total FROM t",
    );
    assert_eq!(
        r,
        vec!["1|150", "1|150", "2|380", "2|380", "3|300", "3|300"]
    );

    // RANGE 帧并列语义：两个 100 行共享"≤100 全部行"的总和 50+80+100+100=330
    // 各行帧值：50→50, 80→130, 100→330, 100→330, 200→530, 300→830
    let r = q(&db, "SELECT amt, sum(amt) OVER (ORDER BY amt) AS s FROM t");
    assert_eq!(
        r,
        vec![
            "100|330", "100|330", "200|530", "300|830", "50|50", "80|130"
        ]
    );

    // COUNT OVER：分区非 null 计数
    let r = q(
        &db,
        "SELECT uid, count(amt) OVER (PARTITION BY uid) AS cnt FROM t",
    );
    assert_eq!(r, vec!["1|2", "1|2", "2|2", "2|2", "3|2", "3|2"]);

    // MIN/MAX OVER 整型列（输出类型 Int64）
    let r = q(
        &db,
        "SELECT uid, min(amt) OVER (PARTITION BY uid) AS mn, max(amt) OVER (PARTITION BY uid) AS mx FROM t",
    );
    assert_eq!(
        r,
        vec![
            "1|50|100",
            "1|50|100",
            "2|80|300",
            "2|80|300",
            "3|100|200",
            "3|100|200"
        ]
    );

    // AVG OVER：分区均值
    let r = q(
        &db,
        "SELECT uid, avg(amt) OVER (PARTITION BY uid) AS av FROM t",
    );
    assert_eq!(
        r,
        vec![
            "1|75.0", "1|75.0", "2|190.0", "2|190.0", "3|150.0", "3|150.0"
        ]
    );
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn window_with_filter() {
    let (db, root) = setup("win2");
    db.sql("CREATE TABLE s (grp STRING, v BIGINT) PARTITIONED BY (grp)")
        .unwrap();
    db.sql("INSERT INTO s VALUES ('a', 10), ('a', 20), ('b', 5), ('b', 15), ('b', 25)")
        .unwrap();

    // WHERE 先过滤再开窗：v>5 留下 a(10,20) b(15,25)
    let r = q(
        &db,
        "SELECT grp, v, row_number() OVER (PARTITION BY grp ORDER BY v) AS rn FROM s WHERE v > 5",
    );
    assert_eq!(r, vec!["a|10|1", "a|20|2", "b|15|1", "b|25|2"]);

    // 无 ORDER BY 聚合窗口 + 外层 ORDER BY：每行 = 分区总值
    let r = q(
        &db,
        "SELECT grp, sum(v) OVER (PARTITION BY grp) AS total FROM s ORDER BY grp, total",
    );
    assert_eq!(r, vec!["a|30", "a|30", "b|45", "b|45", "b|45"]);
    std::fs::remove_dir_all(&root).ok();
}
