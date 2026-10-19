//! 窗口函数后置项（M5-A1）：多 spec / 同名区分 / LAG-LEAD / 嵌表达式 / 聚合上窗口。

use qsdb::{Qsdb, QsdbOptions};

/// 测试目录唯一序号：pid 挡跨进程碰撞，序号挡同进程内并发（Windows 时钟
/// 粒度 ~100ns，纳秒后缀在同 tick 内会重复——CI 上真炸过，见 constraints）。
fn _uniq_seq() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    SEQ.fetch_add(1, Ordering::Relaxed)
}

fn setup(tag: &str) -> (Qsdb, std::path::PathBuf) {
    let root = std::env::temp_dir().join("qsdb_m5_winadv").join(format!(
        "{tag}_{:x}_{}",
        std::process::id(),
        _uniq_seq()
    ));
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    db.sql("CREATE TABLE t (dt DATE, uid BIGINT, amt BIGINT) PARTITIONED BY (dt)")
        .unwrap();
    db.sql(
        "INSERT INTO t VALUES
        (DATE '2026-01-01', 1, 100), (DATE '2026-01-01', 2, 300),
        (DATE '2026-01-01', 3, 200),
        (DATE '2026-01-02', 1, 50), (DATE '2026-01-02', 2, 80),
        (DATE '2026-01-02', 3, 100)",
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
fn multiple_specs() {
    let (db, root) = setup("spec");
    // 两个不同 spec（PARTITION BY uid + ORDER BY amt DESC / 全局 ORDER BY amt ASC）
    // → 两个 Window 节点链式叠加
    let r = q(
        &db,
        "SELECT uid, row_number() OVER (PARTITION BY uid ORDER BY amt DESC) AS rn_p,
                row_number() OVER (ORDER BY amt ASC) AS rn_g
         FROM t",
    );
    assert_eq!(
        r,
        vec![
            "1|1|3", // uid1: amt 100(rn_p=1, 全局 asc rn_g=3), 50(rn_p=2, rn_g=1)
            "1|2|1", //
            "2|1|6", // uid2: 300(rn_p=1, rn_g=6), 80(rn_p=2, rn_g=2)
            "2|2|2", //
            "3|1|5", // uid3: 200(rn_p=1, rn_g=5), 100(rn_p=2, rn_g=4)
            "3|2|4",
        ]
    );
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn same_name_distinct_args_and_dedup() {
    let (db, root) = setup("same");
    // 同名不同参：sum(amt) 分区总和 vs sum(uid) 全局总和（uid 各出现两次：1+1+2+2+3+3=12）
    let r = q(
        &db,
        "SELECT sum(amt) OVER (PARTITION BY uid) AS s_amt, sum(uid) OVER () AS s_uid FROM t",
    );
    assert_eq!(
        r,
        vec!["150|12", "150|12", "300|12", "300|12", "380|12", "380|12"]
    );
    // 同名同参不同 spec：asc 与 desc 的 row_number 之和。无并列行恒为 7（1+6..6+1）；
    // 并列对（两个 100）在稳定排序下两侧拿同名次 → 3+3=6、4+4=8
    let r = q(
        &db,
        "SELECT row_number() OVER (ORDER BY amt) + row_number() OVER (ORDER BY amt DESC) AS s FROM t",
    );
    assert_eq!(r, vec!["6", "7", "7", "7", "7", "8"]);
    // 完全相同的窗口调用两次 → 去重为一个窗口列，两投影相同
    let r = q(
        &db,
        "SELECT row_number() OVER (ORDER BY amt) AS x, row_number() OVER (ORDER BY amt) AS y FROM t",
    );
    assert_eq!(r, vec!["1|1", "2|2", "3|3", "4|4", "5|5", "6|6"]);
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn window_embedded_in_expression() {
    let (db, root) = setup("embed");
    // 窗口嵌表达式：row_number() + 1000（并列对 100 的具体名次不定，行集合确定）
    let r = q(
        &db,
        "SELECT amt, row_number() OVER (ORDER BY amt DESC) + 1000 AS scored FROM t",
    );
    assert_eq!(
        r,
        vec![
            "100|1003", "100|1004", "200|1002", "300|1001", "50|1006", "80|1005"
        ]
    );
    // 嵌在标量函数参数里：abs 不改变名次
    let r = q(
        &db,
        "SELECT abs(row_number() OVER (ORDER BY amt) - 7) AS mirror FROM t",
    );
    assert_eq!(r, vec!["1", "2", "3", "4", "5", "6"]);
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn lag_lead() {
    let (db, root) = setup("lag");
    // lag 默认 offset=1，分区内按 amt 降序取前一行的值
    let r = q(
        &db,
        "SELECT uid, amt, lag(amt, 1) OVER (PARTITION BY uid ORDER BY amt DESC) AS prev FROM t",
    );
    assert_eq!(
        r,
        vec![
            "1|100|NULL",
            "1|50|100",
            "2|300|NULL",
            "2|80|300",
            "3|100|200",
            "3|200|NULL",
        ]
    );
    // lead + default（越出分区取 default 而非 NULL）
    let r = q(
        &db,
        "SELECT uid, amt, lead(amt, 1, 0) OVER (PARTITION BY uid ORDER BY amt DESC) AS nxt FROM t",
    );
    assert_eq!(
        r,
        vec![
            "1|100|50",
            "1|50|0",
            "2|300|80",
            "2|80|0",
            "3|100|0",
            "3|200|100"
        ]
    );
    // offset 超出分区大小 → 全 NULL
    let r = q(
        &db,
        "SELECT uid, lag(amt, 2) OVER (PARTITION BY uid ORDER BY amt DESC) AS p2 FROM t",
    );
    assert_eq!(
        r,
        vec!["1|NULL", "1|NULL", "2|NULL", "2|NULL", "3|NULL", "3|NULL"]
    );
    // offset 必须 ≥1 的整数字面量
    assert!(
        db.sql("SELECT lag(amt, 0) OVER (ORDER BY amt) FROM t")
            .is_err()
    );
    assert!(
        db.sql("SELECT lag(amt, -1) OVER (ORDER BY amt) FROM t")
            .is_err()
    );
    assert!(
        db.sql("SELECT lag(amt, amt) OVER (ORDER BY amt) FROM t")
            .is_err()
    );
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn window_over_aggregate() {
    let (db, root) = setup("aggwin");
    db.sql("CREATE TABLE s (grp STRING, v BIGINT) PARTITIONED BY (grp)")
        .unwrap();
    db.sql("INSERT INTO s VALUES ('a', 10), ('a', 20), ('b', 5), ('b', 15), ('b', 25)")
        .unwrap();

    // 窗口在 GROUP BY 输出之上：b 总和 45 → rank 1，a 总和 30 → rank 2
    let r = q(
        &db,
        "SELECT grp, sum(v) AS total, rank() OVER (ORDER BY sum(v) DESC) AS rk
         FROM s GROUP BY grp",
    );
    assert_eq!(r, vec!["a|30|2", "b|45|1"]);

    // spec 引用聚合 count(*)；窗口结果嵌表达式（* 10）
    let r = q(
        &db,
        "SELECT grp, count(*) AS c, rank() OVER (ORDER BY count(*) DESC) * 10 AS rk10
         FROM s GROUP BY grp",
    );
    assert_eq!(r, vec!["a|2|20", "b|3|10"]);

    // PARTITION BY 分组列 + 窗口聚合运行值
    let r = q(
        &db,
        "SELECT grp, sum(v) AS total, sum(sum(v)) OVER (PARTITION BY grp) AS wsum
         FROM s GROUP BY grp",
    );
    assert_eq!(r, vec!["a|30|30", "b|45|45"]);
    std::fs::remove_dir_all(&root).ok();
}
