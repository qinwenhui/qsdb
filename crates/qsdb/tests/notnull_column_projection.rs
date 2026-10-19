//! **非空列 × 列裁剪/掩码** —— 2026-10-17 拿真表测出来的第二个洞。
//!
//! ## 病根
//!
//! 「只解码/只物化下游真读的列」这两条优化（`Scan::restrict_read`、
//! `HashJoinExec::with_needed`）对**没被读的列按 NULL 产出**，可它们沿用的 schema
//! 是**表定义原样**（列声明着 `NOT NULL`）⇒ 组装批时 `RecordBatch::try_new` 当场拒：
//!
//! ```text
//! Column 'uid' is declared as non-nullable but contains null values
//! ```
//!
//! ## 为什么整套测试都没抓到
//!
//! 我们自己的测试表列**全是可空的**（`CREATE TABLE` 默认），而 MySQL 迁来的真表
//! **列全是 `NOT NULL`**（ddl 里带着）。整个测试面从来没覆盖过"非空列 + 列裁剪"。
//!
//! ## 修法
//!
//! 被 NULL 化的那些列在 schema 里**标成可空** —— 这是**如实描述**，不是放宽约束：
//! 那些位置确实会出现 NULL。上层只用类型与列序，不看 nullability。
//!
//! ⚠️ 两处同源、必须一起改（只改一处会看到错误现场"搬家"而不是消失）：
//! `scan.rs::restrict_read` 与 `join.rs::with_needed`。

use qsdb::{Qsdb, QsdbOptions};

/// 测试目录唯一序号：pid 挡跨进程碰撞，序号挡同进程内并发（Windows 时钟
/// 粒度 ~100ns，纳秒后缀在同 tick 内会重复——CI 上真炸过，见 constraints）。
fn _uniq_seq() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    SEQ.fetch_add(1, Ordering::Relaxed)
}

fn setup(tag: &str) -> (Qsdb, std::path::PathBuf) {
    let root = std::env::temp_dir().join("qsdb_notnull_proj").join(format!(
        "{tag}_{:x}_{}",
        std::process::id(),
        _uniq_seq()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    // 列**全是 NOT NULL**（照抄 MySQL 迁来的真表）
    db.sql("CREATE TABLE a (uid VARCHAR NOT NULL, k VARCHAR NOT NULL, v BIGINT NOT NULL)")
        .unwrap();
    db.sql("CREATE TABLE b (uid VARCHAR NOT NULL, k VARCHAR NOT NULL, v BIGINT NOT NULL)")
        .unwrap();
    db.sql("INSERT INTO a VALUES ('p1','k1',1),('p2','k2',2)")
        .unwrap();
    db.sql("INSERT INTO b VALUES ('q1','k1',10),('q2','k2',20)")
        .unwrap();
    (db, root)
}

fn rows(db: &Qsdb, sql: &str) -> Vec<String> {
    let bs = db.sql_collect(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    bs.iter()
        .flat_map(|b| {
            (0..b.num_rows()).map(move |i| {
                (0..b.num_columns())
                    .map(|c| {
                        let a = b.column(c);
                        if a.is_null(i) {
                            "NULL".into()
                        } else {
                            arrow::util::display::array_value_to_string(a.as_ref(), i).unwrap()
                        }
                    })
                    .collect::<Vec<_>>()
                    .join("|")
            })
        })
        .collect()
}

/// ★ 聚合走在 join 之上 ⇒ 触发**扫描只解码若干列**（`restrict_read`）。
#[test]
fn 非空表上聚合跨join() {
    let (db, root) = setup("agg");
    assert_eq!(
        rows(&db, "SELECT sum(a.v) FROM a JOIN b ON a.k = b.k"),
        vec!["3"]
    );
    assert_eq!(
        rows(
            &db,
            "SELECT a.k, sum(a.v) AS s FROM a JOIN b ON a.k = b.k GROUP BY a.k ORDER BY a.k"
        ),
        vec!["k1|1", "k2|2"]
    );
    drop(db);
    let _ = std::fs::remove_dir_all(root);
}

/// 单表聚合也走同一条路（`SELECT sum(v) FROM a` 只读 `v` 一列）。
#[test]
fn 非空表上单表聚合() {
    let (db, root) = setup("single");
    assert_eq!(rows(&db, "SELECT sum(v) FROM a"), vec!["3"]);
    assert_eq!(
        rows(&db, "SELECT k, sum(v) FROM a GROUP BY k ORDER BY k"),
        vec!["k1|1", "k2|2"]
    );
    drop(db);
    let _ = std::fs::remove_dir_all(root);
}

/// 四种连接类型都要过 —— 外连接**补 NULL 的那一侧**同样是"非空列里塞 NULL"。
#[test]
fn 非空表上四种连接() {
    let (db, root) = setup("types");
    for (join, order) in [
        ("JOIN", "a.k"),
        ("LEFT JOIN", "a.k"),
        ("RIGHT JOIN", "b.k"),
        ("FULL JOIN", "a.k"),
    ] {
        let sql = format!("SELECT a.k, b.v FROM a {join} b ON a.k = b.k ORDER BY {order}");
        assert_eq!(rows(&db, &sql), vec!["k1|10", "k2|20"], "{sql}");
    }
    assert_eq!(
        rows(&db, "SELECT count(*) FROM a LEFT JOIN b ON a.k = b.k"),
        vec!["2"]
    );
    drop(db);
    let _ = std::fs::remove_dir_all(root);
}
