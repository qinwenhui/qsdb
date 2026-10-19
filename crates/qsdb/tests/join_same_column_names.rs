//! **两张表有同名列**的 join —— 2026-10-17 拿真表测出来的 P0 的回归。
//!
//! ## 为什么此前全套测试都没抓到
//!
//! `tests/join_*.rs` 里**从来没有 join 过两张同名列的表**。这个形状恰恰是真实业务里
//! 最常见的一种：**同构的对照表**（同一张模板建出来的两张表，11 列同名）。
//!
//! ## 症状（修复前）
//!
//! JOIN 右表的列在计划里是**限定名**（`b.k`），而 `rewrite_expr` 判"这是不是表列"
//! 时拿 `def.columns` 的**裸名**去比 ⇒ 永远判不上 ⇒ `unwrap_or(idx)` 把**旧索引
//! 原样留下** ⇒ 右键指到隔壁列：
//!
//! | 查询 | 修复前 |
//! |---|---|
//! | `SELECT a.k, b.v …` | **静默回 0 行**（应为 k1\|10、k2\|20） |
//! | `SELECT a.k, b.k …` | **panic** `index out of bounds: len 1 index 1` |
//! | `SELECT count(*) …` | **panic** 同上 |
//! | `SELECT a.k …` | **panic** 同上 |
//!
//! 静默错值那一档尤其坏 —— 它和"少一行/多一行"不同，是**看起来跑通了但答案是空的**。
//!
//! ## 这一族测试要盯住的
//!
//! 光测"能不能跑"不够：`a.k, b.v` 修复前**不报错**、只是回空。所以下面每条都
//! **逐值**断言，而且覆盖"投影左列 / 右列 / 两侧 / 只投影 join key / 聚合"几种取用方式
//! —— 它们分别走 `try_project_join` 的不同分支。

use qsdb::{Qsdb, QsdbOptions};

fn setup(tag: &str) -> (Qsdb, std::path::PathBuf) {
    let root = std::env::temp_dir()
        .join("qsdb_join_samecols")
        .join(format!(
            "{tag}_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
    std::fs::create_dir_all(&root).unwrap();
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    // 两张**同名列**的表（照抄真实同构两表的形状）
    db.sql("CREATE TABLE a (uid VARCHAR, k VARCHAR, v BIGINT)")
        .unwrap();
    db.sql("CREATE TABLE b (uid VARCHAR, k VARCHAR, v BIGINT)")
        .unwrap();
    db.sql("INSERT INTO a VALUES ('p1','k1',1),('p2','k2',2)")
        .unwrap();
    db.sql("INSERT INTO b VALUES ('q1','k1',10),('q2','k2',20)")
        .unwrap();
    (db, root)
}

/// 逐行 `c0|c1|…`，**保持引擎给的行序**（下面的 SQL 都带 ORDER BY）。
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

/// 投影**两侧**的列（一左一右）—— 修复前**静默回 0 行**。
#[test]
fn 投影两侧同名列() {
    let (db, root) = setup("both");
    assert_eq!(
        rows(
            &db,
            "SELECT a.k, b.v FROM a JOIN b ON a.k = b.k ORDER BY a.k"
        ),
        vec!["k1|10", "k2|20"]
    );
    assert_eq!(
        rows(
            &db,
            "SELECT b.v, a.k FROM a JOIN b ON a.k = b.k ORDER BY a.k"
        ),
        vec!["10|k1", "20|k2"],
        "换个列序，确认不是'第一列恰好对'"
    );
    drop(db);
    let _ = std::fs::remove_dir_all(root);
}

/// 投影**同名列的两侧**（`a.k` 与 `b.k`）—— 修复前 **panic**。
#[test]
fn 两侧都投影那个同名列() {
    let (db, root) = setup("same");
    assert_eq!(
        rows(
            &db,
            "SELECT a.k, b.k FROM a JOIN b ON a.k = b.k ORDER BY a.k"
        ),
        vec!["k1|k1", "k2|k2"]
    );
    // 只投影一侧的 join key，另一个方向也要能跑
    assert_eq!(
        rows(&db, "SELECT b.k FROM a JOIN b ON a.k = b.k ORDER BY b.k"),
        vec!["k1", "k2"]
    );
    assert_eq!(
        rows(&db, "SELECT a.k FROM a JOIN b ON a.k = b.k ORDER BY a.k"),
        vec!["k1", "k2"]
    );
    drop(db);
    let _ = std::fs::remove_dir_all(root);
}

/// `count(*)` / 聚合走的是**另一条**路（join 计数下推 / 输出掩码）—— 修复前也 panic。
#[test]
fn 计数与聚合() {
    let (db, root) = setup("agg");
    assert_eq!(
        rows(&db, "SELECT count(*) FROM a JOIN b ON a.k = b.k"),
        vec!["2"]
    );
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

/// **非同名的**列取两侧也要对（`uid` 不是 join key，走的分支不同）。
#[test]
fn 非同名列取两侧() {
    let (db, root) = setup("other");
    assert_eq!(
        rows(
            &db,
            "SELECT a.uid, b.uid FROM a JOIN b ON a.k = b.k ORDER BY a.uid"
        ),
        vec!["p1|q1", "p2|q2"]
    );
    assert_eq!(
        rows(
            &db,
            "SELECT a.v, b.v FROM a JOIN b ON a.k = b.k ORDER BY a.v"
        ),
        vec!["1|10", "2|20"]
    );
    drop(db);
    let _ = std::fs::remove_dir_all(root);
}

/// ★ **`SELECT *` 要原样透传两个同名列**（不剥、不合并、不报 ambiguous）。
///
/// 这是"剥限定符"那条纪律的边界：**投影里用户写出来的限定名**要剥（`a.v` ⇒ `v`），
/// 但 `*` 展开出来的列是**引擎给的名字**，两个 `v` 都得在（PG 也允许出两个 `v`）。
/// 真要引用它们才该报 ambiguous。
#[test]
fn 星号透传两份同名列() {
    let (db, root) = setup("star");
    let out = rows(&db, "SELECT * FROM a JOIN b ON a.k = b.k ORDER BY a.uid");
    assert_eq!(out.len(), 2);
    assert_eq!(out[0], "p1|k1|1|q1|k1|10");
    assert_eq!(out[1], "p2|k2|2|q2|k2|20");
    drop(db);
    let _ = std::fs::remove_dir_all(root);
}
