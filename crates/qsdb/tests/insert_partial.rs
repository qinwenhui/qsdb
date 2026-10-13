//! **`INSERT INTO t (cols) VALUES …` 允许部分列**（2026-10-13 放宽）。
//!
//! 原契约：VALUES 源的列清单**必须覆盖全部表列**，否则报
//! `INSERT column list has N entries, table X has M`——理由是"宁可报错，也不要补
//! NULL 写进半行"。但 `INSERT … SELECT` 一直**允许**部分列（缺的补 NULL），同一条
//! 理由在那边不成立；而且两条源的差别对使用者是纯粹的意外（qsdb-jdbc 实测撞到：
//! 驱动的参数化 INSERT 必须凑齐全列才能发）。
//!
//! 现口径（与标准 SQL 一致）：**两条源都允许部分列**，未列出的列补 NULL；
//! 真正的拦截交给写端约束（`NOT NULL` / `CHECK`），不会被"补 NULL"绕过。

use qsdb::{Qsdb, QsdbOptions};

fn setup(tag: &str) -> (Qsdb, std::path::PathBuf) {
    let root = std::env::temp_dir()
        .join("qsdb_insert_partial_test")
        .join(format!(
            "{tag}_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    db.sql("CREATE TABLE t (id BIGINT, name STRING, v BIGINT)")
        .unwrap();
    (db, root)
}

fn rows(db: &Qsdb, sql: &str) -> Vec<String> {
    let bs = db.sql_collect(sql).unwrap();
    let mut out = Vec::new();
    for b in &bs {
        for i in 0..b.num_rows() {
            out.push(
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
                    .join("|"),
            );
        }
    }
    out
}

#[test]
fn partial_column_list_is_null_filled() {
    let (db, _root) = setup("partial");

    db.sql("INSERT INTO t (id, v) VALUES (1, 10)").unwrap();
    db.sql("INSERT INTO t (name) VALUES ('x')").unwrap();
    db.sql("INSERT INTO t VALUES (3, 'c', 30)").unwrap();
    // 多行同样支持
    db.sql("INSERT INTO t (id, v) VALUES (4, 40), (5, 50)")
        .unwrap();

    // 注：这里原写 `ORDER BY id NULLS LAST` ——**该子句当时是被静默忽略的**，
    // 只是 ASC 的默认放置恰好就是 NULLS LAST，所以断言才一直成立。
    // 2026-10-13 起 `NULLS FIRST/LAST` 未实现即显式拒绝（见
    // `tests/sql_silent_drop.rs`），故去掉该子句；期望顺序不变（默认即 NULLS LAST）。
    assert_eq!(
        rows(&db, "SELECT id, name, v FROM t ORDER BY id, v"),
        vec![
            "1|NULL|10",
            "3|c|30",
            "4|NULL|40",
            "5|NULL|50",
            "NULL|x|NULL",
        ]
    );
}

/// 放宽不等于放水：未知列照报、NOT NULL 照拦（缺列补 NULL 后仍会被写端挡下）。
#[test]
fn relaxation_does_not_weaken_checks() {
    let (db, _root) = setup("checks");

    let e = match db.sql("INSERT INTO t (nope) VALUES (1)") {
        Ok(_) => panic!("未知列必须报错"),
        Err(e) => e,
    };
    assert!(
        format!("{e}").contains("nope"),
        "错误应点名未知列，实得：{e}"
    );

    db.sql("CREATE TABLE nn (a BIGINT NOT NULL, b BIGINT)")
        .unwrap();
    let e = match db.sql("INSERT INTO nn (b) VALUES (1)") {
        Ok(_) => panic!("缺 NOT NULL 列 → 补 NULL → 必须被写端拦下"),
        Err(e) => e,
    };
    assert!(
        format!("{e}").to_lowercase().contains("not null"),
        "应报 NOT NULL 约束，实得：{e}"
    );

    // 反面：给出了 NOT NULL 列就正常
    db.sql("INSERT INTO nn (a, b) VALUES (1, 2)").unwrap();
    assert_eq!(rows(&db, "SELECT count(*) FROM nn"), vec!["1"]);
}

/// 两条源口径一致：同样的部分列清单，VALUES 与 SELECT 结果相同。
#[test]
fn values_and_select_sources_agree() {
    let (db, _root) = setup("agree");

    db.sql("CREATE TABLE src (id BIGINT, name STRING, v BIGINT)")
        .unwrap();
    db.sql("INSERT INTO src VALUES (7, 'z', 70)").unwrap();

    db.sql("INSERT INTO t (id, v) VALUES (7, 70)").unwrap();
    db.sql("INSERT INTO t (id, v) SELECT id, v FROM src")
        .unwrap();

    assert_eq!(
        rows(&db, "SELECT id, name, v FROM t ORDER BY id"),
        vec!["7|NULL|70", "7|NULL|70"],
        "VALUES 与 SELECT 两条源的部分列语义必须一致（此前一边拒一边收）"
    );
}

/// **表列前缀**的部分列清单（`(id)`、`(id, name)`）此前被静默当成"恒等映射"。
///
/// 根因：`reorder_rows` 的快速路径判据是 `perm` 逐位等于自己的下标——它对
/// **全宽**恒等是对的，但 `perm = [0]`（单列、正好是表的第 0 列）也满足该判据，
/// 于是行保持"列清单宽度"没有补 NULL，下游 `build_insert_batch` 报
/// `VALUES rows have 1 entries, table t has 3 columns`。
///
/// 为什么此前没抓到：既有用例给的是 `(name)`（perm = [1]）与 `(id, v)`（perm = [0,2]），
/// **都不是前缀**。前缀恰好是驱动按 `setXxx` 顺序填前几列时最常见的形态。
#[test]
fn leading_prefix_column_list_is_not_identity() {
    let (db, _root) = setup("prefix");
    db.sql("INSERT INTO t (id) VALUES (1)").unwrap();
    db.sql("INSERT INTO t (id, name) VALUES (2, 'b')").unwrap();
    db.sql("INSERT INTO t (id, name, v) VALUES (3, 'c', 30)")
        .unwrap(); // 全宽：仍走恒等快路径
    assert_eq!(
        rows(&db, "SELECT id, name, v FROM t ORDER BY id"),
        vec!["1|NULL|NULL", "2|b|NULL", "3|c|30"]
    );
}
