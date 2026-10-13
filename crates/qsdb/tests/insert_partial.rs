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

    assert_eq!(
        rows(&db, "SELECT id, name, v FROM t ORDER BY id NULLS LAST, v"),
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
