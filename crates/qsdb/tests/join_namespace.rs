//! JOIN **名字解析**（十九批 S1）的值级验收：裸名对右侧限定列的回退。
//!
//! 右表列在 ctx 里以 `table.col` **限定名**注册（歧义保护的既有结构）。此前
//! 裸名恒解析左表 —— 右侧的列必须写全限定，比标准更严。十九批起：精确名
//! 落空后做**限定列尾名回退**（唯一命中即绑、多命中歧义错、真没有才报
//! not found）—— PG/MySQL 同判，且**纯放宽**（精确命中优先，既有查询不变）。
//!
//! 数据：`l(k,m)` = (1,10) (2,20) (3,30)；`r(k,m,v)` = (2,20,200) (3,35,300) (4,40,400)

use std::sync::Arc;

use arrow::array::{Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use qsdb::{Qsdb, QsdbOptions};

fn tmp_root(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir()
        .join("qsdb_join_namespace")
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

fn seeded(tag: &str) -> (Qsdb, std::path::PathBuf) {
    let root = tmp_root(tag);
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    db.sql("CREATE TABLE l (k BIGINT, m BIGINT)").unwrap();
    db.sql("CREATE TABLE r (k BIGINT, m BIGINT, v BIGINT)")
        .unwrap();
    let schema2 = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("m", DataType::Int64, true),
    ]));
    let schema3 = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("m", DataType::Int64, true),
        Field::new("v", DataType::Int64, true),
    ]));
    db.append(
        "l",
        &[RecordBatch::try_new(
            schema2,
            vec![
                Arc::new(Int64Array::from(vec![1i64, 2, 3])),
                Arc::new(Int64Array::from(vec![10i64, 20, 30])),
            ],
        )
        .unwrap()],
    )
    .unwrap();
    db.append(
        "r",
        &[RecordBatch::try_new(
            schema3,
            vec![
                Arc::new(Int64Array::from(vec![2i64, 3, 4])),
                Arc::new(Int64Array::from(vec![20i64, 35, 40])),
                Arc::new(Int64Array::from(vec![200i64, 300, 400])),
            ],
        )
        .unwrap()],
    )
    .unwrap();
    (db, root)
}

fn col_i64(db: &Qsdb, sql: &str, c: usize) -> Vec<Option<i64>> {
    let r = db
        .sql(sql)
        .unwrap_or_else(|e| panic!("`{sql}` 应当成功：{e}"));
    let qsdb::sql::SqlResult::Query { batches, .. } = r else {
        panic!("`{sql}` 应当是查询结果");
    };
    let mut out = Vec::new();
    for b in &batches {
        let a = b
            .column(c)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("i64 列");
        for i in 0..a.len() {
            out.push(if a.is_null(i) { None } else { Some(a.value(i)) });
        }
    }
    out
}

/// 输出**列名**（S1 的另一半：名字解析不能把输出名搞乱）。
fn out_names(db: &Qsdb, sql: &str) -> Vec<String> {
    let r = db.sql(sql).unwrap_or_else(|e| panic!("`{sql}` 失败：{e}"));
    let qsdb::sql::SqlResult::Query { schema, .. } = r else {
        panic!("`{sql}` 应当是查询结果");
    };
    schema.fields().iter().map(|f| f.name().clone()).collect()
}

#[test]
fn 裸名解析到右侧限定列_on_形态() {
    let (db, root) = seeded("on");
    // `v` 只在右侧：此前报 "column 'v' not found (… qualify it as r.v)"
    let v = col_i64(&db, "SELECT v FROM l JOIN r ON l.k = r.k ORDER BY v", 0);
    assert_eq!(v, vec![Some(200), Some(300)], "裸名应解析到 r.v");
    // WHERE 里同样可见
    let n = col_i64(
        &db,
        "SELECT count(*) FROM l JOIN r ON l.k = r.k WHERE v = 300",
        0,
    );
    assert_eq!(n, vec![Some(1)]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn using_裸右列与限定合并列() {
    let (db, root) = seeded("using");
    // USING 合并后：右侧非合并列的裸名
    let v = col_i64(&db, "SELECT v FROM l JOIN r USING (k) ORDER BY v", 0);
    assert_eq!(v, vec![Some(200), Some(300)]);
    // 限定名 l.k / r.k **并存**且都指合并列（PG 同款）—— 匹配行上两者相等
    let both = col_i64(
        &db,
        "SELECT l.k, r.k FROM l JOIN r USING (k) ORDER BY l.k",
        0,
    );
    assert_eq!(both, vec![Some(2), Some(3)]);
    let _ = std::fs::remove_dir_all(&root);
}

/// **孤儿行的合并列取非空侧**（错数据级断言：取错一侧 = 合并列变 NULL，
/// `count(*)` 看不出来 —— 与 `join_using.rs` 的核心断言同款，这里锁的是
/// **裸名/限定名解析路径**上的同一个语义）。
#[test]
fn right_full_孤儿行合并列取非空侧() {
    let (db, root) = seeded("orphan");
    // k=1 只在左（RIGHT 的孤儿）、k=4 只在右（LEFT 的孤儿）
    let ks = col_i64(&db, "SELECT k FROM l RIGHT JOIN r USING (k) ORDER BY k", 0);
    assert_eq!(
        ks,
        vec![Some(2), Some(3), Some(4)],
        "k=4 的合并值须来自右表"
    );
    let ks = col_i64(&db, "SELECT k FROM l FULL JOIN r USING (k) ORDER BY k", 0);
    assert_eq!(ks, vec![Some(1), Some(2), Some(3), Some(4)]);
    let _ = std::fs::remove_dir_all(&root);
}

/// **精确名优先于尾名回退**：左右都有 `m`（左裸名 + 右限定名）⇒ 裸 `m`
/// 精确命中左侧 —— 本仓的既定解析序（文档写明；PG 在这里是歧义错，本仓
/// 刻意更宽容：既有查询的行为一个字不变）。
#[test]
fn 精确名优先_两侧同名取左() {
    let (db, root) = seeded("prec");
    let m = col_i64(&db, "SELECT m FROM l JOIN r ON l.k = r.k ORDER BY m", 0);
    assert_eq!(m, vec![Some(20), Some(30)], "裸 m 取左表的 m");
    let _ = std::fs::remove_dir_all(&root);
}

/// 多个限定列同名（`r.v` / `r2.v`）⇒ 裸名**歧义错**（与 F30 的重名口径一致）。
#[test]
fn 多个限定列同名_裸名歧义错() {
    let (db, root) = seeded("ambig");
    let e = db
        .sql("SELECT v FROM l JOIN r ON l.k = r.k JOIN r r2 ON l.k = r2.k")
        .unwrap_err();
    let msg = e.to_string();
    assert!(msg.contains("ambiguous"), "应当歧义错，实为：{msg}");
    let _ = std::fs::remove_dir_all(&root);
}

/// 输出列名不受解析路径影响：`SELECT v` 的输出名是 `v`（AST 决定），不是
/// 绑定到的 `r.v`。
#[test]
fn 输出名仍由_ast决定() {
    let (db, root) = seeded("names");
    let names = out_names(&db, "SELECT v, m FROM l JOIN r ON l.k = r.k");
    assert_eq!(names, vec!["v", "m"], "输出名 = 用户写的名字");
    let _ = std::fs::remove_dir_all(&root);
}
