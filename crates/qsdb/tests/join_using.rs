//! `USING (cols)` 的**值级**验收：它不是纯语法糖，关键在**输出列合并**。
//!
//! 标准 SQL 里 `a JOIN b USING (k)` 的 `SELECT *` 只出**一份** `k`，且那一份的
//! 取值随 join 类型变：INNER 两侧相等取谁都行、LEFT 取左、RIGHT 取右、
//! FULL 取 `COALESCE`。本实现统一用 `COALESCE(l.c, r.c)`（等值键下匹配对相等，
//! 孤儿行各自有值 ⇒ 四种一套覆盖）。所以孤儿行上的合并列**就是这一条的核心**：
//! 补错一侧 = 合并列变成 NULL（静默错值），而 `count(*)` 看不出来。
//!
//! 数据：`l(k,m)` = (1,10) (2,20) (3,30)；`r(k,m,v)` = (2,20,200) (3,35,300) (4,40,400)

use std::sync::Arc;

use arrow::array::{Array, Int64Array};
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
    let d = std::env::temp_dir().join("qsdb_join_using").join(format!(
        "{tag}_{}_{}",
        std::process::id(),
        format!("{:x}_{}", std::process::id(), _uniq_seq())
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
    db.append(
        "l",
        &[RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int64, true),
                Field::new("m", DataType::Int64, true),
            ])),
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
            Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int64, true),
                Field::new("m", DataType::Int64, true),
                Field::new("v", DataType::Int64, true),
            ])),
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
            .unwrap_or_else(|| panic!("第 {c} 列应当是 BIGINT"));
        for i in 0..a.len() {
            out.push(if a.is_null(i) { None } else { Some(a.value(i)) });
        }
    }
    out
}

fn n_cols(db: &Qsdb, sql: &str) -> usize {
    let r = db
        .sql(sql)
        .unwrap_or_else(|e| panic!("`{sql}` 应当成功：{e}"));
    let qsdb::sql::SqlResult::Query { schema, .. } = r else {
        panic!("`{sql}` 应当是查询结果");
    };
    schema.fields().len()
}

#[test]
fn using_的_select_star_只出一份合并列() {
    let (db, root) = seeded("using_star");
    // l(k,m) + r(k,m,v) 用 USING(k)：k 只出一份 ⇒ 4 列（k, l.m, r.m, r.v），
    // **不是** 5 列（那说明合并没做）
    assert_eq!(
        n_cols(&db, "SELECT * FROM l JOIN r USING (k)"),
        4,
        "USING 的 k 必须合并成一份"
    );
    assert_eq!(
        n_cols(&db, "SELECT * FROM l JOIN r ON l.k = r.k"),
        5,
        "对照组：普通 ON 写法那一列出现两次"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn using_的合并列在四种_join_下都取到值() {
    let (db, root) = seeded("using_values");
    // 左孤儿 k=1（只在 l）、右孤儿 k=4（只在 r）、匹配 k=2,3
    // **合并列不能是 NULL** —— 那正是"补错一侧"的症状。
    let cases = [
        ("JOIN", vec![Some(2), Some(3)]),
        ("LEFT JOIN", vec![Some(1), Some(2), Some(3)]),
        ("RIGHT JOIN", vec![Some(2), Some(3), Some(4)]),
        ("FULL JOIN", vec![Some(1), Some(2), Some(3), Some(4)]),
    ];
    for (jt, want) in cases {
        let sql = format!("SELECT k FROM l {jt} r USING (k) ORDER BY k");
        assert_eq!(
            col_i64(&db, &sql, 0),
            want,
            "`{jt} ... USING (k)` 的合并列应当覆盖两侧孤儿"
        );
    }
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn using_之后限定名会回退到裸名_已知偏差() {
    let (db, root) = seeded("using_qual_dev");
    // ⚠️ **实测出来的既有行为（不是 USING 引入的）**：限定名解析会**回退到裸名**
    // ——`l.k`、`r.k`、甚至拼错的 `zz.k` 都解析到同一列（合并后的 `k`）。
    // 证据：三条都 `Ok`（见本文件的探针注释）。
    //
    // 标准 SQL 里 `USING (k)` 之后 `l.k` / `r.k` 仍各指自己那一侧（PostgreSQL），
    // 本实现只留**合并列**一份；而"拼错限定名也照样解析"是本仓**更早就有的**
    // 行为（普通 join 上 `zz.k` 同样能解析）。⇒ 这是**已知问题**：
    // "静默回退"与本仓"拼错就报错"的纪律相反，但**不在本批范围内**。
    //
    // 本用例只钉住**现状**（改天修好后它会红，那时同步改这里的断言）。
    assert_eq!(
        col_i64(&db, "SELECT l.k FROM l FULL JOIN r USING (k) ORDER BY k", 0),
        vec![Some(1), Some(2), Some(3), Some(4)],
        "FULL 下 `l.k` 拿到的是**合并列**（右孤儿那行不是 NULL —— 与 PostgreSQL 不同）"
    );
    assert_eq!(
        col_i64(&db, "SELECT zz.k FROM l JOIN r USING (k) ORDER BY k", 0),
        vec![Some(2), Some(3)],
        "拼错的限定名也解析（回退到裸名）—— 既有行为，已记队列"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn using_多列() {
    let (db, root) = seeded("using_multi");
    // USING (k, m)：两列都合并 ⇒ `SELECT *` = 2 + 0 + 1 = 3 列（k, m, r.v）
    assert_eq!(n_cols(&db, "SELECT * FROM l JOIN r USING (k, m)"), 3);
    // 语义 = ON l.k = r.k AND l.m = r.m ⇒ 只有 k=2 的 m 也相等
    assert_eq!(
        col_i64(&db, "SELECT k FROM l JOIN r USING (k, m)", 0),
        vec![Some(2)]
    );
    assert_eq!(
        col_i64(&db, "SELECT m FROM l JOIN r USING (k, m)", 0),
        vec![Some(20)],
        "第二个合并列同样要出得来"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn using_缺列与混写都明确报错() {
    let (db, root) = seeded("using_mix");
    // `USING (k) AND ...` 不是合法 SQL（USING 自带连接条件）—— sqlparser 层面
    // 就走不到这里；这里只钉"USING 与 ON 不能混写"的解析结果不产生静默行为。
    // 直接用错误的列名验证报错是**明确**的：
    let e = db
        .sql("SELECT * FROM l JOIN r USING (nope)")
        .unwrap_err()
        .to_string();
    assert!(e.contains("nope"), "缺列必须点名：{e}");
    assert!(
        db.sql("SELECT * FROM l JOIN r USING (k) ON l.k = r.k")
            .is_err(),
        "USING 与 ON 同写必须报错"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn using_的合并列可解析且可参与运算() {
    let (db, root) = seeded("using_qual_ok");
    // 合并列是**裸名**（`k`），能绑、能算
    assert_eq!(col_i64(&db, "SELECT k FROM l JOIN r USING (k)", 0).len(), 2);
    assert_eq!(
        col_i64(&db, "SELECT k * 2 FROM l JOIN r USING (k) ORDER BY k", 0),
        vec![Some(4), Some(6)]
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn using_与聚合_where() {
    let (db, root) = seeded("using_agg");
    // 合并列参与 WHERE / 聚合（绑定要跟新列序对上）
    assert_eq!(
        col_i64(
            &db,
            "SELECT count(*) FROM l JOIN r USING (k) WHERE k > 2",
            0
        ),
        vec![Some(1)]
    );
    assert_eq!(
        col_i64(&db, "SELECT sum(k) FROM l FULL JOIN r USING (k)", 0),
        vec![Some(10)]
    );
    let _ = std::fs::remove_dir_all(&root);
}
