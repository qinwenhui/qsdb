//! **结果集列名口径**（F30，qsdb-sync 反馈）的验收。
//!
//! 设计见 `结果集列名口径`。三条要钉住的东西，**性质完全不同**：
//!
//! 1. **列名对齐 PG**：只剥表限定（`t.v` ⇒ `v`），**表达式名不动**
//!    （`count(*)` 保持 `count(*)`，不编造 `count`）。
//! 2. ★ **重名必须报错**（`ambiguous`）。这条不是洁癖：剥完限定符之后
//!    `SELECT a.v, b.v FROM a JOIN b` 的结果就是两个 `v`，而"JOIN 两侧同名列"
//!    极其常见 —— 静默取第一个 = **把一个能跑的查询变成静默错值**。
//! 3. ★ **限定名泄漏进派生表**导致正常 SQL 失败（还报了个讲 JOIN 的错，而那句里
//!    根本没有 JOIN）—— 这是 F30 之外、探针自己撞出来的功能缺陷。
//!
//! ⚠️ 反向也要钉：**绑定语义一个字都不能变**（限定名照常能写、`SELECT *` 要能
//! 透传重名列）。这几条是改动最容易碰坏的地方。

use qsdb::{Qsdb, QsdbOptions};

/// 测试目录唯一序号：pid 挡跨进程碰撞，序号挡同进程内并发（Windows 时钟
/// 粒度 ~100ns，纳秒后缀在同 tick 内会重复——CI 上真炸过，见 constraints）。
fn _uniq_seq() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    SEQ.fetch_add(1, Ordering::Relaxed)
}

fn setup(tag: &str) -> (Qsdb, std::path::PathBuf) {
    let root = std::env::temp_dir().join("qsdb_colnames").join(format!(
        "{tag}_{}_{}",
        std::process::id(),
        format!("{:x}_{}", std::process::id(), _uniq_seq())
    ));
    std::fs::create_dir_all(&root).unwrap();
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    db.sql("CREATE TABLE t (id BIGINT, v BIGINT)").unwrap();
    db.sql("CREATE TABLE u (id BIGINT, name VARCHAR)").unwrap();
    db.sql("INSERT INTO t VALUES (1,10),(2,20)").unwrap();
    db.sql("INSERT INTO u VALUES (1,'a'),(2,'b')").unwrap();
    (db, root)
}

fn names(db: &Qsdb, sql: &str) -> Vec<String> {
    let b = db
        .sql_collect(sql)
        .unwrap_or_else(|e| panic!("`{sql}`：{e}"));
    b.first()
        .map(|b| {
            b.schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect()
        })
        .unwrap_or_default()
}

/// 逐行 `c0|c1|…`，**保持引擎给的行序**。
///
/// ⚠️ **凡是断言 `ORDER BY` 的用例必须用这个**，不能用下面那个排序版 ——
/// 拿排序过的结果去断言"排对了"，断言**恒真**（尺子歪了，被测物看不出来）。
/// 这条是写本文件时真踩的：`ORDER BY … DESC` 的用例用了排序版，当场"失败"，
/// 而失败原因是**助手**而不是引擎。
fn rows_ordered(db: &Qsdb, sql: &str) -> Vec<String> {
    let bs = db
        .sql_collect(sql)
        .unwrap_or_else(|e| panic!("`{sql}`：{e}"));
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

/// 同上去重排序 —— **只给行序无意义的断言用**（集合语义）。
fn rows(db: &Qsdb, sql: &str) -> Vec<String> {
    let mut out = rows_ordered(db, sql);
    out.sort();
    out
}

fn rejected(db: &Qsdb, sql: &str) -> String {
    match db.sql_collect(sql) {
        Ok(_) => panic!("这条该被拒：{sql}"),
        Err(e) => e.to_string(),
    }
}

// ══ ① 列名：只剥限定符，表达式原样 ═════════════════════════════════════════

#[test]
fn 列名剥限定符而表达式原样() {
    let (db, root) = setup("names");
    let cases: Vec<(&str, Vec<&str>)> = vec![
        ("SELECT t.v FROM t", vec!["v"]),
        (
            "SELECT t.id, u.name FROM t JOIN u ON t.id = u.id",
            vec!["id", "name"],
        ),
        // ★ 表达式**不**剥、也**不**编造名字：
        //   PG 给 `count` —— 那是用户从没写过的名字，本仓明确不做（见设计稿 §二）
        ("SELECT count(*) FROM t", vec!["count(*)"]),
        ("SELECT sum(v) FROM t", vec!["sum(v)"]),
        ("SELECT t.v + 1 FROM t", vec!["t.v + 1"]),
        // 起了别名就听用户的
        ("SELECT t.v AS v FROM t", vec!["v"]),
        ("SELECT t.v AS w FROM t", vec!["w"]),
        // 单表 `SELECT *` 本来就没有限定
        ("SELECT * FROM t", vec!["id", "v"]),
    ];
    for (sql, want) in cases {
        assert_eq!(names(&db, sql), want, "`{sql}`");
    }

    // ★ `SELECT *` 跨 join：原来左裸右限定（`id, v, u.id, u.name`），PG 是四个裸名
    assert_eq!(
        names(&db, "SELECT * FROM t JOIN u ON t.id = u.id"),
        vec!["id", "v", "id", "name"]
    );
    std::fs::remove_dir_all(&root).ok();
}

// ══ ② ★ 限定名泄漏进派生表 ⇒ 正常 SQL 失败（F30 之外、探针撞出来的）═══════

#[test]
fn 派生表能按裸名引用() {
    let (db, root) = setup("derived");
    // 改之前：`not found: column 'v' (a JOIN right-side column with this name
    // exists …)` —— **那句里根本没有 JOIN**，用户会往完全错的方向排查。
    assert_eq!(names(&db, "SELECT v FROM (SELECT t.v FROM t) x"), vec!["v"]);
    assert_eq!(
        rows(&db, "SELECT v FROM (SELECT t.v FROM t) x"),
        vec!["10", "20"]
    );
    // 限定名（派生表别名）也照样能用
    assert_eq!(
        rows(&db, "SELECT x.v FROM (SELECT t.v FROM t) x"),
        vec!["10", "20"]
    );
    assert_eq!(names(&db, "SELECT * FROM (SELECT t.v FROM t) x"), vec!["v"]);
    // 报错文案不许再讲 JOIN（把一种情况说成另一种，F29 同族）
    let m = rejected(&db, "SELECT zz FROM (SELECT t.v FROM t) x");
    assert!(!m.contains("JOIN"), "派生表的报错不该提 JOIN：{m}");
    std::fs::remove_dir_all(&root).ok();
}

// ══ ③ ★★ 重名 ⇒ 报错（不是静默取第一个）══════════════════════════════════

#[test]
fn 重名引用必须报错() {
    let (db, root) = setup("ambig");
    for sql in [
        "SELECT v FROM (SELECT 1 AS v, 2 AS v) x",
        "SELECT v FROM (SELECT t.id AS v, u.id AS v FROM t JOIN u ON t.id = u.id) x",
        // ★ JOIN 两侧同名列 —— 改之前这条路「能用」（列名带限定所以唯一），
        //   剥完之后**必须**报错而不是静默取左边那个
        "SELECT id FROM (SELECT t.id, u.id FROM t JOIN u ON t.id = u.id) x",
    ] {
        let m = rejected(&db, sql);
        assert!(m.contains("ambiguous"), "`{sql}` 该报 ambiguous，实得：{m}");
    }
    // 值级反证：**不许**静默取第一个（改之前就是 1 / 2 这种"看着对"的回值）
    let m = rejected(&db, "SELECT v FROM (SELECT 1 AS v, 2 AS v) x");
    assert!(m.contains("ambiguous"), "{m}");
    std::fs::remove_dir_all(&root).ok();
}

/// 但 `SELECT *` **要能透传**重名列 —— 那是通配展开，不是"引用一个名字"。
/// （PG 同样允许 `SELECT * FROM (SELECT a.id, b.id …) x` 出两个 `id`。）
#[test]
fn select_星号透传重名列() {
    let (db, root) = setup("star");
    assert_eq!(
        names(
            &db,
            "SELECT * FROM (SELECT t.id, u.id FROM t JOIN u ON t.id = u.id) x"
        ),
        vec!["id", "id"]
    );
    assert_eq!(
        rows(
            &db,
            "SELECT * FROM (SELECT t.id, u.id FROM t JOIN u ON t.id = u.id) x"
        ),
        vec!["1|1", "2|2"]
    );
    std::fs::remove_dir_all(&root).ok();
}

// ══ ④ 绑定语义一个字都不能变 ═══════════════════════════════════════════════

#[test]
fn 绑定不回归() {
    let (db, root) = setup("bind");
    // 限定名照常能写（只是**输出名**变了）
    assert_eq!(rows(&db, "SELECT t.v FROM t WHERE t.v > 10"), vec!["20"]);
    // 未限定名照常（单表）
    assert_eq!(rows(&db, "SELECT v FROM t WHERE v > 10"), vec!["20"]);
    // ORDER BY 限定名 / 别名 / 裸名
    assert_eq!(
        rows_ordered(&db, "SELECT t.id FROM t ORDER BY t.id DESC"),
        vec!["2", "1"]
    );
    assert_eq!(
        rows_ordered(&db, "SELECT t.v AS w FROM t ORDER BY w DESC"),
        vec!["20", "10"]
    );
    // GROUP BY + ORDER BY 限定名（列名剥了之后 ORDER BY 还要认得出，
    // 靠的是"按投影**表达式原文**匹配"而不是"按末段名"）
    assert_eq!(
        rows_ordered(
            &db,
            "SELECT t.id, count(*) AS n FROM t GROUP BY t.id ORDER BY t.id"
        ),
        vec!["1|1", "2|1"]
    );
    // HAVING 里用限定名
    assert_eq!(
        rows(
            &db,
            "SELECT t.id AS g, count(*) AS n FROM t GROUP BY t.id HAVING t.id > 1"
        ),
        vec!["2|1"]
    );
    // 相关子查询里的限定名
    assert_eq!(
        rows(
            &db,
            "SELECT t.id FROM t WHERE t.id IN (SELECT u.id FROM u WHERE u.name = 'a')"
        ),
        vec!["1"]
    );
    // `SELECT *` 拿来再投影
    assert_eq!(
        rows(&db, "SELECT v FROM (SELECT * FROM t) x"),
        vec!["10", "20"]
    );
    std::fs::remove_dir_all(&root).ok();
}

/// ★ ORDER BY 的表达式匹配**不许串列**：
/// `ORDER BY b.id` 不能因为"末段叫 id"就排到 `a.id` 上去 —— 那是值全对、
/// 顺序错，最难发现的一类。这里让它落到"按输入表达式绑定"的正确那条路。
#[test]
fn order_by_不许串到别的投影列() {
    let (db, root) = setup("ordersort");
    // 输出两列都叫 id；ORDER BY 用的是**没被投影**的 u.name
    assert_eq!(
        rows_ordered(
            &db,
            "SELECT t.id, u.id FROM t JOIN u ON t.id = u.id ORDER BY u.name"
        ),
        vec!["1|1", "2|2"]
    );
    // 显式限定到某一侧时，只能按那一侧排
    assert_eq!(
        rows_ordered(
            &db,
            "SELECT t.id, u.id FROM t JOIN u ON t.id = u.id ORDER BY t.id DESC"
        ),
        vec!["2|2", "1|1"]
    );
    std::fs::remove_dir_all(&root).ok();
}

// ══ ⑤ CTAS 不回归（它本来就剥、本来就拒表达式名）═══════════════════════════

#[test]
fn ctas_口径不变() {
    let (db, root) = setup("ctas");
    db.sql("CREATE TABLE c1 AS SELECT t.id, u.name FROM t JOIN u ON t.id = u.id")
        .unwrap();
    assert_eq!(names(&db, "SELECT * FROM c1"), vec!["id", "name"]);

    // `count(*)` **仍然拒**（表列名必须是标识符；改成 `count` 是编造用户没写过的名）
    let m = rejected(&db, "CREATE TABLE c2 AS SELECT count(*) FROM t");
    assert!(m.contains("不是合法表列名") && m.contains("AS col0"), "{m}");

    // 重名仍然拒
    let m = rejected(
        &db,
        "CREATE TABLE c3 AS SELECT t.id, u.id FROM t JOIN u ON t.id = u.id",
    );
    assert!(m.contains("重复"), "{m}");
    std::fs::remove_dir_all(&root).ok();
}
