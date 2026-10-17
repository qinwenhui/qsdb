//! `least` / `greatest` —— **Spark 语义**的回归（2026-10-16）。
//!
//! ## 为什么要专门钉这一条
//!
//! 日清分那批结算 SQL 用了 `least` 21 次 / `greatest` 10 次，而且**几乎全是嵌套**
//! （`greatest(0, least(a, b))`）+ 小数 —— 也就是 `Fold` 套 `Fold`。
//! **NULL 语义是这里最容易做错的地方**：Spark 的 `least`/`greatest` **忽略 NULL**，
//! 只在**全部**为 NULL 时返回 NULL —— 与 `MIN()` 聚合的"见 NULL 就当 NULL"不同，
//! 也与"NULL 参与比较"的天真实现不同（后者会把整行变成 NULL，**静默错值**）。
//!
//! 下面每一格的期望值都是 **`.14` 上真 Spark 3.5.2 的实测输出**（2026-10-16）。

use qsdb::{Qsdb, QsdbOptions};

fn setup(tag: &str) -> (Qsdb, std::path::PathBuf) {
    let root = std::env::temp_dir()
        .join("qsdb_least_greatest")
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
    (db, root)
}

fn cell(db: &Qsdb, sql: &str) -> String {
    let bs = db.sql_collect(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    let b = bs.first().unwrap();
    arrow::util::display::array_value_to_string(b.column(0).as_ref(), 0).unwrap()
}

/// ★ 真 Spark 3.5.2 原样输出：
/// ```text
/// a  b  c  d    e     f
/// 1  3  1  3    NULL  NULL
/// ```
#[test]
fn 字面量_null_语义与_spark_相同() {
    let (db, _r) = setup("lit");
    assert_eq!(cell(&db, "SELECT least(1,2,3)"), "1");
    assert_eq!(cell(&db, "SELECT greatest(1,2,3)"), "3");
    // ★ 这两格是重点：中间的 NULL **被忽略**，不是整行变 NULL
    assert_eq!(cell(&db, "SELECT least(1,NULL,3)"), "1");
    assert_eq!(cell(&db, "SELECT greatest(1,NULL,3)"), "3");
    // 全 NULL ⇒ NULL
    assert_eq!(cell(&db, "SELECT least(CAST(NULL AS INT), NULL)"), "");
    assert_eq!(cell(&db, "SELECT greatest(NULL, NULL)"), "");
}

/// 真 Spark：`least('b','a','c')` ⇒ `a`；`greatest('b','a','c')` ⇒ `c`。
#[test]
fn 字符串按序比较() {
    let (db, _r) = setup("str");
    assert_eq!(cell(&db, "SELECT least('b','a','c')"), "a");
    assert_eq!(cell(&db, "SELECT greatest('b','a','c')"), "c");
}

/// 真 Spark：
/// ```text
/// d1    d2
/// 1.50  2.00
/// ```
/// ⇒ 小数走 DECIMAL 精确比较，**不是** f64（结算金额不许有浮点误差）。
#[test]
fn 小数走精确比较() {
    let (db, _r) = setup("dec");
    let sql_l = "SELECT least(CAST(1.50 AS DECIMAL(10,2)), CAST(2 AS DECIMAL(10,2)))";
    let sql_g = "SELECT greatest(CAST(1.50 AS DECIMAL(10,2)), CAST(2 AS DECIMAL(10,2)))";
    assert_eq!(cell(&db, sql_l), "1.50");
    assert_eq!(cell(&db, sql_g), "2.00");
}

/// 真 Spark：`least(1, CAST(2.5 AS DOUBLE))` ⇒ `1.0`；`greatest(...)` ⇒ `2.5`
/// （整数 + double ⇒ 公共超类型 double，不是 DECIMAL）。
#[test]
fn 整数与_double_的结果是_double() {
    let (db, _r) = setup("dbl");
    assert_eq!(cell(&db, "SELECT least(1, CAST(2.5 AS DOUBLE))"), "1.0");
    assert_eq!(cell(&db, "SELECT greatest(1, CAST(2.5 AS DOUBLE))"), "2.5");
}

/// **语料里的真实形状**：`least` 套 `least`、`COALESCE` 混在里面，且列里有 NULL。
/// 期望值由逐行手算给出（列值见 INSERT）。
#[test]
fn 嵌套折叠_与列上的_null_混合() {
    let (db, _r) = setup("nested");
    // a: 1 行 NULL、b: 每行都有值、c: 另有 NULL
    db.sql("CREATE TABLE t (a DECIMAL(10,2), b DECIMAL(10,2), c DECIMAL(10,2))")
        .unwrap();
    db.sql(
        "INSERT INTO t VALUES \
         (1.00, 5.00, NULL), (NULL, 2.00, 3.00), (NULL, NULL, NULL), (4.00, NULL, NULL)",
    )
    .unwrap();

    // ① 老写法（COALESCE 兜 0）—— 语料里的 `least(COALESCE(x,0.0), ...)`
    let bs = db
        .sql_collect("SELECT least(COALESCE(a, 0), b) AS r FROM t ORDER BY r")
        .unwrap();
    let got: Vec<String> = (0..bs[0].num_rows())
        .map(|i| arrow::util::display::array_value_to_string(bs[0].column(0).as_ref(), i).unwrap())
        .collect();
    // 行1: least(1, 5) = 1；行2: least(0, 2) = 0；行3: least(0, NULL) = 0（忽略 NULL）；
    // 行4: least(4, NULL) = 4
    assert_eq!(got, vec!["0.00", "0.00", "1.00", "4.00"]);

    // ② 裸列、不兜 COALESCE —— NULL 被忽略；全 NULL 行 ⇒ NULL
    let bs = db
        .sql_collect("SELECT greatest(0, least(a, b), c) AS r FROM t")
        .unwrap();
    let got: Vec<String> = (0..bs[0].num_rows())
        .map(|i| arrow::util::display::array_value_to_string(bs[0].column(0).as_ref(), i).unwrap())
        .collect();
    // 行1: least(1,5)=1，greatest(0,1,NULL)=1
    // 行2: least(NULL,2)=2，greatest(0,2,3)=3
    // 行3: 全 NULL ⇒ least 得 NULL；greatest(0, NULL, NULL) = 0
    // 行4: least(4,NULL)=4，greatest(0,4,NULL)=4
    assert_eq!(got, vec!["1.00", "3.00", "0.00", "4.00"]);
}

/// 参数个数：`least(x)` 单参**显式拒**，不能静默当恒等。
#[test]
fn 单参显式拒() {
    let (db, _r) = setup("arity");
    let e = db.sql_collect("SELECT least(1)").unwrap_err();
    let s = e.to_string();
    assert!(s.contains(">= 2"), "unexpected: {s}");
}

/// **回归**：`CAST(NULL AS <类型>)` 曾经被**拒**（绑定期闸门与求值侧不同步 ——
/// 见 `translate.rs` 里那段注释）。字面量 NULL 的兜底类型是 Boolean，
/// 而过 `cast_rule(Boolean, 数值类型)` 会被判 Impossible，于是这条最普通的写法报
/// `cannot cast BOOLEAN to INT`。求值侧早就按"全 NULL 短路"处理了，只有闸门没跟上。
#[test]
fn cast_null_到任意类型都合法() {
    let (db, _r) = setup("cast_null");
    for (sql, want) in [
        ("SELECT CAST(NULL AS INT)", ""),
        ("SELECT CAST(NULL AS DECIMAL(10,2))", ""),
        ("SELECT CAST(NULL AS VARCHAR)", ""),
        ("SELECT CAST(NULL AS DATE)", ""),
        ("SELECT coalesce(CAST(NULL AS INT), 1)", "1"),
        (
            "SELECT CASE WHEN 1=2 THEN CAST(NULL AS INT) ELSE 1 END",
            "1",
        ),
    ] {
        assert_eq!(cell(&db, sql), want, "sql={sql}");
    }
}

/// **除零 ⇒ NULL**（Spark 语义）：实测真 Spark 3.5.2 三格全是 `NULL`
/// （`1/0` / 小数除零 / `1%0`）。数据相关的运行期状况不该让整批挂掉，
/// 与"小数**溢出**报错"是两条不同口径（后者见 `eval.rs` 里那段注释）。
#[test]
fn 除零给_null_而不是报错() {
    let (db, _r) = setup("divzero");
    assert_eq!(cell(&db, "SELECT 1/0"), "");
    assert_eq!(cell(&db, "SELECT 1%0"), "");
    assert_eq!(
        cell(
            &db,
            "SELECT CAST(1.0 AS DECIMAL(10,2)) / CAST(0 AS DECIMAL(10,2))"
        ),
        ""
    );
    // 非零仍然正常。⚠️ 小数除法的结果 scale 按 Spark 规则是
    // `max(6, s1 + p2 + 1)` = max(6, 2+10+1) = **13**（不是 2）——
    // 所以是 `1.5000000000000`。这里**不改数字去迁就直觉**：
    // 结算场景下这个 scale 是规格的一部分。
    assert_eq!(
        cell(
            &db,
            "SELECT CAST(6 AS DECIMAL(10,2)) / CAST(4 AS DECIMAL(10,2))"
        ),
        "1.5000000000000"
    );
}

/// **`COALESCE(整数列, 0.0)` 的公共超类型必须给整数留位**。
///
/// 真 Spark 3.5.2 实测（2026-10-16）：`coalesce(cast(1 AS INT), 0.0)` ⇒ `decimal(11,1)`、
/// tinyint ⇒ `decimal(4,1)`、bigint ⇒ `decimal(21,1)`（= 1 + 3/5/10/20）。
///
/// 不留位会当场炸：`0.0` 是 `DECIMAL(1,1)`，整数列 cast 过去，值 `1` 装不下
/// （1.0 要 2 位）⇒ `value 1.0 overflows DECIMAL(1, 1)`。语料里 3 个块卡在这条上。
#[test]
fn coalesce_整数与小数留位() {
    let (db, _r) = setup("decimal_int_promote");
    db.sql("CREATE TABLE s (i INT, ti TINYINT, b BIGINT)")
        .unwrap();
    db.sql("INSERT INTO s VALUES (1, 1, 1)").unwrap();
    // 关键：**不再报溢出**，且整数侧的值原样保住
    assert_eq!(cell(&db, "SELECT coalesce(i, 0.0) FROM s"), "1.0");
    assert_eq!(cell(&db, "SELECT coalesce(ti, 0.0) FROM s"), "1.0");
    assert_eq!(cell(&db, "SELECT coalesce(b, 0.0) FROM s"), "1.0");
    // 类型也钉一下：decimal(11,1) ⇒ 显示成 1.0（scale 1）
    assert_eq!(cell(&db, "SELECT nvl(i, 0.0) FROM s"), "1.0");
    // 小数侧的值不受影响
    assert_eq!(cell(&db, "SELECT coalesce(CAST(NULL AS INT), 0.0)"), "0.0");
}
