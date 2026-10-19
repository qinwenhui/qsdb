//! `date_format(x, fmt)` —— **Spark 语义**的回归（2026-10-17）。
//!
//! ## 为什么要专门钉这一条
//!
//! **Spark 和 MySQL 有同名的 `date_format`，语法却完全不同**：
//!
//! | | 格式串 | 例子 |
//! |---|---|---|
//! | **Spark**（Java `SimpleDateFormat`） | `yyyy-MM-dd` | `2026-04-01` |
//! | MySQL | `%Y-%m-%d` | `2026-04-01` |
//!
//! 真实语料那批 SQL 是 **Spark SQL** ⇒ 本仓按 Spark 实现 ✓，
//! 并把 `%` 开头的模式**显式拒**（见 `不支持的格式符必须显式报错`）——
//! 把 `%Y` 当普通字符原样输出是**静默错值**（结果看着像日期，其实全是字面量）。
//!
//! 下面每一格的期望值都是 **`.14` 上真 Spark 3.5.2 的实测输出**。

use qsdb::{Qsdb, QsdbOptions};

/// 测试目录唯一序号：pid 挡跨进程碰撞，序号挡同进程内并发（Windows 时钟
/// 粒度 ~100ns，纳秒后缀在同 tick 内会重复——CI 上真炸过，见 constraints）。
fn _uniq_seq() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    SEQ.fetch_add(1, Ordering::Relaxed)
}

fn setup(tag: &str) -> (Qsdb, std::path::PathBuf) {
    let root = std::env::temp_dir().join("qsdb_date_format").join(format!(
        "{tag}_{:x}_{}",
        std::process::id(),
        _uniq_seq()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    db.sql("CREATE TABLE t (d DATE, ts TIMESTAMP)").unwrap();
    db.sql("INSERT INTO t VALUES (DATE '2026-04-01', TIMESTAMP '2026-04-01 13:05:07')")
        .unwrap();
    (db, root)
}

fn one(db: &Qsdb, sql: &str) -> String {
    let bs = db.sql_collect(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    let b = bs.first().unwrap();
    arrow::util::display::array_value_to_string(b.column(0).as_ref(), 0).unwrap()
}

/// ★ 六格，**全部与真 Spark 逐字相同**（含单字母不补零的行为）。
#[test]
fn 与_spark_逐字相同() {
    let (db, root) = setup("same");
    for (fmt, want) in [
        ("yyyy-MM-dd", "2026-04-01"),
        ("yyyy-MM-dd HH:mm:ss", "2026-04-01 13:05:07"),
        ("yyyyMMdd", "20260401"),
        ("MM/dd/yyyy", "04/01/2026"),
        ("SSS", "000"),
        // **单字母不补零**（Java 语义；`yy-M-d H:m:s` 里 M/d/H/m/s 都是 1 位）
        ("yy-M-d H:m:s", "26-4-1 13:5:7"),
    ] {
        assert_eq!(
            one(&db, &format!("SELECT date_format(ts, '{fmt}') FROM t")),
            want,
            "date_format(ts, '{fmt}') 与 Spark 不一致"
        );
    }
    // DATE（无时间部分）也要能格式化
    assert_eq!(
        one(&db, "SELECT date_format(d, 'MM/dd/yyyy') FROM t"),
        "04/01/2026"
    );
    drop(db);
    let _ = std::fs::remove_dir_all(root);
}

/// ★ **不支持的格式符必须显式报错**，绝不能静默当字面量输出。
///
/// 两档：`%Y`（MySQL 的语法，**指路**到 Java 模式）与没实现的字母（`EEE` 月份名等）。
#[test]
fn 不支持的格式符必须显式报错() {
    let (db, root) = setup("reject");
    let e = db
        .sql_collect("SELECT date_format(ts, '%Y-%m-%d') FROM t")
        .unwrap_err()
        .to_string();
    assert!(
        e.contains("MySQL") && e.contains("Spark"),
        "`%Y` 该指路到 Spark 的 Java 模式，实得：{e}"
    );
    let e = db
        .sql_collect("SELECT date_format(ts, 'EEE') FROM t")
        .unwrap_err()
        .to_string();
    assert!(e.contains("`EEE`"), "该点名是哪个格式符没实现，实得：{e}");
    // 非字符串字面量（逐行格式串）也没实现 —— 同样**显式**拒（绑定器类型检查那层
    // 先拦下，文案是 `<java pattern> expected`；能到求值层则是"必须是常量字符串"）
    let e = db
        .sql_collect("SELECT date_format(ts, d) FROM t")
        .unwrap_err()
        .to_string();
    assert!(
        e.contains("java pattern") || e.contains("常量字符串"),
        "该显式拒（而不是静默把列值当格式串），实得：{e}"
    );
    drop(db);
    let _ = std::fs::remove_dir_all(root);
}
