//! CLI 表格输出的**两条硬约束**（2026-10-17）：
//!
//! 1. **不许 panic**。此前单元格截断写的是 `&v[..21]` —— **按字节**切片，
//!    21 一旦落在多字节字符中间（中文值/中文表意文字）就直接
//!    `panicked at end byte index 21 is not a char boundary`。更糟的是
//!    **前半屏已经打出去了**，看上去像"查询失败" —— 语料 `L5-01_01`
//!    因此被误记成引擎错误，白查了一圈。
//! 2. **重定向到文件/管道时不许截断**。截断只对"人在终端上看"有意义；
//!    输出一旦被程序消费，截断就是**静默丢数据**。这条已经连坑三次：
//!    ① `SHOW TABLES` 的截断表名拿去做 `DESCRIBE`，**39 张表被静默跳过**；
//!    ② 长列名被截进 DDL，拼出语法错；③ 上面那个 panic。
//!
//! 测试用 `Command::output()` 跑 ⇒ stdout 是**管道**（不是终端）
//! ⇒ 正好落在"不截断"这一支上。
//!
//! ⚠️ 触发点必须放在**值**上，不能放在列名上：本仓的标识符是
//! `[A-Za-z_][A-Za-z0-9_]*`，中文列名在 `CREATE TABLE` 就被拒（实测）。

use std::process::Command;

fn tmp(tag: &str) -> std::path::PathBuf {
    let p = std::env::temp_dir().join("qsdb_cli_out").join(format!(
        "{tag}_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn run(args: &[&str]) -> (bool, String, String) {
    let out = Command::new(env!("CARGO_BIN_EXE_qsdb"))
        .args(args)
        .output()
        .expect("spawn qsdb");
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

fn sql(db: &std::path::Path, s: &str) -> (bool, String, String) {
    run(&["query", "--path", &db.to_string_lossy(), "--", s])
}

fn sql_ok(db: &std::path::Path, s: &str) {
    let (ok, o, e) = sql(db, s);
    assert!(ok, "`{s}` 失败\nout: {o}\nerr: {e}");
}

#[test]
fn 多字节值不panic且重定向时不截断() {
    let db = tmp("cjk");
    sql_ok(&db, "CREATE TABLE t (k INT, item_name VARCHAR)");
    // 用真汉字（三字节），不复用上面的替换字符
    let long_cn = "abcdefghijklmnopqrs发用不平衡偏差电量";
    let (ok, _, err) = sql(&db, &format!("INSERT INTO t VALUES (1, '{long_cn}')"));
    assert!(ok, "插入失败: {err}");

    let (ok, out, err) = sql(&db, "SELECT k, item_name FROM t");
    assert!(ok, "查询失败（**这条以前是 panic**）:\n{err}");
    assert!(!err.contains("char boundary"), "又炸在字符边界上了: {err}");
    // 重定向 ⇒ 完整值必须原样在输出里，不许出现 `…`
    assert!(out.contains(long_cn), "长值被截断了:\n{out}");
    assert!(!out.contains('…'), "重定向输出不该有截断省略号:\n{out}");
}

/// 恰好压在多字节边界上的几种长度都要安全（22/23/24/25 字节各来一条）。
#[test]
fn 边界长度都不炸() {
    let db = tmp("edge");
    sql_ok(&db, "CREATE TABLE t (n INT, v VARCHAR)");
    for (i, s) in [
        "a".repeat(21),
        "a".repeat(19) + "平",
        "a".repeat(20) + "平",
        "a".repeat(21) + "平",
        "a".repeat(22) + "平",
        "平".repeat(12),
    ]
    .iter()
    .enumerate()
    {
        let (ok, _, e) = sql(&db, &format!("INSERT INTO t VALUES ({i}, '{s}')"));
        assert!(ok, "插入 `{s}` 失败: {e}");
    }
    let (ok, out, err) = sql(&db, "SELECT n, v FROM t");
    assert!(ok, "查询失败: {err}");
    assert!(!err.contains("char boundary"), "炸在字符边界上: {err}");
    for s in ["a".repeat(22) + "平", "平".repeat(12)] {
        assert!(out.contains(&s), "值 `{s}` 没原样出来:\n{out}");
    }
}

/// 多行多列 + 中文值：**列宽按字符数算**，竖线必须对齐
/// （此前 `width` 用 `s.len()` 是**字节数**、填充 `{:<width$}` 却按字符 —— 中文一律错位）。
#[test]
fn 多字节时列仍然对齐() {
    let db = tmp("width");
    sql_ok(&db, "CREATE TABLE t (name VARCHAR, v INT)");
    sql_ok(
        &db,
        "INSERT INTO t VALUES ('中', 1), ('中等长度', 22), ('一二三四五六七八九十十一十二', 333)",
    );
    let (ok, out, err) = sql(&db, "SELECT * FROM t ORDER BY v");
    assert!(ok, "查询失败: {err}");
    let cols: Vec<usize> = out
        .lines()
        .filter(|l| l.contains('|'))
        .map(|l| l.chars().position(|c| c == '|').unwrap())
        .collect();
    assert!(cols.len() >= 4, "输出行太少:\n{out}");
    assert!(
        cols.windows(2).all(|w| w[0] == w[1]),
        "分隔符没对齐（列宽按字节算了?）: {cols:?}\n{out}"
    );
}
