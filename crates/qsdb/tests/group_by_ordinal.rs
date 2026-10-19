//! **生态功能批次 2：`GROUP BY 1` 位置序号**（+ 顺带修好"表达式分组键"）。
//!
//! 为什么非做不可：`ORDER BY 1` 早就支持、`GROUP BY 1` 不支持 —— 这个**不对称**
//! 会反复踩（qsdb-sync 的 F29② 实测报的还是"列 'g' 必须出现在 GROUP BY 里"，
//! 把"没实现"说成"你写错了"）。
//!
//! 落地时**顺带挖出一个更大的洞**：`GROUP BY <表达式>`（`upper(s)` / `v % 2` /
//! `CAST(...)` / `CASE WHEN ...` …）**全都不工作** —— 根因是判据用了两个**不同的
//! 渲染器**（AST 的 `to_string()` 带不出 `PhysExpr::display()` 的括号，
//! `(g + 1)` vs `g + 1`），永远比不中。序号只是个引子：`GROUP BY 1` 指向表达式时
//! 会撞同一堵墙。本文件把两类一起钉住。

use qsdb::{Qsdb, QsdbOptions};

/// 测试目录唯一序号：pid 挡跨进程碰撞，序号挡同进程内并发（Windows 时钟
/// 粒度 ~100ns，纳秒后缀在同 tick 内会重复——CI 上真炸过，见 constraints）。
fn _uniq_seq() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    SEQ.fetch_add(1, Ordering::Relaxed)
}

fn setup(tag: &str) -> (Qsdb, std::path::PathBuf) {
    let root = std::env::temp_dir().join("qsdb_groupord").join(format!(
        "{tag}_{}_{}",
        std::process::id(),
        format!("{:x}_{}", std::process::id(), _uniq_seq())
    ));
    std::fs::create_dir_all(&root).unwrap();
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    db.sql("CREATE TABLE t (g BIGINT, v BIGINT, s VARCHAR)")
        .unwrap();
    db.sql("INSERT INTO t VALUES (1,10,'a'),(2,20,'A'),(1,30,'b')")
        .unwrap();
    (db, root)
}

/// 查全部列 → "c0|c1|…"，**排序后**（不把行序耦合进值级断言）。
fn q(db: &Qsdb, sql: &str) -> Vec<String> {
    let batches = db.sql_collect(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    let mut out = Vec::new();
    for b in &batches {
        for i in 0..b.num_rows() {
            out.push(
                (0..b.num_columns())
                    .map(|c| {
                        arrow::util::display::array_value_to_string(b.column(c).as_ref(), i)
                            .unwrap()
                    })
                    .collect::<Vec<_>>()
                    .join("|"),
            );
        }
    }
    out.sort();
    out
}

fn err_of(db: &Qsdb, sql: &str) -> String {
    match db.sql(sql) {
        Ok(_) => panic!("这条必须被拒: {sql}"),
        Err(e) => e.to_string(),
    }
}

// ── ① 序号指向**列**：与写列名完全等价 ────────────────────────────────────
#[test]
fn 序号与列名等价() {
    let (db, root) = setup("eq");
    assert_eq!(
        q(&db, "SELECT g, count(*) FROM t GROUP BY 1 ORDER BY 1"),
        q(&db, "SELECT g, count(*) FROM t GROUP BY g ORDER BY g"),
    );
    assert_eq!(
        q(&db, "SELECT g, count(*) FROM t GROUP BY 1 ORDER BY 1"),
        vec!["1|2", "2|1"]
    );
    // 输出别名也照常（`GROUP BY 1` 指的是**投射第 1 个表达式**，不是别名）
    assert_eq!(
        q(&db, "SELECT v AS vv, count(*) FROM t GROUP BY 1 ORDER BY 1"),
        vec!["10|1", "20|1", "30|1"]
    );
    // 序号排在聚合前面也认（位置 2 = g）
    assert_eq!(
        q(&db, "SELECT count(*), g FROM t GROUP BY 2 ORDER BY 1"),
        vec!["1|2", "2|1"]
    );
    let _ = std::fs::remove_dir_all(&root);
}

// ── ② 序号指向**表达式**（这批的真正难点）────────────────────────────────
#[test]
fn 序号指向表达式() {
    let (db, root) = setup("expr");
    // upper('a')='A' / upper('A')='A' / upper('b')='B'
    assert_eq!(
        q(
            &db,
            "SELECT upper(s), count(*) FROM t GROUP BY 1 ORDER BY 1"
        ),
        vec!["A|2", "B|1"]
    );
    // v % 2：10→0 / 20→0 / 30→0 ⇒ 全落一组
    assert_eq!(
        q(&db, "SELECT v % 2, count(*) FROM t GROUP BY 1 ORDER BY 1"),
        vec!["0|3"]
    );
    assert_eq!(
        q(
            &db,
            "SELECT CAST(v AS VARCHAR), count(*) FROM t GROUP BY 1 ORDER BY 1"
        ),
        vec!["10|1", "20|1", "30|1"]
    );
    assert_eq!(
        q(
            &db,
            "SELECT CASE WHEN v > 15 THEN 'hi' ELSE 'lo' END, count(*) FROM t GROUP BY 1 ORDER BY 1"
        ),
        vec!["hi|2", "lo|1"]
    );
    assert_eq!(
        q(
            &db,
            "SELECT s || '!', count(*) FROM t GROUP BY 1 ORDER BY 1"
        ),
        vec!["A!|1", "a!|1", "b!|1"]
    );
    assert_eq!(
        q(&db, "SELECT g + 1, count(*) FROM t GROUP BY 1 ORDER BY 1"),
        vec!["2|2", "3|1"]
    );
    assert_eq!(
        q(&db, "SELECT -v, count(*) FROM t GROUP BY 1 ORDER BY 1"),
        vec!["-10|1", "-20|1", "-30|1"]
    );
    let _ = std::fs::remove_dir_all(&root);
}

// ── ③ 显式写表达式（不经序号）也必须能跑 —— 这是**同一堵墙**的另一面 ──────
//
// 此前 `GROUP BY upper(s)` 自己就报"列 's' 不在 GROUP BY 里"（判据用错了渲染器）。
// 只测序号的话，会以为"序号做好了、表达式也顺带好了"。
#[test]
fn 显式表达式分组键() {
    let (db, root) = setup("expkey");
    assert_eq!(
        q(
            &db,
            "SELECT upper(s), count(*) FROM t GROUP BY upper(s) ORDER BY 1"
        ),
        vec!["A|2", "B|1"]
    );
    assert_eq!(
        q(
            &db,
            "SELECT v + 1, count(*) FROM t GROUP BY v + 1 ORDER BY 1"
        ),
        vec!["11|1", "21|1", "31|1"]
    );
    // 子句位置不同（HAVING 也走同一条重绑路径）
    assert_eq!(
        q(
            &db,
            "SELECT upper(s) AS u, count(*) FROM t GROUP BY 1 HAVING count(*) > 1 ORDER BY 1"
        ),
        vec!["A|2"]
    );
    let _ = std::fs::remove_dir_all(&root);
}

// ── ④ 该拒的都拒：每条都要能分辨"拒了"与"默默给错值" ─────────────────────
#[test]
fn 序号的拒绝面() {
    let (db, root) = setup("reject");
    // 越界：报清楚有幾列，别默默并成一组
    let e = err_of(&db, "SELECT g, count(*) FROM t GROUP BY 0");
    assert!(e.contains("超出投射列范围"), "{e}");
    let e = err_of(&db, "SELECT g, count(*) FROM t GROUP BY 5");
    assert!(e.contains("超出投射列范围"), "{e}");
    // 不是整数：裸数字按标准是序号，`1.5` 必是笔误
    let e = err_of(&db, "SELECT g, count(*) FROM t GROUP BY 1.5");
    assert!(e.contains("位置序号"), "{e}");
    // 指向聚合：不能当分组键
    let e = err_of(&db, "SELECT count(*) FROM t GROUP BY 1");
    assert!(e.contains("聚合不能当分组键"), "{e}");
    let e = err_of(&db, "SELECT g, count(*) FROM t GROUP BY 1, 2");
    assert!(e.contains("聚合不能当分组键"), "{e}");
    // 指向 `SELECT *`：通配展不成"第 n 个表达式"
    let e = err_of(&db, "SELECT * FROM t GROUP BY 1");
    assert!(e.contains("SELECT *"), "{e}");
    // **对照组（别修过头）**：真·没分组的列照旧报错，且文案准确
    let e = err_of(&db, "SELECT v, count(*) FROM t GROUP BY g");
    assert!(e.contains("must appear in GROUP BY"), "{e}");
    let e = err_of(&db, "SELECT upper(s), count(*) FROM t GROUP BY g");
    assert!(e.contains("must appear in GROUP BY"), "{e}");
    let _ = std::fs::remove_dir_all(&root);
}
