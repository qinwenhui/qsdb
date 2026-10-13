//! `ORDER BY … LIMIT n`（TopN）回归测试。
//!
//! 两件事一起测：
//! 1. **接线**：`exec_builder` 曾对 `Sort` 恒传 `limit: None` —— 于是 `ORDER BY … LIMIT n`
//!    走的是"全量物化 + 全排序 + 截断"，`SortExec` 的 limit 能力（部分排序）没人用。
//! 2. **有界缓冲**：带 limit 时缓冲攒到 `8n + 4096` 行就**就地截断**到前 n 行，
//!    内存从 O(输入) 降到 O(n)（`ORDER BY … LIMIT 10` 排全表是这类查询的常态）。
//!
//! 中间截断必须用**同一套比较口径**（pg 语义 ASC NULLS LAST / DESC NULLS FIRST），
//! 否则会悄悄丢答案——故本文件的行数刻意超过截断阈值（多次截断），并覆盖
//! NULL、并列值、多列排序、LIMIT 0。
//!
//! 实测（3M 行 3 轮中位，`ab_gate` 的 `order_topn`）：0.0975s → 0.0233s（4.2x）。

use qsdb::{Qsdb, QsdbOptions};

fn setup(tag: &str) -> (Qsdb, std::path::PathBuf) {
    let root = std::env::temp_dir().join("qsdb_topn_test").join(format!(
        "{tag}_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    (db, root)
}

fn q(db: &Qsdb, sql: &str) -> Vec<String> {
    let batches = db.sql_collect(sql).unwrap();
    let mut out = Vec::new();
    for b in &batches {
        for i in 0..b.num_rows() {
            let cells: Vec<String> = (0..b.num_columns())
                .map(|c| {
                    let arr = b.column(c);
                    if arr.is_null(i) {
                        "NULL".into()
                    } else {
                        arrow::util::display::array_value_to_string(arr.as_ref(), i).unwrap()
                    }
                })
                .collect();
            out.push(cells.join("|"));
        }
    }
    out
}

/// 造 `rows` 行：`v` 有并列（`i % 500`），`g` 有 NULL（每 7 行一个），`w` 递增。
/// `rows` 远大于 `8n + 4096`（n 小）→ 必然触发**多次中间截断**。
fn load(db: &Qsdb, rows: i64) {
    db.sql("CREATE TABLE t (g BIGINT, v BIGINT, w BIGINT)")
        .unwrap();
    let mut stmt = String::from("INSERT INTO t VALUES ");
    for i in 0..rows {
        if i > 0 {
            stmt.push(',');
        }
        let g = if i % 7 == 0 {
            "NULL".to_string()
        } else {
            (i % 13).to_string()
        };
        stmt.push_str(&format!("({g},{},{i})", i % 500));
    }
    db.sql(&stmt).unwrap();
}

#[test]
fn order_by_limit_takes_the_true_top_n() {
    let (db, _root) = setup("asc");
    let rows = 20_000i64;
    load(&db, rows);

    // 升序 v：v = i % 500 → 前 5 行都是 v=0（并列，用 w 决定谁在前）
    let got = q(&db, "SELECT v, w FROM t ORDER BY v, w LIMIT 5");
    assert_eq!(
        got,
        vec!["0|0", "0|500", "0|1000", "0|1500", "0|2000"],
        "TopN 结果必须是真正的全量前 5（中间截断不得丢答案）"
    );

    // 降序 v：v=499 的行 w 最大者在前
    let got = q(&db, "SELECT v, w FROM t ORDER BY v DESC, w DESC LIMIT 5");
    assert_eq!(
        got,
        vec![
            "499|19999",
            "499|19499",
            "499|18999",
            "499|18499",
            "499|17999"
        ]
    );

    // 单列 limit 但**不带** tie-breaker：只锁值域（并列行序不承诺）
    let got = q(&db, "SELECT v FROM t ORDER BY v LIMIT 5");
    assert_eq!(got, vec!["0", "0", "0", "0", "0"]);
}

/// pg 语义：ASC NULLS LAST / DESC NULLS FIRST（中间截断必须同一口径，
/// 否则 NULL 行会在"每轮截断"里被悄悄筛掉或错误保留）。
#[test]
fn order_by_limit_null_ordering_matches_full_sort() {
    let (db, _root) = setup("nulls");
    load(&db, 20_000);

    // g 每 7 行一个 NULL。升序：NULL 在最后 → LIMIT 5 全是非 NULL
    let asc5 = q(&db, "SELECT g FROM t ORDER BY g, w LIMIT 5");
    assert!(
        asc5.iter().all(|s| s != "NULL"),
        "ASC 时 NULL 应排最后: {asc5:?}"
    );
    assert_eq!(asc5, vec!["0", "0", "0", "0", "0"]);

    // 升序全量取最后 3 行（含 NULL 尾巴）——与 LIMIT 截断路径对账
    let asc_all = q(&db, "SELECT g FROM t ORDER BY g, w");
    let tail: Vec<String> = asc_all[asc_all.len() - 3..].to_vec();
    assert!(tail.iter().all(|s| s == "NULL"), "尾部应为 NULL: {tail:?}");

    // 降序：NULL 在最前
    let desc5 = q(&db, "SELECT g FROM t ORDER BY g DESC, w LIMIT 5");
    assert_eq!(
        desc5,
        vec!["NULL", "NULL", "NULL", "NULL", "NULL"],
        "DESC 时 NULL 应排最前"
    );
}

/// LIMIT 0 与"limit 大于输入"两个边界。
#[test]
fn limit_boundaries() {
    let (db, _root) = setup("bounds");
    load(&db, 5_000);

    assert!(q(&db, "SELECT v FROM t ORDER BY v LIMIT 0").is_empty());
    assert_eq!(
        q(&db, "SELECT v FROM t ORDER BY v LIMIT 1000000").len(),
        5_000
    );

    // LIMIT n 与 LIMIT（先全排后取前 n）逐值一致：TopN 的中间截断不改变答案
    let top = q(&db, "SELECT v, w FROM t ORDER BY v, w LIMIT 37");
    let full = q(&db, "SELECT v, w FROM t ORDER BY v, w");
    assert_eq!(top, full[..37].to_vec());
}

/// 极小配额 + 大输入：有界缓冲让内存与输入规模**脱钩**（回前 n 行），
/// 查询要能完成且答案正确（此前这条必然全量物化，靠溢出落盘才能完成）。
#[test]
fn topn_under_tiny_quota_still_correct() {
    let root = std::env::temp_dir().join("qsdb_topn_test").join(format!(
        "quota_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let db = Qsdb::open(
        &root,
        QsdbOptions {
            memory_budget_bytes: Some(512 * 1024),
            scan_cache_bytes: 0,
            ..Default::default()
        },
    )
    .unwrap();
    load(&db, 20_000);

    let ctx = db.new_query_context(None);
    let out = db
        .sql_with_context("SELECT w FROM t ORDER BY w DESC LIMIT 3", &ctx)
        .unwrap_or_else(|e| panic!("小预算下 TopN 应完成: {e}"));
    let batches = match out {
        qsdb::sql::SqlResult::Query { batches, .. } => batches,
        _ => panic!("expected query"),
    };
    let vals: Vec<String> = batches
        .iter()
        .flat_map(|b| {
            (0..b.num_rows()).map(|i| {
                arrow::util::display::array_value_to_string(b.column(0).as_ref(), i).unwrap()
            })
        })
        .collect();
    assert_eq!(vals, vec!["19999", "19998", "19997"]);

    let _ = std::fs::remove_dir_all(&root);
}
