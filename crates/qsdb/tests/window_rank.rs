//! `rank()` / `dense_rank()` / `row_number()` 的**分区名次**回归测试。
//!
//! 背景（2026-10-11 修复）：`rank()` 曾把名次算成**全局行号** `i + 1`——
//! 单分区（`OVER (ORDER BY …)`，无 `PARTITION BY`）下 `i + 1` 恰好等于分区内
//! 位置，所以既有的 `cte_window.rs` 用例全绿，问题只在**第二个分区起**暴露：
//! 6 行两分区 → `1,2,3,1,5,6`（应为 `1,2,3,1,2,3`）。
//!
//! 本文件锁死三件事：① 名次按分区**重置**；② 并列**跳号**（rank）与**不跳号**
//! （dense_rank）；③ 单分区/无 `ORDER BY` 的既有语义不被改动顺带带歪。

use qsdb::{Qsdb, QsdbOptions};

/// 测试目录唯一序号：pid 挡跨进程碰撞，序号挡同进程内并发（Windows 时钟
/// 粒度 ~100ns，纳秒后缀在同 tick 内会重复——CI 上真炸过，见 constraints）。
fn _uniq_seq() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    SEQ.fetch_add(1, Ordering::Relaxed)
}

fn setup(tag: &str) -> (Qsdb, std::path::PathBuf) {
    let root = std::env::temp_dir()
        .join("qsdb_window_rank_test")
        .join(format!(
            "{tag}_{}_{}",
            std::process::id(),
            format!("{:x}_{}", std::process::id(), _uniq_seq())
        ));
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    (db, root)
}

/// 取行 + 排序（**排序是必须的**：并列行之间谁在前由排序稳定性决定，
/// 名次/名次并列值不受影响，但行序会，故断言前统一排序）。
fn q(db: &Qsdb, sql: &str) -> Vec<String> {
    let batches = db.sql_collect(sql).unwrap();
    let mut out = Vec::new();
    for b in &batches {
        for i in 0..b.num_rows() {
            let mut cells = Vec::new();
            for c in 0..b.num_columns() {
                let arr = b.column(c);
                if arr.is_null(i) {
                    cells.push("NULL".into());
                } else {
                    cells.push(
                        arrow::util::display::array_value_to_string(arr.as_ref(), i).unwrap(),
                    );
                }
            }
            out.push(cells.join("|"));
        }
    }
    out.sort();
    out
}

fn sorted(v: &[&str]) -> Vec<String> {
    let mut out: Vec<String> = v.iter().map(|s| s.to_string()).collect();
    out.sort();
    out
}

/// 造数：两个分区，各自带并列值。
///
/// 分区 0：`v = 10,10,20,30,30,40` → rank `1,1,3,4,4,6`（**跳号**，3 是 6 的坑）
/// 分区 1：`v =  5, 5, 5, 9,20,20` → rank `1,1,1,4,5,5`（**从 1 重新起**）
///
/// 旧实现给分区 1 的是 `1,1,1,10,10,12`（沿用全局行号）——本用例即为此而立。
fn two_partitions(db: &Qsdb) {
    db.sql("CREATE TABLE t (g BIGINT, v BIGINT)").unwrap();
    db.sql(
        "INSERT INTO t VALUES
        (0,10),(0,10),(0,20),(0,30),(0,30),(0,40),
        (1,5),(1,5),(1,5),(1,9),(1,20),(1,20)",
    )
    .unwrap();
}

#[test]
fn rank_restarts_per_partition_and_skips_on_ties() {
    let (db, _root) = setup("rank");
    two_partitions(&db);

    let r = q(
        &db,
        "SELECT g, v, rank() OVER (PARTITION BY g ORDER BY v) AS rk FROM t",
    );
    assert_eq!(
        r,
        sorted(&[
            "0|10|1", "0|10|1", "0|20|3", "0|30|4", "0|30|4", "0|40|6", "1|5|1", "1|5|1", "1|5|1",
            "1|9|4", "1|20|5", "1|20|5",
        ])
    );

    // 降序同理：名次是"排在我前面的行数 + 1"，与排序方向无关
    let desc = q(
        &db,
        "SELECT g, v, rank() OVER (PARTITION BY g ORDER BY v DESC) AS rk FROM t",
    );
    assert_eq!(
        desc,
        sorted(&[
            "0|40|1", "0|30|2", "0|30|2", "0|20|4", "0|10|5", "0|10|5", "1|20|1", "1|20|1",
            "1|9|3", "1|5|4", "1|5|4", "1|5|4",
        ])
    );
}

#[test]
fn dense_rank_and_row_number_stay_correct() {
    let (db, _root) = setup("dense");
    two_partitions(&db);

    // dense_rank：并列不跳号 → 分区 0 `1,1,2,3,3,4`；分区 1 `1,1,1,2,3,3`
    let dr = q(
        &db,
        "SELECT g, v, dense_rank() OVER (PARTITION BY g ORDER BY v) AS dr FROM t",
    );
    assert_eq!(
        dr,
        sorted(&[
            "0|10|1", "0|10|1", "0|20|2", "0|30|3", "0|30|3", "0|40|4", "1|5|1", "1|5|1", "1|5|1",
            "1|9|2", "1|20|3", "1|20|3",
        ])
    );

    // row_number：每分区 1..6 连续（并列行之间谁先谁后不承诺 → 只锁值域形状）
    let rn = q(
        &db,
        "SELECT g, row_number() OVER (PARTITION BY g ORDER BY v) AS rn FROM t",
    );
    let mut per_g: std::collections::BTreeMap<String, Vec<String>> = Default::default();
    for row in rn {
        let (g, rn) = row.split_once('|').unwrap();
        per_g.entry(g.to_string()).or_default().push(rn.to_string());
    }
    for (g, nums) in per_g {
        let mut nums = nums;
        nums.sort();
        assert_eq!(
            nums,
            (1..=6).map(|i| i.to_string()).collect::<Vec<_>>(),
            "分区 {g} 的 row_number 应为 1..6"
        );
    }
}

/// 单分区（无 `PARTITION BY`）= 修复前就正确的路径，锁住不被带歪。
#[test]
fn single_partition_semantics_unchanged() {
    let (db, _root) = setup("single");
    db.sql("CREATE TABLE s (v BIGINT)").unwrap();
    db.sql("INSERT INTO s VALUES (10),(10),(20),(30),(30),(40)")
        .unwrap();

    let r = q(&db, "SELECT v, rank() OVER (ORDER BY v) AS rk FROM s");
    assert_eq!(r, sorted(&["10|1", "10|1", "20|3", "30|4", "30|4", "40|6"]));
}

/// 无 `ORDER BY` 时全部为并列 → `rank()` 恒 1（每分区），不得因修复变成行号。
#[test]
fn rank_without_order_by_is_all_ones() {
    let (db, _root) = setup("noorder");
    db.sql("CREATE TABLE t2 (g BIGINT, v BIGINT)").unwrap();
    db.sql("INSERT INTO t2 VALUES (0,1),(0,2),(1,7),(1,8),(1,9)")
        .unwrap();

    let r = q(&db, "SELECT g, rank() OVER (PARTITION BY g) AS rk FROM t2");
    assert_eq!(r, sorted(&["0|1", "0|1", "1|1", "1|1", "1|1"]));
}
