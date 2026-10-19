//! **段缓存探针**（诊断用）：把"缓存预算 ↔ 真实 RSS"对上账。
//!
//! 起因（2026-10-15）：`bench_suite` 的内存那一栏随 `--cache-mb` 近乎线性放大
//! **~3×**（256MB 预算 ⇒ RSS +838MB）。而 `SegmentCache` 明明是按预算驱逐的
//! （`used + bytes > budget` ⇒ 逐出 LRU）。**两边对不上账** ⇒ 要么记账漏了，
//! 要么有别的东西跟着预算一起长。
//!
//! 本探针把 `cache_stats()`（缓存**自己**的记账）与 `VmHWM`（进程真实峰值）
//! 并排打出来 —— 差值就是"预算之外"的那部分。
//!
//! ```text
//! cache_probe <db_dir> <cache_mb> [sql] [rounds]
//! ```

use std::time::Instant;

use qsdb::{Qsdb, QsdbOptions};

fn hwm_kb() -> u64 {
    // Linux: /proc/self/status 的 VmHWM（峰值，单调）
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("VmHWM:"))
                .and_then(|l| l.split_whitespace().nth(1))
                .and_then(|v| v.parse().ok())
        })
        .unwrap_or(0)
}

fn rss_kb() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("VmRSS:"))
                .and_then(|l| l.split_whitespace().nth(1))
                .and_then(|v| v.parse().ok())
        })
        .unwrap_or(0)
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let dir = a
        .get(1)
        .cloned()
        .unwrap_or_else(|| "/home/bench/qsdb-suite".into());
    let cache_mb: usize = a.get(2).and_then(|v| v.parse().ok()).unwrap_or(256);
    let sql = a
        .get(3)
        .cloned()
        .unwrap_or_else(|| "SELECT g, v, row_number() OVER (PARTITION BY g ORDER BY v DESC) rn FROM t WHERE g < 50 LIMIT 200".into());
    let rounds: usize = a.get(4).and_then(|v| v.parse().ok()).unwrap_or(3);

    let opts = QsdbOptions {
        scan_cache_bytes: cache_mb * 1024 * 1024,
        ..Default::default()
    };
    let db = Qsdb::open(&dir, opts).expect("open db");
    println!("cache 预算 = {cache_mb} MB   查询 = {sql}");

    for i in 0..rounds {
        let t0 = Instant::now();
        let r = db.sql_collect(&sql).expect("query");
        let n: usize = r.iter().map(|b| b.num_rows()).sum();
        let ms = t0.elapsed().as_millis();
        // (条数, 缓存自己记的 used 字节, 命中, 未命中)
        let (entries, used, hits, misses) = db.cache_stats();
        println!(
            "round {}: {ms} ms / {n} 行 | 缓存 条数={entries} used={:.1}MB 命中={hits} 未命中={misses} | VmRSS={:.1}MB VmHWM={:.1}MB",
            i + 1,
            used as f64 / 1048576.0,
            rss_kb() as f64 / 1024.0,
            hwm_kb() as f64 / 1024.0,
        );
    }
}
