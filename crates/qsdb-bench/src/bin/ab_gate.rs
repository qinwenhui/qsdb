//! A/B gate：**QSDB 自身跨版本对比 + 峰值内存**（不依赖 DuckDB）。
//!
//! 为什么另开一个（而不是直接用 `fair_vs`）：
//! - `fair_vs` 要 `--features duckdb`（bundled C++），在 CentOS 7/gcc 4.8 上编不过，
//!   而跨版本回归根本不需要对手引擎——**A/B 两侧都是 QSDB**；
//! - 内存预算是 P0/P1 的主战场，把**峰值 RSS**（Linux `/proc/self/status` 的
//!   `VmHWM`，无新依赖）与耗时一起报，才看得见"内存受控"是否真的成立。
//!
//! 口径（沿用 `fair_vs` v2 的教训）：
//! - 每个「用例 × 版本」**独立子进程**（`--case N`），进程退出即释放全部
//!   内存/线程/分配器状态 → 零残留影响；
//! - 子进程内跑 R 轮取**中位数**并报离散度；
//! - 数据只准备一次（`--prepare`），跨进程复用。
//!
//! 用法：
//! ```text
//! ab_gate --prepare --data /root/bench/ab-data [--rows 5000000]
//! ab_gate --case 3 --data /root/bench/ab-data [--rounds 5] [--limit-mb 64]
//! ```

use std::sync::Arc;
use std::time::Instant;

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use qsdb::query::QueryContext;
use qsdb::{Qsdb, QsdbOptions};

/// 用例：`(名字, SQL, 是否用极小配额跑)`。
///
/// 最后一个标记是**内存溢出**专用：用 `--limit-mb` 给该查询一个远小于工作集的
/// 每查询配额，验证它**落盘完成**（而不是被拒/OOM）——P1 的承诺在大数据上的复现。
fn cases() -> Vec<(&'static str, String, bool)> {
    vec![
        (
            "agg",
            "SELECT count(*), sum(v), min(v), max(v) FROM t".into(),
            false,
        ),
        ("filter_num", "SELECT count(*) FROM t WHERE v < 100".into(), false),
        (
            "filter_str",
            "SELECT count(*) FROM t WHERE s LIKE 's-1%'".into(),
            false,
        ),
        (
            "join_count",
            "SELECT count(*) FROM t JOIN dim d ON t.g = d.g".into(),
            false,
        ),
        (
            "join_mat",
            "SELECT t.v, d.name FROM t JOIN dim d ON t.g = d.g LIMIT 1000".into(),
            false,
        ),
        ("point", "SELECT count(*) FROM t WHERE dp = 7".into(), false),
        (
            "union_all",
            "SELECT count(*) FROM (SELECT v FROM t UNION ALL SELECT v FROM t)".into(),
            false,
        ),
        // ↓ 内存侧重点：高基数分组（组表是内存大头）、去重、排序
        (
            "group_by_hi",
            "SELECT g, count(*) AS c, sum(v) AS s FROM t GROUP BY g".into(),
            false,
        ),
        ("distinct_s", "SELECT DISTINCT s FROM t".into(), false),
        (
            "order_topn",
            "SELECT v FROM t ORDER BY v LIMIT 10".into(),
            false,
        ),
        // 极小配额：**必须落盘完成**（P1 的溢出承诺）
        (
            "spill_group_by",
            "SELECT g, count(*) AS c, sum(v) AS s FROM t GROUP BY g".into(),
            true,
        ),
        (
            "spill_order_by",
            "SELECT v FROM t ORDER BY v".into(),
            true,
        ),
    ]
}

/// 峰值常驻内存（KB）：Linux `/proc/self/status` 的 `VmHWM`。
fn peak_rss_kb() -> u64 {
    if let Ok(s) = std::fs::read_to_string("/proc/self/status") {
        for line in s.lines() {
            if let Some(rest) = line.strip_prefix("VmHWM:") {
                if let Some(kb) = rest.split_whitespace().next() {
                    return kb.parse().unwrap_or(0);
                }
            }
        }
    }
    0
}

fn arg(name: &str) -> Option<String> {
    let a: Vec<String> = std::env::args().collect();
    a.iter().position(|x| x == name).and_then(|i| a.get(i + 1).cloned())
}

fn has(flag: &str) -> bool {
    std::env::args().any(|x| x == flag)
}

/// 打开（冷读口径：`scan_cache_bytes = 0`，与 `fair_vs` 一致）。
///
/// **刻意不设 `memory_budget_bytes`**：本工具要能在**改动前的版本**上编译
/// （A 侧还没有那个字段），而 A/B 对比也不需要全局预算——"极小配额"用例走的是
/// 每查询配额（`QueryContext::new(.., Some(limit))`），两侧都有。
fn open(data: &str) -> Qsdb {
    Qsdb::open(
        data,
        QsdbOptions {
            scan_cache_bytes: 0,
            ..Default::default()
        },
    )
    .expect("open db")
}

/// 建数据：`t`（分区表，多列多段） + `dim`。幂等：已存在则跳过。
fn prepare(data: &str, rows: i64) {
    let db = open(data);
    if db
        .sql("SELECT count(*) FROM t")
        .ok()
        .is_some()
    {
        println!("数据已存在，跳过准备: {data}");
        return;
    }
    db.sql("CREATE TABLE t (dp BIGINT, g BIGINT, v BIGINT, x DOUBLE, s STRING) PARTITIONED BY (dp)")
        .expect("create t");
    db.sql("CREATE TABLE dim (g BIGINT, name STRING)")
        .expect("create dim");

    let schema = Arc::new(Schema::new(vec![
        Field::new("dp", DataType::Int64, true),
        Field::new("g", DataType::Int64, true),
        Field::new("v", DataType::Int64, true),
        Field::new("x", DataType::Float64, true),
        Field::new("s", DataType::Utf8, true),
    ]));
    // 每批 64K 行，逐批 append（多 segment：让并行扫描与分区都真实生效）
    let per: i64 = 1 << 16;
    let parts: i64 = 16;
    let rows_per_part = rows / parts;
    let t0 = Instant::now();
    for p in 0..parts {
        let mut done = 0i64;
        while done < rows_per_part {
            let n = per.min(rows_per_part - done);
            let base = p * rows_per_part + done;
            let dp: Vec<i64> = vec![p; n as usize];
            let g: Vec<i64> = (0..n).map(|i| (base + i) % 200_000).collect();
            let v: Vec<i64> = (0..n).map(|i| base + i).collect();
            let x: Vec<f64> = (0..n).map(|i| (base + i) as f64 * 0.5).collect();
            let s: Vec<String> = (0..n)
                .map(|i| format!("s-{}/{:06}", (base + i) % 1000, (base + i) % 100_000))
                .collect();
            let b = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int64Array::from(dp)),
                    Arc::new(Int64Array::from(g)),
                    Arc::new(Int64Array::from(v)),
                    Arc::new(Float64Array::from(x)),
                    Arc::new(StringArray::from(s)),
                ],
            )
            .unwrap();
            db.append("t", &[b]).expect("append t");
            done += n;
        }
    }
    // dim：g ∈ [0, 200000)，含少量重复以制造多命中
    let dim_schema = Arc::new(Schema::new(vec![
        Field::new("g", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
    ]));
    for chunk in 0..4 {
        let n = 50_000i64;
        let g: Vec<i64> = (0..n).map(|i| (chunk * n + i) % 200_000).collect();
        let name: Vec<String> = (0..n).map(|i| format!("n{}", chunk * n + i)).collect();
        let b = RecordBatch::try_new(
            Arc::clone(&dim_schema),
            vec![
                Arc::new(Int64Array::from(g)),
                Arc::new(StringArray::from(name)),
            ],
        )
        .unwrap();
        db.append("dim", &[b]).expect("append dim");
    }
    println!(
        "数据准备完成：t={rows} 行（{parts} 分区）、dim=200000 行，耗时 {:.1}s",
        t0.elapsed().as_secs_f64()
    );
}

/// 单用例：R 轮取中位数，报「中位秒数 / 离散度 / 峰值 RSS」。
fn run_case(idx: usize, data: &str, rounds: usize, limit_mb: u64) {
    let cs = cases();
    let (name, sql, constrained) = &cs[idx];
    let db = open(data);
    let limit = if *constrained {
        Some(limit_mb.max(1) * 1024 * 1024)
    } else {
        None
    };
    let mut times: Vec<f64> = Vec::with_capacity(rounds);
    for _ in 0..rounds {
        let ctx = match limit {
            Some(l) => QueryContext::new("ab", None, Some(l)),
            None => QueryContext::new("ab", None, None),
        };
        let t0 = Instant::now();
        let r = db.sql_with_context(sql, &ctx);
        let dt = t0.elapsed().as_secs_f64();
        match r {
            Ok(_) => times.push(dt),
            Err(e) => {
                // 失败要如实报（尤其 spill_* 用例——"落盘完成"是承诺）
                println!("{name} FAILED after {dt:.3}s: {e}");
                return;
            }
        }
    }
    times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let med = times[times.len() / 2];
    let spread = (times[times.len() - 1] - times[0]) / med.max(1e-9) * 100.0;
    println!(
        "{name} median={med:.4}s spread={spread:.1}% rss_kb={}",
        peak_rss_kb()
    );
}

fn main() {
    let data = arg("--data").unwrap_or_else(|| "/root/bench/ab-data".to_string());
    if has("--prepare") {
        let rows: i64 = arg("--rows").and_then(|v| v.parse().ok()).unwrap_or(5_000_000);
        // 给足准备阶段的内存（这是建数据，不是被测对象）
        prepare(&data, rows);
        return;
    }
    let rounds: usize = arg("--rounds").and_then(|v| v.parse().ok()).unwrap_or(5);
    let limit_mb: u64 = arg("--limit-mb").and_then(|v| v.parse().ok()).unwrap_or(64);
    match arg("--case").and_then(|v| v.parse::<usize>().ok()) {
        Some(idx) => run_case(idx, &data, rounds, limit_mb),
        None => {
            let cs = cases();
            println!("用例清单（用 --case N 逐个跑，独立子进程隔离）:");
            for (i, (n, _, c)) in cs.iter().enumerate() {
                println!("  {i:>2} {n}{}", if *c { "  [极小配额：验证溢出]" } else { "" });
            }
        }
    }
}
