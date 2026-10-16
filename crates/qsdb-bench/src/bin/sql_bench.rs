//! **SQL 对照计时探针**：把一组查询在**同一进程内**跑 N 轮，报中位耗时 + 结果摘要。
//!
//! 为什么不直接用 `qsdb query` 计时：那每次都要**进程启动 + 打开库**（十几毫秒），
//! 对 µs/ms 级的查询是纯噪声。这里开一次句柄、连跑多轮。
//!
//! ```text
//! sql_bench <库目录> <SQL 文件> [轮数] [--cache-mb N]
//! ```
//!
//! SQL 文件格式：一行一条查询，`#` 开头是注释。输出：
//! ```text
//! <序号>  median=<中位 ms>  rows=<行数>  digest=<结果指纹>  | <SQL 前 60 字>
//! ```
//!
//! `digest` 是**结果值的指纹**（含顺序）—— 用来跨引擎对账"结果是不是同一个"。

use std::time::Instant;

use arrow::array::Array;
use qsdb::{Qsdb, QsdbOptions};

fn digest(batches: &[arrow::record_batch::RecordBatch]) -> (usize, u64) {
    // FNV-1a：不引依赖，够用（对账用，不是安全用途）
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    let mut rows = 0usize;
    for b in batches {
        for i in 0..b.num_rows() {
            rows += 1;
            for c in 0..b.num_columns() {
                let a = b.column(c);
                let s = if a.is_null(i) {
                    "\u{0}NULL".to_string()
                } else {
                    arrow::util::display::array_value_to_string(a.as_ref(), i).unwrap_or_default()
                };
                for byte in s.as_bytes() {
                    h ^= *byte as u64;
                    h = h.wrapping_mul(0x100_0000_01b3);
                }
                h ^= 0x1f;
                h = h.wrapping_mul(0x100_0000_01b3);
            }
        }
    }
    (rows, h)
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let dir = a
        .get(1)
        .expect("用法: sql_bench <库目录> <SQL 文件> [轮数] [--cache-mb N]");
    let sql_file = a
        .get(2)
        .expect("用法: sql_bench <库目录> <SQL 文件> [轮数]");
    let rounds: usize = a.get(3).and_then(|v| v.parse().ok()).unwrap_or(3);
    let cache_mb: usize = a
        .iter()
        .position(|s| s == "--cache-mb")
        .and_then(|i| a.get(i + 1))
        .and_then(|v| v.parse().ok())
        .unwrap_or(0); // 默认 0：量的是工作集，不是"灌缓存"

    let text = std::fs::read_to_string(sql_file).expect("read sql file");
    let queries: Vec<String> = text
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .map(|l| l.to_string())
        .collect();

    let opts = QsdbOptions {
        scan_cache_bytes: cache_mb * 1024 * 1024,
        ..Default::default()
    };
    let db = Qsdb::open(dir, opts).expect("open db");
    println!(
        "# 库={dir} 轮数={rounds} 缓存={cache_mb}MB 查询数={}",
        queries.len()
    );

    for (i, q) in queries.iter().enumerate() {
        let mut times = Vec::new();
        let mut last = (0usize, 0u64);
        for _ in 0..rounds {
            let t0 = Instant::now();
            let r = match db.sql_collect(q) {
                Ok(r) => r,
                Err(e) => {
                    println!("{:>3}  ERROR: {e}  | {}", i + 1, short(q));
                    times.clear();
                    break;
                }
            };
            times.push(t0.elapsed().as_secs_f64() * 1000.0);
            last = digest(&r);
        }
        if times.is_empty() {
            continue;
        }
        times.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let med = times[times.len() / 2];
        println!(
            "{:>3}  median={:>9.2}ms  min={:>9.2}ms  rows={:<8} digest={:016x}  | {}",
            i + 1,
            med,
            times[0],
            last.0,
            last.1,
            short(q)
        );
    }
}

fn short(s: &str) -> String {
    let t: String = s.split_whitespace().collect::<Vec<_>>().join(" ");
    t.chars().take(70).collect()
}
