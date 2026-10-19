//! P2 内存对比：qsdb vs DuckDB 分进程峰值 RSS（同负载各自独立进程跑）。
//!
//! 公平性口径：同一合成数据（40M 行 × 8 分区 × 7 列）；qsdb 默认配置
//! （cache 256MB）；DuckDB 默认配置（memory_limit 80% RAM 但流式执行）。
//! 相位：空闲/加载（写）→ 过滤 → 聚合 → group by，各报峰值/当前工作集。
//!
//! 运行（分进程）：
//!   cargo run --release -p qsdb-bench --bin mem_vs -- qsdb [rows_per_part]
//!   cargo run --release -p qsdb-bench --features duckdb --bin mem_vs -- duckdb [rows_per_part]

use std::sync::Arc;
use std::time::Instant;

use arrow::array::{Date32Array, Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

#[cfg(windows)]
fn mem_mb() -> (f64, f64) {
    use windows_sys::Win32::System::ProcessStatus::{
        GetProcessMemoryInfo, PROCESS_MEMORY_COUNTERS,
    };
    use windows_sys::Win32::System::Threading::GetCurrentProcess;
    let mut pmc = PROCESS_MEMORY_COUNTERS::default();
    unsafe {
        let h = GetCurrentProcess();
        if GetProcessMemoryInfo(
            h,
            &mut pmc,
            std::mem::size_of::<PROCESS_MEMORY_COUNTERS>() as u32,
        ) != 0
        {
            (
                pmc.WorkingSetSize as f64 / 1048576.0,
                pmc.PeakWorkingSetSize as f64 / 1048576.0,
            )
        } else {
            (0.0, 0.0)
        }
    }
}
#[cfg(not(windows))]
fn mem_mb() -> (f64, f64) {
    (0.0, 0.0)
}

fn report(tag: &str) {
    let (cur, peak) = mem_mb();
    println!("[mem] {tag:<22} cur {cur:8.0} MB   peak {peak:8.0} MB");
}

fn synth(rows: usize, dt: i32) -> RecordBatch {
    let mut x = 0x9e37_79b9_7f4a_7c15u64 | (dt as u64 & 0xff);
    let mut g1 = Vec::with_capacity(rows);
    let mut g2 = Vec::with_capacity(rows);
    let mut g3 = Vec::with_capacity(rows);
    let mut a = Vec::with_capacity(rows);
    let mut b = Vec::with_capacity(rows);
    let mut score = Vec::with_capacity(rows);
    for _ in 0..rows {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        g1.push((x % 64) as i64);
        g2.push((x % 100) as i64);
        g3.push((x % 50) as i64);
        a.push((x % 1_000_000) as i64);
        b.push((x >> 11) as i64);
        score.push(x as f64 / 65536.0);
    }
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("dt", DataType::Date32, true),
            Field::new("g1", DataType::Int64, true),
            Field::new("g2", DataType::Int64, true),
            Field::new("g3", DataType::Int64, true),
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
            Field::new("score", DataType::Float64, true),
        ])),
        vec![
            Arc::new(Date32Array::from(vec![dt; rows])),
            Arc::new(Int64Array::from(g1)),
            Arc::new(Int64Array::from(g2)),
            Arc::new(Int64Array::from(g3)),
            Arc::new(Int64Array::from(a)),
            Arc::new(Int64Array::from(b)),
            Arc::new(Float64Array::from(score)),
        ],
    )
    .unwrap()
}

const QUERIES: &[(&str, &str)] = &[
    ("filter", "SELECT count(*) FROM t WHERE a > 0"),
    (
        "aggregate",
        "SELECT count(*), sum(a), sum(b), min(score), max(score) FROM t",
    ),
    (
        "group by",
        "SELECT g1, g2, g3, count(*), sum(a) FROM t GROUP BY g1, g2, g3",
    ),
];

fn main() {
    let mode = std::env::args().nth(1).unwrap_or_else(|| "qsdb".into());
    let rows: usize = std::env::args()
        .nth(2)
        .and_then(|v| v.parse().ok())
        .unwrap_or(5_000_000);
    let parts = 8usize;
    let root = std::env::temp_dir().join("qsdb_mem_vs");
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    println!("== mode={mode} rows={}x{parts} ==", rows * parts);

    match mode.as_str() {
        "qsdb" => {
            report("idle (after open)");
            let db = qsdb::Qsdb::open(&root, qsdb::QsdbOptions::default()).unwrap();
            report("open");
            db.sql("CREATE TABLE t (dt DATE, g1 BIGINT, g2 BIGINT, g3 BIGINT, a BIGINT, b BIGINT, score DOUBLE) PARTITIONED BY (dt)")
                .unwrap();
            let t = Instant::now();
            for p in 0..parts {
                db.append("t", &[synth(rows, 19_000 + p as i32)]).unwrap();
            }
            println!(
                "write: {} rows in {:.1}s",
                rows * parts,
                t.elapsed().as_secs_f64()
            );
            report("after write");
            for (name, sql) in QUERIES {
                let t = Instant::now();
                let n: usize = db
                    .sql_collect(sql)
                    .unwrap()
                    .iter()
                    .map(|b| b.num_rows())
                    .sum();
                println!("{name}: {n} rows in {:.3}s", t.elapsed().as_secs_f64());
                report(&format!("after {name}"));
            }
            drop(db);
            report("after close");
        }
        #[cfg(feature = "duckdb")]
        "duckdb" | "duckdb_parquet" => {
            use parquet::arrow::arrow_writer::ArrowWriter;
            let via_table = mode == "duckdb";
            report("idle");
            // 先用 arrow 直接写 parquet 文件（DuckDB 数据源；不经过 qsdb）
            let mut files = Vec::new();
            let t = Instant::now();
            for p in 0..parts {
                let path = root.join(format!("part{p}.parquet"));
                let batch = synth(rows, 19_000 + p as i32);
                let f = std::fs::File::create(&path).unwrap();
                let mut w = ArrowWriter::try_new(f, batch.schema(), None).unwrap();
                w.write(&batch).unwrap();
                w.close().unwrap();
                files.push(path);
            }
            println!(
                "parquet prepared: {} files in {:.1}s",
                files.len(),
                t.elapsed().as_secs_f64()
            );
            report("after parquet prep");
            let duck = duckdb::Connection::open_in_memory().unwrap();
            let glob = root
                .join("part*.parquet")
                .to_string_lossy()
                .replace('\\', "/");
            let from = if via_table {
                let t = Instant::now();
                duck.execute_batch(&format!(
                    "CREATE TABLE t AS SELECT * FROM read_parquet('{glob}')"
                ))
                .unwrap();
                let n: i64 = duck
                    .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
                    .unwrap();
                println!(
                    "write/load(建表驻留): {n} rows in {:.1}s",
                    t.elapsed().as_secs_f64()
                );
                report("after load");
                "t".to_string()
            } else {
                format!("read_parquet('{glob}')")
            };
            for (name, sql) in QUERIES {
                let sql = sql.replacen("FROM t", &format!("FROM {from}"), 1);
                let t = Instant::now();
                let n: i64 = duck
                    .query_row(&format!("SELECT count(*) FROM ({sql})"), [], |r| r.get(0))
                    .unwrap();
                println!("{name}: {n} rows in {:.3}s", t.elapsed().as_secs_f64());
                report(&format!("after {name}"));
            }
            drop(duck);
            report("after close");
        }
        #[cfg(not(feature = "duckdb"))]
        "duckdb" | "duckdb_parquet" => {
            eprintln!("duckdb 模式需要 --features duckdb");
            std::process::exit(1);
        }
        other => {
            eprintln!("unknown mode {other} (qsdb|duckdb)");
            std::process::exit(1);
        }
    }
    let _ = std::fs::remove_dir_all(&root);
}
