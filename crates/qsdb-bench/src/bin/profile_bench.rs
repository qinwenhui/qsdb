//! P1/P2 剖析台：filter / group by 差距归因 + 内存占用。
//!
//! 手法：
//! - 冷/热双测：第一遍（冷）= parquet 解码 + 算子；第二遍起（热缓存命中）= 纯算子内核。
//! - 组基数切换：big = g1%1000×g2%100×g3%10（100 万组），small = %64×%100×%50（32 万组，
//!   duckdb_vs 同款）——验证分组内核对组数的敏感性。
//! - zstd（默认）vs snappy 表对比 + 原生 parquet-rs 直读：解码物理上限。
//! - 峰值/当前工作集按阶段打印（Windows PeakWorkingSetSize）。
//!
//! 运行：cargo run --release -p qsdb-bench --bin profile_bench -- [rows_per_partition] [big|small]
//!       （环境变量 RAYON_NUM_THREADS 控并行度）

use std::sync::Arc;
use std::time::Instant;

use arrow::array::{Array, Date32Array, Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use qsdb::{Qsdb, QsdbOptions};
use rayon::prelude::*;

#[cfg(windows)]
fn mem_bytes() -> (u64, u64) {
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
            (pmc.WorkingSetSize as u64, pmc.PeakWorkingSetSize as u64)
        } else {
            (0, 0)
        }
    }
}
#[cfg(not(windows))]
fn mem_bytes() -> (u64, u64) {
    (0, 0)
}

fn report_mem(tag: &str) {
    let (cur, peak) = mem_bytes();
    println!(
        "  [mem] {tag}: cur {:.0} MB, peak {:.0} MB",
        cur as f64 / 1048576.0,
        peak as f64 / 1048576.0
    );
}

fn synth(rows: usize, dt: i32, m: (i64, i64, i64)) -> RecordBatch {
    let mut rng: u64 = 0x9E3779B97F4A7C15 ^ (dt as u64);
    let mut next = move || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng
    };
    let dt_a = Date32Array::from(vec![dt; rows]);
    let g1: Vec<i64> = (0..rows).map(|_| (next() % m.0 as u64) as i64).collect();
    let g2: Vec<i64> = (0..rows).map(|_| (next() % m.1 as u64) as i64).collect();
    let g3: Vec<i64> = (0..rows).map(|_| (next() % m.2 as u64) as i64).collect();
    let a: Vec<i64> = (0..rows).map(|_| (next() % 1_000_000) as i64).collect();
    let b: Vec<i64> = (0..rows).map(|_| (next() % 100) as i64).collect();
    let score: Vec<f64> = (0..rows).map(|i| (i as f64) * 0.5 % 1000.0).collect();
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
            Arc::new(dt_a),
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

fn rows_of(db: &Qsdb, sql: &str) -> usize {
    db.sql_collect(sql)
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum()
}

/// 冷（首跑，填缓存）+ 热（best of 3）双测。
fn cold_hot(db: &Qsdb, name: &str, sql: &str) -> (f64, f64) {
    let t = Instant::now();
    let n = rows_of(db, sql);
    let cold = t.elapsed().as_secs_f64();
    let mut hot = f64::MAX;
    for _ in 0..3 {
        let t = Instant::now();
        let _ = rows_of(db, sql);
        hot = hot.min(t.elapsed().as_secs_f64());
    }
    println!(
        "{name:<30} groups={n:<8} cold {cold:.3}s  hot {hot:.3}s  (decode≈{:.3}s)",
        cold - hot
    );
    (cold, hot)
}

fn find_parquet(p: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
    if let Ok(rd) = std::fs::read_dir(p) {
        for e in rd.flatten() {
            let ep = e.path();
            if ep.is_dir() {
                find_parquet(&ep, out);
            } else if ep.extension().is_some_and(|x| x == "parquet") {
                out.push(ep);
            }
        }
    }
}

fn dir_bytes(p: &std::path::Path) -> u64 {
    let mut n = 0;
    if let Ok(rd) = std::fs::read_dir(p) {
        for e in rd.flatten() {
            let ep = e.path();
            if ep.is_dir() {
                n += dir_bytes(&ep);
            } else if let Ok(m) = e.metadata() {
                n += m.len();
            }
        }
    }
    n
}

fn main() {
    let rows: usize = std::env::args()
        .nth(1)
        .and_then(|v| v.parse().ok())
        .unwrap_or(1_250_000); // ×8 分区 = 10M
    let card = std::env::args().nth(2).unwrap_or_else(|| "big".into());
    let m: (i64, i64, i64) = if card == "small" {
        (64, 100, 50) // ≈ 32 万组（duckdb_vs 同款）
    } else {
        (1000, 100, 10) // 100 万组
    };
    let parts = 8usize;
    let root = std::env::temp_dir().join("qsdb_profile");
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let db = Qsdb::open(
        &root,
        QsdbOptions {
            scan_cache_bytes: 4096 * 1024 * 1024,
            ..Default::default()
        },
    )
    .unwrap();
    println!(
        "rayon threads = {}, rows/partition = {rows}, groups mode = {card} ({:?})",
        rayon::current_num_threads(),
        m
    );

    // --- 数据：zstd 表 t + snappy 表 ts（同数据同形状） ---
    let ddl = "(dt DATE, g1 BIGINT, g2 BIGINT, g3 BIGINT, a BIGINT, b BIGINT, score DOUBLE) PARTITIONED BY (dt)";
    db.sql(&format!("CREATE TABLE t {ddl}")).unwrap();
    let t0 = Instant::now();
    for p in 0..parts {
        db.append("t", &[synth(rows, 19_000 + p as i32, m)])
            .unwrap();
    }
    println!(
        "zstd table: {} rows in {:.1}s",
        rows * parts,
        t0.elapsed().as_secs_f64()
    );
    let t1 = Instant::now();
    db.sql(&format!(
        "CREATE TABLE ts {ddl} WITH (compression='snappy')"
    ))
    .unwrap();
    for p in 0..parts {
        db.append("ts", &[synth(rows, 19_000 + p as i32, m)])
            .unwrap();
    }
    println!(
        "snappy table: {} rows in {:.1}s",
        rows * parts,
        t1.elapsed().as_secs_f64()
    );
    let tdir = root.join("default").join("t");
    let tsdir = root.join("default").join("ts");
    println!(
        "storage: zstd {:.2} GB vs snappy {:.2} GB (ratio {:.1}x)",
        dir_bytes(&tdir) as f64 / 1073741824.0,
        dir_bytes(&tsdir) as f64 / 1073741824.0,
        dir_bytes(&tsdir) as f64 / dir_bytes(&tdir) as f64
    );
    report_mem("after gen");

    // --- A. 单列聚合（解码吞吐） ---
    println!("\n== A. sum(a)（单列解码吞吐） ==");
    cold_hot(&db, "zstd   sum(a)", "SELECT sum(a) FROM t");
    cold_hot(&db, "snappy sum(a)", "SELECT sum(a) FROM ts");
    report_mem("after A");

    // --- B. group by 3 列（内核 vs 解码分离） ---
    println!("\n== B. group by g1,g2,g3 → count/sum/sum ==");
    cold_hot(
        &db,
        "zstd   group-by-3",
        "SELECT g1, g2, g3, count(*), sum(a), sum(b) FROM t GROUP BY g1, g2, g3",
    );
    report_mem("after B");

    // --- C. 过滤扫描 ---
    println!("\n== C. count WHERE a > 0 ==");
    cold_hot(&db, "zstd   filter", "SELECT count(*) FROM t WHERE a > 0");
    report_mem("after C");

    // --- D. 原生 parquet-rs 直读（解码物理上限） ---
    println!("\n== D. 原生 parquet-rs 直读（{parts} 文件并行） ==");
    let mut zfiles = Vec::new();
    find_parquet(&tdir, &mut zfiles);
    let mut sfiles = Vec::new();
    find_parquet(&tsdir, &mut sfiles);
    println!("files: zstd {} snappy {}", zfiles.len(), sfiles.len());
    let read_col = |files: &[std::path::PathBuf], label: &str| {
        let t = Instant::now();
        let total: i64 = files
            .par_iter()
            .map(|f| {
                let fl = std::fs::File::open(f).unwrap();
                let builder = ParquetRecordBatchReaderBuilder::try_new(fl).unwrap();
                let idx = builder.schema().index_of("a").unwrap();
                let mask = ProjectionMask::leaves(builder.parquet_schema(), std::iter::once(idx));
                let reader = builder
                    .with_projection(mask)
                    .with_batch_size(65536)
                    .build()
                    .unwrap();
                let mut s = 0i64;
                for b in reader {
                    let b = b.unwrap();
                    let arr = b.column(0);
                    let arr = arr.as_any().downcast_ref::<Int64Array>().unwrap();
                    for i in 0..arr.len() {
                        if !arr.is_null(i) {
                            s += arr.value(i);
                        }
                    }
                }
                s
            })
            .sum();
        println!("{label}: sum={total} in {:.3}s", t.elapsed().as_secs_f64());
    };
    read_col(&zfiles, "raw zstd   read a-col + sum");
    read_col(&sfiles, "raw snappy read a-col + sum");

    let read_all = |files: &[std::path::PathBuf], label: &str| {
        static TOTAL: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let t = Instant::now();
        files.par_iter().for_each(|f| {
            let fl = std::fs::File::open(f).unwrap();
            let builder = ParquetRecordBatchReaderBuilder::try_new(fl).unwrap();
            let reader = builder
                .with_projection(ProjectionMask::all())
                .with_batch_size(65536)
                .build()
                .unwrap();
            let mut n = 0usize;
            for b in reader {
                n += b.unwrap().num_rows();
            }
            TOTAL.fetch_add(n, std::sync::atomic::Ordering::Relaxed);
        });
        let total = TOTAL.swap(0, std::sync::atomic::Ordering::Relaxed);
        println!(
            "{label}: {total} rows 7-cols in {:.3}s",
            t.elapsed().as_secs_f64()
        );
    };
    read_all(&zfiles, "raw zstd   read 7 cols");
    read_all(&sfiles, "raw snappy read 7 cols");
    report_mem("after D");

    let _ = std::fs::remove_dir_all(&root);
}
