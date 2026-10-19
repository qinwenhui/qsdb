//! DuckDB 对比基准（feature 门控：`--features duckdb`）。
//!
//! 公平性口径：同一批 parquet 文件（QSDB 写出），DuckDB 经 read_parquet 读
//! 同一文件集；同 SQL、同进程族、同机器。对比维度：过滤扫描 / 聚合 /
//! 3 列 group by / 写入。
//!
//! 运行：cargo run --release -p qsdb-bench --features duckdb --bin duckdb_vs -- [rows]

#[cfg(feature = "duckdb")]
use std::sync::Arc;
#[cfg(feature = "duckdb")]
use std::time::Instant;

#[cfg(feature = "duckdb")]
use arrow::array::{Date32Array, Float64Array, Int64Array};
#[cfg(feature = "duckdb")]
use arrow::datatypes::{DataType, Field, Schema};
#[cfg(feature = "duckdb")]
use arrow::record_batch::RecordBatch;
#[cfg(feature = "duckdb")]
use duckdb::Connection;
#[cfg(feature = "duckdb")]
use qsdb::api::{Qsdb, QsdbOptions};

#[cfg(feature = "duckdb")]
fn main() {
    let rows: usize = std::env::args()
        .nth(1)
        .and_then(|v| v.parse().ok())
        .unwrap_or(5_000_000);
    let root = std::env::temp_dir().join("qsdb_vs_duckdb");
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();

    // 1) 数据准备（QSDB 写出 parquet，双方共用）
    let cache_mb: usize = std::env::var("QSDB_CACHE_MB")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(4096);
    let db = Qsdb::open(
        &root,
        QsdbOptions {
            scan_cache_bytes: cache_mb * 1024 * 1024,
            ..Default::default()
        },
    )
    .unwrap();
    println!("qsdb hot cache: {cache_mb} MB");
    db.sql("CREATE TABLE t (dt DATE, g1 BIGINT, g2 BIGINT, g3 BIGINT, a BIGINT, b BIGINT, score DOUBLE) PARTITIONED BY (dt)")
        .unwrap();
    let t_write = Instant::now();
    let parts = 8usize;
    for p in 0..parts {
        let batch = synth(rows, 19_000 + p as i32);
        db.append("t", &[batch]).unwrap();
    }
    let total = rows * parts;
    println!(
        "data: {total} rows, {parts} partitions (qsdb write {:.1}s)",
        t_write.elapsed().as_secs_f64()
    );
    let tdir = root.join("default").join("t");
    let glob = tdir.join("*").join("*.parquet");
    let glob_str = glob.to_string_lossy().replace('\\', "/");

    // 2) DuckDB 就绪（读同一批文件）
    let duck = Connection::open_in_memory().unwrap();
    let check = |sql: &str| {
        println!("  duck: {sql}");
    };
    let _ = check;

    // 预热（threads 默认即全部核）
    duck.execute_batch(&format!("SELECT count(*) FROM read_parquet('{glob_str}')"))
        .unwrap();
    let threads = duck
        .query_row("SELECT current_setting('threads')", [], |r| {
            r.get::<_, i64>(0)
        })
        .unwrap();
    println!("duckdb threads = {threads}");

    // --- A. 全表过滤扫描（数值谓词） ---
    bench_pair(
        "filter scan  (a > 0, count)",
        || {
            let out = db
                .sql_collect("SELECT count(*) FROM t WHERE a > 0")
                .unwrap();
            arrow::util::display::array_value_to_string(out[0].column(0).as_ref(), 0)
                .unwrap()
                .parse::<i64>()
                .unwrap()
        },
        || {
            duck.query_row(
                &format!("SELECT count(*) FROM read_parquet('{glob_str}') WHERE a > 0"),
                [],
                |r| r.get::<_, i64>(0),
            )
            .unwrap()
        },
    );

    // --- B. 聚合 ---
    bench_pair(
        "aggregate    (count/sum/min/max)",
        || {
            let out = db
                .sql_collect("SELECT count(*), sum(a), sum(b), min(score), max(score) FROM t")
                .unwrap();
            let b = &out[0];
            arrow::util::display::array_value_to_string(b.column(0).as_ref(), 0)
                .unwrap()
                .parse::<i64>()
                .unwrap()
        },
        || {
            duck.query_row(
                &format!(
                    "SELECT count(*), sum(a), sum(b), min(score), max(score) FROM read_parquet('{glob_str}')"
                ),
                [],
                |r| r.get::<_, i64>(0),
            )
            .unwrap()
        },
    );

    // --- C. 3 列 group by + 聚合（M2 gate 同款） ---
    bench_pair(
        "group by     (g1,g2,g3 → count/sum)",
        || {
            let out = db
                .sql_collect("SELECT g1, g2, g3, count(*), sum(a) FROM t GROUP BY g1, g2, g3")
                .unwrap();
            out.iter().map(|b| b.num_rows()).sum::<usize>() as i64
        },
        || {
            let mut stmt = duck
                .prepare(&format!(
                    "SELECT count(*) FROM (SELECT g1, g2, g3, count(*), sum(a) FROM read_parquet('{glob_str}') GROUP BY g1, g2, g3)"
                ))
                .unwrap();
            stmt.query_row([], |r| r.get::<_, i64>(0)).unwrap()
        },
    );

    // --- D. 分区裁剪点查（等值分区列） ---
    bench_pair(
        "point query  (dt = one partition)",
        || {
            db.sql_collect("SELECT count(*) FROM t WHERE dt = DATE '2022-01-08'")
                .unwrap()[0]
                .num_rows() as i64
        },
        || {
            duck.query_row(
                &format!(
                    "SELECT count(*) FROM read_parquet('{glob_str}') WHERE dt = DATE '2022-01-08'"
                ),
                [],
                |r| r.get::<_, i64>(0),
            )
            .unwrap()
        },
    );

    // --- E. 写入（模型差异大：qsdb append parquet vs duckdb COPY 建表） ---
    let src = root.join("src.parquet");
    write_parquet(&src, &synth(rows, 29_000));
    let t = Instant::now();
    db.append("t", &[synth(rows, 19_100)]).unwrap();
    let qsdb_w = t.elapsed().as_secs_f64();
    let t = Instant::now();
    duck.execute_batch(&format!(
        "CREATE TABLE duck_t AS SELECT * FROM read_parquet('{}')",
        src.to_string_lossy().replace('\\', "/")
    ))
    .unwrap();
    let duck_w = t.elapsed().as_secs_f64();
    println!(
        "write         qsdb {:.0} rows/s vs duckdb(COPY) {:.0} rows/s",
        rows as f64 / qsdb_w,
        rows as f64 / duck_w
    );

    println!("\n（QSDB 执行线程 = rayon 默认；DuckDB threads = {threads}；同机对比）");
    let _ = std::fs::remove_dir_all(&root);
}

#[cfg(feature = "duckdb")]
fn bench_pair(name: &str, qsdb_fn: impl Fn() -> i64, duck_fn: impl Fn() -> i64) {
    // 各跑 3 次取最好
    let mut best_q = f64::MAX;
    for _ in 0..3 {
        let t = std::time::Instant::now();
        let r = qsdb_fn();
        best_q = best_q.min(t.elapsed().as_secs_f64());
        let _ = r;
    }
    let mut best_d = f64::MAX;
    for _ in 0..3 {
        let t = std::time::Instant::now();
        let r = duck_fn();
        best_d = best_d.min(t.elapsed().as_secs_f64());
        let _ = r;
    }
    let ratio = best_d / best_q;
    println!(
        "{name}  qsdb {best_q:.3}s  duckdb {best_d:.3}s  → qsdb/duckdb = {ratio:.2}x {}",
        if ratio >= 0.9 {
            "🟢"
        } else if ratio >= 0.5 {
            "🟡"
        } else {
            "🔴"
        }
    );
}

#[cfg(feature = "duckdb")]
fn synth(rows: usize, dt: i32) -> RecordBatch {
    let mut g1 = Vec::with_capacity(rows);
    let mut g2 = Vec::with_capacity(rows);
    let mut g3 = Vec::with_capacity(rows);
    let mut a = Vec::with_capacity(rows);
    let mut b = Vec::with_capacity(rows);
    let mut score = Vec::with_capacity(rows);
    let mut x = 0x9e37_79b9_7f4a_7c15u64 | 1;
    for _ in 0..rows {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        g1.push((x % 64) as i64);
        g2.push((x % 100) as i64);
        g3.push((x % 50) as i64);
        a.push((x % 1_000_000) as i64);
        b.push((x >> 11) as i64);
        score.push((x as f64) / 65536.0);
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("dt", DataType::Date32, true),
        Field::new("g1", DataType::Int64, true),
        Field::new("g2", DataType::Int64, true),
        Field::new("g3", DataType::Int64, true),
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Int64, true),
        Field::new("score", DataType::Float64, true),
    ]));
    RecordBatch::try_new(
        schema,
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

#[cfg(feature = "duckdb")]
fn write_parquet(path: &std::path::Path, batch: &RecordBatch) {
    let file = std::fs::File::create(path).unwrap();
    let props = parquet::file::properties::WriterProperties::builder()
        .set_compression(parquet::basic::Compression::ZSTD(
            parquet::basic::ZstdLevel::try_new(1).unwrap(),
        ))
        .build();
    let mut w =
        parquet::arrow::arrow_writer::ArrowWriter::try_new(file, batch.schema(), Some(props))
            .unwrap();
    w.write(batch).unwrap();
    w.close().unwrap();
}

#[cfg(not(feature = "duckdb"))]
fn main() {
    eprintln!(
        "本 bench 需要 duckdb feature：cargo run --release -p qsdb-bench --features duckdb --bin duckdb_vs"
    );
}
