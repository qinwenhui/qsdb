//! M7-B4 新 SQL 特性 DuckDB 对比基准（feature 门控：`--features duckdb`）。
//!
//! 维度：LIKE 过滤（prefix / contains / ILIKE）、UNION ALL、UNION distinct、
//! INTERSECT / EXCEPT。公平性口径：同一批 parquet（QSDB 写出），
//! DuckDB 经 read_parquet 读同文件；同 SQL、同进程族、同机器、best of N。
//!
//! 运行：cargo run --release -p qsdb-bench --features duckdb --bin sql_gaps_vs -- [rows]

#[cfg(feature = "duckdb")]
use std::time::Instant;

#[cfg(feature = "duckdb")]
use arrow::array::{Int64Array, StringArray};
#[cfg(feature = "duckdb")]
use arrow::datatypes::{DataType, Field, Schema};
#[cfg(feature = "duckdb")]
use arrow::record_batch::RecordBatch;
#[cfg(feature = "duckdb")]
use duckdb::Connection;
#[cfg(feature = "duckdb")]
use qsdb::api::{Qsdb, QsdbOptions};
#[cfg(feature = "duckdb")]
use std::sync::Arc;

#[cfg(feature = "duckdb")]
fn synth(rows: usize, seed: i64) -> RecordBatch {
    // 真实形态字符串：tenant-{id}/{seq:05}（~24B/行）
    let schema = Arc::new(Schema::new(vec![
        Field::new("s", DataType::Utf8, false),
        Field::new("k", DataType::Int64, false),
    ]));
    let s: Vec<String> = (0..rows)
        .map(|i| {
            format!(
                "tenant-{}/{:05}",
                (i as i64 + seed) % 1000,
                (i as i64 * 7 + seed) % 100_000
            )
        })
        .collect();
    let s = StringArray::from(s);
    let k: Int64Array = (0..rows)
        .map(|i| (i as i64 + seed * 31) % 4_000_000)
        .collect();
    RecordBatch::try_new(schema, vec![Arc::new(s), Arc::new(k)]).unwrap()
}

#[cfg(feature = "duckdb")]
fn bench_pair<F, G>(name: &str, mut qsdb_fn: F, mut duck_fn: G)
where
    F: FnMut() -> i64,
    G: FnMut() -> i64,
{
    // best of 3（双方同口径；首轮含预热已在外层做过）
    let run = |f: &mut dyn FnMut() -> i64| {
        let mut best = f64::INFINITY;
        let mut got = 0;
        for _ in 0..3 {
            let t = Instant::now();
            got = f();
            let el = t.elapsed().as_secs_f64();
            if el < best {
                best = el;
            }
        }
        (best, got)
    };
    eprintln!("[{name}] qsdb running...");
    let (tq, q) = run(&mut qsdb_fn);
    eprintln!("[{name}] duck running...");
    let (td, d) = run(&mut duck_fn);
    eprintln!("[{name}] done");
    assert_eq!(q, d, "{name}: 结果不一致 qsdb={q} duck={d}");
    let ratio = td / tq;
    println!(
        "{:<34} qsdb {:>7.3}s   duckdb {:>7.3}s   {:>5.2}x {}",
        name,
        tq,
        td,
        ratio,
        if ratio >= 1.0 { "✓" } else { "" }
    );
}

#[cfg(feature = "duckdb")]
fn main() {
    let rows: usize = std::env::args()
        .nth(1)
        .and_then(|v| v.parse().ok())
        .unwrap_or(20_000_000);
    let root = std::env::temp_dir().join("qsdb_vs_gaps");
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();

    // 数据准备（QSDB 写出，双方共用 parquet）
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    db.sql("CREATE TABLE t (s STRING, k BIGINT)").unwrap();
    let parts = 8usize;
    for p in 0..parts {
        let batch = synth(rows, 19_000 + p as i64);
        db.append("t", &[batch]).unwrap();
    }
    let total = rows * parts;
    let tdir = root.join("default").join("t");
    // 无分区表：段文件直接在表目录；分区表：t/<part>/*.parquet——双兼容
    let has_flat = std::fs::read_dir(&tdir)
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .any(|e| e.path().extension().is_some_and(|x| x == "parquet"))
        })
        .unwrap_or(false);
    let glob_str = if has_flat {
        tdir.join("*.parquet").to_string_lossy().replace('\\', "/")
    } else {
        tdir.join("**")
            .join("*.parquet")
            .to_string_lossy()
            .replace('\\', "/")
    };
    println!("glob: {glob_str}");
    println!("data: {total} rows string+int, {parts} partitions\n");

    let duck = Connection::open_in_memory().unwrap();
    duck.execute_batch(&format!("SELECT count(*) FROM read_parquet('{glob_str}')"))
        .unwrap();

    // --- A. LIKE 前缀（1/1000 选择率） ---
    bench_pair(
        "LIKE prefix  (s LIKE 'tenant-42%')",
        || {
            let out = db
                .sql_collect("SELECT count(*) FROM t WHERE s LIKE 'tenant-42%'")
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
                    "SELECT count(*) FROM read_parquet('{glob_str}') WHERE s LIKE 'tenant-42%'"
                ),
                [],
                |r| r.get::<_, i64>(0),
            )
            .unwrap()
        },
    );

    // --- B. LIKE 包含（子串，无快路径下界） ---
    bench_pair(
        "LIKE contains(s LIKE '%/00042')",
        || {
            let out = db
                .sql_collect("SELECT count(*) FROM t WHERE s LIKE '%/00042'")
                .unwrap();
            let b = &out[0];
            arrow::util::display::array_value_to_string(b.column(0).as_ref(), 0)
                .unwrap()
                .parse::<i64>()
                .unwrap()
        },
        || {
            duck.query_row(
                &format!("SELECT count(*) FROM read_parquet('{glob_str}') WHERE s LIKE '%/00042'"),
                [],
                |r| r.get::<_, i64>(0),
            )
            .unwrap()
        },
    );

    // --- C. ILIKE（大小写不敏感前缀；数据小写、模式大写=全扫匹配） ---
    bench_pair(
        "ILIKE prefix (s ILIKE 'TENANT-42%')",
        || {
            let out = db
                .sql_collect("SELECT count(*) FROM t WHERE s ILIKE 'TENANT-42%'")
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
                    "SELECT count(*) FROM read_parquet('{glob_str}') WHERE s ILIKE 'TENANT-42%'"
                ),
                [],
                |r| r.get::<_, i64>(0),
            )
            .unwrap()
        },
    );

    // --- D. UNION ALL ---
    bench_pair(
        "UNION ALL   (k<2M ∪ k<1M)",
        || {
            let out = db
                .sql_collect("SELECT count(*) FROM (SELECT k FROM t WHERE k < 2000000 UNION ALL SELECT k FROM t WHERE k < 1000000)")
                .unwrap();
            let b = &out[0];
            arrow::util::display::array_value_to_string(b.column(0).as_ref(), 0)
                .unwrap()
                .parse::<i64>()
                .unwrap()
        },
        || {
            duck.query_row(
                &format!("SELECT count(*) FROM (SELECT k FROM read_parquet('{glob_str}') WHERE k < 2000000 UNION ALL SELECT k FROM read_parquet('{glob_str}') WHERE k < 1000000)"),
                [],
                |r| r.get::<_, i64>(0),
            )
            .unwrap()
        },
    );

    // --- E. UNION distinct ---
    bench_pair(
        "UNION dist  (k%100 ∪ k%50)",
        || {
            let out = db
                .sql_collect("SELECT count(*) FROM (SELECT k % 100 AS v FROM t WHERE k < 2000000 UNION SELECT k % 50 FROM t WHERE k < 2000000)")
                .unwrap();
            let b = &out[0];
            arrow::util::display::array_value_to_string(b.column(0).as_ref(), 0)
                .unwrap()
                .parse::<i64>()
                .unwrap()
        },
        || {
            duck.query_row(
                &format!("SELECT count(*) FROM (SELECT k % 100 AS v FROM read_parquet('{glob_str}') WHERE k < 2000000 UNION SELECT k % 50 FROM read_parquet('{glob_str}') WHERE k < 2000000)"),
                [],
                |r| r.get::<_, i64>(0),
            )
            .unwrap()
        },
    );

    // --- F. INTERSECT / EXCEPT（int 键域） ---
    bench_pair(
        "INTERSECT   (k<2M ∩ odd-ish)",
        || {
            let out = db
                .sql_collect("SELECT count(*) FROM (SELECT k FROM t WHERE k < 2000000 INTERSECT SELECT k FROM t WHERE k % 3 = 0)")
                .unwrap();
            let b = &out[0];
            arrow::util::display::array_value_to_string(b.column(0).as_ref(), 0)
                .unwrap()
                .parse::<i64>()
                .unwrap()
        },
        || {
            duck.query_row(
                &format!("SELECT count(*) FROM (SELECT k FROM read_parquet('{glob_str}') WHERE k < 2000000 INTERSECT SELECT k FROM read_parquet('{glob_str}') WHERE k % 3 = 0)"),
                [],
                |r| r.get::<_, i64>(0),
            )
            .unwrap()
        },
    );
    bench_pair(
        "EXCEPT      (k<2M − odd-ish)",
        || {
            let out = db
                .sql_collect("SELECT count(*) FROM (SELECT k FROM t WHERE k < 2000000 EXCEPT SELECT k FROM t WHERE k % 3 = 0)")
                .unwrap();
            let b = &out[0];
            arrow::util::display::array_value_to_string(b.column(0).as_ref(), 0)
                .unwrap()
                .parse::<i64>()
                .unwrap()
        },
        || {
            duck.query_row(
                &format!("SELECT count(*) FROM (SELECT k FROM read_parquet('{glob_str}') WHERE k < 2000000 EXCEPT SELECT k FROM read_parquet('{glob_str}') WHERE k % 3 = 0)"),
                [],
                |r| r.get::<_, i64>(0),
            )
            .unwrap()
        },
    );

    println!("\n口径：同批 parquet / 同 SQL / best-of-3；✓ = 快于 DuckDB");
}

#[cfg(not(feature = "duckdb"))]
fn main() {
    println!("需要 --features duckdb");
}
