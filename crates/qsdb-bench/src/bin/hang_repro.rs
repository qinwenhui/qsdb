//! 挂死复现：复刻 sql_gaps_vs 时序（写 → duck 预热 → 每项 qsdb×3 + duck×3 → UNION ALL）。
//! 运行：cargo run --release -p qsdb-bench --features duckdb --bin hang_repro -- [iters]
#[cfg(feature = "duckdb")]
use std::time::Instant;
#[cfg(feature = "duckdb")]
use duckdb::Connection;
#[cfg(feature = "duckdb")]
use std::sync::Arc;
#[cfg(feature = "duckdb")]
use arrow::array::{Int64Array, StringArray};
#[cfg(feature = "duckdb")]
use arrow::datatypes::{DataType, Field, Schema};
#[cfg(feature = "duckdb")]
use arrow::record_batch::RecordBatch;
#[cfg(feature = "duckdb")]
use qsdb::{Qsdb, QsdbOptions};

#[cfg(feature = "duckdb")]
fn main() {
    let iters: usize = std::env::args().nth(1).and_then(|v| v.parse().ok()).unwrap_or(3);
    for it in 0..iters {
        println!("=== iteration {it} ===");
        let root = std::env::temp_dir().join(format!("qsdb_hangr_{it}"));
        let _ = std::fs::remove_dir_all(&root);
        let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
        db.sql("CREATE TABLE t (s STRING, k BIGINT)").unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("s", DataType::Utf8, false),
            Field::new("k", DataType::Int64, false),
        ]));
        for p in 0..8 {
            let rows = 250_000usize;
            let seed = 19_000 + p as i64;
            let s: Vec<String> = (0..rows)
                .map(|i| format!("tenant-{}/{:05}", (i as i64 + seed) % 1000, (i as i64 * 7 + seed) % 100_000))
                .collect();
            let k: Int64Array = (0..rows).map(|i| (i as i64 + seed * 31) % 4_000_000).collect();
            let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(s)), Arc::new(k)]).unwrap();
            db.append("t", &[batch]).unwrap();
        }
        let tdir = root.join("default").join("t");
        let glob = tdir.join("*.parquet").to_string_lossy().replace(std::path::MAIN_SEPARATOR, "/");
        let duck = Connection::open_in_memory().unwrap();
        duck.execute_batch(&format!("SELECT count(*) FROM read_parquet('{glob}')")).unwrap();
        let likes = [
            "s LIKE 'tenant-42%'",
            "s LIKE '%/00042'",
            "s ILIKE 'TENANT-42%'",
        ];
        for l in likes {
            // 与 bench_pair 同序：qsdb×3 先、duck×3 后
            for _ in 0..3 {
                let t0 = Instant::now();
                let out = db.sql_collect(&format!("SELECT count(*) FROM t WHERE {l}")).unwrap();
                let _ = arrow::util::display::array_value_to_string(out[0].column(0).as_ref(), 0).unwrap();
                print!("  q {:>6.3}s |", t0.elapsed().as_secs_f64());
            }
            for _ in 0..3 {
                let t0 = Instant::now();
                duck.query_row(&format!("SELECT count(*) FROM read_parquet('{glob}') WHERE {l}"), [], |r| r.get::<_, i64>(0)).unwrap();
                println!(" d {:>6.3}s  [{l}]", t0.elapsed().as_secs_f64());
            }
        }
        // UNION ALL（bench D 的 qsdb 侧——挂点）
        let t0 = Instant::now();
        let b = db.sql_collect(
            "SELECT count(*) FROM (SELECT k FROM t WHERE k < 2000000 UNION ALL SELECT k FROM t WHERE k < 1000000)",
        )
        .unwrap();
        let n: usize = b.iter().map(|x| x.num_rows()).sum();
        println!("  UNION ALL rows={n} {:>6.3}s", t0.elapsed().as_secs_f64());
        drop(duck);
        drop(db);
    }
    println!("ALL DONE");
}

#[cfg(not(feature = "duckdb"))]
fn main() {
    println!("需要 --features duckdb");
}
