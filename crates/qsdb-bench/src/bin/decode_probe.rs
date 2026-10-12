//! 解码瓶颈定位：分离「元数据短路 (count(*))」/「单列解码」/「全列解码」，
//! 对照 DuckDB 同口径，算出 QSDB 的列解码吞吐与差距来源。
//! 运行：cargo run --release -p qsdb-bench --features duckdb --bin decode_probe -- [rows]
#[cfg(feature = "duckdb")]
mod imp {
    use std::sync::Arc;
    use std::time::Instant;
    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use duckdb::Connection;
    use qsdb::api::{Qsdb, QsdbOptions};

    pub fn run() {
        let rows: usize = std::env::args().nth(1).and_then(|v| v.parse().ok()).unwrap_or(10_000_000);
        let root = std::env::temp_dir().join("qsdb_decode_probe");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let threads = rayon::current_num_threads().min(16).max(1);

        let db = Qsdb::open(&root, QsdbOptions { scan_cache_bytes: 0, ..Default::default() }).unwrap();
        db.sql("CREATE TABLE t (g BIGINT, v BIGINT, x DOUBLE, s STRING)").unwrap();
        let parts = 16usize;
        let per = rows / parts;
        for p in 0..parts {
            let sch = Arc::new(Schema::new(vec![
                Field::new("g", DataType::Int64, false),
                Field::new("v", DataType::Int64, false),
                Field::new("x", DataType::Float64, false),
                Field::new("s", DataType::Utf8, false),
            ]));
            let g: Int64Array = (0..per).map(|i| (i as i64 * 7919 + p as i64) % 5_000_000).collect();
            let v: Int64Array = (0..per).map(|i| (i as i64) % 10_000).collect();
            let x: Float64Array = (0..per).map(|i| ((i % 1000) as f64) * 1.5).collect();
            let s: Vec<String> = (0..per).map(|i| format!("s-{}", i % 250_000)).collect();
            db.append("t", &[RecordBatch::try_new(sch, vec![Arc::new(g), Arc::new(v), Arc::new(x), Arc::new(StringArray::from(s))]).unwrap()]).unwrap();
        }
        let glob = root.join("default").join("t").join("*.parquet").to_string_lossy().replace(std::path::MAIN_SEPARATOR, "/");
        let dir = root.join("default").join("t");
        let bytes: u64 = std::fs::read_dir(&dir).unwrap().filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|x| x == "parquet"))
            .filter_map(|e| e.metadata().ok()).map(|m| m.len()).sum();
        println!("=== 解码瓶颈定位 ===");
        println!("  数据 {rows} 行 / {} 列（g,v,x int+double+s string）, 文件总字节 {:.1} MB", 4, bytes as f64 / 1e6);
        println!("  QSDB scan_cache=0（冷读）, threads={threads}; DuckDB 同 threads\n");

        let duck = Connection::open_in_memory().unwrap();
        duck.execute_batch(&format!("SET threads={threads}; SET memory_limit='4GB';")).unwrap();

        let cases: Vec<(&str, &str, &str)> = vec![
            ("count(*) 元数据短路", "SELECT count(*) FROM t", "SELECT count(*) FROM read_parquet('{g}')"),
            ("单列 int  sum(v)", "SELECT sum(v) FROM t", "SELECT sum(v) FROM read_parquet('{g}')"),
            ("单列 dbl  sum(x)", "SELECT sum(x) FROM t", "SELECT sum(x) FROM read_parquet('{g}')"),
            ("单列 str  count(s)", "SELECT count(s) FROM t", "SELECT count(s) FROM read_parquet('{g}')"),
            ("双列 v+x", "SELECT sum(v), sum(x) FROM t", "SELECT sum(v), sum(x) FROM read_parquet('{g}')"),
            ("全四列", "SELECT sum(g), sum(v), sum(x), count(s) FROM t", "SELECT sum(g), sum(v), sum(x), count(s) FROM read_parquet('{g}')"),
        ];
        println!("{:<22} {:>10} {:>10} {:>8}  {}", "场景", "QSDB", "DuckDB", "比率", "QSDB 有效吞吐");
        for (name, q_sql, d_tmpl) in cases {
            let d_sql = d_tmpl.replace("{g}", &glob);
            let tq = best3(|| db.sql_collect(q_sql).map(|b| b.iter().map(|x| x.num_rows()).sum::<usize>() as i64));
            let td = best3(|| duck.query_row(&d_sql, [], |r| r.get::<_, i64>(0)).map_err(|e| qsdb::error::QsdbError::invalid(e.to_string())));
            // 有效吞吐：假设该场景读的字节数（单列≈总字节/4，全列≈总字节）
            let cols = match name {
                n if n.contains("单列") => 1.0,
                n if n.contains("双列") => 2.0,
                n if n.contains("全四列") => 4.0,
                _ => 0.0,
            };
            let mb = bytes as f64 / 1e6 * cols / 4.0;
            let tput = if tq > 0.0 { format!("{:.0} MB/s", mb / tq) } else { "-".into() };
            println!("{:<22} {:>9.3}s {:>9.3}s {:>7.2}x  {}", name, tq, td, td / tq, tput);
        }
        println!("\n判读：count(*) 差=固定开销；单列差=解码层；单列→全列斜率=列解码扩展性");
    }

    fn best3<F: FnMut() -> Result<i64, qsdb::error::QsdbError>>(mut f: F) -> f64 {
        let mut best = f64::INFINITY;
        for _ in 0..3 {
            let t = Instant::now();
            if f().is_err() { return f64::NAN; }
            best = best.min(t.elapsed().as_secs_f64());
        }
        best
    }
}

#[cfg(feature = "duckdb")]
fn main() { imp::run(); }
#[cfg(not(feature = "duckdb"))]
fn main() { println!("需要 --features duckdb"); }
