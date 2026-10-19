//! join 物化受控对比（避开 fair_vs 的段间相互影响）：只测 join，QSDB+DuckDB 交替
#[cfg(feature = "duckdb")]
mod imp {
    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use duckdb::Connection;
    use qsdb::api::{Qsdb, QsdbOptions};
    use std::sync::Arc;
    use std::time::Instant;

    pub fn run() {
        let root = std::env::temp_dir().join("qsdb_jmat_vs");
        let _ = std::fs::remove_dir_all(&root);
        let db = Qsdb::open(
            &root,
            QsdbOptions {
                scan_cache_bytes: 0,
                ..Default::default()
            },
        )
        .unwrap();
        db.sql("CREATE TABLE t (g BIGINT, v BIGINT, x DOUBLE, s STRING, dp BIGINT) PARTITIONED BY (dp)").unwrap();
        let sc = Arc::new(Schema::new(vec![
            Field::new("g", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
            Field::new("x", DataType::Float64, false),
            Field::new("s", DataType::Utf8, false),
            Field::new("dp", DataType::Int64, false),
        ]));
        for p in 0..16i64 {
            let n = 625_000usize;
            let g: Int64Array = (0..n).map(|i| (i as i64 * 7919 + p) % 5_000_000).collect();
            let v: Int64Array = (0..n).map(|i| (i as i64) % 10_000).collect();
            let x = Float64Array::from(
                (0..n)
                    .map(|i| ((i % 1000) as f64) * 1.5)
                    .collect::<Vec<_>>(),
            );
            let s = StringArray::from(
                (0..n)
                    .map(|i| format!("s-{}", i % 250_000))
                    .collect::<Vec<_>>(),
            );
            let dp: Int64Array = vec![p; n].into();
            db.append(
                "t",
                &[RecordBatch::try_new(
                    sc.clone(),
                    vec![
                        Arc::new(g),
                        Arc::new(v),
                        Arc::new(x),
                        Arc::new(s),
                        Arc::new(dp),
                    ],
                )
                .unwrap()],
            )
            .unwrap();
        }
        db.sql("CREATE TABLE dim (g BIGINT, name BIGINT)").unwrap();
        let dsc = Arc::new(Schema::new(vec![
            Field::new("g", DataType::Int64, false),
            Field::new("name", DataType::Int64, false),
        ]));
        for p in 0..4i64 {
            let n = 156_250usize;
            let g: Int64Array = (0..n)
                .map(|i| (i as i64 * 7919 + p * 11) % 5_000_000)
                .collect();
            let nm: Int64Array = (0..n).map(|i| i as i64).collect();
            db.append(
                "dim",
                &[RecordBatch::try_new(dsc.clone(), vec![Arc::new(g), Arc::new(nm)]).unwrap()],
            )
            .unwrap();
        }
        let glob = root
            .join("default")
            .join("t")
            .join("**")
            .join("*.parquet")
            .to_string_lossy()
            .replace(std::path::MAIN_SEPARATOR, "/");
        let dglob = root
            .join("default")
            .join("dim")
            .join("**")
            .join("*.parquet")
            .to_string_lossy()
            .replace(std::path::MAIN_SEPARATOR, "/");
        let duck = Connection::open_in_memory().unwrap();
        duck.execute_batch("SET threads=16; SET memory_limit='4GB';")
            .unwrap();

        let cases: Vec<(&str, String, String)> = vec![
            (
                "物化 LIMIT 1000",
                "SELECT t.v, d.name FROM t JOIN dim d ON t.g = d.g LIMIT 1000".into(),
                format!(
                    "SELECT t.v, d.name FROM read_parquet('{glob}') AS t JOIN read_parquet('{dglob}') AS d ON t.g = d.g LIMIT 1000"
                ),
            ),
            (
                "物化 全量",
                "SELECT t.v, d.name FROM t JOIN dim d ON t.g = d.g".into(),
                format!(
                    "SELECT t.v, d.name FROM read_parquet('{glob}') AS t JOIN read_parquet('{dglob}') AS d ON t.g = d.g"
                ),
            ),
            (
                "count(*) join",
                "SELECT count(*) FROM t JOIN dim d ON t.g = d.g".into(),
                format!(
                    "SELECT count(*) FROM read_parquet('{glob}') AS t JOIN read_parquet('{dglob}') AS d ON t.g = d.g"
                ),
            ),
        ];
        println!("=== join 受控对比（只测 join，交替 5 轮取中位数）===");
        for (name, q, d) in cases {
            let _ = db.sql_collect(&q).unwrap_or_default(); // 预热
            let _ = duck.query_row(&format!("SELECT count(*) FROM ({d})"), [], |r| {
                r.get::<_, i64>(0)
            });
            let mut qs = vec![];
            let mut ds = vec![];
            for _ in 0..5 {
                let t0 = Instant::now();
                let _ = db.sql_collect(&q).unwrap();
                qs.push(t0.elapsed().as_secs_f64());
                let t0 = Instant::now();
                let _ = duck
                    .query_row(&format!("SELECT count(*) FROM ({d})"), [], |r| {
                        r.get::<_, i64>(0)
                    })
                    .unwrap();
                ds.push(t0.elapsed().as_secs_f64());
            }
            qs.sort_by(|a, b| a.partial_cmp(b).unwrap());
            ds.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let (qm, dm) = (qs[2], ds[2]);
            println!("{name:<18} QSDB {qm:.3}s  DuckDB {dm:.3}s  {:.2}x", dm / qm);
        }
    }
}
#[cfg(feature = "duckdb")]
fn main() {
    imp::run();
}
#[cfg(not(feature = "duckdb"))]
fn main() {
    println!("需要 --features duckdb");
}
