//! 解码路径对决：同次运行内 QSDB（冷读 scan_cache=0）vs DuckDB（read_parquet），
//! 单列 → 多列梯度，裁决"单列是否也慢"（此前两次跨测量结论互相矛盾）。
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
        let rows: usize = std::env::args()
            .nth(1)
            .and_then(|v| v.parse().ok())
            .unwrap_or(10_000_000);
        let root = std::env::temp_dir().join("qsdb_decode_vs");
        let _ = std::fs::remove_dir_all(&root);
        // 双方**各自默认配置**（QSDB 默认 256MB 解码后缓存；DuckDB 默认 buffer pool）——
        // 这才是"出厂状态"的公平对比。此前把 QSDB 设成冷读而 DuckDB 有隐式缓存，
        // 是本探针第一版的偏差来源（DuckDB 时间恒 0.009s 即为缓存证据）。
        let use_cache = std::env::var("QSDB_COLD").is_err();
        let db = Qsdb::open(
            &root,
            QsdbOptions {
                scan_cache_bytes: if use_cache { 256 * 1024 * 1024 } else { 0 },
                ..Default::default()
            },
        )
        .unwrap();
        db.sql("CREATE TABLE t (g BIGINT, v BIGINT, x DOUBLE, s STRING)")
            .unwrap();
        let sc = Arc::new(Schema::new(vec![
            Field::new("g", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
            Field::new("x", DataType::Float64, false),
            Field::new("s", DataType::Utf8, false),
        ]));
        let per = rows / 16;
        for p in 0..16i64 {
            let g: Int64Array = (0..per)
                .map(|i| (i as i64 * 7919 + p) % 5_000_000)
                .collect();
            let v: Int64Array = (0..per).map(|i| i as i64).collect();
            let x = Float64Array::from(
                (0..per)
                    .map(|i| ((i % 1000) as f64) * 1.5)
                    .collect::<Vec<_>>(),
            );
            let s = StringArray::from(
                (0..per)
                    .map(|i| format!("s-{}", i % 250_000))
                    .collect::<Vec<_>>(),
            );
            db.append(
                "t",
                &[RecordBatch::try_new(
                    sc.clone(),
                    vec![Arc::new(g), Arc::new(v), Arc::new(x), Arc::new(s)],
                )
                .unwrap()],
            )
            .unwrap();
        }
        let glob = root
            .join("default")
            .join("t")
            .join("*.parquet")
            .to_string_lossy()
            .replace(std::path::MAIN_SEPARATOR, "/");
        let duck = Connection::open_in_memory().unwrap();
        duck.execute_batch("SET threads=16; SET memory_limit='4GB';")
            .unwrap();

        println!("=== 解码路径对决（{rows} 行，同次运行交替 5 轮取中位数）===");
        println!("  QSDB: scan_cache_bytes=0（冷读）  DuckDB: read_parquet（每查询解码）\n");
        println!("{:<26} {:>9} {:>9} {:>8}", "查询", "QSDB", "DuckDB", "比率");
        for (name, qs, ds) in [
            (
                "sum(v) 单列 int",
                "SELECT sum(v) FROM t",
                format!("SELECT sum(v) FROM read_parquet('{glob}')"),
            ),
            (
                "sum(x) 单列 dbl",
                "SELECT sum(x) FROM t",
                format!("SELECT sum(x) FROM read_parquet('{glob}')"),
            ),
            (
                "count(s) 单列 str",
                "SELECT count(s) FROM t",
                format!("SELECT count(s) FROM read_parquet('{glob}')"),
            ),
            (
                "sum(v),sum(x) 双列",
                "SELECT sum(v), sum(x) FROM t",
                format!("SELECT sum(v), sum(x) FROM read_parquet('{glob}')"),
            ),
            (
                "4 列全扫",
                "SELECT sum(g), sum(v), sum(x), count(s) FROM t",
                format!("SELECT sum(g), sum(v), sum(x), count(s) FROM read_parquet('{glob}')"),
            ),
        ] {
            let _ = db.sql_collect(&qs);
            let _ = duck.query_row(&format!("SELECT count(*) FROM ({ds})"), [], |r| {
                r.get::<_, i64>(0)
            });
            let (mut qv, mut dv) = (vec![], vec![]);
            for _ in 0..5 {
                let t = Instant::now();
                let _ = db.sql_collect(&qs).unwrap();
                qv.push(t.elapsed().as_secs_f64());
                let t = Instant::now();
                let _ = duck
                    .query_row(&format!("SELECT count(*) FROM ({ds})"), [], |r| {
                        r.get::<_, i64>(0)
                    })
                    .unwrap();
                dv.push(t.elapsed().as_secs_f64());
            }
            qv.sort_by(|a, b| a.partial_cmp(b).unwrap());
            dv.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let (q, d) = (qv[2], dv[2]);
            println!("{name:<26} {q:>8.4}s {d:>8.4}s {:>7.2}x", d / q);
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
