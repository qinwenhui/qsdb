//! 决定性验证：agg 的 4.5x 差异是否来自 scan_cache_bytes（0 vs 默认 256MB）
use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use qsdb::{Qsdb, QsdbOptions};
use std::sync::Arc;
use std::time::Instant;
fn build(root: &std::path::Path) {
    let _ = std::fs::remove_dir_all(root);
    let db = Qsdb::open(root, QsdbOptions::default()).unwrap();
    db.sql(
        "CREATE TABLE t (g BIGINT, v BIGINT, x DOUBLE, s STRING, dp BIGINT) PARTITIONED BY (dp)",
    )
    .unwrap();
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
        let v: Int64Array = (0..n).map(|i| i as i64).collect();
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
}
fn main() {
    let root = std::env::temp_dir().join("qsdb_cache_agg");
    build(&root);
    for (label, cache) in [
        ("scan_cache=0（冷读，v2 口径）", 0usize),
        ("scan_cache=256MB（默认，探针口径）", 256 * 1024 * 1024),
    ] {
        let db = Qsdb::open(
            &root,
            QsdbOptions {
                scan_cache_bytes: cache,
                ..Default::default()
            },
        )
        .unwrap();
        let mut best = f64::INFINITY;
        for _ in 0..7 {
            let t = Instant::now();
            db.sql_collect("SELECT count(*), sum(v), sum(x), min(v), max(v) FROM t")
                .unwrap();
            best = best.min(t.elapsed().as_secs_f64());
        }
        println!("{label:<34} {best:.4}s");
    }
}
