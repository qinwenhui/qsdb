//! 冷读口径下验证 agg 的列裁剪（此前探针用默认 256MB 缓存，掩盖了问题）
use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use qsdb::{Qsdb, QsdbOptions};
use std::sync::Arc;
use std::time::Instant;
fn build(root: &std::path::Path, wide: bool) {
    let _ = std::fs::remove_dir_all(root);
    let db = Qsdb::open(root, QsdbOptions::default()).unwrap();
    let schema_sql = if wide {
        "CREATE TABLE t (g BIGINT, v BIGINT, x DOUBLE, s STRING, dp BIGINT) PARTITIONED BY (dp)"
    } else {
        "CREATE TABLE t (v BIGINT, x DOUBLE, dp BIGINT) PARTITIONED BY (dp)"
    };
    db.sql(schema_sql).unwrap();
    for p in 0..16i64 {
        let n = 625_000usize;
        let v: Int64Array = (0..n).map(|i| i as i64).collect();
        let x = Float64Array::from(
            (0..n)
                .map(|i| ((i % 1000) as f64) * 1.5)
                .collect::<Vec<_>>(),
        );
        let dp: Int64Array = vec![p; n].into();
        if wide {
            let g: Int64Array = (0..n).map(|i| (i as i64 * 7919 + p) % 5_000_000).collect();
            let s = StringArray::from(
                (0..n)
                    .map(|i| format!("s-{}", i % 250_000))
                    .collect::<Vec<_>>(),
            );
            let sc = Arc::new(Schema::new(vec![
                Field::new("g", DataType::Int64, false),
                Field::new("v", DataType::Int64, false),
                Field::new("x", DataType::Float64, false),
                Field::new("s", DataType::Utf8, false),
                Field::new("dp", DataType::Int64, false),
            ]));
            db.append(
                "t",
                &[RecordBatch::try_new(
                    sc,
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
        } else {
            let sc = Arc::new(Schema::new(vec![
                Field::new("v", DataType::Int64, false),
                Field::new("x", DataType::Float64, false),
                Field::new("dp", DataType::Int64, false),
            ]));
            db.append(
                "t",
                &[RecordBatch::try_new(sc, vec![Arc::new(v), Arc::new(x), Arc::new(dp)]).unwrap()],
            )
            .unwrap();
        }
    }
}
fn main() {
    for (label, wide) in [("3列表 (v,x,dp)", false), ("5列表 (g,v,x,s,dp)", true)] {
        let root =
            std::env::temp_dir().join(format!("qsdb_coldprune_{}", if wide { "w" } else { "n" }));
        build(&root, wide);
        let db = Qsdb::open(
            &root,
            QsdbOptions {
                scan_cache_bytes: 0,
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
        println!("{label:<22} 冷读 agg = {best:.4}s");
    }
    println!("\n若两者接近 → 列裁剪生效；若 5 列明显慢 → 冷读下读了多余列");
}
