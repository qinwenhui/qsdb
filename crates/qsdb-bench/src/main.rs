//! qsdb-bench：宏观基准 harness。
//!
//! M0 只带 `smoke`（验证 harness 管线：arrow 批生成 + filter 计时）；
//! `scan` / `meta` / `write` 随 M1 落地，直接对标性能红线。

use std::time::Instant;

use arrow::{
    array::Int64Array,
    compute::filter_record_batch,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};

fn main() {
    let mut rows: usize = 1_000_000;
    let mut repeat: usize = 5;
    let mut bench = String::from("smoke");
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--bench" => bench = args.next().unwrap_or_default(),
            "--rows" => rows = args.next().and_then(|v| v.parse().ok()).unwrap_or(rows),
            "--repeat" => repeat = args.next().and_then(|v| v.parse().ok()).unwrap_or(repeat),
            other => {
                eprintln!("unknown arg '{other}'");
                std::process::exit(2);
            }
        }
    }

    match bench.as_str() {
        "smoke" => smoke(rows, repeat),
        "scan" | "meta" | "write" => {
            eprintln!("bench '{bench}' lands in M1 (see docs/QSDB设计开发文档.md §14)");
        }
        other => {
            eprintln!("unknown bench '{other}' (expect smoke|scan|meta|write)");
            std::process::exit(2);
        }
    }
}

/// 生成合成批次并对谓词过滤计时，验证基准管线可用。
fn smoke(rows: usize, repeat: usize) {
    let batch = synth_batch(rows);
    println!(
        "smoke: rows={rows}, bytes≈{}MB (2 x Int64)",
        batch.get_array_memory_size() / 1024 / 1024
    );

    let mask = {
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("col0 is Int64");
        arrow::array::BooleanArray::from_iter((0..col.len()).map(|i| col.value(i) % 7 == 0))
    };

    let mut best = u128::MAX;
    for _ in 0..repeat {
        let t = Instant::now();
        let out = filter_record_batch(&batch, &mask).expect("filter");
        let el = t.elapsed().as_nanos();
        assert_eq!(
            out.num_rows(),
            mask.iter().filter(|v| *v == Some(true)).count()
        );
        best = best.min(el);
    }
    println!(
        "filter best: {best} ns ({:.1} M rows/s)",
        rows as f64 / (best as f64 / 1e9) / 1e6
    );
}

fn synth_batch(rows: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Int64, false),
    ]));
    let a = Int64Array::from_iter_values((0..rows as i64).map(|i| i * 3));
    let b = Int64Array::from_iter_values((0..rows as i64).map(|i| i ^ 0x5a5a));
    RecordBatch::try_new(schema, vec![Arc::new(a), Arc::new(b)]).expect("build batch")
}

use std::sync::Arc;
