//! GroupTable 内核微基准：内存批直灌 ingest（绕过扫描/求值），隔离分组内核。
//! 运行：cargo run --release -p qsdb-bench --bin agg_micro -- [rows] [groups]
use arrow::array::{Date32Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use qsdb::exec::agg::GroupTable;
use qsdb::expr::{AggExpr, AggFunc, PhysExpr};
use std::sync::Arc;
use std::time::Instant;

fn main() {
    let rows: usize = std::env::args()
        .nth(1)
        .and_then(|v| v.parse().ok())
        .unwrap_or(5_000_000);
    let _groups: i64 = std::env::args()
        .nth(2)
        .and_then(|v| v.parse().ok())
        .unwrap_or(1600);
    let bs = 8192usize;
    let _nb = rows / bs;

    let g = vec![
        PhysExpr::Column {
            idx: 1,
            name: "g1".into(),
            ty: qsdb::types::LogicalType::Int64,
        },
        PhysExpr::Column {
            idx: 2,
            name: "g2".into(),
            ty: qsdb::types::LogicalType::Int64,
        },
        PhysExpr::Column {
            idx: 3,
            name: "g3".into(),
            ty: qsdb::types::LogicalType::Int64,
        },
    ];
    let aggs = vec![
        AggExpr::new(AggFunc::Count, None, "count(*)").unwrap(),
        AggExpr::new(
            AggFunc::Sum,
            Some(PhysExpr::Column {
                idx: 4,
                name: "a".into(),
                ty: qsdb::types::LogicalType::Int64,
            }),
            "sum(a)",
        )
        .unwrap(),
        AggExpr::new(
            AggFunc::Sum,
            Some(PhysExpr::Column {
                idx: 5,
                name: "b".into(),
                ty: qsdb::types::LogicalType::Int64,
            }),
            "sum(b)",
        )
        .unwrap(),
    ];

    let shard_rows = rows / 8;

    use rayon::prelude::*;
    let t0 = Instant::now();
    let results: Vec<(usize, usize)> = (0..8usize)
        .into_par_iter()
        .map(|shard| {
            let mut total = 0usize;
            let nb = shard_rows / bs;
            let mut rng2: u64 = 0x9E3779B97F4A7C15 ^ (shard as u64 + 1);
            let mut next2 = move || {
                rng2 ^= rng2 << 13;
                rng2 ^= rng2 >> 7;
                rng2 ^= rng2 << 17;
                rng2
            };
            let mut t = GroupTable::new();
            t.set_arity(3);
            for bi in 0..nb {
                // 每批现造（含造批成本，但与内核同一循环可比相对量）
                let mut g1 = Vec::with_capacity(bs);
                let mut g2 = Vec::with_capacity(bs);
                let mut g3 = Vec::with_capacity(bs);
                let mut a = Vec::with_capacity(bs);
                let mut b = Vec::with_capacity(bs);
                let dt = Date32Array::from(vec![19000i32; bs]);
                for _ in 0..bs {
                    let x = next2();
                    g1.push((x % 64) as i64);
                    g2.push((x % 100) as i64);
                    g3.push((x % 50) as i64);
                    a.push((x % 1_000_000) as i64);
                    b.push((x % 100) as i64);
                }
                let batch = RecordBatch::try_new(
                    Arc::new(Schema::new(vec![
                        Field::new("dt", DataType::Date32, true),
                        Field::new("g1", DataType::Int64, true),
                        Field::new("g2", DataType::Int64, true),
                        Field::new("g3", DataType::Int64, true),
                        Field::new("a", DataType::Int64, true),
                        Field::new("b", DataType::Int64, true),
                    ])),
                    vec![
                        Arc::new(dt),
                        Arc::new(Int64Array::from(
                            g1.into_iter().map(Some).collect::<Vec<_>>(),
                        )),
                        Arc::new(Int64Array::from(
                            g2.into_iter().map(Some).collect::<Vec<_>>(),
                        )),
                        Arc::new(Int64Array::from(
                            g3.into_iter().map(Some).collect::<Vec<_>>(),
                        )),
                        Arc::new(Int64Array::from(
                            a.into_iter().map(Some).collect::<Vec<_>>(),
                        )),
                        Arc::new(Int64Array::from(
                            b.into_iter().map(Some).collect::<Vec<_>>(),
                        )),
                    ],
                )
                .unwrap();
                t.ingest(&g, &aggs, &batch).unwrap();
                total += bs;
                if bi == 0 && shard == 0 {
                    println!("首批后组数: {}", t.len());
                }
            }
            (total, t.len())
        })
        .collect();
    let e = t0.elapsed().as_secs_f64();
    let total: usize = results.iter().map(|(r, _)| r).sum();
    println!(
        "{total} rows 8-shard-parallel in {e:.3}s → {:.0} 万行/s（含造批）, 组数 {:?}",
        total as f64 / e / 10000.0,
        results.iter().map(|(_, g)| g).collect::<Vec<_>>()
    );
}
