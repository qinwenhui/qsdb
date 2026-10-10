//! M5-Gate 特性开销基准：窗口 / CTE vs 派生表 / 约束写开销 / UDAF vs 内置。
//!
//! 运行：cargo run --release -p qsdb-bench --bin m5_bench -- [rows_per_partition]

use std::sync::Arc;
use std::time::Instant;

use arrow::array::{Date32Array, Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use qsdb::QsdbOptions;
use qsdb::{Qsdb, WriteResult};

fn synth(rows: usize, dt: i32) -> RecordBatch {
    let mut rng: u64 = 0x9E3779B97F4A7C15 ^ (dt as u64);
    let mut next = move || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng
    };
    let dt_a = Date32Array::from(vec![dt; rows]);
    let g1: Vec<i64> = (0..rows).map(|_| (next() % 1000) as i64).collect();
    let g2: Vec<i64> = (0..rows).map(|_| (next() % 100) as i64).collect();
    let a: Vec<i64> = (0..rows).map(|_| (next() % 1_000_000) as i64).collect();
    let score: Vec<f64> = (0..rows).map(|i| (i as f64) * 0.5 % 1000.0).collect();
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("dt", DataType::Date32, true),
            Field::new("g1", DataType::Int64, true),
            Field::new("g2", DataType::Int64, true),
            Field::new("a", DataType::Int64, true),
            Field::new("score", DataType::Float64, true),
        ])),
        vec![
            Arc::new(dt_a),
            Arc::new(Int64Array::from(g1)),
            Arc::new(Int64Array::from(g2)),
            Arc::new(Int64Array::from(a)),
            Arc::new(Float64Array::from(score)),
        ],
    )
    .unwrap()
}

fn time_sql(db: &Qsdb, sql: &str, repeat: usize) -> f64 {
    // 首跑 warmup
    let _ = db.sql_collect(sql).unwrap();
    let mut best = f64::MAX;
    for _ in 0..repeat {
        let t = Instant::now();
        let n: usize = db
            .sql_collect(sql)
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();
        let e = t.elapsed().as_secs_f64();
        if n > 0 || sql.contains("LIMIT 0") {
            best = best.min(e);
        }
    }
    best
}

fn time_append(db: &Qsdb, table: &str, batch: &RecordBatch, repeat: usize) -> (f64, u64) {
    let mut best = f64::MAX;
    let mut rows = 0;
    for _ in 0..repeat {
        let t = Instant::now();
        let r: WriteResult = db.append(table, std::slice::from_ref(batch)).unwrap();
        best = best.min(t.elapsed().as_secs_f64());
        rows = r.rows;
    }
    (best, rows)
}

fn main() {
    let rows: usize = std::env::args()
        .nth(1)
        .and_then(|v| v.parse().ok())
        .unwrap_or(1_000_000);
    let parts = 8usize;
    let root = std::env::temp_dir().join("qsdb_m5_bench");
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();

    // 数据：8 分区 × rows
    let t0 = Instant::now();
    db.sql("CREATE TABLE t (dt DATE, g1 BIGINT, g2 BIGINT, a BIGINT, score DOUBLE) PARTITIONED BY (dt)")
        .unwrap();
    for p in 0..parts {
        db.append("t", &[synth(rows, 19_000 + p as i32)]).unwrap();
    }
    let total = rows * parts;
    println!(
        "data: {total} rows in {parts} partitions ({:.1}s)",
        t0.elapsed().as_secs_f64()
    );

    // --- 1. 窗口开销：row_number/sum OVER vs 等价 ORDER BY 基线 ---
    let base_sort = time_sql(&db, "SELECT g1, a FROM t ORDER BY g1, a DESC", 3);
    println!("baseline  ORDER BY g1,a DESC      : {base_sort:.3}s");
    let w_rn = time_sql(
        &db,
        "SELECT g1, row_number() OVER (PARTITION BY g1 ORDER BY a DESC) FROM t",
        3,
    );
    println!(
        "window    row_number() OVER (...)  : {w_rn:.3}s  (overhead {:.2}x)",
        w_rn / base_sort
    );
    let w_sum = time_sql(&db, "SELECT g1, sum(a) OVER (PARTITION BY g1) FROM t", 3);
    println!("window    sum(a) OVER (PARTITION)  : {w_sum:.3}s");
    let w_agg_win = time_sql(
        &db,
        "SELECT g2, sum(a) AS s, rank() OVER (ORDER BY sum(a) DESC) FROM t GROUP BY g2",
        3,
    );
    println!("agg+win   rank() OVER (ORDER BY sum): {w_agg_win:.3}s");

    // --- 2. CTE vs 派生表（应同量级） ---
    let cte = time_sql(
        &db,
        "WITH s AS (SELECT g1, sum(a) AS s FROM t GROUP BY g1) SELECT count(*) FROM s WHERE s > 100",
        3,
    );
    let derived = time_sql(
        &db,
        "SELECT count(*) FROM (SELECT g1, sum(a) AS s FROM t GROUP BY g1) d WHERE s > 100",
        3,
    );
    println!(
        "CTE       {cte:.3}s vs derived {derived:.3}s (ratio {:.2}x)",
        cte / derived
    );

    // --- 3. 约束写开销：无约束 vs NOT NULL + CHECK ---
    db.sql("CREATE TABLE plain (dt DATE, g1 BIGINT, a BIGINT) PARTITIONED BY (dt)")
        .unwrap();
    db.sql(
        "CREATE TABLE cons (dt DATE, g1 BIGINT NOT NULL, a BIGINT CHECK (a >= 0 AND a < 1000000)) PARTITIONED BY (dt)",
    )
    .unwrap();
    let full = synth(rows, 19_123);
    // 投影到 3 列（dt, g1, a）匹配测试表
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("dt", DataType::Date32, true),
            Field::new("g1", DataType::Int64, true),
            Field::new("a", DataType::Int64, true),
        ])),
        vec![
            Arc::clone(full.column(0)),
            Arc::clone(full.column(1)),
            Arc::clone(full.column(3)),
        ],
    )
    .unwrap();
    let (t_plain, n) = time_append(&db, "plain", &batch, 3);
    let (t_cons, _) = time_append(&db, "cons", &batch, 3);
    println!(
        "write {n} rows: plain {t_plain:.3}s vs constraints {t_cons:.3}s (overhead {:.2}x)",
        t_cons / t_plain
    );

    // --- 4. UDAF vs 内置 max（整组物化路径 vs 状态机路径） ---
    let b_max = time_sql(&db, "SELECT g1, max(a) FROM t GROUP BY g1", 3);
    println!("builtin   max(a) GROUP BY g1       : {b_max:.3}s");
    // 注册等价 UDAF（max）对比串行物化路径代价
    let ret: qsdb::udaf::RetTyFn = Arc::new(|args: &[qsdb::types::LogicalType]| match args {
        [qsdb::types::LogicalType::Int64] => Ok(qsdb::types::LogicalType::Int64),
        _ => Err(qsdb::QsdbError::type_err("bigint expected")),
    });
    let ev: qsdb::udaf::EvalFn = Arc::new(|args: &[arrow::array::ArrayRef]| {
        let a = args[0]
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| qsdb::QsdbError::type_err("bigint expected"))?;
        let m = a.iter().flatten().max();
        Ok(Arc::new(Int64Array::from(vec![m])) as arrow::array::ArrayRef)
    });
    qsdb::udaf::register("m5_bench_max", ret, ev).unwrap();
    let u_max = time_sql(&db, "SELECT g1, m5_bench_max(a) FROM t GROUP BY g1", 3);
    println!(
        "udaf      m5_bench_max GROUP BY    : {u_max:.3}s  (vs builtin {:.2}x)",
        u_max / b_max
    );

    let _ = std::fs::remove_dir_all(&root);
}
