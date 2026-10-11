//! semi-join（EXISTS / IN 子查询）DuckDB 对比（feature `duckdb`）。
//!
//! 场景：40M 行主表 t（8 分区）× 维表 dims（低基数 g ∈ [0,256)——
//! 稠密整数域，命中我们的位图快路径）。对比：
//! 1. IN 子查询（semi-join，等价执行计划）
//! 2. NOT IN（anti-join，三值 NULL）
//!
//! 同 parquet 文件、同 SQL 语义、同机交替。
//!
//! 运行：cargo run --release -p qsdb-bench --features duckdb --bin semi_vs -- [rows_per_part]

#[cfg(feature = "duckdb")]
fn main() {
    use std::sync::Arc;
    use std::time::Instant;

    use arrow::array::{Date32Array, Float64Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    let rows: usize = std::env::args()
        .nth(1)
        .and_then(|v| v.parse().ok())
        .unwrap_or(5_000_000);
    let parts = 8usize;
    let root = std::env::temp_dir().join("qsdb_semi_vs");
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();

    let db = qsdb::Qsdb::open(
        &root,
        qsdb::QsdbOptions {
            scan_cache_bytes: 1024 * 1024 * 1024,
            ..Default::default()
        },
    )
    .unwrap();
    db.sql("CREATE TABLE t (dt DATE, g BIGINT, a BIGINT, score DOUBLE) PARTITIONED BY (dt)")
        .unwrap();
    db.sql("CREATE TABLE dims (g BIGINT, label STRING)").unwrap();

    let mut dims_g = Vec::new();
    let mut dims_label = Vec::new();
    let schema_dims = Arc::new(Schema::new(vec![
        Field::new("g", DataType::Int64, true),
        Field::new("label", DataType::Utf8, true),
    ]));
    for i in 0..256i64 {
        if i % 3 == 0 {
            // 256 域内取 ~1/3 → 命中率 ~1/3
            dims_g.push(Some(i));
            dims_label.push(Some(format!("lbl{i}")));
        }
    }
    let dims_batch = RecordBatch::try_new(
        Arc::clone(&schema_dims),
        vec![
            Arc::new(Int64Array::from(dims_g)),
            Arc::new(arrow::array::StringArray::from(dims_label)),
        ],
    )
    .unwrap();
    db.append("dims", &[dims_batch]).unwrap();

    let synth = |rows: usize, dt: i32, seed: u64| {
        let mut x = seed | 0x9e37_79b9_7f4a_7c15;
        let mut g = Vec::with_capacity(rows);
        let mut a = Vec::with_capacity(rows);
        let mut score = Vec::with_capacity(rows);
        for _ in 0..rows {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            g.push((x % 256) as i64);
            a.push((x % 1_000_000) as i64);
            score.push(x as f64 / 65536.0);
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("dt", DataType::Date32, true),
            Field::new("g", DataType::Int64, true),
            Field::new("a", DataType::Int64, true),
            Field::new("score", DataType::Float64, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Date32Array::from(vec![dt; rows])),
                Arc::new(Int64Array::from(g)),
                Arc::new(Int64Array::from(a)),
                Arc::new(Float64Array::from(score)),
            ],
        )
        .unwrap()
    };
    let t_write = Instant::now();
    for p in 0..parts {
        db.append("t", &[synth(rows, 19_000 + p as i32, 0x1000 + p as u64)])
            .unwrap();
    }
    println!(
        "data: t {} rows / dims 85 rows (qsdb write {:.1}s)",
        rows * parts,
        t_write.elapsed().as_secs_f64()
    );

    // DuckDB 读同一批 parquet（公平口径：双方都走 parquet 解码，非内存驻留表）
    let duck = duckdb::Connection::open_in_memory().unwrap();
    let tdir = root.join("default").join("t");
    let ddir = root.join("default").join("dims");
    let glob = |d: &std::path::Path| {
        let partitioned = std::fs::read_dir(d)
            .map(|rd| {
                rd.flatten()
                    .any(|e| e.file_name().to_string_lossy().starts_with("dt="))
            })
            .unwrap_or(false);
        let pat = if partitioned {
            d.join("*").join("*.parquet")
        } else {
            d.join("*.parquet")
        };
        pat.to_string_lossy().replace('\\', "/")
    };
    let t_glob = glob(&tdir);
    let d_glob = glob(&ddir);
    duck.execute_batch(&format!(
        "CREATE VIEW t AS SELECT * FROM read_parquet('{t_glob}')"
    ))
    .unwrap();
    duck.execute_batch(&format!(
        "CREATE VIEW dims AS SELECT * FROM read_parquet('{d_glob}')"
    ))
    .unwrap();
    duck.execute_batch("SELECT count(*) FROM t").unwrap();

    let queries: &[(&str, &str)] = &[
        ("基线聚合(无semi)", "SELECT count(*), sum(a) FROM t"),
        ("基线过滤+聚合", "SELECT count(*), sum(a) FROM t WHERE g > 100"),
        ("IN 子查询", "SELECT count(*), sum(a) FROM t WHERE g IN (SELECT g FROM dims)"),
        ("NOT IN", "SELECT count(*) FROM t WHERE g NOT IN (SELECT g FROM dims)"),
        ("EXISTS(关联)", "SELECT count(*) FROM t WHERE EXISTS (SELECT 1 FROM dims WHERE dims.g = t.g)"),
    ];
    println!("\n（QSDB = 位图/免编码快路径；DuckDB = hash semi join；交替 2 轮）");
    for (name, sql) in queries {
        for round in 0..2 {
            let t = Instant::now();
            let q = db.sql_collect(sql).unwrap();
            let qv = format!(
                "{}|{}",
                arrow::util::display::array_value_to_string(q[0].column(0).as_ref(), 0).unwrap(),
                if q[0].num_columns() > 1 {
                    arrow::util::display::array_value_to_string(q[0].column(1).as_ref(), 0).unwrap()
                } else {
                    "-".into()
                }
            );
            let tq = t.elapsed().as_secs_f64();
            let t = Instant::now();
            // 同一聚合 SQL 直读首列（外层再包 count 会数结果行数）
            let d: i64 = duck.query_row(sql, [], |r| r.get(0)).unwrap();
            let td = t.elapsed().as_secs_f64();
            if round == 0 {
                println!("  预热: qsdb {tq:.3}s duckdb {td:.3}s");
                // 校验行数一致
                let qn: i64 = qv.split('|').next().unwrap().parse().unwrap();
                assert_eq!(qn, d, "{name} 行数不一致（qsdb {qv} vs duckdb {d}）");
            } else {
                println!(
                    "  {name:<12} qsdb {tq:.3}s  duckdb {td:.3}s  → {:.2}x {}",
                    td / tq,
                    if td > tq { "快" } else { "慢" }
                );
            }
        }
    }
    let _ = std::fs::remove_dir_all(&root);
}

#[cfg(not(feature = "duckdb"))]
fn main() {
    eprintln!("需要 duckdb feature：cargo run --release -p qsdb-bench --features duckdb --bin semi_vs");
}
