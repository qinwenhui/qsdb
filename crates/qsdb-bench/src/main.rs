//! qsdb-bench：宏观基准（对标性能红线）。
//!
//! - `smoke`：harness 管线自检（arrow filter 计时）
//! - `write`：批量 append 吞吐（红线：≥ 100MB/s 或 ≥ 1M rows/s 单线程）
//! - `scan`：全表扫描吞吐（红线：≥ 300MB/s 单核热缓存，数值列）
//! - `meta`：万级分区元数据（红线：冷/热 < 50ms）

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{ArrayRef, Date32Array, Decimal128Array, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use qsdb::catalog::ColumnDef;
use qsdb::types::{DecimalMeta, Literal, LogicalType};
use qsdb::{Predicate, Qsdb, QsdbOptions, ScanRequest, TableDef};

fn main() {
    let mut bench = String::from("smoke");
    let mut rows: usize = 1_000_000;
    let mut repeat: usize = 3;
    let mut parts: usize = 10_000;
    let mut keep = false;
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--bench" => bench = args.next().unwrap_or_default(),
            "--rows" => rows = args.next().and_then(|v| v.parse().ok()).unwrap_or(rows),
            "--repeat" => repeat = args.next().and_then(|v| v.parse().ok()).unwrap_or(repeat),
            "--partitions" => parts = args.next().and_then(|v| v.parse().ok()).unwrap_or(parts),
            "--keep" => keep = true,
            other => {
                eprintln!("unknown arg '{other}'");
                std::process::exit(2);
            }
        }
    }
    match bench.as_str() {
        "smoke" => smoke(rows.max(10_000), repeat),
        "write" => write_bench(rows, repeat),
        "scan" => scan_bench(rows, repeat),
        "meta" => meta_bench(parts),
        "agg" => agg_bench(rows),
        "rss" => rss_bench(),
        other => {
            eprintln!("unknown bench '{other}' (expect smoke|write|scan|meta)");
            std::process::exit(2);
        }
    }
    if !keep {
        let _ = std::fs::remove_dir_all(work_root());
    }
}

fn work_root() -> PathBuf {
    let d = std::env::temp_dir().join("qsdb_bench");
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn events_def() -> TableDef {
    let col = |name: &str, ty: LogicalType| ColumnDef {
        name: name.into(),
        logical_type: ty,
        nullable: true,
        default: None,
        comment: None,
    };
    TableDef::new(
        "events",
        vec![
            col("dt", LogicalType::Date),
            col("a", LogicalType::Int64),
            col("b", LogicalType::Int64),
            col("c", LogicalType::Float64),
            col("s", LogicalType::Varchar { max_chars: None }),
            col("amt", LogicalType::Decimal(DecimalMeta::new(18, 4))),
        ],
        vec!["dt".into()],
    )
    .unwrap()
}

fn synth_batch(rows: usize, dt: i32, seed: u64) -> RecordBatch {
    let mut a = Vec::with_capacity(rows);
    let mut b = Vec::with_capacity(rows);
    let mut c = Vec::with_capacity(rows);
    let mut s = Vec::with_capacity(rows);
    let mut amt = Vec::with_capacity(rows);
    let mut x = seed | 1;
    for i in 0..rows {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        a.push(x as i64);
        b.push((x.wrapping_mul(3) ^ (i as u64)) as i64);
        c.push((x as f64) / 65536.0);
        s.push(format!("tenant{:06}", x % 100_000));
        amt.push((x % 1_000_000) as i128);
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("dt", DataType::Date32, true),
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Int64, true),
        Field::new("c", DataType::Float64, true),
        Field::new("s", DataType::Utf8, true),
        Field::new("amt", DataType::Decimal128(18, 4), true),
    ]));
    let amt_arr: ArrayRef =
        Arc::new(Decimal128Array::from(amt).with_data_type(DataType::Decimal128(18, 4)));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Date32Array::from(vec![dt; rows])),
            Arc::new(Int64Array::from(a)),
            Arc::new(Int64Array::from(b)),
            Arc::new(Float64Array::from(c)),
            Arc::new(StringArray::from(s)),
            amt_arr,
        ],
    )
    .unwrap()
}

/// 逻辑（解压后）字节数估算。
fn logical_bytes(rows: usize) -> usize {
    rows * (8 + 8 + 8 + 15 /* string avg */ + 16/* decimal128 */)
}

// ---------------------------------------------------------------------------

fn smoke(rows: usize, repeat: usize) {
    let batch = {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]));
        let a = Int64Array::from_iter_values((0..rows as i64).map(|i| i * 3));
        let b = Int64Array::from_iter_values((0..rows as i64).map(|i| i ^ 0x5a5a));
        RecordBatch::try_new(schema, vec![Arc::new(a), Arc::new(b)]).unwrap()
    };
    println!("smoke: rows={rows}");
    let mask = {
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        arrow::array::BooleanArray::from_iter((0..col.len()).map(|i| col.value(i) % 7 == 0))
    };
    let mut best = u128::MAX;
    for _ in 0..repeat {
        let t = Instant::now();
        let out = arrow::compute::filter_record_batch(&batch, &mask).unwrap();
        best = best.min(t.elapsed().as_nanos());
        assert!(out.num_rows() > 0);
    }
    println!(
        "filter best: {best} ns ({:.1} M rows/s)",
        rows as f64 / (best as f64 / 1e9) / 1e6
    );
}

fn write_bench(rows: usize, repeat: usize) {
    let root = work_root();
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    db.create_table(events_def()).unwrap();
    println!("write: {rows} rows/batch x {repeat} append（单线程，zstd(3)）");
    let mut total_rows = 0usize;
    let mut total_bytes = 0u64;
    let t0 = Instant::now();
    for r in 0..repeat {
        let batch = synth_batch(rows, 19_000 + r as i32, 0x1234 + r as u64);
        let res = db.append("events", &[batch]).unwrap();
        total_rows += res.rows as usize;
        total_bytes += res.bytes;
    }
    let el = t0.elapsed();
    println!(
        "append: {total_rows} rows, {:.1} MB compressed, {:.2} s → {:.1} MB/s, {:.0} rows/s",
        total_bytes as f64 / 1e6,
        el.as_secs_f64(),
        total_bytes as f64 / 1e6 / el.as_secs_f64(),
        total_rows as f64 / el.as_secs_f64()
    );
    println!(
        "logical: {:.1} MB/s (红线 ≥ 100MB/s 或 ≥ 1M rows/s)",
        logical_bytes(total_rows) as f64 / 1e6 / el.as_secs_f64()
    );
}

fn scan_bench(rows: usize, repeat: usize) {
    let root = work_root();
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    db.create_table(events_def()).unwrap();
    // 预写数据（热缓存）
    let mut written = 0usize;
    for r in 0..repeat {
        let batch = synth_batch(rows, 19_100 + r as i32, 0xbeef + r as u64);
        db.append("events", &[batch]).unwrap();
        written += rows;
    }
    // 热身 + 计时
    let mut best = f64::MAX;
    for _ in 0..3 {
        let t = Instant::now();
        let mut n = 0usize;
        for b in db
            .scan(ScanRequest {
                table: "events".into(),
                ..Default::default()
            })
            .unwrap()
        {
            n += b.unwrap().num_rows();
        }
        assert_eq!(n, written);
        best = best.min(t.elapsed().as_secs_f64());
    }
    let mb = logical_bytes(written) as f64 / 1e6;
    println!(
        "scan: {written} rows ({mb:.0} MB logical) best {best:.3}s → {:.0} MB/s 单线程（红线 ≥ 300MB/s）",
        mb / best
    );
    // 谓词裁剪：只扫一个分区
    let t = Instant::now();
    let n: usize = db
        .scan_collect(ScanRequest {
            table: "events".into(),
            predicate: Some(Predicate::eq("dt", Literal::Date(19_100))),
            ..Default::default()
        })
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum();
    let el = t.elapsed().as_secs_f64();
    println!("scan(dt=eq): {n}/{written} rows in {el:.3}s（分区裁剪后 1/{repeat} 数据）");
}

/// M2 gate：1 亿行 3 列 group by（单线程/多线程由 RAYON_NUM_THREADS 控制）。
fn agg_bench(rows: usize) {
    let root = work_root();
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    db.sql(
        "CREATE TABLE agg_bench (g1 BIGINT, g2 BIGINT, g3 BIGINT, v BIGINT) PARTITIONED BY (g1)",
    )
    .unwrap();
    let threads = rayon_threads();
    println!("agg: {rows} rows, 3-col group by, rayon threads = {threads}");
    // 写入：分批
    let t = Instant::now();
    const BATCH: usize = 2_000_000;
    let mut written = 0usize;
    let mut part = 0i32;
    while written < rows {
        let n = BATCH.min(rows - written);
        let batch = synth_agg_batch(n, part);
        db.append("agg_bench", &[batch]).unwrap();
        written += n;
        part = part.wrapping_add(1) & 63; // 64 个分区
    }
    println!("write {written} rows in {:.1}s", t.elapsed().as_secs_f64());

    // 热身（页缓存）
    let _ = db.sql_collect("SELECT count(*) FROM agg_bench").unwrap();

    let t = Instant::now();
    let out = db
        .sql_collect("SELECT g1, g2, g3, count(*), sum(v) FROM agg_bench GROUP BY g1, g2, g3")
        .unwrap();
    let el = t.elapsed().as_secs_f64();
    let groups: usize = out.iter().map(|b| b.num_rows()).sum();
    println!(
        "group by: {groups} groups in {el:.2}s → {:.0} rows/s（红线：1 亿行 < 2s 单线程 / < 0.5s 8 线程）",
        rows as f64 / el
    );
    std::fs::remove_dir_all(&root).ok();
}

fn synth_agg_batch(rows: usize, part: i32) -> RecordBatch {
    let mut g2 = Vec::with_capacity(rows);
    let mut g3 = Vec::with_capacity(rows);
    let mut v = Vec::with_capacity(rows);
    let mut x = (part as u64) | 1;
    for _ in 0..rows {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        g2.push((x % 100) as i64);
        g3.push((x % 50) as i64);
        v.push((x % 1000) as i64);
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("g1", DataType::Int64, true),
        Field::new("g2", DataType::Int64, true),
        Field::new("g3", DataType::Int64, true),
        Field::new("v", DataType::Int64, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![i64::from(part); rows])),
            Arc::new(Int64Array::from(g2)),
            Arc::new(Int64Array::from(g3)),
            Arc::new(Int64Array::from(v)),
        ],
    )
    .unwrap()
}

fn rayon_threads() -> usize {
    std::env::var("RAYON_NUM_THREADS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(rayon::current_num_threads)
}

/// M2 gate：嵌入态空闲 RSS（unix /proc；Windows 打印跳过）。
fn rss_bench() {
    let root = work_root();
    let _db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    #[cfg(unix)]
    {
        let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
        for line in status.lines() {
            if line.starts_with("VmRSS") {
                println!("{line}（红线 < 16MB 嵌入态）");
                return;
            }
        }
    }
    #[cfg(not(unix))]
    println!("rss: /proc 检查仅 unix；Windows 由 CI 任务管理器口径另行验证");
    std::fs::remove_dir_all(&root).ok();
}

fn meta_bench(parts: usize) {
    let root = work_root();
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    db.create_table(events_def()).unwrap();
    println!("meta: writing {parts} partitions (1 row each)…");
    let mut batch_parts = Vec::with_capacity(parts);
    for p in 0..parts {
        batch_parts.push(synth_batch(1, 19_000 + p as i32, p as u64));
    }
    let t = Instant::now();
    db.append("events", &batch_parts).unwrap();
    println!("write: {} in {:.2}s", parts, t.elapsed().as_secs_f64());

    // 冷：新实例（无缓存）打开 + limit=1 扫描 = 纯元数据路径
    // （分区 readdir + manifest 读取；文件惰性打开只碰第一个）
    let t = Instant::now();
    let db2 = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    let open_ms = t.elapsed().as_secs_f64() * 1000.0;
    let t = Instant::now();
    let n: usize = db2
        .scan_collect(ScanRequest {
            table: "events".into(),
            limit: Some(1),
            ..Default::default()
        })
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum();
    let scan_ms = t.elapsed().as_secs_f64() * 1000.0;
    assert_eq!(n, 1);
    println!(
        "open: {open_ms:.1}ms + discover/read {parts} manifests: {scan_ms:.1}ms（红线合计 < 50ms）"
    );
    // 热：再来一遍（OS 页缓存热）
    let t = Instant::now();
    let n: usize = db2
        .scan_collect(ScanRequest {
            table: "events".into(),
            limit: Some(1),
            ..Default::default()
        })
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum();
    let hot_ms = t.elapsed().as_secs_f64() * 1000.0;
    assert_eq!(n, 1);
    println!("hot repeat: {hot_ms:.1}ms");
    // 全表行数校验（走 manifest 统计而非读文件，便于未来加速）
    let rows: u64 = {
        let def = db2.table_def("events").unwrap();
        let tdir = root.join("default").join("events");
        qsdb::scan::list_partitions(&tdir, def.partition_keys.len())
            .iter()
            .filter_map(|(p, _)| qsdb::manifest::PartitionManifest::read(p).ok().flatten())
            .map(|m| m.total_rows())
            .sum()
    };
    assert_eq!(rows as usize, parts);
    println!("manifest-only row count: {rows} ✓");
}
