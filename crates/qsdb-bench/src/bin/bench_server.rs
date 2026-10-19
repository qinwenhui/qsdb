//! M3 gate：HTTP 写入吞吐（回环 ≥500MB/s）与 scan vs parquet 原生读取对比。
//!
//! server 由本进程 spawn（cargo run 同一 target），客户端用 std TcpStream
//! 直发 HTTP（避免引入 reqwest）。

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{Date32Array, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;

fn main() {
    let mut args = std::env::args().skip(1);
    let bench = args.next().unwrap_or_else(|| "http".into());
    let rows: usize = args
        .next()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2_000_000);
    match bench.as_str() {
        "http" => http_write_bench(rows),
        "scan-vs-raw" => scan_vs_raw(rows),
        other => eprintln!("unknown bench '{other}' (expect http|scan-vs-raw)"),
    }
}

fn exe_dir() -> std::path::PathBuf {
    std::env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(|d| d.to_path_buf()))
        .unwrap_or_else(|| ".".into())
}

struct ServerGuard(Child);

impl Drop for ServerGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[allow(clippy::zombie_processes)] // 守卫 Drop 统一 kill+wait
fn start_server(root: &std::path::Path, port: u16) -> ServerGuard {
    // 从 bench 同目录找 qsdb 二进制（同一 target/release）
    let exe = exe_dir().join(if cfg!(windows) { "qsdb.exe" } else { "qsdb" });
    let child = Command::new(exe)
        .args(["server", "--path"])
        .arg(root)
        .args(["--addr", &format!("127.0.0.1:{port}")])
        .spawn()
        .expect("spawn qsdb server（先 cargo build --release -p qsdb-cli）");
    // 等就绪
    for _ in 0..100 {
        std::thread::sleep(std::time::Duration::from_millis(50));
        if let Ok(mut s) = TcpStream::connect(("127.0.0.1", port)) {
            let _ = s.write_all(b"GET /health HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n");
            let mut buf = [0u8; 512];
            if let Ok(n) = s.read(&mut buf) {
                if n > 0 && std::str::from_utf8(&buf[..n]).unwrap_or("").contains("200") {
                    return ServerGuard(child);
                }
            }
        }
    }
    panic!("server not ready in 5s");
}

fn http_call(
    port: u16,
    method: &str,
    path: &str,
    content_type: Option<&str>,
    body: &[u8],
) -> (u16, Vec<u8>) {
    let mut s = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    let mut req = format!("{method} {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n");
    if let Some(ct) = content_type {
        req.push_str(&format!("Content-Type: {ct}\r\n"));
    }
    req.push_str(&format!("Content-Length: {}\r\n\r\n", body.len()));
    s.write_all(req.as_bytes()).unwrap();
    s.write_all(body).unwrap();
    let mut resp = Vec::new();
    s.read_to_end(&mut resp).unwrap();
    // 解析状态行 + body（空行分割）
    let text_end = resp
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("no header/body sep");
    let status: u16 = std::str::from_utf8(&resp[..12])
        .unwrap()
        .split(' ')
        .nth(1)
        .and_then(|c| c.parse().ok())
        .unwrap_or(0);
    let body = resp[text_end + 4..].to_vec();
    // chunked 编码时的简化处理：JSON 响应一般足够小，axum 默认非 chunked（有 Content-Length）
    (status, body)
}

fn synth_batch(rows: usize, dt: i32) -> RecordBatch {
    let mut a = Vec::with_capacity(rows);
    let mut b = Vec::with_capacity(rows);
    let mut c = Vec::with_capacity(rows);
    let mut s = Vec::with_capacity(rows);
    let mut x = 0x1234_5678u64 | 1;
    for _ in 0..rows {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        a.push(x as i64);
        b.push((x % 100_000) as i64);
        c.push((x as f64) / 4096.0);
        s.push(format!("tenant{:06}", x % 1000));
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("dt", DataType::Date32, true),
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Int64, true),
        Field::new("score", DataType::Float64, true),
        Field::new("s", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Date32Array::from(vec![dt; rows])),
            Arc::new(Int64Array::from(a)),
            Arc::new(Int64Array::from(b)),
            Arc::new(Float64Array::from(c)),
            Arc::new(StringArray::from(s)),
        ],
    )
    .unwrap()
}

fn http_write_bench(rows: usize) {
    // 并发多流：4 客户端 × 不同分区（并发聚合口径）
    let root = std::env::temp_dir().join("qsdb_bench_http");
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    const PORT: u16 = 9441;
    let _guard = start_server(&root, PORT);

    let (st, _) = http_call(
        PORT,
        "POST",
        "/api/v1/query",
        Some("application/json"),
        br#"{"sql":"CREATE TABLE http_bench (dt DATE, a BIGINT, b BIGINT, score DOUBLE, s STRING) PARTITIONED BY (dt)"}"#,
    );
    assert_eq!(st, 200, "create table failed");
    let _ = rows;

    let batch = synth_batch(rows, 19_000);
    let logical = batch.get_array_memory_size();
    let repeat = 5;
    let streams = 8;
    // 计时外预生成：每流一个独立分区（dt = 19000 + sid）的 IPC
    let per_stream: Vec<Arc<Vec<u8>>> = (0..streams)
        .map(|sid| {
            let b = synth_batch(rows, 19_000 + sid);
            let mut buf = Vec::new();
            let mut w = StreamWriter::try_new(&mut buf, &b.schema()).unwrap();
            w.write(&b).unwrap();
            w.finish().unwrap();
            Arc::new(buf)
        })
        .collect();
    println!(
        "prepared {streams} streams x {rows} rows（每流 {:.1} MB logical）",
        logical as f64 / 1e6
    );
    let t = Instant::now();
    let handles: Vec<_> = per_stream
        .into_iter()
        .map(|ipc| {
            std::thread::spawn(move || {
                let mut written = 0usize;
                for _ in 0..repeat {
                    let (st, body) = http_call(
                        PORT,
                        "POST",
                        "/api/v1/write?table=http_bench&mode=append",
                        Some("application/octet-stream"),
                        &ipc,
                    );
                    assert_eq!(st, 200, "{}", String::from_utf8_lossy(&body));
                    written += rows;
                }
                written
            })
        })
        .collect();
    let mut total_rows = 0usize;
    for h in handles {
        total_rows += h.join().unwrap();
    }
    let el = t.elapsed().as_secs_f64();
    println!(
        "http write ({} streams x {} writes x {rows} rows): {total_rows} rows in {el:.2}s → {:.0} rows/s, {:.1} MB/s logical（红线 ≥ 500MB/s 并发回环）",
        streams,
        repeat,
        total_rows as f64 / el,
        (logical * total_rows / rows) as f64 / 1e6 / el
    );
    // 验证
    let (st, body) = http_call(
        PORT,
        "POST",
        "/api/v1/query",
        Some("application/json"),
        br#"{"sql":"SELECT count(*) FROM http_bench"}"#,
    );
    assert_eq!(st, 200);
    assert!(
        String::from_utf8_lossy(&body).contains(&format!("{total_rows}")),
        "{}",
        String::from_utf8_lossy(&body)
    );
    println!("verify: count = {total_rows} ✓");
    drop(_guard);
    let _ = std::fs::remove_dir_all(&root);
}

/// qsdb scan vs parquet-rs 原生 ArrowReader（同文件集、同投影）。
fn scan_vs_raw(rows: usize) {
    use qsdb::api::{Qsdb, QsdbOptions, ScanRequest};
    let root = std::env::temp_dir().join("qsdb_bench_svr");
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    db.sql(
        "CREATE TABLE t (dt DATE, a BIGINT, b BIGINT, score DOUBLE, s STRING) PARTITIONED BY (dt)",
    )
    .unwrap();
    for p in 0..8 {
        db.append("t", &[synth_batch(rows, 19_000 + p)]).unwrap();
    }
    let total = rows * 8;
    let logical_mb = (total * (8 + 8 + 8)) as f64 / 1e6; // a/b/score

    // 1) qsdb scan（投影 a/b/score）
    let mut best = f64::MAX;
    for _ in 0..3 {
        let t = Instant::now();
        let n: usize = db
            .scan_collect(ScanRequest {
                table: "t".into(),
                projection: Some(vec!["a".into(), "b".into(), "score".into()]),
                ..Default::default()
            })
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(n, total);
        best = best.min(t.elapsed().as_secs_f64());
    }
    println!(
        "qsdb scan: {total} rows best {best:.3}s → {:.0} MB/s",
        logical_mb / best
    );

    // 2) 原生 parquet-rs 逐文件读（同列）
    let tdir = root.join("default").join("t");
    let mut files = Vec::new();
    for e in std::fs::read_dir(&tdir).unwrap().flatten() {
        let p = e.path();
        if p.is_dir() {
            for f in std::fs::read_dir(&p).unwrap().flatten() {
                let fp = f.path();
                if fp.extension().and_then(|e| e.to_str()) == Some("parquet") {
                    files.push(fp);
                }
            }
        }
    }
    let mut best_raw = f64::MAX;
    for _ in 0..3 {
        let t = Instant::now();
        let mut n = 0usize;
        for f in &files {
            let file = std::fs::File::open(f).unwrap();
            let builder =
                parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
                    .unwrap();
            let schema = builder.schema().clone();
            let idx: Vec<usize> = ["a", "b", "score"]
                .iter()
                .map(|c| schema.index_of(c).unwrap())
                .collect();
            let mask = parquet::arrow::ProjectionMask::leaves(
                builder.parquet_schema(),
                idx.iter().copied(),
            );
            let reader = builder
                .with_projection(mask)
                .with_batch_size(8192)
                .build()
                .unwrap();
            for b in reader {
                n += b.unwrap().num_rows();
            }
        }
        assert_eq!(n, total);
        best_raw = best_raw.min(t.elapsed().as_secs_f64());
    }
    println!(
        "parquet-rs raw: {total} rows best {best_raw:.3}s → {:.0} MB/s",
        logical_mb / best_raw
    );
    println!(
        "QSDB 开销系数: {:.2}x（含分区发现/manifest/行过滤管线）",
        best / best_raw
    );
    let _ = std::fs::remove_dir_all(&root);
}

// parquet 依赖（bench 独立用）
use parquet as _;
