//! HTTP 批量写通道的实测：**生态工具 / ODBC·JDBC 桥走的就是这条路**
//! （`POST /api/v1/write?table=T&mode=append`，body = Arrow IPC 流）。
//!
//! 为什么单独一个 bin：内核几条路要能分别报数，否则"生态层加一层 wire 要付多少"
//! 只能靠猜。客户端手写 HTTP/1.1（零新依赖），服务端是 `qsdb server`。
//!
//! 用法：`http_bulk --addr 127.0.0.1:9430 --parquet /path/t.parquet [--rows N]`
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Instant;

use arrow::record_batch::RecordBatch;

fn arg(name: &str) -> Option<String> {
    let a: Vec<String> = std::env::args().collect();
    a.iter()
        .position(|x| x == name)
        .and_then(|i| a.get(i + 1).cloned())
}

fn post(addr: &str, path: &str, body: &[u8]) -> (u16, String) {
    post_ct(addr, path, "application/octet-stream", body)
}

fn post_ct(addr: &str, path: &str, ct: &str, body: &[u8]) -> (u16, String) {
    let mut s = TcpStream::connect(addr).expect("connect");
    // 注意：**不要**用续行反斜杠拼 HTTP 头——续行会把缩进空格带进头部，
    // 于是 "         Content-Type: …" 成了非法头行（axum 的 JSON 提取器
    // 直接报 "Expected request with Content-Type: application/json"，很难看出）。
    let req = format!(
        "POST {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nContent-Type: {ct}\r\nContent-Length: {}\r\n\r\n",
        body.len()
    );
    s.write_all(req.as_bytes()).unwrap();
    s.write_all(body).unwrap();
    let mut raw = Vec::new();
    s.read_to_end(&mut raw).unwrap();
    let text = String::from_utf8_lossy(&raw).to_string();
    let status: u16 = text
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse().ok())
        .unwrap_or(0);
    (status, text)
}

fn main() {
    let addr = arg("--addr").unwrap_or_else(|| "127.0.0.1:9430".into());
    let pq = arg("--parquet").expect("--parquet");
    let rows: usize = arg("--rows")
        .and_then(|v| v.parse().ok())
        .unwrap_or(5_000_000);

    let batches: Vec<RecordBatch> = {
        let f = std::fs::File::open(&pq).expect("open parquet");
        let mut r = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(f)
            .expect("parquet builder")
            .with_batch_size(65536)
            .build()
            .expect("reader");
        r.by_ref().map(|b| b.expect("batch")).collect()
    };
    let schema = batches[0].schema();

    // ① 一次性 IPC 编码全部数据，一个请求发出（"工具把整批交上来"）
    let mut whole = Vec::new();
    {
        let mut w = arrow::ipc::writer::StreamWriter::try_new(&mut whole, &schema).unwrap();
        for b in &batches {
            w.write(b).unwrap();
        }
        w.finish().unwrap();
    }
    println!(
        "IPC body = {:.1} MB / {} 批",
        whole.len() as f64 / 1e6,
        batches.len()
    );

    // 目标表（三种格式各一个）
    for fmt in ["ipc", "bulk", "csv"] {
        let sql = format!(
            "CREATE TABLE t_{fmt} (id BIGINT, g BIGINT, gs STRING, v BIGINT, x DOUBLE, s STRING, dt DATE)"
        );
        let body = serde_json::json!({"sql": sql}).to_string();
        let (st, resp) = post_ct(&addr, "/api/v1/query", "application/json", body.as_bytes());
        println!("建表 t_{fmt}: status={st} {}", resp.lines().last().unwrap_or(""));
    }
    for fmt in ["ipc", "bulk", "csv"] {
        let body = format!(
            "{{\"sql\":\"CREATE TABLE t_{fmt} (id BIGINT, g BIGINT, gs STRING, v BIGINT, x DOUBLE, s STRING, dt DATE)\"}}"
        );
        let mut s = TcpStream::connect(&addr).expect("connect");
        let req = format!(
            "POST /api/v1/query HTTP/1.1
Host: {addr}
Connection: close
             Content-Type: application/json
Content-Length: {}

",
            body.len()
        );
        s.write_all(req.as_bytes()).unwrap();
        s.write_all(body.as_bytes()).unwrap();
        let mut raw = Vec::new();
        s.read_to_end(&mut raw).unwrap();
        let text = String::from_utf8_lossy(&raw);
        println!("建表 t_{fmt}: {}", text.lines().last().unwrap_or(""));
    }
    for fmt in ["ipc", "bulk", "csv"] {
        let body: Vec<u8> = match fmt {
            "ipc" => whole.clone(),
            "bulk" => qsdb::bulk::encode(&batches).expect("bulk encode"),
            _ => {
                // CSV：列序 = 表列序（与 /write 约定一致）；这里手写表头 + 逐行
                let schema = batches[0].schema();
                let mut buf: Vec<u8> = Vec::with_capacity(600 << 20);
                let mut w = arrow::csv::WriterBuilder::new()
                    .with_header(true)
                    .build(&mut buf);
                for b in &batches {
                    w.write(b).unwrap();
                }
                drop(w);
                let _ = schema;
                buf
            }
        };
        let t0 = Instant::now();
        let (st, resp) = post(
            &addr,
            &format!("/api/v1/write?table=t_{fmt}&mode=append&format={fmt}"),
            &body,
        );
        println!(
            "HTTP 整批 format={fmt:<5} body={:>7.1}MB: {:.3}s status={st} {}",
            body.len() as f64 / 1e6,
            t0.elapsed().as_secs_f64(),
            resp.lines().last().unwrap_or("")
        );
    }

    // ② 分批（每请求 1 批）——"工具按自己的节奏流式推"的形态
    let t0 = Instant::now();
    let mut n = 0usize;
    for b in batches.iter().take(rows / 65536) {
        let mut buf = Vec::new();
        {
            let mut w = arrow::ipc::writer::StreamWriter::try_new(&mut buf, &schema).unwrap();
            w.write(b).unwrap();
            w.finish().unwrap();
        }
        let (st, _) = post(&addr, "/api/v1/write?table=t_http2&mode=append", &buf);
        if st != 200 {
            println!("  批 {n} 失败 status={st}");
            break;
        }
        n += 1;
    }
    println!("HTTP 逐批({} 批): {:.3}s", n, t0.elapsed().as_secs_f64());
}
