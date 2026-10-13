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
    let mut s = TcpStream::connect(addr).expect("connect");
    let req = format!(
        "POST {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\
         Content-Type: application/octet-stream\r\nContent-Length: {}\r\n\r\n",
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
    let rows: usize = arg("--rows").and_then(|v| v.parse().ok()).unwrap_or(5_000_000);

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
    println!("IPC body = {:.1} MB / {} 批", whole.len() as f64 / 1e6, batches.len());

    let t0 = Instant::now();
    let (st, resp) = post(&addr, "/api/v1/write?table=t_http&mode=append", &whole);
    println!(
        "HTTP 一次整批: {:.3}s status={st} {}",
        t0.elapsed().as_secs_f64(),
        resp.lines().last().unwrap_or("")
    );

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
