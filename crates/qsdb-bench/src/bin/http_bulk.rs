//! HTTP 批量**写**通道的实测：**生态工具 / ODBC·JDBC 桥走的就是这条路**
//! （`POST /api/v1/write?table=T&mode=append&format=…`）。
//!
//! 为什么单独一个 bin：内核几条路要能分别报数，否则"生态层加一层 wire 要付多少"
//! 只能靠猜。客户端手写 HTTP/1.1（零新依赖），服务端是 `qsdb server`。
//!
//! 四种 body 编码（[`docs/bulk-format.md`] / [`docs/rows-format.md`]）：
//!
//! | `format` | 形态 | 谁该用 |
//! |---|---|---|
//! | `ipc` | Arrow IPC 流 | 有 Arrow 库的（Rust/Arrow 生态） |
//! | `bulk` | 无依赖**列式** | 无依赖的列式生产者 |
//! | `rows` | 无依赖**行式** | **驱动**（JDBC/ODBC/Python，按行取值、可流式） |
//! | `csv` | 文本 | 人/脚本（慢，仅"能用"） |
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
    let (st, raw) = post_body(addr, path, ct, body);
    (st, String::from_utf8_lossy(&raw).to_string())
}

/// 同 [`post_ct`]，但返回**原始 body 字节**（二进制通道要算真实体积）。
fn post_body(addr: &str, path: &str, ct: &str, body: &[u8]) -> (u16, Vec<u8>) {
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
    let sep = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("no header/body separator");
    let head = String::from_utf8_lossy(&raw[..sep]).to_ascii_lowercase();
    let status: u16 = head
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse().ok())
        .unwrap_or(0);
    let rest = raw[sep + 4..].to_vec();
    let body = if head.contains("transfer-encoding: chunked") {
        dechunk(&rest)
    } else {
        rest
    };
    (status, body)
}

/// 极简 chunked 解码（大响应 axum 会分块）。
fn dechunk(mut b: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    while let Some(pos) = b.windows(2).position(|w| w == b"\r\n") {
        let n = usize::from_str_radix(
            String::from_utf8_lossy(&b[..pos])
                .split(';')
                .next()
                .unwrap_or("0")
                .trim(),
            16,
        )
        .unwrap_or(0);
        if n == 0 {
            break;
        }
        let start = pos + 2;
        out.extend_from_slice(&b[start..start + n]);
        b = &b[start + n + 2..];
    }
    out
}

/// 建表（列与源 parquet 一致；可重复跑——先删后建）。
fn create_table(addr: &str, name: &str) {
    let _ = post_ct(
        addr,
        "/api/v1/query",
        "application/json",
        serde_json::json!({ "sql": format!("DROP TABLE IF EXISTS {name}") })
            .to_string()
            .as_bytes(),
    );
    let sql = format!(
        "CREATE TABLE {name} (id BIGINT, g BIGINT, gs STRING, v BIGINT, x DOUBLE, s STRING, dt DATE)"
    );
    let body = serde_json::json!({ "sql": sql }).to_string();
    let (st, resp) = post_ct(addr, "/api/v1/query", "application/json", body.as_bytes());
    assert_eq!(st, 200, "建表 {name} 失败: {resp}");
}

/// 整批 body（一个请求发完）。
fn whole_body(fmt: &str, batches: &[RecordBatch]) -> Vec<u8> {
    match fmt {
        "ipc" => {
            let mut buf = Vec::new();
            let mut w =
                arrow::ipc::writer::StreamWriter::try_new(&mut buf, &batches[0].schema()).unwrap();
            for b in batches {
                w.write(b).unwrap();
            }
            w.finish().unwrap();
            buf
        }
        "bulk" => qsdb::bulk::encode(&batches[0].schema(), batches).expect("bulk encode"),
        // 行式**写侧形态**：flags=0，schema 由端点按 table= 查表定义给
        "rows" => qsdb::rows::encode_values(&batches[0].schema(), batches).expect("rows encode"),
        "csv" => {
            let mut buf: Vec<u8> = Vec::with_capacity(600 << 20);
            let mut w = arrow::csv::WriterBuilder::new()
                .with_header(true)
                .build(&mut buf);
            for b in batches {
                w.write(b).unwrap();
            }
            drop(w);
            buf
        }
        other => panic!("未知 format {other}"),
    }
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
    println!(
        "源：{} 批 / {} 行 / schema = {:?}",
        batches.len(),
        batches.iter().map(|b| b.num_rows()).sum::<usize>(),
        batches[0]
            .schema()
            .fields()
            .iter()
            .map(|f| f.name())
            .collect::<Vec<_>>()
    );

    let fmts = ["ipc", "bulk", "rows", "csv"];
    for fmt in fmts {
        create_table(&addr, &format!("t_{fmt}"));
    }

    // ① 整批（一个请求）——"工具把整批交上来"
    println!("\n① 整批（1 个请求）");
    for fmt in fmts {
        let t_enc = Instant::now();
        let body = whole_body(fmt, &batches);
        let enc = t_enc.elapsed().as_secs_f64();
        let t0 = Instant::now();
        let (st, resp) = post(
            &addr,
            &format!("/api/v1/write?table=t_{fmt}&mode=append&format={fmt}"),
            &body,
        );
        println!(
            "  format={fmt:<5} body={:>7.1}MB 编码{:.3}s → 写{:.3}s（{:.2}M 行/s）status={st} {}",
            body.len() as f64 / 1e6,
            enc,
            t0.elapsed().as_secs_f64(),
            rows as f64 / t0.elapsed().as_secs_f64() / 1e6,
            resp.lines().last().unwrap_or("")
        );
    }

    // ② 流式（每请求 1 块）——"驱动攒够 65536 行就发"，客户端内存恒定
    println!("\n② 流式（{} 个请求 × 65536 行）", rows / 65536);
    for fmt in fmts {
        let table = format!("t_{fmt}_s");
        create_table(&addr, &table);
        let t0 = Instant::now();
        let mut n = 0usize;
        for b in batches.iter().take(rows / 65536) {
            let body = whole_body(fmt, std::slice::from_ref(b));
            let (st, resp) = post(
                &addr,
                &format!("/api/v1/write?table={table}&mode=append&format={fmt}"),
                &body,
            );
            if st != 200 {
                println!(
                    "  批 {n} 失败 status={st} {}",
                    resp.lines().last().unwrap_or("")
                );
                break;
            }
            n += 1;
        }
        println!(
            "  format={fmt:<5} {n} 个请求：{:.3}s（{:.2}M 行/s）",
            t0.elapsed().as_secs_f64(),
            (n * 65536) as f64 / t0.elapsed().as_secs_f64() / 1e6
        );
    }

    // ③ 读侧（`/query` 的 format）——驱动取结果集走的就是这条
    println!("\n③ 读（SELECT * FROM t_rows，{rows} 行）");
    for fmt in fmts {
        let body = serde_json::json!({ "sql": "SELECT * FROM t_rows", "format": fmt }).to_string();
        let t0 = Instant::now();
        let (st, raw) = post_body(&addr, "/api/v1/query", "application/json", body.as_bytes());
        println!(
            "  format={fmt:<5} 耗时{:.3}s body={:>7.1}MB（{:.2}M 行/s）status={st}{}",
            t0.elapsed().as_secs_f64(),
            raw.len() as f64 / 1e6,
            rows as f64 / t0.elapsed().as_secs_f64() / 1e6,
            if st == 200 { "" } else { " ← 失败" }
        );
    }
}
