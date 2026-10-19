//! HTTP 契约回归测试（本仓首个 HTTP 层测试）。
//!
//! 起**真** `qsdb server` 子进程（`CARGO_BIN_EXE_qsdb`），用手写 HTTP/1.1 打真实端点
//! ——不引 HTTP 客户端依赖（同 `qsdb-bench/src/bin/bench_server.rs` 的做法）。
//!
//! 覆盖的是**协议面契约**，即"驱动/外部工具依赖的稳定行为"：
//! - J11：`X-QSDB-Query-Id` 透传（采用 / 非法 400 / 冲突回退 / 精确取消）
//! - 错误体恒为 `{error, code}`（含框架层抽取失败——此前是纯文本）
//! - 请求严格化：未知 body 字段、非法的 format / mode / withSchema 值都显式报错
//! - 并发闸的专属 code `TOO_MANY_QUERIES`（与只读 503 区分）
//! - `PUT /api/v1/config` 的键形态与 `applied` 诚实性
//!
//! 慢查询用非等值 join（`CrossJoinFilter` 嵌套循环，流式 count：不吃执行内存，
//! 天然长查询，用来制造并发窗口）。

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Date32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

// ---------------------------------------------------------------------------
// 测试用 server 进程 + 手写 HTTP 客户端
// ---------------------------------------------------------------------------

/// 起一个真 server；Drop 时杀进程 + 清目录。
struct Server {
    child: Child,
    port: u16,
    root: std::path::PathBuf,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

impl Server {
    fn start(extra_args: &[&str]) -> Server {
        // 每个 server 独立目录：进程内自增序号（`Instant::now().elapsed()` 恒 ≈0，
        // 会让并行跑的测试共用一个目录，互相删——踩过）
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let uniq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let root =
            std::env::temp_dir().join(format!("qsdb_http_test_{}_{}", std::process::id(), uniq));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("mkdir temp root");
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_qsdb"));
        cmd.args(["server", "--path"])
            .arg(&root)
            .args(["--addr", "127.0.0.1:0"])
            .args(extra_args)
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        let mut child = cmd.spawn().expect("spawn qsdb server");
        // 从 stderr 抓实际绑定端口（`--addr :0` 会回显真实端口）
        let mut stderr = child.stderr.take().expect("stderr piped");
        let mut buf = Vec::new();
        let deadline = Instant::now() + Duration::from_secs(30);
        let port = loop {
            if Instant::now() > deadline {
                let _ = child.kill();
                panic!("server 未在 30s 内就绪: {}", String::from_utf8_lossy(&buf));
            }
            let mut chunk = [0u8; 512];
            match stderr.read(&mut chunk) {
                Ok(0) | Err(_) => {
                    let _ = child.kill();
                    panic!("server 提前退出: {}", String::from_utf8_lossy(&buf));
                }
                Ok(n) => {
                    buf.extend_from_slice(&chunk[..n]);
                    let text = String::from_utf8_lossy(&buf);
                    if let Some(p) = parse_listening_port(&text) {
                        break p;
                    }
                }
            }
        };
        Server { child, port, root }
    }
}

/// 从 `qsdb server listening on http://127.0.0.1:PORT …` 抓端口。
fn parse_listening_port(text: &str) -> Option<u16> {
    let idx = text.find("listening on http://127.0.0.1:")?;
    let rest = &text[idx + "listening on http://127.0.0.1:".len()..];
    let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
    digits.parse().ok()
}

/// 响应：状态码 + 头（小写名）+ body 字节。
struct Resp {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl Resp {
    fn header(&self, name: &str) -> Option<&str> {
        let name = name.to_ascii_lowercase();
        self.headers
            .iter()
            .find(|(k, _)| *k == name)
            .map(|(_, v)| v.as_str())
    }
    fn json(&self) -> serde_json::Value {
        serde_json::from_slice(&self.body).unwrap_or_else(|e| {
            panic!(
                "响应不是 JSON（{}）: {}",
                e,
                String::from_utf8_lossy(&self.body)
            )
        })
    }
    fn code(&self) -> String {
        self.json()
            .get("code")
            .and_then(|c| c.as_str())
            .unwrap_or("<no code>")
            .to_string()
    }
}

/// 手写 HTTP/1.1（`Connection: close` → 读到 EOF）。
fn http(port: u16, method: &str, path: &str, headers: &[(&str, &str)], body: &[u8]) -> Resp {
    let mut s = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    s.set_read_timeout(Some(Duration::from_secs(120))).unwrap();
    let mut req = format!("{method} {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n");
    req.push_str(&format!("Content-Length: {}\r\n", body.len()));
    for (k, v) in headers {
        req.push_str(&format!("{k}: {v}\r\n"));
    }
    req.push_str("\r\n");
    if std::env::var("QSDB_HTTP_DEBUG").is_ok() {
        eprintln!("--- REQ ---\n{req}{}", String::from_utf8_lossy(body));
    }
    s.write_all(req.as_bytes()).unwrap();
    s.write_all(body).unwrap();
    let mut raw = Vec::new();
    s.read_to_end(&mut raw).unwrap();

    let sep = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("no header/body separator");
    let head = String::from_utf8_lossy(&raw[..sep]).to_string();
    let mut lines = head.lines();
    let status: u16 = lines
        .next()
        .and_then(|l| l.split(' ').nth(1))
        .and_then(|c| c.parse().ok())
        .unwrap_or(0);
    let headers: Vec<(String, String)> = lines
        .filter_map(|l| l.split_once(':'))
        .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_string()))
        .collect();
    let rest = raw[sep + 4..].to_vec();
    let chunked = headers
        .iter()
        .any(|(k, v)| k == "transfer-encoding" && v.contains("chunked"));
    let body = if chunked { dechunk(&rest) } else { rest };
    Resp {
        status,
        headers,
        body,
    }
}

/// 极简 chunked 解码（JSON 响应一般非 chunked，但别让测试因编码形式假失败）。
fn dechunk(mut b: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    while let Some(pos) = b.windows(2).position(|w| w == b"\r\n") {
        let size_str = String::from_utf8_lossy(&b[..pos]);
        let Ok(size) = usize::from_str_radix(size_str.split(';').next().unwrap_or("0").trim(), 16)
        else {
            break;
        };
        if size == 0 {
            break;
        }
        let start = pos + 2;
        out.extend_from_slice(&b[start..start + size]);
        b = &b[start + size + 2..];
    }
    out
}

fn query(port: u16, sql: &str, qid: Option<&str>) -> Resp {
    let mut h = vec![("Content-Type", "application/json")];
    if let Some(id) = qid {
        h.push(("X-QSDB-Query-Id", id));
    }
    let body = serde_json::json!({ "sql": sql }).to_string();
    http(port, "POST", "/api/v1/query", &h, body.as_bytes())
}

/// 带任意鉴权头的查询（会话契约用）。
fn query_auth(port: u16, sql: &str, qid: Option<&str>, auth: &[(&str, &str)]) -> Resp {
    let mut h: Vec<(&str, &str)> = vec![("Content-Type", "application/json")];
    h.extend_from_slice(auth);
    if let Some(id) = qid {
        h.push(("X-QSDB-Query-Id", id));
    }
    let body = serde_json::json!({ "sql": sql }).to_string();
    http(port, "POST", "/api/v1/query", &h, body.as_bytes())
}

/// Basic 登录 → token + sessionId（M-A）。
fn login(port: u16, user: &str, pass: &str) -> Resp {
    use base64::Engine;
    let b64 = base64::engine::general_purpose::STANDARD.encode(format!("{user}:{pass}"));
    let auth = format!("Basic {b64}");
    http(
        port,
        "POST",
        "/api/v1/auth/login",
        &[("Authorization", auth.as_str())],
        b"",
    )
}

/// 带 params 的查询（协议面：`{"sql": …, "params": [...]}`）。
fn query_params(port: u16, sql: &str, params: serde_json::Value) -> Resp {
    let h = vec![("Content-Type", "application/json")];
    let body = serde_json::json!({ "sql": sql, "params": params }).to_string();
    http(port, "POST", "/api/v1/query", &h, body.as_bytes())
}

/// Arrow IPC 流：两列 Int64（nullable，与 DDL 默认一致）。
fn ipc_two_int_cols(n: usize) -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Int64, true),
    ]));
    let vals: Vec<i64> = (0..n as i64).collect();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vals.clone())),
            Arc::new(Int64Array::from(vals)),
        ],
    )
    .expect("build batch");
    let mut buf = Vec::new();
    {
        let mut w =
            arrow::ipc::writer::StreamWriter::try_new(&mut buf, &schema).expect("ipc writer");
        w.write(&batch).expect("write batch");
        w.finish().expect("finish ipc");
    }
    buf
}

// ---------------------------------------------------------------------------
// 参数化查询（协议面）
// ---------------------------------------------------------------------------

/// `params` 是**新加的可选字段**：缺省行为与从前一字不差；给了就按位置绑定。
///
/// 这里只钉**协议面**（字段名、错误码、值语义）；绑定语义的逐值验收在
/// `qsdb/tests/params.rs`。注入那条也在这里再走一遍 HTTP 全程 —— 值的模样
/// 经过 JSON 编解码之后**仍然只是数据**。
#[test]
fn params_bind_over_http() {
    let srv = Server::start(&[]);
    let p = srv.port;
    assert_eq!(
        query(p, "CREATE TABLE t (g BIGINT, s STRING)", None).status,
        200
    );
    assert_eq!(
        query(p, "INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'c')", None).status,
        200
    );

    // ① 不给 params 的老请求：**一字不差**（回归护栏）
    let r = query(p, "SELECT count(*) FROM t", None);
    assert_eq!(r.status, 200);
    assert_eq!(r.json()["rows"][0][0], 3);

    // ② `$n` 与 `?` 都认
    let r = query_params(p, "SELECT s FROM t WHERE g = $1", serde_json::json!([2]));
    assert_eq!(r.status, 200);
    assert_eq!(r.json()["rows"][0][0], "b");
    let r = query_params(p, "SELECT s FROM t WHERE g = ?", serde_json::json!([3]));
    assert_eq!(r.status, 200);
    assert_eq!(r.json()["rows"][0][0], "c");

    // ③ **注入**：值经过 JSON 往返仍只是数据 —— 整串原样进表、原样回来
    let evil = "'; DROP TABLE t; --";
    let r = query_params(p, "INSERT INTO t VALUES (9, $1)", serde_json::json!([evil]));
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
    let r = query_params(p, "SELECT s FROM t WHERE g = $1", serde_json::json!([9]));
    assert_eq!(r.json()["rows"][0][0], evil);
    // 表还在（`DROP TABLE t` 没有被执行）
    let r = query(p, "SELECT count(*) FROM t", None);
    assert_eq!(r.json()["rows"][0][0], 4);

    // ④ 个数对不上 ⇒ 400（不是 200 + 错值，也不是 500）
    let r = query_params(
        p,
        "SELECT * FROM t WHERE g = $1 AND g = $2",
        serde_json::json!([1]),
    );
    assert_eq!(r.status, 400);
    assert!(r.json()["error"].as_str().unwrap().contains("参数不够"));
    let r = query_params(p, "SELECT * FROM t WHERE g = $1", serde_json::json!([1, 2]));
    assert_eq!(r.status, 400);
    assert!(r.json()["error"].as_str().unwrap().contains("参数多了"));

    // ⑤ `params` 不是数组 ⇒ 400（**类型不对就报错**，不静默忽略）
    let r = query_params(p, "SELECT 1", serde_json::json!({"a": 1}));
    assert_eq!(r.status, 400);
    assert!(r.json()["error"].as_str().unwrap().contains("必须是数组"));

    // ⑥ `params` 现在是被**认识**的字段 —— 不能再落到 deny_unknown_fields 上
    let r = query_params(p, "SELECT 1", serde_json::json!([]));
    assert_eq!(r.status, 200);
}

// ---------------------------------------------------------------------------
// 提交事件订阅（T2）
// ---------------------------------------------------------------------------

/// `GET /api/v1/watch`：**增量同步的 push 源**。
///
/// 这里钉协议面 + **一条最容易做错的语义**：游标接不上时报 `CURSOR_TOO_OLD`，
/// **绝不**假装连续 —— 消费方据此去对账，而不是永远等一个不会到来的序号。
#[test]
fn watch_pushes_commit_events() {
    let srv = Server::start(&[]);
    let p = srv.port;
    assert_eq!(
        query(
            p,
            "CREATE TABLE t (dt STRING, id BIGINT) PARTITIONED BY (dt)",
            None
        )
        .status,
        200
    );

    // ① 首次订阅（不带 since）：**从现在开始**，立刻回一个空批次 + 游标
    let r = http(p, "GET", "/api/v1/watch?wait=1", &[], &[]);
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
    assert_eq!(r.json()["events"].as_array().unwrap().len(), 0);
    let cursor = r.json()["next"].as_u64().unwrap();

    // ② 写两个分区 ⇒ 用刚才的游标能拿到**两条**分区级事件
    assert_eq!(
        query(
            p,
            "INSERT INTO t VALUES ('2026-01-01', 1), ('2026-01-02', 2)",
            None
        )
        .status,
        200
    );
    let r = http(
        p,
        "GET",
        &format!("/api/v1/watch?since={cursor}&wait=1"),
        &[],
        &[],
    );
    assert_eq!(r.status, 200);
    let evs = r.json()["events"].as_array().unwrap().clone();
    assert_eq!(evs.len(), 2, "两个分区 ⇒ 两条事件：{evs:?}");
    for e in &evs {
        assert_eq!(e["table"], "t");
        assert_eq!(e["kind"], "Append");
        assert!(e["generation"].as_u64().unwrap() > 0);
        assert_eq!(e["partition"].as_array().unwrap().len(), 1);
    }
    // 分区值用**目录名那套编码**（消费方靠它定位分区）
    let mut parts: Vec<String> = evs
        .iter()
        .map(|e| e["partition"][0].as_str().unwrap().to_string())
        .collect();
    parts.sort();
    assert_eq!(parts, vec!["2026-01-01", "2026-01-02"]);
    let next = r.json()["next"].as_u64().unwrap();
    assert!(next > cursor);

    // ③ 追平后再订阅：空批次（不重复吐）
    let r = http(
        p,
        "GET",
        &format!("/api/v1/watch?since={next}&wait=1"),
        &[],
        &[],
    );
    assert_eq!(r.json()["events"].as_array().unwrap().len(), 0);

    // ④ ★ 游标太新（服务端重启过 / 换了实例）⇒ 409 CURSOR_TOO_OLD，**不是**静静等
    let r = http(p, "GET", "/api/v1/watch?since=999999&wait=1", &[], &[]);
    assert_eq!(r.status, 409);
    assert_eq!(r.code(), "CURSOR_TOO_OLD");
}

/// **无依赖批量通道**（`format=bulk` / `format=csv`，读**与**写两个方向）。
///
/// 动机：生态工具（尤其 Java）不想引 arrow-java（JDK 11+ + FlatBuffers/Jackson），
/// 于是拿不到 IPC 快通道、只能逐条发 SQL（实测 5M 行 81s vs 二进制 2.18s）。
/// 见 `docs/bulk-format.md`。
#[test]
fn bulk_and_csv_channels_both_directions() {
    let srv = Server::start(&[]);
    let p = srv.port;
    assert_eq!(
        query(p, "CREATE TABLE t (a BIGINT, s STRING)", None).status,
        200
    );

    // ① 写：bulk（列式二进制）
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("s", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])),
            Arc::new(StringArray::from(vec![Some("x"), Some("y"), None])),
        ],
    )
    .unwrap();
    let bulk = qsdb::bulk::encode(&schema, &[batch]).unwrap();
    let r = http(
        p,
        "POST",
        "/api/v1/write?table=t&mode=append&format=bulk",
        &[("Content-Type", "application/vnd.qsdb.bulk")],
        &bulk,
    );
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));

    // ② 读：bulk 往返（含 NULL 保真）
    let q = |sql: &str, fmt: &str| {
        let body = serde_json::json!({"sql": sql, "format": fmt}).to_string();
        http(
            p,
            "POST",
            "/api/v1/query",
            &[("Content-Type", "application/json")],
            body.as_bytes(),
        )
    };
    let r = q("SELECT a, s FROM t ORDER BY a", "bulk");
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
    assert_eq!(r.header("content-type"), Some("application/vnd.qsdb.bulk"));
    let back = qsdb::bulk::decode(&r.body).unwrap();
    assert_eq!(back.num_rows(), 3);
    let disp = |b: &RecordBatch, c: usize| -> Vec<String> {
        (0..b.num_rows())
            .map(|i| {
                if b.column(c).is_null(i) {
                    "NULL".into()
                } else {
                    arrow::util::display::array_value_to_string(b.column(c), i).unwrap()
                }
            })
            .collect()
    };
    assert_eq!(disp(&back, 0), vec!["1", "3", "NULL"]);
    assert_eq!(disp(&back, 1), vec!["x", "NULL", "y"]);

    // ③ 读：csv（文本，零依赖）
    let r = q("SELECT a, s FROM t ORDER BY a", "csv");
    assert_eq!(r.status, 200);
    assert_eq!(r.header("content-type"), Some("text/csv"));
    let text = String::from_utf8_lossy(&r.body).to_string();
    assert!(text.starts_with("a,s"), "首行应为表头，实得 {text:?}");

    // ④ 写：csv（列序 = 表列序；空字段 = NULL；不做类型推断）
    let r = http(
        p,
        "POST",
        "/api/v1/write?table=t&mode=append&format=csv",
        &[("Content-Type", "text/csv")],
        b"a,s
7,seven
8,
",
    );
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
    let r = query(p, "SELECT count(*) FROM t", None);
    assert_eq!(r.json()["rows"][0][0], 5, "应共 5 行（3 + 2）");

    // ⑤ 未知 format → 400（显式拒绝，不静默兜底）
    let r = http(
        p,
        "POST",
        "/api/v1/write?table=t&mode=append&format=arrow",
        &[],
        b"",
    );
    assert_eq!(r.status, 400);
    assert_eq!(r.code(), "INVALID_ARGUMENT");
}

/// **行式通道**（`format=rows`，读**与**写两个方向）——见 `docs/rows-format.md`。
///
/// 写侧的 body 是**照着规范手写**的（不调用内核的 `encode`）：这既是契约测试，
/// 也是"第三方照文档能不能实现"的实证——驱动侧要做的正是这件事。
#[test]
fn rows_channel_both_directions() {
    let srv = Server::start(&[]);
    let p = srv.port;
    assert_eq!(
        query(p, "CREATE TABLE t (id BIGINT, name STRING, dt DATE)", None).status,
        200
    );

    // ① 写：两个块（2 行含 NULL + 1 行无 NULL），flags=0（schema 由表定义给）
    let body = rows_body_without_schema();
    let r = http(
        p,
        "POST",
        "/api/v1/write?table=t&mode=append&format=rows",
        &[("Content-Type", "application/vnd.qsdb.rows")],
        &body,
    );
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));

    // ② 读：rows 往返（列名/类型随头一起发；NULL 保真）
    let q = |sql: &str, fmt: &str| {
        let body = serde_json::json!({"sql": sql, "format": fmt}).to_string();
        http(
            p,
            "POST",
            "/api/v1/query",
            &[("Content-Type", "application/json")],
            body.as_bytes(),
        )
    };
    let r = q("SELECT id, name, dt FROM t ORDER BY id", "rows");
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
    assert_eq!(r.header("content-type"), Some("application/vnd.qsdb.rows"));
    let back = qsdb::rows::decode(&r.body).unwrap();
    let all: Vec<&RecordBatch> = back.iter().collect();
    let rows: usize = all.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 3, "两块合计 3 行");
    let disp = |c: usize| -> Vec<String> {
        all.iter()
            .flat_map(|b| {
                (0..b.num_rows()).map(move |i| {
                    if b.column(c).is_null(i) {
                        "NULL".into()
                    } else {
                        arrow::util::display::array_value_to_string(b.column(c), i).unwrap()
                    }
                })
            })
            .collect()
    };
    assert_eq!(disp(0), vec!["1", "3", "NULL"]);
    assert_eq!(disp(1), vec!["alice", "中文", "NULL"]);
    assert_eq!(disp(2), vec!["2022-01-08", "2022-01-09", "NULL"]);

    // ③ 自描述负载喂给 /write → 400（写侧 schema 由端点给，不静默忽略多余的头）
    let r = http(
        p,
        "POST",
        "/api/v1/write?table=t&mode=append&format=rows",
        &[],
        &r.body,
    );
    assert_eq!(r.status, 400);
    assert_eq!(r.code(), "INVALID_ARGUMENT");
    assert!(
        String::from_utf8_lossy(&r.body).contains("schema block"),
        "{}",
        String::from_utf8_lossy(&r.body)
    );

    // ④ 列数与表不符 → 400（不按前缀凑合）
    let narrow = {
        let mut b = b"QSDBROWS".to_vec();
        b.extend_from_slice(&1u16.to_le_bytes());
        b.push(0);
        b.extend_from_slice(&2u16.to_le_bytes()); // 表有 3 列
        b.extend_from_slice(&0u32.to_le_bytes());
        b.push(0);
        b
    };
    let r = http(
        p,
        "POST",
        "/api/v1/write?table=t&mode=append&format=rows",
        &[],
        &narrow,
    );
    assert_eq!(r.status, 400, "{}", String::from_utf8_lossy(&r.body));
}

/// 照 `docs/rows-format.md` **手写**的 rows 负载（flags=0，两个块）。
///
/// 列：`id BIGINT`(tag 3/8B) / `name STRING`(tag 5/变长) / `dt DATE`(tag 6/4B)。
fn rows_body_without_schema() -> Vec<u8> {
    let mut b: Vec<u8> = Vec::new();
    b.extend_from_slice(b"QSDBROWS"); // magic
    b.extend_from_slice(&1u16.to_le_bytes()); // version
    b.push(0); // flags：无列描述（写侧 schema 由端点给）
    b.extend_from_slice(&3u16.to_le_bytes()); // n_cols

    // 块 1：2 行，第 1 行整行 NULL
    b.extend_from_slice(&2u32.to_le_bytes()); // row_count
    b.push(1); // chunk_flags：HAS_NULLS
    b.extend_from_slice(&[0b10, 0b10, 0b10]); // 每列 1 字节位图，第 1 行置位
    b.extend_from_slice(&1i64.to_le_bytes()); // id
    b.extend_from_slice(&5u32.to_le_bytes()); // name 长度
    b.extend_from_slice(b"alice");
    b.extend_from_slice(&19_000i32.to_le_bytes()); // dt
    b.extend_from_slice(&0i64.to_le_bytes()); // NULL 行占位
    b.extend_from_slice(&0u32.to_le_bytes()); // NULL 变长 = 空
    b.extend_from_slice(&0i32.to_le_bytes());

    // 块 2：1 行，无 NULL（多块 = 驱动攒够一拨就发）
    b.extend_from_slice(&1u32.to_le_bytes());
    b.push(0); // 无位图
    b.extend_from_slice(&3i64.to_le_bytes());
    b.extend_from_slice(&6u32.to_le_bytes()); // "中文" = 6 字节 UTF-8
    b.extend_from_slice("中文".as_bytes());
    b.extend_from_slice(&19_001i32.to_le_bytes());
    b
}

/// **F11：物理复制的段下载端点**（`GET /api/v1/files`）。
///
/// 这条端点此前**一个测试都没有**——正是"契约文档写可用、却没有东西钉着"的形态
/// （同一个坑刚在 HTTP overwrite 上踩过）。这里补上：字节级往返（含跨多个读块的
/// 大文件）、路径白名单、目录与缺失的 404。
///
/// 流式（qsdb-sync 回灌项）不在此处断言内存——**用"大文件能逐字节拿全"间接证明
/// 分块读没把内容搞坏**；内存本身由实现决定（`ReaderStream` 按块读，不再整文件载入）。
#[test]
fn file_download_roundtrip_and_whitelist() {
    let srv = Server::start(&[]);
    let p = srv.port;
    assert_eq!(
        query(p, "CREATE TABLE t (a BIGINT, s STRING)", None).status,
        200
    );
    assert_eq!(
        query(p, "INSERT INTO t VALUES (1, 'x'), (2, 'y')", None).status,
        200
    );

    let get = |path: &str| http(p, "GET", &format!("/api/v1/files?path={path}"), &[], b"");

    // ① 真段文件/manifest：与磁盘逐字节一致
    let manifest = srv.root.join("default").join("t").join("manifest.postcard");
    assert!(manifest.exists(), "段/manifest 应已落盘: {manifest:?}");
    let on_disk = std::fs::read(&manifest).unwrap();
    let r = get("default/t/manifest.postcard");
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
    assert_eq!(r.header("content-type"), Some("application/octet-stream"));
    assert_eq!(r.body, on_disk, "下载内容必须与磁盘逐字节一致");

    // ② 跨多个读块的大文件（8KB 一块 → 5MB 要读 600+ 次）
    let mut big = vec![0u8; 5 * 1024 * 1024];
    for (i, b) in big.iter_mut().enumerate() {
        *b = (i % 251) as u8;
    }
    std::fs::write(srv.root.join("big.bin"), &big).unwrap();
    let r = get("big.bin");
    assert_eq!(r.status, 200);
    assert_eq!(r.body.len(), big.len(), "长度必须完整");
    assert!(
        r.body == big,
        "大文件内容必须逐字节一致（分块读不能错位/截断）"
    );

    // ③ 路径白名单（显式拒绝，不静默兜底）
    assert_eq!(get("..%2FCargo.toml").status, 400, "上跳必须 400");
    assert_eq!(get("/etc/passwd").status, 400, "绝对路径必须 400");
    assert_eq!(get("_trash/x").status, 403, "内部目录必须 403");
    assert_eq!(get("_spill/seg").status, 403, "内部目录必须 403");

    // ④ 目录 → 404（**不能**变成"截断的 200"：Linux 上 File::open 能打开目录）
    assert_eq!(get("default").status, 404);
    // ⑤ 不存在的文件 → 404
    assert_eq!(get("default/t/nope.parquet").status, 404);
}

/// **J15：零行结果在每种 format 下都要读得回来**（`WHERE false` / `LIMIT 0`）。
///
/// 这是 BI 工具探测元数据的标准姿势（DBeaver 之类先 `LIMIT 0` 拿列名/类型）。
/// 原状：`rows` 与 `bulk` 的编码器以"首批"取 schema，零批就报 `empty batches`
/// → 501；`csv` 更阴——返回 200 但 **0 字节 body**（arrow 的 CSV writer 在第一次
/// write 时才吐表头），客户端把列名丢了。三种都在这条用例里钉住。
#[test]
fn zero_row_result_survives_every_format() {
    let srv = Server::start(&[]);
    let p = srv.port;
    assert_eq!(
        query(p, "CREATE TABLE t (a BIGINT, s STRING)", None).status,
        200
    );
    assert_eq!(query(p, "INSERT INTO t VALUES (1, 'x')", None).status, 200);

    let q = |sql: &str, fmt: &str| {
        let body = serde_json::json!({"sql": sql, "format": fmt}).to_string();
        http(
            p,
            "POST",
            "/api/v1/query",
            &[("Content-Type", "application/json")],
            body.as_bytes(),
        )
    };
    for sql in [
        "SELECT a, s FROM t WHERE false",
        "SELECT a, s FROM t LIMIT 0",
    ] {
        // json：schema 在，rows 为空
        let r = q(sql, "json");
        assert_eq!(
            r.status,
            200,
            "{sql} json: {}",
            String::from_utf8_lossy(&r.body)
        );
        let v = r.json();
        assert_eq!(v["rowCount"], 0);
        assert_eq!(
            v["schema"][0]["name"], "a",
            "列名仍要透出（探测元数据就靠它）"
        );
        assert!(v["rows"].as_array().unwrap().is_empty());

        // ipc：schema 消息必在；**批可有可无，但读了必须是零行**。
        // 2026-10-13：库层零行结果改为携带一个空批（schema 协商契约，见
        // `tests/agg_contract.rs`），ipc 流随之从"零个批"变成"一个空批"——
        // 两者都是合法流、都读不出行，故断言落在**行数**上而不是批数上。
        let r = q(sql, "ipc");
        assert_eq!(r.status, 200);
        let mut rd = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(r.body), None)
            .expect("ipc stream 应能解析（零行也是合法流）");
        assert_eq!(rd.schema().field(0).name(), "a");
        let mut read_rows = 0usize;
        for b in rd.by_ref() {
            read_rows += b.expect("零行批应可解").num_rows();
        }
        assert_eq!(read_rows, 0, "ipc 流读出来必须是零行");

        // bulk：列描述照写、n_rows = 0
        let r = q(sql, "bulk");
        assert_eq!(
            r.status,
            200,
            "{sql} bulk: {}",
            String::from_utf8_lossy(&r.body)
        );
        let back = qsdb::bulk::decode(&r.body).expect("零行 bulk 应可解码");
        assert_eq!(back.num_rows(), 0);
        assert_eq!(back.schema().field(0).name(), "a");

        // rows：头 + 零个块
        let r = q(sql, "rows");
        assert_eq!(
            r.status,
            200,
            "{sql} rows: {}",
            String::from_utf8_lossy(&r.body)
        );
        let back = qsdb::rows::decode(&r.body).expect("零行 rows 应可解码");
        let total: usize = back.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 0);
        // 列描述从**头**里读（没有块可依）
        assert!(
            !r.body.is_empty() && r.body.len() > 13,
            "头里必须带列描述（否则客户端拿不到列名）"
        );

        // csv：至少要给表头（否则列名丢了）
        let r = q(sql, "csv");
        assert_eq!(r.status, 200);
        assert_eq!(
            String::from_utf8_lossy(&r.body).trim(),
            "a,s",
            "零行也要表头"
        );
    }
}

/// **J12**：DML 回执要有**结构化字段**（`command` + `rowsAffected`），
/// 驱动不该解析 `message` 文案（码表文档明确"文案不做兼容承诺"）。
#[test]
fn dml_receipt_has_structured_fields() {
    let srv = Server::start(&[]);
    let p = srv.port;
    assert_eq!(
        query(p, "CREATE TABLE t (a BIGINT, b STRING)", None).status,
        200
    );

    let r = query(p, "INSERT INTO t VALUES (1,'x'),(2,'y'),(3,'z')", None);
    assert_eq!(r.status, 200);
    assert_eq!(r.json()["command"], "INSERT");
    assert_eq!(r.json()["rowsAffected"], 3);
    assert!(r.json()["message"].is_string(), "人读文案仍要保留");

    // DELETE：**只支持分区键谓词**（行级删除未做 → 501，已知边界）
    assert_eq!(
        query(
            p,
            "CREATE TABLE pt (dt DATE, v BIGINT) PARTITIONED BY (dt)",
            None
        )
        .status,
        200
    );
    assert_eq!(
        query(
            p,
            "INSERT INTO pt VALUES (DATE '2026-01-01', 1), (DATE '2026-01-02', 2)",
            None
        )
        .status,
        200
    );
    let r = query(p, "DELETE FROM pt WHERE dt = DATE '2026-01-01'", None);
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
    assert_eq!(r.json()["command"], "DELETE");
    assert_eq!(r.json()["rowsAffected"], 1);
    // 非分区键谓词 → 501（行级删除未做）
    assert_eq!(query(p, "DELETE FROM pt WHERE v = 1", None).status, 501);

    // DDL 无行数：不出现这两个字段（不是 0——"没有行数概念"与"影响 0 行"不同）
    let r = query(p, "CREATE TABLE t2 (a BIGINT)", None);
    assert_eq!(r.status, 200);
    assert!(
        r.json().get("rowsAffected").is_none(),
        "DDL 不该有 rowsAffected"
    );
    assert!(r.json().get("command").is_none());
}

/// **回归**：HTTP `mode=overwrite` 本身要能用。
///
/// 长期存在的 bug：该分支先把批按分区**剥**好，再交给 `Qsdb::overwrite_partition`
/// （而它的输入约定是**全列批**、库里自己剥）→ 库里再剥一次 → 400
/// `batch lacks partition column`。embed 用户传全列批，所以从未暴露；
/// **HTTP overwrite 一直是坏的**，而契约文档写着它可用。
#[test]
fn http_overwrite_mode_works() {
    let srv = Server::start(&[]);
    let p = srv.port;
    assert_eq!(
        query(
            p,
            "CREATE TABLE t (dt DATE, v BIGINT) PARTITIONED BY (dt)",
            None
        )
        .status,
        200
    );

    // 覆盖两次：第二次必须**替换**第一次（分区语义），不是追加
    for v in [1, 2] {
        let r = http(
            p,
            "POST",
            "/api/v1/write?table=t&mode=overwrite",
            &[("Content-Type", "application/octet-stream")],
            &ipc_dt_v(20_454, v),
        );
        assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
    }
    let r = query(p, "SELECT count(*), sum(v) FROM t", None);
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
    let json = r.json();
    assert_eq!(
        json["rows"][0].to_string(),
        "[1,2]",
        "overwrite 应是替换语义（1 行、v=2）"
    );
}

/// **F24 HTTP 面**：`mode=overwrite&expected_generation=N` 的 CAS 语义与拒绝边界。
#[test]
fn write_cas_over_http() {
    let srv = Server::start(&[]);
    let p = srv.port;
    let q = |sql: &str| query(p, sql, None);
    assert_eq!(
        q("CREATE TABLE t (dt DATE, v BIGINT) PARTITIONED BY (dt)").status,
        200
    );

    // 单分区覆盖（全列批：dt + v）——期望 0 = 首次写入
    let body = ipc_dt_v(20_454, 1);
    {
        // 自检：IPC 往返后 schema 必须仍带 dt（否则测的是别的毛病）
        let mut r =
            arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(body.clone()), None)
                .unwrap();
        let b = r.next().unwrap().unwrap();
        eprintln!("SELFTEST schema={:?}", b.schema());
    }
    let r = http(
        p,
        "POST",
        "/api/v1/write?table=t&mode=overwrite&expected_generation=0",
        &[("Content-Type", "application/octet-stream")],
        &body,
    );
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));

    // 同样的过期期望（分区已是 generation 1）→ 409 WRITE_CONFLICT
    let r = http(
        p,
        "POST",
        "/api/v1/write?table=t&mode=overwrite&expected_generation=0",
        &[("Content-Type", "application/octet-stream")],
        &body,
    );
    assert_eq!(r.status, 409, "{}", String::from_utf8_lossy(&r.body));
    assert_eq!(r.code(), "WRITE_CONFLICT");
    // SQLState 不在**错误响应体**里（响应体只带 error+code；驱动从
    // `GET /api/v1/error-codes` 的目录取映射）——这里断言目录里的映射正确
    let cat = http(p, "GET", "/api/v1/error-codes", &[], &[]);
    let entry = cat.json()["codes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["code"] == "WRITE_CONFLICT")
        .cloned()
        .expect("目录应含 WRITE_CONFLICT");
    assert_eq!(entry["http"], 409);
    assert_eq!(entry["retry"], false);
    assert_eq!(entry["category"], "resource");
    assert_eq!(entry["sqlState"], "40001");

    // 取最新 generation 再写 → 200
    let r = query(p, "SELECT 1", None);
    assert_eq!(r.status, 200);
    let parts = http(p, "GET", "/api/v1/tables/t/partitions", &[], &[]);
    let g = parts.json()["detail"][0]["generation"].as_u64().unwrap();
    let r = http(
        p,
        "POST",
        &format!("/api/v1/write?table=t&mode=overwrite&expected_generation={g}"),
        &[("Content-Type", "application/octet-stream")],
        &body,
    );
    assert_eq!(r.status, 200, "拿最新 generation 应成功");

    // append 带 CAS → 400（append 是锁内合并语义，没有"期望 generation"这回事）
    let r = http(
        p,
        "POST",
        "/api/v1/write?table=t&mode=append&expected_generation=1",
        &[("Content-Type", "application/octet-stream")],
        &ipc_dt_v(20_454, 2),
    );
    assert_eq!(r.status, 400, "{}", String::from_utf8_lossy(&r.body));
    assert_eq!(r.code(), "INVALID_ARGUMENT");

    // 一次命中多分区 + CAS → 400（"期望 generation"没有唯一所指，不许挑一个用）
    let r = http(
        p,
        "POST",
        "/api/v1/write?table=t&mode=overwrite&expected_generation=1",
        &[("Content-Type", "application/octet-stream")],
        &ipc_two_partitions(),
    );
    assert_eq!(r.status, 400, "{}", String::from_utf8_lossy(&r.body));
    assert_eq!(r.code(), "INVALID_ARGUMENT");
}

/// 单分区全列批（dt + v）。
fn ipc_dt_v(dt: i32, v: i64) -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("dt", DataType::Date32, true),
        Field::new("v", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Date32Array::from(vec![dt])),
            Arc::new(Int64Array::from(vec![v])),
        ],
    )
    .expect("build batch");
    let mut buf = Vec::new();
    {
        let mut w =
            arrow::ipc::writer::StreamWriter::try_new(&mut buf, &schema).expect("ipc writer");
        w.write(&batch).expect("write batch");
        w.finish().expect("finish ipc");
    }
    buf
}

/// 两个分区的批（dt 两个值）。
fn ipc_two_partitions() -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("dt", DataType::Date32, true),
        Field::new("v", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Date32Array::from(vec![20_454, 20_455])),
            Arc::new(Int64Array::from(vec![1, 2])),
        ],
    )
    .expect("build batch");
    let mut buf = Vec::new();
    {
        let mut w =
            arrow::ipc::writer::StreamWriter::try_new(&mut buf, &schema).expect("ipc writer");
        w.write(&batch).expect("write batch");
        w.finish().expect("finish ipc");
    }
    buf
}

/// 建表 + 灌 n 行；返回慢查询 SQL（非等值 join：n=3000 → 9e6 对 ≈ 1s 量级）。
fn setup_slow_table(port: u16, n: usize) -> String {
    let r = query(port, "CREATE TABLE t (a BIGINT, b BIGINT)", None);
    assert_eq!(
        r.status,
        200,
        "create table: {}",
        String::from_utf8_lossy(&r.body)
    );
    let r = http(
        port,
        "POST",
        "/api/v1/write?table=t&mode=append",
        &[("Content-Type", "application/octet-stream")],
        &ipc_two_int_cols(n),
    );
    assert_eq!(r.status, 200, "write: {}", String::from_utf8_lossy(&r.body));
    "SELECT count(*) FROM t a JOIN t b ON a.a > b.a".to_string()
}

// ---------------------------------------------------------------------------
// 1. 契约面：J11 采用/回显 + 请求严格化 + PUT /config 键形态
// ---------------------------------------------------------------------------

#[test]
fn j11_adoption_echo_and_request_strictness() {
    let srv = Server::start(&[]);
    let p = srv.port;

    // --- J11：客户端 id 被采用，且响应头 + body 都回显 ---
    let r = query(p, "SELECT 42 AS x", Some("drv-abc-1"));
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
    assert_eq!(
        r.header("x-qsdb-query-id"),
        Some("drv-abc-1"),
        "响应头应回显 id"
    );
    assert_eq!(
        r.json()["queryId"],
        "drv-abc-1",
        "body.queryId 应为客户端 id"
    );

    // 不带 header → 服务端生成 `q…`
    let r = query(p, "SELECT 1", None);
    let id = r.json()["queryId"].as_str().unwrap_or_default().to_string();
    assert!(id.starts_with('q'), "服务端生成的 id 应有 q 前缀: {id}");
    assert_eq!(
        r.header("x-qsdb-query-id"),
        Some(id.as_str()),
        "服务端 id 也要回显"
    );

    // --- J11：非法 id → 400 INVALID_ARGUMENT（不静默忽略） ---
    for bad in ["bad id!", "", &"x".repeat(65)] {
        let r = query(p, "SELECT 1", Some(bad));
        assert_eq!(r.status, 400, "非法 id {bad:?} 应 400");
        assert_eq!(r.code(), "INVALID_ARGUMENT", "非法 id {bad:?}");
    }

    // --- 框架层抽取失败 → 结构化错误体（此前是纯文本，无 code） ---
    let r = http(
        p,
        "POST",
        "/api/v1/query",
        &[("Content-Type", "application/json")],
        b"{not json",
    );
    assert_eq!(r.status, 400, "非法 JSON 应 400");
    assert_eq!(r.code(), "INVALID_ARGUMENT", "非法 JSON 也要有 code");

    let r = http(
        p,
        "POST",
        "/api/v1/query",
        &[("Content-Type", "application/json")],
        br#"{"sql":123}"#,
    );
    assert_eq!(r.status, 400, "body 类型错应归一为 400");
    assert_eq!(r.code(), "INVALID_ARGUMENT");

    // --- 未知 body 字段（拼错键）→ 400，而不是静默忽略 ---
    let r = http(
        p,
        "POST",
        "/api/v1/query",
        &[("Content-Type", "application/json")],
        br#"{"sql":"SELECT 1","formatt":"ipc"}"#,
    );
    assert_eq!(r.status, 400, "拼错键应 400");
    assert_eq!(r.code(), "INVALID_ARGUMENT");

    // --- 值校验：format / mode / withSchema ---
    let r = http(
        p,
        "POST",
        "/api/v1/query",
        &[("Content-Type", "application/json")],
        br#"{"sql":"SELECT 1","format":"arrow"}"#,
    );
    assert_eq!(r.status, 400, "format=arrow 应 400");
    assert_eq!(r.code(), "INVALID_ARGUMENT");
    // 合法值仍工作
    let r = http(
        p,
        "POST",
        "/api/v1/query",
        &[("Content-Type", "application/json")],
        br#"{"sql":"SELECT 1 AS x","format":"ipc"}"#,
    );
    assert_eq!(r.status, 200);
    assert!(
        r.header("content-type").unwrap_or("").contains("arrow"),
        "format=ipc 应回 Arrow 流"
    );
    assert!(r.header("x-qsdb-query-id").is_some(), "IPC 通道也要回显 id");

    let r = http(p, "GET", "/api/v1/tables?withSchema=maybe", &[], &[]);
    assert_eq!(r.status, 400, "withSchema=maybe 应 400（此前静默当 false）");
    assert_eq!(r.code(), "INVALID_ARGUMENT");
    for ok in ["1", "true", "yes", "on", ""] {
        let r = http(
            p,
            "GET",
            &format!("/api/v1/tables?withSchema={ok}"),
            &[],
            &[],
        );
        assert_eq!(r.status, 200, "withSchema={ok:?} 应被接受");
        assert!(
            r.json().get("tables").is_some(),
            "withSchema=1 应回带列集形态"
        );
    }
    for no in ["0", "false", "no", "off"] {
        let r = http(
            p,
            "GET",
            &format!("/api/v1/tables?withSchema={no}"),
            &[],
            &[],
        );
        assert_eq!(r.status, 200, "withSchema={no:?} 应被接受（假）");
        assert!(r.json().get("tables").is_some());
    }

    let r = http(
        p,
        "POST",
        "/api/v1/write?table=t&mode=mangle",
        &[("Content-Type", "application/octet-stream")],
        b"x",
    );
    assert_eq!(r.status, 400, "mode=mangle 应 400");
    assert_eq!(r.code(), "INVALID_ARGUMENT");

    // 缺 query 参数 → 结构化 400（此前纯文本）
    let r = http(
        p,
        "POST",
        "/api/v1/write?mode=append",
        &[("Content-Type", "application/octet-stream")],
        b"x",
    );
    assert_eq!(r.status, 400);
    assert_eq!(r.code(), "INVALID_ARGUMENT");

    // --- PUT /api/v1/config：GET 的键形态（camelCase）必须能用 + applied 诚实 ---
    let r = http(
        p,
        "PUT",
        "/api/v1/config",
        &[("Content-Type", "application/json")],
        br#"{"queryTimeoutMs":12345}"#,
    );
    assert_eq!(r.status, 200);
    assert_eq!(r.json()["applied"], true, "camelCase 键应生效");
    assert_eq!(r.json()["queryTimeoutMs"], 12345);
    // 拼错键 → applied:false（不再假装成功）
    let r = http(
        p,
        "PUT",
        "/api/v1/config",
        &[("Content-Type", "application/json")],
        br#"{"queryTimeout":999}"#,
    );
    assert_eq!(r.status, 200);
    assert_eq!(r.json()["applied"], false, "拼错键应报 applied:false");
    assert_eq!(r.json()["queryTimeoutMs"], 12345, "值不该被改动");
    // snake_case 亦可（与 --config 文件同形）
    let r = http(
        p,
        "PUT",
        "/api/v1/config",
        &[("Content-Type", "application/json")],
        br#"{"query_timeout_ms":0}"#,
    );
    assert_eq!(r.json()["queryTimeoutMs"], 0);
}

// ---------------------------------------------------------------------------
// 2. J11 并发：同 id 冲突回退 + 按 id 精确取消
// ---------------------------------------------------------------------------

#[test]
fn j11_conflict_fallback_and_precise_cancel() {
    let srv = Server::start(&[]);
    let p = srv.port;
    let slow = setup_slow_table(p, 3000);

    // 两条**同 id** 并发：一条采用、另一条回退服务端生成（不报错、不顶掉在跑的）
    let mut handles = Vec::new();
    for _ in 0..2 {
        let sql = slow.clone();
        handles.push(std::thread::spawn(move || {
            query(p, &sql, Some("drv-race-1"))
        }));
    }
    let ids: Vec<String> = handles
        .into_iter()
        .map(|h| {
            let r = h.join().expect("thread");
            assert_eq!(
                r.status,
                200,
                "并发查询都应 200: {}",
                String::from_utf8_lossy(&r.body)
            );
            r.header("x-qsdb-query-id").unwrap_or_default().to_string()
        })
        .collect();
    assert_eq!(
        ids.iter().filter(|i| *i == "drv-race-1").count(),
        1,
        "恰好一条采用客户端 id: {ids:?}"
    );
    assert_eq!(
        ids.iter().filter(|i| i.starts_with('q')).count(),
        1,
        "另一条应回退服务端生成: {ids:?}"
    );

    // 按客户端 id 精确取消（零扫描）
    let sql = slow.clone();
    let run = std::thread::spawn(move || query(p, &sql, Some("drv-cancel-1")));
    std::thread::sleep(Duration::from_millis(400));
    let listed = http(p, "GET", "/api/v1/queries", &[], &[]);
    assert_eq!(listed.status, 200);
    let listed_txt = String::from_utf8_lossy(&listed.body).to_string();
    assert!(
        listed_txt.contains("drv-cancel-1"),
        "运行中查询列表应以客户端 id 为键: {listed_txt}"
    );
    let c = http(p, "POST", "/api/v1/queries/drv-cancel-1/cancel", &[], b"");
    assert_eq!(
        c.status,
        200,
        "取消请求应 200: {}",
        String::from_utf8_lossy(&c.body)
    );
    let r = run.join().expect("thread");
    assert_eq!(r.status, 400, "被取消的查询应失败");
    assert_eq!(r.code(), "QUERY_CANCELLED", "应带 QUERY_CANCELLED 码");
}

// ---------------------------------------------------------------------------
// 4. 错误码目录：运行期可查 + 码表外的码不存在 + 判定不靠文案嗅探
// ---------------------------------------------------------------------------

#[test]
fn error_catalog_endpoint_and_no_off_catalog_codes() {
    let srv = Server::start(&[]);
    let p = srv.port;

    // 目录端点：免鉴权（冷启动即可拉），码集合与文档一致（条数由代码侧枚举定）
    let r = http(p, "GET", "/api/v1/error-codes", &[], &[]);
    assert_eq!(r.status, 200, "目录端点应免鉴权可读");
    let codes = r.json()["codes"].as_array().cloned().unwrap_or_default();
    // 条数与代码侧枚举一致（docs/error-codes.md ↔ enum 的逐字对账在单测里）
    // 条数（集成测试看不到 crate 内部枚举，故这里写死；**逐字对账在 `error_code.rs`
    // 的单测里**，那条才是防漂移的权威）
    assert_eq!(codes.len(), 28, "目录应列出全部码（数错说明漏挂在 ALL 上）");
    for c in &codes {
        for k in ["code", "http", "retry", "category", "sqlState"] {
            assert!(c.get(k).is_some(), "目录条目缺字段 {k}: {c}");
        }
    }
    let find = |want: &str| {
        codes
            .iter()
            .find(|c| c["code"] == want)
            .unwrap_or_else(|| panic!("目录缺 {want}"))
            .clone()
    };
    // 二十批新码：409 但**不**与 WRITE_CONFLICT 同族（那个"重读再试"有用，
    // 这个必须换动作）——码的存在本身就是给驱动的动作提示
    assert_eq!(find("SEGMENT_REFERENCED")["http"], 409);
    assert_eq!(find("SEGMENT_REFERENCED")["retry"], false);
    assert_eq!(find("SEGMENT_REFERENCED")["sqlState"], "55006");
    // 两个 503：状态码相同、retry 相反——驱动靠 code 而非状态码决策
    assert_eq!(find("TOO_MANY_QUERIES")["http"], 503);
    assert_eq!(find("TOO_MANY_QUERIES")["retry"], true);
    assert_eq!(find("READ_ONLY")["http"], 503);
    assert_eq!(find("READ_ONLY")["retry"], false);

    // --- 表名里带 "column"：不能因为文案含 column 就判成列不存在 ---
    let r = query(p, "SELECT * FROM t_column", None);
    assert_eq!(r.status, 400, "查询端点的表不存在应 400");
    assert_eq!(
        r.code(),
        "TABLE_NOT_FOUND",
        "`t_column` 是表名不是列名（旧实现嗅探文案会误判）: {}",
        String::from_utf8_lossy(&r.body)
    );

    // 真·列不存在 → COLUMN_NOT_FOUND
    let r = query(p, "CREATE TABLE real_t (a BIGINT, b BIGINT)", None);
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
    let r = query(p, "SELECT nope FROM real_t", None);
    assert_eq!(r.status, 400);
    assert_eq!(
        r.code(),
        "COLUMN_NOT_FOUND",
        "{}",
        String::from_utf8_lossy(&r.body)
    );

    // --- /write 的 IPC 解码失败 → INVALID_IPC（此前发的是目录外的 BAD_REQUEST）---
    let r = http(
        p,
        "POST",
        "/api/v1/write?table=real_t&mode=append",
        &[("Content-Type", "application/octet-stream")],
        b"definitely not arrow ipc",
    );
    assert_eq!(r.status, 400, "{}", String::from_utf8_lossy(&r.body));
    assert_eq!(r.code(), "INVALID_IPC");

    // --- 缺 Content-Type：415 归一到 400（一个码只对应一个状态码）---
    let r = http(p, "POST", "/api/v1/query", &[], br#"{"sql":"SELECT 1"}"#);
    assert_eq!(
        r.status,
        400,
        "抽取器拒绝统一 400：{}",
        String::from_utf8_lossy(&r.body)
    );
    assert_eq!(r.code(), "INVALID_ARGUMENT");
}

// ---------------------------------------------------------------------------
// 5. 只读库：READ_ONLY（503，永不重试）——与并发闸的 503 分开
// ---------------------------------------------------------------------------

#[test]
fn read_only_rejects_writes_with_dedicated_code() {
    let srv = Server::start(&["--read-only"]);
    let p = srv.port;

    // 读端点照常（只读 ≠ 不可用）
    let r = query(p, "SELECT 1", None);
    assert_eq!(
        r.status,
        200,
        "只读库的读必须可用: {}",
        String::from_utf8_lossy(&r.body)
    );

    // /write → 503 READ_ONLY（不是 UNAVAILABLE）
    let r = http(
        p,
        "POST",
        "/api/v1/write?table=t&mode=append",
        &[("Content-Type", "application/octet-stream")],
        &ipc_two_int_cols(1),
    );
    assert_eq!(r.status, 503, "{}", String::from_utf8_lossy(&r.body));
    assert_eq!(r.code(), "READ_ONLY");
    assert_eq!(r.json()["error"], "database is read-only");

    // 走 SQL 的写（DDL）也经内核只读闸 → 同一个码
    let r = query(p, "CREATE TABLE t (a BIGINT)", None);
    assert_eq!(
        r.status,
        503,
        "内核只读拒写也应 503: {}",
        String::from_utf8_lossy(&r.body)
    );
    assert_eq!(
        r.code(),
        "READ_ONLY",
        "内核 ReadOnly 错误要落到专属码（此前是 INVALID_ARGUMENT）"
    );
}

// ---------------------------------------------------------------------------
// 3. 并发闸：专属 code TOO_MANY_QUERIES（与只读 503 区分）
// ---------------------------------------------------------------------------

#[test]
fn concurrency_gate_has_dedicated_code() {
    let srv = Server::start(&["--max-concurrent-queries", "1"]);
    let p = srv.port;
    let slow = setup_slow_table(p, 3000);

    let sql = slow.clone();
    let first = std::thread::spawn(move || query(p, &sql, None));
    std::thread::sleep(Duration::from_millis(400));

    // 闸满：立刻 503 + 专属 code（不排队）
    let r = query(p, "SELECT 1", None);
    assert_eq!(
        r.status,
        503,
        "第 2 条并发应 503: {}",
        String::from_utf8_lossy(&r.body)
    );
    assert_eq!(
        r.code(),
        "TOO_MANY_QUERIES",
        "闸门应有专属 code，区别于只读 503"
    );
    assert!(
        r.json()["error"]
            .as_str()
            .unwrap_or("")
            .contains("concurrent"),
        "文案应说明并发上限"
    );

    let r1 = first.join().expect("thread");
    assert_eq!(r1.status, 200, "第一条应正常完成");

    // 闸门是闸门不是熔断：空出后立刻可查
    let r = query(p, "SELECT 1", None);
    assert_eq!(r.status, 200, "闸门释放后应立刻可查");
}

// ---------------------------------------------------------------------------
// 用户面导出端点（qsdb-sync：`--users × HTTP src` 的那一半）
// ---------------------------------------------------------------------------

/// `GET /api/v1/users/export`：admin 专属 + 与 `import_json` 往返可用。
///
/// 这条端点补的是"物理复制能走 HTTP 了，但**用户表导不出来**"（qsdb-sync
/// `--users × HTTP src` 只能显式报错）。契约的核心不是"能返回 JSON"，而是
/// **返回的东西能直接喂给 `UserStore::import_json`**——所以这里真跑一次往返。
#[test]
fn users_export_over_http_roundtrips_into_import() {
    let s = Server::start(&["--token", "s3cret"]);
    let p = s.port;
    let admin = [("Authorization", "Bearer s3cret")];

    // 先建两个用户（无 token 配置时开放；这里必须带 admin）
    for sql in [
        "CREATE USER alice WITH PASSWORD 'pass123' ROLE admin",
        "CREATE USER bob WITH PASSWORD 'pass456' ROLE reader",
    ] {
        let body = serde_json::json!({ "sql": sql }).to_string();
        let r = http(
            p,
            "POST",
            "/api/v1/query",
            &[("Content-Type", "application/json"), admin[0]],
            body.as_bytes(),
        );
        assert_eq!(r.status, 200, "{sql}: {}", String::from_utf8_lossy(&r.body));
    }

    // ① admin 导出成功，形状是 {"users":[{name,role,salt,hash}]}
    let r = http(p, "GET", "/api/v1/users/export", &admin, b"");
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
    assert_eq!(
        r.header("content-type").unwrap_or(""),
        "application/json",
        "内容类型要明确（消费方据此选解析器）"
    );
    let v = r.json();
    let users = v["users"].as_array().expect("users 数组");
    assert_eq!(users.len(), 2, "两个用户都要在");
    let names: Vec<&str> = users.iter().filter_map(|u| u["name"].as_str()).collect();
    assert!(
        names.contains(&"alice") && names.contains(&"bob"),
        "{names:?}"
    );
    for u in users {
        for k in ["name", "role", "salt", "hash"] {
            assert!(u.get(k).is_some(), "缺字段 {k}: {u}");
        }
    }

    // ② **不许有明文口令**（拖走哈希可以，明文不行）
    let text = String::from_utf8_lossy(&r.body);
    assert!(
        !text.contains("pass123") && !text.contains("pass456"),
        "导出体里出现了明文口令"
    );

    // ③ **往返**：导出体直接喂 import_json（replica `--users` 就是这么干的）
    let scratch = std::env::temp_dir().join(format!("qsdb_users_import_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&scratch);
    std::fs::create_dir_all(&scratch).unwrap();
    let store = qsdb::store::Store::local(&scratch);
    let n = qsdb::users::UserStore::import_json(&store, &text, true)
        .unwrap_or_else(|e| panic!("导出体必须能被 import_json 吃下: {e}"));
    assert_eq!(n, 2, "两个用户都该导入");
    // 导入后能用原密码验过（凭证无缝迁移是 F15 的承诺）
    assert!(
        qsdb::users::UserStore::verify(&store, "bob", "pass456").is_some(),
        "导入后原密码必须仍可登录"
    );
    let _ = std::fs::remove_dir_all(&scratch);

    // ④ 非 admin（reader 用 Basic）→ 403：体里有全部哈希
    use base64::Engine;
    let b64 = base64::engine::general_purpose::STANDARD.encode("bob:pass456");
    let basic = format!("Basic {b64}");
    let r = http(
        p,
        "GET",
        "/api/v1/users/export",
        &[("Authorization", basic.as_str())],
        b"",
    );
    assert_eq!(r.status, 403, "reader 不许导出用户面");
    assert_eq!(r.code(), "FORBIDDEN");

    // ⑤ 无凭证 → 401
    let r = http(p, "GET", "/api/v1/users/export", &[], b"");
    assert_eq!(r.status, 401);
    assert_eq!(r.code(), "UNAUTHORIZED");
}

/// **`COPY … TO` 走 `/query`**（F9 湖导出）。
///
/// 契约文档（`生态能力契约.md`）把湖导出标成"✅ 可用"，那是一条**对生态的
/// 承诺** —— 按本仓纪律（F24 那条教训：契约写"可用"的端点必须有契约测试钉着），
/// 这里钉两件事：① 它**真的能从网线上触发**（不是只有嵌入方能用）；
/// ② **闭环**——导出去的东西经由同一条 `/query` 的 `read_parquet` 读得回来。
#[test]
fn copy_to_export_over_http() {
    let srv = Server::start(&[]);
    let p = srv.port;
    assert_eq!(
        query(
            p,
            "CREATE TABLE t (dt DATE, region STRING, id BIGINT)",
            None
        )
        .status,
        200
    );
    assert_eq!(
        query(
            p,
            "INSERT INTO t VALUES \
             (CAST('2026-01-01' AS DATE), 'cn', 1), \
             (CAST('2026-01-01' AS DATE), 'cn', 2), \
             (CAST('2026-01-01' AS DATE), 'us', 3), \
             (CAST('2026-01-02' AS DATE), 'cn', 4)",
            None
        )
        .status,
        200
    );

    // 目标目录必须为空 ⇒ 每次跑用一个新的
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let uniq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let out = std::env::temp_dir().join(format!("qsdb_copy_http_{}_{}", std::process::id(), uniq));
    let _ = std::fs::remove_dir_all(&out);
    let out_s = out.to_string_lossy().replace('\\', "/");

    let r = query(
        p,
        &format!(
            "COPY (SELECT dt, region, id FROM t) TO '{out_s}' \
             (FORMAT PARQUET, PARTITION_BY (dt, region))"
        ),
        None,
    );
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
    assert_eq!(r.json()["command"], "COPY");
    assert_eq!(r.json()["rowsAffected"], 4);

    // hive 布局落到盘上（多分区列 ⇒ 逐级目录）
    for d in [
        "dt=2026-01-01/region=cn",
        "dt=2026-01-01/region=us",
        "dt=2026-01-02/region=cn",
    ] {
        assert!(
            out.join(d).join("part-00000.parquet").is_file(),
            "缺 {d}/part-00000.parquet"
        );
    }

    // 闭环：同一条 `/query` 把它读回来（分区列由目录名重建）
    let r = query(
        p,
        &format!(
            "SELECT dt, region, count(*) AS n FROM read_parquet('{out_s}/**/*.parquet') \
             WHERE dt = DATE '2026-01-01' GROUP BY dt, region ORDER BY region"
        ),
        None,
    );
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
    let rows = r.json();
    let rows = rows["rows"].as_array().expect("rows").clone();
    assert_eq!(rows.len(), 2, "{rows:?}");
    assert_eq!(rows[0][1], "cn");
    assert_eq!(rows[0][2], 2);
    assert_eq!(rows[1][1], "us");
    assert_eq!(rows[1][2], 1);

    let _ = std::fs::remove_dir_all(&out);
}

/// **结果集列名（F30）在网线上也一致**。
///
/// 契约文档把「列名」当协议面（JDBC/ODBC 元数据就是它），所以内核改了必须在
/// **网线上**再钉一遍 —— 别只在内核测试里绿。这里同时钉住两条最容易漏的：
/// ① 限定符剥掉（`t.v` ⇒ `v`）；② **重名引用显式报错**而不是静默取一个。
#[test]
fn result_column_names_over_http() {
    let srv = Server::start(&[]);
    let p = srv.port;
    assert_eq!(
        query(p, "CREATE TABLE t (id BIGINT, v BIGINT)", None).status,
        200
    );
    assert_eq!(query(p, "CREATE TABLE u (id BIGINT)", None).status, 200);
    assert_eq!(
        query(p, "INSERT INTO t VALUES (1,10),(2,20)", None).status,
        200
    );
    assert_eq!(query(p, "INSERT INTO u VALUES (1),(2)", None).status, 200);

    // ① 限定符剥掉、表达式名原样
    let cols = |sql: &str| -> Vec<String> {
        let r = query(p, sql, None);
        assert_eq!(r.status, 200, "{sql}: {}", String::from_utf8_lossy(&r.body));
        r.json()["schema"]
            .as_array()
            .expect("schema")
            .iter()
            .map(|f| f["name"].as_str().unwrap().to_string())
            .collect()
    };
    assert_eq!(cols("SELECT t.v FROM t"), vec!["v"]);
    assert_eq!(
        cols("SELECT t.id, u.id FROM t JOIN u ON t.id = u.id"),
        vec!["id", "id"]
    );
    // ★ 表达式列名**不编造**（PG 给 `count`，那是用户从没写过的名字）
    assert_eq!(cols("SELECT count(*) FROM t"), vec!["count(*)"]);

    // ② 重名引用 ⇒ 显式报错（不是静默取第一个）
    let r = query(
        p,
        "SELECT id FROM (SELECT t.id, u.id FROM t JOIN u ON t.id = u.id) x",
        None,
    );
    assert_eq!(r.status, 400, "{}", String::from_utf8_lossy(&r.body));
    assert!(
        r.json()["error"]
            .as_str()
            .unwrap_or("")
            .contains("ambiguous"),
        "{}",
        String::from_utf8_lossy(&r.body)
    );
}

// ---------------------------------------------------------------------------
// 6. 会话（十六批 M-A）：登录建会话 / 列表与归属 / 踢人 / 吊销专属码
// ---------------------------------------------------------------------------

/// 会话生命周期：Basic 登录签发会话 → 查询挂归属 → 踢人取消在跑查询 →
/// token 得 `SESSION_REVOKED`。
///
/// 契约核心：**被踢 ≠ 没登录**。笼统 401 会让客户端拿同一把 token 反复重试
/// （反正"再登一次"和"再试一次"看起来一个样）；专属码告诉驱动"这把 token
/// 永远不会好，重新登录"。
#[test]
fn session_lifecycle_kick_revocation_and_attribution() {
    let srv = Server::start(&["--token", "s3cret"]);
    let p = srv.port;
    let admin = [("Authorization", "Bearer s3cret")];

    // ① 建 reader 用户并以 Basic 登录 → token + sessionId
    let r = query_auth(
        p,
        "CREATE USER bob WITH PASSWORD 'pass456' ROLE reader",
        None,
        &admin,
    );
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
    let r = login(p, "bob", "pass456");
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
    let v = r.json();
    let tok = v["token"].as_str().unwrap().to_string();
    let sid = v["sessionId"].as_str().unwrap().to_string();
    assert!(sid.starts_with("s-"), "会话 id 形态: {sid}");
    assert_eq!(v["role"], "reader");
    let bearer = format!("Bearer {tok}");
    let bob = [("Authorization", bearer.as_str())];

    // ② 带 token 查询可用；会话在 admin 视角可见（归属 + 统计 + client 定格）
    let r = query_auth(p, "SELECT 1", None, &bob);
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
    let r = http(p, "GET", "/api/v1/sessions", &admin, b"");
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
    let sessions = r.json()["sessions"].as_array().unwrap().clone();
    assert_eq!(sessions.len(), 1, "只有一个登录会话: {sessions:?}");
    let s0 = &sessions[0];
    assert_eq!(s0["id"], sid.as_str());
    assert_eq!(s0["user"], "bob");
    assert_eq!(s0["role"], "reader");
    assert_eq!(s0["defaultDb"], "default");
    assert!(
        s0["queriesTotal"].as_u64().unwrap() >= 1,
        "发起过的查询要计入统计"
    );
    assert!(
        s0["client"].as_str().unwrap().contains("127.0.0.1"),
        "client 要定格 ip:port: {:?}",
        s0["client"]
    );

    // ③ 权限边界（十九批Ⅶ 升值）：reader 列会话 → **200 但只看到自己的**
    // （可见性按用户过滤）；看自己的详情 → 200；踢人（即使踢自己）→ 403
    let r = http(p, "GET", "/api/v1/sessions", &bob, b"");
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
    let mine = r.json();
    let mine_list = mine["sessions"].as_array().unwrap();
    assert_eq!(mine_list.len(), 1, "bob 只看到自己的会话");
    assert_eq!(mine_list[0]["user"], "bob");
    let r = http(p, "GET", &format!("/api/v1/sessions/{sid}"), &bob, b"");
    assert_eq!(
        r.status,
        200,
        "本会话可自查: {}",
        String::from_utf8_lossy(&r.body)
    );
    let r = http(p, "DELETE", &format!("/api/v1/sessions/{sid}"), &bob, b"");
    assert_eq!(r.status, 403);

    // ④ 踢"有在跑查询"的会话：查询被取消 + token 变 401 SESSION_REVOKED
    let r = query_auth(p, "CREATE TABLE t (a BIGINT, b BIGINT)", None, &admin);
    assert_eq!(r.status, 200);
    let r = http(
        p,
        "POST",
        "/api/v1/write?table=t&mode=append",
        &[("Content-Type", "application/octet-stream"), admin[0]],
        &ipc_two_int_cols(3000),
    );
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
    let sql = "SELECT count(*) FROM t a JOIN t b ON a.a > b.a".to_string();
    let tb = format!("Bearer {tok}");
    let run =
        std::thread::spawn(move || query_auth(p, &sql, None, &[("Authorization", tb.as_str())]));
    std::thread::sleep(Duration::from_millis(400));
    // 在跑查询挂上 sessionId（踢人靠注册表反查的就是它）
    let r = http(p, "GET", "/api/v1/queries", &admin, b"");
    assert_eq!(r.status, 200);
    let txt = String::from_utf8_lossy(&r.body).to_string();
    assert!(txt.contains(&sid), "在跑查询应带 sessionId={sid}: {txt}");
    // 踢
    let r = http(p, "DELETE", &format!("/api/v1/sessions/{sid}"), &admin, b"");
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
    assert_eq!(r.json()["kicked"], sid.as_str());
    assert_eq!(r.json()["cancelledQueries"], 1);
    let r = run.join().expect("thread");
    assert_eq!(
        r.status,
        400,
        "被踢会话的在跑查询应被取消: {}",
        String::from_utf8_lossy(&r.body)
    );
    assert_eq!(r.code(), "QUERY_CANCELLED");
    // token 失效 → 专属码（不是笼统 UNAUTHORIZED）
    let r = query_auth(p, "SELECT 1", None, &bob);
    assert_eq!(r.status, 401);
    assert_eq!(r.code(), "SESSION_REVOKED");
    // 再踢 → 404
    let r = http(p, "DELETE", &format!("/api/v1/sessions/{sid}"), &admin, b"");
    assert_eq!(r.status, 404);
    assert_eq!(r.code(), "NOT_FOUND");

    // ⑤ 凭证形态判定：伪造 sess- → SESSION_REVOKED；随机串 → UNAUTHORIZED；
    //    错密码登录 → UNAUTHORIZED（它没有"被踢"，是单纯没登进去）
    let r = query_auth(
        p,
        "SELECT 1",
        None,
        &[("Authorization", "Bearer sess-forged")],
    );
    assert_eq!(r.status, 401);
    assert_eq!(r.code(), "SESSION_REVOKED");
    let r = query_auth(
        p,
        "SELECT 1",
        None,
        &[("Authorization", "Bearer random-junk")],
    );
    assert_eq!(r.status, 401);
    assert_eq!(r.code(), "UNAUTHORIZED");
    let r = login(p, "bob", "wrong");
    assert_eq!(r.status, 401);
    assert_eq!(r.code(), "UNAUTHORIZED");
}

/// **库级 grants（十六批 M-C）**：白名单判定、每请求现查、/write 写入闸、
/// 登录回显 grants、CLI `--grant`。
///
/// 契约核心：**改 grants 立即生效**（同一把 token 不用重登）——管理员收权后
/// 在跑的会话下一个请求就是 403，这是"角色不缓存进会话"的纪律。
#[test]
fn grants_whitelist_per_request_effect_and_write_gate() {
    let srv = Server::start(&["--token", "s3cret"]);
    let p = srv.port;
    let admin = [("Authorization", "Bearer s3cret")];

    // ① 建 reader（旧格式全局角色）+ 登录：grants 回显 {"*":"reader"}
    let r = query_auth(
        p,
        "CREATE USER dave WITH PASSWORD 'pass123' ROLE reader",
        None,
        &admin,
    );
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
    let r = login(p, "dave", "pass123");
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
    assert_eq!(r.json()["grants"], serde_json::json!({ "*": "reader" }));
    assert_eq!(r.json()["role"], "reader");
    let tok = r.json()["token"].as_str().unwrap().to_string();
    let bearer = format!("Bearer {tok}");
    let dave = [("Authorization", bearer.as_str())];

    // ② reader 能读；**不能经 /write 写**（M-C 补上的写入闸——此前 reader 可写）
    let r = query_auth(p, "SELECT 1", None, &dave);
    assert_eq!(r.status, 200);
    let r = query_auth(p, "CREATE TABLE t (a BIGINT, b BIGINT)", None, &admin);
    assert_eq!(r.status, 200);
    let r = http(
        p,
        "POST",
        "/api/v1/write?table=t&mode=append",
        &[("Content-Type", "application/octet-stream"), dave[0]],
        &ipc_two_int_cols(1),
    );
    assert_eq!(r.status, 403, "{}", String::from_utf8_lossy(&r.body));
    assert_eq!(r.code(), "FORBIDDEN");
    assert!(
        String::from_utf8_lossy(&r.body).contains("writer or admin"),
        "{}",
        String::from_utf8_lossy(&r.body)
    );

    // ③ 收权到全拒（{"*": null}）→ **同一把 token** 下一请求即 403（现查）
    let r = http(
        p,
        "PUT",
        "/api/v1/users/dave/grants",
        &[("Content-Type", "application/json"), admin[0]],
        br#"{"grants": {"*": null}}"#,
    );
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
    assert_eq!(r.json()["grants"], serde_json::json!({ "*": null }));
    let r = query_auth(p, "SELECT 1", None, &dave);
    assert_eq!(
        r.status,
        403,
        "收权后同 token 应立即 403: {}",
        String::from_utf8_lossy(&r.body)
    );
    assert_eq!(r.code(), "FORBIDDEN");
    assert!(
        String::from_utf8_lossy(&r.body).contains("no grant on database 'default'"),
        "文案要区分'无权限': {}",
        String::from_utf8_lossy(&r.body)
    );
    // 资源端点同样 403（不是 401——凭证是好的，只是没授权）
    let r = http(p, "GET", "/api/v1/tables", &dave, b"");
    assert_eq!(r.status, 403);
    assert_eq!(r.code(), "FORBIDDEN");

    // ④ 授回精确库 → 立刻恢复（不需要重登）
    let r = http(
        p,
        "PUT",
        "/api/v1/users/dave/grants",
        &[("Content-Type", "application/json"), admin[0]],
        br#"{"grants": {"default": "reader"}}"#,
    );
    assert_eq!(r.status, 200);
    let r = query_auth(p, "SELECT 1", None, &dave);
    assert_eq!(
        r.status,
        200,
        "授回后应立即恢复: {}",
        String::from_utf8_lossy(&r.body)
    );
    // 重登的 grants 回显是新形态
    let r = login(p, "dave", "pass123");
    assert_eq!(
        r.json()["grants"],
        serde_json::json!({ "default": "reader" })
    );
    assert_eq!(r.json()["role"], "reader");

    // ⑤ 非法 body / 非法角色名 → 400；不存在的用户 → 404
    let r = http(
        p,
        "PUT",
        "/api/v1/users/dave/grants",
        &[("Content-Type", "application/json"), admin[0]],
        br#"{"grantz": {}}"#,
    );
    assert_eq!(r.status, 400);
    assert_eq!(r.code(), "INVALID_ARGUMENT");
    let r = http(
        p,
        "PUT",
        "/api/v1/users/ghost/grants",
        &[("Content-Type", "application/json"), admin[0]],
        br#"{"grants": {"*": "reader"}}"#,
    );
    assert_eq!(r.status, 404);
    let r = http(
        p,
        "PUT",
        "/api/v1/users/dave/grants",
        &[("Content-Type", "application/json"), admin[0]],
        br#"{"grants": {"*": "root"}}"#,
    );
    assert_eq!(r.status, 400, "非法角色名 → 400");

    // ⑥ CLI `user add --grant`（白名单制：只授 default=writer，无 "*"）——
    //    server 运行中同库跑 admin 子命令（写走 catalog 锁，安全）
    let out = Command::new(env!("CARGO_BIN_EXE_qsdb"))
        .args(["admin", "--path"])
        .arg(&srv.root)
        .args([
            "user-add",
            "erin",
            "--password",
            "pw123",
            "--grant",
            "default=writer",
        ])
        .output()
        .expect("run admin user add");
    assert!(
        out.status.success(),
        "user add 失败: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let r = login(p, "erin", "pw123");
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
    assert_eq!(
        r.json()["grants"],
        serde_json::json!({ "default": "writer" })
    );
    assert_eq!(r.json()["role"], "writer", "默认库角色现查回显");
    let etok = r.json()["token"].as_str().unwrap().to_string();
    let ebearer = format!("Bearer {etok}");
    // writer 真能写（写入闸放行）
    let r = http(
        p,
        "POST",
        "/api/v1/write?table=t&mode=append",
        &[
            ("Content-Type", "application/octet-stream"),
            ("Authorization", ebearer.as_str()),
        ],
        &ipc_two_int_cols(2),
    );
    assert_eq!(
        r.status,
        200,
        "writer 应能写: {}",
        String::from_utf8_lossy(&r.body)
    );

    // ⑦ 白名单制用户对未授库的语义：只授 settle → 登录成功但 default 无权
    let out = Command::new(env!("CARGO_BIN_EXE_qsdb"))
        .args(["admin", "--path"])
        .arg(&srv.root)
        .args([
            "user-add",
            "finn",
            "--password",
            "pw123",
            "--grant",
            "settle=reader",
        ])
        .output()
        .expect("run admin user add");
    assert!(out.status.success());
    let r = login(p, "finn", "pw123");
    assert_eq!(
        r.status,
        200,
        "白名单制用户登录不该被拒: {}",
        String::from_utf8_lossy(&r.body)
    );
    assert_eq!(
        r.json()["grants"],
        serde_json::json!({ "settle": "reader" })
    );
    assert!(r.json()["role"].is_null(), "对 default 无权 → role 回 null");
    let ftok = r.json()["token"].as_str().unwrap().to_string();
    let fbearer = format!("Bearer {ftok}");
    let r = query_auth(p, "SELECT 1", None, &[("Authorization", fbearer.as_str())]);
    assert_eq!(r.status, 403);
    assert!(
        String::from_utf8_lossy(&r.body).contains("no grant"),
        "{}",
        String::from_utf8_lossy(&r.body)
    );
}

/// **多库挂载（十六批 M-B）**：限定名路由、同名表隔离、跨库显式拒、
/// `USE` / `SHOW DATABASES` / `GET /databases`、写入路由。
///
/// 验收基准（设计稿 §八）：**双挂载各库同表名不串**；单库零改动回归由
/// 本文件其余全部用例（单挂载）承担。
#[test]
fn multi_mount_routing_isolation_and_rejects() {
    // 第二个挂载目录（Server::start 只建 root；Drop 只清 root）
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let uniq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let settle_dir =
        std::env::temp_dir().join(format!("qsdb_mm_settle_{}_{}", std::process::id(), uniq));
    let _ = std::fs::remove_dir_all(&settle_dir);
    std::fs::create_dir_all(&settle_dir).unwrap();

    let settle_s = settle_dir.to_string_lossy().replace('\\', "/");
    let srv = Server::start(&["--mount", &format!("settle={settle_s}")]);
    let p = srv.port;
    let q = |sql: &str| query(p, sql, None);

    // ① 同名表各建各的，互不串（验收核心）
    assert_eq!(q("CREATE TABLE t (a BIGINT)").status, 200);
    assert_eq!(q("CREATE TABLE settle.t (a BIGINT)").status, 200);
    assert_eq!(q("INSERT INTO t VALUES (1),(2)").status, 200);
    assert_eq!(q("INSERT INTO settle.t VALUES (10),(20),(30)").status, 200);
    let r = q("SELECT count(*) FROM t");
    assert_eq!(r.json()["rows"][0][0], 2, "default.t");
    let r = q("SELECT count(*) FROM settle.t");
    assert_eq!(r.json()["rows"][0][0], 3, "settle.t——同名表不串");
    // 参数化 + 限定名
    let r = query_params(
        p,
        "SELECT count(*) FROM settle.t WHERE a = $1",
        serde_json::json!([20]),
    );
    assert_eq!(r.json()["rows"][0][0], 1);

    // ② 跨库显式拒（501 UNSUPPORTED，不是静默连错库）
    let r = q("SELECT count(*) FROM t JOIN settle.t ON true");
    assert_eq!(r.status, 501, "{}", String::from_utf8_lossy(&r.body));
    assert_eq!(r.code(), "UNSUPPORTED");
    // 未挂载的库名 → 404
    let r = q("SELECT * FROM nope.t");
    assert_eq!(r.status, 404);
    assert_eq!(r.code(), "NOT_FOUND");

    // ③ REST database 字段（优先级最高）
    let body =
        serde_json::json!({"sql": "SELECT count(*) FROM t", "database": "settle"}).to_string();
    let r = http(
        p,
        "POST",
        "/api/v1/query",
        &[("Content-Type", "application/json")],
        body.as_bytes(),
    );
    assert_eq!(r.json()["rows"][0][0], 3, "database=settle 定向");
    // 未挂载的 database → 404
    let body = serde_json::json!({"sql": "SELECT 1", "database": "nope"}).to_string();
    let r = http(
        p,
        "POST",
        "/api/v1/query",
        &[("Content-Type", "application/json")],
        body.as_bytes(),
    );
    assert_eq!(r.status, 404);

    // ④ 写入路由：?table=settle.t
    let r = http(
        p,
        "POST",
        "/api/v1/write?table=settle.t&mode=append&format=csv",
        &[("Content-Type", "text/csv")],
        b"a
40
",
    );
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
    let r = q("SELECT count(*) FROM settle.t");
    assert_eq!(r.json()["rows"][0][0], 4);

    // ⑤ 元数据端点带限定名
    let r = http(p, "GET", "/api/v1/tables", &[], &[]);
    let txt = String::from_utf8_lossy(&r.body).to_string();
    assert!(
        txt.contains("default.t") && txt.contains("settle.t"),
        "多挂载 /tables 应给限定名: {txt}"
    );
    let r = http(p, "GET", "/api/v1/tables/settle.t/schema", &[], &[]);
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
    assert_eq!(r.json()["table"], "settle.t");
    let r = http(p, "GET", "/api/v1/databases", &[], &[]);
    let v = r.json();
    let dbs = v["databases"].as_array().unwrap();
    assert_eq!(dbs.len(), 2);
    assert_eq!(dbs[0]["name"], "default");
    assert_eq!(dbs[0]["readOnly"], false);

    // ⑥ USE 流程（要登录会话；开放模式登录 = anonymous 会话）
    let r = login(p, "x", "y");
    assert_eq!(
        r.status,
        200,
        "开放模式可登: {}",
        String::from_utf8_lossy(&r.body)
    );
    let tok = r.json()["token"].as_str().unwrap().to_string();
    let bearer = format!("Bearer {tok}");
    let auth = [("Authorization", bearer.as_str())];
    let qa = |sql: &str| query_auth(p, sql, None, &auth);
    let r = qa("USE settle");
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
    assert_eq!(r.json()["defaultDb"], "settle");
    let r = qa("SELECT count(*) FROM t");
    assert_eq!(r.json()["rows"][0][0], 4, "USE 后单段名落 settle");
    let r = qa("USE nope");
    assert_eq!(r.status, 404);
    // SHOW DATABASES
    let r = qa("SHOW DATABASES");
    assert_eq!(r.status, 200);
    assert_eq!(r.json()["rowCount"], 2);

    let _ = std::fs::remove_dir_all(&settle_dir);
}

/// `PUT /api/v1/sessions/current/database`：设会话默认库（`USE` 的 REST 落点）。
/// M-A 单库：唯一挂载是 `default`；未挂载的库名显式 404，拼错键 400，
/// 无会话的请求（超级 token / Basic）403。
#[test]
fn session_default_db_endpoint() {
    let srv = Server::start(&["--token", "s3cret"]);
    let p = srv.port;
    let admin = [("Authorization", "Bearer s3cret")];

    // ① 超级 token 没有会话 → 403（没有"本会话"可设）
    let r = http(
        p,
        "PUT",
        "/api/v1/sessions/current/database",
        &[("Content-Type", "application/json"), admin[0]],
        br#"{"database":"default"}"#,
    );
    assert_eq!(r.status, 403, "{}", String::from_utf8_lossy(&r.body));
    assert_eq!(r.code(), "FORBIDDEN");

    // ② 登录 carol → 设默认库（幂等：值同为 default）
    let r = query_auth(
        p,
        "CREATE USER carol WITH PASSWORD 'pw789' ROLE writer",
        None,
        &admin,
    );
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
    let r = login(p, "carol", "pw789");
    assert_eq!(r.status, 200);
    let v = r.json();
    let tok = v["token"].as_str().unwrap().to_string();
    let sid = v["sessionId"].as_str().unwrap().to_string();
    let bearer = format!("Bearer {tok}");
    let carol = [("Authorization", bearer.as_str())];

    let r = http(
        p,
        "PUT",
        "/api/v1/sessions/current/database",
        &[("Content-Type", "application/json"), carol[0]],
        br#"{"database":"default"}"#,
    );
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
    assert_eq!(r.json()["sessionId"], sid.as_str());
    assert_eq!(r.json()["defaultDb"], "default");

    // ③ 未挂载的库 → 404 NOT_FOUND（显式拒绝；M-B 多库后这里改按挂载表判）
    let r = http(
        p,
        "PUT",
        "/api/v1/sessions/current/database",
        &[("Content-Type", "application/json"), carol[0]],
        br#"{"database":"settle"}"#,
    );
    assert_eq!(r.status, 404);
    assert_eq!(r.code(), "NOT_FOUND");
    assert!(
        String::from_utf8_lossy(&r.body).contains("not mounted"),
        "{}",
        String::from_utf8_lossy(&r.body)
    );

    // ④ 拼错键 → 400（严格化与 /query 同款）
    let r = http(
        p,
        "PUT",
        "/api/v1/sessions/current/database",
        &[("Content-Type", "application/json"), carol[0]],
        br#"{"databse":"default"}"#,
    );
    assert_eq!(r.status, 400);
    assert_eq!(r.code(), "INVALID_ARGUMENT");

    // ⑤ 会话自查可见 defaultDb；metrics 有会话 gauge
    let r = http(p, "GET", &format!("/api/v1/sessions/{sid}"), &carol, b"");
    assert_eq!(r.status, 200);
    assert_eq!(r.json()["defaultDb"], "default");
    let r = http(p, "GET", "/metrics", &[], b"");
    assert!(
        String::from_utf8_lossy(&r.body).contains("qsdb_sessions 1"),
        "metrics 应有会话 gauge"
    );
}

// ---------------------------------------------------------------------------
// 22. 结果护栏（十七批 R1）：超限恒 400 不回部分数据 + 覆盖优先级 + 热更 +
//     体量闸 + DML 不受限 + metrics 计数
// ---------------------------------------------------------------------------

#[test]
fn result_guardrail_limit_override_hot_update_and_dml_exempt() {
    // 行数护栏 3，表 5 行 —— 起步即超限
    let srv = Server::start(&["--max-result-rows", "3"]);
    let p = srv.port;
    let post = |body: serde_json::Value, extra: &[(&str, &str)]| {
        let mut h = vec![("Content-Type", "application/json")];
        h.extend_from_slice(extra);
        http(p, "POST", "/api/v1/query", &h, body.to_string().as_bytes())
    };

    assert_eq!(
        query(p, "CREATE TABLE gt (a BIGINT)", None).status,
        200,
        "{}",
        String::from_utf8_lossy(&query(p, "CREATE TABLE gt (a BIGINT)", None).body)
    );
    let r = query(p, "INSERT INTO gt VALUES (1),(2),(3),(4),(5)", None);
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));

    // 超限 → 400 RESULT_TOO_LARGE；错误体只有 {error, code}，**没有 rows**
    let r = query(p, "SELECT * FROM gt", None);
    assert_eq!(r.status, 400, "{}", String::from_utf8_lossy(&r.body));
    assert_eq!(r.code(), "RESULT_TOO_LARGE");
    assert!(
        r.json().get("rows").is_none(),
        "拒绝响应不得携带部分数据: {}",
        String::from_utf8_lossy(&r.body)
    );
    let err = r.json()["error"].as_str().unwrap_or_default().to_string();
    assert!(err.contains("5 rows"), "文案应带实际行数: {err}");
    assert!(err.contains("max-result-rows"), "文案应带上限名: {err}");

    // **全格式一致**：护栏在执行后、序列化前——ipc/bulk/rows/csv 同样恒 400
    for f in ["ipc", "bulk", "rows", "csv"] {
        let r = post(
            serde_json::json!({"sql": "SELECT * FROM gt", "format": f}),
            &[],
        );
        assert_eq!(r.status, 400, "format={f} 也应被护栏拦下");
        assert_eq!(r.code(), "RESULT_TOO_LARGE", "format={f}");
    }

    // 每请求覆盖：body `maxRows: 0` = 本次不限（5 行全回）
    let r = post(
        serde_json::json!({"sql": "SELECT * FROM gt", "maxRows": 0}),
        &[],
    );
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
    assert_eq!(r.json()["rowCount"], 5);
    // snake_case 别名同样可用
    let r = post(
        serde_json::json!({"sql": "SELECT * FROM gt", "max_rows": 0}),
        &[],
    );
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
    // 头覆盖（body 缺省时生效）
    let r = query_auth(p, "SELECT * FROM gt", None, &[("X-QSDB-Max-Rows", "0")]);
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
    // **body 优先于头**：头 0（不限）+ body 2（收紧）→ 仍拦
    let r = post(
        serde_json::json!({"sql": "SELECT * FROM gt", "maxRows": 2}),
        &[("X-QSDB-Max-Rows", "0")],
    );
    assert_eq!(r.status, 400);
    assert_eq!(r.code(), "RESULT_TOO_LARGE");
    // 覆盖可**收紧**：服务器 3、头给 4 仍是 5 行超 4 → 拦（覆盖双向，不是只许放宽）
    let r = query_auth(p, "SELECT * FROM gt", None, &[("X-QSDB-Max-Rows", "4")]);
    assert_eq!(r.status, 400);
    // LIMIT 后结果变小 → 自然通过（护栏管结果，不管语句）
    assert_eq!(query(p, "SELECT * FROM gt LIMIT 2", None).status, 200);
    // 非法头值 → 显式 400（不静默忽略）
    let r = query_auth(p, "SELECT 1", None, &[("X-QSDB-Max-Rows", "abc")]);
    assert_eq!(r.status, 400);
    assert_eq!(r.code(), "INVALID_ARGUMENT");

    // DML 不受限：CTAS 的源结果集超护栏，但**回执**很小 → 照常成功（护栏只管结果集）
    let r = query(p, "CREATE TABLE gt2 AS SELECT * FROM gt", None);
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
    // 读回 gt2 的 5 行是结果集 → 照样被拦（不是只有 gt 被特殊对待）
    assert_eq!(query(p, "SELECT * FROM gt2", None).status, 400);

    // 热更：PUT /config（camelCase）→ 立即生效；GET /config 回显
    let r = http(
        p,
        "PUT",
        "/api/v1/config",
        &[("Content-Type", "application/json")],
        br#"{"maxResultRows":10}"#,
    );
    assert_eq!(r.status, 200);
    assert_eq!(r.json()["applied"], true, "camelCase 键应生效");
    assert_eq!(r.json()["maxResultRows"], 10);
    assert_eq!(query(p, "SELECT * FROM gt", None).status, 200);
    let r = http(p, "GET", "/api/v1/config", &[], &[]);
    assert_eq!(r.json()["runtime"]["maxResultRows"], 10);
    // 关闭（0）→ 放行
    let r = http(
        p,
        "PUT",
        "/api/v1/config",
        &[("Content-Type", "application/json")],
        br#"{"maxResultRows":0}"#,
    );
    assert_eq!(r.json()["applied"], true);
    assert_eq!(query(p, "SELECT * FROM gt", None).status, 200);

    // 体量闸（估算护栏）：1 字节上限 → 任何非空结果都拦；**覆盖接管护栏整体**
    // ——maxRows=0 连体量闸一并豁免（护栏只有一个豁免口径，客户端显式担责）
    let r = http(
        p,
        "PUT",
        "/api/v1/config",
        &[("Content-Type", "application/json")],
        br#"{"maxResultBytes":1}"#,
    );
    assert_eq!(r.json()["applied"], true);
    let r = query(p, "SELECT * FROM gt LIMIT 1", None);
    assert_eq!(r.status, 400, "{}", String::from_utf8_lossy(&r.body));
    assert_eq!(r.code(), "RESULT_TOO_LARGE");
    let err = r.json()["error"].as_str().unwrap_or_default().to_string();
    assert!(
        err.contains("arrow estimate"),
        "体量文案应注明是估算: {err}"
    );
    let r = post(
        serde_json::json!({"sql": "SELECT * FROM gt LIMIT 1", "maxRows": 0}),
        &[],
    );
    assert_eq!(
        r.status,
        200,
        "覆盖应连体量闸一并接管: {}",
        String::from_utf8_lossy(&r.body)
    );
    let r = http(
        p,
        "PUT",
        "/api/v1/config",
        &[("Content-Type", "application/json")],
        br#"{"maxResultBytes":0}"#,
    );
    assert_eq!(r.json()["applied"], true);

    // metrics：reason="result_size" 计数 > 0（本测试已拦 ≥8 次）
    let r = http(p, "GET", "/metrics", &[], &[]);
    let text = String::from_utf8_lossy(&r.body);
    let line = text
        .lines()
        .find(|l| l.starts_with("qsdb_queries_rejected_total{reason=\"result_size\"}"))
        .expect("metrics 应有 result_size 拒绝计数");
    let v: f64 = line.rsplit(' ').next().unwrap().parse().unwrap();
    assert!(v >= 8.0, "护栏拒绝应已计数: {line}");
}

// ---------------------------------------------------------------------------
// 23. R2：JSON 逐批流式——响应形态逐字段不变（跨批边界 / 零行 / 值形态）
// ---------------------------------------------------------------------------

#[test]
fn json_streaming_shape_unchanged_over_batch_boundaries() {
    let srv = Server::start(&[]);
    let p = srv.port;

    assert_eq!(
        query(p, "CREATE TABLE st (a BIGINT, s STRING)", None).status,
        200
    );
    // 9000 行 > 单批 8192：强制走跨批拼接（批间逗号是最容易错的缝）
    let mut vals = String::new();
    for i in 0..9000i64 {
        if i > 0 {
            vals.push(',');
        }
        vals.push_str(&format!("({i},'s{i}')"));
    }
    let r = query(p, &format!("INSERT INTO st VALUES {vals}"), None);
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));

    let r = query(p, "SELECT * FROM st", None);
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
    // 响应仍是同一个 JSON 对象（能整段 parse 本身就是流式拼接正确性的证据）
    let v = r.json();
    assert_eq!(v["rowCount"], 9000);
    let rows = v["rows"].as_array().expect("rows 是数组");
    assert_eq!(rows.len(), 9000, "流式不得丢行");
    assert_eq!(rows[0][0], 0);
    assert_eq!(rows[0][1], "s0");
    assert_eq!(rows[8191][0], 8191);
    assert_eq!(rows[8192][0], 8192, "8192 处是批边界——不得丢行/重行/坏逗号");
    assert_eq!(rows[8999][0], 8999);
    assert_eq!(rows[8999][1], "s8999");
    assert_eq!(v["schema"][0]["name"], "a");
    assert_eq!(v["schema"][1]["type"], "Utf8");
    assert!(v["queryId"].as_str().is_some_and(|s| !s.is_empty()));
    assert!(v["elapsedMs"].is_number());
    // Content-Type 不变
    assert_eq!(r.header("content-type"), Some("application/json"));

    // 零行：rows=[] / rowCount=0 形态不变
    let r = query(p, "SELECT * FROM st WHERE a > 999999", None);
    assert_eq!(r.status, 200);
    let v = r.json();
    assert_eq!(v["rowCount"], 0);
    assert_eq!(v["rows"].as_array().unwrap().len(), 0);

    // NULL 与值形态经流式路径不变（J1/J2 同款断言）
    let r = query(p, "INSERT INTO st VALUES (NULL, NULL)", None);
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
    let r = query(p, "SELECT * FROM st WHERE a IS NULL", None);
    let v = r.json();
    assert_eq!(v["rowCount"], 1);
    assert!(
        v["rows"][0][0].is_null() && v["rows"][0][1].is_null(),
        "NULL 保持 null"
    );
}

// ---------------------------------------------------------------------------
// 24. /config 的 admin 闸（studio 实测：reader 曾能 GET/PUT 热更三道闸）
// ---------------------------------------------------------------------------

#[test]
fn config_endpoints_are_admin_only() {
    let srv = Server::start(&[]);
    let p = srv.port;
    // 开放模式 = admin 等价：可用（基线不回归）
    assert_eq!(http(p, "GET", "/api/v1/config", &[], &[]).status, 200);

    // reader：GET/PUT 都 403 FORBIDDEN（三道闸的钥匙不能给所有人）
    // ⚠️ CREATE USER 的状态要断言——口令太短会静默失败，后面拿到的就是
    // 匿名 admin 会话（闸看起来失效，其实是测试自己没建出 reader）
    assert_eq!(
        query(p, "CREATE USER ru WITH PASSWORD 'rpass' ROLE reader", None).status,
        200
    );
    let r = login(p, "ru", "rpass");
    let tok = format!("Bearer {}", r.json()["token"].as_str().unwrap());
    let rr = http(p, "GET", "/api/v1/config", &[("Authorization", &tok)], &[]);
    assert_eq!(rr.status, 403, "{}", String::from_utf8_lossy(&rr.body));
    assert_eq!(rr.code(), "FORBIDDEN");
    let rr = http(
        p,
        "PUT",
        "/api/v1/config",
        &[
            ("Authorization", &tok),
            ("Content-Type", "application/json"),
        ],
        br#"{"maxResultRows":0}"#,
    );
    assert_eq!(rr.status, 403, "{}", String::from_utf8_lossy(&rr.body));
    assert_eq!(rr.code(), "FORBIDDEN");
}

// ---------------------------------------------------------------------------
// 25. COPY / `/files` 多挂载 + COPY 相对目标 = 库根（studio e2e 实测的两处不一致）
// ---------------------------------------------------------------------------

#[test]
fn copy_and_files_are_mount_aware() {
    // 第二个挂载目录（同 multi_mount 测试的模式）
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let uniq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let settle_dir =
        std::env::temp_dir().join(format!("qsdb_cm_files_{}_{}", std::process::id(), uniq));
    let _ = std::fs::remove_dir_all(&settle_dir);
    std::fs::create_dir_all(&settle_dir).unwrap();
    let settle_s = settle_dir
        .to_string_lossy()
        .replace(std::path::MAIN_SEPARATOR, "/");
    let srv = Server::start(&["--mount", &format!("settle={settle_s}")]);
    let p = srv.port;
    let q = |sql: &str| query(p, sql, None);

    // settle 库建表 + 写数据
    assert_eq!(q("CREATE TABLE settle.st (a BIGINT, s STRING)").status, 200);
    assert_eq!(
        q("INSERT INTO settle.st VALUES (1,'x'),(2,'y'),(3,'z')").status,
        200
    );

    // COPY：内层限定名路由到 settle + **相对目标按库根解析**（此前按进程 CWD）
    let r = q("COPY (SELECT * FROM settle.st) TO 'exp_mount' (FORMAT CSV, HEADER true)");
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
    assert_eq!(r.json()["command"], "COPY");

    // `/files`：`<库名>/相对路径` 前缀 → settle 库根——下载到刚导出的文件，
    // 内容与 settle 库根下的本地文件逐字节一致
    let exported = settle_dir.join("exp_mount");
    assert!(
        exported.is_file(),
        "相对目标应落在 settle 库根: {}",
        settle_dir.display()
    );
    let local = std::fs::read(&exported).unwrap();
    let r = http(p, "GET", "/api/v1/files?path=settle/exp_mount", &[], &[]);
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
    assert_eq!(r.body, local, "下载逐字节一致");
    // 同一文件在 default 库根不存在 → 404（挂载前缀真的路由了）
    let r = http(p, "GET", "/api/v1/files?path=exp_mount", &[], &[]);
    assert_eq!(r.status, 404);

    // default 库的文件照旧（`default/` 前缀与裸路径两形态）
    assert_eq!(q("CREATE TABLE dft (a BIGINT)").status, 200);
    assert_eq!(q("INSERT INTO dft VALUES (9)").status, 200);
    assert_eq!(
        q("COPY (SELECT * FROM dft) TO 'exp_dft' (FORMAT CSV, HEADER true)").status,
        200
    );
    assert!(http(p, "GET", "/api/v1/files?path=exp_dft", &[], &[]).status == 200);
    // ⚠️ 不对称（如实锁进契约）：COPY 目标 = **存储根**相对（`<根>/exp_dft`）；
    // 表文件 = `<根>/<库名>/<表>/…`（历史形态，`default/…` 是库名目录不是
    // 挂载前缀）。所以 `default/exp_dft` 是"default 库目录下的 exp_dft"⇒ 404。
    assert!(http(p, "GET", "/api/v1/files?path=default/exp_dft", &[], &[]).status == 404);
}

// ---------------------------------------------------------------------------
// 26. 配置文件全量化 + 会话上限 + bootstrap 管理员（十九批Ⅶ）
// ---------------------------------------------------------------------------

#[test]
fn 配置文件驱动启动_会话上限与bootstrap管理员() {
    // 配置文件：token + 会话上限 1 + bootstrap root 管理员
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let uniq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let root = std::env::temp_dir().join(format!("qsdb_cfgfile_{}_{}", std::process::id(), uniq));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let cfg_path = root.join("qsdb.json");
    std::fs::write(
        &cfg_path,
        br#"{
  "path": "PLACEHOLDER",
  "token": "cfgtok",
  "max_sessions": 1,
  "bootstrap_admin": { "user": "root", "password": "rootpass" },
  "cache_mb": 64
}"#,
    )
    .unwrap();

    // 未知键 → 启动即失败（deny_unknown_fields，不静默忽略）
    let bad = root.join("bad.json");
    std::fs::write(&bad, br#"{"cache_mb": 64, "cache_nonsense": 1}"#).unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_qsdb"))
        .args(["server", "--path"])
        .arg(&root)
        .args(["--addr", "127.0.0.1:0", "--config"])
        .arg(&bad)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .unwrap();
    assert!(!out.status.success(), "未知键必须启动失败");
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("unknown field"),
        "要报 unknown field: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // 正式起服务（配置文件给 token/会话上限/bootstrap；--path 走 CLI）
    let mut child = Command::new(env!("CARGO_BIN_EXE_qsdb"))
        .args(["server", "--path"])
        .arg(&root)
        .args(["--addr", "127.0.0.1:0", "--config"])
        .arg(&cfg_path)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stderr = child.stderr.take().unwrap();
    let mut buf = Vec::new();
    let port = loop {
        let mut chunk = [0u8; 512];
        match stderr.read(&mut chunk) {
            Ok(0) | Err(_) => panic!("server 提前退出: {}", String::from_utf8_lossy(&buf)),
            Ok(n) => {
                buf.extend_from_slice(&chunk[..n]);
                if let Some(p) = parse_listening_port(&String::from_utf8_lossy(&buf)) {
                    break p;
                }
            }
        }
    };
    // bootstrap 提示在日志里
    assert!(
        String::from_utf8_lossy(&buf).contains("bootstrap admin 'root'"),
        "日志应有 bootstrap 提示"
    );

    // 配置 token 生效：错 token → 401
    let r = http(
        port,
        "GET",
        "/api/v1/tables",
        &[("Authorization", "Bearer wrong")],
        &[],
    );
    assert_eq!(r.status, 401);
    // bootstrap root 管理员：登录 → admin（能读 /config）
    let r = login(port, "root", "rootpass");
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
    let tok = format!("Bearer {}", r.json()["token"].as_str().unwrap());
    let r = http(
        port,
        "GET",
        "/api/v1/config",
        &[("Authorization", &tok)],
        &[],
    );
    assert_eq!(r.status, 200, "bootstrap admin 应能读 /config");

    // 会话上限 1：root 已占 1 个会话 → 第二个登录 503 TOO_MANY_SESSIONS
    let r = login(port, "root", "rootpass");
    assert_eq!(r.status, 503, "{}", String::from_utf8_lossy(&r.body));
    assert_eq!(r.code(), "TOO_MANY_SESSIONS");

    // 踢掉会话后可以再登（等会话释放的语义闭环）
    let sid = r0_session_id(port, &tok);
    let rr = http(
        port,
        "DELETE",
        &format!("/api/v1/sessions/{sid}"),
        &[("Authorization", &tok)],
        b"",
    );
    assert_eq!(rr.status, 200);
    assert_eq!(login(port, "root", "rootpass").status, 200);

    let _ = child.kill();
    let _ = child.wait();
    let _ = std::fs::remove_dir_all(&root);
}

/// 用 admin token 查当前会话 id（测试辅助）。
fn r0_session_id(port: u16, tok: &str) -> String {
    let r = http(
        port,
        "GET",
        "/api/v1/sessions",
        &[("Authorization", tok)],
        &[],
    );
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
    let sid = r.json()["sessions"][0]["id"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    assert!(!sid.is_empty());
    sid
}

/// 单表字节数（十九批Ⅶ，studio 单表大小菜单的数据源）：分区 detail 带
/// `bytes`（压缩后落盘体积），且导出→重导的体积随之变化（不是恒 0/估算）。
#[test]
fn partitions_detail_携带字节量() {
    let srv = Server::start(&[]);
    let p = srv.port;
    let q = |sql: &str| query(p, sql, None);
    assert_eq!(q("CREATE TABLE bt (a BIGINT, s STRING)").status, 200);
    assert_eq!(
        q("INSERT INTO bt VALUES (1,'xxxxxxxxxx'),(2,'yyyyyyyyyy'),(3,'zzzzzzzzzz')").status,
        200
    );
    // bytes > 0 且随数据变化
    let body1 = http(p, "GET", "/api/v1/tables/bt/partitions", &[], &[]).json();
    let b1 = body1["detail"][0]["bytes"].as_u64().expect("bytes 字段");
    assert!(b1 > 0, "段文件字节量应 > 0: {}", body1);
    assert_eq!(
        q("INSERT INTO bt VALUES (4,'more data here to grow the segment size')").status,
        200
    );
    let body2 = http(p, "GET", "/api/v1/tables/bt/partitions", &[], &[]).json();
    let b2 = body2["detail"][0]["bytes"].as_u64().unwrap();
    assert!(b2 > b1, "追加数据后体积应增长: {b1} → {b2}");
}

/// 会话列表可见性（十九批Ⅶ）：admin 全量 / 非 admin 只看自己的。
/// 服务器带 --token 起（超级 token = Admin；建用户后服务器要求认证，
/// 匿名视角不复存在——测试全程用真实凭证）。
#[test]
fn sessions_list_可见性分档() {
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let uniq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let root = std::env::temp_dir().join(format!("qsdb_sess_vis_{}_{}", std::process::id(), uniq));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_qsdb"))
        .args(["server", "--path"])
        .arg(&root)
        .args(["--addr", "127.0.0.1:0", "--token", "t0k"])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stderr = child.stderr.take().unwrap();
    let mut buf = Vec::new();
    let port = loop {
        let mut chunk = [0u8; 512];
        match stderr.read(&mut chunk) {
            Ok(0) | Err(_) => panic!("server 提前退出"),
            Ok(n) => {
                buf.extend_from_slice(&chunk[..n]);
                if let Some(x) = parse_listening_port(&String::from_utf8_lossy(&buf)) {
                    break x;
                }
            }
        }
    };
    let post = |sql: &str, tok: &str| {
        http(
            port,
            "POST",
            "/api/v1/query",
            &[
                ("Content-Type", "application/json"),
                ("Authorization", &format!("Bearer {tok}")),
            ],
            serde_json::json!({ "sql": sql }).to_string().as_bytes(),
        )
    };
    eprintln!("STEP create table");
    assert_eq!(post("CREATE TABLE t (a BIGINT)", "t0k").status, 200);
    eprintln!("STEP create table ok");

    // 两个普通用户 + rootx（admin）
    for (u, pw) in [("u1", "pass1"), ("u2", "pass2"), ("rootx", "rootpass")] {
        eprintln!("STEP create user {u}");
        assert_eq!(
            post(
                &format!("CREATE USER {u} WITH PASSWORD '{pw}' ROLE reader"),
                "t0k"
            )
            .status,
            200,
            "建 {u}"
        );
        eprintln!("STEP create user {u} ok");
    }
    eprintln!("STEP alter rootx");
    assert_eq!(
        post("ALTER USER rootx SET ROLE admin", "t0k").status,
        200,
        "rootx 升 admin"
    );
    eprintln!("STEP alter ok");

    // 各自登录建会话
    eprintln!("STEP login rootx");
    let la = login(port, "rootx", "rootpass");
    eprintln!("STEP login rootx ok");
    let l1 = login(port, "u1", "pass1");
    let _l2 = login(port, "u2", "pass2"); // u2 也登录建会话（admin 全量断言用）
    let ta = format!("Bearer {}", la.json()["token"].as_str().unwrap());
    let t1 = format!("Bearer {}", l1.json()["token"].as_str().unwrap());

    // admin 全量：能看到 rootx / u1 / u2 三人的会话（user 字段各异）
    let r = http(
        port,
        "GET",
        "/api/v1/sessions",
        &[("Authorization", &ta)],
        &[],
    );
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
    let users: Vec<String> = r.json()["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|x| x["user"].as_str().unwrap_or("").to_string())
        .collect();
    for want in ["rootx", "u1", "u2"] {
        assert!(
            users.contains(&want.to_string()),
            "admin 应看到 {want}: {users:?}"
        );
    }

    // u1 自查：只有自己的（user 字段全是 u1）
    let r = http(
        port,
        "GET",
        "/api/v1/sessions",
        &[("Authorization", &t1)],
        &[],
    );
    assert_eq!(r.status, 200, "{}", String::from_utf8_lossy(&r.body));
    let mine = r.json();
    let list = mine["sessions"].as_array().unwrap();
    assert!(!list.is_empty(), "u1 应至少看到自己的会话");
    for x in list {
        assert_eq!(x["user"], "u1", "非 admin 只看自己的: {x}");
    }

    // Basic 凭证查询：会话归属是**用户**而非凭证形态——u1 login 建的会话，
    // 用 Basic 也能看到（仍是自己的，可见性规则不因凭证通道改变）
    use base64::Engine as _;
    let b64 = base64::engine::general_purpose::STANDARD.encode("u1:pass1");
    let r = http(
        port,
        "GET",
        "/api/v1/sessions",
        &[("Authorization", &format!("Basic {b64}"))],
        b"",
    );
    assert_eq!(r.status, 200);
    assert_eq!(r.json()["count"], 1, "Basic 可见 = 自己名下的会话");

    let _ = child.kill();
    let _ = child.wait();
    let _ = std::fs::remove_dir_all(&root);
}
