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
    let r = q("SELECT a, s FROM t ORDER BY a NULLS LAST", "bulk");
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
    let r = q("SELECT a, s FROM t ORDER BY a NULLS LAST", "csv");
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
    let r = q("SELECT id, name, dt FROM t ORDER BY id NULLS LAST", "rows");
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
    assert_eq!(codes.len(), 23, "目录应列出全部 23 个码");
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
