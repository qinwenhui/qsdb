//! HTTP 线的测试：**自建假服务端**（`std::net::TcpListener`），照字节喂响应。
//!
//! 为什么不用真 server：真 server 只会回它自己想回的那几种形状
//! （`Content-Length` + keep-alive）。而手写 HTTP 客户端的坑全在**边角**：
//! `chunked` 分帧、响应头与 body 分次到达、对端空闲回收后 keep-alive 连接
//! 变成死连接、`Connection: close`、错误体不是 JSON……这些真 server 不陪你演。
//!
//! 每条用例都对着**协议**断言，不对着实现断言。

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::thread::JoinHandle;
use std::time::Duration;

use qsdb_client::{Auth, Client, Error};

// ---------------------------------------------------------------------------
// 假服务端
// ---------------------------------------------------------------------------

/// 一条响应：**分几次写**、写完是否关连接。
struct Step {
    parts: Vec<Vec<u8>>,
    close_after: bool,
}

impl Step {
    /// 原样字节（自己拼分帧时用）。
    fn raw(s: &str) -> Self {
        Self {
            parts: vec![s.as_bytes().to_vec()],
            close_after: false,
        }
    }

    /// 正常 JSON 响应（`Content-Length` 自动算——**手写长度算错会把测试写成假的**）。
    fn json(body: &str) -> Self {
        Self::status("200 OK", body)
    }

    /// 无 `Content-Type` 的纯文本响应（长度自动算）。
    fn plain(status_line: &str, body: &str) -> Self {
        Self::raw(&format!(
            "HTTP/1.1 {status_line}\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        ))
    }

    /// 指定状态行的 JSON 响应（长度同样自动算）。
    fn status(status_line: &str, body: &str) -> Self {
        Self::raw(&format!(
            "HTTP/1.1 {status_line}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
            body.len(),
            body
        ))
    }

    /// 分多次写出（模拟"头到了、body 还没到"）。
    fn parts(parts: &[&str]) -> Self {
        Self {
            parts: parts.iter().map(|p| p.as_bytes().to_vec()).collect(),
            close_after: false,
        }
    }

    fn close(mut self) -> Self {
        self.close_after = true;
        self
    }
}

struct Fake {
    addr: SocketAddr,
    /// 每个请求的**请求头**（已小写化），供断言用（如 Authorization）。
    reqs: Receiver<String>,
    /// 建过的连接数（复用与否的硬证据）。
    conns: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl Fake {
    fn next_req(&self) -> String {
        self.reqs
            .recv_timeout(Duration::from_secs(5))
            .expect("应当收到请求")
    }
    fn conn_count(&self) -> usize {
        self.conns.load(Ordering::SeqCst)
    }
}

impl Drop for Fake {
    fn drop(&mut self) {
        // 置停止位 → 连一次把它从 `accept` 里叫醒 → 收尾。
        // ⚠️ 只连不置位会**死等**：服务端被叫醒后会回去再 `accept`，join 永不返回
        // （这条踩过：整个测试进程挂死）。
        self.stop.store(true, Ordering::SeqCst);
        let _ = TcpStream::connect(self.addr);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

fn fake(script: Vec<Step>) -> Fake {
    let l = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = l.local_addr().expect("addr");
    let (tx, reqs): (Sender<String>, Receiver<String>) = channel();
    let conns = Arc::new(AtomicUsize::new(0));
    let conns_srv = Arc::clone(&conns);
    let stop = Arc::new(AtomicBool::new(false));
    let stop_srv = Arc::clone(&stop);
    let handle = std::thread::spawn(move || {
        let mut script = script.into_iter();
        while !stop_srv.load(Ordering::SeqCst) {
            let Ok((mut s, _)) = l.accept() else { break };
            if stop_srv.load(Ordering::SeqCst) {
                break;
            }
            conns_srv.fetch_add(1, Ordering::SeqCst);
            // 短读超时只为"脚本用完后尽快收摊"——请求都是立刻到的，不影响用例
            s.set_read_timeout(Some(Duration::from_millis(800))).ok();
            while let Some(head) = read_request(&mut s) {
                let _ = tx.send(head);
                let Some(step) = script.next() else { break };
                for p in &step.parts {
                    if s.write_all(p).is_err() {
                        return;
                    }
                    let _ = s.flush();
                    std::thread::sleep(Duration::from_millis(20));
                }
                if step.close_after {
                    break;
                }
            }
        }
    });
    Fake {
        addr,
        reqs,
        conns,
        stop,
        handle: Some(handle),
    }
}

/// 读一个请求，返回**原样**的请求头文本（大小写不能改：base64 是大小写敏感的）。
/// body 一并读掉（否则下一条请求会读到脏数据）。
fn read_request(s: &mut TcpStream) -> Option<String> {
    let mut buf: Vec<u8> = Vec::new();
    let mut tmp = [0u8; 4096];
    let head_end = loop {
        if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break i + 4;
        }
        match s.read(&mut tmp) {
            Ok(0) | Err(_) => return None,
            Ok(n) => buf.extend_from_slice(&tmp[..n]),
        }
    };
    let head = String::from_utf8_lossy(&buf[..head_end]).into_owned();
    let cl = header(&head, "content-length")
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(0);
    let mut have = buf.len() - head_end;
    while have < cl {
        match s.read(&mut tmp) {
            Ok(0) | Err(_) => break,
            Ok(n) => have += n,
        }
    }
    Some(head)
}

/// 按名取头（名字大小写无关，**值原样**）。
fn header(head: &str, name: &str) -> Option<String> {
    head.lines().find_map(|l| {
        let (k, v) = l.split_once(':')?;
        k.trim()
            .eq_ignore_ascii_case(name)
            .then(|| v.trim().to_string())
    })
}

fn client(f: &Fake) -> Client {
    Client::connect(&format!("http://{}", f.addr), Auth::None).expect("connect")
}

fn chunked(body: &str) -> String {
    let (a, b) = body.split_at(body.len() / 2);
    format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n\
         {:x}\r\n{}\r\n{:x}\r\n{}\r\n0\r\n\r\n",
        a.len(),
        a,
        b.len(),
        b
    )
}

const OK1: &str = r#"{"status":"ok","version":"1"}"#;

// ---------------------------------------------------------------------------
// 用例
// ---------------------------------------------------------------------------

#[test]
fn chunked_响应体被完整解码() {
    let f = fake(vec![Step::raw(&chunked(
        r#"{"status":"ok","version":"9.9"}"#,
    ))]);
    assert_eq!(client(&f).health().expect("health"), "9.9");
}

#[test]
fn 头与_body_分次到达也能读全() {
    let body = r#"{"status":"ok","version":"1.2"}"#;
    let head = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
        body.len()
    );
    let (a, b) = body.split_at(10);
    let f = fake(vec![Step::parts(&[&head, a, b])]);
    assert_eq!(client(&f).health().expect("health"), "1.2");
}

#[test]
fn basic_认证头按_base64_发出() {
    let f = fake(vec![Step::json(OK1)]);
    let c = Client::connect(
        &format!("http://{}", f.addr),
        Auth::Basic {
            user: "admin".into(),
            password: "secret".into(),
        },
    )
    .unwrap();
    c.health().unwrap();
    // `admin:secret` 的 base64 是公认值（任何 base64 工具都能复核）
    assert_eq!(
        header(&f.next_req(), "authorization").as_deref(),
        Some("Basic YWRtaW46c2VjcmV0")
    );
}

#[test]
fn bearer_认证头原样发出() {
    let f = fake(vec![Step::json(OK1)]);
    let c = Client::connect(&format!("http://{}", f.addr), Auth::Bearer("tok-1".into())).unwrap();
    c.health().unwrap();
    assert_eq!(
        header(&f.next_req(), "authorization").as_deref(),
        Some("Bearer tok-1")
    );
}

#[test]
fn 匿名时不带认证头() {
    let f = fake(vec![Step::json(OK1)]);
    client(&f).health().unwrap();
    assert_eq!(header(&f.next_req(), "authorization"), None);
}

#[test]
fn 错误体带_code_且不可重试() {
    let f = fake(vec![Step::status(
        "400 Bad Request",
        r#"{"error":"no such table: t","code":"TABLE_NOT_FOUND"}"#,
    )]);
    let e = client(&f).tables().unwrap_err();
    match &e {
        Error::Api {
            status,
            code,
            message,
        } => {
            assert_eq!(*status, 400);
            assert_eq!(code, "TABLE_NOT_FOUND");
            assert_eq!(message, "no such table: t");
        }
        other => panic!("应当是 Api 错误，实为 {other:?}"),
    }
    assert_eq!(e.code(), Some("TABLE_NOT_FOUND"));
    assert!(!e.retriable(), "语义错误不该被判为可重试");
}

#[test]
fn 错误体不是_json_时也保留原文() {
    // 连 Content-Type 都没有（框架层崩了的样子）
    let f = fake(vec![Step::plain("500 Internal Server Error", "boom: died")]);
    let e = client(&f).tables().unwrap_err();
    match &e {
        Error::Api {
            status,
            code,
            message,
        } => {
            assert_eq!(*status, 500);
            assert_eq!(code, "", "拿不到码就该是空串，不编一个假的");
            assert!(message.contains("boom"), "{message}");
        }
        other => panic!("应当是 Api 错误，实为 {other:?}"),
    }
}

#[test]
fn connection_close_之后的下一个请求会重连() {
    let f = fake(vec![
        Step::json(OK1).close(),
        Step::json(r#"{"status":"ok","version":"2"}"#),
    ]);
    let c = client(&f);
    assert_eq!(c.health().unwrap(), "1");
    assert_eq!(c.health().unwrap(), "2");
    assert_eq!(f.conn_count(), 2, "`Connection: close` 之后必须换一条连接");
}

#[test]
fn keepalive_被空闲回收后仍能续用() {
    // 第一条响应正常（客户端会把连接留池），但服务端随后就关了它；
    // 第二次请求若落到这条死连接上，必须**自动重连重发**（请求根本没出去过），
    // 而不是把错误抛给调用方。
    let f = fake(vec![
        Step::json(OK1).close(),
        Step::json(r#"{"status":"ok","version":"3"}"#),
    ]);
    let c = client(&f);
    assert_eq!(c.health().unwrap(), "1");
    assert_eq!(c.health().unwrap(), "3");
}

#[test]
fn 连接被复用_两条请求只用一条连接() {
    let f = fake(vec![Step::json(OK1), Step::json(OK1)]);
    let c = client(&f);
    c.health().unwrap();
    c.health().unwrap();
    assert!(f.reqs.recv_timeout(Duration::from_secs(5)).is_ok());
    assert!(f.reqs.recv_timeout(Duration::from_secs(5)).is_ok());
    assert_eq!(f.conn_count(), 1, "keep-alive 没生效：建了不止一条连接");
}

#[test]
fn 没有长度也没有_chunked_时读到_eof() {
    let f = fake(vec![
        Step::raw("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n{\"status\":\"ok\",\"version\":\"7\"}")
            .close(),
    ]);
    assert_eq!(client(&f).health().unwrap(), "7");
}

#[test]
fn 分帧截断必须报错而不是当读完() {
    // 声明 100 字节、只给 20 字节就断：**必须报错**——静默当"读完了"
    // 就是把半份数据交给上层（写进库里更糟）
    let f = fake(vec![Step::raw(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 100\r\n\r\n{\"status\":\"ok\"}",
    )
    .close()]);
    let e = client(&f).health().unwrap_err();
    assert!(e.retriable(), "传输层错误应当可重试：{e:?}");
    assert!(matches!(e, Error::Io(_)), "{e:?}");
}

#[test]
fn 状态行不合法时报协议错() {
    let f = fake(vec![Step::raw("NOT-HTTP\r\n\r\n").close()]);
    let e = client(&f).health().unwrap_err();
    assert!(matches!(e, Error::Io(_) | Error::Http(_)), "{e:?}");
}

#[test]
fn 响应头没有_content_type_时不瞎猜格式() {
    let f = fake(vec![Step::raw(
        "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}",
    )]);
    let e = client(&f).query("SELECT 1").unwrap_err();
    match e {
        // 响应头没给 Content-Type 时，我们按"不是已知二进制通道"处理——
        // 这里 Content-Type 缺失 → 落到不认识的分支（**不猜成 JSON**）
        Error::Decode(_) | Error::Usage(_) => {}
        other => panic!("应当明确报错，实为 {other:?}"),
    }
}

#[test]
fn url_解析严格() {
    assert!(
        Client::connect("http://127.0.0.1", Auth::None).is_err(),
        "缺端口不猜"
    );
    let e = Client::connect("https://example.com:443", Auth::None).unwrap_err();
    assert!(format!("{e}").contains("TLS"), "{e}");
    assert!(Client::connect("127.0.0.1:9430", Auth::None).is_err());
    assert!(Client::connect("http://:9430", Auth::None).is_err());
    assert!(Client::connect("http://127.0.0.1:99999", Auth::None).is_err());
    assert!(Client::connect("http://127.0.0.1:9430", Auth::None).is_ok());
}

#[test]
fn 查询参数里的表名会被转义() {
    // 表名带 `&` 时若不转义，会被服务端解析成**另一个请求**
    // （表名 `a&mode=overwrite` 能把 append 变成覆盖）
    let f = fake(vec![Step::json(r#"{"rows":0,"mode":"append"}"#)]);
    let c = client(&f);
    let _ = c.append("a&mode=overwrite", &[("id", vec![Some(1i64)].into())]);
    let head = f.next_req();
    let line = head.lines().next().unwrap_or("");
    assert!(
        line.contains("table=a%26mode%3Doverwrite"),
        "表名没被转义，请求行是：{line}"
    );
    // body 编码必须**在查询参数里点名**（`/write` 不按 Content-Type 分派）
    assert!(line.contains("format=bulk"), "写入没指明 body 编码：{line}");
}

#[test]
fn 超时与_query_id_作为请求头发出() {
    let f = fake(vec![Step::json(
        r#"{"rows":[],"rowCount":0,"queryId":"q-1"}"#,
    )]);
    let c = client(&f);
    let opts = qsdb_client::QueryOptions::default()
        .with_timeout_ms(1500)
        .with_query_id("cli-9");
    let _ = c.query_with("SELECT 1", &opts);
    let head = f.next_req();
    assert_eq!(header(&head, "x-qsdb-timeout-ms").as_deref(), Some("1500"));
    assert_eq!(header(&head, "x-qsdb-query-id").as_deref(), Some("cli-9"));
}
