//! 手写极简 HTTP/1.1（**零网络依赖**）。
//!
//! 与"Arrow/parquet 之外无重依赖"同一条红线，同款先例是自研 CSV 解析器。
//! 只实现我们真正用到的子集：
//!
//! - `GET` / `POST`，`Content-Length` 与 `chunked` 两种分帧；
//! - **keep-alive 连接复用**（每查询一次 TCP 握手会把往返延迟翻倍）；
//! - **流式 body**（[`Body`]，`rows` 通道靠它做到内存恒定）。
//!
//! **不做**：重定向、cookie、代理、HTTP/2、TLS。要加密就挂 TLS 终止代理——
//! 手搓"服务端加密/客户端解密"是"看起来加密了、其实没有"的典型，比明文更危险
//! （见 `docs/客户端与嵌入方指南.md` §四）。
//!
//! ## 两条踩过坑的策略（改写时别丢）
//!
//! 1. **超时只加在 connect 上**。给读写加超时是最常见的"好心办坏事"：
//!    `set_read_timeout` 是**每次 `read()` 的**上限，一条跑 5 分钟的查询会在
//!    第一次读时被自家客户端掐断。真正该管的是**服务端**——用
//!    `X-QSDB-Timeout-Ms`（见 `QueryOptions::timeout_ms`），它回的是带 code 的
//!    正常错误体，而不是客户端侧的 `TimedOut`。
//! 2. **透明重发有严格条件**。keep-alive 连接会被服务端空闲回收，此时重连重发是
//!    必要的；但**写出去一半再重发就是重复写入**（`append` 非幂等，见
//!    `docs/对接契约.md` §四.4）。所以只在两种情形下重发：
//!
//!    - **一个字节都没写出去**（`sent == 0`）：服务端根本没收到请求，任何方法都安全；
//!    - **幂等的 `GET` 且一个响应字节都没收到**：这是另一个**真实竞态**——
//!      对端 `FIN` 还在路上时 `peek` 说"活着"，请求写完才拿到 `RST`
//!      （Windows 上表现为 `WSAECONNRESET`）。GET 没有副作用，重发即可；
//!      **POST 不重发**：它可能是 `append`，宁可把 `Io` 错误交给调用方，
//!      也不赌"服务端没处理"。
//!
//!    注意**代价**：POST 撞上这个竞态时会看到一次传输层错误（而不是被悄悄重试）。
//!    这是有意的——错误的代价是重试一次，判断错的代价是**数据写两遍**。

use crate::error::{Error, Result};
use std::io::{self, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::Mutex;
use std::time::Duration;

const READ_CHUNK: usize = 64 * 1024;
/// 响应头上限：正常响应头 < 2KB；给到 256KB 只为了"坏掉时明确报错"而不是吃到 OOM。
const MAX_HEAD: usize = 256 * 1024;

// ---------------------------------------------------------------------------
// 响应头
// ---------------------------------------------------------------------------

/// 响应头（名字统一小写，便于大小写无关查找）。
#[derive(Debug)]
pub(crate) struct Head {
    pub status: u16,
    pub headers: Vec<(String, String)>,
}

impl Head {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    /// 连接是否可复用（`Connection: close` 或 HTTP/1.0 未声明 keep-alive → 不可）。
    pub fn keep_alive(&self) -> bool {
        match self.header("connection") {
            Some(v) => !v.eq_ignore_ascii_case("close"),
            None => true, // HTTP/1.1 默认 keep-alive
        }
    }
}

// ---------------------------------------------------------------------------
// 连接
// ---------------------------------------------------------------------------

/// 一条带读缓冲的 TCP 连接。
#[derive(Debug)]
pub(crate) struct BufConn {
    s: TcpStream,
    /// 已读出、未消费的字节；`buf[pos..]` 是有效区。
    buf: Vec<u8>,
    pos: usize,
    /// 复用的临时读缓冲（避免每次 `fill` 都分配 64KB）。
    tmp: Vec<u8>,
    /// 最近一次 `send` 里**成功写出**的字节数。重发判据，见模块头。
    pub sent: usize,
    /// 本次 `send` 里是否**收到过响应字节**（哪怕只有一个）。
    /// 与 [`BufConn::sent`] 一起决定能不能透明重发，见模块头。
    pub got_response: bool,
}

impl BufConn {
    pub fn connect(host: &str, port: u16, connect_timeout: Option<Duration>) -> io::Result<Self> {
        let addrs = (host, port).to_socket_addrs()?;
        let mut last: Option<io::Error> = None;
        let mut stream = None;
        for a in addrs {
            let r = match connect_timeout {
                Some(t) => TcpStream::connect_timeout(&a, t),
                None => TcpStream::connect(a),
            };
            match r {
                Ok(s) => {
                    stream = Some(s);
                    break;
                }
                Err(e) => last = Some(e),
            }
        }
        let s = match (stream, last) {
            (Some(s), _) => s,
            (None, Some(e)) => return Err(e),
            (None, None) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "host 解析不出任何地址",
                ));
            }
        };
        // 别让 Nagle 把"小请求 + 小响应"的往返拖成两倍
        let _ = s.set_nodelay(true);
        Ok(Self {
            s,
            buf: Vec::with_capacity(READ_CHUNK),
            pos: 0,
            tmp: vec![0u8; READ_CHUNK],
            sent: 0,
            got_response: false,
        })
    }

    /// 设置/清除读写超时（默认**不设**，见模块头第 1 条）。
    pub fn set_io_timeout(&self, t: Option<Duration>) -> io::Result<()> {
        self.s.set_read_timeout(t)?;
        self.s.set_write_timeout(t)
    }

    /// 对端是否还活着（keep-alive 空闲被回收的检测）。
    ///
    /// **必须用非阻塞 peek**：带超时的 `peek` 在空闲连接上会一直阻塞到超时，
    /// 等于给每个请求白加一次延迟。`Ok(0)` = 对端已关（EOF）。
    fn alive(&mut self) -> bool {
        let mut b = [0u8; 1];
        if self.s.set_nonblocking(true).is_err() {
            return true;
        }
        let r = self.s.peek(&mut b);
        let _ = self.s.set_nonblocking(false);
        match r {
            Ok(0) => false,
            Ok(_) => true, // 有残留数据（不该出现在走完的响应后），当活着处理
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => true,
            Err(_) => false,
        }
    }

    fn available(&self) -> &[u8] {
        &self.buf[self.pos..]
    }

    fn consume(&mut self, n: usize) {
        self.pos += n;
    }

    fn compact(&mut self) {
        if self.pos > 0 {
            self.buf.drain(..self.pos);
            self.pos = 0;
        }
    }

    /// 再读一坨进缓冲；返回 0 = EOF。
    fn fill(&mut self) -> io::Result<usize> {
        self.compact();
        let n = self.s.read(&mut self.tmp)?;
        self.buf.extend_from_slice(&self.tmp[..n]);
        Ok(n)
    }

    // -- 写 ----------------------------------------------------------------

    fn write_all_tracked(&mut self, b: &[u8]) -> io::Result<()> {
        let mut off = 0;
        while off < b.len() {
            match self.s.write(&b[off..]) {
                Ok(0) => {
                    self.sent = off;
                    return Err(io::Error::new(io::ErrorKind::WriteZero, "write 返回 0"));
                }
                Ok(n) => off += n,
                Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => {
                    self.sent = off;
                    return Err(e);
                }
            }
        }
        self.sent = off;
        Ok(())
    }

    /// 发一个请求并读回响应头（body 由调用方决定如何消费）。
    ///
    /// `close` = 在请求头里要求 `Connection: close`（流式请求用：body 读完后
    /// 连接状态与"用户提前 drop"无法区分，索性不复用，见 [`Body`]）。
    #[allow(clippy::too_many_arguments)] // 私有底层原语：一个参数一个头，合并成结构体反而更难读
    pub fn send(
        &mut self,
        method: &str,
        path: &str,
        content_type: Option<&str>,
        extra: &[(&str, &str)],
        body: &[u8],
        auth: Option<&str>,
        close: bool,
    ) -> io::Result<Head> {
        self.sent = 0;
        self.got_response = false;
        self.buf.clear();
        self.pos = 0;
        let mut req = Vec::with_capacity(256 + body.len());
        // 目标一定是 origin-form（我们只连直连的 host:port，不做代理）
        req.extend_from_slice(format!("{method} {path} HTTP/1.1\r\n").as_bytes());
        req.extend_from_slice(format!("Host: {}\r\n", self.host_header()).as_bytes());
        req.extend_from_slice(b"Accept: */*\r\n");
        req.extend_from_slice(b"User-Agent: qsdb-client/1\r\n");
        if let Some(ct) = content_type {
            req.extend_from_slice(format!("Content-Type: {ct}\r\n").as_bytes());
        }
        for (k, v) in extra {
            req.extend_from_slice(format!("{k}: {v}\r\n").as_bytes());
        }
        if let Some(a) = auth {
            req.extend_from_slice(format!("Authorization: {a}\r\n").as_bytes());
        }
        if close {
            req.extend_from_slice(b"Connection: close\r\n");
        }
        req.extend_from_slice(format!("Content-Length: {}\r\n\r\n", body.len()).as_bytes());
        req.extend_from_slice(body);
        self.write_all_tracked(&req)?;
        self.read_head()
    }

    fn host_header(&self) -> String {
        match self.s.peer_addr() {
            Ok(a) => a.ip().to_string(),
            Err(_) => String::from("qsdb"),
        }
    }

    fn read_head(&mut self) -> io::Result<Head> {
        loop {
            if !self.available().is_empty() {
                // 收到过响应字节 = 服务端**确实处理了**这个请求 ⇒ 不能透明重发
                self.got_response = true;
            }
            if let Some(i) = find_crlfcrlf(self.available()) {
                let raw = self.available()[..i + 4].to_vec();
                self.consume(i + 4);
                return parse_head(&raw);
            }
            if self.available().len() > MAX_HEAD {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("响应头超过 {MAX_HEAD} 字节"),
                ));
            }
            if self.fill()? == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "连接在响应头结束前被关闭",
                ));
            }
        }
    }
}

fn find_crlfcrlf(b: &[u8]) -> Option<usize> {
    b.windows(4).position(|w| w == b"\r\n\r\n")
}

fn parse_head(raw: &[u8]) -> io::Result<Head> {
    let text = String::from_utf8_lossy(raw);
    let mut lines = text.split("\r\n");
    let status_line = lines.next().unwrap_or("");
    let mut parts = status_line.split(' ');
    let _ver = parts.next().unwrap_or("");
    let status: u16 = parts.next().and_then(|s| s.parse().ok()).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("状态行不合法: {status_line:?}"),
        )
    })?;
    let mut headers = Vec::new();
    for l in lines {
        if l.is_empty() {
            break;
        }
        if let Some((k, v)) = l.split_once(':') {
            headers.push((k.trim().to_ascii_lowercase(), v.trim().to_string()));
        }
    }
    Ok(Head { status, headers })
}

// ---------------------------------------------------------------------------
// 响应体
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy)]
enum Framing {
    Length(u64),
    Chunked,
    /// 既无 `Content-Length` 也无 `chunked`（HTTP/1.0 风格）：读到 EOF 为止。
    Eof,
}

fn framing_of(head: &Head) -> Framing {
    let chunked = head
        .header("transfer-encoding")
        .is_some_and(|v| v.to_ascii_lowercase().contains("chunked"));
    if chunked {
        return Framing::Chunked;
    }
    match head
        .header("content-length")
        .and_then(|v| v.trim().parse().ok())
    {
        Some(n) => Framing::Length(n),
        None => Framing::Eof,
    }
}

/// **拥有连接**的响应体读取器（`Read`）。
///
/// 之所以自己拿着连接：这样 `RowStream` 中途被 drop（用户不读完了）时，
/// 连接跟着一起 drop，不会把"读了一半的 socket"还给池子。
/// 该路径的请求一律带 `Connection: close`（见 [`Http::post_stream`]）。
#[derive(Debug)]
pub(crate) struct Body {
    conn: BufConn,
    framing: Framing,
    /// `Length`：还剩多少字节；`Chunked`：当前 chunk 还剩多少字节。
    left: u64,
    /// chunked：已见到终止块（0 长度），接下来只剩 trailer。
    eof_chunk: bool,
    done: bool,
}

impl Body {
    fn new(conn: BufConn, framing: Framing) -> Self {
        let left = match framing {
            Framing::Length(n) => n,
            _ => 0,
        };
        Self {
            conn,
            framing,
            left,
            eof_chunk: false,
            done: false,
        }
    }

    /// 读一行（到 CRLF 为止，不含 CRLF）。
    fn read_line(&mut self) -> io::Result<String> {
        loop {
            if let Some(i) = self.conn.available().windows(2).position(|w| w == b"\r\n") {
                let line = String::from_utf8_lossy(&self.conn.available()[..i]).into_owned();
                self.conn.consume(i + 2);
                return Ok(line);
            }
            if self.conn.fill()? == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "chunked body 中途断开",
                ));
            }
        }
    }

    /// 一次性读完（`Length` 路径下就是 `read_to_end`）。
    pub fn read_all(&mut self) -> io::Result<Vec<u8>> {
        let mut out = Vec::new();
        self.read_to_end(&mut out)?;
        Ok(out)
    }
}

impl Read for Body {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if self.done || out.is_empty() {
            return Ok(0);
        }
        match self.framing {
            Framing::Length(_) => {
                if self.left == 0 {
                    self.done = true;
                    return Ok(0);
                }
                if self.conn.available().is_empty() && self.conn.fill()? == 0 {
                    // 短于 Content-Length = 响应被截断。**必须报错**：静默当"读完了"
                    // 就是把半份数据当全份交给上层（写进库里更糟）。
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        format!("响应体被截断：还差 {} 字节", self.left),
                    ));
                }
                let take = out
                    .len()
                    .min(self.left as usize)
                    .min(self.conn.available().len());
                out[..take].copy_from_slice(&self.conn.available()[..take]);
                self.conn.consume(take);
                self.left -= take as u64;
                Ok(take)
            }
            Framing::Chunked => loop {
                if self.eof_chunk {
                    // trailer：读到空行为止（大多数服务端直接就是空行）
                    let line = self.read_line()?;
                    if line.is_empty() {
                        self.done = true;
                        return Ok(0);
                    }
                    continue;
                }
                if self.left > 0 {
                    if self.conn.available().is_empty() && self.conn.fill()? == 0 {
                        return Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "chunked body 中途断开",
                        ));
                    }
                    let take = out
                        .len()
                        .min(self.left as usize)
                        .min(self.conn.available().len());
                    out[..take].copy_from_slice(&self.conn.available()[..take]);
                    self.conn.consume(take);
                    self.left -= take as u64;
                    if self.left == 0 {
                        // 吃掉块尾 CRLF
                        let _ = self.read_line()?;
                    }
                    return Ok(take);
                }
                let line = self.read_line()?;
                let size_str = line.split(';').next().unwrap_or("").trim();
                let size = u64::from_str_radix(size_str, 16).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("chunked 块长度不合法: {line:?}"),
                    )
                })?;
                if size == 0 {
                    self.eof_chunk = true;
                } else {
                    self.left = size;
                }
            },
            Framing::Eof => {
                if self.conn.available().is_empty() && self.conn.fill()? == 0 {
                    self.done = true;
                    return Ok(0);
                }
                let take = out.len().min(self.conn.available().len());
                out[..take].copy_from_slice(&self.conn.available()[..take]);
                self.conn.consume(take);
                Ok(take)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// 门面
// ---------------------------------------------------------------------------

/// 一次 HTTP 往返的结果。
#[derive(Debug)]
pub(crate) struct Response {
    pub head: Head,
    pub body: Vec<u8>,
}

/// 一条复用的连接 + 固定目标。
pub(crate) struct Http {
    host: String,
    port: u16,
    /// 当前 `Authorization` 头值（None = 匿名）。**锁内可变**（十八批 C1）：
    /// login / REVOKED 重登会把它从 Basic 换成 Bearer 会话 token——构造后
    /// 仍可换，是会话态的落点。
    auth: Mutex<Option<String>>,
    connect_timeout: Option<Duration>,
    io_timeout: Option<Duration>,
    conn: Mutex<Option<BufConn>>,
}

impl Http {
    pub fn new(
        host: String,
        port: u16,
        auth: Option<String>,
        connect_timeout: Option<Duration>,
        io_timeout: Option<Duration>,
    ) -> Self {
        Self {
            host,
            port,
            auth: Mutex::new(auth),
            connect_timeout,
            io_timeout,
            conn: Mutex::new(None),
        }
    }

    /// 当前 Authorization 头值快照（克隆出锁）。REVOKED 重登的**单飞判据**：
    /// 失败时的快照 ≠ 现值 ⇒ 别的线程已经重登过，直接重发即可。
    pub(crate) fn auth_snapshot(&self) -> Option<String> {
        self.auth.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// 换 Authorization 头（login 成功后调用）。
    pub(crate) fn set_auth(&self, v: Option<String>) {
        *self.auth.lock().unwrap_or_else(|e| e.into_inner()) = v;
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Option<BufConn>> {
        // 中毒（别处 panic 时持锁）不该让客户端永久不可用：拿回内部值继续。
        self.conn.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// 目标描述（`Debug` 用；**不含凭证**）。
    pub fn target(&self) -> String {
        format!("http://{}:{}", self.host, self.port)
    }

    pub fn get(&self, path: &str) -> Result<Response> {
        self.exchange("GET", path, None, &[], &[])
    }

    pub fn post(&self, path: &str, content_type: &str, body: &[u8]) -> Result<Response> {
        self.exchange("POST", path, Some(content_type), body, &[])
    }

    /// `PUT`（十八批 C1：`PUT /sessions/current/database`）。
    pub fn put(&self, path: &str, content_type: &str, body: &[u8]) -> Result<Response> {
        let auth_snap = self.auth_snapshot();
        self.exchange_auth(
            "PUT",
            path,
            Some(content_type),
            &[],
            body,
            auth_snap.as_deref(),
        )
    }

    /// `DELETE`（十八批Ⅲ：`DELETE /sessions/{id}` 踢会话等管理动作）。
    pub fn delete(&self, path: &str) -> Result<Response> {
        let auth_snap = self.auth_snapshot();
        self.exchange_auth("DELETE", path, None, &[], b"", auth_snap.as_deref())
    }

    /// `POST` + **显式** Authorization（login 专用：要带 Basic 凭证，
    /// 不吃当前态的 token——那正是要被换掉的东西）。
    pub(crate) fn post_with_auth(
        &self,
        path: &str,
        content_type: &str,
        body: &[u8],
        auth: Option<&str>,
    ) -> Result<Response> {
        self.exchange_auth("POST", path, Some(content_type), &[], body, auth)
    }

    /// `POST` + 额外请求头（`X-QSDB-Timeout-Ms` / `X-QSDB-Query-Id`）。
    pub fn post_with(
        &self,
        path: &str,
        content_type: &str,
        extra: &[(&str, &str)],
        body: &[u8],
    ) -> Result<Response> {
        self.exchange("POST", path, Some(content_type), body, extra)
    }

    /// `POST` + **流式**响应体：请求带 `Connection: close`，返回的 [`Body`] 独占连接。
    pub fn post_stream(
        &self,
        path: &str,
        content_type: &str,
        extra: &[(&str, &str)],
        body: &[u8],
    ) -> Result<(Head, Body)> {
        self.stream_exchange("POST", Some(content_type), path, extra, body)
    }

    /// `GET` + **流式**响应体（十八批Ⅳ：`GET /api/v1/files` 文件下载——
    /// 大文件边收边写盘，两端内存恒定）。语义与 [`Self::post_stream`] 同款：
    /// `Connection: close`、Body 独占连接。
    pub(crate) fn get_stream(&self, path: &str) -> Result<(Head, Body)> {
        self.stream_exchange("GET", None, path, &[], b"")
    }

    /// 流式请求的公共路径（`post_stream` / `get_stream` 共用）。
    fn stream_exchange(
        &self,
        method: &str,
        content_type: Option<&str>,
        path: &str,
        extra: &[(&str, &str)],
        body: &[u8],
    ) -> Result<(Head, Body)> {
        let mut slot = self.lock();
        // 池里那条拿走（活着就用，失效就丢）
        let mut conn = match slot.take() {
            Some(mut c) => {
                if c.alive() {
                    c
                } else {
                    BufConn::connect(&self.host, self.port, self.connect_timeout)?
                }
            }
            None => BufConn::connect(&self.host, self.port, self.connect_timeout)?,
        };
        conn.set_io_timeout(self.io_timeout)?;
        let auth_snap = self.auth_snapshot();
        let (head, conn) = match conn.send(
            method,
            path,
            content_type,
            extra,
            body,
            auth_snap.as_deref(),
            true,
        ) {
            Ok(h) => (h, conn),
            Err(e) if conn.sent == 0 && e.kind() != io::ErrorKind::TimedOut => {
                // 陈旧连接：重连重发一次（一个字节都没出去，安全）
                let mut fresh = BufConn::connect(&self.host, self.port, self.connect_timeout)?;
                fresh.set_io_timeout(self.io_timeout)?;
                let h = fresh.send(
                    method,
                    path,
                    content_type,
                    extra,
                    body,
                    auth_snap.as_deref(),
                    true,
                )?;
                (h, fresh)
            }
            Err(e) => return Err(Error::Io(e)),
        };
        let framing = framing_of(&head);
        Ok((head, Body::new(conn, framing)))
    }

    fn exchange(
        &self,
        method: &str,
        path: &str,
        content_type: Option<&str>,
        body: &[u8],
        extra: &[(&str, &str)],
    ) -> Result<Response> {
        let auth_snap = self.auth_snapshot();
        self.exchange_auth(
            method,
            path,
            content_type,
            extra,
            body,
            auth_snap.as_deref(),
        )
    }

    /// `exchange` 的显式凭证形态（`exchange` = 用当前态快照调它）。
    fn exchange_auth(
        &self,
        method: &str,
        path: &str,
        content_type: Option<&str>,
        extra: &[(&str, &str)],
        body: &[u8],
        auth: Option<&str>,
    ) -> Result<Response> {
        let mut slot = self.lock();
        let mut first = true;
        // `GET` 无副作用 ⇒ 允许"写出去了但没收到响应"时重发（见模块头第 2 条）
        let idempotent = method == "GET";
        loop {
            let stale = match slot.as_mut() {
                Some(c) => !c.alive(),
                None => true,
            };
            if stale {
                let c = BufConn::connect(&self.host, self.port, self.connect_timeout)?;
                c.set_io_timeout(self.io_timeout)?;
                *slot = Some(c);
            }
            let attempt = {
                let c = slot.as_mut().expect("刚连上");
                c.send(method, path, content_type, extra, body, auth, false)
            };
            match attempt {
                Ok(head) => {
                    let framing = framing_of(&head);
                    let reusable = head.keep_alive() && !matches!(framing, Framing::Eof);
                    // 连接的所有权借给 Body 读完这句响应，再还回池子
                    let conn = slot.take().expect("刚连上");
                    let mut b = Body::new(conn, framing);
                    let r = b.read_all();
                    if reusable {
                        *slot = Some(b.conn);
                    }
                    return Ok(Response { head, body: r? });
                }
                Err(e) => {
                    let (sent, got) = match slot.as_ref() {
                        Some(c) => (c.sent, c.got_response),
                        None => (0, false),
                    };
                    *slot = None;
                    // 见模块头第 2 条：没写出去 → 任何方法都能重发；
                    // 写出去了但一个响应字节都没收到 → **只有 GET** 能重发
                    let safe = sent == 0 || (idempotent && !got);
                    if first && safe && e.kind() != io::ErrorKind::TimedOut {
                        first = false;
                        continue;
                    }
                    return Err(Error::Io(e));
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// 小工具（都不引依赖）
// ---------------------------------------------------------------------------

/// `Basic` 认证用的 base64 编码（**不是加密**，别误会）。
pub(crate) fn b64_encode(data: &[u8]) -> String {
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for c in data.chunks(3) {
        let b0 = c[0] as u32;
        let b1 = *c.get(1).unwrap_or(&0) as u32;
        let b2 = *c.get(2).unwrap_or(&0) as u32;
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(T[(n >> 18) as usize & 63] as char);
        out.push(T[(n >> 12) as usize & 63] as char);
        out.push(if c.len() > 1 {
            T[(n >> 6) as usize & 63] as char
        } else {
            '='
        });
        out.push(if c.len() > 2 {
            T[n as usize & 63] as char
        } else {
            '='
        });
    }
    out
}

/// query string 里一个值的最小百分号编码（unreserved 之外一律 `%XX`）。
///
/// 表名/参数值可能带 `&`、`=`、空格或中文——不编码就会被服务端解析成**另一个
/// 请求**（表名 `a&mode=overwrite` 能让 append 变覆盖）。这类"看着能跑"的注入
/// 必须从客户端这一层堵掉。
pub(crate) fn percent_encode(v: &str) -> String {
    let mut out = String::with_capacity(v.len());
    for b in v.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(*b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}
