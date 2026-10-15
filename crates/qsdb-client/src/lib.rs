//! # qsdb-client —— QSDB 薄客户端
//!
//! **给"库在别的机器上、我只想连过去用"的 Rust 程序**。默认依赖只有
//! `serde` + `serde_json` + 手写的极简 HTTP/1.1：
//!
//! ```text
//! 默认形态：serde, serde_json          ← 零网络依赖
//! feature ipc：+ arrow                 ← 要 Arrow 批才打开（会与内核版本锁步）
//! ```
//!
//! ## 为什么不直接依赖 `qsdb` crate
//!
//! 内核的依赖里有 `arrow`/`parquet`/`sqlparser`/`postcard`/`rayon`/`sha2`……
//! 一个只想发 HTTP 的程序拉上它们，代价是实打实的：编译时间、二进制体积、
//! 供应链面，以及**版本锁步**（客户端被迫跟着内核的 arrow/parquet 版本走）。
//! 这就是本 crate 存在的**唯一**理由。
//!
//! **反过来说**：生态**工具**（qsdb-sync 那种"在存储后端之间搬数据"的）该引内核
//! ——判据是"它是不是另一个存储后端的搬运工"，不是"它连不连 QSDB"。
//!
//! ## 一句话用法
//!
//! ```no_run
//! use qsdb_client::{Auth, Client};
//!
//! # fn main() -> Result<(), qsdb_client::Error> {
//! let c = Client::connect("http://192.168.1.14:9430", Auth::Basic {
//!     user: "admin".into(),
//!     password: "…".into(),
//! })?;
//!
//! // 读：默认 `bulk`（列式、零拷贝、零依赖）
//! let rs = c.query("SELECT cat, count(*) FROM t GROUP BY cat")?;
//! let t = rs.into_bulk()?;
//! for row in t.rows() {
//!     let (cat, n) = (row.get(0)?, row.get(1)?);
//!     println!("{cat} {n}");
//! }
//!
//! // 写：批式装载（**不要**用 SQL 逐行 INSERT——实测差 37×）
//! let ack = c.append("t", &[
//!     ("id", vec![Some(1i64), Some(2)].into()),
//!     ("cat", vec![Some("x".to_string()), None].into()),
//! ])?;
//! println!("写入 {} 行", ack.rows);
//! # Ok(()) }
//! ```
//!
//! ## 三条"必须做对，否则白做"的（也是本 crate 的设计线）
//!
//! 1. **连接复用**：每查询一次 TCP 握手会把往返延迟翻倍 —— [`Client`] 内部
//!    保持一条 keep-alive 连接，并在对端空闲回收后**自动重连重发**（仅限
//!    "一个字节都没写出去"的情形，见下）。
//! 2. **批式 + 列式**：默认 [`Format::Bulk`]，不是逐行 JSON
//!    （体积 5–10×，且每值一次解析）。
//! 3. **别把数据搬回客户端再算**：能在 SQL 里聚合就在服务端聚合
//!    （`Client::query` 搬的是**降维后的结果**）。这条比前两条加起来还值钱。
//!
//! ## SQL 面 vs 函数调用面（两边都要，别二选一）
//!
//! | 你要做的事 | 走哪个 | 为什么 |
//! |---|---|---|
//! | 查询、聚合、DDL/DML | **SQL 文本**：[`Client::query`] / [`Client::execute`] | 它就是 `POST /api/v1/query`，与 jdbc/odbc 走的是同一条路 |
//! | 批量装载 | **函数调用**：[`Client::append`] / [`Client::append_batches`] | 二进制批通道（`bulk`/`rows`/`ipc`）。实测同量数据，走 SQL `INSERT` **81s**、走批通道 **2.18s**——**37×** |
//! | 元数据 | 专用方法：[`Client::tables`] / [`Client::schema`] / [`Client::partitions`] / [`Client::stats`] | 结构化解析，不让你自己解 JSON |
//! | 取消查询 | [`Client::cancel`] | 配合 [`QueryOptions::query_id`] 做 `Statement.cancel()` |
//!
//! 一句话：**值是 SQL 文本进（查询），批是二进制进（装载）**。没有第三条路——
//! 也不用担心"函数调用面绕过了什么"，它落到的是同一批 HTTP 端点。
//!
//! ## 参数化查询：**目前没有**（明说，不装作有）
//!
//! 协议面是 `{"sql": "..."}`，**没有绑定参数**。所以：
//!
//! - **不要把用户输入拼进 SQL 文本**——本 crate **不提供**转义助手，
//!   因为"看着能跑"的转义（漏一个引号、漏一个反斜杠）就是注入；
//! - 需要按值过滤时，**优先走服务端能力**：能当列值传的走 [`Client::append`]
//!   落到临时表，或用 `read_csv`/`read_parquet` 读文件——都不过字符串拼接；
//! - 绑定参数是内核侧的事（记在待办里），补上之前这里保持"没有就是没有"。
//!
//! ## 传输加密：**不手搓**（见 `docs/客户端与嵌入方指南.md` §四）
//!
//! 默认明文直连（内网的标准答案）。要加密：**前面挂 TLS 终止代理**
//! （nginx/haproxy），别自己写"服务端加密 / 客户端解密"——那是
//! "看起来加密了、其实没有"，比明文更危险。
//!
//! ## 幂等性（写路径的硬约束）
//!
//! `append` **非幂等**（`docs/对接契约.md` §四.4）：重试可能重复入段。
//! 所以本客户端的透明重发**只在"一个字节都没写出去"时**发生
//! （那意味着服务端根本没收到请求）。要 exactly-once 请用
//! [`Mode::Overwrite`] / [`Mode::OverwriteCas`]（分区原子替换 + 乐观并发）。

#![deny(missing_docs)]
#![deny(missing_debug_implementations)]

mod bulk;
mod error;
mod http;
#[cfg(feature = "ipc")]
mod ipc;
mod json;
mod rows;
mod value;

use std::time::Duration;

pub use crate::bulk::{BulkTable, Column, ColumnIter, ColumnView, Row, Rows};
pub use crate::error::{Error, Result};
pub use crate::json::{JsonResult, PartitionInfo, Stats, TableInfo, WriteAck};
pub use crate::rows::{Chunk, ChunkRow, RowStream, RowsData};
pub use crate::value::{ColumnDesc, ColumnInfo, Tag, Value, ValueRef};

// ---------------------------------------------------------------------------
// 编解码原语（想把字节交给**别的**传输层时用；一般情况下用 `Client` 即可）
// ---------------------------------------------------------------------------

/// 把若干列编码成一份 `qsdb-bulk` 负载（**列序必须 = 表列序**）。
///
/// 用途：你要用自己的传输（别写 HTTP 库、转发给别的进程、落到文件）时，
/// 自己拼 `POST /api/v1/write?format=bulk` 的 body。绝大多数场景直接用
/// [`Client::append`]。
pub fn encode_bulk(cols: &[(&str, Column)]) -> Result<Vec<u8>> {
    bulk::encode(cols)
}

/// 解一份 `qsdb-bulk` 负载。
pub fn decode_bulk(bytes: Vec<u8>) -> Result<BulkTable> {
    BulkTable::decode(bytes)
}

/// 解一份 `qsdb-rows` 负载（一次性版本；流式见 [`Client::query_stream`]）。
pub fn decode_rows(bytes: &[u8]) -> Result<RowsData> {
    rows::decode_all(bytes)
}

/// 线上格式（请求哪条通道）。
///
/// 响应**按 `Content-Type` 分派**（不是按你请求的格式）：`CREATE`/`INSERT`
/// 这类没有结果集的语句，**无论你请求什么格式，服务端都回 JSON 回执**——
/// 所以 [`ResultSet`] 是个枚举，别假设它一定是你点的那道菜。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Format {
    /// **默认**：无依赖列式（`docs/bulk-format.md`）。整份结果一次性进内存，
    /// 定长列取值零拷贝。绝大多数查询用它。
    #[default]
    Bulk,
    /// 无依赖行式（`docs/rows-format.md`）：**可流式、内存恒定**，
    /// 用 [`Client::query_stream`]（普通 `query` 用它只会多一次转置般的拷贝）。
    Rows,
    /// JSON：调试、人读、以及**无结果集语句的回执恒走它**。
    Json,
    /// Arrow IPC（需 `feature = "ipc"`）：手上已经是 Arrow 批、或要嵌套类型时用。
    #[cfg(feature = "ipc")]
    Ipc,
}

impl Format {
    /// 请求体里的 `format` 值（服务端拼错就 400，别自作聪明）。
    pub fn as_str(self) -> &'static str {
        match self {
            Format::Bulk => "bulk",
            Format::Rows => "rows",
            Format::Json => "json",
            #[cfg(feature = "ipc")]
            Format::Ipc => "ipc",
        }
    }
}

/// 认证方式（优先级由服务端定：Bearer 用户 token > Basic > 静态 token > 开放，见 J9）。
#[derive(Debug, Clone)]
pub enum Auth {
    /// 匿名（服务端未配置任何凭证时可用）。
    None,
    /// 用户名口令。**每次请求都带**（不做 token 换取：少一次往返，也少一份状态）。
    Basic {
        /// 用户名。
        user: String,
        /// 口令。
        password: String,
    },
    /// Bearer token（静态 `--token`，或 `/api/v1/auth/login` 签发的进程态 token）。
    Bearer(String),
}

impl Auth {
    fn header(&self) -> Option<String> {
        match self {
            Auth::None => None,
            Auth::Basic { user, password } => Some(format!(
                "Basic {}",
                http::b64_encode(format!("{user}:{password}").as_bytes())
            )),
            Auth::Bearer(t) => Some(format!("Bearer {t}")),
        }
    }
}

/// 连接层参数。
#[derive(Debug, Clone)]
pub struct ConnectOptions {
    /// TCP 建连超时（默认 10s）。
    ///
    /// **只有建连有超时**：给读写加超时是最常见的"好心办坏事"——
    /// 一条跑 5 分钟的查询会被自家客户端在第一次读时掐断。要限制查询时长，
    /// 用 [`QueryOptions::timeout_ms`]（服务端侧，回的是带 code 的正常错误体）。
    pub connect_timeout: Option<Duration>,
    /// 读写 I/O 超时（默认 `None` = 不设）。除非你的网络真的会黑洞，否则别设。
    pub io_timeout: Option<Duration>,
}

impl Default for ConnectOptions {
    fn default() -> Self {
        Self {
            connect_timeout: Some(Duration::from_secs(10)),
            io_timeout: None,
        }
    }
}

/// 一次查询的参数。
#[derive(Debug, Clone, Default)]
pub struct QueryOptions {
    /// 通道（默认 [`Format::Bulk`]）。
    pub format: Format,
    /// 服务端侧的查询超时（毫秒）→ 请求头 `X-QSDB-Timeout-Ms`。
    ///
    /// 超时是**服务端**回的带 code 错误（`QUERY_TIMEOUT`），不是客户端的
    /// `TimedOut`——驱动能分辨"查询太慢"和"网络断了"。
    pub timeout_ms: Option<u64>,
    /// 执行前指定 query id（J11）。
    ///
    /// 用途：并发环境下**精确取消自己那条**（不按 SQL 文本反查，避免误伤同语句的
    /// 其它连接）。合法值 `1..=64` 字符的 `[A-Za-z0-9._:-]`，非法值服务端回
    /// 400（**不静默忽略**）。
    ///
    /// ⚠️ 冲突回退：若该 id 已被**运行中**查询占用，服务端回退成自己生成的 id。
    /// 所以**以响应回显为准**（[`JsonResult::query_id`] / [`ResultSet::query_id`]），
    /// 别拿你发出去的值去取消。
    pub query_id: Option<String>,
    /// **参数化查询**的值（`docs/参数化查询设计.md`）。
    ///
    /// 服务端在**内核绑定器**里把它们变成 AST 字面量 —— **值从不进入 SQL 文本**，
    /// 所以这里不需要、也**不该**有"转义助手"。
    ///
    /// 占位符有两种写法，用哪个都行（**同一句不许混用**）：
    /// `WHERE g = $1`（显式、可重复引用）或 `WHERE g = ?`（按出现顺序）。
    pub params: Vec<Param>,
}

/// 一个参数值。
///
/// 两种形态对应服务端协议的两种写法（`params: [...]` 数组的元素）：
///
/// ```no_run
/// # use qsdb_client::Param;
/// let a = Param::infer(10);                       // 裸值：服务端按 JSON 形态推类型
/// let b = Param::typed("DATE", "2026-01-01");     // 显式类型：与驱动 setDate 对齐
/// ```
///
/// **显式类型什么时候需要**：JSON 推不出你想要的那个类型时。典型是
/// `"2026-01-01"` —— 裸值会被推成 STRING，而列是 DATE 时比较会不如你预期；
/// 以及 `10` 想当 `SMALLINT` / `DECIMAL(10,2)` 这类窄类型。
/// ⇒ **拿不准就写显式类型**（它是裸值的超集，不会因为将来加类型而改协议）。
#[derive(Debug, Clone, PartialEq)]
pub enum Param {
    /// 裸值 —— 服务端按 JSON 形态推类型（`10`⇒BIGINT、`"a"`⇒STRING、`true`⇒BOOLEAN…）
    Infer(serde_json::Value),
    /// 显式类型 —— `ty` 走 **DDL 同一套解析器**（`BIGINT` / `DATE` /
    /// `DECIMAL(10,2)` / `VARCHAR` …），所以名字不会和建表时漂
    Typed {
        /// 类型名（走 DDL 同一套解析器：`BIGINT` / `DATE` / `DECIMAL(10,2)` …）
        ty: String,
        /// 值（按该类型解析；解析不了**显式报错**，不静默换类型）
        value: serde_json::Value,
    },
}

impl Param {
    /// 裸值（服务端推类型）。
    pub fn infer(v: impl Into<serde_json::Value>) -> Self {
        Param::Infer(v.into())
    }

    /// 显式类型。`ty` 拼错会**在服务端被拒**（不是静默按别的类型跑）。
    pub fn typed(ty: impl Into<String>, v: impl Into<serde_json::Value>) -> Self {
        Param::Typed {
            ty: ty.into(),
            value: v.into(),
        }
    }

    fn to_json(&self) -> serde_json::Value {
        match self {
            Param::Infer(v) => v.clone(),
            Param::Typed { ty, value } => {
                serde_json::json!({ "type": ty, "value": value })
            }
        }
    }
}

impl QueryOptions {
    /// JSON 通道（调试/人读）。
    pub fn json() -> Self {
        Self {
            format: Format::Json,
            ..Default::default()
        }
    }

    /// 指定服务端超时。
    pub fn with_timeout_ms(mut self, ms: u64) -> Self {
        self.timeout_ms = Some(ms);
        self
    }

    /// 指定 query id（见字段说明里的回退规则）。
    pub fn with_query_id(mut self, id: impl Into<String>) -> Self {
        self.query_id = Some(id.into());
        self
    }

    /// 带上参数（见 [`Param`]）。
    pub fn with_params(mut self, params: impl IntoIterator<Item = Param>) -> Self {
        self.params = params.into_iter().collect();
        self
    }
}

/// 写入模式。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// 追加（**非幂等**：重试可能重复入段——要 exactly-once 用另外两个）。
    Append,
    /// 分区级**原子替换**（幂等回写原语）。命中多分区时每个分区整份换掉。
    Overwrite,
    /// 分区级原子替换 + **乐观并发**（F24）：要求该分区当前 `generation == N`，
    /// 否则 `WRITE_CONFLICT`。**只支持单分区**（一次请求命中多个分区 → 400：
    /// "期望 generation"没有唯一所指）。
    OverwriteCas(u64),
}

impl Mode {
    fn as_str(self) -> &'static str {
        match self {
            Mode::Append => "append",
            Mode::Overwrite | Mode::OverwriteCas(_) => "overwrite",
        }
    }

    fn expected_generation(self) -> Option<u64> {
        match self {
            Mode::OverwriteCas(g) => Some(g),
            _ => None,
        }
    }
}

/// 一次写入的参数。
#[derive(Debug, Clone)]
pub struct WriteOptions {
    /// 模式（默认 [`Mode::Append`]）。
    pub mode: Mode,
    /// 是否立刻提交（默认 `true`）。
    ///
    /// `false` = 进写缓冲，由后续请求（或服务端的 flush 阈值）决定提交时机——
    /// **此时返回的 `rows` 是"已缓冲行数"，不是"已落盘行数"**。
    pub flush: bool,
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self {
            mode: Mode::Append,
            flush: true,
        }
    }
}

impl WriteOptions {
    /// 追加（非幂等）。
    pub fn append() -> Self {
        Self::default()
    }

    /// 分区原子替换。
    pub fn overwrite() -> Self {
        Self {
            mode: Mode::Overwrite,
            flush: true,
        }
    }

    /// 分区原子替换 + CAS（单分区）。
    pub fn overwrite_cas(generation: u64) -> Self {
        Self {
            mode: Mode::OverwriteCas(generation),
            flush: true,
        }
    }

    /// 不立刻提交（进写缓冲攒批）。
    pub fn buffered(mut self) -> Self {
        self.flush = false;
        self
    }
}

// ---------------------------------------------------------------------------
// 结果
// ---------------------------------------------------------------------------

/// 结果载荷（**按响应的 `Content-Type` 分派**，不按你请求的格式）。
#[derive(Debug)]
pub enum ResultData {
    /// 列式（默认通道）。
    Bulk(BulkTable),
    /// 行式（普通 `query` 请求 `format=rows` 时；流式请用 [`Client::query_stream`]）。
    Rows(RowsData),
    /// JSON：回执，或显式请求了 JSON。
    Json(JsonResult),
    /// Arrow IPC（`feature = "ipc"`）。
    #[cfg(feature = "ipc")]
    Ipc(Vec<arrow::record_batch::RecordBatch>),
}

impl ResultData {
    /// 通道名（错误文案与诊断用）。
    pub fn kind(&self) -> &'static str {
        match self {
            ResultData::Bulk(_) => "bulk",
            ResultData::Rows(_) => "rows",
            ResultData::Json(_) => "json",
            #[cfg(feature = "ipc")]
            ResultData::Ipc(_) => "ipc",
        }
    }
}

/// 查询结果 = **载荷** + **服务端实际使用的 query id**。
///
/// 为什么把 id 单独拎出来：`CREATE TABLE …` 这类没有结果集的语句，**无论你请求
/// 什么格式，服务端都回 JSON 回执**——所以别假设结果一定是你点的那道菜，
/// 用 [`ResultSet::into_bulk`] 之类的转换器（不是那种通道时会明确报错，不是静默给你空的）。
///
/// id 则**每条响应都有**：JSON 通道在体里，其余通道在响应头
/// （J4/J11；务必以这里的值为准——冲突回退时它与你请求的值不同）。
#[derive(Debug)]
pub struct ResultSet {
    query_id: Option<String>,
    data: ResultData,
}

impl ResultSet {
    pub(crate) fn new(data: ResultData, query_id: Option<String>) -> Self {
        Self { query_id, data }
    }

    /// 服务端**实际使用**的 query id。
    ///
    /// ⚠️ 用它、别用你请求的值去取消：id 已被运行中查询占用时服务端会回退成
    /// 自己生成的（见 [`QueryOptions::query_id`]）。
    pub fn query_id(&self) -> Option<&str> {
        self.query_id.as_deref()
    }

    /// 载荷（想自己 match 时用）。
    pub fn data(&self) -> &ResultData {
        &self.data
    }

    /// 通道名（`bulk` / `rows` / `json` / `ipc`）。
    pub fn kind(&self) -> &'static str {
        self.data.kind()
    }

    /// 取载荷。
    pub fn into_data(self) -> ResultData {
        self.data
    }

    /// 取列式结果（不是 `bulk` 就报错并说明是什么）。
    pub fn into_bulk(self) -> Result<BulkTable> {
        match self.data {
            ResultData::Bulk(t) => Ok(t),
            other => Err(mismatch(&other, "bulk")),
        }
    }

    /// 取行式结果。
    pub fn into_rows(self) -> Result<RowsData> {
        match self.data {
            ResultData::Rows(t) => Ok(t),
            other => Err(mismatch(&other, "rows")),
        }
    }

    /// 取 JSON 结果。
    pub fn into_json(self) -> Result<JsonResult> {
        match self.data {
            ResultData::Json(j) => Ok(j),
            other => Err(mismatch(&other, "json")),
        }
    }

    /// 取 Arrow 批。
    #[cfg(feature = "ipc")]
    pub fn into_ipc(self) -> Result<Vec<arrow::record_batch::RecordBatch>> {
        match self.data {
            ResultData::Ipc(b) => Ok(b),
            other => Err(mismatch(&other, "ipc")),
        }
    }

    /// 是不是无结果集的回执（DDL/DML）。
    pub fn is_exec(&self) -> bool {
        matches!(&self.data, ResultData::Json(j) if j.is_exec())
    }
}

fn mismatch(got: &ResultData, want: &str) -> Error {
    let hint = match got {
        ResultData::Json(j) if j.is_exec() => "（服务端回的是**回执**：这条语句没有结果集）",
        _ => "（响应按 Content-Type 分派，请求的 format 只决定你**要**哪条通道）",
    };
    Error::Usage(format!("结果不是 {want} 而是 {}{hint}", got.kind()))
}

// ---------------------------------------------------------------------------
// 客户端
// ---------------------------------------------------------------------------

/// QSDB 客户端：一条 keep-alive 连接 + 固定目标 + 固定凭证。
///
/// `Client` 是 `Send + Sync`（内部一把锁保护连接），可以 `Arc` 共享。
/// **并发语义**：同一 `Client` 上的请求会**串行**（一条连接）；
/// 要真并发就建多个 `Client`（各自一条连接）——这也正是连接池的形态。
pub struct Client {
    http: http::Http,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // 只暴露目标，**不回显凭证**
        f.debug_struct("Client")
            .field("target", &self.http.target())
            .finish()
    }
}

impl Client {
    /// 连一个 QSDB server。
    ///
    /// `url` 形如 `http://host:9430`（**端口必填**——不猜默认端口）。
    /// 本方法**不建连**（惰性）：连接在第一次请求时建立，所以它不会因为
    /// "服务端慢启动"而失败。
    pub fn connect(url: &str, auth: Auth) -> Result<Client> {
        Client::with_options(url, auth, ConnectOptions::default())
    }

    /// 同上，但可指定连接层参数。
    pub fn with_options(url: &str, auth: Auth, opt: ConnectOptions) -> Result<Client> {
        let (host, port) = parse_url(url)?;
        Ok(Client {
            http: http::Http::new(
                host,
                port,
                auth.header(),
                opt.connect_timeout,
                opt.io_timeout,
            ),
        })
    }

    /// 跑一条 SQL，默认走 [`Format::Bulk`]。
    pub fn query(&self, sql: &str) -> Result<ResultSet> {
        self.query_with(sql, &QueryOptions::default())
    }

    /// 跑一条**带参数**的 SQL（等价于 `query_with(sql, &QueryOptions::default().with_params(…))`）。
    ///
    /// ```no_run
    /// # fn main() -> Result<(), qsdb_client::Error> {
    /// # let c = qsdb_client::Client::connect("http://127.0.0.1:9430", qsdb_client::Auth::None)?;
    /// use qsdb_client::Param;
    /// let rs = c.query_params(
    ///     "SELECT * FROM t WHERE g = $1 AND dt >= ?",
    ///     [Param::infer(10), Param::typed("DATE", "2026-01-01")],
    /// )?;
    /// # Ok(()) }
    /// ```
    pub fn query_params(
        &self,
        sql: &str,
        params: impl IntoIterator<Item = Param>,
    ) -> Result<ResultSet> {
        self.query_with(sql, &QueryOptions::default().with_params(params))
    }

    /// 跑一条 SQL（指定通道/超时/query id）。
    pub fn query_with(&self, sql: &str, opts: &QueryOptions) -> Result<ResultSet> {
        let extra = extra_headers(opts);
        let body = body_of(sql, opts.format.as_str(), &opts.params);
        let resp = self.http.post_with(
            "/api/v1/query",
            "application/json",
            &extra_ref(&extra),
            body.as_bytes(),
        )?;
        // 非 JSON 通道的 queryId 只在响应头里（J4/J11），先取出来再交出去
        let qid = resp.head.header("x-qsdb-query-id").map(str::to_string);
        dispatch(resp.head, resp.body, qid.as_deref())
    }

    /// **流式**读取一行一条的结果（内存恒定：只保留一块）。
    ///
    /// - 恒走 `rows` 通道（唯一可流式的通道）；`opts.format` 被**忽略**——
    ///   这不是静默兜底，而是"流式"这个语义本身只有一个实现；
    /// - 返回的 [`RowStream`] **独占一条连接**（该请求带 `Connection: close`）：
    ///   在它被读完或 drop 之前，这个 `Client` 上的其它请求会用另一条连接；
    /// - 大结果**别用 `query` 再转行式**（那会把整份结果搬进内存），用这个。
    pub fn query_stream(&self, sql: &str, opts: &QueryOptions) -> Result<RowStream> {
        let extra = extra_headers(opts);
        let body = body_of(sql, "rows", &opts.params);
        let (head, mut body) = self.http.post_stream(
            "/api/v1/query",
            "application/json",
            &extra_ref(&extra),
            body.as_bytes(),
        )?;
        if !(200..300).contains(&head.status) {
            let b = body.read_all()?;
            return Err(json::api_error(head.status, &b));
        }
        let ct = head
            .header("content-type")
            .unwrap_or("")
            .to_ascii_lowercase();
        if !ct.contains("vnd.qsdb.rows") {
            // 服务端回了别的（多半是 DDL/DML 的 JSON 回执）——显式说清楚该用什么
            let b = body.read_all()?;
            return Err(match JsonResult::parse(&b) {
                Ok(j) => Error::Usage(format!(
                    "query_stream 需要一个结果集，但服务端回的是回执（{}）——用 execute()",
                    j.message.unwrap_or_default()
                )),
                Err(_) => Error::Decode(format!("期望 rows 通道，服务端 Content-Type 是 {ct:?}")),
            });
        }
        RowStream::new(body)
    }

    /// 跑一条**无结果集**的语句（DDL/DML），拿结构化回执。
    ///
    /// 回执里的 `command` / `rows_affected` 是**结构化字段**；
    /// `message` 是给人看的文案，**不做兼容承诺**——判分支请用前两个。
    /// （`query` 也能拿到同样的东西，只是返回类型是 [`ResultSet`]。）
    pub fn execute(&self, sql: &str) -> Result<JsonResult> {
        self.query_with(sql, &QueryOptions::json())?.into_json()
    }

    /// 批量装载若干列（走 `bulk` 通道，零 arrow 依赖）。
    ///
    /// **列序必须 = 表列序**（含分区列）；列名/列数与表不符由服务端判定并回
    /// 带 code 的错误（客户端不知道表结构，不猜）。行数不齐**在本地就报错**。
    ///
    /// ```no_run
    /// # fn main() -> Result<(), qsdb_client::Error> {
    /// # let c = qsdb_client::Client::connect("http://127.0.0.1:9430", qsdb_client::Auth::None)?;
    /// c.append("t", &[
    ///     ("id", vec![Some(1i64), Some(2), Some(3)].into()),
    ///     ("name", qsdb_client::Column::utf8([Some("a"), None, Some("c")])),
    /// ])?;
    /// # Ok(()) }
    /// ```
    pub fn append(&self, table: &str, cols: &[(&str, Column)]) -> Result<WriteAck> {
        self.append_with(table, cols, &WriteOptions::default())
    }

    /// 同上，可指定模式（覆盖/CAS/缓冲）。
    pub fn append_with(
        &self,
        table: &str,
        cols: &[(&str, Column)],
        opts: &WriteOptions,
    ) -> Result<WriteAck> {
        let body = bulk::encode(cols)?;
        self.write_raw(table, &body, "application/vnd.qsdb.bulk", "bulk", opts)
    }

    /// 批量装载 Arrow 批（需 `feature = "ipc"`）。
    #[cfg(feature = "ipc")]
    pub fn append_batches(
        &self,
        table: &str,
        batches: &[arrow::record_batch::RecordBatch],
        opts: &WriteOptions,
    ) -> Result<WriteAck> {
        let body = ipc::encode(batches)?;
        self.write_raw(
            table,
            &body,
            "application/vnd.apache.arrow.stream",
            "ipc",
            opts,
        )
    }

    fn write_raw(
        &self,
        table: &str,
        body: &[u8],
        content_type: &str,
        format: &str,
        opts: &WriteOptions,
    ) -> Result<WriteAck> {
        // ⚠️ `format` 是**必给**的：`/write` 的 body 解码器从**查询参数**取，
        // 缺省是 `ipc`——不写这一项时，`bulk` 的字节会被当成 IPC 去解，
        // 报出来的是 `INVALID_IPC` 那种**看着像客户端 bug、其实是漏参数**的错。
        // （`Content-Type` 只管给人看，服务端不据此分派。）
        let mut path = format!(
            "/api/v1/write?table={}&mode={}&format={format}",
            http::percent_encode(table),
            opts.mode.as_str()
        );
        if !opts.flush {
            path.push_str("&flush=false");
        }
        if let Some(g) = opts.mode.expected_generation() {
            path.push_str(&format!("&expected_generation={g}"));
        }
        let resp = self.http.post(&path, content_type, body)?;
        if !(200..300).contains(&resp.head.status) {
            return Err(json::api_error(resp.head.status, &resp.body));
        }
        json::parse_write_ack(&resp.body)
    }

    /// 表名列表。
    pub fn tables(&self) -> Result<Vec<String>> {
        match json::parse_tables(&self.get_json("/api/v1/tables")?)? {
            json::TablesBody::Names(v) => Ok(v),
            json::TablesBody::Detailed(v) => Ok(v.into_iter().map(|t| t.name).collect()),
        }
    }

    /// 表名 + 列集（一次拿全，别逐表 N+1）。
    pub fn tables_with_schema(&self) -> Result<Vec<TableInfo>> {
        match json::parse_tables(&self.get_json("/api/v1/tables?withSchema=1")?)? {
            json::TablesBody::Detailed(v) => Ok(v),
            json::TablesBody::Names(_) => Err(Error::Decode(
                "服务端忽略了 withSchema（回的是纯表名列表）".into(),
            )),
        }
    }

    /// 一张表的列定义。
    pub fn schema(&self, table: &str) -> Result<Vec<ColumnInfo>> {
        let p = format!("/api/v1/tables/{}/schema", http::percent_encode(table));
        json::parse_schema(&self.get_json(&p)?)
    }

    /// 一张表的分区（`generation` 是同步增量判据，见 `docs/对接契约.md` §一.3）。
    pub fn partitions(&self, table: &str) -> Result<Vec<PartitionInfo>> {
        let p = format!("/api/v1/tables/{}/partitions", http::percent_encode(table));
        json::parse_partitions(&self.get_json(&p)?)
    }

    /// 库统计。
    pub fn stats(&self) -> Result<Stats> {
        json::parse_stats(&self.get_json("/api/v1/stats")?)
    }

    /// 存活探针（**免鉴权**），返回服务端版本号。
    pub fn health(&self) -> Result<String> {
        #[derive(serde::Deserialize)]
        struct H {
            #[serde(default)]
            status: String,
            #[serde(default)]
            version: String,
        }
        let v: H = serde_json::from_slice(&self.get_json("/health")?)
            .map_err(|e| Error::Decode(format!("json: /health 响应不合法: {e}")))?;
        Ok(if v.version.is_empty() {
            v.status
        } else {
            v.version
        })
    }

    /// 错误码目录（**运行期**拉取，别把码表硬编码进你的程序：
    /// 内核加码时自动跟上）。结构见 `docs/error-codes.md`。
    pub fn error_codes(&self) -> Result<serde_json::Value> {
        let b = self.get_json("/api/v1/error-codes")?;
        serde_json::from_slice(&b)
            .map_err(|e| Error::Decode(format!("json: /error-codes 响应不合法: {e}")))
    }

    /// 运行中的查询（诊断）。
    pub fn running_queries(&self) -> Result<serde_json::Value> {
        let b = self.get_json("/api/v1/queries")?;
        serde_json::from_slice(&b)
            .map_err(|e| Error::Decode(format!("json: /queries 响应不合法: {e}")))
    }

    /// 取消一条查询（配合 [`QueryOptions::query_id`] 用；**用响应回显的 id**）。
    ///
    /// 取消是幂等的：已经跑完/不存在的 id 回的是"没找到"，调用方按需忽略。
    ///
    /// **生效延迟**：取消标志由算子在"块边界"协作轮询——长展开算子（叉积）
    /// 已改成**分块**（64K 行一块，块内每 16K 次再查一次），故延迟是毫秒级。
    /// 实测（2026-10-15，端到端走 HTTP，5000 行非等值 join）：
    /// 从发出取消到查询线程收到响应 **26.9ms**（不取消的基线 13.2s）。
    /// 修复前是 **19.7s**（比基线还慢——请求被受理、也回了 `QUERY_CANCELLED`，
    /// 但执行器要跑到该形状收尾才轮询到标志）。
    /// 回归：`crates/qsdb/tests/cancel_latency.rs`（判据是**比值**，不是绝对秒数）。
    pub fn cancel(&self, query_id: &str) -> Result<()> {
        let p = format!("/api/v1/queries/{}/cancel", http::percent_encode(query_id));
        let resp = self.http.post(&p, "application/json", b"{}")?;
        if !(200..300).contains(&resp.head.status) {
            return Err(json::api_error(resp.head.status, &resp.body));
        }
        Ok(())
    }

    fn get_json(&self, path: &str) -> Result<Vec<u8>> {
        let resp = self.http.get(path)?;
        if !(200..300).contains(&resp.head.status) {
            return Err(json::api_error(resp.head.status, &resp.body));
        }
        Ok(resp.body)
    }
}

/// 按 `Content-Type` 分派响应（**不按请求的 format**，见 [`ResultSet`]）。
fn dispatch(head: http::Head, body: Vec<u8>, query_id_hdr: Option<&str>) -> Result<ResultSet> {
    let status = head.status;
    if !(200..300).contains(&status) {
        return Err(json::api_error(status, &body));
    }
    let ct = head
        .header("content-type")
        .unwrap_or("")
        .to_ascii_lowercase();
    let qid = |j: Option<String>| j.or_else(|| query_id_hdr.map(str::to_string));
    if ct.contains("json") {
        let j = JsonResult::parse(&body)?;
        // JSON 通道的 id 在体里；体里没有才看响应头（两者都回时以体为准）
        let id = qid(j.query_id.clone());
        return Ok(ResultSet::new(ResultData::Json(j), id));
    }
    if ct.contains("vnd.qsdb.bulk") {
        return Ok(ResultSet::new(
            ResultData::Bulk(BulkTable::decode(body)?),
            qid(None),
        ));
    }
    if ct.contains("vnd.qsdb.rows") {
        return Ok(ResultSet::new(
            ResultData::Rows(rows::decode_all(&body)?),
            qid(None),
        ));
    }
    if ct.contains("arrow.stream") {
        #[cfg(feature = "ipc")]
        {
            return Ok(ResultSet::new(
                ResultData::Ipc(ipc::decode_all(&body)?),
                qid(None),
            ));
        }
        #[cfg(not(feature = "ipc"))]
        {
            return Err(Error::Decode(
                "服务端回了 Arrow IPC，但本客户端没开 `ipc` feature —— 请换用默认的 bulk 通道\
                 （Format::Bulk），或打开 ipc feature"
                    .into(),
            ));
        }
    }
    Err(Error::Decode(format!(
        "不认识的响应 Content-Type: {ct:?}（响应体 {} 字节）",
        body.len()
    )))
}
/// 请求体。**只在有参数时才带 `params` 字段** —— 没参数时请求体与从前逐字节相同
/// （老服务端/老代理看到的报文一个字不变）。
fn body_of(sql: &str, format: &str, params: &[Param]) -> String {
    if params.is_empty() {
        serde_json::json!({ "sql": sql, "format": format }).to_string()
    } else {
        let arr: Vec<serde_json::Value> = params.iter().map(Param::to_json).collect();
        serde_json::json!({ "sql": sql, "format": format, "params": arr }).to_string()
    }
}

fn extra_headers(opts: &QueryOptions) -> Vec<(&'static str, String)> {
    let mut v = Vec::new();
    if let Some(ms) = opts.timeout_ms {
        v.push(("X-QSDB-Timeout-Ms", ms.to_string()));
    }
    if let Some(id) = &opts.query_id {
        v.push(("X-QSDB-Query-Id", id.clone()));
    }
    v
}

/// 把拥有所有权的头对转成借用形态（`Http` 只吃借用，免得每一层都 clone）。
fn extra_ref<'a>(v: &'a [(&'static str, String)]) -> Vec<(&'static str, &'a str)> {
    v.iter().map(|(k, s)| (*k, s.as_str())).collect()
}

fn parse_url(url: &str) -> Result<(String, u16)> {
    let rest = if let Some(r) = url.strip_prefix("http://") {
        r
    } else if url.starts_with("https://") {
        return Err(Error::Usage(format!(
            "{url}: 本客户端不做 TLS。内网直连请用 http://；要加密请在库前面挂 TLS 终止代理\
             （见 docs/客户端与嵌入方指南.md §四）"
        )));
    } else {
        return Err(Error::Usage(format!("{url}: 只认 `http://host:port` 形式")));
    };
    let hostport = rest.split('/').next().unwrap_or("");
    let (host, port) = hostport.rsplit_once(':').ok_or_else(|| {
        Error::Usage(format!(
            "{url}: 缺端口。写成 http://host:9430 —— 不猜默认端口，猜错会连到别的服务上"
        ))
    })?;
    if host.is_empty() {
        return Err(Error::Usage(format!("{url}: host 为空")));
    }
    let port: u16 = port
        .parse()
        .map_err(|_| Error::Usage(format!("{url}: 端口 {port:?} 不是 1..=65535 的整数")))?;
    Ok((host.to_string(), port))
}
