//! `qsdb server`：HTTP API（axum）。
//!
//! tokio 只存在于本 crate；查询/写入经 `spawn_blocking` 进入同步核心库。
//! 端点（设计文档 §12）：
//! - POST /api/v1/query        {"sql": "...", "format": "ipc"|"json"} → IPC 流 / JSON
//! - POST /api/v1/write?table=T&mode=append|overwrite   body = Arrow IPC 流
//! - GET  /api/v1/tables
//! - GET  /api/v1/tables/{t}/schema
//! - GET  /api/v1/tables/{t}/partitions
//! - GET  /api/v1/stats        库统计
//! - GET  /health              存活
//! - GET  /metrics             prometheus 文本

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use qsdb::sql::SqlResult;
use qsdb::write_buffer::{WriteBuffer, WriteBufferOptions};
use qsdb::{Qsdb, QsdbError};

use crate::error_code::ErrorCode;

/// 运行中查询注册条目。
pub struct QueryEntry {
    pub ctx: Arc<qsdb::query::QueryContext>,
    pub sql: String,
}

/// 服务共享状态。
pub struct AppState {
    /// 用户 token 表（token → role；登录签发，进程内存态）
    pub tokens: std::sync::Mutex<std::collections::HashMap<String, qsdb::users::Role>>,
    pub db: Arc<Qsdb>,
    pub writer: WriteBuffer,
    pub token: Option<String>,
    pub metrics: Metrics,
    /// 运行中查询（query_id → entry）
    pub queries: std::sync::Mutex<HashMap<String, QueryEntry>>,
    /// 并发查询上限（0 = 不限）
    pub max_concurrent_queries: usize,
}

impl AppState {
    /// 组装共享状态（HTTP 与 Flight 两面共用同一份；Flight 的单测也走这里）。
    pub(crate) fn new(db: Arc<Qsdb>, token: Option<String>, max_concurrent_queries: usize) -> Self {
        let writer = db.writer(WriteBufferOptions::default());
        Self {
            tokens: std::sync::Mutex::new(HashMap::new()),
            db,
            writer,
            token,
            metrics: Metrics::default(),
            queries: std::sync::Mutex::new(HashMap::new()),
            max_concurrent_queries,
        }
    }

    /// 建查询上下文并注册为"运行中"——**并发限额与 id 归属都在同一把锁内定**。
    ///
    /// `want_id` = 客户端经 `X-QSDB-Query-Id` 透传的建议 id（契约 J11）：
    /// 未被占用则原样采用（驱动即可零扫描地 cancel）；**已被运行中查询占用则
    /// 回退服务端生成**——不报错、不顶掉在跑的那条，调用方以返回的 ctx 为准
    /// （响应回显实际 id）。
    pub(crate) fn register_query(
        &self,
        want_id: Option<String>,
        timeout_ms: Option<u64>,
        sql: String,
    ) -> std::result::Result<Arc<qsdb::query::QueryContext>, String> {
        let mut q = self
            .queries
            .lock()
            .map_err(|_| "registry poisoned".to_string())?;
        if self.max_concurrent_queries > 0 && q.len() >= self.max_concurrent_queries {
            return Err(format!(
                "too many concurrent queries ({} running, limit {})",
                q.len(),
                self.max_concurrent_queries
            ));
        }
        let ctx = match want_id {
            Some(id) if !q.contains_key(&id) => {
                self.db.new_query_context_with_id(Some(id), timeout_ms)
            }
            _ => self.db.new_query_context(timeout_ms),
        };
        q.insert(
            ctx.query_id.clone(),
            QueryEntry {
                ctx: Arc::clone(&ctx),
                sql,
            },
        );
        Ok(ctx)
    }

    pub(crate) fn unregister_query(&self, id: &str) {
        if let Ok(mut q) = self.queries.lock() {
            q.remove(id);
        }
    }
}

/// 简易指标（prometheus 文本输出）。
#[derive(Default)]
pub struct Metrics {
    pub requests_total: AtomicU64,
    pub query_nanos: AtomicU64,
    pub query_count: AtomicU64,
    pub write_rows: AtomicU64,
    pub write_nanos: AtomicU64,
    pub write_count: AtomicU64,
    pub errors_total: AtomicU64,
    /// 内存池观测快照（每次渲染前由 handler 刷新）
    pub mem_budget: AtomicU64,
    pub mem_exec: AtomicU64,
    pub mem_cache: AtomicU64,
    /// 被并发闸拒绝（`TOO_MANY_QUERIES`）次数
    pub rejected_concurrency: AtomicU64,
    /// 被全局内存预算拒绝（`MEMORY_PRESSURE`）次数
    pub rejected_memory: AtomicU64,
}

impl Metrics {
    fn observe_query(&self, nanos: u64) {
        self.query_nanos.fetch_add(nanos, Ordering::Relaxed);
        self.query_count.fetch_add(1, Ordering::Relaxed);
    }
    fn observe_write(&self, nanos: u64, rows: u64) {
        self.write_nanos.fetch_add(nanos, Ordering::Relaxed);
        self.write_count.fetch_add(1, Ordering::Relaxed);
        self.write_rows.fetch_add(rows, Ordering::Relaxed);
    }
    fn render(&self, tables: usize, bytes: u64) -> String {
        let qc = self.query_count.load(Ordering::Relaxed);
        let wn = self.write_count.load(Ordering::Relaxed);
        format!(
            "# HELP qsdb_requests_total total requests\n# TYPE qsdb_requests_total counter\nqsdb_requests_total {}\n\
             # HELP qsdb_errors_total total errors\n# TYPE qsdb_errors_total counter\nqsdb_errors_total {}\n\
             # HELP qsdb_query_seconds_avg avg query latency\n# TYPE qsdb_query_seconds_avg gauge\nqsdb_query_seconds_avg {}\n\
             # HELP qsdb_query_total total queries\n# TYPE qsdb_query_total counter\nqsdb_query_total {}\n\
             # HELP qsdb_write_rows_total rows written\n# TYPE qsdb_write_rows_total counter\nqsdb_write_rows_total {}\n\
             # HELP qsdb_write_seconds_avg avg write latency\n# TYPE qsdb_write_seconds_avg gauge\nqsdb_write_seconds_avg {}\n\
             # HELP qsdb_tables tables\n# TYPE qsdb_tables gauge\nqsdb_tables {}\n\
             # HELP qsdb_store_bytes total store bytes\n# TYPE qsdb_store_bytes gauge\nqsdb_store_bytes {}\n\
             # HELP qsdb_memory_used_bytes memory in use (exec = materializing operators, cache = decoded LRU)\n# TYPE qsdb_memory_used_bytes gauge\nqsdb_memory_used_bytes{{pool=\"exec\"}} {}\nqsdb_memory_used_bytes{{pool=\"cache\"}} {}\n\
             # HELP qsdb_memory_budget_bytes global memory budget (0 = unlimited)\n# TYPE qsdb_memory_budget_bytes gauge\nqsdb_memory_budget_bytes {}\n\
             # HELP qsdb_queries_rejected_total queries rejected by admission control\n# TYPE qsdb_queries_rejected_total counter\nqsdb_queries_rejected_total{{reason=\"concurrency\"}} {}\nqsdb_queries_rejected_total{{reason=\"memory\"}} {}\n",
            self.requests_total.load(Ordering::Relaxed),
            self.errors_total.load(Ordering::Relaxed),
            if qc > 0 {
                self.query_nanos.load(Ordering::Relaxed) as f64 / qc as f64 / 1e9
            } else {
                0.0
            },
            qc,
            self.write_rows.load(Ordering::Relaxed),
            if wn > 0 {
                self.write_nanos.load(Ordering::Relaxed) as f64 / wn as f64 / 1e9
            } else {
                0.0
            },
            tables,
            bytes,
            self.mem_exec.load(Ordering::Relaxed),
            self.mem_cache.load(Ordering::Relaxed),
            self.mem_budget.load(Ordering::Relaxed),
            self.rejected_concurrency.load(Ordering::Relaxed),
            self.rejected_memory.load(Ordering::Relaxed),
        )
    }
}

/// 启动服务（阻塞至 Ctrl-C）。
#[allow(clippy::too_many_arguments)]
/// 服务器配置文件（可选 JSON）。
#[derive(serde::Deserialize, Default)]
pub struct ServerConfigFile {
    #[serde(default)]
    pub query_timeout_ms: Option<u64>,
    #[serde(default)]
    pub query_memory_limit_bytes: Option<u64>,
}

/// 单查询执行内存的兜底默认闸门（4GB）。
///
/// **为什么需要一个默认**：`QsdbOptions` 的库默认是 `None`（不限，嵌入方自控），
/// 而 server 是唯一会把多个查询叠在一起跑的部署面——不加默认就是"N 个查询
/// 各自无界"。4GB 只约束**物化算子**的记账（join build / sort spill / window /
/// udaf），流式路径不吃这个额度，故常规分析查询远够。
const DEFAULT_QUERY_MEMORY_LIMIT_BYTES: u64 = 4 * 1024 * 1024 * 1024;

#[allow(clippy::too_many_arguments)]
pub async fn serve(
    db: Arc<Qsdb>,
    addr: &str,
    token: Option<String>,
    auto_compact_secs: Option<u64>,
    max_concurrent_queries: Option<usize>,
    // 单查询执行内存闸门（字节；`Some(0)` = 显式不限，`None` = 未指定，交由
    // 配置文件 / 兜底默认决定）。
    query_memory_limit_bytes: Option<u64>,
    config_file: Option<&std::path::Path>,
    #[cfg_attr(not(feature = "flight"), allow(unused_variables))] flight_bind: Option<String>,
    #[cfg_attr(not(feature = "s3"), allow(unused_variables))] staging_upload_secs: Option<u64>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut cfg = ServerConfigFile::default();
    if let Some(path) = config_file {
        let text = std::fs::read_to_string(path)?;
        cfg = serde_json::from_str(&text)
            .map_err(|e| format!("parse config {}: {e}", path.display()))?;
        eprintln!("config file applied: {}", path.display());
    }
    let rt = db.runtime();
    if let Some(ms) = cfg.query_timeout_ms {
        rt.set_query_timeout_ms(ms);
    }
    // 单查询执行内存闸门（防 OOM 双闸之二）：CLI 显式值 > 配置文件 > 兜底默认。
    // 未经此闸时单查询执行内存无上界（`QsdbOptions` 默认 None = 不限），
    // 并发下即 N 倍无界内存——见 docs/configuration.md 内存模型一节。
    rt.set_query_memory_limit_bytes(
        query_memory_limit_bytes
            .or(cfg.query_memory_limit_bytes)
            .unwrap_or(DEFAULT_QUERY_MEMORY_LIMIT_BYTES),
    );
    let state = Arc::new(AppState::new(
        db,
        token,
        max_concurrent_queries.unwrap_or(0),
    ));

    // 写入走大 body（Arrow IPC 批）；默认 2MB 限制放大到 1GB
    let write_layer = axum::extract::DefaultBodyLimit::max(1024 * 1024 * 1024);
    let app = Router::new()
        .route("/health", get(health))
        .route("/metrics", get(metrics))
        .route("/api/v1/auth/login", post(auth_login))
        .route("/api/v1/error-codes", get(error_codes))
        .route("/api/v1/files", get(download_file))
        .route("/api/v1/query", post(query))
        .route("/api/v1/write", post(write))
        .route("/api/v1/tables", get(tables))
        .route("/api/v1/tables/{table}/schema", get(schema))
        .route("/api/v1/tables/{table}/partitions", get(partitions))
        .route("/api/v1/stats", get(stats))
        .route("/api/v1/queries", get(list_queries))
        .route("/api/v1/queries/{id}/cancel", post(cancel_query))
        .route("/api/v1/config", get(get_config).put(put_config))
        .layer(write_layer)
        .with_state(Arc::clone(&state));

    // staging 后台上传（M6-4，可选）：定时 flush_staged——启动即跑一轮
    // （崩溃恢复补传），错误记日志不中断
    let staging_task = staging_upload_secs.map(|secs| {
        let db = Arc::clone(&state.db);
        tokio::spawn(async move {
            let d0 = Arc::clone(&db);
            let _ = tokio::task::spawn_blocking(move || {
                if let Err(e) = d0.flush_staged() {
                    eprintln!("staging recovery flush error: {e}");
                }
            })
            .await;
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(secs.max(1)));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                let d = Arc::clone(&db);
                let _ = tokio::task::spawn_blocking(move || {
                    if let Err(e) = d.flush_staged() {
                        eprintln!("staging flush error: {e}");
                    }
                })
                .await;
            }
        })
    });

    // 自动 compaction（可选）
    let compact_task = auto_compact_secs.map(|secs| {
        let st = Arc::clone(&state);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(secs.max(1)));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                let st = Arc::clone(&st);
                let _ = tokio::task::spawn_blocking(move || {
                    if let Ok(names) = st.db.tables() {
                        for t in names {
                            let _ = st.db.compact_table(&t);
                        }
                    }
                })
                .await;
            }
        })
    });

    let listener = tokio::net::TcpListener::bind(addr).await?;
    // 回显**实际绑定**的地址（而非入参）：`--addr 127.0.0.1:0` 才能被用起来，
    // 测试/编排也能据此拿到真实端口。
    let bound = listener.local_addr()?;
    eprintln!(
        "qsdb server listening on http://{bound} (token auth: {})",
        if state.token.is_some() { "on" } else { "off" }
    );

    // Arrow Flight（feature-gated）：独立 h2c 口，与 HTTP 并行
    #[cfg(feature = "flight")]
    let flight_task = flight_bind.map(|bind| {
        // Flight 与 HTTP 共用同一份 AppState → 同一张查询注册表（取消 + 并发闸 + J11）
        let st = Arc::clone(&state);
        tokio::spawn(async move {
            if let Err(e) = crate::flight::serve_flight(st, &bind).await {
                eprintln!("flight server error: {e}");
            }
        })
    });

    let shutdown = async {
        let _ = tokio::signal::ctrl_c().await;
        eprintln!("shutting down…");
    };
    let result = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await;
    if let Some(t) = compact_task {
        t.abort();
    }
    if let Some(t) = staging_task {
        t.abort();
    }
    #[cfg(feature = "flight")]
    if let Some(t) = flight_task {
        t.abort();
    }
    // 退出前 flush 写缓冲
    let st = Arc::clone(&state);
    let _ = tokio::task::spawn_blocking(move || st.writer.flush_all()).await;
    result.map_err(Into::into)
}

// ---------------------------------------------------------------------------
// handlers
// ---------------------------------------------------------------------------

/// 认证结果：角色（None = 401）。
/// 优先级：Bearer 用户 token > Basic 用户名密码 > Bearer 超级 token > 超级 token 直连 > 开放。
fn auth_role(state: &AppState, headers: &HeaderMap) -> Option<qsdb::users::Role> {
    let auth_header = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);

    // 用户 token 表（登录后签发的 Bearer）
    if let Some(h) = &auth_header {
        if let Some(token) = h.strip_prefix("Bearer ") {
            if let Some(role) = state.tokens.lock().ok()?.get(token).copied() {
                return Some(role);
            }
        }
    }
    // Basic 用户名密码
    if let Some(h) = &auth_header {
        if let Some(b64) = h.strip_prefix("Basic ") {
            if let Some(role) = auth_basic(state, b64) {
                return Some(role);
            }
        }
    }
    // 超级 token（向后兼容：Admin 等效）
    match (&state.token, &auth_header) {
        (Some(expect), Some(h)) => {
            if h.strip_prefix("Bearer ")
                .map(|t| t == expect.as_str())
                .unwrap_or(false)
            {
                return Some(qsdb::users::Role::Admin);
            }
            None
        }
        (Some(_), None) => None,
        // 无 token 配置 = 用户体系关闭？若有用户文件则要求认证，否则开放
        (None, _) => {
            let has_users = qsdb::users::UserStore::read_all(state.db.store())
                .map(|u| !u.is_empty())
                .unwrap_or(false);
            if has_users {
                None // 有用户但未认证 → 401
            } else {
                Some(qsdb::users::Role::Admin) // 完全开放（现状兼容）
            }
        }
    }
}

fn auth_basic(state: &AppState, b64: &str) -> Option<qsdb::users::Role> {
    use base64::Engine;
    let decoded = base64::engine::general_purpose::STANDARD.decode(b64).ok()?;
    let text = String::from_utf8(decoded).ok()?;
    let (user, pass) = text.split_once(':')?;
    qsdb::users::UserStore::verify(state.db.store(), user, pass)
}

/// 兼容旧调用（仅查 401）。
fn auth(state: &AppState, headers: &HeaderMap) -> StatusCode {
    match auth_role(state, headers) {
        Some(_) => StatusCode::OK,
        None => StatusCode::UNAUTHORIZED,
    }
}

/// 生成随机 token（hex 32 字节）。
fn new_token() -> String {
    // base64(时间戳 ns + 计数器 + 进程启动随机盐)——足够随机且无新依赖
    use std::sync::atomic::AtomicU64;
    use std::time::Instant;
    static CTR: AtomicU64 = AtomicU64::new(0);
    static BOOT: std::sync::OnceLock<u128> = std::sync::OnceLock::new();
    let boot = *BOOT.get_or_init(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    });
    let c = CTR.fetch_add(1, Ordering::Relaxed);
    let now = Instant::now().elapsed().as_nanos();
    let seed = boot ^ (now << 64) ^ ((c as u128) << 32);
    use base64::Engine;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(seed.to_le_bytes())
}

/// 抽取器拒绝 → 契约错误体（`{error, code}`）。
///
/// axum 内建抽取器失败时回的是**纯文本**，违反 `docs/对接契约.md` §3.2 的
/// "错误体恒为 `{error, code}`"承诺——驱动拿不到机器码，只能按文案猜。
/// 现在一律 `400 INVALID_ARGUMENT`（422/415 一并归一到 400）：契约要求
/// **一个码恒定一个状态码**，抽取器拒绝不该另开状态码分支。
fn rejection_response(msg: String) -> Response {
    ErrorCode::InvalidArgument.response(msg)
}

/// `Json<T>`：拒绝时回结构化错误体（语义同 [`rejection_response`]）。
struct StructuredJson<T>(T);

impl<T, S> axum::extract::FromRequest<S> for StructuredJson<T>
where
    T: serde::de::DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = Response;

    async fn from_request(req: axum::extract::Request, state: &S) -> Result<Self, Self::Rejection> {
        match Json::<T>::from_request(req, state).await {
            Ok(Json(v)) => Ok(StructuredJson(v)),
            Err(rej) => Err(rejection_response(rej.body_text())),
        }
    }
}

/// `Query<T>`：同上（query string 缺参 / 类型错）。
///
/// 实现的是 **FromRequestParts**（只读 URI，不碰 body）——否则会与 handler 末尾
/// 的 body 抽取器（`Bytes`）争"消费请求"，写端点直接不是合法 Handler。
struct StructuredQuery<T>(T);

impl<T, S> axum::extract::FromRequestParts<S> for StructuredQuery<T>
where
    T: serde::de::DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = Response;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        state: &S,
    ) -> Result<Self, Self::Rejection> {
        match Query::<T>::from_request_parts(parts, state).await {
            Ok(Query(v)) => Ok(StructuredQuery(v)),
            Err(rej) => Err(rejection_response(rej.body_text())),
        }
    }
}

/// 查询串布尔参数的**严格**解析：只认这份清单，其余一律 400。
///
/// 此前 `?withSchema=maybe` 会静默当 false（走了 N+1 却不报错）——静默失败在
/// 协议面比报错危险得多。`""`（`?withSchema` 裸传）= true，按旗标惯例。
fn parse_bool_param(name: &str, v: Option<&str>) -> std::result::Result<bool, String> {
    let Some(raw) = v else { return Ok(false) };
    match raw.trim().to_ascii_lowercase().as_str() {
        "" | "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        _ => Err(format!(
            "invalid {name}: expect 1/0/true/false/yes/no/on/off (got {raw:?})"
        )),
    }
}

/// POST /api/v1/auth/login：Basic 认证 → 签发 Bearer token（进程内存态）。
async fn auth_login(State(s): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    let Some(role) = auth_role(&s, &headers) else {
        return ErrorCode::Unauthorized.response("invalid credentials");
    };
    let token = new_token();
    if let Ok(mut t) = s.tokens.lock() {
        t.insert(token.clone(), role);
    }
    Json(serde_json::json!({
        "token": token,
        "role": role.name(),
        "note": "process-lifetime token; restart requires re-login"
    }))
    .into_response()
}

/// GET /api/v1/files?path=<相对 db root 的路径>——物理复制源（qsdb-sync F11）。
/// 路径白名单：禁止 `_trash`/`_filecache`/`..`；其余（manifest/segment/table.json/
/// partitions.idx）均可下载。配合 read_only 模式即成为标准复制源。
///
/// **流式返回**（qsdb-sync 回灌：此前是 `fs::read` 整文件进内存再返回，段多大就吃
/// 多少——复制大段会把源端打爆）。现在按 8KB 块边读边发，源端内存与段大小无关。
/// `Content-Length` 照给（从 `metadata()` 拿），所以客户端仍能校验完整性；
/// 中途读失败就断开连接、**响应体短于 Content-Length**，客户端据此可判为失败重试。
#[derive(serde::Deserialize)]
struct FileParams {
    path: String,
}

async fn download_file(
    State(s): State<Arc<AppState>>,
    headers: HeaderMap,
    StructuredQuery(p): StructuredQuery<FileParams>,
) -> Response {
    let Some(role) = auth_role(&s, &headers) else {
        return ErrorCode::Unauthorized.response("unauthorized");
    };
    if !role.is_admin() {
        return ErrorCode::Forbidden.response("file download requires admin");
    }
    // 路径安全检查
    let rel = p.path.replace('\\', "/");
    if rel.contains("..") || rel.starts_with('/') {
        return ErrorCode::InvalidArgument.response("invalid path");
    }
    if rel.starts_with("_trash/") || rel.starts_with("_filecache/") || rel.starts_with("_spill/") {
        return ErrorCode::Forbidden.response("internal paths not downloadable");
    }
    let full = s.db.root().join(&rel);
    // 目录要先挡掉：`File::open` 在 Linux 上能打开目录，读起来报 EISDIR，
    // 那时响应头已经发出去了 —— 只能变成"截断的 200"，很难排查。
    let md = match tokio::fs::metadata(&full).await {
        Ok(m) if m.is_file() => m,
        Ok(_) => return ErrorCode::NotFound.response(format!("not a file: {rel}")),
        Err(e) => return ErrorCode::NotFound.response(format!("file not found: {e}")),
    };
    let f = match tokio::fs::File::open(&full).await {
        Ok(f) => f,
        Err(e) => return ErrorCode::NotFound.response(format!("file not found: {e}")),
    };
    let mut resp =
        axum::body::Body::from_stream(tokio_util::io::ReaderStream::new(f)).into_response();
    let h = resp.headers_mut();
    h.insert(
        axum::http::header::CONTENT_TYPE,
        axum::http::HeaderValue::from_static("application/octet-stream"),
    );
    if let Ok(v) = axum::http::HeaderValue::from_str(&md.len().to_string()) {
        h.insert(axum::http::header::CONTENT_LENGTH, v);
    }
    resp
}

/// 把内存池的实时占用刷进指标原子（渲染前调一次；池本身在 `Qsdb` 上）。
fn refresh_mem_gauges(s: &AppState) {
    let pool = s.db.memory_pool();
    s.metrics.mem_budget.store(pool.budget(), Ordering::Relaxed);
    s.metrics
        .mem_exec
        .store(pool.exec_used(), Ordering::Relaxed);
    s.metrics
        .mem_cache
        .store(pool.cache_used(), Ordering::Relaxed);
}

/// GET /api/v1/error-codes：错误码目录（机器可读，由 [`ErrorCode`] 直接产出）。
///
/// 目录是**静态**的（不含实例信息、不含凭证），故与 `/health`、`/metrics` 同口径
/// **不鉴权**：驱动冷启动即可据此自配置 `code → SQLState/重试策略` 映射，
/// 不必把码表硬编码进驱动——内核加码时驱动自动跟上。
async fn error_codes() -> Response {
    let codes: Vec<serde_json::Value> = ErrorCode::ALL.iter().map(|c| c.entry()).collect();
    Json(serde_json::json!({
        "version": 1,
        "doc": "docs/error-codes.md",
        "stability": "只增不改：已发布码的语义与 HTTP 状态码永久冻结；error 文案不做兼容承诺",
        "codes": codes,
    }))
    .into_response()
}

async fn health(State(_s): State<Arc<AppState>>) -> impl IntoResponse {
    Json(serde_json::json!({ "status": "ok", "version": env!("CARGO_PKG_VERSION") }))
}

async fn metrics(State(s): State<Arc<AppState>>) -> Response {
    refresh_mem_gauges(&s);
    let (tables, bytes) = match tokio::task::spawn_blocking({
        let db = Arc::clone(&s.db);
        move || db.stats()
    })
    .await
    {
        Ok(Ok((t, b))) => (t, b),
        _ => (0, 0),
    };
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4",
        )],
        s.metrics.render(tables, bytes),
    )
        .into_response()
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)] // 拼错键（"formatt"）→ 400，不静默走默认值
struct QueryBody {
    sql: String,
    #[serde(default)]
    format: Option<String>,
}

/// J11：客户端透传 query id 的合法性。放行 `[A-Za-z0-9._:-]`、长度 `1..=64`
/// ——够容纳 UUID / ULID / KSUID / 驱动自有前缀，又不至于让 id 变成日志、
/// 指标或注册表 key 的注入载体。非法值一律 400（**不静默忽略**）。
pub(crate) fn valid_client_query_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 64
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b':'))
}

async fn query(
    State(s): State<Arc<AppState>>,
    headers: HeaderMap,
    StructuredJson(body): StructuredJson<QueryBody>,
) -> Response {
    let Some(role) = auth_role(&s, &headers) else {
        return ErrorCode::Unauthorized.response("unauthorized");
    };
    s.metrics.requests_total.fetch_add(1, Ordering::Relaxed);
    // format 值校验（拼错如 "arrow" 此前静默回 JSON——静默失败在协议面危险）
    let fmt = match body.format.as_deref() {
        None => "json".to_string(),
        Some(f) if f.eq_ignore_ascii_case("json") => "json".to_string(),
        Some(f) if f.eq_ignore_ascii_case("ipc") => "ipc".to_string(),
        // **无依赖二进制**（生态工具：Java 侧不想引 arrow-java）——见 docs/bulk-format.md
        Some(f) if f.eq_ignore_ascii_case("bulk") => "bulk".to_string(),
        // **无依赖行式**（驱动向：JDBC/ODBC/Python 按行取值，可流式）——见 docs/rows-format.md
        Some(f) if f.eq_ignore_ascii_case("rows") => "rows".to_string(),
        // 文本（任何语言零依赖）：列序 = 结果 schema，首行表头
        Some(f) if f.eq_ignore_ascii_case("csv") => "csv".to_string(),
        Some(f) => {
            s.metrics.errors_total.fetch_add(1, Ordering::Relaxed);
            return ErrorCode::InvalidArgument.response(format!(
                "invalid format: expect json|ipc|bulk|rows|csv (got {f:?})"
            ));
        }
    };
    // 会话：每请求一个 QueryContext（超时可经 X-QSDB-Timeout 头覆盖）
    let timeout_ms: Option<u64> = headers
        .get("x-qsdb-timeout-ms")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse().ok());
    // J11：客户端可在执行前指定 query id，从而零扫描、零误伤地取消自己的查询。
    // 非法值**显式 400**——静默忽略正是 J6 那条 serde alias 坑的教训。
    let want_id = match headers.get("x-qsdb-query-id") {
        None => None,
        Some(v) => match v.to_str().ok().filter(|s| valid_client_query_id(s)) {
            Some(id) => Some(id.to_string()),
            None => {
                s.metrics.errors_total.fetch_add(1, Ordering::Relaxed);
                return ErrorCode::InvalidArgument
                    .response("invalid X-QSDB-Query-Id: expect 1..=64 chars of [A-Za-z0-9._:-]");
            }
        },
    };
    let ctx = match s.register_query(want_id, timeout_ms, body.sql.clone()) {
        Ok(ctx) => ctx,
        Err(msg) => {
            s.metrics.errors_total.fetch_add(1, Ordering::Relaxed);
            // 专属 code：同为 503 还有"只读库拒绝写"（UNAVAILABLE），驱动必须能
            // 区分「并发满 → 可退避重试」与「只读 → 永不重试」。
            s.metrics
                .rejected_concurrency
                .fetch_add(1, Ordering::Relaxed);
            return ErrorCode::TooManyQueries.response(msg);
        }
    };
    let query_id = ctx.query_id.clone();
    let reg = Arc::clone(&s);
    let t = Instant::now();
    let st = Arc::clone(&s);
    let sql = body.sql;
    let ctx_exec = Arc::clone(&ctx);
    let result =
        tokio::task::spawn_blocking(move || st.db.sql_with_role_and_context(&sql, role, &ctx_exec))
            .await;
    reg.unregister_query(&query_id);
    let elapsed = t.elapsed().as_nanos() as u64;
    s.metrics.observe_query(elapsed);
    // 响应头回声用（下面的 JSON 分支会把 query_id 移走）
    let echo_id = query_id.clone();
    let mut resp = match result {
        Ok(Ok(r)) => match r {
            SqlResult::Query { schema, batches } => {
                let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                if fmt.eq_ignore_ascii_case("ipc") {
                    match batches_to_ipc(&schema, &batches) {
                        Ok(bytes) => (
                            [(
                                axum::http::header::CONTENT_TYPE,
                                axum::http::HeaderValue::from_static(
                                    "application/vnd.apache.arrow.stream",
                                ),
                            )],
                            bytes,
                        )
                            .into_response(),
                        Err(e) => {
                            s.metrics.errors_total.fetch_add(1, Ordering::Relaxed);
                            ErrorCode::Internal.response(e.to_string())
                        }
                    }
                } else if fmt.eq_ignore_ascii_case("bulk") {
                    // **无依赖二进制**（读侧；与写侧同一格式）
                    match qsdb::bulk::encode(&schema, &batches) {
                        Ok(bytes) => (
                            [(
                                axum::http::header::CONTENT_TYPE,
                                axum::http::HeaderValue::from_static("application/vnd.qsdb.bulk"),
                            )],
                            bytes,
                        )
                            .into_response(),
                        Err(e) => {
                            s.metrics.errors_total.fetch_add(1, Ordering::Relaxed);
                            ErrorCode::Unsupported.response(e.to_string())
                        }
                    }
                } else if fmt.eq_ignore_ascii_case("rows") {
                    // **无依赖行式**（读侧；列名/类型随头一起发，客户端逐块消费）
                    match qsdb::rows::encode(&schema, &batches) {
                        Ok(bytes) => (
                            [(
                                axum::http::header::CONTENT_TYPE,
                                axum::http::HeaderValue::from_static("application/vnd.qsdb.rows"),
                            )],
                            bytes,
                        )
                            .into_response(),
                        Err(e) => {
                            s.metrics.errors_total.fetch_add(1, Ordering::Relaxed);
                            ErrorCode::Unsupported.response(e.to_string())
                        }
                    }
                } else if fmt.eq_ignore_ascii_case("csv") {
                    // 文本 CSV（读侧；列序 = schema，首行表头）
                    match batches_to_csv(&schema, &batches) {
                        Ok(bytes) => (
                            [(
                                axum::http::header::CONTENT_TYPE,
                                axum::http::HeaderValue::from_static("text/csv"),
                            )],
                            bytes,
                        )
                            .into_response(),
                        Err(e) => {
                            s.metrics.errors_total.fetch_add(1, Ordering::Relaxed);
                            ErrorCode::Internal.response(e.to_string())
                        }
                    }
                } else {
                    match batches_to_json(&schema, &batches) {
                        Ok(v) => Json(serde_json::json!({
                            "queryId": query_id,
                            "schema": schema_fields(&schema),
                            "rows": v,
                            "rowCount": rows,
                            "elapsedMs": elapsed as f64 / 1e6,
                        }))
                        .into_response(),
                        Err(e) => {
                            s.metrics.errors_total.fetch_add(1, Ordering::Relaxed);
                            ErrorCode::Internal.response(e.to_string())
                        }
                    }
                }
            }
            SqlResult::Ok {
                message,
                rows_affected,
                command,
            } => {
                // **结构化回执**（J12）：`command` + `rowsAffected` 让驱动不必解析
                // `message` 文案（码表文档明确"文案不做兼容承诺"）。
                // 字段只在有意义时出现（DDL 无行数）——老客户端读 `message` 不受影响。
                let mut body = serde_json::json!({
                    "queryId": query_id,
                    "message": message,
                    "elapsedMs": elapsed as f64 / 1e6,
                });
                if let Some(c) = command {
                    body["command"] = serde_json::json!(c);
                }
                if let Some(n) = rows_affected {
                    body["rowsAffected"] = serde_json::json!(n);
                }
                Json(body).into_response()
            }
        },
        Ok(Err(e)) => {
            s.metrics.errors_total.fetch_add(1, Ordering::Relaxed);
            note_memory_rejection(&s, &e);
            status_of_query(&e, e.to_string())
        }
        Err(e) => ErrorCode::Internal.response(e.to_string()),
    };
    // J4/J11：**所有**已注册查询的响应（JSON 通道、IPC 通道、失败/超时/取消）
    // 统一回显**实际使用**的 query id——客户端透传的 id 若因冲突被回退，驱动
    // 据此处回声即可发现自己拿到的不是原值。
    if let Ok(v) = axum::http::HeaderValue::from_str(&echo_id) {
        resp.headers_mut()
            .insert(axum::http::HeaderName::from_static("x-qsdb-query-id"), v);
    }
    resp
}

#[derive(serde::Deserialize)]
struct WriteParams {
    table: String,
    #[serde(default)]
    mode: Option<String>,
    #[serde(default)]
    flush: Option<bool>,
    /// `mode=overwrite` 的 **CAS 期望分区 generation**（F24）。
    /// 只支持**单分区**覆盖；不传 = 现状（无条件覆盖）。
    /// 同时接受 `expectedGeneration`（camelCase 别名，兼容生态客户端习惯）。
    #[serde(default, alias = "expectedGeneration")]
    expected_generation: Option<u64>,
    /// body 编码：`ipc`（默认，Arrow IPC 流）/ `bulk`（无依赖列式，见
    /// `docs/bulk-format.md`）/ `rows`（无依赖**行式**，见 `docs/rows-format.md`；
    /// 驱动按行取值、可流式，schema 由表定义给）/ `csv`（文本，首行表头可选）。
    /// 四种的列序一律 = **表列序**。生态工具（Java 等不想引 arrow 库）用后三者。
    #[serde(default)]
    format: Option<String>,
}

async fn write(
    State(s): State<Arc<AppState>>,
    headers: HeaderMap,
    StructuredQuery(p): StructuredQuery<WriteParams>,
    body: axum::body::Bytes,
) -> Response {
    if let StatusCode::UNAUTHORIZED = auth(&s, &headers) {
        return ErrorCode::Unauthorized.response("unauthorized");
    }
    if s.db.read_only() {
        return ErrorCode::ReadOnly.response("database is read-only");
    }
    s.metrics.requests_total.fetch_add(1, Ordering::Relaxed);
    let t = Instant::now();
    // mode 值校验（在解码 body 之前——拼错的值此前静默按 append 处理）
    let mode = match p.mode.as_deref() {
        None => "append".to_string(),
        Some(m) if m.eq_ignore_ascii_case("append") => "append".to_string(),
        Some(m) if m.eq_ignore_ascii_case("overwrite") => "overwrite".to_string(),
        Some(m) => {
            s.metrics.errors_total.fetch_add(1, Ordering::Relaxed);
            return ErrorCode::InvalidArgument
                .response(format!("invalid mode: expect append|overwrite (got {m:?})"));
        }
    };
    // CAS 只对 overwrite 有意义（append 是锁内 read-merge-write，天然合并语义）
    if p.expected_generation.is_some() && mode != "overwrite" {
        s.metrics.errors_total.fetch_add(1, Ordering::Relaxed);
        return ErrorCode::InvalidArgument.response(
            "expected_generation only applies to mode=overwrite (append merges under the lock)",
        );
    }
    // body 解码（同步 CPU 工作，直接做）：ipc（默认）/ bulk / csv
    let fmt = p.format.as_deref().unwrap_or("ipc").to_ascii_lowercase();
    let batches = match fmt.as_str() {
        "ipc" => match ipc_to_batches(&body) {
            Ok(b) => b,
            Err(e) => {
                s.metrics.errors_total.fetch_add(1, Ordering::Relaxed);
                return ErrorCode::InvalidIpc.response(e.to_string());
            }
        },
        "bulk" => match qsdb::bulk::decode(&body) {
            Ok(b) => vec![b],
            Err(e) => {
                s.metrics.errors_total.fetch_add(1, Ordering::Relaxed);
                return ErrorCode::InvalidArgument.response(e.to_string());
            }
        },
        "rows" => {
            // 行式（驱动向）：**schema 由表定义给**（payload 只带值，`flags=0`）——
            // 见 docs/rows-format.md §四。列序 = 表列序（与 /write 全列批约定一致）。
            let def = match s.db.table_def(&p.table) {
                Ok(d) => d,
                Err(e) => {
                    s.metrics.errors_total.fetch_add(1, Ordering::Relaxed);
                    return ErrorCode::from_qsdb(&e, true).response(e.to_string());
                }
            };
            let schema = qsdb::write::full_schema(&def);
            match qsdb::rows::decode_with_schema(&body, &schema) {
                Ok(b) => b,
                Err(e) => {
                    s.metrics.errors_total.fetch_add(1, Ordering::Relaxed);
                    return ErrorCode::InvalidArgument.response(e.to_string());
                }
            }
        }
        "csv" => {
            // CSV body：列序 = **表列序**（与 `/write` 的既有约定一致：全列批）。
            // 首行若是表头（与表列名一致）则跳过。
            let def = match s.db.table_def(&p.table) {
                Ok(d) => d,
                Err(e) => {
                    s.metrics.errors_total.fetch_add(1, Ordering::Relaxed);
                    return ErrorCode::from_qsdb(&e, true).response(e.to_string());
                }
            };
            match csv_body_to_batches(&body, &def) {
                Ok(b) => b,
                Err(e) => {
                    s.metrics.errors_total.fetch_add(1, Ordering::Relaxed);
                    return ErrorCode::InvalidArgument.response(e.to_string());
                }
            }
        }
        other => {
            s.metrics.errors_total.fetch_add(1, Ordering::Relaxed);
            return ErrorCode::InvalidArgument.response(format!(
                "invalid format: expect ipc|bulk|rows|csv (got {other:?})"
            ));
        }
    };
    let mode_out = mode.clone();
    let st = Arc::clone(&s);
    let table = p.table.clone();
    let flush = p.flush.unwrap_or(true);
    let expected_generation = p.expected_generation;
    let result = tokio::task::spawn_blocking(move || -> Result<u64, QsdbError> {
        match mode.as_str() {
            "overwrite" => {
                let def = st.db.table_def(&table)?;
                let batch = concat_batches(&batches)?;
                let groups = qsdb::partition::split_by_partition(&batch, &def)?;
                // CAS 是**单分区**语义：一次请求命中多个分区时，"期望 generation"
                // 没有唯一所指 —— 显式拒绝而不是挑一个用（拼错就报错，不静默兜底）
                if let Some(_exp) = expected_generation {
                    if groups.len() > 1 {
                        return Err(QsdbError::invalid(format!(
                            "expected_generation requires a single-partition overwrite, but this request targets {} partitions",
                            groups.len()
                        )));
                    }
                }
                let mut rows = 0u64;
                for (vals, data) in groups {
                    let names: Vec<(&str, qsdb::Literal)> = def
                        .partition_keys
                        .iter()
                        .zip(vals.iter())
                        .map(|(k, v)| (k.as_str(), v.clone()))
                        .collect();
                    // **把分区列补回**：`Qsdb::overwrite_partition` 的约定是**全列批**
                    // （库里自己按分区列路由并剥离）——直接把上面剥过的 `data` 喂进去，
                    // 库里会再剥一次 → `batch lacks partition column`。
                    // 这个坑是本分支长期存在的：**HTTP `mode=overwrite` 一直 400**
                    // （embed 用户传全列批所以从未暴露；先前的契约测试也没覆盖 overwrite）。
                    let full = qsdb::partition::attach_partition_columns(&def, &names, &data)?;
                    let r = match expected_generation {
                        Some(exp) => {
                            st.db.overwrite_partition_cas(&table, &names, &[full], exp)?
                        }
                        None => st.db.overwrite_partition(&table, &names, &[full])?,
                    };
                    rows += r.rows;
                }
                Ok(rows)
            }
            "append" => {
                st.writer.push(&table, &batches)?;
                if flush {
                    let r = st.writer.flush(&table)?;
                    Ok(r.rows)
                } else {
                    Ok(batches.iter().map(|b| b.num_rows() as u64).sum())
                }
            }
            other => Err(QsdbError::invalid(format!(
                "unknown mode '{other}' (expect append|overwrite)"
            ))),
        }
    })
    .await;
    let elapsed = t.elapsed().as_nanos() as u64;
    match result {
        Ok(Ok(rows)) => {
            s.metrics.observe_write(elapsed, rows);
            Json(serde_json::json!({
                "rows": rows,
                "mode": mode_out,
                "elapsedMs": elapsed as f64 / 1e6,
            }))
            .into_response()
        }
        Ok(Err(e)) => {
            s.metrics.errors_total.fetch_add(1, Ordering::Relaxed);
            status_of_query(&e, e.to_string())
        }
        Err(e) => ErrorCode::Internal.response(e.to_string()),
    }
}

/// 表列定义 → JSON（`/schema` 与 `/tables?withSchema=1` 共用）。
fn columns_json(def: &qsdb::catalog::TableDef) -> Vec<serde_json::Value> {
    def.columns
        .iter()
        .map(|c| {
            serde_json::json!({
                "name": c.name,
                "type": c.logical_type.to_string(),
                "nullable": c.nullable,
                "partition": def.partition_keys.contains(&c.name),
            })
        })
        .collect()
}

#[derive(serde::Deserialize, Default)]
struct TablesQuery {
    /// `?withSchema=1`（或 true）：一并返回每表列定义（J6，消除 getColumns 的 N+1）
    #[serde(default, alias = "withSchema")]
    with_schema: Option<String>,
}

async fn tables(
    State(s): State<Arc<AppState>>,
    headers: HeaderMap,
    StructuredQuery(q): StructuredQuery<TablesQuery>,
) -> Response {
    if let StatusCode::UNAUTHORIZED = auth(&s, &headers) {
        return ErrorCode::Unauthorized.response("unauthorized");
    }
    let want_schema = match parse_bool_param("withSchema", q.with_schema.as_deref()) {
        Ok(v) => v,
        Err(msg) => {
            s.metrics.errors_total.fetch_add(1, Ordering::Relaxed);
            return ErrorCode::InvalidArgument.response(msg);
        }
    };
    let st = Arc::clone(&s);
    match tokio::task::spawn_blocking(move || {
        let list = st.db.tables()?;
        if !want_schema {
            return Ok(serde_json::json!({ "tables": list }));
        }
        // J6：JDBC getColumns 原先要 N+1 次请求（列表 + 逐表 schema）。
        // 万级表名下首次连接翻库表明显慢，一次返回列集即可。
        let mut out = Vec::with_capacity(list.len());
        for t in &list {
            let cols = match st.db.table_def(t) {
                Ok(def) => columns_json(&def),
                Err(_) => Vec::new(), // 并发 DROP 等：跳过而非整体失败
            };
            out.push(serde_json::json!({ "name": t, "columns": cols }));
        }
        Ok(serde_json::json!({ "tables": out }))
    })
    .await
    {
        Ok(Ok(v)) => Json(v).into_response(),
        Ok(Err(e)) => status_of(&e, e.to_string()),
        Err(e) => ErrorCode::Internal.response(e.to_string()),
    }
}

async fn schema(
    State(s): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(table): Path<String>,
) -> Response {
    if let StatusCode::UNAUTHORIZED = auth(&s, &headers) {
        return ErrorCode::Unauthorized.response("unauthorized");
    }
    let table_out = table.clone();
    let st = Arc::clone(&s);
    match tokio::task::spawn_blocking(move || st.db.table_def(&table)).await {
        Ok(Ok(def)) => {
            let cols = columns_json(&def);
            Json(serde_json::json!({ "table": table_out, "columns": cols })).into_response()
        }
        Ok(Err(e)) => status_of(&e, e.to_string()),
        Err(e) => ErrorCode::Internal.response(e.to_string()),
    }
}

async fn partitions(
    State(s): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(table): Path<String>,
) -> Response {
    if let StatusCode::UNAUTHORIZED = auth(&s, &headers) {
        return ErrorCode::Unauthorized.response("unauthorized");
    }
    let st = Arc::clone(&s);
    let res = tokio::task::spawn_blocking(move || st.db.partition_stats(&table)).await;
    match res {
        Ok(Ok(stats)) => {
            // 契约形态：partitions = 名单（兼容旧消费方）；detail = 每分区
            // generation/rows/segments（qsdb-sync 轮询增量判据，idx 快路径）
            let parts: Vec<String> = stats.iter().map(|p| p.name.clone()).collect();
            let detail: Vec<serde_json::Value> = stats
                .iter()
                .map(|p| {
                    serde_json::json!({
                        "name": p.name,
                        "generation": p.generation,
                        "rows": p.rows,
                        "segments": p.segments,
                    })
                })
                .collect();
            Json(serde_json::json!({ "partitions": parts, "count": parts.len(), "detail": detail }))
                .into_response()
        }
        Ok(Err(e)) => status_of(&e, e.to_string()),
        Err(e) => ErrorCode::Internal.response(e.to_string()),
    }
}

async fn stats(State(s): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    if let StatusCode::UNAUTHORIZED = auth(&s, &headers) {
        return ErrorCode::Unauthorized.response("unauthorized");
    }
    let st = Arc::clone(&s);
    let res = tokio::task::spawn_blocking(move || {
        let (tables, bytes) = st.db.stats()?;
        let buffered: Vec<_> = st
            .writer
            .buffered()
            .into_iter()
            .map(|(t, b, by)| serde_json::json!({"table": t, "batches": b, "bytes": by}))
            .collect();
        qsdb::Result::Ok(serde_json::json!({
            "tables": tables,
            "storeBytes": bytes,
            "writeBuffer": buffered,
        }))
    })
    .await;
    match res {
        Ok(Ok(v)) => Json(v).into_response(),
        Ok(Err(e)) => status_of(&e, e.to_string()),
        Err(e) => ErrorCode::Internal.response(e.to_string()),
    }
}

async fn get_config(State(s): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    if let StatusCode::UNAUTHORIZED = auth(&s, &headers) {
        return ErrorCode::Unauthorized.response("unauthorized");
    }
    let rt = s.db.runtime();
    let opts = s.db.options();
    Json(serde_json::json!({
        // 运行期（热更生效）
        "runtime": {
            "queryTimeoutMs": rt.query_timeout_ms().unwrap_or(0),
            "queryMemoryLimitBytes": rt.query_memory_limit_bytes().unwrap_or(0),
        },
        // 启动期快照（只读，改动需重启）
        "startup": {
            "scanCacheBytes": opts.scan_cache_bytes,
            "trashGraceMs": opts.trash_grace_ms,
            "spillDir": s.db.spill_dir().map(|p| p.display().to_string()).unwrap_or_default(),
        },
        // 全局内存账本（实时）
        "memory": {
            "budgetBytes": s.db.memory_pool().budget(),
            "execUsedBytes": s.db.memory_pool().exec_used(),
            "cacheUsedBytes": s.db.memory_pool().cache_used(),
            "cacheEvictedBytes": s.db.cache_evicted_bytes(),
            "note": "budget=0 表示不限；压力下先挤兑 cache（纯缓存），再拒查询（503 MEMORY_PRESSURE）",
        },
        // 服务器级
        "server": {
            "maxConcurrentQueries": s.max_concurrent_queries,
        },
        "_hint": "PUT /api/v1/config 可热更 runtime 节；startup/server 节需重启",
    }))
    .into_response()
}

#[derive(serde::Deserialize, Default)]
struct ConfigUpdate {
    // snake_case 为规范名（与 `--config` 文件同形）；**camelCase 别名是必需的**：
    // `GET /api/v1/config` 回的就是 camelCase，不别名则"读-改-写"式整轮 PUT 被
    // **静默忽略**（实测 2026-10-12：PUT {"queryTimeoutMs":12345} → HTTP 200 +
    // applied:true 而值纹丝不动）。契约文档与 GET 的键形态均以 camelCase 为准。
    #[serde(default, alias = "queryTimeoutMs")]
    query_timeout_ms: Option<u64>,
    #[serde(default, alias = "queryMemoryLimitBytes")]
    query_memory_limit_bytes: Option<u64>,
}

async fn put_config(
    State(s): State<Arc<AppState>>,
    headers: HeaderMap,
    StructuredJson(body): StructuredJson<ConfigUpdate>,
) -> Response {
    if let StatusCode::UNAUTHORIZED = auth(&s, &headers) {
        return ErrorCode::Unauthorized.response("unauthorized");
    }
    let rt = s.db.runtime();
    if let Some(ms) = body.query_timeout_ms {
        rt.set_query_timeout_ms(ms);
    }
    if let Some(bytes) = body.query_memory_limit_bytes {
        rt.set_query_memory_limit_bytes(bytes);
    }
    // `applied` = **确实落了至少一个已知字段**（空 body 或全未知/拼错键 → false）。
    // 恒返回 true 会在客户端拼错键时给出"成功"的假信号。
    let applied = body.query_timeout_ms.is_some() || body.query_memory_limit_bytes.is_some();
    Json(serde_json::json!({
        "queryTimeoutMs": rt.query_timeout_ms().unwrap_or(0),
        "queryMemoryLimitBytes": rt.query_memory_limit_bytes().unwrap_or(0),
        "applied": applied,
    }))
    .into_response()
}

async fn list_queries(State(s): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    if let StatusCode::UNAUTHORIZED = auth(&s, &headers) {
        return ErrorCode::Unauthorized.response("unauthorized");
    }
    let entries: Vec<serde_json::Value> = match s.queries.lock() {
        Ok(q) => q
            .iter()
            .map(|(id, e)| {
                serde_json::json!({
                    "id": id,
                    "sql": e.sql,
                    "elapsedMs": e.ctx.elapsed().as_millis() as u64,
                    "memoryBytes": e.ctx.memory_used(),
                    "cancelled": e.ctx.is_cancelled(),
                })
            })
            .collect(),
        Err(_) => {
            return ErrorCode::Internal.response("registry poisoned");
        }
    };
    Json(serde_json::json!({ "queries": entries, "count": entries.len() })).into_response()
}

async fn cancel_query(
    State(s): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    if let StatusCode::UNAUTHORIZED = auth(&s, &headers) {
        return ErrorCode::Unauthorized.response("unauthorized");
    }
    match s.queries.lock() {
        Ok(q) => match q.get(&id) {
            Some(e) => {
                e.ctx.cancel();
                Json(serde_json::json!({ "cancelled": id })).into_response()
            }
            None => ErrorCode::NotFound.response(format!("query {id} not running")),
        },
        Err(_) => ErrorCode::Internal.response("registry poisoned"),
    }
}

// ---------------------------------------------------------------------------
// 辅助
// ---------------------------------------------------------------------------

/// 错误 → 响应（**资源端点**语义：`/tables`、`/schema`、`/partitions`、`/stats`）。
/// 这些端点上 404 是准确的「资源不存在」。
fn status_of(e: &QsdbError, msg: String) -> Response {
    ErrorCode::from_qsdb(e, false).response(msg)
}

/// 错误 → 响应（**查询/写入端点**语义）。
///
/// J5：`/api/v1/query` 上的 `NotFound`（表/列不存在）是 **SQL 语义错误**，
/// 不是 HTTP 资源语义——返回 404 会让网关误判为路由错误。改 400，
/// 并用 code `TABLE_NOT_FOUND` / `COLUMN_NOT_FOUND` 便于驱动映射 SQLState。
fn status_of_query(e: &QsdbError, msg: String) -> Response {
    ErrorCode::from_qsdb(e, true).response(msg)
}

/// 计一次「被内存预算拒绝」（供 `qsdb_queries_rejected_total{reason="memory"}`）。
fn note_memory_rejection(s: &AppState, e: &QsdbError) {
    if matches!(e, QsdbError::MemoryPressure(_)) {
        s.metrics.rejected_memory.fetch_add(1, Ordering::Relaxed);
    }
}

// 错误 → 响应/状态码的映射**只有一处**：`error_code::ErrorCode::from_qsdb`
// （删掉了 `map_err` + `status_code_of` 这组状态派生兜底——它们能让响应发出
// `BAD_REQUEST` 这类目录外的码，且"列不存在"曾靠 `msg.contains("column")`
// 嗅探文案判定，表名含 column 即误判）。

fn batches_to_ipc(
    schema: &arrow::datatypes::SchemaRef,
    batches: &[RecordBatch],
) -> Result<Vec<u8>, QsdbError> {
    let mut buf = Vec::new();
    {
        let mut w = StreamWriter::try_new(&mut buf, schema)
            .map_err(|e| QsdbError::invalid(format!("ipc writer: {e}")))?;
        for b in batches {
            w.write(b)
                .map_err(|e| QsdbError::invalid(format!("ipc write: {e}")))?;
        }
        w.finish()
            .map_err(|e| QsdbError::invalid(format!("ipc finish: {e}")))?;
    }
    Ok(buf)
}

/// CSV body → 全列批（`/write?format=csv`）。
///
/// 列序 = **表列序**（与 `/write` 的既有全列批约定一致）；首行若与表列名逐字相同则
/// 视为表头跳过。类型按表列类型解析（**不做类型推断**——推断会把 `001` 变成 `1`，
/// 这类静默改变在写入面不可接受）。
fn csv_body_to_batches(
    bytes: &[u8],
    def: &qsdb::catalog::TableDef,
) -> Result<Vec<RecordBatch>, QsdbError> {
    let text = std::str::from_utf8(bytes)
        .map_err(|e| QsdbError::invalid(format!("csv body is not UTF-8: {e}")))?;
    let mut lines = text.lines().peekable();
    // 表头（可选）
    if let Some(first) = lines.peek() {
        let cells: Vec<&str> = first.split(',').map(|s| s.trim()).collect();
        let is_header = cells.len() == def.columns.len()
            && cells.iter().zip(&def.columns).all(|(c, col)| {
                c.eq_ignore_ascii_case(&col.name) || c == &format!("\"{}\"", col.name)
            });
        if is_header {
            lines.next();
        }
    }
    let rows: Vec<Vec<qsdb::Literal>> = lines
        .filter(|l| !l.trim().is_empty())
        .map(|l| {
            let cells = split_csv_line(l);
            if cells.len() != def.columns.len() {
                return Err(QsdbError::invalid(format!(
                    "csv row has {} fields, table {} has {} columns",
                    cells.len(),
                    def.name,
                    def.columns.len()
                )));
            }
            cells
                .iter()
                .zip(&def.columns)
                .map(|(c, col)| parse_csv_cell(c, &col.logical_type))
                .collect::<std::result::Result<Vec<_>, QsdbError>>()
        })
        .collect::<std::result::Result<Vec<_>, QsdbError>>()?;
    Ok(vec![qsdb::sql::rows_to_batch(def, &rows)?])
}

/// CSV 行切分（引号内的逗号不算分隔；`""` = 字面引号）。
fn split_csv_line(line: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut in_q = false;
    let mut chars = line.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '"' if in_q => {
                if chars.peek() == Some(&'"') {
                    cur.push('"');
                    chars.next();
                } else {
                    in_q = false;
                }
            }
            '"' if !in_q => in_q = true,
            ',' if !in_q => out.push(std::mem::take(&mut cur)),
            _ => cur.push(c),
        }
    }
    out.push(cur);
    out
}

/// 单个 CSV 字段 → 该列类型的 `Literal`（空串 = NULL——写入面唯一的空值表示）。
fn parse_csv_cell(s: &str, ty: &qsdb::LogicalType) -> Result<qsdb::Literal, QsdbError> {
    if s.is_empty() {
        return Ok(qsdb::Literal::Null);
    }
    qsdb::partition::parse_partition_value(s, ty)
}

fn ipc_to_batches(bytes: &[u8]) -> Result<Vec<RecordBatch>, QsdbError> {
    let mut reader = StreamReader::try_new(std::io::Cursor::new(bytes.to_vec()), None)
        .map_err(|e| QsdbError::invalid(format!("ipc reader: {e}")))?;
    let mut out = Vec::new();
    while let Some(b) = reader
        .next()
        .transpose()
        .map_err(|e| QsdbError::invalid(format!("ipc read: {e}")))?
    {
        if b.num_rows() > 0 {
            out.push(b);
        }
    }
    Ok(out)
}

fn concat_batches(batches: &[RecordBatch]) -> Result<RecordBatch, QsdbError> {
    if batches.len() == 1 {
        return Ok(batches[0].clone());
    }
    let schema = batches[0].schema();
    let mut cols: Vec<arrow::array::ArrayRef> = Vec::with_capacity(schema.fields().len());
    for c in 0..schema.fields().len() {
        let parts: Vec<&dyn arrow::array::Array> =
            batches.iter().map(|b| b.column(c).as_ref()).collect();
        cols.push(
            arrow::compute::concat(&parts)
                .map_err(|e| QsdbError::invalid(format!("concat: {e}")))?,
        );
    }
    let n: usize = batches.iter().map(|b| b.num_rows()).sum();
    RecordBatch::try_new_with_options(
        schema,
        cols,
        &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(n)),
    )
    .map_err(|e| QsdbError::invalid(format!("concat batch: {e}")))
}

/// 结果批 → CSV 文本（读侧 `format=csv`）：列序 = schema，首行表头。
///
/// 给"零依赖客户端"用（Java 拼 CSV 谁都会）。批量场景仍建议 `bulk`/`rows`。
///
/// **零行结果也要有表头**（J15 同族）：arrow 的 CSV writer 在**第一次 write** 时
/// 才吐表头，一批都没有就一个字节都不写 —— 客户端拿到 0 字节 body 就丢了列名。
/// `LIMIT 0` / `WHERE false` 是 BI 工具探测元数据的标准姿势，故这里补一个空批
/// 把表头逼出来（转义交给 arrow，不自写一套）。
fn batches_to_csv(
    schema: &arrow::datatypes::SchemaRef,
    batches: &[RecordBatch],
) -> Result<Vec<u8>, QsdbError> {
    let mut out = Vec::with_capacity(1024);
    let empty;
    let batches = if batches.iter().any(|b| b.num_rows() > 0) {
        batches
    } else {
        empty = [RecordBatch::new_empty(Arc::clone(schema))];
        &empty[..]
    };
    {
        let mut w = arrow::csv::WriterBuilder::new()
            .with_header(true)
            .build(&mut out);
        for b in batches {
            w.write(b)
                .map_err(|e| QsdbError::invalid(format!("csv write: {e}")))?;
        }
    }
    Ok(out)
}

fn schema_fields(schema: &arrow::datatypes::SchemaRef) -> Vec<serde_json::Value> {
    schema
        .fields()
        .iter()
        .map(|f| serde_json::json!({ "name": f.name(), "type": f.data_type().to_string() }))
        .collect()
}

/// 每行每列按 **Arrow 列类型**分派输出（J1+J2，2026-10-12）。
///
/// 原实现忽略列类型，统一走「display 串 → parse::<i64> → parse::<f64> →
/// true/false」的数字启发，造成两类正确性问题：
/// - **字符串列的数值内容被改写为 number**（`"123"` → `123`），消费方无法
///   区分「字符串 123」与「整数 123」；Decimal 走 f64 丢精度（F20）
/// - **BINARY 列彻底不可逆**：display 给出 hex 串 `"6465616462656566"`，
///   启发按**十进制**解析成 6465616462656566 —— 字节内容已换血
///
/// 现按类型分派：数值输出 number、精确类型（Decimal）输出字符串、
/// 二进制输出小写 hex 字符串、其余按各自规范形态。
fn batches_to_json(
    _schema: &arrow::datatypes::SchemaRef,
    batches: &[RecordBatch],
) -> Result<Vec<Vec<serde_json::Value>>, QsdbError> {
    let mut rows = Vec::new();
    for b in batches {
        for i in 0..b.num_rows() {
            let mut row = Vec::with_capacity(b.num_columns());
            for c in 0..b.num_columns() {
                let arr = b.column(c);
                if arr.is_null(i) {
                    row.push(serde_json::Value::Null);
                } else {
                    row.push(json_value_typed(arr.as_ref(), i)?);
                }
            }
            rows.push(row);
        }
    }
    Ok(rows)
}

/// 单值按 Arrow 类型 → JSON（见 `batches_to_json` 的类型分派说明）。
fn json_value_typed(
    arr: &dyn arrow::array::Array,
    i: usize,
) -> Result<serde_json::Value, QsdbError> {
    use arrow::array::{
        BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
        Int8Array, Int16Array, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray,
        StringArray, Time64NanosecondArray, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray,
    };
    use arrow::datatypes::DataType as D;
    let disp = || {
        arrow::util::display::array_value_to_string(arr, i)
            .map_err(|e| QsdbError::invalid(format!("display: {e}")))
    };
    Ok(match arr.data_type() {
        // 整数：i64 内恒精确 → number
        D::Int8 => serde_json::json!(arr.as_any().downcast_ref::<Int8Array>().unwrap().value(i)),
        D::Int16 => serde_json::json!(arr.as_any().downcast_ref::<Int16Array>().unwrap().value(i)),
        D::Int32 => serde_json::json!(arr.as_any().downcast_ref::<Int32Array>().unwrap().value(i)),
        D::Int64 => serde_json::json!(arr.as_any().downcast_ref::<Int64Array>().unwrap().value(i)),
        // 浮点：非有限值固定拼写（J7：生态惯用 Infinity，非 Rust 的 inf）
        D::Float32 => {
            let v = arr
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(i) as f64;
            finite_or_string(v)
        }
        D::Float64 => {
            let v = arr
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(i);
            finite_or_string(v)
        }
        // 精确小数：字符串输出（彻底解决 F20 的 f64 中转精度边界，零成本）。
        // 必须用 display 串——它按列 scale 插入小数点；`.value(i)` 是 unscaled
        // 原始整数（1.5 会输出 "15"）。
        D::Decimal128(..) => {
            let _ = arr.as_any().downcast_ref::<Decimal128Array>();
            serde_json::json!(disp()?)
        }
        // 字符串：**恒 string**（不因内容是数字而改写形态）
        D::Utf8 => serde_json::json!(arr.as_any().downcast_ref::<StringArray>().unwrap().value(i)),
        D::LargeUtf8 => serde_json::json!(
            arr.as_any()
                .downcast_ref::<LargeStringArray>()
                .unwrap()
                .value(i)
        ),
        D::Boolean => serde_json::json!(
            arr.as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(i)
        ),
        // 二进制：恒小写 hex 字符串（display 即 hex；J2 修复点）
        D::Binary => serde_json::json!(hex_lower(
            arr.as_any().downcast_ref::<BinaryArray>().unwrap().value(i)
        )),
        D::LargeBinary => serde_json::json!(hex_lower(
            arr.as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap()
                .value(i)
        )),
        // 时间族：规范形态字符串
        D::Date32 => {
            let d = arr.as_any().downcast_ref::<Date32Array>().unwrap().value(i);
            serde_json::json!(qsdb::types::temporal::format_date(d))
        }
        D::Time64(_) => serde_json::json!(qsdb::types::temporal::format_time_ns(
            arr.as_any()
                .downcast_ref::<Time64NanosecondArray>()
                .unwrap()
                .value(i)
        )),
        D::Timestamp(unit, tz) => {
            let ns = match unit {
                arrow::datatypes::TimeUnit::Second => {
                    arr.as_any()
                        .downcast_ref::<TimestampSecondArray>()
                        .unwrap()
                        .value(i)
                        * 1_000_000_000
                }
                arrow::datatypes::TimeUnit::Millisecond => {
                    arr.as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .unwrap()
                        .value(i)
                        * 1_000_000
                }
                arrow::datatypes::TimeUnit::Microsecond => {
                    arr.as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .unwrap()
                        .value(i)
                        * 1_000
                }
                arrow::datatypes::TimeUnit::Nanosecond => arr
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap()
                    .value(i),
            };
            let s = qsdb::types::temporal::format_timestamp_ns(ns);
            // 带时区：RFC3339（T 分隔 + Z）；naive：空格分隔（JDBC 惯用）
            serde_json::json!(if tz.is_some() {
                format!("{}Z", s.replacen(' ', "T", 1))
            } else {
                s.replacen(' ', "T", 1)
            })
        }
        // 其余（含嵌套/未列类型）：display 兜底为字符串
        other => {
            let _ = other;
            serde_json::json!(disp()?)
        }
    })
}

/// 有限浮点 → number；NaN/±Inf → 固定拼写字符串（J7）。
fn finite_or_string(v: f64) -> serde_json::Value {
    if v.is_nan() {
        serde_json::json!("NaN")
    } else if v.is_infinite() {
        serde_json::json!(if v > 0.0 { "Infinity" } else { "-Infinity" })
    } else {
        serde_json::json!(v)
    }
}

/// 字节 → 小写 hex（BINARY 的 JSON 表示，J2）。
fn hex_lower(b: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(b.len() * 2);
    for x in b {
        s.push(HEX[(x >> 4) as usize] as char);
        s.push(HEX[(x & 0x0f) as usize] as char);
    }
    s
}

// Body 引用占位（保持 import；axum 0.8 body 直接用 Bytes）
#[allow(unused)]
fn _body_hint(_: Body) {}

// HashMap 引用占位
#[allow(unused)]
fn _hm_hint(_: HashMap<String, String>) {}
