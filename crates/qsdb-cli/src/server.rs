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
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use qsdb::sql::SqlResult;
use qsdb::write_buffer::{WriteBuffer, WriteBufferOptions};
use qsdb::{Qsdb, QsdbError};

use crate::error_code::ErrorCode;

/// 运行中查询注册条目。
pub struct QueryEntry {
    pub ctx: Arc<qsdb::query::QueryContext>,
    pub sql: String,
    /// 归属会话（十六批 M-A）。None = Basic / 超级 token / 开放模式的请求
    /// （它们没有登录会话，不可被"踢"）。会话的**在跑查询**不冗余存进
    /// Session——注册表是唯一真源，`GET /api/v1/sessions` 反查同源，
    /// 防两处记账漂移。
    pub session: Option<Arc<Session>>,
}

/// 会话累计统计（原子，登录后全程无锁更新）。
#[derive(Default)]
pub struct SessionStats {
    /// 发起的查询总数（含 SQL 写入；注册时计）
    pub queries_total: AtomicU64,
    /// 写入行数累计（`/write` 端点 + SQL DML 的 rowsAffected）
    pub rows_written: AtomicU64,
}

/// 会话（十六批 M-A）：登录签发，进程内存态，重启即失效（重登零成本）。
///
/// 判定用角色**永远现查**（`role_at_login` 只是展示快照——M-C 库级 grants
/// 落地后管理员改权限要立即生效，这是 M7-A 既有纪律）。
pub struct Session {
    /// 服务端生成，与 token 1:1
    pub id: String,
    /// 登录用户名（审计与展示；`__token__` = 超级凭证，`anonymous` = 开放模式）。
    /// **角色不存会话**（M-C）：判定与展示都现查 grants——管理员改了立即生效。
    pub user: String,
    /// ip:port + User-Agent（登录时定格）
    pub client: String,
    /// 创建时刻（epoch ms，展示用）
    pub created_at_ms: u64,
    /// 最近活跃（epoch ms；**每个认证请求 touch 一次**，一次 `store(Relaxed)`）
    pub last_active_ms: AtomicU64,
    /// 会话默认库（`USE` 的落点；M-A 单库恒 "default"，M-B 多库由挂载表校验）
    pub default_db: std::sync::Mutex<String>,
    pub stats: SessionStats,
}

impl Session {
    fn epoch_ms() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    /// 空闲毫秒数（现在 − 最近活跃）。
    fn idle_ms(&self) -> u64 {
        Self::epoch_ms().saturating_sub(self.last_active_ms.load(Ordering::Relaxed))
    }

    /// 当前默认库（克隆出锁）。
    fn default_db(&self) -> String {
        self.default_db
            .lock()
            .map(|d| d.clone())
            .unwrap_or_default()
    }

    /// 会话条目的 JSON 形态（列表与详情共用）。`running` 由注册表反查注入；
    /// `role` 由调用方**现查 grants** 注入（None = 当前无授权，回 null）。
    fn to_json(&self, running: Vec<String>, role: Option<qsdb::users::Role>) -> serde_json::Value {
        serde_json::json!({
            "id": self.id,
            "user": self.user,
            "role": role.map(|r| serde_json::json!(r.name())).unwrap_or(serde_json::Value::Null),
            "client": self.client,
            "createdAtMs": self.created_at_ms,
            "lastActiveMs": self.last_active_ms.load(Ordering::Relaxed),
            "idleMs": self.idle_ms(),
            "defaultDb": self.default_db(),
            "runningQueries": running,
            "queriesTotal": self.stats.queries_total.load(Ordering::Relaxed),
            "rowsWritten": self.stats.rows_written.load(Ordering::Relaxed),
        })
    }
}

/// 服务共享状态。
pub struct AppState {
    /// 提交事件环形缓冲（T2）。钩子在**开库时**就装好了（`main.rs`），
    /// 这里只是持一份用来服务 `GET /api/v1/watch`。
    pub commits: Arc<crate::commit_log::CommitLog>,
    /// 会话表（token → session；登录签发，进程内存态）。
    /// 十六批 M-A：由 `token → Role` 升级而来，同一把锁、同一处签发/吊销。
    pub sessions: std::sync::Mutex<HashMap<String, Arc<Session>>>,
    /// 挂载表（十六批 M-B）：库名（小写）→ 挂载。恒含 `"default"`（来自 `--path`）。
    /// 会话默认库 / 限定名路由 / grants 键都按这份白名单校验。
    pub mounts: HashMap<String, Mount>,
    /// 默认挂载句柄（= `mounts["default"].db`；用户存储 / `/files` / Flight 面）。
    pub db: Arc<Qsdb>,
    pub token: Option<String>,
    pub metrics: Metrics,
    /// 运行中查询（query_id → entry）
    pub queries: std::sync::Mutex<HashMap<String, QueryEntry>>,
    /// 并发查询上限（0 = 不限）
    pub max_concurrent_queries: usize,
    /// 结果护栏·行数上限（十七批 R1；0 = 不限）。检查点在**执行后、序列化前**，
    /// 超限恒定 400 `RESULT_TOO_LARGE`（绝不返回部分数据）。落 AppState 而非
    /// 各挂载 runtime——护栏管的是**传输体量**（server 面），不是某个库的执行；
    /// 原子 = `PUT /api/v1/config` 热更（与并发闸同族）。
    pub max_result_rows: AtomicU64,
    /// 结果护栏·体量上限（字节，按 Arrow 数组内存估算；0 = 不限）。估算口径：
    /// JSON 序列化后实际体积可达估算的 1.5~3×（字符串转义/数字展开）——要精确
    /// 护栏用行数上限。
    pub max_result_bytes: AtomicU64,
}

/// 一个挂载库（M-B）：句柄 + 该句柄**自己的写缓冲**（预算/缓存/锁随句柄天然隔离）。
pub struct Mount {
    pub db: Arc<Qsdb>,
    pub writer: WriteBuffer,
}

impl AppState {
    /// 组装共享状态（HTTP 与 Flight 两面共用同一份；Flight 的单测也走这里）。
    /// 多挂载形态（M-B）：`("名字", 句柄)` 列表；**必须含** `"default"`
    /// （单库场景传一项 `("default", 句柄)`——与历史行为逐位一致）。
    pub(crate) fn with_mounts(
        mount_list: Vec<(String, Arc<Qsdb>)>,
        token: Option<String>,
        commits: Arc<crate::commit_log::CommitLog>,
        max_concurrent_queries: usize,
    ) -> Self {
        let mut mounts = HashMap::new();
        for (name, db) in mount_list {
            let writer = db.writer(WriteBufferOptions::default());
            mounts.insert(name, Mount { db, writer });
        }
        let db = Arc::clone(&mounts.get("default").expect("default mount is required").db);
        Self {
            commits,
            sessions: std::sync::Mutex::new(HashMap::new()),
            mounts,
            db,
            token,
            metrics: Metrics::default(),
            queries: std::sync::Mutex::new(HashMap::new()),
            max_concurrent_queries,
            max_result_rows: AtomicU64::new(0),
            max_result_bytes: AtomicU64::new(0),
        }
    }

    /// 挂载名清单（排序，展示/文案用）。
    pub(crate) fn mount_names(&self) -> Vec<String> {
        let mut v: Vec<String> = self.mounts.keys().cloned().collect();
        v.sort();
        v
    }

    /// 建查询上下文并注册为"运行中"——**并发限额与 id 归属都在同一把锁内定**。
    ///
    /// `want_id` = 客户端经 `X-QSDB-Query-Id` 透传的建议 id（契约 J11）：
    /// 未被占用则原样采用（驱动即可零扫描地 cancel）；**已被运行中查询占用则
    /// 回退服务端生成**——不报错、不顶掉在跑的那条，调用方以返回的 ctx 为准
    /// （响应回显实际 id）。
    ///
    /// `session` = 归属会话（注册时计一次 `queriesTotal`；踢人靠注册表反查）。
    pub(crate) fn register_query(
        &self,
        db: &Arc<Qsdb>,
        want_id: Option<String>,
        timeout_ms: Option<u64>,
        sql: String,
        session: Option<&Arc<Session>>,
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
        // 上下文建在**目标挂载**上（M-B）：内存池 / 超时 / 单查询配额随句柄走
        let ctx = match want_id {
            Some(id) if !q.contains_key(&id) => db.new_query_context_with_id(Some(id), timeout_ms),
            _ => db.new_query_context(timeout_ms),
        };
        if let Some(s) = session {
            s.stats.queries_total.fetch_add(1, Ordering::Relaxed);
        }
        q.insert(
            ctx.query_id.clone(),
            QueryEntry {
                ctx: Arc::clone(&ctx),
                sql,
                session: session.cloned(),
            },
        );
        Ok(ctx)
    }

    pub(crate) fn unregister_query(&self, id: &str) {
        if let Ok(mut q) = self.queries.lock() {
            q.remove(id);
        }
    }

    /// **踢会话**：移出会话表 + 取消它在跑的全部查询（复用既有 cancel 路径）。
    /// 返回取消条数；`None` = 会话不存在。空闲回收与 `DELETE /sessions/{id}` 同走这里。
    pub(crate) fn revoke_session(&self, id: &str) -> Option<usize> {
        let mut map = self.sessions.lock().ok()?;
        let token = map.iter().find(|(_, s)| s.id == id)?.0.clone();
        map.remove(&token);
        drop(map);
        let mut cancelled = 0;
        if let Ok(q) = self.queries.lock() {
            for e in q.values() {
                if e.session.as_ref().is_some_and(|s| s.id == id) {
                    e.ctx.cancel();
                    cancelled += 1;
                }
            }
        }
        Some(cancelled)
    }
}

/// 登录签发的会话 token 前缀。带此前缀却不在会话表 = 已被踢 / 服务重启过
/// → 客户端拿到 401 `SESSION_REVOKED`（别再重试这把 token），而不是笼统的
/// `UNAUTHORIZED`。
const SESSION_TOKEN_PREFIX: &str = "sess-";

fn new_session_id() -> String {
    format!("s-{}", new_token())
}

fn new_session_token() -> String {
    format!("{SESSION_TOKEN_PREFIX}{}", new_token())
}

/// 空闲回收扫描（`--session-idle-timeout-mins` 的周期体；独立成函数供单测注入）。
/// 超时会话一律走 [`AppState::revoke_session`]（取消在跑查询 + 吊销 token）。
pub(crate) fn sweep_idle_sessions(state: &AppState, timeout: std::time::Duration) -> usize {
    let limit_ms = timeout.as_millis() as u64;
    let expired: Vec<String> = match state.sessions.lock() {
        Ok(m) => m
            .values()
            .filter(|s| s.idle_ms() > limit_ms)
            .map(|s| s.id.clone())
            .collect(),
        Err(_) => return 0,
    };
    expired
        .iter()
        .filter_map(|id| state.revoke_session(id))
        .count()
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
    /// 被结果护栏拒绝（`RESULT_TOO_LARGE`）次数（十七批 R1）
    pub rejected_result_size: AtomicU64,
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
    fn render(&self, tables: usize, bytes: u64, sessions: usize) -> String {
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
             # HELP qsdb_sessions current login sessions\n# TYPE qsdb_sessions gauge\nqsdb_sessions {}\n\
             # HELP qsdb_memory_used_bytes memory in use (exec = materializing operators, cache = decoded LRU)\n# TYPE qsdb_memory_used_bytes gauge\nqsdb_memory_used_bytes{{pool=\"exec\"}} {}\nqsdb_memory_used_bytes{{pool=\"cache\"}} {}\n\
             # HELP qsdb_memory_budget_bytes global memory budget (0 = unlimited)\n# TYPE qsdb_memory_budget_bytes gauge\nqsdb_memory_budget_bytes {}\n\
             # HELP qsdb_queries_rejected_total queries rejected by admission control\n# TYPE qsdb_queries_rejected_total counter\nqsdb_queries_rejected_total{{reason=\"concurrency\"}} {}\nqsdb_queries_rejected_total{{reason=\"memory\"}} {}\nqsdb_queries_rejected_total{{reason=\"result_size\"}} {}\n",
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
            sessions,
            self.mem_exec.load(Ordering::Relaxed),
            self.mem_cache.load(Ordering::Relaxed),
            self.mem_budget.load(Ordering::Relaxed),
            self.rejected_concurrency.load(Ordering::Relaxed),
            self.rejected_memory.load(Ordering::Relaxed),
            self.rejected_result_size.load(Ordering::Relaxed),
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
    /// 结果护栏（十七批 R1）：行数 / 体量（字节）。与 CLI 同名项同族——
    /// CLI 显式值优先。
    #[serde(default)]
    pub max_result_rows: Option<u64>,
    #[serde(default)]
    pub max_result_bytes: Option<u64>,
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
    mounts: Vec<(String, Arc<Qsdb>)>,
    addr: &str,
    token: Option<String>,
    // 提交事件缓冲（T2；`GET /api/v1/watch` 的后端）
    commits: Arc<crate::commit_log::CommitLog>,
    auto_compact_secs: Option<u64>,
    max_concurrent_queries: Option<usize>,
    // 会话空闲回收（分钟；`Some(0)`/`None` = 永不回收，设计文档 §3.3 默认关）
    session_idle_timeout_mins: Option<u64>,
    // 单查询执行内存闸门（字节；`Some(0)` = 显式不限，`None` = 未指定，交由
    // 配置文件 / 兜底默认决定）。
    query_memory_limit_bytes: Option<u64>,
    // 结果护栏（十七批 R1）：行数 / 体量（MB；`None` = 未指定，交由配置文件 /
    // 默认 0 = 关）。默认关 ⇒ 升级零行为变更。
    max_result_rows: Option<u64>,
    max_result_mb: Option<u64>,
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
    let state = Arc::new(AppState::with_mounts(
        mounts,
        token,
        commits,
        max_concurrent_queries.unwrap_or(0),
    ));
    // 运行期配置（超时/单查询配额）落**默认挂载**的 runtime；M-B 各挂载独立
    // runtime，运行期热更按同一值刷全部（见 put_config 的说明）。
    let db = Arc::clone(&state.db);
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
    // 结果护栏（十七批 R1）：**默认关**（0 = 不限）——升级零行为变更；部署方
    // 显式给了才守。CLI 显式值 > 配置文件 > 0。落 AppState 原子（热更入口见
    // put_config），查询端点在执行后、序列化前检查。
    state.max_result_rows.store(
        max_result_rows.or(cfg.max_result_rows).unwrap_or(0),
        Ordering::Relaxed,
    );
    state.max_result_bytes.store(
        max_result_mb
            .map(|mb| mb.saturating_mul(1024 * 1024))
            .or(cfg.max_result_bytes)
            .unwrap_or(0),
        Ordering::Relaxed,
    );

    // 写入走大 body（Arrow IPC 批）；默认 2MB 限制放大到 1GB
    let write_layer = axum::extract::DefaultBodyLimit::max(1024 * 1024 * 1024);
    let app = Router::new()
        .route("/health", get(health))
        .route("/metrics", get(metrics))
        .route("/api/v1/auth/login", post(auth_login))
        .route("/api/v1/error-codes", get(error_codes))
        .route("/api/v1/files", get(download_file))
        .route("/api/v1/users/export", get(export_users))
        .route("/api/v1/users/{name}/grants", put(put_user_grants))
        .route("/api/v1/query", post(query))
        .route("/api/v1/write", post(write))
        .route("/api/v1/tables", get(tables))
        .route("/api/v1/tables/{table}/schema", get(schema))
        .route("/api/v1/tables/{table}/partitions", get(partitions))
        .route("/api/v1/stats", get(stats))
        .route("/api/v1/queries", get(list_queries))
        .route("/api/v1/watch", get(watch))
        .route("/api/v1/queries/{id}/cancel", post(cancel_query))
        // 会话（十六批 M-A）：静态段先于 `{id}` 匹配，`current` 不会被参数路由吞掉
        .route("/api/v1/databases", get(list_databases))
        .route("/api/v1/sessions", get(list_sessions))
        .route(
            "/api/v1/sessions/current/database",
            put(set_session_database),
        )
        .route(
            "/api/v1/sessions/{id}",
            get(get_session).delete(kick_session),
        )
        .route("/api/v1/config", get(get_config).put(put_config))
        .layer(write_layer)
        .with_state(Arc::clone(&state));

    // staging 后台上传（M6-4，可选）：定时 flush_staged——启动即跑一轮
    // （崩溃恢复补传），错误记日志不中断
    let staging_task = staging_upload_secs.map(|secs| {
        let st0 = Arc::clone(&state);
        tokio::spawn(async move {
            // 多挂载（M-B）：每个挂载自己的 staging；启动先补传一轮（崩溃恢复）
            let _ = tokio::task::spawn_blocking({
                let st0 = Arc::clone(&st0);
                move || {
                    for m in st0.mounts.values() {
                        if let Err(e) = m.db.flush_staged() {
                            eprintln!("staging recovery flush error: {e}");
                        }
                    }
                }
            })
            .await;
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(secs.max(1)));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                let st = Arc::clone(&st0);
                let _ = tokio::task::spawn_blocking(move || {
                    for m in st.mounts.values() {
                        if let Err(e) = m.db.flush_staged() {
                            eprintln!("staging flush error: {e}");
                        }
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
                    for m in st.mounts.values() {
                        if let Ok(names) = m.db.tables() {
                            for t in names {
                                let _ = m.db.compact_table(&t);
                            }
                        }
                    }
                })
                .await;
            }
        })
    });

    // 会话空闲回收（可选，默认关）：周期扫 `last_active_at`，超时会话按
    // "踢人"路径清理（取消在跑查询 + 吊销 token）。默认关的理由：踢错活跃
    // 会话的代价大于泄漏代价（会话对象 ~百字节级，泄漏是审计噪音不是内存问题）。
    let idle_task = session_idle_timeout_mins.filter(|&m| m > 0).map(|mins| {
        let st = Arc::clone(&state);
        let timeout = std::time::Duration::from_secs(mins.saturating_mul(60));
        // 扫描周期 = min(60s, 超时/2)，下限 1s——最长滞后半个超时窗
        let period = (timeout / 2).min(std::time::Duration::from_secs(60));
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(period.max(std::time::Duration::from_secs(1)));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                let st = Arc::clone(&st);
                let _ = tokio::task::spawn_blocking(move || {
                    let n = sweep_idle_sessions(&st, timeout);
                    if n > 0 {
                        eprintln!("swept {n} idle session(s)");
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
    // ConnectInfo：登录端点要定格客户端 ip:port（Session.client）
    let result = axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown)
    .await;
    if let Some(t) = compact_task {
        t.abort();
    }
    if let Some(t) = staging_task {
        t.abort();
    }
    if let Some(t) = idle_task {
        t.abort();
    }
    #[cfg(feature = "flight")]
    if let Some(t) = flight_task {
        t.abort();
    }
    // 退出前 flush 写缓冲（多挂载：全部）
    let st = Arc::clone(&state);
    let _ = tokio::task::spawn_blocking(move || {
        for m in st.mounts.values() {
            let _ = m.writer.flush_all();
        }
    })
    .await;
    result.map_err(Into::into)
}

// ---------------------------------------------------------------------------
// handlers
// ---------------------------------------------------------------------------

/// 认证结果（十六批 **四态**）：
/// - `Pass`：通过。`session` = 登录签发的会话（Basic / 超级 token / 开放模式为 None）；
/// - `Deny`：未认证（缺凭证 / 凭证无效 / 用户已被删）→ 401 `UNAUTHORIZED`；
/// - `Revoked`：**会话形 token**（`sess-` 前缀）已不在会话表（被踢 / 空闲回收 /
///   服务重启）→ 401 `SESSION_REVOKED`——客户端该重新登录，别再重试这把 token；
/// - `NoGrant`：凭证有效但对该库无授权（M-C 白名单制）→ 403 `FORBIDDEN`——
///   与"未认证"分开：重试和重登都没用，得找管理员授权。
enum Authed {
    Pass {
        role: qsdb::users::Role,
        session: Option<Arc<Session>>,
    },
    Deny,
    Revoked,
    NoGrant {
        user: String,
        db: String,
    },
}

/// 库级授权判定（设计稿 §二 四行规则的落点；**每请求现查**——管理员改了
/// grants 立即生效，角色不缓存进会话）。
fn resolve_def(def: qsdb::users::UserDef, db: &str, session: Option<Arc<Session>>) -> Authed {
    match def.role_for(db) {
        Some(role) => Authed::Pass { role, session },
        None => Authed::NoGrant {
            user: def.name,
            db: db.to_string(),
        },
    }
}

/// 按用户名现查（会话请求走这里；`__token__`/`anonymous` 是隐式 Admin）。
fn resolve_user(state: &AppState, user: &str, db: &str, session: Option<Arc<Session>>) -> Authed {
    if user == "__token__" || user == "anonymous" {
        return Authed::Pass {
            role: qsdb::users::Role::Admin,
            session,
        };
    }
    match qsdb::users::UserStore::read_all(state.db.store())
        .ok()
        .and_then(|m| m.get(user).cloned())
    {
        Some(def) => resolve_def(def, db, session),
        // 登录后用户被删 → 凭证已无效（重登也没用，除非重建同名用户）
        None => Authed::Deny,
    }
}

/// 展示用现查（`/sessions` 列表；不做身份判定，只回答"现在是什么角色"）。
fn resolved_role_for(state: &AppState, user: &str, db: &str) -> Option<qsdb::users::Role> {
    if user == "__token__" || user == "anonymous" {
        return Some(qsdb::users::Role::Admin);
    }
    qsdb::users::UserStore::read_all(state.db.store())
        .ok()
        .and_then(|m| m.get(user).cloned())
        .and_then(|def| def.role_for(db))
}

/// 认证入口。优先级：Bearer 会话 token > Basic 用户名密码 > Bearer 超级 token > 开放。
/// 命中会话时**顺带 touch `last_active_at`**（一次 `store(Relaxed)`，无锁竞争面）。
/// 请求目标库 = 会话默认库（M-A 的 USE 落点）；无会话 = `default`。
fn authenticate(state: &AppState, headers: &HeaderMap) -> Authed {
    let auth_header = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);

    if let Some(h) = &auth_header {
        if let Some(token) = h.strip_prefix("Bearer ") {
            // 会话表（登录后签发的 Bearer）：命中即 touch 并带回会话
            let hit = state
                .sessions
                .lock()
                .ok()
                .and_then(|m| m.get(token).cloned());
            if let Some(s) = hit {
                s.last_active_ms
                    .store(Session::epoch_ms(), Ordering::Relaxed);
                let user = s.user.clone();
                let db = s.default_db();
                return resolve_user(state, &user, &db, Some(s));
            }
            // 会话形 token 却不在表里 = 已吊销（专属码，别落进笼统 401）
            if token.starts_with(SESSION_TOKEN_PREFIX) {
                return Authed::Revoked;
            }
        }
    }
    // Basic 用户名密码（无会话：每请求现校验，~5ms 哈希 + 现查 grants）。
    // 校验失败**落穿**到超级 token / 开放检查（与既有行为一致：完全开放的服务器
    // 忽略凭证形态；有用户的服务器会在开放检查里 Deny）。
    if let Some(h) = &auth_header {
        if let Some(b64) = h.strip_prefix("Basic ") {
            if let Some((user, pass)) = basic_credentials(b64) {
                if let Some(def) =
                    qsdb::users::UserStore::verify_def(state.db.store(), &user, &pass)
                {
                    return resolve_def(def, "default", None);
                }
            }
        }
    }
    // 超级 token（向后兼容：Admin 等效；无会话）
    match (&state.token, &auth_header) {
        (Some(expect), Some(h)) => {
            if h.strip_prefix("Bearer ")
                .map(|t| t == expect.as_str())
                .unwrap_or(false)
            {
                Authed::Pass {
                    role: qsdb::users::Role::Admin,
                    session: None,
                }
            } else {
                Authed::Deny
            }
        }
        (Some(_), None) => Authed::Deny,
        // 无 token 配置 = 用户体系关闭？若有用户文件则要求认证，否则开放
        (None, _) => {
            let has_users = qsdb::users::UserStore::read_all(state.db.store())
                .map(|u| !u.is_empty())
                .unwrap_or(false);
            if has_users {
                Authed::Deny // 有用户但未认证 → 401
            } else {
                Authed::Pass {
                    role: qsdb::users::Role::Admin, // 完全开放（现状兼容）
                    session: None,
                }
            }
        }
    }
}

/// 认证并解析角色；失败直接给出契约错误响应（401/403）——各端点
/// `let role = match require_role(…) { Ok(r) => r, Err(resp) => return resp };`
fn require_role(state: &AppState, headers: &HeaderMap) -> Result<qsdb::users::Role, Box<Response>> {
    match authenticate(state, headers) {
        Authed::Pass { role, .. } => Ok(role),
        Authed::NoGrant { user, db } => Err(Box::new(ErrorCode::Forbidden.response(format!(
            "user '{user}' has no grant on database '{db}' (mounted: {})",
            state.mount_names().join(", ")
        )))),
        Authed::Revoked => Err(Box::new(
            ErrorCode::SessionRevoked.response("session revoked or expired; re-login"),
        )),
        Authed::Deny => Err(Box::new(ErrorCode::Unauthorized.response("unauthorized"))),
    }
}

/// 库名必须已挂载；否则 404（文案带挂载清单——"库不存在"与"无权限"在这里
/// 就是同一条，因为挂载表是全量的）。
fn ensure_mounted(state: &AppState, db: &str) -> std::result::Result<(), Box<Response>> {
    if state.mounts.contains_key(db) {
        Ok(())
    } else {
        Err(Box::new(ErrorCode::NotFound.response(format!(
            "database {db:?} is not mounted (mounted: {})",
            state.mount_names().join(", ")
        ))))
    }
}

/// 身份抽取（不判授权）：谁 + 会话。M-B 起 `/query`、`/write` 走这里——
/// 它们的目标库要等解析完 SQL / 表名才知道，授权判定必须**延后到目标库确定**：
/// 先按会话默认库判会把"只授了 settle、查询里写 settle.t"的用户误杀成 403。
/// 401/吊销判定与 [`authenticate`] 完全同源（Revoked → 401 `SESSION_REVOKED`）。
struct Identity {
    user: String,
    session: Option<Arc<Session>>,
}

fn authenticate_identity(
    state: &AppState,
    headers: &HeaderMap,
) -> std::result::Result<Identity, Box<Response>> {
    let auth_header = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    if let Some(h) = &auth_header {
        if let Some(token) = h.strip_prefix("Bearer ") {
            let hit = state
                .sessions
                .lock()
                .ok()
                .and_then(|m| m.get(token).cloned());
            if let Some(s) = hit {
                s.last_active_ms
                    .store(Session::epoch_ms(), Ordering::Relaxed);
                return Ok(Identity {
                    user: s.user.clone(),
                    session: Some(s),
                });
            }
            if token.starts_with(SESSION_TOKEN_PREFIX) {
                return Err(Box::new(
                    ErrorCode::SessionRevoked.response("session revoked or expired; re-login"),
                ));
            }
        }
    }
    if let Some(h) = &auth_header {
        if let Some(b64) = h.strip_prefix("Basic ") {
            if let Some((user, pass)) = basic_credentials(b64) {
                if let Some(def) =
                    qsdb::users::UserStore::verify_def(state.db.store(), &user, &pass)
                {
                    return Ok(Identity {
                        user: def.name,
                        session: None,
                    });
                }
            }
        }
    }
    match (&state.token, &auth_header) {
        (Some(expect), Some(h)) => {
            if h.strip_prefix("Bearer ")
                .map(|t| t == expect.as_str())
                .unwrap_or(false)
            {
                Ok(Identity {
                    user: "__token__".to_string(),
                    session: None,
                })
            } else {
                Err(Box::new(ErrorCode::Unauthorized.response("unauthorized")))
            }
        }
        (Some(_), None) => Err(Box::new(ErrorCode::Unauthorized.response("unauthorized"))),
        (None, _) => {
            let has_users = qsdb::users::UserStore::read_all(state.db.store())
                .map(|u| !u.is_empty())
                .unwrap_or(false);
            if has_users {
                Err(Box::new(ErrorCode::Unauthorized.response("unauthorized")))
            } else {
                Ok(Identity {
                    user: "anonymous".to_string(),
                    session: None,
                })
            }
        }
    }
}

/// 元数据语句（`SHOW PARTITIONS t` / `SHOW CREATE TABLE t` / `DESCRIBE t` /
/// `DESC t`）的**文本级路由**：这些语句由内核手解析（不走 sqlparser 绑定），
/// route_sql 的关系遍历看不见里面的表名——这里按"最后一个空白分隔 token 是表名"
/// 处理（这四种形态没有别的参数排在表名后面）。`db.t` → 剥成 `t` 并路由到 db。
/// `None` = 不是元数据语句（交给 route_sql 正常走）。
fn route_metadata_statement(
    state: &AppState,
    sql: &str,
) -> Option<std::result::Result<(String, String), Box<Response>>> {
    let t = sql.trim();
    let t = t.strip_suffix(';').map(str::trim).unwrap_or(t);
    const PREFIXES: [&str; 4] = [
        "SHOW PARTITIONS ",
        "SHOW CREATE TABLE ",
        "DESCRIBE ",
        "DESC ",
    ];
    let mut matched = None;
    for p in PREFIXES {
        if t.len() > p.len() && t.get(..p.len()).is_some_and(|x| x.eq_ignore_ascii_case(p)) {
            matched = Some(p);
            break;
        }
    }
    let prefix = matched?;
    let rest = t[prefix.len()..].trim();
    // "DESCRIBE TABLE t" 的中间 TABLE 也吃掉——最后一个 token 恒是表名
    let name = rest.rsplit(' ').next()?.trim();
    if name.is_empty() {
        return Some(Err(Box::new(
            ErrorCode::InvalidArgument.response(format!("missing table name after {prefix:?}")),
        )));
    }
    let (qdb, plain) = split_qualified(name);
    let Some(db) = qdb else {
        return None; // 单段名：照常交给 route_sql（落会话默认库）
    };
    if let Err(resp) = ensure_mounted(state, &db) {
        return Some(Err(resp));
    }
    let head = &t[..t.len() - name.len()];
    Some(Ok((format!("{head}{plain}"), db)))
}

/// `USE db` 语句识别（server 面拦截；嵌入面不认这个——单句柄单库，
/// `db.sql("USE x")` 照旧走内核报它的错）。匹配整个语句（剥尾分号）。
fn parse_use_statement(sql: &str) -> Option<String> {
    let t = sql.trim().trim_end_matches(';').trim();
    let rest = t.strip_prefix("USE").or_else(|| t.strip_prefix("use"))?;
    let db = rest.trim();
    if db.is_empty()
        || !db.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
        || db.as_bytes()[0].is_ascii_digit()
    {
        return None;
    }
    Some(db.to_ascii_lowercase())
}

fn is_show_databases(sql: &str) -> bool {
    sql.trim()
        .trim_end_matches(';')
        .trim()
        .eq_ignore_ascii_case("SHOW DATABASES")
}

/// `db.table` 拆分（首点分段；无点 = None）。表名里的点本仓不允许（列名都不许），
/// 所以首点即库名分隔——这跟 SQL 限定名的语义一致。
fn split_qualified(name: &str) -> (Option<String>, String) {
    match name.split_once('.') {
        Some((db, t)) => (Some(db.to_ascii_lowercase()), t.to_string()),
        None => (None, name.to_string()),
    }
}

/// SQL 限定名路由（M-B，设计稿 §4.2）：解析语句里的**表引用**，剥挂载限定符，
/// 定出目标库。语义：
/// - `db.t`：`db` 必须是挂载名（否则 404），路由到该库；
/// - `t`（单段）：落目标库；
/// - 目标库 = 显式指定（REST body `database`）?? 语句里唯一出现过的限定库
///   ?? 会话默认库 ?? `"default"`；
/// - 触及的库多于一个 → **显式拒**（501 `UNSUPPORTED`：跨库 JOIN/事务是另一个
///   项目，"逐个查询再拼"必然是错的半吊子语义——设计稿 §4.2 定案）。
///
/// 解析失败（内核预处理语法如 `COPY … (FORMAT …)` sqlparser 裸解析不了）→
/// **原样透传**给目标句柄：内核给它的错误，显式失败不静默。
/// 返回（改写后的 SQL, 目标库名）。
fn route_sql(
    state: &AppState,
    sql: &str,
    explicit_db: Option<&str>,
    session_default: &str,
) -> std::result::Result<(String, String), Box<Response>> {
    use sqlparser::ast::visit_relations_mut;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;
    use std::ops::ControlFlow;

    let mut stmts = match Parser::parse_sql(&GenericDialect {}, sql) {
        Ok(s) => s,
        Err(_) => {
            let db = explicit_db.unwrap_or(session_default);
            ensure_mounted(state, db)?;
            return Ok((sql.to_string(), db.to_string()));
        }
    };
    // 第一遍：库归属收集（限定首段 = 库名；单段 = 待定）。
    // ⚠️ visit_relations 覆盖不到 `DROP TABLE` 的 names 与 `CREATE TABLE` 的 name
    // （sqlparser 0.63 的 visit_relation 标注没打在这两处）——补收，漏了会把
    // DDL 目标静默留给会话默认库。
    let mut qualified: Vec<String> = Vec::new();
    let mut unqualified = false;
    let mut collect = |name: &sqlparser::ast::ObjectName| {
        if name.0.len() >= 2 {
            if let Some(sqlparser::ast::ObjectNamePart::Identifier(id)) = name.0.first() {
                let db = id.value.to_ascii_lowercase();
                if !qualified.contains(&db) {
                    qualified.push(db);
                }
                return;
            }
        }
        unqualified = true;
    };
    for st in stmts.iter() {
        let _ = sqlparser::ast::visit_relations(st, |name| {
            collect(name);
            ControlFlow::<()>::Continue(())
        });
        match st {
            sqlparser::ast::Statement::Drop { names, .. } => {
                for n in names {
                    collect(n);
                }
            }
            sqlparser::ast::Statement::CreateTable(ct) => collect(&ct.name),
            _ => {}
        }
    }
    for db in &qualified {
        ensure_mounted(state, db)?;
    }
    // 目标库判定（设计稿 §二 + §4.2）：
    // - **单段名锚定**在 显式库 ?? 会话默认库——绝不浮动到限定库里去
    //   （否则 `t JOIN settle.t` 会静默连到 settle 的同名表，错数据级别的事故）；
    // - 带限定名的语句（且无单段名）落那个唯一的限定库——授权也按它判，
    //   白名单用户写 `settle.t` 即可直达，不必先 USE。
    let (target, touched) = match explicit_db {
        Some(exp) => {
            ensure_mounted(state, exp)?;
            let mut touched = qualified.clone();
            if unqualified && !touched.iter().any(|d| d == exp) {
                touched.push(exp.to_string());
            }
            (exp.to_string(), touched)
        }
        None => {
            ensure_mounted(state, session_default)?;
            let base = session_default.to_string();
            match qualified.as_slice() {
                [] => (base.clone(), vec![base]),
                [one] => {
                    let mut touched = vec![one.clone()];
                    if unqualified && one != &base {
                        touched.push(base.clone());
                    }
                    (one.clone(), touched)
                }
                _ => (base.clone(), qualified.clone()),
            }
        }
    };
    if touched.len() > 1 {
        return Err(Box::new(ErrorCode::Unsupported.response(format!(
            "cross-database query is not supported: statement touches databases [{}], \
             target {:?} — 跨库查询未实现，请分别查询或同库",
            touched.join(", "),
            target
        ))));
    }
    // 第二遍：剥挂载限定符（首段 == 目标库的多段名 → 去掉首段）+ DDL 补刀
    fn strip(name: &mut sqlparser::ast::ObjectName, target: &str) {
        if name.0.len() >= 2 {
            if let Some(sqlparser::ast::ObjectNamePart::Identifier(id)) = name.0.first() {
                if id.value.to_ascii_lowercase() == target {
                    name.0.remove(0);
                }
            }
        }
    }
    for st in &mut stmts {
        let _ = visit_relations_mut(st, |name| {
            strip(name, &target);
            ControlFlow::<()>::Continue(())
        });
        match st {
            sqlparser::ast::Statement::Drop { names, .. } => {
                for n in names {
                    strip(n, &target);
                }
            }
            sqlparser::ast::Statement::CreateTable(ct) => strip(&mut ct.name, &target),
            _ => {}
        }
    }
    let out = stmts
        .iter()
        .map(|st| st.to_string())
        .collect::<Vec<_>>()
        .join(";\n");
    Ok((out, target))
}

/// Basic 头 → (用户名, 密码)。
fn basic_credentials(b64: &str) -> Option<(String, String)> {
    use base64::Engine;
    let decoded = base64::engine::general_purpose::STANDARD.decode(b64).ok()?;
    let text = String::from_utf8(decoded).ok()?;
    let (user, pass) = text.split_once(':')?;
    Some((user.to_string(), pass.to_string()))
}

/// 资源端点闸（`/tables`、`/schema`、`/partitions`、`/stats`、`/config`、
/// `/queries`、`/queries/{id}/cancel`）：失败把 401/403 **响应体**原样带回去
/// （M-C 起无授权用户在这些端点上是 403——旧的"只查 401 状态码"写法会把
/// 403 丢掉然后继续执行 handler，M-C 契约测试抓出来的）。
fn auth(state: &AppState, headers: &HeaderMap) -> Result<(), Box<Response>> {
    require_role(state, headers).map(|_| ())
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

/// POST /api/v1/auth/login：Basic 认证 → 签发 Bearer token + **建立会话**（M-A）。
///
/// 会话与 token 1:1、进程内存态（重启即失效，重登零成本）。响应回显
/// `sessionId` / `defaultDb`，客户端可自查归属（`GET /api/v1/sessions/{id}`）。
/// 用旧 token 重登 = 另开一个新会话（旧的照常可用/可被独立踢出）。
async fn auth_login(
    State(s): State<Arc<AppState>>,
    axum::extract::ConnectInfo(addr): axum::extract::ConnectInfo<std::net::SocketAddr>,
    headers: HeaderMap,
) -> Response {
    let auth_header = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    // **先判吊销**（在一切放行逻辑之前）：会话形 token 已不在表 = 被踢 / 重启过
    // ——别让它换到新会话，直接 401 `SESSION_REVOKED`。
    if let Some(h) = &auth_header {
        if let Some(token) = h.strip_prefix("Bearer ") {
            if token.starts_with(SESSION_TOKEN_PREFIX) {
                let known = s
                    .sessions
                    .lock()
                    .map(|m| m.contains_key(token))
                    .unwrap_or(false);
                if !known {
                    return ErrorCode::SessionRevoked
                        .response("session revoked or expired; re-login");
                }
            }
        }
    }
    // 凭证解析：(用户名, grants 摘要, 默认库角色)。失败 → 各自的 401。
    // **登录只验凭证，不做授权判定**——白名单制用户（M-C，可能对 default 无权）
    // 也要能登录，403 留给每请求的现查。
    let cred = (|| {
        let h = auth_header.as_deref()?;
        if let Some(b64) = h.strip_prefix("Basic ") {
            let (user, pass) = basic_credentials(b64)?;
            let def = qsdb::users::UserStore::verify_def(s.db.store(), &user, &pass)?;
            return Some((def.name.clone(), def.grants_json(), def.role_for("default")));
        }
        if let Some(token) = h.strip_prefix("Bearer ") {
            // 旧会话重登：沿用其身份；用户被删 → 401（凭证已无效）
            if let Some(name) = s
                .sessions
                .lock()
                .ok()
                .and_then(|m| m.get(token).cloned())
                .map(|old| old.user.clone())
            {
                let def = qsdb::users::UserStore::read_all(s.db.store())
                    .ok()
                    .and_then(|m| m.get(&name).cloned())?;
                return Some((def.name.clone(), def.grants_json(), def.role_for("default")));
            }
            if s.token.as_deref() == Some(token) {
                return Some((
                    "__token__".to_string(),
                    serde_json::json!({ "*": "admin" }),
                    Some(qsdb::users::Role::Admin),
                ));
            }
        }
        None
    })();
    let (user, grants, role) = match cred {
        Some(c) => c,
        None => {
            // 完全开放（无用户 + 无超级 token）：照常可登（anonymous 会话）；
            // 其余（配了凭证但没给对）→ 401。
            let has_users = qsdb::users::UserStore::read_all(s.db.store())
                .map(|u| !u.is_empty())
                .unwrap_or(false);
            if s.token.is_none() && !has_users {
                (
                    "anonymous".to_string(),
                    serde_json::json!({ "*": "admin" }),
                    Some(qsdb::users::Role::Admin),
                )
            } else {
                return ErrorCode::Unauthorized.response("invalid credentials");
            }
        }
    };
    let token = new_session_token();
    let session = Arc::new(Session {
        id: new_session_id(),
        user: user.clone(),
        client: format!(
            "{}; {}",
            addr,
            headers
                .get("user-agent")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("-")
        ),
        created_at_ms: Session::epoch_ms(),
        last_active_ms: AtomicU64::new(Session::epoch_ms()),
        default_db: std::sync::Mutex::new("default".to_string()),
        stats: SessionStats::default(),
    });
    let session_id = session.id.clone();
    if let Ok(mut m) = s.sessions.lock() {
        m.insert(token.clone(), session);
    }
    Json(serde_json::json!({
        "token": token,
        "sessionId": session_id,
        // 十八批 C1 补：会话身份（客户端 SessionInfo 自查/展示用；开放模式 =
        // "anonymous"）。只增字段，老客户端不受影响
        "user": user,
        "role": role.map(|r| serde_json::json!(r.name())).unwrap_or(serde_json::Value::Null),
        "grants": grants,
        "defaultDb": "default",
        "note": "process-lifetime token + session; restart requires re-login"
    }))
    .into_response()
}

// ---------------------------------------------------------------------------
// 会话端点（十六批 M-A；设计文档 §3.2）
// ---------------------------------------------------------------------------

/// `GET /api/v1/sessions`：列全部会话（admin 专属）。
///
/// 在跑查询由**查询注册表反查**（唯一真源，不冗余存进 Session）。
async fn list_sessions(State(s): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    let role = match require_role(&s, &headers) {
        Ok(r) => r,
        Err(resp) => return *resp,
    };
    if !role.is_admin() {
        return ErrorCode::Forbidden.response("session list requires admin");
    }
    // 注册表反查：session id → 在跑 query ids
    let running: HashMap<String, Vec<String>> = match s.queries.lock() {
        Ok(q) => {
            let mut m: HashMap<String, Vec<String>> = HashMap::new();
            for (qid, e) in q.iter() {
                if let Some(sess) = &e.session {
                    m.entry(sess.id.clone()).or_default().push(qid.clone());
                }
            }
            m
        }
        Err(_) => return ErrorCode::Internal.response("registry poisoned"),
    };
    let mut entries: Vec<Arc<Session>> = match s.sessions.lock() {
        Ok(m) => m.values().cloned().collect(),
        Err(_) => return ErrorCode::Internal.response("session table poisoned"),
    };
    entries.sort_by(|a, b| (a.created_at_ms, &a.id).cmp(&(b.created_at_ms, &b.id)));
    let list: Vec<serde_json::Value> = entries
        .iter()
        .map(|sess| {
            let db = sess.default_db();
            let now_role = resolved_role_for(&s, &sess.user, &db);
            sess.to_json(running.get(&sess.id).cloned().unwrap_or_default(), now_role)
        })
        .collect();
    let count = list.len();
    Json(serde_json::json!({ "sessions": list, "count": count })).into_response()
}

/// `GET /api/v1/sessions/{id}`：单会话详情（admin 或**本会话**自查）。
async fn get_session(
    State(s): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    let (role, own) = match authenticate(&s, &headers) {
        Authed::Pass { role, session } => (role, session.is_some_and(|x| x.id == id)),
        Authed::NoGrant { user, db } => {
            return ErrorCode::Forbidden.response(format!(
                "user '{user}' has no grant on database '{db}' (mounted: {})",
                s.mount_names().join(", ")
            ));
        }
        Authed::Revoked => return ErrorCode::SessionRevoked.response("session revoked; re-login"),
        Authed::Deny => return ErrorCode::Unauthorized.response("unauthorized"),
    };
    if !role.is_admin() && !own {
        return ErrorCode::Forbidden
            .response("session detail requires admin or the session itself");
    }
    let running: Vec<String> = match s.queries.lock() {
        Ok(q) => q
            .iter()
            .filter(|(_, e)| e.session.as_ref().is_some_and(|x| x.id == id))
            .map(|(qid, _)| qid.clone())
            .collect(),
        Err(_) => return ErrorCode::Internal.response("registry poisoned"),
    };
    let found = s
        .sessions
        .lock()
        .ok()
        .and_then(|m| m.values().find(|x| x.id == id).cloned());
    match found {
        Some(sess) => {
            let db = sess.default_db();
            let now_role = resolved_role_for(&s, &sess.user, &db);
            Json(sess.to_json(running, now_role)).into_response()
        }
        None => ErrorCode::NotFound.response(format!("session {id} not found")),
    }
}

/// `DELETE /api/v1/sessions/{id}`：**踢人**（admin 专属）——取消在跑的全部查询
/// （复用既有 cancel 路径）+ 吊销 token。被踢会话的下一个请求 401
/// `SESSION_REVOKED`。
async fn kick_session(
    State(s): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    let role = match require_role(&s, &headers) {
        Ok(r) => r,
        Err(resp) => return *resp,
    };
    if !role.is_admin() {
        return ErrorCode::Forbidden.response("kicking sessions requires admin");
    }
    match s.revoke_session(&id) {
        Some(cancelled) => {
            Json(serde_json::json!({ "kicked": id, "cancelledQueries": cancelled })).into_response()
        }
        None => ErrorCode::NotFound.response(format!("session {id} not found")),
    }
}

/// 403 无授权响应（统一文案；提示怎么定向到自己的库）。
fn no_grant_response(state: &AppState, user: &str, db: &str) -> Response {
    ErrorCode::Forbidden.response(format!(
        "user '{user}' has no grant on database '{db}' (mounted: {});          target it via the \"database\" field or USE",
        state.mount_names().join(", ")
    ))
}

/// X-QSDB-Query-Id 头抽取（非法 → 400，与 /query 主路径同一纪律）。
fn want_id_from(headers: &HeaderMap) -> std::result::Result<Option<String>, Box<Response>> {
    match headers.get("x-qsdb-query-id") {
        None => Ok(None),
        Some(v) => match v.to_str().ok().filter(|s| valid_client_query_id(s)) {
            Some(id) => Ok(Some(id.to_string())),
            None => Err(Box::new(ErrorCode::InvalidArgument.response(
                "invalid X-QSDB-Query-Id: expect 1..=64 chars of [A-Za-z0-9._:-]",
            ))),
        },
    }
}

/// `USE db`（server 面语句拦截；语义同 `PUT /sessions/current/database`）：
/// 需要**会话**（Basic / 超级 token 无会话可切 → 403）；库名按挂载表校验。
fn use_database(s: &Arc<AppState>, ident: &Identity, db: &str, headers: &HeaderMap) -> Response {
    let Some(sess) = ident.session.as_ref() else {
        return ErrorCode::Forbidden.response(
            "USE requires a login session (POST /api/v1/auth/login) — Basic / super-token requests have no session to switch",
        );
    };
    if let Err(resp) = ensure_mounted(s, db) {
        return *resp;
    }
    if let Err(resp) = want_id_from(headers) {
        return *resp;
    }
    if let Ok(mut d) = sess.default_db.lock() {
        *d = db.to_string();
    }
    let (qid, echo) = match want_id_from(headers) {
        Ok(Some(id)) => (id.clone(), Some(id)),
        _ => (format!("q{}", new_token()), None),
    };
    let mut resp = Json(serde_json::json!({
        "queryId": qid,
        "message": format!("database changed to '{db}'"),
        "defaultDb": db,
        "elapsedMs": 0.0,
    }))
    .into_response();
    if let Some(id) = echo {
        if let Ok(v) = axum::http::HeaderValue::from_str(&id) {
            resp.headers_mut()
                .insert(axum::http::HeaderName::from_static("x-qsdb-query-id"), v);
        }
    }
    resp
}

/// `SHOW DATABASES`（server 面语句拦截）：admin 全列；非 admin 只列**有 grant**
/// 的库（白名单制不泄露库名）。
fn show_databases(s: &Arc<AppState>, ident: &Identity, headers: &HeaderMap) -> Response {
    if let Err(resp) = want_id_from(headers) {
        return *resp;
    }
    let session_default = ident
        .session
        .as_ref()
        .map(|x| x.default_db())
        .unwrap_or_else(|| "default".to_string());
    let is_admin =
        resolved_role_for(s, &ident.user, &session_default).is_some_and(|r| r.is_admin());
    let rows: Vec<Vec<serde_json::Value>> = s
        .mount_names()
        .into_iter()
        .filter(|name| is_admin || resolved_role_for(s, &ident.user, name).is_some())
        .map(|name| vec![serde_json::json!(name)])
        .collect();
    let n = rows.len();
    let (qid, echo) = match want_id_from(headers) {
        Ok(Some(id)) => (id.clone(), Some(id)),
        _ => (format!("q{}", new_token()), None),
    };
    let mut resp = Json(serde_json::json!({
        "queryId": qid,
        "schema": [{ "name": "database", "type": "Utf8" }],
        "rows": rows,
        "rowCount": n,
        "elapsedMs": 0.0,
    }))
    .into_response();
    if let Some(id) = echo {
        if let Ok(v) = axum::http::HeaderValue::from_str(&id) {
            resp.headers_mut()
                .insert(axum::http::HeaderName::from_static("x-qsdb-query-id"), v);
        }
    }
    resp
}

/// GET /api/v1/databases：挂载清单与各自只读态（M-B；授权过滤同 SHOW DATABASES）。
async fn list_databases(State(s): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    let ident = match authenticate_identity(&s, &headers) {
        Ok(i) => i,
        Err(resp) => return *resp,
    };
    let session_default = ident
        .session
        .as_ref()
        .map(|x| x.default_db())
        .unwrap_or_else(|| "default".to_string());
    let is_admin =
        resolved_role_for(&s, &ident.user, &session_default).is_some_and(|r| r.is_admin());
    let list: Vec<serde_json::Value> = s
        .mount_names()
        .into_iter()
        .filter(|name| is_admin || resolved_role_for(&s, &ident.user, name).is_some())
        .map(|name| {
            serde_json::json!({
                "name": name,
                "readOnly": s.mounts[&name].db.read_only(),
            })
        })
        .collect();
    let count = list.len();
    Json(serde_json::json!({ "databases": list, "count": count })).into_response()
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionDatabaseBody {
    database: String,
}

/// `PUT /api/v1/sessions/current/database`：设**本会话**默认库
/// （SQL `USE db` 的 REST 落点；body `{"database": "…"}`）。
///
/// 库名按挂载白名单校验——不存在的库**显式 404**（M-A 单库恒 `default`；
/// M-B 多库后由 `--mount` 填充这份白名单）。无会话的请求（Basic / 超级
/// token / 开放模式）显式 403：它们没有"本会话"可设。
async fn set_session_database(
    State(s): State<Arc<AppState>>,
    headers: HeaderMap,
    StructuredJson(body): StructuredJson<SessionDatabaseBody>,
) -> Response {
    let session = match authenticate(&s, &headers) {
        Authed::Pass { session, .. } => session,
        Authed::NoGrant { user, db } => {
            return ErrorCode::Forbidden.response(format!(
                "user '{user}' has no grant on database '{db}' (mounted: {})",
                s.mount_names().join(", ")
            ));
        }
        Authed::Revoked => return ErrorCode::SessionRevoked.response("session revoked; re-login"),
        Authed::Deny => return ErrorCode::Unauthorized.response("unauthorized"),
    };
    let Some(sess) = session else {
        return ErrorCode::Forbidden.response(
            "setting a default database requires a login session (POST /api/v1/auth/login)",
        );
    };
    if !s.mounts.contains_key(&body.database) {
        return ErrorCode::NotFound.response(format!(
            "database {:?} is not mounted (mounted: {})",
            body.database,
            s.mount_names().join(", ")
        ));
    }
    if let Ok(mut d) = sess.default_db.lock() {
        *d = body.database; // 保存失败的唯一形态是锁中毒（进程已坏），由后续请求暴露
    }
    Json(serde_json::json!({
        "sessionId": sess.id,
        "defaultDb": sess.default_db(),
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
    let role = match require_role(&s, &headers) {
        Ok(r) => r,
        Err(resp) => return *resp,
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

/// GET /api/v1/users/export —— **用户面导出**（qsdb-sync 用户面同步的 HTTP 源）。
///
/// 起因（qsdb-sync）：物理复制早就能走 HTTP 了（F11），但**用户表导不出来**——
/// `--users × HTTP src` 只能显式报错。本端点补上那一半。
///
/// 契约要点：
/// - 体与 `qsdb admin user-export` **同源**（同一个 `UserStore::export_json`），
///   因此可直接喂给 `UserStore::import_json`（replica 的 `--users` 就是
///   「export src → import dst overwrite」）——**export↔import 往返由回归钉住**。
/// - **含口令哈希、不含明文密码**（与 CLI 一致）。
/// - **admin 专属**：体里有哈希，拖走等于拖走全部凭证。
async fn export_users(State(s): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    let role = match require_role(&s, &headers) {
        Ok(r) => r,
        Err(resp) => return *resp,
    };
    if !role.is_admin() {
        return ErrorCode::Forbidden.response("user export requires admin");
    }
    match qsdb::users::UserStore::export_json(s.db.store()) {
        Ok(json) => {
            let mut resp = json.into_response();
            resp.headers_mut().insert(
                axum::http::header::CONTENT_TYPE,
                axum::http::HeaderValue::from_static("application/json"),
            );
            resp
        }
        Err(e) => ErrorCode::Internal.response(e.to_string()),
    }
}

/// `PUT /api/v1/users/{name}/grants` —— **库级授权热改**（M-C；admin 专属）。
///
/// body `{"grants": {"settle": "reader", "sales": null, "*": "writer"}}`：
/// 键 = 库名或 `"*"`，值 = 角色名（大小写不敏感）或 `null`（**显式无权**）。
/// **PUT = 全量替换**；想全拒给 `{"*": null}`，空表 400。
/// 判定侧**每请求现查**（角色不缓存进会话）——本调用返回即生效，被改用户
/// 无需重登。写 `_catalog/users.json` 走既有锁内原子写路径。
async fn put_user_grants(
    State(s): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(name): Path<String>,
    StructuredJson(body): StructuredJson<serde_json::Value>,
) -> Response {
    let role = match require_role(&s, &headers) {
        Ok(r) => r,
        Err(resp) => return *resp,
    };
    if !role.is_admin() {
        return ErrorCode::Forbidden.response("changing grants requires admin");
    }
    // 严格 shape：顶层只认 {"grants": {db: 角色名|null}}
    let Some(obj) = body.as_object() else {
        return ErrorCode::InvalidArgument.response(r#"body must be an object {"grants": {...}}"#);
    };
    if obj.len() != 1 || !obj.contains_key("grants") {
        return ErrorCode::InvalidArgument
            .response(r#"body must be exactly {"grants": {...}} (unknown or missing keys)"#);
    }
    let Some(g) = obj["grants"].as_object() else {
        return ErrorCode::InvalidArgument
            .response(r#""grants" must be an object {db: role|null}"#);
    };
    let mut map = std::collections::BTreeMap::new();
    for (db, v) in g {
        let db = db.trim();
        if db.is_empty() {
            return ErrorCode::InvalidArgument.response("grant key (database) must not be empty");
        }
        let r = match v {
            serde_json::Value::Null => None,
            serde_json::Value::String(role) => match qsdb::users::Role::from_name(role) {
                Ok(r) => Some(r),
                Err(e) => return ErrorCode::InvalidArgument.response(e.to_string()),
            },
            _ => {
                return ErrorCode::InvalidArgument
                    .response(format!("grant for {db:?} must be a role name or null"));
            }
        };
        map.insert(db.to_string(), r);
    }
    match qsdb::users::UserStore::set_grants(s.db.store(), &name, map) {
        Ok(()) => {
            // 回显**落盘后**的实际形态（read_all 再取一次，别信内存里的猜测）
            let grants = qsdb::users::UserStore::read_all(s.db.store())
                .ok()
                .and_then(|m| m.get(&name).cloned())
                .map(|d| d.grants_json())
                .unwrap_or(serde_json::json!({}));
            Json(serde_json::json!({ "user": name, "grants": grants })).into_response()
        }
        Err(e) => status_of(&e, e.to_string()),
    }
}

/// 把内存池的实时占用刷进指标原子（渲染前调一次；池本身在 `Qsdb` 上）。
/// 多挂载（M-B）：每个挂载有自己的池——指标给**总和**（分挂载口径走 /stats 的
/// `mounts` 数组）。
fn refresh_mem_gauges(s: &AppState) {
    let mut budget = 0u64;
    let mut exec = 0u64;
    let mut cache = 0u64;
    for m in s.mounts.values() {
        let pool = m.db.memory_pool();
        budget += pool.budget();
        exec += pool.exec_used();
        cache += pool.cache_used();
    }
    s.metrics.mem_budget.store(budget, Ordering::Relaxed);
    s.metrics.mem_exec.store(exec, Ordering::Relaxed);
    s.metrics.mem_cache.store(cache, Ordering::Relaxed);
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
    let sessions = s.sessions.lock().map(|m| m.len()).unwrap_or(0);
    let (tables, bytes) = match tokio::task::spawn_blocking({
        let st = Arc::clone(&s);
        move || {
            let mut t = 0usize;
            let mut b = 0u64;
            for m in st.mounts.values() {
                if let Ok((x, y)) = m.db.stats() {
                    t += x;
                    b += y;
                }
            }
            (t, b)
        }
    })
    .await
    {
        Ok((t, b)) => (t, b),
        _ => (0, 0),
    };
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4",
        )],
        s.metrics.render(tables, bytes, sessions),
    )
        .into_response()
}

/// `GET /api/v1/watch?since=<游标>&wait=<秒>` —— **提交事件订阅**（T2）。
///
/// 消费方是 qsdb-sync：他们今天靠**轮询** `partition_stats` 发现增量，接上这里
/// 就能换成 push（RPO 秒级 → 毫秒级）。设计见
/// `docs/提交事件订阅与湖导出设计.md` §1.3。
///
/// **长轮询**（不是 SSE 流）：有事件**立刻**回，没有就挂到 `wait` 再回空批次。
/// 交付同样的延迟，但不用处理流式解析 / keep-alive / 半开连接；将来要加流式是
/// **新端点**（契约允许只增不改），`since` 游标与事件 schema 不用动。
///
/// ⚠️ **这是加速器，不是真相来源**：内核钩子可能丢事件（回调 panic 被吞、
/// 远端写路径暂不透出 generation），所以消费方**应当保留一个慢轮询兜底**；
/// 游标接不上时这里**显式报 `CURSOR_TOO_OLD`**，绝不假装连续。
#[derive(serde::Deserialize)]
struct WatchParams {
    /// 上次响应里的 `next`。**省略 = 从现在开始**（首次订阅不该收历史全量）。
    #[serde(default)]
    since: Option<u64>,
    /// 最长挂起秒数（默认 25，上限 60）
    #[serde(default)]
    wait: Option<u64>,
}

async fn watch(
    State(s): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(p): Query<WatchParams>,
) -> Response {
    // 与 /query 同一套鉴权；**reader 角色即可**（订阅只是读事件）
    if let Err(resp) = require_role(&s, &headers) {
        return *resp;
    }
    let wait = std::time::Duration::from_secs(
        p.wait
            .unwrap_or(crate::commit_log::DEFAULT_WAIT_SECS)
            .min(crate::commit_log::MAX_WAIT_SECS),
    );
    match s.commits.wait(p.since, wait).await {
        Ok(b) => Json(serde_json::json!({
            "events": b.events,
            "next": b.next,
            "more": b.more,
        }))
        .into_response(),
        Err(_) => ErrorCode::CursorTooOld.response(
            "订阅游标太旧（事件缓冲已滚过 / 服务端重启过）——              请改用 /api/v1/tables 与 /partitions 做一次全量对账，再从新的 next 续订",
        ),
    }
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)] // 拼错键（"formatt"）→ 400，不静默走默认值
struct QueryBody {
    sql: String,
    #[serde(default)]
    format: Option<String>,
    /// 请求目标库（M-B，**优先级最高**）：未给 = 语句里唯一出现的限定库
    /// ?? 会话默认库 ?? "default"。授权按它判，单段表名落在这里。
    #[serde(default)]
    database: Option<String>,
    /// 参数化查询（`docs/参数化查询设计.md`）：**位置数组**，`$n` 取第 n 个。
    /// 元素可以是裸 JSON 值（推类型）或 `{"type": …, "value": …}`（显式）。
    /// 缺省 = 没有参数（行为与从前一字不差）。
    #[serde(default)]
    params: Option<serde_json::Value>,
    /// 结果护栏的**本请求覆盖**（十七批 R1）：给出即接管本请求的护栏整体
    /// （行数 = 此值，体量闸一并放开——护栏只有一个豁免口径，客户端显式担责；
    /// 要在护栏内自限请在 SQL 里加 LIMIT）。`0` = 本请求不限。
    /// 优先级：body `maxRows` > 头 `X-QSDB-Max-Rows` > 服务器值。
    /// 规范名 camelCase（契约文档以此为准）；`alias` 是**追加**名，规范名
    /// 须 `rename` —— `deny_unknown_fields` 下写反了 camelCase 会被当未知键 400。
    #[serde(default, rename = "maxRows", alias = "max_rows")]
    max_rows: Option<u64>,
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

/// R1 覆盖优先级收敛（纯函数，供单测）：body > 头 > 服务器值。
/// 覆盖**给出即接管护栏整体**——行数取覆盖值、体量闸随行覆盖一并放开；
/// `0` = 本请求不限。两侧都没给 → 服务器值原样（默认 0 = 关）。
fn effective_result_guard(
    body: Option<u64>,
    header: Option<u64>,
    server_rows: u64,
    server_bytes: u64,
) -> (u64, u64) {
    match body.or(header) {
        Some(n) => (n, 0),
        None => (server_rows, server_bytes),
    }
}

/// 结果护栏的字节估算口径：Arrow 数组内存量（含缓冲区与偏移）。**估算而非
/// 精确**——JSON 序列化后实际体积可达此值的 1.5~3×（转义/数字展开），
/// 契约文档如实标注。
pub(crate) fn batches_arrow_bytes(batches: &[RecordBatch]) -> u64 {
    batches
        .iter()
        .map(|b| {
            b.columns()
                .iter()
                .map(|c| c.get_array_memory_size() as u64)
                .sum::<u64>()
        })
        .sum()
}

async fn query(
    State(s): State<Arc<AppState>>,
    headers: HeaderMap,
    StructuredJson(body): StructuredJson<QueryBody>,
) -> Response {
    let ident = match authenticate_identity(&s, &headers) {
        Ok(i) => i,
        Err(resp) => return *resp,
    };
    s.metrics.requests_total.fetch_add(1, Ordering::Relaxed);
    let session_default = ident
        .session
        .as_ref()
        .map(|x| x.default_db())
        .unwrap_or_else(|| "default".to_string());

    // ---- server 面语句拦截（M-B）：USE / SHOW DATABASES ----
    if let Some(db) = parse_use_statement(&body.sql) {
        return use_database(&s, &ident, &db, &headers);
    }
    if is_show_databases(&body.sql) {
        return show_databases(&s, &ident, &headers);
    }
    // 元数据语句（SHOW PARTITIONS / DESCRIBE …）文本级路由
    let routed_pre = match route_metadata_statement(&s, &body.sql) {
        Some(r) => match r {
            Ok((sql2, db)) => (sql2, Some(db)),
            Err(resp) => {
                s.metrics.errors_total.fetch_add(1, Ordering::Relaxed);
                return *resp;
            }
        },
        None => (body.sql.clone(), None),
    };

    // ---- 限定名路由：先定目标库，**再**按目标库判权（设计稿 §二 + §4.2）。
    // 权限判定若抢在路由前（按会话默认库），会把"只授了 settle、查询里写
    // settle.t"的用户误杀成 403——顺序就是语义。
    let explicit_db = routed_pre.1.or(body.database.clone());
    let (sql_routed, target_db) =
        match route_sql(&s, &routed_pre.0, explicit_db.as_deref(), &session_default) {
            Ok(r) => r,
            Err(resp) => {
                s.metrics.errors_total.fetch_add(1, Ordering::Relaxed);
                return *resp;
            }
        };
    let session = ident.session.clone();
    let (role, session) = match resolve_user(&s, &ident.user, &target_db, session) {
        Authed::Pass { role, session } => (role, session),
        Authed::NoGrant { user, db } => {
            s.metrics.errors_total.fetch_add(1, Ordering::Relaxed);
            return ErrorCode::Forbidden.response(format!(
                "user '{user}' has no grant on database '{db}' (mounted: {});                  target it via the \"database\" field or USE",
                s.mount_names().join(", ")
            ));
        }
        Authed::Revoked => {
            return ErrorCode::SessionRevoked.response("session revoked or expired; re-login");
        }
        Authed::Deny => return ErrorCode::Unauthorized.response("unauthorized"),
    };
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
    // R1 每请求覆盖：头 `X-QSDB-Max-Rows`（body maxRows 优先，见检查点处收敛）。
    // 非法值显式 400——静默忽略正是 J6/J11 的教训。
    let max_rows_header = match headers.get("x-qsdb-max-rows") {
        None => None,
        Some(v) => match v.to_str().ok().and_then(|s| s.parse::<u64>().ok()) {
            Some(n) => Some(n),
            None => {
                s.metrics.errors_total.fetch_add(1, Ordering::Relaxed);
                return ErrorCode::InvalidArgument.response(
                    "invalid X-QSDB-Max-Rows: expect an unsigned integer (0 = unlimited)",
                );
            }
        },
    };
    let (guard_rows, guard_bytes) = effective_result_guard(
        body.max_rows,
        max_rows_header,
        s.max_result_rows.load(Ordering::Relaxed),
        s.max_result_bytes.load(Ordering::Relaxed),
    );
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
    let target = Arc::clone(&s.mounts[&target_db].db);
    let ctx = match s.register_query(
        &target,
        want_id,
        timeout_ms,
        sql_routed.clone(),
        session.as_ref(),
    ) {
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
    let sql = sql_routed;
    // 参数在**请求线程**上先转好（类型/格式错立刻 400，不进 spawn_blocking）
    let params = match qsdb::sql::params::params_from_json(body.params.as_ref()) {
        Ok(p) => p,
        Err(e) => {
            s.metrics.errors_total.fetch_add(1, Ordering::Relaxed);
            reg.unregister_query(&query_id);
            return ErrorCode::InvalidArgument.response(e.to_string());
        }
    };
    let ctx_exec = Arc::clone(&ctx);
    let result = tokio::task::spawn_blocking(move || {
        target.sql_with_role_and_params_context(&sql, params, role, &ctx_exec)
    })
    .await;
    reg.unregister_query(&query_id);
    let elapsed = t.elapsed().as_nanos() as u64;
    s.metrics.observe_query(elapsed);
    // 响应头回声用（下面的 JSON 分支会把 query_id 移走）
    let echo_id = query_id.clone();
    let mut resp = match result {
        Ok(Ok(r)) => match r {
            SqlResult::Query {
                schema, batches, ..
            } => {
                let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                // ---- R1 结果护栏检查点：**执行后、序列化前**，全格式一致 ----
                // 行数精确（执行完即知）；字节按 Arrow 内存估算。超限恒定拒绝
                // 且**绝不返回部分数据**（throw 不 break——静默部分数据是本仓
                // 红线）。只作用于结果集：DML 回执恒小、COPY TO 是服务端写盘
                // 不是结果集，都不经过这里。
                if guard_rows > 0 && rows as u64 > guard_rows {
                    s.metrics
                        .rejected_result_size
                        .fetch_add(1, Ordering::Relaxed);
                    s.metrics.errors_total.fetch_add(1, Ordering::Relaxed);
                    ErrorCode::ResultTooLarge.response(format!(
                        "result has {rows} rows, exceeds max-result-rows {guard_rows}; \
                         add LIMIT, use a streaming format (ipc/rows/bulk), or override \
                         this request with body maxRows / header X-QSDB-Max-Rows \
                         (full export: COPY (SELECT …) TO …)"
                    ))
                } else {
                    let est_bytes = (guard_bytes > 0).then(|| batches_arrow_bytes(&batches));
                    if est_bytes.is_some_and(|est| est > guard_bytes) {
                        let est = est_bytes.unwrap_or(0);
                        s.metrics
                            .rejected_result_size
                            .fetch_add(1, Ordering::Relaxed);
                        s.metrics.errors_total.fetch_add(1, Ordering::Relaxed);
                        // 体量给人读的形态：< 1 MiB 直接报字节（整除归零的
                        // "0 MiB" 是假信号）；≥ 1 MiB 才换算
                        let human = |b: u64| {
                            if b >= 1024 * 1024 {
                                format!("~{} MiB ({} bytes)", b / (1024 * 1024), b)
                            } else {
                                format!("{b} bytes")
                            }
                        };
                        ErrorCode::ResultTooLarge.response(format!(
                            "result is {} (arrow estimate), exceeds max-result-bytes {}; \
                             add LIMIT, use a streaming format (ipc/rows/bulk), or override \
                             this request with body maxRows / header X-QSDB-Max-Rows \
                             (full export: COPY (SELECT …) TO …)",
                            human(est),
                            human(guard_bytes)
                        ))
                    } else if fmt.eq_ignore_ascii_case("ipc") {
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
                                    axum::http::HeaderValue::from_static(
                                        "application/vnd.qsdb.bulk",
                                    ),
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
                                    axum::http::HeaderValue::from_static(
                                        "application/vnd.qsdb.rows",
                                    ),
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
                        // R2：JSON 逐批流式——服务端驻留与单个批（8192 行）相关、
                        // 与总行数无关（原 batches_to_json 全量物化，500 万行 =
                        // 行×列个 serde_json::Value 走 RSS 尖峰）。响应形态不变。
                        json_query_response(&query_id, &schema, batches, rows, elapsed as f64 / 1e6)
                    }
                }
            }
            SqlResult::Ok {
                message,
                rows_affected,
                command,
                ..
            } => {
                // **结构化回执**（J12）：`command` + `rowsAffected` 让驱动不必解析
                // `message` 文案（码表文档明确"文案不做兼容承诺"）。
                // 字段只在有意义时出现（DDL 无行数）——老客户端读 `message` 不受影响。
                if let (Some(s), Some(n)) = (&session, rows_affected) {
                    s.stats.rows_written.fetch_add(n, Ordering::Relaxed);
                }
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
            // `SqlResult` 是 `#[non_exhaustive]`（F27）：内核加变体不该打断
            // 嵌入方编译，故此处必须有通配臂。（`SqlResult` 无 `Debug`，别用 `{other:?}`）
            _ => ErrorCode::Internal.response("unhandled SqlResult variant".to_string()),
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
    /// 请求目标库（M-B）：`table=db.t` 的限定符优先于本字段；都缺 = 会话默认库。
    #[serde(default)]
    database: Option<String>,
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
    let ident = match authenticate_identity(&s, &headers) {
        Ok(i) => i,
        Err(resp) => return *resp,
    };
    // 目标库：table 限定符（db.t）> ?database= > 会话默认库 > "default"
    let (qdb, table_only) = split_qualified(&p.table);
    let session_default = ident
        .session
        .as_ref()
        .map(|x| x.default_db())
        .unwrap_or_else(|| "default".to_string());
    let target_db = qdb.unwrap_or_else(|| p.database.clone().unwrap_or(session_default));
    if let Err(resp) = ensure_mounted(&s, &target_db) {
        return *resp;
    }
    let (role, session) = match resolve_user(&s, &ident.user, &target_db, ident.session.clone()) {
        Authed::Pass { role, session } => (role, session),
        Authed::NoGrant { user, db } => return no_grant_response(&s, &user, &db),
        Authed::Revoked => {
            return ErrorCode::SessionRevoked.response("session revoked or expired; re-login");
        }
        Authed::Deny => return ErrorCode::Unauthorized.response("unauthorized"),
    };
    // **写入闸**（M-C 补漏）：此前 /write 只查"登录没登录"，reader 也能写——
    // 与 SQL 面的 `role.can_write()` 闸不对齐（配置矩阵承诺 reader 拒写）。
    if !role.can_write() {
        return ErrorCode::Forbidden.response("write requires writer or admin role");
    }
    if s.mounts[&target_db].db.read_only() {
        return ErrorCode::ReadOnly.response("database is read-only");
    }
    s.metrics.requests_total.fetch_add(1, Ordering::Relaxed);
    let mount_db = Arc::clone(&s.mounts[&target_db].db);
    let table = table_only;
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
            let def = match mount_db.table_def(&table) {
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
            let def = match mount_db.table_def(&table) {
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
    let flush = p.flush.unwrap_or(true);
    let expected_generation = p.expected_generation;
    let result = tokio::task::spawn_blocking(move || -> Result<u64, QsdbError> {
        let mount = &st.mounts[&target_db];
        let mount_db = Arc::clone(&mount.db);
        match mode.as_str() {
            "overwrite" => {
                let def = mount_db.table_def(&table)?;
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
                            mount_db.overwrite_partition_cas(&table, &names, &[full], exp)?
                        }
                        None => mount_db.overwrite_partition(&table, &names, &[full])?,
                    };
                    rows += r.rows;
                }
                Ok(rows)
            }
            "append" => {
                mount.writer.push(&table, &batches)?;
                if flush {
                    let r = mount.writer.flush(&table)?;
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
            if let Some(sess) = &session {
                sess.stats.rows_written.fetch_add(rows, Ordering::Relaxed);
            }
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
    if let Err(resp) = auth(&s, &headers) {
        return *resp;
    }
    let want_schema = match parse_bool_param("withSchema", q.with_schema.as_deref()) {
        Ok(v) => v,
        Err(msg) => {
            s.metrics.errors_total.fetch_add(1, Ordering::Relaxed);
            return ErrorCode::InvalidArgument.response(msg);
        }
    };
    let st = Arc::clone(&s);
    // 多挂载：跨库列出，名字给限定形态 `db.table`（可直接当表名查）；单挂载照旧裸名
    let multi = st.mounts.len() > 1;
    let mounts: Vec<(String, Arc<Qsdb>)> = st
        .mount_names()
        .into_iter()
        .map(|n| {
            let db = Arc::clone(&st.mounts[&n].db);
            (n, db)
        })
        .collect();
    match tokio::task::spawn_blocking(move || {
        let mut names = Vec::new();
        let mut out = Vec::new();
        for (dbname, db) in &mounts {
            let list = db.tables()?;
            for t in &list {
                let shown = if multi {
                    format!("{dbname}.{t}")
                } else {
                    t.clone()
                };
                if !want_schema {
                    names.push(shown);
                    continue;
                }
                // J6：JDBC getColumns 原先要 N+1 次请求（列表 + 逐表 schema）。
                // 万级表名下首次连接翻库表明显慢，一次返回列集即可。
                let cols = match db.table_def(t) {
                    Ok(def) => columns_json(&def),
                    Err(_) => Vec::new(), // 并发 DROP 等：跳过而非整体失败
                };
                out.push(serde_json::json!({ "name": shown, "columns": cols }));
            }
        }
        if !want_schema {
            return Ok(serde_json::json!({ "tables": names }));
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

/// 路径参数表的 (目标库, 纯表名) + 该库授权判定（401/403 响应直通）。
/// `db.t` 限定符优先；单段名 = 会话默认库。
fn target_from_path(
    state: &AppState,
    headers: &HeaderMap,
    raw: &str,
) -> std::result::Result<(String, String), Box<Response>> {
    let ident = authenticate_identity(state, headers)?;
    let (qdb, t) = split_qualified(raw);
    let session_default = ident
        .session
        .as_ref()
        .map(|x| x.default_db())
        .unwrap_or_else(|| "default".to_string());
    let target = qdb.unwrap_or(session_default);
    ensure_mounted(state, &target)?;
    match resolve_user(state, &ident.user, &target, ident.session) {
        Authed::Pass { .. } => Ok((target, t)),
        Authed::NoGrant { user, db } => Err(Box::new(no_grant_response(state, &user, &db))),
        Authed::Revoked => Err(Box::new(
            ErrorCode::SessionRevoked.response("session revoked or expired; re-login"),
        )),
        Authed::Deny => Err(Box::new(ErrorCode::Unauthorized.response("unauthorized"))),
    }
}

async fn schema(
    State(s): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(table): Path<String>,
) -> Response {
    let (target_db, table_name) = match target_from_path(&s, &headers, &table) {
        Ok(x) => x,
        Err(resp) => return *resp,
    };
    let table_out = table.clone();
    let st = Arc::clone(&s);
    match tokio::task::spawn_blocking(move || st.mounts[&target_db].db.table_def(&table_name)).await
    {
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
    let (target_db, table_name) = match target_from_path(&s, &headers, &table) {
        Ok(x) => x,
        Err(resp) => return *resp,
    };
    let st = Arc::clone(&s);
    let res =
        tokio::task::spawn_blocking(move || st.mounts[&target_db].db.partition_stats(&table_name))
            .await;
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
    if let Err(resp) = auth(&s, &headers) {
        return *resp;
    }
    let st = Arc::clone(&s);
    let res = tokio::task::spawn_blocking(move || {
        // 多挂载：聚合 + 分库明细（M-B）；单挂载形态不变（mounts 数组只增）
        let mut tables = 0usize;
        let mut bytes = 0u64;
        let mut mounts_out = Vec::new();
        let mut buffered: Vec<_> = Vec::new();
        for name in st.mount_names() {
            let mount = &st.mounts[&name];
            let (t, b) = mount.db.stats()?;
            tables += t;
            bytes += b;
            mounts_out.push(
                serde_json::json!({"database": name, "tables": t, "storeBytes": b}),
            );
            for (tb, batches, by) in mount.writer.buffered() {
                buffered.push(
                    serde_json::json!({"database": name, "table": tb, "batches": batches, "bytes": by}),
                );
            }
        }
        qsdb::Result::Ok(serde_json::json!({
            "tables": tables,
            "storeBytes": bytes,
            "writeBuffer": buffered,
            "mounts": mounts_out,
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
    if let Err(resp) = auth(&s, &headers) {
        return *resp;
    }
    let rt = s.db.runtime();
    let opts = s.db.options();
    Json(serde_json::json!({
        // 运行期（热更生效）
        "runtime": {
            "queryTimeoutMs": rt.query_timeout_ms().unwrap_or(0),
            "queryMemoryLimitBytes": rt.query_memory_limit_bytes().unwrap_or(0),
            // 结果护栏（十七批 R1；0 = 不限）
            "maxResultRows": s.max_result_rows.load(Ordering::Relaxed),
            "maxResultBytes": s.max_result_bytes.load(Ordering::Relaxed),
        },
        // 启动期快照（只读，改动需重启）
        "startup": {
            "scanCacheBytes": opts.scan_cache_bytes,
            "trashGraceMs": opts.trash_grace_ms,
            "spillDir": s.db.spill_dir().map(|p| p.display().to_string()).unwrap_or_default(),
        },
        // 全局内存账本（实时）
        "memory": {
            // 多挂载（M-B）：各库独立预算，这里给总和
            "budgetBytes": s.mounts.values().map(|m| m.db.memory_pool().budget()).sum::<u64>(),
            "execUsedBytes": s.mounts.values().map(|m| m.db.memory_pool().exec_used()).sum::<u64>(),
            "cacheUsedBytes": s.mounts.values().map(|m| m.db.memory_pool().cache_used()).sum::<u64>(),
            "cacheEvictedBytes": s.db.cache_evicted_bytes(),
            "note": "budget=0 表示不限；压力下先挤兑 cache（纯缓存），再拒查询（503 MEMORY_PRESSURE）",
        },
        // 服务器级
        "server": {
            "maxConcurrentQueries": s.max_concurrent_queries,
            "mounts": s.mount_names(),
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
    // 结果护栏（十七批 R1）—— camelCase 别名同样是**必需的**：GET 回的就是
    // camelCase（同上实测教训，不别名则"读-改-写"整轮 PUT 静默失效）
    #[serde(default, alias = "maxResultRows")]
    max_result_rows: Option<u64>,
    #[serde(default, alias = "maxResultBytes")]
    max_result_bytes: Option<u64>,
}

async fn put_config(
    State(s): State<Arc<AppState>>,
    headers: HeaderMap,
    StructuredJson(body): StructuredJson<ConfigUpdate>,
) -> Response {
    if let Err(resp) = auth(&s, &headers) {
        return *resp;
    }
    let rt = s.db.runtime();
    if let Some(ms) = body.query_timeout_ms {
        rt.set_query_timeout_ms(ms);
    }
    if let Some(bytes) = body.query_memory_limit_bytes {
        rt.set_query_memory_limit_bytes(bytes);
    }
    // 结果护栏（十七批 R1）：落 AppState 原子，下一条查询立即可见
    if let Some(n) = body.max_result_rows {
        s.max_result_rows.store(n, Ordering::Relaxed);
    }
    if let Some(bytes) = body.max_result_bytes {
        s.max_result_bytes.store(bytes, Ordering::Relaxed);
    }
    // `applied` = **确实落了至少一个已知字段**（空 body 或全未知/拼错键 → false）。
    // 恒返回 true 会在客户端拼错键时给出"成功"的假信号。
    let applied = body.query_timeout_ms.is_some()
        || body.query_memory_limit_bytes.is_some()
        || body.max_result_rows.is_some()
        || body.max_result_bytes.is_some();
    Json(serde_json::json!({
        "queryTimeoutMs": rt.query_timeout_ms().unwrap_or(0),
        "queryMemoryLimitBytes": rt.query_memory_limit_bytes().unwrap_or(0),
        "maxResultRows": s.max_result_rows.load(Ordering::Relaxed),
        "maxResultBytes": s.max_result_bytes.load(Ordering::Relaxed),
        "applied": applied,
    }))
    .into_response()
}

async fn list_queries(State(s): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    if let Err(resp) = auth(&s, &headers) {
        return *resp;
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
                    "sessionId": e.session.as_ref().map(|x| x.id.clone()),
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
    if let Err(resp) = auth(&s, &headers) {
        return *resp;
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

/// 单批 → JSON 行片段（`rows` 数组元素的逗号拼接；**不含**首尾括号）。
/// `first` = 是否整个结果的第一个批（决定要不要前导逗号——批间那道缝是
/// 流式拼接最容易错的地方，单测锁定）。
///
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
fn batch_rows_to_json_text(b: &RecordBatch, first: bool) -> Result<String, QsdbError> {
    let mut s = String::new();
    for i in 0..b.num_rows() {
        if i > 0 || !first {
            s.push(',');
        }
        let mut row = Vec::with_capacity(b.num_columns());
        for c in 0..b.num_columns() {
            let arr = b.column(c);
            if arr.is_null(i) {
                row.push(serde_json::Value::Null);
            } else {
                row.push(json_value_typed(arr.as_ref(), i)?);
            }
        }
        s.push_str(&serde_json::Value::Array(row).to_string());
    }
    Ok(s)
}

/// 流式 JSON 响应的**头**片段：`{"queryId":…,"schema":[…],"rows":[`。
/// queryId/schema 的序列化经 serde_json 本尊（先建对象、剥尾括号后接
/// `,"rows":[`——schema 恰是末键，`}` 前没有逗号，得自己补），不手拼值——
/// 保证与全量物化路径逐字节同源。
fn json_stream_head(query_id: &str, schema: &arrow::datatypes::SchemaRef) -> String {
    let head_obj = serde_json::json!({
        "queryId": query_id,
        "schema": schema_fields(schema),
    });
    let s = head_obj.to_string();
    format!("{},\"rows\":[", &s[..s.len() - 1])
}

/// 流式 JSON 响应的**尾**片段：`],"rowCount":N,"elapsedMs":X}`（键序随
/// serde_json，合法即可——对象键序无关）。
fn json_stream_tail(rows: usize, elapsed_ms: f64) -> String {
    let tail_obj = serde_json::json!({ "rowCount": rows, "elapsedMs": elapsed_ms });
    let s = tail_obj.to_string();
    // 剥 `{` 补 `],`——rows 收括号与下一个键之间的逗号要自己补（与 head 同理）
    format!("],{}", &s[1..])
}

/// 逐批 JSON 片段流（十七批 R2）：head → 每批一段 → tail。
///
/// 手写 `Stream`——`futures-core` 只提供 trait 本身（已是 tokio-util 的传递
/// 依赖，零新增代码进树）。片段生成是纯 CPU 同步段（单批 8192 行量级），
/// 在 poll 内直接做；**服务端驻留与单个批相关、与总行数无关**。
struct JsonBatchStream {
    batches: std::vec::IntoIter<RecordBatch>,
    head: Option<String>,
    tail: String,
    first: bool,
    done: bool,
}

impl futures_core::Stream for JsonBatchStream {
    /// `Err` = 中途序列化失败（Arrow display 错，实际极罕见）：流以错误收尾，
    /// hyper 截断 body ⇒ 客户端 JSON 解析必失败——**不是"成功的部分数据"**
    /// （throw 不 break 同款判据；200 头已发出，截断是唯一诚实的失败形态）。
    type Item = Result<axum::body::Bytes, Box<dyn std::error::Error + Send + Sync>>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use std::task::Poll;
        Poll::Ready(if let Some(h) = self.head.take() {
            Some(Ok(axum::body::Bytes::from(h)))
        } else if let Some(b) = self.batches.next() {
            match batch_rows_to_json_text(&b, self.first) {
                Ok(text) => {
                    self.first = false;
                    Some(Ok(axum::body::Bytes::from(text)))
                }
                Err(e) => Some(Err(Box::new(e))),
            }
        } else if !self.done {
            self.done = true;
            Some(Ok(axum::body::Bytes::from(std::mem::take(&mut self.tail))))
        } else {
            None
        })
    }
}

/// 查询结果的 JSON 响应——**逐批流式**（十七批 R2）。
///
/// 原 `batches_to_json` 先把全部行攒成 `Vec<Vec<Value>>` 再序列化：500 万行
/// 的中间体是 行×列 个 `serde_json::Value`（每个 ~24B 起含装箱），服务端 RSS
/// 跟着结果体量走尖峰。现按批产出片段（`Body::from_stream`）。**契约不变**：
/// 响应仍是同一个 JSON 对象 `{"queryId","schema","rows":[…],"rowCount",
/// "elapsedMs"}`——对象键序无关，任何 JSON 解析器按名取值；流式只改变
/// "什么时候吐字节"，不改字段、不改值形态。
fn json_query_response(
    query_id: &str,
    schema: &arrow::datatypes::SchemaRef,
    batches: Vec<RecordBatch>,
    rows: usize,
    elapsed_ms: f64,
) -> Response {
    let stream = JsonBatchStream {
        batches: batches.into_iter(),
        head: Some(json_stream_head(query_id, schema)),
        tail: json_stream_tail(rows, elapsed_ms),
        first: true,
        done: false,
    };
    (
        [(
            axum::http::header::CONTENT_TYPE,
            axum::http::HeaderValue::from_static("application/json"),
        )],
        Body::from_stream(stream),
    )
        .into_response()
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

#[cfg(test)]
mod tests {
    use super::*;

    /// 空库 + 空 AppState（内存态会话表不落盘，Qsdb 只是给注册表供给 ctx）。
    fn test_state() -> (Arc<AppState>, std::path::PathBuf) {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let uniq = SEQ.fetch_add(1, Ordering::Relaxed);
        let root =
            std::env::temp_dir().join(format!("qsdb_session_ut_{}_{}", std::process::id(), uniq));
        let _ = std::fs::remove_dir_all(&root);
        let db = Arc::new(Qsdb::open(&root, qsdb::QsdbOptions::default()).expect("open temp db"));
        let state = Arc::new(AppState::with_mounts(
            vec![("default".to_string(), db)],
            None,
            crate::commit_log::CommitLog::new(),
            0,
        ));
        (state, root)
    }

    /// 造一个会话；`idle_ms_ago` = 把 last_active 拨回多少毫秒（测空闲回收）。
    /// 返回 (token, session)。
    fn make_session(id: &str, idle_ms_ago: u64) -> (String, Arc<Session>) {
        let now = Session::epoch_ms();
        let s = Arc::new(Session {
            id: id.to_string(),
            user: "bob".to_string(),
            client: "127.0.0.1:1; ut".to_string(),
            created_at_ms: now,
            last_active_ms: AtomicU64::new(now - idle_ms_ago),
            default_db: std::sync::Mutex::new("default".to_string()),
            stats: SessionStats::default(),
        });
        (new_session_token(), s)
    }

    /// M-A 验收第一条：踢会话 ⇒ 它的在跑查询全取消、别人的不连坐、token 失效。
    #[test]
    fn revoke_kicks_session_and_cancels_its_queries() {
        let (state, root) = test_state();
        let (tok_a, sa) = make_session("s-a", 0);
        let (tok_b, sb) = make_session("s-b", 0);
        state
            .sessions
            .lock()
            .unwrap()
            .insert(tok_a.clone(), Arc::clone(&sa));
        state
            .sessions
            .lock()
            .unwrap()
            .insert(tok_b.clone(), Arc::clone(&sb));

        // a 跑两条（其中一条带客户端 id）、b 跑一条
        let db = Arc::clone(&state.db);
        let q1 = state
            .register_query(&db, None, None, "SELECT 1".into(), Some(&sa))
            .unwrap();
        let q2 = state
            .register_query(
                &db,
                Some("drv-x".into()),
                None,
                "SELECT 2".into(),
                Some(&sa),
            )
            .unwrap();
        let q3 = state
            .register_query(&db, None, None, "SELECT 3".into(), Some(&sb))
            .unwrap();
        assert_eq!(state.queries.lock().unwrap().len(), 3);
        assert_eq!(
            sa.stats.queries_total.load(Ordering::Relaxed),
            2,
            "注册时计一次 queriesTotal"
        );

        let cancelled = state.revoke_session("s-a").expect("session exists");
        assert_eq!(cancelled, 2, "a 的两条都该被取消");
        assert!(q1.is_cancelled() && q2.is_cancelled());
        assert!(!q3.is_cancelled(), "b 的查询不该被连坐");
        // a 的 token 已失效、b 的还在
        assert!(!state.sessions.lock().unwrap().contains_key(&tok_a));
        assert!(state.sessions.lock().unwrap().contains_key(&tok_b));
        // 再踢一次 → None（不存在）
        assert!(state.revoke_session("s-a").is_none());
        let _ = std::fs::remove_dir_all(&root);
    }

    /// 空闲回收只踢超时的：活跃会话原样保留。
    #[test]
    fn sweep_idle_kicks_only_expired() {
        let (state, root) = test_state();
        let (tok_old, s_old) = make_session("s-old", 10 * 60 * 1000); // 空闲 10 分钟
        let (tok_new, s_new) = make_session("s-new", 0);
        {
            let mut m = state.sessions.lock().unwrap();
            m.insert(tok_old.clone(), Arc::clone(&s_old));
            m.insert(tok_new.clone(), Arc::clone(&s_new));
        }
        let swept = sweep_idle_sessions(&state, std::time::Duration::from_secs(60));
        assert_eq!(swept, 1, "只回收超时那一个");
        let alive: Vec<String> = state
            .sessions
            .lock()
            .unwrap()
            .values()
            .map(|s| s.id.clone())
            .collect();
        assert_eq!(alive, vec!["s-new"]);
        assert!(state.sessions.lock().unwrap().contains_key(&tok_new));
        let _ = std::fs::remove_dir_all(&root);
    }

    /// 会话 token 前缀契约：签发的都在 `sess-` 下——这是"吊销可判定"的根据
    /// （不在表里的 `sess-*` ⇒ 401 SESSION_REVOKED，而非笼统 401）。
    #[test]
    fn session_tokens_are_prefixed() {
        let (tok, _) = make_session("s-x", 0);
        assert!(tok.starts_with(SESSION_TOKEN_PREFIX));
        assert!(new_session_id().starts_with("s-"));
    }

    /// 双挂载 AppState（M-B 测试底座）。
    fn two_mount_state() -> (Arc<AppState>, std::path::PathBuf, std::path::PathBuf) {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let uniq = SEQ.fetch_add(1, Ordering::Relaxed);
        let mk = |tag: &str| {
            let d = std::env::temp_dir()
                .join(format!("qsdb_mb_ut_{}_{uniq}_{tag}", std::process::id()));
            let _ = std::fs::remove_dir_all(&d);
            d
        };
        let (d1, d2) = (mk("a"), mk("b"));
        let db1 = Arc::new(Qsdb::open(&d1, qsdb::QsdbOptions::default()).expect("open a"));
        let db2 = Arc::new(Qsdb::open(&d2, qsdb::QsdbOptions::default()).expect("open b"));
        let state = Arc::new(AppState::with_mounts(
            vec![("default".to_string(), db1), ("settle".to_string(), db2)],
            None,
            crate::commit_log::CommitLog::new(),
            0,
        ));
        (state, d1, d2)
    }

    /// 限定名路由（M-B 核心）：剥限定符、单段名锚定、显式库、跨库拒、
    /// 未挂载拒、DDL 目标（Drop/CreateTable 是 sqlparser visitor 的盲区）。
    #[test]
    fn route_sql_qualified_anchor_and_rejects() {
        let (state, d1, d2) = two_mount_state();

        // 限定名 → 剥掉并路由
        let (sql, db) = route_sql(&state, "SELECT * FROM settle.t", None, "default").unwrap();
        assert_eq!(db, "settle");
        assert!(!sql.contains("settle"), "{sql}");
        // 单段名锚定会话默认库（**不浮动**到限定库——那是错数据级事故）
        let (_, db) = route_sql(&state, "SELECT * FROM t", None, "default").unwrap();
        assert_eq!(db, "default");
        // 显式库最高
        let (_, db) = route_sql(&state, "SELECT * FROM t", Some("settle"), "default").unwrap();
        assert_eq!(db, "settle");
        // 无限定名 + 无显式 → 会话默认库
        let (_, db) = route_sql(&state, "SELECT 1", None, "default").unwrap();
        assert_eq!(db, "default");

        // 跨库：单段名锚 default + 限定 settle → 显式拒（501 UNSUPPORTED）
        let e = route_sql(
            &state,
            "SELECT count(*) FROM t JOIN settle.t ON true",
            None,
            "default",
        )
        .unwrap_err();
        assert_eq!(e.status().as_u16(), 501, "{e:?}");
        // 同库双限定是合法的（都落 settle），不拒
        let (_, db) =
            route_sql(&state, "SELECT * FROM settle.a, settle.b", None, "default").unwrap();
        assert_eq!(db, "settle");

        // 未挂载 → 404
        let e = route_sql(&state, "SELECT * FROM nope.t", None, "default").unwrap_err();
        assert_eq!(e.status().as_u16(), 404);

        // DDL 目标（visitor 盲区，补刀覆盖）
        let (sql, db) = route_sql(&state, "DROP TABLE settle.x", None, "default").unwrap();
        assert_eq!(db, "settle");
        assert!(!sql.contains("settle"), "{sql}");
        let (sql, db) =
            route_sql(&state, "CREATE TABLE settle.x (a BIGINT)", None, "default").unwrap();
        assert_eq!(db, "settle");
        assert!(!sql.contains("settle"), "{sql}");
        // INSERT 目标
        let (sql, db) =
            route_sql(&state, "INSERT INTO settle.t VALUES (1)", None, "default").unwrap();
        assert_eq!(db, "settle");
        assert!(!sql.contains("settle"), "{sql}");
        // 子查询里的限定名
        let (sql, db) = route_sql(
            &state,
            "SELECT count(*) FROM (SELECT a FROM settle.t WHERE a > 0) x",
            None,
            "default",
        )
        .unwrap();
        assert_eq!(db, "settle");
        assert!(!sql.contains("settle"), "{sql}");

        let _ = std::fs::remove_dir_all(&d1);
        let _ = std::fs::remove_dir_all(&d2);
    }

    /// USE / 元数据语句识别。
    #[test]
    fn use_and_metadata_statement_parsing() {
        assert_eq!(parse_use_statement("USE settle"), Some("settle".into()));
        assert_eq!(parse_use_statement("  use SETTLE ;"), Some("settle".into()));
        assert_eq!(parse_use_statement("SELECT 1"), None);
        assert_eq!(parse_use_statement("USE 9bad"), None);
        assert_eq!(parse_use_statement("USE"), None);
        assert_eq!(
            parse_use_statement("UPDATE t SET a = 1"),
            None,
            "USE 前缀匹配必须整语句"
        );
        assert!(is_show_databases(" show databases "));
        assert!(!is_show_databases("SHOW TABLES"));

        let (state, d1, d2) = two_mount_state();
        // SHOW PARTITIONS settle.t → 剥 + 路由
        let (sql, db) = route_metadata_statement(&state, "SHOW PARTITIONS settle.t")
            .expect("metadata")
            .unwrap();
        assert_eq!(db, "settle");
        assert_eq!(sql, "SHOW PARTITIONS t");
        // DESCRIBE 同理
        let (_, db) = route_metadata_statement(&state, "DESCRIBE settle.t")
            .expect("metadata")
            .unwrap();
        assert_eq!(db, "settle");
        // 单段名 → 不拦（交 route_sql 落会话默认库）
        assert!(route_metadata_statement(&state, "DESCRIBE t").is_none());
        // 未挂载 → 404
        let e = route_metadata_statement(&state, "SHOW PARTITIONS nope.t")
            .expect("metadata")
            .unwrap_err();
        assert_eq!(e.status().as_u16(), 404);
        let _ = std::fs::remove_dir_all(&d1);
        let _ = std::fs::remove_dir_all(&d2);
    }

    /// R1 覆盖优先级：body > 头 > 服务器值；覆盖给出即**接管护栏整体**
    /// （体量闸随行覆盖一并放开——护栏只有一个豁免口径）；0 = 本请求不限；
    /// 两侧都没给 → 服务器值原样。
    #[test]
    fn effective_result_guard_precedence() {
        // body 压过头
        assert_eq!(effective_result_guard(Some(7), Some(3), 100, 200), (7, 0));
        // 头压过服务器值
        assert_eq!(effective_result_guard(None, Some(3), 100, 200), (3, 0));
        // 都没给 → 服务器值（含"服务器也没设"= 双 0 = 关）
        assert_eq!(effective_result_guard(None, None, 100, 200), (100, 200));
        assert_eq!(effective_result_guard(None, None, 0, 0), (0, 0));
        // 0 = 显式不限（本请求豁免），不是"回退服务器值"
        assert_eq!(effective_result_guard(Some(0), None, 100, 200), (0, 0));
        // 覆盖可以**收紧**（服务器关、本请求自限）——双向覆盖
        assert_eq!(effective_result_guard(Some(50), None, 0, 0), (50, 0));
    }

    /// R1 字节估算口径：Arrow 数组内存量随行数增长（非零、单调不减）。
    #[test]
    fn batches_arrow_bytes_tracks_rows() {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("a", arrow::datatypes::DataType::Int64, false),
        ]));
        let mk = |n: i64| {
            let arr = arrow::array::Int64Array::from((0..n).collect::<Vec<i64>>());
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(arr)]).unwrap()
        };
        let empty = mk(0);
        let small = mk(10);
        let big = mk(10_000);
        let b_empty = batches_arrow_bytes(&[empty]);
        let b_small = batches_arrow_bytes(&[small]);
        let b_big = batches_arrow_bytes(&[big]);
        assert!(b_small > b_empty, "行数增长 ⇒ 估算增长");
        assert!(b_big > b_small, "行数增长 ⇒ 估算增长");
    }

    /// R2：头片段 + 跨批行片段 + 尾片段 拼出**同一个 JSON 对象**——键值形态
    /// 与全量物化路径逐字段一致（含 NULL → null、批间逗号那道缝）。
    #[test]
    fn json_stream_fragments_reassemble_to_same_shape() {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("a", arrow::datatypes::DataType::Int64, true),
            arrow::datatypes::Field::new("s", arrow::datatypes::DataType::Utf8, true),
        ]));
        let mk = |vals: Vec<(Option<i64>, Option<&str>)>| {
            let a = arrow::array::Int64Array::from(vals.iter().map(|v| v.0).collect::<Vec<_>>());
            let s = arrow::array::StringArray::from(vals.iter().map(|v| v.1).collect::<Vec<_>>());
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(a), Arc::new(s)]).unwrap()
        };
        let b1 = mk(vec![(Some(1), Some("x")), (None, None)]);
        let b2 = mk(vec![(Some(2), Some("y"))]);

        let mut text = String::new();
        text.push_str(&batch_rows_to_json_text(&b1, true).unwrap());
        text.push_str(&batch_rows_to_json_text(&b2, false).unwrap());
        let full = format!(
            "{}{}{}",
            json_stream_head("q-test", &schema),
            text,
            json_stream_tail(3, 1.5)
        );
        let v: serde_json::Value = serde_json::from_str(&full).expect("片段拼接必须是合法 JSON");
        assert_eq!(v["queryId"], "q-test");
        assert_eq!(v["rowCount"], 3);
        assert_eq!(v["elapsedMs"], 1.5);
        assert_eq!(v["schema"][0]["name"], "a");
        assert_eq!(v["schema"][0]["type"], "Int64");
        let rows = v["rows"].as_array().expect("rows 是数组");
        assert_eq!(rows.len(), 3, "跨批拼接不得丢行");
        assert_eq!(rows[0][0], 1);
        assert_eq!(rows[0][1], "x");
        assert!(
            rows[1][0].is_null() && rows[1][1].is_null(),
            "NULL 保持 null: {}",
            rows[1]
        );
        assert_eq!(rows[2][0], 2, "第二批的首行（批间逗号缝）");
        assert_eq!(rows[2][1], "y");

        // 空批：first=true 的空批产出空片段——head+tail 直接闭合，仍合法
        let empty_text = batch_rows_to_json_text(&mk(vec![]), true).unwrap();
        let full0 = format!(
            "{}{}{}",
            json_stream_head("q0", &schema),
            empty_text,
            json_stream_tail(0, 0.0)
        );
        let v0: serde_json::Value = serde_json::from_str(&full0).expect("空结果合法 JSON");
        assert_eq!(v0["rowCount"], 0);
        assert_eq!(v0["rows"].as_array().unwrap().len(), 0);
    }
}
