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

/// 运行中查询注册条目。
pub struct QueryEntry {
    pub ctx: Arc<qsdb::query::QueryContext>,
    pub sql: String,
}

/// 服务共享状态。
pub struct AppState {
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
    fn register_query(
        &self,
        ctx: Arc<qsdb::query::QueryContext>,
        sql: String,
    ) -> std::result::Result<(), String> {
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
        q.insert(ctx.query_id.clone(), QueryEntry { ctx, sql });
        Ok(())
    }

    fn unregister_query(&self, id: &str) {
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
             # HELP qsdb_store_bytes total store bytes\n# TYPE qsdb_store_bytes gauge\nqsdb_store_bytes {}\n",
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
        )
    }
}

/// 启动服务（阻塞至 Ctrl-C）。
#[allow(clippy::too_many_arguments)]
pub async fn serve(
    db: Arc<Qsdb>,
    addr: &str,
    token: Option<String>,
    auto_compact_secs: Option<u64>,
    max_concurrent_queries: Option<usize>,
) -> Result<(), Box<dyn std::error::Error>> {
    let writer = db.writer(WriteBufferOptions::default());
    let state = Arc::new(AppState {
        db,
        writer,
        token,
        metrics: Metrics::default(),
        queries: std::sync::Mutex::new(HashMap::new()),
        max_concurrent_queries: max_concurrent_queries.unwrap_or(0),
    });

    // 写入走大 body（Arrow IPC 批）；默认 2MB 限制放大到 1GB
    let write_layer = axum::extract::DefaultBodyLimit::max(1024 * 1024 * 1024);
    let app = Router::new()
        .route("/health", get(health))
        .route("/metrics", get(metrics))
        .route("/api/v1/query", post(query))
        .route("/api/v1/write", post(write))
        .route("/api/v1/tables", get(tables))
        .route("/api/v1/tables/{table}/schema", get(schema))
        .route("/api/v1/tables/{table}/partitions", get(partitions))
        .route("/api/v1/stats", get(stats))
        .route("/api/v1/queries", get(list_queries))
        .route("/api/v1/queries/{id}/cancel", post(cancel_query))
        .layer(write_layer)
        .with_state(Arc::clone(&state));

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
    eprintln!(
        "qsdb server listening on http://{addr} (token auth: {})",
        if state.token.is_some() { "on" } else { "off" }
    );

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
    // 退出前 flush 写缓冲
    let st = Arc::clone(&state);
    let _ = tokio::task::spawn_blocking(move || st.writer.flush_all()).await;
    result.map_err(Into::into)
}

// ---------------------------------------------------------------------------
// handlers
// ---------------------------------------------------------------------------

fn auth(state: &AppState, headers: &HeaderMap) -> StatusCode {
    match &state.token {
        None => StatusCode::OK,
        Some(expect) => match headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
        {
            Some(got) if got == expect => StatusCode::OK,
            _ => StatusCode::UNAUTHORIZED,
        },
    }
}

fn err_response(status: StatusCode, msg: String) -> Response {
    (status, Json(serde_json::json!({ "error": msg }))).into_response()
}

async fn health(State(_s): State<Arc<AppState>>) -> impl IntoResponse {
    Json(serde_json::json!({ "status": "ok", "version": env!("CARGO_PKG_VERSION") }))
}

async fn metrics(State(s): State<Arc<AppState>>) -> Response {
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
struct QueryBody {
    sql: String,
    #[serde(default)]
    format: Option<String>,
}

async fn query(
    State(s): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<QueryBody>,
) -> Response {
    if let StatusCode::UNAUTHORIZED = auth(&s, &headers) {
        return err_response(StatusCode::UNAUTHORIZED, "unauthorized".into());
    }
    s.metrics.requests_total.fetch_add(1, Ordering::Relaxed);
    // 会话：每请求一个 QueryContext（超时可经 X-QSDB-Timeout 头覆盖）
    let timeout_ms: Option<u64> = headers
        .get("x-qsdb-timeout-ms")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse().ok());
    let ctx = s.db.new_query_context(timeout_ms);
    if let Err(msg) = s.register_query(Arc::clone(&ctx), body.sql.clone()) {
        s.metrics.errors_total.fetch_add(1, Ordering::Relaxed);
        return err_response(StatusCode::SERVICE_UNAVAILABLE, msg);
    }
    let query_id = ctx.query_id.clone();
    let reg = Arc::clone(&s);
    let t = Instant::now();
    let st = Arc::clone(&s);
    let sql = body.sql;
    let fmt = body.format.unwrap_or_else(|| "json".into());
    let ctx_exec = Arc::clone(&ctx);
    let result = tokio::task::spawn_blocking(move || st.db.sql_with_context(&sql, &ctx_exec)).await;
    reg.unregister_query(&query_id);
    let elapsed = t.elapsed().as_nanos() as u64;
    s.metrics.observe_query(elapsed);
    match result {
        Ok(Ok(r)) => match r {
            SqlResult::Query { schema, batches } => {
                let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                if fmt.eq_ignore_ascii_case("ipc") {
                    match batches_to_ipc(&schema, &batches) {
                        Ok(bytes) => (
                            [(
                                axum::http::header::CONTENT_TYPE,
                                "application/vnd.apache.arrow.stream",
                            )],
                            bytes,
                        )
                            .into_response(),
                        Err(e) => {
                            s.metrics.errors_total.fetch_add(1, Ordering::Relaxed);
                            err_response(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
                        }
                    }
                } else {
                    match batches_to_json(&schema, &batches) {
                        Ok(v) => Json(serde_json::json!({
                            "schema": schema_fields(&schema),
                            "rows": v,
                            "rowCount": rows,
                            "elapsedMs": elapsed as f64 / 1e6,
                        }))
                        .into_response(),
                        Err(e) => {
                            s.metrics.errors_total.fetch_add(1, Ordering::Relaxed);
                            err_response(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
                        }
                    }
                }
            }
            SqlResult::Ok { message } => Json(serde_json::json!({
                "message": message,
                "elapsedMs": elapsed as f64 / 1e6,
            }))
            .into_response(),
        },
        Ok(Err(e)) => {
            s.metrics.errors_total.fetch_add(1, Ordering::Relaxed);
            status_of(&e, e.to_string())
        }
        Err(e) => err_response(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

#[derive(serde::Deserialize)]
struct WriteParams {
    table: String,
    #[serde(default)]
    mode: Option<String>,
    #[serde(default)]
    flush: Option<bool>,
}

async fn write(
    State(s): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(p): Query<WriteParams>,
    body: axum::body::Bytes,
) -> Response {
    if let StatusCode::UNAUTHORIZED = auth(&s, &headers) {
        return err_response(StatusCode::UNAUTHORIZED, "unauthorized".into());
    }
    s.metrics.requests_total.fetch_add(1, Ordering::Relaxed);
    let t = Instant::now();
    // IPC 解码（同步 CPU 工作，直接做）
    let batches = match ipc_to_batches(&body) {
        Ok(b) => b,
        Err(e) => {
            s.metrics.errors_total.fetch_add(1, Ordering::Relaxed);
            return err_response(StatusCode::BAD_REQUEST, e.to_string());
        }
    };
    let mode = p.mode.unwrap_or_else(|| "append".into());
    let mode_out = mode.clone();
    let st = Arc::clone(&s);
    let table = p.table.clone();
    let flush = p.flush.unwrap_or(true);
    let result = tokio::task::spawn_blocking(move || -> Result<u64, QsdbError> {
        match mode.as_str() {
            "overwrite" => {
                let def = st.db.table_def(&table)?;
                let batch = concat_batches(&batches)?;
                let groups = qsdb::partition::split_by_partition(&batch, &def)?;
                let mut rows = 0u64;
                for (vals, data) in groups {
                    let names: Vec<(&str, qsdb::Literal)> = def
                        .partition_keys
                        .iter()
                        .zip(vals.iter())
                        .map(|(k, v)| (k.as_str(), v.clone()))
                        .collect();
                    let r = st.db.overwrite_partition(&table, &names, &[data])?;
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
            status_of(&e, e.to_string())
        }
        Err(e) => err_response(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

async fn tables(State(s): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    if let StatusCode::UNAUTHORIZED = auth(&s, &headers) {
        return err_response(StatusCode::UNAUTHORIZED, "unauthorized".into());
    }
    let st = Arc::clone(&s);
    match tokio::task::spawn_blocking(move || st.db.tables()).await {
        Ok(Ok(list)) => Json(serde_json::json!({ "tables": list })).into_response(),
        Ok(Err(e)) => status_of(&e, e.to_string()),
        Err(e) => err_response(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

async fn schema(
    State(s): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(table): Path<String>,
) -> Response {
    if let StatusCode::UNAUTHORIZED = auth(&s, &headers) {
        return err_response(StatusCode::UNAUTHORIZED, "unauthorized".into());
    }
    let table_out = table.clone();
    let st = Arc::clone(&s);
    match tokio::task::spawn_blocking(move || st.db.table_def(&table)).await {
        Ok(Ok(def)) => {
            let cols: Vec<serde_json::Value> = def
                .columns
                .iter()
                .map(|c| {
                    serde_json::json!({
                        "name": c.name,
                        "type": c.logical_type.to_string(),
                        "nullable": c.nullable,
                        "partition": def.partition_keys.contains(&c.name),
                    })
                })
                .collect();
            Json(serde_json::json!({ "table": table_out, "columns": cols })).into_response()
        }
        Ok(Err(e)) => status_of(&e, e.to_string()),
        Err(e) => err_response(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

async fn partitions(
    State(s): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(table): Path<String>,
) -> Response {
    if let StatusCode::UNAUTHORIZED = auth(&s, &headers) {
        return err_response(StatusCode::UNAUTHORIZED, "unauthorized".into());
    }
    let st = Arc::clone(&s);
    let res = tokio::task::spawn_blocking(move || {
        let sql = format!("SHOW PARTITIONS {table}");
        st.db.sql(&sql)
    })
    .await;
    match res {
        Ok(Ok(SqlResult::Query { batches, .. })) => {
            let mut parts: Vec<String> = Vec::new();
            for b in &batches {
                for i in 0..b.num_rows() {
                    if let Ok(v) =
                        arrow::util::display::array_value_to_string(b.column(0).as_ref(), i)
                    {
                        parts.push(v);
                    }
                }
            }
            parts.sort();
            Json(serde_json::json!({ "partitions": parts, "count": parts.len() })).into_response()
        }
        Ok(Ok(SqlResult::Ok { message })) => err_response(StatusCode::BAD_REQUEST, message),
        Ok(Err(e)) => status_of(&e, e.to_string()),
        Err(e) => err_response(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

async fn stats(State(s): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    if let StatusCode::UNAUTHORIZED = auth(&s, &headers) {
        return err_response(StatusCode::UNAUTHORIZED, "unauthorized".into());
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
        Err(e) => err_response(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}

async fn list_queries(State(s): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    if let StatusCode::UNAUTHORIZED = auth(&s, &headers) {
        return err_response(StatusCode::UNAUTHORIZED, "unauthorized".into());
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
            return err_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "registry poisoned".into(),
            );
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
        return err_response(StatusCode::UNAUTHORIZED, "unauthorized".into());
    }
    match s.queries.lock() {
        Ok(q) => match q.get(&id) {
            Some(e) => {
                e.ctx.cancel();
                Json(serde_json::json!({ "cancelled": id })).into_response()
            }
            None => err_response(StatusCode::NOT_FOUND, format!("query {id} not running")),
        },
        Err(_) => err_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            "registry poisoned".into(),
        ),
    }
}

// ---------------------------------------------------------------------------
// 辅助
// ---------------------------------------------------------------------------

fn status_of(e: &QsdbError, msg: String) -> Response {
    let code = match e {
        QsdbError::NotFound(_) => StatusCode::NOT_FOUND,
        QsdbError::Parse(_) | QsdbError::InvalidArgument(_) => StatusCode::BAD_REQUEST,
        QsdbError::Unsupported(_) => StatusCode::NOT_IMPLEMENTED,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    err_response(code, msg)
}

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

fn schema_fields(schema: &arrow::datatypes::SchemaRef) -> Vec<serde_json::Value> {
    schema
        .fields()
        .iter()
        .map(|f| serde_json::json!({ "name": f.name(), "type": f.data_type().to_string() }))
        .collect()
}

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
                    let s = arrow::util::display::array_value_to_string(arr.as_ref(), i)
                        .map_err(|e| QsdbError::invalid(format!("display: {e}")))?;
                    // 数值类型尽量输出数字
                    row.push(if let Ok(n) = s.parse::<i64>() {
                        serde_json::json!(n)
                    } else if let Ok(f) = s.parse::<f64>() {
                        serde_json::json!(f)
                    } else if s == "true" {
                        serde_json::json!(true)
                    } else if s == "false" {
                        serde_json::json!(false)
                    } else {
                        serde_json::json!(s)
                    });
                }
            }
            rows.push(row);
        }
    }
    Ok(rows)
}

// Body 引用占位（保持 import；axum 0.8 body 直接用 Bytes）
#[allow(unused)]
fn _body_hint(_: Body) {}

// HashMap 引用占位
#[allow(unused)]
fn _hm_hint(_: HashMap<String, String>) {}
