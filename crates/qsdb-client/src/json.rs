//! JSON 通道与元数据端点的响应解析。
//!
//! JSON 是**调试与非二进制场景**用的通道（体积 5–10×、每值一次解析）。
//! 但它有两个不可替代的用途：
//!
//! 1. **DDL/DML 回执**：`CREATE` / `INSERT` / `DELETE` 的响应**恒为 JSON**
//!    （哪怕你请求的是 `bulk`/`rows`）——服务端对非查询结果只有这一种回。
//!    所以客户端必须**按 `Content-Type` 分派**，而不是按自己请求的 format 分派。
//! 2. **元数据**：`/tables` `/schema` `/partitions` `/stats` `/error-codes`。
//!
//! ## 值形态（J1/J2/J7，别在这里"改良"）
//!
//! 服务端**按列类型分派**输出：数值列 number、`Utf8` 列**恒 string**
//! （`'123'` 输出 `"123"`，不因内容是数字而改写）、`Binary` 输出小写 hex、
//! `Decimal128` 输出字符串（不丢精度）、`NaN`/`±Inf` 输出固定拼写。
//! **客户端不做任何数值启发**——它正是那批事故的根因（`"123"` 被当数字、
//! hex 被按十进制解析导致字节换血）。取到的 [`serde_json::Value`] **原样**交给调用方，
//! 它想知道类型就看 `schema`。

use crate::error::{Error, Result};
use crate::value::ColumnInfo;

/// JSON 通道的查询结果 / 回执（两者共用一个结构，字段按有无填充）。
#[derive(Debug, Clone, Default)]
pub struct JsonResult {
    /// 服务端**实际使用**的 query id（并发冲突回退时与请求值不同，以它为准）。
    pub query_id: Option<String>,
    /// 列集（DDL/DML 回执为空）。
    pub schema: Vec<ColumnInfo>,
    /// 行（`值形态见模块头`）。
    pub rows: Vec<Vec<serde_json::Value>>,
    /// `rowCount`（查询才有）。
    pub row_count: Option<usize>,
    /// 服务端侧耗时（毫秒；**不含网络**）。
    pub elapsed_ms: Option<f64>,
    /// 人读回执（**文案不做兼容承诺**，判分支请用 `command` / `rows_affected`）。
    pub message: Option<String>,
    /// 结构化命令名（`INSERT` / `DELETE` / `CREATE` …）。
    pub command: Option<String>,
    /// DML 影响行数（DDL 无此字段）。
    pub rows_affected: Option<u64>,
}

impl JsonResult {
    /// 是不是 DDL/DML 回执（没有结果集）。
    pub fn is_exec(&self) -> bool {
        self.row_count.is_none() && self.message.is_some()
    }

    /// 行数（查询回 `rowCount`；回执回 `rows_affected`；都没有 → 0）。
    pub fn affected_or_rows(&self) -> u64 {
        self.rows_affected
            .unwrap_or_else(|| self.row_count.unwrap_or(0) as u64)
    }
}

#[derive(serde::Deserialize)]
struct RawField {
    name: String,
    #[serde(rename = "type")]
    type_name: String,
}

#[derive(serde::Deserialize)]
struct RawResult {
    #[serde(default, rename = "queryId")]
    query_id: Option<String>,
    #[serde(default)]
    schema: Vec<RawField>,
    #[serde(default)]
    rows: Vec<Vec<serde_json::Value>>,
    #[serde(default, rename = "rowCount")]
    row_count: Option<usize>,
    #[serde(default, rename = "elapsedMs")]
    elapsed_ms: Option<f64>,
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    command: Option<String>,
    #[serde(default, rename = "rowsAffected")]
    rows_affected: Option<u64>,
}

impl JsonResult {
    /// 解析 `POST /api/v1/query`（`format=json`）或任何回执体。
    pub(crate) fn parse(bytes: &[u8]) -> Result<Self> {
        let raw: RawResult = serde_json::from_slice(bytes)
            .map_err(|e| Error::Decode(format!("json: 响应体不是预期的结果结构: {e}")))?;
        Ok(Self {
            query_id: raw.query_id,
            schema: raw
                .schema
                .into_iter()
                .map(|f| ColumnInfo {
                    name: f.name,
                    type_name: f.type_name,
                    // JSON 通道的 schema 只给 name/type（`/schema` 端点才给这两项）
                    nullable: true,
                    partition: false,
                })
                .collect(),
            rows: raw.rows,
            row_count: raw.row_count,
            elapsed_ms: raw.elapsed_ms,
            message: raw.message,
            command: raw.command,
            rows_affected: raw.rows_affected,
        })
    }
}

// ---------------------------------------------------------------------------
// 元数据端点
// ---------------------------------------------------------------------------

/// 一张表 + 它的列（`GET /api/v1/tables?withSchema=1`）。
#[derive(Debug, Clone)]
pub struct TableInfo {
    /// 表名。
    pub name: String,
    /// 列。
    pub columns: Vec<ColumnInfo>,
}

/// 一个分区（`GET /api/v1/tables/{t}/partitions` 的 `detail`）。
#[derive(Debug, Clone)]
pub struct PartitionInfo {
    /// 分区名（目录名形态）。
    pub name: String,
    /// **分区提交计数器**：每次提交 +1，回答"改过几次"。
    pub generation: u64,
    /// 行数。
    pub rows: u64,
    /// 段数。
    pub segments: u64,
}

/// 库统计（`GET /api/v1/stats`）。
#[derive(Debug, Clone)]
pub struct Stats {
    /// 表数。
    pub tables: usize,
    /// 存储字节数。
    pub store_bytes: u64,
    /// 写缓冲（表名, 批数, 字节数）。
    pub write_buffer: Vec<(String, u64, u64)>,
}

#[derive(serde::Deserialize)]
struct RawColumn {
    name: String,
    #[serde(rename = "type")]
    type_name: String,
    #[serde(default)]
    nullable: bool,
    #[serde(default)]
    partition: bool,
}

impl From<RawColumn> for ColumnInfo {
    fn from(c: RawColumn) -> Self {
        ColumnInfo {
            name: c.name,
            type_name: c.type_name,
            nullable: c.nullable,
            partition: c.partition,
        }
    }
}

#[derive(serde::Deserialize)]
struct RawTableInfo {
    name: String,
    #[serde(default)]
    columns: Vec<RawColumn>,
}

#[derive(serde::Deserialize)]
#[serde(untagged)]
enum RawTables {
    /// `{"tables": ["a","b"]}`
    Names(Vec<String>),
    /// `{"tables": [{"name":…,"columns":[…]}]}`（`?withSchema=1`）
    Detailed(Vec<RawTableInfo>),
}

#[derive(serde::Deserialize)]
struct RawTablesBody {
    tables: RawTables,
}

/// 解析 `/tables`（两种形态都认）。
pub(crate) enum TablesBody {
    /// 仅表名。
    Names(Vec<String>),
    /// 表名 + 列。
    Detailed(Vec<TableInfo>),
}

pub(crate) fn parse_tables(bytes: &[u8]) -> Result<TablesBody> {
    let b: RawTablesBody = serde_json::from_slice(bytes)
        .map_err(|e| Error::Decode(format!("json: /tables 响应不合法: {e}")))?;
    Ok(match b.tables {
        RawTables::Names(v) => TablesBody::Names(v),
        RawTables::Detailed(v) => TablesBody::Detailed(
            v.into_iter()
                .map(|t| TableInfo {
                    name: t.name,
                    columns: t.columns.into_iter().map(Into::into).collect(),
                })
                .collect(),
        ),
    })
}

#[derive(serde::Deserialize)]
struct RawSchemaBody {
    // 体里另有 `table` 字段（回显表名）——不取它，忽略未知字段即可
    columns: Vec<RawColumn>,
}

pub(crate) fn parse_schema(bytes: &[u8]) -> Result<Vec<ColumnInfo>> {
    let b: RawSchemaBody = serde_json::from_slice(bytes)
        .map_err(|e| Error::Decode(format!("json: /schema 响应不合法: {e}")))?;
    Ok(b.columns.into_iter().map(Into::into).collect())
}

#[derive(serde::Deserialize)]
struct RawPartitionDetail {
    name: String,
    #[serde(default)]
    generation: u64,
    #[serde(default)]
    rows: u64,
    #[serde(default)]
    segments: u64,
}

#[derive(serde::Deserialize)]
struct RawPartitionsBody {
    #[serde(default)]
    detail: Vec<RawPartitionDetail>,
}

pub(crate) fn parse_partitions(bytes: &[u8]) -> Result<Vec<PartitionInfo>> {
    let b: RawPartitionsBody = serde_json::from_slice(bytes)
        .map_err(|e| Error::Decode(format!("json: /partitions 响应不合法: {e}")))?;
    Ok(b.detail
        .into_iter()
        .map(|p| PartitionInfo {
            name: p.name,
            generation: p.generation,
            rows: p.rows,
            segments: p.segments,
        })
        .collect())
}

#[derive(serde::Deserialize)]
struct RawWriteBuffer {
    table: String,
    #[serde(default)]
    batches: u64,
    #[serde(default)]
    bytes: u64,
}

#[derive(serde::Deserialize)]
struct RawStatsBody {
    #[serde(default)]
    tables: usize,
    #[serde(default, rename = "storeBytes")]
    store_bytes: u64,
    #[serde(default, rename = "writeBuffer")]
    write_buffer: Vec<RawWriteBuffer>,
}

pub(crate) fn parse_stats(bytes: &[u8]) -> Result<Stats> {
    let b: RawStatsBody = serde_json::from_slice(bytes)
        .map_err(|e| Error::Decode(format!("json: /stats 响应不合法: {e}")))?;
    Ok(Stats {
        tables: b.tables,
        store_bytes: b.store_bytes,
        write_buffer: b
            .write_buffer
            .into_iter()
            .map(|w| (w.table, w.batches, w.bytes))
            .collect(),
    })
}

#[derive(serde::Deserialize)]
struct RawWriteAck {
    #[serde(default)]
    rows: u64,
    #[serde(default)]
    mode: Option<String>,
    #[serde(default, rename = "elapsedMs")]
    elapsed_ms: Option<f64>,
}

/// `/write` 的回执（`rows` = 本次提交的行数）。
#[derive(Debug, Clone)]
pub struct WriteAck {
    /// 本次提交的行数。
    pub rows: u64,
    /// 服务端理解的 mode（`append`/`overwrite`）。
    pub mode: Option<String>,
    /// 服务端侧耗时（毫秒）。
    pub elapsed_ms: Option<f64>,
}

pub(crate) fn parse_write_ack(bytes: &[u8]) -> Result<WriteAck> {
    let b: RawWriteAck = serde_json::from_slice(bytes)
        .map_err(|e| Error::Decode(format!("json: /write 响应不合法: {e}")))?;
    Ok(WriteAck {
        rows: b.rows,
        mode: b.mode,
        elapsed_ms: b.elapsed_ms,
    })
}

/// 错误体 `{error, code}`（**契约恒为这个形状**，含框架层抽取失败）。
#[derive(serde::Deserialize)]
pub(crate) struct RawError {
    #[serde(default)]
    error: String,
    #[serde(default)]
    code: String,
}

/// 把错误体解成 [`Error::Api`]；解不动就退回原始文本（**不丢信息**）。
pub(crate) fn api_error(status: u16, body: &[u8]) -> Error {
    match serde_json::from_slice::<RawError>(body) {
        Ok(e) => Error::Api {
            status,
            code: e.code,
            message: e.error,
        },
        Err(_) => Error::Api {
            status,
            code: String::new(),
            message: String::from_utf8_lossy(body).into_owned(),
        },
    }
}

// ---------------------------------------------------------------------------
// 会话（十八批 C1：POST /api/v1/auth/login）
// ---------------------------------------------------------------------------

/// 一次登录会话（`Client::login` 的产物；也经 `Client::session()` 可查）。
#[derive(Debug, Clone)]
pub struct SessionInfo {
    /// 会话 id（服务端日志/审计里对应它；`Client` 自己用它没用——凭证是 token）。
    pub session_id: String,
    /// 登录用户（开放模式 = `anonymous`；超级 token 面不建会话、走不到这里）。
    pub user: String,
    /// 登录时**默认库**的角色快照（`None` = 白名单制用户当前无默认库授权）。
    /// 判定以服务端**每请求现查**为准，这里只是登录那一刻的展示值。
    pub role: Option<String>,
    /// 授权摘要（键 = 库名或 `"*"`，值 = 角色名或 `null`）——供调用方**自查**，
    /// 不用撞 403 试探。
    pub grants: serde_json::Value,
    /// 会话默认库（`USE` 的落点；`QueryOptions::database` 缺省时的锚点）。
    pub default_db: String,
}

#[derive(serde::Deserialize)]
struct RawLogin {
    token: String,
    #[serde(rename = "sessionId")]
    session_id: String,
    #[serde(default)]
    user: String,
    #[serde(default)]
    role: Option<String>,
    #[serde(default)]
    grants: serde_json::Value,
    #[serde(rename = "defaultDb", default)]
    default_db: String,
}

/// 解析 login 响应；token 单独交还（进 `Http` 的 auth 头，**不进**公开结构——
/// 凭证不该被调用方随手 `Debug` 出去）。
pub(crate) fn parse_login(bytes: &[u8]) -> Result<(String, SessionInfo)> {
    let r: RawLogin = serde_json::from_slice(bytes)
        .map_err(|e| Error::Decode(format!("json: /auth/login 响应不合法: {e}")))?;
    Ok((
        r.token,
        SessionInfo {
            session_id: r.session_id,
            user: r.user,
            role: r.role,
            grants: r.grants,
            default_db: r.default_db,
        },
    ))
}

// ---------------------------------------------------------------------------
// 多库（十八批 C3：GET /api/v1/databases）
// ---------------------------------------------------------------------------

/// 一个挂载库。
#[derive(Debug, Clone)]
pub struct DbInfo {
    /// 库名（进 `QueryOptions::database` / `Client::use_database`）。
    pub name: String,
    /// 只读挂载（写 → 503 `READ_ONLY`，客户端**永不重试**，换可写实例）。
    pub read_only: bool,
}

pub(crate) fn parse_databases(bytes: &[u8]) -> Result<Vec<DbInfo>> {
    #[derive(serde::Deserialize)]
    struct RawDb {
        name: String,
        #[serde(rename = "readOnly", default)]
        read_only: bool,
    }
    #[derive(serde::Deserialize)]
    struct RawBody {
        #[serde(default)]
        databases: Vec<RawDb>,
    }
    let b: RawBody = serde_json::from_slice(bytes)
        .map_err(|e| Error::Decode(format!("json: /databases 响应不合法: {e}")))?;
    Ok(b.databases
        .into_iter()
        .map(|d| DbInfo {
            name: d.name,
            read_only: d.read_only,
        })
        .collect())
}
