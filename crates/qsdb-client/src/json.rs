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
    /// 段文件字节总量（压缩后落盘体积；十九批Ⅶ，单表大小 = 各分区求和）。
    pub bytes: u64,
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
    /// `{"tables": [{"name":…,"columns":[…]}]}`（`?withSchema=1`）。
    /// **必须排在 `Names` 前**：untagged 按声明序尝试，空数组 `[]` 两者都
    /// 匹配——Names 在前会把"空实例的 withSchema 响应"误判成"服务端忽略
    /// 了 withSchema"（studio e2e 实测）。两形态的元素类型（对象 vs 字符串）
    /// 无重叠，Detailed 在前对非空场景零影响。
    Detailed(Vec<RawTableInfo>),
    /// `{"tables": ["a","b"]}`
    Names(Vec<String>),
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
    #[serde(default)]
    bytes: u64,
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
            bytes: p.bytes,
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

// ---------------------------------------------------------------------------
// 管理面一等类型（十八批Ⅲ：/config /watch /sessions /queries /grants）
// ---------------------------------------------------------------------------

/// 运行期配置节（`GET /api/v1/config` 的 `runtime`；四个键全部热更）。
#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    /// 默认查询超时（毫秒；0 = 不超时）。
    pub query_timeout_ms: u64,
    /// 单查询执行内存配额（字节；0 = 不限——server 默认 4GB）。
    pub query_memory_limit_bytes: u64,
    /// 结果护栏·行数（0 = 关）。
    pub max_result_rows: u64,
    /// 结果护栏·体量（字节，估算口径；0 = 关）。
    pub max_result_bytes: u64,
    /// 整个响应体（`startup`/`memory`/`server` 节给需要的人原样读）。
    pub raw: serde_json::Value,
}

pub(crate) fn parse_runtime_config(bytes: &[u8]) -> Result<RuntimeConfig> {
    #[derive(serde::Deserialize, Default)]
    struct RawRuntime {
        #[serde(default, rename = "queryTimeoutMs")]
        query_timeout_ms: u64,
        #[serde(default, rename = "queryMemoryLimitBytes")]
        query_memory_limit_bytes: u64,
        #[serde(default, rename = "maxResultRows")]
        max_result_rows: u64,
        #[serde(default, rename = "maxResultBytes")]
        max_result_bytes: u64,
    }
    #[derive(serde::Deserialize)]
    struct RawBody {
        #[serde(default)]
        runtime: RawRuntime,
    }
    let v: serde_json::Value = serde_json::from_slice(bytes)
        .map_err(|e| Error::Decode(format!("json: /config 响应不合法: {e}")))?;
    let rt: RawBody = serde_json::from_value(v.clone())
        .map_err(|e| Error::Decode(format!("json: /config runtime 节不合法: {e}")))?;
    Ok(RuntimeConfig {
        query_timeout_ms: rt.runtime.query_timeout_ms,
        query_memory_limit_bytes: rt.runtime.query_memory_limit_bytes,
        max_result_rows: rt.runtime.max_result_rows,
        max_result_bytes: rt.runtime.max_result_bytes,
        raw: v,
    })
}

/// 热更补丁（`PUT /api/v1/config` 的 body）。`None` = 不动该项。
/// 服务端校验 `applied` 诚实性；**客户端在空补丁上直接报用法错**
/// （发出去注定 `applied:false`，没有意义）。
#[derive(Debug, Clone, Default)]
pub struct ConfigPatch {
    /// 默认查询超时（毫秒）。
    pub query_timeout_ms: Option<u64>,
    /// 单查询执行内存配额（字节）。
    pub query_memory_limit_bytes: Option<u64>,
    /// 结果护栏·行数（0 = 关）。
    pub max_result_rows: Option<u64>,
    /// 结果护栏·体量（字节）。
    pub max_result_bytes: Option<u64>,
}

impl ConfigPatch {
    /// 是否一个键都没给（[`crate::Client::config_put`] 会拒绝空补丁）。
    pub fn is_empty(&self) -> bool {
        self.query_timeout_ms.is_none()
            && self.query_memory_limit_bytes.is_none()
            && self.max_result_rows.is_none()
            && self.max_result_bytes.is_none()
    }

    pub(crate) fn to_body(&self) -> String {
        // camelCase 规范名（与 GET 回显同形——读-改-写整轮可行）
        let mut v = serde_json::Map::new();
        if let Some(n) = self.query_timeout_ms {
            v.insert("queryTimeoutMs".into(), serde_json::json!(n));
        }
        if let Some(n) = self.query_memory_limit_bytes {
            v.insert("queryMemoryLimitBytes".into(), serde_json::json!(n));
        }
        if let Some(n) = self.max_result_rows {
            v.insert("maxResultRows".into(), serde_json::json!(n));
        }
        if let Some(n) = self.max_result_bytes {
            v.insert("maxResultBytes".into(), serde_json::json!(n));
        }
        serde_json::Value::Object(v).to_string()
    }
}

/// `PUT /config` 的回执（回显**生效后**的值，客户端可直接比对）。
#[derive(Debug, Clone)]
pub struct ConfigPutAck {
    /// 生效后的默认查询超时（毫秒）。
    pub query_timeout_ms: u64,
    /// 生效后的单查询内存配额（字节）。
    pub query_memory_limit_bytes: u64,
    /// 生效后的结果护栏·行数。
    pub max_result_rows: u64,
    /// 生效后的结果护栏·体量（字节）。
    pub max_result_bytes: u64,
    /// **确实落了至少一个已知字段**（键名拼错 = false——不许假成功）。
    pub applied: bool,
}

pub(crate) fn parse_config_put_ack(bytes: &[u8]) -> Result<ConfigPutAck> {
    #[derive(serde::Deserialize)]
    struct Raw {
        #[serde(default, rename = "queryTimeoutMs")]
        query_timeout_ms: u64,
        #[serde(default, rename = "queryMemoryLimitBytes")]
        query_memory_limit_bytes: u64,
        #[serde(default, rename = "maxResultRows")]
        max_result_rows: u64,
        #[serde(default, rename = "maxResultBytes")]
        max_result_bytes: u64,
        #[serde(default)]
        applied: bool,
    }
    let r: Raw = serde_json::from_slice(bytes)
        .map_err(|e| Error::Decode(format!("json: PUT /config 回执不合法: {e}")))?;
    Ok(ConfigPutAck {
        query_timeout_ms: r.query_timeout_ms,
        query_memory_limit_bytes: r.query_memory_limit_bytes,
        max_result_rows: r.max_result_rows,
        max_result_bytes: r.max_result_bytes,
        applied: r.applied,
    })
}

/// 一条提交事件（`GET /api/v1/watch`；reader 角色即可订阅）。
#[derive(Debug, Clone)]
pub struct CommitEvent {
    /// 事件所属库（多挂载后同缓冲收所有库的事件）。
    pub database: String,
    /// 事件所属表。
    pub table: String,
    /// 分区值（已解码为目录值；无分区表 = 空）。
    pub partition: Vec<String>,
    /// 该分区提交后的 generation（同步增量判据）。
    pub generation: u64,
    /// `Append` / `Overwrite` / `Clear`；**未知值原样透出**（服务端只增不改，
    /// 老客户端按字符串比较别穷举）。
    pub kind: String,
}

/// 一次 watch 长轮询的页。
#[derive(Debug, Clone)]
pub struct WatchPage {
    /// 本页事件（可能为空 = wait 到期无新事件）。
    pub events: Vec<CommitEvent>,
    /// 下次请求带的游标（**唯一真源**；缓冲滚过/服务端重启 ⇒ 409
    /// `CURSOR_TOO_OLD` ⇒ 必须全量对账一次再续订，原游标永远不会好）。
    pub next: u64,
    /// true = 还有积压，**立刻**用 `next` 再拉。
    pub more: bool,
}

pub(crate) fn parse_watch(bytes: &[u8]) -> Result<WatchPage> {
    #[derive(serde::Deserialize)]
    struct RawEvent {
        #[serde(default)]
        database: String,
        #[serde(default)]
        table: String,
        #[serde(default)]
        partition: Vec<String>,
        #[serde(default)]
        generation: u64,
        #[serde(default)]
        kind: String,
    }
    #[derive(serde::Deserialize)]
    struct Raw {
        #[serde(default)]
        events: Vec<RawEvent>,
        #[serde(default)]
        next: u64,
        #[serde(default)]
        more: bool,
    }
    let r: Raw = serde_json::from_slice(bytes)
        .map_err(|e| Error::Decode(format!("json: /watch 响应不合法: {e}")))?;
    Ok(WatchPage {
        next: r.next,
        more: r.more,
        events: r
            .events
            .into_iter()
            .map(|e| CommitEvent {
                database: e.database,
                table: e.table,
                partition: e.partition,
                generation: e.generation,
                kind: e.kind,
            })
            .collect(),
    })
}

/// 一个登录会话（`GET /api/v1/sessions`，admin 专属）。
/// 与登录返回的 [`SessionInfo`] 分工：这是**管理视角的全量清单**（含活跃度
/// 与在跑查询），那个是**本连接的身份**。
#[derive(Debug, Clone)]
pub struct AdminSession {
    /// 会话 id（`session_kick` 的键）。
    pub id: String,
    /// 登录用户（`anonymous` = 开放模式）。
    pub user: String,
    /// 角色快照（每请求现查的展示值；白名单制用户可为 `None`）。
    pub role: Option<String>,
    /// `ip:port; User-Agent`（登录时定格）。
    pub client: String,
    /// 创建时刻（epoch 毫秒）。
    pub created_at_ms: u64,
    /// 最近活跃（epoch 毫秒）。
    pub last_active_ms: u64,
    /// 空闲毫秒数。
    pub idle_ms: u64,
    /// 会话默认库（`USE` 的落点）。
    pub default_db: String,
    /// 在跑查询的 id 列表。
    pub running_queries: Vec<String>,
    /// 累计发起查询数。
    pub queries_total: u64,
    /// 累计写入行数。
    pub rows_written: u64,
}

pub(crate) fn parse_sessions(bytes: &[u8]) -> Result<Vec<AdminSession>> {
    #[derive(serde::Deserialize)]
    struct Raw {
        #[serde(default)]
        sessions: Vec<RawSession>,
    }
    #[derive(serde::Deserialize)]
    struct RawSession {
        id: String,
        #[serde(default)]
        user: String,
        #[serde(default)]
        role: Option<String>,
        #[serde(default)]
        client: String,
        #[serde(default, rename = "createdAtMs")]
        created_at_ms: u64,
        #[serde(default, rename = "lastActiveMs")]
        last_active_ms: u64,
        #[serde(default, rename = "idleMs")]
        idle_ms: u64,
        #[serde(default, rename = "defaultDb")]
        default_db: String,
        #[serde(default, rename = "runningQueries")]
        running_queries: Vec<String>,
        #[serde(default, rename = "queriesTotal")]
        queries_total: u64,
        #[serde(default, rename = "rowsWritten")]
        rows_written: u64,
    }
    let r: Raw = serde_json::from_slice(bytes)
        .map_err(|e| Error::Decode(format!("json: /sessions 响应不合法: {e}")))?;
    Ok(r.sessions
        .into_iter()
        .map(|s| AdminSession {
            id: s.id,
            user: s.user,
            role: s.role,
            client: s.client,
            created_at_ms: s.created_at_ms,
            last_active_ms: s.last_active_ms,
            idle_ms: s.idle_ms,
            default_db: s.default_db,
            running_queries: s.running_queries,
            queries_total: s.queries_total,
            rows_written: s.rows_written,
        })
        .collect())
}

/// 一条运行中的查询（`GET /api/v1/queries`）。
#[derive(Debug, Clone)]
pub struct QueryInfo {
    /// 服务端 query id（cancel 的键；客户端透传的 id 冲突回退后以此为准）。
    pub id: String,
    /// SQL 文本（注册时的原句）。
    pub sql: String,
    /// 已运行毫秒数。
    pub elapsed_ms: u64,
    /// 执行内存记账（字节；只含物化算子）。
    pub memory_bytes: u64,
    /// 是否已被取消（取消是协作式，标志置位到真正停下有毫秒级延迟）。
    pub cancelled: bool,
    /// 归属会话（Basic/超级 token/开放模式 = `None`）。
    pub session_id: Option<String>,
}

pub(crate) fn parse_queries(bytes: &[u8]) -> Result<Vec<QueryInfo>> {
    #[derive(serde::Deserialize)]
    struct Raw {
        #[serde(default)]
        queries: Vec<RawQuery>,
    }
    #[derive(serde::Deserialize)]
    struct RawQuery {
        id: String,
        #[serde(default)]
        sql: String,
        #[serde(default, rename = "elapsedMs")]
        elapsed_ms: u64,
        #[serde(default, rename = "memoryBytes")]
        memory_bytes: u64,
        #[serde(default)]
        cancelled: bool,
        #[serde(default, rename = "sessionId")]
        session_id: Option<String>,
    }
    let r: Raw = serde_json::from_slice(bytes)
        .map_err(|e| Error::Decode(format!("json: /queries 响应不合法: {e}")))?;
    Ok(r.queries
        .into_iter()
        .map(|q| QueryInfo {
            id: q.id,
            sql: q.sql,
            elapsed_ms: q.elapsed_ms,
            memory_bytes: q.memory_bytes,
            cancelled: q.cancelled,
            session_id: q.session_id,
        })
        .collect())
}

// ---------------------------------------------------------------------------
// 文件管理面（二十批：/files/list、/files/detail、/files/trash）
// ---------------------------------------------------------------------------

/// 可见挂载（`files/list` 的 `mounts`——文件浏览器的起点清单）。
#[derive(Debug, Clone)]
pub struct FileMount {
    /// 挂载名（路径寻址的前缀）。
    pub name: String,
    /// 是否默认库。
    pub is_default: bool,
}

/// 一列的 min/max/NULL 统计（manifest 内嵌，**不读 parquet** 就有）。
#[derive(Debug, Clone)]
pub struct FileColumnStat {
    /// 列名。
    pub name: String,
    /// 最小值（`Literal` 的 Display 形态；全 NULL 列为 `None`）。
    pub min: Option<String>,
    /// 最大值（同上）。
    pub max: Option<String>,
    /// NULL 计数。
    pub null_count: u64,
}

/// 段摘要（来自同目录 manifest）。`referenced` 三态：
/// `Some(true)` = 被存活 manifest 引用（**不可删**）；`Some(false)` = 孤儿
/// （GC 遗留/崩溃残留，可进回收站）；`None` = 未知（manifest 缺失/损坏——
/// 未知 ≠ 未引用，别当 false 用）。
#[derive(Debug, Clone)]
pub struct FileSegment {
    /// 三态引用标记，见类型文档。
    pub referenced: Option<bool>,
    /// 行数。
    pub rows: Option<u64>,
    /// 落盘字节。
    pub bytes: Option<u64>,
    /// 段创建时间（毫秒 epoch）。
    pub created_at_ms: Option<i64>,
    /// 所在分区的提交计数器。
    pub generation: Option<u64>,
    /// 列统计（仅 `files_detail` 填；list 不带）。
    pub columns: Vec<FileColumnStat>,
}

/// 目录清单里的一条（目录排前、名字序——服务端已排）。
#[derive(Debug, Clone)]
pub struct FileEntry {
    /// 条目名（不含路径）。
    pub name: String,
    /// 目录还是文件。
    pub is_dir: bool,
    /// 仅文件有。
    pub size: Option<u64>,
    /// 最后修改（毫秒 epoch）。
    pub modified_ms: Option<u64>,
    /// 仅段文件（`seg_*.parquet`）有。
    pub segment: Option<FileSegment>,
}

/// 目录浏览一页（`GET /api/v1/files/list`）。
#[derive(Debug, Clone)]
pub struct FileListing {
    /// 归一化后的目录（空串 = 存储根顶层）。
    pub dir: String,
    /// 调用方可见的挂载（非 admin 只含有 grant 的）——文件浏览器的起点。
    pub mounts: Vec<FileMount>,
    /// 目录条目。
    pub entries: Vec<FileEntry>,
}

/// 目录浅层汇总（`files_detail` 对目录的 `summary`）。
#[derive(Debug, Clone, Default)]
pub struct DirSummary {
    /// 子目录数。
    pub dirs: u64,
    /// 文件数。
    pub files: u64,
    /// 文件字节合计（不含子目录内部）。
    pub bytes: u64,
}

/// manifest 文件本身的详情（`files_detail` 的 `manifest` 字段）。
#[derive(Debug, Clone)]
pub struct ManifestDetail {
    /// 分区提交计数器。
    pub generation: u64,
    /// 分区总行数。
    pub rows: u64,
    /// 段数。
    pub segments: u64,
    /// manifest 磁盘格式版本。
    pub version: u32,
    /// 分区键值（Display 形态）。
    pub partition_values: Vec<(String, String)>,
}

/// 单文件/目录详情（`GET /api/v1/files/detail`）。
#[derive(Debug, Clone)]
pub struct FileDetail {
    /// 寻址路径（调用方传入的 `<挂载>/相对路径` 原样回显）。
    pub path: String,
    /// 最后一节名字。
    pub name: String,
    /// 目录还是文件。
    pub is_dir: bool,
    /// `segment` | `manifest` | `index` | `tabledef` | `other`；目录恒 `dir`。
    pub kind: String,
    /// 字节。
    pub size: u64,
    /// 最后修改（毫秒 epoch）。
    pub modified_ms: u64,
    /// 归属库（挂载名；grants 判定键）。
    pub db: String,
    /// 归属表（路径形状推断，尽力而为）。
    pub table: Option<String>,
    /// 归属分区（`dt=2026-01-01` 形态；无分区表为 `None`）。
    pub partition: Option<String>,
    /// 目录时的浅层汇总。
    pub summary: Option<DirSummary>,
    /// 段详情（kind = `segment`；孤儿时 `referenced=false`、统计为空）。
    pub segment: Option<FileSegment>,
    /// manifest 文件详情（kind = `manifest`）。
    pub manifest: Option<ManifestDetail>,
    /// table.json 原样 JSON（kind = `tabledef` 且解析成功）。
    pub table_def: Option<serde_json::Value>,
    /// 单文件预览 SQL（本地库才有——拿去走查询面，权限由查询面管）。
    pub read_sql: Option<String>,
}

/// 进回收站回执（`POST /api/v1/files/trash`）。
#[derive(Debug, Clone)]
pub struct TrashAck {
    /// 恒 `true`（失败走 [`Error::Api`]）。
    pub trashed: bool,
    /// 寻址路径回显。
    pub path: String,
}

#[derive(serde::Deserialize)]
struct RawFileMount {
    name: String,
    #[serde(default, rename = "isDefault")]
    is_default: bool,
}

#[derive(serde::Deserialize)]
struct RawFileColumnStat {
    name: String,
    #[serde(default)]
    min: Option<String>,
    #[serde(default)]
    max: Option<String>,
    #[serde(default, rename = "nullCount")]
    null_count: u64,
}

#[derive(serde::Deserialize)]
struct RawFileSegment {
    #[serde(default)]
    referenced: Option<bool>,
    #[serde(default)]
    rows: Option<u64>,
    #[serde(default)]
    bytes: Option<u64>,
    #[serde(default, rename = "createdAtMs")]
    created_at_ms: Option<i64>,
    #[serde(default)]
    generation: Option<u64>,
    #[serde(default)]
    columns: Vec<RawFileColumnStat>,
}

impl RawFileSegment {
    fn into_segment(self) -> FileSegment {
        FileSegment {
            referenced: self.referenced,
            rows: self.rows,
            bytes: self.bytes,
            created_at_ms: self.created_at_ms,
            generation: self.generation,
            columns: self
                .columns
                .into_iter()
                .map(|c| FileColumnStat {
                    name: c.name,
                    min: c.min,
                    max: c.max,
                    null_count: c.null_count,
                })
                .collect(),
        }
    }
}

#[derive(serde::Deserialize)]
struct RawFileEntry {
    name: String,
    #[serde(rename = "type")]
    type_name: String,
    #[serde(default)]
    size: Option<u64>,
    #[serde(default, rename = "modifiedMs")]
    modified_ms: Option<u64>,
    #[serde(default)]
    segment: Option<RawFileSegment>,
}

#[derive(serde::Deserialize)]
struct RawFileListing {
    #[serde(default)]
    dir: String,
    #[serde(default)]
    mounts: Vec<RawFileMount>,
    #[serde(default)]
    entries: Vec<RawFileEntry>,
}

pub(crate) fn parse_file_list(bytes: &[u8]) -> Result<FileListing> {
    let r: RawFileListing = serde_json::from_slice(bytes)
        .map_err(|e| Error::Decode(format!("json: /files/list 响应不合法: {e}")))?;
    Ok(FileListing {
        dir: r.dir,
        mounts: r
            .mounts
            .into_iter()
            .map(|m| FileMount {
                name: m.name,
                is_default: m.is_default,
            })
            .collect(),
        entries: r
            .entries
            .into_iter()
            .map(|e| FileEntry {
                name: e.name,
                is_dir: e.type_name == "dir",
                size: e.size,
                modified_ms: e.modified_ms,
                segment: e.segment.map(RawFileSegment::into_segment),
            })
            .collect(),
    })
}

#[derive(serde::Deserialize)]
struct RawFileDetail {
    #[serde(default)]
    path: String,
    #[serde(default)]
    name: String,
    #[serde(rename = "type")]
    type_name: String,
    #[serde(default)]
    kind: String,
    #[serde(default)]
    size: u64,
    #[serde(default, rename = "modifiedMs")]
    modified_ms: u64,
    #[serde(default)]
    db: String,
    #[serde(default)]
    table: Option<String>,
    #[serde(default)]
    partition: Option<String>,
    #[serde(default)]
    summary: Option<RawDirSummary>,
    #[serde(default)]
    segment: Option<RawFileSegment>,
    #[serde(default)]
    manifest: Option<RawManifestDetail>,
    #[serde(default, rename = "tableDef")]
    table_def: Option<serde_json::Value>,
    #[serde(default, rename = "readSql")]
    read_sql: Option<String>,
}

#[derive(serde::Deserialize)]
struct RawDirSummary {
    #[serde(default)]
    dirs: u64,
    #[serde(default)]
    files: u64,
    #[serde(default)]
    bytes: u64,
}

#[derive(serde::Deserialize)]
struct RawManifestDetail {
    #[serde(default)]
    generation: u64,
    #[serde(default)]
    rows: u64,
    #[serde(default)]
    segments: u64,
    #[serde(default)]
    version: u32,
    #[serde(default, rename = "partitionValues")]
    partition_values: std::collections::BTreeMap<String, serde_json::Value>,
}

pub(crate) fn parse_file_detail(bytes: &[u8]) -> Result<FileDetail> {
    let r: RawFileDetail = serde_json::from_slice(bytes)
        .map_err(|e| Error::Decode(format!("json: /files/detail 响应不合法: {e}")))?;
    Ok(FileDetail {
        path: r.path,
        name: r.name,
        is_dir: r.type_name == "dir",
        kind: r.kind,
        size: r.size,
        modified_ms: r.modified_ms,
        db: r.db,
        table: r.table,
        partition: r.partition,
        summary: r.summary.map(|s| DirSummary {
            dirs: s.dirs,
            files: s.files,
            bytes: s.bytes,
        }),
        segment: r.segment.map(RawFileSegment::into_segment),
        manifest: r.manifest.map(|m| ManifestDetail {
            generation: m.generation,
            rows: m.rows,
            segments: m.segments,
            version: m.version,
            partition_values: m
                .partition_values
                .into_iter()
                .map(|(k, v)| {
                    let s = match v {
                        serde_json::Value::String(s) => s,
                        other => other.to_string(),
                    };
                    (k, s)
                })
                .collect(),
        }),
        table_def: r.table_def,
        read_sql: r.read_sql,
    })
}

pub(crate) fn parse_trash(bytes: &[u8]) -> Result<TrashAck> {
    #[derive(serde::Deserialize)]
    struct Raw {
        #[serde(default)]
        trashed: bool,
        #[serde(default)]
        path: String,
    }
    let r: Raw = serde_json::from_slice(bytes)
        .map_err(|e| Error::Decode(format!("json: /files/trash 响应不合法: {e}")))?;
    Ok(TrashAck {
        trashed: r.trashed,
        path: r.path,
    })
}
