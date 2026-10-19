//! 错误码目录的**单一真源**（契约文档：`docs/error-codes.md`）。
//!
//! 设计要点：
//! - 每个码固定绑定 `code` / HTTP 状态 / 可重试性 / 类别 / SQLState 建议，
//!   响应一律经 [`ErrorCode::response`] 构造；
//! - **没有"状态派生码"兜底**：此前 `err_response(status, msg)` + `status_code_of()`
//!   能让响应发出 `BAD_REQUEST` 这种**目录外的码**（`/write` 的 IPC 解码失败就在发）。
//!   删掉之后，"每个错误都必须是目录里的某个码"由**编译器**保证，不靠自觉；
//! - 目录端点 `GET /api/v1/error-codes` 直接由本枚举产出，不另写一份表；
//! - 文末单测解析 `docs/error-codes.md` 的标记区**逐字对账**（改码不改文档 → 测试红）。
//!
//! 稳定性政策见文档 §六：**只增不改**——已发布的码语义与状态码永久冻结，
//! 文案（`error` 字段）不做兼容承诺。

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use qsdb::{NotFoundKind, QsdbError};

/// 目录里的全部错误码（28 个）。
///
/// 顺序 = 文档 §三 表格顺序 = 目录端点输出顺序，便于对账。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    // ---- query ----
    QueryTimeout,
    QueryCancelled,
    QueryMemoryLimit,
    /// 结果护栏超限（十七批 R1）：单次查询的结果行数/体量超过部署护栏。
    /// 检查点在**执行后、序列化前**——恒定拒绝、绝不返回部分数据
    /// （throw 不 break；静默部分数据是本仓红线）。与 QUERY_MEMORY_LIMIT
    /// 同构："改查询/换流式格式才有用，重试无用"。
    ResultTooLarge,
    QueryPanic,
    TableNotFound,
    ColumnNotFound,
    ParseError,
    TypeError,
    Unsupported,
    // ---- resource ----
    AlreadyExists,
    WriteConflict,
    StaleWriter,
    /// 订阅游标太旧（缓冲区挤掉 / 服务端重启）—— **必须重新对账**，不能续着等
    CursorTooOld,
    /// 段文件被存活 manifest 引用（二十批文件管理面）：拒绝移入回收站——
    /// 删被引用段 = 表读取永久损坏。与 WRITE_CONFLICT 同为 409 但语义相反：
    /// 那个"重读再试"有用，这个**必须换动作**（DELETE WHERE / 清空分区）
    SegmentReferenced,
    NotFound,
    // ---- request ----
    InvalidArgument,
    InvalidIpc,
    // ---- auth ----
    Unauthorized,
    /// 会话已失效（被踢出 / 空闲回收 / 服务端重启）——与"从未认证"分开：
    /// 客户端不该再拿这把 token 重试，直接重新登录（十六批 M-A）
    SessionRevoked,
    Forbidden,
    // ---- capacity ----
    TooManyQueries,
    /// 会话数超上限（十九批Ⅶ --max-sessions；登录时拒，等会话释放或被踢）
    TooManySessions,
    MemoryPressure,
    ReadOnly,
    // ---- storage ----
    MetadataError,
    IoError,
    // ---- internal ----
    Internal,
}

impl ErrorCode {
    /// 全量清单（目录端点与漂移测试的唯一来源）。
    pub const ALL: [ErrorCode; 28] = [
        Self::QueryTimeout,
        Self::QueryCancelled,
        Self::QueryMemoryLimit,
        Self::ResultTooLarge,
        Self::QueryPanic,
        Self::TableNotFound,
        Self::ColumnNotFound,
        Self::ParseError,
        Self::TypeError,
        Self::Unsupported,
        Self::AlreadyExists,
        Self::WriteConflict,
        Self::StaleWriter,
        Self::CursorTooOld,
        Self::SegmentReferenced,
        Self::InvalidArgument,
        Self::InvalidIpc,
        Self::NotFound,
        Self::Unauthorized,
        Self::SessionRevoked,
        Self::Forbidden,
        Self::TooManyQueries,
        Self::TooManySessions,
        Self::MemoryPressure,
        Self::ReadOnly,
        Self::MetadataError,
        Self::IoError,
        Self::Internal,
    ];

    /// 机器码（`SCREAMING_SNAKE_CASE`，契约级稳定）。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::QueryTimeout => "QUERY_TIMEOUT",
            Self::QueryCancelled => "QUERY_CANCELLED",
            Self::QueryMemoryLimit => "QUERY_MEMORY_LIMIT",
            Self::ResultTooLarge => "RESULT_TOO_LARGE",
            Self::QueryPanic => "QUERY_PANIC",
            Self::TableNotFound => "TABLE_NOT_FOUND",
            Self::ColumnNotFound => "COLUMN_NOT_FOUND",
            Self::ParseError => "PARSE_ERROR",
            Self::TypeError => "TYPE_ERROR",
            Self::Unsupported => "UNSUPPORTED",
            Self::AlreadyExists => "ALREADY_EXISTS",
            Self::WriteConflict => "WRITE_CONFLICT",
            Self::StaleWriter => "STALE_WRITER",
            Self::CursorTooOld => "CURSOR_TOO_OLD",
            Self::SegmentReferenced => "SEGMENT_REFERENCED",
            Self::InvalidArgument => "INVALID_ARGUMENT",
            Self::InvalidIpc => "INVALID_IPC",
            Self::NotFound => "NOT_FOUND",
            Self::Unauthorized => "UNAUTHORIZED",
            Self::SessionRevoked => "SESSION_REVOKED",
            Self::Forbidden => "FORBIDDEN",
            Self::TooManyQueries => "TOO_MANY_QUERIES",
            Self::TooManySessions => "TOO_MANY_SESSIONS",
            Self::MemoryPressure => "MEMORY_PRESSURE",
            Self::ReadOnly => "READ_ONLY",
            Self::MetadataError => "METADATA_ERROR",
            Self::IoError => "IO_ERROR",
            Self::Internal => "INTERNAL",
        }
    }

    /// HTTP 状态码（**同一码恒定同一状态码**——这条不变式由本函数保证）。
    pub fn status(self) -> StatusCode {
        match self {
            Self::QueryTimeout => StatusCode::REQUEST_TIMEOUT,
            Self::QueryCancelled
            | Self::QueryMemoryLimit
            | Self::ResultTooLarge
            | Self::TableNotFound
            | Self::ColumnNotFound
            | Self::ParseError
            | Self::TypeError
            | Self::InvalidArgument
            | Self::InvalidIpc => StatusCode::BAD_REQUEST,
            Self::QueryPanic | Self::MetadataError | Self::IoError | Self::Internal => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
            Self::Unsupported => StatusCode::NOT_IMPLEMENTED,
            Self::AlreadyExists
            | Self::WriteConflict
            | Self::StaleWriter
            | Self::CursorTooOld
            | Self::SegmentReferenced => StatusCode::CONFLICT,
            Self::NotFound => StatusCode::NOT_FOUND,
            Self::Unauthorized | Self::SessionRevoked => StatusCode::UNAUTHORIZED,
            Self::Forbidden => StatusCode::FORBIDDEN,
            Self::TooManyQueries
            | Self::TooManySessions
            | Self::MemoryPressure
            | Self::ReadOnly => StatusCode::SERVICE_UNAVAILABLE,
        }
    }

    /// 瞬时故障 → 退避重试有意义；`false` = 重试无用（需改请求/改配置/换实例/上报）。
    ///
    /// **同为 503 的两个码在这里分道扬镳**：并发满是"等一会儿再来"，
    /// 只读库是"这台永远不行"。
    pub fn retryable(self) -> bool {
        matches!(
            self,
            Self::TooManyQueries | Self::TooManySessions | Self::MemoryPressure | Self::IoError
        )
    }

    /// 归组（给人看的分类，不参与映射）。
    pub fn category(self) -> &'static str {
        match self {
            Self::QueryTimeout
            | Self::QueryCancelled
            | Self::QueryMemoryLimit
            | Self::ResultTooLarge
            | Self::QueryPanic
            | Self::TableNotFound
            | Self::ColumnNotFound
            | Self::ParseError
            | Self::TypeError
            | Self::Unsupported => "query",
            Self::AlreadyExists
            | Self::WriteConflict
            | Self::StaleWriter
            | Self::CursorTooOld
            | Self::SegmentReferenced
            | Self::NotFound => "resource",
            Self::InvalidArgument | Self::InvalidIpc => "request",
            Self::Unauthorized | Self::SessionRevoked | Self::Forbidden => "auth",
            Self::TooManyQueries
            | Self::TooManySessions
            | Self::MemoryPressure
            | Self::ReadOnly => "capacity",
            Self::MetadataError | Self::IoError => "storage",
            Self::Internal => "internal",
        }
    }

    /// SQLState **建议**映射（JDBC 用；最终归驱动所有）。
    pub fn sqlstate(self) -> &'static str {
        match self {
            // 超时与取消**刻意分开**：57014 是 Postgres 把"查询被终止"合在一起的码，
            // 而"超时"（容量/性能）与"取消"（用户/编排主动）在监控告警里要分开算。
            // HY008 = JDBC 标准 "operation canceled"——qsdb-jdbc 已采用，内核建议值对齐。
            Self::QueryTimeout => "57014",
            Self::QueryCancelled => "HY008",
            Self::QueryMemoryLimit => "53200",
            // 54000 = program_limit_exceeded：结果超部署护栏与"程序性上限"同族
            // （PG 把磁盘满/超出上限都归此类），与 QUERY_MEMORY_LIMIT 的 53200
            // （out_of_memory 类）刻意区分——护栏管的是**传输体量**不是内存
            Self::ResultTooLarge => "54000",
            Self::QueryPanic | Self::MetadataError | Self::Internal => "XX000",
            Self::TableNotFound => "42P01",
            Self::ColumnNotFound => "42703",
            Self::ParseError => "42601",
            Self::TypeError => "42804",
            Self::Unsupported => "0A000",
            Self::AlreadyExists => "42P07",
            // 40001 = serialization_failure：并发更新冲突的**标准码**（客户端据此
            // "重读再决定"，不是"改名"）
            Self::WriteConflict => "40001",
            Self::StaleWriter => "40001",
            // 游标过期**不是**任何 SQL 标准条件 —— HY000 是 JDBC 的"一般错误"，
            // 比硬套 40001（并发冲突）诚实
            Self::CursorTooOld => "HY000",
            // 55006 = object_in_use：段正被存活 manifest 引用——"对象在用中"
            // 的标准类；与 WRITE_CONFLICT 的 40001（并发冲突，重读再试）刻意
            // 区分——这里重试无用，必须换动作（DELETE WHERE / 清空分区）
            Self::SegmentReferenced => "55006",
            Self::InvalidArgument => "42000",
            Self::InvalidIpc => "08P01",
            Self::NotFound => "42704",
            Self::Unauthorized | Self::SessionRevoked => "28000",
            Self::Forbidden => "42501",
            Self::TooManyQueries => "53300",
            // 08004 = SQL 标准 connection_rejected: too_many_connections——
            // 会话是**登录时**拒的，用连接类的标准码比容量类更准
            Self::TooManySessions => "08004",
            Self::MemoryPressure => "53000",
            Self::ReadOnly => "25006",
            Self::IoError => "58030",
        }
    }

    /// 由内核错误 + 语义面判定（**唯一的映射入口**）。
    ///
    /// `query_ctx`：查询/写入端点为 `true`（SQL 语义面），资源端点（`/tables`、
    /// `/schema`、`/partitions`、`/stats`）为 `false`（HTTP 资源语义面）。
    /// 这就是 J5 的分工：同一个"表不存在"，查询端点回 400（免得网关把 SQL 错
    /// 判成路由错），资源端点回 404。
    pub fn from_qsdb(e: &QsdbError, query_ctx: bool) -> Self {
        match e {
            QsdbError::QueryTimeout(_) => Self::QueryTimeout,
            QsdbError::QueryCancelled(_) => Self::QueryCancelled,
            QsdbError::QueryMemoryLimit(_) => Self::QueryMemoryLimit,
            // 独立于 INTERNAL：进程无恙、只这一条查询挂了——驱动该提示
            // "该查询异常终止"而不是"服务不可用"
            QsdbError::QueryPanic(_) => Self::QueryPanic,
            QsdbError::Parse(_) => Self::ParseError,
            QsdbError::Type(_) => Self::TypeError,
            QsdbError::InvalidArgument(_) => Self::InvalidArgument,
            QsdbError::AlreadyExists(_) => Self::AlreadyExists,
            QsdbError::WriteConflict { .. } => Self::WriteConflict,
            QsdbError::StaleWriter(_) => Self::StaleWriter,
            QsdbError::Unsupported(_) => Self::Unsupported,
            QsdbError::Meta(_) => Self::MetadataError,
            // 只读库拒绝变更：对客户端是"这台不行"，不是"请求写错了"
            QsdbError::ReadOnly(_) => Self::ReadOnly,
            // 角色不足：身份不够格（403 FORBIDDEN）——不是参数错（400）。
            // 与 /write 面的 can_write 闸对齐（同一动作两个面必须同码）
            QsdbError::Forbidden(_) => Self::Forbidden,
            // 全库预算此刻耗尽（多为**他人占用**）→ 503 + 可退避重试
            QsdbError::MemoryPressure(_) => Self::MemoryPressure,
            QsdbError::NotFound { .. } => match (query_ctx, e.not_found_kind()) {
                // 查询端点：表/列各有专属码（驱动可映射 42P01/42703）；
                // 其余对象（文件/视图/函数……）在 SQL 面一律归"参数指向了不存在的东西"
                (true, Some(NotFoundKind::Table)) => Self::TableNotFound,
                (true, Some(NotFoundKind::Column)) => Self::ColumnNotFound,
                (true, _) => Self::InvalidArgument,
                (false, _) => Self::NotFound,
            },
            // I/O 类瞬时故障独立成码（此前落 INTERNAL → 驱动永不重试）
            _ if e.is_transient_io() => Self::IoError,
            // 兜底：Arrow 编解码/DLL 之类未归类的内部问题
            _ => Self::Internal,
        }
    }

    /// 构造契约错误响应 `{"error": <文案>, "code": <码>}`。
    pub fn response(self, msg: impl Into<String>) -> Response {
        (
            self.status(),
            axum::Json(serde_json::json!({ "error": msg.into(), "code": self.as_str() })),
        )
            .into_response()
    }

    /// 目录条目（`GET /api/v1/error-codes` 与漂移测试共用）。
    pub fn entry(self) -> serde_json::Value {
        serde_json::json!({
            "code": self.as_str(),
            "http": self.status().as_u16(),
            "retry": self.retryable(),
            "category": self.category(),
            "sqlState": self.sqlstate(),
        })
    }

    /// Flight（gRPC）状态码映射（契约 `docs/error-codes.md` §七）。
    ///
    /// Flight 通道没有 `code` 字段，只能靠 gRPC status 承载；映射不把
    /// "可重试性"丢掉——`TOO_MANY_QUERIES` → `RESOURCE_EXHAUSTED`、
    /// `IO_ERROR` → `UNAVAILABLE` 都是 gRPC 客户端默认可重试的状态。
    #[cfg(feature = "flight")]
    pub fn grpc(self) -> tonic::Code {
        use tonic::Code;
        match self {
            Self::QueryTimeout => Code::DeadlineExceeded,
            Self::QueryCancelled => Code::Cancelled,
            Self::TooManyQueries | Self::TooManySessions | Self::MemoryPressure => {
                Code::ResourceExhausted
            }
            Self::ReadOnly | Self::SegmentReferenced => Code::FailedPrecondition,
            Self::Unauthorized | Self::SessionRevoked => Code::Unauthenticated,
            Self::Forbidden => Code::PermissionDenied,
            Self::Unsupported => Code::Unimplemented,
            Self::NotFound => Code::NotFound,
            Self::AlreadyExists => Code::AlreadyExists,
            // gRPC 无专用并发冲突码；ABORTED 是标准类（"请重读再试"），
            // 不用 AlreadyExists（那是"改名"语义，会误导调用方）
            // 与 WRITE_CONFLICT 同族（"分区已前进，你的写被拒"），gRPC 侧一并用
            // ABORTED（标准类 = 读最新状态后重新决定；盲目重试无用）
            // 同上：游标过期也是"你的状态陈旧了，重新对账再继续"
            Self::WriteConflict | Self::StaleWriter | Self::CursorTooOld => Code::Aborted,
            Self::IoError => Code::Unavailable,
            Self::QueryMemoryLimit
            | Self::ResultTooLarge
            | Self::QueryPanic
            | Self::TableNotFound
            | Self::ColumnNotFound
            | Self::ParseError
            | Self::TypeError
            | Self::InvalidArgument
            | Self::InvalidIpc => Code::InvalidArgument,
            Self::MetadataError | Self::Internal => Code::Internal,
        }
    }
}

/// 内核错误 → Flight status（语义面同 HTTP 查询端点）。
#[cfg(feature = "flight")]
pub fn grpc_status(e: &QsdbError) -> tonic::Status {
    grpc_status_msg(ErrorCode::from_qsdb(e, true), e)
}

/// 码 + 文案 → Flight status。**文案里附带码名**：gRPC 没有 `code` 字段，
/// 不带码名的话调用方只能靠 status 猜（并发闸的 503 与只读库的 503 就是靠码分的）。
#[cfg(feature = "flight")]
pub fn grpc_status_msg(code: ErrorCode, msg: impl std::fmt::Display) -> tonic::Status {
    tonic::Status::new(code.grpc(), format!("{msg} [{}]", code.as_str()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 文档 §三 的标记区定义（与 `docs/error-codes.md` 必须逐字一致）。
    const DOC: &str = include_str!("../../../docs/error-codes.md");
    const BEGIN: &str = "<!-- BEGIN CODE TABLE -->";
    const END: &str = "<!-- END CODE TABLE -->";

    /// 解析标记区里的表格行：`(code, http, retry, category, sqlstate)`。
    fn doc_rows() -> Vec<(String, u16, bool, String, String)> {
        let body = DOC
            .split_once(BEGIN)
            .expect("docs/error-codes.md 缺 BEGIN 标记行")
            .1
            .split_once(END)
            .expect("docs/error-codes.md 缺 END 标记行")
            .0;
        let mut out = Vec::new();
        for line in body.lines() {
            let line = line.trim();
            if !line.starts_with("| `") {
                continue; // 表头 / 分隔行
            }
            let cells: Vec<&str> = line
                .trim_matches('|')
                .split('|')
                .map(|c| c.trim())
                .collect();
            assert!(cells.len() >= 7, "码表列数不足: {line}");
            let code = cells[0].trim_matches('`').to_string();
            let http: u16 = cells[1].parse().expect("HTTP 列必须是数字");
            let retry = match cells[2] {
                "yes" => true,
                "no" => false,
                other => panic!("Retry 列只认 yes/no，得到 {other:?}"),
            };
            out.push((
                code,
                http,
                retry,
                cells[3].to_string(),
                cells[6].trim_matches('`').to_string(),
            ));
        }
        out
    }

    /// **防漂移**：码集合、HTTP、Retry、类别、SQLState 与文档逐字一致。
    ///
    /// 这是本方案的核心机制——只加码不改文档（或反过来）都会让 `cargo test` 变红。
    #[test]
    fn catalog_matches_authoritative_doc() {
        let rows = doc_rows();
        assert_eq!(
            rows.len(),
            ErrorCode::ALL.len(),
            "文档码表条目数 {} ≠ 代码侧枚举数 {}",
            rows.len(),
            ErrorCode::ALL.len()
        );
        for (i, code) in ErrorCode::ALL.iter().enumerate() {
            let (doc_code, http, retry, category, sqlstate) = &rows[i];
            assert_eq!(doc_code, code.as_str(), "第 {} 行码名不一致", i + 1);
            assert_eq!(
                *http,
                code.status().as_u16(),
                "{} 的 HTTP 状态码与文档不一致",
                code.as_str()
            );
            assert_eq!(
                *retry,
                code.retryable(),
                "{} 的可重试性与文档不一致",
                code.as_str()
            );
            assert_eq!(
                category,
                code.category(),
                "{} 的类别与文档不一致",
                code.as_str()
            );
            assert_eq!(
                sqlstate,
                code.sqlstate(),
                "{} 的 SQLState 与文档不一致",
                code.as_str()
            );
        }
    }

    /// `code ↔ 状态码` 是一对一：同一码不可能有两种状态码（由本枚举天然保证），
    /// 且每个状态码的语义不冲突（503 有两种，靠码区分——正是两种 503 的由来）。
    #[test]
    fn codes_are_unique_and_shaped() {
        let mut seen = std::collections::HashSet::new();
        for c in ErrorCode::ALL {
            assert!(seen.insert(c.as_str()), "重复码 {}", c.as_str());
            assert!(
                c.as_str()
                    .bytes()
                    .all(|b| b.is_ascii_uppercase() || b == b'_'),
                "{} 不是 SCREAMING_SNAKE_CASE",
                c.as_str()
            );
        }
        assert_eq!(ErrorCode::ALL.len(), seen.len());
    }

    #[test]
    fn error_body_shape() {
        let resp = ErrorCode::TableNotFound.response("table default.t");
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    /// 两个 503 必须能区分（并发满可重试 / 只读永不重试）。
    #[test]
    fn three_flavors_of_503() {
        for c in [
            ErrorCode::TooManyQueries,
            ErrorCode::MemoryPressure,
            ErrorCode::ReadOnly,
        ] {
            assert_eq!(c.status(), StatusCode::SERVICE_UNAVAILABLE);
        }
        assert!(ErrorCode::TooManyQueries.retryable());
        assert!(ErrorCode::MemoryPressure.retryable());
        assert!(!ErrorCode::ReadOnly.retryable());
    }

    /// 「内存不够」的两种：配额错（改查询）vs 全局压力（退避重试）——动作相反。
    #[test]
    fn two_flavors_of_out_of_memory() {
        assert_eq!(
            ErrorCode::from_qsdb(&QsdbError::QueryMemoryLimit("x".into()), true),
            ErrorCode::QueryMemoryLimit
        );
        assert_eq!(
            ErrorCode::from_qsdb(&QsdbError::MemoryPressure("x".into()), true),
            ErrorCode::MemoryPressure
        );
        assert_eq!(
            ErrorCode::QueryMemoryLimit.status(),
            StatusCode::BAD_REQUEST
        );
        assert!(!ErrorCode::QueryMemoryLimit.retryable());
    }

    /// 三闸正交（十七批 R1）：超时管**计算时长**、QUERY_MEMORY_LIMIT 管**执行
    /// 内存**、RESULT_TOO_LARGE 管**结果传输体量**。护栏与内存配额同为 400
    /// no-retry，但 SQLState 刻意不同（54000 程序性上限 vs 53200 内存类）——
    /// 混用一个码，驱动就无法提示"改 LIMIT/换流式格式"这个正确动作。
    #[test]
    fn result_guard_is_its_own_gate() {
        let c = ErrorCode::ResultTooLarge;
        assert_eq!(c.status(), StatusCode::BAD_REQUEST);
        assert!(!c.retryable());
        assert_eq!(c.category(), "query");
        assert_eq!(c.sqlstate(), "54000");
        assert_ne!(c.sqlstate(), ErrorCode::QueryMemoryLimit.sqlstate());
    }

    /// NotFound 的类别决定查询端点的码——不再嗅探文案。
    #[test]
    fn not_found_kind_drives_code() {
        let table = QsdbError::table_not_found("table default.t_column");
        // 表名里带 "column" 也曾被误判成列不存在（旧实现嗅探 `msg.contains("column")`）
        assert_eq!(ErrorCode::from_qsdb(&table, true), ErrorCode::TableNotFound);
        let column = QsdbError::column_not_found("column 'x' not in table t");
        assert_eq!(
            ErrorCode::from_qsdb(&column, true),
            ErrorCode::ColumnNotFound
        );
        // 资源端点：同一错误 → 404
        assert_eq!(ErrorCode::from_qsdb(&table, false), ErrorCode::NotFound);
        // 查询端点上的其它对象（文件/视图/函数）→ 参数错，不是 TABLE_NOT_FOUND
        let file = QsdbError::file_not_found("read_parquet: x.parquet");
        assert_eq!(
            ErrorCode::from_qsdb(&file, true),
            ErrorCode::InvalidArgument
        );
    }

    /// 角色不足 = FORBIDDEN（403），不是 INVALID_ARGUMENT（400）——身份不够格
    /// 与参数错是两件事；与 /write 面的 can_write 闸同码（十九批Ⅲ修正）。
    #[test]
    fn role_denial_is_forbidden_not_invalid() {
        let e = QsdbError::forbidden("role 'reader' cannot execute DDL");
        assert_eq!(
            ErrorCode::from_qsdb(&e, true),
            ErrorCode::Forbidden,
            "查询面的角色拒绝"
        );
        assert_eq!(
            ErrorCode::from_qsdb(&e, false),
            ErrorCode::Forbidden,
            "资源面同理（角色问题不是'资源不存在'）"
        );
        assert_eq!(ErrorCode::Forbidden.status(), StatusCode::FORBIDDEN);
        assert!(!ErrorCode::Forbidden.retryable());
    }

    #[test]
    fn read_only_and_io_are_not_swallowed() {
        assert_eq!(
            ErrorCode::from_qsdb(&QsdbError::read_only("read-only"), true),
            ErrorCode::ReadOnly
        );
        let io = QsdbError::Io(std::io::Error::other("disk"));
        assert_eq!(ErrorCode::from_qsdb(&io, false), ErrorCode::IoError);
        assert!(ErrorCode::IoError.retryable());
        // Arrow 里的 IoError 子类同样是瞬时 I/O；其余 Arrow 问题归 INTERNAL
        let arrow_io = QsdbError::Arrow(arrow::error::ArrowError::IoError(
            "read".into(),
            std::io::Error::other("boom"),
        ));
        assert_eq!(ErrorCode::from_qsdb(&arrow_io, false), ErrorCode::IoError);
    }
}
