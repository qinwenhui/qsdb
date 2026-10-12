//! QSDB 错误体系。
//!
//! 所有错误统一走 [`QsdbError`]；模块内部尽量携带可操作的上下文
//! （期望什么、实际得到什么），让上层能直接面向用户呈现。

/// 全库统一 `Result` 别名。
pub type Result<T, E = QsdbError> = std::result::Result<T, E>;

/// "什么对象不存在"的**机器可读**类别。
///
/// 为什么要它：上层（HTTP/Flight 面）要把"表不存在"映射成 `TABLE_NOT_FOUND`、
/// "列不存在"映射成 `COLUMN_NOT_FOUND`，而此前只能嗅探文案
/// （`msg.contains("column")`）——表名里带 "column"（如 `default.t_column`）
/// 就会被误判成列不存在。类别随错误一起走，判定不再依赖人读文案。
///
/// 刻意保持**最小**：只细化会改变上层映射的类别，其余一律 [`NotFoundKind::Object`]
/// （"存在性"这件事本身就是资源端点语义，映射成一类即可）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotFoundKind {
    Table,
    Column,
    /// 外表文件（`read_parquet` 路径/glob 未命中）
    File,
    /// 未细分的对象（视图/用户/分区/函数/SQL 名称等）
    Object,
}

#[derive(Debug, thiserror::Error)]
pub enum QsdbError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    /// 参数非法（API 层面误用）
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// 类型系统错误：不匹配、溢出、精度超限等
    #[error("type error: {0}")]
    Type(String),

    /// 文本解析失败（SQL 字面量、日期、数字等）
    #[error("parse error: {0}")]
    Parse(String),

    /// 当前版本不支持的能力（按里程碑分级开放）
    #[error("unsupported: {0}")]
    Unsupported(String),

    /// 元数据/表格式层错误
    #[error("metadata error: {0}")]
    Meta(String),

    /// 对象（库/表/分区/列/文件）不存在。
    ///
    /// `kind` = 机器可读类别（上层据此选错误码）；`name` = 人读描述
    /// （沿用历史文案形态，如 `table default.t` / `column 'x' not in table t`）。
    #[error("not found: {name}")]
    NotFound { kind: NotFoundKind, name: String },

    /// 对象已存在
    #[error("already exists: {0}")]
    AlreadyExists(String),

    /// 查询被取消（kill / 客户端断连）
    #[error("query cancelled: {0}")]
    QueryCancelled(String),

    /// 查询超时
    #[error("query timeout: {0}")]
    QueryTimeout(String),

    /// 查询内存超限（查询终止而非 OOM 全库）
    #[error("query memory limit exceeded: {0}")]
    QueryMemoryLimit(String),

    /// 查询内部 panic（已被隔离边界捕获，进程无恙）
    #[error("query panicked (isolated, process unaffected): {0}")]
    QueryPanic(String),

    /// 只读句柄拒绝变更（standby / 只读副本）。
    ///
    /// 独立变体而非塞进 `InvalidArgument`：上层要把它映射成 `READ_ONLY`
    /// （503，**永不重试**——重试只读库毫无意义），而参数错是"改请求就好"。
    /// 两者对客户端的动作完全不同，靠文案的 "read-only" 字样区分太脆。
    #[error("read-only: {0}")]
    ReadOnly(String),
}

impl QsdbError {
    pub fn invalid(msg: impl Into<String>) -> Self {
        Self::InvalidArgument(msg.into())
    }

    pub fn type_err(msg: impl Into<String>) -> Self {
        Self::Type(msg.into())
    }

    pub fn parse_err(msg: impl Into<String>) -> Self {
        Self::Parse(msg.into())
    }

    pub fn unsupported(msg: impl Into<String>) -> Self {
        Self::Unsupported(msg.into())
    }

    pub fn meta(msg: impl Into<String>) -> Self {
        Self::Meta(msg.into())
    }

    /// 未细分类别的不存在（视图/用户/分区/函数……）。
    pub fn not_found(msg: impl Into<String>) -> Self {
        Self::NotFound {
            kind: NotFoundKind::Object,
            name: msg.into(),
        }
    }

    pub fn table_not_found(name: impl Into<String>) -> Self {
        Self::NotFound {
            kind: NotFoundKind::Table,
            name: name.into(),
        }
    }

    /// `name` 建议写成 `column 'x' not in table t` 形态（人读）。
    pub fn column_not_found(name: impl Into<String>) -> Self {
        Self::NotFound {
            kind: NotFoundKind::Column,
            name: name.into(),
        }
    }

    pub fn file_not_found(name: impl Into<String>) -> Self {
        Self::NotFound {
            kind: NotFoundKind::File,
            name: name.into(),
        }
    }

    /// 不存在类错误的机器可读类别（非 NotFound 返回 `None`）。
    pub fn not_found_kind(&self) -> Option<NotFoundKind> {
        match self {
            Self::NotFound { kind, .. } => Some(*kind),
            _ => None,
        }
    }

    /// 是否为 I/O 类瞬时故障（上层据此标记"可重试"）。
    ///
    /// Arrow 错误里只有其 `IoError` 子类是瞬时 I/O；其余是编解码/数据问题。
    pub fn is_transient_io(&self) -> bool {
        match self {
            Self::Io(_) => true,
            Self::Arrow(e) => matches!(e, arrow::error::ArrowError::IoError(_, _)),
            _ => false,
        }
    }

    pub fn already_exists(msg: impl Into<String>) -> Self {
        Self::AlreadyExists(msg.into())
    }

    pub fn read_only(msg: impl Into<String>) -> Self {
        Self::ReadOnly(msg.into())
    }

    /// 不存在类 IO 错误（NotFound/AlreadyExists）转 Option
    pub fn is_not_found(&self) -> bool {
        matches!(self, Self::NotFound { .. } | Self::AlreadyExists(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_messages() {
        assert_eq!(QsdbError::type_err("x").to_string(), "type error: x");
        assert_eq!(
            QsdbError::Io(std::io::Error::other("boom")).to_string(),
            "io error: boom"
        );
    }
}
