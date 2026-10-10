//! QSDB 错误体系。
//!
//! 所有错误统一走 [`QsdbError`]；模块内部尽量携带可操作的上下文
//! （期望什么、实际得到什么），让上层能直接面向用户呈现。

/// 全库统一 `Result` 别名。
pub type Result<T, E = QsdbError> = std::result::Result<T, E>;

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

    /// 对象（库/表/分区/列）不存在
    #[error("not found: {0}")]
    NotFound(String),

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

    pub fn not_found(msg: impl Into<String>) -> Self {
        Self::NotFound(msg.into())
    }

    pub fn already_exists(msg: impl Into<String>) -> Self {
        Self::AlreadyExists(msg.into())
    }

    /// 不存在类 IO 错误（NotFound/AlreadyExists）转 Option
    pub fn is_not_found(&self) -> bool {
        matches!(self, Self::NotFound(_) | Self::AlreadyExists(_))
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
