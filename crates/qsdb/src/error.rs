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
