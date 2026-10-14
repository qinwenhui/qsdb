//! 客户端错误：**传输层与语义层必须能分辨**。
//!
//! 把两者混成一个 `String` 是驱动最容易犯的错——调用方只能靠文案猜
//! "该退避重试还是该报给用户"。这里分档：
//!
//! - [`Error::Io`] / [`Error::Http`]：**传输/协议层**（连不上、body 截断、
//!   分帧坏了）。重试有意义，且是幂等的（请求根本没被服务端处理）。
//! - [`Error::Api`]：**服务端语义层**（`{error, code}`）。带着**机器码**回来，
//!   重试与否由码决定（[`Error::retriable`]）——码表在 `GET /api/v1/error-codes`，
//!   **别把码硬编码进客户端**（内核加码时驱动自动跟上）。
//! - [`Error::Decode`]：响应体坏了（bulk/rows/json/ipc 不合法）。
//! - [`Error::Usage`]：客户端**用法**错（URL 缺端口、列名没给、列数不符）。
//!   这类错在发请求之前就该报，不该变成一次 400。
//!
//! `error` 文案**不做兼容承诺**（见 `docs/error-codes.md`）——所以要判分支就判 `code`。

use std::fmt;

/// 客户端结果别名。
pub type Result<T> = std::result::Result<T, Error>;

/// 客户端错误。
#[derive(Debug)]
pub enum Error {
    /// 传输层：连接、读写、超时。
    Io(std::io::Error),
    /// HTTP 协议层：状态行/头/分帧不合法，或响应被截断。
    Http(String),
    /// 服务端语义错误体 `{error, code}`。
    Api {
        /// HTTP 状态码。
        status: u16,
        /// 机器码（`docs/error-codes.md`）。空串 = 服务端没给（不该发生）。
        code: String,
        /// 人读文案（**不做兼容承诺**）。
        message: String,
    },
    /// 响应体解码失败。
    Decode(String),
    /// 客户端用法错误（请求未发出）。
    Usage(String),
}

impl Error {
    /// 机器码（仅 [`Error::Api`] 有）。
    ///
    /// 用法：`if e.code() == Some("TOO_MANY_QUERIES") { backoff(); }`
    pub fn code(&self) -> Option<&str> {
        match self {
            Error::Api { code, .. } if !code.is_empty() => Some(code),
            _ => None,
        }
    }

    /// 是否值得**立刻**重试。
    ///
    /// 传输层：值得（请求未被处理）。语义层：**一律 false** ——
    /// 重试一个语义错误只会把同一个错误再来一遍，压力测试服务端。
    /// 驱动要按码自己决定（如 `TOO_MANY_QUERIES` 退避后重试）。
    pub fn retriable(&self) -> bool {
        matches!(self, Error::Io(_) | Error::Http(_))
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "传输失败: {e}"),
            Error::Http(m) => write!(f, "HTTP 协议错: {m}"),
            Error::Api {
                status,
                code,
                message,
            } => write!(f, "服务端错误 {status} [{code}]: {message}"),
            Error::Decode(m) => write!(f, "响应解码失败: {m}"),
            Error::Usage(m) => write!(f, "客户端用法错: {m}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}

// ---------------------------------------------------------------------------
// 内部：解码辅助（各格式实现共用，错误文案统一带格式名）
// ---------------------------------------------------------------------------

/// 解码到一半发现字节不够/不合法 → 一律 [`Error::Decode`]，**显式报错不猜**。
pub(crate) fn decode_err(fmt_name: &str, msg: impl fmt::Display) -> Error {
    Error::Decode(format!("{fmt_name}: {msg}"))
}
