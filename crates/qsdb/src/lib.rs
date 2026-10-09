//! QSDB 核心库：单机、嵌入式、列式、Arrow-native、Parquet-backed 的分析型读写库。
//!
//! 里程碑路线（见 `docs/QSDB设计开发文档.md` §14）：
//! - M0：类型系统（当前）
//! - M1：表格式 + 提交协议 + 裁剪扫描 + 嵌入式 API
//! - M2：向量化执行 + SQL-0/1
//! - M3：HTTP 服务 + compaction
//!
//! 核心库刻意保持无 tokio、无后台常驻线程，可安全嵌入宿主引擎。

pub mod error;
pub mod types;

pub use error::{QsdbError, Result};
pub use types::{Literal, LogicalType};
