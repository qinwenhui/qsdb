//! QSDB 核心库：单机、嵌入式、列式、Arrow-native、Parquet-backed 的分析型读写库。
//!
//! 里程碑路线（见 `docs/QSDB设计开发文档.md` §14）：
//! - M0：类型系统 ✓
//! - M1：表格式 + 提交协议 + 裁剪扫描 + 嵌入式 API（当前）
//! - M2：向量化执行 + SQL-0/1
//! - M3：HTTP 服务 + compaction
//!
//! 核心库刻意保持无 tokio、无后台常驻线程，可安全嵌入宿主引擎。
//! 同一数据目录允许多实例/多进程共存（分区级文件锁协调）。

pub mod api;
pub mod cache;
pub mod catalog;
pub mod compact;
pub mod error;
pub mod exec;
pub mod expr;
pub mod filecache;
pub mod fsutil;
pub mod index;
pub mod manifest;
pub mod partition;
pub mod query;
pub mod scan;
#[cfg(feature = "s3")]
pub mod remote;
pub mod sql;
pub mod stats;
pub mod store;
pub mod types;
pub mod write;
pub mod write_buffer;

pub use api::{Qsdb, QsdbOptions, ScanRequest};
pub use catalog::{ColumnDef, TableDef};
pub use error::{QsdbError, Result};
pub use expr::udaf;
pub use expr::udf;
pub use manifest::{ColumnStats, SegmentEntry};
pub use scan::{Predicate, Scan};
pub use store::Store;
pub use types::{Literal, LogicalType};
pub use write::WriteResult;
