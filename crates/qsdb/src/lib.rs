//! QSDB 核心库：单机、嵌入式、列式、Arrow-native、Parquet-backed 的分析型读写库。
//!
//! 里程碑路线（见 `主设计稿` §14）：
//! - M0：类型系统 ✓
//! - M1：表格式 + 提交协议 + 裁剪扫描 + 嵌入式 API（当前）
//! - M2：向量化执行 + SQL-0/1
//! - M3：HTTP 服务 + compaction
//!
//! 核心库刻意保持无 tokio、无后台常驻线程，可安全嵌入宿主引擎。
//! 同一数据目录允许多实例/多进程共存（分区级文件锁协调）。

pub mod api;
pub mod backup;
pub mod bulk;
pub mod cache;
pub mod catalog;
pub mod commit;
pub mod compact;
pub mod csv;
pub mod error;
pub mod exec;
pub mod export;
pub mod expr;
pub mod filecache;
pub mod fsutil;
#[cfg(feature = "hdfs")]
pub mod hdfs_backend;
pub mod index;
pub mod manifest;
pub mod memory;
pub mod partition;
pub mod query;
#[cfg(any(feature = "s3", feature = "remote"))]
pub mod remote;
pub mod rows;
pub mod scan;
pub mod sql;
#[cfg_attr(not(feature = "remote"), allow(dead_code))] // 仅远端写路径消费
pub mod staging;
pub mod stats;
pub mod store;
pub mod types;
pub mod users;
pub mod views;
pub mod write;
pub mod write_buffer;

pub use api::{PartitionStat, Qsdb, QsdbOptions, ScanRequest};
pub use catalog::{ColumnDef, TableDef};
pub use error::{NotFoundKind, QsdbError, Result};
pub use expr::udaf;
pub use expr::udf;
pub use manifest::{ColumnStats, PartitionManifest, SegmentEntry};
pub use memory::{MemoryPool, Reservation};
// ---- format-spec 原语门面（F13）：生态工具实现**合规布局**的契约级稳定面 ----
//
// **为什么单独成组、且要在根上一处拿全**：生态工具（qsdb-sync 的 sink / replica、
// 未来的湖导出）需要**独立实现**符合 format-spec 的读写，而它们**不该去 fork 一份
// 内核实现**——qsdb-sync 的 sink-parquet 初版就复刻过一份 `encode_partition_value`，
// 那种复刻一旦与内核漂移，产出的是"看着对、读不回"的数据。
//
// 本组列出的是**格式契约的一部分**：与 `docs/format-spec.md` 同源冻结，变更走
// format-spec 的流程（追加式 / 版本号），**不是普通 pub 项的"可能随版本变化"**。
// 未列出的 pub 项仍按 `对接契约` 开头的原则——内部实现，不通知不迁移。
pub use index::{INDEX_FILE, INDEX_LOCK, PartitionsIndex};
pub use manifest::MANIFEST_FILE;
pub use partition::{
    decode_partition_value, encode_partition_value, is_partition_dir_name, parse_partition_dir,
    parse_partition_value, partition_dir, partition_value_string,
};
pub use scan::{Predicate, Scan};
pub use store::Store;
pub use types::{Literal, LogicalType};
pub use users::{Role, UserStore};
pub use write::WriteResult;
