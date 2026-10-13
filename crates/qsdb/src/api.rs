//! `Qsdb` 门面：嵌入式 API（一等公民）。
//!
//! 句柄无状态（磁盘为真值 + mtime 缓存），`Send + Sync`，
//! 同目录可多实例/多进程共存（分区 flock 协调）。

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::record_batch::RecordBatch;

use crate::catalog::{
    DEFAULT_DB, TableDef, TableDefCache, create_table, db_dir, drop_table, read_table_names,
    table_dir,
};
use crate::error::{QsdbError, Result};
use crate::fsutil::{dir_size, purge_trash};
use crate::partition::split_by_partition;
use crate::scan::{Predicate, Scan};
use crate::store::Store;
use crate::types::Literal;
use crate::write::{self, WriteResult};

/// 数据耐久性级别（启动期；写入路径的 fsync 策略）。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Durability {
    /// **进程崩溃安全**（默认）：页缓存即持久——kill -9 / 进程退出后数据完好。
    /// 掉电可能丢失最近若干 segment 并留下「manifest 引用未落盘段」的损坏
    /// （GC 可清孤儿，但需人工核对）。
    #[default]
    ProcessCrash,
    /// **掉电安全**：segment / 元数据 / staging 文件均在 rename 前 fsync
    /// （POSIX 另加目录 sync）。代价 = 每段一次强制落盘（损耗实测见
    /// docs/QSDB设计开发文档.md 耐久性专项）。
    PowerLoss,
}

/// 存储后端配置（启动期，改后需重开句柄）。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum StorageConfig {
    /// 本地文件系统（默认）
    #[default]
    Local,
    /// HDFS 管理表（feature `hdfs`；存储设计 §6.3/§7.3——CAS 由
    /// 锁对象模拟，见 `hdfs_backend`）。`url` 形如 `hdfs://host:9000`
    #[cfg(feature = "hdfs")]
    Hdfs {
        url: String,
        /// HDFS 内路径前缀（建议用 /tmp/... 或专用目录）
        prefix: String,
    },
    /// S3 及兼容对象存储管理表（feature `s3`；存储设计 §5/§7.2）
    #[cfg(feature = "s3")]
    S3 {
        /// S3 兼容端点（AWS 官方留空走默认链；`http://` 端点自动 allow_http）
        endpoint: Option<String>,
        region: Option<String>,
        bucket: String,
        /// bucket 内 key 前缀（多租户/多库共存）
        prefix: String,
    },
}

/// 打开选项。
#[derive(Debug, Clone)]
pub struct QsdbOptions {
    /// 存储后端（默认本地；`StorageConfig::S3` 需编译 feature `s3`）
    pub storage: StorageConfig,
    /// trash 静默期（毫秒）；`gc()` 只清理更早的条目
    /// （远端模式 = 孤儿对象安全期：不被 manifest 引用且早于该期限才可删）
    pub trash_grace_ms: u64,
    /// 热数据缓存预算（字节）：segment 级**解码后**的 Arrow 批 LRU 缓存。
    /// `0` = 关闭；默认 **256MB**（保守值，守低占用定位）。
    ///
    /// **权衡（2026-10-12 实测，10M × 4 列）**：
    /// - 256MB：缓存上界 `budget/8 = 32MB` **卡在分片片段（32~64MB）边缘**
    ///   → 16 分片只进 11 个 → **付了缓存成本却几乎不命中**（0.18s，比关缓存
    ///   的 0.14s 还慢）；但查询期工作集仅 **15~17MB**。
    /// - 1GB：全命中 → 宽投影 **0.011s（快 16x）**；代价是查询期 RSS 到
    ///   **1.2GB**（mem_vs 实测）——**违背"极致低占用"定位**。
    ///
    /// **故默认取保守值**：宽投影要快的部署显式调大（`--cache-mb`）。
    /// 根治方向是**缓存粒度**（按批/RG 分块，使小预算也能部分命中），
    /// 而不是放大预算。
    ///
    /// **注意**：本预算是**缓存索引的记账上限**（LRU 驱逐保证 `used ≤ budget`），
    /// **不是进程内存硬上限**——已被驱逐但仍在途使用的批由 Arc 持有、
    /// 不计入 `used`，故实际峰值可超预算。
    ///
    /// **性能口径警告**：本缓存让同一 segment 的重复查询免解码，收益显著——
    /// 但它不是与"每次现解码"的引擎（如 DuckDB `read_parquet`）可直接比较的
    /// 口径。对外性能声明必须显式标注是否启用（`crates/qsdb-bench/src/bin/
    /// fair_vs.rs` 的权威口径为 `scan_cache_bytes = 0` 冷读 + 分进程独立测量；
    /// 详见 `docs/QSDB设计开发文档.md` 性能口径一节）。
    pub scan_cache_bytes: usize,
    /// 默认查询超时（毫秒）；None/0 = 不超时
    pub query_timeout_ms: Option<u64>,
    /// 默认单查询内存上限（字节）；None = 不限。超限查询被终止而非 OOM 全库。
    ///
    /// **库默认不限**（嵌入方自控内存策略）；`qsdb server` 兜底 **4GB**
    /// （`--query-memory-limit-mb`）——server 是唯一会叠加多查询的部署面。
    /// 只约束**显式物化算子**（join build / sort spill / window / udaf）的记账，
    /// 流式扫描/聚合不驻留故不记账。
    ///
    /// **与并发闸（server `--max-concurrent-queries`）相互独立**：二者不构成
    /// 一个内存上界，最坏占用 ≈ `limit × 并发数`，需按机器容量各自收紧。
    /// 详见 `docs/configuration.md` 内存模型一节。
    pub query_memory_limit_bytes: Option<u64>,
    /// **全局内存预算**（字节；0/None = 不限）：本句柄内**所有**查询共享的
    /// 执行内存上界 + 解码缓存的总上界（缓存可被执行内存**挤兑**）。
    ///
    /// 与 [`Self::query_memory_limit_bytes`] 的分工：那个是「单个查询的配额」，
    /// 这个是「整个句柄的天花板」。三者（本预算 / 每查询配额 / 并发闸）叠加后
    /// 才是真实上界：
    /// - 单查询要不到额度（多为**他人占用**）→ `MemoryPressure`（503，**可退避重试**）
    /// - 单查询超**自己**配额 → `QueryMemoryLimit`（400，改查询才有用）
    /// - 压力下**先挤兑缓存**（纯缓存，丢了大不了重算）——故缓存不会挤掉查询
    ///
    /// **库默认不限**（嵌入方自控）；`qsdb server` 兜底 **4GB**
    /// （`--memory-budget-mb`）。只约束**记账到的**内存：物化算子 + 解码缓存；
    /// 流式在途批的残留上界见 `docs/内存预算与溢出设计.md` §七。
    pub memory_budget_bytes: Option<u64>,
    /// **溢出目录根**（`None` = 系统临时目录）。
    ///
    /// 溢出（排序/聚合/join 落盘）体量可以很大——5M 行排序就是几十上百 MB，
    /// 落在系统盘上可能把小分区写满。生产建议指向**与数据同盘**的目录
    /// （`qsdb server` 默认 `<库根>/_spill`）：
    /// - 目录不存在会自动创建；
    /// - 每次查询在其下开 `qsdb_spill_<uuid>/` 子目录，查询结束即删；
    /// - `open` 时清理**超过 `trash_grace_ms` 未动过**的残留子目录（崩溃遗留），
    ///   不动近期活跃的（同目录可能有多进程）。
    pub spill_dir: Option<PathBuf>,
    /// 远端外表（read_parquet s3://）段文件本地缓存预算（字节）；0 = 关闭。
    /// 布局 `{root}/_filecache/{url_hash}/{etag}/…`，ETag 键控自动失效。
    pub file_cache_bytes: u64,
    /// 远端写 staging 目录（M6-4）：Some = 启用——远端写先落本地盘（ACK 即回），
    /// `flush_staged()` 批量上传提交。需 S3 后端。可见性语义：未 flush 的
    /// staged 数据对查询不可见（manifest 唯一真值）。
    pub staging_dir: Option<PathBuf>,
    /// 耐久性档位（本地文件写 fsync 策略；远端 PUT 由后端保证，仅作用于
    /// staging 本地文件）。见 [`Durability`]。
    pub durability: Durability,
    /// **只读模式**：拒绝一切写路径（append/overwrite/DDL/compact/gc/
    /// refresh-index；server /write 返回 503）。standby/只读副本用——
    /// 与「多写者协议」正交：只读句柄永不做写者，杜绝误连双主。
    pub read_only: bool,
}

impl Default for QsdbOptions {
    fn default() -> Self {
        Self {
            storage: StorageConfig::Local,
            trash_grace_ms: 10 * 60 * 1000,
            scan_cache_bytes: 256 * 1024 * 1024,
            query_timeout_ms: None,
            query_memory_limit_bytes: None,
            memory_budget_bytes: None,
            spill_dir: None,
            file_cache_bytes: 0,
            staging_dir: None,
            durability: Durability::ProcessCrash,
            read_only: false,
        }
    }
}

impl QsdbOptions {
    /// 本地后端默认选项（等价 `Default`，语义显式）。
    pub fn local() -> Self {
        Self::default()
    }

    /// S3 后端选项（feature `s3`）。
    #[cfg(feature = "s3")]
    pub fn s3(
        endpoint: Option<String>,
        region: Option<String>,
        bucket: impl Into<String>,
        prefix: impl Into<String>,
    ) -> Self {
        Self {
            storage: StorageConfig::S3 {
                endpoint,
                region,
                bucket: bucket.into(),
                prefix: prefix.into(),
            },
            ..Self::default()
        }
    }
}

/// 扫描请求。
#[derive(Debug, Clone, Default)]
pub struct ScanRequest {
    /// 表名（必填，运行时校验）
    pub table: String,
    /// 库名；None = "default"
    pub db: Option<String>,
    /// 投影列（None = 全部列，含分区列常量重建）
    pub projection: Option<Vec<String>>,
    /// 谓词（分区/segment/row group 三级裁剪 + 行过滤）
    pub predicate: Option<Predicate>,
    pub limit: Option<u64>,
}

/// 分区统计（`Qsdb::partition_stats`；qsdb-sync 轮询契约）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionStat {
    /// 相对表目录的分区名（`/` 分隔）
    pub name: String,
    /// manifest 代数（单调递增；同步增量的判据）
    pub generation: u64,
    pub rows: u64,
    pub segments: usize,
}

/// 运行期可热更配置：原子量存储，新查询/新请求即时生效，**无需重启**。
///
/// 初始值取自 [`QsdbOptions`]；之后经 setter 调整（server 的
/// `PUT /api/v1/config` 或嵌入方直接调用）。
#[derive(Debug)]
pub struct QsdbRuntime {
    /// 默认查询超时（毫秒；0 = 不超时）
    query_timeout_ms: AtomicU64,
    /// 默认单查询内存上限（字节；0 = 不限）
    query_memory_limit_bytes: AtomicU64,
}

impl QsdbRuntime {
    fn from_opts(opts: &QsdbOptions) -> Self {
        Self {
            query_timeout_ms: AtomicU64::new(opts.query_timeout_ms.unwrap_or(0)),
            query_memory_limit_bytes: AtomicU64::new(opts.query_memory_limit_bytes.unwrap_or(0)),
        }
    }

    pub fn query_timeout_ms(&self) -> Option<u64> {
        match self.query_timeout_ms.load(Ordering::Relaxed) {
            0 => None,
            v => Some(v),
        }
    }

    pub fn set_query_timeout_ms(&self, ms: u64) {
        self.query_timeout_ms.store(ms, Ordering::Relaxed);
    }

    pub fn query_memory_limit_bytes(&self) -> Option<u64> {
        match self.query_memory_limit_bytes.load(Ordering::Relaxed) {
            0 => None,
            v => Some(v),
        }
    }

    pub fn set_query_memory_limit_bytes(&self, bytes: u64) {
        self.query_memory_limit_bytes
            .store(bytes, Ordering::Relaxed);
    }
}

/// 清理溢出目录里**陈旧**的残留（`qsdb_spill_*` 子目录）。
///
/// 崩溃/被杀会留下子目录；但同一目录可能有多进程在用，故只清**超过静默期
/// 未被动过**的（`trash_grace_ms`，与 `_trash` 同一个"安全期"概念）。
fn purge_stale_spills(dir: &Path, grace_ms: u64) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    let now = std::time::SystemTime::now();
    for e in entries.flatten() {
        let name = e.file_name();
        let name = name.to_string_lossy();
        if !name.starts_with("qsdb_spill_") {
            continue;
        }
        let stale = e
            .metadata()
            .and_then(|m| m.modified())
            .ok()
            .and_then(|m| now.duration_since(m).ok())
            .map(|age| age.as_millis() as u64 > grace_ms)
            .unwrap_or(false);
        if stale {
            let _ = std::fs::remove_dir_all(e.path());
        }
    }
}

/// QSDB 句柄。
pub struct Qsdb {
    root: PathBuf,
    store: Arc<Store>,
    opts: QsdbOptions,
    defs: TableDefCache,
    cache: Arc<crate::cache::SegmentCache>,
    /// 全局内存账本（一键共享：执行内存 + 可挤兑的缓存）
    pool: Arc<crate::memory::MemoryPool>,
    /// 溢出目录根（`None` = 系统临时目录；只读句柄恒 None）
    spill_dir: Option<PathBuf>,
    runtime: Arc<QsdbRuntime>,
    /// 远端外表段文件缓存（预算 0 = 禁用实例，API 全 no-op）
    file_cache: Arc<crate::filecache::FileCache>,
    /// 远端写 staging（M6-4；None = 关闭/本地后端）
    staging: Option<Arc<crate::staging::Staging>>,
    /// 只读模式（见 QsdbOptions::read_only）
    read_only: bool,
}

impl Qsdb {
    /// 打开（目录不存在则初始化为空库；`opts.storage` 选后端，默认本地）。
    pub fn open(root: impl AsRef<Path>, opts: QsdbOptions) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        let store = match &opts.storage {
            StorageConfig::Local => {
                Store::local_dur(&root, opts.durability == Durability::PowerLoss)
            }
            #[cfg(feature = "hdfs")]
            StorageConfig::Hdfs { url, prefix } => {
                let os = crate::hdfs_backend::build_hdfs_store(url)?;
                let vroot = PathBuf::from(format!("{url}/{prefix}"));
                Store::remote_dur(
                    vroot,
                    os,
                    prefix.clone(),
                    opts.durability == Durability::PowerLoss,
                )
            }
            #[cfg(feature = "s3")]
            StorageConfig::S3 {
                endpoint,
                region,
                bucket,
                prefix,
            } => {
                let os =
                    crate::remote::build_s3_store(endpoint.as_deref(), region.as_deref(), bucket)?;
                let vroot = PathBuf::from(format!("s3://{bucket}/{prefix}"));
                Store::remote_dur(
                    vroot,
                    os,
                    prefix.clone(),
                    opts.durability == Durability::PowerLoss,
                )
            }
        };
        Self::open_with_store(root, opts, store)
    }

    /// 打开并注入自定义存储后端（测试/嵌入方；生产用 [`Qsdb::open`]）。
    /// `root` 仍为本地目录（段缓存/文件锁等本地状态），命名空间以
    /// `store.root()` 为准（Remote = 虚拟根 `s3://bucket/prefix`）。
    pub fn open_with_store(
        root: impl AsRef<Path>,
        opts: QsdbOptions,
        store: Store,
    ) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        let store = Arc::new(store);
        store
            .create_dir_all(&store.root().join("_catalog"))
            .map_err(|e| QsdbError::meta(format!("init db root failed: {e}")))?;
        store
            .create_dir_all(&db_dir(store.root(), DEFAULT_DB))
            .map_err(|e| QsdbError::meta(format!("init default db failed: {e}")))?;
        let mut cache = crate::cache::SegmentCache::new(opts.scan_cache_bytes);
        // 全局内存账本：缓存挂成**可挤兑**租户（压力下先丢缓存，见 memory.rs）。
        // 预算 0/None = 不限（嵌入方自控；server 兜 4GB）。
        let pool = Arc::new(crate::memory::MemoryPool::new(
            opts.memory_budget_bytes.unwrap_or(0),
        ));
        if cache.is_enabled() {
            cache.attach_pool(Arc::clone(&pool));
            pool.register_yielder(crate::memory::Yielder::SegmentCache(cache.clone()));
        }
        let cache = Arc::new(cache);
        let runtime = Arc::new(QsdbRuntime::from_opts(&opts));
        let file_cache = Arc::new(crate::filecache::FileCache::new(
            root.join("_filecache"),
            opts.file_cache_bytes,
        ));
        let read_only = opts.read_only;
        // 溢出目录：只读句柄不写库根（回落系统 temp），并清理崩溃残留
        let spill_dir = if read_only { None } else { opts.spill_dir.clone() };
        if let Some(dir) = &spill_dir {
            std::fs::create_dir_all(dir)
                .map_err(|e| QsdbError::meta(format!("init spill dir {} failed: {e}", dir.display())))?;
            purge_stale_spills(dir, opts.trash_grace_ms);
        }
        // staging（M6-4）：仅远端后端 + feature s3
        let staging = match &opts.staging_dir {
            None => None,
            #[cfg(feature = "remote")]
            Some(dir) => {
                if store.is_local() {
                    return Err(QsdbError::invalid(
                        "staging_dir requires a remote (s3) storage backend",
                    ));
                }
                Some(Arc::new(crate::staging::Staging::new(dir)?))
            }
            #[cfg(not(feature = "remote"))]
            Some(_) => {
                return Err(QsdbError::unsupported(
                    "staging_dir requires building with feature 's3' or 'hdfs'",
                ));
            }
        };
        Ok(Self {
            root,
            store,
            opts,
            defs: TableDefCache::default(),
            cache,
            pool,
            spill_dir,
            runtime,
            file_cache,
            staging,
            read_only,
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// 存储后端句柄（本地默认；M6-2/3 起可配置远端）。
    pub fn store(&self) -> &Arc<Store> {
        &self.store
    }

    /// staging 句柄（写路径内部用；None = 关闭）。
    #[cfg_attr(not(feature = "remote"), allow(unused))]
    pub(crate) fn staging(&self) -> Option<&crate::staging::Staging> {
        self.staging.as_deref()
    }

    fn db_name<'a>(&self, db: Option<&'a str>) -> &'a str {
        db.unwrap_or(DEFAULT_DB)
    }

    /// 只读模式（standby/只读副本）。
    pub fn read_only(&self) -> bool {
        self.read_only
    }

    /// 写路径统一闸门：只读句柄拒绝一切变更。
    fn ensure_writable(&self) -> Result<()> {
        if self.read_only {
            return Err(QsdbError::read_only(
                "database is opened read-only; write/DDL/compact/gc rejected",
            ));
        }
        Ok(())
    }

    fn def(&self, db: Option<&str>, table: &str) -> Result<Arc<TableDef>> {
        let db = self.db_name(db);
        let names = read_table_names(&self.store, db)?;
        if !names.iter().any(|n| n == table) {
            return Err(QsdbError::table_not_found(format!("table {db}.{table}")));
        }
        self.defs.get(&self.store, db, table)
    }

    // -- DDL ----------------------------------------------------------------

    /// 建表。
    pub fn create_table(&self, def: TableDef) -> Result<()> {
        self.ensure_writable()?;
        create_table(&self.store, DEFAULT_DB, &def)
    }

    /// 删表（目录进 trash，`gc` 后真正释放）。
    pub fn drop_table(&self, table: &str) -> Result<()> {
        self.ensure_writable()?;
        drop_table(&self.store, DEFAULT_DB, table)
    }

    /// 表 schema（生态工具高频路径——schema 协商 / LIMIT 0 的便捷替代；
    /// qsdb-sync F4）。内部 = LIMIT 0 契约同款 `SqlResult::Query { schema }`。
    pub fn table_schema(&self, table: &str) -> Result<arrow::datatypes::SchemaRef> {
        match self.sql(&format!("SELECT * FROM {table} LIMIT 0"))? {
            crate::sql::SqlResult::Query { schema, .. } => Ok(schema),
            crate::sql::SqlResult::Ok { message } => {
                Err(QsdbError::invalid(format!("not a table: {message}")))
            }
        }
    }

    /// 表定义。
    pub fn table_def(&self, table: &str) -> Result<Arc<TableDef>> {
        self.def(None, table)
    }

    /// ALTER TABLE ADD COLUMN（只加不删；列必须 nullable）。
    /// 旧 segment 读取时缺列补 NULL；宽松写接受缺尾列批。
    pub fn alter_add_column(&self, table: &str, column: crate::catalog::ColumnDef) -> Result<()> {
        self.ensure_writable()?;
        crate::catalog::alter_add_column(&self.store, crate::catalog::DEFAULT_DB, table, column)
    }

    /// 表名清单。
    pub fn tables(&self) -> Result<Vec<String>> {
        read_table_names(&self.store, DEFAULT_DB)
    }

    // -- 写 ------------------------------------------------------------------

    /// 批量追加：自动按分区列路由（跨分区自动拆分）。
    pub fn append(&self, table: &str, batches: &[RecordBatch]) -> Result<WriteResult> {
        self.ensure_writable()?;
        let def = self.def(None, table)?;
        let tdir = table_dir(self.store.root(), DEFAULT_DB, table);
        let mut total = WriteResult::default();
        for batch in batches {
            for (values, data) in split_by_partition(batch, &def)? {
                total = merge_write(
                    total,
                    write::append_partition(
                        &self.store,
                        self.staging(),
                        &tdir,
                        &def,
                        &values,
                        &[data],
                    )?,
                );
            }
        }
        Ok(total)
    }

    /// 分区原子替换（幂等回写）；空输入 = 清空该分区。
    pub fn overwrite_partition(
        &self,
        table: &str,
        values: &[(&str, Literal)],
        batches: &[RecordBatch],
    ) -> Result<WriteResult> {
        self.ensure_writable()?;
        let def = self.def(None, table)?;
        if values.len() != def.partition_keys.len() {
            return Err(QsdbError::invalid(format!(
                "table has {} partition keys, got {} values",
                def.partition_keys.len(),
                values.len()
            )));
        }
        // 值按表分区键顺序对齐 + cast 到列类型
        let mut ordered: Vec<Literal> = Vec::with_capacity(values.len());
        for key in &def.partition_keys {
            let (_, raw) = values.iter().find(|(k, _)| k == key).ok_or_else(|| {
                QsdbError::invalid(format!("partition value for '{key}' missing"))
            })?;
            let col = def.column(key).unwrap();
            let v = if raw.logical_type().as_ref() == Some(&col.logical_type) {
                raw.clone()
            } else {
                crate::types::cast::cast_literal(raw, &col.logical_type).map_err(|e| {
                    QsdbError::type_err(format!("partition value for '{key}' incompatible: {e}"))
                })?
            };
            ordered.push(v);
        }
        let tdir = table_dir(self.store.root(), DEFAULT_DB, table);
        // 输入与 append 同约定：全列批；库内剥离分区列并校验与目标分区一致
        let mut data_batches: Vec<RecordBatch> = Vec::with_capacity(batches.len());
        for batch in batches {
            for (group_vals, data) in split_by_partition(batch, &def)? {
                let mismatch = group_vals.iter().zip(ordered.iter()).any(|(a, b)| {
                    !matches!(
                        a.partial_cmp_literal(b),
                        Ok(Some(std::cmp::Ordering::Equal))
                    )
                });
                if mismatch {
                    return Err(QsdbError::invalid(
                        "overwrite_partition input contains rows of other partitions",
                    ));
                }
                data_batches.push(data);
            }
        }
        write::overwrite_partition(
            &self.store,
            self.staging(),
            &tdir,
            &def,
            &ordered,
            &data_batches,
        )
    }

    // -- 读 ------------------------------------------------------------------

    /// 流式扫描。
    pub fn scan(&self, req: ScanRequest) -> Result<Scan> {
        self.scan_inner(req, crate::query::QueryContext::benign())
    }

    /// 带查询上下文的扫描（取消/超时在批边界生效）。
    pub fn scan_with_context(
        &self,
        req: ScanRequest,
        ctx: Arc<crate::query::QueryContext>,
    ) -> Result<Scan> {
        self.scan_inner(req, ctx)
    }

    fn scan_inner(&self, req: ScanRequest, ctx: Arc<crate::query::QueryContext>) -> Result<Scan> {
        let def = self.def(req.db.as_deref(), &req.table)?;
        let tdir = table_dir(
            self.store.root(),
            self.db_name(req.db.as_deref()),
            &req.table,
        );
        Scan::with_cache(
            &self.store,
            &tdir,
            def,
            req.projection,
            req.predicate,
            req.limit,
            Some(Arc::clone(&self.cache)),
            ctx,
        )
    }

    /// 外表扫描（read_parquet；exec_builder 调用）。
    pub(crate) fn external_scan(
        &self,
        def: Arc<TableDef>,
        files: Vec<crate::scan::FileLoc>,
        projection: Option<Vec<String>>,
        predicate: Option<Predicate>,
        ctx: Arc<crate::query::QueryContext>,
    ) -> Result<Scan> {
        let fc = self
            .file_cache
            .is_enabled()
            .then(|| Arc::clone(&self.file_cache));
        Scan::external(
            &self.store,
            def,
            files,
            projection,
            predicate,
            None,
            Some(Arc::clone(&self.cache)),
            fc,
            ctx,
        )
    }

    /// 分区级 DELETE：谓词必须**只引用分区键**（等值/范围/AND 组合）。
    /// 命中的分区原子清空（空 manifest 替换 + 旧文件 trash）。
    /// 非分区键谓词 → 报错（行级 DELETE 是独立大特性，见设计文档 M5）。
    pub fn delete_partitions_where(&self, table: &str, selection: Option<&str>) -> Result<u64> {
        self.ensure_writable()?;
        let def = self.def(None, table)?;
        let tdir = table_dir(self.store.root(), DEFAULT_DB, table);
        // 无 WHERE = 清全表（等价 TRUNCATE）
        let Some(sel) = selection else {
            return self.truncate_table(table);
        };
        // WHERE → 下推谓词（经 SELECT 包装走 binder + split_pushdown）
        let predicate =
            crate::sql::translate::predicate_from_where(table, sel, &|n| self.table_def(n))?;
        // 谓词必须只引用分区列
        let mut cols = std::collections::HashSet::new();
        predicate.referenced_columns(&mut cols);
        for c in &cols {
            if !def
                .partition_keys
                .iter()
                .any(|k| k.eq_ignore_ascii_case(c.as_str()))
            {
                return Err(QsdbError::unsupported(format!(
                    "DELETE WHERE references non-partition column '{c}' — only partition-key                      predicates are supported (partition-granular); row-level DELETE is a                      separate feature"
                )));
            }
        }
        // 逐分区裁剪 + 清空
        let parts = crate::scan::list_partitions(&self.store, &tdir, def.partition_keys.len());
        let mut cleared = 0u64;
        for (_pdir, dir_vals) in parts {
            let mut pvals: HashMap<String, Literal> = HashMap::new();
            let mut ok = true;
            for (k, raw) in &dir_vals {
                match def.column(k) {
                    Some(c) => {
                        match crate::partition::parse_partition_value(raw, &c.logical_type) {
                            Ok(v) => {
                                pvals.insert(k.clone(), v);
                            }
                            Err(_) => ok = false,
                        }
                    }
                    None => ok = false,
                }
            }
            if !ok {
                continue;
            }
            let lookup: HashMap<&str, &Literal> =
                pvals.iter().map(|(k, v)| (k.as_str(), v)).collect();
            // 无法确定裁剪（Unknown）→ 保守清空？不——DELETE 语义要求确定性：
            // False → 跳过；True/Unknown → 清空（保守删除）
            if crate::scan::eval_partition_predicate(&predicate, &lookup) == crate::scan::Tri::False
            {
                continue;
            }
            let cleared_rows = self.clear_partition(&tdir, &def, &pvals)?;
            cleared += cleared_rows;
        }
        Ok(cleared)
    }

    /// 清空单个分区（本地：空 manifest 原子替换 + 旧文件 trash；
    /// 远端：OCC 空替换，旧对象由引用 GC 回收）。返回清除行数。
    fn clear_partition(
        &self,
        tdir: &std::path::Path,
        def: &Arc<TableDef>,
        values: &HashMap<String, Literal>,
    ) -> Result<u64> {
        let ordered: Vec<Literal> = def
            .partition_keys
            .iter()
            .map(|k| values.get(k).cloned().unwrap_or(Literal::Null))
            .collect();
        let dir = tdir.join(crate::partition::partition_dir(
            &def.partition_keys,
            &ordered,
        )?);
        if !self.store.is_local() {
            #[cfg(feature = "remote")]
            return self.clear_partition_remote(&dir, def, &ordered);
            #[cfg(not(feature = "remote"))]
            return Err(QsdbError::unsupported(
                "remote store requires an s3/hdfs build",
            ));
        }
        let _guard = crate::fsutil::PartitionLock::acquire(&dir.join(".lock"))?;
        let Some(old) = crate::manifest::PartitionManifest::read(&self.store, &dir)? else {
            return Ok(0);
        };
        let rows = old.total_rows();
        let mut empty = crate::manifest::PartitionManifest::new(vec![], def.schema_fingerprint());
        // generation 继承（F2 修复，qsdb-sync 反馈）：与远端路径一致——
        // 清空是「提交」不是「重置」，单调递增是同步工具的增量判据契约
        empty.generation = old.generation;
        empty.partition_values = def
            .partition_keys
            .iter()
            .cloned()
            .zip(ordered.iter().cloned())
            .collect();
        empty.segments = Vec::new();
        empty.commit(&self.store, &dir)?;
        let rel = dir
            .strip_prefix(tdir)
            .unwrap_or(&dir)
            .to_string_lossy()
            .replace('\\', "/");
        let _ = crate::index::PartitionsIndex::upsert(&self.store, tdir, &rel, &empty);
        // 旧文件留原地（延迟回收语义，保护在途扫描；GC 按 manifest 年龄清理）
        let _ = old;
        Ok(rows)
    }

    /// 清空全表所有分区（保留表定义）。
    pub fn truncate_table(&self, table: &str) -> Result<u64> {
        self.ensure_writable()?;
        let def = self.def(None, table)?;
        let tdir = table_dir(self.store.root(), DEFAULT_DB, table);
        // F18 修复：无分区表没有 manifest 与分区目录，list_partitions(depth=0)
        // 返回空 → 原实现空循环后返回 Ok(0) 却不清数据（静默无效）。
        // 无分区表的 segment 直接位于表目录下，走专门清空路径。
        if def.partition_keys.is_empty() {
            return self.clear_unpartitioned(&tdir);
        }
        let parts = crate::scan::list_partitions(&self.store, &tdir, def.partition_keys.len());
        let mut cleared = 0u64;
        for (pdir, _) in parts {
            // 从 manifest 提取分区值
            if let Some(m) = crate::manifest::PartitionManifest::read(&self.store, &pdir)? {
                let vals: HashMap<String, Literal> = m.partition_values.iter().cloned().collect();
                if !vals.is_empty() {
                    cleared += self.clear_partition(&tdir, &def, &vals)?;
                }
            }
        }
        Ok(cleared)
    }

    /// 清空**无分区表**：无 manifest/分区目录，segment 直接位于表目录下。
    /// 表目录锁内逐个移入 trash（延迟回收语义与分区路径一致）。
    fn clear_unpartitioned(&self, tdir: &std::path::Path) -> Result<u64> {
        if !self.store.is_local() {
            #[cfg(feature = "remote")]
            {
                return Err(QsdbError::unsupported(
                    "TRUNCATE on unpartitioned remote table not supported yet",
                ));
            }
            #[cfg(not(feature = "remote"))]
            {
                return Err(QsdbError::unsupported(
                    "remote store requires an s3/hdfs build",
                ));
            }
        }
        let _guard = crate::fsutil::PartitionLock::acquire(&tdir.join(".lock"))?;
        let mut rows = 0u64;
        for entry in self.store.read_dir(tdir).unwrap_or_default() {
            if !entry.name.ends_with(".parquet") {
                continue;
            }
            rows += crate::scan::parquet_row_count(&entry.path).unwrap_or(0);
            crate::fsutil::move_to_trash(self.store.root(), &entry.path)?;
        }
        Ok(rows)
    }

    // -- SQL（M2：SQL-0/1）----------------------------------------------------

    /// 新建查询上下文（server 会话层用；嵌入用户也可自建以获得取消/超时能力）。
    /// 超时/内存上限取**运行期配置**（可热更）；显式 timeout_ms 优先。
    pub fn new_query_context(&self, timeout_ms: Option<u64>) -> Arc<crate::query::QueryContext> {
        self.new_query_context_with_id(None, timeout_ms)
    }

    /// 同上，但允许**指定 query id**（`None` = 服务端生成 `q` + uuid v7）。
    ///
    /// 用途（契约 J11）：调用方在执行前就知道 id，才能把它与自己的请求标识
    /// （如 JDBC `Statement`）绑定，从而**零扫描、零误伤**地取消——否则只能按
    /// SQL 文本反查运行中查询，同 SQL 并发会一并取消。id 原样使用（不加前缀）
    /// 以便调用方直接按自己发出的值匹配。
    ///
    /// **唯一性由调用方负责**：内核不查重，Server 层在"已被运行中查询占用"时
    /// 回退为服务端生成，并以响应头回显**实际**使用的 id。
    pub fn new_query_context_with_id(
        &self,
        query_id: Option<String>,
        timeout_ms: Option<u64>,
    ) -> Arc<crate::query::QueryContext> {
        // 每查询配额（本查询自己的上限）+ 全局预算（本句柄所有查询共享）+ 溢出目录
        crate::query::QueryContext::new_full(
            query_id.unwrap_or_else(|| format!("q{}", uuid::Uuid::now_v7().simple())),
            timeout_ms.or_else(|| self.runtime.query_timeout_ms()),
            self.runtime.query_memory_limit_bytes(),
            Some(Arc::clone(&self.pool)),
            self.spill_dir.clone(),
        )
    }

    /// 全局内存账本（观测/运维：`exec_used` / `cache_used` / `budget`）。
    /// 溢出目录根（`None` = 系统临时目录）。
    pub fn spill_dir(&self) -> Option<&std::path::Path> {
        self.spill_dir.as_deref()
    }

    pub fn memory_pool(&self) -> Arc<crate::memory::MemoryPool> {
        Arc::clone(&self.pool)
    }

    /// 运行期配置（热更）：`Arc` 引用可直接调 setter。
    pub fn runtime(&self) -> Arc<QsdbRuntime> {
        Arc::clone(&self.runtime)
    }

    /// 启动期选项快照（只读；改动需重开句柄）。
    pub fn options(&self) -> &QsdbOptions {
        &self.opts
    }

    /// 执行单条 SQL（DDL/DML/SELECT）——panic 隔离 + 默认超时/内存上限。
    pub fn sql(&self, sql: &str) -> Result<crate::sql::SqlResult> {
        let ctx = self.new_query_context(None);
        self.sql_with_context(sql, &ctx)
    }

    /// 带角色闸门的 SQL 执行（server 认证后调用；嵌入方宿主可选）。
    /// 语句类型 × 角色矩阵在 parse 后执行前检查（见 sql::check_role_permission）。
    pub fn sql_with_role(
        &self,
        sql: &str,
        role: crate::users::Role,
    ) -> Result<crate::sql::SqlResult> {
        let ctx = self.new_query_context(None);
        crate::sql::execute_sql_with_role(self, sql, role, &ctx)
    }

    /// 带角色 + 上下文（server 用，可取消/超时）。
    pub fn sql_with_role_and_context(
        &self,
        sql: &str,
        role: crate::users::Role,
        ctx: &Arc<crate::query::QueryContext>,
    ) -> Result<crate::sql::SqlResult> {
        crate::sql::execute_sql_with_role(self, sql, role, ctx)
    }

    /// 带外部上下文执行（可取消/超时/记账）。
    pub fn sql_with_context(
        &self,
        sql: &str,
        ctx: &Arc<crate::query::QueryContext>,
    ) -> Result<crate::sql::SqlResult> {
        crate::sql::execute_sql_ctx(self, sql, ctx)
    }

    /// 便捷：SELECT 物化执行（错误路径直接返回）。
    pub fn sql_collect(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        match crate::sql::execute_sql(self, sql)? {
            crate::sql::SqlResult::Query { batches, .. } => Ok(batches),
            crate::sql::SqlResult::Ok { message } => {
                Err(QsdbError::invalid(format!("not a query: {message}")))
            }
        }
    }

    /// 便捷：扫描并物化（调试/小结果集用）。
    pub fn scan_collect(&self, req: ScanRequest) -> Result<Vec<RecordBatch>> {
        let mut out = Vec::new();
        for b in self.scan(req)? {
            out.push(b?);
        }
        Ok(out)
    }

    // -- 运维 ----------------------------------------------------------------

    /// trash 清理 + 孤儿数据文件清扫。
    ///
    /// 本地：孤儿 = 分区目录内不被 manifest 引用且早于 `orphan_grace_ms` 的
    /// `seg_*.parquet` / `*.tmp`。
    /// 远端（§12 引用扫描式 GC）：list 表前缀全部对象 → 读分区 manifest（真值）
    /// → 未被引用且 mtime 超过安全期（`orphan_grace_ms`，覆盖在途上传与
    /// OCC 重试窗口）→ delete。安全期内的孤儿绝不删。
    pub fn gc(&self) -> Result<(u64, u64)> {
        self.ensure_writable()?;
        #[cfg(feature = "remote")]
        if !self.store.is_local() {
            return self.gc_remote();
        }
        let (purged, mut freed) = purge_trash(&self.root, self.opts.trash_grace_ms)?;
        let orphan_grace = self.opts.trash_grace_ms;
        let tables = self.tables()?;
        for table in &tables {
            let Some(def) = self.def(None, table).ok() else {
                continue;
            };
            let tdir = table_dir(self.store.root(), DEFAULT_DB, table);
            for (pdir, _) in
                crate::scan::list_partitions(&self.store, &tdir, def.partition_keys.len())
            {
                let Some(manifest) = crate::manifest::PartitionManifest::read(&self.store, &pdir)?
                else {
                    continue;
                };
                // 安全期锚点 = 替换 manifest 的提交时间（mtime）：刚提交的分区
                // 跳过孤儿清理——按旧快照规划的在途扫描需要旧段在窗口内
                // 仍可打开（延迟回收语义，soak 实测竞争的修复）
                let manifest_age_ok = self
                    .store
                    .stat(&crate::manifest::PartitionManifest::manifest_path(&pdir))
                    .ok()
                    .flatten()
                    .map(|md| crate::fsutil::now_ms().saturating_sub(md.mtime_ms) >= orphan_grace)
                    .unwrap_or(true);
                if !manifest_age_ok {
                    continue;
                }
                let live: std::collections::HashSet<String> = manifest
                    .segments
                    .iter()
                    .map(|s| s.file_name.clone())
                    .collect();
                let Ok(entries) = self.store.read_dir(&pdir) else {
                    continue;
                };
                for entry in entries {
                    let name = entry.name;
                    let is_data = name.starts_with("seg_") && name.ends_with(".parquet");
                    let is_tmp = name.ends_with(".tmp");
                    if !is_data && !is_tmp {
                        continue;
                    }
                    if live.contains(name.as_str()) {
                        continue;
                    }
                    // mtime 静默期，防止误清正在写的文件
                    let fresh = self
                        .store
                        .stat(&entry.path)
                        .ok()
                        .flatten()
                        .map(|md| {
                            crate::fsutil::now_ms().saturating_sub(md.mtime_ms) < orphan_grace
                        })
                        .unwrap_or(true);
                    if fresh {
                        continue;
                    }
                    let p = entry.path;
                    freed += dir_size(&p);
                    let _ = crate::fsutil::move_to_trash(&self.root, &p);
                }
            }
        }
        Ok((purged, freed))
    }

    /// 远端清空分区：OCC 空替换（重放继承 generation）；旧对象由引用 GC 回收。
    #[cfg(feature = "remote")]
    fn clear_partition_remote(
        &self,
        dir: &std::path::Path,
        def: &Arc<TableDef>,
        ordered: &[Literal],
    ) -> Result<u64> {
        let mut rows = 0u64;
        let mpath = crate::manifest::PartitionManifest::manifest_path(dir);
        crate::remote::with_partition_mutex(dir, || {
            crate::remote::occ_commit(&self.store, &mpath, |cur| {
                let mut m = cur.unwrap_or_else(|| {
                    crate::manifest::PartitionManifest::new(vec![], def.schema_fingerprint())
                });
                rows = m.total_rows();
                m.partition_values = def
                    .partition_keys
                    .iter()
                    .cloned()
                    .zip(ordered.iter().cloned())
                    .collect();
                m.segments = Vec::new();
                Ok(m)
            })
        })?;
        Ok(rows)
    }

    /// 远端引用扫描式 GC（§12）：逐表 list 全部对象 → manifest 真值比对 →
    /// 未引用且超安全期的 seg 对象删除。
    #[cfg(feature = "remote")]
    fn gc_remote(&self) -> Result<(u64, u64)> {
        let grace = self.opts.trash_grace_ms;
        let now = crate::fsutil::now_ms();
        let mut freed = 0u64;
        let tables = self.tables()?;
        for table in &tables {
            let Some(def) = self.def(None, table).ok() else {
                continue;
            };
            let tdir = table_dir(self.store.root(), DEFAULT_DB, table);
            let objects = self.store.list_all(&tdir)?;
            // manifest 真值：引用集（manifest 自身与 table.json/_index 恒保留）
            let mut referenced: std::collections::HashSet<String> =
                std::collections::HashSet::new();
            for obj in &objects {
                let name = obj.key.rsplit('/').next().unwrap_or_default();
                if name == crate::manifest::MANIFEST_FILE {
                    if let Some(bytes) = self.store.read(&self.store.root().join(&obj.key))? {
                        if let Ok(m) =
                            postcard::from_bytes::<crate::manifest::PartitionManifest>(&bytes)
                        {
                            for seg in &m.segments {
                                if let Some(parent) = obj.key.rsplit_once('/') {
                                    referenced.insert(format!("{}/{}", parent.0, seg.file_name));
                                }
                            }
                        }
                    }
                }
            }
            for obj in objects {
                let name = obj.key.rsplit('/').next().unwrap_or_default();
                let is_seg = name.starts_with("seg_") && name.ends_with(".parquet");
                if !is_seg || referenced.contains(&obj.key) {
                    continue;
                }
                // 安全期：覆盖在途上传与 OCC 重试窗口
                if now.saturating_sub(obj.mtime_ms) < grace {
                    continue;
                }
                let _ = def.as_ref();
                if self
                    .store
                    .delete_object(&self.store.root().join(&obj.key))
                    .is_ok()
                {
                    freed += obj.size;
                }
            }
        }
        Ok((0, freed))
    }

    /// 库统计（表数/总字节；本地含 trash，远端 = list 求和）。
    pub fn stats(&self) -> Result<(usize, u64)> {
        let tables = self.tables()?;
        let mut bytes = 0u64;
        for t in &tables {
            let tdir = table_dir(self.store.root(), DEFAULT_DB, t);
            if self.store.is_local() {
                bytes += dir_size(&tdir);
            } else {
                #[cfg(feature = "remote")]
                {
                    bytes += self
                        .store
                        .list_all(&tdir)?
                        .iter()
                        .map(|o| o.size)
                        .sum::<u64>();
                }
                #[cfg(not(feature = "remote"))]
                {
                    let _ = tdir;
                    return Err(QsdbError::unsupported(
                        "remote store requires an s3/hdfs build",
                    ));
                }
            }
        }
        if self.store.is_local() {
            bytes += dir_size(&self.root.join("_trash"));
        }
        Ok((tables.len(), bytes))
    }

    /// 表级 compaction（小文件合并；详见 `compact` 模块）。
    pub fn compact_table(&self, table: &str) -> Result<crate::compact::CompactResult> {
        self.ensure_writable()?;
        let def = self.def(None, table)?;
        let tdir = table_dir(self.store.root(), DEFAULT_DB, table);
        crate::compact::compact_table(
            &self.store,
            Some(&self.file_cache),
            &tdir,
            &def,
            &crate::compact::CompactOptions::default(),
        )
    }

    /// 带选项 compaction。
    pub fn compact_table_with(
        &self,
        table: &str,
        opts: crate::compact::CompactOptions,
    ) -> Result<crate::compact::CompactResult> {
        self.ensure_writable()?;
        let def = self.def(None, table)?;
        let tdir = table_dir(self.store.root(), DEFAULT_DB, table);
        crate::compact::compact_table(&self.store, Some(&self.file_cache), &tdir, &def, &opts)
    }

    /// 元数据应答：按谓词裁剪分区后，从 manifest 统计直接合成结果（不打开 parquet）。
    ///
    /// 当前支持的聚合：count(*)（= 各分区 row_count 之和）。
    /// 谓词仅支持分区列等值/范围形态（非分区列谓词返回 None，走正常扫描）。
    pub fn scan_metadata_count(
        &self,
        table: &str,
        predicate: Option<&Predicate>,
    ) -> Result<Option<u64>> {
        let def = self.def(None, table)?;
        let tdir = table_dir(self.store.root(), DEFAULT_DB, table);
        if def.partition_keys.is_empty() {
            // 无分区表：累加各 segment 的 parquet footer num_rows（零解码）
            let mut total = 0u64;
            for entry in self.store.read_dir(&tdir).unwrap_or_default() {
                if !entry.name.ends_with(".parquet") {
                    continue;
                }
                total += crate::scan::parquet_row_count(&entry.path)?;
            }
            return Ok(Some(total));
        }
        // ★ idx 快路径：partitions.idx 已含全分区 manifest 快照（index.rs 模块
        // 注释即为此动机——逐分区 open+read 在 Windows ~0.2ms/文件 → 万分区 2s+）。
        // 单文件读替代 N 次文件读：实测 16 分区 25ms → 1ms 量级。
        // ★ F19b：idx 是「最终一致」的——写者顺序是 commit manifest（建分区目录）
        // → 索引锁内 upsert idx，两步间有毫秒窗口。读者若恰在此窗口读 idx 会
        // 拿到陈旧分区集（实测 e2e 中 count 首调陈旧、紧接的次调正确）。
        // 防护：先用 readdir 校验分区数（文件系统真值），一致才信 idx；
        // 不一致则落到下方慢路径逐分区读。代价仅一次 readdir，且慢路径本就要
        // readdir，故最坏情况不比原实现差。
        if let Some(idx) =
            crate::index::PartitionsIndex::read_fresh(&self.store, &tdir, def.partition_keys.len())
        {
            let mut total = 0u64;
            let mut any = false;
            for (_dir, m) in &idx.entries {
                if let Some(pred) = predicate {
                    let lookup: HashMap<&str, &Literal> = m
                        .partition_values
                        .iter()
                        .map(|(k, v)| (k.as_str(), v))
                        .collect();
                    if crate::scan::eval_partition_predicate(pred, &lookup)
                        == crate::scan::Tri::False
                    {
                        continue;
                    }
                }
                any = true;
                total += m.total_rows();
            }
            return Ok(Some(if any { total } else { 0 }));
        }
        // 慢路径 fallback（idx 缺失/损坏：乐观回退逐分区读）
        let depth = def.partition_keys.len();
        let parts = crate::scan::list_partitions(&self.store, &tdir, depth);
        let mut total = 0u64;
        let mut any = false;
        for (pdir, dir_vals) in parts {
            let mut pvals: HashMap<String, Literal> = HashMap::new();
            let mut ok = true;
            for (k, raw) in &dir_vals {
                match def.column(k) {
                    Some(c) => {
                        match crate::partition::parse_partition_value(raw, &c.logical_type) {
                            Ok(v) => {
                                pvals.insert(k.clone(), v);
                            }
                            Err(_) => {
                                ok = false;
                                break;
                            }
                        }
                    }
                    None => {
                        ok = false;
                        break;
                    }
                }
            }
            if !ok {
                continue;
            }
            // 分区裁剪
            if let Some(pred) = predicate {
                let lookup: HashMap<&str, &Literal> =
                    pvals.iter().map(|(k, v)| (k.as_str(), v)).collect();
                if crate::scan::eval_partition_predicate(pred, &lookup) == crate::scan::Tri::False {
                    continue;
                }
            }
            let Some(m) = crate::manifest::PartitionManifest::read(&self.store, &pdir)? else {
                continue;
            };
            any = true;
            total += m.total_rows();
        }
        if any { Ok(Some(total)) } else { Ok(Some(0)) }
    }

    /// 上传全部 staged 数据（M6-4）：逐对 PUT segment → OCC 提交（幂等：
    /// 同名去重 + uuid7 时间序重放）→ 删除已传对。崩溃残留对在下次调用补传。
    /// 返回 (上传对数, 行数)。server 由定时器驱动；嵌入方按需调用。
    pub fn flush_staged(&self) -> Result<(u64, u64)> {
        #[cfg(not(feature = "remote"))]
        {
            Ok((0, 0))
        }
        #[cfg(feature = "remote")]
        {
            let Some(st) = &self.staging else {
                return Ok((0, 0));
            };
            let _guard = st.lock();
            let mut uploaded = 0u64;
            let mut rows = 0u64;
            for (rec, parquet) in st.pending() {
                // 表已删：暂存数据无主，整表清理
                let Ok(def) = self.def(None, &rec.table) else {
                    st.remove_table(&rec.table);
                    continue;
                };
                let bytes = std::fs::read(&parquet).map_err(|e| {
                    QsdbError::meta(format!("read staged {}: {e}", parquet.display()))
                })?;
                let dir =
                    table_dir(self.store.root(), DEFAULT_DB, &rec.table).join(&rec.partition_rel);
                self.store
                    .put_object(&dir.join(&rec.entry.file_name), &bytes)?;
                if rec.overwrite {
                    crate::write::occ_replace_entries(
                        &self.store,
                        &dir,
                        &def,
                        &rec.partition_values,
                        std::slice::from_ref(&rec.entry),
                    )?;
                } else {
                    crate::write::occ_append_entry(
                        &self.store,
                        &dir,
                        &def,
                        &rec.partition_values,
                        &rec.entry,
                    )?;
                }
                st.remove_pair(&rec, &parquet);
                uploaded += 1;
                rows += rec.entry.row_count;
            }
            Ok((uploaded, rows))
        }
    }

    /// staging 待传对数（诊断：server /stats 与运维）。
    pub fn staged_pending(&self) -> usize {
        #[cfg(feature = "remote")]
        if let Some(st) = &self.staging {
            return st.pending().len();
        }
        0
    }

    /// 热缓存状态：(entries, bytes, hits, misses)。
    pub fn cache_stats(&self) -> (usize, usize, u64, u64) {
        self.cache.stats()
    }

    /// 热缓存累计被挤兑/驱逐的字节（观测：内存压力是否在拿缓存换执行内存）。
    pub fn cache_evicted_bytes(&self) -> u64 {
        self.cache.evicted_bytes()
    }

    /// 清空热缓存。
    pub fn cache_clear(&self) {
        self.cache.clear();
    }

    /// 热缓存预算（字节；0 = 关闭）。
    pub fn cache_budget(&self) -> usize {
        self.cache.budget()
    }

    /// 分区统计（qsdb-sync 轮询契约）：每分区 (名, generation, 行数, 段数)。
    /// 优先读表级索引 `partitions.idx`（单文件快路径，万级分区 <50ms）；
    /// 缺失/损坏回退 list + 逐分区 manifest。generation 单调——同步工具
    /// 以此判断增量，见 docs/主备与qsdb-sync设计.md §3.2。
    pub fn partition_stats(&self, table: &str) -> Result<Vec<PartitionStat>> {
        let def = self.def(None, table)?;
        let tdir = table_dir(self.store.root(), DEFAULT_DB, table);
        if let Some(idx) =
            crate::index::PartitionsIndex::read_fresh(&self.store, &tdir, def.partition_keys.len())
        {
            return Ok(idx
                .entries
                .into_iter()
                .map(|(name, m)| PartitionStat {
                    name,
                    generation: m.generation,
                    rows: m.total_rows(),
                    segments: m.segments.len(),
                })
                .collect());
        }
        let mut out = Vec::new();
        for (pdir, _) in crate::scan::list_partitions(&self.store, &tdir, def.partition_keys.len())
        {
            let name = pdir
                .strip_prefix(&tdir)
                .unwrap_or(&pdir)
                .to_string_lossy()
                .replace('\\', "/");
            if let Some(m) = crate::manifest::PartitionManifest::read(&self.store, &pdir)? {
                out.push(PartitionStat {
                    name,
                    generation: m.generation,
                    rows: m.total_rows(),
                    segments: m.segments.len(),
                });
            }
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }

    /// 强制重建表级分区索引（`partitions.idx`）。
    ///
    /// 场景：索引损坏、跨进程写入造成 stale、或旧版本目录首次启用索引。
    pub fn refresh_index(&self, table: &str) -> Result<()> {
        self.ensure_writable()?;
        let def = self.def(None, table)?;
        let tdir = table_dir(self.store.root(), DEFAULT_DB, table);
        crate::index::PartitionsIndex::rebuild(&self.store, &tdir, def.partition_keys.len())?;
        Ok(())
    }
}

fn merge_write(a: WriteResult, b: WriteResult) -> WriteResult {
    WriteResult {
        rows: a.rows + b.rows,
        bytes: a.bytes + b.bytes,
        segments: a.segments + b.segments,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::ColumnDef;
    use crate::types::LogicalType;
    use arrow::array::{Date32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    fn col(name: &str, ty: LogicalType) -> ColumnDef {
        ColumnDef {
            name: name.into(),
            logical_type: ty,
            nullable: true,
            check: None,
            default: None,
            comment: None,
        }
    }

    fn tmp_root(tag: &str) -> PathBuf {
        let d = std::env::temp_dir()
            .join("qsdb_api_test")
            .join(format!("{tag}_{}", uuid::Uuid::now_v7().simple()));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn events_def() -> TableDef {
        TableDef::new(
            "events",
            vec![
                col("dt", LogicalType::Date),
                col("tenant", LogicalType::Varchar { max_chars: None }),
                col("amount", LogicalType::Int64),
            ],
            vec!["dt".into()],
        )
        .unwrap()
    }

    fn batch(days: Vec<i32>, tenants: Vec<&str>, amounts: Vec<i64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("dt", DataType::Date32, true),
            Field::new("tenant", DataType::Utf8, true),
            Field::new("amount", DataType::Int64, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Date32Array::from(days)),
                Arc::new(StringArray::from(tenants)),
                Arc::new(Int64Array::from(amounts)),
            ],
        )
        .unwrap()
    }

    #[test]
    fn end_to_end_append_scan() {
        let root = tmp_root("e2e");
        let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
        db.create_table(events_def()).unwrap();
        db.append(
            "events",
            &[batch(vec![1, 1, 2], vec!["a", "b", "a"], vec![10, 20, 30])],
        )
        .unwrap();

        // 全表扫描：分区列重建
        let batches = db
            .scan_collect(ScanRequest {
                table: "events".into(),
                ..Default::default()
            })
            .unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 3);
        assert_eq!(batches[0].num_columns(), 3);

        // 分区裁剪：只取 dt=1970-01-02
        let batches = db
            .scan_collect(ScanRequest {
                table: "events".into(),
                predicate: Some(Predicate::eq("dt", Literal::Date(1))),
                ..Default::default()
            })
            .unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 2);

        // 行级过滤 + 投影
        let batches = db
            .scan_collect(ScanRequest {
                table: "events".into(),
                projection: Some(vec!["tenant".into()]),
                predicate: Some(Predicate::gt("amount", Literal::Int64(15))),
                ..Default::default()
            })
            .unwrap();
        let got: Vec<String> = batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
                    .map(|v| v.unwrap().to_string())
                    .collect::<Vec<_>>()
            })
            .collect();
        assert_eq!(got, vec!["b".to_string(), "a".to_string()]);

        // limit
        let batches = db
            .scan_collect(ScanRequest {
                table: "events".into(),
                limit: Some(1),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn overwrite_and_gc() {
        let root = tmp_root("ow");
        let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
        db.create_table(events_def()).unwrap();
        db.append("events", &[batch(vec![1], vec!["a"], vec![1])])
            .unwrap();
        db.overwrite_partition(
            "events",
            &[("dt", Literal::Date(1))],
            &[batch(vec![1, 1], vec!["x", "y"], vec![5, 6])],
        )
        .unwrap();
        let batches = db
            .scan_collect(ScanRequest {
                table: "events".into(),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);

        // 孤儿清理（trash 静默期 0 + orphan grace 0 → 全清）
        let opts = QsdbOptions {
            trash_grace_ms: 0,
            scan_cache_bytes: 0,
            ..Default::default()
        };
        let db2 = Qsdb::open(&root, opts).unwrap();
        db2.gc().unwrap();
        // 数据仍完整
        let batches = db
            .scan_collect(ScanRequest {
                table: "events".into(),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
        // drop 表 → trash → gc 释放
        db.drop_table("events").unwrap();
        assert!(db.tables().unwrap().is_empty());
        db2.gc().unwrap();
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn power_loss_durability_smoke() {
        // 掉电安全档：写入路径全 fsync 后 rename——功能面与默认档一致
        let root = tmp_root("dur");
        let db = Qsdb::open(
            &root,
            QsdbOptions {
                durability: Durability::PowerLoss,
                ..QsdbOptions::default()
            },
        )
        .unwrap();
        db.create_table(events_def()).unwrap();
        db.append("events", &[batch(vec![1, 2], vec!["a", "b"], vec![10, 20])])
            .unwrap();
        db.overwrite_partition(
            "events",
            &[("dt", Literal::Date(1))],
            &[batch(vec![1], vec!["x"], vec![5])],
        )
        .unwrap();
        let batches = db
            .scan_collect(ScanRequest {
                table: "events".into(),
                ..Default::default()
            })
            .unwrap();
        // append 跨 dt=1/dt=2 两分区各 1 行；overwrite 只替换 dt=1（1 行）→ 共 2 行
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
        // tmp 残留由 GC 清（本用例只验证写读路径不因 fsync 报错）
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn underscore_prefixed_partition_key_is_visible() {
        // F22：分区列名以 `_` 开头（qsdb-sync 的 `__dt`）时，目录是 `__dt=2026-…`，
        // 而读路径按「不以 `_` 开头」筛分区目录 → 写者建了、读者全跳过：
        // 行在盘上而 count/scan 归零，且写路径毫无报错。前缀不再是分区判据。
        let root = tmp_root("f22");
        let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
        db.sql("CREATE TABLE t (__dt DATE, v BIGINT) PARTITIONED BY (__dt)")
            .unwrap();
        db.sql("INSERT INTO t VALUES ('2026-10-12', 1), ('2026-10-12', 2)")
            .unwrap();

        // ① 元数据面可见
        let parts = db.partition_stats("t").unwrap();
        assert_eq!(parts.len(), 1, "分区目录必须被认出来: {parts:?}");
        assert_eq!(parts[0].name, "__dt=2026-10-12");
        assert_eq!(parts[0].rows, 2);

        // ② 扫描面可见（这才是 F22 的病灶：数据在、读不到）
        let rows: usize = db
            .scan_collect(ScanRequest {
                table: "t".into(),
                ..Default::default()
            })
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(rows, 2);

        // ③ 谓词下推仍走分区裁剪（`__dt` 与普通分区列同权）
        let hit: usize = db
            .scan_collect(ScanRequest {
                table: "t".into(),
                predicate: Some(crate::scan::Predicate::Eq {
                    column: "__dt".into(),
                    value: crate::types::cast::cast_literal(
                        &crate::types::Literal::Utf8("2026-10-12".into()),
                        &LogicalType::Date,
                    )
                    .unwrap(),
                }),
                ..Default::default()
            })
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(hit, 2);

        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn read_only_rejects_writes() {
        let root = tmp_root("ro");
        // 先以读写模式建表写数
        {
            let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
            db.create_table(events_def()).unwrap();
            db.append("events", &[batch(vec![1], vec!["a"], vec![10])])
                .unwrap();
        }
        // 只读打开：读正常，写/DDL/compact/gc 全拒
        let db = Qsdb::open(
            &root,
            QsdbOptions {
                read_only: true,
                ..QsdbOptions::default()
            },
        )
        .unwrap();
        assert!(db.read_only());
        let n: usize = db
            .scan_collect(ScanRequest {
                table: "events".into(),
                ..Default::default()
            })
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(n, 1);
        assert!(
            db.append("events", &[batch(vec![1], vec!["b"], vec![20])])
                .is_err()
        );
        assert!(
            db.sql("INSERT INTO events VALUES (DATE '1970-01-02','c',30)")
                .is_err()
        );
        assert!(db.sql("CREATE TABLE t2 (a BIGINT)").is_err());
        assert!(db.sql("TRUNCATE TABLE events").is_err());
        assert!(db.compact_table("events").is_err());
        assert!(db.gc().is_err());
        // partition_stats（读路径）可用
        let stats = db.partition_stats("events").unwrap();
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].generation, 1);
        assert_eq!(stats[0].rows, 1);
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn errors_not_found() {
        let root = tmp_root("nf");
        let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
        // 类别是机器可读的（上层据此选 TABLE_NOT_FOUND / COLUMN_NOT_FOUND，
        // 不再嗅探文案）
        assert!(matches!(
            db.append("nope", &[]),
            Err(ref e) if e.not_found_kind() == Some(crate::error::NotFoundKind::Table)
        ));
        assert!(
            db.scan(ScanRequest {
                table: "nope".into(),
                ..Default::default()
            })
            .is_err()
        );
        std::fs::remove_dir_all(&root).ok();
    }
}
