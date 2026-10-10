//! `Qsdb` 门面：嵌入式 API（一等公民）。
//!
//! 句柄无状态（磁盘为真值 + mtime 缓存），`Send + Sync`，
//! 同目录可多实例/多进程共存（分区 flock 协调）。

use std::collections::HashMap;
use std::fs;
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
use crate::types::Literal;
use crate::write::{self, WriteResult};

/// 打开选项。
#[derive(Debug, Clone)]
pub struct QsdbOptions {
    /// trash 静默期（毫秒）；`gc()` 只清理更早的条目
    pub trash_grace_ms: u64,
    /// 热数据缓存预算（字节）：segment 级解码后 LRU 缓存。
    /// `0` = 关闭；默认 256MB。
    pub scan_cache_bytes: usize,
    /// 默认查询超时（毫秒）；None/0 = 不超时
    pub query_timeout_ms: Option<u64>,
    /// 默认单查询内存上限（字节）；None = 不限。超限查询被终止而非 OOM 全库。
    pub query_memory_limit_bytes: Option<u64>,
}

impl Default for QsdbOptions {
    fn default() -> Self {
        Self {
            trash_grace_ms: 10 * 60 * 1000,
            scan_cache_bytes: 256 * 1024 * 1024,
            query_timeout_ms: None,
            query_memory_limit_bytes: None,
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

/// QSDB 句柄。
pub struct Qsdb {
    root: PathBuf,
    opts: QsdbOptions,
    defs: TableDefCache,
    cache: Arc<crate::cache::SegmentCache>,
    runtime: Arc<QsdbRuntime>,
}

impl Qsdb {
    /// 打开（目录不存在则初始化为空库）。
    pub fn open(root: impl AsRef<Path>, opts: QsdbOptions) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(root.join("_catalog"))
            .map_err(|e| QsdbError::meta(format!("init db root failed: {e}")))?;
        fs::create_dir_all(db_dir(&root, DEFAULT_DB))
            .map_err(|e| QsdbError::meta(format!("init default db failed: {e}")))?;
        let cache = Arc::new(crate::cache::SegmentCache::new(opts.scan_cache_bytes));
        let runtime = Arc::new(QsdbRuntime::from_opts(&opts));
        Ok(Self {
            root,
            opts,
            defs: TableDefCache::default(),
            cache,
            runtime,
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    fn db_name<'a>(&self, db: Option<&'a str>) -> &'a str {
        db.unwrap_or(DEFAULT_DB)
    }

    fn def(&self, db: Option<&str>, table: &str) -> Result<Arc<TableDef>> {
        let db = self.db_name(db);
        let names = read_table_names(&self.root, db)?;
        if !names.iter().any(|n| n == table) {
            return Err(QsdbError::not_found(format!("table {db}.{table}")));
        }
        self.defs.get(&self.root, db, table)
    }

    // -- DDL ----------------------------------------------------------------

    /// 建表。
    pub fn create_table(&self, def: TableDef) -> Result<()> {
        create_table(&self.root, DEFAULT_DB, &def)
    }

    /// 删表（目录进 trash，`gc` 后真正释放）。
    pub fn drop_table(&self, table: &str) -> Result<()> {
        drop_table(&self.root, DEFAULT_DB, table)
    }

    /// 表定义。
    pub fn table_def(&self, table: &str) -> Result<Arc<TableDef>> {
        self.def(None, table)
    }

    /// 表名清单。
    pub fn tables(&self) -> Result<Vec<String>> {
        read_table_names(&self.root, DEFAULT_DB)
    }

    // -- 写 ------------------------------------------------------------------

    /// 批量追加：自动按分区列路由（跨分区自动拆分）。
    pub fn append(&self, table: &str, batches: &[RecordBatch]) -> Result<WriteResult> {
        let def = self.def(None, table)?;
        let tdir = table_dir(&self.root, DEFAULT_DB, table);
        let mut total = WriteResult::default();
        for batch in batches {
            for (values, data) in split_by_partition(batch, &def)? {
                total = merge_write(
                    total,
                    write::append_partition(&tdir, &def, &values, &[data])?,
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
        let tdir = table_dir(&self.root, DEFAULT_DB, table);
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
        write::overwrite_partition(&self.root, &tdir, &def, &ordered, &data_batches)
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
        let tdir = table_dir(&self.root, self.db_name(req.db.as_deref()), &req.table);
        Scan::with_cache(
            &self.root,
            &tdir,
            def,
            req.projection,
            req.predicate,
            req.limit,
            Some(Arc::clone(&self.cache)),
            ctx,
        )
    }

    /// 分区级 DELETE：谓词必须**只引用分区键**（等值/范围/AND 组合）。
    /// 命中的分区原子清空（空 manifest 替换 + 旧文件 trash）。
    /// 非分区键谓词 → 报错（行级 DELETE 是独立大特性，见设计文档 M5）。
    pub fn delete_partitions_where(&self, table: &str, selection: Option<&str>) -> Result<u64> {
        let def = self.def(None, table)?;
        let tdir = table_dir(&self.root, DEFAULT_DB, table);
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
        let parts = crate::scan::list_partitions(&tdir, def.partition_keys.len());
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

    /// 清空单个分区（空 manifest 原子替换 + 旧文件 trash）。返回清除行数。
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
        let _guard = crate::fsutil::PartitionLock::acquire(&dir.join(".lock"))?;
        let Some(old) = crate::manifest::PartitionManifest::read(&dir)? else {
            return Ok(0);
        };
        let rows = old.total_rows();
        let mut empty = crate::manifest::PartitionManifest::new(vec![], def.schema_fingerprint());
        empty.partition_values = def
            .partition_keys
            .iter()
            .cloned()
            .zip(ordered.iter().cloned())
            .collect();
        empty.segments = Vec::new();
        empty.commit(&dir)?;
        let rel = dir
            .strip_prefix(tdir)
            .unwrap_or(&dir)
            .to_string_lossy()
            .replace('\\', "/");
        let _ = crate::index::PartitionsIndex::upsert(tdir, &rel, &empty);
        for f in old.segment_paths(&dir) {
            let _ = crate::fsutil::move_to_trash(&self.root, &f);
        }
        Ok(rows)
    }

    /// 清空全表所有分区（保留表定义）。
    pub fn truncate_table(&self, table: &str) -> Result<u64> {
        let def = self.def(None, table)?;
        let tdir = table_dir(&self.root, DEFAULT_DB, table);
        let parts = crate::scan::list_partitions(&tdir, def.partition_keys.len());
        let mut cleared = 0u64;
        for (pdir, _) in parts {
            // 从 manifest 提取分区值
            if let Some(m) = crate::manifest::PartitionManifest::read(&pdir)? {
                let vals: HashMap<String, Literal> = m.partition_values.iter().cloned().collect();
                if !vals.is_empty() {
                    cleared += self.clear_partition(&tdir, &def, &vals)?;
                }
            }
        }
        Ok(cleared)
    }

    // -- SQL（M2：SQL-0/1）----------------------------------------------------

    /// 新建查询上下文（server 会话层用；嵌入用户也可自建以获得取消/超时能力）。
    /// 超时/内存上限取**运行期配置**（可热更）；显式 timeout_ms 优先。
    pub fn new_query_context(&self, timeout_ms: Option<u64>) -> Arc<crate::query::QueryContext> {
        crate::query::QueryContext::new(
            format!("q{}", uuid::Uuid::now_v7().simple()),
            timeout_ms.or_else(|| self.runtime.query_timeout_ms()),
            self.runtime.query_memory_limit_bytes(),
        )
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
    /// 孤儿 = 分区目录内不被 manifest 引用且早于 `orphan_grace_ms` 的
    /// `seg_*.parquet` / `*.tmp`。
    pub fn gc(&self) -> Result<(u64, u64)> {
        let (purged, mut freed) = purge_trash(&self.root, self.opts.trash_grace_ms)?;
        let orphan_grace = self.opts.trash_grace_ms;
        let tables = self.tables()?;
        for table in &tables {
            let Some(def) = self.def(None, table).ok() else {
                continue;
            };
            let tdir = table_dir(&self.root, DEFAULT_DB, table);
            for (pdir, _) in crate::scan::list_partitions(&tdir, def.partition_keys.len()) {
                let Some(manifest) = crate::manifest::PartitionManifest::read(&pdir)? else {
                    continue;
                };
                let live: std::collections::HashSet<String> = manifest
                    .segments
                    .iter()
                    .map(|s| s.file_name.clone())
                    .collect();
                let Ok(rd) = fs::read_dir(&pdir) else {
                    continue;
                };
                for entry in rd.flatten() {
                    let name = entry.file_name();
                    let name = name.to_string_lossy();
                    let is_data = name.starts_with("seg_") && name.ends_with(".parquet");
                    let is_tmp = name.ends_with(".tmp");
                    if !is_data && !is_tmp {
                        continue;
                    }
                    if live.contains(name.as_ref()) {
                        continue;
                    }
                    // mtime 静默期，防止误清正在写的文件
                    let fresh = entry
                        .metadata()
                        .ok()
                        .and_then(|m| m.modified().ok())
                        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                        .map(|d| d.as_millis() as u64)
                        .map(|mt| crate::fsutil::now_ms().saturating_sub(mt) < orphan_grace)
                        .unwrap_or(true);
                    if fresh {
                        continue;
                    }
                    let p = entry.path();
                    freed += dir_size(&p);
                    let _ = crate::fsutil::move_to_trash(&self.root, &p);
                }
            }
        }
        Ok((purged, freed))
    }

    /// 库统计（表数/总字节，含 trash）。
    pub fn stats(&self) -> Result<(usize, u64)> {
        let tables = self.tables()?;
        let mut bytes = 0u64;
        for t in &tables {
            bytes += dir_size(&table_dir(&self.root, DEFAULT_DB, t));
        }
        bytes += dir_size(&self.root.join("_trash"));
        Ok((tables.len(), bytes))
    }

    /// 表级 compaction（小文件合并；详见 `compact` 模块）。
    pub fn compact_table(&self, table: &str) -> Result<crate::compact::CompactResult> {
        let def = self.def(None, table)?;
        let tdir = table_dir(&self.root, DEFAULT_DB, table);
        crate::compact::compact_table(
            &self.root,
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
        let def = self.def(None, table)?;
        let tdir = table_dir(&self.root, DEFAULT_DB, table);
        crate::compact::compact_table(&self.root, &tdir, &def, &opts)
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
        if def.partition_keys.is_empty() {
            return Ok(None);
        }
        let tdir = table_dir(&self.root, DEFAULT_DB, table);
        let depth = def.partition_keys.len();
        let parts = crate::scan::list_partitions(&tdir, depth);
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
            let Some(m) = crate::manifest::PartitionManifest::read(&pdir)? else {
                continue;
            };
            any = true;
            total += m.total_rows();
        }
        if any { Ok(Some(total)) } else { Ok(Some(0)) }
    }

    /// 热缓存状态：(entries, bytes, hits, misses)。
    pub fn cache_stats(&self) -> (usize, usize, u64, u64) {
        self.cache.stats()
    }

    /// 清空热缓存。
    pub fn cache_clear(&self) {
        self.cache.clear();
    }

    /// 热缓存预算（字节；0 = 关闭）。
    pub fn cache_budget(&self) -> usize {
        self.cache.budget()
    }

    /// 强制重建表级分区索引（`partitions.idx`）。
    ///
    /// 场景：索引损坏、跨进程写入造成 stale、或旧版本目录首次启用索引。
    pub fn refresh_index(&self, table: &str) -> Result<()> {
        let def = self.def(None, table)?;
        let tdir = table_dir(&self.root, DEFAULT_DB, table);
        crate::index::PartitionsIndex::rebuild(&tdir, def.partition_keys.len())?;
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
            default: None,
            comment: None,
        }
    }

    fn tmp_root(tag: &str) -> PathBuf {
        let d = std::env::temp_dir()
            .join("qsdb_api_test")
            .join(format!("{tag}_{}", uuid::Uuid::now_v7().simple()));
        fs::create_dir_all(&d).unwrap();
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
        fs::remove_dir_all(&root).ok();
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
        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn errors_not_found() {
        let root = tmp_root("nf");
        let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
        assert!(matches!(
            db.append("nope", &[]),
            Err(QsdbError::NotFound(_))
        ));
        assert!(
            db.scan(ScanRequest {
                table: "nope".into(),
                ..Default::default()
            })
            .is_err()
        );
        fs::remove_dir_all(&root).ok();
    }
}
