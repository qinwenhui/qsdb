//! `Qsdb` 门面：嵌入式 API（一等公民）。
//!
//! 句柄无状态（磁盘为真值 + mtime 缓存），`Send + Sync`，
//! 同目录可多实例/多进程共存（分区 flock 协调）。

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

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
}

impl Default for QsdbOptions {
    fn default() -> Self {
        Self {
            trash_grace_ms: 10 * 60 * 1000,
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

/// QSDB 句柄。
pub struct Qsdb {
    root: PathBuf,
    opts: QsdbOptions,
    defs: TableDefCache,
}

impl Qsdb {
    /// 打开（目录不存在则初始化为空库）。
    pub fn open(root: impl AsRef<Path>, opts: QsdbOptions) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(root.join("_catalog"))
            .map_err(|e| QsdbError::meta(format!("init db root failed: {e}")))?;
        fs::create_dir_all(db_dir(&root, DEFAULT_DB))
            .map_err(|e| QsdbError::meta(format!("init default db failed: {e}")))?;
        Ok(Self {
            root,
            opts,
            defs: TableDefCache::default(),
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
        let def = self.def(req.db.as_deref(), &req.table)?;
        let tdir = table_dir(&self.root, self.db_name(req.db.as_deref()), &req.table);
        Scan::new(
            &self.root,
            &tdir,
            def,
            req.projection,
            req.predicate,
            req.limit,
        )
    }

    // -- SQL（M2：SQL-0/1）----------------------------------------------------

    /// 执行单条 SQL（DDL/DML/SELECT）。
    pub fn sql(&self, sql: &str) -> Result<crate::sql::SqlResult> {
        crate::sql::execute_sql(self, sql)
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
        let opts = QsdbOptions { trash_grace_ms: 0 };
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
