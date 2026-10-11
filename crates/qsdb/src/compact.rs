//! Compaction：分区级小文件在线合并（设计文档 §13）。
//!
//! 分区锁内：读全部小 segments → 合并写单个大 parquet → manifest 原子替换
//! （generation 继承）→ 旧文件移 `_trash`。与读并发安全（manifest 快照语义），
//! 与写靠同一把分区锁串行。表级多分区 rayon 并行。

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::arrow_writer::ArrowWriter;

use crate::catalog::TableDef;
use crate::error::{QsdbError, Result};
use crate::fsutil::{PartitionLock, move_to_trash, now_ms};
use crate::index::PartitionsIndex;
use crate::manifest::{PartitionManifest, SegmentEntry};
use crate::scan::list_partitions;
use crate::stats;
use crate::store::Store;
use crate::write::{data_schema, writer_props};

/// Compaction 结果。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CompactResult {
    pub partitions_compacted: u64,
    pub segments_merged: u64,
    pub rows: u64,
    pub bytes_written: u64,
}

#[derive(Debug, Clone)]
pub struct CompactOptions {
    /// 分区 segment 数达到该值才合并
    pub min_segments: usize,
    /// 平均 segment 大于该值（字节）则跳过（已足够大）
    pub max_avg_segment_bytes: u64,
}

impl Default for CompactOptions {
    fn default() -> Self {
        Self {
            min_segments: 4,
            max_avg_segment_bytes: 64 * 1024 * 1024,
        }
    }
}

/// 表级 compaction：遍历分区，满足条件的合并。返回累计结果。
pub fn compact_table(
    store: &Store,
    file_cache: Option<&Arc<crate::filecache::FileCache>>,
    table_dir: &Path,
    def: &Arc<TableDef>,
    opts: &CompactOptions,
) -> Result<CompactResult> {
    use rayon::prelude::*;
    let partitions = list_partitions(store, table_dir, def.partition_keys.len());
    let results: Vec<Result<Option<SingleResult>>> = partitions
        .par_iter()
        .map(|(pdir, _)| compact_partition(store, file_cache, table_dir, def, pdir, opts))
        .collect();
    let mut total = CompactResult::default();
    for r in results {
        if let Some(single) = r? {
            total.partitions_compacted += 1;
            total.segments_merged += single.merged;
            total.rows += single.rows;
            total.bytes_written += single.bytes_written;
        }
    }
    Ok(total)
}

struct SingleResult {
    merged: u64,
    rows: u64,
    bytes_written: u64,
}

/// 单分区 compaction（不满足条件 → Ok(None)，不动任何文件）。
#[cfg_attr(not(feature = "remote"), allow(unused_variables))]
fn compact_partition(
    store: &Store,
    file_cache: Option<&Arc<crate::filecache::FileCache>>,
    table_dir: &Path,
    def: &Arc<TableDef>,
    pdir: &Path,
    opts: &CompactOptions,
) -> Result<Option<SingleResult>> {
    if !store.is_local() {
        #[cfg(feature = "remote")]
        return compact_partition_remote(store, file_cache, table_dir, def, pdir, opts);
        #[cfg(not(feature = "remote"))]
        return Err(QsdbError::unsupported("remote store requires feature 's3'"));
    }
    compact_partition_inner(store, table_dir, def, pdir, opts)
}

/// 本地 compaction（现状协议：flock + tmp+rename + trash + 索引）。
fn compact_partition_inner(
    store: &Store,
    table_dir: &Path,
    def: &Arc<TableDef>,
    pdir: &Path,
    opts: &CompactOptions,
) -> Result<Option<SingleResult>> {
    let Some(mut manifest) = PartitionManifest::read(store, pdir)? else {
        return Ok(None);
    };
    if manifest.segments.len() < opts.min_segments.max(2) {
        return Ok(None);
    }
    let avg = if manifest.segments.is_empty() {
        0
    } else {
        manifest.segments.iter().map(|s| s.byte_size).sum::<u64>() / manifest.segments.len() as u64
    };
    if avg >= opts.max_avg_segment_bytes {
        return Ok(None);
    }

    // 分区锁（写路径同一把锁，天然互斥）
    let _guard = PartitionLock::acquire(&pdir.join(".lock"))?;
    // 锁内重读（等待期间可能已被其他提交者变更）
    manifest = PartitionManifest::read(store, pdir)?.unwrap_or(manifest);
    if manifest.segments.len() < 2 {
        return Ok(None);
    }

    // 读全部 segments → 合并批（旧 segment 缺列补 NULL → 自然迁移到新 schema）
    let schema = data_schema(def);
    let want: Vec<(String, crate::types::LogicalType)> = def
        .data_columns()
        .iter()
        .map(|c| (c.name.clone(), c.logical_type.clone()))
        .collect();
    let mut batches: Vec<RecordBatch> = Vec::new();
    for seg in &manifest.segments {
        let path = pdir.join(&seg.file_name);
        let file = File::open(&path)
            .map_err(|e| QsdbError::meta(format!("open {} failed: {e}", path.display())))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| QsdbError::meta(format!("read parquet {}: {e}", path.display())))?;
        let reader = builder
            .with_batch_size(8192)
            .with_projection(parquet::arrow::ProjectionMask::all())
            .build()
            .map_err(|e| QsdbError::meta(format!("build reader failed: {e}")))?;
        for b in reader {
            let b = b.map_err(|e| QsdbError::meta(format!("read batch failed: {e}")))?;
            if b.num_rows() > 0 {
                let missing: Vec<(String, crate::types::LogicalType)> = want
                    .iter()
                    .filter(|(n, _)| b.schema().index_of(n).is_err())
                    .cloned()
                    .collect();
                let b = if missing.is_empty() {
                    b
                } else {
                    crate::scan::pad_missing_columns(b, &missing)?
                };
                batches.push(b);
            }
        }
    }
    let rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
    if rows == 0 {
        return Ok(None);
    }

    // 合并写新 segment
    let entry = write_merged(store, pdir, &batches, &schema, def)?;
    let old_files: Vec<std::path::PathBuf> = manifest
        .segments
        .iter()
        .map(|s| pdir.join(&s.file_name))
        .collect();
    let merged = manifest.segments.len() as u64;

    // 原子提交：单 segment manifest
    manifest.segments = vec![entry.clone()];
    manifest.commit(store, pdir)?;
    let rel = pdir
        .strip_prefix(table_dir)
        .unwrap_or(pdir)
        .to_string_lossy()
        .replace('\\', "/");
    let _ = PartitionsIndex::upsert(store, table_dir, &rel, &manifest);

    // 旧文件延迟善后
    for f in old_files {
        let _ = move_to_trash(store.root(), &f);
    }
    Ok(Some(SingleResult {
        merged,
        rows,
        bytes_written: entry.byte_size,
    }))
}

/// 合并写单个大 segment（统计重算）。序列化直写本地文件（§4 热路径豁免）。
fn write_merged(
    store: &Store,
    pdir: &Path,
    batches: &[RecordBatch],
    schema: &Arc<arrow::datatypes::Schema>,
    def: &Arc<TableDef>,
) -> Result<SegmentEntry> {
    let seg_name = format!("seg_{}.parquet", uuid::Uuid::now_v7().simple());
    let final_path = pdir.join(&seg_name);
    let tmp_path = crate::fsutil::tmp_sibling(&final_path);

    let mut rows: u64 = 0;
    {
        let file = File::create(&tmp_path)
            .map_err(|e| QsdbError::meta(format!("create {} failed: {e}", tmp_path.display())))?;
        let mut writer =
            ArrowWriter::try_new(file, Arc::clone(schema), Some(writer_props(def)?))
                .map_err(|e| QsdbError::meta(format!("parquet writer init failed: {e}")))?;
        for b in batches {
            writer
                .write(b)
                .map_err(|e| QsdbError::meta(format!("parquet write failed: {e}")))?;
            rows += b.num_rows() as u64;
        }
        writer
            .close()
            .map_err(|e| QsdbError::meta(format!("parquet close failed: {e}")))?;
    }
    let bytes = store
        .stat(&tmp_path)?
        .ok_or_else(|| QsdbError::meta("stat segment failed: tmp missing after write"))?
        .size;
    store.commit_object(&tmp_path, &final_path)?;

    // 统计重算（跨批合并）
    let names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
    let mut accs: Vec<(String, crate::write::ColAcc)> = names
        .iter()
        .map(|n| (n.clone(), crate::write::ColAcc::default()))
        .collect();
    for b in batches {
        for (i, (_, acc)) in accs.iter_mut().enumerate() {
            let st = stats::column_stats(b.column(i))?;
            acc.nulls += st.null_count;
            acc.min = crate::write::combine_lit(acc.min.take(), st.min, true);
            acc.max = crate::write::combine_lit(acc.max.take(), st.max, false);
        }
    }
    let column_stats = accs
        .into_iter()
        .map(|(n, a)| {
            (
                n,
                crate::manifest::ColumnStats {
                    min: a.min,
                    max: a.max,
                    null_count: a.nulls,
                },
            )
        })
        .collect();

    Ok(SegmentEntry {
        file_name: seg_name,
        row_count: rows,
        byte_size: bytes,
        created_at: now_ms() as i64,
        column_stats,
    })
}

// RecordBatchOptions 引用占位（合并批未直接用 options——保持 import 可见性）
#[allow(unused)]
fn _opts_hint(_: &RecordBatchOptions) {}

// --- 远端 compaction（feature `s3`，§11：读走段缓存 → 内存合并 → PUT → OCC） ---

#[cfg(feature = "remote")]
fn compact_partition_remote(
    store: &Store,
    file_cache: Option<&Arc<crate::filecache::FileCache>>,
    table_dir: &Path,
    def: &Arc<TableDef>,
    pdir: &Path,
    opts: &CompactOptions,
) -> Result<Option<SingleResult>> {
    let Some(manifest) = PartitionManifest::read(store, pdir)? else {
        return Ok(None);
    };
    if manifest.segments.len() < opts.min_segments.max(2) {
        return Ok(None);
    }
    let avg = manifest
        .segments
        .iter()
        .map(|s| s.byte_size)
        .sum::<u64>()
        / manifest.segments.len() as u64;
    if avg >= opts.max_avg_segment_bytes {
        return Ok(None);
    }

    crate::remote::with_partition_mutex(pdir, || {
        // 进程内互斥下重读（等待期间可能已被其他写者变更）
        let Some(manifest) = PartitionManifest::read(store, pdir)? else {
            return Ok(None);
        };
        if manifest.segments.len() < 2 {
            return Ok(None);
        }

        let schema = data_schema(def);
        let want: Vec<(String, crate::types::LogicalType)> = def
            .data_columns()
            .iter()
            .map(|c| (c.name.clone(), c.logical_type.clone()))
            .collect();
        // 读全部 segments（compaction 全读 → 段缓存准入 prefer_whole）
        let mut batches: Vec<RecordBatch> = Vec::new();
        for seg in &manifest.segments {
            let path = pdir.join(&seg.file_name);
            let src = crate::scan::ChunkSource::open(
                Some(store),
                &crate::scan::FileLoc::Managed(path.clone()),
                file_cache.map(|f| f.as_ref()),
                true,
            )?;
            let reader = ParquetRecordBatchReaderBuilder::try_new(src)
                .map_err(|e| QsdbError::meta(format!("read parquet {path:?}: {e}")))?
                .with_batch_size(8192)
                .with_projection(parquet::arrow::ProjectionMask::all())
                .build()
                .map_err(|e| QsdbError::meta(format!("build reader failed: {e}")))?;
            for b in reader {
                let b = b.map_err(|e| QsdbError::meta(format!("read batch failed: {e}")))?;
                if b.num_rows() > 0 {
                    let missing: Vec<(String, crate::types::LogicalType)> = want
                        .iter()
                        .filter(|(n, _)| b.schema().index_of(n).is_err())
                        .cloned()
                        .collect();
                    let b = if missing.is_empty() {
                        b
                    } else {
                        crate::scan::pad_missing_columns(b, &missing)?
                    };
                    batches.push(b);
                }
            }
        }
        let rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
        if rows == 0 {
            return Ok(None);
        }

        // 合并写新 segment（内存缓冲 → PUT）
        let names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
        let entry = crate::write::put_segment_remote(
            store,
            pdir,
            &batches,
            &schema,
            crate::write::writer_props(def)?,
            &names,
        )?;
        let merged = manifest.segments.len() as u64;

        // manifest OCC 替换提交（重放继承最新 generation）
        let mpath = PartitionManifest::manifest_path(pdir);
        let new_entry = entry.clone();
        crate::remote::occ_commit(store, &mpath, |cur| {
            let mut m = cur.unwrap_or_else(|| {
                PartitionManifest::new(
                    manifest.partition_values.clone(),
                    def.schema_fingerprint(),
                )
            });
            m.generation += 1;
            m.segments = vec![new_entry.clone()];
            Ok(m)
        })?;
        let _ = table_dir;
        Ok(Some(SingleResult {
            merged,
            rows,
            bytes_written: entry.byte_size,
        }))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::{Qsdb, QsdbOptions};
    use crate::catalog::DEFAULT_DB;
    use crate::store::Store;
    use arrow::array::{Date32Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};

    fn tmp_root(tag: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir()
            .join("qsdb_compact_test")
            .join(format!("{tag}_{}", uuid::Uuid::now_v7().simple()));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn batch(dt: i32, vals: Vec<i64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("dt", DataType::Date32, true),
            Field::new("v", DataType::Int64, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Date32Array::from(vec![dt; vals.len()])),
                Arc::new(Int64Array::from(vals)),
            ],
        )
        .unwrap()
    }

    #[test]
    fn compact_merges_small_segments() {
        let root = tmp_root("merge");
        let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
        db.sql("CREATE TABLE t (dt DATE, v BIGINT) PARTITIONED BY (dt)")
            .unwrap();
        for i in 0..6 {
            db.append(
                "t",
                &[batch(19_000 + i, vec![i64::from(i), i64::from(i) + 1])],
            )
            .unwrap();
        }
        // 同分区 6 次小 append
        for _ in 0..6 {
            db.append("t", &[batch(19_100, vec![1, 2])]).unwrap();
        }
        let tdir = root.join(DEFAULT_DB).join("t");
        let pdir = tdir.join("dt=2022-04-18");
        let store = Store::local(&root);
        let m = PartitionManifest::read(&store, &pdir).unwrap().unwrap();
        assert_eq!(m.segments.len(), 6);

        // compaction（默认阈值）
        let def = db.table_def("t").unwrap();
        let r = compact_table(&store, None, &tdir, &def, &CompactOptions::default()).unwrap();
        assert_eq!(r.partitions_compacted, 1);
        assert_eq!(r.segments_merged, 6);
        assert_eq!(r.rows, 12);

        // manifest 变单 segment，数据完整
        let m2 = PartitionManifest::read(&store, &pdir).unwrap().unwrap();
        assert_eq!(m2.segments.len(), 1);
        assert_eq!(m2.total_rows(), 12);
        let batches = db
            .sql_collect("SELECT v FROM t WHERE dt = DATE '2022-04-18' ORDER BY v")
            .unwrap();
        let got: Vec<String> = batches
            .iter()
            .flat_map(|b| {
                (0..b.num_rows()).map(|i| {
                    arrow::util::display::array_value_to_string(b.column(0).as_ref(), i).unwrap()
                })
            })
            .collect();
        assert_eq!(
            got,
            vec!["1", "1", "1", "1", "1", "1", "2", "2", "2", "2", "2", "2"]
        );
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn compact_respects_thresholds() {
        let root = tmp_root("thr");
        let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
        db.sql("CREATE TABLE t (dt DATE, v BIGINT) PARTITIONED BY (dt)")
            .unwrap();
        db.append("t", &[batch(19_000, vec![1])]).unwrap();
        db.append("t", &[batch(19_000, vec![2])]).unwrap();
        let def = db.table_def("t").unwrap();
        let tdir = root.join(DEFAULT_DB).join("t");
        let store = Store::local(&root);
        // min_segments=3：2 个不触发
        let r = compact_table(
            &store,
            None,
            &tdir,
            &def,
            &CompactOptions {
                min_segments: 3,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(r.partitions_compacted, 0);
        // min_segments=2：触发
        let r = compact_table(
            &store,
            None,
            &tdir,
            &def,
            &CompactOptions {
                min_segments: 2,
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(r.partitions_compacted, 1);
        std::fs::remove_dir_all(&root).ok();
    }
}
