//! 写路径：append / 分区原子 overwrite 的提交协议（设计文档 §7）。
//!
//! append：锁内 写数据文件(tmp+rename) → manifest 追加 → 原子替换（提交点）
//! overwrite：锁内 写新数据 → 只含新 segment 的 manifest 原子替换（提交点）
//!            → 旧数据文件移 `_trash`

use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::Array;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::{EnabledStatistics, WriterProperties};

use crate::catalog::TableDef;
use crate::error::{QsdbError, Result};
use crate::fsutil::{PartitionLock, move_to_trash, now_ms};
use crate::manifest::{PartitionManifest, SegmentEntry};
use crate::partition::partition_dir;
use crate::stats;

/// 写入结果。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WriteResult {
    pub rows: u64,
    pub bytes: u64,
    pub segments: u32,
}

/// 表数据列（不含分区列）的 arrow schema。
pub fn data_schema(def: &TableDef) -> SchemaRef {
    let fields: Vec<Field> = def
        .data_columns()
        .iter()
        .map(|c| Field::new(&c.name, c.logical_type.arrow_type(), c.nullable))
        .collect();
    Arc::new(Schema::new(fields))
}

/// parquet 写参数（format-spec §5；表属性 `compression` 可改 snappy）。
pub fn writer_props(def: &TableDef) -> Result<WriterProperties> {
    let compression = match def.properties.get("compression").map(|s| s.as_str()) {
        None | Some("zstd") => Compression::ZSTD(
            parquet::basic::ZstdLevel::try_new(1)
                .map_err(|e| QsdbError::invalid(format!("zstd level: {e}")))?,
        ),
        Some("snappy") => Compression::SNAPPY,
        Some(other) => {
            return Err(QsdbError::invalid(format!(
                "unknown compression '{other}' (expect zstd|snappy)"
            )));
        }
    };
    Ok(WriterProperties::builder()
        .set_compression(compression)
        .set_max_row_group_row_count(Some(1_000_000))
        .set_data_page_row_count_limit(20_000)
        .set_statistics_enabled(EnabledStatistics::Page)
        .build())
}

/// 校验输入批 schema 与表数据列一致（按 名字+类型；空批忽略）。
fn validate_batches(batches: &[RecordBatch], expected: &SchemaRef) -> Result<Option<SchemaRef>> {
    let mut src: Option<SchemaRef> = None;
    for b in batches {
        if b.num_rows() == 0 && b.num_columns() == 0 {
            continue;
        }
        if b.num_columns() != expected.fields().len() {
            return Err(QsdbError::invalid(format!(
                "batch has {} columns, table expects {}",
                b.num_columns(),
                expected.fields().len()
            )));
        }
        for (i, f) in b.schema().fields().iter().enumerate() {
            let want = expected.field(i);
            if f.name() != want.name() {
                return Err(QsdbError::invalid(format!(
                    "batch column[{i}] is '{}', table expects '{}'",
                    f.name(),
                    want.name()
                )));
            }
            if !type_compatible(f.data_type(), want.data_type()) {
                return Err(QsdbError::type_err(format!(
                    "batch column '{}' is {:#?}, table expects {:#?}",
                    f.name(),
                    f.data_type(),
                    want.data_type()
                )));
            }
        }
        match &src {
            None => src = Some(Arc::clone(&b.schema())),
            Some(prev) if prev.fields() != b.schema().fields() => {
                return Err(QsdbError::invalid(
                    "input batches have inconsistent schemas",
                ));
            }
            Some(_) => {}
        }
    }
    Ok(src)
}

fn type_compatible(a: &DataType, b: &DataType) -> bool {
    a == b
}

/// 写一个 segment：所有批进一个 parquet 文件（tmp → rename 定名）。
fn write_segment(
    partition_dir: &Path,
    batches: &[RecordBatch],
    file_schema: &SchemaRef,
    props: WriterProperties,
    column_names: &[String],
) -> Result<SegmentEntry> {
    let seg_name = format!("seg_{}.parquet", uuid::Uuid::now_v7().simple());
    let final_path = partition_dir.join(&seg_name);
    let tmp_path = crate::fsutil::tmp_sibling(&final_path);

    let mut rows: u64 = 0;
    {
        let file = File::create(&tmp_path)
            .map_err(|e| QsdbError::meta(format!("create {} failed: {e}", tmp_path.display())))?;
        let mut writer = ArrowWriter::try_new(file, Arc::clone(file_schema), Some(props))
            .map_err(|e| QsdbError::meta(format!("parquet writer init failed: {e}")))?;
        for b in batches {
            if b.num_rows() == 0 {
                continue;
            }
            writer
                .write(b)
                .map_err(|e| QsdbError::meta(format!("parquet write failed: {e}")))?;
            rows += b.num_rows() as u64;
        }
        writer
            .close()
            .map_err(|e| QsdbError::meta(format!("parquet close failed: {e}")))?;
    }
    let bytes = fs::metadata(&tmp_path)
        .map_err(|e| QsdbError::meta(format!("stat segment failed: {e}")))?
        .len();
    fs::rename(&tmp_path, &final_path)
        .map_err(|e| QsdbError::meta(format!("rename segment failed: {e}")))?;

    // 统计：跨批合并
    let mut merged: Vec<(String, stats_merge::Acc)> = column_names
        .iter()
        .map(|n| (n.clone(), stats_merge::Acc::default()))
        .collect();
    for b in batches {
        if b.num_rows() == 0 {
            continue;
        }
        for (i, (_, acc)) in merged.iter_mut().enumerate() {
            let st = stats::column_stats(b.column(i))?;
            acc.nulls += st.null_count;
            acc.min = stats_merge::min_opt(acc.min.take(), st.min);
            acc.max = stats_merge::max_opt(acc.max.take(), st.max);
        }
    }
    let column_stats = merged
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

/// 列统计累积器（compaction 复用）。
#[derive(Default)]
pub struct ColAcc {
    pub min: Option<crate::types::Literal>,
    pub max: Option<crate::types::Literal>,
    pub nulls: u64,
}

/// Literal min/max 合并（compaction 复用）。
pub fn combine_lit(
    a: Option<crate::types::Literal>,
    b: Option<crate::types::Literal>,
    keep_min: bool,
) -> Option<crate::types::Literal> {
    stats_merge::combine(a, b, keep_min)
}

mod stats_merge {
    use crate::types::Literal;

    #[derive(Default)]
    pub(super) struct Acc {
        pub min: Option<Literal>,
        pub max: Option<Literal>,
        pub nulls: u64,
    }

    pub(super) fn min_opt(a: Option<Literal>, b: Option<Literal>) -> Option<Literal> {
        combine(a, b, true)
    }

    pub(super) fn max_opt(a: Option<Literal>, b: Option<Literal>) -> Option<Literal> {
        combine(a, b, false)
    }

    pub(super) fn combine(
        a: Option<Literal>,
        b: Option<Literal>,
        keep_min: bool,
    ) -> Option<Literal> {
        match (a, b) {
            (None, x) | (x, None) => x,
            (Some(x), Some(y)) => match x.partial_cmp_literal(&y) {
                Ok(Some(o)) => {
                    let x_wins = if keep_min {
                        o == std::cmp::Ordering::Less
                    } else {
                        o == std::cmp::Ordering::Greater
                    };
                    if x_wins { Some(x) } else { Some(y) }
                }
                _ => Some(x), // 不可比（理论不发生）：保守保留
            },
        }
    }
}

/// 分区目录路径（含表目录）。
fn pdir(table_dir: &Path, def: &TableDef, values: &[crate::types::Literal]) -> Result<PathBuf> {
    Ok(table_dir.join(partition_dir(&def.partition_keys, values)?))
}

fn lock_path(partition_dir: &Path) -> PathBuf {
    partition_dir.join(".lock")
}

/// 写入端约束检查（M5-B）：
/// - NOT NULL：位图 null_count() O(1)，无约束列零开销
/// - CHECK：每批一次表达式求值，TRUE/NULL 通过（SQL 标准 unknown 视为满足）
pub(crate) fn check_constraints(def: &TableDef, batches: &[RecordBatch]) -> Result<()> {
    for (i, col) in def.data_columns().iter().enumerate() {
        if !col.nullable {
            for b in batches.iter().filter(|b| b.num_rows() > 0) {
                if let Some(arr) = b.columns().get(i) {
                    let nulls = arr.null_count();
                    if nulls > 0 {
                        return Err(QsdbError::invalid(format!(
                            "NOT NULL constraint violated: column '{}' has {nulls} null(s)",
                            col.name
                        )));
                    }
                }
            }
        }
    }
    let checks: Vec<&str> = def
        .data_columns()
        .iter()
        .filter_map(|c| c.check.as_deref())
        .collect();
    if checks.is_empty() {
        return Ok(());
    }
    let ctx = crate::sql::translate::SchemaCtx {
        cols: def
            .data_columns()
            .iter()
            .map(|c| (c.name.clone(), c.logical_type.clone()))
            .collect(),
    };
    for sql in checks {
        let pred =
            crate::sql::translate::bind_expr(&crate::sql::translate::parse_expr_sql(sql)?, &ctx)?;
        for b in batches.iter().filter(|b| b.num_rows() > 0) {
            let arr = crate::expr::eval::eval(&pred, b)?;
            let ba = arr
                .as_any()
                .downcast_ref::<arrow::array::BooleanArray>()
                .ok_or_else(|| QsdbError::type_err("CHECK predicate must be boolean"))?;
            if let Some(row) = (0..ba.len()).find(|&i| !ba.is_null(i) && !ba.value(i)) {
                return Err(QsdbError::invalid(format!(
                    "CHECK constraint violated ({sql}): first failing row {row}"
                )));
            }
        }
    }
    Ok(())
}

/// append（单分区；输入批必须已按分区分组，含全表数据列）。
pub(crate) fn append_partition(
    table_dir: &Path,
    def: &TableDef,
    values: &[crate::types::Literal],
    batches: &[RecordBatch],
) -> Result<WriteResult> {
    let schema = data_schema(def);
    let Some(file_schema) = validate_batches(batches, &schema)? else {
        return Ok(WriteResult::default()); // 全空输入 → no-op
    };
    check_constraints(def, batches)?;
    let dir = pdir(table_dir, def, values)?;
    fs::create_dir_all(&dir)
        .map_err(|e| QsdbError::meta(format!("mkdir partition failed: {e}")))?;
    let _guard = PartitionLock::acquire(&lock_path(&dir))?;

    let entry = write_segment(
        &dir,
        batches,
        &file_schema,
        writer_props(def)?,
        &schema
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect::<Vec<_>>(),
    )?;

    let mut manifest = PartitionManifest::read(&dir)?.unwrap_or_else(|| {
        PartitionManifest::new(named_values(def, values), def.schema_fingerprint())
    });
    if manifest.schema_fingerprint != def.schema_fingerprint() {
        return Err(QsdbError::meta(
            "table schema changed since partition was written; use overwrite_partition",
        ));
    }
    manifest.segments.push(entry.clone());
    manifest.commit(&dir)?;
    sync_index(table_dir, &dir, &manifest);
    drop(_guard);

    Ok(WriteResult {
        rows: entry.row_count,
        bytes: entry.byte_size,
        segments: 1,
    })
}

/// 提交成功后同步表级索引（best-effort：失败不影响已提交数据，可由重建兜底）。
/// 必须在分区锁持有期间调用（锁顺序：分区锁 → 索引锁）。
fn sync_index(table_dir: &Path, partition_dir: &Path, manifest: &PartitionManifest) {
    let rel = partition_dir
        .strip_prefix(table_dir)
        .unwrap_or(partition_dir)
        .to_string_lossy()
        .replace('\\', "/");
    let _ = crate::index::PartitionsIndex::upsert(table_dir, &rel, manifest);
}

/// overwrite（单分区原子替换；空输入 = 清空分区）。
pub(crate) fn overwrite_partition(
    root: &Path,
    table_dir: &Path,
    def: &TableDef,
    values: &[crate::types::Literal],
    batches: &[RecordBatch],
) -> Result<WriteResult> {
    let schema = data_schema(def);
    let file_schema = validate_batches(batches, &schema)?;
    check_constraints(def, batches)?;
    let dir = pdir(table_dir, def, values)?;
    fs::create_dir_all(&dir)
        .map_err(|e| QsdbError::meta(format!("mkdir partition failed: {e}")))?;
    let _guard = PartitionLock::acquire(&lock_path(&dir))?;

    // 旧 manifest（不存在 = 新分区）
    let old = PartitionManifest::read(&dir)?;

    let mut result = WriteResult::default();
    let mut new_segments: Vec<SegmentEntry> = Vec::new();
    if let Some(fs) = &file_schema {
        let entry = write_segment(
            &dir,
            batches,
            fs,
            writer_props(def)?,
            &schema
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect::<Vec<_>>(),
        );
        match entry {
            Ok(e) => {
                result = WriteResult {
                    rows: e.row_count,
                    bytes: e.byte_size,
                    segments: 1,
                };
                new_segments.push(e);
            }
            Err(err) => {
                // overwrite 失败：不动旧 manifest，清理可能的孤儿 tmp（GC 兜底）
                return Err(err);
            }
        }
    }

    // ★ 提交点：只含新 segments 的 manifest 原子替换（generation 继承旧值）
    let mut manifest = {
        let mut m = PartitionManifest::new(named_values(def, values), def.schema_fingerprint());
        m.generation = old.as_ref().map_or(0, |m| m.generation);
        m
    };
    manifest.segments = new_segments;
    let old_files: Vec<PathBuf> = old.map(|m| m.segment_paths(&dir)).unwrap_or_default();
    manifest.commit(&dir)?;
    sync_index(table_dir, &dir, &manifest);
    drop(_guard);

    // 旧数据文件延迟善后（提交后；失败不影响正确性，GC 兜底）
    for f in old_files {
        let _ = move_to_trash(root, &f);
    }

    Ok(result)
}

fn named_values(
    def: &TableDef,
    values: &[crate::types::Literal],
) -> Vec<(String, crate::types::Literal)> {
    def.partition_keys
        .iter()
        .cloned()
        .zip(values.iter().cloned())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::ColumnDef;
    use crate::types::{Literal, LogicalType};
    use arrow::array::{Int64Array, StringArray};

    fn def() -> TableDef {
        TableDef::new(
            "t",
            vec![
                ColumnDef {
                    name: "dt".into(),
                    logical_type: LogicalType::Date,
                    nullable: true,
                    check: None,
                    default: None,
                    comment: None,
                },
                ColumnDef {
                    name: "v".into(),
                    logical_type: LogicalType::Int64,
                    nullable: true,
                    check: None,
                    default: None,
                    comment: None,
                },
            ],
            vec!["dt".into()],
        )
        .unwrap()
    }

    /// 纯数据列批（append/overwrite 的输入约定：分区列已剥离）
    fn data_batch(vals: Vec<Option<i64>>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vals))]).unwrap()
    }

    fn tmp_root(tag: &str) -> PathBuf {
        let d = std::env::temp_dir()
            .join("qsdb_write_test")
            .join(format!("{tag}_{}", uuid::Uuid::now_v7().simple()));
        fs::create_dir_all(&d).unwrap();
        d
    }

    #[test]
    fn append_creates_manifest_and_files() {
        let root = tmp_root("append");
        let d = def();
        let tdir = root.join("t");
        let vals = [Literal::Date(1)];
        let r = append_partition(&tdir, &d, &vals, &[data_batch(vec![Some(10)])]).unwrap();
        assert_eq!(r.rows, 1);
        let pdir = tdir.join("dt=1970-01-02");
        let m = PartitionManifest::read(&pdir).unwrap().unwrap();
        assert_eq!(m.segments.len(), 1);
        assert_eq!(m.total_rows(), 1);
        assert_eq!(m.generation, 1);
        assert!(pdir.join(&m.segments[0].file_name).exists());
        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn append_accumulates_and_overwrite_replaces() {
        let root = tmp_root("ow");
        let d = def();
        let tdir = root.join("t");
        let vals = [Literal::Date(1)];
        append_partition(&tdir, &d, &vals, &[data_batch(vec![Some(10)])]).unwrap();
        append_partition(&tdir, &d, &vals, &[data_batch(vec![Some(20)])]).unwrap();
        let pdir = tdir.join("dt=1970-01-02");
        assert_eq!(
            PartitionManifest::read(&pdir)
                .unwrap()
                .unwrap()
                .total_rows(),
            2
        );

        // overwrite：只剩新数据
        overwrite_partition(
            &root,
            &tdir,
            &d,
            &vals,
            &[data_batch(vec![Some(7), Some(8)])],
        )
        .unwrap();
        let m = PartitionManifest::read(&pdir).unwrap().unwrap();
        assert_eq!(m.segments.len(), 1);
        assert_eq!(m.total_rows(), 2);
        assert_eq!(m.generation, 3);
        // 旧文件进 trash
        assert!(root.join("_trash").exists());
        // 空 overwrite = 清空分区
        overwrite_partition(&root, &tdir, &d, &vals, &[]).unwrap();
        let m = PartitionManifest::read(&pdir).unwrap().unwrap();
        assert_eq!(m.total_rows(), 0);
        assert!(m.segments.is_empty());
        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn schema_mismatch_rejected() {
        let root = tmp_root("mismatch");
        let d = def();
        let tdir = root.join("t");
        let vals = [Literal::Date(1)];
        append_partition(&tdir, &d, &vals, &[data_batch(vec![Some(1)])]).unwrap();
        // 错列名
        let schema = Arc::new(Schema::new(vec![Field::new(
            "wrong",
            DataType::Int64,
            true,
        )]));
        let b = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1]))],
        )
        .unwrap();
        assert!(append_partition(&tdir, &d, &vals, &[b]).is_err());
        // 错类型
        let schema2 = Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, true)]));
        let b2 =
            RecordBatch::try_new(schema2, vec![Arc::new(StringArray::from(vec!["x"]))]).unwrap();
        assert!(append_partition(&tdir, &d, &vals, &[b2]).is_err());
        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn stats_recorded_in_manifest() {
        let root = tmp_root("stats");
        let d = def();
        let tdir = root.join("t");
        append_partition(
            &tdir,
            &d,
            &[Literal::Date(1)],
            &[data_batch(vec![Some(5), None])],
        )
        .unwrap();
        let pdir = tdir.join("dt=1970-01-02");
        let m = PartitionManifest::read(&pdir).unwrap().unwrap();
        let (name, vstats) = &m.segments[0].column_stats[0];
        assert_eq!(name, "v");
        assert_eq!(vstats.min, Some(Literal::Int64(5)));
        assert_eq!(vstats.max, Some(Literal::Int64(5)));
        assert_eq!(vstats.null_count, 1);
        fs::remove_dir_all(&root).ok();
    }
}
