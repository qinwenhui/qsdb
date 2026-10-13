//! 写路径：append / 分区原子 overwrite 的提交协议（设计文档 §7）。
//!
//! append：锁内 写数据文件(tmp+rename) → manifest 追加 → 原子替换（提交点）
//! overwrite：锁内 写新数据 → 只含新 segment 的 manifest 原子替换（提交点）
//!            → 旧数据文件留原地，GC 安全期后回收（保护在途扫描）

use std::fs::File;
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
use crate::fsutil::{PartitionLock, now_ms};
use crate::manifest::{PartitionManifest, SegmentEntry};
use crate::partition::partition_dir;
use crate::stats;
use crate::store::Store;

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

/// 宽松写：批缺尾列（表数据列前缀）且缺列均 nullable → 补 NULL
/// （ALTER ADD COLUMN 后同步工具仍投旧 schema 批的场景）。
/// NOT NULL 缺列不可补 → 报错。
fn pad_loose_batches<'a>(
    def: &TableDef,
    batches: &'a [RecordBatch],
    expected: &SchemaRef,
) -> Result<std::borrow::Cow<'a, [RecordBatch]>> {
    let full = expected.fields().len();
    if !batches
        .iter()
        .any(|b| b.num_columns() != 0 && b.num_columns() < full)
    {
        return Ok(std::borrow::Cow::Borrowed(batches));
    }
    let mut out: Vec<RecordBatch> = Vec::with_capacity(batches.len());
    for b in batches {
        let has = b.num_columns();
        if has == full || (b.num_rows() == 0 && has == 0) {
            out.push(b.clone());
            continue;
        }
        if has > full {
            return Err(QsdbError::invalid(format!(
                "batch has {has} columns, table expects {full}"
            )));
        }
        let missing: Vec<(String, crate::types::LogicalType)> = def.data_columns()[has..]
            .iter()
            .map(|c| (c.name.clone(), c.logical_type.clone()))
            .collect();
        if let Some((n, _)) = missing
            .iter()
            .find(|(n, _)| def.column(n).is_some_and(|c| !c.nullable))
        {
            return Err(QsdbError::invalid(format!(
                "batch is missing NOT NULL column '{n}'; supply full-width batches"
            )));
        }
        out.push(crate::scan::pad_missing_columns(b.clone(), &missing)?);
    }
    Ok(std::borrow::Cow::Owned(out))
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

/// 批列统计（跨批合并）——本地/远端 segment 写共用。
fn merged_column_stats(
    batches: &[RecordBatch],
    column_names: &[String],
) -> Result<Vec<(String, crate::manifest::ColumnStats)>> {
    let mut merged: Vec<(String, stats_merge::Acc)> = column_names
        .iter()
        .map(|n| (n.clone(), stats_merge::Acc::default()))
        .collect();
    for b in batches.iter().filter(|b| b.num_rows() > 0) {
        for (i, (_, acc)) in merged.iter_mut().enumerate() {
            let st = stats::column_stats(b.column(i))?;
            acc.nulls += st.null_count;
            acc.min = stats_merge::min_opt(acc.min.take(), st.min);
            acc.max = stats_merge::max_opt(acc.max.take(), st.max);
        }
    }
    Ok(merged
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
        .collect())
}

/// 序列化批集到 parquet 字节（行数 + 列统计一并产出）。
/// 本地 = 写文件句柄；远端 = 写内存缓冲（segment 整体 PUT，§7.2）。
fn serialize_segment<W: std::io::Write + Send>(
    mut w: W,
    batches: &[RecordBatch],
    file_schema: &SchemaRef,
    props: WriterProperties,
    column_names: &[String],
) -> Result<(u64, Vec<(String, crate::manifest::ColumnStats)>)> {
    let mut writer = ArrowWriter::try_new(&mut w, Arc::clone(file_schema), Some(props))
        .map_err(|e| QsdbError::meta(format!("parquet writer init failed: {e}")))?;
    let mut rows: u64 = 0;
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
    let stats = merged_column_stats(batches, column_names)?;
    Ok((rows, stats))
}

/// 写一个 segment：所有批进一个 parquet 文件（tmp → rename 定名）。
/// 序列化直写本地文件（§4 热路径豁免）；stat/定名经 Store。
fn write_segment(
    store: &Store,
    partition_dir: &Path,
    batches: &[RecordBatch],
    file_schema: &SchemaRef,
    props: WriterProperties,
    column_names: &[String],
) -> Result<SegmentEntry> {
    let seg_name = format!("seg_{}.parquet", uuid::Uuid::now_v7().simple());
    let final_path = partition_dir.join(&seg_name);
    let tmp_path = crate::fsutil::tmp_sibling(&final_path);

    let durable = store.data_sync();
    let (rows, column_stats) = {
        let mut file = File::create(&tmp_path)
            .map_err(|e| QsdbError::meta(format!("create {} failed: {e}", tmp_path.display())))?;
        let out = serialize_segment(&mut file, batches, file_schema, props, column_names)?;
        if durable {
            // 掉电安全档：段数据落盘后 rename——manifest 一旦提交，段必然可读
            file.sync_all()
                .map_err(|e| QsdbError::meta(format!("fsync segment failed: {e}")))?;
        }
        out
    };
    let bytes = store
        .stat(&tmp_path)?
        .ok_or_else(|| QsdbError::meta("stat segment failed: tmp missing after write"))?
        .size;
    store.commit_object(&tmp_path, &final_path)?;
    if durable {
        crate::fsutil::dir_sync(&final_path);
    }

    Ok(SegmentEntry {
        file_name: seg_name,
        row_count: rows,
        byte_size: bytes,
        created_at: now_ms() as i64,
        column_stats,
    })
}

/// 远端 segment 写（feature `s3`，§7.2 不可变对象）：parquet 序列化到内存
/// 缓冲 → 整体 PUT（UUID 名永不冲突，幂等）。compaction 复用。
/// v1 内存上限 = 单次 flush 的攒批量（默认 128MB 逻辑列，压缩后远小于）；
/// staging 落盘 M6-4。
#[cfg(feature = "remote")]
pub(crate) fn put_segment_remote(
    store: &Store,
    partition_dir: &Path,
    batches: &[RecordBatch],
    file_schema: &SchemaRef,
    props: WriterProperties,
    column_names: &[String],
) -> Result<SegmentEntry> {
    let seg_name = format!("seg_{}.parquet", uuid::Uuid::now_v7().simple());
    let mut buf = Vec::new();
    let (rows, column_stats) =
        serialize_segment(&mut buf, batches, file_schema, props, column_names)?;
    store.put_object(&partition_dir.join(&seg_name), &buf)?;
    Ok(SegmentEntry {
        file_name: seg_name,
        row_count: rows,
        byte_size: buf.len() as u64,
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
    store: &Store,
    staging: Option<&crate::staging::Staging>,
    table_dir: &Path,
    def: &TableDef,
    values: &[crate::types::Literal],
    batches: &[RecordBatch],
) -> Result<WriteResult> {
    let schema = data_schema(def);
    let batches = pad_loose_batches(def, batches, &schema)?;
    let batches: &[RecordBatch] = batches.as_ref();
    let Some(file_schema) = validate_batches(batches, &schema)? else {
        return Ok(WriteResult::default()); // 全空输入 → no-op
    };
    check_constraints(def, batches)?;
    let dir = pdir(table_dir, def, values)?;
    store.create_dir_all(&dir)?;
    if !store.is_local() {
        #[cfg(feature = "remote")]
        return append_partition_remote(
            store,
            staging,
            table_dir,
            def.name.as_str(),
            def,
            values,
            &dir,
            batches,
            &schema,
            &file_schema,
        );
        #[cfg(not(feature = "remote"))]
        {
            let _ = staging;
            return Err(QsdbError::unsupported("remote store requires feature 's3'"));
        }
    }
    let t = std::time::Instant::now();
    let _guard = PartitionLock::acquire_local(&lock_path(&dir))?;
    P_LOCK_NS.fetch_add(t.elapsed().as_nanos() as u64, AtomicOrd::Relaxed);
    P_CALLS.fetch_add(1, AtomicOrd::Relaxed);

    let t = std::time::Instant::now();
    let entry = write_segment(
        store,
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
    P_SEG_NS.fetch_add(t.elapsed().as_nanos() as u64, AtomicOrd::Relaxed);

    let t = std::time::Instant::now();
    let mut manifest = PartitionManifest::read(store, &dir)?.unwrap_or_else(|| {
        PartitionManifest::new(named_values(def, values), def.schema_fingerprint())
    });
    // 指纹一致 或 旧 schema 为当前前缀（ALTER ADD COLUMN）→ 允许追加并升格分区指纹
    if manifest.schema_fingerprint != def.schema_fingerprint() {
        if !def
            .prefix_fingerprints()
            .contains(&manifest.schema_fingerprint)
        {
            return Err(QsdbError::meta(
                "table schema changed since partition was written; use overwrite_partition",
            ));
        }
        manifest.schema_fingerprint = def.schema_fingerprint();
    }
    manifest.segments.push(entry.clone());
    P_MAN_READ_NS.fetch_add(t.elapsed().as_nanos() as u64, AtomicOrd::Relaxed);
    let t = std::time::Instant::now();
    manifest.commit(store, &dir)?;
    P_MAN_COMMIT_NS.fetch_add(t.elapsed().as_nanos() as u64, AtomicOrd::Relaxed);
    if store.is_local() {
        let t = std::time::Instant::now();
        sync_index(store, table_dir, &dir, &manifest);
        P_IDX_NS.fetch_add(t.elapsed().as_nanos() as u64, AtomicOrd::Relaxed);
    }
    drop(_guard);

    Ok(WriteResult {
        rows: entry.row_count,
        bytes: entry.byte_size,
        segments: 1,
    })
}

/// 提交成功后同步表级索引（best-effort：失败不影响已提交数据，可由重建兜底）。
/// 必须在分区锁持有期间调用（锁顺序：分区锁 → 索引锁）。
fn sync_index(store: &Store, table_dir: &Path, partition_dir: &Path, manifest: &PartitionManifest) {
    let rel = partition_dir
        .strip_prefix(table_dir)
        .unwrap_or(partition_dir)
        .to_string_lossy()
        .replace('\\', "/");
    let _ = crate::index::PartitionsIndex::upsert(store, table_dir, &rel, manifest);
}

// ---------------------------------------------------------------------------
// 写路径分段计时（`QSDB_WRITE_PROF=1`）
//
// 动机：`INSERT … SELECT` 比 `db.append` 慢 6 倍，而两者都只是"扫 + 写"；批大小
// 一变，1M 行的落盘从 2.8s 变 15.4s（5.5x）——说明**每次 append 调用有可观固定开销**。
// 要修就得知道这开销是锁、段写、manifest 提交还是索引维护，不能猜。
// 静态累加，进程结束前由调用方（bench 的 `--sql`）打印一次。
// ---------------------------------------------------------------------------
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrd};
static P_LOCK_NS: AtomicU64 = AtomicU64::new(0);
static P_SEG_NS: AtomicU64 = AtomicU64::new(0);
static P_MAN_READ_NS: AtomicU64 = AtomicU64::new(0);
static P_MAN_COMMIT_NS: AtomicU64 = AtomicU64::new(0);
static P_IDX_NS: AtomicU64 = AtomicU64::new(0);
static P_CALLS: AtomicU64 = AtomicU64::new(0);

/// 打印写路径分段累计（`QSDB_WRITE_PROF=1` 时由调用方调用；否则返回 None）。
pub fn write_prof_dump() -> Option<String> {
    if std::env::var("QSDB_WRITE_PROF").is_err() {
        return None;
    }
    let ms = |a: &AtomicU64| a.load(AtomicOrd::Relaxed) as f64 / 1e6;
    Some(format!(
        "calls={} lock={:.1}ms seg={:.1}ms man_read={:.1}ms commit={:.1}ms idx={:.1}ms",
        P_CALLS.load(AtomicOrd::Relaxed),
        ms(&P_LOCK_NS),
        ms(&P_SEG_NS),
        ms(&P_MAN_READ_NS),
        ms(&P_MAN_COMMIT_NS),
        ms(&P_IDX_NS),
    ))
}

/// overwrite（单分区原子替换；空输入 = 清空分区）。
///
/// `expected` = **乐观并发（CAS）**：只在该分区当前 `generation` 恰好等于它时才覆盖，
/// 否则报 [`QsdbError::WriteConflict`]。`None` = 现状行为（无条件覆盖）。
///
/// 为什么要这个：两个写者对同一分区各发 overwrite 时，锁保证"每次替换原子、清单
/// 完整"（不会坏数据），但语义上是**静默 last-writer-wins**——先写者整批数据无声
/// 消失。Delta/Iceberg 对同分区并发 overwrite 都是**冲突报错让调用方裁决**。
/// 机制现成：`generation` 是分区提交计数器（`PartitionManifest::commit` 每次 +1），
/// 比对放在**锁内**（读到旧 manifest 之后、写新段之前）才是真 CAS。
pub(crate) fn overwrite_partition(
    store: &Store,
    staging: Option<&crate::staging::Staging>,
    table_dir: &Path,
    def: &TableDef,
    values: &[crate::types::Literal],
    batches: &[RecordBatch],
    expected: Option<u64>,
) -> Result<WriteResult> {
    let schema = data_schema(def);
    let batches = pad_loose_batches(def, batches, &schema)?;
    let batches: &[RecordBatch] = batches.as_ref();
    let file_schema = validate_batches(batches, &schema)?;
    check_constraints(def, batches)?;
    let dir = pdir(table_dir, def, values)?;
    store.create_dir_all(&dir)?;
    if !store.is_local() {
        #[cfg(feature = "remote")]
        return overwrite_partition_remote(
            store,
            staging,
            table_dir,
            def.name.as_str(),
            def,
            values,
            &dir,
            batches,
            &schema,
            &file_schema,
            expected,
        );
        #[cfg(not(feature = "remote"))]
        {
            let _ = staging;
            return Err(QsdbError::unsupported("remote store requires feature 's3'"));
        }
    }
    let _guard = PartitionLock::acquire_local(&lock_path(&dir))?;

    // 旧 manifest（不存在 = 新分区）
    let old = PartitionManifest::read(store, &dir)?;

    // ★ CAS 检查：**锁内、写段之前**——写段之后再拒会把新段留成孤儿
    if let Some(exp) = expected {
        check_expected_generation(&dir, old.as_ref(), exp)?;
    }

    let mut result = WriteResult::default();
    let mut new_segments: Vec<SegmentEntry> = Vec::new();
    if let Some(fs) = &file_schema {
        let entry = write_segment(
            store,
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
    manifest.commit(store, &dir)?;
    if store.is_local() {
        sync_index(store, table_dir, &dir, &manifest);
    }
    drop(_guard);

    // 旧数据文件不立即回收（本地/远端同语义）：留在原地由 GC 按安全期清理
    // ——立即移动会让「按旧 manifest 快照规划的在途扫描」打开失败
    // （soak 实测竞争；安全期 = 替换 manifest 的年龄，见 api::gc）
    let _ = old_files;

    Ok(result)
}

// --- 远端写协议（feature `s3`，存储设计 §7.2 ProtocolOCC） ------------------

/// manifest OCC 追加（含 **file_name 去重**——staging 补传/重放幂等：
/// 同名 segment 不重复计入，at-least-once 之上得到 exactly-once 可见状态）。
#[cfg(feature = "remote")]
pub(crate) fn occ_append_entry(
    store: &Store,
    dir: &Path,
    def: &TableDef,
    values: &[crate::types::Literal],
    entry: &SegmentEntry,
) -> Result<()> {
    let mpath = PartitionManifest::manifest_path(dir);
    crate::remote::with_partition_mutex(dir, || {
        crate::remote::occ_commit(store, &mpath, |cur| {
            let mut m = cur.unwrap_or_else(|| {
                PartitionManifest::new(named_values(def, values), def.schema_fingerprint())
            });
            // 指纹一致 或 旧 schema 为当前前缀（ALTER ADD COLUMN）→ 升格
            if m.schema_fingerprint != def.schema_fingerprint() {
                if !def.prefix_fingerprints().contains(&m.schema_fingerprint) {
                    return Err(QsdbError::meta(
                        "table schema changed since partition was written; use overwrite_partition",
                    ));
                }
                m.schema_fingerprint = def.schema_fingerprint();
            }
            // 重放：基于最新 cur 追加（并发写者的 segment 不丢）；同名去重
            m.generation += 1;
            if !m.segments.iter().any(|s| s.file_name == entry.file_name) {
                m.segments.push(entry.clone());
            }
            Ok(m)
        })
    })
}

/// append（远端）：segment PUT（不可变，无协议）→ manifest OCC 追加。
/// 进程内分区 Mutex 串行化（§8）——跨进程/多实例由 OCC 收敛，无锁多写者。
/// staging 启用时改为本地暂存（写盘即 ACK），网络段由 flush_staged 补。
#[cfg(feature = "remote")]
#[allow(clippy::too_many_arguments)]
fn append_partition_remote(
    store: &Store,
    staging: Option<&crate::staging::Staging>,
    table_dir: &Path,
    table: &str,
    def: &TableDef,
    values: &[crate::types::Literal],
    dir: &Path,
    batches: &[RecordBatch],
    schema: &SchemaRef,
    file_schema: &SchemaRef,
) -> Result<WriteResult> {
    let col_names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
    let entry = match staging {
        Some(st) => stage_segment(
            st,
            store.data_sync(),
            table_dir,
            table,
            def,
            values,
            dir,
            batches,
            file_schema,
            col_names,
            false,
            None, // append 无 CAS（锁内 read-merge-write，天然合并语义）
        )?,
        None => put_segment_remote(
            store,
            dir,
            batches,
            file_schema,
            writer_props(def)?,
            &col_names,
        )?,
    };
    if staging.is_none() {
        occ_append_entry(store, dir, def, values, &entry)?;
    }
    Ok(WriteResult {
        rows: entry.row_count,
        bytes: entry.byte_size,
        segments: 1,
    })
}

/// segment 序列化到本地 staging（§11：写盘即 ACK；网络段由 flush 补）。
#[cfg(feature = "remote")]
#[allow(clippy::too_many_arguments)]
fn stage_segment(
    st: &crate::staging::Staging,
    durable: bool,
    table_dir: &Path,
    table: &str,
    def: &TableDef,
    values: &[crate::types::Literal],
    dir: &Path,
    batches: &[RecordBatch],
    file_schema: &SchemaRef,
    col_names: Vec<String>,
    overwrite: bool,
    expected_generation: Option<u64>,
) -> Result<SegmentEntry> {
    let seg_name = format!("seg_{}.parquet", uuid::Uuid::now_v7().simple());
    let rel = dir
        .strip_prefix(table_dir)
        .unwrap_or(dir)
        .to_string_lossy()
        .replace('\\', "/");
    let rec = crate::staging::StagedRecord {
        version: 1,
        table: table.to_string(),
        partition_rel: rel,
        partition_values: values.to_vec(),
        overwrite,
        expected_generation,
        entry: SegmentEntry {
            file_name: seg_name,
            row_count: 0,
            byte_size: 0,
            created_at: now_ms() as i64,
            column_stats: vec![],
        },
    };
    let props = writer_props(def)?;
    let fs = Arc::clone(file_schema);
    let names_for_closure = col_names;
    let entry = st.stage(&rec, |parquet| {
        // tmp 先行：崩溃残留 tmp 由 GC 清扫；sidecar 由 stage() 后写
        let tmp = crate::fsutil::tmp_sibling(parquet);
        let mut f = File::create(&tmp)
            .map_err(|e| QsdbError::meta(format!("create {} failed: {e}", tmp.display())))?;
        let (rows, stats) = serialize_segment(&mut f, batches, &fs, props, &names_for_closure)?;
        if durable {
            // staged 数据已 ACK 给客户端——掉电档下落盘后才算数
            f.sync_all()
                .map_err(|e| QsdbError::meta(format!("fsync staged failed: {e}")))?;
        }
        let bytes = std::fs::metadata(&tmp)
            .map_err(|e| QsdbError::meta(format!("stat staged failed: {e}")))?
            .len();
        crate::fsutil::atomic_replace(&tmp, parquet)?;
        Ok(SegmentEntry {
            row_count: rows,
            byte_size: bytes,
            column_stats: stats,
            ..rec.entry.clone()
        })
    })?;
    Ok(entry)
}

/// manifest OCC 替换提交（overwrite/compaction/clear 共用；重放只继承
/// 最新 generation，segments 以本轮为准——旧 segment 不搬 trash，
/// 不被引用即不可见，引用 GC 安全期后回收，§12）。
#[cfg(feature = "remote")]
pub(crate) fn occ_replace_entries(
    store: &Store,
    dir: &Path,
    def: &TableDef,
    values: &[crate::types::Literal],
    entries: &[SegmentEntry],
    expected: Option<u64>,
) -> Result<()> {
    let mpath = PartitionManifest::manifest_path(dir);
    crate::remote::with_partition_mutex(dir, || {
        crate::remote::occ_commit(store, &mpath, |cur| {
            // CAS 在 **OCC 闭包内**校验：每次重试都拿最新 manifest，故并发写者
            // 之间的窗口被 etag 条件写兜住（不是"先读后写"那种假 CAS）
            if let Some(exp) = expected {
                check_expected_generation(dir, cur.as_ref(), exp)?;
            }
            let mut m = PartitionManifest::new(named_values(def, values), def.schema_fingerprint());
            // **generation += 1（与 append 及本地 `commit()` 同口径）**：
            // 此前只写 `= cur.generation` 而不加一 —— 远端 overwrite **根本不推进
            // generation**。后果有两个，都很难看：
            //   ① F24 的 CAS 在远端形同虚设（覆盖后 generation 不变 → 过期写者检测不到）；
            //   ② 生态侧以 generation 为增量判据（qsdb-sync 契约）→ **远端 overwrite
            //      会被同步工具当成"没变"而漏掉**。
            // 本地路径无此问题：那边走 `PartitionManifest::commit()`，它自带 +1。
            m.generation = cur.as_ref().map_or(1, |c| c.generation + 1);
            m.segments = entries.to_vec();
            Ok(m)
        })
    })
}

/// overwrite（远端）：segment PUT → OCC 替换提交。
/// staging 启用时改为本地暂存（overwrite 标记），flush_staged 补网络段。
#[cfg(feature = "remote")]
#[allow(clippy::too_many_arguments)]
fn overwrite_partition_remote(
    store: &Store,
    staging: Option<&crate::staging::Staging>,
    table_dir: &Path,
    table: &str,
    def: &TableDef,
    values: &[crate::types::Literal],
    dir: &Path,
    batches: &[RecordBatch],
    schema: &SchemaRef,
    file_schema: &Option<SchemaRef>,
    expected: Option<u64>,
) -> Result<WriteResult> {
    let col_names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
    let mut result = WriteResult::default();
    let mut new_segments: Vec<SegmentEntry> = Vec::new();
    if let Some(fs) = file_schema {
        let entry = match staging {
            Some(st) => stage_segment(
                st,
                store.data_sync(),
                table_dir,
                table,
                def,
                values,
                dir,
                batches,
                fs,
                col_names,
                true,
                expected,
            )?,
            None => put_segment_remote(store, dir, batches, fs, writer_props(def)?, &col_names)?,
        };
        result = WriteResult {
            rows: entry.row_count,
            bytes: entry.byte_size,
            segments: 1,
        };
        new_segments.push(entry);
    }
    if staging.is_none() {
        occ_replace_entries(store, dir, def, values, &new_segments, expected)?;
    }
    // staging 路径：写盘即 ACK，提交在 `flush_staged` —— 期望值随暂存记录一起落盘，
    // 由 flush 提交时校验（见 `StagedRecord::expected_generation`）
    Ok(result)
}

/// CAS 校验：分区当前 `generation` 必须等于期望值（不存在 = 0 —— 首次写入传 0 即合法）。
fn check_expected_generation(
    dir: &Path,
    old: Option<&PartitionManifest>,
    expected: u64,
) -> Result<()> {
    let actual = old.map_or(0, |m| m.generation);
    if actual != expected {
        return Err(QsdbError::WriteConflict {
            partition: dir
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_else(|| dir.to_string_lossy().into_owned()),
            expected,
            actual,
        });
    }
    Ok(())
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
    use crate::store::Store;
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

    fn tmp_root(tag: &str) -> (Store, PathBuf) {
        let d = std::env::temp_dir()
            .join("qsdb_write_test")
            .join(format!("{tag}_{}", uuid::Uuid::now_v7().simple()));
        std::fs::create_dir_all(&d).unwrap();
        (Store::local(&d), d)
    }

    #[test]
    fn append_creates_manifest_and_files() {
        let (store, root) = tmp_root("append");
        let d = def();
        let tdir = root.join("t");
        let vals = [Literal::Date(1)];
        let r = append_partition(
            &store,
            None,
            &tdir,
            &d,
            &vals,
            &[data_batch(vec![Some(10)])],
        )
        .unwrap();
        assert_eq!(r.rows, 1);
        let pdir = tdir.join("dt=1970-01-02");
        let m = PartitionManifest::read(&store, &pdir).unwrap().unwrap();
        assert_eq!(m.segments.len(), 1);
        assert_eq!(m.total_rows(), 1);
        assert_eq!(m.generation, 1);
        assert!(pdir.join(&m.segments[0].file_name).exists());
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn append_accumulates_and_overwrite_replaces() {
        let (store, root) = tmp_root("ow");
        let d = def();
        let tdir = root.join("t");
        let vals = [Literal::Date(1)];
        append_partition(
            &store,
            None,
            &tdir,
            &d,
            &vals,
            &[data_batch(vec![Some(10)])],
        )
        .unwrap();
        append_partition(
            &store,
            None,
            &tdir,
            &d,
            &vals,
            &[data_batch(vec![Some(20)])],
        )
        .unwrap();
        let pdir = tdir.join("dt=1970-01-02");
        assert_eq!(
            PartitionManifest::read(&store, &pdir)
                .unwrap()
                .unwrap()
                .total_rows(),
            2
        );

        // overwrite：只剩新数据
        overwrite_partition(
            &store,
            None,
            &tdir,
            &d,
            &vals,
            &[data_batch(vec![Some(7), Some(8)])],
            None,
        )
        .unwrap();
        let m = PartitionManifest::read(&store, &pdir).unwrap().unwrap();
        assert_eq!(m.segments.len(), 1);
        assert_eq!(m.total_rows(), 2);
        assert_eq!(m.generation, 3);
        // 延迟回收语义：旧文件留原地（不被 manifest 引用 = 孤儿），
        // GC 安全期后清理——不再立即进 trash（保护在途扫描）
        // 空 overwrite = 清空分区
        overwrite_partition(&store, None, &tdir, &d, &vals, &[], None).unwrap();
        let m = PartitionManifest::read(&store, &pdir).unwrap().unwrap();
        assert_eq!(m.total_rows(), 0);
        assert!(m.segments.is_empty());
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn schema_mismatch_rejected() {
        let (store, root) = tmp_root("mismatch");
        let d = def();
        let tdir = root.join("t");
        let vals = [Literal::Date(1)];
        append_partition(&store, None, &tdir, &d, &vals, &[data_batch(vec![Some(1)])]).unwrap();
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
        assert!(append_partition(&store, None, &tdir, &d, &vals, &[b]).is_err());
        // 错类型
        let schema2 = Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, true)]));
        let b2 =
            RecordBatch::try_new(schema2, vec![Arc::new(StringArray::from(vec!["x"]))]).unwrap();
        assert!(append_partition(&store, None, &tdir, &d, &vals, &[b2]).is_err());
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn stats_recorded_in_manifest() {
        let (store, root) = tmp_root("stats");
        let d = def();
        let tdir = root.join("t");
        append_partition(
            &store,
            None,
            &tdir,
            &d,
            &[Literal::Date(1)],
            &[data_batch(vec![Some(5), None])],
        )
        .unwrap();
        let pdir = tdir.join("dt=1970-01-02");
        let m = PartitionManifest::read(&store, &pdir).unwrap().unwrap();
        let (name, vstats) = &m.segments[0].column_stats[0];
        assert_eq!(name, "v");
        assert_eq!(vstats.min, Some(Literal::Int64(5)));
        assert_eq!(vstats.max, Some(Literal::Int64(5)));
        assert_eq!(vstats.null_count, 1);
        std::fs::remove_dir_all(&root).ok();
    }
}
