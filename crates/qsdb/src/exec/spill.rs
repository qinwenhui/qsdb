//! 排序外排（spill）：内存超限时分块排序 → 临时 parquet 落盘 → k 路归并。
//!
//! - 每个 run 内部已按排序键有序；归并做活跃 run 线性选优 + 行级键比较
//!   （arrow `make_comparator`；run 数通常个位数，线性扫在批粒度下成本可接受）
//! - 内存上限来自 QueryContext（排序物化记账触发落盘）
//! - 临时文件在系统临时目录独立子目录，Drop 兜底清理

use std::cmp::Ordering;
use std::collections::HashMap;
use std::fs::{self, File};
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, UInt32Array};
use arrow::compute::{concat, take};

use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::arrow_writer::ArrowWriter;

use crate::error::{QsdbError, Result};

use super::SortKey;

// ---------------------------------------------------------------------------
// 临时目录
// ---------------------------------------------------------------------------

/// spill 临时目录（独立子目录，Drop 清理）。
pub struct SpillDir {
    path: PathBuf,
    counter: u32,
}

impl SpillDir {
    /// 在**配置的溢出根目录**下开一个本次查询专用的子目录（`<base>/qsdb_spill_<uuid>/`）。
    ///
    /// `base = None` → 系统临时目录（**库默认**：嵌入方不配置就不改变既有行为）。
    /// `qsdb server` 默认给 `<库根>/_spill`——溢出量可以很大（5M 行排序就是几十上百
    /// MB），落系统盘可能把小分区写满（生产上踩过）。
    pub fn new_in(base: Option<&std::path::Path>) -> Self {
        let path = match base {
            Some(b) => b.join(format!("qsdb_spill_{}", uuid::Uuid::now_v7().simple())),
            None => std::env::temp_dir().join(format!("qsdb_spill_{}", uuid::Uuid::now_v7().simple())),
        };
        let _ = std::fs::create_dir_all(&path);
        Self { path, counter: 0 }
    }

    pub fn new() -> Self {
        let path =
            std::env::temp_dir().join(format!("qsdb_spill_{}", uuid::Uuid::now_v7().simple()));
        let _ = fs::create_dir_all(&path);
        Self { path, counter: 0 }
    }

    fn next_run(&mut self) -> PathBuf {
        self.counter += 1;
        self.path.join(format!("run_{:06}.parquet", self.counter))
    }
}

impl Default for SpillDir {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for SpillDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

// ---------------------------------------------------------------------------
// run 读写
// ---------------------------------------------------------------------------

/// 排序 chunk（一批或多批合并）按键排序后落盘为一个 run。
pub fn write_sorted_run(
    dir: &mut SpillDir,
    batches: &[RecordBatch],
    keys: &[SortKey],
    schema: &Arc<arrow::datatypes::Schema>,
) -> Result<PathBuf> {
    let batch = concat_batches(batches, schema)?;
    let sorted = sort_batch(&batch, keys)?;
    let path = dir.next_run();
    let props = parquet::file::properties::WriterProperties::builder()
        .set_compression(parquet::basic::Compression::ZSTD(
            parquet::basic::ZstdLevel::try_new(1)
                .map_err(|e| QsdbError::invalid(format!("zstd: {e}")))?,
        ))
        .build();
    let file = File::create(&path)
        .map_err(|e| QsdbError::meta(format!("spill create {}: {e}", path.display())))?;
    let mut w = ArrowWriter::try_new(file, Arc::clone(schema), Some(props))
        .map_err(|e| QsdbError::meta(format!("spill writer: {e}")))?;
    w.write(&sorted)
        .map_err(|e| QsdbError::meta(format!("spill write: {e}")))?;
    w.close()
        .map_err(|e| QsdbError::meta(format!("spill close: {e}")))?;
    Ok(path)
}

/// 无序 run：批合并后**直接落盘**（不排序）。
///
/// 用于**聚合/去重的分区溢出**：先把输入行按 group key 的 hash 分到 P 个分区
/// 各自落盘，收尾时逐分区读回、分区内聚合——同一组的行必然落在同一分区，
/// 故每分区可独立聚合出正确结果（见 `exec/agg_spill.rs`）。
pub fn write_run(
    dir: &mut SpillDir,
    batches: &[RecordBatch],
    schema: &Arc<arrow::datatypes::Schema>,
) -> Result<PathBuf> {
    let batch = concat_batches(batches, schema)?;
    let path = dir.next_run();
    let props = parquet::file::properties::WriterProperties::builder()
        .set_compression(parquet::basic::Compression::ZSTD(
            parquet::basic::ZstdLevel::try_new(1)
                .map_err(|e| QsdbError::invalid(format!("zstd: {e}")))?,
        ))
        .build();
    let file = File::create(&path)
        .map_err(|e| QsdbError::meta(format!("spill create {}: {e}", path.display())))?;
    let mut w = ArrowWriter::try_new(file, Arc::clone(schema), Some(props))
        .map_err(|e| QsdbError::meta(format!("spill writer: {e}")))?;
    w.write(&batch)
        .map_err(|e| QsdbError::meta(format!("spill write: {e}")))?;
    w.close()
        .map_err(|e| QsdbError::meta(format!("spill close: {e}")))?;
    Ok(path)
}

/// 读取一个 run 为批列表（run 已排序）。
pub fn read_run(path: &std::path::Path) -> Result<Vec<RecordBatch>> {
    let file = File::open(path)
        .map_err(|e| QsdbError::meta(format!("spill open {}: {e}", path.display())))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| QsdbError::meta(format!("spill footer: {e}")))?;
    let reader = builder
        .with_batch_size(8192)
        .build()
        .map_err(|e| QsdbError::meta(format!("spill reader: {e}")))?;
    let mut out = Vec::new();
    for b in reader {
        out.push(b.map_err(|e| QsdbError::meta(format!("spill read: {e}")))?);
    }
    Ok(out)
}

/// 批内按排序键排序（升序/降序 + pg NULL 序）。
pub fn sort_batch(batch: &RecordBatch, keys: &[SortKey]) -> Result<RecordBatch> {
    use arrow::compute::kernels::sort::{SortColumn, lexsort_to_indices};
    let mut sort_cols: Vec<SortColumn> = Vec::with_capacity(keys.len());
    for k in keys {
        let arr = crate::expr::eval::eval(&k.expr, batch)?;
        sort_cols.push(SortColumn {
            values: arr,
            options: Some(arrow::compute::SortOptions {
                descending: k.descending,
                nulls_first: k.descending,
            }),
        });
    }
    let indices = lexsort_to_indices(&sort_cols, None)
        .map_err(|e| QsdbError::invalid(format!("sort: {e}")))?;
    take_batch(batch, &indices)
}

fn take_batch(batch: &RecordBatch, indices: &UInt32Array) -> Result<RecordBatch> {
    let mut cols = Vec::with_capacity(batch.num_columns());
    for c in 0..batch.num_columns() {
        cols.push(
            take(batch.column(c), indices, None)
                .map_err(|e| QsdbError::invalid(format!("take: {e}")))?,
        );
    }
    RecordBatch::try_new_with_options(
        batch.schema(),
        cols,
        &RecordBatchOptions::new().with_row_count(Some(indices.len())),
    )
    .map_err(|e| QsdbError::invalid(format!("sort output: {e}")))
}

pub fn concat_batches(
    batches: &[RecordBatch],
    schema: &Arc<arrow::datatypes::Schema>,
) -> Result<RecordBatch> {
    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::clone(schema)));
    }
    if batches.len() == 1 {
        return Ok(batches[0].clone());
    }
    let n_cols = schema.fields().len();
    let mut cols = Vec::with_capacity(n_cols);
    for c in 0..n_cols {
        let parts: Vec<&dyn Array> = batches.iter().map(|b| b.column(c).as_ref()).collect();
        cols.push(concat(&parts).map_err(|e| QsdbError::invalid(format!("concat: {e}")))?);
    }
    let n: usize = batches.iter().map(|b| b.num_rows()).sum();
    RecordBatch::try_new_with_options(
        Arc::clone(schema),
        cols,
        &RecordBatchOptions::new().with_row_count(Some(n)),
    )
    .map_err(|e| QsdbError::invalid(format!("concat batch: {e}")))
}

// ---------------------------------------------------------------------------
// 行级单元格比较（归并专用；不依赖 arrow-ord comparator）
// ---------------------------------------------------------------------------

enum CellOrd {
    Vals(Ordering),
    NullVsVal,
    ValVsNull,
    BothNull,
}

macro_rules! prim_cmp {
    ($ty:ty, $a:expr, $b:expr, $ai_row:expr, $bi_row:expr) => {
        if $a.is_null($ai_row) && $b.is_null($bi_row) {
            CellOrd::BothNull
        } else if $a.is_null($ai_row) {
            CellOrd::NullVsVal
        } else if $b.is_null($bi_row) {
            CellOrd::ValVsNull
        } else {
            CellOrd::Vals(
                $a.value($ai_row)
                    .partial_cmp(&$b.value($bi_row))
                    .unwrap_or(Ordering::Equal),
            )
        }
    };
}

fn cmp_cell(a: &dyn Array, ai_row: usize, b: &dyn Array, bi_row: usize) -> CellOrd {
    use arrow::array::*;
    use arrow::datatypes::*;
    match a.data_type() {
        DataType::Int8 => {
            let (a, b) = (
                a.as_any().downcast_ref::<Int8Array>().unwrap(),
                b.as_any().downcast_ref::<Int8Array>().unwrap(),
            );
            prim_cmp!(i8, a, b, ai_row, bi_row)
        }
        DataType::Int16 => {
            let (a, b) = (
                a.as_any().downcast_ref::<Int16Array>().unwrap(),
                b.as_any().downcast_ref::<Int16Array>().unwrap(),
            );
            prim_cmp!(i16, a, b, ai_row, bi_row)
        }
        DataType::Int32 | DataType::Date32 => {
            let (a, b) = (
                a.as_any().downcast_ref::<Int32Array>().unwrap(),
                b.as_any().downcast_ref::<Int32Array>().unwrap(),
            );
            prim_cmp!(i32, a, b, ai_row, bi_row)
        }
        DataType::Int64 | DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let (a, b) = (
                a.as_any().downcast_ref::<Int64Array>().unwrap(),
                b.as_any().downcast_ref::<Int64Array>().unwrap(),
            );
            prim_cmp!(i64, a, b, ai_row, bi_row)
        }
        DataType::Float32 => {
            let (a, b) = (
                a.as_any().downcast_ref::<Float32Array>().unwrap(),
                b.as_any().downcast_ref::<Float32Array>().unwrap(),
            );
            prim_cmp!(f32, a, b, ai_row, bi_row)
        }
        DataType::Float64 => {
            let (a, b) = (
                a.as_any().downcast_ref::<Float64Array>().unwrap(),
                b.as_any().downcast_ref::<Float64Array>().unwrap(),
            );
            prim_cmp!(f64, a, b, ai_row, bi_row)
        }
        DataType::Utf8 => {
            let (a, b) = (
                a.as_any().downcast_ref::<StringArray>().unwrap(),
                b.as_any().downcast_ref::<StringArray>().unwrap(),
            );
            if a.is_null(ai_row) && b.is_null(bi_row) {
                CellOrd::BothNull
            } else if a.is_null(ai_row) {
                CellOrd::NullVsVal
            } else if b.is_null(bi_row) {
                CellOrd::ValVsNull
            } else {
                CellOrd::Vals(a.value(ai_row).cmp(b.value(bi_row)))
            }
        }
        DataType::Boolean => {
            let (a, b) = (
                a.as_any().downcast_ref::<BooleanArray>().unwrap(),
                b.as_any().downcast_ref::<BooleanArray>().unwrap(),
            );
            if a.is_null(ai_row) && b.is_null(bi_row) {
                CellOrd::BothNull
            } else if a.is_null(ai_row) {
                CellOrd::NullVsVal
            } else if b.is_null(bi_row) {
                CellOrd::ValVsNull
            } else {
                CellOrd::Vals(a.value(ai_row).cmp(&b.value(bi_row)))
            }
        }
        DataType::Decimal128(_, _) => {
            let (a, b) = (
                a.as_any().downcast_ref::<Decimal128Array>().unwrap(),
                b.as_any().downcast_ref::<Decimal128Array>().unwrap(),
            );
            prim_cmp!(i128, a, b, ai_row, bi_row)
        }
        _ => CellOrd::BothNull, // 不支持类型视作相等（排序键不应出现）
    }
}

// ---------------------------------------------------------------------------
// k 路归并
// ---------------------------------------------------------------------------

/// 一次选中：(cursor, 该 cursor 的批下标, 批内行号)
#[derive(Clone, Copy)]
struct Pick {
    cursor: usize,
    batch: usize,
    row: u32,
}

struct RunCursor {
    batches: Vec<RecordBatch>,
    /// 每批的键数组（与 keys 一一对应）
    key_cols: Vec<Vec<ArrayRef>>,
    batch_idx: usize,
    row_idx: usize,
}

impl RunCursor {
    fn new(batches: Vec<RecordBatch>, keys: &[SortKey]) -> Result<Self> {
        let mut key_cols = Vec::with_capacity(batches.len());
        for b in &batches {
            let mut ks = Vec::with_capacity(keys.len());
            for k in keys {
                ks.push(crate::expr::eval::eval(&k.expr, b)?);
            }
            key_cols.push(ks);
        }
        Ok(Self {
            batches,
            key_cols,
            batch_idx: 0,
            row_idx: 0,
        })
    }

    #[inline]
    fn valid(&self) -> bool {
        self.batch_idx < self.batches.len()
            && self.row_idx < self.batches[self.batch_idx].num_rows()
    }

    #[inline]
    fn advance(&mut self) {
        self.row_idx += 1;
        if self.batch_idx < self.batches.len()
            && self.row_idx >= self.batches[self.batch_idx].num_rows()
        {
            self.batch_idx += 1;
            self.row_idx = 0;
        }
    }
}

/// k 路归并已排序 run（每个 run = 批列表，可为内存尾块）。流式输出。
pub struct RunMerger {
    cursors: Vec<RunCursor>,
    out_batch_size: usize,
}

impl RunMerger {
    pub fn new(runs: Vec<Vec<RecordBatch>>, keys: &[SortKey]) -> Result<Self> {
        let mut cursors = Vec::with_capacity(runs.len());
        for batches in runs {
            if batches.iter().any(|b| b.num_rows() > 0) {
                cursors.push(RunCursor::new(batches, keys)?);
            }
        }
        Ok(Self {
            cursors,
            out_batch_size: 8192,
        })
    }

    /// 比较两个 cursor 当前行（key 序；全等按 run 序稳定）。
    fn cmp_cursors(&self, ai: usize, bi: usize, keys: &[SortKey]) -> Ordering {
        let a = &self.cursors[ai];
        let b = &self.cursors[bi];
        for (ki, k) in keys.iter().enumerate() {
            let a_arr = &a.key_cols[a.batch_idx][ki];
            let b_arr = &b.key_cols[b.batch_idx][ki];
            let ord = cmp_cell(a_arr.as_ref(), a.row_idx, b_arr.as_ref(), b.row_idx);
            // pg 序：ASC NULLS LAST / DESC NULLS FIRST（与 SortExec 一致）
            let ord = match (ord, k.descending) {
                (CellOrd::NullVsVal, false) => Ordering::Greater, // NULL LAST
                (CellOrd::NullVsVal, true) => Ordering::Less,     // NULL FIRST
                (CellOrd::ValVsNull, false) => Ordering::Less,
                (CellOrd::ValVsNull, true) => Ordering::Greater,
                (CellOrd::BothNull, _) => Ordering::Equal,
                (CellOrd::Vals(o), false) => o,
                (CellOrd::Vals(o), true) => o.reverse(),
            };
            if ord != Ordering::Equal {
                return ord;
            }
        }
        ai.cmp(&bi)
    }

    /// 拉取下一输出批（None = 归并完成）。
    pub fn next_batch(&mut self, keys: &[SortKey]) -> Result<Option<RecordBatch>> {
        if self.cursors.is_empty() {
            return Ok(None);
        }
        let mut picks: Vec<Pick> = Vec::with_capacity(self.out_batch_size);
        loop {
            let mut best: Option<usize> = None;
            for i in 0..self.cursors.len() {
                if !self.cursors[i].valid() {
                    continue;
                }
                match best {
                    None => best = Some(i),
                    Some(b) => {
                        if self.cmp_cursors(i, b, keys) == Ordering::Less {
                            best = Some(i);
                        }
                    }
                }
            }
            let Some(bi) = best else { break };
            let c = &self.cursors[bi];
            picks.push(Pick {
                cursor: bi,
                batch: c.batch_idx,
                row: c.row_idx as u32,
            });
            self.cursors[bi].advance();
            if picks.len() >= self.out_batch_size {
                break;
            }
        }
        if picks.is_empty() {
            return Ok(None);
        }
        assemble_picks(&self.cursors, &picks)
    }
}

/// picks → 输出批：按（cursor, batch）分组 take 成段，再按 picks 序 interleave。
fn assemble_picks(cursors: &[RunCursor], picks: &[Pick]) -> Result<Option<RecordBatch>> {
    let schema = cursors[picks[0].cursor].batches[0].schema();
    let n_cols = schema.fields().len();
    let total = picks.len();

    // 段 id：连续同 (cursor, batch) 的 pick 归一段
    let mut seg_of: Vec<usize> = Vec::with_capacity(total);
    let mut seg_rows: Vec<Vec<u32>> = Vec::new();
    let mut seg_src: Vec<(usize, usize)> = Vec::new(); // (cursor, batch)
    for p in picks {
        let key = (p.cursor, p.batch);
        if seg_src.last() != Some(&key) {
            seg_src.push(key);
            seg_rows.push(Vec::new());
        }
        seg_rows.last_mut().unwrap().push(p.row);
        seg_of.push(seg_rows.len() - 1);
    }

    let mut cols: Vec<ArrayRef> = Vec::with_capacity(n_cols);
    for c in 0..n_cols {
        let mut segments: Vec<ArrayRef> = Vec::with_capacity(seg_src.len());
        for (si, &(ci, bi)) in seg_src.iter().enumerate() {
            let cursor = &cursors[ci];
            let batch = &cursor.batches[bi];
            let indices = UInt32Array::from(seg_rows[si].clone());
            segments.push(
                take(batch.column(c), &indices, None)
                    .map_err(|e| QsdbError::invalid(format!("merge take: {e}")))?,
            );
        }
        // interleave 索引：(段 id, 段内行号)——picks 顺序即段内顺序
        let mut within: HashMap<usize, usize> = HashMap::new();
        let idx_pairs: Vec<(usize, usize)> = seg_of
            .iter()
            .map(|&si| {
                let w = within.entry(si).or_insert(0);
                let pair = (si, *w);
                *w += 1;
                pair
            })
            .collect();
        let seg_refs: Vec<&dyn Array> = segments.iter().map(|a| a.as_ref()).collect();
        cols.push(
            arrow::compute::interleave(&seg_refs, &idx_pairs)
                .map_err(|e| QsdbError::invalid(format!("merge interleave: {e}")))?,
        );
    }
    RecordBatch::try_new_with_options(
        schema,
        cols,
        &RecordBatchOptions::new().with_row_count(Some(total)),
    )
    .map(Some)
    .map_err(|e| QsdbError::invalid(format!("merge output: {e}")))
}
