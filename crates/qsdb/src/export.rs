//! **湖导出**（F9 / A3）：`COPY (SELECT …) TO '<目录>' (FORMAT PARQUET, PARTITION_BY (…))`。
//!
//! 设计见 `docs/提交事件订阅与湖导出设计.md` §2.2。产出的布局与 qsdb-sync 的
//! `sink-parquet`、以及 Spark/Hive 生态一致：
//!
//! ```text
//! <目录>/_common_metadata                 全 schema（含分区列）的零行 parquet
//! <目录>/dt=2026-01-01/region=cn/part-00000.parquet
//! <目录>/dt=2026-01-02/region=cn/part-00000.parquet
//! ```
//!
//! # 三条口径（都是有理由的，别随手改）
//!
//! 1. **分区列不进数据文件**，只在目录名里（Spark 惯例）。理由与我们自己的
//!    读侧（`scan::resolve_read_parquet` 的 hive 语义，F12）对得上：读的时候
//!    是从 `k=v` 路径段把列**组装**出来的，文件里再来一份就会撞名。
//! 2. **目标是目录，不是文件**。哪怕不写 `PARTITION_BY` 也可能产出多个
//!    `part-*.parquet`（分块并行写，见下的 `CHUNK_ROWS`）—— 说是文件就是骗人。
//! 3. **目标目录非空 ⇒ 报错**，不做静默覆盖。理由：残留的旧 `part-*.parquet`
//!    会被读成**多余的数据**，而那是一次"看起来成功的导出"。
//!
//! # 内存与并行
//!
//! 轮次式：读满 `ROUND_ROWS` 行 → 切成若干 `CHUNK_ROWS` 的块 → **`par_iter`
//! 并行编码**。内存上界 ≈ 一轮的缓冲（不是整个结果集），并行度 ≈ 块数。
//! 这是 `真机基准对账.md` 里 `export_parquet` 的正解（3.58s → **0.766s**，
//! vs DuckDB 0.862s）：根因不是调参，是**没有导出原语**时调用方各自拿到的
//! arrow-rs 单线程水平。
//!
//! ⚠️ **已知取舍（刻意保留，不是没想到）**：有 `PARTITION_BY` 时 part 文件数
//! = `分区数 × 轮次数`（实测 365 分区 × 10 轮 = 3650 个 ~12KB 小文件）。
//! 根因是内存上界按**全局行数**卡、不是按分区卡 —— 分区一多，每轮落到每个
//! 分区的行就少。改成"攒到 `CHUNK_ROWS` 再刷"能把文件数压下来，但缓冲会变成
//! `分区数 × CHUNK_ROWS`（365 × 64K 行），与**低占用**定位冲突 ——
//! 按本仓定则（速度 vs 占用冲突时默认选低占用）保留现状。
//! 后续两项可做：自适应刷盘、导出后小文件合并。

use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::Array;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use rayon::prelude::*;

use crate::error::{QsdbError, Result};
use crate::store::Store;
use crate::types::LogicalType;

/// 一个 part 文件的目标行数。切块的目的**不是**控制文件大小，而是**制造并行度**
/// （一个 parquet 文件只能串行编码）。
const CHUNK_ROWS: u64 = 64 * 1024;

/// 一轮读入多少行。内存上界的来源：一轮缓冲完 → 并行写掉 → 再读下一轮。
const ROUND_ROWS: u64 = 512 * 1024;

/// `_common_metadata`：与 qsdb-sync `sink-parquet` 同名同义（全 schema、零行）。
/// 它是**给工具看的**（`schema_of` 那种"不用扫数据文件就能拿到全 schema"），
/// pyarrow/Spark 会把它当隐藏文件跳过。
const COMMON_METADATA: &str = "_common_metadata";

/// 导出格式。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExportFormat {
    /// parquet + hive 目录布局（`docs/提交事件订阅与湖导出设计.md` §2.2）。
    Parquet,
    /// CSV（`header` 控制表头）。**单文件**。
    ///
    /// ⚠️ 与 parquet 的两点不同，都是**刻意的**：
    ///
    /// 1. **不支持 `PARTITION_BY`**。本仓的 `read_csv` **没有 hive 目录语义**
    ///    （只有 `read_parquet` 有）⇒ 带分区写出去的数据**我们自己读不回来**。
    ///    "写得出、读不回"是本仓一路在拒的形态，不能因为"别的引擎能读"就放行。
    ///    （`translate` 层就拒，见 `ParsedStatement::CopyTo` 的构造。）
    /// 2. **只支持本地**。`Store` 的远端写是**整体 PUT**（`put_object(&[u8])`
    ///    没有流式接口）⇒ 单文件 CSV 要么把整个输出攒在内存、要么先落本地再整体
    ///    上传，两条都与低占用冲突。远端请用 parquet。
    Csv { header: bool },
}

/// 导出结果。
pub(crate) struct ExportStats {
    pub rows: u64,
    pub files: u64,
    pub bytes: u64,
    /// 落到几个分区目录（无 `PARTITION_BY` = 1）
    pub partitions: u64,
}

/// 一次导出。
///
/// - `schema`：查询输出 schema（**含**分区列）。
/// - `partition_by`：分区列名（大小写不敏感匹配 `schema`）。
/// - `next`：拉下一批（`Ok(None)` = 结束）。由调用方绑着 exec + `ctx.check()`。
pub(crate) fn write_dataset(
    store: &Store,
    target: &str,
    schema: &SchemaRef,
    partition_by: &[String],
    format: ExportFormat,
    next: &mut dyn FnMut() -> Result<Option<RecordBatch>>,
) -> Result<ExportStats> {
    match format {
        ExportFormat::Parquet => write_parquet_dataset(store, target, schema, partition_by, next),
        ExportFormat::Csv { header } => {
            write_csv_dataset(store, target, schema, partition_by, header, next)
        }
    }
}

fn write_parquet_dataset(
    store: &Store,
    target: &str,
    schema: &SchemaRef,
    partition_by: &[String],
    next: &mut dyn FnMut() -> Result<Option<RecordBatch>>,
) -> Result<ExportStats> {
    let plan = Plan::new(schema, partition_by)?;
    let root = plan_target(target)?;
    ensure_empty_dir(store, &root)?;

    let props = writer_props();
    let props_kept = props.clone();
    let mut w = Writer {
        store,
        root: root.clone(),
        plan,
        props,
        next_ix: BTreeMap::new(),
        rows: 0,
        files: 0,
        bytes: 0,
        dirs: BTreeMap::new(),
    };

    // 轮次式：读满一轮 → 写掉。缓冲有界，编码并行。
    let mut buf: Vec<RecordBatch> = Vec::new();
    let mut buf_rows: u64 = 0;
    while let Some(b) = next()? {
        buf_rows += b.num_rows() as u64;
        buf.push(b);
        if buf_rows >= ROUND_ROWS {
            w.flush(std::mem::take(&mut buf))?;
            buf_rows = 0;
        }
    }
    if !buf.is_empty() {
        w.flush(buf)?;
    }

    let partitions = w.dirs.len() as u64;
    let (rows, files, bytes) = (w.rows, w.files, w.bytes);
    drop(w);

    // `_common_metadata` **放在最后**写：导出中途失败时它缺席 —— 那本身就是
    // "这个目录不完整"的信号（先写它的话，半个导出看起来是完整的）。
    write_common_metadata(store, &root, schema, &props_kept)?;

    Ok(ExportStats {
        rows,
        files,
        bytes,
        partitions: partitions.max(1),
    })
}

// ---------------------------------------------------------------------------
// CSV（`FORMAT CSV`）
// ---------------------------------------------------------------------------

/// CSV 单文件导出。
///
/// **为什么是单文件而不是目录**（与 parquet 那条刻意不同）：CSV 的消费者
/// （Excel / pandas / 别的库的 `LOAD DATA`）要的就是**一个文件**；而 parquet
/// 那边我们把它做成目录是因为**可能出多个 part**（分块并行编码）——
/// 这里用"分块并行**格式化**、按序写进同一个文件"绕开了那个必要性。
fn write_csv_dataset(
    store: &Store,
    target: &str,
    schema: &SchemaRef,
    partition_by: &[String],
    header: bool,
    next: &mut dyn FnMut() -> Result<Option<RecordBatch>>,
) -> Result<ExportStats> {
    if !partition_by.is_empty() {
        // 与 `ExportFormat::Csv` 的文档同一条理由：`read_csv` 没有 hive 目录语义，
        // 带分区写出去就**我们自己读不回来**。
        return Err(QsdbError::unsupported(
            "CSV 不支持 PARTITION_BY —— 本仓的 `read_csv` **没有 hive 目录语义**\n             （只有 `read_parquet` 有），分区列写出去就重建不回来，等于导出一份\n             我们自己读不了的数据。要分区导出请用 `FORMAT PARQUET`（出 + 读闭环）",
        ));
    }
    if !store.is_local() {
        return Err(QsdbError::unsupported(
            "远端导出暂不支持 `FORMAT CSV`：Store 的远端写是**整体 PUT**\n             （没有流式接口）⇒ 单文件 CSV 得把整个输出攒在内存里，与低占用冲突。\n             远端请用 `FORMAT PARQUET`",
        ));
    }
    let dest = csv_target(target)?;
    if store.exists(&dest) {
        return Err(QsdbError::invalid(format!(
            "导出目标 `{}` 已存在 —— **不做静默覆盖**（同 parquet 那条）。\n             请先删掉它，或换一个路径",
            dest.display()
        )));
    }

    let tmp = crate::fsutil::tmp_sibling(&dest);
    let mut rows: u64 = 0;
    {
        let mut f = std::fs::File::create(&tmp)
            .map_err(|e| QsdbError::meta(format!("create {} failed: {e}", tmp.display())))?;
        // 表头**写一次**（后面的块都不带）—— 用零行批触发 arrow 的 header 分支，
        // 这样"零行结果 + HEADER"也会得到一份只有表头的文件（而不是空文件）。
        if header {
            f.write_all(&encode_csv(
                &[RecordBatch::new_empty(Arc::clone(schema))],
                true,
            )?)
            .map_err(|e| QsdbError::meta(format!("csv header failed: {e}")))?;
        }

        // 轮次式：与 parquet 同款 —— 缓冲有界，**格式化并行**。
        let mut buf: Vec<RecordBatch> = Vec::new();
        let mut buf_rows: u64 = 0;
        while let Some(b) = next()? {
            buf_rows += b.num_rows() as u64;
            rows += b.num_rows() as u64;
            buf.push(b);
            if buf_rows >= ROUND_ROWS {
                flush_csv_round(&mut f, std::mem::take(&mut buf))?;
                buf_rows = 0;
            }
        }
        if !buf.is_empty() {
            flush_csv_round(&mut f, buf)?;
        }
        if store.data_sync() {
            f.sync_all()
                .map_err(|e| QsdbError::meta(format!("fsync csv failed: {e}")))?;
        }
    }
    store.commit_object(&tmp, &dest)?;
    if store.data_sync() {
        crate::fsutil::dir_sync(&dest);
    }
    let bytes = store.stat(&dest)?.map(|m| m.size).unwrap_or(0);
    Ok(ExportStats {
        rows,
        files: 1,
        bytes,
        partitions: 1,
    })
}

/// CSV 目标是**文件**（parquet 那边才是目录）。
fn csv_target(target: &str) -> Result<PathBuf> {
    let t = target.trim();
    if t.is_empty() {
        return Err(QsdbError::invalid("COPY … TO 的路径是空的"));
    }
    if t.ends_with('/') || t.ends_with('\\') {
        return Err(QsdbError::invalid(format!(
            "`FORMAT CSV` 的目标是**文件**不是目录（`{t}`）—— CSV 写单个文件。\
             要产出一个数据集目录请用 `FORMAT PARQUET`"
        )));
    }
    Ok(PathBuf::from(t.replace('\\', "/")))
}

/// 一轮：切块 → **并行格式化** → **按序**写进同一个文件。
///
/// `collect()` 到 `Vec` 保住块间顺序（`par_iter` 本身是无序执行的，
/// 但 `collect` 会按下标归位）；内存上界 = 一轮编码后的字节数。
fn flush_csv_round(f: &mut std::fs::File, buf: Vec<RecordBatch>) -> Result<()> {
    let chunks = chunk_batches(&buf, CHUNK_ROWS);
    let encoded: Vec<Result<Vec<u8>>> = chunks
        .into_par_iter()
        .map(|c| encode_csv(&c, false))
        .collect();
    for e in encoded {
        f.write_all(&e?)
            .map_err(|e| QsdbError::meta(format!("csv write failed: {e}")))?;
    }
    Ok(())
}

/// 一批 → CSV 字节。**与 bench 里原来那条 `scan_to_csv` 同一个 writer**
/// （arrow 的 `csv::Writer`）⇒ 换口径前后产出的文件逐字节可比。
fn encode_csv(batches: &[RecordBatch], header: bool) -> Result<Vec<u8>> {
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut w = arrow::csv::WriterBuilder::new()
            .with_header(header)
            .build(&mut buf);
        for b in batches {
            w.write(b)
                .map_err(|e| QsdbError::meta(format!("csv encode failed: {e}")))?;
        }
    }
    Ok(buf)
}

// ---------------------------------------------------------------------------
// 目标路径
// ---------------------------------------------------------------------------

/// 目标是**目录**。挡掉最容易犯的那个错：把它当文件写。
fn plan_target(target: &str) -> Result<PathBuf> {
    let t = target.trim();
    if t.is_empty() {
        return Err(QsdbError::invalid("COPY … TO 的路径是空的"));
    }
    if t.to_ascii_lowercase().ends_with(".parquet") {
        return Err(QsdbError::invalid(format!(
            "COPY … TO 的目标是**目录**不是文件（`{t}`）—— 导出会产出 \
             `<目录>/part-00000.parquet`（写了 `PARTITION_BY` 还会再分 `k=v/` 子目录）。\
             去掉 `.parquet` 后缀即可"
        )));
    }
    Ok(PathBuf::from(t.replace('\\', "/")))
}

/// 目标目录必须为空（见模块文档第 3 条）。
///
/// 不做"只清我认识的文件"这种聪明事：判据一旦是启发式，就有"清漏了"的那天，
/// 而后果是**多读出一批数据**。宁可让调用方自己先清。
fn ensure_empty_dir(store: &Store, root: &Path) -> Result<()> {
    let Ok(entries) = store.read_dir(root) else {
        return Ok(()); // 不存在 = 空
    };
    let mut names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
    names.sort_unstable();
    if names.is_empty() {
        return Ok(());
    }
    let shown: Vec<&str> = names.iter().take(5).copied().collect();
    Err(QsdbError::invalid(format!(
        "导出目标 `{}` 不是空目录（现有 {} 项：{}{}）—— **不做静默覆盖**：\
         上一轮残留的 `part-*.parquet` 会被读成多余的数据。请先清空该目录，或换一个路径",
        root.display(),
        names.len(),
        shown.join(", "),
        if names.len() > shown.len() {
            ", …"
        } else {
            ""
        },
    )))
}

// ---------------------------------------------------------------------------
// 分区方案（列定位 + 类型可往返性校验）
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct Plan {
    /// 分区列在**输出 schema** 里的下标（与 `partition_by` 同序）
    part_idx: Vec<usize>,
    /// 分区列名（原样，用于目录名）
    part_names: Vec<String>,
    /// 数据 schema = 输出 schema 去掉分区列
    data_schema: SchemaRef,
    /// 数据列在输出里的下标（升序）
    data_idx: Vec<usize>,
}

impl Plan {
    fn new(schema: &SchemaRef, partition_by: &[String]) -> Result<Self> {
        let mut part_idx: Vec<usize> = Vec::with_capacity(partition_by.len());
        for name in partition_by {
            part_idx.push(locate(schema, name)?);
        }

        // ★ 类型必须先过关：**读侧是按目录值推断类型的**（F12），所以分区列的
        //   类型若不在这张白名单里，导出会**静默改类型**（`FLOAT` 的 `1.5` 读回来
        //   是 `VARCHAR`）。这种"写得出、读不对"的形态一律当场拒。
        for (i, &ci) in part_idx.iter().enumerate() {
            let ty = logical_type_of(schema.field(ci).data_type())?;
            if !matches!(
                ty,
                LogicalType::Varchar { .. }
                    | LogicalType::Date
                    | LogicalType::Int64
                    | LogicalType::Boolean
            ) {
                return Err(QsdbError::unsupported(format!(
                    "分区列 `{}` 的类型是 {ty:?}，导出后**读不回来**：hive 目录只有文本，\
                     `read_parquet` 按首个值推断类型，认不出它。可用：STRING / BIGINT / DATE / BOOLEAN\
                     （要按别的类型分区，先 `CAST(… AS VARCHAR)` 起个别名再导）",
                    partition_by[i],
                )));
            }
        }

        let data_idx: Vec<usize> = (0..schema.fields().len())
            .filter(|i| !part_idx.contains(i))
            .collect();
        if data_idx.is_empty() {
            return Err(QsdbError::invalid(
                "所有输出列都被 PARTITION_BY 拿走了 —— 数据文件会是零列的，读不出来。\
                 至少留一列非分区列",
            ));
        }
        let fields: Vec<_> = data_idx.iter().map(|&i| schema.field(i).clone()).collect();
        let data_schema = Arc::new(Schema::new(fields));

        Ok(Self {
            part_names: partition_by.to_vec(),
            part_idx,
            data_schema,
            data_idx,
        })
    }
}

/// 在输出 schema 里定位 `PARTITION_BY` 写的那一列。
///
/// 两步（与 CTAS 剥表限定同一个理由——join 出来的列名带限定，
/// `SELECT a.x, b.x …` 的输出列叫 `a.x` / `b.x`，用户却只会写 `x`）：
/// ① 先按**全名**（大小写不敏感）比；② 没中的话比**去表限定的尾名**。
///
/// 任何一步命中多于一个 —— **报错**，不挑一个（挑谁都是静默改语义）。
/// 列名不可能真的带 `.`（`catalog::ident_ok` 只认 `[A-Za-z_][A-Za-z0-9_]*`），
/// 所以尾名比对不会把两列真的不同的列混淆。
fn locate(schema: &SchemaRef, name: &str) -> Result<usize> {
    let tail = |s: &str| s.rsplit('.').next().unwrap_or(s).to_ascii_lowercase();
    for by_full_name in [true, false] {
        let probe = if by_full_name {
            name.to_ascii_lowercase()
        } else {
            tail(name)
        };
        let hits: Vec<usize> = schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| {
                if by_full_name {
                    f.name().to_ascii_lowercase() == probe
                } else {
                    tail(f.name()) == probe
                }
            })
            .map(|(i, _)| i)
            .collect();
        match hits.len() {
            1 => return Ok(hits[0]),
            0 => continue,
            n => {
                return Err(QsdbError::invalid(format!(
                    "查询输出里有 {n} 个叫 `{name}` 的列 —— PARTITION_BY 指谁说不清。\
                     给它起个别名再导出（如 `… AS part_{name}`）"
                )));
            }
        }
    }
    let avail: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    Err(QsdbError::invalid(format!(
        "PARTITION_BY 里的 `{name}` 不在查询输出里（可用列：{}）",
        avail.join(", ")
    )))
}

/// arrow 类型 → 本仓逻辑类型（判定用；认不出就报错，**不猜**）。
fn logical_type_of(dt: &arrow::datatypes::DataType) -> Result<LogicalType> {
    LogicalType::try_from_arrow(dt)
        .map_err(|e| QsdbError::unsupported(format!("导出时遇到不认识的列类型 {dt:?}：{e}")))
}

/// 探针：`(类型, 值)` **能不能原样往返**一趟目录名。
///
/// 判据就是读侧那两步 —— `infer_partition_type(text)` 再 `parse_partition_value`。
/// 用它来预演"这个值写进 `k=v` 之后会被读成什么"。
fn roundtrip_ok(ty: &LogicalType, text: &str) -> bool {
    let inferred = crate::scan::infer_partition_type(text);
    if &inferred != ty {
        return false;
    }
    crate::partition::parse_partition_value(text, ty).is_ok()
}

// ---------------------------------------------------------------------------
// 写
// ---------------------------------------------------------------------------

struct Writer<'a> {
    store: &'a Store,
    root: PathBuf,
    plan: Plan,
    props: WriterProperties,
    /// 每个目录下一个 part 的序号（跨轮次连续）
    next_ix: BTreeMap<PathBuf, u32>,
    rows: u64,
    files: u64,
    bytes: u64,
    /// 见过的分区目录（用于报"落到几个分区"）
    dirs: BTreeMap<PathBuf, ()>,
}

impl Writer<'_> {
    /// 写掉一轮缓冲：按分区切 → 切块 → **并行编码**。
    fn flush(&mut self, buf: Vec<RecordBatch>) -> Result<()> {
        // ① 按分区目录归拢（无分区列时只有一个"空 key"的桶）
        let mut by_dir: BTreeMap<PathBuf, Vec<RecordBatch>> = BTreeMap::new();
        if self.plan.part_idx.is_empty() {
            by_dir.insert(self.root.clone(), buf);
        } else {
            for b in &buf {
                for (dir, sub) in split_by_partition(&self.plan, &self.root, b)? {
                    by_dir.entry(dir).or_default().push(sub);
                }
            }
        }

        // ② 每个目录切成 ≤ CHUNK_ROWS 的块；块名在这里**定死**
        //    （`par_iter` 是无序的，名字必须先分配好，否则同一份输入两次导出
        //     会得到不同的文件名）
        let mut jobs: Vec<Job> = Vec::new();
        for (dir, batches) in by_dir {
            self.dirs.insert(dir.clone(), ());
            let next = self.next_ix.entry(dir.clone()).or_insert(0);
            for chunk in chunk_batches(&batches, CHUNK_ROWS) {
                let name = format!("part-{:05}.parquet", *next);
                *next += 1;
                jobs.push(Job {
                    dir: dir.clone(),
                    name,
                    batches: chunk,
                });
            }
        }

        // ③ 并行编码 + 落盘
        let schema = Arc::clone(&self.plan.data_schema);
        let props = self.props.clone();
        let store = self.store;
        let done: Vec<Result<(u64, u64)>> = jobs
            .into_par_iter()
            .map(|j| {
                let b = encode(&j.batches, &schema, &props)?;
                put_part(store, &j.dir, &j.name, &b)?;
                Ok((b.len() as u64, count_rows(&j.batches)))
            })
            .collect();
        for r in done {
            let (bytes, rows) = r?;
            self.bytes += bytes;
            self.rows += rows;
            self.files += 1;
        }
        Ok(())
    }
}

struct Job {
    dir: PathBuf,
    name: String,
    batches: Vec<RecordBatch>,
}

fn count_rows(batches: &[RecordBatch]) -> u64 {
    batches.iter().map(|b| b.num_rows() as u64).sum()
}

/// 把一批按分区列切成 `(目录, 数据批)`。
///
/// 分区列的取值**只出现在目录名里**，数据批里没有它们（模块文档第 1 条）。
fn split_by_partition(
    plan: &Plan,
    root: &Path,
    batch: &RecordBatch,
) -> Result<Vec<(PathBuf, RecordBatch)>> {
    let n = batch.num_rows();
    if n == 0 {
        return Ok(Vec::new());
    }
    // 逐行算 key（编码后的目录串），再按 key 归组
    let mut key_of_row: Vec<String> = Vec::with_capacity(n);
    let mut seen: std::collections::HashMap<String, ()> = std::collections::HashMap::new();
    for r in 0..n {
        let mut parts: Vec<String> = Vec::with_capacity(plan.part_idx.len());
        for (k, &ci) in plan.part_names.iter().zip(plan.part_idx.iter()) {
            let col = batch.column(ci);
            if col.is_null(r) {
                // 与 qsdb-sync 同一口径：**不采** hive 的 `__HIVE_DEFAULT_PARTITION__`
                // 惯例。静默落到一个"默认分区"目录 = 数据看着在、WHERE 又筛不到。
                return Err(QsdbError::invalid(format!(
                    "分区列 `{k}` 第 {} 行是 NULL —— hive 的 \
                     `__HIVE_DEFAULT_PARTITION__` 惯例本仓不采（那会让数据看着在、\
                     `WHERE {k} = …` 又筛不到）。导出前先补齐或过滤",
                    r + 1
                )));
            }
            let text = arrow::util::display::array_value_to_string(col.as_ref(), r)
                .map_err(|e| QsdbError::invalid(format!("读分区列 `{k}` 失败：{e}")))?;
            // 每个**不同**的取值只判一次（分区值的基数就是分区的个数，很小）
            if seen.insert(text.clone(), ()).is_none() {
                let ty = logical_type_of(batch.schema().field(ci).data_type())?;
                if !roundtrip_ok(&ty, &text) {
                    return Err(QsdbError::invalid(format!(
                        "分区列 `{k}` 的值 `{text}` 导出后读不回来（目录里只有文本，\
                         `read_parquet` 会把它读成 {:?}，与列类型 {ty:?} 不符）—— \
                         分区列请用 STRING / BIGINT / DATE / BOOLEAN 且取值形态一致",
                        crate::scan::infer_partition_type(&text)
                    )));
                }
            }
            parts.push(format!(
                "{k}={}",
                crate::partition::encode_partition_value(&text)
            ));
        }
        key_of_row.push(parts.join("/"));
    }

    // 归组 → 每组一次 `take`（而不是逐行切片）
    let mut groups: BTreeMap<String, Vec<u32>> = BTreeMap::new();
    for (r, k) in key_of_row.iter().enumerate() {
        groups.entry(k.clone()).or_default().push(r as u32);
    }
    let mut out = Vec::with_capacity(groups.len());
    for (key, rows) in groups {
        let idx = arrow::array::UInt32Array::from(rows);
        let taken = arrow::compute::take_record_batch(batch, &idx)
            .map_err(|e| QsdbError::invalid(format!("按分区取行失败：{e}")))?;
        let cols: Vec<_> = plan
            .data_idx
            .iter()
            .map(|&i| taken.column(i).clone())
            .collect();
        let data = RecordBatch::try_new(Arc::clone(&plan.data_schema), cols)
            .map_err(|e| QsdbError::invalid(format!("重建数据批失败：{e}")))?;
        let mut dir = root.to_path_buf();
        for seg in key.split('/') {
            dir.push(seg);
        }
        out.push((dir, data));
    }
    Ok(out)
}

/// 把一批切成 ≤ `chunk_rows` 的若干组（组内保持顺序）。
fn chunk_batches(batches: &[RecordBatch], chunk_rows: u64) -> Vec<Vec<RecordBatch>> {
    let mut out: Vec<Vec<RecordBatch>> = Vec::new();
    let mut cur: Vec<RecordBatch> = Vec::new();
    let mut cur_rows: u64 = 0;
    for b in batches {
        let mut off = 0usize;
        while off < b.num_rows() {
            let room = (chunk_rows - cur_rows) as usize;
            let take = room.min(b.num_rows() - off).max(1);
            cur.push(b.slice(off, take));
            cur_rows += take as u64;
            off += take;
            if cur_rows >= chunk_rows {
                out.push(std::mem::take(&mut cur));
                cur_rows = 0;
            }
        }
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

/// 导出的 parquet 写参数：与建表默认一致（zstd level 1）。
fn writer_props() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::ZSTD(
            parquet::basic::ZstdLevel::try_new(1).expect("zstd level 1 恒定合法"),
        ))
        .set_max_row_group_row_count(Some(1_000_000))
        .set_data_page_row_count_limit(20_000)
        .set_statistics_enabled(EnabledStatistics::Page)
        .build()
}

/// 序列化到内存。远端走整体 PUT，本地写临时文件后 rename —— 两边的字节完全一样，
/// 所以统一先编码到内存（一个 part 只有 `CHUNK_ROWS` 行，量级可控）。
fn encode(
    batches: &[RecordBatch],
    schema: &SchemaRef,
    props: &WriterProperties,
) -> Result<Vec<u8>> {
    let mut buf: Vec<u8> = Vec::new();
    let mut w = ArrowWriter::try_new(&mut buf, Arc::clone(schema), Some(props.clone()))
        .map_err(|e| QsdbError::meta(format!("parquet writer init failed: {e}")))?;
    for b in batches {
        if b.num_rows() == 0 {
            continue;
        }
        w.write(b)
            .map_err(|e| QsdbError::meta(format!("parquet write failed: {e}")))?;
    }
    w.close()
        .map_err(|e| QsdbError::meta(format!("parquet close failed: {e}")))?;
    Ok(buf)
}

/// 落一个 part：本地 tmp+rename（durability 档加 fsync），远端整体 PUT。
fn put_part(store: &Store, dir: &Path, name: &str, bytes: &[u8]) -> Result<()> {
    let dest = dir.join(name);
    if store.is_local() {
        store.create_dir_all(dir)?;
        let tmp = crate::fsutil::tmp_sibling(&dest);
        {
            use std::io::Write;
            let mut f = std::fs::File::create(&tmp)
                .map_err(|e| QsdbError::meta(format!("create {} failed: {e}", tmp.display())))?;
            f.write_all(bytes)
                .map_err(|e| QsdbError::meta(format!("write {} failed: {e}", tmp.display())))?;
            if store.data_sync() {
                f.sync_all()
                    .map_err(|e| QsdbError::meta(format!("fsync {} failed: {e}", tmp.display())))?;
            }
        }
        store.commit_object(&tmp, &dest)?;
        if store.data_sync() {
            crate::fsutil::dir_sync(&dest);
        }
        Ok(())
    } else {
        store.put_object(&dest, bytes)
    }
}

/// `_common_metadata`：全 schema（**含分区列**）、零行。
fn write_common_metadata(
    store: &Store,
    root: &Path,
    schema: &SchemaRef,
    props: &WriterProperties,
) -> Result<()> {
    let bytes = encode(&[], schema, props)?;
    put_part(store, root, COMMON_METADATA, &bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field};

    fn schema(fields: &[(&str, DataType)]) -> SchemaRef {
        Arc::new(Schema::new(
            fields
                .iter()
                .map(|(n, t)| Field::new(*n, t.clone(), true))
                .collect::<Vec<_>>(),
        ))
    }

    #[test]
    fn 分区列找不到要报出可用列() {
        let s = schema(&[("a", DataType::Int64), ("b", DataType::Utf8)]);
        let e = Plan::new(&s, &["zz".into()]).unwrap_err().to_string();
        assert!(
            e.contains("zz") && e.contains('a') && e.contains('b'),
            "{e}"
        );
    }

    #[test]
    fn 分区列类型不可往返要拒() {
        let s = schema(&[("f", DataType::Float64), ("a", DataType::Int64)]);
        let e = Plan::new(&s, &["f".into()]).unwrap_err().to_string();
        assert!(e.contains("Float64") || e.contains("读不回来"), "{e}");
    }

    #[test]
    fn 全列都拿去分区要拒() {
        let s = schema(&[("dt", DataType::Utf8)]);
        let e = Plan::new(&s, &["dt".into()]).unwrap_err().to_string();
        assert!(e.contains("零列"), "{e}");
    }

    #[test]
    fn 目标不能写成文件() {
        let e = plan_target("/lake/t.parquet").unwrap_err().to_string();
        assert!(e.contains("目录"), "{e}");
    }

    /// 往返探针：白名单里的类型在典型取值上必须过；不往返的必须拦下。
    #[test]
    fn 往返探针的判据() {
        assert!(roundtrip_ok(&LogicalType::Date, "2026-01-01"));
        assert!(roundtrip_ok(&LogicalType::Int64, "42"));
        assert!(roundtrip_ok(&LogicalType::Boolean, "true"));
        assert!(roundtrip_ok(
            &LogicalType::Varchar { max_chars: None },
            "cn"
        ));
        // 字符串列里存着日期形态的值 ⇒ 读回来会变 DATE，拦下
        assert!(!roundtrip_ok(
            &LogicalType::Varchar { max_chars: None },
            "2026-01-01"
        ));
        // 字符串列里存着数字 ⇒ 读回来会变 BIGINT，拦下
        assert!(!roundtrip_ok(
            &LogicalType::Varchar { max_chars: None },
            "42"
        ));
    }
}
