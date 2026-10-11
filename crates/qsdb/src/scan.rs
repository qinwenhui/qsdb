//! 读路径：谓词、三级裁剪（分区目录 → manifest 列统计 → parquet footer
//! row group）、行级过滤、流式扫描迭代器。
//!
//! 语义约定：
//! - 分区级：谓词只作用于分区列的常量值，只有「确定为假」才丢弃（Kleene 三值）
//! - segment 级：min/max 统计，只有「确定无匹配」才丢弃；无统计的列不裁剪
//! - row group 级：parquet footer 统计（数值/字符串），同上保守策略
//! - 行级：arrow cmp kernels；NULL 比较结果为 NULL → 行被过滤（SQL 语义）
//!
//! 存储收口（M6-1）：发现/manifest/元数据经 `Store`；parquet 列读本地
//! 直开 `File`（存储设计 §4 热路径豁免），远端走段缓存路径（M6-2）。

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, StringArray, Time64NanosecondArray,
    TimestampNanosecondArray,
};
use arrow::compute::kernels::cmp;
use arrow::compute::{and_kleene, filter_record_batch, or_kleene};
use arrow::datatypes::{DataType, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use parquet::file::reader::ChunkReader as ChunkReaderParquet;

use crate::catalog::TableDef;
use crate::error::{QsdbError, Result};
use crate::manifest::{ColumnStats, PartitionManifest};
use crate::partition::{parse_partition_dir, parse_partition_value};
use crate::store::Store;
use crate::types::cast::cast_literal;
use crate::types::{Literal, LogicalType};

pub const BATCH_SIZE: usize = 8192;

// ---------------------------------------------------------------------------
// 谓词
// ---------------------------------------------------------------------------

/// 扫描谓词（M1 叶子集：六种比较 + 逻辑组合）。
#[derive(Debug, Clone, PartialEq)]
pub enum Predicate {
    Eq { column: String, value: Literal },
    NotEq { column: String, value: Literal },
    Lt { column: String, value: Literal },
    LtEq { column: String, value: Literal },
    Gt { column: String, value: Literal },
    GtEq { column: String, value: Literal },
    And(Box<Predicate>, Box<Predicate>),
    Or(Box<Predicate>, Box<Predicate>),
    Not(Box<Predicate>),
}

impl Predicate {
    pub fn eq(col: impl Into<String>, v: Literal) -> Self {
        Self::Eq {
            column: col.into(),
            value: v,
        }
    }
    pub fn not_eq(col: impl Into<String>, v: Literal) -> Self {
        Self::NotEq {
            column: col.into(),
            value: v,
        }
    }
    pub fn lt(col: impl Into<String>, v: Literal) -> Self {
        Self::Lt {
            column: col.into(),
            value: v,
        }
    }
    pub fn lt_eq(col: impl Into<String>, v: Literal) -> Self {
        Self::LtEq {
            column: col.into(),
            value: v,
        }
    }
    pub fn gt(col: impl Into<String>, v: Literal) -> Self {
        Self::Gt {
            column: col.into(),
            value: v,
        }
    }
    pub fn gt_eq(col: impl Into<String>, v: Literal) -> Self {
        Self::GtEq {
            column: col.into(),
            value: v,
        }
    }
    pub fn and(a: Self, b: Self) -> Self {
        Self::And(Box::new(a), Box::new(b))
    }
    pub fn or(a: Self, b: Self) -> Self {
        Self::Or(Box::new(a), Box::new(b))
    }
    #[allow(clippy::should_implement_trait)] // 谓词构造器，非 std::ops::Not
    pub fn not(a: Self) -> Self {
        Self::Not(Box::new(a))
    }

    /// 收集引用列。
    pub fn referenced_columns(&self, out: &mut HashSet<String>) {
        match self {
            Self::Eq { column, .. }
            | Self::NotEq { column, .. }
            | Self::Lt { column, .. }
            | Self::LtEq { column, .. }
            | Self::Gt { column, .. }
            | Self::GtEq { column, .. } => {
                out.insert(column.clone());
            }
            Self::And(a, b) | Self::Or(a, b) => {
                a.referenced_columns(out);
                b.referenced_columns(out);
            }
            Self::Not(a) => a.referenced_columns(out),
        }
    }
}

// ---------------------------------------------------------------------------
// 三值逻辑 + Literal 比较（裁剪专用）
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Tri {
    True,
    False,
    Unknown,
}

fn tri_and(a: Tri, b: Tri) -> Tri {
    match (a, b) {
        (Tri::False, _) | (_, Tri::False) => Tri::False,
        (Tri::True, Tri::True) => Tri::True,
        _ => Tri::Unknown,
    }
}

fn tri_or(a: Tri, b: Tri) -> Tri {
    match (a, b) {
        (Tri::True, _) | (_, Tri::True) => Tri::True,
        (Tri::False, Tri::False) => Tri::False,
        _ => Tri::Unknown,
    }
}

fn tri_not(t: Tri) -> Tri {
    match t {
        Tri::True => Tri::False,
        Tri::False => Tri::True,
        Tri::Unknown => Tri::Unknown,
    }
}

fn lit_eq_bool(a: &Literal, b: &Literal) -> bool {
    matches!(
        a.partial_cmp_literal(b),
        Ok(Some(std::cmp::Ordering::Equal))
    )
}

fn lit_eq(a: &Literal, b: &Literal) -> Tri {
    match a.partial_cmp_literal(b) {
        Ok(Some(std::cmp::Ordering::Equal)) => Tri::True,
        Ok(Some(_)) => Tri::False,
        _ => Tri::Unknown,
    }
}

fn lit_lt(a: &Literal, b: &Literal) -> bool {
    matches!(a.partial_cmp_literal(b), Ok(Some(std::cmp::Ordering::Less)))
}
fn lit_le(a: &Literal, b: &Literal) -> bool {
    matches!(
        a.partial_cmp_literal(b),
        Ok(Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal))
    )
}
fn lit_gt(a: &Literal, b: &Literal) -> bool {
    matches!(
        a.partial_cmp_literal(b),
        Ok(Some(std::cmp::Ordering::Greater))
    )
}
fn lit_ge(a: &Literal, b: &Literal) -> bool {
    matches!(
        a.partial_cmp_literal(b),
        Ok(Some(
            std::cmp::Ordering::Greater | std::cmp::Ordering::Equal
        ))
    )
}

/// 对分区常量求值：`vals` 提供已解析的分区列值；仅确定性结果返回 True/False。
/// （元数据短路复用；None=Unknown 表示无法裁剪）
pub(crate) fn eval_partition_predicate(pred: &Predicate, vals: &HashMap<&str, &Literal>) -> Tri {
    eval_on_partition(pred, vals)
}

fn eval_on_partition(pred: &Predicate, vals: &HashMap<&str, &Literal>) -> Tri {
    let cmp_part =
        |column: &str, value: &Literal, ok: &dyn Fn(std::cmp::Ordering) -> bool| -> Tri {
            match vals.get(column) {
                None => Tri::Unknown,
                Some(v) => match v.partial_cmp_literal(value) {
                    Ok(Some(o)) if ok(o) => Tri::True,
                    Ok(Some(_)) => Tri::False,
                    _ => Tri::Unknown,
                },
            }
        };
    match pred {
        Predicate::Eq { column, value } => match vals.get(column.as_str()) {
            None => Tri::Unknown,
            Some(v) => lit_eq(v, value),
        },
        Predicate::NotEq { column, value } => match vals.get(column.as_str()) {
            None => Tri::Unknown,
            Some(v) => tri_not(lit_eq(v, value)),
        },
        Predicate::Lt { column, value } => {
            cmp_part(column, value, &|o| o == std::cmp::Ordering::Less)
        }
        Predicate::LtEq { column, value } => {
            cmp_part(column, value, &|o| o != std::cmp::Ordering::Greater)
        }
        Predicate::Gt { column, value } => {
            cmp_part(column, value, &|o| o == std::cmp::Ordering::Greater)
        }
        Predicate::GtEq { column, value } => {
            cmp_part(column, value, &|o| o != std::cmp::Ordering::Less)
        }
        Predicate::And(a, b) => tri_and(eval_on_partition(a, vals), eval_on_partition(b, vals)),
        Predicate::Or(a, b) => tri_or(eval_on_partition(a, vals), eval_on_partition(b, vals)),
        Predicate::Not(a) => tri_not(eval_on_partition(a, vals)),
    }
}

// ---------------------------------------------------------------------------
// segment 级裁剪（min/max 统计；只在确定无匹配时丢弃）
// ---------------------------------------------------------------------------

fn segment_may_match(pred: &Predicate, lookup: &dyn Fn(&str) -> Option<ColumnStats>) -> bool {
    match pred {
        Predicate::Eq { column, value } => {
            if value.is_null() {
                return false; // x = NULL 恒 UNKNOWN → 无匹配
            }
            match lookup(column) {
                None => true,
                Some(st) => match (&st.min, &st.max) {
                    (Some(min), Some(max)) => lit_ge(value, min) && lit_le(value, max),
                    _ => false, // min/max 缺失 = 全 NULL 列 → 等值无匹配
                },
            }
        }
        Predicate::NotEq { column, value } => {
            if value.is_null() {
                return false;
            }
            match lookup(column) {
                None => true,
                Some(st) => match (&st.min, &st.max) {
                    // min == max == value → 所有非 NULL 值都等于 v → != 无匹配
                    (Some(min), Some(max)) => !(lit_eq_bool(min, max) && lit_eq_bool(min, value)),
                    // 全 NULL 列：NULL != v 为 UNKNOWN → 无匹配
                    _ => false,
                },
            }
        }
        Predicate::Lt { column, value } => lower_bound_may_match(column, value, lookup, lit_lt),
        Predicate::LtEq { column, value } => lower_bound_may_match(column, value, lookup, lit_le),
        Predicate::Gt { column, value } => upper_bound_may_match(column, value, lookup, lit_gt),
        Predicate::GtEq { column, value } => upper_bound_may_match(column, value, lookup, lit_ge),
        Predicate::And(a, b) => segment_may_match(a, lookup) && segment_may_match(b, lookup),
        Predicate::Or(a, b) => segment_may_match(a, lookup) || segment_may_match(b, lookup),
        Predicate::Not(a) => match a.as_ref() {
            // Not(leaf) 用补比较裁剪；复合结构保守放行
            Predicate::Eq { column, value } => segment_may_match(
                &Predicate::NotEq {
                    column: column.clone(),
                    value: value.clone(),
                },
                lookup,
            ),
            Predicate::NotEq { column, value } => segment_may_match(
                &Predicate::Eq {
                    column: column.clone(),
                    value: value.clone(),
                },
                lookup,
            ),
            Predicate::Lt { column, value } => segment_may_match(
                &Predicate::GtEq {
                    column: column.clone(),
                    value: value.clone(),
                },
                lookup,
            ),
            Predicate::LtEq { column, value } => segment_may_match(
                &Predicate::Gt {
                    column: column.clone(),
                    value: value.clone(),
                },
                lookup,
            ),
            Predicate::Gt { column, value } => segment_may_match(
                &Predicate::LtEq {
                    column: column.clone(),
                    value: value.clone(),
                },
                lookup,
            ),
            Predicate::GtEq { column, value } => segment_may_match(
                &Predicate::Lt {
                    column: column.clone(),
                    value: value.clone(),
                },
                lookup,
            ),
            _ => true,
        },
    }
}

/// x < / <= v：需要 min < / <= v
fn lower_bound_may_match(
    column: &str,
    value: &Literal,
    lookup: &dyn Fn(&str) -> Option<ColumnStats>,
    ok: fn(&Literal, &Literal) -> bool,
) -> bool {
    if value.is_null() {
        return false;
    }
    match lookup(column) {
        None => true,
        Some(st) => match &st.min {
            Some(min) => ok(min, value),
            None => false, // 全 NULL
        },
    }
}

/// x > / >= v：需要 max > / >= v
fn upper_bound_may_match(
    column: &str,
    value: &Literal,
    lookup: &dyn Fn(&str) -> Option<ColumnStats>,
    ok: fn(&Literal, &Literal) -> bool,
) -> bool {
    if value.is_null() {
        return false;
    }
    match lookup(column) {
        None => true,
        Some(st) => match &st.max {
            Some(max) => ok(max, value),
            None => false,
        },
    }
}

// ---------------------------------------------------------------------------
// 外表数据文件位置（read_parquet，M6-2）
// ---------------------------------------------------------------------------

/// 外表/管理表数据文件位置。
#[derive(Debug, Clone)]
pub enum FileLoc {
    Local(PathBuf),
    /// 完整 `s3://bucket/key` URL（feature `s3`，外表独立定位）
    #[cfg(feature = "s3")]
    S3(String),
    /// 管理表 segment（虚拟路径；由持有者的 Store 解析——Local=真实路径直开，
    /// Remote=按 root strip 推 key 走 range GET/段缓存）
    Managed(PathBuf),
}

impl FileLoc {
    /// 展示/缓存键路径（远端 = URL/虚拟路径字符串本身）。
    pub fn display_path(&self) -> PathBuf {
        match self {
            Self::Local(p) | Self::Managed(p) => p.clone(),
            #[cfg(feature = "s3")]
            Self::S3(u) => PathBuf::from(u),
        }
    }
}

/// 简单 glob 匹配：`*` 不跨 `/`，`**` 任意（含 `/`），分隔符统一 `/`。
pub(crate) fn glob_match(pattern: &str, text: &str) -> bool {
    /// 分隔符归一 + 连续星号折叠（≥2 个 → `**`）。
    fn normalize(p: &str) -> Vec<char> {
        let cs: Vec<char> = p.replace('\\', "/").chars().collect();
        let mut res: Vec<char> = Vec::with_capacity(cs.len());
        let mut i = 0;
        while i < cs.len() {
            if cs[i] == '*' {
                let mut n = 0;
                while i < cs.len() && cs[i] == '*' {
                    n += 1;
                    i += 1;
                }
                res.push('*');
                if n >= 2 {
                    res.push('*');
                }
            } else {
                res.push(cs[i]);
                i += 1;
            }
        }
        res
    }
    fn m(p: &[char], t: &[char]) -> bool {
        if p.is_empty() {
            return t.is_empty();
        }
        if p[0] == '*' {
            if p.len() > 1 && p[1] == '*' {
                // '**'：任意（含 '/'）——尝试全部吞噬长度
                let rest = &p[2..];
                (0..=t.len()).any(|i| m(rest, &t[i..]))
            } else {
                // '*'：段内任意
                let rest = &p[1..];
                for i in 0..=t.len() {
                    if m(rest, &t[i..]) {
                        return true;
                    }
                    if i < t.len() && t[i] == '/' {
                        break;
                    }
                }
                false
            }
        } else {
            !t.is_empty() && t[0] == p[0] && m(&p[1..], &t[1..])
        }
    }
    let p = normalize(pattern);
    let t: Vec<char> = text.replace('\\', "/").chars().collect();
    m(&p, &t)
}

/// 解析 read_parquet 路径模式 → 数据文件位置列表（glob 展开；无匹配报错）。
pub(crate) fn resolve_file_locs(pattern: &str) -> Result<Vec<FileLoc>> {
    #[cfg(feature = "s3")]
    if pattern.starts_with("s3://") {
        return crate::remote::expand_s3_glob(pattern)
            .map(|urls| urls.into_iter().map(FileLoc::S3).collect());
    }
    #[cfg(not(feature = "s3"))]
    if pattern.starts_with("s3://") {
        return Err(QsdbError::unsupported(
            "s3:// path requires qsdb built with feature 's3'",
        ));
    }
    let norm = pattern.replace('\\', "/");
    if !norm.contains('*') {
        let p = PathBuf::from(&norm);
        if p.is_file() {
            return Ok(vec![FileLoc::Local(p)]);
        }
        return Err(QsdbError::not_found(format!(
            "read_parquet: file not found: {norm}"
        )));
    }
    // 常量前缀目录 → 递归枚举 → 相对路径 glob 匹配
    let idx = norm.find('*').expect("checked");
    let before = &norm[..idx];
    let (base_str, rel_pat) = match before.rsplit_once('/') {
        Some((dir, _)) => (dir.to_string(), norm[dir.len() + 1..].to_string()),
        None => (".".to_string(), norm.clone()),
    };
    let base = PathBuf::from(&base_str);
    if !base.is_dir() {
        return Err(QsdbError::not_found(format!(
            "read_parquet: directory not found: {base_str}"
        )));
    }
    let mut all = Vec::new();
    walk_files(&base, &mut all);
    let mut out: Vec<FileLoc> = all
        .into_iter()
        .filter(|p| {
            let rel = p
                .strip_prefix(&base)
                .unwrap_or(p)
                .to_string_lossy()
                .replace('\\', "/");
            glob_match(&rel_pat, &rel)
        })
        .map(FileLoc::Local)
        .collect();
    out.sort_by_key(FileLoc::display_path);
    if out.is_empty() {
        return Err(QsdbError::not_found(format!(
            "read_parquet: no file matches {norm}"
        )));
    }
    Ok(out)
}

fn walk_files(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).into_iter().flatten().flatten() {
        let p = entry.path();
        if p.is_dir() {
            walk_files(&p, out);
        } else if p.is_file() {
            out.push(p);
        }
    }
}

// ---------------------------------------------------------------------------
// parquet 字节源（本地 File / 远端 range GET，M6-2）
// ---------------------------------------------------------------------------

/// parquet `ChunkReader` 派发源。本地直开 `File`（§4 热路径现状不变）；
/// 远端优先段缓存命中（零网络），未命中且整读提示时整文件下载入缓存，
/// 否则 range GET 惰性取字节（footer/选中 row group 才传输）。
pub(crate) enum ChunkSource {
    File(File),
    #[cfg(feature = "s3")]
    Remote(crate::remote::RemoteReader),
}

impl ChunkSource {
    /// `prefer_whole`：调用方将读取全部 row group 的提示（远端整文件缓存准入）。
    /// `store`：Managed 位置的解析源（外表 Local/S3 不需要）。
    #[allow(unused_variables)]
    pub(crate) fn open(
        store: Option<&Store>,
        loc: &FileLoc,
        file_cache: Option<&crate::filecache::FileCache>,
        prefer_whole: bool,
    ) -> Result<Self> {
        match loc {
            FileLoc::Local(p) => File::open(p)
                .map(Self::File)
                .map_err(|e| QsdbError::meta(format!("open {} failed: {e}", p.display()))),
            #[cfg(feature = "s3")]
            FileLoc::S3(url) => {
                let l = crate::remote::locate(url)?;
                open_remote_object(
                    l.store,
                    l.location,
                    url.to_string(),
                    file_cache,
                    prefer_whole,
                )
            }
            FileLoc::Managed(p) => {
                let Some(store) = store else {
                    return Err(QsdbError::meta("managed file loc requires the db store"));
                };
                match store.kind() {
                    crate::store::StoreKind::Local => {
                        File::open(p).map(Self::File).map_err(|e| {
                            QsdbError::meta(format!("open {} failed: {e}", p.display()))
                        })
                    }
                    #[cfg(feature = "s3")]
                    crate::store::StoreKind::Remote => {
                        let handle = store.remote_handle().expect("remote kind implies handle");
                        let key = store.remote_obj_key(p).expect("remote kind implies key");
                        let disp = p.to_string_lossy().replace('\\', "/");
                        open_remote_object(
                            Arc::clone(handle),
                            key,
                            disp,
                            file_cache,
                            prefer_whole,
                        )
                    }
                    #[cfg(not(feature = "s3"))]
                    crate::store::StoreKind::Remote => {
                        Err(QsdbError::unsupported("remote store requires feature 's3'"))
                    }
                }
            }
        }
    }
}

/// 远端对象打开（外表 s3:// 与管理表远端共用）：段缓存命中/整读回填 → 本地
/// File；否则 range GET 惰性 reader。
#[cfg(feature = "s3")]
fn open_remote_object(
    handle: Arc<dyn object_store::ObjectStore>,
    key: object_store::path::Path,
    cache_key: String,
    file_cache: Option<&crate::filecache::FileCache>,
    prefer_whole: bool,
) -> Result<ChunkSource> {
    let meta = crate::remote::head_parts(Arc::clone(&handle), &key)?;
    if let (Some(fc), Some(etag)) = (file_cache, meta.e_tag.as_deref()) {
        if let Some(lp) = fc.get(&cache_key, etag) {
            return File::open(lp).map(ChunkSource::File).map_err(|e| {
                QsdbError::meta(format!("open cached {cache_key} failed: {e}"))
            });
        }
        if prefer_whole {
            let mut rd = crate::remote::stream_parts(Arc::clone(&handle), key.clone())?;
            let lp = fc.insert(&cache_key, etag, &mut rd)?;
            return File::open(lp).map(ChunkSource::File).map_err(|e| {
                QsdbError::meta(format!("open cached {cache_key} failed: {e}"))
            });
        }
    }
    Ok(ChunkSource::Remote(crate::remote::RemoteReader::from_parts(
        handle, key, meta.size,
    )))
}

pub(crate) enum ChunkRead {
    /// parquet 对 File 的 ChunkReader 实现返回 BufReader<File>
    File(std::io::BufReader<File>),
    #[cfg(feature = "s3")]
    Remote(crate::remote::ChunkedRangeRead),
}

impl std::io::Read for ChunkRead {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::File(f) => f.read(out),
            #[cfg(feature = "s3")]
            Self::Remote(r) => r.read(out),
        }
    }
}

impl parquet::file::reader::Length for ChunkSource {
    fn len(&self) -> u64 {
        match self {
            Self::File(f) => parquet::file::reader::Length::len(f),
            #[cfg(feature = "s3")]
            Self::Remote(r) => parquet::file::reader::Length::len(r),
        }
    }
}

impl parquet::file::reader::ChunkReader for ChunkSource {
    type T = ChunkRead;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        Ok(match self {
            Self::File(f) => {
                ChunkRead::File(<File as ChunkReaderParquet>::get_read(f, start)?)
            }
            #[cfg(feature = "s3")]
            Self::Remote(r) => ChunkRead::Remote(
                <crate::remote::RemoteReader as ChunkReaderParquet>::get_read(r, start)?,
            ),
        })
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<bytes::Bytes> {
        match self {
            Self::File(f) => <File as ChunkReaderParquet>::get_bytes(f, start, length),
            #[cfg(feature = "s3")]
            Self::Remote(r) => {
                <crate::remote::RemoteReader as ChunkReaderParquet>::get_bytes(r, start, length)
            }
        }
    }
}

/// 读外表单文件的 arrow schema（footer，binder 期调用）。
pub(crate) fn file_arrow_schema(loc: &FileLoc) -> Result<SchemaRef> {
    // 外表专用（Local/S3），无需管理表 store
    let src = ChunkSource::open(None, loc, None, false)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(src)
        .map_err(|e| QsdbError::meta(format!("read parquet footer {}: {e}", loc.display_path().display())))?;
    Ok(Arc::clone(builder.schema()))
}

// ---------------------------------------------------------------------------
// 扫描计划
// ---------------------------------------------------------------------------

/// 一个待读 parquet 文件。
#[derive(Clone)]
struct FilePlan {
    loc: FileLoc,
    partition_values: HashMap<String, Literal>,
    row_groups: Option<Vec<usize>>, // None = 全部
}

/// 扫描计划（构建后不可变）。
struct ScanPlan {
    def: Arc<TableDef>,
    /// 输出列（按请求顺序；None 请求 = 全表列序）
    projection: Vec<String>,
    /// 数据文件中需要读取的列（投影 ∪ 谓词数据列，按表列序）
    read_columns: Vec<String>,
    /// 值已 cast 到列类型的谓词
    predicate: Option<Predicate>,
    output_schema: SchemaRef,
}

impl ScanPlan {
    fn build(
        def: Arc<TableDef>,
        projection: Option<Vec<String>>,
        predicate: Option<Predicate>,
        _limit: Option<u64>,
    ) -> Result<Self> {
        let projection = match projection {
            Some(p) => {
                if p.is_empty() {
                    return Err(QsdbError::invalid("empty projection"));
                }
                for name in &p {
                    if def.column(name).is_none() {
                        return Err(QsdbError::not_found(format!(
                            "column '{name}' not in table {}",
                            def.name
                        )));
                    }
                }
                p
            }
            None => def.columns.iter().map(|c| c.name.clone()).collect(),
        };

        let predicate = predicate
            .map(|p| Self::resolve_predicate(p, &def))
            .transpose()?;

        // 读列 = 投影中的数据列 ∪ 谓词中的数据列
        let mut read: HashSet<String> = HashSet::new();
        for name in &projection {
            if !def.partition_keys.contains(name) {
                read.insert(name.clone());
            }
        }
        if let Some(p) = &predicate {
            let mut cols = HashSet::new();
            p.referenced_columns(&mut cols);
            for name in cols {
                if !def.partition_keys.contains(&name) {
                    read.insert(name);
                }
            }
        }
        let mut read_columns: Vec<String> = read.into_iter().collect();
        read_columns.sort_by_key(|n| def.columns.iter().position(|c| &c.name == n).unwrap());

        let fields: Vec<_> = projection
            .iter()
            .map(|n| {
                let c = def.column(n).unwrap();
                arrow::datatypes::Field::new(&c.name, c.logical_type.arrow_type(), c.nullable)
            })
            .collect();

        Ok(Self {
            def,
            projection,
            read_columns,
            predicate,
            output_schema: Arc::new(Schema::new(fields)),
        })
    }

    fn resolve_predicate(p: Predicate, def: &TableDef) -> Result<Predicate> {
        let resolve_leaf = |column: String, value: Literal| -> Result<(String, Literal)> {
            Ok((column.clone(), resolve_value(&column, value, def)?))
        };
        Ok(match p {
            Predicate::Eq { column, value } => {
                let (c, v) = resolve_leaf(column, value)?;
                Predicate::Eq {
                    column: c,
                    value: v,
                }
            }
            Predicate::NotEq { column, value } => {
                let (c, v) = resolve_leaf(column, value)?;
                Predicate::NotEq {
                    column: c,
                    value: v,
                }
            }
            Predicate::Lt { column, value } => {
                let (c, v) = resolve_leaf(column, value)?;
                Predicate::Lt {
                    column: c,
                    value: v,
                }
            }
            Predicate::LtEq { column, value } => {
                let (c, v) = resolve_leaf(column, value)?;
                Predicate::LtEq {
                    column: c,
                    value: v,
                }
            }
            Predicate::Gt { column, value } => {
                let (c, v) = resolve_leaf(column, value)?;
                Predicate::Gt {
                    column: c,
                    value: v,
                }
            }
            Predicate::GtEq { column, value } => {
                let (c, v) = resolve_leaf(column, value)?;
                Predicate::GtEq {
                    column: c,
                    value: v,
                }
            }
            Predicate::And(a, b) => Predicate::and(
                Self::resolve_predicate(*a, def)?,
                Self::resolve_predicate(*b, def)?,
            ),
            Predicate::Or(a, b) => Predicate::or(
                Self::resolve_predicate(*a, def)?,
                Self::resolve_predicate(*b, def)?,
            ),
            Predicate::Not(a) => Predicate::not(Self::resolve_predicate(*a, def)?),
        })
    }
}

fn resolve_value(column: &str, value: Literal, def: &TableDef) -> Result<Literal> {
    let col = def.column(column).ok_or_else(|| {
        QsdbError::not_found(format!("column '{column}' not in table {}", def.name))
    })?;
    if value.is_null() {
        return Ok(value);
    }
    let ty = &col.logical_type;
    if value.logical_type().as_ref() == Some(ty) {
        return Ok(value);
    }
    cast_literal(&value, ty).map_err(|e| {
        QsdbError::type_err(format!(
            "predicate on '{column}' expects {ty}, got {}: {e}",
            value
                .logical_type()
                .map(|t| t.to_string())
                .unwrap_or_default()
        ))
    })
}

// ---------------------------------------------------------------------------
// 发现 + 规划
// ---------------------------------------------------------------------------

/// 列出表的分区目录（Hive 风格逐层下钻；隐藏/下划线目录跳过）。
pub fn list_partitions(
    store: &Store,
    table_dir: &Path,
    depth: usize,
) -> Vec<(PathBuf, Vec<(String, String)>)> {
    if depth == 0 {
        return Vec::new(); // 非分区表
    }
    let mut level_dirs = vec![table_dir.to_path_buf()];
    for _ in 0..depth {
        let mut next = Vec::new();
        for dir in &level_dirs {
            let Ok(entries) = store.read_dir(dir) else {
                continue;
            };
            for entry in entries {
                if entry.is_dir
                    && entry.name.contains('=')
                    && !entry.name.starts_with('_')
                    && !entry.name.starts_with('.')
                {
                    next.push(entry.path);
                }
            }
        }
        level_dirs = next;
    }
    let mut out = Vec::new();
    for dir in level_dirs {
        if let Ok(vals) = parse_partition_dir(&dir, depth) {
            out.push((dir, vals));
        }
    }
    out
}

/// 规划扫描：分区裁剪 → manifest → segment 裁剪 → row group 裁剪。
///
/// 优先走表级索引 `partitions.idx`（单文件，万级分区 <50ms）；
/// 索引缺失/损坏时回退为 readdir + 逐分区读 manifest。
fn plan_scan(store: &Store, table_dir: &Path, plan: &ScanPlan) -> Result<Vec<FilePlan>> {
    let mut files = Vec::new();
    if let Some(idx) = crate::index::PartitionsIndex::read(store, table_dir) {
        for (rel, manifest) in &idx.entries {
            let pdir = table_dir.join(rel);
            // 索引内 manifest 自带分区值 Literal，无需解析目录名
            let pvals: HashMap<String, Literal> =
                manifest.partition_values.iter().cloned().collect();
            add_partition_files(store, &pdir, pvals, manifest, plan, &mut files)?;
        }
        return Ok(files);
    }
    for (pdir, dir_vals) in list_partitions(store, table_dir, plan.def.partition_keys.len()) {
        // 分区值 → Literal（按表列类型解析；失败跳过该分区）
        let mut pvals: HashMap<String, Literal> = HashMap::new();
        let mut ok = true;
        for (k, raw) in &dir_vals {
            match plan.def.column(k) {
                Some(c) => match parse_partition_value(raw, &c.logical_type) {
                    Ok(v) => {
                        pvals.insert(k.clone(), v);
                    }
                    Err(_) => {
                        ok = false;
                        break;
                    }
                },
                None => {
                    ok = false;
                    break;
                }
            }
        }
        if !ok {
            continue;
        }
        let Some(manifest) = PartitionManifest::read(store, &pdir)? else {
            continue;
        };
        add_partition_files(store, &pdir, pvals, &manifest, plan, &mut files)?;
    }
    Ok(files)
}

/// 单分区的裁剪与文件规划（分区裁剪 + fingerprint 校验 + segment 裁剪）。
fn add_partition_files(
    store: &Store,
    pdir: &Path,
    pvals: HashMap<String, Literal>,
    manifest: &PartitionManifest,
    plan: &ScanPlan,
    files: &mut Vec<FilePlan>,
) -> Result<()> {
    // 分区裁剪（仅确定为假才丢弃）
    if let Some(pred) = &plan.predicate {
        let lookup: HashMap<&str, &Literal> = pvals.iter().map(|(k, v)| (k.as_str(), v)).collect();
        if eval_on_partition(pred, &lookup) == Tri::False {
            return Ok(());
        }
    }

    // 指纹一致 或 旧 schema 为当前 schema 前缀（ALTER ADD COLUMN 后向兼容）
    if manifest.schema_fingerprint != plan.def.schema_fingerprint()
        && !plan
            .def
            .prefix_fingerprints()
            .contains(&manifest.schema_fingerprint)
    {
        let rel = pdir.strip_prefix(store.root()).unwrap_or(pdir);
        return Err(QsdbError::meta(format!(
            "partition {} was written under a different table schema (overwrite it first)",
            rel.display()
        )));
    }

    for seg in &manifest.segments {
        if let Some(pred) = &plan.predicate {
            let stats_map: HashMap<&str, ColumnStats> = seg
                .column_stats
                .iter()
                .map(|(n, s)| (n.as_str(), s.clone()))
                .collect();
            let lookup = |c: &str| stats_map.get(c).cloned();
            if !segment_may_match(pred, &lookup) {
                continue;
            }
        }
        let loc = FileLoc::Managed(pdir.join(&seg.file_name));
        let row_groups = prune_row_groups(store, &loc, plan, None)?;
        files.push(FilePlan {
            loc,
            partition_values: pvals.clone(),
            row_groups,
        });
    }
    Ok(())
}

/// parquet footer row group 裁剪（数值/字符串；无统计/失败 → 全读）。
/// 字节源经 [`ChunkSource`]（本地 File / 远端 range GET）。
fn prune_row_groups(
    store: &Store,
    loc: &FileLoc,
    plan: &ScanPlan,
    file_cache: Option<&crate::filecache::FileCache>,
) -> Result<Option<Vec<usize>>> {
    let Some(pred) = &plan.predicate else {
        return Ok(None);
    };
    // 只有谓词引用的数据列参与裁剪
    let mut pred_cols = HashSet::new();
    pred.referenced_columns(&mut pred_cols);
    let mut targets: Vec<(String, LogicalType)> = Vec::new();
    for name in &pred_cols {
        if plan.def.partition_keys.contains(name) {
            continue;
        }
        if let Some(c) = plan.def.column(name) {
            targets.push((name.clone(), c.logical_type.clone()));
        }
    }
    if targets.is_empty() {
        return Ok(None);
    }

    let disp = loc.display_path();
    let src = ChunkSource::open(Some(store), loc, file_cache, false)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(src)
        .map_err(|e| QsdbError::meta(format!("read parquet footer {}: {e}", disp.display())))?;
    let meta = builder.metadata();
    let file_schema = builder.schema();

    let mut chunks: Vec<(usize, &str, &LogicalType)> = Vec::new();
    for (name, logical) in &targets {
        match file_schema.index_of(name) {
            Ok(idx) => chunks.push((idx, name.as_str(), logical)),
            Err(_) => return Ok(None), // 文件缺谓词列 → 保守全读
        }
    }

    let mut keep = Vec::with_capacity(meta.num_row_groups());
    'rg: for (rg_idx, rg) in meta.row_groups().iter().enumerate() {
        for (chunk_idx, name, logical) in &chunks {
            let chunk = rg.column(*chunk_idx);
            let Some(stats) = chunk.statistics() else {
                continue;
            };
            let (Some(min), Some(max)) = (
                footer_bound(stats, logical, true),
                footer_bound(stats, logical, false),
            ) else {
                continue;
            };
            let st = ColumnStats {
                min: Some(min),
                max: Some(max),
                null_count: 0,
            };
            let name = name.to_string();
            let lookup = |c: &str| if c == name { Some(st.clone()) } else { None };
            if !segment_may_match(pred, &lookup) {
                continue 'rg;
            }
        }
        keep.push(rg_idx);
    }
    Ok(if keep.len() == meta.num_row_groups() {
        None
    } else {
        Some(keep)
    })
}

/// footer 统计边界 → Literal（目标逻辑类型）。不支持 → None（不裁剪）。
fn footer_bound(
    stats: &parquet::file::statistics::Statistics,
    target: &LogicalType,
    is_min: bool,
) -> Option<Literal> {
    use parquet::file::statistics::Statistics as S;
    match (stats, target) {
        (S::Int32(s), LogicalType::Int32) => {
            opt_bound(s.min_opt(), s.max_opt(), is_min).map(|v| Literal::Int32(*v))
        }
        (S::Int32(s), LogicalType::Int16) => {
            let v = opt_bound(s.min_opt(), s.max_opt(), is_min)?;
            i16::try_from(*v).ok().map(Literal::Int16)
        }
        (S::Int32(s), LogicalType::Int8) => {
            let v = opt_bound(s.min_opt(), s.max_opt(), is_min)?;
            i8::try_from(*v).ok().map(Literal::Int8)
        }
        (S::Int32(s), LogicalType::Date) => {
            opt_bound(s.min_opt(), s.max_opt(), is_min).map(|v| Literal::Date(*v))
        }
        (S::Int64(s), LogicalType::Int64) => {
            opt_bound(s.min_opt(), s.max_opt(), is_min).map(|v| Literal::Int64(*v))
        }
        (S::Int64(s), LogicalType::Timestamp { .. }) => {
            opt_bound(s.min_opt(), s.max_opt(), is_min).map(|v| Literal::Timestamp(*v))
        }
        (S::Int64(s), LogicalType::Time) => {
            opt_bound(s.min_opt(), s.max_opt(), is_min).map(|v| Literal::Time(*v))
        }
        (S::Float(s), LogicalType::Float32) => {
            f32_bound(s.min_opt(), s.max_opt(), is_min).map(Literal::Float32)
        }
        (S::Double(s), LogicalType::Float64) => {
            f64_bound(s.min_opt(), s.max_opt(), is_min).map(Literal::Float64)
        }
        (S::ByteArray(s), LogicalType::Varchar { .. } | LogicalType::Char { .. }) => {
            let v = if is_min { s.min_opt() } else { s.max_opt() }?;
            v.as_utf8().ok().map(|s| Literal::Utf8(s.to_string()))
        }
        _ => None,
    }
}

fn opt_bound<'a, T>(min: Option<&'a T>, max: Option<&'a T>, is_min: bool) -> Option<&'a T> {
    if is_min { min } else { max }
}

macro_rules! float_bound_impl {
    ($fn_name:ident, $ty:ty) => {
        fn $fn_name(min: Option<&$ty>, max: Option<&$ty>, is_min: bool) -> Option<$ty> {
            let v = opt_bound(min, max, is_min).copied();
            match v {
                Some(x) if x.is_nan() => None,
                other => other,
            }
        }
    };
}

float_bound_impl!(f32_bound, f32);
float_bound_impl!(f64_bound, f64);

// ---------------------------------------------------------------------------
// 行过滤 + 输出组装
// ---------------------------------------------------------------------------

enum CmpOp {
    Eq,
    Neq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

fn leaf_pred(column: &str, value: Literal, op: &CmpOp) -> Predicate {
    match op {
        CmpOp::Eq => Predicate::eq(column, value),
        CmpOp::Neq => Predicate::not_eq(column, value),
        CmpOp::Lt => Predicate::lt(column, value),
        CmpOp::LtEq => Predicate::lt_eq(column, value),
        CmpOp::Gt => Predicate::gt(column, value),
        CmpOp::GtEq => Predicate::gt_eq(column, value),
    }
}

/// 谓词 → 行掩码（含分区列常量折叠）。
fn eval_mask(
    pred: &Predicate,
    batch: &RecordBatch,
    partition_values: &HashMap<String, Literal>,
) -> Result<BooleanArray> {
    match pred {
        Predicate::Eq { column, value } => {
            cmp_mask(batch, partition_values, column, value, CmpOp::Eq)
        }
        Predicate::NotEq { column, value } => {
            cmp_mask(batch, partition_values, column, value, CmpOp::Neq)
        }
        Predicate::Lt { column, value } => {
            cmp_mask(batch, partition_values, column, value, CmpOp::Lt)
        }
        Predicate::LtEq { column, value } => {
            cmp_mask(batch, partition_values, column, value, CmpOp::LtEq)
        }
        Predicate::Gt { column, value } => {
            cmp_mask(batch, partition_values, column, value, CmpOp::Gt)
        }
        Predicate::GtEq { column, value } => {
            cmp_mask(batch, partition_values, column, value, CmpOp::GtEq)
        }
        Predicate::And(a, b) => and_kleene(
            &eval_mask(a, batch, partition_values)?,
            &eval_mask(b, batch, partition_values)?,
        )
        .map_err(|e| QsdbError::invalid(format!("and failed: {e}"))),
        Predicate::Or(a, b) => or_kleene(
            &eval_mask(a, batch, partition_values)?,
            &eval_mask(b, batch, partition_values)?,
        )
        .map_err(|e| QsdbError::invalid(format!("or failed: {e}"))),
        Predicate::Not(a) => {
            let m = eval_mask(a, batch, partition_values)?;
            // arrow `not` 保留 null（Kleene NOT：unknown 的否定仍是 unknown）
            arrow::compute::not(&m).map_err(|e| QsdbError::invalid(format!("not failed: {e}")))
        }
    }
}

fn cmp_mask(
    batch: &RecordBatch,
    partition_values: &HashMap<String, Literal>,
    column: &str,
    value: &Literal,
    op: CmpOp,
) -> Result<BooleanArray> {
    let n = batch.num_rows();
    // NULL 字面量：比较结果全 UNKNOWN → 全部过滤（SQL 语义）
    if value.is_null() {
        return Ok(BooleanArray::from(vec![None::<bool>; n]));
    }
    // 分区列：常量折叠
    if let Some(pv) = partition_values.get(column) {
        let matched = match &op {
            CmpOp::Eq => lit_eq(pv, value) == Tri::True,
            CmpOp::Neq => lit_eq(pv, value) == Tri::False,
            _ => {
                let mut m = HashMap::new();
                m.insert(column, pv);
                eval_on_partition(&leaf_pred(column, value.clone(), &op), &m) == Tri::True
            }
        };
        return Ok((0..n).map(|_| Some(matched)).collect());
    }
    // 数据列
    let idx = batch
        .schema()
        .index_of(column)
        .map_err(|_| QsdbError::invalid(format!("column '{column}' not in read schema")))?;
    let col = batch.column(idx);
    let scalar = arrow::array::Scalar::new(literal_to_array(value, col.data_type())?);
    let lhs = ArrDatum(col.as_ref());
    let mask = match op {
        CmpOp::Eq => cmp::eq(&lhs, &scalar),
        CmpOp::Neq => cmp::neq(&lhs, &scalar),
        CmpOp::Lt => cmp::lt(&lhs, &scalar),
        CmpOp::LtEq => cmp::lt_eq(&lhs, &scalar),
        CmpOp::Gt => cmp::gt(&lhs, &scalar),
        CmpOp::GtEq => cmp::gt_eq(&lhs, &scalar),
    }
    .map_err(|e| QsdbError::type_err(format!("compare '{column}' failed: {e}")))?;
    Ok(mask)
}

/// RecordBatch 浅克隆（列 Arc 共享，零拷贝）。
fn clone_batch(b: &RecordBatch) -> RecordBatch {
    b.clone()
}

/// `&dyn Array` → Datum 适配器（Datum 仅为 Sized 数组实现）。
struct ArrDatum<'a>(&'a dyn Array);

impl arrow::array::Datum for ArrDatum<'_> {
    fn get(&self) -> (&dyn Array, bool) {
        (self.0, false)
    }
}

/// Literal → 单行 arrow 数组（data_type 取目标列，保证 tz/decimal 一致）。
fn literal_to_array(lit: &Literal, dt: &DataType) -> Result<ArrayRef> {
    Ok(match (lit, dt) {
        (Literal::Boolean(v), DataType::Boolean) => Arc::new(BooleanArray::from(vec![Some(*v)])),
        (Literal::Int8(v), DataType::Int8) => Arc::new(Int8Array::from(vec![Some(*v)])),
        (Literal::Int16(v), DataType::Int16) => Arc::new(Int16Array::from(vec![Some(*v)])),
        (Literal::Int32(v), DataType::Int32) => Arc::new(Int32Array::from(vec![Some(*v)])),
        (Literal::Int64(v), DataType::Int64) => Arc::new(Int64Array::from(vec![Some(*v)])),
        (Literal::Float32(v), DataType::Float32) => Arc::new(Float32Array::from(vec![Some(*v)])),
        (Literal::Float64(v), DataType::Float64) => Arc::new(Float64Array::from(vec![Some(*v)])),
        (Literal::Utf8(v), DataType::Utf8) => Arc::new(StringArray::from(vec![Some(v.as_str())])),
        (Literal::Date(v), DataType::Date32) => Arc::new(Date32Array::from(vec![Some(*v)])),
        (Literal::Time(v), DataType::Time64(arrow::datatypes::TimeUnit::Nanosecond)) => {
            Arc::new(Time64NanosecondArray::from(vec![Some(*v)]))
        }
        (Literal::Timestamp(v), DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, _)) => {
            Arc::new(TimestampNanosecondArray::from(vec![Some(*v)]).with_data_type(dt.clone()))
        }
        (Literal::Decimal { unscaled, .. }, DataType::Decimal128(_, _)) => {
            Arc::new(Decimal128Array::from(vec![Some(*unscaled)]).with_data_type(dt.clone()))
        }
        (l, dt) => {
            return Err(QsdbError::type_err(format!(
                "cannot build scalar of {dt:?} from {l:?}"
            )));
        }
    })
}

/// 组装输出批：读列取值 + 分区列常量 + 请求投影顺序。
fn assemble_output(
    filtered: &RecordBatch,
    plan: &ScanPlan,
    partition_values: &HashMap<String, Literal>,
) -> Result<RecordBatch> {
    let n = filtered.num_rows();
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(plan.projection.len());
    for name in &plan.projection {
        if let Some(pv) = partition_values.get(name) {
            let c = plan.def.column(name).unwrap();
            cols.push(constant_array(pv, &c.logical_type.arrow_type(), n)?);
        } else {
            let idx = filtered
                .schema()
                .index_of(name)
                .map_err(|_| QsdbError::invalid(format!("column '{name}' missing after read")))?;
            cols.push(Arc::clone(filtered.column(idx)));
        }
    }
    RecordBatch::try_new(Arc::clone(&plan.output_schema), cols)
        .map_err(|e| QsdbError::invalid(format!("assemble output failed: {e}")))
}

fn constant_array(lit: &Literal, dt: &DataType, n: usize) -> Result<ArrayRef> {
    let one = literal_to_array(lit, dt)?;
    let indices: Vec<(usize, usize)> = vec![(0, 0); n];
    arrow::compute::interleave(&[one.as_ref()], &indices)
        .map_err(|e| QsdbError::invalid(format!("const array failed: {e}")))
}

/// 文件 row group 数（分片细分用；失败由调用方回退文件粒度）。
fn row_group_count(store: &Store, loc: &FileLoc) -> Result<usize> {
    let disp = loc.display_path();
    let src = ChunkSource::open(Some(store), loc, None, false)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(src)
        .map_err(|e| QsdbError::meta(format!("read parquet footer {}: {e}", disp.display())))?;
    Ok(builder.metadata().num_row_groups())
}

/// 旧 segment 缺列补 NULL（ALTER ADD COLUMN 后向兼容；compaction/宽松写复用）。
/// missing 按表列序，追加在批尾（列集为表列前缀时序即表列序）。
pub(crate) fn pad_missing_columns(
    batch: RecordBatch,
    missing: &[(String, crate::types::LogicalType)],
) -> Result<RecordBatch> {
    let n = batch.num_rows();
    let mut fields: Vec<arrow::datatypes::FieldRef> =
        batch.schema().fields().iter().cloned().collect();
    let mut cols: Vec<ArrayRef> = batch.columns().to_vec();
    for (name, ty) in missing {
        fields.push(std::sync::Arc::new(arrow::datatypes::Field::new(
            name,
            ty.arrow_type(),
            true,
        )));
        cols.push(arrow::array::new_null_array(&ty.arrow_type(), n));
    }
    let schema = std::sync::Arc::new(arrow::datatypes::Schema::new(fields));
    RecordBatch::try_new(schema, cols)
        .map_err(|e| QsdbError::invalid(format!("pad missing columns: {e}")))
}

// ---------------------------------------------------------------------------
// 扫描迭代器
// ---------------------------------------------------------------------------

/// 流式扫描迭代器：`Iterator<Item = Result<RecordBatch>>`。
pub struct Scan {
    plan: Arc<ScanPlan>,
    files: std::collections::VecDeque<FilePlan>,
    current: Option<(BatchSource, FilePlan)>,
    /// 当前文件缺的列（ALTER ADD COLUMN 后旧 segment）：批读出后补 NULL
    missing_cols: Vec<(String, crate::types::LogicalType)>,
    /// miss 路径的回填收集器（文件读尽时 finish）
    pending_collector: Option<crate::cache::CacheCollector>,
    remaining: Option<u64>,
    cache: Option<Arc<crate::cache::SegmentCache>>,
    ctx: Arc<crate::query::QueryContext>,
    /// 存储句柄（缓存签名 stat；M6-2 起承载远端段缓存路径）
    store: Arc<Store>,
    /// 远端段文件磁盘缓存（外表专用；管理表本地不启用）
    file_cache: Option<Arc<crate::filecache::FileCache>>,
}

/// 批来源：parquet 流式 reader 或缓存命中批列表。
enum BatchSource {
    Parquet(ParquetRecordBatchReader),
    Cached(std::vec::IntoIter<RecordBatch>),
}

impl BatchSource {
    fn next_batch(&mut self) -> Option<Result<RecordBatch>> {
        match self {
            Self::Parquet(r) => r
                .next()
                .map(|b| b.map_err(|e| QsdbError::meta(format!("read batch failed: {e}")))),
            Self::Cached(v) => v.next().map(Ok),
        }
    }
}

impl Scan {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn with_cache(
        store: &Arc<Store>,
        table_dir: &Path,
        def: Arc<TableDef>,
        projection: Option<Vec<String>>,
        predicate: Option<Predicate>,
        limit: Option<u64>,
        cache: Option<Arc<crate::cache::SegmentCache>>,
        ctx: Arc<crate::query::QueryContext>,
    ) -> Result<Self> {
        let plan = ScanPlan::build(def, projection, predicate, limit)?;
        let files = plan_scan(store, table_dir, &plan)?;
        Ok(Self {
            plan: Arc::new(plan),
            files: files.into(),
            current: None,
            missing_cols: Vec::new(),
            pending_collector: None,
            remaining: limit,
            cache,
            ctx,
            store: Arc::clone(store),
            file_cache: None,
        })
    }

    /// 外表扫描（read_parquet，M6-2）：显式文件列表，无分区列。
    /// 每文件 footer row group 裁剪（远端 = range GET footer）。
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn external(
        store: &Arc<Store>,
        def: Arc<TableDef>,
        files: Vec<FileLoc>,
        projection: Option<Vec<String>>,
        predicate: Option<Predicate>,
        limit: Option<u64>,
        cache: Option<Arc<crate::cache::SegmentCache>>,
        file_cache: Option<Arc<crate::filecache::FileCache>>,
        ctx: Arc<crate::query::QueryContext>,
    ) -> Result<Self> {
        let plan = ScanPlan::build(def, projection, predicate, limit)?;
        let mut plans = Vec::with_capacity(files.len());
        for loc in files {
            let row_groups = prune_row_groups(store, &loc, &plan, file_cache.as_deref())?;
            plans.push(FilePlan {
                loc,
                partition_values: HashMap::new(),
                row_groups,
            });
        }
        Ok(Self {
            plan: Arc::new(plan),
            files: plans.into(),
            current: None,
            missing_cols: Vec::new(),
            pending_collector: None,
            remaining: limit,
            cache,
            ctx,
            store: Arc::clone(store),
            file_cache,
        })
    }

    /// 输出 schema（投影顺序）。
    pub fn schema(&self) -> &SchemaRef {
        &self.plan.output_schema
    }

    /// 按（近似）均分把扫描工作切给 n 个分片。`split_row_groups` = true 时文件数
    /// 不足 n 则按 parquet row group 细分大文件（读 footer 取 RG 数，LPT 加权均衡）
    /// ——适合扫描受限路径（过滤扫描）；聚合 partial 路径传 false（高基数分组下
    /// 细分会使每分片重复构建几乎全量组状态，得不偿失）。
    /// 不可分（文件数 < 2 或带 limit）→ None。分片共享计划（投影/谓词）。
    pub fn shard(&self, n: usize, split_row_groups: bool) -> Option<Vec<Scan>> {
        if self.remaining.is_some() || self.files.len() < 2 {
            return None;
        }
        // 展开为 RG 级工作单元（文件数 ≥ n 时保持文件粒度）
        let mut units: Vec<FilePlan> = Vec::new();
        let split = if split_row_groups {
            (n / self.files.len()).max(1)
        } else {
            1
        };
        for f in &self.files {
            if split > 1 {
                if let Ok(nrg) = row_group_count(&self.store, &f.loc) {
                    let universe: Vec<usize> = match &f.row_groups {
                        Some(list) if !list.is_empty() => list.clone(),
                        _ => (0..nrg).collect(),
                    };
                    let per = universe.len().div_ceil(split);
                    if universe.len() >= 2 && per < universe.len() {
                        for chunk in universe.chunks(per) {
                            units.push(FilePlan {
                                loc: f.loc.clone(),
                                partition_values: f.partition_values.clone(),
                                row_groups: Some(chunk.to_vec()),
                            });
                        }
                    } else {
                        units.push(FilePlan {
                            loc: f.loc.clone(),
                            partition_values: f.partition_values.clone(),
                            row_groups: Some(universe),
                        });
                    }
                    continue;
                }
            }
            units.push(FilePlan {
                loc: f.loc.clone(),
                partition_values: f.partition_values.clone(),
                row_groups: f.row_groups.clone(),
            });
        }
        // LPT：按 RG 数（缺省 1）加权均衡到 n 个分片
        let n = n.min(units.len());
        let weight = |u: &FilePlan| u.row_groups.as_ref().map_or(1, |r| r.len().max(1));
        let mut order: Vec<usize> = (0..units.len()).collect();
        order.sort_by_key(|&i| std::cmp::Reverse(weight(&units[i])));
        let mut loads = vec![0usize; n];
        let mut buckets: Vec<Vec<FilePlan>> = (0..n).map(|_| Vec::new()).collect();
        for i in order {
            let (bi, _) = loads.iter().enumerate().min_by_key(|(_, l)| **l).unwrap();
            buckets[bi].push(units[i].clone());
            loads[bi] += weight(&units[i]);
        }
        let mut shards: Vec<Scan> = (0..n)
            .map(|_| Scan {
                plan: Arc::clone(&self.plan),
                files: std::collections::VecDeque::new(),
                current: None,
                missing_cols: Vec::new(),
                pending_collector: None,
                remaining: None,
                cache: self.cache.clone(),
                ctx: Arc::clone(&self.ctx),
                store: Arc::clone(&self.store),
                file_cache: self.file_cache.clone(),
            })
            .collect();
        for (s, b) in shards.iter_mut().zip(buckets) {
            s.files = b.into();
        }
        Some(shards)
    }

    fn open_next(&mut self) -> Result<bool> {
        let Some(file) = self.files.pop_front() else {
            return Ok(false);
        };
        self.missing_cols.clear();
        let disp = file.loc.display_path();
        let src = ChunkSource::open(
            Some(&self.store),
            &file.loc,
            self.file_cache.as_deref(),
            file.row_groups.is_none(),
        )
        .map_err(|e| QsdbError::meta(format!("open {} failed: {e}", disp.display())))?;
        let mut builder = ParquetRecordBatchReaderBuilder::try_new(src)
            .map_err(|e| QsdbError::meta(format!("open parquet {}: {e}", disp.display())))?;
        // 投影：read_columns → parquet 叶列索引（扁平 schema：列 i = 叶 i）。
        // 文件缺列（ALTER ADD COLUMN 前的旧 segment）→ 记入 missing_cols，批读出后补 NULL。
        let file_schema = builder.schema();
        let mut leaves: Vec<usize> = Vec::with_capacity(self.plan.read_columns.len());
        for name in &self.plan.read_columns {
            match file_schema.index_of(name) {
                Ok(idx) => leaves.push(idx),
                Err(_) => {
                    let c = self.plan.def.column(name).ok_or_else(|| {
                        QsdbError::meta(format!(
                            "column '{name}' missing in parquet file {}",
                            disp.display()
                        ))
                    })?;
                    self.missing_cols
                        .push((name.clone(), c.logical_type.clone()));
                }
            }
        }
        // 热缓存：命中零解码（签名 stat 经 Store）。外表 s3:// 不经内存批缓存
        // （路径非本地，其本地性由段缓存承载）；管理表（本地/远端）照常。
        #[cfg(feature = "s3")]
        let cacheable = !matches!(file.loc, FileLoc::S3(_));
        #[cfg(not(feature = "s3"))]
        let cacheable = true;
        if let (Some(cache), true) = (&self.cache, cacheable) {
            if cache.is_enabled() {
                if let Ok(Some(md)) = self.store.stat(&disp) {
                    if let Some(hit) = cache.get(
                        &disp,
                        md.mtime_ms,
                        md.size,
                        &self.plan.read_columns,
                        &file.row_groups,
                    ) {
                        self.current = Some((
                            BatchSource::Cached(
                                hit.iter().cloned().collect::<Vec<_>>().into_iter(),
                            ),
                            file,
                        ));
                        return Ok(true);
                    }
                    // miss：装 collector 供回填
                    let collector = cache.collector(
                        disp.clone(),
                        md.mtime_ms,
                        md.size,
                        self.plan.read_columns.clone(),
                        file.row_groups.clone(),
                    );
                    self.pending_collector = Some(collector);
                }
            }
        }
        let mask = ProjectionMask::leaves(builder.parquet_schema(), leaves.iter().copied());
        builder = builder.with_projection(mask);
        if let Some(rgs) = &file.row_groups {
            builder = builder.with_row_groups(rgs.clone());
        }
        let reader = builder
            .with_batch_size(BATCH_SIZE)
            .build()
            .map_err(|e| QsdbError::meta(format!("build reader failed: {e}")))?;
        self.current = Some((BatchSource::Parquet(reader), file));
        Ok(true)
    }

    fn finish_batch(
        &mut self,
        batch: RecordBatch,
        partition_values: &HashMap<String, Literal>,
    ) -> Result<Option<RecordBatch>> {
        let mut out = batch;
        if !self.missing_cols.is_empty() {
            out = pad_missing_columns(out, &self.missing_cols.clone())?;
        }
        if let Some(pred) = &self.plan.predicate {
            let mask = eval_mask(pred, &out, partition_values)?;
            out = filter_record_batch(&out, &mask)
                .map_err(|e| QsdbError::invalid(format!("filter failed: {e}")))?;
        }
        if out.num_rows() == 0 {
            return Ok(None);
        }
        if let Some(rem) = self.remaining {
            if rem == 0 {
                return Ok(None);
            }
            if out.num_rows() as u64 > rem {
                out = out.slice(0, rem as usize);
            }
            let left = rem.saturating_sub(out.num_rows() as u64);
            self.remaining = Some(left);
            if left == 0 {
                self.files.clear(); // 提前终止
                self.current = None;
            }
        }
        let assembled = assemble_output(&out, &self.plan, partition_values)?;
        Ok(Some(assembled))
    }
}

impl Iterator for Scan {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Err(e) = self.ctx.check() {
                return Some(Err(e));
            }
            // 先取出批次与分区值，结束对 self 的借用
            let taken = match self.current.as_mut() {
                Some((source, file)) => match source.next_batch() {
                    None => None,
                    Some(Err(e)) => Some(Err(e)),
                    Some(Ok(b)) => Some(Ok((b, file.partition_values.clone()))),
                },
                None => None,
            };
            match taken {
                Some(Err(e)) => {
                    self.current = None;
                    return Some(Err(e));
                }
                Some(Ok((batch, pvals))) => {
                    // miss 路径：收集原始解码批（行过滤/组装前的，Arc clone 零拷贝）
                    if let Some(c) = &mut self.pending_collector {
                        c.push(clone_batch(&batch));
                    }
                    return match self.finish_batch(batch, &pvals) {
                        Ok(Some(b)) => Some(Ok(b)),
                        Ok(None) => None,
                        Err(e) => Some(Err(e)),
                    };
                }
                None => {
                    if self.current.is_some() {
                        self.current = None; // 当前文件读完：回填缓存
                        if let Some(c) = self.pending_collector.take() {
                            c.finish();
                        }
                    }
                }
            }
            match self.open_next() {
                Err(e) => return Some(Err(e)),
                Ok(false) => return None,
                Ok(true) => {}
            }
        }
    }
}
