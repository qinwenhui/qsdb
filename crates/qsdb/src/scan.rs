//! 读路径：谓词、三级裁剪（分区目录 → manifest 列统计 → parquet footer
//! row group）、行级过滤、流式扫描迭代器。
//!
//! 语义约定：
//! - 分区级：谓词只作用于分区列的常量值，只有「确定为假」才丢弃（Kleene 三值）
//! - segment 级：min/max 统计，只有「确定无匹配」才丢弃；无统计的列不裁剪
//! - row group 级：parquet footer 统计（数值/字符串），同上保守策略
//! - 行级：arrow cmp kernels；NULL 比较结果为 NULL → 行被过滤（SQL 语义）

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

use crate::catalog::TableDef;
use crate::error::{QsdbError, Result};
use crate::manifest::{ColumnStats, PartitionManifest};
use crate::partition::{parse_partition_dir, parse_partition_value};
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
enum Tri {
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
// 扫描计划
// ---------------------------------------------------------------------------

/// 一个待读 parquet 文件。
struct FilePlan {
    path: PathBuf,
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
pub fn list_partitions(table_dir: &Path, depth: usize) -> Vec<(PathBuf, Vec<(String, String)>)> {
    let mut level_dirs = vec![table_dir.to_path_buf()];
    for _ in 0..depth {
        let mut next = Vec::new();
        for dir in &level_dirs {
            let Ok(rd) = std::fs::read_dir(dir) else {
                continue;
            };
            for entry in rd.flatten() {
                let p = entry.path();
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if p.is_dir()
                    && name.contains('=')
                    && !name.starts_with('_')
                    && !name.starts_with('.')
                {
                    next.push(p);
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
fn plan_scan(root: &Path, table_dir: &Path, plan: &ScanPlan) -> Result<Vec<FilePlan>> {
    let mut files = Vec::new();
    if let Some(idx) = crate::index::PartitionsIndex::read(table_dir) {
        for (rel, manifest) in &idx.entries {
            let pdir = table_dir.join(rel);
            // 索引内 manifest 自带分区值 Literal，无需解析目录名
            let pvals: HashMap<String, Literal> =
                manifest.partition_values.iter().cloned().collect();
            add_partition_files(root, &pdir, pvals, manifest, plan, &mut files)?;
        }
        return Ok(files);
    }
    for (pdir, dir_vals) in list_partitions(table_dir, plan.def.partition_keys.len()) {
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
        let Some(manifest) = PartitionManifest::read(&pdir)? else {
            continue;
        };
        add_partition_files(root, &pdir, pvals, &manifest, plan, &mut files)?;
    }
    Ok(files)
}

/// 单分区的裁剪与文件规划（分区裁剪 + fingerprint 校验 + segment 裁剪）。
fn add_partition_files(
    root: &Path,
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

    if manifest.schema_fingerprint != plan.def.schema_fingerprint() {
        let rel = pdir.strip_prefix(root).unwrap_or(pdir);
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
        let path = pdir.join(&seg.file_name);
        let row_groups = prune_row_groups(&path, plan)?;
        files.push(FilePlan {
            path,
            partition_values: pvals.clone(),
            row_groups,
        });
    }
    Ok(())
}

/// parquet footer row group 裁剪（数值/字符串；无统计/失败 → 全读）。
fn prune_row_groups(path: &Path, plan: &ScanPlan) -> Result<Option<Vec<usize>>> {
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

    let file = File::open(path)
        .map_err(|e| QsdbError::meta(format!("open {} failed: {e}", path.display())))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| QsdbError::meta(format!("read parquet footer {}: {e}", path.display())))?;
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

// ---------------------------------------------------------------------------
// 扫描迭代器
// ---------------------------------------------------------------------------

/// 流式扫描迭代器：`Iterator<Item = Result<RecordBatch>>`。
pub struct Scan {
    plan: Arc<ScanPlan>,
    files: std::collections::VecDeque<FilePlan>,
    current: Option<(ParquetRecordBatchReader, FilePlan)>,
    remaining: Option<u64>,
}

impl Scan {
    pub(crate) fn new(
        root: &Path,
        table_dir: &Path,
        def: Arc<TableDef>,
        projection: Option<Vec<String>>,
        predicate: Option<Predicate>,
        limit: Option<u64>,
    ) -> Result<Self> {
        let plan = ScanPlan::build(def, projection, predicate, limit)?;
        let files = plan_scan(root, table_dir, &plan)?;
        Ok(Self {
            plan: Arc::new(plan),
            files: files.into(),
            current: None,
            remaining: limit,
        })
    }

    /// 输出 schema（投影顺序）。
    pub fn schema(&self) -> &SchemaRef {
        &self.plan.output_schema
    }

    fn open_next(&mut self) -> Result<bool> {
        let Some(file) = self.files.pop_front() else {
            return Ok(false);
        };
        let f = File::open(&file.path)
            .map_err(|e| QsdbError::meta(format!("open {} failed: {e}", file.path.display())))?;
        let mut builder = ParquetRecordBatchReaderBuilder::try_new(f)
            .map_err(|e| QsdbError::meta(format!("open parquet {}: {e}", file.path.display())))?;
        // 投影：read_columns → parquet 叶列索引（扁平 schema：列 i = 叶 i）
        let file_schema = builder.schema();
        let mut leaves: Vec<usize> = Vec::with_capacity(self.plan.read_columns.len());
        for name in &self.plan.read_columns {
            let idx = file_schema.index_of(name).map_err(|_| {
                QsdbError::meta(format!(
                    "column '{name}' missing in parquet file {}",
                    file.path.display()
                ))
            })?;
            leaves.push(idx);
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
        self.current = Some((reader, file));
        Ok(true)
    }

    fn finish_batch(
        &mut self,
        batch: RecordBatch,
        partition_values: &HashMap<String, Literal>,
    ) -> Result<Option<RecordBatch>> {
        let mut out = batch;
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
            // 先取出批次与分区值，结束对 self 的借用
            let taken = match self.current.as_mut() {
                Some((reader, file)) => match reader.next() {
                    None => None,
                    Some(Err(e)) => Some(Err(QsdbError::meta(format!("read batch failed: {e}")))),
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
                    return match self.finish_batch(batch, &pvals) {
                        Ok(Some(b)) => Some(Ok(b)),
                        Ok(None) => None,
                        Err(e) => Some(Err(e)),
                    };
                }
                None => {
                    if self.current.is_some() {
                        self.current = None; // 当前文件读完
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
