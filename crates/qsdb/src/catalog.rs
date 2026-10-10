//! 表定义与 catalog：`table.json`（人类可读，低频变更）与
//! `_catalog/{db}.postcard`（表名清单）。
//!
//! 真值层级：磁盘文件为准，内存仅做 mtime 缓存（多进程安全靠
//! 「stat 比对 + 锁内提交」）。

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};

use crate::error::{QsdbError, Result};
use crate::fsutil::{PartitionLock, now_ms, write_atomic};
use crate::types::{Literal, LogicalType};

pub const DEFAULT_DB: &str = "default";

/// 列定义（table.json 一级结构）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub logical_type: LogicalType,
    pub nullable: bool,
    /// 列级 CHECK 约束（原始表达式文本；写入端重解析绑定。仅可引用数据列）
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub check: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<Literal>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

/// 表定义。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableDef {
    pub version: u32,
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub partition_keys: Vec<String>,
    pub created_at: i64,
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

impl TableDef {
    /// 构造并校验（当前时间戳自动填充）。
    pub fn new(
        name: impl Into<String>,
        columns: Vec<ColumnDef>,
        partition_keys: Vec<String>,
    ) -> Result<Self> {
        let def = Self {
            version: 1,
            name: name.into(),
            columns,
            partition_keys,
            created_at: now_ms() as i64,
            properties: HashMap::new(),
        };
        def.validate()?;
        Ok(def)
    }

    /// 构造后校验（从磁盘读回时也调用）。
    pub fn validate(&self) -> Result<()> {
        validate_ident(&self.name, "table")?;
        if self.version != 1 {
            return Err(QsdbError::meta(format!(
                "table.json version {} not supported (expect 1)",
                self.version
            )));
        }
        if self.columns.is_empty() {
            return Err(QsdbError::invalid("table needs at least one column"));
        }
        let mut seen = std::collections::HashSet::new();
        for col in &self.columns {
            validate_ident(&col.name, "column")?;
            if !seen.insert(col.name.as_str()) {
                return Err(QsdbError::invalid(format!(
                    "duplicate column '{}'",
                    col.name
                )));
            }
            if !col.nullable && self.partition_keys.iter().any(|k| k == &col.name) {
                // 分区列的值存在目录名中、数据文件内不存储，读回时由分区值重建，
                // 声明为 NOT NULL 会造成读写语义不一致，一律要求 nullable
                return Err(QsdbError::invalid(format!(
                    "partition column '{}' must be nullable (values live in directory names)",
                    col.name
                )));
            }
        }
        for key in &self.partition_keys {
            let col = self
                .columns
                .iter()
                .find(|c| &c.name == key)
                .ok_or_else(|| {
                    QsdbError::invalid(format!("partition key '{key}' is not a column"))
                })?;
            if !Self::partition_type_ok(&col.logical_type) {
                return Err(QsdbError::invalid(format!(
                    "partition column '{}' type {} not supported for partitioning (allow: integers/string/date/decimal/timestamp/boolean)",
                    key, col.logical_type
                )));
            }
        }
        Ok(())
    }

    /// 分区列类型限制：排除 float（NaN 目录名不稳定）与 interval（无全序）。
    fn partition_type_ok(t: &LogicalType) -> bool {
        !matches!(
            t,
            LogicalType::Float32
                | LogicalType::Float64
                | LogicalType::Interval
                | LogicalType::Time
                | LogicalType::Binary
        )
    }

    pub fn column(&self, name: &str) -> Option<&ColumnDef> {
        self.columns.iter().find(|c| c.name == name)
    }

    /// 数据文件 schema（不含分区列；分区值在目录名中）。
    pub fn data_columns(&self) -> Vec<&ColumnDef> {
        self.columns
            .iter()
            .filter(|c| !self.partition_keys.contains(&c.name))
            .collect()
    }

    /// schema 指纹：列定义 postcard 字节的 SipHash。
    pub fn schema_fingerprint(&self) -> u64 {
        use std::hash::{Hash, Hasher};
        let bytes =
            postcard::to_allocvec(&(&self.columns, &self.partition_keys)).unwrap_or_default();
        let mut h = std::collections::hash_map::DefaultHasher::new();
        bytes.hash(&mut h);
        h.finish()
    }
}

/// 标识符：`[A-Za-z_][A-Za-z0-9_]*`，≤64 字符。
fn validate_ident(name: &str, what: &str) -> Result<()> {
    let ok = name.len() <= 64
        && !name.is_empty()
        && name
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
    if ok {
        Ok(())
    } else {
        Err(QsdbError::invalid(format!(
            "invalid {what} name '{name}' (expect [A-Za-z_][A-Za-z0-9_]*, <=64 chars)"
        )))
    }
}

// ---------------------------------------------------------------------------
// 路径布局
// ---------------------------------------------------------------------------

pub fn db_dir(root: &Path, db: &str) -> PathBuf {
    root.join(db)
}

pub fn table_dir(root: &Path, db: &str, table: &str) -> PathBuf {
    db_dir(root, db).join(table)
}

pub fn table_json_path(root: &Path, db: &str, table: &str) -> PathBuf {
    table_dir(root, db, table).join("table.json")
}

pub fn catalog_file(root: &Path, db: &str) -> PathBuf {
    root.join("_catalog").join(format!("{db}.postcard"))
}

pub fn catalog_lock(root: &Path) -> PathBuf {
    root.join("_catalog").join(".lock")
}

// ---------------------------------------------------------------------------
// 表名清单（_catalog/{db}.postcard = 排序表名表）
// ---------------------------------------------------------------------------

pub fn read_table_names(root: &Path, db: &str) -> Result<Vec<String>> {
    let path = catalog_file(root, db);
    match fs::read(&path) {
        Ok(bytes) => postcard::from_bytes(&bytes)
            .map_err(|e| QsdbError::meta(format!("corrupt catalog {db}: {e}"))),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(QsdbError::meta(format!("read catalog {db} failed: {e}"))),
    }
}

fn write_table_names(root: &Path, db: &str, mut names: Vec<String>) -> Result<()> {
    names.sort();
    let bytes = postcard::to_allocvec(&names)
        .map_err(|e| QsdbError::meta(format!("serialize catalog failed: {e}")))?;
    write_atomic(catalog_file(root, db), &bytes)
}

/// 建表：catalog 锁内（清单+目录+table.json）一次性落地。
pub fn create_table(root: &Path, db: &str, def: &TableDef) -> Result<()> {
    def.validate()?;
    let _guard = PartitionLock::acquire(&catalog_lock(root))?;
    let mut names = read_table_names(root, db)?;
    if names.iter().any(|n| n == &def.name) {
        return Err(QsdbError::already_exists(format!(
            "table {db}.{}",
            def.name
        )));
    }
    let tdir = table_dir(root, db, &def.name);
    if tdir.exists() {
        return Err(QsdbError::already_exists(format!(
            "table dir exists: {}",
            tdir.display()
        )));
    }
    fs::create_dir_all(&tdir).map_err(|e| QsdbError::meta(format!("mkdir table failed: {e}")))?;
    let json = serde_json_compat::to_string(def)?;
    write_atomic(table_json_path(root, db, &def.name), json.as_bytes())?;
    names.push(def.name.clone());
    write_table_names(root, db, names)?;
    Ok(())
}

/// 删表：目录整体移 trash，清单去名。
pub fn drop_table(root: &Path, db: &str, table: &str) -> Result<()> {
    let _guard = PartitionLock::acquire(&catalog_lock(root))?;
    let names = read_table_names(root, db)?;
    if !names.iter().any(|n| n == table) {
        return Err(QsdbError::not_found(format!("table {db}.{table}")));
    }
    let tdir = table_dir(root, db, table);
    crate::fsutil::move_to_trash(root, &tdir)?;
    let names: Vec<String> = names.into_iter().filter(|n| n != table).collect();
    write_table_names(root, db, names)
}

// ---------------------------------------------------------------------------
// table.json 读取（带 mtime 缓存）
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
pub struct TableDefCache {
    inner: RwLock<HashMap<(String, String), CacheEntry>>,
}

#[derive(Debug)]
struct CacheEntry {
    mtime_ms: u64,
    size: u64,
    def: Arc<TableDef>,
}

impl TableDefCache {
    /// 读取表定义：缓存命中且 stat 未变 → 直接返回；否则读盘解析。
    pub fn get(&self, root: &Path, db: &str, table: &str) -> Result<Arc<TableDef>> {
        let path = table_json_path(root, db, table);
        let md =
            fs::metadata(&path).map_err(|_| QsdbError::not_found(format!("table {db}.{table}")))?;
        let mtime_ms = md
            .modified()
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let key = (db.to_string(), table.to_string());
        {
            let cache = self
                .inner
                .read()
                .map_err(|_| QsdbError::meta("table cache poisoned"))?;
            if let Some(e) = cache.get(&key) {
                if e.mtime_ms == mtime_ms && e.size == md.len() {
                    return Ok(Arc::clone(&e.def));
                }
            }
        }
        let text = fs::read_to_string(&path)
            .map_err(|e| QsdbError::meta(format!("read table.json failed: {e}")))?;
        let def: TableDef = serde_json_compat::from_str(&text).map_err(|e| {
            QsdbError::meta(format!("parse table.json of {db}.{table} failed: {e}"))
        })?;
        def.validate()?;
        let arc = Arc::new(def);
        self.inner
            .write()
            .map_err(|_| QsdbError::meta("table cache poisoned"))?
            .insert(
                key,
                CacheEntry {
                    mtime_ms,
                    size: md.len(),
                    def: Arc::clone(&arc),
                },
            );
        Ok(arc)
    }
}

/// JSON 兼容层：直接用 serde_json（保持依赖最小）。
pub(crate) mod serde_json_compat {
    use super::*;

    pub fn to_string(def: &TableDef) -> Result<String> {
        serde_json::to_string_pretty(def)
            .map_err(|e| QsdbError::meta(format!("serialize table.json failed: {e}")))
    }

    pub fn from_str(s: &str) -> Result<TableDef> {
        serde_json::from_str(s)
            .map_err(|e| QsdbError::meta(format!("deserialize table.json failed: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::DecimalMeta;

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
            .join("qsdb_catalog_test")
            .join(format!("{tag}_{}", uuid::Uuid::now_v7().simple()));
        fs::create_dir_all(&d).unwrap();
        d
    }

    fn sample_def() -> TableDef {
        TableDef::new(
            "events",
            vec![
                col("tenant", LogicalType::Varchar { max_chars: None }),
                col("amount", LogicalType::Decimal(DecimalMeta::new(18, 4))),
                col("dt", LogicalType::Date),
            ],
            vec!["dt".into()],
        )
        .unwrap()
    }

    #[test]
    fn validate_rules() {
        assert!(TableDef::new("bad-name", vec![col("a", LogicalType::Int64)], vec![]).is_err());
        assert!(TableDef::new("t", vec![], vec![]).is_err());
        // 重复列
        let dup = TableDef::new(
            "t",
            vec![col("a", LogicalType::Int64), col("a", LogicalType::Int32)],
            vec![],
        );
        assert!(dup.is_err());
        // 分区键必须是列
        let bad_key = TableDef::new("t", vec![col("a", LogicalType::Int64)], vec!["nope".into()]);
        assert!(bad_key.is_err());
        // float 不可做分区列
        let float_part = TableDef::new("t", vec![col("f", LogicalType::Float64)], vec!["f".into()]);
        assert!(float_part.is_err());
        assert!(sample_def().validate().is_ok());
    }

    #[test]
    fn fingerprint_stable_and_sensitive() {
        let a = sample_def();
        let b = sample_def();
        assert_eq!(a.schema_fingerprint(), b.schema_fingerprint());
        let mut c = sample_def();
        c.columns[1].logical_type = LogicalType::Decimal(DecimalMeta::new(18, 2));
        assert_ne!(a.schema_fingerprint(), c.schema_fingerprint());
    }

    #[test]
    fn create_read_drop_roundtrip() {
        let root = tmp_root("rt");
        let def = sample_def();
        create_table(&root, DEFAULT_DB, &def).unwrap();
        // 重复建表
        assert!(create_table(&root, DEFAULT_DB, &def).is_err());
        // 清单
        let names = read_table_names(&root, DEFAULT_DB).unwrap();
        assert_eq!(names, vec!["events".to_string()]);
        // 读取 + 缓存
        let cache = TableDefCache::default();
        let got = cache.get(&root, DEFAULT_DB, "events").unwrap();
        assert_eq!(*got, def);
        // 不存在
        assert!(cache.get(&root, DEFAULT_DB, "nope").is_err());
        // 删除
        drop_table(&root, DEFAULT_DB, "events").unwrap();
        assert!(read_table_names(&root, DEFAULT_DB).unwrap().is_empty());
        assert!(cache.get(&root, DEFAULT_DB, "events").is_err());
        // trash 立即清理
        crate::fsutil::purge_trash(&root, 0).unwrap();
        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn data_columns_excludes_partition() {
        let def = sample_def();
        let names: Vec<&str> = def.data_columns().iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["tenant", "amount"]);
    }
}
