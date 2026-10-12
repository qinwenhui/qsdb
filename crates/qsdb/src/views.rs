//! 视图（M7-B4）：`_catalog/views.json` 存视图 SQL；SELECT 时重解析展开为子计划。
//! 非物化视图——每次查询展开重算（分析库读多写少，与表同路径享受裁剪/并行）。

use std::collections::HashMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::error::{QsdbError, Result};
use crate::store::Store;

#[derive(Debug, Serialize, Deserialize, Default)]
struct ViewsFile {
    /// (name, sql) 按名排序落盘
    views: Vec<(String, String)>,
}

/// 视图存储（`_catalog/views.json`；catalog 锁内原子写）。
pub struct ViewStore;

impl ViewStore {
    /// 存储路径。
    pub fn path(store: &Store) -> PathBuf {
        store.root().join("_catalog").join("views.json")
    }

    /// 读取全部视图（name → SQL；无文件 = 空集）。
    pub fn read_all(store: &Store) -> Result<HashMap<String, String>> {
        let path = Self::path(store);
        match store.read_to_string(&path)? {
            Some(text) => {
                let f: ViewsFile = serde_json::from_str(&text)
                    .map_err(|e| QsdbError::meta(format!("corrupt views.json: {e}")))?;
                Ok(f.views.into_iter().collect())
            }
            None => Ok(HashMap::new()),
        }
    }

    /// catalog 锁内写回（整体替换）。
    fn write_all(store: &Store, views: &HashMap<String, String>) -> Result<()> {
        let mut list: Vec<(&String, &String)> = views.iter().collect();
        list.sort_by(|a, b| a.0.cmp(b.0));
        let f = ViewsFile {
            views: list
                .into_iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        };
        let text = serde_json::to_string_pretty(&f)
            .map_err(|e| QsdbError::meta(format!("serialize views failed: {e}")))?;
        let _guard =
            crate::fsutil::PartitionLock::acquire(&store.root().join("_catalog").join(".lock"))?;
        store.write_atomic(&Self::path(store), text.as_bytes())
    }

    /// 创建/替换视图（SQL 已过可解析校验）。
    pub fn create(store: &Store, name: &str, sql: &str, or_replace: bool) -> Result<()> {
        let mut all = Self::read_all(store)?;
        if all.contains_key(name) && !or_replace {
            return Err(QsdbError::already_exists(format!(
                "view '{name}' already exists"
            )));
        }
        all.insert(name.to_string(), sql.to_string());
        Self::write_all(store, &all)
    }

    /// 删除视图。
    pub fn drop_view(store: &Store, name: &str, if_exists: bool) -> Result<()> {
        let mut all = Self::read_all(store)?;
        if all.remove(name).is_none() && !if_exists {
            return Err(QsdbError::not_found(format!("view '{name}' not found")));
        }
        Self::write_all(store, &all)
    }
}
