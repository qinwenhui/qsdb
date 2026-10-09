//! 表级分区索引 `partitions.idx`：全分区 manifest 快照的单一缓存文件。
//!
//! 动机（M1 gate 实测）：万级分区逐个 open+read manifest 在 Windows 上
//! ~0.2ms/文件 → 2s+，远超 50ms 红线；单文件索引读+反序列化 ~10ms 达标。
//!
//! 一致性协议：
//! - 写侧：分区锁内 commit manifest → 表级 `_index.lock` 内 upsert 条目
//!   → tmp+rename（锁顺序恒为 分区锁 → 索引锁，无死锁）
//! - 读侧：乐观信任 idx；idx 缺失/损坏 → 回退逐分区读（慢路径）
//! - 跨进程 stale 窗口：他进程 commit manifest 与 upsert idx 之间有毫秒级
//!   延迟（最终一致）；`Qsdb::refresh_index` 可强制重建

use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{QsdbError, Result};
use crate::fsutil::{PartitionLock, write_atomic};
use crate::manifest::PartitionManifest;
use crate::scan::list_partitions;

pub const INDEX_FILE: &str = "partitions.idx";
pub const INDEX_LOCK: &str = "_index.lock";

/// 表级分区索引。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PartitionsIndex {
    pub version: u32,
    /// (相对表目录的分区目录名, 该分区 manifest 快照)，按目录名排序
    pub entries: Vec<(String, PartitionManifest)>,
}

impl PartitionsIndex {
    /// 读取索引；不存在 → None；损坏 → None（读侧乐观回退）。
    pub fn read(table_dir: &Path) -> Option<Self> {
        let bytes = fs::read(table_dir.join(INDEX_FILE)).ok()?;
        let idx: Self = postcard::from_bytes(&bytes).ok()?;
        (idx.version == 1).then_some(idx)
    }

    /// 锁内 upsert 一个分区条目（commit 后调用）。
    pub fn upsert(table_dir: &Path, rel_dir: &str, manifest: &PartitionManifest) -> Result<()> {
        let _guard = PartitionLock::acquire(&table_dir.join(INDEX_LOCK))?;
        let mut idx = Self::read(table_dir).unwrap_or_else(|| Self {
            version: 1,
            entries: Vec::new(),
        });
        match idx
            .entries
            .binary_search_by(|(d, _)| d.as_str().cmp(rel_dir))
        {
            Ok(pos) => idx.entries[pos].1 = manifest.clone(),
            Err(pos) => idx
                .entries
                .insert(pos, (rel_dir.to_string(), manifest.clone())),
        }
        let bytes = postcard::to_allocvec(&idx)
            .map_err(|e| QsdbError::meta(format!("serialize index failed: {e}")))?;
        write_atomic(table_dir.join(INDEX_FILE), &bytes)
    }

    /// 全量重建（锁内）：readdir + 逐分区读 manifest。
    pub fn rebuild(table_dir: &Path, partition_depth: usize) -> Result<Self> {
        let _guard = PartitionLock::acquire(&table_dir.join(INDEX_LOCK))?;
        let mut entries: Vec<(String, PartitionManifest)> = Vec::new();
        for (pdir, _) in list_partitions(table_dir, partition_depth) {
            if let Some(m) = PartitionManifest::read(&pdir)? {
                let rel = pdir
                    .strip_prefix(table_dir)
                    .unwrap_or(&pdir)
                    .to_string_lossy()
                    .replace('\\', "/");
                entries.push((rel, m));
            }
        }
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        let idx = Self {
            version: 1,
            entries,
        };
        let bytes = postcard::to_allocvec(&idx)
            .map_err(|e| QsdbError::meta(format!("serialize index failed: {e}")))?;
        write_atomic(table_dir.join(INDEX_FILE), &bytes)?;
        Ok(idx)
    }

    /// 索引文件路径（删除/运维用）。
    pub fn path(table_dir: &Path) -> PathBuf {
        table_dir.join(INDEX_FILE)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::SegmentEntry;
    use crate::types::Literal;

    fn tmpdir(tag: &str) -> PathBuf {
        let d = std::env::temp_dir()
            .join("qsdb_index_test")
            .join(format!("{tag}_{}", uuid::Uuid::now_v7().simple()));
        fs::create_dir_all(&d).unwrap();
        d
    }

    fn manifest(generation: u64, rows: u64) -> PartitionManifest {
        PartitionManifest {
            version: 1,
            generation,
            partition_values: vec![("dt".into(), Literal::Date(1))],
            segments: vec![SegmentEntry {
                file_name: "seg_x.parquet".into(),
                row_count: rows,
                byte_size: 1,
                created_at: 0,
                column_stats: vec![],
            }],
            schema_fingerprint: 7,
        }
    }

    #[test]
    fn upsert_sorted_and_roundtrip() {
        let dir = tmpdir("upsert");
        PartitionsIndex::upsert(&dir, "dt=b", &manifest(1, 10)).unwrap();
        PartitionsIndex::upsert(&dir, "dt=a", &manifest(2, 20)).unwrap();
        PartitionsIndex::upsert(&dir, "dt=b", &manifest(3, 30)).unwrap(); // 覆盖
        let idx = PartitionsIndex::read(&dir).unwrap();
        let dirs: Vec<&str> = idx.entries.iter().map(|(d, _)| d.as_str()).collect();
        assert_eq!(dirs, vec!["dt=a", "dt=b"]); // 有序
        assert_eq!(idx.entries[1].1.generation, 3);
        assert_eq!(idx.entries[1].1.total_rows(), 30);
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn corrupt_index_falls_back_to_none() {
        let dir = tmpdir("corrupt");
        fs::write(dir.join(INDEX_FILE), b"not postcard").unwrap();
        assert!(PartitionsIndex::read(&dir).is_none());
        fs::remove_dir_all(&dir).ok();
    }
}
