//! 分区 manifest：单一文件 = 分区唯一提交点（format-spec §4）。
//!
//! - append：锁内 旧 segments + 新 segment → generation+1 → 原子替换
//! - overwrite：锁内 只含新 segments 的 manifest 原子替换
//! - manifest 不存在 = 空分区（合法状态）
//! - rename 即提交；rename 前中断产生的孤儿数据文件由 GC 回收

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{QsdbError, Result};
use crate::store::Store;
use crate::types::Literal;

pub const MANIFEST_FILE: &str = "manifest.postcard";

/// 列统计（manifest 内嵌；min/max 不含 NULL 与 NaN）。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ColumnStats {
    pub min: Option<Literal>,
    pub max: Option<Literal>,
    pub null_count: u64,
}

/// 单个数据文件（segment）条目。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SegmentEntry {
    pub file_name: String,
    pub row_count: u64,
    pub byte_size: u64,
    pub created_at: i64,
    /// (列名, 统计)，按表列序（只含数据列）
    pub column_stats: Vec<(String, ColumnStats)>,
}

/// 分区清单。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PartitionManifest {
    pub version: u32,
    pub generation: u64,
    pub partition_values: Vec<(String, Literal)>,
    pub segments: Vec<SegmentEntry>,
    pub schema_fingerprint: u64,
    /// 远端多写者诊断（存储设计 §7.2）：写者进程标识，仅用于 GC/诊断，
    /// 不参与正确性。本地提交恒 0（旧 manifest 兼容：字段缺省即 0）。
    ///
    /// **别与下面的 `epoch` 混用**：这个是"哪个进程写的"（每次启动随机），
    /// `epoch` 是"哪一代写者"（持久、单调、参与正确性）。
    #[serde(default)]
    pub writer_epoch: u64,
    /// **写者身份代**（qsdb-sync F25）。由部署显式配置
    /// （`QsdbOptions::writer_epoch`，默认 0），每次提交原样落盘。
    ///
    /// 与 `generation` 的分工：
    /// - `generation` 是**分区提交计数器**（本地每次 +1），回答"这个分区被改过几次"；
    /// - `epoch` 是**写者身份**，回答"这块数据属于哪一代写者"，**接管（failover）时
    ///   由运维显式抬高**，进程自己永不自增。
    ///
    /// 为什么要它：`generation` 是计数器不是内容指纹，防脑裂只是**单向**保证
    /// （`dst > src` 拒绝），`dst == src` 并不蕴含内容一致 —— 脑裂后两边各自演进
    /// 到同一个 generation 就会**静默分歧**。生态侧（qsdb-sync replica）改用
    /// `(epoch, generation)` 复合序即可根除撞值：epoch 不同即拒绝协商，
    /// 不依赖任何内容比对。
    ///
    /// 缺省 0 = 未启用身份代（旧 manifest 兼容：字段缺省即 0）。
    #[serde(default)]
    pub epoch: u64,
}

impl PartitionManifest {
    pub fn new(partition_values: Vec<(String, Literal)>, schema_fingerprint: u64) -> Self {
        Self {
            version: 1,
            generation: 0,
            partition_values,
            segments: Vec::new(),
            schema_fingerprint,
            writer_epoch: 0,
            epoch: 0,
        }
    }

    /// **写者身份校验**：盘上的 epoch 比本进程配置的大 ⇒ 本进程是**过期写者**，
    /// 拒绝写入（qsdb-sync F25 的第二半——光记录 epoch 只是让生态能判，
    /// 这条让内核自己也不许"退回去写"）。
    ///
    /// 默认全 0 时**恒通过**（未启用身份代 = 旧行为不变）；只有运维显式抬高过
    /// epoch 之后，忘配/配小的进程才会被拦下——这正是我们要的"显式拒绝"。
    pub fn check_writer_epoch(&self, configured: u64) -> Result<()> {
        if self.epoch > configured {
            return Err(QsdbError::stale_writer(format!(
                "partition (values {:?}) was last written by writer epoch {} but this process                  is epoch {} — refusing to write as a stale writer. If this process really                  should take over, start it with --writer-epoch {} (or larger); otherwise                  it is pointed at the wrong database.",
                self.partition_values, self.epoch, configured, self.epoch
            )));
        }
        Ok(())
    }

    pub fn manifest_path(partition_dir: &Path) -> PathBuf {
        partition_dir.join(MANIFEST_FILE)
    }

    /// 读取 manifest；不存在 → Ok(None)（空分区）。
    pub fn read(store: &Store, partition_dir: &Path) -> Result<Option<Self>> {
        let path = Self::manifest_path(partition_dir);
        match store.read(&path)? {
            Some(bytes) => {
                let m: Self = postcard::from_bytes(&bytes).map_err(|e| {
                    QsdbError::meta(format!("corrupt manifest {}: {e}", path.display()))
                })?;
                if m.version != 1 {
                    return Err(QsdbError::meta(format!(
                        "manifest {} version {} not supported",
                        path.display(),
                        m.version
                    )));
                }
                Ok(Some(m))
            }
            None => Ok(None),
        }
    }

    /// 原子提交（在分区锁内调用）：generation+1、**写者身份代盖章**后替换磁盘文件。
    ///
    /// `epoch` 来自部署配置（[`crate::QsdbOptions::writer_epoch`]）——提交时无条件
    /// 盖章，所以"谁最后写的"在盘上永远可查。
    pub fn commit(&mut self, store: &Store, partition_dir: &Path, epoch: u64) -> Result<()> {
        self.generation += 1;
        self.epoch = epoch;
        let bytes = postcard::to_allocvec(self)
            .map_err(|e| QsdbError::meta(format!("serialize manifest failed: {e}")))?;
        store.write_atomic(&Self::manifest_path(partition_dir), &bytes)
    }

    pub fn stats_of(&self, column: &str) -> Option<&ColumnStats> {
        self.segments.iter().find_map(|s| {
            s.column_stats
                .iter()
                .find(|(n, _)| n == column)
                .map(|(_, st)| st)
        })
    }

    /// 分区总行数。
    pub fn total_rows(&self) -> u64 {
        self.segments.iter().map(|s| s.row_count).sum()
    }

    /// 数据文件绝对路径列表。
    pub fn segment_paths(&self, partition_dir: &Path) -> Vec<PathBuf> {
        self.segments
            .iter()
            .map(|s| partition_dir.join(&s.file_name))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::Store;
    use crate::types::DecimalMeta;

    fn tmpdir(tag: &str) -> (Store, PathBuf) {
        let d = std::env::temp_dir()
            .join("qsdb_manifest_test")
            .join(format!("{tag}_{}", uuid::Uuid::now_v7().simple()));
        std::fs::create_dir_all(&d).unwrap();
        (Store::local(&d), d)
    }

    fn sample(vals: Vec<(String, Literal)>) -> PartitionManifest {
        let mut m = PartitionManifest::new(vals, 42);
        m.segments.push(SegmentEntry {
            file_name: "seg_x.parquet".into(),
            row_count: 10,
            byte_size: 100,
            created_at: 1,
            column_stats: vec![(
                "a".into(),
                ColumnStats {
                    min: Some(Literal::Int64(1)),
                    max: Some(Literal::Int64(9)),
                    null_count: 1,
                },
            )],
        });
        m
    }

    #[test]
    fn read_missing_is_empty_partition() {
        let (store, dir) = tmpdir("empty");
        assert!(PartitionManifest::read(&store, &dir).unwrap().is_none());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn commit_roundtrip_and_generation() {
        let (store, dir) = tmpdir("rt");
        let mut m = sample(vec![("dt".into(), Literal::Date(1))]);
        m.commit(&store, &dir, 0).unwrap();
        assert_eq!(m.generation, 1);
        let back = PartitionManifest::read(&store, &dir).unwrap().unwrap();
        assert_eq!(back, m);
        assert_eq!(back.total_rows(), 10);
        // 二次提交 generation 递增
        let mut m2 = back.clone();
        m2.segments.push(SegmentEntry {
            file_name: "seg_y.parquet".into(),
            row_count: 5,
            byte_size: 50,
            created_at: 2,
            column_stats: vec![],
        });
        m2.commit(&store, &dir, 0).unwrap();
        let back2 = PartitionManifest::read(&store, &dir).unwrap().unwrap();
        assert_eq!(back2.generation, 2);
        assert_eq!(back2.total_rows(), 15);
        assert_eq!(back2.segment_paths(&dir).len(), 2);
        assert_eq!(back2.stats_of("a").unwrap().min, Some(Literal::Int64(1)));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn postcard_bytes_are_compact() {
        let mut m = sample(vec![("k".into(), Literal::Utf8("v".into()))]);
        m.segments[0].column_stats[0].1.min = Some(Literal::Decimal {
            unscaled: -12345678901234567890123456789012345678,
            meta: DecimalMeta::new(38, 12),
        });
        let bytes = postcard::to_allocvec(&m).unwrap();
        assert!(bytes.len() < 256, "manifest too fat: {} bytes", bytes.len());
    }
}
