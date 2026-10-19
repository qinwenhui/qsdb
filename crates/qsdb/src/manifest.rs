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

/// 当前 manifest **磁盘格式**版本。
///
/// ## 加字段 = 升版本（这条不是建议，是 postcard 的硬约束）
///
/// postcard **不是自描述格式**：结构体按**字段声明序**依次编码，没有字段名、
/// 没有长度前缀。于是——
///
/// - **尾部加字段**：旧文件的字节在读完已知字段后就到末尾了，新内核读它必然
///   报 `Hit the end of buffer`；
/// - 此时 `#[serde(default)]` **一点用都没有**：它只在 JSON 这类自描述格式里
///   生效。本文件里两处"旧 manifest 兼容：字段缺省即 0"的注释**曾经是错的**，
///   而且错得会咬人——2026-10-11 实测：`epoch` 落盘后，20:48 生成的数据被
///   21:58 的内核读成 `corrupt manifest … Hit the end of buffer`，
///   **报错说"损坏"，真相是"版本不同"**，人会去查磁盘、查 GC，方向全错。
///
/// 所以：**改本结构体字段 = 同时改这个常量**。
/// 版本 1 = 无 `epoch`（有 `writer_epoch`）；版本 2 = 现行。
pub const MANIFEST_VERSION: u32 = 2;

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
    /// 不参与正确性。本地提交恒 0。
    ///
    /// ⚠️ 别指望 `#[serde(default)]` 能兼容旧文件——见 [`MANIFEST_VERSION`]。
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
    /// 缺省 0 = 未启用身份代。
    ///
    /// ⚠️ 本字段是**尾部**新增的，正是 [`MANIFEST_VERSION`] 存在的理由：
    /// v1 文件读进来时会少这一段字节，由 `read` 显式补 0（见那里的注释）。
    #[serde(default)]
    pub epoch: u64,
}

/// 单独解出 manifest 的**前导版本字段**（postcard 的 u32 = LEB128 变长整数）。
///
/// 存在的理由：`version` 是结构体第一个字段，所以它总是文件的前几个字节——
/// 于是"版本不认识"这类问题可以**在反序列化之前**判掉，不必等 postcard 抛出
/// 与版本毫无关系的 "Hit the end of buffer"。没有这个函数，格式不匹配就只会
/// 表现为 `corrupt manifest`，把人引向磁盘损坏的错误方向。
///
/// 返回 `None` = 连版本都读不出（文件被截断/根本不是 manifest）。
fn leading_version(bytes: &[u8]) -> Option<u32> {
    let mut v: u32 = 0;
    for (i, &b) in bytes.iter().take(5).enumerate() {
        v |= ((b & 0x7f) as u32) << (7 * i);
        if b & 0x80 == 0 {
            return Some(v);
        }
    }
    None // 超过 5 字节还不是结尾 → 不是合法 u32 变长整数
}

impl PartitionManifest {
    pub fn new(partition_values: Vec<(String, Literal)>, schema_fingerprint: u64) -> Self {
        Self {
            version: MANIFEST_VERSION,
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
    ///
    /// **先看版本，再解结构体**：`version` 是结构体第一个字段，postcard 顺序编码
    /// ⇒ 它恒是**前导变长整数**，可以在完整反序列化**之前**单独读出来。
    /// 这样"版本不认识"就能报成版本问题，而不是等反序列化炸成 `corrupt manifest`
    /// （2026-10-11 实测的误导，见 [`MANIFEST_VERSION`]）。
    ///
    /// **v1 兼容**：v1 与 v2 的全部差别就是 v2 结尾多一个 `epoch` 变长整数。
    /// 所以 v1 文件按原样解会"读穿了"，补一个 `0x00`（变长整数 0）再解即得
    /// `epoch = 0`——正是 v1 时代"未启用身份代"的确切语义。
    ///
    /// ⚠️ 补零**只在版本号 == 1 时**发生。这一条是防"把截断当成旧版本"的：
    /// 截断的 v2 文件版本号仍是 2，走不到这条路，如实报损坏。
    pub fn read(store: &Store, partition_dir: &Path) -> Result<Option<Self>> {
        let path = Self::manifest_path(partition_dir);
        match store.read(&path)? {
            Some(bytes) => {
                let ver = leading_version(&bytes).ok_or_else(|| {
                    QsdbError::meta(format!(
                        "corrupt manifest {}: 前导版本字段解不出（文件 {} 字节）",
                        path.display(),
                        bytes.len()
                    ))
                })?;
                let m: Self = match ver {
                    1 => {
                        // v1：尾部缺 `epoch`。先按 v2 解（万一将来 v1 形态一致），
                        // 失败再补一个 0 字节重解。
                        postcard::from_bytes(&bytes).or_else(|_| {
                            let mut patched = bytes.clone();
                            patched.push(0);
                            postcard::from_bytes(&patched).map_err(|e| {
                                QsdbError::meta(format!(
                                    "manifest {} 是 v1 格式，补 epoch=0 后仍解不出：{e}",
                                    path.display()
                                ))
                            })
                        })
                    }
                    v if v == MANIFEST_VERSION => postcard::from_bytes(&bytes).map_err(|e| {
                        QsdbError::meta(format!("corrupt manifest {}: {e}", path.display()))
                    }),
                    v => {
                        return Err(QsdbError::meta(format!(
                            "manifest {} 是格式 v{v}，本内核读 v{MANIFEST_VERSION}\
                             （v1 = 无 writer-identity 字段）",
                            path.display()
                        )));
                    }
                }?;
                if m.version != 1 && m.version != MANIFEST_VERSION {
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
        // 本次提交后盘上就是**当前格式**了：把 v1 读进来的那份一并升到 v2。
        // 不写这一行会出现"声称 v1、实际带 epoch 字段"的文件——正是
        // `MANIFEST_VERSION` 想根除的那种自相矛盾。
        self.version = MANIFEST_VERSION;
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

    /// **v1（无 `epoch` 字段）的文件仍要能读**——升级内核不该把已有数据变成砖头。
    ///
    /// 造 v1 文件的办法：v1 与 v2 的差别只在**结尾那一个变长整数**，所以把
    /// `epoch = 0`（变长编码就是单字节 `0x00`）的 v2 字节**去掉最后一字节**
    /// 就是一份逐字节真实的 v1 文件（不是"近似"，是等价）。
    #[test]
    fn v1_manifest_without_epoch_is_still_readable() {
        let (store, dir) = tmpdir("v1compat");
        let mut m = sample(vec![("dt".into(), Literal::Date(1))]);
        m.version = 1; // 模拟 v1 写者
        let bytes = postcard::to_allocvec(&m).unwrap();
        assert_eq!(bytes.last(), Some(&0), "epoch=0 应编码为单个 0x00");
        let v1_bytes = &bytes[..bytes.len() - 1]; // ← 去掉尾部 epoch：即 v1 文件

        store
            .write_atomic(&PartitionManifest::manifest_path(&dir), v1_bytes)
            .unwrap();
        let back = PartitionManifest::read(&store, &dir)
            .expect("v1 文件必须仍可读")
            .unwrap();
        assert_eq!(back.total_rows(), 10);
        assert_eq!(back.epoch, 0, "v1 时代没有身份代 ⇒ 0");
        assert_eq!(back.partition_values, m.partition_values);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// 版本不认识 ⇒ **报版本**，不许报成"损坏"（2026-10-11 实测的误导就在这里）。
    #[test]
    fn unknown_version_reports_version_not_corruption() {
        let (store, dir) = tmpdir("badver");
        let mut m = sample(vec![]);
        m.version = 7;
        let bytes = postcard::to_allocvec(&m).unwrap();
        store
            .write_atomic(&PartitionManifest::manifest_path(&dir), &bytes)
            .unwrap();
        let e = format!("{}", PartitionManifest::read(&store, &dir).unwrap_err());
        assert!(e.contains("格式 v7"), "应点名版本：{e}");
        assert!(!e.contains("corrupt"), "不该说损坏：{e}");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// **截断的 v2 文件不许被当成 v1 糊过去**（补零兼容只在版本号 == 1 时生效）。
    /// 这条是"别把兼容做成静默兜底"的守门测试。
    #[test]
    fn truncated_v2_is_reported_as_corrupt_not_as_v1() {
        let (store, dir) = tmpdir("trunc");
        let mut m = sample(vec![("dt".into(), Literal::Date(1))]);
        m.commit(&store, &dir, 5).unwrap();
        let p = PartitionManifest::manifest_path(&dir);
        let bytes = std::fs::read(&p).unwrap();
        assert_eq!(leading_version(&bytes), Some(MANIFEST_VERSION));
        // 砍掉尾部 3 字节（epoch 之外还有别的字段）→ 版本仍是 2，必须报损坏
        store.write_atomic(&p, &bytes[..bytes.len() - 3]).unwrap();
        let e = format!("{}", PartitionManifest::read(&store, &dir).unwrap_err());
        assert!(e.contains("corrupt"), "截断应报损坏：{e}");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// 提交会把 v1 读进来的那份**升到当前格式**（不许写出"声称 v1、实际带 epoch"的文件）。
    #[test]
    fn commit_upgrades_a_v1_manifest_to_current_version() {
        let (store, dir) = tmpdir("upgrade");
        let mut m = sample(vec![]);
        m.version = 1;
        let bytes = postcard::to_allocvec(&m).unwrap();
        store
            .write_atomic(
                &PartitionManifest::manifest_path(&dir),
                &bytes[..bytes.len() - 1],
            )
            .unwrap();
        let mut back = PartitionManifest::read(&store, &dir).unwrap().unwrap();
        assert_eq!(back.version, 1);
        back.commit(&store, &dir, 3).unwrap();
        let raw = std::fs::read(PartitionManifest::manifest_path(&dir)).unwrap();
        assert_eq!(leading_version(&raw), Some(MANIFEST_VERSION));
        let again = PartitionManifest::read(&store, &dir).unwrap().unwrap();
        assert_eq!(again.version, MANIFEST_VERSION);
        assert_eq!(again.epoch, 3);
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
