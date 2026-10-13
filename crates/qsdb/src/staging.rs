//! 远端写暂存（M6-4，feature `s3`，存储设计 §11）。
//!
//! segment 先写本地 staging 目录（**写盘即 ACK**，写延迟与网络解耦），
//! 后台/手动 `flush_staged()` 批量 PUT + OCC 提交后删除已传对。
//!
//! 布局：`{dir}/{table}/{partition_rel}/seg_*.parquet` + 同名 `.meta`
//! sidecar（postcard [`StagedRecord`]）。**sidecar 后写 = 完成标记**：
//! 崩溃残留的无 sidecar parquet 是半成品，恢复时忽略（GC 清扫）。
//!
//! 崩溃恢复 = 下次 flush 重放剩余对，幂等成立靠两条：
//! ① segment PUT 幂等（UUID 名不可变对象）；② manifest 追加带
//! file_name 去重（补传不重复计入）。
//!
//! 可见性语义：staged 未 flush 的数据对查询**不可见**（manifest 是唯一
//! 真值）——server 模式可见性延迟 = 上传周期；需要读己之写的调用方先
//! `flush_staged()`。核心库不驻线程：flush 由 server 定时器/嵌入方驱动。

use std::path::{Path, PathBuf};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use crate::error::{QsdbError, Result};
use crate::manifest::SegmentEntry;
use crate::types::Literal;

const META_EXT: &str = "meta";

/// sidecar 记录（一对 staged 文件的提交描述）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StagedRecord {
    pub version: u32,
    pub table: String,
    /// 相对表目录的分区路径（`/` 分隔；非分区表 = "."）
    pub partition_rel: String,
    pub partition_values: Vec<Literal>,
    /// true = overwrite（替换分区）；false = append（追加）
    pub overwrite: bool,
    /// overwrite 的 **CAS 期望 generation**（F24）：写盘即 ACK，提交在 `flush_staged`，
    /// 故期望值随记录落盘、由提交处校验。`None` = 无条件覆盖。
    /// `serde(default)`：旧记录（无此字段）反序列化后即 None（向后兼容）。
    #[serde(default)]
    pub expected_generation: Option<u64>,
    pub entry: SegmentEntry,
}

/// staging 管理器（Clone 即共享；flush 互斥）。
#[derive(Debug)]
pub struct Staging {
    dir: PathBuf,
    flush_lock: Mutex<()>,
}

impl Staging {
    pub fn new(dir: impl Into<PathBuf>) -> Result<Self> {
        let dir = dir.into();
        std::fs::create_dir_all(&dir)
            .map_err(|e| QsdbError::meta(format!("mkdir staging {} failed: {e}", dir.display())))?;
        Ok(Self {
            dir,
            flush_lock: Mutex::new(()),
        })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    fn pair_dir(&self, rec: &StagedRecord) -> PathBuf {
        self.dir.join(&rec.table).join(&rec.partition_rel)
    }

    /// 暂存 segment：parquet tmp→rename 先行，sidecar 后写（完成标记）。
    /// `write_parquet` 以 parquet 终名为参序列化落盘，返回**最终** entry
    /// （行数/字节/统计在写盘后才能定）——sidecar 按最终值序列化。
    pub(crate) fn stage(
        &self,
        rec: &StagedRecord,
        write_parquet: impl FnOnce(&Path) -> Result<SegmentEntry>,
    ) -> Result<SegmentEntry> {
        let dir = self.pair_dir(rec);
        std::fs::create_dir_all(&dir)
            .map_err(|e| QsdbError::meta(format!("mkdir staging failed: {e}")))?;
        let parquet = dir.join(&rec.entry.file_name);
        let entry = write_parquet(&parquet)?;
        let mut final_rec = rec.clone();
        final_rec.entry = entry.clone();
        let meta = dir.join(format!("{}.{}", entry.file_name, META_EXT));
        let bytes = postcard::to_allocvec(&final_rec)
            .map_err(|e| QsdbError::meta(format!("serialize staging meta failed: {e}")))?;
        crate::fsutil::write_atomic(meta, &bytes)?;
        Ok(entry)
    }

    /// 枚举完成对（有 sidecar），按（表, 分区, 文件名）排序——
    /// uuid v7 文件名时间有序 → 同分区多笔暂存按提交顺序重放。
    pub(crate) fn pending(&self) -> Vec<(StagedRecord, PathBuf)> {
        let mut out: Vec<(StagedRecord, PathBuf)> = Vec::new();
        let tables = match std::fs::read_dir(&self.dir) {
            Ok(rd) => rd,
            Err(_) => return out,
        };
        for t in tables.flatten() {
            let mut stack = vec![t.path()];
            while let Some(d) = stack.pop() {
                let rd = match std::fs::read_dir(&d) {
                    Ok(rd) => rd,
                    Err(_) => continue,
                };
                for e in rd.flatten() {
                    let p = e.path();
                    if p.is_dir() {
                        stack.push(p);
                        continue;
                    }
                    let Some(ext) = p.extension() else { continue };
                    if ext != META_EXT {
                        continue;
                    }
                    let Some(raw) = std::fs::read(&p).ok() else {
                        continue;
                    };
                    let Some(rec) = postcard::from_bytes::<StagedRecord>(&raw).ok() else {
                        continue; // 损坏 sidecar：跳过（保留现场，运维处置）
                    };
                    if rec.version != 1 {
                        continue;
                    }
                    let parquet = p.with_extension("");
                    if !parquet.is_file() {
                        continue; // parquet 缺失（半删状态）：跳过
                    }
                    out.push((rec, parquet));
                }
            }
        }
        out.sort_by(|a, b| {
            (&a.0.table, &a.0.partition_rel, &a.0.entry.file_name).cmp(&(
                &b.0.table,
                &b.0.partition_rel,
                &b.0.entry.file_name,
            ))
        });
        out
    }

    /// 删除一对已传文件（先 sidecar 后 parquet：中断残留 parquet 无 sidecar
    /// → 半成品，恢复忽略）。
    pub(crate) fn remove_pair(&self, rec: &StagedRecord, parquet: &Path) {
        let dir = self.pair_dir(rec);
        let meta = dir.join(format!("{}.{}", rec.entry.file_name, META_EXT));
        let _ = std::fs::remove_file(meta);
        let _ = std::fs::remove_file(parquet);
        // 顺带清空目录（失败无碍）
        let _ = std::fs::remove_dir(&dir);
    }

    /// 清掉指定表的全部暂存（表已删除时调用）。
    pub(crate) fn remove_table(&self, table: &str) {
        let _ = std::fs::remove_dir_all(self.dir.join(table));
    }

    /// flush 互斥锁（server 定时器与手动 flush 并发防护）。
    pub(crate) fn lock(&self) -> std::sync::MutexGuard<'_, ()> {
        self.flush_lock.lock().unwrap_or_else(|p| p.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(table: &str, seg: &str, rel: &str) -> StagedRecord {
        StagedRecord {
            expected_generation: None,
            version: 1,
            table: table.into(),
            partition_rel: rel.into(),
            partition_values: vec![Literal::Date(1)],
            overwrite: false,
            entry: SegmentEntry {
                file_name: seg.into(),
                row_count: 2,
                byte_size: 10,
                created_at: 0,
                column_stats: vec![],
            },
        }
    }

    #[test]
    fn stage_pending_remove_roundtrip() {
        let dir = std::env::temp_dir().join(format!(
            "qsdb_staging_test_{}",
            uuid::Uuid::now_v7().simple()
        ));
        let st = Staging::new(&dir).unwrap();
        let r1 = rec("t", "seg_aaa.parquet", "dt=1");
        let r2 = rec("t", "seg_bbb.parquet", "dt=1");
        let r3 = rec("t", "seg_ccc.parquet", "dt=2");
        for r in [&r1, &r2, &r3] {
            st.stage(r, |p| {
                std::fs::write(p, b"pq").unwrap();
                Ok(r.entry.clone())
            })
            .unwrap();
        }
        let mut pending = st.pending();
        assert_eq!(pending.len(), 3);
        // 排序：同表同分区内按文件名
        assert_eq!(pending[0].0.entry.file_name, "seg_aaa.parquet");
        assert_eq!(pending[2].0.entry.file_name, "seg_ccc.parquet");
        // 删除一对
        let (rec, pq) = pending.remove(0);
        st.remove_pair(&rec, &pq);
        assert_eq!(st.pending().len(), 2);
        // 半成品（parquet 无 sidecar）不计入
        std::fs::write(dir.join("t/dt=1/seg_orphan.parquet"), b"x").unwrap();
        assert_eq!(st.pending().len(), 2);
        // 表级清理
        st.remove_table("t");
        assert_eq!(st.pending().len(), 0);
        std::fs::remove_dir_all(&dir).ok();
    }
}
