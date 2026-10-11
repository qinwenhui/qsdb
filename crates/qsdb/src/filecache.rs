//! 远端段文件磁盘缓存（M6-2，存储设计 §10.2）：url + ETag 键控的预算制 LRU。
//!
//! 布局：`{root}/{url_hash[0..2]}/{url_hash}/{etag}/{file_name}`
//! - ETag 在键中：对象被覆盖（理论不应发生，segment 不可变）自动失效
//! - 命中即读本地文件（零网络）；`get()` 顺带 touch 目录维持 LRU 序
//! - 预算按目录 mtime 驱逐（插入即更新 mtime）；`budget = 0` 全 API no-op
//!
//! 本模块不含远端访问（填充方提供 `&mut dyn Read`），object_store 无关。

use std::io::Read;
use std::path::{Path, PathBuf};

use crate::error::{QsdbError, Result};

/// 段文件缓存（Clone 即共享；预算 0 = 关闭）。
#[derive(Debug)]
pub struct FileCache {
    root: PathBuf,
    budget: u64,
}

fn hash_hex(s: &str) -> String {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    s.hash(&mut h);
    format!("{:016x}", h.finish())
}

impl FileCache {
    pub fn new(root: impl Into<PathBuf>, budget_bytes: u64) -> Self {
        Self {
            root: root.into(),
            budget: budget_bytes,
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.budget > 0
    }

    pub fn budget(&self) -> u64 {
        self.budget
    }

    /// 条目目录（不含文件名层）。
    fn entry_dir(&self, url: &str, etag: &str) -> PathBuf {
        let h = hash_hex(url);
        self.root
            .join(&h[..2])
            .join(h)
            .join(sanitize_seg(etag).unwrap_or_else(|| "none".into()))
    }

    /// 命中查询：返回已缓存文件的本地路径（目录内第一个文件）。
    pub fn get(&self, url: &str, etag: &str) -> Option<PathBuf> {
        if !self.is_enabled() {
            return None;
        }
        let dir = self.entry_dir(url, etag);
        let f = read_first_file(&dir)?;
        // touch 目录 mtime（marker 写入即更新），维持近似 LRU
        let _ = std::fs::write(dir.join(".t"), b"");
        Some(f)
    }

    /// 流式填充：写入 tmp → rename 定名；超预算按目录 mtime 驱逐。
    /// 返回缓存文件本地路径。
    pub fn insert(
        &self,
        url: &str,
        etag: &str,
        reader: &mut dyn Read,
    ) -> Result<PathBuf> {
        if !self.is_enabled() {
            return Err(QsdbError::meta("file cache disabled (budget 0)"));
        }
        let dir = self.entry_dir(url, etag);
        std::fs::create_dir_all(&dir)
            .map_err(|e| QsdbError::meta(format!("mkdir filecache failed: {e}")))?;
        let name = sanitize_seg(url.rsplit('/').next().unwrap_or("object"))
            .unwrap_or_else(|| "object".into());
        let dest = dir.join(name);
        let tmp = crate::fsutil::tmp_sibling(&dest);
        let mut f = std::fs::File::create(&tmp)
            .map_err(|e| QsdbError::meta(format!("create filecache tmp failed: {e}")))?;
        std::io::copy(reader, &mut f)
            .map_err(|e| QsdbError::meta(format!("download to filecache failed: {e}")))?;
        crate::fsutil::atomic_replace(&tmp, &dest)?;
        self.evict_to_budget();
        Ok(dest)
    }

    /// 驱逐到预算内（按条目目录 mtime 从旧到新；失败条目跳过）。
    fn evict_to_budget(&self) {
        let mut entries: Vec<(PathBuf, u64, u64)> = Vec::new(); // (dir, mtime_ms, bytes)
        let mut used = 0u64;
        for l1 in flatten(std::fs::read_dir(&self.root)) {
            for l2 in flatten(std::fs::read_dir(l1.path())) {
                for etag_dir in flatten(std::fs::read_dir(l2.path())) {
                    let d = etag_dir.path();
                    let bytes = crate::fsutil::dir_size(&d);
                    let mtime = etag_dir
                        .metadata()
                        .ok()
                        .and_then(|m| m.modified().ok())
                        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                        .map(|d| d.as_millis() as u64)
                        .unwrap_or(0);
                    used += bytes;
                    entries.push((d, mtime, bytes));
                }
            }
        }
        if used <= self.budget {
            return;
        }
        entries.sort_by_key(|(_, mtime, _)| *mtime);
        for (dir, _, bytes) in entries {
            if used <= self.budget {
                break;
            }
            if std::fs::remove_dir_all(&dir).is_ok() {
                used = used.saturating_sub(bytes);
            }
        }
    }

    /// 当前占用（诊断/测试）。
    pub fn used_bytes(&self) -> u64 {
        crate::fsutil::dir_size(&self.root)
    }

    /// 条目数（诊断/测试）。
    pub fn entries(&self) -> usize {
        let mut n = 0;
        for l1 in flatten(std::fs::read_dir(&self.root)) {
            for l2 in flatten(std::fs::read_dir(l1.path())) {
                for d in flatten(std::fs::read_dir(l2.path())) {
                    if read_first_file(&d.path()).is_some() {
                        n += 1;
                    }
                }
            }
        }
        n
    }
}

fn flatten(rd: std::io::Result<std::fs::ReadDir>) -> Vec<std::fs::DirEntry> {
    rd.map(|r| r.flatten().collect()).unwrap_or_default()
}

fn read_first_file(dir: &Path) -> Option<PathBuf> {
    let rd = std::fs::read_dir(dir).ok()?;
    for e in rd.flatten() {
        let p = e.path();
        if p.extension().is_some_and(|x| x == "t") && p.file_name()? == ".t" {
            continue;
        }
        if e.file_type().ok().is_some_and(|t| t.is_file()) {
            return Some(p);
        }
    }
    None
}

/// 目录名安全化：仅保留常见安全字符，空/异常 → None。
fn sanitize_seg(s: &str) -> Option<String> {
    let ok = !s.is_empty()
        && s.len() <= 128
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'));
    ok.then(|| s.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmpdir(tag: &str) -> PathBuf {
        let d = std::env::temp_dir()
            .join("qsdb_filecache_test")
            .join(format!("{tag}_{}", uuid::Uuid::now_v7().simple()));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    #[test]
    fn disabled_is_noop() {
        let c = FileCache::new(tmpdir("off"), 0);
        assert!(!c.is_enabled());
        assert!(c.get("s3://b/f.parquet", "e1").is_none());
        assert!(c.insert("s3://b/f.parquet", "e1", &mut &b"x"[..]).is_err());
    }

    #[test]
    fn insert_get_and_etag_invalidation() {
        let root = tmpdir("rt");
        let c = FileCache::new(&root, 1 << 20);
        let mut data = &b"hello parquet"[..];
        let p = c.insert("s3://b/f.parquet", "e1", &mut data).unwrap();
        assert_eq!(std::fs::read(&p).unwrap(), b"hello parquet");
        // 命中
        assert_eq!(c.get("s3://b/f.parquet", "e1").unwrap(), p);
        // ETag 变 → 失效
        assert!(c.get("s3://b/f.parquet", "e2").is_none());
        assert_eq!(c.entries(), 1);
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn evicts_oldest_over_budget() {
        let root = tmpdir("evict");
        let unit = 4096; // 4KB 条目
        let c = FileCache::new(&root, (unit * 2 + 1) as u64);
        let mut urls = Vec::new();
        for i in 0..3 {
            let url = format!("s3://b/f{i}.parquet");
            let data = vec![0u8; unit];
            c.insert(&url, "e1", &mut &data[..]).unwrap();
            urls.push(url);
            std::thread::sleep(std::time::Duration::from_millis(20)); // mtime 分层
        }
        assert!(c.entries() <= 2, "budget 只容 2 条");
        // 最新条目仍在
        assert!(c.get(&urls[2], "e1").is_some());
        assert!(c.get(&urls[0], "e1").is_none(), "最旧条目应被驱逐");
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn sanitize_rejects_unsafe() {
        assert_eq!(sanitize_seg("abc-1.parquet").as_deref(), Some("abc-1.parquet"));
        assert!(sanitize_seg("a/b").is_none());
        assert!(sanitize_seg("").is_none());
        assert!(sanitize_seg("他").is_none());
    }
}
