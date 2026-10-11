//! 存储抽象层（M6-1）：所有文件/对象访问的单一收口点。
//!
//! 设计（`docs/存储层设计.md` §4/§6）：
//! - **enum 静态派发**：Local 分支单态化，与 v1 代码路径等价（零虚调用、
//!   零额外拷贝）；远端变体（object_store）于 M6-2/3 feature-gate 加入
//! - **路径契约**：方法收 db root 下的绝对路径；Local 原样使用，
//!   远端实现按 root strip 推导对象 key（`/` 分隔，与本地路径一一映射，§5）
//! - **热路径豁免（§4）**：parquet 列读（scan/compact 的 `File::open` +
//!   footer 读）不经本层——本地直读是红线；远端走 range GET + 段缓存（M6-2）
//! - **本地原语归属**：flock / trash / tmp 命名仍在 `fsutil`（本地专属
//!   语义）；提交协议按后端能力选择（`BackendCaps` + CommitOCC 于 M6-3 落地）
//!
//! M6-1 只实现现有代码实际用到的原语；条件写（put_if_absent/put_if_etag）
//! 随 M6-3 协议层加入——不预建无消费者的 API。

use std::fs;
use std::path::{Path, PathBuf};

use crate::error::{QsdbError, Result};
use crate::fsutil;

/// 本地后端（std::fs 直通）。
#[derive(Debug)]
pub struct LocalStore {
    root: PathBuf,
}

/// 存储后端（enum 静态派发，见模块文档）。
#[derive(Debug)]
pub enum Store {
    Local(LocalStore),
}

/// 对象元数据（HEAD 语义；mtime_ms = epoch 毫秒）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjMeta {
    pub size: u64,
    pub mtime_ms: u64,
}

/// 目录枚举条目（list 语义；远端实现由前缀 list 合成）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreEntry {
    pub path: PathBuf,
    pub name: String,
    pub is_dir: bool,
}

fn io_err(path: &Path, what: &str, e: std::io::Error) -> QsdbError {
    QsdbError::meta(format!("{what} {} failed: {e}", path.display()))
}

fn mtime_ms(md: &fs::Metadata) -> u64 {
    md.modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

impl Store {
    /// 本地后端（默认，也是 M6-1 唯一实现）。
    pub fn local(root: impl Into<PathBuf>) -> Self {
        Self::Local(LocalStore {
            root: root.into(),
        })
    }

    /// db 根路径（trash/锁/目录尺寸等本地语义操作的基准）。
    pub fn root(&self) -> &Path {
        match self {
            Self::Local(s) => &s.root,
        }
    }

    /// 读整个对象；不存在 → `Ok(None)`。
    pub fn read(&self, path: &Path) -> Result<Option<Vec<u8>>> {
        match fs::read(path) {
            Ok(b) => Ok(Some(b)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(io_err(path, "read", e)),
        }
    }

    /// 读文本对象；不存在 → `Ok(None)`。
    pub fn read_to_string(&self, path: &Path) -> Result<Option<String>> {
        match fs::read_to_string(path) {
            Ok(s) => Ok(Some(s)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(io_err(path, "read", e)),
        }
    }

    /// 原子写（tmp+fsync+rename）：元数据对象的提交点原语
    /// （manifest/catalog/table.json/索引）。
    pub fn write_atomic(&self, path: &Path, data: &[u8]) -> Result<()> {
        fsutil::write_atomic(path, data)
    }

    /// 元数据；不存在 → `Ok(None)`。
    pub fn stat(&self, path: &Path) -> Result<Option<ObjMeta>> {
        match fs::metadata(path) {
            Ok(md) => Ok(Some(ObjMeta {
                size: md.len(),
                mtime_ms: mtime_ms(&md),
            })),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(io_err(path, "stat", e)),
        }
    }

    /// 存在性（含目录；错误按不存在，与 `Path::exists` 同语义）。
    pub fn exists(&self, path: &Path) -> bool {
        fs::metadata(path).is_ok()
    }

    /// 建目录（含父级）；远端为 no-op（目录即 key 前缀）。
    pub fn create_dir_all(&self, path: &Path) -> Result<()> {
        fs::create_dir_all(path).map_err(|e| io_err(path, "mkdir", e))
    }

    /// 枚举目录一层（名字序未定义，调用方自行排序）。
    pub fn read_dir(&self, dir: &Path) -> Result<Vec<StoreEntry>> {
        let rd = fs::read_dir(dir).map_err(|e| io_err(dir, "read_dir", e))?;
        let mut out = Vec::new();
        for entry in rd.flatten() {
            let is_dir = entry.file_type().map(|t| t.is_dir()).unwrap_or(false);
            out.push(StoreEntry {
                path: entry.path(),
                name: entry.file_name().to_string_lossy().into_owned(),
                is_dir,
            });
        }
        Ok(out)
    }

    /// 不可变对象定名（segment tmp → UUID 终名）：目标不存在是 UUID 命名
    /// 不变量 → rename 即提交，无需替换语义（与 `write_atomic` 区分）。
    /// 远端写路径（整体 PUT）不走此方法（M6-3）。
    pub fn commit_object(&self, tmp: &Path, dest: &Path) -> Result<()> {
        fs::rename(tmp, dest).map_err(|e| io_err(dest, "rename", e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmpdir(tag: &str) -> PathBuf {
        let d = std::env::temp_dir()
            .join("qsdb_store_test")
            .join(format!("{tag}_{}", uuid::Uuid::now_v7().simple()));
        fs::create_dir_all(&d).unwrap();
        d
    }

    #[test]
    fn read_missing_is_none() {
        let dir = tmpdir("miss");
        let s = Store::local(&dir);
        assert!(s.read(&dir.join("x.bin")).unwrap().is_none());
        assert!(s.read_to_string(&dir.join("x.json")).unwrap().is_none());
        assert!(s.stat(&dir.join("x.bin")).unwrap().is_none());
        assert!(!s.exists(&dir.join("x.bin")));
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn write_read_stat_roundtrip() {
        let dir = tmpdir("rt");
        let s = Store::local(&dir);
        let p = dir.join("m.postcard");
        s.write_atomic(&p, b"v1").unwrap();
        assert_eq!(s.read(&p).unwrap().unwrap(), b"v1");
        s.write_atomic(&p, b"v2").unwrap();
        assert_eq!(s.read_to_string(&p).unwrap().unwrap(), "v2");
        let meta = s.stat(&p).unwrap().unwrap();
        assert_eq!(meta.size, 2);
        assert!(meta.mtime_ms > 0);
        assert!(s.exists(&p));
        // 无 tmp 残留
        let names: Vec<String> = s
            .read_dir(&dir)
            .unwrap()
            .into_iter()
            .map(|e| e.name)
            .collect();
        assert_eq!(names, vec!["m.postcard".to_string()]);
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn read_dir_and_commit_object() {
        let dir = tmpdir("rd");
        let s = Store::local(&dir);
        s.create_dir_all(&dir.join("dt=1")).unwrap();
        let entries = s.read_dir(&dir).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "dt=1");
        assert!(entries[0].is_dir);
        // 不可变对象定名
        let tmp = dir.join("seg.tmp");
        fs::write(&tmp, b"seg").unwrap();
        let dest = dir.join("seg_abc.parquet");
        s.commit_object(&tmp, &dest).unwrap();
        assert!(s.exists(&dest));
        assert!(!s.exists(&tmp));
        fs::remove_dir_all(&dir).ok();
    }
}
