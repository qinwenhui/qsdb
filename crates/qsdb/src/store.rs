//! 存储抽象层（M6-1 收口，M6-3 Remote 变体）：所有文件/对象访问的单一收口点。
//!
//! 设计（`docs/存储层设计.md` §4/§6/§7）：
//! - **enum 静态派发**：Local 分支单态化，与 v1 代码路径等价（零虚调用、
//!   零额外拷贝）；Remote（feature `s3`）统一走 object_store
//! - **路径契约**：方法收 db root 下的绝对路径；Local 原样使用，
//!   Remote 按 root strip 推导对象 key（`/` 分隔，与本地路径一一映射，§5）——
//!   Remote 的 root 是**虚拟根**（`s3://bucket/prefix`），M6-1 的全部调用面
//!   （catalog/manifest/write/scan/compact）零改动直通
//! - **提交协议（§7）**：Local = flock + tmp+rename（现状）；Remote = OCC
//!   （get_with_etag + put_if_etag 条件写，重放循环见 write/remote 路径）
//! - **热路径豁免（§4）**：parquet 列读不经本层逐字节包装——本地直读 `File`，
//!   远端走 range GET ChunkReader + 段缓存（scan::ChunkSource）
//! - **本地原语归属**：flock / trash / tmp 命名仍在 `fsutil`

use std::fs;
use std::path::{Path, PathBuf};

use crate::error::{QsdbError, Result};
use crate::fsutil;

#[cfg(feature = "remote")]
use object_store::ObjectStoreExt;

/// 本地后端（std::fs 直通）。
#[derive(Debug)]
pub struct LocalStore {
    root: PathBuf,
}

/// 远端后端（feature `s3`，M6-3）：object_store 句柄 + 虚拟根映射。
#[cfg(feature = "remote")]
#[derive(Debug)]
pub struct RemoteStore {
    /// 虚拟根（`s3://bucket/prefix`）——路径契约锚点（路径 → key 的 strip 基准）
    root: PathBuf,
    /// 对象 store（AmazonS3；测试可注入 InMemory/LocalFileSystem）
    os: std::sync::Arc<dyn object_store::ObjectStore>,
    /// bucket 内 key 前缀（"" = bucket 根）
    prefix: String,
}

/// 存储后端（enum 静态派发，见模块文档）。
#[derive(Debug)]
pub enum Store {
    Local(LocalStore),
    #[cfg(feature = "remote")]
    Remote(RemoteStore),
}

/// 后端类别（协议选择：Local=flock+rename，Remote=OCC）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreKind {
    Local,
    Remote,
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

/// 远端对象描述（list_all；GC/统计用）。
#[derive(Debug, Clone)]
pub struct RemoteObj {
    /// 相对虚拟根的完整 key 路径（`/` 分隔）
    pub key: String,
    pub size: u64,
    pub mtime_ms: u64,
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

fn unsupported_remote() -> QsdbError {
    QsdbError::unsupported("operation requires a remote (s3) store")
}

// --- Remote key 映射与 object_store 桥接 -----------------------------------

#[cfg(feature = "remote")]
impl RemoteStore {
    /// 路径 → 对象 key（strip 虚拟根 + 分隔符归一 + 前缀拼接）。
    fn key(&self, path: &Path) -> object_store::path::Path {
        let rel = path
            .strip_prefix(&self.root)
            .unwrap_or(path)
            .to_string_lossy()
            .replace('\\', "/");
        let full = if self.prefix.is_empty() {
            rel
        } else {
            format!("{}/{}", self.prefix, rel)
        };
        // key 均由受控路径派生（非空、无空段）；根路径本身（"."）无对象语义
        object_store::path::Path::parse(full).unwrap_or_else(|_| object_store::path::Path::from("_"))
    }

    fn os_err(&self, what: &str, key: &object_store::path::Path, e: object_store::Error) -> QsdbError {
        QsdbError::meta(format!("{what} {key}: {e}"))
    }
}

impl Store {
    /// 本地后端（默认）。
    pub fn local(root: impl Into<PathBuf>) -> Self {
        Self::Local(LocalStore {
            root: root.into(),
        })
    }

    /// 远端后端（feature `s3`）：`root` = 虚拟根（`s3://bucket/prefix`），
    /// `prefix` = bucket 内 key 前缀。store 句柄由调用方构建
    /// （生产 = AmazonS3；测试/嵌入方可注入任意 object_store 实现）。
    #[cfg(feature = "remote")]
    pub fn remote(
        root: impl Into<PathBuf>,
        os: std::sync::Arc<dyn object_store::ObjectStore>,
        prefix: impl Into<String>,
    ) -> Self {
        Self::Remote(RemoteStore {
            root: root.into(),
            os,
            prefix: prefix.into(),
        })
    }

    /// 后端类别（协议选择）。
    pub fn kind(&self) -> StoreKind {
        match self {
            Self::Local(_) => StoreKind::Local,
            #[cfg(feature = "remote")]
            Self::Remote(_) => StoreKind::Remote,
        }
    }

    pub fn is_local(&self) -> bool {
        self.kind() == StoreKind::Local
    }

    /// db 根路径。Local = 真实目录（trash/锁/尺寸基准）；
    /// Remote = 虚拟根（路径→key 的 strip 锚点）。
    pub fn root(&self) -> &Path {
        match self {
            Self::Local(s) => &s.root,
            #[cfg(feature = "remote")]
            Self::Remote(s) => &s.root,
        }
    }

    /// 读整个对象；不存在 → `Ok(None)`。
    pub fn read(&self, path: &Path) -> Result<Option<Vec<u8>>> {
        match self {
            Self::Local(_) => match fs::read(path) {
                Ok(b) => Ok(Some(b)),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Err(e) => Err(io_err(path, "read", e)),
            },
            #[cfg(feature = "remote")]
            Self::Remote(s) => {
                let key = s.key(path);
                let rt = crate::remote::rt();
                match rt.block_on(s.os.get(&key)) {
                    Ok(r) => {
                        let b = rt
                            .block_on(r.bytes())
                            .map_err(|e| s.os_err("get", &key, e))?;
                        Ok(Some(b.to_vec()))
                    }
                    Err(object_store::Error::NotFound { .. }) => Ok(None),
                    Err(e) => Err(s.os_err("get", &key, e)),
                }
            }
        }
    }

    /// 读文本对象；不存在 → `Ok(None)`。
    pub fn read_to_string(&self, path: &Path) -> Result<Option<String>> {
        match self.read(path)? {
            Some(b) => String::from_utf8(b)
                .map(Some)
                .map_err(|e| QsdbError::meta(format!("utf8 {}: {e}", path.display()))),
            None => Ok(None),
        }
    }

    /// 元数据原子写：Local = tmp+fsync+rename（提交点原语）；
    /// Remote = 单对象 PUT（原子替换，§7.2 manifest 原子切换的 Overwrite 形态——
    /// 仅用于无竞争覆盖；竞争路径走 [`Store::put_if_etag`] OCC）。
    pub fn write_atomic(&self, path: &Path, data: &[u8]) -> Result<()> {
        match self {
            Self::Local(_) => fsutil::write_atomic(path, data),
            #[cfg(feature = "remote")]
            Self::Remote(_) => self.put_object(path, data),
        }
    }

    /// 元数据；不存在 → `Ok(None)`。
    pub fn stat(&self, path: &Path) -> Result<Option<ObjMeta>> {
        match self {
            Self::Local(_) => match fs::metadata(path) {
                Ok(md) => Ok(Some(ObjMeta {
                    size: md.len(),
                    mtime_ms: mtime_ms(&md),
                })),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
                Err(e) => Err(io_err(path, "stat", e)),
            },
            #[cfg(feature = "remote")]
            Self::Remote(s) => {
                let key = s.key(path);
                match crate::remote::rt().block_on(s.os.head(&key)) {
                    Ok(m) => Ok(Some(ObjMeta {
                        size: m.size,
                        mtime_ms: m.last_modified.timestamp_millis().max(0) as u64,
                    })),
                    Err(object_store::Error::NotFound { .. }) => Ok(None),
                    Err(e) => Err(s.os_err("head", &key, e)),
                }
            }
        }
    }

    /// 存在性（含目录；错误按不存在，与 `Path::exists` 同语义）。
    pub fn exists(&self, path: &Path) -> bool {
        match self {
            Self::Local(_) => fs::metadata(path).is_ok(),
            #[cfg(feature = "remote")]
            Self::Remote(s) => {
                let key = s.key(path);
                crate::remote::rt()
                    .block_on(s.os.head(&key))
                    .is_ok()
            }
        }
    }

    /// 建目录（含父级）；远端为 no-op（目录即 key 前缀）。
    pub fn create_dir_all(&self, path: &Path) -> Result<()> {
        match self {
            Self::Local(_) => fs::create_dir_all(path).map_err(|e| io_err(path, "mkdir", e)),
            #[cfg(feature = "remote")]
            Self::Remote(_) => Ok(()),
        }
    }

    /// 枚举目录一层（名字序未定义，调用方自行排序）。
    /// Remote = list_with_delimiter（公共前缀 = 目录，对象 = 文件）。
    pub fn read_dir(&self, dir: &Path) -> Result<Vec<StoreEntry>> {
        match self {
            Self::Local(_) => {
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
            #[cfg(feature = "remote")]
            Self::Remote(s) => {
                let key = s.key(dir);
                let res = crate::remote::rt()
                    .block_on(s.os.list_with_delimiter(Some(&key)))
                    .map_err(|e| s.os_err("list", &key, e))?;
                let mut out: Vec<StoreEntry> = res
                    .common_prefixes
                    .into_iter()
                    .map(|p| {
                        let name = p.as_ref().rsplit('/').next().unwrap_or_default().to_string();
                        StoreEntry {
                            path: dir.join(&name),
                            name,
                            is_dir: true,
                        }
                    })
                    .collect();
                out.extend(res.objects.into_iter().map(|m| {
                    let name = m
                        .location
                        .as_ref()
                        .rsplit('/')
                        .next()
                        .unwrap_or_default()
                        .to_string();
                    StoreEntry {
                        path: dir.join(&name),
                        name,
                        is_dir: false,
                    }
                }));
                out.sort_by(|a, b| a.name.cmp(&b.name));
                Ok(out)
            }
        }
    }

    /// 不可变对象定名（segment tmp → UUID 终名）：本地专属（rename 即提交）。
    /// Remote 写路径 = [`Store::put_object`]（整体 PUT）。
    pub fn commit_object(&self, tmp: &Path, dest: &Path) -> Result<()> {
        match self {
            Self::Local(_) => fs::rename(tmp, dest).map_err(|e| io_err(dest, "rename", e)),
            #[cfg(feature = "remote")]
            Self::Remote(_) => {
                let _ = tmp;
                Err(QsdbError::unsupported(
                    "commit_object(tmp rename) is local-only; remote writes use put_object",
                ))
            }
        }
    }

    // --- Remote 专用原语（Local → Unsupported；协议层按 kind() 分支调用） ----

    /// OCC 读：内容 + ETag（条件写凭证）；不存在 → `Ok(None)`。
    /// 后端必须提供 ETag（S3 系天然支持；无 ETag 后端走不了 OCC，启动期即失败）。
    #[cfg_attr(not(feature = "remote"), allow(unused_variables))]
    pub fn get_with_etag(&self, path: &Path) -> Result<Option<(Vec<u8>, String)>> {
        match self {
            Self::Local(_) => Err(unsupported_remote()),
            #[cfg(feature = "remote")]
            Self::Remote(s) => {
                let key = s.key(path);
                let rt = crate::remote::rt();
                let r = match rt.block_on(s.os.get(&key)) {
                    Ok(r) => r,
                    Err(object_store::Error::NotFound { .. }) => {
                        return Ok(None) // 不存在 = 新对象（OCC put_if_absent 路径）
                    }
                    Err(e) => return Err(s.os_err("get", &key, e)),
                };
                let etag = r.meta.e_tag.clone().ok_or_else(|| {
                    QsdbError::meta(format!("object {key} has no ETag; OCC requires it"))
                })?;
                let b = rt
                    .block_on(r.bytes())
                    .map_err(|e| s.os_err("get", &key, e))?;
                Ok(Some((b.to_vec(), etag)))
            }
        }
    }

    /// 条件写（§7.2 唯一 CAS 点）：`etag = None` → put_if_absent；
    /// `Some(t)` → ETag 匹配才写。前置条件冲突 → `Ok(false)`（调用方重放重试）；
    /// 其他 IO 错误 → `Err`。
    #[cfg_attr(not(feature = "remote"), allow(unused_variables))]
    pub fn put_if_etag(&self, path: &Path, data: &[u8], etag: Option<&str>) -> Result<bool> {
        match self {
            Self::Local(_) => Err(unsupported_remote()),
            #[cfg(feature = "remote")]
            Self::Remote(s) => {
                use object_store::{PutMode, PutOptions, PutPayload, UpdateVersion};
                let key = s.key(path);
                let mode = match etag {
                    None => PutMode::Create,
                    Some(t) => PutMode::Update(UpdateVersion {
                        e_tag: Some(t.to_string()),
                        version: None,
                    }),
                };
                let r = crate::remote::rt().block_on(s.os.put_opts(
                    &key,
                    PutPayload::from(data.to_vec()),
                    PutOptions {
                        mode,
                        ..Default::default()
                    },
                ));
                match r {
                    Ok(_) => Ok(true),
                    // 前置条件冲突（ETag 不匹配 / Create 时已存在）→ false，调用方重放
                    Err(
                        e @ (object_store::Error::Precondition { .. }
                        | object_store::Error::AlreadyExists { .. }),
                    ) => {
                        let _ = e;
                        Ok(false)
                    }
                    Err(e) => Err(s.os_err("put_if_etag", &key, e)),
                }
            }
        }
    }

    /// 不可变对象写入（segment）：UUID 命名永不冲突，普通 PUT + 幂等重试（§7.2）。
    #[cfg_attr(not(feature = "remote"), allow(unused_variables))]
    pub fn put_object(&self, path: &Path, data: &[u8]) -> Result<()> {
        match self {
            Self::Local(_) => Err(unsupported_remote()),
            #[cfg(feature = "remote")]
            Self::Remote(s) => {
                use object_store::{PutOptions, PutPayload};
                let key = s.key(path);
                crate::remote::rt()
                    .block_on(s.os.put_opts(
                        &key,
                        PutPayload::from(data.to_vec()),
                        PutOptions::default(),
                    ))
                    .map(|_| ())
                    .map_err(|e| s.os_err("put", &key, e))
            }
        }
    }

    /// 列出前缀下全部对象（GC 引用扫描/统计用）。
    /// `RemoteObj.key` 相对**虚拟根**（已剥离 bucket 前缀——与路径契约一致，
    /// `store.root().join(key)` 即得可操作路径）。
    #[cfg_attr(not(feature = "remote"), allow(unused_variables))]
    pub fn list_all(&self, prefix: &Path) -> Result<Vec<RemoteObj>> {
        match self {
            Self::Local(_) => Err(unsupported_remote()),
            #[cfg(feature = "remote")]
            Self::Remote(s) => {
                use futures_util::StreamExt;
                let key = s.key(prefix);
                let stream = s.os.list(Some(&key));
                let metas: Vec<object_store::ObjectMeta> =
                    crate::remote::rt().block_on(async {
                        let mut out = Vec::new();
                        let mut st = stream;
                        while let Some(m) = st.next().await {
                            match m {
                                Ok(meta) => out.push(meta),
                                Err(e) => {
                                    return Err(s.os_err("list", &key, e));
                                }
                            }
                        }
                        Ok(out)
                    })?;
                Ok(metas
                    .into_iter()
                    .map(|m| {
                        let full = m.location.as_ref();
                        let rel = if s.prefix.is_empty() {
                            full.to_string()
                        } else {
                            full.strip_prefix(&format!("{}/", s.prefix))
                                .unwrap_or(full)
                                .to_string()
                        };
                        RemoteObj {
                            key: rel,
                            size: m.size,
                            mtime_ms: m.last_modified.timestamp_millis().max(0) as u64,
                        }
                    })
                    .collect())
            }
        }
    }

    /// 删除对象；不存在 → Ok（幂等）。
    #[cfg_attr(not(feature = "remote"), allow(unused_variables))]
    pub fn delete_object(&self, path: &Path) -> Result<()> {
        match self {
            Self::Local(_) => Err(unsupported_remote()),
            #[cfg(feature = "remote")]
            Self::Remote(s) => {
                let key = s.key(path);
                match crate::remote::rt().block_on(s.os.delete(&key)) {
                    Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
                    Err(e) => Err(s.os_err("delete", &key, e)),
                }
            }
        }
    }

    /// 内部 object_store 句柄（scan::ChunkSource 远端读取用）。
    #[cfg(feature = "remote")]
    pub(crate) fn remote_handle(&self) -> Option<&std::sync::Arc<dyn object_store::ObjectStore>> {
        match self {
            Self::Local(_) => None,
            Self::Remote(s) => Some(&s.os),
        }
    }

    /// 管理表路径 → 对象 key（ChunkSource::Managed 远端读取用；Local → None）。
    #[cfg(feature = "remote")]
    pub(crate) fn remote_obj_key(
        &self,
        path: &Path,
    ) -> Option<object_store::path::Path> {
        match self {
            Self::Local(_) => None,
            Self::Remote(s) => Some(s.key(path)),
        }
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
