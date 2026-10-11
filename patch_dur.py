# -*- coding: utf-8 -*-
# Durability: fsync on segment/metadata writes under PowerLoss mode
import re

# 1. fsutil: write_atomic_opts + dir_sync
p='crates/qsdb/src/fsutil.rs'
s=open(p,encoding='utf-8').read()
old='''/// 原子写文件：`{dest}.{uuid}.tmp` 写入 → fsync → 原子替换到 `dest`。
pub fn write_atomic<D: AsRef<Path>>(dest: D, data: &[u8]) -> Result<()> {
    let dest = dest.as_ref();
    let tmp = tmp_sibling(dest);
    {
        let mut f = File::create(&tmp)
            .map_err(|e| QsdbError::meta(format!("create tmp {} failed: {e}", tmp.display())))?;
        use std::io::Write;
        f.write_all(data)
            .map_err(|e| QsdbError::meta(format!("write tmp failed: {e}")))?;
        f.sync_all().ok(); // 元数据落盘尽力而为；提交点语义由 rename 保证
    }
    atomic_replace(&tmp, dest)?;
    Ok(())
}'''
new='''/// 原子写文件：`{dest}.{uuid}.tmp` 写入 → fsync（尽力而为）→ 原子替换到 `dest`。
/// 提交点语义（进程崩溃安全）由 rename 保证；掉电安全用 [`write_atomic_opts`]。
pub fn write_atomic<D: AsRef<Path>>(dest: D, data: &[u8]) -> Result<()> {
    write_atomic_opts(dest, data, false)
}

/// 原子写（耐久性可选）：`sync = true`（PowerLoss 档）时
/// fsync 错误**上抛**（承诺耐久则不允许静默失败），并在 rename 后同步
/// 父目录（POSIX；Windows 由 MOVEFILE_WRITE_THROUGH 覆盖文件数据路径）。
pub fn write_atomic_opts<D: AsRef<Path>>(dest: D, data: &[u8], sync: bool) -> Result<()> {
    let dest = dest.as_ref();
    let tmp = tmp_sibling(dest);
    {
        let mut f = File::create(&tmp)
            .map_err(|e| QsdbError::meta(format!("create tmp {} failed: {e}", tmp.display())))?;
        use std::io::Write;
        f.write_all(data)
            .map_err(|e| QsdbError::meta(format!("write tmp failed: {e}")))?;
        if sync {
            f.sync_all()
                .map_err(|e| QsdbError::meta(format!("fsync tmp failed: {e}")))?;
        } else {
            f.sync_all().ok(); // 尽力而为；提交点语义由 rename 保证
        }
    }
    atomic_replace(&tmp, dest)?;
    if sync {
        dir_sync(dest);
    }
    Ok(())
}

/// rename/fsync 后同步父目录（POSIX 耐久性要求；失败尽力而为——
/// 多数日志型文件系统（ext4 data=ordered）rename 自身已持久目录项）。
#[cfg(unix)]
pub fn dir_sync(path: &Path) {
    if let Some(parent) = path.parent() {
        if let Ok(d) = File::open(parent) {
            let _ = d.sync_all();
        }
    }
}

/// Windows 无目录 fsync；文件数据路径由 MOVEFILE_WRITE_THROUGH 覆盖。
#[cfg(windows)]
pub fn dir_sync(_path: &Path) {}'''
assert old in s
s=s.replace(old,new)
open(p,'w',encoding='utf-8').write(s)

# 2. store.rs: LocalStore/RemoteStore carry power_loss; data_sync(); _dur constructors
p='crates/qsdb/src/store.rs'
s=open(p,encoding='utf-8').read()
old='''pub struct LocalStore {
    root: PathBuf,
}'''
new='''pub struct LocalStore {
    root: PathBuf,
    /// 掉电安全档：segment/元数据写 fsync 后再 rename（见 api::Durability）
    power_loss: bool,
}'''
assert old in s
s=s.replace(old,new)
old='''pub struct RemoteStore {
    /// 虚拟根（`s3://bucket/prefix`）——路径契约锚点（路径 → key 的 strip 基准）
    root: PathBuf,
    /// 对象 store（AmazonS3；测试可注入 InMemory/LocalFileSystem）
    os: std::sync::Arc<dyn object_store::ObjectStore>,
    /// bucket 内 key 前缀（"" = bucket 根）
    prefix: String,
}'''
new='''pub struct RemoteStore {
    /// 虚拟根（`s3://bucket/prefix`）——路径契约锚点（路径 → key 的 strip 基准）
    root: PathBuf,
    /// 对象 store（AmazonS3；测试可注入 InMemory/LocalFileSystem）
    os: std::sync::Arc<dyn object_store::ObjectStore>,
    /// bucket 内 key 前缀（"" = bucket 根）
    prefix: String,
    /// 掉电安全档：远端 PUT 由后端保证耐久；此标志只作用于**本地 staging
    /// 文件**（写盘即 ACK 的数据在 flush 前不能因掉电丢失）
    power_loss: bool,
}'''
assert old in s
s=s.replace(old,new)
old='''    /// 本地后端（默认）。
    pub fn local(root: impl Into<PathBuf>) -> Self {
        Self::Local(LocalStore {
            root: root.into(),
        })
    }'''
new='''    /// 本地后端（默认，ProcessCrash 耐久档）。
    pub fn local(root: impl Into<PathBuf>) -> Self {
        Self::local_dur(root, false)
    }

    /// 本地后端 + 耐久档选择（`power_loss = true` = 掉电安全，见 api::Durability）。
    pub fn local_dur(root: impl Into<PathBuf>, power_loss: bool) -> Self {
        Self::Local(LocalStore {
            root: root.into(),
            power_loss,
        })
    }'''
assert old in s
s=s.replace(old,new)
old='''    #[cfg(feature = "remote")]
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
    }'''
new='''    #[cfg(feature = "remote")]
    pub fn remote(
        root: impl Into<PathBuf>,
        os: std::sync::Arc<dyn object_store::ObjectStore>,
        prefix: impl Into<String>,
    ) -> Self {
        Self::remote_dur(root, os, prefix, false)
    }

    /// 远端后端 + 耐久档（`power_loss` 作用于本地 staging 文件）。
    #[cfg(feature = "remote")]
    pub fn remote_dur(
        root: impl Into<PathBuf>,
        os: std::sync::Arc<dyn object_store::ObjectStore>,
        prefix: impl Into<String>,
        power_loss: bool,
    ) -> Self {
        Self::Remote(RemoteStore {
            root: root.into(),
            os,
            prefix: prefix.into(),
            power_loss,
        })
    }

    /// 掉电安全档（本地文件写 fsync 后 rename；远端 PUT 天然耐久）。
    pub fn data_sync(&self) -> bool {
        match self {
            Self::Local(s) => s.power_loss,
            #[cfg(feature = "remote")]
            Self::Remote(s) => s.power_loss,
        }
    }'''
assert old in s
s=s.replace(old,new)
# write_atomic local arm → durability-aware
old='''    pub fn write_atomic(&self, path: &Path, data: &[u8]) -> Result<()> {
        match self {
            Self::Local(_) => fsutil::write_atomic(path, data),'''
new='''    pub fn write_atomic(&self, path: &Path, data: &[u8]) -> Result<()> {
        match self {
            Self::Local(s) => fsutil::write_atomic_opts(path, data, s.power_loss),'''
assert old in s
s=s.replace(old,new)
open(p,'w',encoding='utf-8').write(s)
print('fsutil/store ok')
