//! 文件系统原语：原子替换、文件锁、trash。
//!
//! 跨平台要点（设计文档 §7.5）：
//! - Windows `std::fs::rename` 不能覆盖已存在文件 → 用 `MoveFileExW(REPLACE_EXISTING)`
//! - Windows 删除已打开文件会失败 → 一律先移入 `_trash`，静默期后清理
//! - 分区锁：fs2（Windows LockFileEx / POSIX flock）

use std::fs::{self, File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};

use fs2::FileExt;

use crate::error::{QsdbError, Result};

/// 原子替换：`tmp` → `dest`（dest 存在则替换）。
///
/// 这是所有元数据提交的唯一原语：rename 成功即提交。
#[cfg(unix)]
pub fn atomic_replace<T: AsRef<Path>, D: AsRef<Path>>(tmp: T, dest: D) -> Result<()> {
    fs::rename(tmp.as_ref(), dest.as_ref())
        .map_err(|e| QsdbError::meta(format!("atomic rename failed: {e}")))
}

#[cfg(windows)]
pub fn atomic_replace<T: AsRef<Path>, D: AsRef<Path>>(tmp: T, dest: D) -> Result<()> {
    use windows_sys::Win32::Storage::FileSystem::{
        MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW,
    };
    let src_w: Vec<u16> = tmp
        .as_ref()
        .as_os_str()
        .to_string_lossy()
        .encode_utf16()
        .chain(std::iter::once(0))
        .collect();
    let dst_w: Vec<u16> = dest
        .as_ref()
        .as_os_str()
        .to_string_lossy()
        .encode_utf16()
        .chain(std::iter::once(0))
        .collect();
    // SAFETY: 两个指针均为合法 NUL 结尾 UTF-16 缓冲
    let ok = unsafe {
        MoveFileExW(
            src_w.as_ptr(),
            dst_w.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    };
    if ok != 0 {
        Ok(())
    } else {
        // 兜底：个别文件系统不支持替换（如网络盘），锁内退化为 remove+rename
        let _ = fs::remove_file(dest.as_ref());
        fs::rename(tmp.as_ref(), dest.as_ref())
            .map_err(|e| QsdbError::meta(format!("atomic rename failed: {e}")))
    }
}

/// 原子写文件：`{dest}.{uuid}.tmp` 写入 → fsync（尽力而为）→ 原子替换到 `dest`。
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
        // **默认档（`sync = false`）不做 fsync**：`Durability::ProcessCrash` 的承诺是
        // 「页缓存即持久——kill -9 后数据完好」，页缓存本来就够；这里再 sync 一次的
        // 后果是**每次元数据写都强制落盘**（实测 45ms/次，见 `基准对账笔记`：
        // 一次 `db.append` = 段 + manifest + idx 三次 fsync ≈ 112ms，导入因此慢几十倍）。
        // 原先写的是 `f.sync_all().ok()`（"尽力而为"）——既付了代价，又因为吞错
        // 没有任何语义收益；与文档承诺也不一致。掉电安全请显式选 `PowerLoss`。
        if sync {
            f.sync_all()
                .map_err(|e| QsdbError::meta(format!("fsync tmp failed: {e}")))?;
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
pub fn dir_sync(_path: &Path) {}

/// 临时文件同目录兄弟路径。
pub fn tmp_sibling(path: &Path) -> PathBuf {
    let mut name = path
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| "file".into());
    name.push('.');
    name.push_str(&uuid::Uuid::now_v7().simple().to_string());
    name.push_str(".tmp");
    path.with_file_name(name)
}

/// **本地**分区互斥锁：阻塞获取独占锁，守卫释放文件句柄即解锁。
///
/// 名字里带 `local` 是刻意的：它只认**真实文件系统**。远端 Store（S3/HDFS）的
/// root 是虚拟路径，`create_dir_all("s3://…")` 只会炸（F23 的原始事故）。
/// 跨后端安全的入口是 [`with_dir_lock`]，新代码一律用它。
pub struct PartitionLock {
    _file: File,
}

impl PartitionLock {
    /// `path` 为锁文件路径（通常 `{partition}/.lock`），不存在则创建。
    /// **仅限 Local 后端**（见类型文档）。
    pub fn acquire_local(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|e| QsdbError::meta(format!("mkdir {} failed: {e}", parent.display())))?;
        }
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(path)
            .map_err(|e| QsdbError::meta(format!("open lock {} failed: {e}", path.display())))?;
        file.lock_exclusive()
            .map_err(|e| QsdbError::meta(format!("lock {} failed: {e}", path.display())))?;
        Ok(Self { _file: file })
    }
}

/// **目录级互斥（按后端分派）**——跨后端安全的加锁入口，除纯本地内部路径外一律用它。
///
/// - `Local`：同目录 flock（[`PartitionLock::acquire_local`]）——跨进程/多实例互斥；
/// - `Remote`：**进程内**同路径互斥（`remote::with_partition_mutex`）——远端没有
///   "文件锁"这种原语，跨进程/多实例的收敛交给对象存储的条件写（OCC / CAS）。
///
/// 为什么是**闭包**而不是 RAII 守卫：Remote 臂要持 `Arc<Mutex<()>>` 的 guard，
/// 而 `Arc` 存在全局表里——guard 会自引用，生命周期过不了 borrowck；闭包把临界区
/// 圈起来，两臂形状一致，调用点也不会有"忘了 drop / 提前 drop"的余地。
///
/// F23（qsdb-sync）：远端库 `CREATE USER` / 远端 idx 维护此前走本地 flock 直接炸，
/// 影响面不止单一调用方——所以把分派收在这一个入口，调用点不必各自 `is_local()`。
pub fn with_dir_lock<T>(
    store: &crate::store::Store,
    path: &Path,
    f: impl FnOnce() -> Result<T>,
) -> Result<T> {
    if store.is_local() {
        let _guard = PartitionLock::acquire_local(path)?;
        return f();
    }
    #[cfg(feature = "remote")]
    {
        crate::remote::with_partition_mutex(path, f)
    }
    #[cfg(not(feature = "remote"))]
    {
        let _ = (path, f);
        Err(QsdbError::unsupported(
            "remote store requires an s3/hdfs build",
        ))
    }
}

/// 把文件/目录移入 `{root}/_trash/{ts}/...`（跨平台安全删除的第一步）。
///
/// 返回是否移动成功（源不存在 → Ok(false)）。
pub fn move_to_trash(root: &Path, src: &Path) -> Result<bool> {
    if !src.exists() {
        return Ok(false);
    }
    let ts = uuid::Uuid::now_v7()
        .get_timestamp()
        .map(|t| {
            let (secs, nanos) = t.to_unix();
            secs * 1000 + u64::from(nanos) / 1_000_000
        })
        .unwrap_or(0);
    let rel = src
        .strip_prefix(root)
        .map_err(|_| QsdbError::invalid("trash source must be inside db root"))?;
    // 分隔符归一，避免平台差异
    let rel = rel.to_string_lossy().replace('\\', "/");
    let dest = root.join("_trash").join(ts.to_string()).join(&rel);
    if let Some(parent) = dest.parent() {
        fs::create_dir_all(parent)
            .map_err(|e| QsdbError::meta(format!("mkdir trash failed: {e}")))?;
    }
    // 目标已存在（极小概率同毫秒同名）→ 追加后缀
    let dest = if dest.exists() {
        let mut d = dest.into_os_string();
        d.push(format!(".{}", uuid::Uuid::now_v7().simple()));
        PathBuf::from(d)
    } else {
        dest
    };
    // **记下"原来在哪"**：`_trash/{ts}/` 里躺着的东西从路径看不出身份
    // （`default/t` 与 `_spill/xxx` 长得没区别），而"捞回来"必须知道往哪放。
    // 写在 `{ts}` 根上，且**先写标记再 rename** —— 反过来的话中途崩会留下
    // 一个"有货但认不出来"的条目。
    let ts_dir = root.join("_trash").join(ts.to_string());
    fs::write(ts_dir.join(TRASH_MARKER), rel.as_bytes())
        .map_err(|e| QsdbError::meta(format!("write trash marker failed: {e}")))?;
    match fs::rename(src, &dest) {
        Ok(()) => Ok(true),
        // Windows 上可能被读句柄占用：跳过，留待下轮 GC
        Err(e) if locked_on_windows(&e) => Ok(false),
        Err(e) => Err(QsdbError::meta(format!(
            "move {} to trash failed: {e}",
            src.display()
        ))),
    }
}

/// 回收站条目的**来源标记**文件名（内容 = 原始相对路径，`/` 分隔）。
pub const TRASH_MARKER: &str = ".qsdb_src";

/// 回收站里的一条。
pub struct TrashEntry {
    /// 目录名（毫秒时间戳）—— 恢复时的句柄
    pub ts: u64,
    /// 原始相对路径（相对库根）。`None` = **旧版遗留**（加标记之前删的），
    /// 认不出原来是什么 ⇒ 只能看、不能恢复（**不猜**）。
    pub src: Option<String>,
    /// 占用字节
    pub bytes: u64,
}

/// 列回收站。按时间倒序（最近的在前 —— 误删通常刚发生）。
pub fn list_trash(root: &Path) -> Result<Vec<TrashEntry>> {
    let trash = root.join("_trash");
    let mut out = Vec::new();
    let Ok(rd) = fs::read_dir(&trash) else {
        return Ok(out); // 没有 _trash 目录 = 空的
    };
    for entry in rd.flatten() {
        let Some(ts) = entry
            .file_name()
            .to_str()
            .and_then(|s| s.parse::<u64>().ok())
        else {
            continue;
        };
        let src = fs::read_to_string(entry.path().join(TRASH_MARKER))
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
        out.push(TrashEntry {
            ts,
            src,
            bytes: dir_size(&entry.path()),
        });
    }
    out.sort_by_key(|e| std::cmp::Reverse(e.ts));
    Ok(out)
}

/// 把 `ts` 这条捞回它原来的位置。
///
/// ⚠️ **目标已存在 ⇒ 拒绝**（不做静默覆盖）：恢复是"撤销一次删除"，
/// 而那个位置现在有东西，说明**删除之后又建了同名的** —— 覆盖它等于用一次
/// 撤销毁掉另一份数据。让调用方自己决定先删哪个。
pub fn restore_trash(root: &Path, ts: u64) -> Result<PathBuf> {
    let ts_dir = root.join("_trash").join(ts.to_string());
    if !ts_dir.is_dir() {
        return Err(QsdbError::not_found(format!("回收站里没有条目 {ts}")));
    }
    let raw = fs::read_to_string(ts_dir.join(TRASH_MARKER)).map_err(|_| {
        QsdbError::invalid(format!(
            "条目 {ts} 没有来源标记（`{TRASH_MARKER}`）—— 多半是加标记之前的旧遗留，             认不出原来在哪，**不猜**"
        ))
    })?;
    let src = raw.trim();
    if src.is_empty() {
        return Err(QsdbError::meta(format!("条目 {ts} 的来源标记是空的")));
    }
    let dest = root.join(src.replace('/', std::path::MAIN_SEPARATOR_STR));
    if dest.exists() {
        return Err(QsdbError::invalid(format!(
            "恢复目标 `{}` 已经存在 —— 拒绝覆盖（那多半是你删掉之后又建了同名的）。             请先处理掉它，或直接去 `{}` 里手动取",
            dest.display(),
            ts_dir.display()
        )));
    }
    // 条目的**实际数据**未必正好在 `{ts}/{src}`：同毫秒撞名时会加后缀。
    // 两种都认：先试原路径，再找 `{src}.*`（后缀形态）。
    let direct = ts_dir.join(src.replace('/', std::path::MAIN_SEPARATOR_STR));
    let actual = if direct.exists() {
        direct
    } else {
        let (parent, base): (Option<&str>, &str) = match src.rsplit_once('/') {
            Some((p, b)) => (Some(p), b),
            None => (None, src),
        };
        let dir = match parent {
            Some(p) => ts_dir.join(p.replace('/', std::path::MAIN_SEPARATOR_STR)),
            None => ts_dir.clone(),
        };
        let mut hit: Option<PathBuf> = None;
        for e in fs::read_dir(&dir).into_iter().flatten().flatten() {
            let n = e.file_name().to_string_lossy().into_owned();
            if n == base || n.starts_with(&format!("{base}.")) {
                hit = Some(e.path());
                break;
            }
        }
        hit.ok_or_else(|| QsdbError::meta(format!("条目 {ts} 里找不到 `{src}` 对应的数据")))?
    };
    if let Some(p) = dest.parent() {
        fs::create_dir_all(p).map_err(|e| QsdbError::meta(format!("mkdir failed: {e}")))?;
    }
    fs::rename(&actual, &dest)
        .map_err(|e| QsdbError::meta(format!("restore {} failed: {e}", dest.display())))?;
    // 数据挪走了，条目壳（含标记）留着没意义 → 清掉
    let _ = fs::remove_dir_all(&ts_dir);
    Ok(dest)
}

fn locked_on_windows(e: &io::Error) -> bool {
    cfg!(windows)
        && matches!(
            e.raw_os_error(),
            Some(5) | Some(32) | Some(33) // ACCESS_DENIED / SHARING_VIOLATION / LOCK_VIOLATION
        )
}

/// 清理 trash：删除早于 `grace_ms` 的条目。
pub fn purge_trash(root: &Path, grace_ms: u64) -> Result<(u64, u64)> {
    let trash = root.join("_trash");
    if !trash.exists() {
        return Ok((0, 0));
    }
    let mut purged = 0u64;
    let mut freed = 0u64;
    for entry in
        fs::read_dir(&trash).map_err(|e| QsdbError::meta(format!("read trash failed: {e}")))?
    {
        let entry = entry.map_err(|e| QsdbError::meta(format!("read trash entry failed: {e}")))?;
        let name = entry.file_name();
        let Some(ts) = name.to_str().and_then(|s| s.parse::<u64>().ok()) else {
            continue;
        };
        // 年龄判定（而非 ts 与 cutoff 直接比较）：Linux 快 I/O 下 move 与
        // purge 可落在同一毫秒，ts >= cutoff 会把零龄条目误判为太新
        if now_ms().saturating_sub(ts) < grace_ms {
            continue;
        }
        freed += dir_size(&entry.path());
        match fs::remove_dir_all(entry.path()) {
            Ok(()) => purged += 1,
            Err(e) if locked_on_windows(&e) => {} // 句柄占用，下轮再清
            Err(e) => {
                return Err(QsdbError::meta(format!(
                    "purge trash {} failed: {e}",
                    entry.path().display()
                )));
            }
        }
    }
    // 清空后顺手删除 _trash 本身（失败无碍，下轮再删）
    let _ = fs::remove_dir(&trash);
    Ok((purged, freed))
}

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// 目录/文件字节数（尽力而为，出错按 0）。
pub fn dir_size(path: &Path) -> u64 {
    let md = match fs::symlink_metadata(path) {
        Ok(md) => md,
        Err(_) => return 0,
    };
    if md.is_file() {
        return md.len();
    }
    let mut total = 0;
    if let Ok(rd) = fs::read_dir(path) {
        for entry in rd.flatten() {
            total += dir_size(&entry.path());
        }
    }
    total
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmpdir(tag: &str) -> PathBuf {
        let d = std::env::temp_dir()
            .join("qsdb_fsutil_test")
            .join(format!("{tag}_{}", uuid::Uuid::now_v7().simple()));
        fs::create_dir_all(&d).unwrap();
        d
    }

    #[test]
    fn atomic_write_and_replace() {
        let dir = tmpdir("aw");
        let dest = dir.join("m.bin");
        write_atomic(&dest, b"v1").unwrap();
        assert_eq!(fs::read(&dest).unwrap(), b"v1");
        write_atomic(&dest, b"v2").unwrap();
        assert_eq!(fs::read(&dest).unwrap(), b"v2");
        // 无 tmp 残留
        let leftover: Vec<_> = fs::read_dir(&dir).unwrap().flatten().collect();
        assert_eq!(leftover.len(), 1, "only dest should remain");
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn lock_mutual_exclusion() {
        let dir = tmpdir("lock");
        let lock_path = dir.join(".lock");
        let guard = PartitionLock::acquire_local(&lock_path).unwrap();
        // 同进程同文件二次加锁：fs2 flock 语义下同进程会成功（句柄独立），
        // 跨进程/跨线程互斥由 OS 保证；这里只验证守卫可正常获取与释放
        drop(guard);
        let g2 = PartitionLock::acquire_local(&lock_path).unwrap();
        drop(g2);
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn trash_roundtrip() {
        let dir = tmpdir("trash");
        let victim = dir.join("db/t/x.parquet");
        fs::create_dir_all(victim.parent().unwrap()).unwrap();
        fs::write(&victim, b"data").unwrap();
        assert!(move_to_trash(&dir, &victim).unwrap());
        assert!(!victim.exists());
        // trash 内存在副本
        assert!(dir.join("_trash").exists());
        // 重复移动不存在文件 → false
        assert!(!move_to_trash(&dir, &victim).unwrap());
        // 静默期 0 → 立即可清
        let (purged, _) = purge_trash(&dir, 0).unwrap();
        assert_eq!(purged, 1);
        assert!(!dir.join("_trash").exists());
        fs::remove_dir_all(&dir).ok();
    }
}
