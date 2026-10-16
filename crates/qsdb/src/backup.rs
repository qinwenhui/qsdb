//! **物理快照**：`qsdb backup` / `qsdb recover`。
//!
//! # 它和逻辑备份（`SHOW CREATE TABLE` + `COPY`）不是一回事
//!
//! | | 逻辑备份（`dump`/`restore`） | **物理快照**（`backup`/`recover`） |
//! |---|---|---|
//! | 产物 | DDL + parquet 数据 | **库根的原样拷贝** |
//! | 恢复出来的是 | **另一份等价的库** | **同一个库**（generation / epoch / 段边界 / 格式版本都在） |
//! | 能被别的引擎读吗 | ✅ 能（Spark/pyarrow） | ❌ 不能（那是本仓的盘上格式） |
//! | 给谁用 | 交付 / 迁移 / 跨版本搬 | **主备 / 时间点回滚 / 整机搬迁** |
//!
//! # 一致性口径（**别想当然**）
//!
//! - 持 **catalog 锁**（挡住 DDL：`CREATE`/`DROP`/`ALTER`/`TRUNCATE`）——
//!   否则目录树可能在遍历中途变（建表建到一半、表被删掉）。
//! - 逐**分区**持分区锁（目录里有 `manifest.postcard` 的就是一个锁定单元）。
//!   ⇒ 写入与 compaction 都被挡在该分区之外，**那一个分区的快照是一个干净的时间点**。
//!   锁一次只拿一个分区，不长时间霸占全局。
//! - **并发写入是安全的**：段不可变 + manifest 原子替换 ⇒ 拿到的一定是某个时刻的一致点。
//! - ⚠️ **`gc` 不持分区锁**（它按"超过静默期"判定，把东西挪进 `_trash`）。
//!   若它在备份中途动了我们正在读的东西，**读会失败 ⇒ 备份报错**。
//!   **这是刻意的：宁可报错，也不给一份缺文件的"备份"**（那种备份比没有更坏）。
//!
//! # 不拷什么（都在这里写死，不靠调用方记）
//!
//! `_trash`（删掉的东西，不是活库的一部分）、`_spill`（查询临时）、
//! `_filecache`（纯缓存）、`*.lock` / `*.tmp`（进程态 / 半成品）。
//! ⚠️ **staging 目录**（远端写用，`--staging-dir`）若配在库根里**不在此列** ——
//! 它由调用方配，本模块不认识；**建议配在库根之外**。

use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::catalog::catalog_lock;
use crate::error::{QsdbError, Result};
use crate::fsutil::{PartitionLock, now_ms};
use crate::manifest::{MANIFEST_FILE, MANIFEST_VERSION};
use crate::store::Store;

/// 快照根目录里的元信息文件名。
pub const SNAPSHOT_FILE: &str = "qsdb_snapshot.json";

/// 一次快照的元信息（写在快照根上，`recover` 会校验它）。
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SnapshotInfo {
    pub created_at_ms: u64,
    /// 打快照的内核版本（排障用；**不做兼容闸门**）
    pub kernel_version: String,
    /// manifest 磁盘格式版本。**恢复时对不上要能说清楚** ——
    /// 老快照拿到新内核上、或反过来，得给一句人话而不是"文件损坏"。
    pub manifest_format: u32,
    /// 快照里的表（`db.table` 形式，排序）
    pub tables: Vec<String>,
    pub files: u64,
    pub bytes: u64,
}

/// 打一份物理快照到 `to`。
///
/// `to` 必须**不存在或为空** —— 不做静默合并（半个旧快照 + 半个新快照
/// 出来的东西既不是这次也不是上次，而没人看得出）。
pub fn snapshot(store: &Store, to: &Path) -> Result<SnapshotInfo> {
    require_local(store, "备份")?;
    let root = store.root();
    if to.starts_with(root) {
        // 否则遍历会把快照自己吃进去（而且可能递归）
        return Err(QsdbError::invalid(format!(
            "快照目录不能在库根里面（库 `{}`，快照 `{}`）",
            root.display(),
            to.display()
        )));
    }
    ensure_empty_target(to)?;

    // 持 catalog 锁：挡住 DDL（见模块文档"一致性口径"）
    let _cat = PartitionLock::acquire_local(&catalog_lock(root))?;

    let mut info = SnapshotInfo {
        created_at_ms: now_ms(),
        kernel_version: env!("CARGO_PKG_VERSION").to_string(),
        manifest_format: MANIFEST_VERSION,
        tables: Vec::new(),
        files: 0,
        bytes: 0,
    };
    copy_tree(root, root, to, &mut info)?;

    let json = serde_json::to_string_pretty(&info)
        .map_err(|e| QsdbError::meta(format!("serialize snapshot info failed: {e}")))?;
    fs::write(to.join(SNAPSHOT_FILE), json)
        .map_err(|e| QsdbError::meta(format!("write snapshot info failed: {e}")))?;
    Ok(info)
}

/// 从物理快照恢复到一个**空的库根**。
pub fn recover(store: &Store, from: &Path) -> Result<SnapshotInfo> {
    require_local(store, "恢复")?;
    let root = store.root();
    let meta = from.join(SNAPSHOT_FILE);
    let text = fs::read_to_string(&meta).map_err(|_| {
        QsdbError::invalid(format!(
            "`{}` 里没有 `{SNAPSHOT_FILE}` —— 这不是一份物理快照（逻辑备份请用 `qsdb restore`）",
            from.display()
        ))
    })?;
    let info: SnapshotInfo = serde_json::from_str(&text)
        .map_err(|e| QsdbError::invalid(format!("快照元信息解析失败：{e}")))?;
    // 版本对不上：**说清楚**，而不是让后面去报"文件损坏"
    if info.manifest_format != MANIFEST_VERSION {
        return Err(QsdbError::unsupported(format!(
            "这份快照的磁盘格式是 v{}，本内核是 v{MANIFEST_VERSION} —— \
             跨格式版本的恢复还没做（不是文件坏了，是版本不同）",
            info.manifest_format
        )));
    }
    ensure_empty_target(root)?;

    let mut written = SnapshotInfo {
        created_at_ms: info.created_at_ms,
        kernel_version: info.kernel_version.clone(),
        manifest_format: info.manifest_format,
        tables: info.tables.clone(),
        files: 0,
        bytes: 0,
    };
    // 反向：从快照拷回库根。**不再持锁**：目标是空库，没人跟它抢。
    copy_tree(from, from, root, &mut written)?;
    // 快照元信息**不是**库的一部分，别留在恢复出来的库里
    let stray = root.join(SNAPSHOT_FILE);
    if stray.is_file() {
        let _ = fs::remove_file(&stray);
    }
    Ok(written)
}

// ---------------------------------------------------------------------------

fn require_local(store: &Store, what: &str) -> Result<()> {
    if store.is_local() {
        Ok(())
    } else {
        Err(QsdbError::unsupported(format!(
            "{what}暂不支持远端库：`backup` 是**目录级**的字节拷贝（且依赖分区文件锁），\
             对象存储上既没有「目录」也没有 flock。远端请用逻辑备份（`qsdb dump --data`）"
        )))
    }
}

/// 目标必须不存在或为空目录。
///
/// ⚠️ **判据不是"目录非空就拒"**：调用方手里的 `Store` 来自 `Qsdb::open`，
/// 而 `open` **会在根下建目录**（`_catalog/` 等）—— 刚 open 完的库根天然不空，
/// 但它里面确实没有数据。所以判据精确到「**除了初始化产物之外什么都没有**」：
///
/// - 顶层只允许 `_catalog` / `_spill` / `_trash` / `_filecache`；
/// - 且 `_catalog` 里只允许锁文件（**有 `*.postcard` / `users.json` 就是真有东西了**）。
///
/// 别的任何一项 —— 包括任何一张表的目录 —— 都算"有数据"，一律拒。
fn ensure_empty_target(dir: &Path) -> Result<()> {
    if !dir.exists() {
        return fs::create_dir_all(dir)
            .map_err(|e| QsdbError::meta(format!("mkdir {} failed: {e}", dir.display())));
    }
    let mut offenders: Vec<String> = Vec::new();
    let rd = fs::read_dir(dir)
        .map_err(|e| QsdbError::meta(format!("read_dir {} failed: {e}", dir.display())))?;
    for e in rd.flatten() {
        let name = e.file_name().to_string_lossy().into_owned();
        if !matches!(
            name.as_str(),
            "_catalog" | "_spill" | "_trash" | "_filecache"
        ) {
            // `open` 会建**空的** `default/` 库目录 ⇒ 空的算初始化产物，有内容就不算
            if name == crate::catalog::DEFAULT_DB
                && e.file_type().map(|t| t.is_dir()).unwrap_or(false)
                && fs::read_dir(e.path())
                    .map(|mut d| d.next().is_none())
                    .unwrap_or(false)
            {
                continue;
            }
            offenders.push(name);
            continue;
        }
        // `_catalog` 里只认锁
        if name == "_catalog" {
            let Ok(sub) = fs::read_dir(e.path()) else {
                continue;
            };
            for s in sub.flatten() {
                let n = s.file_name().to_string_lossy().into_owned();
                if !n.ends_with(".lock") {
                    offenders.push(format!("_catalog/{n}"));
                }
            }
        }
    }
    if offenders.is_empty() {
        return Ok(());
    }
    offenders.sort();
    Err(QsdbError::invalid(format!(
        "目标 `{}` 里已经有东西（{}）—— **不做静默合并**（半个旧快照 + 半个新快照\
         出来的东西既不是这次也不是上次，而没人看得出）。请换一个空目录",
        dir.display(),
        offenders.join(", ")
    )))
}

/// 这些**不拷**。判据见模块文档"不拷什么"。
fn skip(rel: &str, name: &str) -> bool {
    // 顶层临时目录：**只在根一级判**（`rel` 是空的才是根）。
    // ⚠️ 判据是 `rel.is_empty()` 而不是 `!rel.contains('/')` —— 后者的意思是
    // "父目录在根下"，那会把**一张叫 `_trash` 的表**（`default/_trash`，`ident_ok`
    // 允许下划线开头）一起跳过。单测当场抓到。
    if rel.is_empty() && matches!(name, "_trash" | "_spill" | "_filecache") {
        return true;
    }
    name.ends_with(".lock") || name.ends_with(".tmp")
}

/// 递归拷贝。`root`/`from_root` 用来算相对路径（判"是不是顶层"）。
fn copy_tree(src_root: &Path, src: &Path, dst: &Path, info: &mut SnapshotInfo) -> Result<()> {
    fs::create_dir_all(dst)
        .map_err(|e| QsdbError::meta(format!("mkdir {} failed: {e}", dst.display())))?;
    // **锁定单元**：目录里有 manifest 就是一个分区（或一张无分区表）。
    // 持它的分区锁 → 该分区的写入与 compaction 被挡在外面。
    let _guard = if src.join(MANIFEST_FILE).is_file() {
        Some(PartitionLock::acquire_local(&src.join(".lock"))?)
    } else {
        None
    };

    // 表定义（`<db>/<table>/table.json`）—— 记进元信息
    let rel = src.strip_prefix(src_root).unwrap_or(Path::new(""));
    let rel_s = rel.to_string_lossy().replace('\\', "/");
    if src.join("table.json").is_file() && !rel_s.is_empty() {
        info.tables.push(rel_s.replace('/', "."));
    }

    let rd = fs::read_dir(src)
        .map_err(|e| QsdbError::meta(format!("read_dir {} failed: {e}", src.display())))?;
    // 排序：同一份输入两次快照产出同样的顺序（`tables` 稳定）
    let mut entries: Vec<(String, PathBuf)> = Vec::new();
    for e in rd.flatten() {
        let name = e.file_name().to_string_lossy().into_owned();
        if skip(&rel_s, &name) {
            continue;
        }
        entries.push((name, e.path()));
    }
    entries.sort();

    for (name, p) in entries {
        let d = dst.join(&name);
        if p.is_dir() {
            copy_tree(src_root, &p, &d, info)?;
        } else {
            let n = fs::copy(&p, &d).map_err(|e| {
                QsdbError::meta(format!(
                    "拷 {} → {} 失败：{e}（备份中途文件消失？见模块文档的一致性口径）",
                    p.display(),
                    d.display()
                ))
            })?;
            info.files += 1;
            info.bytes += n;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skip_规则() {
        assert!(skip("", "_trash"));
        assert!(skip("", "_spill"));
        assert!(
            !skip("default", "_trash"),
            "库里有个叫 _trash 的表不该被跳过"
        );
        assert!(skip("default/t", ".lock"));
        assert!(skip("default/t", "x.tmp"));
        assert!(!skip("default/t", "table.json"));
    }
}
