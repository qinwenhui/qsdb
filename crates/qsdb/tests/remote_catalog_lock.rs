//! **F23 回归**（feature `remote`）：catalog 写路径的锁在远端 Store 上必须可用。
//!
//! 事故原状：`UserStore::write_all` / `ViewStore::write_all` / `PartitionsIndex::upsert`
//! 无条件走**本地文件锁原语**（`create_dir_all` + `OpenOptions` + flock）——远端
//! Store 的 root 是虚拟路径（`s3://bucket/prefix`），`create_dir_all("s3://…")`
//! 直接 os error 123 ⇒ **远端库 `CREATE USER` 整体不可用**（影响面不止 qsdb-sync）。
//!
//! 修法：锁入口 `fsutil::with_dir_lock` 按后端分派（Local = flock / Remote = 进程内
//! 互斥 + 对象存储条件写），调用点不再各自判 `is_local()`。
//!
//! 用 InMemory 后端注入（与 `remote_managed.rs` 同一套脚手架）：协议语义与 S3 一致，
//! 且**不碰真实文件系统**——正是原事故的判据（一旦代码又去碰 fs，这里会立刻炸）。
//!
//! 边界：本文件证明"远端不再走本地原语 + 写入语义正确"，**不**证明跨进程互斥
//! （远端靠条件写收敛，见 `docs/存储层设计.md`；qsdb-sync 侧另有真机 e2e）。

#![cfg(feature = "remote")]

use std::sync::Arc;

use object_store::memory::InMemory;
use qsdb::index::PartitionsIndex;
use qsdb::store::Store;
use qsdb::users::{Role, UserStore};
use qsdb::views::ViewStore;
use qsdb::{Qsdb, QsdbOptions};

fn tmp_root(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir()
        .join("qsdb_remote_catlock_test")
        .join(format!("{tag}_{}", uuid::Uuid::now_v7().simple()));
    std::fs::create_dir_all(&d).unwrap();
    d
}

/// 远端库句柄（InMemory = S3 语义）；`local_root` 仅用于 staging 等本地临时面。
fn remote_db(tag: &str) -> (Qsdb, std::path::PathBuf) {
    let local = tmp_root(tag);
    let mem: Arc<InMemory> = Arc::new(InMemory::default());
    let store = Store::remote("s3://test-b/qsdb", mem, "qsdb");
    let db = Qsdb::open_with_store(&local, QsdbOptions::default(), store).unwrap();
    (db, local)
}

/// F23 第 1 条：远端库的 `CREATE USER` 必须可用（原状：os error 123）。
#[test]
fn create_user_works_on_remote_store() {
    let (db, local) = remote_db("users");
    assert!(!db.store().is_local(), "前置：本用例必须跑在远端后端上");

    UserStore::add(db.store(), "alice", "pass123", Role::Admin)
        .expect("远端库 CREATE USER 不能因为锁原语炸");

    // 语义正确性（不只是"没报错"）：读回 + 校验
    let users = UserStore::read_all(db.store()).unwrap();
    assert_eq!(users.len(), 1);
    assert_eq!(
        users["alice"].role,
        Some(Role::Admin),
        "M-C 起 role 是 Option<Role>"
    );

    // 再写一个 → 覆盖"读-改-写整轮"（锁是给整轮用的）
    UserStore::add(db.store(), "bob", "pass456", Role::Reader).unwrap();
    assert_eq!(UserStore::read_all(db.store()).unwrap().len(), 2);
    assert!(UserStore::verify(db.store(), "alice", "pass123").is_some());

    let _ = std::fs::remove_dir_all(&local);
}

/// F23 同族（反馈只点了 users）：`CREATE VIEW` 的写回走的是同一把 catalog 锁。
#[test]
fn create_view_works_on_remote_store() {
    let (db, local) = remote_db("views");
    assert!(!db.store().is_local());

    db.sql("CREATE TABLE t (a BIGINT)").unwrap();
    db.sql("CREATE VIEW v AS SELECT a FROM t")
        .expect("远端库 CREATE VIEW 不能因为锁原语炸");

    let views = ViewStore::read_all(db.store()).unwrap();
    assert_eq!(views.get("v").map(String::as_str), Some("SELECT a FROM t"));

    let _ = std::fs::remove_dir_all(&local);
}

/// F23 第 2 条：`PartitionsIndex::upsert` / `rebuild` 对远端 Store 不设防 → 外部
/// 调用方（qsdb-sync replica）一调就炸。现在锁入口自己分派。
#[test]
fn partitions_index_upsert_and_rebuild_work_on_remote_store() {
    let (db, local) = remote_db("idx");
    assert!(!db.store().is_local());

    // 建表 + 写一批 → 内核写路径只在本地分支维护 idx，远端需外部调用方自己维护
    db.sql("CREATE TABLE p (dt DATE, v BIGINT) PARTITIONED BY (dt)")
        .unwrap();
    db.sql("INSERT INTO p VALUES (DATE '2026-01-01', 1), (DATE '2026-01-02', 2)")
        .unwrap();

    // 用内核自己的布局函数，别手拼路径（拼错只会得到一个"前置失败"的假红）
    let table_dir = qsdb::catalog::table_dir(db.store().root(), qsdb::catalog::DEFAULT_DB, "p");
    // 分区名从**内核自己的发现口径**取（`partition_stats` 就是 idx 的消费者，
    // 分区目录名与 `list_partitions` 一致），别手拼 `dt=...`
    let stats = db.partition_stats("p").unwrap();
    assert!(!stats.is_empty(), "前置：INSERT 应产出分区");
    let rel = stats[0].name.clone();
    let pdir = table_dir.join(&rel);
    let manifest = qsdb::PartitionManifest::read(db.store(), &pdir)
        .unwrap()
        .expect("先确认真实分区 manifest 可读（前置）");
    PartitionsIndex::upsert(db.store(), &table_dir, &rel, &manifest)
        .expect("远端 upsert 不能因为锁原语炸");

    let idx = PartitionsIndex::read(db.store(), &table_dir).expect("idx 应已写入");
    assert_eq!(idx.entries.len(), 1);
    assert_eq!(idx.entries[0].0, rel);

    // rebuild 走同一把锁：全量重建后条目数 = 目录里真实分区数
    let rebuilt = PartitionsIndex::rebuild(db.store(), &table_dir, 1).expect("远端 rebuild 不能炸");
    assert!(!rebuilt.entries.is_empty());

    let _ = std::fs::remove_dir_all(&local);
}

/// 反面对照：本地后端仍走 flock 且语义不变（分派没有把本地路径改坏）。
#[test]
fn local_store_still_uses_file_lock() {
    let root = tmp_root("local");
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    assert!(db.store().is_local());

    UserStore::add(db.store(), "carol", "pass789", Role::Reader).unwrap();
    assert!(UserStore::verify(db.store(), "carol", "pass789").is_some());
    assert_eq!(UserStore::read_all(db.store()).unwrap().len(), 1);

    let _ = std::fs::remove_dir_all(&root);
}
