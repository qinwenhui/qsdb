//! **远端写路径的 watch 事件**（十九批Ⅵ，feature `remote`）。
//!
//! 事故原状：`/watch` 的"门铃"只在本地盘写入路径里按——远端（S3/HDFS 语义）
//! 的写路径在 `occ_commit` 后拿不到 generation，`WriteResult.generation = 0`
//! ⇒ `api::emit_commit` 的守卫（generation=0 不发）把事件**整条吞掉**。订阅方
//! 对远端库只能慢轮询兜底（qsdb-sync F 系列实测记录；studio M2.3 面板同源）。
//!
//! 修法：`occ_commit` 回传提交后的 generation（它在 OCC 闭包里产生，此前没有
//! 回传通道）→ 直写路径的 `WriteResult`、`flush_staged` 的暂存提交、远端清空
//! 三处全部接上 → 事件带**真实** generation 发出。
//!
//! 用 InMemory 后端注入（与 `remote_catalog_lock.rs` 同一套脚手架）：S3 语义、
//! 不碰真实文件系统。**判据不是"有事件"而是"generation 单调且逐次 +1"**——
//! 修一半（比如只发事件不带版本）在这里会当场红。

#![cfg(feature = "remote")]

use std::sync::{Arc, Mutex};

use object_store::memory::InMemory;
use qsdb::commit::CommitEvent;
use qsdb::store::Store;
use qsdb::{Qsdb, QsdbOptions};

fn tmp_root(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir()
        .join("qsdb_remote_watch")
        .join(format!("{tag}_{}", uuid::Uuid::now_v7().simple()));
    std::fs::create_dir_all(&d).unwrap();
    d
}

/// 远端库句柄 + 事件收集器（on_commit 钩子原样记录）。
fn remote_db_with_hook(tag: &str) -> (Qsdb, std::path::PathBuf, Arc<Mutex<Vec<CommitEvent>>>) {
    let local = tmp_root(tag);
    let mem: Arc<InMemory> = Arc::new(InMemory::default());
    let store = Store::remote("s3://test-b/qsdb", mem, "qsdb");
    let events: Arc<Mutex<Vec<CommitEvent>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&events);
    let opts = QsdbOptions {
        on_commit: Some(qsdb::commit::CommitHook::new(move |ev: &CommitEvent| {
            sink.lock().unwrap().push(ev.clone());
        })),
        ..Default::default()
    };
    let db = Qsdb::open_with_store(&local, opts, store).unwrap();
    (db, local, events)
}

/// 直写路径：append 事件逐条带真实且单调的 generation。
#[test]
fn 远端append_事件带真实generation() {
    let (db, local, events) = remote_db_with_hook("append");
    db.sql("CREATE TABLE t (a BIGINT)").unwrap();

    db.sql("INSERT INTO t VALUES (1)").unwrap();
    db.sql("INSERT INTO t VALUES (2)").unwrap();
    db.sql("INSERT INTO t VALUES (3)").unwrap();

    let evs = events.lock().unwrap();
    assert_eq!(evs.len(), 3, "三次 append 三条事件: {evs:?}");
    for (i, e) in evs.iter().enumerate() {
        assert_eq!(e.table, "t");
        assert_eq!(e.kind, qsdb::commit::CommitKind::Append);
        assert_eq!(
            e.generation,
            (i + 1) as u64,
            "第 {} 次提交后 generation = {}",
            i + 1,
            e.generation
        );
    }
    drop(evs);
    let _ = std::fs::remove_dir_all(&local);
}

/// 覆盖与清空：各自的事件 + generation 单调（覆盖 +1、清空继承后由下次提交 +1）。
#[test]
fn 远端overwrite与clear_事件与generation单调() {
    let (db, local, events) = remote_db_with_hook("ow");
    db.sql("CREATE TABLE t (dt DATE, a BIGINT) PARTITIONED BY (dt)")
        .unwrap();
    db.sql("INSERT INTO t VALUES (DATE '2026-01-01', 1)")
        .unwrap();
    db.sql("INSERT INTO t VALUES (DATE '2026-01-01', 2)")
        .unwrap();

    // overwrite（分区级原子替换）
    db.sql("INSERT OVERWRITE t VALUES (DATE '2026-01-01', 9)")
        .unwrap();
    // clear（分区键谓词）
    db.sql("DELETE FROM t WHERE dt = DATE '2026-01-01'")
        .unwrap();

    let evs = events.lock().unwrap();
    let kinds: Vec<&str> = evs
        .iter()
        .map(|e| match e.kind {
            qsdb::commit::CommitKind::Append => "A",
            qsdb::commit::CommitKind::Overwrite => "O",
            qsdb::commit::CommitKind::Clear => "C",
            ref other => unreachable!("未登记 kind: {other:?}"),
        })
        .collect();
    assert_eq!(kinds, vec!["A", "A", "O", "C"], "事件序列: {evs:?}");
    // generation 全程单调不减（覆盖 +1；清空继承；后续提交在其上加）
    let gens: Vec<u64> = evs.iter().map(|e| e.generation).collect();
    assert!(
        gens.windows(2).all(|w| w[1] >= w[0]),
        "generation 必须单调不减: {gens:?}"
    );
    assert!(gens[2] > gens[1], "overwrite 必须推进 generation");
    drop(evs);
    let _ = std::fs::remove_dir_all(&local);
}

/// staged 写（写盘即 ACK）：事件在 **flush_staged 提交时**发，带真实 generation；
/// ACK 时刻不发（generation=0 守卫）。
#[test]
fn 远端staged写_事件在flush时发() {
    let local = tmp_root("staged");
    let mem: Arc<InMemory> = Arc::new(InMemory::default());
    let store = Store::remote("s3://test-b/qsdb", mem, "qsdb");
    let events: Arc<Mutex<Vec<CommitEvent>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&events);
    let opts = QsdbOptions {
        staging_dir: Some(local.join("staging")),
        on_commit: Some(qsdb::commit::CommitHook::new(move |ev: &CommitEvent| {
            sink.lock().unwrap().push(ev.clone());
        })),
        ..Default::default()
    };
    let db = Qsdb::open_with_store(&local, opts, store).unwrap();
    db.sql("CREATE TABLE t (a BIGINT)").unwrap();

    db.sql("INSERT INTO t VALUES (1)").unwrap();
    db.sql("INSERT INTO t VALUES (2)").unwrap();
    assert!(
        events.lock().unwrap().is_empty(),
        "ACK 时刻无提交 ⇒ 不该有事件"
    );

    let (uploaded, rows) = db.flush_staged().unwrap();
    assert_eq!((uploaded, rows), (2, 2));
    let evs = events.lock().unwrap();
    assert_eq!(evs.len(), 2, "flush 提交 ⇒ 两条 Append");
    assert_eq!(evs[0].generation, 1);
    assert_eq!(evs[1].generation, 2);
    drop(evs);

    // 幂等：再次 flush 无残留 ⇒ 无新事件（崩溃补传不重复按铃）
    let (uploaded2, _) = db.flush_staged().unwrap();
    assert_eq!(uploaded2, 0);
    assert!(events.lock().unwrap().len() == 2, "补传不得重复发事件");
    let _ = std::fs::remove_dir_all(&local);
}
