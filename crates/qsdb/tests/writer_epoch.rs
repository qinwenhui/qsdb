//! **写者身份代 `writer_epoch`**（qsdb-sync F25）。
//!
//! 问题：`generation` 是**分区提交计数器**，不是内容指纹。防脑裂只是单向保证
//! （`dst > src` 拒绝），而 `dst == src` 并不蕴含内容一致——脑裂后两边各自演进
//! 恰好追平到同一个 generation 时，replica 会判 "up_to_date" 而**静默分歧**。
//!
//! 约定（内核侧）：
//! - `QsdbOptions::writer_epoch`（默认 0）是**持久身份代**，每次提交盖进
//!   `PartitionManifest::epoch`；生态用 `(epoch, generation)` 复合序判增量。
//! - 进程**永不自增**它——抬起它是一次**运维动作**（接管/failover）。
//! - 内核自己也不许"退回去写"：盘上 epoch 比本进程配置大 → `STALE_WRITER`。
//!
//! 本文件锁的就是这三条。

use qsdb::{Qsdb, QsdbOptions};

/// 测试目录唯一序号：pid 挡跨进程碰撞，序号挡同进程内并发（Windows 时钟
/// 粒度 ~100ns，纳秒后缀在同 tick 内会重复——CI 上真炸过，见 constraints）。
fn _uniq_seq() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    SEQ.fetch_add(1, Ordering::Relaxed)
}

fn tmp(tag: &str) -> std::path::PathBuf {
    let root = std::env::temp_dir()
        .join("qsdb_writer_epoch_test")
        .join(format!("{tag}_{:x}_{}", std::process::id(), _uniq_seq()));
    let _ = std::fs::remove_dir_all(&root);
    root
}

fn opts(epoch: u64) -> QsdbOptions {
    QsdbOptions {
        writer_epoch: epoch,
        ..Default::default()
    }
}

/// 单列行数（`sql_collect` 返回批列表）。
fn count(db: &Qsdb, sql: &str) -> usize {
    db.sql_collect(sql)
        .unwrap()
        .iter()
        .map(|b| b.num_rows())
        .sum()
}

/// `INSERT` 被拒时的错误文案（`SqlResult` 不实现 `Debug`，用不了 `expect_err`）。
fn insert_err(db: &Qsdb, sql: &str) -> String {
    match db.sql(sql) {
        Ok(_) => panic!("本应被拒却成功了: {sql}"),
        Err(e) => format!("{e}"),
    }
}

/// 读指定分区（本文件里恒为 `p=1`）manifest。
fn manifest_of(db: &Qsdb, table: &str) -> qsdb::manifest::PartitionManifest {
    let pdir = db.root().join("default").join(table).join("p=1");
    qsdb::manifest::PartitionManifest::read(db.store(), &pdir)
        .unwrap()
        .unwrap_or_else(|| panic!("{} 的 manifest 不存在", pdir.display()))
}

/// 建两列分区表：`p` 分区列、`id` 数据列。
///
/// 刻意**不**造"只有分区列一列"（零数据列）的退化形状——那种表另有边界，
/// 与本文件无关。
fn setup(db: &Qsdb) {
    db.sql("CREATE TABLE t (id BIGINT, p BIGINT) PARTITIONED BY (p)")
        .unwrap();
}

/// 提交把配置的 epoch 盖进 manifest；`generation` 照旧自增（两者互不干扰）。
#[test]
fn commit_stamps_the_configured_epoch() {
    let root = tmp("stamp");
    let db = Qsdb::open(&root, opts(7)).unwrap();
    setup(&db);
    db.sql("INSERT INTO t VALUES (1, 1), (2, 1)").unwrap();
    assert_eq!(manifest_of(&db, "t").epoch, 7, "提交必须把身份代盖章到盘上");
    assert_eq!(count(&db, "SELECT id FROM t"), 2);

    // 再提交一次：generation 递增，但 epoch 不变（**它不是计数器**）
    let before = manifest_of(&db, "t").generation;
    db.sql("INSERT INTO t VALUES (3, 1)").unwrap();
    let after = manifest_of(&db, "t");
    assert_eq!(after.generation, before + 1, "generation 照常自增");
    assert_eq!(after.epoch, 7, "epoch 不该跟着自增");

    std::fs::remove_dir_all(&root).ok();
}

/// **过期写者必须被拒**：盘上 epoch 更大 ⇒ 本进程不许写。
///
/// 这是 F25 的第二半——光记录 epoch 只是让生态能判，这条让内核自己也不许回退。
#[test]
fn stale_writer_is_rejected() {
    let root = tmp("stale");
    {
        let db = Qsdb::open(&root, opts(5)).unwrap();
        setup(&db);
        db.sql("INSERT INTO t VALUES (1, 1)").unwrap();
    } // 第 5 代写者退场

    // 同一个库，用更小的 epoch 打开 —— 模拟"接管后老进程又连回来了"
    let db_old = Qsdb::open(&root, opts(3)).unwrap();
    assert_eq!(count(&db_old, "SELECT id FROM t"), 1, "前置：数据在");

    // ① append 被拒
    let msg = insert_err(&db_old, "INSERT INTO t VALUES (2, 1)");
    assert!(
        msg.contains("stale writer") && msg.contains("epoch 5"),
        "错误要说清是哪一代写者占着（实得 {msg}）"
    );
    // ② overwrite 同样被拒（两条写路径都要拦）
    let e2 = insert_err(&db_old, "INSERT OVERWRITE t VALUES (9, 1)");
    assert!(e2.contains("stale writer"), "实得 {e2}");
    // ③ 数据**没被动过**（拒绝发生在写段/换 manifest 之前）
    assert_eq!(count(&db_old, "SELECT id FROM t"), 1, "被拒的写不能动数据");
    assert_eq!(manifest_of(&db_old, "t").epoch, 5, "被拒的写不能改身份代");
    drop(db_old);

    // ④ 同代（= 同一代写者）与更高代（= 正当接管）都照常可写
    for (why, ep) in [("同代续写", 5u64), ("抬高接管", 6)] {
        let db = Qsdb::open(&root, opts(ep)).unwrap();
        db.sql("INSERT INTO t VALUES (100, 1)")
            .unwrap_or_else(|e| panic!("{why}（epoch={ep}）应当可写，实得 {e}"));
        assert_eq!(
            manifest_of(&db, "t").epoch,
            ep,
            "{why}：提交后盘上应更新为该代"
        );
    }
    // 第 6 代写下的数据在；且第 5 代**再也写不动**（身份只增不减）
    let db6 = Qsdb::open(&root, opts(6)).unwrap();
    assert_eq!(count(&db6, "SELECT id FROM t"), 3);
    drop(db6);
    let db5 = Qsdb::open(&root, opts(5)).unwrap();
    assert!(insert_err(&db5, "INSERT INTO t VALUES (7, 1)").contains("stale writer"));
    drop(db5);

    std::fs::remove_dir_all(&root).ok();
}

/// 默认 `epoch = 0`（不配置）时行为与旧版**完全一致**：旧库照写、不报错。
#[test]
fn default_epoch_zero_keeps_old_behavior() {
    let root = tmp("zero");
    {
        let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
        setup(&db);
        db.sql("INSERT INTO t VALUES (1, 1)").unwrap();
        db.sql("INSERT INTO t VALUES (2, 1)").unwrap();
    }
    // 另起句柄（默认 epoch）继续写：全 0 时代际校验恒通过
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    db.sql("INSERT INTO t VALUES (3, 1)").unwrap();
    assert_eq!(count(&db, "SELECT id FROM t"), 3);
    assert_eq!(
        manifest_of(&db, "t").epoch,
        0,
        "未启用身份代 → manifest 上是 0"
    );
    std::fs::remove_dir_all(&root).ok();
}
