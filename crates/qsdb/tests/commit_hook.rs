//! **提交事件钩子（T2）** 的验收。
//!
//! 设计见 `提交事件订阅设计稿` §1。这份清单里最要紧的不是"能收到事件"
//! （那是最容易写对的部分），而是**钩子坏掉时写入必须毫发无伤**：
//!
//! - 回调 **panic** ⇒ 写入**仍然返回成功**。反过来（让 panic 传播）是最坏的一种
//!   "静默改语义"：数据已经提交了，调用方却收到错误，于是它会去重试或回滚。
//! - `on_commit = None` ⇒ 走的是另一条分支（零开销那一条）。
//! - 事件是**分区级**的：一次跨 N 个分区的 append 要发 N 个事件（订阅方据此按分区
//!   记进度），而不是一个"表变了"的粗事件。

use std::sync::{Arc, Mutex};

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use qsdb::commit::{CommitEvent, CommitHook, CommitKind};
use qsdb::{Qsdb, QsdbOptions};

/// 测试目录唯一序号：pid 挡跨进程碰撞，序号挡同进程内并发（Windows 时钟
/// 粒度 ~100ns，纳秒后缀在同 tick 内会重复——CI 上真炸过，见 constraints）。
fn _uniq_seq() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    SEQ.fetch_add(1, Ordering::Relaxed)
}

fn setup(tag: &str, opts: QsdbOptions) -> (Qsdb, std::path::PathBuf) {
    let root = std::env::temp_dir().join("qsdb_hook").join(format!(
        "{tag}_{:x}_{}",
        std::process::id(),
        _uniq_seq()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let db = Qsdb::open(&root, opts).unwrap();
    (db, root)
}

fn batch(ids: &[i64], dts: &[&str]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("dt", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(arrow::array::StringArray::from(dts.to_vec())),
        ],
    )
    .unwrap()
}

fn collector() -> (CommitHook, Arc<Mutex<Vec<CommitEvent>>>) {
    let seen: Arc<Mutex<Vec<CommitEvent>>> = Arc::new(Mutex::new(Vec::new()));
    let s = Arc::clone(&seen);
    (
        CommitHook::new(move |ev: &CommitEvent| s.lock().unwrap().push(ev.clone())),
        seen,
    )
}

// ── ① 分区级事件：跨 N 个分区 ⇒ N 个事件 ─────────────────────────────────
#[test]
fn 逐分区发事件() {
    let (hook, seen) = collector();
    let (db, root) = setup(
        "perpart",
        QsdbOptions {
            on_commit: Some(hook),
            ..Default::default()
        },
    );
    db.sql("CREATE TABLE t (dt STRING, id BIGINT) PARTITIONED BY (dt)")
        .unwrap();
    db.append(
        "t",
        &[batch(
            &[1, 2, 3],
            &["2026-01-01", "2026-01-02", "2026-01-01"],
        )],
    )
    .unwrap();

    let evs = seen.lock().unwrap().clone();
    assert_eq!(evs.len(), 2, "两个分区 ⇒ 两个事件，实得 {evs:?}");
    for e in &evs {
        assert_eq!(e.table, "t");
        assert_eq!(e.kind, CommitKind::Append);
        assert_eq!(e.partition_values.len(), 1);
        assert!(e.generation > 0, "generation 要报**提交后**的值：{e:?}");
    }
    // 分区值确实带出来了（订阅方靠它定位）
    // ⚠️ 别拿 `Display` 去比（`Literal::Utf8` 的 display 是 **SQL 文本** `'2026-01-01'`，
    // 带引号）；直接比 `Literal` 本身
    let mut pv: Vec<String> = evs
        .iter()
        .map(|e| match &e.partition_values[0] {
            qsdb::types::Literal::Utf8(v) => v.clone(),
            other => panic!("分区值应是 Utf8，实得 {other:?}"),
        })
        .collect();
    pv.sort();
    assert_eq!(pv, vec!["2026-01-01", "2026-01-02"]);
    let _ = std::fs::remove_dir_all(&root);
}

// ── ② ★ 回调 panic **不能**把已提交的写变成错误 ──────────────────────────
//
// 这条是这批里最要紧的：数据**已经落盘提交**，此时返回错误 ⇒ 调用方重试/回滚
// ⇒ 一次静默改语义。所以回调的 panic 必须被吃掉，写入照常返回成功。
#[test]
fn 回调_panic_不影响写入() {
    let (db, root) = setup(
        "panic",
        QsdbOptions {
            on_commit: Some(CommitHook::new(|_ev| panic!("订阅方炸了"))),
            ..Default::default()
        },
    );
    db.sql("CREATE TABLE t (dt STRING, id BIGINT) PARTITIONED BY (dt)")
        .unwrap();
    let r = db.append("t", &[batch(&[1, 2], &["2026-01-01", "2026-01-01"])]);
    assert!(r.is_ok(), "回调 panic 不该影响写入结果：{r:?}");
    // 数据**真的**在（不是"报成功但其实没写"）
    let bs = db.sql_collect("SELECT count(*) FROM t").unwrap();
    assert_eq!(
        arrow::util::display::array_value_to_string(bs[0].column(0).as_ref(), 0).unwrap(),
        "2"
    );
    let _ = std::fs::remove_dir_all(&root);
}

// ── ③ `None`（默认）照常工作 ─────────────────────────────────────────────
#[test]
fn 默认没有钩子也照常() {
    let (db, root) = setup("none", QsdbOptions::default());
    db.sql("CREATE TABLE t (dt STRING, id BIGINT) PARTITIONED BY (dt)")
        .unwrap();
    assert!(db.append("t", &[batch(&[1], &["2026-01-01"])]).is_ok());
    assert!(db.sql("TRUNCATE TABLE t").is_ok());
    let _ = std::fs::remove_dir_all(&root);
}

// ── ④ overwrite / clear 各自发对应 kind ──────────────────────────────────
#[test]
fn overwrite_与_clear_的_kind() {
    let (hook, seen) = collector();
    let (db, root) = setup(
        "kinds",
        QsdbOptions {
            on_commit: Some(hook),
            ..Default::default()
        },
    );
    db.sql("CREATE TABLE t (dt STRING, id BIGINT) PARTITIONED BY (dt)")
        .unwrap();
    db.append("t", &[batch(&[1], &["2026-01-01"])]).unwrap();
    db.overwrite_partition(
        "t",
        &[("dt", qsdb::types::Literal::Utf8("2026-01-01".into()))],
        &[batch(&[9], &["2026-01-01"])],
    )
    .unwrap();
    db.sql("DELETE FROM t WHERE dt = '2026-01-01'").unwrap();

    let kinds: Vec<CommitKind> = seen.lock().unwrap().iter().map(|e| e.kind).collect();
    assert_eq!(
        kinds,
        vec![CommitKind::Append, CommitKind::Overwrite, CommitKind::Clear],
        "顺序也钉住（提交点先后就是它）"
    );
    let _ = std::fs::remove_dir_all(&root);
}
