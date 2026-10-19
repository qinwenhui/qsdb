//! **F24 回归**：同分区并发 overwrite 的 CAS（乐观并发）选项。
//!
//! 事故语义（qsdb-sync F24）：两个写者各发一次 `overwrite_partition` 覆盖同一分区——
//! 分区锁保证"每次替换原子、清单完整"（**不坏数据**），但语义上是**静默
//! last-writer-wins**：先写者整批数据无声消失，且无任何告警。Delta/Iceberg 对同分区
//! 并发 overwrite 都是**冲突报错让调用方裁决**，不是静默覆盖。
//!
//! 本文件锁死三件事：
//! 1. 传 `expected_generation` 且分区已被他人改写 → `WriteConflict`，**且先写者的数据
//!    仍在**（这是本条的全部意义：不许静默丢数据）；
//! 2. 传对了 generation → 正常覆盖；不存在的分区传 `0` 合法（首次写入）；
//! 3. **不传** = 现状行为（无条件覆盖）——向后兼容，别把既有调用方搞挂。
//!
//! generation 口径：分区提交计数器，`PartitionManifest::commit` 每次 +1
//! （append / overwrite / clear 都算），从 `Qsdb::partition_stats()` 读。

use std::sync::Arc;

use arrow::array::{Date32Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use qsdb::error::QsdbError;
use qsdb::{Literal, Qsdb, QsdbOptions};

/// 测试目录唯一序号：pid 挡跨进程碰撞，序号挡同进程内并发（Windows 时钟
/// 粒度 ~100ns，纳秒后缀在同 tick 内会重复——CI 上真炸过，见 constraints）。
fn _uniq_seq() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    SEQ.fetch_add(1, Ordering::Relaxed)
}

fn tmp_root(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir()
        .join("qsdb_write_cas_test")
        .join(format!("{tag}_{:x}_{}", std::process::id(), _uniq_seq()));
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn db_at(root: &std::path::Path) -> Qsdb {
    let db = Qsdb::open(root, QsdbOptions::default()).unwrap();
    db.sql("CREATE TABLE t (dt DATE, v BIGINT) PARTITIONED BY (dt)")
        .unwrap();
    db
}

/// 全列批（含分区列）—— `overwrite_partition` / `append` 的输入约定：
/// 库里按分区列路由并剥离，调用方给"整行"。
fn data(vals: Vec<i64>) -> RecordBatch {
    let n = vals.len();
    let schema = Arc::new(Schema::new(vec![
        Field::new("dt", DataType::Date32, true),
        Field::new("v", DataType::Int64, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Date32Array::from(vec![D1; n])),
            Arc::new(Int64Array::from(vals)),
        ],
    )
    .unwrap()
}

const D1: i32 = 20_454; // 2026-01-01

fn pv(d: i32) -> Vec<(&'static str, Literal)> {
    vec![("dt", Literal::Date(d))]
}

/// 分区当前 generation（不存在 → None）。
fn gen_of(db: &Qsdb, d: i32) -> Option<u64> {
    db.partition_stats("t")
        .unwrap()
        .into_iter()
        .find(|s| s.name.contains(&format!("dt={}", fmt_date(d))))
        .map(|s| s.generation)
}

fn fmt_date(d: i32) -> String {
    let epoch = chrono_like(d);
    format!("{:04}-{:02}-{:02}", epoch.0, epoch.1, epoch.2)
}

/// 日期 → (y, m, d)（简单民用历换算，够测试用）
fn chrono_like(days: i32) -> (i32, u32, u32) {
    let mut days = days;
    let mut y = 1970i32;
    loop {
        let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
        let len = if leap { 366 } else { 365 };
        if days < len {
            break;
        }
        days -= len;
        y += 1;
    }
    let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
    let ml = [
        31,
        if leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    let mut m = 0usize;
    while days >= ml[m] {
        days -= ml[m];
        m += 1;
    }
    (y, (m + 1) as u32, (days + 1) as u32)
}

fn rows_of(db: &Qsdb, sql: &str) -> Vec<String> {
    let batches = db.sql_collect(sql).unwrap();
    let mut out = Vec::new();
    for b in &batches {
        for i in 0..b.num_rows() {
            out.push(arrow::util::display::array_value_to_string(b.column(0).as_ref(), i).unwrap());
        }
    }
    out
}

/// 核心用例：**后写者用过期 generation → 冲突报错，先写者的数据完好**。
#[test]
fn stale_cas_write_is_rejected_and_earlier_data_survives() {
    let root = tmp_root("stale");
    let db = db_at(&root);

    // 写者 A：首次覆盖（分区不存在 → generation 概念上为 0）
    db.overwrite_partition_cas("t", &pv(D1), &[data(vec![1, 2, 3])], 0)
        .expect("首次写入（期望 0）应成功");
    let g1 = gen_of(&db, D1).expect("分区应存在");
    assert_eq!(g1, 1, "commit 一次 → generation 1");

    // 写者 B 拿着**过期的** generation（0）来覆盖 → 必须冲突
    let err = db
        .overwrite_partition_cas("t", &pv(D1), &[data(vec![99])], 0)
        .expect_err("过期 generation 必须冲突");
    match err {
        QsdbError::WriteConflict {
            expected, actual, ..
        } => {
            assert_eq!(expected, 0);
            assert_eq!(actual, 1, "报告实际 generation，便于调用方重读");
        }
        other => panic!("应为 WriteConflict，实得 {other:?}"),
    }

    // ★ 本条的全部意义：A 的数据**还在**（不是被 99 无声替换）
    assert_eq!(
        rows_of(&db, "SELECT v FROM t ORDER BY v"),
        vec!["1", "2", "3"]
    );
    assert_eq!(
        gen_of(&db, D1),
        Some(1),
        "冲突不改变分区状态（generation 不动）"
    );

    // 重读最新 generation 再写 → 成功
    let g = gen_of(&db, D1).unwrap();
    db.overwrite_partition_cas("t", &pv(D1), &[data(vec![99])], g)
        .expect("拿最新 generation 应成功");
    assert_eq!(rows_of(&db, "SELECT v FROM t"), vec!["99"]);
    assert_eq!(gen_of(&db, D1), Some(2));

    let _ = std::fs::remove_dir_all(&root);
}

/// 不传参数 = 现状行为（无条件覆盖）——向后兼容。
#[test]
fn without_cas_the_old_silent_overwrite_behaviour_is_unchanged() {
    let root = tmp_root("nocas");
    let db = db_at(&root);

    db.overwrite_partition("t", &pv(D1), &[data(vec![1, 2, 3])])
        .unwrap();
    // 第二个写者不带 CAS → 照旧覆盖（本仓既有语义，qsdb-sync 管道 sink 依赖它）
    db.overwrite_partition("t", &pv(D1), &[data(vec![7])])
        .unwrap();
    assert_eq!(rows_of(&db, "SELECT v FROM t"), vec!["7"]);
    assert_eq!(gen_of(&db, D1), Some(2));

    let _ = std::fs::remove_dir_all(&root);
}

/// append 也推进 generation —— 与"清空/覆盖后 CAS 用的就是最新值"一致
/// （写者若在此期间追加过，覆盖者的期望值自然过期 → 冲突，正是想要的语义）。
#[test]
fn append_bumps_generation_so_stale_overwriter_conflicts() {
    let root = tmp_root("append");
    let db = db_at(&root);

    db.overwrite_partition_cas("t", &pv(D1), &[data(vec![1])], 0)
        .unwrap();
    let g = gen_of(&db, D1).unwrap();

    // 另一写者追加（锁内 read-merge-write）
    db.append("t", &[data(vec![2])]).unwrap();

    // 拿着 append 之前的 generation 覆盖 → 冲突
    let err = db
        .overwrite_partition_cas("t", &pv(D1), &[data(vec![9])], g)
        .expect_err("append 之后旧 generation 必须冲突");
    assert!(
        matches!(err, QsdbError::WriteConflict { .. }),
        "实得 {err:?}"
    );
    assert_eq!(rows_of(&db, "SELECT v FROM t ORDER BY v"), vec!["1", "2"]);

    let _ = std::fs::remove_dir_all(&root);
}

/// 边界：对**不存在**的分区传非 0 → 冲突（它被删过/从没存在到那个代数）；
/// 传 0 → 成功（首次写入）。
#[test]
fn expected_zero_means_first_write() {
    let root = tmp_root("zero");
    let db = db_at(&root);

    let err = db
        .overwrite_partition_cas("t", &pv(D1), &[data(vec![1])], 5)
        .expect_err("不存在分区传 5 应冲突");
    assert!(
        matches!(err, QsdbError::WriteConflict { actual: 0, .. }),
        "实得 {err:?}"
    );

    db.overwrite_partition_cas("t", &pv(D1), &[data(vec![1])], 0)
        .unwrap();
    assert_eq!(gen_of(&db, D1), Some(1));

    let _ = std::fs::remove_dir_all(&root);
}
