//! M6-4 staging 写协议测试（feature `s3`）：写盘即 ACK、flush 补网络段、
//! 崩溃补传幂等（dedup 不重复计入）、overwrite 暂存语义、配置校验。
#![cfg(feature = "remote")]

use std::sync::Arc;

use arrow::array::{Date32Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use object_store::memory::InMemory;

use qsdb::store::Store;
use qsdb::{Qsdb, QsdbOptions};

fn tmp_root(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir()
        .join("qsdb_remote_staging_test")
        .join(format!("{tag}_{}", uuid::Uuid::now_v7().simple()));
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn opts_with_staging(staging: &std::path::Path) -> QsdbOptions {
    QsdbOptions {
        staging_dir: Some(staging.to_path_buf()),
        ..QsdbOptions::default()
    }
}

fn db_over(mem: Arc<InMemory>, root: &std::path::Path, staging: Option<&std::path::Path>) -> Qsdb {
    let store = Store::remote("s3://test-b/qsdb", mem, "qsdb");
    let opts = match staging {
        Some(s) => opts_with_staging(s),
        None => QsdbOptions::default(),
    };
    Qsdb::open_with_store(root, opts, store).unwrap()
}

fn batch(dt: i32, vals: Vec<i64>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("dt", DataType::Date32, true),
        Field::new("v", DataType::Int64, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Date32Array::from(vec![dt; vals.len()])),
            Arc::new(Int64Array::from(vals)),
        ],
    )
    .unwrap()
}

fn count(db: &Qsdb, sql: &str) -> i64 {
    db.sql_collect(sql)
        .unwrap()
        .iter()
        .flat_map(|b| {
            (0..b.num_rows()).map(move |i| {
                arrow::util::display::array_value_to_string(b.column(0).as_ref(), i)
                    .unwrap()
                    .parse::<i64>()
                    .unwrap()
            })
        })
        .sum()
}

#[test]
fn stage_ack_invisible_then_flush_visible() {
    let root = tmp_root("vis");
    let staging = root.join("_staging");
    let mem: Arc<InMemory> = Arc::new(InMemory::default());
    let db = db_over(mem.clone(), &root, Some(&staging));

    db.sql("CREATE TABLE t (dt DATE, v BIGINT) PARTITIONED BY (dt)")
        .unwrap();
    for i in 0..3 {
        db.append("t", &[batch(19_000, vec![i])]).unwrap();
    }
    // 写盘即 ACK：staged 数据对查询不可见（manifest 唯一真值）
    assert_eq!(count(&db, "SELECT count(*) FROM t"), 0);
    assert_eq!(db.staged_pending(), 3);

    let (uploaded, rows) = db.flush_staged().unwrap();
    assert_eq!((uploaded, rows), (3, 3));
    assert_eq!(count(&db, "SELECT count(*) FROM t"), 3);
    assert_eq!(db.staged_pending(), 0);

    // 再写再 flush（增量）
    db.append("t", &[batch(19_000, vec![100])]).unwrap();
    db.flush_staged().unwrap();
    assert_eq!(count(&db, "SELECT count(*) FROM t"), 4);
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn crash_recovery_replays_exactly_once() {
    let root = tmp_root("crash");
    let staging = root.join("_staging");
    let mem: Arc<InMemory> = Arc::new(InMemory::default());
    {
        let db = db_over(mem.clone(), &root, Some(&staging));
        db.sql("CREATE TABLE t (dt DATE, v BIGINT) PARTITIONED BY (dt)")
            .unwrap();
        db.append("t", &[batch(19_000, vec![1, 2])]).unwrap();
        db.append("t", &[batch(19_000, vec![3])]).unwrap();
        // 模拟崩溃：staged、未 flush，进程退出（drop 句柄）
    }
    assert!(staging.join("t/dt=2022-01-08").exists(), "staged 文件应在盘上");

    // 重启：启动后 flush（server 由定时器做；测试显式调用）
    let db2 = db_over(mem.clone(), &root, Some(&staging));
    let (uploaded, _) = db2.flush_staged().unwrap();
    assert_eq!(uploaded, 2);
    assert_eq!(count(&db2, "SELECT count(*) FROM t"), 3);
    assert_eq!(db2.staged_pending(), 0);

    // 二次 flush 幂等（无残留）
    assert_eq!(db2.flush_staged().unwrap().0, 0);
    assert_eq!(count(&db2, "SELECT count(*) FROM t"), 3);
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn dedup_prevents_double_commit_on_reprocess() {
    let root = tmp_root("dedup");
    let staging = root.join("_staging");
    let mem: Arc<InMemory> = Arc::new(InMemory::default());
    let db = db_over(mem.clone(), &root, Some(&staging));
    db.sql("CREATE TABLE t (dt DATE, v BIGINT) PARTITIONED BY (dt)")
        .unwrap();
    db.append("t", &[batch(19_000, vec![7])]).unwrap();

    // 备份 staged 对 → flush 上传删除 → 把备份放回（模拟「已提交但
    // 删除前崩溃」的残留）→ 再 flush：dedup 保证不重复计入
    let part = staging.join("t/dt=2022-01-08");
    let files: Vec<_> = std::fs::read_dir(&part).unwrap().flatten().collect();
    assert_eq!(files.len(), 2);
    let backup: Vec<(std::path::PathBuf, Vec<u8>)> = files
        .iter()
        .map(|f| {
            let p = f.path();
            (p.clone(), std::fs::read(&p).unwrap())
        })
        .collect();
    db.flush_staged().unwrap();
    assert_eq!(count(&db, "SELECT count(*) FROM t"), 1);

    // flush 删除对时顺带清了空分区目录——恢复前重建
    std::fs::create_dir_all(&part).unwrap();
    for (p, bytes) in &backup {
        std::fs::write(p, bytes).unwrap();
    }
    let (uploaded, _) = db.flush_staged().unwrap();
    assert_eq!(uploaded, 1, "残留对会被重放处理");
    assert_eq!(
        count(&db, "SELECT count(*) FROM t"),
        1,
        "同名 segment 去重——不重复计入"
    );
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn staged_overwrite_replaces_partition() {
    let root = tmp_root("ow");
    let staging = root.join("_staging");
    let mem: Arc<InMemory> = Arc::new(InMemory::default());
    let db = db_over(mem.clone(), &root, Some(&staging));
    db.sql("CREATE TABLE t (dt DATE, v BIGINT) PARTITIONED BY (dt)")
        .unwrap();
    // 先直接（无 staging 句柄）写一批：用另一个同后端实例直写
    let direct = db_over(mem.clone(), &root, None);
    direct
        .append("t", &[batch(19_000, vec![1, 2, 3])])
        .unwrap();
    assert_eq!(count(&db, "SELECT count(*) FROM t"), 3);

    // staged overwrite → flush 后分区被替换
    db.overwrite_partition(
        "t",
        &[("dt", qsdb::Literal::Date(19_000))],
        &[batch(19_000, vec![9, 8])],
    )
    .unwrap();
    assert_eq!(db.staged_pending(), 1);
    assert_eq!(count(&db, "SELECT count(*) FROM t"), 3, "flush 前旧值可见");
    db.flush_staged().unwrap();
    assert_eq!(count(&db, "SELECT count(*) FROM t"), 2);
    let rows = db.sql_collect("SELECT v FROM t ORDER BY v").unwrap();
    let got: Vec<String> = rows
        .iter()
        .flat_map(|b| {
            (0..b.num_rows()).map(|i| {
                arrow::util::display::array_value_to_string(b.column(0).as_ref(), i).unwrap()
            })
        })
        .collect();
    assert_eq!(got, vec!["8".to_string(), "9".to_string()]);
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn staging_requires_remote_backend() {
    let root = tmp_root("cfg");
    let opts = QsdbOptions {
        staging_dir: Some(root.join("_staging")),
        ..QsdbOptions::default() // Local 后端
    };
    let err = match Qsdb::open(&root, opts) {
        Err(e) => e,
        Ok(_) => panic!("staging + local store should fail"),
    };
    assert!(
        err.to_string().contains("remote"),
        "error should mention remote: {err}"
    );
    std::fs::remove_dir_all(&root).ok();
}
