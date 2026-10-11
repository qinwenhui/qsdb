//! M6-3 S3 管理表协议测试（feature `s3`）。
//!
//! 用 object_store InMemory 作为注入后端（`Qsdb::open_with_store`）——
//! 单对象原子 PUT / ETag 条件写（PutMode::Create/Update）/ list 语义与
//! S3 一致，协议全链路无网络验证（存储设计 §15 故障矩阵的进程内子集）。
#![cfg(feature = "remote")]

use std::sync::Arc;

use arrow::array::{Date32Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use object_store::memory::InMemory;
use object_store::path::Path as StorePath;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload, UpdateVersion};

use qsdb::store::Store;
use qsdb::{Qsdb, QsdbOptions};

fn tmp_root(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir()
        .join("qsdb_remote_managed_test")
        .join(format!("{tag}_{}", uuid::Uuid::now_v7().simple()));
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn db_over(mem: Arc<InMemory>, local_root: &std::path::Path) -> Qsdb {
    let store = Store::remote("s3://test-b/qsdb", mem, "qsdb");
    Qsdb::open_with_store(local_root, QsdbOptions::default(), store).unwrap()
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

fn block_on<F: std::future::Future>(f: F) -> F::Output {
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(f)
}

fn count(db: &Qsdb, sql: &str) -> i64 {
    let batches = db.sql_collect(sql).unwrap();
    batches
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

// --- OCC 原语（Store 层）----------------------------------------------------

#[test]
fn occ_primitives_create_update_conflict() {
    let mem: Arc<InMemory> = Arc::new(InMemory::default());
    let store = Store::remote("s3://b/p", mem.clone(), "p");
    let p = std::path::Path::new("s3://b/p/m.bin");
    // 不存在 → put_if_absent 成功
    assert!(store.put_if_etag(p, b"v1", None).unwrap());
    // 再 create → 冲突 false
    assert!(!store.put_if_etag(p, b"x", None).unwrap());
    // 读回 (bytes, etag)
    let (bytes, etag) = store.get_with_etag(p).unwrap().unwrap();
    assert_eq!(bytes, b"v1");
    // 错 etag → false；对 etag → true
    assert!(!store
        .put_if_etag(p, b"v2", Some("\"wrong-etag\""))
        .unwrap());
    assert!(store.put_if_etag(p, b"v2", Some(&etag)).unwrap());
    assert_eq!(store.read(p).unwrap().unwrap(), b"v2");
}

// --- OCC 冲突重放（协议正确性核心：并发写者不丢） ---------------------------

#[test]
fn occ_commit_replays_on_conflict_no_loss() {
    use qsdb::manifest::{PartitionManifest, SegmentEntry};
    let mem: Arc<InMemory> = Arc::new(InMemory::default());
    let store = Store::remote("s3://b/qsdb", mem.clone(), "qsdb");
    let dir = std::path::Path::new("s3://b/qsdb/t/dt=1");
    let mpath = PartitionManifest::manifest_path(dir);

    let mk = |file: &str| SegmentEntry {
        file_name: file.into(),
        row_count: 1,
        byte_size: 10,
        created_at: 0,
        column_stats: vec![],
    };
    // 初始 manifest：他人已提交 seg_a
    let mut m0 = PartitionManifest::new(vec![], 7);
    m0.segments.push(mk("seg_a.parquet"));
    let b0 = postcard::to_allocvec(&m0).unwrap();
    block_on(mem.put(
        &StorePath::from("qsdb/t/dt=1/manifest.postcard"),
        PutPayload::from(b0),
    ))
    .unwrap();

    // 我方提交 seg_b；首轮 build 期间模拟并发写者抢先提交 seg_c
    let entry_b = mk("seg_b.parquet");
    let mem_clone = mem.clone();
    let mut injected = false;
    qsdb::remote::occ_commit(&store, &mpath, |cur| {
        if !injected {
            injected = true;
            // 并发写者：基于同一 cur 抢先提交 seg_c（直接 PUT 覆盖 etag）
            let mut m = cur.clone().unwrap();
            m.segments.push(mk("seg_c.parquet"));
            let bytes = postcard::to_allocvec(&m).unwrap();
            block_on(mem_clone.put(
                &StorePath::from("qsdb/t/dt=1/manifest.postcard"),
                PutPayload::from(bytes),
            ))
            .unwrap();
        }
        let mut next = cur.unwrap_or_else(|| PartitionManifest::new(vec![], 7));
        next.segments.push(entry_b.clone());
        Ok(next)
    })
    .unwrap();

    // 重放后三方 segment 全在（a + c + b），无一丢失
    let back = PartitionManifest::read(&store, dir).unwrap().unwrap();
    let names: Vec<&str> = back.segments.iter().map(|s| s.file_name.as_str()).collect();
    assert!(names.contains(&"seg_a.parquet"));
    assert!(names.contains(&"seg_b.parquet"));
    assert!(names.contains(&"seg_c.parquet"), "并发写者的 segment 不得丢失");
    assert!(back.writer_epoch != 0, "远端提交写入 writer_epoch");
}

// --- 全生命周期 e2e ----------------------------------------------------------

#[test]
fn managed_lifecycle_over_inmemory() {
    let root = tmp_root("e2e");
    let mem: Arc<InMemory> = Arc::new(InMemory::default());
    let db = db_over(mem.clone(), &root);

    db.sql("CREATE TABLE t (dt DATE, v BIGINT) PARTITIONED BY (dt)")
        .unwrap();
    for i in 0..5 {
        db.append("t", &[batch(19_000, vec![i, i + 1])]).unwrap();
    }
    assert_eq!(count(&db, "SELECT count(*) FROM t"), 10);
    // 值集 {0,1,1,2,2,3,3,4,4,5}（i, i+1 for i in 0..5）：v >= 4 → 4,4,5 三行
    assert_eq!(count(&db, "SELECT count(*) FROM t WHERE v >= 4"), 3);

    // overwrite（远端 = OCC 替换）
    db.overwrite_partition("t", &[("dt", qsdb::Literal::Date(19_000))], &[batch(19_000, vec![100, 200])])
        .unwrap();
    assert_eq!(count(&db, "SELECT count(*) FROM t"), 2);

    // compaction（远端：读 → 内存合并 → PUT → OCC）
    for i in 0..4 {
        db.append("t", &[batch(19_000, vec![i])]).unwrap();
    }
    let def = db.table_def("t").unwrap();
    let tdir = db.store().root().join("default").join("t");
    let r = qsdb::compact::compact_table(
        db.store(),
        None,
        &tdir,
        &def,
        &qsdb::compact::CompactOptions {
            min_segments: 2,
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(r.partitions_compacted, 1);
    assert_eq!(count(&db, "SELECT count(*) FROM t"), 6);

    // TRUNCATE（远端清空 = OCC 空替换）
    assert!(db.sql("TRUNCATE TABLE t").is_ok());
    assert_eq!(count(&db, "SELECT count(*) FROM t"), 0);

    // stats（list 求和）
    let (_, bytes) = db.stats().unwrap();
    assert!(bytes > 0, "远端 stats = 对象字节求和");

    // drop：清单去名 + 前缀删除
    db.drop_table("t").unwrap();
    assert!(db.tables().unwrap().is_empty());
    let left = db.store().list_all(&tdir).unwrap();
    assert!(left.is_empty(), "drop 后前缀应清空，剩 {left:?}");

    std::fs::remove_dir_all(&root).ok();
}

// --- 崩溃窗口（§15）：孤儿 segment 不可见，GC 安全期后回收 -------------------

#[test]
fn orphan_segment_invisible_and_gc_reclaims() {
    let root = tmp_root("orphan");
    let mem: Arc<InMemory> = Arc::new(InMemory::default());
    let db = db_over(mem.clone(), &root);
    db.sql("CREATE TABLE t (dt DATE, v BIGINT) PARTITIONED BY (dt)")
        .unwrap();
    db.append("t", &[batch(19_000, vec![1, 2])]).unwrap();
    assert_eq!(count(&db, "SELECT count(*) FROM t"), 2);

    // 模拟「segment 已 PUT、manifest 未提交」：直接塞一个不受引用的 seg 对象
    let orphan_key = StorePath::from("qsdb/default/t/dt=2022-01-09/seg_deadbeef.parquet");
    block_on(mem.put(&orphan_key, PutPayload::from(vec![1u8, 2, 3]))).unwrap();

    // 不可见（manifest 是唯一真值）
    assert_eq!(count(&db, "SELECT count(*) FROM t"), 2);

    // 安全期 0 → gc 引用扫描删除孤儿
    let opts = QsdbOptions {
        trash_grace_ms: 0,
        ..QsdbOptions::default()
    };
    let db2 = {
        let store = Store::remote("s3://test-b/qsdb", mem.clone(), "qsdb");
        Qsdb::open_with_store(&root, opts, store).unwrap()
    };
    let (_, freed) = db2.gc().unwrap();
    assert!(freed >= 3, "孤儿对象被回收: freed={freed}");
    assert_eq!(count(&db2, "SELECT count(*) FROM t"), 2); // 活数据无损

    std::fs::remove_dir_all(&root).ok();
}

// --- 多实例并发写（同一 InMemory = 模拟多进程同 bucket） ----------------------

#[test]
fn two_instances_concurrent_append_no_loss() {
    let root = tmp_root("multi");
    let mem: Arc<InMemory> = Arc::new(InMemory::default());
    let db1 = db_over(mem.clone(), &root);
    let db2 = db_over(mem.clone(), &root);
    db1.sql("CREATE TABLE t (dt DATE, v BIGINT) PARTITIONED BY (dt)")
        .unwrap();

    let h1 = std::thread::spawn(move || {
        for i in 0..8 {
            db1.append("t", &[batch(19_000, vec![i])]).unwrap();
        }
    });
    let h2 = std::thread::spawn(move || {
        for i in 0..8 {
            db2.append("t", &[batch(19_000, vec![100 + i])])
                .unwrap();
        }
    });
    h1.join().unwrap();
    h2.join().unwrap();

    assert_eq!(count(&db_over(mem.clone(), &root), "SELECT count(*) FROM t"), 16);
    std::fs::remove_dir_all(&root).ok();
}

// --- 保留原语（编译期锚定 object_store 条件写 API 形状） ---------------------

#[test]
fn inmemory_supports_conditional_put() {
    let mem: Arc<InMemory> = Arc::new(InMemory::default());
    let key = StorePath::from("probe");
    block_on(mem.put(&key, PutPayload::from(vec![1u8]))).unwrap();
    // Create on existing → AlreadyExists
    let err = block_on(mem.put_opts(
        &key,
        PutPayload::from(vec![2u8]),
        PutOptions {
            mode: PutMode::Create,
            ..Default::default()
        },
    ))
    .unwrap_err();
    assert!(matches!(err, object_store::Error::AlreadyExists { .. }));
    // Update with wrong etag → Precondition
    let err = block_on(mem.put_opts(
        &key,
        PutPayload::from(vec![2u8]),
        PutOptions {
            mode: PutMode::Update(UpdateVersion {
                e_tag: Some("\"nope\"".into()),
                version: None,
            }),
            ..Default::default()
        },
    ))
    .unwrap_err();
    assert!(matches!(err, object_store::Error::Precondition { .. }));
}
