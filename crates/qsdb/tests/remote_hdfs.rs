//! M6-5 HDFS 后端真实集群 e2e（feature `hdfs`；环境变量 `QSDB_HDFS_URL`
//! 指向 NameNode 时启用，未设置则整文件跳过——CI 无集群不跑）。
//!
//! 验证（存储设计 §7.3/§15 矩阵的 HDFS 列）：
//! - LockingStore CAS 模拟：put_if_absent / get_with_etag（合成 etag）/
//!   put_if_etag 冲突拒写
//! - 管理表全生命周期：建表/append/scan/overwrite/TRUNCATE/drop/引用 GC
//! - staging 写：写盘即 ACK → flush 补传可见
//!
//! 运行（用户身份走 HADOOP_USER_NAME；路径用 /tmp 下独立前缀）：
//! ```text
//! QSDB_HDFS_URL=hdfs://192.168.1.12:9000 HADOOP_USER_NAME=hadoop \
//!   cargo test -p qsdb --features hdfs --test remote_hdfs -- --nocapture
//! ```
#![cfg(feature = "hdfs")]

use std::sync::Arc;

use arrow::array::{Date32Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use qsdb::store::Store;
use qsdb::{Qsdb, QsdbOptions};

fn hdfs_url() -> Option<String> {
    std::env::var("QSDB_HDFS_URL")
        .ok()
        .filter(|s| !s.is_empty())
}

/// 每个用例独立前缀（uuid），结束清理。
fn fresh_prefix(tag: &str) -> String {
    format!("/tmp/qsdb-m6-5/{tag}-{}", uuid::Uuid::now_v7().simple())
}

fn db_at(prefix: &str, local_root: &std::path::Path) -> Qsdb {
    let url = hdfs_url().expect("QSDB_HDFS_URL");
    let os = qsdb::hdfs_backend::build_hdfs_store(&url).unwrap();
    let vroot = std::path::PathBuf::from(format!("{url}{prefix}"));
    let store = Store::remote(vroot, os, prefix.trim_start_matches('/').to_string());
    Qsdb::open_with_store(local_root, QsdbOptions::default(), store).unwrap()
}

fn drop_prefix(prefix: &str, db: &Qsdb) {
    // 前缀整体删除（对象层；drop_table 只清单表）
    for obj in db.store().list_all(std::path::Path::new("/")).unwrap() {
        let _ = db.store().delete_object(&db.store().root().join(obj.key));
    }
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

fn local_root(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir()
        .join("qsdb_hdfs_test")
        .join(format!("{tag}_{}", uuid::Uuid::now_v7().simple()));
    std::fs::create_dir_all(&d).unwrap();
    d
}

#[test]
fn hdfs_cas_primitives() {
    let Some(_) = hdfs_url() else {
        eprintln!("QSDB_HDFS_URL not set — skip");
        return;
    };
    let prefix = fresh_prefix("cas");
    let root = local_root("cas");
    let db = db_at(&prefix, &root);
    let store = db.store();
    let p = store.root().join("cas-test.bin");

    // put_if_absent
    assert!(store.put_if_etag(&p, b"v1", None).unwrap());
    assert!(!store.put_if_etag(&p, b"x", None).unwrap(), "重复创建拒绝");
    // etag 读 + 条件更新
    let (bytes, etag) = store.get_with_etag(&p).unwrap().unwrap();
    assert_eq!(bytes, b"v1");
    assert!(
        !store
            .put_if_etag(&p, b"bad", Some("\"stale-etag\""))
            .unwrap(),
        "错 etag 拒写"
    );
    assert!(store.put_if_etag(&p, b"v2", Some(&etag)).unwrap());
    assert_eq!(store.read(&p).unwrap().unwrap(), b"v2");
    // 锁对象已清理
    let lock = store.root().join("cas-test.bin.__cas_lock");
    assert!(store.read(&lock).unwrap().is_none(), "CAS 锁应已释放");
    drop_prefix(&prefix, &db);
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn hdfs_managed_lifecycle() {
    let Some(_) = hdfs_url() else {
        eprintln!("QSDB_HDFS_URL not set — skip");
        return;
    };
    let prefix = fresh_prefix("life");
    let root = local_root("life");
    let db = db_at(&prefix, &root);

    db.sql("CREATE TABLE t (dt DATE, v BIGINT) PARTITIONED BY (dt)")
        .unwrap();
    for i in 0..3 {
        db.append("t", &[batch(19_000, vec![i])]).unwrap();
    }
    assert_eq!(count(&db, "SELECT count(*) FROM t"), 3);
    assert_eq!(count(&db, "SELECT count(*) FROM t WHERE v >= 1"), 2);

    // overwrite（CAS 模拟提交）
    db.overwrite_partition(
        "t",
        &[("dt", qsdb::Literal::Date(19_000))],
        &[batch(19_000, vec![7, 8])],
    )
    .unwrap();
    assert_eq!(count(&db, "SELECT count(*) FROM t"), 2);

    // TRUNCATE + 引用 GC 清孤儿
    assert!(db.sql("TRUNCATE TABLE t").is_ok());
    assert_eq!(count(&db, "SELECT count(*) FROM t"), 0);
    let opts = QsdbOptions {
        trash_grace_ms: 0,
        ..QsdbOptions::default()
    };
    let db2 = {
        let url = hdfs_url().unwrap();
        let os = qsdb::hdfs_backend::build_hdfs_store(&url).unwrap();
        let vroot = std::path::PathBuf::from(format!("{url}{prefix}"));
        let store = Store::remote(vroot, os, prefix.trim_start_matches('/').to_string());
        Qsdb::open_with_store(&root, opts, store).unwrap()
    };
    let (_, freed) = db2.gc().unwrap();
    assert!(freed > 0, "孤儿 segment 被引用 GC 回收: freed={freed}");

    // drop 清空
    db.drop_table("t").unwrap();
    assert!(db.tables().unwrap().is_empty());
    drop_prefix(&prefix, &db);
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn hdfs_staging_write() {
    let Some(_) = hdfs_url() else {
        eprintln!("QSDB_HDFS_URL not set — skip");
        return;
    };
    let prefix = fresh_prefix("stg");
    let root = local_root("stg");
    let staging = root.join("_staging");
    let url = hdfs_url().unwrap();
    let os = qsdb::hdfs_backend::build_hdfs_store(&url).unwrap();
    let vroot = std::path::PathBuf::from(format!("{url}{prefix}"));
    let store = Store::remote(vroot, os, prefix.trim_start_matches('/').to_string());
    let opts = QsdbOptions {
        staging_dir: Some(staging),
        ..QsdbOptions::default()
    };
    let db = Qsdb::open_with_store(&root, opts, store).unwrap();

    db.sql("CREATE TABLE t (dt DATE, v BIGINT) PARTITIONED BY (dt)")
        .unwrap();
    db.append("t", &[batch(19_000, vec![1, 2])]).unwrap();
    db.append("t", &[batch(19_000, vec![3])]).unwrap();
    assert_eq!(count(&db, "SELECT count(*) FROM t"), 0, "staged 不可见");
    let (uploaded, rows) = db.flush_staged().unwrap();
    assert_eq!((uploaded, rows), (2, 3));
    assert_eq!(count(&db, "SELECT count(*) FROM t"), 3);
    drop_prefix(&prefix, &db);
    std::fs::remove_dir_all(&root).ok();
}
