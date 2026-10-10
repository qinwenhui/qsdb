//! 配置体系测试：运行期热更生效（无需重开句柄）。

use std::sync::Arc;
use std::time::Duration;

use qsdb::query::QueryContext;
use qsdb::{Qsdb, QsdbError, QsdbOptions};

fn tmp_root(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join("qsdb_cfg_test").join(format!(
        "{tag}_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&d).unwrap();
    d
}

#[test]
fn runtime_timeout_hot_applies_to_next_query() {
    let root = tmp_root("hot");
    let db = Arc::new(Qsdb::open(&root, QsdbOptions::default()).unwrap());
    db.sql("CREATE TABLE t (dt DATE, v BIGINT) PARTITIONED BY (dt)")
        .unwrap();
    db.sql("INSERT INTO t VALUES (DATE '2026-01-01', 1), (DATE '2026-01-02', 2)")
        .unwrap();

    // 初始：不超时
    assert!(db.runtime().query_timeout_ms().is_none());
    // 热更：1ms 超时
    db.runtime().set_query_timeout_ms(1);
    assert_eq!(db.runtime().query_timeout_ms(), Some(1));
    // 等 deadline 过期后，默认（无显式覆盖）查询超时
    std::thread::sleep(Duration::from_millis(3));
    let err = match db.sql("SELECT v FROM t") {
        Err(e) => e,
        Ok(_) => panic!("expected timeout after hot config"),
    };
    assert!(matches!(err, QsdbError::QueryTimeout(_)), "{err}");
    // 显式覆盖优先于运行期默认
    let ctx = QueryContext::new("explicit", None, None);
    assert!(db.sql_with_context("SELECT v FROM t", &ctx).is_ok());
    // 关回默认
    db.runtime().set_query_timeout_ms(0);
    assert!(db.sql("SELECT v FROM t").is_ok());
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn runtime_memory_limit_hot() {
    let root = tmp_root("mem");
    let db = Arc::new(Qsdb::open(&root, QsdbOptions::default()).unwrap());
    db.sql("CREATE TABLE t (dt DATE, v BIGINT) PARTITIONED BY (dt)")
        .unwrap();
    db.sql("INSERT INTO t VALUES (DATE '2026-01-01', 1), (DATE '2026-01-02', 2), (DATE '2026-01-03', 3)").unwrap();
    db.runtime().set_query_memory_limit_bytes(1); // 1 字节：任何物化即超
    let err = match db.sql("SELECT v FROM t ORDER BY v") {
        Err(e) => e,
        Ok(_) => panic!("expected memory limit"),
    };
    assert!(matches!(err, QsdbError::QueryMemoryLimit(_)), "{err}");
    db.runtime().set_query_memory_limit_bytes(0); // 关
    assert!(db.sql("SELECT v FROM t ORDER BY v").is_ok());
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn startup_opts_are_initial_values_only() {
    // opts 中的 query_timeout_ms 只是初值：runtime setter 可覆盖
    let root = tmp_root("init");
    let db = Arc::new(
        Qsdb::open(
            &root,
            QsdbOptions {
                query_timeout_ms: Some(999),
                ..Default::default()
            },
        )
        .unwrap(),
    );
    assert_eq!(db.runtime().query_timeout_ms(), Some(999));
    db.runtime().set_query_timeout_ms(0);
    assert_eq!(db.runtime().query_timeout_ms(), None);
    // 启动期快照不被热更污染
    assert_eq!(db.options().query_timeout_ms, Some(999));
    std::fs::remove_dir_all(&root).ok();
}
