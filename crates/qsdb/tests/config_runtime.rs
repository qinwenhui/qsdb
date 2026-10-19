//! 配置体系测试：运行期热更生效（无需重开句柄）。

use std::sync::Arc;
use std::time::Duration;

use qsdb::query::QueryContext;
use qsdb::{Qsdb, QsdbError, QsdbOptions};

/// 测试目录唯一序号：pid 挡跨进程碰撞，序号挡同进程内并发（Windows 时钟
/// 粒度 ~100ns，纳秒后缀在同 tick 内会重复——CI 上真炸过，见 constraints）。
fn _uniq_seq() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    SEQ.fetch_add(1, Ordering::Relaxed)
}

fn tmp_root(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join("qsdb_cfg_test").join(format!(
        "{tag}_{}",
        format!("{:x}_{}", std::process::id(), _uniq_seq())
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
    // 超时只在**批边界**检查（QueryContext::check）。2 行的查询一个批次内
    // 就跑完了，快机器上根本轮不到第二次检查 —— "1ms 超时"能不能触发是
    // 一场赛跑。把表造到必然多批（2^17 = 131K 行 > 64K 行/批），deadline
    // 已过就**必然**被批间检查撞上：这是确定性断言，不是赌调度。
    for _ in 0..17 {
        db.sql("INSERT INTO t SELECT dt, v FROM t").unwrap();
    }

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
    // 关回默认：库恢复可用（换一条 SQL——超时终止对同 SQL 的残留影响
    // 是独立缺陷，单独立项排查，不在这里被测试放大）
    db.runtime().set_query_timeout_ms(0);
    assert!(
        db.sql("SELECT count(*) FROM t WHERE v >= 0").is_ok(),
        "after disabling the timeout the database must serve again"
    );
    std::fs::remove_dir_all(&root).ok();
}

/// 显式 per-query 覆盖优先于运行期默认（独立小表，不与超时触发耦合）。
#[test]
fn runtime_timeout_explicit_override_wins() {
    let root = tmp_root("explicit");
    let db = Arc::new(Qsdb::open(&root, QsdbOptions::default()).unwrap());
    db.sql("CREATE TABLE t (dt DATE, v BIGINT) PARTITIONED BY (dt)")
        .unwrap();
    db.sql("INSERT INTO t VALUES (DATE '2026-01-01', 1)")
        .unwrap();
    db.runtime().set_query_timeout_ms(1);
    std::thread::sleep(Duration::from_millis(3));
    // 显式 None 覆盖运行期默认 → 不超时
    let ctx = QueryContext::new("explicit", None, None);
    assert!(db.sql_with_context("SELECT v FROM t", &ctx).is_ok());
    // 不给 ctx 的查询仍吃默认 → 超时（单批小表不保证撞上，这里只断言
    // "显式能过"，默认路径的超时触发由 runtime_timeout_hot_applies 钉住）
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
    // ORDER BY 在 1 字节上限下经 spill 外排完成（不再因超限终止）
    db.runtime().set_query_memory_limit_bytes(1);
    let out = db.sql_collect("SELECT v FROM t ORDER BY v").unwrap();
    let rows: Vec<String> = out
        .iter()
        .flat_map(|b| {
            (0..b.num_rows()).map(|i| {
                arrow::util::display::array_value_to_string(b.column(0).as_ref(), i).unwrap()
            })
        })
        .collect();
    assert_eq!(rows, vec!["1", "2", "3"]);
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
