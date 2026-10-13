//! 聚合的**两条 SQL 契约** + 聚合 `DISTINCT` 的显式拒绝（2026-10-13 自查）。
//!
//! 三条都是"合法 SQL、悄悄给错结果"这一类，且都因为 `count(*)` 走元数据短路
//! 而长期隐形（那条路径恰好是对的）：
//!
//! ① **无 `GROUP BY` 的聚合恰好一行**。空输入时此前返回**零个批**——
//!    `SELECT count(a) FROM t WHERE <不匹配>` 在网线上就是空数组，
//!    而不是 `0`；`sum`/`avg`/`min`/`max` 同理（应为一行 NULL）。
//!    根因：`ingest` 对零行批 early-return，组表里一个组都没有。
//! ② **零行结果仍带一个空批 + 完整 schema**（`对接契约.md` §二 的 schema
//!    协商承诺，`LIMIT 0` 是标准姿势）。此前是零个批，schema 只在
//!    `SqlResult::Query.schema` 里，逐批消费的调用方拿不到列名。
//! ③ **聚合参数上的 `DISTINCT` 此前被静默丢弃**：`count(DISTINCT a)` 当成
//!    `count(a)` 执行，回**错的数字**（`{1,1,2}` 上回 3 而非 2）。现在绑定层
//!    显式拒绝，并给出等价改写。

use qsdb::{Qsdb, QsdbOptions};

fn tmp(tag: &str) -> std::path::PathBuf {
    let root = std::env::temp_dir()
        .join("qsdb_agg_contract_test")
        .join(format!(
            "{tag}_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
    let _ = std::fs::remove_dir_all(&root);
    root
}

/// 单列单行结果（`count`/`sum` 等聚合用）→ 字符串（NULL 渲染为空串）。
fn scalar(db: &Qsdb, sql: &str) -> String {
    let bs = db
        .sql_collect(sql)
        .unwrap_or_else(|e| panic!("{sql} 应成功，实得 {e}"));
    assert_eq!(bs.len(), 1, "{sql}: 聚合必须恰好一个批，实得 {}", bs.len());
    assert_eq!(bs[0].num_rows(), 1, "{sql}: 聚合必须恰好一行");
    (0..bs[0].num_columns())
        .map(|c| arrow::util::display::array_value_to_string(bs[0].column(c).as_ref(), 0).unwrap())
        .collect::<Vec<_>>()
        .join(",")
}

fn rows(db: &Qsdb, sql: &str) -> usize {
    db.sql_collect(sql)
        .unwrap_or_else(|e| panic!("{sql} 应成功，实得 {e}"))
        .iter()
        .map(|b| b.num_rows())
        .sum()
}

fn sql_err(db: &Qsdb, sql: &str) -> String {
    match db.sql(sql) {
        Ok(_) => panic!("本应被拒却成功了: {sql}"),
        Err(e) => format!("{e}"),
    }
}

/// **① 空输入的无 `GROUP BY` 聚合：恰好一行，值正确**（count→0，其余→NULL）。
#[test]
fn global_aggregate_over_empty_input_returns_exactly_one_row() {
    let root = tmp("empty");
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    db.sql("CREATE TABLE e (a BIGINT, b DOUBLE)").unwrap();

    // 空表：每个聚合函数都要那唯一一行
    assert_eq!(scalar(&db, "SELECT count(a) FROM e"), "0", "count(空) = 0");
    assert_eq!(scalar(&db, "SELECT count(*) FROM e"), "0");
    assert_eq!(scalar(&db, "SELECT sum(a) FROM e"), "", "sum(空) = NULL");
    assert_eq!(scalar(&db, "SELECT min(a) FROM e"), "", "min(空) = NULL");
    assert_eq!(scalar(&db, "SELECT max(a) FROM e"), "", "max(空) = NULL");
    assert_eq!(scalar(&db, "SELECT avg(b) FROM e"), "", "avg(空) = NULL");
    // 多个聚合一列里混用（状态列按类型分派，别串了）
    assert_eq!(scalar(&db, "SELECT count(a), sum(b), avg(b) FROM e"), "0,,");

    // 有数据的表 + 谓词全不匹配：同一契约（这条曾是网线上最典型的"空数组"）
    db.sql("CREATE TABLE n (a BIGINT, p BIGINT) PARTITIONED BY (p)")
        .unwrap();
    db.sql("INSERT INTO n VALUES (1, 1), (2, 1)").unwrap();
    assert_eq!(scalar(&db, "SELECT count(a) FROM n WHERE a > 99"), "0");
    assert_eq!(scalar(&db, "SELECT sum(a) FROM n WHERE false"), "");
    assert_eq!(scalar(&db, "SELECT count(a) FROM n WHERE p = 999"), "0");
    assert_eq!(scalar(&db, "SELECT sum(a) FROM n WHERE p = 999"), "");

    // 非空输入不能被这条修坏（分片路径尤其：全局聚合只该产出一份）
    assert_eq!(scalar(&db, "SELECT sum(a) FROM n"), "3");
    assert_eq!(scalar(&db, "SELECT count(a) FROM n"), "2");

    // **有 `GROUP BY` 零组仍是零行**——别修过头
    assert_eq!(rows(&db, "SELECT a, count(*) FROM e GROUP BY a"), 0);
    assert_eq!(rows(&db, "SELECT a FROM n WHERE false GROUP BY a"), 0);

    std::fs::remove_dir_all(&root).ok();
}

/// **② 零行结果 → 一个空批 + 完整 schema**（`对接契约.md` §二）。
#[test]
fn zero_row_result_carries_one_empty_batch_with_schema() {
    let root = tmp("zero");
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    db.sql("CREATE TABLE e (a BIGINT, b VARCHAR)").unwrap();
    db.sql("INSERT INTO e VALUES (1, 'x')").unwrap();
    db.sql("DELETE FROM e").unwrap();

    for sql in [
        "SELECT a, b FROM e",          // 空表
        "SELECT a, b FROM e LIMIT 0",  // 契约的标准姿势
        "SELECT a FROM e WHERE false", // 谓词不匹配
        "SELECT 1 WHERE false",        // 连表都没有
    ] {
        let bs = db.sql_collect(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
        assert_eq!(bs.len(), 1, "{sql}: 零行结果也该有一个空批");
        assert_eq!(bs[0].num_rows(), 0, "{sql}: 该批必须是空的");
        assert!(
            bs[0].num_columns() > 0,
            "{sql}: 空批必须带列（schema 协商）"
        );
    }
    // 列名/类型照旧可协商
    match db.sql("SELECT a, b FROM e LIMIT 0") {
        Ok(qsdb::sql::SqlResult::Query { schema, .. }) => {
            assert_eq!(schema.fields().len(), 2);
            assert_eq!(schema.field(0).name(), "a");
            assert_eq!(schema.field(1).name(), "b");
        }
        _ => panic!("expected Query"),
    }
    std::fs::remove_dir_all(&root).ok();
}

/// **③ 聚合 `DISTINCT`：显式拒绝（绝不静默给错值）+ 等价改写正确**。
#[test]
fn aggregate_distinct_is_rejected_not_silently_ignored() {
    let root = tmp("distinct");
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    db.sql("CREATE TABLE d (a BIGINT, g BIGINT)").unwrap();
    // 值集 {1,1,2,3,3,3}：count=6、distinct=3 —— 两者**必须**能区分
    db.sql("INSERT INTO d VALUES (1,1),(1,1),(2,1),(3,2),(3,2),(3,2)")
        .unwrap();

    for sql in [
        "SELECT count(DISTINCT a) FROM d",
        "SELECT sum(DISTINCT a) FROM d",
        "SELECT avg(DISTINCT a) FROM d",
        "SELECT min(DISTINCT a) FROM d",
        "SELECT max(DISTINCT a) FROM d",
        "SELECT count(DISTINCT a) FROM d WHERE a > 1",
    ] {
        let e = sql_err(&db, sql);
        assert!(
            e.contains("DISTINCT") && e.contains("not implemented"),
            "{sql}: 要说清是 DISTINCT 未实现（实得 {e}）"
        );
    }

    // 等价改写（报错信息里给的那条）必须是对的
    assert_eq!(
        scalar(&db, "SELECT count(*) FROM (SELECT DISTINCT a FROM d)"),
        "3"
    );
    assert_eq!(
        scalar(&db, "SELECT sum(a) FROM (SELECT DISTINCT a FROM d)"),
        "6"
    );
    assert_eq!(
        scalar(
            &db,
            "SELECT count(*) FROM (SELECT DISTINCT a FROM d WHERE a > 1)"
        ),
        "2"
    );
    // 带分组的改写
    assert_eq!(
        rows(
            &db,
            "SELECT g, count(*) FROM (SELECT DISTINCT g, a FROM d) GROUP BY g"
        ),
        2
    );
    // 对照：不带 DISTINCT 的原样查询值不变（别把普通聚合一起拒了）
    assert_eq!(scalar(&db, "SELECT count(a) FROM d"), "6");
    assert_eq!(scalar(&db, "SELECT sum(a) FROM d"), "13");

    std::fs::remove_dir_all(&root).ok();
}
