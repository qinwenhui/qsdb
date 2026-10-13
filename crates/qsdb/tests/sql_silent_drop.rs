//! **「静默丢弃」闸门的回归**（2026-10-13 系统扫描）。
//!
//! 背景：绑定器是**逐字段读**的——只读它认识的 AST 字段，其余**默认被忽略**。
//! 于是"语法合法但没实现"的子句会安静地按"没写"执行，**不报错、不打日志**。
//! 这类 bug 只能靠人撞见（`count(DISTINCT)` 那条就是手滑撞见的）。
//!
//! 本文件把扫描出的每一条钉住：**要么正确执行，要么显式拒绝**——没有第三条路。
//! 每条拒绝都配**对照组**（正例），防止"一刀切"把正常查询也拒了。
//!
//! 判据写在 `translate::audit_query` / `audit_select` / `audit_function` /
//! `audit_window_spec` 的文档里；支持面清单见 `docs/SQL支持面.md`。

use qsdb::{Qsdb, QsdbOptions};

fn setup(tag: &str) -> (Qsdb, std::path::PathBuf) {
    let root = std::env::temp_dir()
        .join("qsdb_silent_drop_test")
        .join(format!(
            "{tag}_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    db.sql("CREATE TABLE t (a BIGINT, b BIGINT, s VARCHAR)")
        .unwrap();
    db.sql("INSERT INTO t VALUES (1,10,'x'),(2,NULL,'y'),(NULL,30,'z'),(3,40,NULL)")
        .unwrap();
    db.sql("CREATE TABLE u (a BIGINT, c VARCHAR)").unwrap();
    db.sql("INSERT INTO u VALUES (1,'u1'),(9,'u9')").unwrap();
    (db, root)
}

/// 断言**被拒**且理由说得清（文案里要有标识这场拒绝的关键词）。
fn rejected(db: &Qsdb, sql: &str, must_say: &str) {
    match db.sql(sql) {
        Ok(_) => panic!("本应被拒（静默丢弃），却通过了: {sql}"),
        Err(e) => {
            let msg = format!("{e}");
            assert!(
                msg.contains(must_say),
                "{sql}\n  拒绝理由应提到 {must_say:?}，实得：{msg}"
            );
        }
    }
}

/// 对某一格值取字符串（正例断言用）。
fn scalar(db: &Qsdb, sql: &str) -> String {
    let bs = db.sql_collect(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    arrow::util::display::array_value_to_string(bs[0].column(0).as_ref(), 0).unwrap()
}

/// ① 函数子句：`FILTER (WHERE …)` / `IGNORE NULLS` / 参数内子句。
///
/// `FILTER` 是这一批里**最典型的静默错值**：`count(*) FILTER (WHERE a>1)`
/// 曾返回 4（= `count(*)`）而不是 2。
#[test]
fn aggregate_filter_and_null_treatment_are_rejected() {
    let (db, root) = setup("filter");
    // FILTER：投影 / HAVING / 窗口三处路径都要拦
    rejected(&db, "SELECT count(*) FILTER (WHERE a > 1) FROM t", "FILTER");
    rejected(&db, "SELECT sum(b) FILTER (WHERE a = 1) FROM t", "FILTER");
    rejected(
        &db,
        "SELECT s FROM t GROUP BY s HAVING count(*) FILTER (WHERE a > 1) > 0",
        "FILTER",
    );
    rejected(
        &db,
        "SELECT count(*) FILTER (WHERE a > 1) OVER () FROM t",
        "FILTER",
    );
    // IGNORE/RESPECT NULLS（窗口）
    rejected(
        &db,
        "SELECT lag(b) IGNORE NULLS OVER (ORDER BY a) FROM t",
        "IGNORE",
    );
    rejected(
        &db,
        "SELECT lag(b) RESPECT NULLS OVER (ORDER BY a) FROM t",
        "IGNORE/RESPECT",
    );
    // 对照组：不带子句的同一批查询照常
    assert_eq!(scalar(&db, "SELECT count(*) FROM t"), "4");
    assert_eq!(scalar(&db, "SELECT count(*) FROM t WHERE a > 1"), "2");
    assert_eq!(scalar(&db, "SELECT sum(b) FROM t"), "80");
    std::fs::remove_dir_all(&root).ok();
}

/// ② 窗口帧：静默忽略会**换一个数字**（用默认累计帧）而不报错。
#[test]
fn window_frame_is_rejected() {
    let (db, root) = setup("frame");
    rejected(
        &db,
        "SELECT sum(b) OVER (ORDER BY a ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t",
        "window frame",
    );
    rejected(
        &db,
        "SELECT sum(b) OVER (ORDER BY a RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM t",
        "window frame",
    );
    // 对照组：默认帧（不写帧）是对的（累计）
    let bs = db
        .sql_collect("SELECT sum(b) OVER (ORDER BY b) FROM t")
        .unwrap();
    let vals: Vec<String> = (0..bs[0].num_rows())
        .map(|i| arrow::util::display::array_value_to_string(bs[0].column(0).as_ref(), i).unwrap())
        .collect();
    assert_eq!(vals, vec!["10", "40", "80", "80"], "默认帧 = 累计和");
    std::fs::remove_dir_all(&root).ok();
}

/// ③ 限制子句：`OFFSET`（三种写法）此前被**静默丢掉** → 少翻页、多返回行。
#[test]
fn offset_and_fetch_are_rejected() {
    let (db, root) = setup("offset");
    rejected(&db, "SELECT a FROM t ORDER BY a LIMIT 2 OFFSET 1", "OFFSET");
    rejected(&db, "SELECT a FROM t ORDER BY a OFFSET 2 ROWS", "OFFSET");
    rejected(&db, "SELECT a FROM t ORDER BY a LIMIT 1, 2", "OFFSET");
    rejected(
        &db,
        "SELECT a FROM t FETCH FIRST 2 ROWS ONLY",
        "FETCH FIRST",
    );
    // 对照组：OFFSET 为 0 / 不写 OFFSET 时照常
    assert_eq!(
        scalar(
            &db,
            "SELECT count(*) FROM (SELECT a FROM t ORDER BY a LIMIT 2) x"
        ),
        "2"
    );
    std::fs::remove_dir_all(&root).ok();
}

/// ④ 排序选项：`NULLS FIRST/LAST` 此前被忽略 → 恒用默认，**顺序静默不对**。
#[test]
fn nulls_first_last_is_rejected() {
    let (db, root) = setup("nulls");
    rejected(
        &db,
        "SELECT b FROM t ORDER BY b ASC NULLS FIRST",
        "NULLS FIRST/LAST",
    );
    rejected(
        &db,
        "SELECT b FROM t ORDER BY b DESC NULLS LAST",
        "NULLS FIRST/LAST",
    );
    // 窗口内 ORDER BY 同一选项
    rejected(
        &db,
        "SELECT sum(b) OVER (ORDER BY b ASC NULLS FIRST) FROM t",
        "NULLS FIRST/LAST",
    );
    // 对照组：默认放置（ASC ⇒ NULLS LAST / DESC ⇒ NULLS FIRST）是对的
    let bs = db.sql_collect("SELECT b FROM t ORDER BY b ASC").unwrap();
    let last = (0..bs[0].num_rows())
        .map(|i| arrow::util::display::array_value_to_string(bs[0].column(0).as_ref(), i).unwrap())
        .next_back()
        .unwrap();
    assert_eq!(last, "", "ASC 默认把 NULL 放最后（渲染成空串）");
    std::fs::remove_dir_all(&root).ok();
}

/// ⑤ 选择级子句：`DISTINCT ON` / `TABLESAMPLE` / 通配符附加项 / 各类修饰。
#[test]
fn select_level_clauses_are_rejected() {
    let (db, root) = setup("select");
    rejected(&db, "SELECT DISTINCT ON (s) s, a FROM t", "DISTINCT ON");
    rejected(
        &db,
        "SELECT count(*) FROM t TABLESAMPLE (10 PERCENT)",
        "TABLESAMPLE",
    );
    rejected(&db, "SELECT * EXCLUDE (b) FROM t", "EXCLUDE");
    rejected(&db, "SELECT * REPLACE (a + 1 AS a) FROM t", "EXCLUDE");
    rejected(&db, "SELECT * RENAME (a AS aa) FROM t", "EXCLUDE");
    rejected(&db, "SELECT TOP 2 a FROM t", "TOP n");
    rejected(&db, "SELECT a INTO zz FROM t", "INTO");
    rejected(&db, "SELECT a FROM t QUALIFY a > 1", "QUALIFY");
    rejected(&db, "SELECT a FROM t FOR UPDATE", "row locking");
    // 对照组：普通 DISTINCT / 普通通配 / 普通排序照常
    assert_eq!(
        scalar(&db, "SELECT count(*) FROM (SELECT DISTINCT s FROM t) x"),
        "4"
    );
    assert_eq!(scalar(&db, "SELECT count(*) FROM (SELECT * FROM t) x"), "4");
    std::fs::remove_dir_all(&root).ok();
}

/// ⑥ 别名与 DML：CTE/派生表**列别名**、`INSERT … ON CONFLICT`。
#[test]
fn aliases_and_on_conflict_are_rejected() {
    let (db, root) = setup("alias");
    rejected(
        &db,
        "WITH w(q) AS (SELECT a FROM t) SELECT q FROM w",
        "CTE column alias",
    );
    rejected(
        &db,
        "SELECT x FROM (SELECT a FROM t) AS d(x)",
        "derived-table column alias",
    );
    rejected(
        &db,
        "INSERT INTO u VALUES (1,'x') ON CONFLICT DO NOTHING",
        "ON CONFLICT",
    );
    // 对照组：带别名的 CTE（不带列清单）与普通派生表照常
    assert_eq!(
        scalar(&db, "WITH w AS (SELECT a FROM t) SELECT count(*) FROM w"),
        "4"
    );
    assert_eq!(
        scalar(&db, "SELECT count(*) FROM (SELECT a FROM t) AS d"),
        "4"
    );
    // 对照：ON CONFLICT 未写时行数真的变了（证明上面那条拦的是"会真插进去"的写法）
    assert_eq!(scalar(&db, "SELECT count(*) FROM u"), "2");
    std::fs::remove_dir_all(&root).ok();
}

/// ⑦ JOIN 右表**裸列**：我们的规则是"裸名 = 左表、右表必须限定"（SQL-2 约束，
/// 见 `translate_select_inner` 的 ctx 扩展注释）。
///
/// 这不是静默错值（永远只有一个候选），但与标准 SQL 不同——所以**报错要能指路**，
/// 而不是干巴巴一句"列不存在"。
#[test]
fn right_side_bare_column_error_points_at_the_fix() {
    let (db, root) = setup("joinhint");
    match db.sql("SELECT t.a, c FROM t LEFT JOIN u ON t.a = u.a") {
        Ok(_) => panic!("右表裸列应报错（本仓要求限定名）"),
        Err(e) => {
            let msg = format!("{e}");
            assert!(msg.contains("column 'c'"), "{msg}");
            assert!(
                msg.contains("u.c") && msg.contains("left input only"),
                "报错应指出限定名写法与规则，实得：{msg}"
            );
        }
    }
    // 对照组：限定名照常
    assert_eq!(
        scalar(
            &db,
            "SELECT count(*) FROM t LEFT JOIN u ON t.a = u.a WHERE u.c IS NOT NULL"
        ),
        "1"
    );
    std::fs::remove_dir_all(&root).ok();
}

/// ⑧ 闸门必须**覆盖嵌套**：子查询/派生表/CTE 体里的静默子句同样要拦。
#[test]
fn gates_cover_nested_queries() {
    let (db, root) = setup("nested");
    rejected(
        &db,
        "SELECT count(*) FROM (SELECT a FROM t FETCH FIRST 1 ROWS ONLY) x",
        "FETCH FIRST",
    );
    rejected(
        &db,
        "SELECT count(*) FROM (SELECT DISTINCT ON (s) s FROM t) x",
        "DISTINCT ON",
    );
    rejected(
        &db,
        "WITH w AS (SELECT a FROM t ORDER BY a NULLS FIRST) SELECT count(*) FROM w",
        "NULLS FIRST/LAST",
    );
    rejected(
        &db,
        "SELECT (SELECT count(*) FILTER (WHERE a > 1) FROM t) FROM t LIMIT 1",
        "FILTER",
    );
    std::fs::remove_dir_all(&root).ok();
}
