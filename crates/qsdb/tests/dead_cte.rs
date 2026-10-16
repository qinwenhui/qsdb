//! 死 CTE（定义了但引用不到）**不参与绑定** —— 2026-10-16。
//!
//! ## 为什么要这条
//!
//! 日清分那批结算 SQL 是**机器生成、逐版累加**出来的：同一个任务有 5 个产出，
//! 于是**每个产出块里都塞着另外 4 个分支的全部 CTE**。实测那 154 个 SQL 块：
//!
//! | | |
//! |---|---|
//! | CTE 定义总数 | **2890** |
//! | 其中**死**的 | **562（19.4%）** |
//! | 含死 CTE 的块 | **55 / 154** |
//! | **死 CTE 里含 `PIVOT`/`stack` 的** | **46 处** |
//!
//! 最臃肿的一块（`L8-03` 第 1 块）46 个定义里 **38 个是死的**，主查询只是
//! `SELECT * FROM t23`，活链只有 `t23←t18←t13←t8←t2` 五节。
//!
//! ⇒ **死代码里那些我们还没实现的 `PIVOT`/`stack` 不该拦路。**
//!
//! ## 与 Spark 的差异（刻意，且只有一处）
//!
//! 实测真 Spark 3.5.2：**死 CTE 里写错表/列/函数照样报错**（它的 analysis 覆盖全文，
//! 只是不执行）。我们提前砍 ⇒ **在非法 SQL 上比 Spark 宽松**。
//! 方向安全：**永远不产错值**，只是少报一个"你那段死代码写错了"。
//!
//! ⚠️ 判定是**保守**的（按分段文本扫标识符 token，见 `translate.rs::live_cte_names`）：
//! 只会**多留**、不会**少留**。少留会退化成硬报错，所以下面专门有一条钉住这个方向。

use qsdb::{Qsdb, QsdbOptions};

fn setup(tag: &str) -> (Qsdb, std::path::PathBuf) {
    let root = std::env::temp_dir().join("qsdb_dead_cte").join(format!(
        "{tag}_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    db.sql("CREATE TABLE t (a INT, b INT)").unwrap();
    db.sql("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    (db, root)
}

fn rows(db: &Qsdb, sql: &str) -> Vec<String> {
    let bs = db.sql_collect(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    if bs.is_empty() {
        return vec![];
    }
    let b = &bs[0];
    (0..b.num_rows())
        .map(|i| {
            (0..b.num_columns())
                .map(|c| {
                    arrow::util::display::array_value_to_string(b.column(c).as_ref(), i).unwrap()
                })
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

/// 死 CTE 引用**不存在的表** —— 我们（与 Spark 不同）不再报错。
#[test]
fn 死_cte_里的错表名不再拦路() {
    let (db, _r) = setup("notable");
    let got = rows(
        &db,
        "WITH dead AS (SELECT * FROM no_such_table_xyz), \
              live AS (SELECT a FROM t) \
         SELECT a FROM live ORDER BY a",
    );
    assert_eq!(got, vec!["1", "2", "3"]);
}

/// ★ **本体**：死 CTE 里的 `PIVOT` —— 我们还没实现，但只要它是死的就不该拦路。
/// 这个形状就是 `L8-03` 那批"转横表"块里死分支的样子。
#[test]
fn 死_cte_里的_pivot_不再拦路() {
    let (db, _r) = setup("deadpivot");
    let got = rows(
        &db,
        "WITH dead AS (SELECT * FROM (SELECT a, b FROM t) x \
                             PIVOT (SUM(b) FOR a IN (1))), \
              live AS (SELECT a FROM t) \
         SELECT a FROM live ORDER BY a",
    );
    assert_eq!(got, vec!["1", "2", "3"]);
}

/// 死的**传递性**：`dead2` 引用 `dead1`，主查询谁都不引用 ⇒ 两个都不绑定。
/// （只判一跳是不够的 —— 语料里 `t7` 引 `t1`、两个都是死的。）
#[test]
fn 死_cte_引用的_cte_也是死的() {
    let (db, _r) = setup("transitive");
    let got = rows(
        &db,
        "WITH dead1 AS (SELECT * FROM no_such_table_xyz), \
              dead2 AS (SELECT a FROM dead1), \
              dead3 AS (SELECT * FROM (SELECT a, b FROM t) x PIVOT (SUM(b) FOR a IN (1))), \
              live  AS (SELECT a FROM t) \
         SELECT a FROM live ORDER BY a",
    );
    assert_eq!(got, vec!["1", "2", "3"]);
}

/// **活的** CTE 里的 `PIVOT` 仍然**显式拒** —— 裁剪不能把真拦路虎也放过去。
#[test]
fn 活的_cte_里的_pivot_仍然拒() {
    let (db, _r) = setup("livepivot");
    let e = db
        .sql_collect(
            "WITH live AS (SELECT * FROM (SELECT a, b FROM t) x PIVOT (SUM(b) FOR a IN (1))) \
             SELECT * FROM live",
        )
        .unwrap_err();
    let s = e.to_string().to_lowercase();
    assert!(
        s.contains("pivot") || s.contains("unsupported") || s.contains("not supported"),
        "unexpected: {s}"
    );
}

/// ★ **保守性**：CTE 只在**表达式里的子查询**被引用（不在 FROM / JOIN 里）。
/// 判活必须覆盖到它，否则会被误判成死 ⇒ 绑定期报 `table not found`。
#[test]
fn 只在表达式子查询里被引用_不算死() {
    let (db, _r) = setup("exprsub");
    let got = rows(
        &db,
        "WITH t1 AS (SELECT a FROM t), \
              t2 AS (SELECT a FROM t WHERE a > 1) \
         SELECT a FROM t1 WHERE a IN (SELECT a FROM t2) ORDER BY a",
    );
    assert_eq!(got, vec!["2", "3"]);
}

/// 保守性（二）：CTE 只在**派生表 / 集合操作的一侧**被引用。
#[test]
fn 只在派生表或集合操作里被引用_不算死() {
    let (db, _r) = setup("derived");
    let got = rows(
        &db,
        "WITH t1 AS (SELECT a FROM t), \
              t2 AS (SELECT 99 AS a) \
         SELECT a FROM (SELECT a FROM t1 UNION ALL SELECT a FROM t2) u ORDER BY a",
    );
    assert_eq!(got, vec!["1", "2", "3", "99"]);
}

/// 死 CTE **之间**重名照样报错 —— 名字空间与"用不用得上"是两件事，
/// 裁剪不许把这一类错误悄悄吞掉。
#[test]
fn 死_cte_之间的重名仍然报错() {
    let (db, _r) = setup("dup");
    let e = db
        .sql_collect(
            "WITH dup1 AS (SELECT a FROM t), \
                  dup1 AS (SELECT b FROM t), \
                  live AS (SELECT a FROM t) \
             SELECT a FROM live",
        )
        .unwrap_err();
    assert!(e.to_string().contains("duplicate"), "unexpected: {e}");
}

/// 裁剪**不改变结果**：同一条查询，一条把死 CTE 留在那、一条删掉，逐值必须一样。
#[test]
fn 裁剪不改变结果() {
    let (db, _r) = setup("same");
    let with_dead = "WITH dead1 AS (SELECT * FROM no_such_table_xyz), \
                          dead2 AS (SELECT a FROM t WHERE a < 0), \
                          live AS (SELECT a, b FROM t) \
                     SELECT a, b FROM live ORDER BY a";
    let without_dead = "WITH live AS (SELECT a, b FROM t) SELECT a, b FROM live ORDER BY a";
    assert_eq!(rows(&db, with_dead), rows(&db, without_dead));
    assert_eq!(
        rows(&db, without_dead),
        vec!["1|10", "2|20", "3|30"]
    );
}
