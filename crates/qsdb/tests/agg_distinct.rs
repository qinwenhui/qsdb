//! **聚合参数 `DISTINCT`**（`count/sum/avg/min/max(DISTINCT x)`）的契约回归。
//!
//! 背景：2026-10-13 之前绑定层**从不读** `duplicate_treatment`，于是
//! `count(DISTINCT x)` 被当成 `count(x)` 执行 —— **回错的数字、不报错**。
//! 当时改成显式拒绝；2026-10-14 补上原生语义（等价改写：
//! `Distinct` 去重 + 原聚合，见 `translate::rewrite_distinct_aggs`）。
//!
//! ## 这些测试的重点是 **NULL 语义**（改写下最容易错的一处）
//!
//! * 无分组：`count(DISTINCT x)` **跳过 NULL**，而 `count(*)` 不跳 ⇒ 必须先滤 NULL。
//! * 有分组：**不能**滤 NULL —— 某组可能只有 NULL 的 x，滤掉会让**整组消失**
//!   （正确结果是该组回 **0**，而不是没有这一行）。这条单靠"合计对不对"看不出来，
//!   所以 `g=2` 那组的**行存在性**单独断言。
//!
//! ## 数据分布（必须能区分 `count(x)` 与 `count(DISTINCT x)`，否则等于没测）
//!
//! | g | x | count(x) | count(DISTINCT x) |
//! |---|---|---|---|
//! | 1 | 1, 1, 2 | 3 | **2** |
//! | 2 | NULL, NULL | 0 | **0**（但这一组必须存在） |
//! | 3 | 5, NULL, 5 | 2 | **1** |
//!
//! 全表非空 x = {1, 2, 5} ⇒ `count(DISTINCT x)` = **3**，而 `count(x)` = 5。
//! 两者**必须不同**，否则断言分不清语义。

use qsdb::{Qsdb, QsdbOptions};

fn tmp(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join("qsdb_agg_distinct").join(format!(
        "{tag}_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

fn open(tag: &str) -> (Qsdb, std::path::PathBuf) {
    let root = tmp(tag);
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    (db, root)
}

fn rows(db: &Qsdb, sql: &str) -> Vec<String> {
    let bs = db.sql_collect(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    let mut out = Vec::new();
    for b in &bs {
        for i in 0..b.num_rows() {
            let cells: Vec<String> = (0..b.num_columns())
                .map(|c| {
                    let a = b.column(c);
                    if a.is_null(i) {
                        "NULL".into()
                    } else {
                        arrow::util::display::array_value_to_string(a.as_ref(), i).unwrap()
                    }
                })
                .collect();
            out.push(cells.join("|"));
        }
    }
    out.sort();
    out
}

fn one(db: &Qsdb, sql: &str) -> String {
    let r = rows(db, sql);
    assert_eq!(r.len(), 1, "{sql} 应回恰好一行，实得 {r:?}");
    r.into_iter().next().unwrap()
}

/// 造数：见文件头表格。`s` 列用于字符串去重与"参数不同必须报错"。
fn seed(db: &Qsdb) {
    db.sql("CREATE TABLE t (id BIGINT, g BIGINT, x BIGINT, s STRING)")
        .unwrap();
    db.sql(
        "INSERT INTO t VALUES \
         (1,1,1,'a'),(2,1,1,'a'),(3,1,2,'b'),\
         (4,2,NULL,'a'),(5,2,NULL,NULL),\
         (6,3,5,'c'),(7,3,NULL,'c'),(8,3,5,NULL)",
    )
    .unwrap();
}

/// 未优化的等价写法（`SELECT DISTINCT` 子查询）—— 交叉对照，防改写写出"另一种错"。
fn eq_distinct(ungrouped: &str) -> String {
    format!("SELECT count(*) FROM (SELECT DISTINCT {ungrouped} FROM t WHERE x IS NOT NULL)")
}

#[test]
fn count_distinct_ungrouped_skips_null() {
    let (db, root) = open("cdu");
    seed(&db);
    // 全表非空 x = {1,2,5} ⇒ 3（而 count(x) = 5，两者必须不同）
    assert_eq!(one(&db, "SELECT count(x) FROM t"), "5");
    assert_eq!(one(&db, "SELECT count(DISTINCT x) FROM t"), "3");
    // 交叉对照：等价改写
    assert_eq!(one(&db, &eq_distinct("x")), "3");
    drop(db);
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn count_distinct_grouped_keeps_all_null_group() {
    let (db, root) = open("cdg");
    seed(&db);
    assert_eq!(
        rows(&db, "SELECT g, count(DISTINCT x) FROM t GROUP BY g"),
        vec!["1|2", "2|0", "3|1"],
        "g=2 那组只有 NULL 的 x：必须**存在**且回 0（过滤 NULL 会让整组消失）"
    );
    // 交叉对照（派生表写法，同样保留 NULL 组）
    assert_eq!(
        rows(
            &db,
            "SELECT g, count(x) FROM (SELECT DISTINCT g, x FROM t) GROUP BY g"
        ),
        vec!["1|2", "2|0", "3|1"]
    );
    drop(db);
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn sum_avg_min_max_distinct() {
    let (db, root) = open("sad");
    seed(&db);
    // 非空 x 去重后 = {1,2,5}：和 8、均 2.666…、min 1、max 5
    // 对照：不去重是 5 个值 {1,1,2,5,5} ⇒ 和 14、均 2.8
    assert_eq!(one(&db, "SELECT sum(x) FROM t"), "14");
    assert_eq!(one(&db, "SELECT sum(DISTINCT x) FROM t"), "8");
    assert_eq!(one(&db, "SELECT avg(x) FROM t"), "2.8");
    assert_eq!(
        one(&db, "SELECT avg(DISTINCT x) FROM t"),
        "2.6666666666666665"
    );
    assert_eq!(one(&db, "SELECT min(DISTINCT x) FROM t"), "1");
    assert_eq!(one(&db, "SELECT max(DISTINCT x) FROM t"), "5");
    drop(db);
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn distinct_on_string_column() {
    let (db, root) = open("str");
    seed(&db);
    // 非空 s = {a,a,b,a,c,c} ⇒ 去重 {a,b,c} = 3
    assert_eq!(one(&db, "SELECT count(s) FROM t"), "6");
    assert_eq!(one(&db, "SELECT count(DISTINCT s) FROM t"), "3");
    assert_eq!(
        rows(&db, "SELECT g, count(DISTINCT s) FROM t GROUP BY g"),
        vec!["1|2", "2|1", "3|1"]
    );
    drop(db);
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn multiple_aggs_same_argument() {
    let (db, root) = open("multi");
    seed(&db);
    // 同一个参数上的多个 DISTINCT 聚合可以共享一次去重
    assert_eq!(
        rows(&db, "SELECT count(DISTINCT x), sum(DISTINCT x) FROM t"),
        vec!["3|8"]
    );
    // 显示名：`count(DISTINCT x)` 与 `count(x)` **必须不同**（PG/DuckDB 同款）。
    // 若同名，`collect_aggs` 按 out_name 去重会把两者合并成一个 —— 少一列。
    let bs = db.sql_collect("SELECT count(DISTINCT x) FROM t").unwrap();
    let names: Vec<String> = bs[0]
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert_eq!(names, vec!["count(DISTINCT x)"]);
    drop(db);
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn all_null_input_ungrouped_returns_zero() {
    let (db, root) = open("null");
    db.sql("CREATE TABLE t (g BIGINT, x BIGINT)").unwrap();
    db.sql("INSERT INTO t VALUES (1,NULL),(1,NULL)").unwrap();
    // 去重前滤掉全部行 ⇒ 零行输入上的全局聚合必须回 **一行 0**（不是零行）
    assert_eq!(one(&db, "SELECT count(DISTINCT x) FROM t"), "0");
    assert_eq!(one(&db, "SELECT count(DISTINCT g) FROM t"), "1");
    drop(db);
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn empty_table_returns_zero() {
    let (db, root) = open("empty");
    db.sql("CREATE TABLE t (g BIGINT, x BIGINT)").unwrap();
    assert_eq!(one(&db, "SELECT count(DISTINCT x) FROM t"), "0");
    assert_eq!(
        rows(&db, "SELECT g, count(DISTINCT x) FROM t GROUP BY g"),
        Vec::<String>::new()
    );
    drop(db);
    let _ = std::fs::remove_dir_all(root);
}

/// 绑一条 SELECT，拿回未执行的逻辑计划（与 `join_plan_shape.rs` 同款）。
fn plan_of(db: &Qsdb, sql: &str) -> qsdb::sql::translate::LogicalPlan {
    use qsdb::sql::translate::{self, ParsedStatement};
    let mut stmts = translate::parse_statements(sql).expect("parse");
    assert_eq!(stmts.len(), 1, "形状测试一次一条");
    let ParsedStatement::Select(q) = stmts.remove(0) else {
        panic!("不是 SELECT：{sql}");
    };
    let resolver = translate::TableResolver {
        resolve: Box::new(|n: &str| db.table_def(n)),
        views: Default::default(),
    };
    translate::translate_select_full(*q, &resolver).unwrap_or_else(|e| panic!("{sql}: {e}"))
}

/// 剥掉顶层 `Project`，取出 `Aggregate`。
fn agg_of(p: &qsdb::sql::translate::LogicalPlan) -> &qsdb::sql::translate::LogicalPlan {
    use qsdb::sql::translate::LogicalPlan;
    match p {
        LogicalPlan::Aggregate { .. } => p,
        LogicalPlan::Project { input, .. } | LogicalPlan::Filter { input, .. } => agg_of(input),
        other => panic!("没找到 Aggregate，实得 {other:?}"),
    }
}

/// **形状断言**：`count(DISTINCT x)` 必须改写成
/// `Aggregate{group:[], aggs:[count(*)]} ← Distinct ← Filter(x IS NOT NULL) ← Project[x]`。
///
/// 为什么用形状而不是耗时：改写写错（比如漏了滤 NULL、或忘了收紧成 `count(*)`）
/// 在**小数据上结果可能碰巧对**，而耗时机断言在 CI 上必然 flaky。形状是唯一稳定的判据。
#[test]
fn ungrouped_rewrite_shape() {
    use qsdb::sql::translate::LogicalPlan;
    let (db, root) = open("shape");
    seed(&db);
    let p = plan_of(&db, "SELECT count(DISTINCT x) FROM t");
    let LogicalPlan::Aggregate { input, group, aggs } = agg_of(&p) else {
        unreachable!()
    };
    assert!(group.is_empty(), "无分组：{group:?}");
    assert_eq!(aggs.len(), 1);
    assert!(
        aggs[0].arg.is_none(),
        "应收紧成 count(*)，实得 {:?}",
        aggs[0]
    );
    assert!(!aggs[0].distinct, "落到计划上的聚合不许再带 DISTINCT 标记");
    let LogicalPlan::Distinct { input } = input.as_ref() else {
        panic!("聚合输入应是 Distinct，实得 {input:?}");
    };
    // 去重的是 `Project[x]`，而**滤 NULL 在投影之下**（贴着扫描，可下推）
    let LogicalPlan::Project { input, exprs, .. } = input.as_ref() else {
        panic!("Distinct 输入应是 Project[x]，实得 {input:?}");
    };
    assert_eq!(exprs.len(), 1, "只投影去重参数：{exprs:?}");
    let LogicalPlan::Filter { predicate, .. } = input.as_ref() else {
        panic!("Project 之下应是 Filter(x IS NOT NULL)，实得 {input:?}");
    };
    assert!(
        matches!(predicate, qsdb::expr::PhysExpr::IsNull(_, true)),
        "谓词应是 IS NOT NULL，实得 {predicate:?}"
    );
    drop(db);
    let _ = std::fs::remove_dir_all(root);
}

/// **有分组时绝不能滤 NULL** —— 滤了会让"只有 NULL 的组"整组消失。
#[test]
fn grouped_rewrite_shape_has_no_null_filter() {
    use qsdb::sql::translate::LogicalPlan;
    let (db, root) = open("shapeg");
    seed(&db);
    let p = plan_of(&db, "SELECT g, count(DISTINCT x) FROM t GROUP BY g");
    let LogicalPlan::Aggregate { input, group, aggs } = agg_of(&p) else {
        unreachable!()
    };
    assert_eq!(
        group.len(),
        1,
        "分组键应改成对去重输出列的位置引用：{group:?}"
    );
    assert!(
        matches!(group[0], qsdb::expr::PhysExpr::Column { idx: 0, .. }),
        "{:?}",
        group[0]
    );
    let LogicalPlan::Distinct { input } = input.as_ref() else {
        panic!("聚合输入应是 Distinct，实得 {input:?}");
    };
    assert!(
        matches!(input.as_ref(), LogicalPlan::Project { .. }),
        "有分组时 Distinct 应直接吃投影（**没有** Filter），实得 {input:?}"
    );
    // 聚合参数仍指向"去重输出里 x 那一列"（位置 = 分组键数），而不是 count(*)
    let arg = aggs[0].arg.as_ref().expect("有分组时保持 count(x)");
    assert!(
        matches!(arg, qsdb::expr::PhysExpr::Column { idx: 1, .. }),
        "{arg:?}"
    );
    drop(db);
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn mixed_distinct_and_plain_is_rejected() {
    let (db, root) = open("mixed");
    seed(&db);
    // 一次去重会改掉 `count(x)` 的输入 ⇒ 显式拒绝，不许硬猜
    let e = db
        .sql_collect("SELECT count(DISTINCT x), count(x) FROM t")
        .expect_err("混用必须报错");
    let m = e.to_string().to_ascii_lowercase();
    assert!(m.contains("distinct"), "{m}");

    // 换个聚合函数再验一遍（`sum(s)` 会先在绑定期报类型错，测不到混用那条）
    let e = db
        .sql_collect("SELECT count(DISTINCT x), sum(x) FROM t")
        .expect_err("混用必须报错");
    assert!(e.to_string().to_ascii_lowercase().contains("distinct"));
    drop(db);
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn distinct_aggs_with_different_arguments_are_rejected() {
    let (db, root) = open("diffargs");
    seed(&db);
    let e = db
        .sql_collect("SELECT count(DISTINCT x), count(DISTINCT s) FROM t")
        .expect_err("参数不同的 DISTINCT 聚合必须报错");
    assert!(e.to_string().to_ascii_lowercase().contains("distinct"));
    drop(db);
    let _ = std::fs::remove_dir_all(root);
}
