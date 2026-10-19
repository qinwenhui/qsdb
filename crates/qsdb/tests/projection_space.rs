//! **列裁剪 × 索引空间**（"表达式活在哪个列空间"）的回归测试。
//!
//! ## 背景（2026-10-11 发现，**先于本次改动就存在**的静默错值）
//!
//! `exec_builder` 的列裁剪把「表列序 → 扫描投影序」重映射一遍。它判断一个
//! `Column` 是不是表列，原先是**按列名猜**（`all.contains(name)`）。于是在
//! 「上下两层列名相同、但列空间不同」的形状上，会拿**外层的列位置**去查
//! **表空间的映射**，静默指到隔壁那一列：
//!
//! ```sql
//! -- t(id, g, x, s)
//! SELECT g, x FROM (SELECT DISTINCT g, x FROM t)
//! -- 修复前实际回 (1,1),(1,1),(2,2),(3,3),(3,3)：x 列整个变成了 g 列（mapping[1] = g）
//! SELECT g, count(x) FROM (SELECT DISTINCT g, x FROM t) GROUP BY g
//! -- 修复前实际回 2|1、3|2（正确 2|0、3|1）：NULL 组被算进了 count
//! ```
//!
//! 不报错、不打日志，**值断言之外没有任何东西能拦住它**。同类前科：`Sort` 的
//! 排序键（见 `exec_builder` 的 `Sort` 臂注释，回归 `tests/order_by_prune.rs`）。
//! 修法：改成看**结构性事实**——「该节点自身表达式是否按表列序编号」
//! （`exprs_are_in_table_space`），说不清的空间一律**原样保留**。
//!
//! ## 为什么这些用例挑这种形状
//!
//! 触发条件是「**Prjoection/Distinct 之上还有一层**引用同名列的表达式」：
//! 外层 Project、外层 Filter、外层 Aggregate 都能踩到。所以三种都各来一条。
//! 表里有 `id` 且外层用不到 ⇒ 裁剪**必然**发生（`id` 被裁掉，映射非恒等），
//! 这是让 bug 显形的必要条件。

use qsdb::sql::exec_builder::prune_decision;
use qsdb::sql::translate::{self, ParsedStatement};
use qsdb::{Qsdb, QsdbOptions};

/// 测试目录唯一序号：pid 挡跨进程碰撞，序号挡同进程内并发（Windows 时钟
/// 粒度 ~100ns，纳秒后缀在同 tick 内会重复——CI 上真炸过，见 constraints）。
fn _uniq_seq() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    SEQ.fetch_add(1, Ordering::Relaxed)
}

fn tmp(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join("qsdb_proj_space").join(format!(
        "{tag}_{:x}_{}",
        std::process::id(),
        _uniq_seq()
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
            out.push(
                (0..b.num_columns())
                    .map(|c| {
                        let a = b.column(c);
                        if a.is_null(i) {
                            "NULL".into()
                        } else {
                            arrow::util::display::array_value_to_string(a.as_ref(), i).unwrap()
                        }
                    })
                    .collect::<Vec<_>>()
                    .join("|"),
            );
        }
    }
    out.sort();
    out
}

fn plan_of(db: &Qsdb, sql: &str) -> translate::LogicalPlan {
    let mut stmts = translate::parse_statements(sql).expect("parse");
    let ParsedStatement::Select(q) = stmts.remove(0) else {
        panic!("不是 SELECT：{sql}");
    };
    let resolver = translate::TableResolver {
        resolve: Box::new(|n: &str| db.table_def(n)),
        views: Default::default(),
    };
    translate::translate_select_full(*q, &resolver).unwrap_or_else(|e| panic!("{sql}: {e}"))
}

/// `t(id, g, x, s)`：`id` 全局唯一 ⇒ `DISTINCT` 只作用在 `(g,x)` 上，
/// 于是「去重后的行」与「原表的行」列名一模一样、列空间却不同 —— 正是踩雷形状。
///
/// | g | x |
/// |---|---|
/// | 1 | 1, 1, 2 |
/// | 2 | NULL, NULL |
/// | 3 | 5, NULL, 5 |
///
/// ⇒ `DISTINCT (g,x)` = `(1,1) (1,2) (2,NULL) (3,5) (3,NULL)` 共 5 行。
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

#[test]
fn outer_projection_over_distinct_keeps_the_second_column() {
    let (db, root) = open("outer_proj");
    seed(&db);
    // 修复前：x 列整个变成 g 列 → ["1|1","1|1","2|2","3|3","3|3"]
    assert_eq!(
        rows(&db, "SELECT g, x FROM (SELECT DISTINCT g, x FROM t)"),
        vec!["1|1", "1|2", "2|NULL", "3|5", "3|NULL"]
    );
    // 换个列序，确认不是"第二列恰好对"
    assert_eq!(
        rows(&db, "SELECT x, g FROM (SELECT DISTINCT g, x FROM t)"),
        // `rows()` 按字符串排序：数字在 `N` 之前
        vec!["1|1", "2|1", "5|3", "NULL|2", "NULL|3"]
    );
    drop(db);
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn outer_filter_over_distinct_uses_the_right_column() {
    let (db, root) = open("outer_filter");
    seed(&db);
    // `x = 5` 若被重写成 `g = 5`（旧 bug 的映射结果）→ 回 0 行。
    assert_eq!(
        rows(
            &db,
            "SELECT g, x FROM (SELECT DISTINCT g, x FROM t) WHERE x = 5"
        ),
        vec!["3|5"]
    );
    // 谓词落在第一列上：两种错误映射都可能"碰巧对"，所以两个方向都测
    assert_eq!(
        rows(
            &db,
            "SELECT g, x FROM (SELECT DISTINCT g, x FROM t) WHERE g = 3"
        ),
        vec!["3|5", "3|NULL"]
    );
    drop(db);
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn outer_aggregate_over_distinct_counts_the_right_column() {
    let (db, root) = open("outer_agg");
    seed(&db);
    // 修复前回 1|2、2|1、3|2（NULL 组被算进 count）
    assert_eq!(
        rows(
            &db,
            "SELECT g, count(x) FROM (SELECT DISTINCT g, x FROM t) GROUP BY g"
        ),
        vec!["1|2", "2|0", "3|1"]
    );
    // 不带 DISTINCT 的对照：count(x) 数的是原表的行
    assert_eq!(
        rows(&db, "SELECT g, count(x) FROM t GROUP BY g"),
        vec!["1|3", "2|0", "3|2"]
    );
    drop(db);
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn derived_table_then_group_by() {
    let (db, root) = open("derived");
    seed(&db);
    assert_eq!(
        rows(
            &db,
            "SELECT g, count(*) FROM (SELECT g, x FROM t) GROUP BY g"
        ),
        vec!["1|3", "2|2", "3|3"]
    );
    drop(db);
    let _ = std::fs::remove_dir_all(root);
}

/// 修复不能靠"把裁剪关掉"蒙混过去 —— 这条钉住**裁剪仍然发生**。
///
/// 只看结果对不对是**看不出**裁剪有没有白跑的：少裁一列只是变慢，不是变错。
#[test]
fn pruning_still_happens_for_the_new_shapes() {
    let (db, root) = open("still_prunes");
    seed(&db);
    // `id`/`s` 没人读 ⇒ 必须被裁掉（proj 是表全列的**真子集**）
    let (all, proj) = prune_decision(&plan_of(
        &db,
        "SELECT g, x FROM (SELECT DISTINCT g, x FROM t)",
    ))
    .expect("这条计划应走列裁剪");
    assert_eq!(all, vec!["id", "g", "x", "s"]);
    assert_eq!(
        proj,
        vec!["g", "x"],
        "id/s 应被裁掉（裁剪没被我的修复关掉）"
    );

    // 聚合形态同样要裁
    let (_, proj) = prune_decision(&plan_of(
        &db,
        "SELECT g, count(x) FROM (SELECT DISTINCT g, x FROM t) GROUP BY g",
    ))
    .expect("应走列裁剪");
    assert_eq!(proj, vec!["g", "x"]);

    // 普通投影（无子查询）——既有能力不能退化
    let (_, proj) = prune_decision(&plan_of(&db, "SELECT g FROM t")).unwrap();
    assert_eq!(proj, vec!["g"]);
    drop(db);
    let _ = std::fs::remove_dir_all(root);
}
