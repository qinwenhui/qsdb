//! 多表 join 的**计划形状**回归（确定性断言，不靠计时）。
//!
//! 背景（2026-10-14，`complex_three_way` 113× 的根因）：**两个 bug 互相掩护**——
//!
//! ① `translate_select_inner` 里 join 循环用「**非限定列数**」当左侧宽度
//!    （`!name.contains('.')`）。第一次 join 时左侧恰好全是裸列，两者相等；
//!    **第二次 join 起**左侧已含上次带进来的限定列（`d.cat`），于是两个 ON 操作数
//!    都被判成"右侧" ⇒ `split_equi_condition` 报错 ⇒ 落到 `CrossJoinFilter`
//!    （叉积 + 过滤）。**结果是对的，代价是 左表行数 × 右表行数**。
//! ② `plan_output_columns` 遇 `LogicalPlan::Join` 直接报错，而 `exec_builder`
//!    在构建 `Join` 时要用它拿左输入宽度（`l_full`）——于是**被正确识别成等值 join 的
//!    多表查询根本绑不出来**。
//!
//! 合起来的效果：三张表以上，**只有退化那条路能跑通**，所以谁都没撞见。
//!
//! 为什么用"形状"而不是"耗时"断言：叉积与 hash join **结果逐行相同**，
//! 值断言（含本仓既有的三表 join 用例）**永远绿**；而耗时断言在 CI 上必然 flaky。
//! 形状是这两者之间唯一稳定的判据——`CrossJoinFilter` 只应出现在**真非等值** ON 上。

use qsdb::sql::translate::{self, LogicalPlan};

/// 绑一条 SELECT，拿回未执行的逻辑计划。
fn plan_of(db: &qsdb::Qsdb, sql: &str) -> LogicalPlan {
    let mut stmts = translate::parse_statements(sql).expect("parse");
    assert_eq!(stmts.len(), 1, "shape 测试一次一条");
    let translate::ParsedStatement::Select(q) = stmts.remove(0) else {
        panic!("不是 SELECT：{sql}");
    };
    let resolver = translate::TableResolver {
        resolve: Box::new(|n: &str| db.table_def(n)),
        views: Default::default(),
    };
    translate::translate_select_full(*q, &resolver).unwrap_or_else(|e| panic!("{sql}: {e}"))
}

/// 数一数计划里的节点种类（递归全树）。
fn count_nodes(p: &LogicalPlan, join: &mut usize, cross: &mut usize) {
    match p {
        LogicalPlan::Join { left, right, .. } => {
            *join += 1;
            count_nodes(left, join, cross);
            count_nodes(right, join, cross);
        }
        LogicalPlan::CrossJoinFilter { left, right, .. } => {
            *cross += 1;
            count_nodes(left, join, cross);
            count_nodes(right, join, cross);
        }
        LogicalPlan::Project { input, .. }
        | LogicalPlan::Filter { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Distinct { input }
        | LogicalPlan::Window { input, .. } => count_nodes(input, join, cross),
        LogicalPlan::Aggregate { input, .. } => count_nodes(input, join, cross),
        LogicalPlan::SetOp { left, right, .. } => {
            count_nodes(left, join, cross);
            count_nodes(right, join, cross);
        }
        _ => {}
    }
}

fn nodes(p: &LogicalPlan) -> (usize, usize) {
    let (mut j, mut c) = (0, 0);
    count_nodes(p, &mut j, &mut c);
    (j, c)
}

fn seeded(tag: &str) -> (qsdb::Qsdb, std::path::PathBuf) {
    let root = std::env::temp_dir().join("qsdb_join_shape").join(format!(
        "{tag}_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let db = qsdb::Qsdb::open(&root, qsdb::QsdbOptions::default()).unwrap();
    db.sql("CREATE TABLE t (g BIGINT, v BIGINT)").unwrap();
    db.sql("CREATE TABLE d (g BIGINT, cat STRING)").unwrap();
    db.sql("CREATE TABLE d2 (cat STRING, alias STRING)")
        .unwrap();
    // d3 的 join 键是**第二列**：`l_full`（左输入宽度）算错时会静默连错列
    db.sql("CREATE TABLE d3 (note STRING, alias STRING)")
        .unwrap();
    db.sql("INSERT INTO t VALUES (1,10),(2,20),(3,30)").unwrap();
    db.sql("INSERT INTO d VALUES (1,'a'),(2,'b'),(3,'a')")
        .unwrap();
    db.sql("INSERT INTO d2 VALUES ('a','A'),('b','B')").unwrap();
    db.sql("INSERT INTO d3 VALUES ('x','A'),('y','B')").unwrap();
    (db, root)
}

/// **主回归**：三表等值 join 必须是两级 hash join，不许有叉积。
///
/// 这是 `complex_three_way`（113×）的形状——`JOIN d2 ON d.cat = d2.cat`
/// 的 `d.cat` 来自上一次 join，正是把 `n_left` 算成"非限定列数"时出错的位置。
#[test]
fn three_table_equi_join_is_not_a_cross_product() {
    let (db, root) = seeded("three");
    let p = plan_of(
        &db,
        "SELECT d2.alias, count(*) c FROM t JOIN d ON t.g = d.g \
         JOIN d2 ON d.cat = d2.cat GROUP BY d2.alias",
    );
    let (j, c) = nodes(&p);
    assert_eq!(j, 2, "三表等值 join 应有 2 个 Join 节点：{p:?}");
    assert_eq!(
        c, 0,
        "等值 join **不许**退化成叉积（结果会同、代价是乘积）：{p:?}"
    );
    std::fs::remove_dir_all(&root).ok();
}

/// 四表链式：每多一张表多一级 Join，叉积计数恒为 0（`n_left` 需**逐次**重算）。
#[test]
fn four_table_chain_stays_hash_joins() {
    let (db, root) = seeded("four");
    let p = plan_of(
        &db,
        "SELECT d3.note, count(*) c FROM t JOIN d ON t.g = d.g \
         JOIN d2 ON d.cat = d2.cat JOIN d3 ON d2.alias = d3.alias GROUP BY d3.note",
    );
    let (j, c) = nodes(&p);
    assert_eq!(j, 3, "四表应有 3 个 Join 节点：{p:?}");
    assert_eq!(c, 0, "第四张表起不许掉回叉积：{p:?}");
    std::fs::remove_dir_all(&root).ok();
}

/// 单表 join 仍走 hash join（基线，防止改坏）。
#[test]
fn single_join_is_hash_join() {
    let (db, root) = seeded("single");
    let p = plan_of(&db, "SELECT count(*) FROM t JOIN d ON t.g = d.g");
    assert_eq!(nodes(&p), (1, 0), "{p:?}");
    std::fs::remove_dir_all(&root).ok();
}

/// **对照组**：真非等值 ON 仍走 `CrossJoinFilter`——这是**有意保留**的回退，
/// 不是"一刀切把叉积删了"（否则非等值 join 会变成不支持）。
#[test]
fn non_equi_join_still_uses_cross_join_filter() {
    let (db, root) = seeded("nonequi");
    let p = plan_of(&db, "SELECT count(*) FROM t JOIN d ON t.g < d.g");
    assert_eq!(nodes(&p), (0, 1), "非等值 ON 应仍走叉积 + 过滤：{p:?}");
    std::fs::remove_dir_all(&root).ok();
}

/// `l_full` 正确性的**值断言**：右键是右表第二列时，序号换算错会连到错的列。
#[test]
fn right_key_index_maps_to_the_right_column() {
    let (db, root) = seeded("lfull");
    let bs = db
        .sql_collect(
            "SELECT d3.note, count(*) c FROM t JOIN d ON t.g = d.g \
             JOIN d2 ON d.cat = d2.cat JOIN d3 ON d2.alias = d3.alias GROUP BY d3.note",
        )
        .unwrap();
    let mut got: Vec<(String, i64)> = Vec::new();
    for b in &bs {
        for i in 0..b.num_rows() {
            got.push((
                arrow::util::display::array_value_to_string(b.column(0).as_ref(), i).unwrap(),
                b.column(1)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .unwrap()
                    .value(i),
            ));
        }
    }
    got.sort();
    // t(1,a)(2,b)(3,a) → d2: a→A, b→B → d3: A→x, B→y ⇒ x 计 2 行、y 计 1 行
    assert_eq!(
        got,
        vec![("x".to_string(), 2), ("y".to_string(), 1)],
        "键列序号换算错会静默连到别的列"
    );
    std::fs::remove_dir_all(&root).ok();
}

/// 限定名写法的**两种顺序**都要能识别（`d.cat = d2.cat` 与 `d2.cat = d.cat`）。
#[test]
fn on_operand_order_does_not_matter() {
    let (db, root) = seeded("order");
    for sql in [
        "SELECT count(*) FROM t JOIN d ON t.g = d.g JOIN d2 ON d.cat = d2.cat",
        "SELECT count(*) FROM t JOIN d ON t.g = d.g JOIN d2 ON d2.cat = d.cat",
    ] {
        let p = plan_of(&db, sql);
        assert_eq!(nodes(&p), (2, 0), "{sql}：{p:?}");
    }
    std::fs::remove_dir_all(&root).ok();
}
