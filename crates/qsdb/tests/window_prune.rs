//! **列裁剪穿透窗口**的验收（设计稿：`列裁剪穿透窗口设计稿`）。
//!
//! ## 为什么验收长这样
//!
//! 这类优化的失败模式是**静默指错列**：少裁一列只是变慢，**裁错一列是错值**。
//! 而"结果对不对"看不出裁剪有没有白跑 —— 所以这里两条都要：
//!
//! 1. **两条路对撞**：同一件事跑两遍，一条**走裁剪**（投影里只写用得着的列 ⇒
//!    `prune_decision` 会重写整棵计划的 idx），一条**不走**（把所有列都写进投影
//!    ⇒ `proj == all` ⇒ `build()` 直接跳过重写）。**逐值**必须一致。
//!    这是本仓对这类优化的标准做法 —— 少了它，"实现错了但碰巧值相同"与
//!    "实现对了"从外面**完全分不出来**。
//! 2. **裁剪确实发生**（`prune_decision` 直接断言）：否则第 1 条会因为"两条路
//!    其实都没裁"而**永远绿**。
//!
//! ## 窗口输出空间的两段（第一版就栽在这）
//!
//! `Window` 输出 = 输入 ++ 窗口列。所以"窗口之上"的表达式里：
//! - `< 表宽` 的是**透传列** ⇒ 位置仍是表列序；
//! - `>= 表宽` 的是**追加的窗口列** ⇒ 在尾部整体左移。
//!
//! 第一版按"整段 −k"统一平移，透传列 `i` 被减成了隔壁 `g` 列 ——
//! `window_frame` 的 `count_星号数行而_count_列数非空` 首列变成恒定 1 当场变红。
//! 下面 `透传列与窗口列同在一个投影里` 就是钉这一条的。

use qsdb::sql::exec_builder::prune_decision;
use qsdb::sql::translate::{self, ParsedStatement};
use qsdb::{Qsdb, QsdbOptions};

fn tmp(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join("qsdb_window_prune").join(format!(
        "{tag}_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

/// `t(id, g, x, s)`，`id` 与 `x` 都**唯一**（行序可判、帧边界无并列）。
///
/// | id | g | x | s |
/// |---|---|---|---|
/// | 1 | a | 5 | p |
/// | 2 | a | 1 | q |
/// | 3 | b | 4 | r |
/// | 4 | b | 2 | s |
/// | 5 | c | 3 | t |
fn setup(tag: &str) -> (Qsdb, std::path::PathBuf) {
    let root = tmp(tag);
    std::fs::create_dir_all(&root).unwrap();
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    db.sql("CREATE TABLE t (id BIGINT, g VARCHAR, x BIGINT, s VARCHAR)")
        .unwrap();
    db.sql(
        "INSERT INTO t VALUES (1,'a',5,'p'),(2,'a',1,'q'),(3,'b',4,'r'),\
         (4,'b',2,'s'),(5,'c',3,'t')",
    )
    .unwrap();
    (db, root)
}

/// 取指定列的逐行值（`列号|列号…`）。**保持引擎给的行序** —— 两条路必须连序都一致。
fn cols(db: &Qsdb, sql: &str, keep: &[usize]) -> Vec<String> {
    let bs = db.sql_collect(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    let mut out = Vec::new();
    for b in &bs {
        for i in 0..b.num_rows() {
            out.push(
                keep.iter()
                    .map(|&c| {
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

// ══ ① 两条路对撞 ═══════════════════════════════════════════════════════════

/// ★ 透传列（`id`）与窗口列（`sum(x)`）**同在一个投影里** —— 窗口输出空间的两段
/// 一次都走到。第一版的"整段平移"就是在这里错的。
#[test]
fn 透传列与窗口列同在一个投影里() {
    let (db, root) = setup("both");
    // 裁剪路：used = {id, x, sum(x)} ⇒ proj = [id, x]（g/s 被裁掉）
    let pruned = cols(
        &db,
        "SELECT id, sum(x) OVER (ORDER BY x) AS w FROM t",
        &[0, 1],
    );
    // 对照路：把 4 列全写进投影 ⇒ proj == all ⇒ 完全不重写
    let plain = cols(
        &db,
        "SELECT id, g, x, s, sum(x) OVER (ORDER BY x) AS w FROM t",
        &[0, 4],
    );
    assert_eq!(pruned, plain, "裁剪路与对照路必须逐行逐值一致");
    // 顺带钉住值本身（防止"两条路一起错"）
    // 按 x 升序：x=1(id2) 2(id4) 3(id5) 4(id3) 5(id1) ⇒ 累计和 1,3,6,10,15
    assert_eq!(
        pruned,
        vec!["2|1", "4|3", "5|6", "3|10", "1|15"],
        "窗口累计和（x 升序）"
    );
    drop(db);
    let _ = std::fs::remove_dir_all(root);
}

/// 真表那句的形状：`Project(Window(Aggregate(Scan)))`。
///
/// 这里**窗口输出的左移量是 0**（聚合另起列空间，宽度与裁剪无关）——
/// 与上一条走的是完全不同的分支，必须各测一遍。
#[test]
fn 聚合之上的窗口() {
    let (db, root) = setup("agg");
    let pruned = cols(
        &db,
        "SELECT g, sum(x) AS f, count(*) OVER (ORDER BY g) AS c FROM t GROUP BY g",
        &[0, 1, 2],
    );
    // 对照：把 id 与 s 也读进来（`min(id)`/`max(s)`）⇒ proj == all
    let plain = cols(
        &db,
        "SELECT g, sum(x) AS f, count(*) OVER (ORDER BY g) AS c, \
         min(id) AS mi, max(s) AS ms FROM t GROUP BY g",
        &[0, 1, 2],
    );
    assert_eq!(pruned, plain);
    assert_eq!(
        pruned,
        vec!["a|6|1", "b|6|2", "c|3|3"],
        "g 升序：a(6, 累计 1 组) b(6, 2 组) c(3, 3 组)"
    );
    drop(db);
    let _ = std::fs::remove_dir_all(root);
}

/// 谓词被下推进 `Scan` 的形状（`WHERE id > 1`）+ `PARTITION BY`。
#[test]
fn 带分区与谓词的窗口() {
    let (db, root) = setup("part");
    let pruned = cols(
        &db,
        "SELECT g, sum(x) OVER (PARTITION BY g ORDER BY x) AS w FROM t WHERE id > 1",
        &[0, 1],
    );
    let plain = cols(
        &db,
        "SELECT g, sum(x) OVER (PARTITION BY g ORDER BY x) AS w, s FROM t WHERE id > 1",
        &[0, 1],
    );
    assert_eq!(pruned, plain);
    drop(db);
    let _ = std::fs::remove_dir_all(root);
}

/// 窗口之上还有**外层的 `ORDER BY`**（`Sort(Project(Window(Scan)))`）。
///
/// **真表上最自然的写法就是它**（`.14` 那三条查询尾巴上都挂着 `ORDER BY pdate`）；
/// 设计稿 §4.3 原本只放行 `Project`/`Limit`，放行 `Sort` 的理由见 `phase1_window`：
/// `Sort` 的键活在 `Project` 的输出空间里，而那个空间**位置与含义都不变**。
///
/// 这里连**行序**一起对撞 —— 排序键万一被改错，值可能还对、**序必错**，
/// 而只看"值集合相等"是抓不到的。
#[test]
fn 窗口之上有_order_by() {
    let (db, root) = setup("orderby");
    // 键是**已投影**的列 ⇒ `Sort(Project(Window(Scan)))`
    let pruned = cols(
        &db,
        "SELECT id, sum(x) OVER (PARTITION BY g) AS w FROM t ORDER BY id DESC",
        &[0, 1],
    );
    let plain = cols(
        &db,
        "SELECT id, g, x, s, sum(x) OVER (PARTITION BY g) AS w FROM t ORDER BY id DESC",
        &[0, 4],
    );
    assert_eq!(pruned, plain);
    // 键是**未投影**的列 ⇒ translate 会在 Sort 之上再补一层收回隐藏列的 Project，
    // 形状变成 `Project(Sort(Project(Window(Scan))))`（链更长，也要能过闸）
    let pruned = cols(
        &db,
        "SELECT id, sum(x) OVER (PARTITION BY g) AS w FROM t ORDER BY s",
        &[0, 1],
    );
    let plain = cols(
        &db,
        "SELECT id, g, x, s, sum(x) OVER (PARTITION BY g) AS w FROM t ORDER BY s",
        &[0, 4],
    );
    assert_eq!(pruned, plain);
    drop(db);
    let _ = std::fs::remove_dir_all(root);
}

/// `Limit` 叠在窗口之上（设计稿放行的 `(Project|Limit)*`）。
#[test]
fn 窗口之上有_limit() {
    let (db, root) = setup("limit");
    let pruned = cols(
        &db,
        "SELECT id, sum(x) OVER (ORDER BY x) AS w FROM t LIMIT 3",
        &[0, 1],
    );
    let plain = cols(
        &db,
        "SELECT id, g, x, s, sum(x) OVER (ORDER BY x) AS w FROM t LIMIT 3",
        &[0, 4],
    );
    assert_eq!(pruned, plain);
    drop(db);
    let _ = std::fs::remove_dir_all(root);
}

/// `DECIMAL` 列上对撞 —— 真实业务表的核心列全是 `DECIMAL(30,8)`。
///
/// 错位取到**另一列 DECIMAL** 时值仍然"像个正常的数"，肉眼看不出来 ⇒ 这条不是多余的。
#[test]
fn decimal_列上对撞() {
    let root = tmp("dec");
    std::fs::create_dir_all(&root).unwrap();
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    db.sql("CREATE TABLE m (id BIGINT, p VARCHAR, k VARCHAR, v DECIMAL(30,8))")
        .unwrap();
    db.sql(
        "INSERT INTO m VALUES \
         (1,'x','a',1.5),(2,'y','b',22.25),(3,'z','c',-3.125),(4,'w','d',400.0)",
    )
    .unwrap();
    let pruned = cols(
        &db,
        "SELECT id, sum(v) OVER (ORDER BY id) AS s FROM m",
        &[0, 1],
    );
    let plain = cols(
        &db,
        "SELECT id, p, k, v, sum(v) OVER (ORDER BY id) AS s FROM m",
        &[0, 4],
    );
    assert_eq!(pruned, plain);
    assert_eq!(
        pruned,
        vec![
            "1|1.50000000",
            "2|23.75000000",
            "3|20.62500000",
            "4|420.62500000"
        ],
        "i256 精确累加，分毫不能差"
    );
    drop(db);
    let _ = std::fs::remove_dir_all(root);
}

// ══ ② 裁剪确实发生（否则上面全绿也说明不了什么）═══════════════════════════

#[test]
fn 窗口计划现在真的会被裁剪() {
    let (db, root) = setup("prunes");
    let (all, proj) = prune_decision(&plan_of(
        &db,
        "SELECT id, sum(x) OVER (ORDER BY x) AS w FROM t",
    ))
    .expect("这条计划应走列裁剪");
    assert_eq!(all, vec!["id", "g", "x", "s"]);
    assert_eq!(proj, vec!["id", "x"], "g/s 没人读，应被裁掉");

    // 聚合形态：`GROUP BY g` 之上叠窗口
    let (_, proj) = prune_decision(&plan_of(
        &db,
        "SELECT g, sum(x) AS f, count(*) OVER (ORDER BY g) AS c FROM t GROUP BY g",
    ))
    .expect("应走列裁剪");
    assert_eq!(proj, vec!["g", "x"]);

    // 窗口函数**参数**是唯一提到 `x` 的地方（上层只引用输出名 `sum(x)`）——
    // 若没走 `Window` 臂收集参数列，这里会退化成"只读一列"。
    let (_, proj) =
        prune_decision(&plan_of(&db, "SELECT sum(x) OVER () AS w FROM t")).expect("应走列裁剪");
    assert_eq!(proj, vec!["x"], "窗口参数列必须被收集进来");
    drop(db);
    let _ = std::fs::remove_dir_all(root);
}

// ══ ③ 反例：设计稿明确**不**放行的形状，必须仍然是 `None` ═════════════════
//
// 每一个的"为什么不敢开"都在 `phase1_window` 的文档里。放行它们之前，
// 得先证明它的列空间；在那之前**宁可慢**。

#[test]
fn 反例仍然拒绝() {
    let (db, root) = setup("refuse");
    let cases = [
        (
            "窗口之上还有 Aggregate",
            "SELECT count(*) FROM (SELECT sum(x) OVER () AS w FROM t) z",
        ),
        (
            "两个不同 spec 的 Window（链式叠加）",
            "SELECT sum(x) OVER (ORDER BY x) AS a, avg(x) OVER (PARTITION BY g) AS b FROM t",
        ),
        (
            "窗口接 join",
            "SELECT u.id, sum(z.x) OVER () AS w FROM t u JOIN t z ON u.id = z.id",
        ),
        (
            "窗口之上还有 Distinct",
            "SELECT DISTINCT sum(x) OVER () AS w FROM t",
        ),
    ];
    for (why, sql) in cases {
        assert!(
            prune_decision(&plan_of(&db, sql)).is_none(),
            "{why}：这条没放行，`prune_decision` 必须回 None —— {sql}"
        );
    }
    drop(db);
    let _ = std::fs::remove_dir_all(root);
}
