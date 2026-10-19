//! **列裁剪 × ORDER BY** 的回归测试（2026-10-11 发现并修复的两个静默错误）。
//!
//! 背景：`exec_builder` 的列裁剪下推会把「表列序」重写成「扫描投影序」，而
//! ORDER BY 的排序键 idx 指的是**输入自身的输出序**——两个索引空间被混用，于是：
//!
//! 1. **排序静默失效**：`SELECT v, w FROM t ORDER BY v, w`（t 有 g,v,w）里键 `w`
//!    的输出号 1 被当成表列号查映射 → 落到「g 的投影位置」→ 两个键变成同一列，
//!    v 的并列**完全不按 w 排**。无报错、顺序错——最坏的那类 bug。
//! 2. **隐藏排序列出网**：ORDER BY 引用未投影列时会追加 `__sort_N` 隐藏列，
//!    而它**没被收回**：`SELECT v FROM t ORDER BY w` 给客户端回**两列**
//!    （按列数/列名解析的客户端会错位）。
//!
//! 本文件按"能区分对错"的最小数据构造用例：**并列值**（否则排序错了也看不出来）
//! 与**列数断言**（否则泄漏发现不了）。

use qsdb::{Qsdb, QsdbOptions};

/// 测试目录唯一序号：pid 挡跨进程碰撞，序号挡同进程内并发（Windows 时钟
/// 粒度 ~100ns，纳秒后缀在同 tick 内会重复——CI 上真炸过，见 constraints）。
fn _uniq_seq() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    SEQ.fetch_add(1, Ordering::Relaxed)
}

fn setup(tag: &str) -> (Qsdb, std::path::PathBuf) {
    let root = std::env::temp_dir()
        .join("qsdb_ordprune_test")
        .join(format!(
            "{tag}_{}_{}",
            std::process::id(),
            format!("{:x}_{}", std::process::id(), _uniq_seq())
        ));
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    // 表列序 = (g, v, w)：故意让「输出号」与「表列号」错位
    db.sql("CREATE TABLE t (g BIGINT, v BIGINT, w BIGINT)")
        .unwrap();
    db.sql("INSERT INTO t VALUES (1,10,30),(1,10,20),(1,10,40),(2,5,9),(1,20,7)")
        .unwrap();
    (db, root)
}

/// 取值 + 列名（两样都要：顺序错、列数错是两类不同的 bug）。
fn q(db: &Qsdb, sql: &str) -> (Vec<String>, Vec<String>) {
    let batches = db.sql_collect(sql).unwrap();
    let names: Vec<String> = batches
        .first()
        .map(|b| {
            b.schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect()
        })
        .unwrap_or_default();
    let mut rows = Vec::new();
    for b in &batches {
        for i in 0..b.num_rows() {
            let cells: Vec<String> = (0..b.num_columns())
                .map(|c| {
                    let arr = b.column(c);
                    if arr.is_null(i) {
                        "NULL".into()
                    } else {
                        arrow::util::display::array_value_to_string(arr.as_ref(), i).unwrap()
                    }
                })
                .collect();
            rows.push(cells.join("|"));
        }
    }
    (names, rows)
}

/// 用例 1：两个键**都在投影里**，且投影是表的真子集（触发列裁剪）。
/// 并列值 (v=10 三行) 必须按 w 排——修复前这里是 30,20,40（原插入序）。
#[test]
fn pruned_projection_still_sorts_by_second_key() {
    let (db, _root) = setup("subset");

    let (names, rows) = q(&db, "SELECT v, w FROM t ORDER BY v, w");
    assert_eq!(names, vec!["v", "w"], "结果列必须就是投影列");
    assert_eq!(rows, vec!["5|9", "10|20", "10|30", "10|40", "20|7"]);

    // 投影顺序**与排序键顺序不同**：键仍绑定到输出列（v 在前、w 在后），
    // 而不是"第 1 个输出列、第 2 个输出列"
    let (names, rows) = q(&db, "SELECT w, v FROM t ORDER BY v, w");
    assert_eq!(names, vec!["w", "v"]);
    assert_eq!(rows, vec!["9|5", "20|10", "30|10", "40|10", "7|20"]);

    // 降序：v 降 → 20 在前；并列 v=10 内部按 w **降序**
    let (_n, rows) = q(&db, "SELECT v, w FROM t ORDER BY v DESC, w DESC");
    assert_eq!(rows, vec!["20|7", "10|40", "10|30", "10|20", "5|9"]);

    // 全列投影（不触发裁剪）作为对照：两者必须一致
    let (_, all_rows) = q(&db, "SELECT g, v, w FROM t ORDER BY v, w");
    let (_, sub_rows) = q(&db, "SELECT v, w FROM t ORDER BY v, w");
    let all_tail: Vec<String> = all_rows
        .iter()
        .map(|r| r.splitn(3, '|').skip(1).collect::<Vec<_>>().join("|"))
        .collect();
    assert_eq!(all_tail, sub_rows, "裁剪与不裁剪的排序结果必须一致");
}

/// 用例 2：ORDER BY 引用**未投影**列 → 隐藏列不得出网。
#[test]
fn hidden_sort_column_does_not_leak_to_client() {
    let (db, _root) = setup("hidden");

    // 一个投影列 + 一个未投影排序键
    let (names, rows) = q(&db, "SELECT v FROM t ORDER BY w");
    assert_eq!(names, vec!["v"], "隐藏排序列 `__sort_*` 绝不能出网");
    assert_eq!(
        rows,
        vec!["20", "5", "10", "10", "10"],
        "按 w 升序：7,9,20,30,40 → v=20,5,10,10,10"
    );

    // 投影键 + 隐藏键混用
    let (names, rows) = q(&db, "SELECT v FROM t ORDER BY v, w");
    assert_eq!(names, vec!["v"]);
    assert_eq!(rows, vec!["5", "10", "10", "10", "20"]);

    // 别名投影 + 未投影键
    let (names, rows) = q(&db, "SELECT v AS vv FROM t ORDER BY w DESC");
    assert_eq!(names, vec!["vv"]);
    assert_eq!(
        rows,
        vec!["10", "10", "10", "5", "20"],
        "w 降序：40,30,20,9,7 → v=10,10,10,5,20"
    );

    // 表达式投影（类型不能靠占位 Int64——字符串列必须仍是字符串）
    let (names, rows) = q(&db, "SELECT CAST(v AS STRING) AS s FROM t ORDER BY w");
    assert_eq!(names, vec!["s"]);
    assert_eq!(rows, vec!["20", "5", "10", "10", "10"]);
}

/// 用例 3：与 `LIMIT`（TopN 接线）叠加——`Limit{Project{Sort}}` 形状也要走 TopN，
/// 且隐藏列同样不能出网。
#[test]
fn topn_with_hidden_sort_column() {
    let (db, _root) = setup("topn");

    let (names, rows) = q(&db, "SELECT v FROM t ORDER BY w LIMIT 2");
    assert_eq!(names, vec!["v"]);
    assert_eq!(rows, vec!["20", "5"], "w 最小的两行：w=7→v=20、w=9→v=5");

    let (names, rows) = q(&db, "SELECT v, w FROM t ORDER BY v, w LIMIT 3");
    assert_eq!(names, vec!["v", "w"]);
    assert_eq!(rows, vec!["5|9", "10|20", "10|30"]);
}
