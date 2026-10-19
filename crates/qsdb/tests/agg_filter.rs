//! **生态功能批次 3：聚合 `FILTER (WHERE …)`** 的逐值验收。
//!
//! 此前它是**静默忽略**的 —— `count(*) FILTER (WHERE a>1)` 被当成 `count(*)` 跑，
//! 回的是**全量行数**（实测 4 而非 2），不报错、不打日志。"完全合法的 SQL、
//! 悄悄给个错数字"这一类（2026-10-11 的静默丢弃扫描里它是头号案例）。
//!
//! 本文件除了取值口径，还专门钉住**三条会被绕过的捷径**（都是实测撞出来的：
//! 每条都各自让 FILTER 静默失效，而且是**不同的**失效方式）：
//! ① `count(*)` 的**元数据短路**（直接回 parquet footer 行数，压根不看行）；
//! ② `count(*) over INNER JOIN` 的**计数下推**（只按键计数）；
//! ③ `count(*) over DISTINCT` 的**融合计数**（整行计数）。
//! 这三条都不"报错"，只会**回个大数字** —— 所以每条都得有值级断言。

use qsdb::{Qsdb, QsdbOptions};

fn setup(tag: &str) -> (Qsdb, std::path::PathBuf) {
    let root = std::env::temp_dir().join("qsdb_aggfilter").join(format!(
        "{tag}_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    db.sql("CREATE TABLE t (g BIGINT, v BIGINT)").unwrap();
    db.sql("INSERT INTO t VALUES (1,10),(1,30),(2,20),(2,5),(3,NULL)")
        .unwrap();
    db.sql("CREATE TABLE u (g BIGINT, w BIGINT)").unwrap();
    db.sql("INSERT INTO u VALUES (1,100),(2,200),(4,400)")
        .unwrap();
    (db, root)
}

fn q(db: &Qsdb, sql: &str) -> Vec<String> {
    let batches = db.sql_collect(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    let mut out = Vec::new();
    for b in &batches {
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

fn err_of(db: &Qsdb, sql: &str) -> String {
    match db.sql(sql) {
        Ok(_) => panic!("这条必须被拒: {sql}"),
        Err(e) => e.to_string(),
    }
}

// ── ① 那个最初的错值案例 ─────────────────────────────────────────────────
#[test]
fn 静默忽略时的错值被钉死() {
    let (db, root) = setup("basic");
    // 文档记的正是这一条：静默忽略时回 5（全量），正确答案是 2（v=30 与 20）
    assert_eq!(
        q(&db, "SELECT count(*) FILTER (WHERE v > 15) FROM t"),
        ["2"]
    );
    assert_eq!(q(&db, "SELECT count(*) FROM t"), ["5"]);
    // 同一条 SELECT 里带与不带 FILTER 并存
    assert_eq!(
        q(
            &db,
            "SELECT count(*) FILTER (WHERE v > 15), count(*) FROM t"
        ),
        ["2|5"]
    );
    let _ = std::fs::remove_dir_all(&root);
}

// ── ② 两个不同谓词**不许**被去重成同一个聚合 ─────────────────────────────
//
// `collect_aggs` 按 `out_name` 去重、而 `out_name` 一直是 `count(*)` ⇒ 不加区分
// 就会把两个语义不同的聚合并成一个，**两列拿到同一个数字**。
// 与窗口帧那次的 `window_spec_sig` 是同一类坑（去重键漏了语义维度）。
#[test]
fn 不同谓词不许被去重() {
    let (db, root) = setup("dedup");
    assert_eq!(
        q(
            &db,
            "SELECT count(*) FILTER (WHERE v > 15), count(*) FILTER (WHERE v < 15) FROM t"
        ),
        // v>15: 30,20 ⇒ 2；v<15: 10,5 ⇒ 2。被去重的话会回 "2|2" 之外的错配
        ["2|2"]
    );
    // 为了让"被去重"必然暴露，再取一组**不相等**的
    assert_eq!(
        q(
            &db,
            "SELECT count(*) FILTER (WHERE v > 5), count(*) FILTER (WHERE v > 25) FROM t"
        ),
        // v>5: 10,30,20 ⇒ 3；v>25: 30 ⇒ 1
        ["3|1"]
    );
    let _ = std::fs::remove_dir_all(&root);
}

// ── ③ 分组：**全不匹配的组照样出现**，回 0 / NULL ────────────────────────
//
// 谓词只决定"这行喂不喂给这个聚合"，**不决定组存不存在**（PG 同样）。
#[test]
fn 分组与非匹配组() {
    let (db, root) = setup("group");
    assert_eq!(
        q(
            &db,
            "SELECT g, count(*) FILTER (WHERE v > 15) FROM t GROUP BY g ORDER BY g"
        ),
        // g=3 那行 v 是 NULL ⇒ 谓词为 UNKNOWN ⇒ 0，但**组仍在**
        ["1|1", "2|1", "3|0"]
    );
    assert_eq!(
        q(
            &db,
            "SELECT g, sum(v) FILTER (WHERE v > 15), sum(v) FROM t GROUP BY g ORDER BY g"
        ),
        // g=3 一个都没通过 ⇒ sum 为 NULL（不是 0）；不带 FILTER 的 sum 同样 NULL
        ["1|30|40", "2|20|25", "3|NULL|NULL"]
    );
    // HAVING 里也能用
    assert_eq!(
        q(
            &db,
            "SELECT g FROM t GROUP BY g HAVING count(*) FILTER (WHERE v > 15) > 0 ORDER BY g"
        ),
        ["1", "2"]
    );
    let _ = std::fs::remove_dir_all(&root);
}

// ── ④ NULL 语义：谓词为 UNKNOWN 不通过；参数 NULL 各种聚合各按各的规矩 ──
#[test]
fn 空值语义() {
    let (db, root) = setup("nulls");
    // 第 5 行 v=NULL ⇒ `v > 0` 是 UNKNOWN ⇒ 不通过（5 行里通过 4 行）
    assert_eq!(q(&db, "SELECT count(*) FILTER (WHERE v > 0) FROM t"), ["4"]);
    assert_eq!(q(&db, "SELECT count(v) FILTER (WHERE v > 0) FROM t"), ["4"]);
    // FILTER 与 count(x) 的 NULL 口径是**两件事**，别混：谓词放行的行里，
    // count(x) 仍然不数 x 为 NULL 的
    assert_eq!(
        q(&db, "SELECT count(*) FILTER (WHERE v IS NULL) FROM t"),
        ["1"]
    );
    assert_eq!(
        q(&db, "SELECT count(v) FILTER (WHERE v IS NULL) FROM t"),
        ["0"]
    );
    let _ = std::fs::remove_dir_all(&root);
}

// ── ⑤ 各聚合都认 FILTER ─────────────────────────────────────────────────
#[test]
fn 各聚合都认() {
    let (db, root) = setup("funcs");
    assert_eq!(
        q(
            &db,
            "SELECT sum(v) FILTER (WHERE v > 15), min(v) FILTER (WHERE v > 15), \
                    max(v) FILTER (WHERE v > 15), avg(v) FILTER (WHERE v > 15) FROM t"
        ),
        ["50|20|30|25.0"]
    );
    // 与 WHERE 组合：WHERE 先筛掉整行，FILTER 再筛该聚合
    assert_eq!(
        q(
            &db,
            "SELECT count(*) FILTER (WHERE v > 15) FROM t WHERE g = 1"
        ),
        ["1"]
    );
    assert_eq!(
        q(
            &db,
            "SELECT sum(v) FILTER (WHERE v > 0), sum(v) FROM t WHERE g > 1"
        ),
        ["25|25"]
    );
    let _ = std::fs::remove_dir_all(&root);
}

// ── ⑥ 三条**捷径**各自不许绕过 FILTER（每条都是实测撞出来的）────────────
#[test]
fn 三条捷径都不许绕过() {
    let (db, root) = setup("shortcut");
    // 捷径①：`count(*)` 的元数据短路（直接回 parquet footer 行数）
    assert_eq!(
        q(&db, "SELECT count(*) FILTER (WHERE v > 15) FROM t"),
        ["2"]
    );

    // 捷径②：`count(*) over INNER JOIN` 的计数下推（只按键计数）
    // join 匹配 4 对（t 里 g∈{1,2} 的 4 行各配到 u 的同 g 行），其中 v>15 的有 2 对
    assert_eq!(
        q(
            &db,
            "SELECT count(*) FILTER (WHERE a.v > 15) FROM t a JOIN u b ON a.g = b.g"
        ),
        ["2"]
    );
    assert_eq!(
        q(&db, "SELECT count(*) FROM t a JOIN u b ON a.g = b.g"),
        ["4"]
    );

    // 捷径③：`count(*) over DISTINCT` 的融合计数
    assert_eq!(
        q(
            &db,
            "SELECT count(*) FILTER (WHERE g > 1) FROM (SELECT DISTINCT g FROM t) d"
        ),
        // 去重后 g ∈ {1,2,3}，其中 >1 的有 2 个
        ["2"]
    );
    assert_eq!(
        q(&db, "SELECT count(*) FROM (SELECT DISTINCT g FROM t) d"),
        ["3"]
    );
    let _ = std::fs::remove_dir_all(&root);
}

// ── ⑥b 溢出/分区路径下的 FILTER 与内存路径逐值相同 ───────────────────────
//
// 溢出路径会按分区键把行**再喂一遍**（`spill_next` 里对每个分区重新 `ingest`）
// ⇒ 任何"只算一次"的假设都会在这里露馅。FILTER 是按行筛的，理论上天然成立，
// 但**理论不算证据**（本仓在 join 残留谓词上就栽过"溢出路径漏了判否"）。
#[test]
fn 溢出路径逐值相同() {
    let root = std::env::temp_dir().join("qsdb_aggfilter").join(format!(
        "spill_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    db.sql("CREATE TABLE big (g BIGINT, v BIGINT)").unwrap();
    let schema = std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("g", arrow::datatypes::DataType::Int64, true),
        arrow::datatypes::Field::new("v", arrow::datatypes::DataType::Int64, true),
    ]));
    let rows = 40_000i64;
    let per = 500i64;
    let mut done = 0i64;
    while done < rows {
        let n = per.min(rows - done);
        let b = arrow::record_batch::RecordBatch::try_new(
            std::sync::Arc::clone(&schema),
            vec![
                std::sync::Arc::new(arrow::array::Int64Array::from(
                    (0..n).map(|i| (done + i) % 500).collect::<Vec<i64>>(),
                )),
                std::sync::Arc::new(arrow::array::Int64Array::from(
                    (0..n).map(|i| done + i).collect::<Vec<i64>>(),
                )),
            ],
        )
        .unwrap();
        db.append("big", &[b]).unwrap();
        done += n;
    }
    let ctx = |limit: Option<u64>| match limit {
        Some(l) => qsdb::query::QueryContext::new("af", None, Some(l)),
        None => qsdb::query::QueryContext::new("af-free", None, None),
    };
    let run = |sql: &str, l: Option<u64>| -> Vec<String> {
        let batches = match db.sql_with_context(sql, &ctx(l)).unwrap() {
            qsdb::sql::SqlResult::Query { batches, .. } => batches,
            other => panic!("{other:?}"),
        };
        let mut v: Vec<String> = batches
            .iter()
            .flat_map(|b| {
                (0..b.num_rows())
                    .map(|i| {
                        (0..b.num_columns())
                            .map(|c| {
                                let a = b.column(c);
                                if a.is_null(i) {
                                    "NULL".into()
                                } else {
                                    arrow::util::display::array_value_to_string(a.as_ref(), i)
                                        .unwrap()
                                }
                            })
                            .collect::<Vec<_>>()
                            .join("|")
                    })
                    .collect::<Vec<_>>()
            })
            .collect();
        v.sort();
        v
    };
    for sql in [
        "SELECT g, count(*) FILTER (WHERE v % 3 = 0), sum(v) FILTER (WHERE v > 20000) \
         FROM big GROUP BY g",
        "SELECT count(*) FILTER (WHERE v % 7 = 0) FROM big",
        "SELECT g, count(*) FILTER (WHERE v % 3 = 0), count(*) FROM big GROUP BY g",
    ] {
        let free = run(sql, None);
        assert!(!free.is_empty(), "不限内存应有结果: {sql}");
        let spilled = run(sql, Some(64 * 1024));
        assert_eq!(free, spilled, "溢出路径必须与内存路径逐值相同\nSQL: {sql}");
    }
    let _ = std::fs::remove_dir_all(&root);
}

// ── ⑦ 拒绝面：没实现的形态一律显式拒（绝不静默丢掉谓词）────────────────
#[test]
fn 拒绝面() {
    let (db, root) = setup("reject");
    // 谓词不是布尔
    let e = err_of(&db, "SELECT sum(v) FILTER (WHERE g) FROM t");
    assert!(e.contains("必须是布尔"), "{e}");
    // 谓词恒 UNKNOWN ⇒ 聚合永远回空，几乎必然是笔误
    let e = err_of(&db, "SELECT sum(v) FILTER (WHERE NULL) FROM t");
    assert!(e.contains("恒 UNKNOWN"), "{e}");
    // 窗口 FILTER 未实现（静默丢掉 = 回全量）
    let e = err_of(
        &db,
        "SELECT sum(v) FILTER (WHERE v>0) OVER (ORDER BY g) FROM t",
    );
    assert!(e.contains("窗口函数上的 FILTER"), "{e}");
    // DISTINCT + FILTER 同时写：去重改写会换掉输入，谓词引用的列就不在了
    let e = err_of(&db, "SELECT count(DISTINCT v) FILTER (WHERE v > 0) FROM t");
    assert!(e.contains("DISTINCT") && e.contains("FILTER"), "{e}");
    // 标量函数不吃 FILTER
    let e = err_of(&db, "SELECT abs(v) FILTER (WHERE v > 0) FROM t");
    assert!(e.contains("scalar function"), "{e}");
    // 对照组：不带 FILTER 的同一批查询照常
    assert_eq!(q(&db, "SELECT count(DISTINCT v) FROM t"), ["4"]);
    assert_eq!(
        q(&db, "SELECT abs(v) FROM t WHERE v > 0"),
        ["10", "20", "30", "5"]
    );
    let _ = std::fs::remove_dir_all(&root);
}
