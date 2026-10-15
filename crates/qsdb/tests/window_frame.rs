//! **窗口帧 `ROWS BETWEEN a AND b`**（生态功能批次 #1）的逐值验收。
//!
//! 为什么这批非做不可：此前帧是**静默忽略**的 —— `ROWS BETWEEN 1 PRECEDING AND
//! CURRENT ROW` 被当成默认帧（累计）跑，**换个数字、不报错**。BI 的移动平均 /
//! 移动窗口全是这个语法，且**没有等价改写**（等价写法是自连接，没人会那么写）。
//!
//! 语义基准 = PostgreSQL / DuckDB（本仓唯一对标物），逐条手算对照。三条最容易
//! 写错的，这里各钉一条：
//! ① **空帧**（边界全落在当前行之外）⇒ SUM/AVG/MIN/MAX 为 NULL、COUNT 为 0
//!    —— 不能"夹到分区边"（夹了就把不该进帧的行算进来了）；
//! ② **帧不许跨分区**；
//! ③ **min/max 滑窗**（单调队列）与暴力逐值对照。

use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use qsdb::error::Result;
use qsdb::query::QueryContext;
use qsdb::{Qsdb, QsdbOptions};

fn setup(tag: &str) -> (Qsdb, std::path::PathBuf) {
    let root = std::env::temp_dir().join("qsdb_winframe").join(format!(
        "{tag}_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    // s：单分区、递增 —— 手算移动和/移动平均用
    db.sql("CREATE TABLE s (i BIGINT, x BIGINT)").unwrap();
    db.sql("INSERT INTO s VALUES (1,1),(2,2),(3,3),(4,4),(5,5)")
        .unwrap();
    // m：单分区、**非单调** —— 滑窗 min/max 用（单调序列下暴力与队列都对，测不出错）
    db.sql("CREATE TABLE m (i BIGINT, x BIGINT)").unwrap();
    db.sql("INSERT INTO m VALUES (1,5),(2,1),(3,4),(4,2),(5,3)")
        .unwrap();
    // n：两分区（g=1 五行、g=2 两行）+ NULL —— 分区边界与空值口径
    db.sql("CREATE TABLE n (g BIGINT, i BIGINT, x BIGINT)")
        .unwrap();
    db.sql(
        "INSERT INTO n VALUES
         (1,1,10),(1,2,NULL),(1,3,30),(1,4,40),(1,5,50),
         (2,1,100),(2,2,200)",
    )
    .unwrap();
    (db, root)
}

/// 查一行一列的结果（列 0 即为所需值）。
fn q(db: &Qsdb, sql: &str) -> Vec<String> {
    let batches = db.sql_collect(sql).unwrap();
    let mut out = Vec::new();
    for b in &batches {
        for i in 0..b.num_rows() {
            let arr = b.column(0);
            out.push(if arr.is_null(i) {
                "NULL".into()
            } else {
                arrow::util::display::array_value_to_string(arr.as_ref(), i).unwrap()
            });
        }
    }
    out
}

/// 查**全部列** → "c0|c1|…" 串，**排序后**返回。
///
/// 为什么排序：窗口输出行序 = (PARTITION BY, ORDER BY)，把它耦合进值级断言会
/// 让"值对不对"和"行序对不对"混在一起。⚠️ 多分区查询的 SELECT 里**必须带上
/// 分区列**（否则字符串排序会把两个分区的同号行交错，看着像值错了）。
fn q2(db: &Qsdb, sql: &str) -> Vec<String> {
    let batches = db.sql_collect(sql).unwrap();
    let mut out = Vec::new();
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
            out.push(cells.join("|"));
        }
    }
    out.sort();
    out
}

fn err_of(db: &Qsdb, sql: &str) -> String {
    match db.sql(sql) {
        Ok(_) => panic!("这条必须被拒（静默跑下去只会给错值）: {sql}"),
        Err(e) => e.to_string(),
    }
}

// ── ① 居中三行和：帧 = 前后各一行 ──────────────────────────────────────────
#[test]
fn 居中三行和() {
    let (db, root) = setup("centered");
    assert_eq!(
        q2(
            &db,
            "SELECT i, sum(x) OVER (ORDER BY i ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)
             FROM s"
        ),
        // i=1: 1+2=3 / i=2: 1+2+3=6 / i=3: 2+3+4=9 / i=4: 3+4+5=12 / i=5: 4+5=9
        vec!["1|3", "2|6", "3|9", "4|12", "5|9"]
    );
    let _ = std::fs::remove_dir_all(&root);
}

// ── ② 移动平均 ────────────────────────────────────────────────────────────
#[test]
fn 移动平均() {
    let (db, root) = setup("mavg");
    assert_eq!(
        q2(
            &db,
            "SELECT i, avg(x) OVER (ORDER BY i ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
             FROM s"
        ),
        // 1/1=1, (1+2)/2=1.5, 6/3=2, 9/3=3, 12/3=4
        vec!["1|1.0", "2|1.5", "3|2.0", "4|3.0", "5|4.0"]
    );
    let _ = std::fs::remove_dir_all(&root);
}

// ── ③ 显式默认帧 == 隐式默认帧（两种写法必须逐值相同）────────────────────
#[test]
fn 显式默认帧与隐式默认帧逐值相同() {
    let (db, root) = setup("defaulteq");
    let implicit = q2(&db, "SELECT i, sum(x) OVER (ORDER BY i) FROM s");
    let explicit = q2(
        &db,
        "SELECT i, sum(x) OVER (ORDER BY i ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
         FROM s",
    );
    assert_eq!(implicit, vec!["1|1", "2|3", "3|6", "4|10", "5|15"]);
    assert_eq!(implicit, explicit);
    // 不带 BETWEEN 的简写 `ROWS 2 PRECEDING` ≡ `ROWS BETWEEN 2 PRECEDING AND CURRENT ROW`
    assert_eq!(
        q2(
            &db,
            "SELECT i, sum(x) OVER (ORDER BY i ROWS 2 PRECEDING) FROM s"
        ),
        q2(
            &db,
            "SELECT i, sum(x) OVER (ORDER BY i ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM s"
        )
    );
    let _ = std::fs::remove_dir_all(&root);
}

// ── ④ 滑窗 min/max（最容易写错的一条：单调序列测不出错，这里用非单调）──
#[test]
fn 滑窗极值与非单调序列() {
    let (db, root) = setup("minmax");
    // m.x = 5,1,4,2,3
    assert_eq!(
        q2(
            &db,
            "SELECT i, min(x) OVER (ORDER BY i ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)
             FROM m"
        ),
        // min(5,1)=1, min(5,1,4)=1, min(1,4,2)=1, min(4,2,3)=2, min(2,3)=2
        vec!["1|1", "2|1", "3|1", "4|2", "5|2"]
    );
    assert_eq!(
        q2(
            &db,
            "SELECT i, max(x) OVER (ORDER BY i ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)
             FROM m"
        ),
        // max(5,1)=5, 5, max(1,4,2)=4, max(4,2,3)=4, max(2,3)=3
        vec!["1|5", "2|5", "3|4", "4|4", "5|3"]
    );
    // 更宽的帧（3 前 1 后）→ 队列里同时有"该出队"和"该被压掉"的元素
    assert_eq!(
        q2(
            &db,
            "SELECT i, min(x) OVER (ORDER BY i ROWS BETWEEN 3 PRECEDING AND 1 FOLLOWING)
             FROM m"
        ),
        vec!["1|1", "2|1", "3|1", "4|1", "5|1"]
    );
    let _ = std::fs::remove_dir_all(&root);
}

// ── ⑤ 帧不许跨分区 ────────────────────────────────────────────────────────
#[test]
fn 帧不跨分区() {
    let (db, root) = setup("nopart");
    // 整分区帧 = 分区内总和（g=1: 130、g=2: 300），不是全表 430
    assert_eq!(
        q2(
            &db,
            "SELECT g, i, sum(x) OVER (PARTITION BY g ORDER BY i
                                       ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
             FROM n"
        ),
        // 分区内总和（g=1: 10+30+40+50=130、g=2: 100+200=300），不是全表 430
        vec![
            "1|1|130", "1|2|130", "1|3|130", "1|4|130", "1|5|130", "2|1|300", "2|2|300",
        ]
    );
    // 跨边界的滑窗：g=2 的首行**不许**看见 g=1 的最后一行（50）
    assert_eq!(
        q2(
            &db,
            "SELECT g, i, sum(x) OVER (PARTITION BY g ORDER BY i
                                       ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
             FROM n"
        ),
        vec![
            "1|1|10", "1|2|10", // 前一行是 NULL ⇒ 只有 10
            "1|3|30", "1|4|70", "1|5|90", "2|1|100", // 帧 = 只有自己（不是 50+100）
            "2|2|300", // 100 + 200
        ]
    );
    let _ = std::fs::remove_dir_all(&root);
}

// ── ⑥ 帧比分区还长 ⇒ 夹到分区边界（不补 NULL、不越界 panic）──────────────
#[test]
fn 帧长于分区时夹到分区边界() {
    let (db, root) = setup("clamp");
    // s 只有 5 行，帧要 7 前行 ⇒ 与"无界前驱"等价
    assert_eq!(
        q2(
            &db,
            "SELECT i, sum(x) OVER (ORDER BY i ROWS BETWEEN 7 PRECEDING AND CURRENT ROW)
             FROM s"
        ),
        q2(&db, "SELECT i, sum(x) OVER (ORDER BY i) FROM s")
    );
    // 向后取 100 行 ⇒ 该行到分区末尾
    assert_eq!(
        q2(
            &db,
            "SELECT i, sum(x) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND 100 FOLLOWING)
             FROM s"
        ),
        vec!["1|15", "2|14", "3|12", "4|9", "5|5"]
    );
    let _ = std::fs::remove_dir_all(&root);
}

// ── ⑦ 空帧：边界全落在当前行之外 ⇒ SUM/AVG/MIN/MAX = NULL、COUNT = 0 ──────
//
// 这条是"夹边界"与"标准"的分水岭：`3 PRECEDING AND 1 PRECEDING` 在首行是**空帧**
// （PG / DuckDB 给 NULL）。要是把下界回夹到分区首行，就变成"取首行"——错值。
#[test]
fn 空帧给空值而不是夹到分区边() {
    let (db, root) = setup("empty");
    assert_eq!(
        q2(
            &db,
            "SELECT i, sum(x) OVER (ORDER BY i ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING)
             FROM s"
        ),
        // 帧 = 行 [i-3, i-1]：i=1 空 / i=2 取 x1=1 / i=3 取 x1..2=3 /
        //                     i=4 取 x1..3=6 / i=5 取 x2..4=9
        vec!["1|NULL", "2|1", "3|3", "4|6", "5|9"]
    );
    assert_eq!(
        q2(
            &db,
            "SELECT i, avg(x) OVER (ORDER BY i ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING)
             FROM s"
        ),
        // 帧 = 行 [i+1, i+2] 夹到分区末：i=1:(2+3)/2 / i=2:(3+4)/2 / i=3:(4+5)/2 /
        //                              i=4: 只剩 x5=5 / i=5: 越出分区 ⇒ 空 ⇒ NULL
        vec!["1|2.5", "2|3.5", "3|4.5", "4|5.0", "5|NULL"]
    );
    // count 是唯一例外：空帧 = 0（它数的是行）
    assert_eq!(
        q2(
            &db,
            "SELECT i, count(x) OVER (ORDER BY i ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING)
             FROM s"
        ),
        vec!["1|0", "2|1", "3|2", "4|3", "5|3"]
    );
    let _ = std::fs::remove_dir_all(&root);
}

// ── ⑧ NULL 口径：sum/avg 跳过 NULL、count(x) 不计 NULL ───────────────────
#[test]
fn 空值口径() {
    let (db, root) = setup("nulls");
    // n.g=1 的 x = 10, NULL, 30, 40, 50
    assert_eq!(
        q2(
            &db,
            "SELECT i, sum(x) OVER (PARTITION BY g ORDER BY i
                                   ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)
             FROM n WHERE g = 1"
        ),
        // 帧 = 行 [i-1, i+1]：i=1 {10,NULL}=10 / i=2 {10,NULL,30}=40 /
        //                    i=3 {NULL,30,40}=70 / i=4 {30,40,50}=120 / i=5 {40,50}=90
        vec!["1|10", "2|40", "3|70", "4|120", "5|90"]
    );
    assert_eq!(
        q2(
            &db,
            "SELECT i, count(x) OVER (PARTITION BY g ORDER BY i
                                     ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)
             FROM n WHERE g = 1"
        ),
        // count(x) 不计 NULL（帧 {NULL,30,40} 只有 2 个非 NULL）
        vec!["1|1", "2|2", "3|2", "4|3", "5|2"]
    );
    // 帧里一个非 NULL 都没有 ⇒ NULL（不是 0 —— 0 是个会被下游当真的数字）
    assert_eq!(
        q2(
            &db,
            "SELECT i, sum(x) OVER (PARTITION BY g ORDER BY i
                                   ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
             FROM n WHERE g = 1 AND i <= 2"
        ),
        vec!["1|10", "2|10"]
    );
    let (db2, root2) = setup("nulls2");
    db2.sql("CREATE TABLE z (i BIGINT, x BIGINT)").unwrap();
    db2.sql("INSERT INTO z VALUES (1,NULL),(2,NULL),(3,7)")
        .unwrap();
    assert_eq!(
        q2(
            &db2,
            "SELECT i, sum(x) OVER (ORDER BY i ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
             FROM z"
        ),
        vec!["1|NULL", "2|NULL", "3|7"]
    );
    // 与**默认帧**同一口径 —— 两种写法逐值相同（含 NULL 列）
    assert_eq!(
        q2(&db2, "SELECT i, sum(x) OVER (ORDER BY i) FROM z"),
        q2(
            &db2,
            "SELECT i, sum(x) OVER (ORDER BY i ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
             FROM z"
        )
    );
    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::remove_dir_all(&root2);
}

// ── ⑧b `count(*)` 数**行**、`count(x)` 数非 NULL —— 同一帧下必须不同 ───────
//
// 这条同时钉住一个**此前不存在的写法**：`count(*) OVER (…)` 原先报
// "needs an argument"（把"没实现"说成"你写错了"），而支持面文档声称窗口
// `count` 可用 —— F29 那一类。现在 `*` 与 `x` 走同一个 count，只差"计不计 NULL"。
#[test]
fn count_星号数行而_count_列数非空() {
    let (db, root) = setup("countstar");
    // n.g=1 的 x = 10, NULL, 30, 40, 50
    assert_eq!(
        q2(
            &db,
            "SELECT i, count(*) OVER (PARTITION BY g ORDER BY i
                                      ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)
             FROM n WHERE g = 1"
        ),
        // 帧 = 行 [i-1, i+1] 夹边界：都是 2/3/3/3/2 **行**
        vec!["1|2", "2|3", "3|3", "4|3", "5|2"]
    );
    // 对照：同一帧 `count(x)` 少掉 NULL 那一行（i=2、i=3 处差 1）
    assert_eq!(
        q2(
            &db,
            "SELECT i, count(x) OVER (PARTITION BY g ORDER BY i
                                      ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)
             FROM n WHERE g = 1"
        ),
        vec!["1|1", "2|2", "3|2", "4|3", "5|2"]
    );
    // 不带帧（整分区）也要能用 —— 这是 BI 里最常见的"分区内行数"
    assert_eq!(
        q2(&db, "SELECT g, count(*) OVER (PARTITION BY g) FROM n"),
        vec!["1|5", "1|5", "1|5", "1|5", "1|5", "2|2", "2|2"]
    );
    // 输出列名照用户写的来（不是 `count(1)` 之类的内部表示）
    let bs = db
        .sql_collect("SELECT count(*) OVER (PARTITION BY g) FROM n")
        .unwrap();
    let name = bs[0].schema().field(0).name().to_string();
    assert!(
        name.starts_with("count(*)") && !name.contains("count(1)"),
        "列名要照用户写的 `count(*)`，实得 {name:?}"
    );
    // `*` 只有 count 能用：sum(*) 显式拒（别静默当 sum(1)）
    let e = err_of(&db, "SELECT sum(*) OVER (ORDER BY i) FROM n");
    assert!(e.contains("只有 count"), "{e}");
    let _ = std::fs::remove_dir_all(&root);
}

// ── ⑧c 窄类型参数的 `min/max`：输出类型 = **参数类型** ────────────────────
//
// 顺带修掉一个**既有 bug**（这条覆盖默认帧与显式帧两条路）：`min/max` 的声明
// 输出类型是参数类型（`INT` 列 ⇒ `Int32`），而取值路径都按 `i64` 算 ⇒ 类型对不上
// 时 `RecordBatch::try_new` 抛内部错误
// （`column types must match schema types, expected Int32 but found Int64`）。
// 只测 `BIGINT` 的话永远撞不到 —— 所以这里显式用 `INT` / `FLOAT`。
#[test]
fn 窄类型参数的极值输出类型对得上() {
    let (db, root) = setup("narrow");
    db.sql("CREATE TABLE w (i INT, x INT, f FLOAT)").unwrap();
    db.sql("INSERT INTO w VALUES (1,5,2.5),(2,1,1.5),(3,4,3.5)")
        .unwrap();
    // 默认帧
    assert_eq!(
        q2(&db, "SELECT i, min(x) OVER (ORDER BY i) FROM w"),
        vec!["1|5", "2|1", "3|1"]
    );
    assert_eq!(
        q2(&db, "SELECT i, max(f) OVER (ORDER BY i) FROM w"),
        vec!["1|2.5", "2|2.5", "3|3.5"]
    );
    // 显式帧（两条路必须是同一套类型口径，否则只差一个帧就一个能跑一个报错）
    assert_eq!(
        q2(
            &db,
            "SELECT i, min(x) OVER (ORDER BY i ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM w"
        ),
        vec!["1|1", "2|1", "3|1"]
    );
    // sum 仍是 Int64 / Float64（与 `max(int)` 的 Int32 不同 —— 输出类型随函数变）
    let bs = db
        .sql_collect(
            "SELECT sum(x) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND CURRENT ROW) FROM w",
        )
        .unwrap();
    assert_eq!(bs[0].schema().field(0).data_type().to_string(), "Int64");
    let _ = std::fs::remove_dir_all(&root);
}

// ── ⑨ 同一 spec 两个不同帧 ⇒ 不许被去重成一个 ────────────────────────────
//
// `collect_window_calls` 按「调用签名 + spec 签名」去重；帧不在 spec 签名里就会
// 把这两列并成一列，**两列都拿第一个帧的结果**（数字只差一点，极难发现）。
#[test]
fn 同_spec_不同帧不许去重() {
    let (db, root) = setup("dedup");
    let r = q2(
        &db,
        "SELECT i,
                sum(x) OVER (ORDER BY i ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS a,
                sum(x) OVER (ORDER BY i ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS b
         FROM s",
    );
    assert_eq!(
        r,
        vec![
            "1|3|6",   // a=1+2, b=1+2+3
            "2|6|10",  // a=1+2+3, b=1+2+3+4
            "3|9|15",  // a=2+3+4, b=1..5
            "4|12|14", // a=3+4+5, b=2..5
            "5|9|12",  // a=4+5, b=3+4+5
        ]
    );
    let _ = std::fs::remove_dir_all(&root);
}

// ── ⑩ 拒绝面：每条都要能分辨"拒了"与"跑错" ──────────────────────────────
#[test]
fn 该拒的都拒且指路() {
    let (db, root) = setup("reject");
    // RANGE 仍拒（按值分帧是另一件事；当 ROWS 跑会换个数字）
    let e = err_of(
        &db,
        "SELECT sum(x) OVER (ORDER BY i RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM s",
    );
    assert!(e.contains("RANGE"), "{e}");
    // 起点在终点之后 ⇒ 帧恒空，必是笔误
    let e = err_of(
        &db,
        "SELECT sum(x) OVER (ORDER BY i ROWS BETWEEN 3 PRECEDING AND 5 PRECEDING) FROM s",
    );
    assert!(e.contains("after its end"), "{e}");
    // 起点 UNBOUNDED FOLLOWING / 终点 UNBOUNDED PRECEDING
    assert!(
        err_of(
            &db,
            "SELECT sum(x) OVER (ORDER BY i ROWS BETWEEN UNBOUNDED FOLLOWING AND CURRENT ROW) FROM s"
        )
        .contains("UNBOUNDED FOLLOWING")
    );
    assert!(
        err_of(
            &db,
            "SELECT sum(x) OVER (ORDER BY i ROWS BETWEEN CURRENT ROW AND UNBOUNDED PRECEDING) FROM s"
        )
        .contains("UNBOUNDED PRECEDING")
    );
    // 偏移量不是字面量
    assert!(
        err_of(
            &db,
            "SELECT sum(x) OVER (ORDER BY i ROWS BETWEEN i PRECEDING AND CURRENT ROW) FROM s"
        )
        .contains("integer literal")
    );
    // 排名函数 + 帧 ⇒ 显式拒（标准说忽略帧；本仓不静默忽略）
    let e = err_of(
        &db,
        "SELECT row_number() OVER (ORDER BY i ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM s",
    );
    assert!(e.contains("does not take a frame"), "{e}");
    // 无 ORDER BY 的按行分帧 ⇒ 行序不确定，必须拒（整分区那种放行）
    let e = err_of(
        &db,
        "SELECT sum(x) OVER (ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM s",
    );
    assert!(e.contains("without ORDER BY"), "{e}");
    assert!(
        !q(
            &db,
            "SELECT sum(x) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM s"
        )
        .is_empty()
    );
    let _ = std::fs::remove_dir_all(&root);
}

// ── ⑪ 溢出路径与内存路径逐值相同（帧在分区内算，与内存路径同一段代码）──
#[test]
fn 溢出路径逐值相同() {
    let root = std::env::temp_dir().join("qsdb_winframe").join(format!(
        "spill_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    db.sql("CREATE TABLE t (g BIGINT, v BIGINT)").unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("g", DataType::Int64, true),
        Field::new("v", DataType::Int64, true),
    ]));
    let rows = 20_000i64;
    let per = 500i64;
    let mut done = 0i64;
    while done < rows {
        let n = per.min(rows - done);
        let b = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(
                    (0..n).map(|i| (done + i) % 200).collect::<Vec<i64>>(),
                )),
                Arc::new(Int64Array::from(
                    (0..n).map(|i| done + i).collect::<Vec<i64>>(),
                )),
            ],
        )
        .unwrap();
        db.append("t", &[b]).unwrap();
        done += n;
    }
    let ctx = |limit: Option<u64>| match limit {
        Some(l) => QueryContext::new("wf", None, Some(l)),
        None => QueryContext::new("wf-free", None, None),
    };
    let run = |sql: &str, l: Option<u64>| -> Result<Vec<String>> {
        let batches = match db.sql_with_context(sql, &ctx(l))? {
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
        Ok(v)
    };
    for sql in [
        "SELECT g, v, sum(v) OVER (PARTITION BY g ORDER BY v ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) \
         FROM t",
        "SELECT g, v, min(v) OVER (PARTITION BY g ORDER BY v ROWS BETWEEN 3 PRECEDING AND 2 FOLLOWING) \
         FROM t",
        "SELECT g, v, avg(v) OVER (PARTITION BY g ORDER BY v ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) \
         FROM t",
        "SELECT g, v, count(v) OVER (PARTITION BY g ORDER BY v ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) \
         FROM t",
    ] {
        let free = run(sql, None).unwrap();
        assert!(!free.is_empty(), "不限内存应有结果: {sql}");
        let spilled = run(sql, Some(64 * 1024)).unwrap();
        assert_eq!(free, spilled, "溢出路径必须与内存路径逐值相同\nSQL: {sql}");
    }
    let _ = std::fs::remove_dir_all(&root);
}
