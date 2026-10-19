//! **`count(*) over DISTINCT` 的融合计数节点**（`exec::DistinctCountExec`）回归。
//!
//! 形状（`build_aggregate` 里认）：
//!
//! ```text
//! Aggregate{ group: [], aggs: [count(*)] } ← Distinct ← X
//! ```
//!
//! 语义 = "X 的不同行数"。基准那句 `SELECT count(*) FROM (SELECT DISTINCT gs FROM t)`
//! 与绑定层从 `count(DISTINCT x)` 改写出来的计划都长这样，所以两条路都要测。
//!
//! ## 为什么必须有这些断言
//!
//! 1. **融合静默不触发，正确性测试一条都不会红**（只是变慢）⇒ 单独的
//!    `distinct_fuse_applies` 形状断言；少了它，优化白写没人知道。
//! 2. 它替代的是 `Aggregate ← Distinct` **两层**，输出 schema 必须换成聚合的
//!    （曾经写成输入 schema 就是这类错）。
//! 3. **溢出重分区**用的是键字节口径：内存侧（`key_of`）与批侧（`encode_key_row`）
//!    一旦口径不同，同一个键会落进两个分区、各算一次 ⇒ **数值偏大且不报错**。
//!    所以单有一条 `values_survive_spill_repartition`（40 万行 + 1MB 预算）。
//!    ⚠️ 那条的"规模"是它成立的前提 —— 小数据下它**根本不会溢出**，
//!    等于没测（这坑我踩过一次，见该测试的注释）。
//!
//! ## 对照写法（不融合）
//!
//! `count(gs)` 而不是 `count(*)`：聚合参数非 None ⇒ 融合条件不成立，
//! 走的是"物化去重结果再数一遍"的老路径。同一条 SQL 的两种写法结果必须一致。

use qsdb::sql::exec_builder::distinct_fuse_applies;
use qsdb::sql::translate::{self, LogicalPlan, ParsedStatement};
use qsdb::{Qsdb, QsdbOptions};

/// 测试目录唯一序号：pid 挡跨进程碰撞，序号挡同进程内并发（Windows 时钟
/// 粒度 ~100ns，纳秒后缀在同 tick 内会重复——CI 上真炸过，见 constraints）。
fn _uniq_seq() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    SEQ.fetch_add(1, Ordering::Relaxed)
}

fn tmp(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir()
        .join("qsdb_distinct_fused")
        .join(format!("{tag}_{:x}_{}", std::process::id(), _uniq_seq()))
}

/// `budget = None` → 不限内存（融合节点不会溢出）；`Some(b)` → 逼它走溢出重分区。
fn open(tag: &str, budget: Option<u64>) -> (Qsdb, std::path::PathBuf) {
    let root = tmp(tag);
    let db = Qsdb::open(
        &root,
        QsdbOptions {
            memory_budget_bytes: budget,
            ..Default::default()
        },
    )
    .unwrap();
    (db, root)
}

fn scalar(db: &Qsdb, sql: &str) -> String {
    let bs = db.sql_collect(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    assert_eq!(bs.len(), 1, "{sql}: 聚合必须恰好一批");
    assert_eq!(bs[0].num_rows(), 1, "{sql}: 聚合必须恰好一行");
    arrow::util::display::array_value_to_string(bs[0].column(0).as_ref(), 0).unwrap()
}

fn plan_of(db: &Qsdb, sql: &str) -> LogicalPlan {
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

/// 取出顶层 `Aggregate` 的 (group, aggs) 与它的输入。
fn agg_parts(
    p: &LogicalPlan,
) -> (
    &[qsdb::expr::PhysExpr],
    &[qsdb::expr::AggExpr],
    &LogicalPlan,
) {
    match p {
        LogicalPlan::Aggregate { input, group, aggs } => (group, aggs, input),
        LogicalPlan::Project { input, .. } | LogicalPlan::Filter { input, .. } => agg_parts(input),
        other => panic!("没找到 Aggregate：{other:?}"),
    }
}

/// `s`：**半数列值重复**（`v-{i % 100}`），所以"去重了没有"能从数值上看出来。
/// `n`：带 NULL 的整数列。行数 1000、基数 100。
fn seed(db: &Qsdb) {
    db.sql("CREATE TABLE t (id BIGINT, s STRING, n BIGINT)")
        .unwrap();
    for chunk in 0..10i64 {
        let rows: Vec<String> = (0..100)
            .map(|j| {
                let i = chunk * 100 + j;
                // n：每 7 行一个 NULL，其余 i%100 —— 只有 NULL 的那一组必须也算一个"值"
                let n = if i % 7 == 0 {
                    "NULL".to_string()
                } else {
                    format!("{}", i % 100)
                };
                format!("({i},'v-{}',{n})", i % 100)
            })
            .collect();
        db.sql(&format!("INSERT INTO t VALUES {}", rows.join(",")))
            .unwrap();
    }
}

// ---------------------------------------------------------------------------
// 形状：融合**必须**被认出来（否则优化白写）
// ---------------------------------------------------------------------------

#[test]
fn fusion_is_recognized_for_both_query_forms() {
    let (db, root) = open("shape", None);
    seed(&db);

    // ① 显式 `SELECT DISTINCT` 子查询（基准那句）
    let p = plan_of(&db, "SELECT count(*) FROM (SELECT DISTINCT s FROM t)");
    let (g, a, i) = agg_parts(&p);
    assert!(
        distinct_fuse_applies(g, a, i),
        "`count(*) over DISTINCT` 必须走融合计数：{p:?}"
    );

    // ② 绑定层从 `count(DISTINCT s)` 改写出来的计划（同一形状）
    let p = plan_of(&db, "SELECT count(DISTINCT s) FROM t");
    let (g, a, i) = agg_parts(&p);
    assert!(
        distinct_fuse_applies(g, a, i),
        "`count(DISTINCT s)` 的改写结果也必须走融合计数：{p:?}"
    );

    // ③ 不该融合的：参数非 None（`count(s)` 是"数非空值"，不是"数不同行"）
    let p = plan_of(&db, "SELECT count(s) FROM (SELECT DISTINCT s FROM t)");
    let (g, a, i) = agg_parts(&p);
    assert!(!distinct_fuse_applies(g, a, i), "count(expr) 不该融合");

    // ④ 有分组的 DISTINCT 计数不该融合（融合节点只出**一个数**）
    let p = plan_of(&db, "SELECT count(*) FROM (SELECT DISTINCT s FROM t) x");
    let (g, a, i) = agg_parts(&p);
    assert!(distinct_fuse_applies(g, a, i), "派生表不该改变形状：{p:?}");

    drop(db);
    let _ = std::fs::remove_dir_all(root);
}

// ---------------------------------------------------------------------------
// 值：融合路径 vs 不融合路径必须一致（同一份数据、两种写法）
// ---------------------------------------------------------------------------

/// 把两条写法的结果都算出来比对，并返回（融合, 对照）。
fn both(db: &Qsdb, fused: &str, control: &str) -> (String, String) {
    (scalar(db, fused), scalar(db, control))
}

fn check_values(tag: &str, budget: Option<u64>) {
    let (db, root) = open(tag, budget);
    seed(&db);

    // ① 字符串：1000 行、基数 100 ⇒ 去重 = 100，不去重 = 1000
    let (f, c) = both(
        &db,
        "SELECT count(*) FROM (SELECT DISTINCT s FROM t)",
        "SELECT count(s) FROM (SELECT DISTINCT s FROM t)",
    );
    assert_eq!(f, "100", "融合计数（字符串）");
    assert_eq!(f, c, "融合与不融合必须一致");
    assert_ne!(
        scalar(&db, "SELECT count(s) FROM t"),
        "100",
        "不去重不该是 100"
    );

    // ② 整数 + NULL：非空值 `i%100` 有 100 个不同值，另外每 7 行一个 NULL。
    //    两个数**必须不同**，这正是 NULL 语义的体现：
    //      `count(*)` over `DISTINCT n` = **101**（NULL 自成一组，也是"一行"）
    //      `count(n)` over `DISTINCT n` = **100**（跳过 NULL）
    assert_eq!(
        scalar(&db, "SELECT count(*) FROM (SELECT DISTINCT n FROM t)"),
        "101",
        "整列去重：100 个不同整数 + NULL 自成一组"
    );
    assert_eq!(
        scalar(&db, "SELECT count(n) FROM (SELECT DISTINCT n FROM t)"),
        "100",
        "对照路径跳过 NULL"
    );
    // 原生 `count(DISTINCT n)` 必须等于"跳过 NULL"的那个（= 100，不是 101）
    assert_eq!(
        scalar(&db, "SELECT count(DISTINCT n) FROM t"),
        "100",
        "count(DISTINCT n) 跳过 NULL —— 与 `count(*) over DISTINCT` 差的就是 NULL 那一组"
    );

    // ③ 多列去重（`Distinct` 是整行去重，融合节点的键 = 全部列）
    let (f, c) = both(
        &db,
        "SELECT count(*) FROM (SELECT DISTINCT s, n FROM t)",
        "SELECT count(s) FROM (SELECT DISTINCT s, n FROM t)",
    );
    assert_eq!(f, c, "多列去重：融合与不融合必须一致");
    assert!(
        f.parse::<i64>().unwrap() > 100,
        "两列组合的去重行数应更多：{f}"
    );

    // ④ 空表 / 单值
    db.sql("CREATE TABLE e (a BIGINT)").unwrap();
    assert_eq!(
        scalar(&db, "SELECT count(*) FROM (SELECT DISTINCT a FROM e)"),
        "0"
    );
    db.sql("INSERT INTO e VALUES (7),(7),(7)").unwrap();
    assert_eq!(
        scalar(&db, "SELECT count(*) FROM (SELECT DISTINCT a FROM e)"),
        "1"
    );

    drop(db);
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn values_without_spill() {
    check_values("novalue", None);
}

/// **单 Int64 列的内联槽扩容**：组数跨过初始容量时，命中的 mask 必须逐行重取。
///
/// 为什么单独立一条：count-only 的**单 Int64 分组列**走的是 `int_slots`
/// （键存在槽里、不碰 `key_arena`，实测比通用路径快 5.4×）。而既有的溢出测试
/// 用的是**字符串**列——字符串走不了这条内联路径 ⇒ **扩容逻辑一条测试都没盖到**。
/// 实测代价：mask 提到循环外之后，扩容后探测只在老区间走 ⇒ 已有键一律找不到
/// ⇒ 每行新建组（**静默多数 + 内存雪崩**，`.14` 上 500 万行直接跑到超时）。
#[test]
fn int_slot_growth_keeps_counts_correct() {
    let (db, root) = open("growth", None);
    db.sql("CREATE TABLE t (n BIGINT)").unwrap();
    // 3000 个不同值、每个重复 2 次 ⇒ 去重 = 3000（跨过初始容量 1024 ⇒ 至少扩容一次）
    let mut rows: Vec<String> = Vec::new();
    for i in 0..3000 {
        rows.push(format!("({i})"));
        rows.push(format!("({i})"));
    }
    db.sql(&format!("INSERT INTO t VALUES {}", rows.join(",")))
        .unwrap();
    assert_eq!(
        scalar(&db, "SELECT count(DISTINCT n) FROM t"),
        "3000",
        "扩容后仍必须只回不同值个数"
    );
    assert_eq!(
        scalar(&db, "SELECT count(n) FROM t"),
        "6000",
        "不去重 = 6000"
    );

    // NULL 同样跨扩容：它走的是**单独的槽**（`i64::MIN` 是合法值，不能用哨兵）
    db.sql("CREATE TABLE u (n BIGINT)").unwrap();
    let mut rows: Vec<String> = Vec::new();
    for i in 0..3000 {
        rows.push(format!("({i})"));
        rows.push("(NULL)".to_string());
    }
    db.sql(&format!("INSERT INTO u VALUES {}", rows.join(",")))
        .unwrap();
    // 3000 个不同整数 + NULL 自成一组 = 3001；`count(n)` 跳过 NULL = 3000
    assert_eq!(
        scalar(&db, "SELECT count(*) FROM (SELECT DISTINCT n FROM u)"),
        "3001"
    );
    assert_eq!(scalar(&db, "SELECT count(DISTINCT n) FROM u"), "3000");
    drop(db);
    let _ = std::fs::remove_dir_all(root);
}

/// **溢出重分区**：值必须照样对。
///
/// 这是本文件里最重要的一条 —— 分区口径（键字节）一旦内存侧与批侧不一致，
/// 同一个逻辑键会在两个分区各算一次，**数值偏大、不报错**。
///
/// ⚠️ **规模是这条测试的成立前提**。最初写成"1000 行 + 512KB/1MB 预算"，
/// 实测（`QSDB_DISTINCT_PROF=1`）**根本没有溢出** ⇒ 那是个**假覆盖**：
/// 1000 行只建 101 个组，离预算差得远，溢出分支一行都没跑到。
/// 现在用 `db.append` 灌 40 万行（2 万个不同字符串组）配 1MB 预算，
/// 组表必然超额 ⇒ 真走溢出。`QSDB_DISTINCT_PROF=1` 会打出
/// `[distinct-count] 转溢出重分区：内存中已建组 N` —— 这条诊断就是**用来盯住
/// 上面那个"假覆盖"别再回来的**，别删。
#[test]
fn values_survive_spill_repartition() {
    const ROWS: i64 = 400_000;
    const CARD: i64 = 20_000; // s 的基数：i % CARD
    let (db, root) = open("spill", Some(1024 * 1024));
    db.sql("CREATE TABLE t (id BIGINT, s STRING, n BIGINT)")
        .unwrap();
    let schema = std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, true),
        arrow::datatypes::Field::new("s", arrow::datatypes::DataType::Utf8, true),
        arrow::datatypes::Field::new("n", arrow::datatypes::DataType::Int64, true),
    ]));
    let mut done = 0i64;
    while done < ROWS {
        let n = 20_000.min(ROWS - done);
        let ids: Vec<i64> = (done..done + n).collect();
        let ss: Vec<String> = ids.iter().map(|i| format!("v-{:06}", i % CARD)).collect();
        let ns: Vec<i64> = ids.iter().map(|i| i % CARD).collect();
        let b = arrow::record_batch::RecordBatch::try_new(
            std::sync::Arc::clone(&schema),
            vec![
                std::sync::Arc::new(arrow::array::Int64Array::from(ids)),
                std::sync::Arc::new(arrow::array::StringArray::from(ss)),
                std::sync::Arc::new(arrow::array::Int64Array::from(ns)),
            ],
        )
        .unwrap();
        db.append("t", &[b]).unwrap();
        done += n;
    }

    // 融合路径（会转溢出）
    assert_eq!(
        scalar(&db, "SELECT count(*) FROM (SELECT DISTINCT s FROM t)"),
        CARD.to_string(),
        "溢出后各分区的组数相加必须仍等于全局基数"
    );
    // 对照路径（物化去重结果再数）
    assert_eq!(
        scalar(&db, "SELECT count(s) FROM (SELECT DISTINCT s FROM t)"),
        CARD.to_string()
    );
    // 原生改写路径
    assert_eq!(
        scalar(&db, "SELECT count(DISTINCT s) FROM t"),
        CARD.to_string()
    );
    // 两列组合：s 与 n 一一对应（都取 i % CARD）⇒ 组合去重后仍是 CARD 行
    assert_eq!(
        scalar(&db, "SELECT count(*) FROM (SELECT DISTINCT s, n FROM t)"),
        CARD.to_string()
    );

    drop(db);
    let _ = std::fs::remove_dir_all(root);
}

/// 明确对照：融合开关关掉后结果不变（`QSDB_NO_DISTINCT_FUSE`）。
///
/// ⚠️ 环境变量是**进程级**的，与并行跑的其他测试会互相干扰 —— 所以这条用
/// 串行互斥锁把整个测试体罩住（与 `tests/literals_to_array.rs` 同款做法）。
#[test]
fn disable_switch_gives_the_same_answer() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let (db, root) = open("switch", None);
    seed(&db);
    let sql = "SELECT count(*) FROM (SELECT DISTINCT s FROM t)";
    let on = scalar(&db, sql);
    unsafe { std::env::set_var("QSDB_NO_DISTINCT_FUSE", "1") };
    let off = scalar(&db, sql);
    unsafe { std::env::remove_var("QSDB_NO_DISTINCT_FUSE") };
    assert_eq!(on, "100");
    assert_eq!(on, off, "开/关融合必须回同一个数");
    drop(db);
    let _ = std::fs::remove_dir_all(root);
}

/// 环境变量类测试的串行锁（同进程内互斥）。
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// **各分区共享键时，计数必须相加（不是求并集）** —— P1⑪ 的核心正确性。
///
/// 分区按**键哈希**切 ⇒ 同一个键必落同一分区 ⇒ 各分区键集**不相交**
/// ⇒ **组数相加是精确的**。这条刻意造**重叠加满**的数据：每批 2 万行、
/// `s` 的基数也是 2 万 ⇒ **每个分区都含全部 2 万个值**。
/// 若哪天误按"行"分片再相加，会回 4×2 万 ⇒ 立刻红。
///
/// ⚠️ 环境变量是进程级的 ⇒ 用 SERIAL 锁罩住整个测试体。
#[test]
fn partitioned_count_sums_disjoint_keys() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    const ROWS: i64 = 400_000;
    const CARD: i64 = 20_000;
    let (db, root) = open("parts", None);
    db.sql("CREATE TABLE t (s STRING)").unwrap();
    let schema = std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("s", arrow::datatypes::DataType::Utf8, true),
    ]));
    let mut done = 0i64;
    while done < ROWS {
        let n = 20_000.min(ROWS - done);
        let ss: Vec<String> = (done..done + n)
            .map(|i| format!("v-{:06}", i % CARD))
            .collect();
        let b = arrow::record_batch::RecordBatch::try_new(
            std::sync::Arc::clone(&schema),
            vec![std::sync::Arc::new(arrow::array::StringArray::from(ss))],
        )
        .unwrap();
        db.append("t", &[b]).unwrap();
        done += n;
    }
    let sql = "SELECT count(*) FROM (SELECT DISTINCT s FROM t)";
    // 基线：单分区（走单线程路径）
    unsafe { std::env::set_var("QSDB_DISTINCT_PARTS", "1") };
    let one = scalar(&db, sql);
    assert_eq!(one, CARD.to_string(), "基数就是 CARD");
    // 多分区：**必须同值**。数据刻意让每个分区都含全部值 ——
    // 按行分片再相加会回 4×CARD，这里是加法正确性的判别点。
    for n in ["2", "4", "16"] {
        unsafe { std::env::set_var("QSDB_DISTINCT_PARTS", n) };
        assert_eq!(
            scalar(&db, sql),
            one,
            "分区数 {n} 必须与单分区同值（键集不相交 ⇒ 相加精确）"
        );
    }
    unsafe { std::env::remove_var("QSDB_DISTINCT_PARTS") };
    drop(db);
    let _ = std::fs::remove_dir_all(root);
}
