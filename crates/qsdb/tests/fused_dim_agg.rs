//! 维表链聚合融合（`exec/fused_dim_agg.rs`）的**多路对撞**验收
//! （`join 发射设计稿` §三/§四）。
//!
//! 同一条查询必须在四条执行路下**逐值相等**：
//! * 缺省 —— 融合（预计算 matches + 直累计，零发射物化）；
//! * `QSDB_NO_FUSED_JOIN=1` —— 现有 join+聚合路（发射物化）；
//! * `QSDB_NO_JOIN_DENSE=1` —— 融合路 + i64 哈希索引（关刀 B 的密集直索引）；
//! * `QSDB_JOIN_NO_PROJ=1` —— 现有路 + 全列发射（关刀 A 的列收窄）。
//!
//! 这类优化的失败模式是**静默少数/多数**（融合路不物化，算错不报错），
//! 只看"跑得通"看不出来 —— 逐值对撞是唯一的日常 exercising。
//!
//! ⚠️ 环境变量是**进程级**的 ⇒ 所有碰 env 的测试用 `SERIAL` 锁罩住整个测试体
//! （与 `distinct_count_v2.rs` 同款做法）。

use qsdb::sql::exec_builder::fused_dim_applies;
use qsdb::sql::translate::{self, LogicalPlan, ParsedStatement};
use qsdb::{Qsdb, QsdbOptions};

static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn open(tag: &str) -> (Qsdb, std::path::PathBuf) {
    let root = std::env::temp_dir()
        .join("qsdb_fused_dim_agg")
        .join(format!(
            "{tag}_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
    std::fs::create_dir_all(&root).unwrap();
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    (db, root)
}

/// 结果集 → 规范字符串（逐行 `值|值|…`，行序按查询里的 ORDER BY）。
fn result_string(db: &Qsdb, sql: &str) -> String {
    let bs = db.sql_collect(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    let mut lines: Vec<String> = Vec::new();
    for b in &bs {
        for row in 0..b.num_rows() {
            let mut parts: Vec<String> = Vec::new();
            for c in 0..b.num_columns() {
                let col = b.column(c);
                if col.is_null(row) {
                    parts.push("NULL".into());
                } else {
                    match col.data_type() {
                        arrow::datatypes::DataType::Int64 => parts.push(format!(
                            "{}",
                            col.as_any()
                                .downcast_ref::<arrow::array::Int64Array>()
                                .unwrap()
                                .value(row)
                        )),
                        arrow::datatypes::DataType::Utf8 => parts.push(
                            col.as_any()
                                .downcast_ref::<arrow::array::StringArray>()
                                .unwrap()
                                .value(row)
                                .to_string(),
                        ),
                        other => panic!("测试没覆盖的类型 {other:?}"),
                    }
                }
            }
            lines.push(parts.join("|"));
        }
    }
    lines.join("\n")
}

/// 四条路逐值对撞，返回缺省（融合）路的结果。
fn assert_four_paths(db: &Qsdb, sql: &str) -> String {
    let fused = result_string(db, sql);
    for (var, val, what) in [
        ("QSDB_NO_FUSED_JOIN", "1", "现有 join+聚合路"),
        ("QSDB_NO_JOIN_DENSE", "1", "融合路+哈希索引"),
        ("QSDB_JOIN_NO_PROJ", "1", "现有路+全列发射"),
    ] {
        unsafe { std::env::set_var(var, val) };
        let other = result_string(db, sql);
        unsafe { std::env::remove_var(var) };
        assert_eq!(fused, other, "{sql}: 融合路与{what}必须逐值相等");
    }
    fused
}

/// 锚定数据（手工算好期望，防"三条路一起错"）：
/// t:  (g,v) = (1,10),(1,20),(2,30),(3,40),(NULL,50),(2,NULL),(1,60)
/// d:  (g,cat) = (1,'a'),(1,'b'),(2,'a'),(3,NULL),(NULL,'a'),(2,'a')
///     —— g=2 出现两次（一对多）；g=3 的 cat 是 NULL；NULL g 行永不匹配
/// d2: (cat,tag) = ('a',1),('a',2),('b',3),(NULL,9),('',4)
///     —— 'a' 出现两次（一对多乘法）；NULL cat 与空串各是一行
///
/// 深度 1（t⋈d GROUP BY d.cat）手工期望：
///   a|7|150|5   （g=1 的 3 行 ×(a,b) + g=2 的 2 行 ×(a,a)；v=10+20+60+30+30=150，
///                count(t.v) 非 NULL=5——g=2 那行的 NULL v 不计）
///   b|3|90|3
///   NULL|1|40|1 （t.g=3 配上 d(3,NULL)：键不 NULL 就配上，组键 cat 是 NULL ⇒
///                **NULL 组**——组键 NULL 与连接键 NULL 是两回事）
/// 深度 2（再 ⋈ d2 GROUP BY d2.cat）手工期望：
///   a|14        （7 对 × d2 里 'a' 有 2 行 = 乘法）
///   b|3         （NULL 组的键是 NULL，配不上任何 d2 行 ⇒ 消失）
fn seed(db: &Qsdb) {
    db.sql("CREATE TABLE t (g INT, v INT)").unwrap();
    db.sql("INSERT INTO t VALUES (1,10),(1,20),(2,30),(3,40),(NULL,50),(2,NULL),(1,60)")
        .unwrap();
    db.sql("CREATE TABLE d (g INT, cat STRING)").unwrap();
    db.sql("INSERT INTO d VALUES (1,'a'),(1,'b'),(2,'a'),(3,NULL),(NULL,'a'),(2,'a')")
        .unwrap();
    db.sql("CREATE TABLE d2 (cat STRING, tag INT)").unwrap();
    db.sql("INSERT INTO d2 VALUES ('a',1),('a',2),('b',3),(NULL,9),('',4)")
        .unwrap();
}

const SQL_D1: &str = "SELECT d.cat, count(*) c, sum(t.v) s, count(t.v) cv \
                      FROM t JOIN d ON t.g = d.g GROUP BY d.cat ORDER BY d.cat";
const SQL_D2: &str = "SELECT d2.cat, count(*) c \
                      FROM t JOIN d ON t.g = d.g JOIN d2 ON d.cat = d2.cat \
                      GROUP BY d2.cat ORDER BY d2.cat";

#[test]
fn 锚定数据_深度1_深度2_四路对撞() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let (db, root) = open("anchor");
    seed(&db);

    // 形状必须真的命中融合（可断言入口——静默不触发 = 测试空转）
    assert!(applies(&db, SQL_D1), "深度 1 必须命中融合");
    assert!(applies(&db, SQL_D2), "深度 2 必须命中融合");

    let r1 = assert_four_paths(&db, SQL_D1);
    assert_eq!(r1, "a|7|150|5\nb|3|90|3\nNULL|1|40|1", "深度 1 手工锚定");

    let r2 = assert_four_paths(&db, SQL_D2);
    assert_eq!(r2, "a|14\nb|3", "深度 2 手工锚定（一对多乘法）");
    drop(db);
    let _ = std::fs::remove_dir_all(root);
}

/// 大表分片对撞：事实表多文件（可分片）⇒ 分片融合路；`QSDB_SHARDS` 1/2/4
/// 必须与不分片同值。期望值由现有路（关融合）自算。
#[test]
fn 大表分片_融合与现有路同值() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let (db, root) = open("shards");
    // 事实表：6 万行、g ∈ [0,100)，若干 NULL 键/NULL v
    db.sql("CREATE TABLE t (g INT, v INT)").unwrap();
    let schema = std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("g", arrow::datatypes::DataType::Int32, true),
        arrow::datatypes::Field::new("v", arrow::datatypes::DataType::Int32, true),
    ]));
    let mut st: u64 = 0x243f_6a88_85a3_08d3;
    let mut next = move || {
        st ^= st << 13;
        st ^= st >> 7;
        st ^= st << 17;
        st
    };
    let mut done = 0usize;
    while done < 60_000 {
        let n = 10_000.min(60_000 - done);
        let (gs, vs): (Vec<Option<i32>>, Vec<Option<i32>>) = (0..n)
            .map(|_| {
                let r = next();
                let g = if r % 53 == 0 {
                    None
                } else {
                    Some((r % 100) as i32)
                };
                let v = if r % 71 == 0 {
                    None
                } else {
                    Some((r % 1000) as i32)
                };
                (g, v)
            })
            .unzip();
        let b = arrow::record_batch::RecordBatch::try_new(
            std::sync::Arc::clone(&schema),
            vec![
                std::sync::Arc::new(arrow::array::Int32Array::from(gs)),
                std::sync::Arc::new(arrow::array::Int32Array::from(vs)),
            ],
        )
        .unwrap();
        db.append("t", &[b]).unwrap();
        done += n;
    }
    db.sql("CREATE TABLE d (g INT, cat STRING)").unwrap();
    // 100 个 g，其中 3 个 NULL；cat 10 类（每类 ~10 行 ⇒ 一对多重）
    let mut ins = String::from("INSERT INTO d VALUES ");
    for g in 0..100i64 {
        if g % 33 == 0 {
            ins.push_str(&format!("(NULL,'c{}'),", g % 10));
            continue;
        }
        ins.push_str(&format!("({g},'c{}'),", g % 10));
    }
    ins.pop();
    db.sql(&ins).unwrap();
    db.sql("CREATE TABLE d2 (cat STRING)").unwrap();
    db.sql("INSERT INTO d2 VALUES ('c0'),('c1'),('c2'),('c3'),('c4'),('c5'),('c6'),('c7'),('c8'),('c9'),(NULL)")
        .unwrap();

    let sql1 = "SELECT d.cat, count(*) c, sum(t.v) s FROM t JOIN d ON t.g = d.g \
                GROUP BY d.cat ORDER BY d.cat";
    let sql2 = "SELECT d2.cat, count(*) c FROM t JOIN d ON t.g = d.g \
                JOIN d2 ON d.cat = d2.cat GROUP BY d2.cat ORDER BY d2.cat";
    // 期望 = 现有路（关融合）自算
    unsafe { std::env::set_var("QSDB_NO_FUSED_JOIN", "1") };
    let e1 = result_string(&db, sql1);
    let e2 = result_string(&db, sql2);
    unsafe { std::env::remove_var("QSDB_NO_FUSED_JOIN") };
    assert!(!e1.is_empty() && !e2.is_empty(), "数据必须真的产出组");

    for (sql, expect) in [(sql1, e1), (sql2, e2)] {
        unsafe { std::env::remove_var("QSDB_SHARDS") };
        let fused = result_string(&db, sql);
        assert_eq!(fused, expect, "{sql}: 融合与现有路必须同值");
        for sh in ["1", "2", "4"] {
            unsafe { std::env::set_var("QSDB_SHARDS", sh) };
            assert_eq!(
                result_string(&db, sql),
                expect,
                "{sql}: QSDB_SHARDS={sh} 必须同值"
            );
        }
        unsafe { std::env::remove_var("QSDB_SHARDS") };
    }
    drop(db);
    let _ = std::fs::remove_dir_all(root);
}

/// 空侧边界：d 为空 / d2 为空 / t 为空 ⇒ 零行；全 NULL 键 ⇒ 零行。
#[test]
fn 空侧与全null键_四路对撞() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let (db, root) = open("empty");
    seed(&db);

    // d2 清空 ⇒ 深度 2 零行
    db.sql("DELETE FROM d2").unwrap();
    let r = assert_four_paths(&db, SQL_D2);
    assert_eq!(r, "", "d2 空 ⇒ 深度 2 零行");
    db.sql("INSERT INTO d2 VALUES ('a',1),('a',2),('b',3),(NULL,9),('',4)")
        .unwrap();

    // d 清空 ⇒ 两级都零行
    db.sql("DELETE FROM d").unwrap();
    assert_eq!(assert_four_paths(&db, SQL_D1), "");
    assert_eq!(assert_four_paths(&db, SQL_D2), "");
    db.sql("INSERT INTO d VALUES (1,'a'),(1,'b'),(2,'a'),(3,NULL),(NULL,'a'),(2,'a')")
        .unwrap();

    // t 全 NULL 键 ⇒ 两级都零行
    db.sql("DELETE FROM t").unwrap();
    db.sql("INSERT INTO t VALUES (NULL,1),(NULL,2)").unwrap();
    assert_eq!(assert_four_paths(&db, SQL_D1), "");
    assert_eq!(assert_four_paths(&db, SQL_D2), "");
    drop(db);
    let _ = std::fs::remove_dir_all(root);
}

/// 形状门的**否定用例**：这些形状必须不命中融合（命中了 = 门判错列的温床）。
#[test]
fn 形状门_否定用例() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let (db, root) = open("gate");
    seed(&db);
    // 残留谓词（ON 键外条件）——融合路没有逐对判否
    let sql = "SELECT d2.cat, count(*) c FROM t JOIN d ON t.g = d.g AND t.v > 15 \
               JOIN d2 ON d.cat = d2.cat GROUP BY d2.cat";
    assert!(!applies(&db, sql), "残留谓词必须门拒");
    // LEFT JOIN
    let sql = "SELECT d2.cat, count(*) c FROM t LEFT JOIN d ON t.g = d.g \
               JOIN d2 ON d.cat = d2.cat GROUP BY d2.cat";
    assert!(!applies(&db, sql), "LEFT 必须门拒");
    // sum(DISTINCT …) 之类在绑定期就改写/报错，这里验 FILTER：
    let sql = "SELECT d.cat, count(*) FILTER (WHERE t.v > 10) c \
               FROM t JOIN d ON t.g = d.g GROUP BY d.cat";
    assert!(!applies(&db, sql), "FILTER 聚合必须门拒");
    // min/max（门只放 Count/Sum）
    let sql = "SELECT d.cat, min(t.v) m FROM t JOIN d ON t.g = d.g GROUP BY d.cat";
    assert!(!applies(&db, sql), "min/max 必须门拒");
    // 分组键在 probe 侧
    let sql = "SELECT t.g, count(*) c FROM t JOIN d ON t.g = d.g GROUP BY t.g";
    assert!(!applies(&db, sql), "probe 侧分组键必须门拒");
    // sum over Decimal（类型门）
    db.sql("CREATE TABLE td (g INT, v DECIMAL(10,2))").unwrap();
    db.sql("INSERT INTO td VALUES (1,10.5),(1,2.5)").unwrap();
    db.sql("CREATE TABLE dd (g INT, cat STRING)").unwrap();
    db.sql("INSERT INTO dd VALUES (1,'a')").unwrap();
    let sql = "SELECT dd.cat, sum(td.v) s FROM td JOIN dd ON td.g = dd.g GROUP BY dd.cat";
    assert!(!applies(&db, sql), "Decimal sum 必须门拒");
    drop(db);
    let _ = std::fs::remove_dir_all(root);
}

/// 计划提取（`distinct_count_fused.rs` 同款：parse → resolve → translate），
/// 取顶层 `Aggregate` 的 (group, aggs, input) 交给形状判定——
/// 执行期 `build_aggregate` 看到的就是这棵子树（annotate 只加 `needed`，形状不动）。
fn applies(db: &Qsdb, sql: &str) -> bool {
    let plan = plan_of(db, sql);
    let (g, a, input) = agg_parts(&plan);
    fused_dim_applies(input, g, a)
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

fn agg_parts(
    p: &LogicalPlan,
) -> (
    &[qsdb::expr::PhysExpr],
    &[qsdb::expr::AggExpr],
    &LogicalPlan,
) {
    match p {
        LogicalPlan::Aggregate { input, group, aggs } => (group, aggs, input),
        LogicalPlan::Project { input, .. }
        | LogicalPlan::Filter { input, .. }
        | LogicalPlan::Sort { input, .. } => agg_parts(input),
        other => panic!("没找到 Aggregate：{other:?}"),
    }
}
