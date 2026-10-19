//! count-only 单 Int64 去重的**密集直索引**（`GroupTable::dense`，第十五批）
//! 的**多路对撞**验收（`整型去重密集直索引设计稿` §三/§四）。
//!
//! 同一批查询必须在 dense 路与**槽路**（`QSDB_NO_DENSE_GROUP=1`）下逐值相等；
//! CAP 坠回形状另用 `QSDB_DENSE_GROUP_CAP` 逼出来（默认 8M 槽，测试里调小）。
//!
//! 这类优化的失败模式是**静默多数/少数**（索引错了不报错，只是多建组或漏建组），
//! 只看"跑得通"看不出来 —— 期望值手工锚定 + A/B 对撞是唯一的日常 exercising。
//!
//! ⚠️ 环境变量是**进程级**的 ⇒ 所有碰 env 的测试用 `SERIAL` 锁罩住整个测试体
//! （与 `fused_dim_agg.rs` / `distinct_count_v2.rs` 同款做法）。
//! 增长/坠回/重锚定的**确定性**覆盖在 `exec::agg` 的 in-module 单测里
//!（那里用显式批边界驱动，不依赖扫描器的分批行为）；本文件管端到端。

use arrow::array::{ArrayRef, Int64Array};
use arrow::record_batch::RecordBatch;
use qsdb::{Qsdb, QsdbOptions};
use std::sync::Arc;

static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn open(tag: &str) -> (Qsdb, std::path::PathBuf) {
    let root = std::env::temp_dir()
        .join("qsdb_distinct_int_dense")
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

/// 标量查询（单行单列 INT）。
fn q(db: &Qsdb, sql: &str) -> i64 {
    let bs = db.sql_collect(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    let a = bs[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap_or_else(|| panic!("{sql}: 首列不是 INT64"));
    a.value(0)
}

/// 在 env 开关下跑一组查询（进入前设置、离开前撤销）。
fn with_env1(var: &str, val: &str, f: impl FnOnce()) {
    unsafe { std::env::set_var(var, val) };
    f();
    unsafe { std::env::remove_var(var) };
}

/// A/B 对撞：缺省（dense）vs `QSDB_NO_DENSE_GROUP=1`（槽路），逐值相等。
fn assert_ab(db: &Qsdb, sqls: &[&str]) {
    let base: Vec<i64> = sqls.iter().map(|s| q(db, s)).collect();
    with_env1("QSDB_NO_DENSE_GROUP", "1", || {
        for (s, b) in sqls.iter().zip(&base) {
            assert_eq!(q(db, s), *b, "{s}: dense 路与槽路必须同值");
        }
    });
}

/// 追加一批 Int64（一次 append = 一个段）。
fn append(db: &Qsdb, vals: Vec<Option<i64>>) {
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("v", arrow::datatypes::DataType::Int64, true),
    ]));
    let arr: ArrayRef = Arc::new(Int64Array::from(vals));
    let b = RecordBatch::try_new(schema, vec![arr]).unwrap();
    db.append("t", &[b]).unwrap();
}

/// 锚定数据（手工算好期望，防"两条路一起错"）：
/// v = 5, 3, 3, NULL, -7, 5, NULL, 1000000
/// * `count(DISTINCT v)` = **4**：{5,3,-7,1000000}——绑定层对无分组的
///   `count(DISTINCT)` 在去重前插了 `IS NOT NULL`（translate.rs）⇒ NULL 不计；
/// * `count(*) FROM (SELECT DISTINCT v)` = **5**：DISTINCT 行集里 NULL 是一行
///   （dense 的 null 槽被这一形态真实走到）；
/// * `count(v) FROM (SELECT DISTINCT v)` = **4**：count(v) 跳 NULL。
#[test]
fn 锚定数据_三形态_双路对撞() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let (db, root) = open("anchor");
    db.sql("CREATE TABLE t (v BIGINT)").unwrap();
    db.sql("INSERT INTO t VALUES (5),(3),(3),(NULL),(-7),(5),(NULL),(1000000)")
        .unwrap();
    let sqls = [
        "SELECT count(DISTINCT v) FROM t",
        "SELECT count(*) FROM (SELECT DISTINCT v FROM t)",
        "SELECT count(v) FROM (SELECT DISTINCT v FROM t)",
    ];
    assert_eq!(q(&db, sqls[0]), 4, "手工期望：4 个非空不同值");
    assert_eq!(q(&db, sqls[1]), 5, "手工期望：+1 个 NULL 行");
    assert_eq!(q(&db, sqls[2]), 4);
    assert_ab(&db, &sqls);
    let _ = std::fs::remove_dir_all(root);
}

/// 跨批增长：段与段之间值域跳着长（20k → 1M 档 → 5M 档），dense 一路增长重建；
/// 末段回灌重复值 + NULL，全走命中路。
#[test]
fn 跨批增长_值域跳长_双路对撞() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let (db, root) = open("grow");
    db.sql("CREATE TABLE t (v BIGINT)").unwrap();
    append(&db, (0..20_000).map(Some).collect());
    append(&db, (1_000_000..1_020_000).map(Some).collect());
    append(&db, (5_000_000..5_020_000).map(Some).collect());
    let mut tail: Vec<Option<i64>> = (0..500).map(Some).collect(); // 全是重复
    tail.push(None);
    append(&db, tail);
    let sqls = [
        "SELECT count(DISTINCT v) FROM t",
        "SELECT count(*) FROM (SELECT DISTINCT v FROM t)",
    ];
    assert_eq!(q(&db, sqls[0]), 60_000, "手工期望：三段各 2 万");
    assert_eq!(q(&db, sqls[1]), 60_001, "手工期望：+1 个 NULL 行");
    assert_ab(&db, &sqls);
    let _ = std::fs::remove_dir_all(root);
}

/// CAP 坠回：`QSDB_DENSE_GROUP_CAP=1024` 下值域 5000——首批 0..512 还锚得住，
/// 第二批一越界 need=4000 > CAP ⇒ 中途坠回槽路；与"从头就不锚"（第三批单批
/// 2000 > CAP，锚定直接拒）两条形状都必须与槽路同值。
#[test]
fn cap坠回_中途与锚定期_双路对撞() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let sqls = ["SELECT count(DISTINCT v) FROM t"];

    // 形状 A：首批小锚定成功、第二批中途坠回
    {
        let (db, root) = open("cap_mid");
        db.sql("CREATE TABLE t (v BIGINT)").unwrap();
        append(&db, (0..512).map(Some).collect());
        append(&db, (512..4_000).map(Some).collect());
        with_env1("QSDB_DENSE_GROUP_CAP", "1024", || {
            assert_eq!(q(&db, sqls[0]), 4_000, "手工期望");
            assert_ab(&db, &sqls);
        });
        // 默认 CAP 下同一批数据也必须同值（走纯 dense 不坠回）
        assert_eq!(q(&db, sqls[0]), 4_000);
        let _ = std::fs::remove_dir_all(root);
    }
    // 形状 B：单批 span 就超 CAP ⇒ 锚定直接拒，整批槽路
    {
        let (db, root) = open("cap_anchor");
        db.sql("CREATE TABLE t (v BIGINT)").unwrap();
        append(&db, (0..2_000).map(Some).collect());
        with_env1("QSDB_DENSE_GROUP_CAP", "1024", || {
            assert_eq!(q(&db, sqls[0]), 2_000, "手工期望");
            assert_ab(&db, &sqls);
        });
        let _ = std::fs::remove_dir_all(root);
    }
}

/// i64 端值：span 计算与下标全程 i128，`i64::MIN..=i64::MAX` 全域必须
/// 坠回而不 panic、不错分。SQL 字面量写不出 i64::MIN ⇒ 用 append。
#[test]
fn i64端值_坠回不panic_双路对撞() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let (db, root) = open("i64");
    db.sql("CREATE TABLE t (v BIGINT)").unwrap();
    append(&db, (0..10).map(Some).collect()); // 先锚一个小域
    append(
        &db,
        vec![Some(i64::MIN), Some(i64::MAX), Some(5), Some(5), None],
    );
    let sqls = [
        "SELECT count(DISTINCT v) FROM t",
        "SELECT count(*) FROM (SELECT DISTINCT v FROM t)",
    ];
    assert_eq!(
        q(&db, sqls[0]),
        12,
        "手工期望：0..9 十个 + MIN + MAX（NULL 不计）"
    );
    assert_eq!(q(&db, sqls[1]), 13, "手工期望：+1 个 NULL 行");
    assert_ab(&db, &sqls);
    let _ = std::fs::remove_dir_all(root);
}

/// 定种随机 parity：5 批 × 3000 行，值域 [-2000, 2000]（~5% NULL、大量重复），
/// dense 的增长重建与命中路在多批下反复进出，两条路必须逐值相等。
#[test]
fn 随机parity_定种多批_双路对撞() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let (db, root) = open("rand");
    db.sql("CREATE TABLE t (v BIGINT)").unwrap();
    let mut st: u64 = 0x9e37_79b9_7f4a_7c15;
    let mut next = move || {
        st ^= st << 13;
        st ^= st >> 7;
        st ^= st << 17;
        st
    };
    for _ in 0..5 {
        let vals: Vec<Option<i64>> = (0..3_000)
            .map(|_| {
                let r = next();
                if r % 19 == 0 {
                    None
                } else {
                    Some((r % 4_001) as i64 - 2_000)
                }
            })
            .collect();
        append(&db, vals);
    }
    let sqls = [
        "SELECT count(DISTINCT v) FROM t",
        "SELECT count(*) FROM (SELECT DISTINCT v FROM t)",
        "SELECT count(v) FROM (SELECT DISTINCT v FROM t)",
    ];
    let a = q(&db, sqls[0]);
    assert!(
        (3_000..=4_001).contains(&a),
        "随机批次基数应落在值域内，实际 {a}"
    );
    assert_eq!(q(&db, sqls[1]), a + 1, "+1 个 NULL 行");
    assert_eq!(q(&db, sqls[2]), a);
    assert_ab(&db, &sqls);
    let _ = std::fs::remove_dir_all(root);
}
