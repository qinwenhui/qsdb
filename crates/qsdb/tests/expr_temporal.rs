//! M7-B2 日期算术 e2e：DATE±INTEGER / DATE−DATE / date_diff / extract，
//! 含 F16（temporal × 字符串字面量比较的绑定期提升）。

use qsdb::{Qsdb, QsdbOptions};

fn db(tag: &str) -> (Qsdb, std::path::PathBuf) {
    let root = std::env::temp_dir()
        .join("qsdb_expr_temporal")
        .join(format!(
            "{tag}_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    (db, root)
}

fn q(db: &Qsdb, sql: &str) -> Vec<String> {
    let mut r = raw(db, sql);
    r.sort();
    r
}

fn raw(db: &Qsdb, sql: &str) -> Vec<String> {
    let batches = db.sql_collect(sql).unwrap();
    let mut out = Vec::new();
    for b in &batches {
        for i in 0..b.num_rows() {
            let mut cells = Vec::with_capacity(b.num_columns());
            for c in 0..b.num_columns() {
                let arr = b.column(c);
                if arr.is_null(i) {
                    cells.push("NULL".to_string());
                } else {
                    cells.push(
                        arrow::util::display::array_value_to_string(arr.as_ref(), i).unwrap(),
                    );
                }
            }
            out.push(cells.join("|"));
        }
    }
    out
}

fn setup(tag: &str) -> (Qsdb, std::path::PathBuf) {
    let (db, root) = db(tag);
    db.sql("CREATE TABLE t (dt DATE, ts TIMESTAMP, cnt BIGINT)")
        .unwrap();
    db.sql(
        r#"INSERT INTO t VALUES
        (DATE '2026-01-31', TIMESTAMP '2026-01-31 08:30:00', 1),
        (DATE '2026-02-01', TIMESTAMP '2026-02-01 23:59:59', 2),
        (NULL, NULL, 3),
        (DATE '2026-03-10', TIMESTAMP '2026-03-10 12:00:00', 4)"#,
    )
    .unwrap();
    (db, root)
}

// ── DATE ± INTEGER / DATE − DATE ──────────────────────────────────────────

#[test]
fn date_plus_minus_int() {
    let (db, _r) = setup("date_pm_int");
    // 2026-01-31 + 1 = 2026-02-01（跨月）
    assert!(q(&db, "SELECT dt + 1 FROM t WHERE cnt = 1") == vec!["2026-02-01"]);
    // 2026-02-01 - 1 = 2026-01-31
    assert!(q(&db, "SELECT dt - 1 FROM t WHERE cnt = 2") == vec!["2026-01-31"]);
    // int + date 可换序
    assert!(q(&db, "SELECT 7 + dt FROM t WHERE cnt = 1") == vec!["2026-02-07"]);
    // 大跨度：+365 跨年（2026 非闰 → 2027-01-31）
    assert!(q(&db, "SELECT dt + 365 FROM t WHERE cnt = 1") == vec!["2027-01-31"]);
    // NULL 传播
    assert!(q(&db, "SELECT dt + 1 FROM t WHERE cnt = 3") == vec!["NULL"]);
}

#[test]
fn date_minus_date_days() {
    let (db, _r) = setup("date_minus_date");
    // 2026-01-31 → 2026-03-10：2 月 28 天 → 38 天
    assert!(q(&db, "SELECT dt - DATE '2026-01-31' FROM t WHERE cnt = 4") == vec!["38"]);
    // 反向为负
    assert!(q(&db, "SELECT DATE '2026-01-31' - dt FROM t WHERE cnt = 4") == vec!["-38"]);
}

#[test]
fn date_arith_errors() {
    let (db, _r) = setup("date_arith_err");
    assert!(
        db.sql_collect("SELECT dt + dt FROM t WHERE cnt = 1")
            .is_err()
    );
    assert!(
        db.sql_collect("SELECT 5 - dt FROM t WHERE cnt = 1")
            .is_err()
    );
    assert!(
        db.sql_collect("SELECT dt + 1.5 FROM t WHERE cnt = 1")
            .is_err()
    );
    assert!(
        db.sql_collect("SELECT dt * 2 FROM t WHERE cnt = 1")
            .is_err()
    );
}

// ── date_diff ─────────────────────────────────────────────────────────────

#[test]
fn date_diff_units() {
    let (db, _r) = setup("date_diff_units");
    assert!(
        q(
            &db,
            "SELECT date_diff('day', DATE '2026-01-31', dt) FROM t WHERE cnt = 4"
        ) == vec!["38"]
    );
    // 月边界跨越：1-31 → 3-10 跨 2/1、3/1 两个月边界 = 2
    assert!(
        q(
            &db,
            "SELECT date_diff('month', DATE '2026-01-31', dt) FROM t WHERE cnt = 4"
        ) == vec!["2"]
    );
    // 1-31 → 2-01 = 1 个月（跨 2 月边界）
    assert!(
        q(
            &db,
            "SELECT date_diff('month', dt, DATE '2026-02-01') FROM t WHERE cnt = 1"
        ) == vec!["1"]
    );
    assert!(
        q(
            &db,
            "SELECT date_diff('year', DATE '2025-12-31', DATE '2026-01-01') FROM t WHERE cnt = 1"
        ) == vec!["1"]
    );
    // 周：2026-01-31 是周六、2026-03-10 是周二 → 跨 6 个周一
    assert!(
        q(
            &db,
            "SELECT date_diff('week', dt, DATE '2026-03-10') FROM t WHERE cnt = 1"
        ) == vec!["6"]
    );
    // 复数单位收单数
    assert!(
        q(
            &db,
            "SELECT date_diff('days', DATE '2026-01-31', dt) FROM t WHERE cnt = 4"
        ) == vec!["38"]
    );
}

#[test]
fn date_diff_string_literal_promotion() {
    let (db, _r) = setup("date_diff_promo");
    // 字符串日期字面量 → DATE 提升（F16 同款白名单）
    assert!(
        q(
            &db,
            "SELECT date_diff('day', '2026-01-31', dt) FROM t WHERE cnt = 4"
        ) == vec!["38"]
    );
}

#[test]
fn date_diff_timestamp_units() {
    let (db, _r) = setup("date_diff_ts");
    // 边界跨越语义：08:30 → 12:00 跨 09/10/11/12 四个小时边界 = 4；分钟 510→720 = 210
    assert!(
        q(
            &db,
            "SELECT date_diff('hour', ts, TIMESTAMP '2026-01-31 12:00:00') FROM t WHERE cnt = 1"
        ) == vec!["4"]
    );
    assert!(
        q(
            &db,
            "SELECT date_diff('minute', ts, TIMESTAMP '2026-01-31 12:00:00') FROM t WHERE cnt = 1"
        ) == vec!["210"]
    );
    // DATE 侧 vs TIMESTAMP 侧 → Timestamp 归一（当日 00:00 起）
    assert!(
        q(
            &db,
            "SELECT date_diff('hour', DATE '2026-03-10', ts) FROM t WHERE cnt = 4"
        ) == vec!["12"]
    );
    // DATE 上用时间单位 → 报错
    assert!(
        db.sql_collect("SELECT date_diff('hour', dt, dt) FROM t WHERE cnt = 1")
            .is_err()
    );
}

// ── EXTRACT ───────────────────────────────────────────────────────────────

#[test]
fn extract_from_date() {
    let (db, _r) = setup("extract_date");
    assert!(q(&db, "SELECT EXTRACT(YEAR FROM dt) FROM t WHERE cnt = 1") == vec!["2026"]);
    assert!(q(&db, "SELECT EXTRACT(MONTH FROM dt) FROM t WHERE cnt = 2") == vec!["2"]);
    assert!(q(&db, "SELECT EXTRACT(DAY FROM dt) FROM t WHERE cnt = 1") == vec!["31"]);
    // 季度
    assert!(q(&db, "SELECT EXTRACT(QUARTER FROM dt) FROM t WHERE cnt = 4") == vec!["1"]);
    // doy：2026-01-31 = 31
    assert!(q(&db, "SELECT EXTRACT(DOY FROM dt) FROM t WHERE cnt = 1") == vec!["31"]);
    // dow：2026-01-31 是周六 = 6（0=周日）
    assert!(q(&db, "SELECT EXTRACT(DOW FROM dt) FROM t WHERE cnt = 1") == vec!["6"]);
    // NULL 传播
    assert!(q(&db, "SELECT EXTRACT(YEAR FROM dt) FROM t WHERE cnt = 3") == vec!["NULL"]);
    // 函数形态（逗号）
    assert!(q(&db, "SELECT extract('year', dt) FROM t WHERE cnt = 1") == vec!["2026"]);
}

#[test]
fn extract_from_timestamp() {
    let (db, _r) = setup("extract_ts");
    assert!(q(&db, "SELECT EXTRACT(HOUR FROM ts) FROM t WHERE cnt = 1") == vec!["8"]);
    assert!(q(&db, "SELECT EXTRACT(MINUTE FROM ts) FROM t WHERE cnt = 1") == vec!["30"]);
    assert!(q(&db, "SELECT EXTRACT(SECOND FROM ts) FROM t WHERE cnt = 2") == vec!["59"]);
    assert!(q(&db, "SELECT EXTRACT(DAY FROM ts) FROM t WHERE cnt = 4") == vec!["10"]);
}

// ── F16：temporal × 字符串字面量比较 ──────────────────────────────────────

#[test]
fn f16_string_literal_compare_date() {
    let (db, _r) = setup("f16_date");
    // 字符串字面量 → DATE 提升（qsdb-sync 真库 e2e 踩到的迁移摩擦）
    assert!(q(&db, "SELECT cnt FROM t WHERE dt = '2026-01-31'") == vec!["1"]);
    assert!(q(&db, "SELECT cnt FROM t WHERE dt > '2026-02-15'") == vec!["4"]);
    // IN（OR 链）同路径；BETWEEN 表达式 binder 本不支持（既有缺口，与本特性无关）
    assert!(
        q(
            &db,
            "SELECT cnt FROM t WHERE dt IN ('2026-01-31', '2026-03-10')"
        ) == vec!["1", "4"]
    );
    // 解析失败 → 明确指引（方向 2 错误信息并入）
    let err = db
        .sql_collect("SELECT cnt FROM t WHERE dt = 'not-a-date'")
        .unwrap_err()
        .to_string();
    assert!(err.contains("DATE 'not-a-date'"), "got: {err}");
    // 字符串**列** × temporal 仍诚实报错（白名单仅字面量）
    assert!(
        db.sql_collect("SELECT 1 FROM t, (SELECT 'x' AS s) v WHERE dt = v.s")
            .is_err()
    );
}

#[test]
fn f16_string_literal_compare_timestamp() {
    let (db, _r) = setup("f16_ts");
    // 完整时间戳字符串
    assert!(q(&db, "SELECT cnt FROM t WHERE ts = '2026-01-31 08:30:00'") == vec!["1"]);
    // 纯日期字符串 → 当日 00:00（MySQL 语义）
    assert!(q(&db, "SELECT cnt FROM t WHERE ts > '2026-03-01'") == vec!["4"]);
    assert!(q(&db, "SELECT cnt FROM t WHERE ts = '2026-03-10'") == Vec::<String>::new());
    // CASE 简单形态 / nullif 同款提升（bind_cmp_phys）
    assert!(
        q(
            &db,
            "SELECT CASE dt WHEN '2026-01-31' THEN 'jan' ELSE 'other' END FROM t WHERE cnt = 1"
        ) == vec!["jan"]
    );
}

#[test]
fn f16_promoted_predicate_still_prunes() {
    let (db, _r) = setup("f16_prune");
    // 提升后谓词 = typed 字面量 → 与显式 DATE '' 完全同形（含分区裁剪路径）
    assert!(
        q(&db, "SELECT cnt FROM t WHERE dt = '2026-01-31'")
            == q(&db, "SELECT cnt FROM t WHERE dt = DATE '2026-01-31'")
    );
}

// ── BETWEEN（M7-B3）───────────────────────────────────────────────────────

#[test]
fn between_temporal() {
    let (db, _r) = setup("between_ts");
    // 字符串字面量界（F16 提升在 BETWEEN 内生效）
    assert!(
        q(
            &db,
            "SELECT cnt FROM t WHERE dt BETWEEN '2026-01-01' AND '2026-02-01'"
        ) == vec!["1", "2"]
    );
    // typed 字面量同形
    assert!(
        q(
            &db,
            "SELECT cnt FROM t WHERE dt BETWEEN DATE '2026-01-01' AND DATE '2026-02-01'"
        ) == vec!["1", "2"]
    );
    // NOT BETWEEN
    assert!(
        q(
            &db,
            "SELECT cnt FROM t WHERE dt NOT BETWEEN '2026-01-01' AND '2026-03-01'"
        ) == vec!["4"]
    );
    // NULL 界 → UNKNOWN → 全排除
    assert!(
        q(
            &db,
            "SELECT cnt FROM t WHERE dt BETWEEN NULL AND '2026-02-01'"
        ) == Vec::<String>::new()
    );
    // dt NULL 行（cnt=3）两侧都不取
    assert!(
        q(
            &db,
            "SELECT cnt FROM t WHERE dt BETWEEN '2020-01-01' AND '2030-01-01'"
        ) == vec!["1", "2", "4"]
    );
    // timestamp 界
    assert!(
        q(
            &db,
            "SELECT cnt FROM t WHERE ts BETWEEN '2026-01-31 00:00:00' AND '2026-02-01 23:59:59'"
        ) == vec!["1", "2"]
    );
    // 表达式主体（算术结果作 BETWEEN 输入）
    assert!(
        q(
            &db,
            "SELECT cnt FROM t WHERE dt + 10 BETWEEN '2026-02-05' AND '2026-02-15'"
        ) == vec!["1", "2"]
    );
}

// ── date_trunc（M7-B3：文档漂移补齐）────────────────────────────────────

#[test]
fn date_trunc_units() {
    let (db, _r) = setup("date_trunc");
    assert!(
        q(&db, "SELECT date_trunc('month', ts) FROM t WHERE cnt = 4")
            == vec!["2026-03-01T00:00:00"]
    );
    assert!(
        q(&db, "SELECT date_trunc('year', ts) FROM t WHERE cnt = 4") == vec!["2026-01-01T00:00:00"]
    );
    // 2026-03-10 周二 → 周一 03-09
    assert!(
        q(&db, "SELECT date_trunc('week', ts) FROM t WHERE cnt = 4") == vec!["2026-03-09T00:00:00"]
    );
    assert!(
        q(&db, "SELECT date_trunc('hour', ts) FROM t WHERE cnt = 1") == vec!["2026-01-31T08:00:00"]
    );
    assert!(
        q(&db, "SELECT date_trunc('minute', ts) FROM t WHERE cnt = 1")
            == vec!["2026-01-31T08:30:00"]
    );
    // DATE 入参 → DATE 出（同型）
    assert!(q(&db, "SELECT date_trunc('month', dt) FROM t WHERE cnt = 4") == vec!["2026-03-01"]);
    assert!(q(&db, "SELECT date_trunc('quarter', dt) FROM t WHERE cnt = 4") == vec!["2026-01-01"]);
    // 2026-01-31 周六 → 周一 01-26
    assert!(q(&db, "SELECT date_trunc('week', dt) FROM t WHERE cnt = 1") == vec!["2026-01-26"]);
    // DATE 上时间单位 → 报错；NULL 传播
    assert!(
        db.sql_collect("SELECT date_trunc('hour', dt) FROM t WHERE cnt = 1")
            .is_err()
    );
    assert!(q(&db, "SELECT date_trunc('month', dt) FROM t WHERE cnt = 3") == vec!["NULL"]);
}
