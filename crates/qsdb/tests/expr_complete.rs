//! M7-B 表达式补全 e2e：CASE / coalesce / nullif / || / Decimal round 族 /
//! Decimal % / 数学函数（向量化路径语义 + NULL 传播 + 域错误）。

use arrow::record_batch::RecordBatch;
use qsdb::{Qsdb, QsdbOptions};

fn db(tag: &str) -> (Qsdb, std::path::PathBuf) {
    let root = std::env::temp_dir()
        .join("qsdb_expr_complete")
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
    rows(&batches)
}

fn rows(batches: &[RecordBatch]) -> Vec<String> {
    let mut out = Vec::new();
    for b in batches {
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
    db.sql(
        r#"CREATE TABLE t (
        tenant STRING,
        amount DECIMAL(18,4),
        cnt BIGINT,
        score DOUBLE
    )"#,
    )
    .unwrap();
    db.sql(
        r#"INSERT INTO t VALUES
        ('a', 10.5000, 1, 1.5),
        ('b', 20.2500, 2, 2.5),
        ('a', NULL, NULL, 3.5),
        ('b', 30.0000, 3, NULL),
        ('c', 40.1250, 4, 4.5),
        (NULL, 50.0000, 5, 5.5)"#,
    )
    .unwrap();
    (db, root)
}

// ── CASE WHEN ─────────────────────────────────────────────────────────────

#[test]
fn case_searched_form() {
    let (db, _r) = setup("case_searched");
    // NULL 条件（cnt NULL 行）→ UNKNOWN → else 分支
    assert!(
        q(
            &db,
            "SELECT CASE WHEN cnt >= 3 THEN 'high' ELSE 'low' END FROM t"
        ) == vec!["high"; 3]
            .into_iter()
            .chain(vec!["low"; 3])
            .collect::<Vec<_>>()
    );
}

#[test]
fn case_simple_form() {
    let (db, _r) = setup("case_simple");
    assert!(
        q(
            &db,
            "SELECT CASE cnt WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'many' END FROM t"
        ) == vec!["many"; 4]
            .into_iter()
            .chain(vec!["one", "two"])
            .collect::<Vec<_>>()
    );
}

#[test]
fn case_no_else_is_null() {
    let (db, _r) = setup("case_no_else");
    assert!(q(&db, "SELECT CASE WHEN cnt > 10 THEN 'x' END FROM t") == vec!["NULL"; 6]);
}

#[test]
fn case_type_unification_int_float() {
    let (db, _r) = setup("case_unify");
    // THEN 1 ELSE 2.5：公共超类型 Float64（不丢 2.5）
    assert!(
        q(&db, "SELECT CASE WHEN cnt = 1 THEN 1 ELSE 2.5 END FROM t")
            == vec!["1.0", "2.5", "2.5", "2.5", "2.5", "2.5"]
    );
}

#[test]
fn case_null_branch_keeps_decimal_type() {
    let (db, _r) = setup("case_null_branch");
    assert!(
        q(
            &db,
            "SELECT CASE WHEN cnt = 3 THEN NULL ELSE amount END FROM t WHERE cnt <= 3"
        ) == vec!["10.5000", "20.2500", "NULL"]
    );
}

#[test]
fn case_in_where() {
    let (db, _r) = setup("case_where");
    assert!(
        q(
            &db,
            "SELECT cnt FROM t WHERE (CASE WHEN cnt >= 3 THEN 'y' ELSE 'n' END) = 'y'"
        ) == vec!["3", "4", "5"]
    );
}

#[test]
fn case_nested() {
    let (db, _r) = setup("case_nested");
    // cnt NULL 行：NULL > 2 → UNKNOWN → else 'small'
    assert!(
        q(
            &db,
            "SELECT CASE WHEN cnt > 2 THEN (CASE WHEN cnt > 4 THEN 'big' ELSE 'mid' END) ELSE 'small' END FROM t"
        ) == vec!["big", "mid", "mid", "small", "small", "small"]
    );
}

// ── coalesce / nullif ─────────────────────────────────────────────────────

#[test]
fn coalesce_basic() {
    let (db, _r) = setup("coalesce_basic");
    assert!(
        q(&db, "SELECT coalesce(tenant, 'unknown') FROM t")
            == vec!["a", "a", "b", "b", "c", "unknown"]
    );
}

#[test]
fn coalesce_all_null_chain() {
    let (db, _r) = setup("coalesce_chain");
    assert!(
        q(
            &db,
            "SELECT coalesce(NULL, NULL, cnt) FROM t WHERE cnt IS NULL"
        ) == vec!["NULL"]
    );
    assert!(
        q(
            &db,
            "SELECT coalesce(NULL, cnt, tenant) FROM t WHERE cnt = 1"
        ) == vec!["1"]
    );
}

#[test]
fn coalesce_decimal_literal_supertype() {
    let (db, _r) = setup("coalesce_supertype");
    // -1.0 是 decimal 字面量（Postgres 语义）：DECIMAL(18,4) + (2,1) → scale 大者
    assert!(
        q(
            &db,
            "SELECT coalesce(amount, -1.0) FROM t WHERE cnt IS NULL OR cnt <= 2"
        ) == vec!["-1.0000", "10.5000", "20.2500"]
    );
}

#[test]
fn coalesce_single_arg_errors() {
    let (db, _r) = setup("coalesce_arity");
    assert!(db.sql_collect("SELECT coalesce(cnt) FROM t").is_err());
}

#[test]
fn nullif_string() {
    let (db, _r) = setup("nullif_string");
    assert!(
        q(&db, "SELECT nullif(tenant, 'a') FROM t") == vec!["NULL", "NULL", "NULL", "b", "b", "c"]
    );
}

#[test]
fn nullif_int() {
    let (db, _r) = setup("nullif_int");
    assert!(q(&db, "SELECT nullif(cnt, 3) FROM t") == vec!["1", "2", "4", "5", "NULL", "NULL"]);
}

// ── || 拼接 ───────────────────────────────────────────────────────────────

#[test]
fn concat_basic() {
    let (db, _r) = setup("concat_basic");
    assert!(q(&db, "SELECT tenant || '-x' FROM t WHERE cnt <= 2") == vec!["a-x", "b-x"]);
}

#[test]
fn concat_null_propagates() {
    let (db, _r) = setup("concat_null");
    // tenant NULL 行 → NULL（SQL 语义）
    assert!(q(&db, "SELECT tenant || '!' FROM t WHERE cnt = 5") == vec!["NULL"]);
    assert!(q(&db, "SELECT 'n' || tenant FROM t") == vec!["NULL", "na", "na", "nb", "nb", "nc"]);
}

#[test]
fn concat_numeric_operand() {
    let (db, _r) = setup("concat_num");
    assert!(q(&db, "SELECT 'n' || cnt FROM t WHERE cnt = 1") == vec!["n1"]);
    assert!(q(&db, "SELECT tenant || '-' || 'y' FROM t WHERE cnt <= 1") == vec!["a-y"]);
}

// ── Decimal round / floor / ceil / abs ────────────────────────────────────

#[test]
fn decimal_round_half_up() {
    let (db, _r) = setup("dec_round");
    // 40.1250 round 2 → HALF_UP → 40.1300（scale 不变）
    assert!(q(&db, "SELECT round(amount, 2) FROM t WHERE cnt = 4") == vec!["40.1300"]);
    // digits >= scale → 零拷贝原样
    assert!(q(&db, "SELECT round(amount, 6) FROM t WHERE cnt = 4") == vec!["40.1250"]);
    assert!(q(&db, "SELECT round(amount, 2) FROM t WHERE cnt = 5") == vec!["50.0000"]);
    // NULL 传播
    assert!(q(&db, "SELECT round(amount, 2) FROM t WHERE cnt IS NULL") == vec!["NULL"]);
}

#[test]
fn decimal_floor_ceil() {
    let (db, _r) = setup("dec_floor_ceil");
    assert!(q(&db, "SELECT floor(amount) FROM t WHERE cnt = 4") == vec!["40.0000"]);
    assert!(q(&db, "SELECT ceil(amount) FROM t WHERE cnt = 1") == vec!["11.0000"]);
    assert!(q(&db, "SELECT ceil(amount) FROM t WHERE cnt = 5") == vec!["50.0000"]);
}

#[test]
fn decimal_abs() {
    let (db, _r) = setup("dec_abs");
    // |50 - 60| = 10.0000（decimal 保持 scale）
    assert!(q(&db, "SELECT abs(amount - 60) FROM t WHERE cnt = 5") == vec!["10.0000"]);
}

#[test]
fn decimal_mod() {
    let (db, _r) = setup("dec_mod");
    // 10.5 % 7 = 3.5（scale 4 对齐）
    assert!(q(&db, "SELECT amount % 7 FROM t WHERE cnt = 1") == vec!["3.5000"]);
    // 20.25 % 3 = 2.25
    assert!(q(&db, "SELECT amount % 3 FROM t WHERE cnt = 2") == vec!["2.2500"]);
    // NULL 传播
    assert!(q(&db, "SELECT amount % 7 FROM t WHERE cnt IS NULL") == vec!["NULL"]);
    // **除零 ⇒ NULL**（2026-10-17 行为**有变**，见下）。
    //
    // 这里原先是 `assert!(… .is_err())` —— 断言"除零必须报错"。
    // 按**真 Spark 3.5.2 实测**改成 NULL：`1/0`、小数除零、`1%0` **三格全是 NULL**，
    // 这是它 non-ANSI 模式的既定语义（真实语料就是 Spark SQL 方言，9 个块
    // 因为这条整批挂掉）。除零是**数据相关**的运行期状况（某行分母恰好 0），
    // 报错会让整批任务因为一个坏行全挂。
    //
    // ⚠️ **只改除零，不动溢出**：小数**溢出**仍然报错（`eval.rs::decimal_arith` 里
    // 的 `Mul` 那条），那是本仓"宁可报错也不静默回绕"的既有口径，与这条不是一件事。
    assert!(q(&db, "SELECT amount % 0 FROM t WHERE cnt = 1") == vec!["NULL"]);
}

// ── 数学函数 ──────────────────────────────────────────────────────────────

#[test]
fn math_unary_exact() {
    let (db, _r) = setup("math_exact");
    assert!(q(&db, "SELECT sqrt(cnt) FROM t WHERE cnt = 4") == vec!["2.0"]);
    assert!(q(&db, "SELECT power(cnt, 3) FROM t WHERE cnt = 2") == vec!["8.0"]);
    assert!(q(&db, "SELECT pow(2, 10) FROM t WHERE cnt = 1") == vec!["1024.0"]);
    assert!(
        q(
            &db,
            "SELECT cnt FROM t WHERE ln(score) > 0.4 AND ln(score) < 0.41"
        ) == vec!["1"]
    );
    assert!(q(&db, "SELECT exp(score - 1.5) FROM t WHERE cnt = 1") == vec!["1.0"]);
    assert!(q(&db, "SELECT sin(0), cos(0) FROM t WHERE cnt = 1") == vec!["0.0|1.0"]);
}

#[test]
fn math_decimal_and_null_inputs() {
    let (db, _r) = setup("math_dec_null");
    // Decimal 入参 → f64 换算（√40.125 ≈ 6.334）
    assert!(
        q(
            &db,
            "SELECT cnt FROM t WHERE sqrt(amount) > 6.33 AND sqrt(amount) < 6.34"
        ) == vec!["4"]
    );
    // NULL 传播
    assert!(q(&db, "SELECT sqrt(amount) FROM t WHERE cnt IS NULL") == vec!["NULL"]);
}

#[test]
fn math_domain_errors() {
    let (db, _r) = setup("math_domain");
    assert!(
        db.sql_collect("SELECT sqrt(-score) FROM t WHERE cnt = 1")
            .is_err()
    );
    assert!(
        db.sql_collect("SELECT ln(0 - score) FROM t WHERE cnt = 1")
            .is_err()
    );
    assert!(
        db.sql_collect("SELECT power(0, -1) FROM t WHERE cnt = 1")
            .is_err()
    );
}

// ── BETWEEN（M7-B3）───────────────────────────────────────────────────────

#[test]
fn between_numeric() {
    let (db, _r) = setup("between_num");
    assert!(q(&db, "SELECT cnt FROM t WHERE cnt BETWEEN 2 AND 4") == vec!["2", "3", "4"]);
    // NOT BETWEEN（NULL 行两侧都不取）
    assert!(q(&db, "SELECT cnt FROM t WHERE cnt NOT BETWEEN 2 AND 4") == vec!["1", "5"]);
    // 浮点界
    assert!(q(&db, "SELECT cnt FROM t WHERE score BETWEEN 1.5 AND 2.5") == vec!["1", "2"]);
    // 边界含入（>= / <=）
    assert!(q(&db, "SELECT cnt FROM t WHERE cnt BETWEEN 1 AND 2") == vec!["1", "2"]);
}

// ── 标量函数套聚合（M7-B3：round(sum(x)) 等）────────────────────────────

#[test]
fn scalar_over_aggregate() {
    let (db, _r) = setup("scalar_over_agg");
    // round(sum(score))：17.5 HALF_UP → 18
    assert!(q(&db, "SELECT round(sum(score)) FROM t") == vec!["18.0"]);
    assert!(q(&db, "SELECT round(sum(score), 1) FROM t") == vec!["17.5"]);
    // decimal 聚合 → round（scale 保持）
    assert!(q(&db, "SELECT round(sum(amount), 2) FROM t WHERE cnt <= 2") == vec!["30.7500"]);
    // abs / ceil
    assert!(q(&db, "SELECT abs(sum(cnt)) FROM t") == vec!["15"]);
    assert!(q(&db, "SELECT ceil(avg(score)) FROM t") == vec!["4.0"]);
    // 聚合 × 算术 × 标量嵌套
    assert!(q(&db, "SELECT round(sum(cnt) * 1.4) FROM t") == vec!["21.0"]);
    // group 列上包标量（此前同样报 not supported）
    assert!(
        q(&db, "SELECT upper(tenant), count(*) FROM t GROUP BY tenant")
            == vec!["A|2", "B|2", "C|1", "NULL|1"]
    );
    // HAVING 里标量套聚合
    assert!(
        q(
            &db,
            "SELECT tenant FROM t GROUP BY tenant HAVING round(sum(cnt)) >= 4"
        ) == vec!["NULL", "b", "c"]
    );
    // HAVING 聚合 BETWEEN
    assert!(
        q(
            &db,
            "SELECT tenant FROM t GROUP BY tenant HAVING sum(cnt) BETWEEN 4 AND 5"
        ) == vec!["NULL", "b", "c"]
    );
}

/// **数值/小数列 与 字符串字面量** 的比较：转不动的字符串 ⇒ 整条比较是 `NULL`
/// （Spark `ansi.enabled=false` 的语义），**不是**类型错误。
///
/// 2026-10-17：语料里"空串当零"的惯用写法
/// `CASE WHEN SUM_VALUE = '' THEN 0 ELSE SUM_VALUE END`（`SUM_VALUE` 是
/// `DECIMAL(38,8)`）**一下卡住 9 个块**，报的是
/// `cannot compare DECIMAL(38, 8) with BOOLEAN` —— 方向完全指错。
///
/// 病根：`promote_string_lit` 按 Spark 语义把转不动的字符串变成 `Lit(Null)`，
/// 可 "一侧是 NULL 就短路" 当时挂在提升**之前**，于是轮到 `cmp_common_type` 时
/// 拿到的是 `DECIMAL(38,8)` 对 `Lit(Null)`，而 `Lit(Null).data_type()` 的兜底
/// 恰好是 **`Boolean`** ⇒ 报出上面那条莫名其妙的错。
#[test]
fn 数值与字符串字面量比较_转不动给_null_而不是报错() {
    let (db, _r) = setup("strlit_cmp");
    // 空串（语料里的真实形状）：整条比较 NULL ⇒ `WHEN` 不取，落到 `ELSE`
    assert!(
        q(
            &db,
            "SELECT tenant, CASE WHEN amount = '' THEN 0 ELSE amount END FROM t"
        )
        .iter()
        .all(|r| !r.ends_with("|0")),
        "空串比较必须是 NULL（WHEN 不取）"
    );
    // 直接看比较结果本身：全 NULL
    assert!(
        q(&db, "SELECT amount = '' FROM t")
            .iter()
            .all(|r| r == "NULL")
    );
    assert!(
        q(&db, "SELECT amount <> '' FROM t")
            .iter()
            .all(|r| r == "NULL")
    );
    // 同族的整数 / 浮点 / 能转动的字符串
    assert!(
        q(&db, "SELECT cnt = 'z' FROM t")
            .iter()
            .all(|r| r == "NULL")
    );
    assert!(
        q(&db, "SELECT score = 'z' FROM t")
            .iter()
            .all(|r| r == "NULL")
    );
    assert!(
        q(&db, "SELECT CAST(1 AS BIGINT) = '1'")
            .iter()
            .all(|r| r == "true")
    );
    // 显式 NULL 仍然短路（这条一直都在，别被上面的改动带跑）
    assert!(
        q(&db, "SELECT cnt = NULL FROM t")
            .iter()
            .all(|r| r == "NULL")
    );
}
