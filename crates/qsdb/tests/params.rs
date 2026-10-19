//! **参数化查询**的逐值验收（设计见 `参数化查询设计稿`）。
//!
//! 这份清单里最要紧的三条，各自对应一个"看着能跑、其实错"的失败模式：
//! ① **注入**：值必须是**数据**，不是可执行文本（跑完表还得在）；
//! ② **`?` 的序号不能错位**：本仓有"把同一个表达式再走一遍"的路径
//!    （HAVING / 窗口重绑），绑定期编号会错位 ⇒ 所以序号在**解析前**就写死在文本里；
//! ③ **个数必须精确对账**：少给 = 绑不上，多给 = 调用方以为绑上了其实没有。

use qsdb::query::QueryContext;
use qsdb::sql::params::params_from_json;
use qsdb::sql::{SqlResult, execute_sql_params_ctx};
use qsdb::{Qsdb, QsdbOptions};

/// 测试目录唯一序号：pid 挡跨进程碰撞，序号挡同进程内并发（Windows 时钟
/// 粒度 ~100ns，纳秒后缀在同 tick 内会重复——CI 上真炸过，见 constraints）。
fn _uniq_seq() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    SEQ.fetch_add(1, Ordering::Relaxed)
}

fn setup(tag: &str) -> (Qsdb, std::path::PathBuf) {
    let root = std::env::temp_dir().join("qsdb_params").join(format!(
        "{tag}_{}_{}",
        std::process::id(),
        format!("{:x}_{}", std::process::id(), _uniq_seq())
    ));
    std::fs::create_dir_all(&root).unwrap();
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    db.sql("CREATE TABLE t (g BIGINT, v BIGINT, s VARCHAR, d DATE)")
        .unwrap();
    db.sql(
        "INSERT INTO t VALUES
         (1,10,'a',DATE '2026-01-01'),(2,20,'b',DATE '2026-02-01'),
         (1,30,'x; DROP TABLE t',NULL)",
    )
    .unwrap();
    (db, root)
}

/// 跑一条带参数的查询，返回每行 "c0|c1|…"（**保持行序** —— 值级断言要看顺序）。
fn q(db: &Qsdb, sql: &str, params: serde_json::Value) -> Vec<String> {
    let ps = params_from_json(Some(&params)).unwrap_or_else(|e| panic!("参数解析 {params}: {e}"));
    match execute_sql_params_ctx(db, sql, ps, &QueryContext::benign())
        .unwrap_or_else(|e| panic!("{sql} (params={params}): {e}"))
    {
        SqlResult::Query { batches, .. } => batches
            .iter()
            .flat_map(|b| {
                (0..b.num_rows())
                    .map(|r| {
                        (0..b.num_columns())
                            .map(|c| {
                                let a = b.column(c);
                                if a.is_null(r) {
                                    "NULL".into()
                                } else {
                                    arrow::util::display::array_value_to_string(a.as_ref(), r)
                                        .unwrap()
                                }
                            })
                            .collect::<Vec<_>>()
                            .join("|")
                    })
                    .collect::<Vec<_>>()
            })
            .collect(),
        other => panic!("期望查询结果，实得 {other:?}"),
    }
}

fn err_of(db: &Qsdb, sql: &str, params: serde_json::Value) -> String {
    let ps = match params_from_json(Some(&params)) {
        Ok(p) => p,
        Err(e) => return e.to_string(),
    };
    match execute_sql_params_ctx(db, sql, ps, &QueryContext::benign()) {
        Ok(r) => panic!("这条该被拒: {sql} (params={params}) → {r:?}"),
        Err(e) => e.to_string(),
    }
}

// ── ① 两种占位符都认，位置都对 ───────────────────────────────────────────
#[test]
fn 位置占位符() {
    let (db, root) = setup("pos");
    assert_eq!(
        q(
            &db,
            "SELECT g, v FROM t WHERE g = $1 ORDER BY v",
            serde_json::json!([1])
        ),
        ["1|10", "1|30"]
    );
    // `?` 按出现顺序
    assert_eq!(
        q(
            &db,
            "SELECT v FROM t WHERE g = ? ORDER BY v",
            serde_json::json!([2])
        ),
        ["20"]
    );
    assert_eq!(
        q(
            &db,
            "SELECT v FROM t WHERE g = ? OR v = ? ORDER BY v",
            serde_json::json!([1, 20])
        ),
        ["10", "20", "30"]
    );
    // `$n` **可重复引用**（`?` 做不到，这是选它的理由之一）
    assert_eq!(
        q(
            &db,
            "SELECT count(*) FROM t WHERE g = $1 OR v = $1",
            serde_json::json!([20])
        ),
        ["1"]
    );
    let _ = std::fs::remove_dir_all(&root);
}

// ── ② ★ 注入：值只能是数据 ──────────────────────────────────────────────
#[test]
fn 值只是数据不是文本() {
    let (db, root) = setup("inject");
    // 表里第 3 行的 s 就是一段"看起来像 SQL 的文本"
    assert_eq!(
        q(
            &db,
            "SELECT s FROM t WHERE g = $1 ORDER BY s",
            serde_json::json!([1])
        ),
        ["a", "x; DROP TABLE t"]
    );
    // 参数里塞更完整的注入串：整串必须**原样回显**，且什么都没发生
    let evil = "'; DROP TABLE t; --";
    assert_eq!(
        q(
            &db,
            "SELECT s FROM t WHERE s = $1",
            serde_json::json!([evil])
        ),
        Vec::<String>::new(), // 表里没有这一行（这本身证明它被当成**数据**比较了）
    );
    assert_eq!(
        q(&db, "SELECT count(*) FROM t", serde_json::json!([])),
        ["3"]
    );
    // 换个角度：让注入串**真的匹配上**，回显必须是原串
    q(&db, "SELECT 1", serde_json::json!([]));
    db.sql(&format!(
        "INSERT INTO t VALUES (9, 99, '{}', NULL)",
        evil.replace('\'', "''")
    ))
    .unwrap();
    assert_eq!(
        q(&db, "SELECT s FROM t WHERE g = $1", serde_json::json!([9])),
        [evil]
    );
    assert_eq!(
        q(&db, "SELECT count(*) FROM t", serde_json::json!([])),
        ["4"]
    );
    let _ = std::fs::remove_dir_all(&root);
}

// ── ③ ★ `?` 的序号不许错位（HAVING / 窗口会**重走**表达式）──────────────
//
// 这条钉的是设计里那个"为什么必须在解析前编号"的决定：`rebind_over_agg`
// 会把投影/HAVING 表达式**再绑一遍**，绑定期用计数器编号就会错位。
#[test]
fn 重走表达式时序号不错位() {
    let (db, root) = setup("rewalk");
    // HAVING 走重绑路径；两个 `?` 分别是 WHERE 的阈值与 HAVING 的阈值
    assert_eq!(
        q(
            &db,
            "SELECT g, count(*) FROM t WHERE v > ? GROUP BY g HAVING count(*) >= ? ORDER BY g",
            serde_json::json!([5, 2])
        ),
        // v>5 全过：g=1 两行、g=2 一行 ⇒ HAVING count>=2 只留 g=1
        ["1|2"]
    );
    // 反过来传（阈值对调）必须**给出不同答案** —— 否则说明两个位置绑到了同一个值
    assert_eq!(
        q(
            &db,
            "SELECT g, count(*) FROM t WHERE v > ? GROUP BY g HAVING count(*) >= ? ORDER BY g",
            serde_json::json!([25, 1])
        ),
        // v>25 只剩 g=1 的一行 ⇒ g=1 计数 1 ≥ 1 通过
        ["1|1"]
    );
    // 窗口函数的 spec 里也带 `?`（帧偏移、lag/lead 偏移是在**绑定期之前**从 AST 上
    // 直接读整数的两处 —— 它们也认参数，否则用户会收到"必须是整数字面量"）
    assert_eq!(
        q(
            &db,
            // 用 v（唯一）排序 ⇒ 行序确定：10, 20, 30 ⇒ 滑窗和 10 / 30 / 50
            "SELECT g, sum(v) OVER (ORDER BY v ROWS BETWEEN ? PRECEDING AND CURRENT ROW)              FROM t ORDER BY v",
            serde_json::json!([1])
        ),
        ["1|10", "2|30", "1|50"]
    );
    assert_eq!(
        q(
            &db,
            "SELECT lag(v, ?) OVER (ORDER BY g) FROM t LIMIT 1",
            serde_json::json!([1])
        ),
        ["NULL"]
    );
    let _ = std::fs::remove_dir_all(&root);
}

// ── ④ `?` 在字符串/注释/带引号标识符里**不是**占位符 ─────────────────────
#[test]
fn 问号只在占位符位置才算() {
    let (db, root) = setup("qmark");
    // 字符串字面量里的 `?` 与 `$1` 都只是文本
    assert_eq!(
        q(
            &db,
            "SELECT '?' AS a, '含中文?' AS b",
            serde_json::json!([])
        ),
        ["?|含中文?"]
    );
    // ⚠️ 含中文的 SQL + 真占位符：`Location.column` 数字符不数字节，
    //    换算错位的话这里会绑错（或直接失败）
    assert_eq!(
        q(
            &db,
            "SELECT '含中文内容' AS c FROM t WHERE g = ? ORDER BY c LIMIT 1",
            serde_json::json!([1])
        ),
        ["含中文内容"]
    );
    // 注释里的 `?` 不算
    assert_eq!(
        q(
            &db,
            "SELECT 1 AS x -- 这里有 ?\nFROM t WHERE g = ? LIMIT 1",
            serde_json::json!([2])
        ),
        ["1"]
    );
    let _ = std::fs::remove_dir_all(&root);
}

// ── ⑤ 显式类型 / 推断 ───────────────────────────────────────────────────
#[test]
fn 类型映射() {
    let (db, root) = setup("types");
    // 推断：整数 / 小数 / 布尔 / 字符串 / NULL
    assert_eq!(q(&db, "SELECT $1", serde_json::json!([7])), ["7"]);
    assert_eq!(q(&db, "SELECT $1", serde_json::json!([1.5])), ["1.5"]);
    assert_eq!(q(&db, "SELECT $1", serde_json::json!([true])), ["true"]);
    assert_eq!(q(&db, "SELECT $1", serde_json::json!(["hi"])), ["hi"]);
    assert_eq!(q(&db, "SELECT $1", serde_json::json!([null])), ["NULL"]);
    // 显式：DECIMAL 用**字符串**保精度（过 f64 会丢）
    assert_eq!(
        q(
            &db,
            "SELECT $1",
            serde_json::json!([{"type": "DECIMAL(18,2)", "value": "12345678901234.56"}])
        ),
        ["12345678901234.56"]
    );
    // 显式 DATE
    assert_eq!(
        q(
            &db,
            "SELECT count(*) FROM t WHERE d = $1",
            serde_json::json!([{"type": "DATE", "value": "2026-01-01"}])
        ),
        ["1"]
    );
    // 字符串推成 Varchar 后，比时间列**照样能用**（与不带参数写 '…' 同一套规则）
    assert_eq!(
        q(
            &db,
            "SELECT count(*) FROM t WHERE d = $1",
            serde_json::json!(["2026-02-01"])
        ),
        ["1"]
    );
    let _ = std::fs::remove_dir_all(&root);
}

// ── ⑥ 错误面（每条都要能分辨"拒了"与"给错值"）────────────────────────────
#[test]
fn 错误面() {
    let (db, root) = setup("errors");
    // 少给 / 多给 —— **在任何副作用之前**就拒（这里顺带证明：写不进去）
    assert!(
        err_of(
            &db,
            "SELECT * FROM t WHERE g = $1 AND v = $2",
            serde_json::json!([1])
        )
        .contains("参数不够")
    );
    assert!(
        err_of(
            &db,
            "SELECT * FROM t WHERE g = $1",
            serde_json::json!([1, 2])
        )
        .contains("参数多了")
    );
    // 混用
    assert!(
        err_of(
            &db,
            "SELECT * FROM t WHERE g = ? AND v = $2",
            serde_json::json!([1, 2])
        )
        .contains("不能混用")
    );
    // `$0`
    assert!(
        err_of(&db, "SELECT * FROM t WHERE g = $0", serde_json::json!([1]))
            .contains("从 **1** 开始")
    );
    // 类型对不上：字符串参数落在数值列上 ⇒ 比较本身是 **NULL**（Spark 非 ANSI
    // 语义，`1 = 'z'` ⇒ NULL —— 与真 Spark 3.5.2 对过账，见
    // `promote_string_lit` 与 `expr_complete::数值与字符串字面量比较_…`），
    // WHERE 过滤后就是**空结果**，不是绑定期报错。
    // （历史注：这里曾断言 "cannot compare" —— 那是字面量提升落地前的旧契约。）
    assert!(
        q(
            &db,
            "SELECT * FROM t WHERE v = $1",
            serde_json::json!(["abc"])
        )
        .is_empty()
    );
    // 显式类型名不认识 / 值转不过去
    assert!(
        err_of(
            &db,
            "SELECT $1",
            serde_json::json!([{"type": "NOSUCHTYPE", "value": 1}])
        )
        .contains("不认识")
    );
    let e = err_of(
        &db,
        "SELECT $1",
        serde_json::json!([{"type": "DATE", "value": "不是日期"}]),
    );
    assert!(
        e.contains("第 1 个参数（DATE）") && e.contains("DATE"),
        "{e}"
    );
    // `params` 不是数组
    assert!(err_of(&db, "SELECT 1", serde_json::json!({"a": 1})).contains("必须是数组"));
    // **没给参数却写了占位符 ⇒ 报错**（绝不能静默当 NULL）
    assert!(
        err_of(&db, "SELECT * FROM t WHERE g = $1", serde_json::json!([])).contains("参数不够")
    );
    let _ = std::fs::remove_dir_all(&root);
}

// ── ⑦ 写路径也能用参数（且个数不对时**什么都没写进去**）────────────────
#[test]
fn 写路径() {
    let (db, root) = setup("write");
    let ps = params_from_json(Some(&serde_json::json!([7, 70, "seven"]))).unwrap();
    execute_sql_params_ctx(
        &db,
        "INSERT INTO t VALUES ($1, $2, $3, NULL)",
        ps,
        &QueryContext::benign(),
    )
    .unwrap();
    assert_eq!(
        q(&db, "SELECT v FROM t WHERE g = $1", serde_json::json!([7])),
        ["70"]
    );
    // 个数不对 ⇒ 报错，且**一行都没写**
    let before = q(&db, "SELECT count(*) FROM t", serde_json::json!([]));
    assert!(
        err_of(
            &db,
            "INSERT INTO t VALUES ($1, $2, $3, NULL)",
            serde_json::json!([8, 80])
        )
        .contains("参数不够")
    );
    assert_eq!(
        q(&db, "SELECT count(*) FROM t", serde_json::json!([])),
        before
    );
    let _ = std::fs::remove_dir_all(&root);
}
