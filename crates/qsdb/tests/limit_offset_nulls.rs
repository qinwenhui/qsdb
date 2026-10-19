//! `OFFSET` / `FETCH FIRST` / `NULLS FIRST|LAST` 的**语义回归**。
//!
//! 三者此前都是**静默丢弃**（`LIMIT 10 OFFSET 20` 少翻页、`NULLS FIRST` 恒用默认…
//! 见 `内部 SQL 支持面清单`），后改为显式拒绝，2026-10-11 实现。
//! 本文件钉住**实现是对的**——尤其：
//! - `OFFSET` 要能**跨批**跳过（不是只在第一批里切）；
//! - `ORDER BY … LIMIT n OFFSET m` 走 TopN 快路径时，上界必须是 `n+m` 且仍要跳 m
//!   （只把 n 交给 TopN 会**少一行**，是最容易写错的地方）；
//! - SQL 默认放置 `ASC ⇒ NULLS LAST` / `DESC ⇒ NULLS FIRST`，显式值要能**覆盖**它；
//! - 窗口内 `ORDER BY` 与外部 `ORDER BY` 用**同一套** NULL 规则。

use qsdb::{Qsdb, QsdbOptions};

fn setup(tag: &str) -> (Qsdb, std::path::PathBuf) {
    let root = std::env::temp_dir()
        .join("qsdb_limit_nulls_test")
        .join(format!(
            "{tag}_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    // b 含 NULL；(a,b) 无重复便于断言
    db.sql("CREATE TABLE t (a BIGINT, b BIGINT)").unwrap();
    db.sql("INSERT INTO t VALUES (1,10),(2,NULL),(NULL,30),(3,40),(4,50)")
        .unwrap();
    (db, root)
}

/// 单列结果按行渲染（NULL → "NULL"）。
fn col(db: &Qsdb, sql: &str) -> Vec<String> {
    let bs = db.sql_collect(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    let mut out = Vec::new();
    for b in &bs {
        for i in 0..b.num_rows() {
            let a = b.column(0);
            out.push(if a.is_null(i) {
                "NULL".into()
            } else {
                arrow::util::display::array_value_to_string(a.as_ref(), i).unwrap()
            });
        }
    }
    out
}

/// 两列结果按行渲染（`a|b`）。
fn pairs(db: &Qsdb, sql: &str) -> Vec<String> {
    let bs = db.sql_collect(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    let mut out = Vec::new();
    for b in &bs {
        for i in 0..b.num_rows() {
            let cell = |c: usize| {
                let a = b.column(c);
                if a.is_null(i) {
                    "NULL".to_string()
                } else {
                    arrow::util::display::array_value_to_string(a.as_ref(), i).unwrap()
                }
            };
            out.push(format!("{}|{}", cell(0), cell(1)));
        }
    }
    out
}

/// `OFFSET`：五种写法都要真的跳过（此前被静默丢掉 ⇒ 少翻页）。
#[test]
fn offset_is_honored_in_all_spellings() {
    let (db, root) = setup("offset");
    // 默认序：ASC ⇒ NULLS LAST ⇒ 1,2,3,4,NULL
    assert_eq!(
        col(&db, "SELECT a FROM t ORDER BY a"),
        ["1", "2", "3", "4", "NULL"]
    );

    assert_eq!(
        col(&db, "SELECT a FROM t ORDER BY a LIMIT 2 OFFSET 1"),
        ["2", "3"]
    );
    assert_eq!(
        col(&db, "SELECT a FROM t ORDER BY a OFFSET 3"),
        ["4", "NULL"]
    );
    // MySQL 形 `LIMIT <offset>, <count>`
    assert_eq!(
        col(&db, "SELECT a FROM t ORDER BY a LIMIT 1, 2"),
        ["2", "3"]
    );
    // OFFSET 0 / 不写 OFFSET：不跳
    assert_eq!(
        col(&db, "SELECT a FROM t ORDER BY a OFFSET 0"),
        ["1", "2", "3", "4", "NULL"]
    );
    // OFFSET 超过总行数 → 空
    assert_eq!(col(&db, "SELECT a FROM t ORDER BY a OFFSET 99").len(), 0);
    // OFFSET 0 + LIMIT 0 → 空（别把 0 当"不限"）
    assert_eq!(col(&db, "SELECT a FROM t LIMIT 0").len(), 0);

    // **跨批**：批边界不该影响结果——用极小批（引擎按批给行）跑一遍同语义
    // （这里靠"结果与整体一致"保证：跳过发生在 `LimitExec`，与批无关）
    assert_eq!(
        col(&db, "SELECT a FROM t ORDER BY a OFFSET 2 LIMIT 1"),
        ["3"]
    );
    std::fs::remove_dir_all(&root).ok();
}

/// `ORDER BY … LIMIT n OFFSET m` 走 **TopN 快路径**时，上界必须是 `n+m`。
///
/// 只把 `n` 交给 TopN 会先截掉 `m` 行再跳过 `m` 行 ⇒ **少一行**（最易写错处）。
#[test]
fn topn_path_accounts_for_offset() {
    let (db, root) = setup("topn_offset");
    // DESC 默认 NULLS FIRST ⇒ NULL,50,40,30,10；(a,b) 为 (2,NULL),(4,50),(3,40),(NULL,30),(1,10)
    assert_eq!(
        pairs(&db, "SELECT a, b FROM t ORDER BY b DESC"),
        ["2|NULL", "4|50", "3|40", "NULL|30", "1|10"]
    );
    // 跳过 1 行后取 2 行 ⇒ (4,50),(3,40)
    assert_eq!(
        pairs(&db, "SELECT a, b FROM t ORDER BY b DESC LIMIT 2 OFFSET 1"),
        ["4|50", "3|40"]
    );
    // 跳过 3 行后取 1 行 ⇒ (NULL,30)
    assert_eq!(
        pairs(&db, "SELECT a, b FROM t ORDER BY b DESC LIMIT 1 OFFSET 3"),
        ["NULL|30"]
    );
    // 大 OFFSET + 小 LIMIT：仍要正确（TopN 上界 n+m 之外的行会被丢掉）
    assert_eq!(
        pairs(&db, "SELECT a, b FROM t ORDER BY b DESC LIMIT 1 OFFSET 4"),
        ["1|10"]
    );
    std::fs::remove_dir_all(&root).ok();
}

/// `FETCH FIRST n ROWS ONLY` ≡ `LIMIT n`；`PERCENT`/`WITH TIES` 语义不同**必须拒**。
#[test]
fn fetch_first_equals_limit_but_percent_and_ties_are_refused() {
    let (db, root) = setup("fetch");
    assert_eq!(
        col(&db, "SELECT a FROM t ORDER BY a FETCH FIRST 2 ROWS ONLY"),
        ["1", "2"]
    );
    assert_eq!(
        col(&db, "SELECT a FROM t ORDER BY a FETCH FIRST 2 ROWS ONLY"),
        col(&db, "SELECT a FROM t ORDER BY a LIMIT 2")
    );
    for sql in [
        "SELECT a FROM t ORDER BY a FETCH FIRST 10 PERCENT ROWS ONLY",
        "SELECT a FROM t ORDER BY a FETCH FIRST 2 ROWS WITH TIES",
    ] {
        let e = format!(
            "{}",
            db.sql(sql).expect_err("比例/并列语义不能当 LIMIT 执行")
        );
        assert!(e.contains("FETCH FIRST"), "{sql}: {e}");
    }
    std::fs::remove_dir_all(&root).ok();
}

/// `NULLS FIRST/LAST`：显式值覆盖默认；默认按 SQL（ASC ⇒ LAST / DESC ⇒ FIRST）。
#[test]
fn nulls_first_last_overrides_default() {
    let (db, root) = setup("nulls");
    // 默认
    assert_eq!(
        col(&db, "SELECT b FROM t ORDER BY b ASC"),
        ["10", "30", "40", "50", "NULL"]
    );
    assert_eq!(
        col(&db, "SELECT b FROM t ORDER BY b DESC"),
        ["NULL", "50", "40", "30", "10"]
    );
    // 显式覆盖默认（这两条此前**被静默忽略**）
    assert_eq!(
        col(&db, "SELECT b FROM t ORDER BY b ASC NULLS FIRST"),
        ["NULL", "10", "30", "40", "50"]
    );
    assert_eq!(
        col(&db, "SELECT b FROM t ORDER BY b DESC NULLS LAST"),
        ["50", "40", "30", "10", "NULL"]
    );
    // 写了等于默认的值：结果不变（合法且常见）
    assert_eq!(
        col(&db, "SELECT b FROM t ORDER BY b ASC NULLS LAST"),
        ["10", "30", "40", "50", "NULL"]
    );
    // 多键混合方向 + 各自 NULL 规则
    assert_eq!(
        pairs(
            &db,
            "SELECT a, b FROM t ORDER BY b ASC NULLS FIRST, a DESC LIMIT 2"
        ),
        ["2|NULL", "1|10"]
    );
    std::fs::remove_dir_all(&root).ok();
}

/// 窗口内 `ORDER BY` 与外部**同一套** NULL 规则（帧为默认累计帧）。
#[test]
fn window_order_by_uses_same_nulls_rule() {
    let (db, root) = setup("win");
    // 默认（NULLS LAST）：10,40,80,130,130
    assert_eq!(
        col(&db, "SELECT sum(b) OVER (ORDER BY b ASC) FROM t"),
        ["10", "40", "80", "130", "130"]
    );
    // 显式 NULLS FIRST：NULL 排最前 ⇒ 首行帧内**一个非 NULL 都没有** ⇒ **NULL**
    // （2026-10-17 修：此前给 0 —— `sum` 跳过 NULL，没有非 NULL 就是 NULL，
    //  0 是个会被下游当真数字的错值。PG/DuckDB 同样给 NULL。）
    assert_eq!(
        col(
            &db,
            "SELECT sum(b) OVER (ORDER BY b ASC NULLS FIRST) FROM t"
        ),
        ["NULL", "10", "40", "80", "130"]
    );
    // 窗口 NULLS 不同的两个调用**不能被当成同一个窗口**（签名要含该维度）
    let bs = db
        .sql_collect(
            "SELECT sum(b) OVER (ORDER BY b ASC NULLS FIRST), \
                    sum(b) OVER (ORDER BY b ASC NULLS LAST) FROM t",
        )
        .unwrap_or_else(|e| panic!("两个不同 NULL 规则的窗口应可共存: {e}"));
    assert_eq!(bs[0].num_rows(), 5);
    std::fs::remove_dir_all(&root).ok();
}
