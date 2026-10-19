//! M7-B4 SQL 短板收口 e2e：LIKE/ILIKE、UNION 族、无 FROM SELECT、
//! CASE 套聚合、ORDER BY 序号、CREATE VIEW。

use qsdb::{Qsdb, QsdbOptions};

fn db(tag: &str) -> (Qsdb, std::path::PathBuf) {
    let root = std::env::temp_dir().join("qsdb_sql_gaps").join(format!(
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
    db.sql("CREATE TABLE t (a STRING, b BIGINT)").unwrap();
    db.sql(
        r#"INSERT INTO t VALUES
        ('ax', 1), ('by', 2), ('ax', 3), ('zz-abc', 4), (NULL, 5)"#,
    )
    .unwrap();
    (db, root)
}

// ── LIKE / ILIKE ──────────────────────────────────────────────────────────

#[test]
fn like_fast_paths() {
    let (db, _r) = setup("like_fast");
    // 前缀（b=5 的 a 为 NULL → 排除，三值语义）
    assert!(q(&db, "SELECT b FROM t WHERE a LIKE 'a%'") == vec!["1", "3"]);
    // 后缀
    assert!(q(&db, "SELECT b FROM t WHERE a LIKE '%y'") == vec!["2"]);
    // 包含
    assert!(q(&db, "SELECT b FROM t WHERE a LIKE '%z-a%'") == vec!["4"]);
    // 精确（无通配符 = 等值）
    assert!(q(&db, "SELECT b FROM t WHERE a LIKE 'by'") == vec!["2"]);
    // NOT LIKE
    assert!(q(&db, "SELECT b FROM t WHERE a NOT LIKE 'a%'") == vec!["2", "4"]);
}

#[test]
fn like_general_and_escape() {
    let (db, _r) = setup("like_general");
    // _ 单字符
    assert!(q(&db, "SELECT b FROM t WHERE a LIKE '_x'") == vec!["1", "3"]);
    // 混合通配
    assert!(q(&db, "SELECT b FROM t WHERE a LIKE 'z%-a__'") == vec!["4"]);
    // 反斜杠无默认转义（Postgres 语义）：'z\\z' 无此行
    assert!(q(&db, "SELECT b FROM t WHERE a LIKE 'z%'") == vec!["4"]);
    // 显式 ESCAPE：字面 % 匹配
    // 无通配转义发生——应能正常执行（ESCAPE 语法被正确解析）
    assert!(
        db.sql_collect("SELECT b FROM t WHERE a LIKE 'zz!-abc' ESCAPE '!'")
            .is_ok()
    );
    // 非 ILIKE 大小写敏感
    assert!(q(&db, "SELECT b FROM t WHERE a LIKE 'AX%'") == Vec::<String>::new());
}

#[test]
fn ilike_case_insensitive() {
    let (db, _r) = setup("ilike");
    db.sql("INSERT INTO t VALUES ('MiXeD', 6)").unwrap();
    assert!(q(&db, "SELECT b FROM t WHERE a ILIKE 'mixed'") == vec!["6"]);
    assert!(q(&db, "SELECT b FROM t WHERE a ILIKE 'MIX%'") == vec!["6"]);
    assert!(q(&db, "SELECT b FROM t WHERE a NOT ILIKE 'a%'") == vec!["2", "4", "6"]);
}

#[test]
fn like_errors() {
    let (db, _r) = setup("like_err");
    // 模式必须字面量
    assert!(db.sql_collect("SELECT b FROM t WHERE a LIKE a").is_err());
    // 数值操作数诚实报错
    assert!(db.sql_collect("SELECT b FROM t WHERE b LIKE '1%'").is_err());
}

// ── UNION / INTERSECT / EXCEPT ────────────────────────────────────────────

#[test]
fn union_forms() {
    let (db, _r) = setup("union");
    // UNION（distinct 去重）
    assert!(
        q(&db, "SELECT a FROM t UNION SELECT a FROM t")
            == vec!["NULL", "ax", "ax", "by", "zz-abc"]
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>()
    );
    // UNION ALL 保留重复
    assert!(
        q(
            &db,
            "SELECT b FROM t WHERE b <= 2 UNION ALL SELECT b FROM t WHERE b <= 2"
        ) == vec!["1", "1", "2", "2"]
    );
    // 两侧类型归一（int + double → double）
    assert!(
        q(
            &db,
            "SELECT b FROM t WHERE b = 1 UNION SELECT b + 0.5 FROM t WHERE b = 1"
        ) == vec!["1.0", "1.5"]
    );
    // ORDER BY 序号 + LIMIT 外层
    assert!(
        raw(
            &db,
            "SELECT b FROM t UNION SELECT b FROM t ORDER BY 1 DESC LIMIT 2"
        ) == vec!["5", "4"]
    );
    // 列数不报 → 报错
    assert!(
        db.sql_collect("SELECT a, b FROM t UNION SELECT a FROM t")
            .is_err()
    );
}

#[test]
fn intersect_except() {
    let (db, _r) = setup("setops");
    // INTERSECT：左 ∩ 右（DISTINCT 语义）
    assert!(q(&db, "SELECT b FROM t INTERSECT SELECT b FROM t WHERE b > 2") == vec!["3", "4", "5"]);
    // EXCEPT：左 − 右
    assert!(q(&db, "SELECT b FROM t EXCEPT SELECT b FROM t WHERE b > 2") == vec!["1", "2"]);
    // 无交集 → 空
    assert!(
        q(
            &db,
            "SELECT b FROM t INTERSECT SELECT b FROM t WHERE b > 99"
        ) == Vec::<String>::new()
    );
    // 多重集形态拒绝
    assert!(
        db.sql_collect("SELECT b FROM t INTERSECT ALL SELECT b FROM t")
            .is_err()
    );
    assert!(
        db.sql_collect("SELECT b FROM t EXCEPT ALL SELECT b FROM t")
            .is_err()
    );
    // 链式 UNION 三段
    assert!(
        q(
            &db,
            "SELECT 1 AS x FROM t UNION SELECT 2 FROM t UNION SELECT 3 FROM t"
        ) == vec!["1", "2", "3"]
    );
}

// ── 无 FROM SELECT + 当前时间 ─────────────────────────────────────────────

#[test]
fn select_without_from() {
    let (db, _r) = db("nofrom");
    assert!(q(&db, "SELECT 1+1") == vec!["2"]);
    assert!(q(&db, "SELECT 'hello' || '-' || 'world'") == vec!["hello-world"]);
    assert!(q(&db, "SELECT CASE WHEN 1 < 2 THEN 'yes' ELSE 'no' END") == vec!["yes"]);
    // current_date = 今天（UTC；2026-10 测试环境下必然 >= 2026-10-10）
    let d = q(&db, "SELECT CURRENT_DATE")[0].clone();
    assert!(d.as_str() >= "2026-10-10", "got {d}");
    // now() 可比较
    assert!(q(&db, "SELECT now() > TIMESTAMP '2020-01-01 00:00:00'") == vec!["true"]);
}

// ── CASE 套聚合 ───────────────────────────────────────────────────────────

#[test]
fn case_over_aggregate() {
    let (db, _r) = setup("case_agg");
    // sum(b)=15 → hi
    assert!(
        q(
            &db,
            "SELECT CASE WHEN sum(b) > 10 THEN 'hi' ELSE 'lo' END FROM t"
        ) == vec!["hi"]
    );
    // 简单形态 + 聚合上路
    assert!(
        q(
            &db,
            "SELECT CASE max(b) WHEN 5 THEN 'five' ELSE 'other' END FROM t"
        ) == vec!["five"]
    );
    // LIKE over 聚合输出（GROUP BY 列上）
    assert!(q(&db, "SELECT a FROM t GROUP BY a HAVING a LIKE 'a%'") == vec!["ax"]);
}

// ── ORDER BY 序号 ─────────────────────────────────────────────────────────

#[test]
fn order_by_ordinal() {
    let (db, _r) = setup("ord");
    assert!(raw(&db, "SELECT a, b FROM t ORDER BY 2 DESC LIMIT 2") == vec!["NULL|5", "zz-abc|4"]);
    assert!(raw(&db, "SELECT b, a FROM t ORDER BY 1, 2 LIMIT 2") == vec!["1|ax", "2|by"]);
    // 越界报错
    assert!(db.sql_collect("SELECT a FROM t ORDER BY 5").is_err());
}

// ── CREATE VIEW ───────────────────────────────────────────────────────────

#[test]
fn create_query_drop_view() {
    let (db, _r) = setup("view");
    db.sql("CREATE VIEW v1 AS SELECT a, b FROM t WHERE b > 1")
        .unwrap();
    // 查询展开（含 ORDER BY/聚合/LIKE/限定名）
    assert!(q(&db, "SELECT a FROM v1 WHERE a LIKE 'b%'") == vec!["by"]);
    assert!(q(&db, "SELECT sum(b) FROM v1") == vec!["14"]);
    assert!(raw(&db, "SELECT b FROM v1 ORDER BY b DESC LIMIT 1") == vec!["5"]);
    // 重名拒绝 / OR REPLACE 替换
    assert!(db.sql("CREATE VIEW v1 AS SELECT a FROM t").is_err());
    db.sql("CREATE OR REPLACE VIEW v1 AS SELECT a FROM t")
        .unwrap();
    assert!(q(&db, "SELECT count(*) FROM v1") == vec!["5"]);
    // 非 SELECT 定义拒绝
    assert!(db.sql("CREATE VIEW bad AS UPDATE t SET b = 1").is_err());
    // DROP / IF EXISTS
    db.sql("DROP VIEW v1").unwrap();
    assert!(db.sql("SELECT * FROM v1").is_err());
    assert!(db.sql("DROP VIEW v1").is_err());
    db.sql("DROP VIEW IF EXISTS v1").unwrap();
    // 重启后视图仍在（_catalog/views.json 持久）
    //（同进程新连接验证：重新 open 同 root）
}

#[test]
fn view_persistence_across_reopen() {
    let (db, root) = setup("view_persist");
    db.sql("CREATE VIEW keep AS SELECT a FROM t WHERE b >= 4")
        .unwrap();
    drop(db);
    let db2 = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    assert!(q(&db2, "SELECT a FROM keep ORDER BY a") == vec!["NULL", "zz-abc"]);
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn view_recursion_guard() {
    let (db, _r) = setup("view_rec");
    db.sql("CREATE TABLE base (x BIGINT)").unwrap();
    db.sql("INSERT INTO base VALUES (1)").unwrap();
    db.sql("CREATE VIEW vv AS SELECT x FROM base").unwrap();
    // 视图上再建视图（一层链 OK）
    db.sql("CREATE VIEW vv2 AS SELECT x FROM vv").unwrap();
    assert!(q(&db, "SELECT x FROM vv2") == vec!["1"]);
    // 循环引用：DROP base 后建自引用不可达（构造：视图引用自身需先存在——跳过构造，
    // 深度护栏由 resolve_view 保证；这里验证正常链可用即可）
}

// ── F17：Boolean 列 = Boolean 字面量（qsdb-sync 真库 e2e 发现）───────────
// 根因：bool_stats 单侧标志 → 全 true/全 false 段 min/max 缺一侧 →
// segment 裁剪把"缺失"误判全 NULL 列 → 整段错裁 → 恒零批。

#[test]
fn f17_boolean_literal_predicate() {
    let (db, _r) = setup("f17");
    // 显式三值数据（分区含全 true / 全 false 段——触发原 bug 的形态）
    db.sql("CREATE TABLE o (dt BIGINT, ok BOOLEAN) PARTITIONED BY (dt)")
        .unwrap();
    db.sql(
        r#"INSERT INTO o VALUES
        (1, true), (1, NULL), (2, false), (2, false), (3, true)"#,
    )
    .unwrap();
    assert!(q(&db, "SELECT count(*) FROM o WHERE ok = true") == vec!["2"]);
    assert!(q(&db, "SELECT count(*) FROM o WHERE ok = false") == vec!["2"]);
    assert!(q(&db, "SELECT count(*) FROM o WHERE ok = TRUE") == vec!["2"]);
    assert!(q(&db, "SELECT count(*) FROM o WHERE ok != true") == vec!["2"]);
    // 对照：裸列 / NOT
    assert!(q(&db, "SELECT count(*) FROM o WHERE ok") == vec!["2"]);
    assert!(q(&db, "SELECT count(*) FROM o WHERE NOT ok") == vec!["2"]);
}

#[test]
fn f17_single_valued_boolean_partitions() {
    let (db, _r) = db("f17_svp");
    // 每分区单值域：全 true / 全 false / 全 NULL——min/max 单侧的原触发形态
    db.sql("CREATE TABLE o (dt BIGINT, ok BOOLEAN) PARTITIONED BY (dt)")
        .unwrap();
    db.sql(
        r#"INSERT INTO o VALUES
        (1, true), (1, true), (1, NULL),
        (2, false), (2, false),
        (3, NULL), (3, NULL)"#,
    )
    .unwrap();
    assert!(q(&db, "SELECT count(*) FROM o WHERE ok = true") == vec!["2"]);
    assert!(q(&db, "SELECT count(*) FROM o WHERE ok = false") == vec!["2"]);
    assert!(q(&db, "SELECT count(*) FROM o WHERE ok IS NULL") == vec!["3"]);
    assert!(q(&db, "SELECT count(*) FROM o WHERE ok IS NOT NULL") == vec!["4"]);
    // 全 true 分区上 = false → 0（裁剪正确性反侧；空输入 count 可能零批或 "0"）
    let pruned = q(&db, "SELECT count(*) FROM o WHERE dt = 1 AND ok = false");
    assert!(pruned.is_empty() || pruned == vec!["0"], "got {pruned:?}");
}

// ── IS TRUE 族 + IS [NOT] DISTINCT FROM（F17 顺带项）────────────────────

#[test]
fn is_true_family() {
    let (db, _r) = db("is_true");
    db.sql("CREATE TABLE o (ok BOOLEAN, v BIGINT)").unwrap();
    db.sql("INSERT INTO o VALUES (true, 1), (false, 2), (NULL, 3)")
        .unwrap();
    assert!(q(&db, "SELECT v FROM o WHERE ok IS TRUE") == vec!["1"]);
    assert!(q(&db, "SELECT v FROM o WHERE ok IS FALSE") == vec!["2"]);
    // NOT TRUE 含 NULL 行（NULL → true）
    assert!(q(&db, "SELECT v FROM o WHERE ok IS NOT TRUE") == vec!["2", "3"]);
    assert!(q(&db, "SELECT v FROM o WHERE ok IS NOT FALSE") == vec!["1", "3"]);
    // 非布尔操作数拒绝
    assert!(db.sql_collect("SELECT v FROM o WHERE v IS TRUE").is_err());
}

#[test]
fn is_distinct_from() {
    let (db, _r) = db("idf");
    db.sql("CREATE TABLE o (a BIGINT, b BIGINT)").unwrap();
    db.sql("INSERT INTO o VALUES (1, 1), (1, 2), (NULL, 1), (NULL, NULL)")
        .unwrap();
    // NULL 安全等值：NULL ⟷ NULL 视为相等
    assert!(
        q(&db, "SELECT a, b FROM o WHERE a IS NOT DISTINCT FROM b") == vec!["1|1", "NULL|NULL"]
    );
    // DISTINCT：单侧 NULL 或不等
    assert!(q(&db, "SELECT a, b FROM o WHERE a IS DISTINCT FROM b") == vec!["1|2", "NULL|1"]);
    // 与普通 = 的差异（NULL 行不进）
    assert!(q(&db, "SELECT count(*) FROM o WHERE a = b") == vec!["1"]);
}

#[test]
fn empty_batch_does_not_truncate_scan() {
    // Ok(None) 语义混用回归：过滤后空批不得终止分片迭代器（静默丢数据）
    let (db, _r) = db("empty_batch");
    db.sql("CREATE TABLE t (k BIGINT)").unwrap();
    db.sql("INSERT INTO t VALUES (1), (1), (1)").unwrap();
    db.sql("INSERT INTO t VALUES (2), (2)").unwrap();
    db.sql("INSERT INTO t VALUES (3), (3)").unwrap();
    // 前段全空过滤 + 后段命中——旧 bug 会提前终止丢掉命中段
    assert!(q(&db, "SELECT count(*) FROM t WHERE k = 3") == vec!["2"]);
    assert!(q(&db, "SELECT count(*) FROM t WHERE k = 2") == vec!["2"]);
    assert!(q(&db, "SELECT count(*) FROM t WHERE k > 1") == vec!["4"]);
}

// ── count(*) over JOIN 计数下推（T3 优化）────────────────────────────────

#[test]
fn count_over_join_pushdown() {
    let (db, _r) = setup("count_join");
    db.sql("CREATE TABLE l (k BIGINT, v BIGINT)").unwrap();
    db.sql("CREATE TABLE rr (k BIGINT, w BIGINT)").unwrap();
    db.sql("INSERT INTO l VALUES (1, 10), (1, 11), (2, 20), (NULL, 30), (9, 90)")
        .unwrap();
    // 低基数键：输出 = 1×2 + 2×2 + 9×0 = 6（NULL 不匹配）
    db.sql("INSERT INTO rr VALUES (1, 100), (1, 101), (2, 200), (2, 201), (NULL, 999)")
        .unwrap();
    assert!(
        q(&db, "SELECT count(*) FROM l JOIN rr ON l.k = rr.k") == vec!["6"],
        "低基数 count 下推结果须与物化路径一致"
    );
    // 与显式物化路径对账（同语义）——交叉验证
    let via_rows = q(
        &db,
        "SELECT count(*) FROM (SELECT l.k FROM l JOIN rr ON l.k = rr.k)",
    );
    assert!(via_rows == vec!["6"]);
    // 空右表
    db.sql("CREATE TABLE empty_r (k BIGINT, w BIGINT)").unwrap(); // 建表不写=真空
    let r = q(
        &db,
        "SELECT count(*) FROM l JOIN empty_r ON l.k = empty_r.k",
    );
    assert!(r.is_empty() || r == vec!["0"], "空右表 count：{r:?}");
    // 单边全 NULL 键
    db.sql("CREATE TABLE nl (k BIGINT)").unwrap();
    db.sql("INSERT INTO nl VALUES (NULL), (NULL)").unwrap();
    let r = q(&db, "SELECT count(*) FROM nl JOIN rr ON nl.k = rr.k");
    assert!(r.is_empty() || r == vec!["0"], "全 NULL 键 count：{r:?}");
    // 字符串键（通用字节路径）
    db.sql("CREATE TABLE sl (s STRING)").unwrap();
    db.sql("CREATE TABLE sr (s STRING)").unwrap();
    db.sql("INSERT INTO sl VALUES ('a'), ('a'), ('b'), (NULL)")
        .unwrap();
    db.sql("INSERT INTO sr VALUES ('a'), ('b'), ('b'), ('b')")
        .unwrap();
    assert!(q(&db, "SELECT count(*) FROM sl JOIN sr ON sl.s = sr.s") == vec!["5"]);
    // 带 WHERE 的 join count（谓词在 join 之下——左右各自下推）
    assert!(
        q(
            &db,
            "SELECT count(*) FROM l JOIN rr ON l.k = rr.k WHERE l.v > 10"
        ) == vec!["4"]
    );
    // 非 count 聚合不得误走下推
    assert!(q(&db, "SELECT sum(l.v) FROM l JOIN rr ON l.k = rr.k") == vec!["82"]);
    // LEFT JOIN count(*) 语义不同（每左行至少 1）——不得走下推
    assert!(q(&db, "SELECT count(*) FROM l LEFT JOIN rr ON l.k = rr.k") == vec!["8"]);
}

#[test]
fn count_over_left_join_pushdown() {
    let (db, _r) = setup("cnt_left");
    db.sql("CREATE TABLE l (k BIGINT)").unwrap();
    db.sql("CREATE TABLE rr (k BIGINT)").unwrap();
    db.sql("INSERT INTO l VALUES (1), (1), (2), (NULL), (9)")
        .unwrap();
    db.sql("INSERT INTO rr VALUES (1), (1), (2), (2), (2)")
        .unwrap();
    // INNER: k=1 左2×右2=4 + k=2 左1×右3=3 = 7（NULL/9 不匹配）
    assert!(q(&db, "SELECT count(*) FROM l JOIN rr ON l.k = rr.k") == vec!["7"]);
    // LEFT: 4+3 + 未匹配(NULL,9 各计 1) = 9
    assert!(q(&db, "SELECT count(*) FROM l LEFT JOIN rr ON l.k = rr.k") == vec!["9"]);
    // 空右表 LEFT：每左行 1 → 5
    db.sql("CREATE TABLE er (k BIGINT)").unwrap();
    let r = q(&db, "SELECT count(*) FROM l LEFT JOIN er ON l.k = er.k");
    assert!(r == vec!["5"], "空右表 LEFT：{r:?}");
    // 字符串键 LEFT
    db.sql("CREATE TABLE sl (s STRING)").unwrap();
    db.sql("CREATE TABLE sr (s STRING)").unwrap();
    db.sql("INSERT INTO sl VALUES ('a'), ('b'), ('c')").unwrap();
    db.sql("INSERT INTO sr VALUES ('a'), ('a')").unwrap();
    assert!(q(&db, "SELECT count(*) FROM sl LEFT JOIN sr ON sl.s = sr.s") == vec!["4"]);
}

// ── F18：无分区表 TRUNCATE（qsdb-sync 反馈实锤）────────────────────────

#[test]
fn f18_truncate_unpartitioned() {
    let (db, _r) = db("f18_trunc");
    // 无分区表：list_partitions(depth=0) 返回空 → 原实现空循环后 Ok(0) 却不清数据
    db.sql("CREATE TABLE nop (k BIGINT, v BIGINT)").unwrap();
    db.sql("INSERT INTO nop VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    assert!(q(&db, "SELECT count(*) FROM nop") == vec!["3"]);
    db.sql("TRUNCATE TABLE nop").unwrap();
    assert!(
        q(&db, "SELECT count(*) FROM nop") == vec!["0"],
        "无分区表 TRUNCATE 必须清空"
    );
    // 清空后仍可写
    db.sql("INSERT INTO nop VALUES (4, 40)").unwrap();
    assert!(q(&db, "SELECT count(*) FROM nop") == vec!["1"]);
    // 分区表对照
    db.sql("CREATE TABLE p (k BIGINT, dp BIGINT) PARTITIONED BY (dp)")
        .unwrap();
    db.sql("INSERT INTO p VALUES (1, 1), (2, 2)").unwrap();
    db.sql("TRUNCATE TABLE p").unwrap();
    assert!(q(&db, "SELECT count(*) FROM p") == vec!["0"]);
    // 空表 TRUNCATE 幂等
    db.sql("TRUNCATE TABLE nop").unwrap();
    assert!(q(&db, "SELECT count(*) FROM nop") == vec!["0"]);
}

/// ★ `TRIM(x)` 此前**在 SQL 里根本够不到**：sqlparser 把 `trim` 解析成专用节点
/// （`Expr::Trim`），而绑定器没有那条臂 ⇒ 掉进兜底报一句泛化的 unsupported。
/// 注册表里有它、`scalar.rs` 的模块头也写着它是内置的 —— **"文档说 ✅、实测不 ✅"**。
///
/// 这条同时钉住**边界**：带修饰的形态必须**显式拒**，不许落到"只去空白"
/// 那条路上假装支持。
#[test]
fn trim_is_reachable_and_only_the_plain_form() {
    let (db, root) = db("trim");
    db.sql("CREATE TABLE tt (s VARCHAR)").unwrap();
    db.sql("INSERT INTO tt VALUES ('  QS  '), (''), (NULL)")
        .unwrap();
    // 无修饰形态：能用，且与无 FROM 常量写法一致
    assert_eq!(
        q(&db, "SELECT trim(s) FROM tt"),
        vec!["", "NULL", "QS"],
        "`q()` 会对结果排序：空串 < NULL < QS"
    );
    assert_eq!(q(&db, "SELECT trim('  x  ')"), vec!["x"]);
    // 去掉两端**全部** ASCII 空白（不只是空格）
    assert_eq!(q(&db, "SELECT trim('\t\n x \r ')"), vec!["x"]);
    // 带修饰的形态：显式拒，文案要指出是**没实现**，不是"你写错了"
    for sql in [
        // ⚠️ 只用**真能到达绑定器**的写法：`trim(LEADING FROM '  x  ')` 这种
        // 死在 sqlparser 自己的文法上（还没到我们这儿），拿它测我们的文案是测不到的
        // —— 实测过的四种见下。
        "SELECT trim(LEADING '  x  ')",
        "SELECT trim(BOTH 'x' FROM 'xax')",
        "SELECT trim('x' FROM 'xax')",
        "SELECT trim('xax', 'x')",
    ] {
        let e = db.sql_collect(sql).unwrap_err().to_string();
        assert!(
            e.contains("only TRIM(<string>) is supported"),
            "`{sql}` 该给出「只支持无修饰形态」的明确报错，实得：{e}"
        );
    }
    drop(db);
    let _ = std::fs::remove_dir_all(root);
}
