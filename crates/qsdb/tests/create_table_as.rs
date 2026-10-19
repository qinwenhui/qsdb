//! **生态功能批次 4：`CREATE TABLE … AS SELECT`（CTAS）** 的逐值验收。
//!
//! 此前它报的是 `table needs at least one column` —— 把"没实现"说成"你建表建错了"
//! （文档一度声称"报错文案已明确"，实测根本没落地，与 F29② 同款）。
//!
//! 设计上只有两处与"手工两步"（`CREATE TABLE` + `INSERT INTO … SELECT …`）不同：
//! ① 列名/类型由 SELECT 的**输出**推导（没写列清单时）；
//! ② 失败时把刚建的表**删掉**（PG 的 CTAS 全有全无）。
//! 本文件把这两条**各自**钉住 —— 尤其是 ② 的**边界**：**已存在的表绝不能被回滚删掉**。

use qsdb::{Qsdb, QsdbOptions};

/// 测试目录唯一序号：pid 挡跨进程碰撞，序号挡同进程内并发（Windows 时钟
/// 粒度 ~100ns，纳秒后缀在同 tick 内会重复——CI 上真炸过，见 constraints）。
fn _uniq_seq() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    SEQ.fetch_add(1, Ordering::Relaxed)
}

fn setup(tag: &str) -> (Qsdb, std::path::PathBuf) {
    let root = std::env::temp_dir().join("qsdb_ctas").join(format!(
        "{tag}_{}_{}",
        std::process::id(),
        format!("{:x}_{}", std::process::id(), _uniq_seq())
    ));
    std::fs::create_dir_all(&root).unwrap();
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    db.sql("CREATE TABLE t (g BIGINT, v BIGINT, s VARCHAR)")
        .unwrap();
    db.sql("INSERT INTO t VALUES (1,10,'a'),(1,30,'b'),(2,20,'c'),(3,NULL,NULL)")
        .unwrap();
    (db, root)
}

/// 查全部列 → "c0|c1|…"，排序后返回。
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

/// 表的列名与类型（证明 CTAS 真的把 schema 派生对了，而不只是"数据塞进去了"）。
fn schema_of(db: &Qsdb, table: &str) -> Vec<String> {
    let batches = db
        .sql_collect(&format!("SELECT * FROM {table} LIMIT 1"))
        .unwrap_or_else(|e| panic!("{table}: {e}"));
    batches
        .first()
        .map(|b| {
            b.schema()
                .fields()
                .iter()
                .map(|f| format!("{}:{}", f.name(), f.data_type()))
                .collect()
        })
        .unwrap_or_default()
}

fn err_of(db: &Qsdb, sql: &str) -> String {
    match db.sql(sql) {
        Ok(_) => panic!("这条必须被拒: {sql}"),
        Err(e) => e.to_string(),
    }
}

fn table_exists(db: &Qsdb, name: &str) -> bool {
    db.sql(&format!("SELECT count(*) FROM {name}")).is_ok()
}

// ── ① 基本：列名与类型都由 SELECT 的输出推导 ─────────────────────────────
#[test]
fn 派生的列名与类型() {
    let (db, root) = setup("basic");
    db.sql("CREATE TABLE t2 AS SELECT g, v FROM t").unwrap();
    assert_eq!(schema_of(&db, "t2"), ["g:Int64", "v:Int64"]);
    assert_eq!(
        q(&db, "SELECT g, v FROM t2 ORDER BY g, v"),
        vec!["1|10", "1|30", "2|20", "3|NULL"]
    );
    // `SELECT *` 源
    db.sql("CREATE TABLE t3 AS SELECT * FROM t").unwrap();
    assert_eq!(schema_of(&db, "t3"), ["g:Int64", "v:Int64", "s:Utf8"]);
    assert_eq!(q(&db, "SELECT count(*) FROM t3"), ["4"]);
    // 空结果也是**建了表**（0 行），不是没建
    db.sql("CREATE TABLE empty AS SELECT g, v FROM t WHERE g = 999")
        .unwrap();
    assert_eq!(q(&db, "SELECT count(*) FROM empty"), ["0"]);
    assert_eq!(schema_of(&db, "empty"), ["g:Int64", "v:Int64"]);
    let _ = std::fs::remove_dir_all(&root);
}

// ── ② 类型保真：日期/浮点/字符串/大整数各按各的 ──────────────────────────
#[test]
fn 类型保真() {
    let (db, root) = setup("types");
    db.sql("CREATE TABLE ty (a DATE, b FLOAT, c VARCHAR, d BIGINT)")
        .unwrap();
    db.sql("INSERT INTO ty VALUES (DATE '2026-01-01', 1.5, 'x', 9)")
        .unwrap();
    db.sql("CREATE TABLE ty2 AS SELECT a, b, c, d FROM ty")
        .unwrap();
    assert_eq!(
        schema_of(&db, "ty2"),
        ["a:Date32", "b:Float32", "c:Utf8", "d:Int64"]
    );
    assert_eq!(
        q(&db, "SELECT a, b, c, d FROM ty2"),
        vec!["2026-01-01|1.5|x|9"]
    );
    let _ = std::fs::remove_dir_all(&root);
}

// ── ③ 表达式列名：**显式指路**而不是静默改名 ─────────────────────────────
//
// `count(*)` 是 SELECT 的输出名，但不是合法表列名（表列名限 [A-Za-z_][A-Za-z0-9_]*）。
// PG 会把它改成 `count` —— 那是**另一种静默**（表里出现一个用户从没写过的列名）。
// 本仓选择**拒 + 指路**，且两条出路都实测可用。
#[test]
fn 表达式列名指路() {
    let (db, root) = setup("exprname");
    let e = err_of(
        &db,
        "CREATE TABLE agg AS SELECT g, count(*) FROM t GROUP BY g",
    );
    assert!(e.contains("不是合法表列名"), "{e}");
    assert!(e.contains("AS col1"), "要点明出路: {e}");
    assert!(!table_exists(&db, "agg"), "被拒的 CTAS 不该留下表");
    // 出路 A：起别名
    db.sql("CREATE TABLE agg_a AS SELECT g, count(*) AS n FROM t GROUP BY g")
        .unwrap();
    assert_eq!(
        q(&db, "SELECT g, n FROM agg_a ORDER BY g"),
        ["1|2", "2|1", "3|1"]
    );
    // 出路 B：显式列清单（名字与类型都用用户写的）
    db.sql("CREATE TABLE agg_b (g BIGINT, n BIGINT) AS SELECT g, count(*) FROM t GROUP BY g")
        .unwrap();
    assert_eq!(
        q(&db, "SELECT g, n FROM agg_b ORDER BY g"),
        ["1|2", "2|1", "3|1"]
    );
    let _ = std::fs::remove_dir_all(&root);
}

// ── ④ JOIN 的**限定名要去掉表限定**（`a.g` ⇒ `g`）────────────────────────
//
// 本仓 JOIN 场景下列名是限定名（右表列必须限定），而表列名不允许 `.`
// ⇒ 不剥的话最普通的 `CREATE TABLE j AS SELECT a.g, b.v FROM … JOIN …` 必被拒。
#[test]
fn 限定名去限定() {
    let (db, root) = setup("qual");
    db.sql("CREATE TABLE j AS SELECT a.g, b.v FROM t a JOIN t b ON a.g = b.g")
        .unwrap();
    assert_eq!(schema_of(&db, "j"), ["g:Int64", "v:Int64"]);
    // 3 行互相匹配：t 里 g∈{1,1,2} 各配到同 g 的行 ⇒ 6 对
    assert_eq!(q(&db, "SELECT count(*) FROM j"), ["6"]);
    // 剥完重名 ⇒ 报清楚（不是默默建出一张两列同名的表）
    let e = err_of(
        &db,
        "CREATE TABLE dup AS SELECT a.g, b.g FROM t a JOIN t b ON a.g = b.g",
    );
    assert!(e.contains("重复"), "{e}");
    assert!(!table_exists(&db, "dup"));
    let _ = std::fs::remove_dir_all(&root);
}

// ── ⑤ 分区 CTAS：分区键照样进目录名 ──────────────────────────────────────
#[test]
fn 分区表_ctas() {
    let (db, root) = setup("part");
    db.sql("CREATE TABLE pt PARTITIONED BY (g) AS SELECT g, v FROM t")
        .unwrap();
    assert_eq!(
        q(&db, "SELECT g, v FROM pt ORDER BY g, v"),
        q(&db, "SELECT g, v FROM t ORDER BY g, v")
    );
    // 真成了分区表：分区列在目录名里 ⇒ `SHOW PARTITIONS` 列出 1/2/3
    let parts = q(&db, "SHOW PARTITIONS pt");
    assert_eq!(parts.len(), 3, "应有 3 个分区，实得 {parts:?}");
    // 分区键必须是 SELECT 的一列
    let e = err_of(
        &db,
        "CREATE TABLE pbad PARTITIONED BY (zz) AS SELECT g, v FROM t",
    );
    assert!(e.contains("partition key"), "{e}");
    let _ = std::fs::remove_dir_all(&root);
}

// ── ⑥ 回滚：失败不留空表；**已存在的表绝不能被删** ───────────────────────
#[test]
fn 失败回滚且不误删() {
    let (db, root) = setup("rollback");
    // ① **建表之前**就失败（源表不存在）
    assert!(db.sql("CREATE TABLE rb AS SELECT g FROM nope").is_err());
    assert!(!table_exists(&db, "rb"), "失败的 CTAS 不该留下表");
    // ② 同样在建表之前（列数不符）
    assert!(
        db.sql("CREATE TABLE bad (g BIGINT) AS SELECT g, v FROM t")
            .is_err()
    );
    assert!(!table_exists(&db, "bad"));
    // ③ ★ **真的写进去一半才失败** —— 这才是回滚代码存在的理由。
    //    类型转换是在 InsertSink 落盘那一步做的（不是在绑定期），所以这里
    //    "表建好了、正在塞数据、塞到一半发现塞不进去"。只测 ①② 的话，
    //    回滚分支**一行都不会执行**（那正是"没测到的代码 = 坏的代码"）。
    let e = err_of(&db, "CREATE TABLE m2 (g DATE) AS SELECT s FROM t");
    assert!(e.contains("无法转换"), "{e}");
    assert!(
        !table_exists(&db, "m2"),
        "写了一半失败的 CTAS 必须回滚掉整张表"
    );
    // ⚠️ **边界**：表已存在时 create 先失败 ⇒ 绝不能走到回滚把旧表删了
    db.sql("CREATE TABLE keep (x BIGINT)").unwrap();
    db.sql("INSERT INTO keep VALUES (7)").unwrap();
    let e = err_of(&db, "CREATE TABLE keep AS SELECT g FROM t");
    assert!(e.contains("already exists"), "{e}");
    assert_eq!(q(&db, "SELECT x FROM keep"), ["7"], "旧表必须原封不动");
    let _ = std::fs::remove_dir_all(&root);
}

// ── ⑦ 源可以是 CTE / 派生表 / JOIN ──────────────────────────────────────
#[test]
fn 各种源() {
    let (db, root) = setup("sources");
    // ⚠️ `WITH w AS (…) CREATE TABLE …` 这种"CTE 在前"的写法 sqlparser 不认
    //（解析期就拒），得写成 `CREATE TABLE … AS WITH w AS (…) SELECT …`
    db.sql("CREATE TABLE cte_t AS WITH w AS (SELECT g, v FROM t WHERE g < 3) SELECT * FROM w")
        .unwrap();
    assert_eq!(q(&db, "SELECT count(*) FROM cte_t"), ["3"]);
    db.sql("CREATE TABLE sq AS SELECT * FROM (SELECT g, v FROM t) d")
        .unwrap();
    assert_eq!(q(&db, "SELECT count(*) FROM sq"), ["4"]);
    // 建完之后**源表改动不影响**新表（快照语义，不是视图）
    db.sql("INSERT INTO t VALUES (9,90,'z')").unwrap();
    assert_eq!(q(&db, "SELECT count(*) FROM sq"), ["4"]);
    assert_eq!(q(&db, "SELECT count(*) FROM t"), ["5"]);
    let _ = std::fs::remove_dir_all(&root);
}

// ── ⑧ 仍然拒的形态 ──────────────────────────────────────────────────────
#[test]
fn 拒绝面() {
    let (db, root) = setup("reject");
    // CREATE OR REPLACE（含 CTAS 形态）—— 得在 CTAS 分支**之前**判，
    // 否则最容易让人误以为"反正要重建，OR REPLACE 无所谓"
    let e = err_of(&db, "CREATE OR REPLACE TABLE r AS SELECT g FROM t");
    assert!(e.contains("CREATE OR REPLACE TABLE"), "{e}");
    assert!(!table_exists(&db, "r"));
    // 列清单列数与查询产出不符
    let e = err_of(
        &db,
        "CREATE TABLE bad (g BIGINT, h BIGINT) AS SELECT g FROM t",
    );
    assert!(e.contains("而查询产出"), "{e}");
    // 对照组：不带 OR REPLACE 的同一句照常
    db.sql("CREATE TABLE ok AS SELECT g FROM t").unwrap();
    assert_eq!(q(&db, "SELECT count(*) FROM ok"), ["4"]);
    let _ = std::fs::remove_dir_all(&root);
}
