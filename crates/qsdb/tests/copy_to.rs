//! **湖导出**（F9 / A3）的验收：`COPY (SELECT …) TO '<目录>' (FORMAT PARQUET, PARTITION_BY (…))`。
//!
//! 设计见 `docs/提交事件订阅与湖导出设计.md` §2.2。
//!
//! 这份清单里最要紧的一条是**闭环**：导出去的东西，**我们自己要能读回来、逐值一致**。
//! 只测"文件生成了"是不够的 —— 那样分区列丢了、类型漂了、行少了，全都看不出来。
//! 所以下面几乎每条都走 `read_parquet(...)` 回到库里再比。
//!
//! 另一类是**拒绝**：写得出、读不对的形态（NULL 分区值 / 不可往返的类型 /
//! 非空目录 / 把目录写成文件）必须**当场报错**，一条都不许静默。

use qsdb::users::Role;
use qsdb::{Qsdb, QsdbOptions};

fn setup(tag: &str) -> (Qsdb, std::path::PathBuf) {
    let root = std::env::temp_dir().join("qsdb_copy_to").join(format!(
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

/// 路径 → 正斜杠字符串（`read_parquet` / `COPY` 都吃这个形态）。
fn p(path: &std::path::Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

/// 查全部列 → 每行 `c0|c1|…`，**排序后**返回（行序不参与比较）。
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

/// 列的 `名字:类型`（证明**类型**也对，不只是值对）。
fn cols(db: &Qsdb, sql: &str) -> Vec<String> {
    let b = db.sql_collect(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    b.first()
        .map(|b| {
            b.schema()
                .fields()
                .iter()
                .map(|f| format!("{}:{}", f.name(), f.data_type()))
                .collect()
        })
        .unwrap_or_default()
}

fn seed(db: &Qsdb) {
    db.sql("CREATE TABLE orders (id BIGINT, amount BIGINT, region VARCHAR, dt DATE)")
        .unwrap();
    db.sql(
        "INSERT INTO orders VALUES \
         (1, 10, 'cn', CAST('2026-01-01' AS DATE)), \
         (2, 20, 'cn', CAST('2026-01-01' AS DATE)), \
         (3, 30, 'us', CAST('2026-01-01' AS DATE)), \
         (4, 40, 'cn', CAST('2026-01-02' AS DATE))",
    )
    .unwrap();
}

// ══ ① 不分区：导出 → 读回来逐值一致 ════════════════════════════════════════

#[test]
fn 不分区导出读回来逐值一致() {
    let (db, root) = setup("basic");
    seed(&db);
    let out = root.join("out");

    db.sql(&format!(
        "COPY (SELECT id, amount, region, dt FROM orders) TO '{}' (FORMAT PARQUET)",
        p(&out)
    ))
    .unwrap();

    // 布局：目标是**目录**，part 文件在里面
    assert!(out.join("part-00000.parquet").is_file(), "缺 part 文件");
    assert!(
        out.join("_common_metadata").is_file(),
        "缺 _common_metadata"
    );

    let pat = format!("{}/**/*.parquet", p(&out));
    let read = q(
        &db,
        &format!("SELECT id, amount, region, dt FROM read_parquet('{pat}')"),
    );
    let src = q(&db, "SELECT id, amount, region, dt FROM orders");
    assert_eq!(read, src, "导出再读回来必须逐值一致");
    // 类型也要一致（值对但类型漂了，驱动侧一样是错的）
    assert_eq!(
        cols(&db, &format!("SELECT * FROM read_parquet('{pat}')")),
        vec!["id:Int64", "amount:Int64", "region:Utf8", "dt:Date32"],
    );
    std::fs::remove_dir_all(&root).ok();
}

// ══ ② 分区：目录布局 + 分区列**不入数据文件** + 读回来重建 ═══════════════════

#[test]
fn 分区导出的目录布局与重建() {
    let (db, root) = setup("hive");
    seed(&db);
    let out = root.join("lake");

    db.sql(&format!(
        "COPY (SELECT id, amount, region, dt FROM orders) TO '{}' \
         (FORMAT PARQUET, PARTITION_BY (dt, region))",
        p(&out)
    ))
    .unwrap();

    // 布局：k=v 逐级
    for d in [
        "dt=2026-01-01/region=cn",
        "dt=2026-01-01/region=us",
        "dt=2026-01-02/region=cn",
    ] {
        assert!(out.join(d).is_dir(), "缺分区目录 {d}");
        assert!(
            out.join(d).join("part-00000.parquet").is_file(),
            "缺 {d}/part-00000.parquet"
        );
    }

    // ★ 分区列**不在数据文件里** —— 把它拷到一个没有 k=v 祖先的平目录再读，
    //   读出来就不该有 dt/region（拿原路径读是证明不了这一点的：hive 重建
    //   会把它们补回来，看着"有"其实来自目录名）。
    let flat = root.join("flat");
    std::fs::create_dir_all(&flat).unwrap();
    std::fs::copy(
        out.join("dt=2026-01-01/region=cn/part-00000.parquet"),
        flat.join("x.parquet"),
    )
    .unwrap();
    assert_eq!(
        cols(
            &db,
            &format!("SELECT * FROM read_parquet('{}/x.parquet')", p(&flat))
        ),
        vec!["id:Int64", "amount:Int64"],
        "数据文件里不该有分区列"
    );

    // ★ 闭环：从 hive 目录读回来，分区列重建 + 值一致
    let pat = format!("{}/**/*.parquet", p(&out));
    assert_eq!(
        q(
            &db,
            &format!("SELECT id, amount, dt, region FROM read_parquet('{pat}')")
        ),
        q(&db, "SELECT id, amount, dt, region FROM orders"),
        "分区导出的闭环必须逐值一致（含重建出来的分区列）"
    );
    // 重建的类型是 DATE 而不是字符串 —— 与 A4（F12 读侧）同一套推断
    assert_eq!(
        cols(&db, &format!("SELECT * FROM read_parquet('{pat}')")),
        vec!["id:Int64", "amount:Int64", "dt:Date32", "region:Utf8"],
    );

    // 分区谓词筛得动
    let pat_cn = format!("{}/**/*.parquet", p(&out));
    let n = q(
        &db,
        &format!(
            "SELECT count(*) FROM read_parquet('{pat_cn}') \
             WHERE dt = DATE '2026-01-01' AND region = 'cn'"
        ),
    );
    assert_eq!(n, vec!["2"]);
    std::fs::remove_dir_all(&root).ok();
}

/// `_common_metadata` 是**全 schema（含分区列）**、零行 —— 与 qsdb-sync 的
/// `sink-parquet` 同一约定。工具靠它不扫数据文件就能拿到完整 schema。
#[test]
fn 公共元数据带全_schema_含分区列() {
    let (db, root) = setup("meta");
    seed(&db);
    let out = root.join("lake");
    db.sql(&format!(
        "COPY (SELECT id, region, dt FROM orders) TO '{}' \
         (FORMAT PARQUET, PARTITION_BY (dt))",
        p(&out)
    ))
    .unwrap();

    // 零行 ⇒ 读它拿不到行；但 schema 里**有** dt
    let c = cols(
        &db,
        &format!(
            "SELECT * FROM read_parquet('{}')",
            p(&out.join("_common_metadata"))
        ),
    );
    assert!(
        c.contains(&"dt:Date32".to_string()),
        "_common_metadata 必须带上分区列（那是它存在的理由）：{c:?}"
    );
    assert!(c.contains(&"id:Int64".to_string()), "{c:?}");
    std::fs::remove_dir_all(&root).ok();
}

// ══ ③ 拒绝：写得出、读不对的形态一律当场报错 ═══════════════════════════════

fn rejected(db: &Qsdb, sql: &str) -> String {
    match db.sql(sql) {
        Ok(r) => panic!("这条该被拒：{sql}\n实得 {r:?}"),
        Err(e) => e.to_string(),
    }
}

#[test]
fn 目标目录非空要拒() {
    let (db, root) = setup("nonempty");
    seed(&db);
    let out = root.join("out");
    let sql = format!(
        "COPY (SELECT id FROM orders) TO '{}' (FORMAT PARQUET)",
        p(&out)
    );
    db.sql(&sql).unwrap();
    let m = rejected(&db, &sql);
    assert!(m.contains("不是空目录") && m.contains("part-"), "{m}");
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn 目标写成文件要拒并指路() {
    let (db, root) = setup("asfile");
    seed(&db);
    let m = rejected(
        &db,
        &format!(
            "COPY (SELECT id FROM orders) TO '{}/o.parquet' (FORMAT PARQUET)",
            p(&root)
        ),
    );
    assert!(m.contains("目录"), "{m}");
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn null_分区值要拒() {
    let (db, root) = setup("nullpart");
    db.sql("CREATE TABLE t (id BIGINT, dt DATE)").unwrap();
    db.sql("INSERT INTO t VALUES (1, DATE '2026-01-01'), (2, NULL)")
        .unwrap();
    let m = rejected(
        &db,
        &format!(
            "COPY (SELECT id, dt FROM t) TO '{}' (FORMAT PARQUET, PARTITION_BY (dt))",
            p(&root.join("o"))
        ),
    );
    assert!(
        m.contains("NULL") && m.contains("__HIVE_DEFAULT_PARTITION__"),
        "{m}"
    );
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn 分区列不在输出里要拒并列出可用列() {
    let (db, root) = setup("nocol");
    seed(&db);
    let m = rejected(
        &db,
        &format!(
            "COPY (SELECT id, amount FROM orders) TO '{}' (FORMAT PARQUET, PARTITION_BY (dt))",
            p(&root.join("o"))
        ),
    );
    assert!(
        m.contains("dt") && m.contains("id") && m.contains("amount"),
        "{m}"
    );
    std::fs::remove_dir_all(&root).ok();
}

/// ★ 这条是"读侧按目录值推断类型"逼出来的：`FLOAT` 的 `1.5` 写进目录名之后，
///   读回来会变成 `VARCHAR`。宁可拒，也不许**静默改类型**。
#[test]
fn 不可往返的分区列类型要拒() {
    let (db, root) = setup("floatpart");
    db.sql("CREATE TABLE t (id BIGINT, r DOUBLE)").unwrap();
    db.sql("INSERT INTO t VALUES (1, 1.5)").unwrap();
    let m = rejected(
        &db,
        &format!(
            "COPY (SELECT id, r FROM t) TO '{}' (FORMAT PARQUET, PARTITION_BY (r))",
            p(&root.join("o"))
        ),
    );
    assert!(m.contains("读不回来"), "{m}");
    std::fs::remove_dir_all(&root).ok();
}

/// 字符串列里存着**日期形态**的值 —— 类型是 STRING，但目录里只有文本，
/// 读回来会被推成 DATE。同样是静默改类型，拦下。
#[test]
fn 字符串列里存日期形态要拒() {
    let (db, root) = setup("datestr");
    db.sql("CREATE TABLE t (id BIGINT, k VARCHAR)").unwrap();
    db.sql("INSERT INTO t VALUES (1, 'cn'), (2, '2026-01-01')")
        .unwrap();
    let m = rejected(
        &db,
        &format!(
            "COPY (SELECT id, k FROM t) TO '{}' (FORMAT PARQUET, PARTITION_BY (k))",
            p(&root.join("o"))
        ),
    );
    assert!(m.contains("2026-01-01") && m.contains("读不回来"), "{m}");
    std::fs::remove_dir_all(&root).ok();
}

/// 分区列**重名**（`SELECT a.x, b.x`）—— `PARTITION_BY (x)` 指谁说不清。
#[test]
fn 分区列重名要拒() {
    let (db, root) = setup("dupname");
    db.sql("CREATE TABLE a (x BIGINT, v BIGINT)").unwrap();
    db.sql("CREATE TABLE b (x BIGINT, w BIGINT)").unwrap();
    db.sql("INSERT INTO a VALUES (1, 10)").unwrap();
    db.sql("INSERT INTO b VALUES (1, 20)").unwrap();
    let m = rejected(
        &db,
        &format!(
            "COPY (SELECT a.x, b.x, a.v FROM a JOIN b ON a.x = b.x) TO '{}' \
             (FORMAT PARQUET, PARTITION_BY (x))",
            p(&root.join("o"))
        ),
    );
    assert!(m.contains("2 个") && m.contains('x'), "{m}");
    std::fs::remove_dir_all(&root).ok();
}

// ══ ④ 语法面：每条不支持的形态都**指路**，不 dump 语法树 ═══════════════════

#[test]
fn 语法拒绝都指路() {
    let (db, root) = setup("syntax");
    seed(&db);
    let o = p(&root.join("o"));
    let cases: Vec<(String, &str)> = vec![
        (
            format!("COPY orders TO '{o}' (FORMAT PARQUET)"),
            "SELECT * FROM t",
        ),
        (
            format!("COPY orders FROM '{o}' (FORMAT PARQUET)"),
            "read_csv",
        ),
        (
            format!("COPY (SELECT id FROM orders) TO '{o}'"),
            "FORMAT PARQUET",
        ),
        (
            format!("COPY (SELECT id FROM orders) TO '{o}' (FORMAT CSV)"),
            "PARQUET",
        ),
        (
            format!("COPY (SELECT id FROM orders) TO '{o}' (FORMAT PARQUET, DELIMITER ',')"),
            "不支持选项",
        ),
        (
            format!("COPY (SELECT id FROM orders) TO '{o}' (PARTITION_BY (id + 1))"),
            "只接受列名",
        ),
    ];
    for (sql, needle) in cases {
        let m = rejected(&db, &sql);
        assert!(
            m.contains(needle),
            "`{sql}` 的报错该含 `{needle}`，实得：{m}"
        );
        assert!(
            !m.contains("Ident {") && !m.contains("Span("),
            "报错不许 dump 语法树：{m}"
        );
    }
    std::fs::remove_dir_all(&root).ok();
}

// ══ ⑤ 权限：往任意路径写文件 ⇒ 与 PG 的 `COPY TO` 同档，要 admin ═══════════

#[test]
fn 角色闸门() {
    let (db, root) = setup("role");
    seed(&db);
    let sql = format!(
        "COPY (SELECT id FROM orders) TO '{}' (FORMAT PARQUET)",
        p(&root.join("o"))
    );
    for r in [Role::Reader, Role::Writer] {
        let e = db
            .sql_with_role(&sql, r)
            .expect_err("非 admin 不该能导出")
            .to_string();
        assert!(e.contains("DDL/admin"), "{r:?}: {e}");
    }
    db.sql_with_role(&sql, Role::Admin).unwrap();
    assert!(root.join("o/part-00000.parquet").is_file());
    std::fs::remove_dir_all(&root).ok();
}

// ══ ⑥ 与既有路径不打架 ═══════════════════════════════════════════════════════

/// 库里那张表**没被动过**（导出是只读的副作用：只写目标目录）。
#[test]
fn 导出不改库里的数据() {
    let (db, root) = setup("readonly");
    seed(&db);
    let before = q(&db, "SELECT id, amount, region, dt FROM orders");
    db.sql(&format!(
        "COPY (SELECT id, amount, region, dt FROM orders) TO '{}' \
         (FORMAT PARQUET, PARTITION_BY (dt))",
        p(&root.join("o"))
    ))
    .unwrap();
    assert_eq!(q(&db, "SELECT id, amount, region, dt FROM orders"), before);
    std::fs::remove_dir_all(&root).ok();
}

/// 导出**带聚合/排序**的查询（导出的是查询结果，不是表）。
#[test]
fn 导出查询结果而非表() {
    let (db, root) = setup("query");
    seed(&db);
    let out = root.join("agg");
    db.sql(&format!(
        "COPY (SELECT region, sum(amount) AS total FROM orders GROUP BY region) TO '{}' \
         (FORMAT PARQUET, PARTITION_BY (region))",
        p(&out)
    ))
    .unwrap();
    let pat = format!("{}/**/*.parquet", p(&out));
    assert_eq!(
        q(
            &db,
            &format!("SELECT region, total FROM read_parquet('{pat}')")
        ),
        vec!["cn|70", "us|30"],
    );
    std::fs::remove_dir_all(&root).ok();
}
