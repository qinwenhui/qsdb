//! 嵌套子查询（M5-A3）：FROM 派生表 / JOIN 右派生表 / 标量子查询 / IN 值列表。

use qsdb::{Qsdb, QsdbOptions};

fn setup(tag: &str) -> (Qsdb, std::path::PathBuf) {
    let root = std::env::temp_dir().join("qsdb_m5_subq").join(format!(
        "{tag}_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    db.sql("CREATE TABLE sales (dt DATE, tenant STRING, amt BIGINT) PARTITIONED BY (dt)")
        .unwrap();
    db.sql(
        "INSERT INTO sales VALUES
        (DATE '2026-01-01', 'a', 10), (DATE '2026-01-01', 'b', 20),
        (DATE '2026-01-02', 'a', 30), (DATE '2026-01-02', 'c', 40)",
    )
    .unwrap();
    (db, root)
}

fn q(db: &Qsdb, sql: &str) -> Vec<String> {
    let batches = db.sql_collect(sql).unwrap();
    let mut out = Vec::new();
    for b in &batches {
        for i in 0..b.num_rows() {
            let mut cells = Vec::new();
            for c in 0..b.num_columns() {
                let arr = b.column(c);
                if arr.is_null(i) {
                    cells.push("NULL".into());
                } else {
                    cells.push(
                        arrow::util::display::array_value_to_string(arr.as_ref(), i).unwrap(),
                    );
                }
            }
            out.push(cells.join("|"));
        }
    }
    out.sort();
    out
}

#[test]
fn derived_table_from() {
    let (db, root) = setup("derived");
    // 派生表聚合 + 外层排序
    let r = q(
        &db,
        "SELECT tenant, total FROM
            (SELECT tenant, sum(amt) AS total FROM sales GROUP BY tenant) AS t
         ORDER BY tenant",
    );
    assert_eq!(r, vec!["a|40", "b|20", "c|40"]);

    // 外层 WHERE 过滤派生表输出
    let r = q(
        &db,
        "SELECT tenant FROM
            (SELECT tenant, sum(amt) AS total FROM sales GROUP BY tenant) t
         WHERE total > 25 ORDER BY tenant",
    );
    assert_eq!(r, vec!["a", "c"]);

    // 派生表嵌套两层
    let r = q(
        &db,
        "SELECT max(total) AS m FROM
            (SELECT tenant, sum(amt) AS total FROM
                (SELECT * FROM sales WHERE amt > 15) s
             GROUP BY tenant) t",
    );
    // amt>15: b(20),a(30),c(40) → totals a=30,b=20,c=40 → max 40
    assert_eq!(r, vec!["40"]);

    // CTE + 派生表混用（派生表可见外层 CTE）
    let r = q(
        &db,
        "WITH big AS (SELECT amt FROM sales WHERE amt > 15)
         SELECT sum(amt) FROM (SELECT amt FROM big) x",
    );
    assert_eq!(r, vec!["90"]);
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn derived_join_right() {
    let (db, root) = setup("djoin");
    let r = q(
        &db,
        "SELECT sales.tenant, t.total FROM sales
         JOIN (SELECT tenant, sum(amt) AS total FROM sales GROUP BY tenant) AS t
              ON sales.tenant = t.tenant
         ORDER BY sales.tenant, t.total",
    );
    assert_eq!(r, vec!["a|40", "a|40", "b|20", "c|40"]);
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn scalar_subquery() {
    let (db, root) = setup("scalar");
    // 投影中的标量子查询
    let r = q(
        &db,
        "SELECT tenant, amt, (SELECT max(amt) FROM sales) AS mx
         FROM sales WHERE tenant = 'a' ORDER BY amt",
    );
    assert_eq!(r, vec!["a|10|40", "a|30|40"]);

    // WHERE 中的标量子查询
    let r = q(
        &db,
        "SELECT tenant, amt FROM sales WHERE amt = (SELECT max(amt) FROM sales)",
    );
    assert_eq!(r, vec!["c|40"]);

    // 表达式内（算术 + 比较）：avg=25 → amt > 15 → b/a(30)/c
    let r = q(
        &db,
        "SELECT tenant FROM sales WHERE amt > (SELECT avg(amt) FROM sales) - 10 ORDER BY tenant",
    );
    assert_eq!(r, vec!["a", "b", "c"]);

    // 必须恰好一列
    assert!(
        db.sql("SELECT (SELECT tenant, amt FROM sales) FROM sales")
            .is_err()
    );

    // 关联子查询不支持：子查询独立绑定，限定名经裸名回退解析到子查询自己的列
    // （同名表自引用恰好语义等价；异表关联引用会静默解析错位——已知限制）
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn in_value_list() {
    let (db, root) = setup("inlist");
    let r = q(
        &db,
        "SELECT amt FROM sales WHERE tenant IN ('a', 'c') ORDER BY amt",
    );
    assert_eq!(r, vec!["10", "30", "40"]);

    let r = q(
        &db,
        "SELECT amt FROM sales WHERE tenant NOT IN ('a') ORDER BY amt",
    );
    assert_eq!(r, vec!["20", "40"]);

    // IN 与其他谓词组合
    let r = q(
        &db,
        "SELECT amt FROM sales WHERE tenant IN ('a', 'b') AND amt > 15 ORDER BY amt",
    );
    assert_eq!(r, vec!["20", "30"]);
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn limit_zero_schema_contract() {
    // 同步工具 schema 协商契约：SELECT ... LIMIT 0 → 零行仍携带完整 schema
    let (db, root) = setup("limit0");
    match db.sql("SELECT tenant, amt FROM sales LIMIT 0") {
        Ok(qsdb::sql::SqlResult::Query {
            schema, batches, ..
        }) => {
            assert_eq!(schema.fields().len(), 2);
            assert_eq!(schema.field(0).name(), "tenant");
            assert_eq!(schema.field(1).name(), "amt");
            assert!(batches.iter().all(|b| b.num_rows() == 0));
        }
        Err(e) => panic!("LIMIT 0 should succeed: {e}"),
        _ => panic!("expected Query result"),
    }
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn predicate_column_not_in_projection() {
    // 回归：谓词列不在投影中（count(*) WHERE v>15）——下推进 Scan 的谓词
    // 引用列必须进列裁剪收集，否则投影被裁到只剩分区列、谓词静默失效
    let (db, root) = setup("predcol");
    let r = q(&db, "SELECT count(*) FROM sales WHERE amt > 15");
    // amt>15: b(20), a(30), c(40) → 3（失效则 4）
    assert_eq!(r, vec!["3"]);

    // 谓词列在投影中（原本就正确）对照
    let r = q(&db, "SELECT amt FROM sales WHERE amt > 15 ORDER BY amt");
    assert_eq!(r, vec!["20", "30", "40"]);
    std::fs::remove_dir_all(&root).ok();
}

// ---------------------------------------------------------------------------
// EXISTS / IN 子查询（semi-join / anti-join）
// ---------------------------------------------------------------------------

fn setup_semi(tag: &str) -> (Qsdb, std::path::PathBuf) {
    let root = std::env::temp_dir().join("qsdb_semi_join").join(format!(
        "{tag}_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    db.sql("CREATE TABLE t (dt DATE, g BIGINT, v BIGINT) PARTITIONED BY (dt)")
        .unwrap();
    db.sql(
        "INSERT INTO t VALUES
        (DATE '2026-01-01', 1, 100), (DATE '2026-01-01', 2, 101),
        (DATE '2026-01-02', 3, 102), (DATE '2026-01-02', 1, 103)",
    )
    .unwrap();
    db.sql("CREATE TABLE u (dt DATE, g BIGINT) PARTITIONED BY (dt)")
        .unwrap();
    db.sql(
        "INSERT INTO u VALUES
        (DATE '2026-01-01', 1), (DATE '2026-01-01', 1), (DATE '2026-01-02', 2)",
    )
    .unwrap();
    db.sql("CREATE TABLE dims (g BIGINT, label STRING)")
        .unwrap();
    db.sql("INSERT INTO dims VALUES (1, 'one'), (3, 'three'), (5, 'five')")
        .unwrap();
    (db, root)
}

#[test]
fn in_subquery_basic_and_dupes() {
    let (db, root) = setup_semi("in");
    // g IN {1,3,5}：t 的 g=1×2, g=3×1 → 3 行；子查询含重复 g=1 —— semi
    // 至多发射一次（无重复）是硬语义
    let r = q(
        &db,
        "SELECT count(*) FROM t WHERE g IN (SELECT g FROM dims)",
    );
    assert_eq!(r, vec!["3"]);
    let r = q(
        &db,
        "SELECT v FROM t WHERE g IN (SELECT g FROM dims) ORDER BY v",
    );
    assert_eq!(r, vec!["100", "102", "103"]);
    // NOT IN：g=2 行
    let r = q(
        &db,
        "SELECT v FROM t WHERE g NOT IN (SELECT g FROM dims) ORDER BY v",
    );
    assert_eq!(r, vec!["101"]);
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn in_subquery_with_local_filter() {
    let (db, root) = setup_semi("infilter");
    // 子查询内本地谓词：只留 g=3 → 1 行
    let r = q(
        &db,
        "SELECT count(*) FROM t WHERE g IN (SELECT g FROM dims WHERE label = 'three')",
    );
    assert_eq!(r, vec!["1"]);
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn not_in_null_semantics() {
    let (db, root) = setup_semi("notin-null");
    // build 含 NULL → NOT IN 恒空（SQL 三值）。空输入的无 GROUP BY 聚合
    // **恰好一行、值为 0**——此前返回零行，本用例按现状断言并把"单行化"
    // 记为后置项；2026-10-13 已实现（`exec::agg::finalize_agg` 补"空组"），
    // 故这里改为断言正确语义。
    db.sql("INSERT INTO dims VALUES (NULL, 'nil')").unwrap();
    let r = q(
        &db,
        "SELECT count(*) FROM t WHERE g NOT IN (SELECT g FROM dims)",
    );
    assert_eq!(r, vec!["0"]);
    // IN 不受 build NULL 影响（NULL 键不命中）
    let r = q(
        &db,
        "SELECT count(*) FROM t WHERE g IN (SELECT g FROM dims)",
    );
    assert_eq!(r, vec!["3"]);
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn correlated_exists_equi() {
    let (db, root) = setup_semi("exists");
    // u 有 (dt,g) 对 {0101-1, 0102-2}；t 按 (dt,g) 匹配：t(0101,1)×2 命中
    // t(0101,2) 不命中（g 不等），t(0102,3)/(0102,1) 不命中
    let r = q(
        &db,
        "SELECT v FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.g = t.g AND u.dt = t.dt) ORDER BY v",
    );
    // u(dt,g) = {(01-01,1)×2, (01-02,2)}：仅 t(01-01, g=1) 命中 → v=100
    assert_eq!(r, vec!["100"]);
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn correlated_exists_multi_key_precise() {
    let (db, root) = setup_semi("exists2");
    // 单键 g：u.g={1,1,2} → t.g∈{1,2,3,1} 命中 g∈{1,2} → 三行
    let r = q(
        &db,
        "SELECT v FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.g = t.g) ORDER BY v",
    );
    assert_eq!(r, vec!["100", "101", "103"]);
    // NOT EXISTS：g=3 一行
    let r = q(
        &db,
        "SELECT v FROM t WHERE NOT EXISTS (SELECT 1 FROM u WHERE u.g = t.g) ORDER BY v",
    );
    assert_eq!(r, vec!["102"]);
    // 本地谓词在子查询内：u.g=2 才算存在 → t.g=2 行
    let r = q(
        &db,
        "SELECT v FROM t WHERE EXISTS (SELECT 1 FROM u WHERE u.g = t.g AND u.dt = DATE '2026-01-02') ORDER BY v",
    );
    assert_eq!(r, vec!["101"]);
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn semi_mixed_with_other_where() {
    let (db, root) = setup_semi("mix");
    // semi + 普通谓词混合：g∈{1,3,5} 且 v>100
    let r = q(
        &db,
        "SELECT v FROM t WHERE v > 100 AND g IN (SELECT g FROM dims) ORDER BY v",
    );
    assert_eq!(r, vec!["102", "103"]);
    // 聚合叠加
    let r = q(
        &db,
        "SELECT count(*), sum(v) FROM t WHERE g IN (SELECT g FROM dims)",
    );
    assert_eq!(r, vec!["3|305"]);
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn semi_join_chained_two_subqueries() {
    let (db, root) = setup_semi("chain");
    db.sql("CREATE TABLE dims2 (g BIGINT)").unwrap();
    db.sql("INSERT INTO dims2 VALUES (1)").unwrap();
    // 两个 IN 合取 → 两个 semi join 链
    let r = q(
        &db,
        "SELECT v FROM t WHERE g IN (SELECT g FROM dims) AND g IN (SELECT g FROM dims2) ORDER BY v",
    );
    assert_eq!(r, vec!["100", "103"]);
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn semi_date_key_uses_int_fast_path() {
    // 分区键 Date32 → 整数快路径/位图路径（执行形态由内核自选，本测试锁语义）
    let (db, root) = setup_semi("datekey");
    let r = q(
        &db,
        "SELECT count(*) FROM t WHERE dt IN (SELECT dt FROM u WHERE g = 1)",
    );
    // u.g=1 的 dt = 2026-01-01 → t 两行
    assert_eq!(r, vec!["2"]);
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn semi_error_paths() {
    let (db, root) = setup_semi("err");
    // 非关联 EXISTS：v1 报不支持
    assert!(
        db.sql("SELECT 1 FROM t WHERE EXISTS (SELECT 1 FROM dims)")
            .is_err()
    );
    // 多列子查询
    assert!(
        db.sql("SELECT 1 FROM t WHERE g IN (SELECT g, label FROM dims)")
            .is_err()
    );
    // OR 内 semi：v1 不支持（顶层 AND 合取级）
    assert!(
        db.sql("SELECT 1 FROM t WHERE v > 100 OR g IN (SELECT g FROM dims)")
            .is_err()
    );
    std::fs::remove_dir_all(&root).ok();
}

// ---------------------------------------------------------------------------
// qsdb-sync 反馈回归：F2 generation 继承 / F3 us 时间戳外表 / F4 table_schema
// ---------------------------------------------------------------------------

#[test]
fn f2_clear_partition_inherits_generation() {
    let (db, root) = setup("f2gen");
    // 三次提交 → generation 3
    let sql = "SELECT count(*) FROM sales";
    let _ = q(&db, sql);
    let store = qsdb::Store::local(&root);
    let pdir = root.join("default").join("sales").join("dt=2026-01-01");
    let m1 = qsdb::manifest::PartitionManifest::read(&store, &pdir)
        .unwrap()
        .unwrap();
    assert!(m1.generation >= 1);
    // TRUNCATE（清空）→ generation 必须继承 +1（不回退为 1）
    db.sql("TRUNCATE TABLE sales").unwrap();
    let m2 = qsdb::manifest::PartitionManifest::read(&store, &pdir)
        .unwrap()
        .unwrap();
    assert_eq!(
        m2.generation,
        m1.generation + 1,
        "F2: clear must inherit generation (sync 的增量判据契约)"
    );
    assert_eq!(m2.total_rows(), 0);
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn f4_table_schema_method() {
    let (db, root) = setup("f4schema");
    let schema = db.table_schema("sales").unwrap();
    let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(names, vec!["dt", "tenant", "amt"]);
    // 不存在的表报错
    assert!(db.table_schema("nope").is_err());
    std::fs::remove_dir_all(&root).ok();
}
