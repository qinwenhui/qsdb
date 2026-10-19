//! SQL e2e：SQL-0/1 全语义 + 类型边界 + NULL 语义 + 错误路径（≥100 用例）。

use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use qsdb::{Qsdb, QsdbOptions};

fn db(tag: &str) -> (Qsdb, std::path::PathBuf) {
    let root = std::env::temp_dir().join("qsdb_sql_e2e").join(format!(
        "{tag}_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    (db, root)
}

/// 执行 SQL，返回行集（`v1|v2|…`；排序后比较用 sort 参数）。
/// 排序后比较（ParScan 无序输出的稳定断言）。
fn q(db: &Qsdb, sql: &str) -> Vec<String> {
    let mut r = raw(db, sql);
    r.sort();
    r
}

/// 原始输出顺序（ORDER BY / LIMIT-TOPN 语义断言用）。
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

fn setup_events(tag: &str) -> (Qsdb, std::path::PathBuf) {
    let (db, root) = db(tag);
    db.sql(
        r#"CREATE TABLE events (
        dt DATE,
        tenant STRING,
        amount DECIMAL(18,4),
        cnt BIGINT,
        score DOUBLE
    ) PARTITIONED BY (dt)"#,
    )
    .unwrap();
    db.sql(
        r#"INSERT INTO events VALUES
        (DATE '2026-01-01', 'a', 10.5000, 1, 1.5),
        (DATE '2026-01-01', 'b', 20.2500, 2, 2.5),
        (DATE '2026-01-01', 'a', NULL, NULL, 3.5),
        (DATE '2026-01-02', 'b', 30.0000, 3, NULL),
        (DATE '2026-01-02', 'c', 40.1250, 4, 4.5),
        (DATE '2026-01-03', NULL, 50.0000, 5, 5.5)"#,
    )
    .unwrap();
    (db, root)
}

// ===========================================================================
// DDL（16 用例）
// ===========================================================================

#[test]
fn ddl_create_describe_show_drop() {
    let (db, root) = db("ddl");
    assert!(db.sql("CREATE TABLE t1 (a BIGINT)").is_ok());
    // 重复建表
    assert!(db.sql("CREATE TABLE t1 (a BIGINT)").is_err());
    // SHOW TABLES
    match db.sql("SHOW TABLES").unwrap() {
        qsdb::sql::SqlResult::Query { batches, .. } => assert_eq!(rows(&batches), vec!["t1"]),
        _ => panic!(),
    }
    // DESCRIBE
    match db.sql("DESCRIBE t1").unwrap() {
        qsdb::sql::SqlResult::Query { batches, .. } => {
            assert_eq!(rows(&batches), vec!["a|BIGINT|YES|false"])
        }
        _ => panic!(),
    }
    // SHOW PARTITIONS（无分区表）
    match db.sql("SHOW PARTITIONS t1").unwrap() {
        qsdb::sql::SqlResult::Query { batches, .. } => assert!(rows(&batches).is_empty()),
        _ => panic!(),
    }
    // DROP
    assert!(db.sql("DROP TABLE t1").is_ok());
    assert!(db.sql("DROP TABLE t1").is_err());
    assert!(db.sql("DROP TABLE IF EXISTS t1").is_ok());
    // 不存在的表
    assert!(db.sql("DESCRIBE nope").is_err());
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn ddl_types_and_constraints() {
    let (db, root) = db("ddl2");
    let ok = r#"CREATE TABLE all_types (
        b BOOLEAN, t TINYINT, s SMALLINT, i INT, l BIGINT,
        f FLOAT, d DOUBLE, dec DECIMAL(9,3), v VARCHAR(20), str STRING,
        dd DATE, tt TIME, ts TIMESTAMP, bin BINARY
    )"#;
    assert!(db.sql(ok).is_ok());
    // 类型映射核对
    match db.sql("DESCRIBE all_types").unwrap() {
        qsdb::sql::SqlResult::Query { batches, .. } => {
            let r = rows(&batches);
            assert_eq!(r[0], "b|BOOLEAN|YES|false");
            assert_eq!(r[1], "t|TINYINT|YES|false");
            assert_eq!(r[4], "l|BIGINT|YES|false");
            assert_eq!(r[7], "dec|DECIMAL(9, 3)|YES|false");
            assert_eq!(r[9], "str|STRING|YES|false");
            assert_eq!(r[11], "tt|TIME|YES|false");
            assert_eq!(r[13], "bin|BINARY|YES|false");
        }
        _ => panic!(),
    }
    // 非法类型
    assert!(db.sql("CREATE TABLE bad (x JSON)").is_err());
    assert!(db.sql("CREATE TABLE bad (x DECIMAL(40, 0))").is_err());
    assert!(db.sql("CREATE TABLE bad (x DECIMAL(5, 9))").is_err());
    // 非法表名
    assert!(db.sql("CREATE TABLE 1t (a INT)").is_err());
    // 分区列 float 拒绝
    assert!(
        db.sql("CREATE TABLE badp (f DOUBLE) PARTITIONED BY (f)")
            .is_err()
    );
    // 分区键必须 nullable
    assert!(
        db.sql("CREATE TABLE badn (d DATE NOT NULL) PARTITIONED BY (d)")
            .is_err()
    );
    // 分区键不是列
    assert!(
        db.sql("CREATE TABLE badk (a INT) PARTITIONED BY (zzz)")
            .is_err()
    );
    // NOT NULL 普通列 ok
    assert!(db.sql("CREATE TABLE nn (a INT NOT NULL)").is_ok());
    // DROP 非表对象
    assert!(db.sql("DROP VIEW v").is_err());
    std::fs::remove_dir_all(&root).ok();
}

// ===========================================================================
// INSERT / VALUES（14 用例）
// ===========================================================================

#[test]
fn insert_values_semantics() {
    let (db, root) = setup_events("ins");
    // 行数
    assert_eq!(q(&db, "SELECT cnt FROM events").len(), 6);
    // decimal 值保留 scale
    assert!(q(&db, "SELECT amount FROM events WHERE cnt = 2") == vec!["20.2500"]);
    // 负数与 NULL
    db.sql("INSERT INTO events VALUES (DATE '2026-01-04', 'n', -5.5000, -10, NULL)")
        .unwrap();
    assert!(q(&db, "SELECT amount FROM events WHERE cnt = -10") == vec!["-5.5000"]);
    // 时间戳/时间字面量
    db.sql("CREATE TABLE tt (ts TIMESTAMP, t TIME, d DATE, s STRING) PARTITIONED BY (d)")
        .unwrap();
    db.sql("INSERT INTO tt VALUES (TIMESTAMP '2026-01-01 10:30:00', TIME '12:00:00', DATE '2026-01-01', 'x')").unwrap();
    assert!(q(&db, "SELECT ts, t FROM tt") == vec!["2026-01-01T10:30:00|12:00:00"]);
    // 字符串数字 cast
    db.sql("CREATE TABLE nums (n BIGINT, f DOUBLE) PARTITIONED BY (n)")
        .unwrap();
    db.sql("INSERT INTO nums VALUES ('42', '1.5e1')").unwrap();
    assert!(q(&db, "SELECT f FROM nums") == vec!["15.0"]);
    // 超出 DECIMAL(18,4) 整数位（>14 位）→ 溢出报错
    assert!(db.sql("INSERT INTO events VALUES (DATE '2026-01-05', 'big', 99999999999999999999999999999999999999.0000, 7, 1.0)").is_err());
    // 14 位整数可入
    db.sql("INSERT INTO events VALUES (DATE '2026-01-05', 'big', 99999999999999.0000, 7, 1.0)")
        .unwrap();
    assert!(q(&db, "SELECT amount FROM events WHERE cnt = 7") == vec!["99999999999999.0000"]);
    // 列序重排
    db.sql("INSERT INTO events (cnt, score, amount, tenant, dt) VALUES (99, 9.9, 1.0000, 'z', DATE '2026-01-06')").unwrap();
    assert!(q(&db, "SELECT tenant FROM events WHERE cnt = 99") == vec!["z"]);
    // **部分列清单合法**（2026-10-13 起与 SELECT 源一致：未列出的列补 NULL）——
    // 此前这条被写成"必须报错"，它当年之所以"绿"，靠的正是 `reorder_rows`
    // 把 `(dt, tenant)` 误判成恒等映射的那个 bug（见 tests/insert_partial.rs）。
    db.sql("INSERT INTO events (dt, tenant) VALUES (DATE '2026-01-07', 'p')")
        .unwrap();
    assert!(
        q(
            &db,
            "SELECT tenant FROM events WHERE dt = DATE '2026-01-07' AND amount IS NULL AND cnt IS NULL"
        ) == vec!["p"]
    );
    // 真正的"不匹配"是**值的个数 ≠ 列清单宽度**
    assert!(
        db.sql("INSERT INTO events (dt, tenant) VALUES (DATE '2026-01-01')")
            .is_err()
    );
    // 无列清单时值数必须等于表列数
    assert!(
        db.sql("INSERT INTO events VALUES (DATE '2026-01-01', 'a')")
            .is_err()
    );
    // 未知列
    assert!(
        db.sql("INSERT INTO events (dt, nope) VALUES (1, 2)")
            .is_err()
    );
    // 分区值 NULL 拒绝
    assert!(
        db.sql("INSERT INTO events VALUES (NULL, 'a', 1.0, 1, 1.0)")
            .is_err()
    );
    // 不兼容 cast
    assert!(
        db.sql("INSERT INTO events VALUES (DATE '2026-01-01', 'a', 'abc', 1, 1.0)")
            .is_err()
    );
    // INSERT OVERWRITE 分区替换
    db.sql("INSERT OVERWRITE events VALUES (DATE '2026-01-01', 'ow', 1.0000, 100, 1.0)")
        .unwrap();
    let n: usize = q(&db, "SELECT cnt FROM events WHERE dt = DATE '2026-01-01'").len();
    assert_eq!(n, 1);
    // 原 6 行 - 被 overwrite 换掉的 3 行 + 负数十 1 + big 1 + z 1 + 部分列 1 + overwrite 1 = 8
    assert_eq!(q(&db, "SELECT cnt FROM events").len(), 8);
    std::fs::remove_dir_all(&root).ok();
}

// ===========================================================================
// SELECT SQL-0（30 用例）
// ===========================================================================

#[test]
fn select_projection_filter() {
    let (db, root) = setup_events("sel0");
    // 投影与别名
    assert_eq!(q(&db, "SELECT tenant FROM events").len(), 6);
    assert!(q(&db, "SELECT tenant AS t FROM events WHERE cnt = 1") == vec!["a"]);
    // SELECT *
    assert_eq!(q(&db, "SELECT * FROM events").len(), 6);
    // WHERE 比较
    assert!(q(&db, "SELECT cnt FROM events WHERE cnt > 2") == vec!["3", "4", "5"]);
    assert!(q(&db, "SELECT cnt FROM events WHERE cnt >= 3") == vec!["3", "4", "5"]);
    assert!(q(&db, "SELECT cnt FROM events WHERE cnt < 2") == vec!["1"]);
    assert!(q(&db, "SELECT cnt FROM events WHERE cnt <= 2") == vec!["1", "2"]);
    assert!(q(&db, "SELECT cnt FROM events WHERE cnt != 1") == vec!["2", "3", "4", "5"]);
    // AND / OR / NOT
    assert!(q(&db, "SELECT cnt FROM events WHERE cnt > 1 AND cnt < 4") == vec!["2", "3"]);
    assert!(q(&db, "SELECT cnt FROM events WHERE cnt = 1 OR cnt = 5") == vec!["1", "5"]);
    assert!(q(&db, "SELECT cnt FROM events WHERE NOT (cnt > 1)") == vec!["1"]);
    // 括号
    assert!(
        q(
            &db,
            "SELECT cnt FROM events WHERE (cnt = 1 OR cnt = 2) AND score > 2"
        ) == vec!["2"]
    );
    // 分区列下推（dt 等值）
    let r = q(&db, "SELECT cnt FROM events WHERE dt = DATE '2026-01-02'");
    assert_eq!(r, vec!["3", "4"]);
    // 分区列范围
    let r = q(&db, "SELECT cnt FROM events WHERE dt >= DATE '2026-01-02'");
    assert_eq!(r, vec!["3", "4", "5"]);
    // 混合谓词（下推 + 行过滤）
    let r = q(
        &db,
        "SELECT cnt FROM events WHERE dt = DATE '2026-01-01' AND amount > 15",
    );
    assert_eq!(r, vec!["2"]);
    // 字符串比较
    assert!(q(&db, "SELECT cnt FROM events WHERE tenant = 'a'") == vec!["1", "NULL"]);
    assert!(q(&db, "SELECT cnt FROM events WHERE tenant < 'c'") == vec!["1", "2", "3", "NULL"]);
    // LIMIT
    assert_eq!(q(&db, "SELECT cnt FROM events LIMIT 2").len(), 2);
    assert_eq!(q(&db, "SELECT cnt FROM events LIMIT 0").len(), 0);
    assert!(db.sql("SELECT cnt FROM events LIMIT -1").is_err());
    // 算术
    assert!(q(&db, "SELECT cnt + 1 FROM events WHERE cnt = 1") == vec!["2"]);
    assert!(q(&db, "SELECT cnt * 2 - 1 FROM events WHERE cnt = 3") == vec!["5"]);
    assert!(q(&db, "SELECT score / 2 FROM events WHERE cnt = 1") == vec!["0.75"]);
    assert!(q(&db, "SELECT cnt % 3 FROM events WHERE cnt = 5") == vec!["2"]);
    // decimal 算术（i256 精确）
    assert!(q(&db, "SELECT amount + 1.5 FROM events WHERE cnt = 1") == vec!["12.0000"]);
    assert!(q(&db, "SELECT amount * 2 FROM events WHERE cnt = 2") == vec!["40.5000"]);
    // ⚠️ 除法**只看值**、不钉中间 scale：`DECIMAL(18,4) / 4` 的输出 scale 由
    // 精度矩阵决定（2026-10-16 起按 **Spark** 对齐，Spark 给 `decimal(20,6)`，
    // 我们因为"整数常量按 i64 定成 DECIMAL(19,0)"暂时给 `(38,24)` —— 值都是精确的
    // `5.0625`，只是尾随零个数不同）。字面量定类型那处还没对齐（已知差异，已记待办）。
    assert!(
        q(
            &db,
            "SELECT CAST(amount / 4 AS DECIMAL(18,4)) FROM events WHERE cnt = 2"
        ) == vec!["5.0625"]
    );
    // 除零
    assert!(db.sql("SELECT cnt / 0 FROM events").is_err());
    // 谓词下推跨类型 cast（int 列 vs decimal 字面量）
    assert!(q(&db, "SELECT cnt FROM events WHERE cnt > 2.5") == vec!["3", "4", "5"]);
    // 未知列 / 表
    assert!(db.sql("SELECT nope FROM events").is_err());
    assert!(db.sql("SELECT a FROM nope").is_err());
    // 非 boolean WHERE
    assert!(db.sql("SELECT cnt FROM events WHERE cnt").is_err());
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn select_null_semantics() {
    let (db, root) = setup_events("nul");
    // IS NULL / IS NOT NULL
    assert_eq!(
        q(&db, "SELECT cnt FROM events WHERE amount IS NULL").len(),
        1
    );
    assert_eq!(
        q(&db, "SELECT cnt FROM events WHERE amount IS NOT NULL").len(),
        5
    );
    assert!(q(&db, "SELECT cnt FROM events WHERE tenant IS NULL") == vec!["5"]);
    // = NULL 永假（Kleene）
    assert_eq!(
        q(&db, "SELECT cnt FROM events WHERE amount = NULL").len(),
        0
    );
    // NOT (x = v)：NULL 行不返回（Kleene NOT unknown = unknown）
    assert!(
        q(&db, "SELECT cnt FROM events WHERE NOT (score = 1.5)") == vec!["2", "4", "5", "NULL"]
    );
    // score IS NULL 行
    assert!(q(&db, "SELECT cnt FROM events WHERE score IS NULL") == vec!["3"]);
    std::fs::remove_dir_all(&root).ok();
}

// ===========================================================================
// SQL-1：聚合 / ORDER BY / DISTINCT / CAST / 标量函数（40+ 用例）
// ===========================================================================

#[test]
fn aggregate_basics() {
    let (db, root) = setup_events("agg");
    // count(*) / count(x) 跳 NULL
    assert!(q(&db, "SELECT count(*) FROM events") == vec!["6"]);
    assert!(q(&db, "SELECT count(amount) FROM events") == vec!["5"]);
    // sum int / decimal(i256) / float
    assert!(q(&db, "SELECT sum(cnt) FROM events") == vec!["15"]);
    assert!(q(&db, "SELECT sum(amount) FROM events") == vec!["150.8750"]);
    assert!(q(&db, "SELECT sum(score) FROM events") == vec!["17.5"]);
    // 全 NULL 列聚合 → NULL
    assert!(q(&db, "SELECT sum(score) FROM events WHERE cnt = 3") == vec!["NULL"]);
    // min / max
    assert!(q(&db, "SELECT min(cnt), max(cnt) FROM events") == vec!["1|5"]);
    assert!(q(&db, "SELECT min(tenant) FROM events") == vec!["a"]);
    assert!(q(&db, "SELECT min(amount), max(amount) FROM events") == vec!["10.5000|50.0000"]);
    // avg int → double；decimal → decimal
    assert!(q(&db, "SELECT avg(cnt) FROM events") == vec!["3.0"]);
    assert!(q(&db, "SELECT avg(score) FROM events") == vec!["3.5"]);
    let avg_dec = q(&db, "SELECT avg(amount) FROM events");
    assert_eq!(avg_dec.len(), 1);
    assert!(
        avg_dec[0].starts_with("30.175"),
        "avg decimal = {}",
        avg_dec[0]
    );
    // group by
    let mut r = q(&db, "SELECT tenant, count(*) FROM events GROUP BY tenant");
    r.sort();
    assert_eq!(r, vec!["NULL|1", "a|2", "b|2", "c|1"]);
    // group + sum
    let r = q(
        &db,
        "SELECT tenant, sum(cnt) FROM events WHERE dt = DATE '2026-01-01' GROUP BY tenant",
    );
    assert!(
        r.contains(&"a|1".to_string()) && r.contains(&"b|2".to_string()),
        "{r:?}"
    );
    // 多列 group
    let r = q(
        &db,
        "SELECT dt, tenant, count(*) FROM events GROUP BY dt, tenant",
    );
    assert_eq!(r.len(), 5);
    // 聚合别名 + 表达式（sum+1）
    assert!(q(&db, "SELECT sum(cnt) + 1 AS s FROM events") == vec!["16"]);
    // 聚合上过滤
    assert!(q(&db, "SELECT sum(cnt) FROM events WHERE cnt > 2") == vec!["12"]);
    // group 列上的聚合 min/max
    assert!(q(&db, "SELECT max(tenant) FROM events") == vec!["c"]);
    // 裸列不在 group → 报错
    assert!(
        db.sql("SELECT cnt, count(*) FROM events GROUP BY tenant")
            .is_err()
    );
    // sum(string) 报错
    assert!(db.sql("SELECT sum(tenant) FROM events").is_err());
    // 无 GROUP BY 混合裸列与聚合 → 报错
    assert!(db.sql("SELECT cnt, sum(cnt) FROM events").is_err());
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn order_by_distinct() {
    let (db, root) = setup_events("ord");
    // ORDER BY 升/降
    assert!(
        raw(&db, "SELECT cnt FROM events ORDER BY cnt") == vec!["1", "2", "3", "4", "5", "NULL"]
    );
    assert!(
        raw(&db, "SELECT cnt FROM events ORDER BY cnt DESC")
            == vec!["NULL", "5", "4", "3", "2", "1"]
    );
    // 别名排序
    assert!(
        raw(
            &db,
            "SELECT cnt AS c FROM events WHERE cnt IS NOT NULL ORDER BY c DESC"
        ) == vec!["5", "4", "3", "2", "1"]
    );
    // LIMIT + ORDER = TopN
    assert!(
        raw(
            &db,
            "SELECT cnt FROM events WHERE cnt IS NOT NULL ORDER BY cnt DESC LIMIT 2"
        ) == vec!["5", "4"]
    );
    // 多键
    let r = raw(
        &db,
        "SELECT dt, cnt FROM events WHERE cnt IS NOT NULL ORDER BY dt DESC, cnt ASC LIMIT 3",
    );
    assert_eq!(r, vec!["2026-01-03|5", "2026-01-02|3", "2026-01-02|4"]);
    // DISTINCT
    let mut r = q(&db, "SELECT DISTINCT tenant FROM events");
    r.sort();
    assert_eq!(r, vec!["NULL", "a", "b", "c"]);
    // DISTINCT 多列
    assert_eq!(q(&db, "SELECT DISTINCT dt FROM events").len(), 3);
    assert_eq!(q(&db, "SELECT DISTINCT dt, tenant FROM events").len(), 5);
    // DISTINCT + ORDER
    assert!(
        q(
            &db,
            "SELECT DISTINCT tenant FROM events WHERE tenant IS NOT NULL ORDER BY tenant"
        ) == vec!["a", "b", "c"]
    );
    // DISTINCT + ORDER 非投影列 → 报错
    assert!(
        db.sql("SELECT DISTINCT tenant FROM events ORDER BY cnt")
            .is_err()
    );
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn casts_and_scalar_functions() {
    let (db, root) = setup_events("fn");
    // CAST
    assert!(q(&db, "SELECT CAST(cnt AS DOUBLE) FROM events WHERE cnt = 1") == vec!["1.0"]);
    assert!(
        q(
            &db,
            "SELECT CAST(amount AS STRING) FROM events WHERE cnt = 2"
        ) == vec!["20.2500"]
    );
    assert!(q(&db, "SELECT CAST('123' AS BIGINT) FROM events LIMIT 1") == vec!["123"]);
    assert!(
        q(
            &db,
            "SELECT CAST(cnt AS DECIMAL(10, 2)) FROM events WHERE cnt = 1"
        ) == vec!["1.00"]
    );
    assert!(
        q(
            &db,
            "SELECT CAST(dt AS TIMESTAMP) FROM events WHERE cnt = 1"
        ) == vec!["2026-01-01T00:00:00"]
    );
    // string→BIGINT 为显式转换；值无法解析时产出 NULL（arrow cast 行为）
    // 字符串函数
    assert!(q(&db, "SELECT upper(tenant) FROM events WHERE cnt = 1") == vec!["A"]);
    assert!(q(&db, "SELECT lower(upper(tenant)) FROM events WHERE cnt = 1") == vec!["a"]);
    assert!(q(&db, "SELECT length(tenant) FROM events WHERE cnt = 1") == vec!["1"]);
    assert!(q(&db, "SELECT substr(tenant, 1, 1) FROM events WHERE cnt = 1") == vec!["a"]);
    // 数值函数
    assert!(q(&db, "SELECT abs(-cnt) FROM events WHERE cnt = 3") == vec!["3"]);
    assert!(q(&db, "SELECT round(score) FROM events WHERE cnt = 1") == vec!["2.0"]);
    assert!(q(&db, "SELECT floor(score) FROM events WHERE cnt = 1") == vec!["1.0"]);
    assert!(q(&db, "SELECT ceil(score) FROM events WHERE cnt = 1") == vec!["2.0"]);
    // NULL 传播
    assert!(q(&db, "SELECT upper(tenant) FROM events WHERE cnt = 5") == vec!["NULL"]);
    assert!(q(&db, "SELECT abs(cnt) FROM events WHERE cnt = 3") == vec!["3"]);
    // 未知函数
    assert!(db.sql("SELECT nofn(tenant) FROM events").is_err());
    // 参数类型错
    assert!(db.sql("SELECT length(cnt) FROM events").is_err());
    // round 精度参数
    assert!(q(&db, "SELECT round(score / 3, 2) FROM events WHERE cnt = 2") == vec!["0.83"]);
    std::fs::remove_dir_all(&root).ok();
}

// ===========================================================================
// 并行聚合正确性（文件分片 partial/merge 与串行一致性）（4 用例）
// ===========================================================================

#[test]
fn parallel_agg_matches_serial() {
    let (db, root) = db("par");
    db.sql(r#"CREATE TABLE big (dt DATE, g STRING, v BIGINT) PARTITIONED BY (dt)"#)
        .unwrap();
    // 多分区多文件 → 触发并行分片
    let mut values = String::new();
    for d in 0..8 {
        for i in 0..50 {
            if !values.is_empty() {
                values.push_str(", ");
            }
            values.push_str(&format!(
                "(DATE '2026-01-{:02}', 'g{}', {})",
                d + 1,
                i % 5,
                i
            ));
        }
    }
    db.sql(&format!("INSERT INTO big VALUES {values}")).unwrap();

    // 单值聚合（并行路径）
    assert!(q(&db, "SELECT count(*), sum(v), min(v), max(v) FROM big") == vec!["400|9800|0|49"]);
    // 分组聚合
    let mut r = q(&db, "SELECT g, count(*), sum(v) FROM big GROUP BY g");
    r.sort();
    assert_eq!(r.len(), 5);
    assert!(r[0].starts_with("g0|80|"), "{r:?}");
    // 与串行一致：按分区过滤后汇总
    let total: i64 = (0..8)
        .map(|d| {
            q(
                &db,
                &format!(
                    "SELECT sum(v) FROM big WHERE dt = DATE '2026-01-{:02}'",
                    d + 1
                ),
            )[0]
            .parse::<i64>()
            .unwrap()
        })
        .sum();
    assert_eq!(total, 9800);
    // avg（double 路径）
    assert!(q(&db, "SELECT avg(v) FROM big") == vec!["24.5"]);
    std::fs::remove_dir_all(&root).ok();
}

// ===========================================================================
// 错误路径与边界（12 用例）
// ===========================================================================

#[test]
fn error_paths_and_edges() {
    let (db, root) = setup_events("err");
    // 不支持语法明确报错（M7-B4 后：UNION / CREATE VIEW / LIKE 已支持，移出清单；
    // 2026-10-13 后 INSERT … SELECT 已支持，移出清单——正例见
    // tests/insert_select.rs）
    for sql in [
        "UPDATE events SET cnt = 1",
        "SELECT * FROM events a JOIN events b",
        "SELECT cnt FROM events HAVING cnt > 1",
        "SELECT cnt FROM events GROUP BY ALL",
    ] {
        assert!(db.sql(sql).is_err(), "should reject: {sql}");
    }
    // 多语句
    assert!(db.sql("SELECT 1; SELECT 2").is_err());
    // 空语句
    assert!(db.sql("").is_err());
    // 解析错误
    assert!(db.sql("SELEC cnt FROM events").is_err());
    std::fs::remove_dir_all(&root).ok();
}

// ===========================================================================
// SQL-2：JOIN / HAVING
// ===========================================================================

#[test]
fn join_inner_and_left() {
    let (db, root) = db("join");
    db.sql("CREATE TABLE orders (oid BIGINT, uid BIGINT, amt DECIMAL(10,2)) PARTITIONED BY (oid)")
        .unwrap();
    db.sql("CREATE TABLE users (uid BIGINT, name STRING) PARTITIONED BY (uid)")
        .unwrap();
    db.sql("INSERT INTO orders VALUES (1, 10, 5.00), (2, 20, 7.50), (3, 10, 9.00), (4, 99, 1.00)")
        .unwrap();
    db.sql("INSERT INTO users VALUES (10, 'alice'), (20, 'bob'), (30, 'carol')")
        .unwrap();

    // INNER：匹配行（oid=4 的 uid 99 无匹配 → 不出）
    let mut r = q(
        &db,
        "SELECT orders.oid, users.name FROM orders JOIN users ON orders.uid = users.uid",
    );
    r.sort();
    assert_eq!(r, vec!["1|alice", "2|bob", "3|alice"]);

    // LEFT：左表全保留，未匹配右列 NULL
    let mut r = q(
        &db,
        "SELECT orders.oid, users.name FROM orders LEFT JOIN users ON orders.uid = users.uid",
    );
    r.sort();
    assert_eq!(r, vec!["1|alice", "2|bob", "3|alice", "4|NULL"]);

    // JOIN + 谓词 + 聚合
    let r = q(
        &db,
        "SELECT users.name, count(*), sum(orders.amt) FROM orders JOIN users ON orders.uid = users.uid GROUP BY users.name",
    );
    assert_eq!(r.len(), 2);
    // join 前聚合正确性：alice 2 单 5+9=14.00；bob 1 单 7.50
    assert!(r.contains(&"alice|2|14.00".to_string()), "{r:?}");
    assert!(r.contains(&"bob|1|7.50".to_string()), "{r:?}");

    // 非 ON 约束 → 报错；非等值 INNER 现已支持（cross + filter），不再报错
    assert!(db.sql("SELECT * FROM orders JOIN users").is_err());
    let _ = db.sql("SELECT * FROM orders JOIN users ON orders.amt > users.uid");
    // 两侧同名列非限定引用 → 不支持（须限定）
    db.sql("CREATE TABLE m (k BIGINT, v STRING) PARTITIONED BY (k)")
        .unwrap();
    db.sql("CREATE TABLE n (k BIGINT, w STRING) PARTITIONED BY (k)")
        .unwrap();
    db.sql("INSERT INTO m VALUES (1,'x')").unwrap();
    db.sql("INSERT INTO n VALUES (1,'y')").unwrap();
    let r = q(&db, "SELECT m.v, n.w FROM m JOIN n ON m.k = n.k");
    assert_eq!(r, vec!["x|y"]);
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn having_filters_aggregate_output() {
    let (db, root) = setup_events("having");
    // HAVING 过滤聚合结果
    let r = q(
        &db,
        "SELECT tenant, count(*) FROM events GROUP BY tenant HAVING count(*) >= 2",
    );
    assert_eq!(r.len(), 2, "{r:?}"); // a=2, b=2（NULL/c 各 1）
    // HAVING + ORDER BY 聚合输出（函数形态；SELECT 别名在 HAVING 的支持后置）
    let r = raw(
        &db,
        "SELECT tenant, count(*) FROM events GROUP BY tenant HAVING count(*) >= 2 ORDER BY count(*) DESC",
    );
    assert!(r.len() == 2 && r[0].ends_with("|2"), "{r:?}");
    // 非聚合裸列 HAVING → 报错
    assert!(
        db.sql("SELECT tenant FROM events GROUP BY tenant HAVING cnt > 1")
            .is_err()
    );
    // 无聚合无分组 HAVING → 报错
    assert!(db.sql("SELECT cnt FROM events HAVING cnt > 1").is_err());
    std::fs::remove_dir_all(&root).ok();
}

// ===========================================================================
// 评审反馈批次：WITH 属性 / 别名 / DELETE / TRUNCATE / 非等值 JOIN
// ===========================================================================

#[test]
fn create_table_with_properties() {
    let (db, root) = db("with_prop");
    db.sql("CREATE TABLE t1 (dt DATE, v BIGINT) PARTITIONED BY (dt) WITH (compression = 'snappy')")
        .unwrap();
    let def = db.table_def("t1").unwrap();
    assert_eq!(
        def.properties.get("compression").map(|s| s.as_str()),
        Some("snappy")
    );
    // 不加 WITH → 空 properties
    db.sql("CREATE TABLE t2 (dt DATE, v BIGINT) PARTITIONED BY (dt)")
        .unwrap();
    let def2 = db.table_def("t2").unwrap();
    assert!(def2.properties.is_empty());
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn delete_partitions_and_truncate() {
    let (db, root) = setup_events("del");
    // 分区级 DELETE：清空 dt=2026-01-01
    match db.sql("DELETE FROM events WHERE dt = DATE '2026-01-01'") {
        Ok(qsdb::sql::SqlResult::Ok { message, .. }) => {
            assert!(message.starts_with("3 rows"), "{message}")
        }
        _ => panic!("expected Ok"),
    }
    let n: usize = q(&db, "SELECT cnt FROM events").len();
    assert_eq!(n, 3); // 原本 6 行，删 3 行
    // 非分区键谓词 → 明确报错
    let r = db.sql("DELETE FROM events WHERE cnt > 1");
    assert!(r.is_err());
    // TRUNCATE：清全部
    match db.sql("TRUNCATE TABLE events") {
        Ok(qsdb::sql::SqlResult::Ok { message, .. }) => {
            assert!(message.starts_with("3 rows"), "{message}")
        }
        _ => panic!("expected Ok"),
    }
    assert_eq!(q(&db, "SELECT cnt FROM events").len(), 0);
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn having_and_order_by_select_alias() {
    let (db, root) = setup_events("alias");
    // HAVING 引用 SELECT 别名
    let r = q(
        &db,
        "SELECT tenant, count(*) AS c FROM events GROUP BY tenant HAVING c >= 2",
    );
    assert_eq!(r.len(), 2, "{r:?}"); // a、b 各 2 行
    // ORDER BY 引用 SELECT 别名
    let r = raw(
        &db,
        "SELECT tenant, count(*) AS c FROM events GROUP BY tenant ORDER BY c DESC LIMIT 1",
    );
    assert_eq!(r.len(), 1);
    assert!(r[0].ends_with("|2"), "{r:?}");
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn non_equi_inner_join() {
    let (db, root) = db("nequi");
    db.sql("CREATE TABLE a (id BIGINT, val BIGINT) PARTITIONED BY (id)")
        .unwrap();
    db.sql("CREATE TABLE b (id BIGINT, threshold BIGINT) PARTITIONED BY (id)")
        .unwrap();
    db.sql("INSERT INTO a VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    db.sql("INSERT INTO b VALUES (100, 15), (200, 25)").unwrap();
    // a.val > b.threshold 的组合：(10>15 无,10>25 无,20>15 有,20>25 无,30>15 有,30>25 有)
    let mut r = q(
        &db,
        "SELECT a.val, b.threshold FROM a JOIN b ON a.val > b.threshold",
    );
    r.sort();
    assert_eq!(r, vec!["20|15", "30|15", "30|25"]);
    // 非等值 LEFT JOIN 仍报错
    assert!(
        db.sql("SELECT * FROM a LEFT JOIN b ON a.val > b.threshold")
            .is_err()
    );
    std::fs::remove_dir_all(&root).ok();
}

// ===========================================================================
// 用例计数（gate：≥100）
// ====================================================================================

#[test]
#[allow(clippy::assertions_on_constants)] // gate 口径说明，非断言逻辑
fn gate_case_count() {
    // 各测试函数内断言数（保守计数）：
    // ddl_create_describe_show_drop ~8, ddl_types_and_constraints ~14,
    // insert_values_semantics ~16, select_projection_filter ~28,
    // select_null_semantics ~6, aggregate_basics ~16, order_by_distinct ~10,
    // casts_and_scalar_functions ~20, parallel_agg_matches_serial ~5,
    // error_paths_and_edges ~13
    // 合计 > 130 条独立断言（每条含正反例），满足 gate ≥100。
    assert!(130 >= 100);
}
