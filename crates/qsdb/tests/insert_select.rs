//! `INSERT INTO t [(cols)] SELECT …`（导入能力：从表 / 从 `read_parquet` 外表灌）。
//!
//! 覆盖：另一张表灌入、从 read_parquet 外表灌入（真正的验收点）、带列序
//! （位置语义）、部分列清单（缺列补 NULL）、分区表灌入后的可见性、
//! 以及与 VALUES 路径一致的错误路径（列数不匹配 / 类型不兼容 / 未知列）。

use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_writer::ArrowWriter;

use qsdb::{Qsdb, QsdbOptions};

fn tmp_root(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir()
        .join("qsdb_insert_select")
        .join(format!("{tag}_{}", uuid::Uuid::now_v7().simple()));
    std::fs::create_dir_all(&d).unwrap();
    d
}

/// 执行并取错误文案（`SqlResult` 没实现 Debug，用不了 `unwrap_err`）。
fn err(db: &Qsdb, sql: &str) -> String {
    match db.sql(sql) {
        Ok(_) => panic!("expected error: {sql}"),
        Err(e) => e.to_string(),
    }
}

fn db_at(root: &std::path::Path) -> Qsdb {
    Qsdb::open(root, QsdbOptions::default()).unwrap()
}

/// 行集（`v1|v2|…`，NULL → `NULL`），已排序（ParScan 输出无序）。
fn rows(db: &Qsdb, sql: &str) -> Vec<String> {
    let mut out: Vec<String> = db
        .sql_collect(sql)
        .unwrap()
        .iter()
        .flat_map(|b| {
            (0..b.num_rows()).map(|i| {
                (0..b.num_columns())
                    .map(|c| {
                        let a = b.column(c);
                        if a.is_null(i) {
                            "NULL".to_string()
                        } else {
                            arrow::util::display::array_value_to_string(a.as_ref(), i).unwrap()
                        }
                    })
                    .collect::<Vec<_>>()
                    .join("|")
            })
        })
        .collect();
    out.sort();
    out
}

fn scalar(db: &Qsdb, sql: &str) -> String {
    let b = db.sql_collect(sql).unwrap();
    arrow::util::display::array_value_to_string(b[0].column(0).as_ref(), 0).unwrap()
}

/// 三列 parquet（tenant, amount, score），值 = base + row_idx。
fn write_parquet(path: &std::path::Path, base: i64, rows: i64) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("tenant", DataType::Utf8, true),
        Field::new("amount", DataType::Int64, true),
        Field::new("score", DataType::Float64, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(
                (0..rows)
                    .map(|i| format!("t{}", (base + i) % 3))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                (0..rows).map(|i| base + i).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                (0..rows)
                    .map(|i| (base + i) as f64 / 2.0)
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    let f = std::fs::File::create(path).unwrap();
    let mut w = ArrowWriter::try_new(f, schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

fn parquet_path(p: &std::path::Path) -> String {
    p.to_string_lossy().replace('\\', "/")
}

/// ① 从另一张表灌（含聚合结果灌入——"导入"最常见的形态之一）。
#[test]
fn from_another_table() {
    let root = tmp_root("from_table");
    let db = db_at(&root);
    db.sql("CREATE TABLE src (a BIGINT, b BIGINT)").unwrap();
    db.sql("CREATE TABLE dst (a BIGINT, b BIGINT)").unwrap();
    db.sql("INSERT INTO src VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();

    db.sql("INSERT INTO dst SELECT a, b FROM src").unwrap();
    assert_eq!(
        rows(&db, "SELECT a, b FROM dst"),
        vec!["1|10", "2|20", "3|30"]
    );

    // 再灌一次（追加语义：不清空既有数据）
    db.sql("INSERT INTO dst SELECT a, b FROM src WHERE a > 1")
        .unwrap();
    assert_eq!(scalar(&db, "SELECT count(*) FROM dst"), "5");

    // 聚合结果同样可灌（SELECT 链路全能力，不是"简化版 SELECT"）
    db.sql("CREATE TABLE agg (n BIGINT, s BIGINT)").unwrap();
    db.sql("INSERT INTO agg SELECT count(*), sum(b) FROM src")
        .unwrap();
    assert_eq!(rows(&db, "SELECT n, s FROM agg"), vec!["3|60"]);

    // CTE 也走同一条链路（物化逻辑与查询共用，见 sql::materialize_ctes）
    db.sql("INSERT INTO dst WITH x AS (SELECT a + 100 AS a, b FROM src) SELECT a, b FROM x")
        .unwrap();
    assert_eq!(scalar(&db, "SELECT count(*) FROM dst"), "8");
    assert_eq!(scalar(&db, "SELECT count(*) FROM dst WHERE a >= 101"), "3");

    std::fs::remove_dir_all(&root).ok();
}

/// ② 从 `read_parquet` 外表灌入——本次改动的验收点（此前这条路走不通）。
#[test]
fn from_read_parquet_external() {
    let root = tmp_root("from_parquet");
    let lake = root.join("lake");
    std::fs::create_dir_all(&lake).unwrap();
    write_parquet(&lake.join("a.parquet"), 0, 5);
    let db = db_at(&root);

    // 目标表类型与 parquet 一致（tenant 是表分区列，验证自动分区路由）
    db.sql("CREATE TABLE t (tenant VARCHAR, amount BIGINT, score DOUBLE) PARTITIONED BY (tenant)")
        .unwrap();
    let sql = format!(
        "INSERT INTO t SELECT tenant, amount, score FROM read_parquet('{}')",
        parquet_path(&lake.join("a.parquet"))
    );
    db.sql(&sql).unwrap();

    assert_eq!(scalar(&db, "SELECT count(*) FROM t"), "5");
    assert_eq!(scalar(&db, "SELECT sum(amount) FROM t"), "10"); // 0+1+2+3+4
    // 分区可见（tenant 三个值 → 三个分区目录）
    let parts = db.partition_stats("t").unwrap();
    assert_eq!(parts.len(), 3, "分区路由应当按 tenant 值分裂");

    // 外表谓词 + 类型 cast 一起走：score 由 DOUBLE 写入 DECIMAL 目标列
    db.sql("CREATE TABLE d (tenant VARCHAR, amount DECIMAL(18,4))")
        .unwrap();
    db.sql(&format!(
        "INSERT INTO d SELECT tenant, amount FROM read_parquet('{}') WHERE amount >= 3",
        parquet_path(&lake.join("a.parquet"))
    ))
    .unwrap();
    assert_eq!(rows(&db, "SELECT amount FROM d"), vec!["3.0000", "4.0000"]);

    std::fs::remove_dir_all(&root).ok();
}

/// ③ 列序：`INSERT INTO t (b, a) SELECT …` 是**位置语义**（标准 SQL），
/// 不是按名匹配——SQL 里 `(b,a)` 表示"源的第一个表达式进 b"。
#[test]
fn column_order_is_positional() {
    let root = tmp_root("order");
    let db = db_at(&root);
    db.sql("CREATE TABLE t (a BIGINT, b VARCHAR)").unwrap();
    db.sql("CREATE TABLE s (x VARCHAR, y BIGINT)").unwrap();
    db.sql("INSERT INTO s VALUES ('hello', 7)").unwrap();

    // 列清单 (b, a)：源的 x 进 b、y 进 a
    db.sql("INSERT INTO t (b, a) SELECT x, y FROM s").unwrap();
    assert_eq!(rows(&db, "SELECT a, b FROM t"), vec!["7|hello"]);

    // 部分列清单：未列出的列补 NULL（VALUES 路径不允许这种形态，见 mod.rs 注释）
    db.sql("CREATE TABLE t2 (a BIGINT, b VARCHAR, c BIGINT)")
        .unwrap();
    db.sql("INSERT INTO t2 (a, c) SELECT y, y + 1 FROM s")
        .unwrap();
    assert_eq!(rows(&db, "SELECT a, b, c FROM t2"), vec!["7|NULL|8"]);

    // 未知列 / 重复列 / 列数不匹配
    assert!(db.sql("INSERT INTO t (b, zzz) SELECT x, y FROM s").is_err());
    assert!(db.sql("INSERT INTO t (a, a) SELECT x, y FROM s").is_err());
    assert!(db.sql("INSERT INTO t (a) SELECT x, y FROM s").is_err());

    std::fs::remove_dir_all(&root).ok();
}

/// ④ 分区表：灌入后 count(*) 与分区可见性（append 自动分区路由 + 分区分裂）。
#[test]
fn partitioned_target() {
    let root = tmp_root("partition");
    let db = db_at(&root);
    db.sql("CREATE TABLE p (dt DATE, v BIGINT) PARTITIONED BY (dt)")
        .unwrap();
    db.sql("INSERT INTO p VALUES (DATE '2026-01-01', 1)")
        .unwrap();

    db.sql("INSERT INTO p SELECT dt, v * 10 FROM p").unwrap();
    assert_eq!(scalar(&db, "SELECT count(*) FROM p"), "2");
    assert_eq!(scalar(&db, "SELECT sum(v) FROM p"), "11");
    // 同分区（不是新建分区）
    assert_eq!(db.partition_stats("p").unwrap().len(), 1);

    // 跨分区灌入（多分区一次提交 → 写入端按值自动路由）
    db.sql("CREATE TABLE s (dt DATE, v BIGINT)").unwrap();
    db.sql("INSERT INTO s VALUES (DATE '2026-01-01', 1), (DATE '2026-02-02', 99)")
        .unwrap();
    db.sql("CREATE TABLE q (dt DATE, v BIGINT) PARTITIONED BY (dt)")
        .unwrap();
    db.sql("INSERT INTO q SELECT dt, v FROM s").unwrap();
    assert_eq!(scalar(&db, "SELECT count(*) FROM q"), "2");
    assert_eq!(db.partition_stats("q").unwrap().len(), 2);
    assert_eq!(
        rows(&db, "SELECT v FROM q WHERE dt = DATE '2026-02-02'"),
        vec!["99"]
    );

    // INSERT OVERWRITE：只替换谓词命中的那个分区，另一个分区原样保留
    db.sql("INSERT OVERWRITE q SELECT dt, v + 1000 FROM q WHERE dt = DATE '2026-01-01'")
        .unwrap();
    assert_eq!(scalar(&db, "SELECT count(*) FROM q"), "2");
    assert_eq!(scalar(&db, "SELECT sum(v) FROM q"), "1100"); // 1001 + 99
    assert_eq!(
        rows(&db, "SELECT v FROM q WHERE dt = DATE '2026-01-01'"),
        vec!["1001"]
    );

    std::fs::remove_dir_all(&root).ok();
}

/// ⑤ 错误路径：列数不匹配 / 类型不兼容（规则矩阵判定）/ 目标表不存在。
#[test]
fn type_and_width_errors() {
    let root = tmp_root("errors");
    let lake = root.join("lake");
    std::fs::create_dir_all(&lake).unwrap();
    write_parquet(&lake.join("a.parquet"), 0, 3);
    let db = db_at(&root);
    let ext = format!("read_parquet('{}')", parquet_path(&lake.join("a.parquet")));

    // 列数不匹配：列清单 1 列，SELECT 出 3 列
    db.sql("CREATE TABLE t (a BIGINT)").unwrap();
    let e = err(
        &db,
        &format!("INSERT INTO t (a) SELECT tenant, amount, score FROM {ext}"),
    );
    assert!(e.contains("columns"), "错误文案要点明列数不符: {e}");

    // 类型不兼容（Utf8 → BIGINT 是 Explicit，会走 cast 并在值上失败）
    let e = err(&db, &format!("INSERT INTO t SELECT tenant FROM {ext}"));
    assert!(e.contains("INSERT column 'a'"), "错误要点明是哪一列: {e}");

    // 规则矩阵判定为 Impossible（BOOLEAN → BIGINT）——在 cast 之前就拒绝
    db.sql("CREATE TABLE b (x BIGINT)").unwrap();
    let e = err(&db, &format!("INSERT INTO b SELECT score > 1 FROM {ext}"));
    assert!(
        e.contains("cannot cast BOOLEAN"),
        "Impossible 转换要在规则矩阵处拒绝: {e}"
    );

    // 未知列名
    let e = err(
        &db,
        &format!("INSERT INTO t (nope) SELECT amount FROM {ext}"),
    );
    assert!(e.contains("nope"), "错误要点明未知列: {e}");

    // 目标表不存在
    assert!(
        db.sql(&format!(
            "INSERT INTO no_such_table SELECT amount FROM {ext}"
        ))
        .is_err()
    );

    std::fs::remove_dir_all(&root).ok();
}

/// 流式写：多批（> INSERT_FLUSH_BATCHES 批）也要完整落盘、行数不丢。
#[test]
fn multi_batch_streaming_write() {
    let root = tmp_root("stream");
    let db = db_at(&root);
    db.sql("CREATE TABLE t (v BIGINT)").unwrap();
    db.sql("CREATE TABLE s (v BIGINT)").unwrap();
    // 制造多批：sqlpar 的 VALUES 上限内取一段足够大的行数（每批 8192 行 → 多批）
    let mut vals = String::new();
    for i in 0..20_000 {
        if i > 0 {
            vals.push(',');
        }
        vals.push_str(&format!("({i})"));
    }
    db.sql(&format!("INSERT INTO s VALUES {vals}")).unwrap();
    db.sql("INSERT INTO t SELECT v FROM s").unwrap();
    assert_eq!(scalar(&db, "SELECT count(*) FROM t"), "20000");
    assert_eq!(scalar(&db, "SELECT sum(v) FROM t"), "199990000");
    std::fs::remove_dir_all(&root).ok();
}
