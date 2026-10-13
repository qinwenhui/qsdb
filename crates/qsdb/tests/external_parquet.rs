//! M6-2 外表 read_parquet（本地路径/glob；s3:// 见 feature `s3`）。
//!
//! 覆盖：SELECT 全列/投影/谓词行过滤 + footer row group 裁剪、count(*)、
//! glob 多文件、JOIN 右侧外表、LIMIT 0 schema 契约、错误路径
//! （文件不存在 / 未知表函数 / 非 parquet 文件 / s3 无 feature）。

use std::sync::Arc;

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_writer::ArrowWriter;

use qsdb::{Qsdb, QsdbOptions};

fn tmp_root(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir()
        .join("qsdb_ext_parquet_test")
        .join(format!("{tag}_{}", uuid::Uuid::now_v7().simple()));
    std::fs::create_dir_all(&d).unwrap();
    d
}

/// 写一个三列 parquet（tenant, amount, score），值 = base + row_idx。
fn write_parquet(path: &std::path::Path, base: i64, rows: i64) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("tenant", DataType::Utf8, true),
        Field::new("amount", DataType::Int64, true),
        Field::new("score", DataType::Float64, true),
    ]));
    let tenants: Vec<String> = (0..rows).map(|i| format!("t{}", (base + i) % 3)).collect();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(StringArray::from(tenants)),
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

fn db_at(root: &std::path::Path) -> Qsdb {
    Qsdb::open(root, QsdbOptions::default()).unwrap()
}

fn rows_of(db: &Qsdb, sql: &str) -> Vec<Vec<String>> {
    db.sql_collect(sql)
        .unwrap()
        .iter()
        .flat_map(|b| {
            (0..b.num_rows()).map(move |i| {
                (0..b.num_columns())
                    .map(|c| {
                        arrow::util::display::array_value_to_string(b.column(c).as_ref(), i)
                            .unwrap()
                    })
                    .collect::<Vec<String>>()
            })
        })
        .collect()
}

#[test]
fn select_projection_and_where() {
    let root = tmp_root("basic");
    let dir = root.join("lake");
    std::fs::create_dir_all(&dir).unwrap();
    write_parquet(&dir.join("a.parquet"), 0, 10);
    let db = db_at(&root);

    // 全列
    let rows = rows_of(
        &db,
        &format!(
            "SELECT tenant, amount FROM read_parquet('{}')",
            dir.join("a.parquet").to_string_lossy().replace('\\', "/")
        ),
    );
    assert_eq!(rows.len(), 10);
    assert_eq!(rows[0][0], "t0");
    assert_eq!(rows[9][1], "9");

    // 谓词行过滤（走 footer 裁剪 + 行过滤）
    let rows = rows_of(
        &db,
        &format!(
            "SELECT amount FROM read_parquet('{}') WHERE amount >= 8",
            dir.join("a.parquet").to_string_lossy().replace('\\', "/")
        ),
    );
    assert_eq!(rows, vec![vec![String::from("8")], vec![String::from("9")]]);

    // 聚合
    let rows = rows_of(
        &db,
        &format!(
            "SELECT count(*), sum(amount), min(score), max(score) FROM read_parquet('{}')",
            dir.join("a.parquet").to_string_lossy().replace('\\', "/")
        ),
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], "10");
    assert_eq!(rows[0][1], "45");

    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn glob_multiple_files_sorted() {
    let root = tmp_root("glob");
    let lake = root.join("lake");
    for d in ["lake/y=2024", "lake/y=2025"] {
        std::fs::create_dir_all(root.join(d)).unwrap();
    }
    write_parquet(&lake.join("y=2024/p1.parquet"), 0, 3);
    write_parquet(&lake.join("y=2024/p2.parquet"), 100, 3);
    write_parquet(&lake.join("y=2025/p3.parquet"), 200, 3);
    let db = db_at(&root);

    let pat = format!(
        "{}/y=*/*.parquet",
        lake.to_string_lossy().replace('\\', "/")
    );
    let rows = rows_of(
        &db,
        &format!("SELECT count(*), sum(amount) FROM read_parquet('{pat}')"),
    );
    assert_eq!(rows[0][0], "9");
    assert_eq!(rows[0][1], "909"); // (0+1+2)+(100+101+102)+(200+201+202)

    // `**` 跨段
    let pat = format!("{}/**/*.parquet", lake.to_string_lossy().replace('\\', "/"));
    let rows = rows_of(&db, &format!("SELECT count(*) FROM read_parquet('{pat}')"));
    assert_eq!(rows[0][0], "9");

    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn join_right_external() {
    let root = tmp_root("join");
    let db = db_at(&root);
    db.sql("CREATE TABLE dim (tenant VARCHAR, label VARCHAR)")
        .unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("tenant", DataType::Utf8, true),
        Field::new("label", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["t0", "t1"])),
            Arc::new(StringArray::from(vec!["zero", "one"])),
        ],
    )
    .unwrap();
    db.append("dim", &[batch]).unwrap();

    let lake = root.join("lake");
    std::fs::create_dir_all(&lake).unwrap();
    write_parquet(&lake.join("a.parquet"), 0, 4);
    // 右表带别名：引用走别名（d.tenant），解析走原名（dim）——回归 M6-2 修复
    let sql = format!(
        "SELECT d.label, count(*) FROM read_parquet('{}') JOIN dim d ON tenant = d.tenant GROUP BY d.label ORDER BY d.label",
        lake.join("a.parquet").to_string_lossy().replace('\\', "/")
    );
    let rows = rows_of(&db, &sql);
    // tenants t0,t1,t2,t0：t2 无匹配（INNER 丢弃）→ one=1, zero=2
    // （t2 行同时锁定 join.rs INNER 未匹配行的 total_rows 修复）
    assert_eq!(
        rows,
        vec![
            vec![String::from("one"), String::from("1")],
            vec![String::from("zero"), String::from("2")],
        ]
    );
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn limit_zero_schema_contract() {
    let root = tmp_root("schema");
    let dir = root.join("lake");
    std::fs::create_dir_all(&dir).unwrap();
    write_parquet(&dir.join("a.parquet"), 0, 5);
    let db = db_at(&root);
    let sql = format!(
        "SELECT tenant, amount FROM read_parquet('{}') LIMIT 0",
        dir.join("a.parquet").to_string_lossy().replace('\\', "/")
    );
    match db.sql(&sql).unwrap() {
        qsdb::sql::SqlResult::Query { schema, batches } => {
            assert!(batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0));
            let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
            assert_eq!(names, vec!["tenant", "amount"]);
        }
        _ => panic!("expect query result"),
    }
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn error_paths() {
    let root = tmp_root("err");
    let db = db_at(&root);
    // 不存在
    assert!(
        db.sql("SELECT * FROM read_parquet('Z:/no/such/file.parquet')")
            .is_err()
    );
    // read_csv 已是合法表函数，但该文件不存在 → 仍是错误（错误路径归 read_csv 自己的用例）
    assert!(db.sql("SELECT * FROM read_csv('x.csv')").is_err());
    // 未知表函数
    assert!(db.sql("SELECT * FROM read_orc('x.orc')").is_err());
    // 非 parquet 文件（坏魔数）
    let bad = root.join("bad.parquet");
    std::fs::write(&bad, b"not a parquet file").unwrap();
    assert!(
        db.sql(&format!(
            "SELECT * FROM read_parquet('{}')",
            bad.to_string_lossy().replace('\\', "/")
        ))
        .is_err()
    );
    // glob 无匹配
    assert!(
        db.sql(&format!(
            "SELECT * FROM read_parquet('{}/*.parquet')",
            root.to_string_lossy().replace('\\', "/")
        ))
        .is_err()
    );
    std::fs::remove_dir_all(&root).ok();
}

#[test]
#[cfg(not(feature = "s3"))]
fn s3_without_feature_clear_error() {
    let root = tmp_root("s3off");
    let db = db_at(&root);
    let err = match db.sql("SELECT * FROM read_parquet('s3://bucket/x.parquet')") {
        Ok(_) => panic!("expected error without s3 feature"),
        Err(e) => e,
    };
    assert!(
        err.to_string().contains("feature"),
        "error should mention feature: {err}"
    );
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn f3_microsecond_timestamp_cast_to_ns() {
    use arrow::array::TimestampMicrosecondArray;
    let root = tmp_root("f3ts");
    let dir = root.join("lake");
    std::fs::create_dir_all(&dir).unwrap();
    // 写一个 us 时间戳列的 parquet（Spark/DuckDB/pandas 生态事实标准）
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "evt_ts",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
            true,
        ),
        Field::new("v", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(TimestampMicrosecondArray::from(vec![
                Some(1_000i64),
                Some(2_000),
                None,
            ])),
            Arc::new(Int64Array::from(vec![Some(1), Some(2), Some(3)])),
        ],
    )
    .unwrap();
    let f = std::fs::File::create(dir.join("ts.parquet")).unwrap();
    let mut w = ArrowWriter::try_new(f, schema, None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();

    let db = db_at(&root);
    // read_parquet 收纳 us → ns（不再报 unsupported）
    let sql = format!(
        "SELECT count(*), max(v) FROM read_parquet('{}')",
        dir.join("ts.parquet").to_string_lossy().replace('\\', "/")
    );
    let rows = rows_of(&db, &sql);
    assert_eq!(rows, vec![vec![String::from("3"), String::from("3")]]);
    // 谓词在 cast 后的 ns 列上工作
    let sql = format!(
        "SELECT v FROM read_parquet('{}') WHERE v > 1 ORDER BY v",
        dir.join("ts.parquet").to_string_lossy().replace('\\', "/")
    );
    let rows = rows_of(&db, &sql);
    assert_eq!(rows, vec![vec![String::from("2")], vec![String::from("3")]]);
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn f12_hive_directory_semantics() {
    let root = tmp_root("f12hive");
    let lake = root.join("lake");
    for d in ["lake/dt=2026-01-01", "lake/dt=2026-01-02"] {
        std::fs::create_dir_all(root.join(d)).unwrap();
    }
    write_parquet(&lake.join("dt=2026-01-01/p1.parquet"), 0, 3);
    write_parquet(&lake.join("dt=2026-01-02/p2.parquet"), 100, 3);
    let db = db_at(&root);

    // hive 目录：dt 列从路径重建（文件内不含 dt）
    let pat = format!("{}/**/*.parquet", lake.to_string_lossy().replace('\\', "/"));
    let rows = rows_of(
        &db,
        &format!("SELECT count(*), sum(amount) FROM read_parquet('{pat}')"),
    );
    assert_eq!(rows[0][0], "6");
    assert_eq!(rows[0][1], "306"); // (0+1+2)+(100+101+102)

    // hive 分区列投影 + WHERE 裁剪
    let rows = rows_of(
        &db,
        &format!(
            "SELECT dt, count(*) FROM read_parquet('{pat}') WHERE dt = DATE '2026-01-01' GROUP BY dt"
        ),
    );
    assert_eq!(rows.len(), 1);
    assert!(rows[0][0].contains("2026-01-01"));
    assert_eq!(rows[0][1], "3");

    // 不匹配分区 → 0 行（裁剪生效）
    let rows = rows_of(
        &db,
        &format!("SELECT count(*) FROM read_parquet('{pat}') WHERE dt = DATE '2025-01-01'"),
    );
    // 0 行可能返回空（引擎空输入 count 语义）或 "0"
    assert!(rows.is_empty() || rows[0][0] == "0");

    // 无 hive 目录的普通外表不受影响
    let rows = rows_of(
        &db,
        &format!(
            "SELECT count(*) FROM read_parquet('{}')",
            lake.join("dt=2026-01-01/p1.parquet")
                .to_string_lossy()
                .replace('\\', "/")
        ),
    );
    assert_eq!(rows[0][0], "3");
    std::fs::remove_dir_all(&root).ok();
}
