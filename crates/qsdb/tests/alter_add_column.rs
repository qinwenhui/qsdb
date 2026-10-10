//! ALTER TABLE ADD COLUMN e2e（M5-B2）：只加不删 + 旧 segment 缺列补 NULL 读 + 宽松写。

use arrow::array::{ArrayRef, Int64Array};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

use qsdb::{Qsdb, QsdbOptions};

fn setup() -> (Qsdb, std::path::PathBuf) {
    let root = std::env::temp_dir().join("qsdb_m5_alter").join(format!(
        "{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    (db, root)
}

fn rows(db: &Qsdb, sql: &str) -> Vec<String> {
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
fn alter_add_column_read_and_write() {
    let (db, root) = setup();
    db.sql("CREATE TABLE t (k BIGINT, v BIGINT) PARTITIONED BY (k)")
        .unwrap();
    db.sql("INSERT INTO t VALUES (1, 10), (2, 20)").unwrap();

    // ALTER ADD COLUMN
    db.sql("ALTER TABLE t ADD COLUMN w BIGINT").unwrap();

    // 旧行 w 读为 NULL；新行写 w
    db.sql("INSERT INTO t VALUES (3, 30, 33)").unwrap();

    let r = rows(&db, "SELECT k, v, w FROM t ORDER BY k");
    assert_eq!(r, vec!["1|10|NULL", "2|20|NULL", "3|30|33"]);

    // 新列可查/可过滤（NULL 比较语义：w > 0 只中 33）
    let r = rows(&db, "SELECT v FROM t WHERE w > 0");
    assert_eq!(r, vec!["30"]);

    // 聚合：count(w) = 1，sum(w) = 33
    let r = rows(&db, "SELECT count(w), sum(w), count(*) FROM t");
    assert_eq!(r, vec!["1|33|3"]);

    // 旧列两值 INSERT 现在被拒（全列批约定）
    assert!(db.sql("INSERT INTO t VALUES (4, 40)").is_err());

    // 重复列名 / NOT NULL / DROP / 分区列名 → 拒绝
    assert!(db.sql("ALTER TABLE t ADD COLUMN w BIGINT").is_err());
    assert!(
        db.sql("ALTER TABLE t ADD COLUMN x BIGINT NOT NULL")
            .is_err()
    );
    assert!(db.sql("ALTER TABLE t DROP COLUMN w").is_err());
    assert!(
        db.sql("ALTER TABLE t ADD COLUMN k2 BIGINT, ADD COLUMN k3 BIGINT")
            .is_err()
    );

    // DESCRIBE 显示新列
    let r = rows(&db, "DESCRIBE t");
    assert!(r.iter().any(|x| x.starts_with("w|")), "got: {r:?}");
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn alter_loose_write_and_append_to_old_partition() {
    let (db, root) = setup();
    db.sql("CREATE TABLE t (k BIGINT, v BIGINT) PARTITIONED BY (k)")
        .unwrap();
    db.sql("INSERT INTO t VALUES (1, 10)").unwrap();
    db.sql("ALTER TABLE t ADD COLUMN w BIGINT").unwrap();

    // 宽松写：旧 schema（缺尾列）的 Arrow 批 → 自动补 NULL
    let schema = arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("k", arrow::datatypes::DataType::Int64, true),
        arrow::datatypes::Field::new("v", arrow::datatypes::DataType::Int64, true),
    ]);
    let k: ArrayRef = Arc::new(Int64Array::from(vec![Some(1)]));
    let v: ArrayRef = Arc::new(Int64Array::from(vec![Some(11)]));
    let batch = RecordBatch::try_new(Arc::new(schema), vec![k, v]).unwrap();
    db.append("t", &[batch]).unwrap();

    // 同一分区：旧 segment（无 w）+ 新 segment（有 w）混读
    let r = rows(&db, "SELECT v, w FROM t WHERE k = 1 ORDER BY v");
    assert_eq!(r, vec!["10|NULL", "11|NULL"]);

    // append 到旧分区（manifest 指纹为旧前缀）→ 允许并升格
    let schema2 = arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("k", arrow::datatypes::DataType::Int64, true),
        arrow::datatypes::Field::new("v", arrow::datatypes::DataType::Int64, true),
        arrow::datatypes::Field::new("w", arrow::datatypes::DataType::Int64, true),
    ]);
    let k2: ArrayRef = Arc::new(Int64Array::from(vec![Some(1)]));
    let v2: ArrayRef = Arc::new(Int64Array::from(vec![Some(12)]));
    let w2: ArrayRef = Arc::new(Int64Array::from(vec![Some(99)]));
    let batch2 = RecordBatch::try_new(Arc::new(schema2), vec![k2, v2, w2]).unwrap();
    db.append("t", &[batch2]).unwrap();

    let r = rows(&db, "SELECT v, w FROM t WHERE k = 1 ORDER BY v");
    assert_eq!(r, vec!["10|NULL", "11|NULL", "12|99"]);
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn alter_compaction_migrates_schema() {
    let (db, root) = setup();
    db.sql("CREATE TABLE t (k BIGINT, v BIGINT) PARTITIONED BY (k)")
        .unwrap();
    db.sql("INSERT INTO t VALUES (1, 10)").unwrap();
    db.sql("ALTER TABLE t ADD COLUMN w BIGINT").unwrap();
    db.sql("INSERT INTO t VALUES (1, 11, 111)").unwrap();

    // 同分区 2 个 segment → compaction 合并（旧缺列补 NULL → 统一新 schema）
    // compaction 入口（手动触发）
    let _ = db.compact_table("t");
    let r = rows(&db, "SELECT v, w FROM t WHERE k = 1 ORDER BY v");
    assert_eq!(r, vec!["10|NULL", "11|111"]);
    std::fs::remove_dir_all(&root).ok();
}
