//! 约束系统 e2e（M5-B）：NOT NULL 强制 / CHECK（含跨列）/ UNIQUE 拒绝。

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

use qsdb::{Qsdb, QsdbOptions};

/// 测试目录唯一序号：pid 挡跨进程碰撞，序号挡同进程内并发（Windows 时钟
/// 粒度 ~100ns，纳秒后缀在同 tick 内会重复——CI 上真炸过，见 constraints）。
fn _uniq_seq() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    SEQ.fetch_add(1, Ordering::Relaxed)
}

fn setup() -> (Qsdb, std::path::PathBuf) {
    let root = std::env::temp_dir().join("qsdb_m5_cons").join(format!(
        "{:x}_{}",
        std::process::id(),
        _uniq_seq()
    ));
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    (db, root)
}

#[test]
fn not_null_enforced() {
    let (db, root) = setup();
    db.sql("CREATE TABLE t (k BIGINT, v BIGINT NOT NULL) PARTITIONED BY (k)")
        .unwrap();

    // 合法写入
    db.sql("INSERT INTO t VALUES (1, 100)").unwrap();

    // SQL VALUES 带 NULL → 拒绝
    let err = match db.sql("INSERT INTO t VALUES (1, NULL)") {
        Err(e) => e.to_string(),
        Ok(_) => panic!("NOT NULL should reject"),
    };
    assert!(err.contains("NOT NULL"), "got: {err}");

    // append API 批含 NULL → 拒绝（v 列 null）
    let schema = arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("k", arrow::datatypes::DataType::Int64, true),
        arrow::datatypes::Field::new("v", arrow::datatypes::DataType::Int64, true),
    ]);
    let k: ArrayRef = Arc::new(Int64Array::from(vec![Some(2)]));
    let v: ArrayRef = Arc::new(Int64Array::from(vec![None::<i64>]));
    let batch = RecordBatch::try_new(Arc::new(schema), vec![k, v]).unwrap();
    assert!(db.append("t", &[batch]).is_err());

    // 已提交数据可读
    let r = db.sql_collect("SELECT count(*) FROM t").unwrap();
    assert_eq!(r[0].num_rows(), 1);
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn check_constraint() {
    let (db, root) = setup();
    // 列级 CHECK（跨列引用：b > a）
    db.sql(
        "CREATE TABLE t (k BIGINT, a BIGINT, b BIGINT CHECK (b > a), s STRING CHECK (length(s) > 0))
         PARTITIONED BY (k)",
    )
    .unwrap();

    // 合法
    db.sql("INSERT INTO t VALUES (1, 10, 20, 'ok')").unwrap();

    // CHECK 违反（b <= a）
    let err = match db.sql("INSERT INTO t VALUES (1, 10, 5, 'ok')") {
        Err(e) => e.to_string(),
        Ok(_) => panic!("CHECK should reject"),
    };
    assert!(err.contains("CHECK"), "got: {err}");

    // CHECK 违反（length(s) = 0）
    assert!(db.sql("INSERT INTO t VALUES (1, 1, 2, '')").is_err());

    // CHECK NULL 语义：b 为 NULL → unknown → 通过（SQL 标准）
    db.sql("INSERT INTO t VALUES (2, 10, NULL, 'x')").unwrap();

    let r = db.sql_collect("SELECT count(*) FROM t").unwrap();
    assert_eq!(r[0].num_rows(), 1);
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn check_ddl_validation() {
    let (db, root) = setup();
    // CHECK 引用分区列 → DDL 期拒绝（分区列不在写入批内）
    assert!(
        db.sql("CREATE TABLE t1 (k BIGINT, v BIGINT CHECK (v > k)) PARTITIONED BY (k)")
            .is_err()
    );
    // CHECK 非布尔 → DDL 期拒绝
    assert!(
        db.sql("CREATE TABLE t2 (k BIGINT, v BIGINT CHECK (v + 1)) PARTITIONED BY (k)")
            .is_err()
    );
    // CHECK 引用不存在列 → DDL 期拒绝
    assert!(
        db.sql("CREATE TABLE t3 (k BIGINT, v BIGINT CHECK (no_such > 0)) PARTITIONED BY (k)")
            .is_err()
    );
    // 语法错误
    assert!(
        db.sql("CREATE TABLE t4 (k BIGINT, v BIGINT CHECK (v >>> 0)) PARTITIONED BY (k)")
            .is_err()
    );
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn unique_and_pk_rejected() {
    let (db, root) = setup();
    assert!(
        db.sql("CREATE TABLE t1 (k BIGINT, v BIGINT UNIQUE) PARTITIONED BY (k)")
            .is_err()
    );
    assert!(
        db.sql("CREATE TABLE t2 (k BIGINT, v BIGINT PRIMARY KEY) PARTITIONED BY (k)")
            .is_err()
    );
    // DEFAULT 显式拒绝（后置）
    assert!(
        db.sql("CREATE TABLE t3 (k BIGINT, v BIGINT DEFAULT 0) PARTITIONED BY (k)")
            .is_err()
    );
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn constraint_via_write_buffer_path() {
    // 约束检查在 write::append_partition 收口——HTTP/WriteBuffer 路径同样生效。
    // 这里用 overwrite_partition 验证第二个入口。
    let (db, root) = setup();
    db.sql("CREATE TABLE t (k BIGINT, v BIGINT CHECK (v >= 0)) PARTITIONED BY (k)")
        .unwrap();
    let schema = arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("k", arrow::datatypes::DataType::Int64, true),
        arrow::datatypes::Field::new("v", arrow::datatypes::DataType::Int64, true),
    ]);
    let k: ArrayRef = Arc::new(Int64Array::from(vec![Some(1)]));
    let v: ArrayRef = Arc::new(Int64Array::from(vec![Some(-5)]));
    let batch = RecordBatch::try_new(Arc::new(schema), vec![k, v]).unwrap();
    let err = match db.overwrite_partition("t", &[("k", qsdb::Literal::Int64(1))], &[batch]) {
        Err(e) => e.to_string(),
        Ok(_) => panic!("CHECK should reject on overwrite path"),
    };
    assert!(err.contains("CHECK"), "got: {err}");

    let _ = StringArray::from(vec!["placeholder"]); // 保持 import 使用
    std::fs::remove_dir_all(&root).ok();
}
