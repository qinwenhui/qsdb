//! UDF 标量函数注册 e2e（M5-5）。
//! 全局注册表 + 测试并行 → 函数名全测试唯一。

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use qsdb::types::LogicalType;
use qsdb::udf;
use qsdb::udf::{EvalFn, RetTyFn};
use qsdb::{Qsdb, QsdbOptions};

/// 测试目录唯一序号：pid 挡跨进程碰撞，序号挡同进程内并发（Windows 时钟
/// 粒度 ~100ns，纳秒后缀在同 tick 内会重复——CI 上真炸过，见 constraints）。
fn _uniq_seq() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    SEQ.fetch_add(1, Ordering::Relaxed)
}

fn setup(tag: &str) -> (Qsdb, std::path::PathBuf) {
    let root = std::env::temp_dir().join("qsdb_m5_udf").join(format!(
        "{tag}_{}",
        format!("{:x}_{}", std::process::id(), _uniq_seq())
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
fn udf_scalar_e2e() {
    // int → int：v + 1
    udf::register(
        "udf_e2e_incr",
        Arc::new(|args: &[LogicalType]| match args {
            [LogicalType::Int64] => Ok(LogicalType::Int64),
            _ => Err(qsdb::QsdbError::type_err("udf_e2e_incr(bigint) expected")),
        }),
        Arc::new(|args: &[ArrayRef]| {
            let a = args[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| qsdb::QsdbError::type_err("bigint arg expected"))?;
            Ok(Arc::new(a.iter().map(|v| v.map(|x| x + 1)).collect::<Int64Array>()) as ArrayRef)
        }),
    )
    .unwrap();

    let (db, root) = setup("e2e");
    db.sql("CREATE TABLE t (k BIGINT, v BIGINT) PARTITIONED BY (k)")
        .unwrap();
    db.sql("INSERT INTO t VALUES (1, 10), (1, 20), (2, 30)")
        .unwrap();

    // 投影
    let r = rows(&db, "SELECT udf_e2e_incr(v) AS w FROM t");
    assert_eq!(r, vec!["11", "21", "31"]);

    // WHERE + ORDER BY（大写函数名 → 小写归一）
    let r = rows(&db, "SELECT v FROM t WHERE UDF_E2E_INCR(v) > 25 ORDER BY v");
    assert_eq!(r, vec!["30"]);

    // 类型检查在 binder：字符串入参 → 拒绝
    let err = match db.sql("SELECT udf_e2e_incr('abc') FROM t") {
        Err(e) => e.to_string(),
        Ok(_) => panic!("expected binder type error"),
    };
    assert!(err.contains("udf_e2e_incr"));

    // 未注册函数
    assert!(db.sql("SELECT no_such_fn(v) FROM t").is_err());

    // 注销 → 再查变未知函数
    assert!(udf::unregister("udf_e2e_incr"));
    assert!(db.sql("SELECT udf_e2e_incr(v) FROM t").is_err());
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn udf_string_and_null() {
    // string → string：加后缀；NULL 传播由 UDF 负责
    udf::register(
        "udf_str_mark",
        Arc::new(|args: &[LogicalType]| match args {
            [t] if t.is_string() => Ok(t.clone()),
            _ => Err(qsdb::QsdbError::type_err("udf_str_mark(string) expected")),
        }),
        Arc::new(|args: &[ArrayRef]| {
            let s = args[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| qsdb::QsdbError::type_err("string arg expected"))?;
            Ok(Arc::new(
                s.iter()
                    .map(|v| v.map(|x| format!("{x}!")))
                    .collect::<StringArray>(),
            ) as ArrayRef)
        }),
    )
    .unwrap();

    let (db, root) = setup("str");
    db.sql("CREATE TABLE s (k BIGINT, name STRING) PARTITIONED BY (k)")
        .unwrap();
    db.sql("INSERT INTO s VALUES (1, 'a'), (1, 'b'), (2, NULL)")
        .unwrap();

    let r = rows(&db, "SELECT udf_str_mark(name) AS m FROM s");
    assert_eq!(r, vec!["NULL", "a!", "b!"]);
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn udf_register_conflicts() {
    // 内置冲突 / 重复注册 / 非法名（ret_ty 用宽松签名避免构造失败干扰断言）
    let ret: RetTyFn = Arc::new(|_: &[LogicalType]| Ok(LogicalType::Int64));
    let ev: EvalFn = Arc::new(|args: &[ArrayRef]| Ok(Arc::clone(&args[0])));
    assert!(udf::register("upper", Arc::clone(&ret), Arc::clone(&ev)).is_err());
    assert!(udf::register("bad-name!", Arc::clone(&ret), Arc::clone(&ev)).is_err());

    udf::register("udf_conflict_probe", ret, ev).unwrap();
    let ret2: RetTyFn = Arc::new(|_: &[LogicalType]| Ok(LogicalType::Int64));
    let ev2: EvalFn = Arc::new(|args: &[ArrayRef]| Ok(Arc::clone(&args[0])));
    assert!(udf::register("udf_conflict_probe", ret2, ev2).is_err());
    assert!(udf::unregister("udf_conflict_probe"));
}
