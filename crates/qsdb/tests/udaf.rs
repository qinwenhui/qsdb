//! UDAF 聚合函数注册 e2e（M5-A4）。
//! 全局注册表 + 测试并行 → 函数名全测试唯一。

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use qsdb::types::LogicalType;
use qsdb::udaf::{self, EvalFn, RetTyFn};
use qsdb::{Qsdb, QsdbOptions};

fn setup(tag: &str) -> (Qsdb, std::path::PathBuf) {
    let root = std::env::temp_dir().join("qsdb_m5_udaf").join(format!(
        "{tag}_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    db.sql("CREATE TABLE s (grp STRING, v BIGINT) PARTITIONED BY (grp)")
        .unwrap();
    db.sql(
        "INSERT INTO s VALUES ('a', 10), ('a', 20), ('b', 5), ('b', 15), ('b', 25), ('c', NULL)",
    )
    .unwrap();
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

/// 注册"第二大值" UDAF：a=[10,20]→10，b=[5,15,25]→15，c=[NULL]→NULL。
/// 多测试共用 → OnceLock 只注册一次。
fn register_second_largest() {
    static ONCE: std::sync::OnceLock<()> = std::sync::OnceLock::new();
    ONCE.get_or_init(|| {
        let ret: RetTyFn = Arc::new(|args: &[LogicalType]| match args {
            [LogicalType::Int64] => Ok(LogicalType::Int64),
            _ => Err(qsdb::QsdbError::type_err(
                "udaf_e2e_second(bigint) expected",
            )),
        });
        let ev: EvalFn = Arc::new(|args: &[ArrayRef]| {
            let a = args[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| qsdb::QsdbError::type_err("bigint arg expected"))?;
            let mut vals: Vec<i64> = a.iter().flatten().collect();
            vals.sort_unstable();
            let out = if vals.len() < 2 {
                None
            } else {
                Some(vals[vals.len() - 2])
            };
            Ok(Arc::new(Int64Array::from(vec![out])) as ArrayRef)
        });
        udaf::register("udaf_e2e_second", ret, ev).unwrap();
    });
}

#[test]
fn udaf_group_and_global() {
    register_second_largest();
    let (db, root) = setup("basic");

    // 分组 UDAF
    let r = rows(
        &db,
        "SELECT grp, udaf_e2e_second(v) AS sl FROM s GROUP BY grp ORDER BY grp",
    );
    assert_eq!(r, vec!["a|10", "b|15", "c|NULL"]);

    // 全局 UDAF（无 GROUP BY）：非空值 [10,20,5,15,25] → 第二大 20
    let r = rows(&db, "SELECT udaf_e2e_second(v) AS sl FROM s");
    assert_eq!(r, vec!["20"]);

    // WHERE 先过滤再分组：a=[10,20]→10，b=[15,25]→15，c 无非空值被滤掉
    let r = rows(
        &db,
        "SELECT grp, udaf_e2e_second(v) AS sl FROM s WHERE v > 5 GROUP BY grp ORDER BY grp",
    );
    assert_eq!(r, vec!["a|10", "b|15"]);
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn udaf_mixed_with_builtins() {
    register_second_largest();
    let (db, root) = setup("mixed");

    // 混合查询：内置聚合在 UDAF 路径按组重算
    let r = rows(
        &db,
        "SELECT grp, udaf_e2e_second(v) AS sl, sum(v) AS s, count(*) AS c,
                min(v) AS mn, max(v) AS mx, avg(v) AS av
         FROM s GROUP BY grp ORDER BY grp",
    );
    assert_eq!(
        r,
        vec![
            "a|10|30|2|10|20|15.0",
            "b|15|45|3|5|25|15.0",
            "c|NULL|NULL|1|NULL|NULL|NULL",
        ]
    );
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn udaf_register_conflicts() {
    // 内置聚合冲突 / 内置标量冲突 / UDF 冲突 / 重复 / 非法名
    let ret: RetTyFn = Arc::new(|_| Ok(LogicalType::Int64));
    let ev: EvalFn = Arc::new(|args: &[ArrayRef]| Ok(Arc::clone(&args[0])));
    assert!(udaf::register("sum", Arc::clone(&ret), Arc::clone(&ev)).is_err());
    assert!(udaf::register("upper", Arc::clone(&ret), Arc::clone(&ev)).is_err());
    assert!(udaf::register("bad-name!", Arc::clone(&ret), Arc::clone(&ev)).is_err());

    udaf::register("udaf_conflict_probe", ret, ev).unwrap();
    let ret2: RetTyFn = Arc::new(|_| Ok(LogicalType::Int64));
    let ev2: EvalFn = Arc::new(|args: &[ArrayRef]| Ok(Arc::clone(&args[0])));
    assert!(udaf::register("udaf_conflict_probe", ret2, ev2).is_err());
    assert!(udaf::unregister("udaf_conflict_probe"));

    // UDF ↔ UDAF 交叉冲突
    let sret: qsdb::udf::RetTyFn = Arc::new(|_| Ok(LogicalType::Int64));
    let sev: qsdb::udf::EvalFn = Arc::new(|args: &[ArrayRef]| Ok(Arc::clone(&args[0])));
    let ret3: RetTyFn = Arc::new(|_| Ok(LogicalType::Int64));
    let ev3: EvalFn = Arc::new(|args: &[ArrayRef]| Ok(Arc::clone(&args[0])));
    udaf::register("udaf_cross_name", ret3, ev3).unwrap();
    assert!(qsdb::udf::register("udaf_cross_name", sret, sev).is_err());
    assert!(udaf::unregister("udaf_cross_name"));
}
