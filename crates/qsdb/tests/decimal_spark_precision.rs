//! **DECIMAL 运算结果的类型必须与 Spark 逐格一致** —— SQL 层的回归。
//!
//! ## 为什么这组测试非有不可
//!
//! 精度口径错**不报错**：形状对、数也算得出来，只是**位数不对**。只有拿另一个引擎
//! 逐格比才看得见。而金额场景下"少一位小数"就是**错值**。
//!
//! 表里每一行的期望值都是 **`.14` 上真 Spark 3.5.2 的 `typeof(...)` 实测输出**，
//! 不是从文档抄的：
//!
//! ```sql
//! spark-sql> SELECT typeof(CAST(1 AS DECIMAL(18,4)) * 4);
//! decimal(20,4)
//! ```
//!
//! ⚠️ **改这张表之前先拿真 Spark 复核**（`.14`：`/opt/module/spark-3.5.2/bin/spark-sql`）。
//!
//! ## 覆盖三件容易漏的
//!
//! 1. **整数常量按它的位数参与运算**（`4` ⇒ `DECIMAL(1,0)`），不是按 i64 的 19 位
//!    —— 定大了会提前撞 38 封顶、把 scale 截掉；
//! 2. **小数常量的位数按用户写的**（`4.0` ⇒ `DECIMAL(2,1)`，**不剪末尾的 0**）；
//! 3. `+ - * / %` 的封顶规则（乘除会超 38）。

use qsdb::{Qsdb, QsdbOptions};

fn setup(tag: &str) -> (Qsdb, std::path::PathBuf) {
    let root = std::env::temp_dir().join("qsdb_dec_spark").join(format!(
        "{tag}_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    db.sql("CREATE TABLE e (d DECIMAL(18,4))").unwrap();
    db.sql("INSERT INTO e VALUES (20.2500)").unwrap();
    (db, root)
}

fn ty(db: &Qsdb, sql: &str) -> String {
    let bs = db.sql_collect(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    bs.first()
        .expect("应有结果")
        .schema()
        .field(0)
        .data_type()
        .to_string()
}

/// ★ **整数 / 小数常量参与 DECIMAL 算术** —— 四格都是真 Spark 的 `typeof` 实测值。
#[test]
fn 常量位数与_spark_一致() {
    let (db, root) = setup("const");
    let cases = [
        // 整数常量 `4` ⇒ DECIMAL(1,0)，不是 (19,0)
        ("SELECT d + 4 FROM e", "Decimal128(19, 4)"),
        ("SELECT d * 4 FROM e", "Decimal128(20, 4)"),
        ("SELECT d / 4 FROM e", "Decimal128(20, 6)"),
        // 小数常量 `4.0` ⇒ DECIMAL(2,1)（**末尾的 0 不许剪**）
        ("SELECT d / 4.0 FROM e", "Decimal128(22, 7)"),
    ];
    for (sql, want) in cases {
        assert_eq!(ty(&db, sql), want, "{sql} 的结果类型与 Spark 不一致");
    }
    drop(db);
    let _ = std::fs::remove_dir_all(root);
}

/// 字面量的**值**不因位数变化而改变（`4.0` 仍是 4）。
#[test]
fn 常量位数变了值不变() {
    let (db, root) = setup("value");
    let q = |sql: &str| {
        let bs = db.sql_collect(sql).unwrap();
        let b = bs.first().unwrap();
        arrow::util::display::array_value_to_string(b.column(0).as_ref(), 0).unwrap()
    };
    // 值不变，**渲染按结果的 scale**（`(20,6)` ⇒ 6 位小数、`(22,7)` ⇒ 7 位）——
    // 尾随零是 scale 的体现，不是"多了没用的零"
    assert_eq!(q("SELECT d / 4 FROM e"), "5.062500"); // (20,6)
    assert_eq!(q("SELECT d / 4.0 FROM e"), "5.0625000"); // (22,7)
    assert_eq!(q("SELECT d * 4 FROM e"), "81.0000"); // (20,4)
    assert_eq!(q("SELECT d + 4 FROM e"), "24.2500"); // (19,4)
    drop(db);
    let _ = std::fs::remove_dir_all(root);
}
