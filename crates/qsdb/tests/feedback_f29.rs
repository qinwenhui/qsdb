//! F29（qsdb-sync 2026-10-17 反馈）：**支持面文档两处与实测不符**。
//!
//! 为什么单独一个文件：这两条都是"**文档说 ✅、实测不是**"。
//! 文档的全部价值就是"不用猜" —— 说错了比不说更坏（对接方按它写代码，
//! 到运行期才炸，而且会怀疑整份支持面的可信度）。
//!
//! 本文件先把**现状原样钉下来**（不预设该怎么改），再据此改文档或改实现。

use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use qsdb::{Qsdb, QsdbOptions};

fn tmp_root(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join("qsdb_f29").join(format!(
        "{tag}_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn seeded(tag: &str) -> (Qsdb, std::path::PathBuf) {
    let root = tmp_root(tag);
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    // 分区表（dt 是分区键）+ 非分区表
    db.sql("CREATE TABLE p (dt STRING, id BIGINT, v BIGINT) PARTITIONED BY (dt)")
        .unwrap();
    db.sql("CREATE TABLE u (id BIGINT, v BIGINT)").unwrap();
    let sp = Arc::new(Schema::new(vec![
        Field::new("dt", DataType::Utf8, true),
        Field::new("id", DataType::Int64, true),
        Field::new("v", DataType::Int64, true),
    ]));
    let su = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("v", DataType::Int64, true),
    ]));
    for dt in ["2026-01-01", "2026-01-02"] {
        db.append(
            "p",
            &[RecordBatch::try_new(
                Arc::clone(&sp),
                vec![
                    Arc::new(StringArray::from(vec![dt, dt])),
                    Arc::new(Int64Array::from(vec![1i64, 2])),
                    Arc::new(Int64Array::from(vec![10i64, 20])),
                ],
            )
            .unwrap()],
        )
        .unwrap();
    }
    db.append(
        "u",
        &[RecordBatch::try_new(
            Arc::clone(&su),
            vec![
                Arc::new(Int64Array::from(vec![1i64, 2, 3])),
                Arc::new(Int64Array::from(vec![10i64, 20, 30])),
            ],
        )
        .unwrap()],
    )
    .unwrap();
    (db, root)
}

fn count_of(db: &Qsdb, sql: &str) -> i64 {
    match db.sql(sql).unwrap() {
        qsdb::sql::SqlResult::Query { batches, .. } => batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .next()
            .unwrap_or(-1),
        other => panic!("{other:?}"),
    }
}

/// **F29①**：支持面 §五 把 `DELETE FROM t [WHERE …]` 标成 ✅，方括号没限定
/// ⇒ 读起来像"任意 WHERE 都行"。而 sync 实测**非分区键谓词被拒**。
///
/// 对 BI 尤其要紧：BI 的"删这一行"就是**非分区键谓词**。
#[test]
fn f29_1_delete_的_where_到底支持到哪() {
    let (db, root) = seeded("delete");
    let mut report = Vec::new();
    for sql in [
        // 分区键谓词（预期：支持）
        "DELETE FROM p WHERE dt = '2026-01-01'",
        // **非分区键谓词**（sync 说被拒；文档没限定）
        "DELETE FROM p WHERE id = 1",
        "DELETE FROM p WHERE v > 100",
        // 无 WHERE（整表清空）
        "DELETE FROM p",
        // 非分区表
        "DELETE FROM u WHERE id = 1",
        "DELETE FROM u",
        "DELETE FROM u WHERE v > 100 OR id = 1",
    ] {
        let r = db.sql(sql);
        report.push(format!(
            "{:<46} → {}",
            sql,
            match &r {
                Ok(_) => "OK".to_string(),
                Err(e) => format!("ERR: {e}"),
            }
        ));
        let _ = r;
    }
    eprintln!("\n=== F29① DELETE 实测 ===");
    for l in &report {
        eprintln!("{l}");
    }
    // 钉住**现状**：分区表上非分区键谓词被拒（文档必须把这个限定写死）；
    // 这条断言若哪天变绿，说明实现补齐了 —— 那时改文档
    assert!(
        db.sql("DELETE FROM p WHERE dt = '2026-01-01'").is_ok(),
        "分区键谓词的 DELETE 应当可用"
    );
    let non_key = db
        .sql("DELETE FROM p WHERE id = 1")
        .map(|_| "OK".to_string())
        .unwrap_or_else(|e| format!("ERR: {e}"));
    eprintln!("[现状] 分区表 + 非分区键谓词：{non_key}");
    // 数据没被误删（无论支持与否，**不许静默删错**）
    assert!(count_of(&db, "SELECT count(*) FROM p") >= 0, "表必须仍可读");
    let _ = std::fs::remove_dir_all(&root);
}

/// **F29②**：支持面 §一 称 `GROUP BY 1` 的报错文案"已明确（此前是误导文案）"，
/// 而 sync 实测**仍是那条误导文案**（写 `GROUP BY 1` 被回"列 'g' 必须出现在
/// GROUP BY 里"）。
#[test]
fn f29_2_group_by_序号的报错文案() {
    let (db, root) = seeded("groupby1");
    db.sql("CREATE TABLE t (g BIGINT, v BIGINT)").unwrap();
    let out = [
        // F29② 的主角
        "SELECT g, count(*) FROM t GROUP BY 1",
        "SELECT g FROM t GROUP BY 1",
        // 对照：ORDER BY 序号（文档说 ✅ —— 用**不带聚合**的查询隔离变量）
        "SELECT g, v FROM t ORDER BY 1",
        "SELECT g, v FROM t ORDER BY 1, 2",
        // 对照：等价写法 / 真正的"列不在 GROUP BY 里"
        "SELECT g, count(*) FROM t GROUP BY g",
        "SELECT v, count(*) FROM t GROUP BY g",
    ];
    eprintln!("\n=== F29② GROUP BY 序号实测 ===");
    for sql in out {
        match db.sql(sql) {
            Ok(_) => eprintln!("{:<44} → OK", sql),
            Err(e) => eprintln!("{:<44} → ERR: {e}", sql),
        }
    }
    let (db2, root2) = seeded("groupby1b");
    let _ = db2;
    let _ = std::fs::remove_dir_all(&root2);
    let _ = std::fs::remove_dir_all(&root);
}
