//! 聚合溢出（P1）验收：内存要不到额度时，聚合**落盘完成**而不是把查询打死，
//! 且结果与内存路径**逐值相同**。
//!
//! 覆盖两条执行路径（它们的溢出接管方式不同）：
//! - **分片并行**（普通表扫描）：每分片 partial 聚合 → 溢出交回调用方，
//!   分区合并时把「各分片表里属于该分区的组」与「该分区落盘的行」合到一起；
//! - **单节点**（派生表/CTE 等不可分片源）：执行器自己逐分区产出。
//!
//! 去重（DISTINCT）复用同一套分区机制（组 = 全部列、无聚合）。

use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use qsdb::query::QueryContext;
use qsdb::{Qsdb, QsdbOptions};

/// 测试目录唯一序号：pid 挡跨进程碰撞，序号挡同进程内并发（Windows 时钟
/// 粒度 ~100ns，纳秒后缀在同 tick 内会重复——CI 上真炸过，见 constraints）。
fn _uniq_seq() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    SEQ.fetch_add(1, Ordering::Relaxed)
}

fn tmp_root(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir()
        .join("qsdb_aggspill_test")
        .join(format!(
            "{tag}_{}_{}",
            std::process::id(),
            format!("{:x}_{}", std::process::id(), _uniq_seq())
        ));
    std::fs::create_dir_all(&d).unwrap();
    d
}

/// 高基数表：`g` 取 2000 个不同值（保证组表真的会涨），`v` 参与聚合。
fn seeded(tag: &str, rows: i64) -> (Qsdb, std::path::PathBuf) {
    let root = tmp_root(tag);
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    db.sql("CREATE TABLE t (g BIGINT, v BIGINT)").unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("g", DataType::Int64, true),
        Field::new("v", DataType::Int64, true),
    ]));
    // 逐批 append：既造多 segment（分片路径生效），也让输入分批流过
    let per = 500;
    let mut done = 0i64;
    while done < rows {
        let n = per.min(rows - done);
        let g: Vec<i64> = (0..n).map(|i| (done + i) % 2000).collect();
        let v: Vec<i64> = (0..n).map(|i| done + i).collect();
        let b = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(g)), Arc::new(Int64Array::from(v))],
        )
        .unwrap();
        db.append("t", &[b]).unwrap();
        done += n;
    }
    (db, root)
}

/// 取结果（每行各列拼串，按行序）。
fn rows_of(db: &Qsdb, sql: &str, limit: Option<u64>) -> Vec<String> {
    let ctx = match limit {
        Some(l) => QueryContext::new("spill-t", None, Some(l)),
        None => QueryContext::new("free-t", None, None),
    };
    let out = db
        .sql_with_context(sql, &ctx)
        .unwrap_or_else(|e| panic!("查询失败（limit={limit:?}）: {e}\nSQL: {sql}"));
    let batches = match out {
        qsdb::sql::SqlResult::Query { batches, .. } => batches,
        _ => panic!("expected query"),
    };
    batches
        .iter()
        .flat_map(|b| {
            (0..b.num_rows())
                .map(|i| {
                    (0..b.num_columns())
                        .map(|c| {
                            arrow::util::display::array_value_to_string(b.column(c).as_ref(), i)
                                .unwrap_or_default()
                        })
                        .collect::<Vec<_>>()
                        .join("|")
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

/// 内存路径 vs 溢出路径结果**逐值相同**（SQL 里带 ORDER BY，行序确定）。
#[test]
fn group_by_results_identical_with_and_without_spill() {
    let (db, root) = seeded("identical", 20_000);
    for sql in [
        // 分片并行路径（普通表扫描）
        "SELECT g, count(*) AS c, sum(v) AS s, min(v) AS mn, max(v) AS mx \
         FROM t GROUP BY g ORDER BY g",
        // 单节点路径（派生表让源不可分片）
        "SELECT g, count(*) AS c, sum(v) AS s \
         FROM (SELECT g, v FROM t) GROUP BY g ORDER BY g",
        // 多列分组（表达式分组当前 binder 不支持，用两列原生键）
        "SELECT g, v, count(*) AS c FROM t GROUP BY g, v ORDER BY g, v",
    ] {
        let free = rows_of(&db, sql, None);
        assert!(!free.is_empty(), "不限内存应有结果: {sql}");
        // 极小配额 → 必然走溢出（组表不可能塞进 64KB）
        let spilled = rows_of(&db, sql, Some(64 * 1024));
        assert_eq!(
            free, spilled,
            "溢出路径结果必须与内存路径逐值相同\nSQL: {sql}"
        );
    }
    let _ = std::fs::remove_dir_all(&root);
}

/// DISTINCT 同样要能在额度不足时落盘完成（走同一套分区机制）。
#[test]
fn distinct_results_identical_with_and_without_spill() {
    let (db, root) = seeded("distinct", 20_000);
    for sql in [
        "SELECT DISTINCT g FROM t ORDER BY g",
        "SELECT DISTINCT g, v FROM t ORDER BY g, v",
    ] {
        let free = rows_of(&db, sql, None);
        let spilled = rows_of(&db, sql, Some(64 * 1024));
        assert_eq!(free, spilled, "DISTINCT 溢出结果不一致\nSQL: {sql}");
    }
    let _ = std::fs::remove_dir_all(&root);
}

/// 多分片 + 溢出：分片表里**已经聚合**的组与**落盘的行**必须归到同一分区，
/// 否则同一组会被算两次/漏算（这是本路径最危险的错误）。
#[test]
fn multi_shard_spill_is_not_double_counted() {
    let (db, root) = seeded("shards", 30_000);
    // 先拿不限内存的基准（每组计数 = 15，因为 30000/2000）
    let free = rows_of(
        &db,
        "SELECT g, count(*) AS c, sum(v) AS s FROM t GROUP BY g ORDER BY g",
        None,
    );
    let spilled = rows_of(
        &db,
        "SELECT g, count(*) AS c, sum(v) AS s FROM t GROUP BY g ORDER BY g",
        Some(128 * 1024),
    );
    assert_eq!(free.len(), 2000, "应有 2000 组");
    assert_eq!(free, spilled, "多分片溢出路径结果不一致（重复计数？）");
    let _ = std::fs::remove_dir_all(&root);
}

/// 全局预算（而非每查询配额）触发溢出时同样成立。
#[test]
fn global_budget_pressure_also_spills() {
    let root = tmp_root("global");
    let db = Qsdb::open(
        &root,
        QsdbOptions {
            memory_budget_bytes: Some(128 * 1024),
            scan_cache_bytes: 0,
            ..Default::default()
        },
    )
    .unwrap();
    db.sql("CREATE TABLE t (g BIGINT, v BIGINT)").unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("g", DataType::Int64, true),
        Field::new("v", DataType::Int64, true),
    ]));
    for chunk in 0..20 {
        let g: Vec<i64> = (0..500).map(|i| (chunk * 500 + i) % 1500).collect();
        let v: Vec<i64> = (0..500).map(|i| chunk * 500 + i).collect();
        db.append(
            "t",
            &[RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from(g)), Arc::new(Int64Array::from(v))],
            )
            .unwrap()],
        )
        .unwrap();
    }
    // 预算 128KB 远小于组表需求 → 必须走溢出完成。
    //
    // **必须显式用句柄上下文**：`sql_collect` 曾经走 `QueryContext::benign()`
    // （无池无配额）→ 这个用例是**空的**（并行工作流插桩发现：全程走内存路径、
    // 预算根本没生效）。`sql_collect` 已修为池感知，这里仍显式传 ctx——
    // 用例本身要测的是"预算生效"，别依赖便捷 API 的实现细节。
    let ctx = db.new_query_context(None);
    let out = match db
        .sql_with_context("SELECT g, count(*) AS c FROM t GROUP BY g ORDER BY g", &ctx)
        .unwrap_or_else(|e| panic!("预算压力下应溢出完成，而不是失败: {e}"))
    {
        qsdb::sql::SqlResult::Query { batches, .. } => batches,
        _ => panic!("expected query"),
    };
    let n: usize = out.iter().map(|b| b.num_rows()).sum();
    assert_eq!(n, 1500, "15 组… 应为 1500 组");
    let _ = std::fs::remove_dir_all(&root);
}
