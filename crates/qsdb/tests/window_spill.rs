//! 窗口算子的**分区溢出**（P3）验收：物化输入要不到额度时，按 `PARTITION BY`
//! 键分区落盘 → 逐分区取回计算产出（而不是把查询打死），且结果与内存路径
//! **逐值相同**。
//!
//! 正确性依据：窗口函数**只在同一个 `PARTITION BY` 分区内**计算，故按分区键
//! 路由后每个分区可独立算——分区内的排序与窗口逻辑与内存路径完全相同。
//!
//! 明确边界（**不覆盖**）：无 `PARTITION BY` 的查询（`OVER (ORDER BY ...)`）
//! ——整个输入就是一个分区，无键可分区，保持现状报错（见
//! `unpartitioned_window_still_errors_clearly`）。设计口径见
//! `内存预算与溢出设计稿` §四 4.2。

use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use qsdb::error::{QsdbError, Result};
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
        .join("qsdb_windowspill_test")
        .join(format!("{tag}_{:x}_{}", std::process::id(), _uniq_seq()));
    std::fs::create_dir_all(&d).unwrap();
    d
}

/// 造数：`v` **全局唯一** → 同一分区内 `ORDER BY v` 不并列，行序唯一确定
/// （溢出路径与内存路径的取行序差异因此不会污染断言）；
/// `g = i % 2000` 造真分区（每组 10 行）；`h = i % 5` 供多列 PARTITION BY。
fn seeded(tag: &str, rows: i64) -> (Qsdb, std::path::PathBuf) {
    let root = tmp_root(tag);
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    load(&db, rows);
    (db, root)
}

/// 写 `rows` 行进 `t`（逐批 append：既造多 segment，也让输入分批流过）。
fn load(db: &Qsdb, rows: i64) {
    db.sql("CREATE TABLE t (g BIGINT, h BIGINT, v BIGINT)")
        .unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("g", DataType::Int64, true),
        Field::new("h", DataType::Int64, true),
        Field::new("v", DataType::Int64, true),
    ]));
    let per = 500i64;
    let mut done = 0i64;
    while done < rows {
        let n = per.min(rows - done);
        let b = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(
                    (0..n).map(|i| (done + i) % 2000).collect::<Vec<i64>>(),
                )),
                Arc::new(Int64Array::from(
                    (0..n).map(|i| (done + i) % 5).collect::<Vec<i64>>(),
                )),
                Arc::new(Int64Array::from(
                    (0..n).map(|i| done + i).collect::<Vec<i64>>(),
                )),
            ],
        )
        .unwrap();
        db.append("t", &[b]).unwrap();
        done += n;
    }
}

/// 极小配额上下文：**不挂全局池**，只有每查询配额生效（与 P1/P2 用例同款）。
fn quota_ctx(limit: Option<u64>) -> Arc<QueryContext> {
    match limit {
        Some(l) => QueryContext::new("ws", None, Some(l)),
        None => QueryContext::new("ws-free", None, None),
    }
}

/// 取结果**批**（不合并、不排序——批数用于分辨溢出路径，见 `assert_spilled`）。
fn batches_of(db: &Qsdb, sql: &str, ctx: &Arc<QueryContext>) -> Result<Vec<RecordBatch>> {
    match db.sql_with_context(sql, ctx) {
        Ok(qsdb::sql::SqlResult::Query { batches, .. }) => Ok(batches),
        Ok(_) => panic!("expected query: {sql}"),
        Err(e) => Err(e),
    }
}

/// 批 → 行（每行各列拼串），**排序后**（两个路径的输出行序不同：内存路径按
/// (PARTITION BY, ORDER BY) 全局有序；溢出路径逐分区产出，分区之间是 hash 序）。
fn rows_from(batches: &[RecordBatch]) -> Vec<String> {
    let mut v: Vec<String> = batches
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
        .collect();
    v.sort();
    v
}

fn rows_of(db: &Qsdb, sql: &str, ctx: &Arc<QueryContext>) -> Vec<String> {
    let batches = batches_of(db, sql, ctx)
        .unwrap_or_else(|e| panic!("查询失败（limit={:?}）: {e}\nSQL: {sql}", ctx.memory_limit));
    rows_from(&batches)
}

/// 溢出路径的**非空洞**判据：溢出时逐分区各产出一个批（≥ 2 个批）。
///
/// 为什么钉这条：P1/P2 的"全局预算"用例经 `sql_collect` 执行，而
/// `sql_collect` → `execute_sql` → `QueryContext::benign()`（不挂池、不设配额）
/// → **全局预算根本没生效**，名字承诺的验证等于没做（已核实）。窗口这边把
/// 判据钉进测试：SQL 因此**不带外层 ORDER BY**（外层 Sort 会把结果合并成一个
/// 批，批数就看不出来了）。
fn assert_spilled(batches: &[RecordBatch], what: &str) {
    assert!(
        batches.len() > 1,
        "{what}: 应当走溢出路径逐分区产出（每分区一个批），实际只有 {} 个批——\
         说明这条用例没真正触发溢出，断言是空洞的",
        batches.len()
    );
}

/// 极小每查询配额（必然走分区溢出）下，窗口结果与内存路径**逐值相同**。
#[test]
fn window_results_identical_with_and_without_spill() {
    let (db, root) = seeded("identical", 20_000);
    // 外层 ORDER BY 让行序确定（比较也仍走排序，双保险）
    for sql in [
        // 排名类（`rank()` 刻意不在此列：它在**内存路径**上就是错的——
        // `compute_window_fn` 的 Rank 分支拿全局行号 `i + 1` 当名次，第二个
        //  分区起名次就跳（实测 1,2,3 → 1,5,6）。既有缺陷，本次不动它。）
        "SELECT g, v, row_number() OVER (PARTITION BY g ORDER BY v) AS rn \
         FROM t ORDER BY g, v",
        "SELECT g, v, dense_rank() OVER (PARTITION BY g ORDER BY v) AS dr \
         FROM t ORDER BY g, v",
        // 累积和（RANGE 默认帧：无界前驱 → 当前行）
        "SELECT g, v, sum(v) OVER (PARTITION BY g ORDER BY v) AS cs \
         FROM t ORDER BY g, v",
        // 整个分区帧（无 ORDER BY → 段 = 整分区）
        "SELECT g, v, count(v) OVER (PARTITION BY g) AS c \
         FROM t ORDER BY g, v",
        // 偏移类
        "SELECT g, v, lag(v) OVER (PARTITION BY g ORDER BY v) AS lg, \
                lead(v) OVER (PARTITION BY g ORDER BY v) AS ld \
         FROM t ORDER BY g, v",
        // **多列 PARTITION BY**
        "SELECT g, h, v, sum(v) OVER (PARTITION BY g, h ORDER BY v) AS cs \
         FROM t ORDER BY g, h, v",
    ] {
        let free = rows_of(&db, sql, &quota_ctx(None));
        assert!(!free.is_empty(), "不限内存应有结果: {sql}");
        // 极小配额 → 必然走溢出（48 万字节的输入塞不进 64KB）
        let spilled = rows_of(&db, sql, &quota_ctx(Some(64 * 1024)));
        assert_eq!(
            free, spilled,
            "溢出路径结果必须与内存路径逐值相同\nSQL: {sql}"
        );
    }
    let _ = std::fs::remove_dir_all(&root);
}

/// 溢出路径确实被走到（防"空洞用例"）：比较同样成立，且输出批数 > 1。
#[test]
fn spill_path_is_really_taken() {
    let (db, root) = seeded("taken", 20_000);
    // 不带外层 ORDER BY：批数即窗口节点自己产出的批数
    let sql = "SELECT g, v, row_number() OVER (PARTITION BY g ORDER BY v) AS rn FROM t";
    let free = batches_of(&db, sql, &quota_ctx(None)).unwrap();
    assert_eq!(free.len(), 1, "内存路径一次物化 → 一个批");
    let spilled = batches_of(&db, sql, &quota_ctx(Some(64 * 1024))).unwrap();
    assert_spilled(&spilled, "极小配额");
    assert_eq!(
        rows_of(&db, sql, &quota_ctx(None)),
        rows_of(&db, sql, &quota_ctx(Some(64 * 1024))),
        "结果仍须逐值相同"
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// 全局预算（而非每查询配额）触发时同样落盘完成。
///
/// **必须用 `db.new_query_context()`**（它挂了全局池）；`db.sql_collect` 走的是
/// `execute_sql` → `QueryContext::benign()`，不挂池、不设配额，全局预算在那条路
/// 上根本不生效。
///
/// 触发点：预算比"一次物化"宽、比"物化 + concat 瞬时双份"紧 →
/// `execute_all` 在 concat 的瞬时双份上要不到额度，转分区溢出；输入（约 72 万
/// 字节）仍装得进 1280KB 的缓冲预算 → 逐分区算完。
#[test]
fn global_budget_pressure_also_spills() {
    let root = tmp_root("global");
    let db = Qsdb::open(
        &root,
        QsdbOptions {
            memory_budget_bytes: Some(1280 * 1024),
            scan_cache_bytes: 0,
            ..Default::default()
        },
    )
    .unwrap();
    load(&db, 30_000);
    // 三列都引用 → 扫描不窄化投影，输入 ≈ 72 万字节
    let sql = "SELECT g, h, v, sum(v) OVER (PARTITION BY g, h ORDER BY v) AS cs FROM t";
    let pooled = db.new_query_context(None); // 挂池：全局预算生效
    let spilled = batches_of(&db, sql, &pooled)
        .unwrap_or_else(|e| panic!("预算压力下应溢出完成，而不是失败: {e}"));
    assert_spilled(&spilled, "全局预算");
    assert_eq!(
        spilled.iter().map(|b| b.num_rows()).sum::<usize>(),
        30_000,
        "行数守恒"
    );
    // 结果与"不限内存"（不挂池的上下文）逐值相同
    assert_eq!(
        rows_of(&db, sql, &quota_ctx(None)),
        rows_from(&spilled),
        "全局预算溢出结果必须与不限内存逐值相同"
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// **边界**（明确不覆盖）：无 `PARTITION BY`（整个输入就是一个分区）无法分区
/// → 保持现状：要不到额度**如实例外**，不硬做。
#[test]
fn unpartitioned_window_still_errors_clearly() {
    let (db, root) = seeded("nopart", 20_000);
    for sql in [
        "SELECT v, row_number() OVER (ORDER BY v) AS rn FROM t",
        "SELECT v, sum(v) OVER () AS s FROM t",
    ] {
        // 不限内存：正常出结果（错误只来自额度，不是语法/功能不支持）
        assert!(
            !rows_of(&db, sql, &quota_ctx(None)).is_empty(),
            "不限内存应有结果: {sql}"
        );
        let err = batches_of(&db, sql, &quota_ctx(Some(64 * 1024)))
            .expect_err("无 PARTITION BY 的查询无法分区，应如实报错而不是完成");
        assert!(
            matches!(
                err,
                QsdbError::QueryMemoryLimit(_) | QsdbError::MemoryPressure(_)
            ),
            "应为明确的内存类错误，实际: {err:?}\nSQL: {sql}"
        );
    }
    let _ = std::fs::remove_dir_all(&root);
}
