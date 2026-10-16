//! 全局内存预算 + 准入（P0）验收测试。
//!
//! 三条不变量（`docs/内存预算与溢出设计.md` §一）：
//! - **C1** 执行内存 ≤ 全局预算——超出的查询被**拒绝**而不是 OOM；
//! - **C2** 缓存可被执行内存**挤兑到 0**（纯缓存丢了大不了重算）；
//! - **C3** 溢出只改「完成 vs 拒绝」的边界，**不改结果**。
//!
//! 外加两条工程约束：**拒绝要能分辨**（配额错 400 vs 全库吃紧 503）、
//! **panic 也要归还额度**（RAII 的意义就在 unwind 下也成立）。

use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use qsdb::query::QueryContext;
use qsdb::{Qsdb, QsdbError, QsdbOptions};

fn tmp_root(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir()
        .join("qsdb_membudget_test")
        .join(format!(
            "{tag}_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
    std::fs::create_dir_all(&d).unwrap();
    d
}

/// 取出 Err（`SqlResult` 没实现 Debug，用不了 `unwrap_err`）。
fn err_of(r: qsdb::Result<qsdb::sql::SqlResult>) -> QsdbError {
    match r {
        Ok(_) => panic!("期望报错，实际成功"),
        Err(e) => e,
    }
}

/// 单列 v BIGINT 的批（n 行，值 0..n）。
fn batch(n: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
    let vals: Vec<i64> = (0..n as i64).collect();
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vals))]).unwrap()
}

/// 建表 + 灌 `rows` 行（分区表：每行一个分区，制造多文件扫描）。
fn seeded_db(tag: &str, rows: usize) -> (Arc<Qsdb>, std::path::PathBuf) {
    let root = tmp_root(tag);
    let db = Arc::new(Qsdb::open(&root, QsdbOptions::default()).unwrap());
    db.sql("CREATE TABLE t (k BIGINT, v BIGINT) PARTITIONED BY (k)")
        .unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("v", DataType::Int64, true),
    ]));
    for i in 0..rows {
        let b = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![i as i64 % 4])),
                Arc::new(Int64Array::from(vec![i as i64])),
            ],
        )
        .unwrap();
        db.append("t", &[b]).unwrap();
    }
    (db, root)
}

/// 全局预算 1MB 的库（每查询配额不限，只考全局账本）。
fn budgeted_db(tag: &str, budget: u64, cache: usize) -> (Arc<Qsdb>, std::path::PathBuf) {
    let root = tmp_root(tag);
    let db = Arc::new(
        Qsdb::open(
            &root,
            QsdbOptions {
                scan_cache_bytes: cache,
                memory_budget_bytes: Some(budget),
                ..Default::default()
            },
        )
        .unwrap(),
    );
    db.sql("CREATE TABLE t (k BIGINT, v BIGINT)").unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("v", DataType::Int64, true),
    ]));
    for i in 0..2000i64 {
        let b = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![i % 64])),
                Arc::new(Int64Array::from(vec![i])),
            ],
        )
        .unwrap();
        db.append("t", &[b]).unwrap();
    }
    (db, root)
}

// ---------------------------------------------------------------------------
// C1：全局预算 → 拒绝而不是 OOM
// ---------------------------------------------------------------------------

#[test]
fn global_budget_rejects_over_budget_query() {
    let (db, root) = seeded_db("c1", 64);
    // 预算 1MB、每查询配额不限：排序要物化全部输入 → 要不到额度即被拒
    let pool = Arc::new(qsdb::memory::MemoryPool::new(1024 * 1024));
    let ctx = QueryContext::new_with_pool("b1", None, None, Some(Arc::clone(&pool)));

    // 先占满预算（模拟并发中的另一个查询）
    let hog = ctx.try_reserve(1024 * 1024).unwrap();
    // 排序**有溢出能力**：要不到额度会落盘完成——这是对的（溢出优先于拒绝），
    // 故此处不假设必失败，只要求"不 OOM、不 panic"
    let _ = db.sql_with_context("SELECT v FROM t ORDER BY v", &ctx);
    drop(hog);

    // 无溢出能力的算子（window）在同样条件下**必须被拒**而不是无界增长
    let hog2 = ctx.try_reserve(1024 * 1024).unwrap();
    let err = err_of(db.sql_with_context(
        "SELECT v, row_number() OVER (ORDER BY v) AS rn FROM t",
        &ctx,
    ));
    assert!(
        matches!(err, QsdbError::MemoryPressure(_)),
        "预算耗尽时无溢出能力的算子应报 MemoryPressure，实际: {err}"
    );
    drop(hog2);
    // 释放后立刻可跑（闸门不是熔断）
    assert!(db.sql_collect("SELECT v FROM t ORDER BY v").is_ok());
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn budget_error_is_distinguishable_from_quota_error() {
    let (db, root) = seeded_db("c1b", 64);
    let pool = Arc::new(qsdb::memory::MemoryPool::new(1024 * 1024));

    // ⚠️ 这条查询**故意多读一列**（`k` 不参与任何运算）：列裁剪会裁掉没人读的列，
    //    而"只读 `v`"时这条查询的最小配额正好是 1KB —— 恰好卡在下面那个配额上、
    //    不再报错，这条测试就测不到配额机制了（2026-10-16 列裁剪穿透窗口后当场撞上）。
    //    多要一列 ⇒ 最小配额 2KB，与配额 1KB 之间留出 2× 余量，且与裁剪行为解耦。
    let q = "SELECT k, v, row_number() OVER (ORDER BY v) AS rn FROM t";

    // ① 自己的配额太小 → QueryMemoryLimit（400，改查询才有用）
    let small = QueryContext::new_with_pool("q-small", None, Some(1024), Some(Arc::clone(&pool)));
    let err = err_of(db.sql_with_context(q, &small));
    assert!(
        matches!(err, QsdbError::QueryMemoryLimit(_)),
        "配额超限应报 QueryMemoryLimit，实际: {err}"
    );

    // ② 全库预算被占满（配额不限）→ MemoryPressure（503，可退避重试）
    let any = QueryContext::new_with_pool("q-any", None, None, Some(Arc::clone(&pool)));
    let _hog = any.try_reserve(1024 * 1024).unwrap();
    let err = err_of(db.sql_with_context(q, &any));
    assert!(
        matches!(err, QsdbError::MemoryPressure(_)),
        "全局吃紧应报 MemoryPressure，实际: {err}"
    );
    std::fs::remove_dir_all(&root).ok();
}

// ---------------------------------------------------------------------------
// C2：缓存可被挤兑（且被挤兑的额度确实让给了执行内存）
// ---------------------------------------------------------------------------

#[test]
fn cache_is_evicted_under_execution_pressure() {
    let (db, root) = budgeted_db("c2", 8 * 1024 * 1024, 4 * 1024 * 1024);
    // 先反复跑同一查询把缓存填起来
    for _ in 0..3 {
        let _ = db.sql_collect("SELECT k, v FROM t");
    }
    let (_, cache_used, _, _) = db.cache_stats();
    assert!(cache_used > 0, "缓存应已填充: {cache_used}");

    let pool = db.memory_pool();
    // 造一个吃满预算的执行侧预留 → 触发挤兑
    let ctx = QueryContext::new_with_pool("squeeze", None, None, Some(Arc::clone(&pool)));
    let _hog = ctx.try_reserve(8 * 1024 * 1024).unwrap();
    assert_eq!(pool.exec_used(), 8 * 1024 * 1024);
    let (_, cache_after, _, _) = db.cache_stats();
    assert_eq!(cache_after, 0, "压力下缓存应被挤兑到 0（纯缓存优先让位）");
    assert!(db.cache_evicted_bytes() > 0, "应有挤兑量记录");

    // 挤兑后执行内存仍可正常释放、查询照常（只读，不依赖缓存）
    drop(_hog);
    assert_eq!(pool.exec_used(), 0);
    assert!(db.sql_collect("SELECT count(*) FROM t").is_ok());
    std::fs::remove_dir_all(&root).ok();
}

// ---------------------------------------------------------------------------
// 账洞：此前完全不记账的算子（Distinct / CTE / 集合运算）现在必须受约束
// ---------------------------------------------------------------------------

#[test]
fn previously_unaccounted_operators_now_respect_limit() {
    let (db, root) = seeded_db("holes", 256);
    let tiny = || QueryContext::new("holes-q", None, Some(1024));

    // ① **有溢出能力**的（P1 起）：额度不够就落盘完成——而不是报错，
    //    更不是从前那样无界增长。
    //    （UNION 的去重走 DistinctExec，故它也进了这一组。）
    for sql in [
        "SELECT DISTINCT v FROM t",              // DistinctExec（P1 分区溢出）
        "SELECT v, count(*) FROM t GROUP BY v",  // HashAgg（P1 分区溢出）
        "SELECT v FROM t UNION SELECT v FROM t", // SetOp(Union) + DistinctExec 溢出
    ] {
        let ctx = tiny();
        assert!(
            db.sql_with_context(sql, &ctx).is_ok(),
            "有溢出能力的算子在极小配额下应落盘完成: {sql}"
        );
    }

    // ② **还没有溢出能力**的：必须**报错**——此前的病灶是"无记账 = 无界增长"，
    //    现在至少要有账并且能拒绝（它们的溢出留待 P1.5/P2）。
    for sql in [
        "WITH c AS (SELECT v FROM t) SELECT count(*) FROM c", // CTE 物化（P1.5）
        "SELECT v FROM t INTERSECT SELECT v FROM t",          // SetOp 右表集合（P1.5）
        "SELECT v, row_number() OVER (ORDER BY v) AS rn FROM t", // Window（P3）
    ] {
        let ctx = tiny();
        let err = err_of(db.sql_with_context(sql, &ctx));
        assert!(
            matches!(err, QsdbError::QueryMemoryLimit(_)),
            "无溢出能力的算子应给出配额错（而不是无界增长）: {sql} → {err}"
        );
    }

    // 不限配额时全部照常成功（保证没把功能改坏）
    for sql in [
        "SELECT DISTINCT v FROM t",
        "SELECT v, count(*) FROM t GROUP BY v",
        "WITH c AS (SELECT v FROM t) SELECT count(*) FROM c",
        "SELECT v FROM t UNION SELECT v FROM t",
        "SELECT v, row_number() OVER (ORDER BY v) AS rn FROM t",
    ] {
        assert!(db.sql_collect(sql).is_ok(), "不限内存时应成功: {sql}");
    }
    std::fs::remove_dir_all(&root).ok();
}

/// CTE 物化批不再跨查询驻留（此前只在"下一条 WITH 查询开始时"才被替换）。
#[test]
fn cte_batches_do_not_outlive_the_query() {
    let (db, root) = seeded_db("cte-hold", 64);
    let before = db.memory_pool().exec_used();
    assert_eq!(before, 0);
    let ctx = QueryContext::new("cte-q", None, None);
    let _ = db
        .sql_with_context("WITH c AS (SELECT v FROM t) SELECT count(*) FROM c", &ctx)
        .unwrap();
    // 查询结束后：ctx 的记账与全局账本都应归零
    assert_eq!(ctx.memory_used(), 0, "查询结束不得留下记账");
    assert_eq!(db.memory_pool().exec_used(), 0, "全局账本不得留下滞留");
    std::fs::remove_dir_all(&root).ok();
}

// ---------------------------------------------------------------------------
// panic 也要归还额度（RAII 的意义）
// ---------------------------------------------------------------------------

#[test]
fn reservation_released_on_panic_unwind() {
    let (db, root) = seeded_db("panic", 16);
    let pool = Arc::new(qsdb::memory::MemoryPool::new(4 * 1024 * 1024));
    let ctx = QueryContext::new_with_pool("p1", None, None, Some(Arc::clone(&pool)));

    // 在持有预留的情况下 panic → 展开时 Drop 必须归还
    let got: qsdb::Result<()> = qsdb::query::run_isolated(|| {
        let _r = ctx.try_reserve(2 * 1024 * 1024)?;
        panic!("boom while holding memory");
    });
    assert!(matches!(got, Err(QsdbError::QueryPanic(_))));
    assert_eq!(pool.exec_used(), 0, "panic 展开后额度必须归还");
    assert_eq!(ctx.memory_used(), 0);
    // 额度回到账上 → 后续查询照常
    assert!(db.sql_collect("SELECT count(*) FROM t").is_ok());
    std::fs::remove_dir_all(&root).ok();
}

// ---------------------------------------------------------------------------
// C3：预算是「边界」不是「结果」——有预算与无预算结果逐值相同
// ---------------------------------------------------------------------------

#[test]
fn budget_does_not_change_results() {
    let (db, root) = seeded_db("c3", 128);
    let sql = "SELECT k, count(*) AS c, sum(v) AS s FROM t GROUP BY k ORDER BY k";
    let free = rows_of(&db, sql, None);
    // 给一个大到不触发的预算：结果应完全相同
    let pool = Arc::new(qsdb::memory::MemoryPool::new(1024 * 1024 * 1024));
    let ctx = QueryContext::new_with_pool("c3-q", None, None, Some(Arc::clone(&pool)));
    let capped = rows_of(&db, sql, Some(&ctx));
    assert_eq!(free, capped, "预算只改完成/拒绝的边界，不改结果");
    std::fs::remove_dir_all(&root).ok();
}

fn rows_of(db: &Qsdb, sql: &str, ctx: Option<&Arc<QueryContext>>) -> Vec<String> {
    let out = match ctx {
        Some(c) => db.sql_with_context(sql, c).unwrap(),
        None => db.sql(sql).unwrap(),
    };
    let batches = match out {
        qsdb::sql::SqlResult::Query { batches, .. } => batches,
        // `SqlResult` 是 `#[non_exhaustive]`（F27）：外部匹配必须留通配臂
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

#[test]
fn write_path_and_streaming_queries_are_unaffected() {
    // 预算极小也不能让**流式**查询（不驻留）失败——否则就是把功能改坏了
    let root = tmp_root("stream");
    let db = Qsdb::open(
        &root,
        QsdbOptions {
            memory_budget_bytes: Some(64 * 1024),
            scan_cache_bytes: 0,
            ..Default::default()
        },
    )
    .unwrap();
    db.sql("CREATE TABLE s (v BIGINT)").unwrap();
    for _ in 0..40 {
        db.append("s", &[batch(500)]).unwrap();
    }
    // 流式过滤/计数不驻留 → 不吃预算
    assert!(
        db.sql_collect("SELECT count(*) FROM s WHERE v >= 0")
            .is_ok()
    );
    std::fs::remove_dir_all(&root).ok();
}

// ---------------------------------------------------------------------------
// 回归：排序收尾的「峰值反冲记账」不得把**装得下配额**的查询打成配额错
// ---------------------------------------------------------------------------

/// 单列 v BIGINT 的表 + `batches × rows_per_batch` 行（批 = 段，扫描逐段出批）。
fn sort_table(tag: &str, batches: usize, rows_per_batch: usize) -> (Arc<Qsdb>, std::path::PathBuf) {
    let root = tmp_root(tag);
    let db = Arc::new(Qsdb::open(&root, QsdbOptions::default()).unwrap());
    db.sql("CREATE TABLE t (v BIGINT)").unwrap();
    for b in 0..batches {
        db.append(
            "t",
            &[batch_from(rows_per_batch, (b * rows_per_batch) as i64)],
        )
        .unwrap();
    }
    (db, root)
}

fn batch_from(n: usize, base: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
    let vals: Vec<i64> = (0..n as i64).map(|i| base + i).collect();
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vals))]).unwrap()
}

/// 读出 `SELECT v FROM t ORDER BY v` 的全部值（单列）。
fn sorted_vals(db: &Qsdb, ctx: &Arc<QueryContext>) -> Vec<i64> {
    let out = db
        .sql_with_context("SELECT v FROM t ORDER BY v", ctx)
        .expect("排序应完成");
    let batches = match out {
        qsdb::sql::SqlResult::Query { batches, .. } => batches,
        // `SqlResult` 是 `#[non_exhaustive]`（F27）：外部匹配必须留通配臂
        _ => panic!("expected query"),
    };
    batches
        .iter()
        .flat_map(|b| {
            (0..b.num_rows())
                .map(|i| {
                    arrow::util::display::array_value_to_string(b.column(0).as_ref(), i)
                        .unwrap()
                        .parse::<i64>()
                        .unwrap()
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

/// 输入 chunk 装得下配额、只是 `concat + take` 的瞬时 2x 峰值装不下时，
/// **必须落盘完成**——而不是把查询打死。
///
/// 回归形态：`SortExec::collect_all` 收尾路径原为 `res.grow(res.bytes())?`。
/// `res` 那一刻已覆盖驻留 chunk（= 全量输入），再 grow 同样的字节是**在已有预留
/// 之上**要第二份 → 只要「输入 > 配额/2」，明明装得下（输入 ≤ 配额）也被上抛
/// `QueryMemoryLimit`。外部实测（CentOS / 5M 行 ORDER BY v / 64MB 配额）报
/// 「query ab at 78MB would exceed its 64MB limit」（39MB 输入 × 2 = 78MB）。
///
/// 守的语义：本算子有溢出能力（P1）→ 峰值记不上就落盘，且落盘/收尾后记账归零。
#[test]
fn sort_fitting_quota_but_not_its_concat_peak_spills_instead_of_failing() {
    // 6 批 × 1000 行 ≈ 48KB 输入：≤ 64KB 配额，但 2x = 96KB > 配额 → 命中收尾路径
    let (db, root) = sort_table("sort-peak", 6, 1000);
    let ctx = QueryContext::new("peak-q", None, Some(64 * 1024));
    let vals = sorted_vals(&db, &ctx);
    let mut want: Vec<i64> = (0..6000).collect();
    want.sort_unstable();
    assert_eq!(vals, want, "落盘完成的结果必须与逐值有序");
    assert_eq!(
        ctx.memory_used(),
        0,
        "查询结束不得留下残余记账（峰值预留收缩/Drop 后必须归零）"
    );
    assert_eq!(db.memory_pool().exec_used(), 0);

    // 不限配额的同查询照旧（没把功能改坏）
    assert_eq!(
        sorted_vals(&db, &QueryContext::new("peak-free", None, None)),
        want
    );
    std::fs::remove_dir_all(&root).ok();
}

/// 反复溢出（每轮多次「攒到配额附近 → 落盘 → 继续攒」）之后，**不得残留记账**：
/// 残留会让下一轮提前失败（外部现象：同一查询第 3 轮才报配额错）。
///
/// 顺带钉住 C3：任意小配额下结果与不限内存**逐值相同**。
#[test]
fn repeated_spill_near_quota_leaves_no_residual_accounting() {
    let (db, root) = sort_table("sort-residual", 6, 1000);
    let mut want: Vec<i64> = (0..6000).collect();
    want.sort_unstable();

    for quota_kb in [4u64, 8, 16, 24, 48] {
        for round in 0..3 {
            // 每轮独立 ctx（与 ab_gate 一致）；同 ctx 复用也不得累积
            let ctx = QueryContext::new("residual-q", None, Some(quota_kb * 1024));
            let vals = sorted_vals(&db, &ctx);
            assert_eq!(
                vals, want,
                "{quota_kb}KB 配额第 {round} 轮：溢出只改完成/拒绝的边界，不改结果"
            );
            assert_eq!(
                ctx.memory_used(),
                0,
                "{quota_kb}KB 配额第 {round} 轮结束后残余记账必须为 0"
            );
            assert_eq!(db.memory_pool().exec_used(), 0);
        }
    }
    std::fs::remove_dir_all(&root).ok();
}
