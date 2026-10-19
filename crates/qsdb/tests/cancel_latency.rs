//! **取消的生效延迟**（不是"取消能不能被受理"）。
//!
//! 这两件事很容易混：`POST /queries/{id}/cancel` 回 200、查询也回了带
//! `QUERY_CANCELLED` 的错误体——**看着全对**，但如果执行器要跑到算子收尾才轮询到
//! 那个标志，那"取消"省下的时间是 **0**，`Statement.cancel()` 就是摆设。
//!
//! 发现经过（2026-10-17，写 `qsdb-client` 时量的）：5000 行非等值 join
//! （`ON a.id < b.id`）**不取消 15.67s / 取消后 19.74s**。根因是
//! `CrossJoinExec::next()` 的粒度：**一次调用展开 `n_left × n_right` 行**，
//! 而 `ctx.check()` 只在 `next()` 的**入口**查一次 —— 5000×5000 全在一次调用里
//! 跑完，中间一个轮询点都没有（除了延迟，那次展开还一次性物化 ~25M 行）。
//!
//! 本文件的判据是**比值**，不是绝对秒数：先量不取消的基线，再量取消的延迟。
//! 机器快慢不影响结论——**只要"取消"比"跑完"快得多**才算数。
//!
//! 顺带还钉着**多块展开的正确性**：49M 对会被切成 ~750 块吐出去，
//! 而基线那一遍断言的是精确值 `n(n-1)/2` ⇒ 切块的边界（`cursor` 推进、
//! 块尾的 `l`/`r` 进位）错一个，和就对不上。

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use qsdb::query::QueryContext;
use qsdb::{Qsdb, QsdbOptions};

fn tmp_root(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join("qsdb_cancel_test").join(format!(
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

/// 建一张 `rows` 行的表（两个整数列，够 join 用）。
fn seeded(tag: &str, rows: i64) -> (Arc<Qsdb>, std::path::PathBuf) {
    let root = tmp_root(tag);
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    db.sql("CREATE TABLE big (id BIGINT, v BIGINT)").unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("v", DataType::Int64, true),
    ]));
    let per = 2000i64;
    let mut done = 0i64;
    while done < rows {
        let n = per.min(rows - done);
        let ids: Vec<i64> = (done..done + n).collect();
        let vs: Vec<i64> = (done..done + n).map(|i| i * 2).collect();
        let b = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(Int64Array::from(vs)),
            ],
        )
        .unwrap();
        db.append("big", &[b]).unwrap();
        done += n;
    }
    (Arc::new(db), root)
}

/// 非等值 join 的整型 oracle：`#{ (i,j) : i < j }` = `n*(n-1)/2`。
fn expected_pairs(n: i64) -> i64 {
    n * (n - 1) / 2
}

#[test]
fn 非等值join_取消必须及时生效() {
    const ROWS: i64 = 7000; // 49M 对（够长，能把"取消有没有用"量出来）
    let (db, root) = seeded("cross", ROWS);
    let sql = "SELECT count(*) FROM big a JOIN big b ON a.id < b.id";

    // ① 基线：不取消，跑完要多久（口径 = 这条查询的真实耗时）
    let t0 = Instant::now();
    let r = db.sql(sql).expect("基线查询应当成功");
    let baseline = t0.elapsed();
    let got = match r {
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
        _ => -1,
    };
    assert_eq!(got, expected_pairs(ROWS), "基线结果就不对，用例本身先错了");
    // 基线太短就测不出"取消有没有用"——宁可显式跳过也不要出一条永远绿的假用例
    if baseline < Duration::from_secs(2) {
        eprintln!("跳过：基线只有 {baseline:?}（< 2s），测不出取消延迟");
        let _ = std::fs::remove_dir_all(&root);
        return;
    }

    // ② 取消：起一条一样的查询，300ms 后取消，量"从发出取消到查询报错返回"
    let ctx = QueryContext::new("cancel-lat", None, None);
    let c2 = Arc::clone(&ctx);
    let d2 = Arc::clone(&db);
    let h = std::thread::spawn(move || {
        let r = d2.sql_with_context(sql, &c2);
        let is_err = r.is_err();
        let code = r.err().map(|e| format!("{e}"));
        (is_err, code)
    });
    std::thread::sleep(Duration::from_millis(300));
    let t_cancel = Instant::now();
    ctx.cancel();
    let (is_err, err) = h.join().expect("查询线程不能 panic");
    let latency = t_cancel.elapsed();

    let _ = std::fs::remove_dir_all(&root);
    assert!(is_err, "取消之后查询必须报错返回，而不是静默跑完");
    let msg = err.unwrap_or_default();
    assert!(
        msg.contains("cancel"),
        "应当是取消错误（`QUERY_CANCELLED`），实为：{msg}"
    );
    eprintln!("[cancel] 基线 {baseline:?} / 取消生效 {latency:?}");
    // 判据是**比值**：取消必须比跑完快一个数量级。
    // （绝对秒数会随机器变，写死就是造一条会 flaky 的尺子。）
    assert!(
        latency * 10 < baseline,
        "取消生效太慢：{latency:?} vs 基线 {baseline:?} —— \
         说明执行器在长循环里没有轮询取消标志"
    );
}
