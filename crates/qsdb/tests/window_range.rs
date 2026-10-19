//! **RANGE 帧**（十九批 S3）的值级验收：按**值**分帧 —— `ORDER BY` 键相等的
//! 行是并列组（peers），整组一起进帧。**键有重复时与 ROWS 同形态数字不同**
//! ——这正是它当初必须显式拒、不能拿 ROWS 冒充的原因（每条断言都成对锁
//! RANGE 与 ROWS 两个数字）。
//!
//! 覆盖三形态：
//! - ① `UNBOUNDED PRECEDING AND CURRENT ROW` ≡ 默认帧（绑定期改写为 None，
//!   走原快路径）—— 并列行**共享组尾累计值**
//! - ② `UNBOUNDED … AND UNBOUNDED …` ≡ 整分区（与单位/行序无关）
//! - ③ `CURRENT ROW AND UNBOUNDED FOLLOWING` —— 起点 = 并列组起点
//!   （`RowFrame.range_peer_start` 执行器专路）
//!
//! 数据：`w(k,v)` = ('A',1) ('A',2) ('B',4) —— A 是键重复组。

use std::sync::Arc;

use arrow::array::{Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use qsdb::{Qsdb, QsdbOptions};

/// 测试目录唯一序号：pid 挡跨进程碰撞，序号挡同进程内并发（Windows 时钟
/// 粒度 ~100ns，纳秒后缀在同 tick 内会重复——CI 上真炸过，见 constraints）。
fn _uniq_seq() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    SEQ.fetch_add(1, Ordering::Relaxed)
}

fn tmp_root(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join("qsdb_window_range").join(format!(
        "{tag}_{:x}_{}",
        std::process::id(),
        _uniq_seq()
    ));
    std::fs::create_dir_all(&d).unwrap();
    d
}

/// `keys`/`vals` → 表 `w(k STRING, v BIGINT)`。
fn seeded(tag: &str, keys: &[&str], vals: &[i64]) -> (Qsdb, std::path::PathBuf) {
    let root = tmp_root(tag);
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    db.sql("CREATE TABLE w (k VARCHAR, v BIGINT)").unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Utf8, true),
        Field::new("v", DataType::Int64, true),
    ]));
    db.append(
        "w",
        &[RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow::array::StringArray::from(
                    keys.iter().map(|k| Some(k.to_string())).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(vals.to_vec())),
            ],
        )
        .unwrap()],
    )
    .unwrap();
    (db, root)
}

/// 窗口列的整数值（保持输出序）。
fn win_vals(db: &Qsdb, sql: &str) -> Vec<i64> {
    let r = db
        .sql(sql)
        .unwrap_or_else(|e| panic!("`{sql}` 应当成功：{e}"));
    let qsdb::sql::SqlResult::Query { batches, .. } = r else {
        panic!("`{sql}` 应当是查询结果");
    };
    let mut out = Vec::new();
    for b in &batches {
        let a = b
            .column(b.num_columns() - 1) // 窗口列恒是投影的最后一列
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("i64 窗口列");
        for i in 0..a.len() {
            out.push(a.value(i));
        }
    }
    out
}

#[test]
fn 形态一_键重复时与rows数字不同() {
    let (db, root) = seeded("f1", &["A", "A", "B"], &[1, 2, 4]);
    // RANGE①：并列组整组进帧 —— 首个 A 行看到的是**整组**的和 3，不是 1
    let range = win_vals(
        &db,
        "SELECT sum(v) OVER (ORDER BY k RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM w",
    );
    assert_eq!(range, vec![3, 3, 7], "RANGE① = 组尾累计（并列行共享）");
    // ROWS 同形态：按行累计 —— 与 RANGE 在键重复处**必须不同**
    let rows = win_vals(
        &db,
        "SELECT sum(v) OVER (ORDER BY k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM w",
    );
    assert_eq!(rows, vec![1, 3, 7], "ROWS① = 逐行累计");
    assert_ne!(
        range, rows,
        "两者相同 = RANGE 被当 ROWS 跑了（本批要防的事）"
    );
    // 键唯一时两者重合（退化形态，既有的 ROWS 语义回归）
    let (db2, root2) = seeded("f1u", &["A", "B", "C"], &[1, 2, 4]);
    assert_eq!(
        win_vals(
            &db2,
            "SELECT sum(v) OVER (ORDER BY k RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM w"
        ),
        vec![1, 3, 7],
        "键唯一 ⇒ RANGE① 与 ROWS 同值"
    );
    let _ = std::fs::remove_dir_all(&root);
    let _ = std::fs::remove_dir_all(&root2);
}

#[test]
fn 形态三_起点是并列组起点() {
    let (db, root) = seeded("f3", &["A", "A", "B"], &[1, 2, 4]);
    // RANGE③：行1/行2 同属 A 组 ⇒ 帧同为 [A组首, 分区尾] ⇒ 同值 7；B 行 = 4
    let range = win_vals(
        &db,
        "SELECT sum(v) OVER (ORDER BY k RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM w",
    );
    assert_eq!(range, vec![7, 7, 4]);
    // ROWS 同形态：起点是当前行 ⇒ 第二个 A 行只从 2 起算 ⇒ 6 ≠ 7
    let rows = win_vals(
        &db,
        "SELECT sum(v) OVER (ORDER BY k ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM w",
    );
    assert_eq!(rows, vec![7, 6, 4]);
    assert_ne!(range, rows);
    // count / min 走同一条 peer 路
    assert_eq!(
        win_vals(
            &db,
            "SELECT count(*) OVER (ORDER BY k RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM w"
        ),
        vec![3, 3, 1]
    );
    assert_eq!(
        win_vals(
            &db,
            "SELECT min(v) OVER (ORDER BY k RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM w"
        ),
        vec![1, 1, 4]
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn 形态二_整分区且无order_by可用() {
    let (db, root) = seeded("f2", &["A", "A", "B"], &[1, 2, 4]);
    // 与单位/行序无关 ⇒ 带/不带 ORDER BY 同值
    for sql in [
        "SELECT sum(v) OVER (ORDER BY k RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM w",
        "SELECT sum(v) OVER (RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM w",
    ] {
        assert_eq!(win_vals(&db, sql), vec![7, 7, 7], "{sql}");
    }
    let _ = std::fs::remove_dir_all(&root);
}

/// NULL 键 = 同一并列组（PG 同判：NULL 与 NULL 是 peers）。
#[test]
fn null键是同一并列组() {
    // NULLS FIRST ⇒ NULL 组在前：RANGE① = [3, 3, 7]
    let (db, root) = seeded("null", &["B", "A", "A"], &[4, 1, 2]);
    let r = win_vals(
        &db,
        "SELECT sum(v) OVER (ORDER BY k NULLS LAST RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM w",
    );
    // ORDER BY k NULLS LAST：A,A,NULL ⇒ A 组和 3 → 3 → +4 = 7（NULL 组的 v=1,2 在……）
    // 等等——数据是 (B,4),(A,1),(A,2)：NULLS LAST ⇒ A 组(1,2) → NULL 组(4)。
    // 组尾累计：3 → 3 → 7。
    assert_eq!(
        r,
        vec![3, 3, 7],
        "A 并列组共享组尾值；NULL 组（k=NULL 的 v=4）殿后"
    );
    // NULL 组自己：RANGE③ 的帧 = [NULL 组起点, 分区尾] = 只有它自己 ⇒ 4；
    // A 组两行同组 ⇒ 帧同为全分区 ⇒ 都 7
    let r = win_vals(
        &db,
        "SELECT sum(v) OVER (ORDER BY k NULLS LAST RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM w",
    );
    assert_eq!(
        r,
        vec![7, 7, 4],
        "A 组两行同帧 7；末组（NULL）帧 = 只有自己"
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// 多分区：peer 组不跨分区（PARTITION BY 边界重置并列组）。
#[test]
fn 分区边界重置并列组() {
    let root = tmp_root("part");
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    db.sql("CREATE TABLE p (g VARCHAR, k VARCHAR, v BIGINT)")
        .unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("g", DataType::Utf8, true),
        Field::new("k", DataType::Utf8, true),
        Field::new("v", DataType::Int64, true),
    ]));
    db.append(
        "p",
        &[RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow::array::StringArray::from(vec![
                    Some("x"),
                    Some("x"),
                    Some("y"),
                    Some("y"),
                ])),
                Arc::new(arrow::array::StringArray::from(vec![
                    Some("A"),
                    Some("A"),
                    Some("A"),
                    Some("A"),
                ])),
                Arc::new(Int64Array::from(vec![1i64, 2, 10, 20])),
            ],
        )
        .unwrap()],
    )
    .unwrap();
    // 每分区独立：x = [3,3]（A 组和 3）、y = [30,30]
    assert_eq!(
        win_vals(
            &db,
            "SELECT sum(v) OVER (PARTITION BY g ORDER BY k RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM p"
        ),
        vec![3, 3, 30, 30]
    );
    assert_eq!(
        win_vals(
            &db,
            "SELECT sum(v) OVER (PARTITION BY g ORDER BY k RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM p"
        ),
        vec![3, 3, 30, 30]
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// 边界：值偏移拒 / 缺 ORDER BY 拒 / 多列 ORDER BY 拒（文案都要指路）。
#[test]
fn 边界_值偏移与order_by约束() {
    let (db, root) = seeded("edge", &["A", "B"], &[1, 2]);
    // 值偏移
    let e = db
        .sql("SELECT sum(v) OVER (ORDER BY k RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM w")
        .unwrap_err();
    assert!(e.to_string().contains("value offset"), "{e}");
    // 缺 ORDER BY（带 CURRENT ROW 界的形态）
    let e = db
        .sql("SELECT sum(v) OVER (RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM w")
        .unwrap_err();
    assert!(e.to_string().contains("exactly one ORDER BY"), "{e}");
    // ORDER BY 两列
    db.sql("CREATE TABLE w2 (k VARCHAR, j BIGINT, v BIGINT)")
        .unwrap();
    db.sql("INSERT INTO w2 VALUES ('A',1,1),('B',1,2)").unwrap();
    let e = db
        .sql("SELECT sum(v) OVER (ORDER BY k, j RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM w2")
        .unwrap_err();
    assert!(e.to_string().contains("exactly one ORDER BY"), "{e}");
    // GROUPS 仍拒
    let e = db
        .sql("SELECT sum(v) OVER (ORDER BY k GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM w")
        .unwrap_err();
    assert!(e.to_string().contains("GROUPS"), "{e}");
    let _ = std::fs::remove_dir_all(&root);
}
