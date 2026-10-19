//! RIGHT / CROSS JOIN 的**值级**验收（`count(*)` 测不出列序错）。
//!
//! 为什么单独一个文件：`join_common_probe.rs` 那类用例只数行数，而 RIGHT 换边
//! 实现最容易错的地方是**列序**——`l RIGHT JOIN r` 的换边计划输出是 `r.* ++ l.*`，
//! 漏掉那层重排投影时 `SELECT l.k, r.k` 会取到**对调的列**，而 `count(*)` 照样
//! 是对的。所以这里逐列读值。
//!
//! 数据（两侧都有孤儿，才能分辨"保留哪一侧"）：
//! `l(k,m)` = (1,10) (2,20) (3,30)；`r(k,m,v)` = (2,20,200) (3,35,300) (4,40,400)

use std::sync::Arc;

use arrow::array::{Array, Int64Array, StringArray};
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
    let d = std::env::temp_dir().join("qsdb_join_rc").join(format!(
        "{tag}_{:x}_{}",
        std::process::id(),
        _uniq_seq()
    ));
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn seeded(tag: &str) -> (Qsdb, std::path::PathBuf) {
    let root = tmp_root(tag);
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    db.sql("CREATE TABLE l (k BIGINT, m BIGINT)").unwrap();
    db.sql("CREATE TABLE r (k BIGINT, m BIGINT, v BIGINT)")
        .unwrap();
    db.append(
        "l",
        &[RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int64, true),
                Field::new("m", DataType::Int64, true),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1i64, 2, 3])),
                Arc::new(Int64Array::from(vec![10i64, 20, 30])),
            ],
        )
        .unwrap()],
    )
    .unwrap();
    db.append(
        "r",
        &[RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int64, true),
                Field::new("m", DataType::Int64, true),
                Field::new("v", DataType::Int64, true),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![2i64, 3, 4])),
                Arc::new(Int64Array::from(vec![20i64, 35, 40])),
                Arc::new(Int64Array::from(vec![200i64, 300, 400])),
            ],
        )
        .unwrap()],
    )
    .unwrap();
    (db, root)
}

/// 取一列（`i64`，NULL → `None`），按行序返回。
fn col_i64(db: &Qsdb, sql: &str, c: usize) -> Vec<Option<i64>> {
    let r = db
        .sql(sql)
        .unwrap_or_else(|e| panic!("`{sql}` 应当成功：{e}"));
    let qsdb::sql::SqlResult::Query { batches, .. } = r else {
        panic!("`{sql}` 应当是查询结果");
    };
    let mut out = Vec::new();
    for b in &batches {
        let a = b
            .column(c)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap_or_else(|| panic!("第 {c} 列应当是 BIGINT"));
        for i in 0..a.len() {
            out.push(if a.is_null(i) { None } else { Some(a.value(i)) });
        }
    }
    out
}

#[test]
fn right_join_的列序不能对调() {
    let (db, root) = seeded("right_cols");
    // 换边后若不补重排投影，`l.*` 与 `r.*` 会整段对调 —— 只数行数看不出来。
    // 两表列名相同（k/m），所以再"对调"也照样跑得通 ⇒ 必须逐值看。
    let sql = "SELECT l.k, l.m, r.k, r.m, r.v FROM l RIGHT JOIN r ON l.k = r.k \
               ORDER BY r.k";
    let lk = col_i64(&db, sql, 0);
    let lm = col_i64(&db, sql, 1);
    let rk = col_i64(&db, sql, 2);
    let rm = col_i64(&db, sql, 3);
    let rv = col_i64(&db, sql, 4);
    // r 三行都在（RIGHT 保留右侧）：k=2 匹配、k=3 匹配、k=4 未匹配
    assert_eq!(rk, vec![Some(2), Some(3), Some(4)]);
    assert_eq!(lm, vec![Some(20), Some(30), None], "l.m 该跟着 l.k 走");
    assert_eq!(
        lk,
        vec![Some(2), Some(3), None],
        "r.k=4 未匹配 ⇒ 左段补 NULL"
    );
    assert_eq!(rm, vec![Some(20), Some(35), Some(40)], "r.m 必须还是 r 的");
    assert_eq!(rv, vec![Some(200), Some(300), Some(400)]);
    // 反例照：把两侧对调后这组断言必然失败（l.m 会变成 [20,35,40]）—— 故本条能分辨
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn right_join_的限定名仍然可解析() {
    let (db, root) = seeded("right_names");
    // 换边后 ctx 的列序仍是 [l.*, r.*]（用户视角），限定名必须还能绑到对的地方
    assert_eq!(
        col_i64(
            &db,
            "SELECT r.v FROM l RIGHT JOIN r ON l.k = r.k ORDER BY r.v",
            0
        ),
        vec![Some(200), Some(300), Some(400)]
    );
    assert_eq!(
        col_i64(
            &db,
            "SELECT l.m FROM l RIGHT JOIN r ON l.k = r.k ORDER BY r.k",
            0
        ),
        vec![Some(20), Some(30), None]
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn right_join_与等价_left_写法逐值一致() {
    let (db, root) = seeded("right_equiv");
    // `l RIGHT JOIN r ON c` ≡ `r LEFT JOIN l ON c`（列序按需要重排）
    let a = col_i64(
        &db,
        "SELECT l.k, r.k, r.v FROM l RIGHT JOIN r ON l.k = r.k ORDER BY r.v",
        0,
    );
    let b = col_i64(
        &db,
        "SELECT l.k, r.k, r.v FROM r LEFT JOIN l ON r.k = l.k ORDER BY r.v",
        0,
    );
    assert_eq!(a, b, "两种写法必须逐值一致");
    assert_eq!(a, vec![Some(2), Some(3), None]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn right_join_带残留谓词() {
    let (db, root) = seeded("right_residual");
    // 残留谓词在换边后要**逐列改 idx**（合并序从「左++右」变成「右++左」）——
    // 漏改就是读错列（这里 r.v 会挂到 l 的列上，判出荒谬的结果）。
    // 语义：r.k=2 的对被 v>250 否掉 → r 那行算未匹配 ⇒ 补 NULL 保留。
    let sql = "SELECT l.k, r.v FROM l RIGHT JOIN r ON l.k = r.k AND r.v > 250 \
               ORDER BY r.v";
    // v=200：匹配上了但被残留否掉 ⇒ **r 那行算未匹配**、补 NULL 保留（不是整行消失）
    // v=300：匹配通过
    // v=400：无匹配
    assert_eq!(col_i64(&db, sql, 0), vec![None, Some(3), None]);
    assert_eq!(col_i64(&db, sql, 1), vec![Some(200), Some(300), Some(400)]);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn cross_join_是笛卡尔积() {
    let (db, root) = seeded("cross_vals");
    let sql = "SELECT l.k, r.k FROM l CROSS JOIN r ORDER BY l.k, r.k";
    assert_eq!(col_i64(&db, sql, 0).len(), 9, "3×3");
    assert_eq!(
        col_i64(&db, sql, 0),
        vec![
            Some(1),
            Some(1),
            Some(1),
            Some(2),
            Some(2),
            Some(2),
            Some(3),
            Some(3),
            Some(3)
        ]
    );
    assert_eq!(
        col_i64(&db, sql, 1),
        vec![
            Some(2),
            Some(3),
            Some(4),
            Some(2),
            Some(3),
            Some(4),
            Some(2),
            Some(3),
            Some(4)
        ]
    );
    // `ON TRUE` 是它此前的替代写法，两者必须一致
    assert_eq!(
        col_i64(&db, "SELECT l.k FROM l JOIN r ON TRUE ORDER BY l.k", 0),
        col_i64(&db, "SELECT l.k FROM l CROSS JOIN r ORDER BY l.k", 0)
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn cross_join_与其它形态可组合() {
    let (db, root) = seeded("cross_combo");
    // 三表：CROSS 之后再等值 join（列序/索引空间的最容易错的组合）
    db.sql("CREATE TABLE s (k BIGINT, tag STRING)").unwrap();
    db.append(
        "s",
        &[RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int64, true),
                Field::new("tag", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![2i64])),
                Arc::new(StringArray::from(vec![Some("x")])),
            ],
        )
        .unwrap()],
    )
    .unwrap();
    // l CROSS r 得 9 行，其中 l.k = s.k = 2 的有 3 行（r 三行）
    assert_eq!(
        col_i64(
            &db,
            "SELECT count(*) FROM l CROSS JOIN r JOIN s ON l.k = s.k",
            0
        ),
        vec![Some(3)]
    );
    let _ = std::fs::remove_dir_all(&root);
}
