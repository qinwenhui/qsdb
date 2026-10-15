//! FULL OUTER JOIN 的**值级**验收。
//!
//! 为什么单独一个文件：FULL 与前两者的唯一差别是"**右孤儿**"——它要等 probe
//! 跑完、看过全量 build 匹配标记才知道是谁（`HashJoinExec::full_tail`）。
//! 漏了那一轮就是**静默少行**；而并发分片下若每片各发一次，则是**静默多行**。
//! 这两种错 `count(*)` 都可能看不出来（数据恰好对称时），所以逐值断言。
//!
//! 数据（两侧都有孤儿，才能把"补哪一侧"分辨出来）：
//! `l(k,m)` = (1,10) (2,20) (3,30)；`r(k,m,v)` = (2,20,200) (3,35,300) (4,40,400)

use std::sync::Arc;

use arrow::array::{Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use qsdb::{Qsdb, QsdbOptions};

fn tmp_root(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join("qsdb_join_full").join(format!(
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

/// 全外连的**正确答案**：匹配对 + 左孤儿 + 右孤儿。
///
/// `ORDER BY l.k NULLS LAST, r.k` ⇒ (1,NULL) (2,2) (3,3) (NULL,4)
const FULL_SQL: &str = "SELECT l.k, r.k FROM l FULL OUTER JOIN r ON l.k = r.k \
                        ORDER BY l.k NULLS LAST, r.k";

#[test]
fn full_outer_两侧孤儿都保留() {
    let (db, root) = seeded("full_both");
    assert_eq!(
        col_i64(&db, FULL_SQL, 0),
        vec![Some(1), Some(2), Some(3), None],
        "左孤儿(k=1)要有行、右孤儿(k=4)那行的左段要是 NULL"
    );
    assert_eq!(
        col_i64(&db, FULL_SQL, 1),
        vec![None, Some(2), Some(3), Some(4)],
        "左孤儿那行的右段要是 NULL；右孤儿(k=4)要出现"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn full_outer_值不会被对调() {
    let (db, root) = seeded("full_cols");
    // 与 RIGHT 同源的"索引空间"风险：漏了任何一处换算都会让 l.* 与 r.* 对调，
    // 而两表列名相同（k/m）⇒ 照样跑得通、`count(*)` 也照样对。逐值看。
    let sql = "SELECT l.k, l.m, r.k, r.m, r.v FROM l FULL OUTER JOIN r ON l.k = r.k \
               ORDER BY l.m NULLS LAST, r.m";
    assert_eq!(col_i64(&db, sql, 0), vec![Some(1), Some(2), Some(3), None]);
    assert_eq!(
        col_i64(&db, sql, 1),
        vec![Some(10), Some(20), Some(30), None]
    );
    assert_eq!(col_i64(&db, sql, 2), vec![None, Some(2), Some(3), Some(4)]);
    assert_eq!(
        col_i64(&db, sql, 3),
        vec![None, Some(20), Some(35), Some(40)],
        "r.m 必须还是 r 的（对调的话这里会变成 l.m 的值）"
    );
    assert_eq!(
        col_i64(&db, sql, 4),
        vec![None, Some(200), Some(300), Some(400)]
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn full_outer_一边匹配一边孤儿的三种情形() {
    let (db, root) = seeded("full_oracles");
    // 计数口径：匹配 2 对 + 左孤儿 1 + 右孤儿 1 = 4
    assert_eq!(
        col_i64(
            &db,
            "SELECT count(*) FROM l FULL OUTER JOIN r ON l.k = r.k",
            0
        ),
        vec![Some(4)]
    );
    assert_eq!(
        col_i64(
            &db,
            "SELECT count(*) FROM l FULL JOIN r ON l.k = r.k WHERE l.k IS NULL",
            0
        ),
        vec![Some(1)],
        "右孤儿恰好一个"
    );
    assert_eq!(
        col_i64(
            &db,
            "SELECT count(*) FROM l FULL JOIN r ON l.k = r.k WHERE r.k IS NULL",
            0
        ),
        vec![Some(1)],
        "左孤儿恰好一个（**唯一多出来的那一行**——漏掉收尾轮就会变 0）"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn full_outer_保留残留谓词语义() {
    let (db, root) = seeded("full_residual");
    // 残留否掉匹配对 ⇒ 那一**对**不算匹配 ⇒ 两侧各自变成孤儿（各保留一行）。
    // v>250：k=2 的对被否掉（v=200 → 左 k=2 成左孤儿、右 k=2 成右孤儿）；
    //        k=3 通过（v=300）；k=4 无匹配（右孤儿）。
    let sql = "SELECT l.k, r.k FROM l FULL OUTER JOIN r ON l.k = r.k AND r.v > 250 \
               ORDER BY l.k NULLS LAST, r.k NULLS LAST";
    assert_eq!(
        col_i64(&db, sql, 0),
        vec![Some(1), Some(2), Some(3), None, None]
    );
    assert_eq!(
        col_i64(&db, sql, 1),
        vec![None, None, Some(3), Some(2), Some(4)]
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn full_outer_空侧() {
    let (db, root) = seeded("full_empty");
    db.sql("CREATE TABLE e (k BIGINT, m BIGINT, v BIGINT)")
        .unwrap();
    // 右表空 ⇒ 全是左孤儿（左表 3 行全保留、右段 NULL）
    assert_eq!(
        col_i64(&db, "SELECT count(*) FROM l FULL JOIN e ON l.k = e.k", 0),
        vec![Some(3)]
    );
    // 左表空 ⇒ 全是右孤儿
    assert_eq!(
        col_i64(&db, "SELECT count(*) FROM e FULL JOIN r ON e.k = r.k", 0),
        vec![Some(3)]
    );
    // 两侧都空 ⇒ 零行（不是"一行 NULL"）
    assert_eq!(
        col_i64(
            &db,
            "SELECT count(*) FROM e FULL JOIN e e2 ON e.k = e2.k",
            0
        ),
        vec![Some(0)]
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn full_outer_与_inner_left_right_的关系() {
    let (db, root) = seeded("full_relation");
    // FULL = INNER ∪ 左孤儿 ∪ 右孤儿；LEFT ∪ 右孤儿 = FULL。用这三条互相钉。
    let inner = col_i64(&db, "SELECT count(*) FROM l JOIN r ON l.k = r.k", 0);
    let left = col_i64(&db, "SELECT count(*) FROM l LEFT JOIN r ON l.k = r.k", 0);
    let right = col_i64(&db, "SELECT count(*) FROM l RIGHT JOIN r ON l.k = r.k", 0);
    let full = col_i64(&db, "SELECT count(*) FROM l FULL JOIN r ON l.k = r.k", 0);
    assert_eq!(inner, vec![Some(2)]);
    assert_eq!(left, vec![Some(3)], "INNER + 左孤儿");
    assert_eq!(right, vec![Some(3)], "INNER + 右孤儿");
    assert_eq!(full, vec![Some(4)], "INNER + 两侧孤儿");
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn full_outer_多行的右孤儿不丢不重() {
    let (db, root) = seeded("full_multi");
    // 右表塞一批**谁都不匹配**的行：收尾轮必须把它们**一次**全发出来
    //（漏发 = 少行；分片路径每片各发 = 多行）
    let n = 500i64;
    let ks: Vec<i64> = (1000..1000 + n).collect();
    let ms: Vec<i64> = (0..n).collect();
    let vs: Vec<i64> = (0..n).map(|i| i * 7).collect();
    db.append(
        "r",
        &[RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int64, true),
                Field::new("m", DataType::Int64, true),
                Field::new("v", DataType::Int64, true),
            ])),
            vec![
                Arc::new(Int64Array::from(ks)),
                Arc::new(Int64Array::from(ms)),
                Arc::new(Int64Array::from(vs)),
            ],
        )
        .unwrap()],
    )
    .unwrap();
    // 2 对匹配 + 1 左孤儿 + (1 右孤儿 + 500 新右孤儿) = 504
    assert_eq!(
        col_i64(&db, "SELECT count(*) FROM l FULL JOIN r ON l.k = r.k", 0),
        vec![Some(504)]
    );
    assert_eq!(
        col_i64(
            &db,
            "SELECT count(*) FROM l FULL JOIN r ON l.k = r.k WHERE l.k IS NULL",
            0
        ),
        vec![Some(501)],
        "右孤儿 501 个，一个不多一个不少"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn full_outer_的限定名与别名() {
    let (db, root) = seeded("full_alias");
    // 别名之后**只能用别名**引用（`r.v` 会报 not found: column 'v' —— 标准 SQL 行为，
    // 别名把原名遮掉了）
    assert_eq!(
        col_i64(
            &db,
            "SELECT b.v FROM l AS a FULL JOIN r AS b ON a.k = b.k ORDER BY b.v NULLS FIRST",
            0
        ),
        vec![None, Some(200), Some(300), Some(400)]
    );
    assert_eq!(
        col_i64(
            &db,
            "SELECT a.m FROM l AS a FULL JOIN r AS b ON a.k = b.k ORDER BY a.m NULLS LAST",
            0
        ),
        vec![Some(10), Some(20), Some(30), None],
        "左孤儿那行的左段要有值"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn full_outer_链式与后续聚合() {
    let (db, root) = seeded("full_chain");
    // FULL 之后再等值 join：`(l FULL r) JOIN s`
    db.sql("CREATE TABLE s (k BIGINT, tag STRING)").unwrap();
    db.append(
        "s",
        &[RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int64, true),
                Field::new("tag", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![2i64, 4])),
                Arc::new(StringArray::from(vec![Some("p"), Some("q")])),
            ],
        )
        .unwrap()],
    )
    .unwrap();
    // FULL 出 4 行：(1,NULL) (2,2) (3,3) (NULL,4)；再按 `s.k = r.k` 连 s(2,4)
    // ⇒ 命中 r.k=2 与 r.k=4 两行（左孤儿那行的 r.k 是 NULL，不命中）
    assert_eq!(
        col_i64(
            &db,
            "SELECT count(*) FROM l FULL JOIN r ON l.k = r.k JOIN s ON s.k = r.k",
            0
        ),
        vec![Some(2)]
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// FULL 的结果在 `QSDB_SHARDS` 1 / 8 两种设置下**必须逐值相同**。
///
/// ⚠️ **这条用例能证明什么、不能证明什么**（别读成"分片路径下的 FULL 是对的"）：
///
/// - 它证明的：同一份数据、同一条 SQL，在两种并行度旋钮下结果一致。
/// - 它**没**证明的：该形状**根本不触发分片**（实测 `QSDB_JOIN_SHARD_PROF=1`
///   无输出）。真正的防线是 `try_shard_source` 里那条**显式臂**（FULL 不进
///   可分片清单）+ 默认臂 `_ => return None`——分片按**行**切左输入，而 FULL
///   的右孤儿要看**全量** build 匹配标记，每片各发各的会**重复发射**。
///   哪天有人把 FULL 加进可分片清单却没同步做全局标记位图，生产上才会暴露；
///   这条用例**抓不到**那次改动（小数据不触发分片），它守的是"另一种设置下
///   结果也得一样"这个弱一些的性质。
///
/// （留这条注释是为了让下一个人别把它当成"分片正确性"的证明——本仓为
///  "用例其实是空的"付过代价。）
#[test]
fn full_outer_在分片旋钮下结果不变() {
    let (db, root) = seeded("full_shards");
    // 右表塞一批孤儿，好让"重复发射"有东西可重
    let n = 300i64;
    let ks: Vec<i64> = (5000..5000 + n).collect();
    let ms: Vec<i64> = (0..n).collect();
    let vs: Vec<i64> = (0..n).collect();
    db.append(
        "r",
        &[RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int64, true),
                Field::new("m", DataType::Int64, true),
                Field::new("v", DataType::Int64, true),
            ])),
            vec![
                Arc::new(Int64Array::from(ks)),
                Arc::new(Int64Array::from(ms)),
                Arc::new(Int64Array::from(vs)),
            ],
        )
        .unwrap()],
    )
    .unwrap();
    let sql = "SELECT count(*), sum(r.v) FROM l FULL JOIN r ON l.k = r.k";
    // SAFETY（读环境变量本身是 safe 的；这里只是把旋钮在进程内设好再查）
    unsafe { std::env::set_var("QSDB_SHARDS", "1") };
    let a = col_i64(&db, sql, 0);
    let a_sum = col_i64(&db, sql, 1);
    unsafe { std::env::set_var("QSDB_SHARDS", "8") };
    let b = col_i64(&db, sql, 0);
    let b_sum = col_i64(&db, sql, 1);
    unsafe { std::env::remove_var("QSDB_SHARDS") };
    assert_eq!(a, b, "分片数变了，行数就变了 —— 右孤儿被重复发射");
    assert_eq!(a_sum, b_sum, "分片数变了，聚合值就变了");
    // 2 对匹配 + 1 左孤儿 + (1 + 300) 右孤儿 = 304
    assert_eq!(a, vec![Some(304)]);
    let _ = std::fs::remove_dir_all(&root);
}
