//! v2 排序去重（`SortedDistinctCountExec`）的**三路对撞**验收
//! （`去重分区路 v2 设计稿` §四.1）。
//!
//! 同一条查询必须在三条执行路下**逐值相等**：
//! * 缺省 —— v2（零拷贝条目 + 排序去重，分片相 1）；
//! * `QSDB_NO_DISTINCT_V2=1` —— v1（编码条目 + 哈希表建组）；
//! * `QSDB_NO_DISTINCT_FUSE=1` —— 物化基线（真去重出 250 万行再数）。
//!
//! 这类优化的失败模式是**静默少数/多数**，只看"跑得通"看不出来 ——
//! 逐值对撞是唯一的日常 exercising。
//!
//! ⚠️ 环境变量是**进程级**的 ⇒ 所有碰 env 的测试用 `SERIAL` 锁罩住整个测试体
//! （与 `distinct_count_fused.rs` 同款做法）。

use qsdb::{Qsdb, QsdbOptions};

static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn open(tag: &str) -> (Qsdb, std::path::PathBuf) {
    let root = std::env::temp_dir().join("qsdb_distinct_v2").join(format!(
        "{tag}_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    (db, root)
}

fn scalar(db: &Qsdb, sql: &str) -> i64 {
    let bs = db.sql_collect(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    assert_eq!(bs.len(), 1, "{sql}: 结果应恰好一批");
    assert_eq!(bs[0].num_rows(), 1, "{sql}: 结果应恰好一行");
    bs[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("count 结果应是 Int64")
        .value(0)
}

/// `SELECT count(DISTINCT s)` / `SELECT count(*) FROM (SELECT DISTINCT s …)`
/// 在三条路下同值。**两个形态都要测**：前者 NULL 已在上游滤掉（永远到不了
/// 执行层），后者 NULL 会真的进执行层 —— NULL 计 1，是 v2 tag 路径的判别点。
fn assert_three_paths(db: &Qsdb, fused: i64, materialized: i64) {
    let fused_sql = "SELECT count(DISTINCT s) FROM t";
    let mat_sql = "SELECT count(*) FROM (SELECT DISTINCT s FROM t)";
    let run = |sql: &str| {
        let v2 = scalar(db, sql);
        unsafe { std::env::set_var("QSDB_NO_DISTINCT_V2", "1") };
        let v1 = scalar(db, sql);
        unsafe { std::env::remove_var("QSDB_NO_DISTINCT_V2") };
        unsafe { std::env::set_var("QSDB_NO_DISTINCT_FUSE", "1") };
        let base = scalar(db, sql);
        unsafe { std::env::remove_var("QSDB_NO_DISTINCT_FUSE") };
        assert_eq!(v2, v1, "{sql}: v2 与 v1 必须同值");
        assert_eq!(v2, base, "{sql}: v2 与物化基线必须同值");
        v2
    };
    assert_eq!(run(fused_sql), fused, "count(DISTINCT s) 跳过 NULL");
    assert_eq!(run(mat_sql), materialized, "DISTINCT 物化形态 NULL 计 1");
}

/// 覆盖面小表：重复、NULL、空串、多字节 UTF-8、单字符。
/// 非空不同值 = {a, b, '', '中'} = 4；NULL 再 +1。
#[test]
fn 小表面_三路对撞() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let (db, root) = open("small");
    db.sql("CREATE TABLE t (s STRING)").unwrap();
    db.sql("INSERT INTO t VALUES ('a'),('b'),('a'),(''),('中'),(NULL),('中'),('b')")
        .unwrap();
    assert_three_paths(&db, 4, 5);
    drop(db);
    let _ = std::fs::remove_dir_all(root);
}

/// 随机串 + 刻意高重复（基数 ~1/10）+ 少量 NULL + 空串：
/// 批间重复照满（每批都覆盖全基数）——**分区相加**与**排序去重**的日常 exercising。
#[test]
fn 五万行随机串_三路对撞() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    const ROWS: usize = 50_000;
    const CARD: usize = 5_000;
    let (db, root) = open("rand");
    db.sql("CREATE TABLE t (s STRING)").unwrap();
    let schema = std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("s", arrow::datatypes::DataType::Utf8, true),
    ]));
    // 确定性 LCG（可复现；不引随机依赖）
    let mut st: u64 = 0x9e37_79b9_7f4a_7c15;
    let mut next = move || {
        st ^= st << 13;
        st ^= st >> 7;
        st ^= st << 17;
        st
    };
    let mut done = 0usize;
    let mut nulls = 0usize;
    while done < ROWS {
        let n = 8_192.min(ROWS - done);
        let vals: Vec<Option<String>> = (0..n)
            .map(|_| {
                let r = next();
                if r % 97 == 0 {
                    nulls += 1;
                    return None; // 少量 NULL
                }
                if r % 89 == 0 {
                    return Some(String::new()); // 空串是合法键
                }
                // 多字节混着来：基数内循环 ⇒ 跨批大量重复
                let k = (r % CARD as u64) as usize;
                Some(format!("键-{k}-τ"))
            })
            .collect();
        let b = arrow::record_batch::RecordBatch::try_new(
            std::sync::Arc::clone(&schema),
            vec![std::sync::Arc::new(arrow::array::StringArray::from(vals))],
        )
        .unwrap();
        db.append("t", &[b]).unwrap();
        done += n;
    }
    // 期望值用**物化基线**自己算（三条路互相独立，谁错都会被另外两条掀出来）
    unsafe { std::env::set_var("QSDB_NO_DISTINCT_FUSE", "1") };
    unsafe { std::env::set_var("QSDB_NO_DISTINCT_V2", "1") };
    let expect_mat = scalar(&db, "SELECT count(*) FROM (SELECT DISTINCT s FROM t)");
    let expect_fused = scalar(&db, "SELECT count(DISTINCT s) FROM t");
    unsafe { std::env::remove_var("QSDB_NO_DISTINCT_FUSE") };
    unsafe { std::env::remove_var("QSDB_NO_DISTINCT_V2") };
    assert!(
        expect_mat > CARD as i64,
        "数据必须真的有多值（got {expect_mat}）"
    );
    assert!(
        expect_mat == expect_fused + 1,
        "恰好多一个 NULL（mat={expect_mat}, fused={expect_fused}）"
    );
    assert_three_paths(&db, expect_fused, expect_mat);
    drop(db);
    let _ = std::fs::remove_dir_all(root);
}

/// 全部同值 ⇒ 1；空表 ⇒ 0；单行 ⇒ 1。边界不红才算数。
#[test]
fn 边界_全同值_空表_单行() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let (db, root) = open("edge");

    // 全部同值
    db.sql("CREATE TABLE t (s STRING)").unwrap();
    db.sql("INSERT INTO t VALUES ('k'),('k'),('k')").unwrap();
    assert_three_paths(&db, 1, 1);
    db.sql("DROP TABLE t").unwrap();

    // 空表
    db.sql("CREATE TABLE t (s STRING)").unwrap();
    assert_three_paths(&db, 0, 0);
    db.sql("DROP TABLE t").unwrap();

    // 单行 NULL：fused=0（跳 NULL）、materialized=1（NULL 计 1）
    db.sql("CREATE TABLE t (s STRING)").unwrap();
    db.sql("INSERT INTO t VALUES (NULL)").unwrap();
    assert_three_paths(&db, 0, 1);
    drop(db);
    let _ = std::fs::remove_dir_all(root);
}

/// 只有 NULL 的多行表：fused=0、materialized=1 —— 与"全同值"不同，
/// 这里物化形态的"一个值"是 NULL 自己。
#[test]
fn 全null表_fused为0_物化为1() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let (db, root) = open("allnull");
    db.sql("CREATE TABLE t (s STRING)").unwrap();
    db.sql("INSERT INTO t VALUES (NULL),(NULL),(NULL)").unwrap();
    assert_three_paths(&db, 0, 1);
    drop(db);
    let _ = std::fs::remove_dir_all(root);
}

/// 分片并行相 1 与顺序相 1 必须同值（`QSDB_SHARDS` 控分片数，进程级 ⇒ SERIAL）。
#[test]
fn 分片数不影响结果() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    const ROWS: i64 = 120_000;
    const CARD: i64 = 30_000;
    let (db, root) = open("shards");
    db.sql("CREATE TABLE t (s STRING)").unwrap();
    let schema = std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("s", arrow::datatypes::DataType::Utf8, true),
    ]));
    let mut done = 0i64;
    while done < ROWS {
        let n = 20_000.min(ROWS - done);
        let vals: Vec<String> = (done..done + n)
            .map(|i| format!("s-{:06}", i % CARD))
            .collect();
        let b = arrow::record_batch::RecordBatch::try_new(
            std::sync::Arc::clone(&schema),
            vec![std::sync::Arc::new(arrow::array::StringArray::from(vals))],
        )
        .unwrap();
        db.append("t", &[b]).unwrap();
        done += n;
    }
    let sql = "SELECT count(DISTINCT s) FROM t";
    unsafe { std::env::remove_var("QSDB_SHARDS") };
    let one = scalar(&db, sql);
    assert_eq!(one, CARD, "基数就是 CARD");
    for n in ["2", "3", "8"] {
        unsafe { std::env::set_var("QSDB_SHARDS", n) };
        assert_eq!(
            scalar(&db, sql),
            one,
            "分片数 {n} 必须与顺序同值（行不相交分片只管路由）"
        );
    }
    unsafe { std::env::remove_var("QSDB_SHARDS") };
    drop(db);
    let _ = std::fs::remove_dir_all(root);
}
