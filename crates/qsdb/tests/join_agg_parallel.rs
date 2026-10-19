//! **join 之后的分聚合**（P0③）的正确性回归。
//!
//! 背景：`exec_builder::try_shard_source` 此前只穿透 semi 族 join —— INNER/LEFT
//! 之后整条管线退回单线程（扫描是 rayon 并行的，join+聚合不是）。这是
//! `complex_three_way`（9.9×）与 `complex_join_group`（5.8×）**同一个**病灶。
//!
//! 现在分片可以带上整棵 join 子树：每片把子树在 `ShardScope` 下建一遍，
//! 最底下那个 `Scan` 被换成分片。正确性靠"分片是左输入的**不相交**划分"
//! （每行左数据只出现在一片里 ⇒ INNER/LEFT 的结果按片求并 = 整体结果）。
//!
//! ## 对照组：为什么每条都要跟"串行跑同一份数据"比
//!
//! 分片路径**结果对不对**不能靠自证——`join_plan_shape` 那种形状断言管不了
//! "连错列"。这里用一把便宜的尺子：把表包进 CTE（`LogicalPlan::Values`），
//! `try_shard_source` 就剥不到底 ⇒ **必走单节点** `HashAggExec`，同一台机、
//! 同一份数据、同一句 SQL 语义。两条路径逐行比，才叫验证过。
//!
//! ⚠️ 对照组本身也会骗人：早先按子串 `"FROM t "` 做替换，对 `t2` 根本不匹配，
//! 于是"串行基准"其实又跑了一遍被测路径——**自己跟自己比，测试空转且全绿**。
//! 故 `via_cte` 现在要求显式传表名。

use qsdb::{Qsdb, QsdbOptions};

/// 测试目录唯一序号：pid 挡跨进程碰撞，序号挡同进程内并发（Windows 时钟
/// 粒度 ~100ns，纳秒后缀在同 tick 内会重复——CI 上真炸过，见 constraints）。
fn _uniq_seq() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    SEQ.fetch_add(1, Ordering::Relaxed)
}

fn tmp(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join("qsdb_join_agg_par").join(format!(
        "{tag}_{:x}_{}",
        std::process::id(),
        _uniq_seq()
    ))
}

fn open(tag: &str) -> (Qsdb, std::path::PathBuf) {
    let root = tmp(tag);
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    (db, root)
}

/// 逐行渲染结果（`a|b|…`）。
fn rows(db: &Qsdb, sql: &str) -> Vec<String> {
    let bs = db.sql_collect(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    let mut out = Vec::new();
    for b in &bs {
        for i in 0..b.num_rows() {
            let cells: Vec<String> = (0..b.num_columns())
                .map(|c| {
                    let a = b.column(c);
                    if a.is_null(i) {
                        "NULL".into()
                    } else {
                        arrow::util::display::array_value_to_string(a.as_ref(), i).unwrap()
                    }
                })
                .collect();
            out.push(cells.join("|"));
        }
    }
    out.sort();
    out
}

/// **分片要生效，表必须多段**（`Scan::shard` 在文件数 < 2 时返回 None）。
/// 每次 `INSERT` 一次提交 = 一个段，故刻意分 4 次插。
fn seed_t(db: &Qsdb) {
    db.sql("CREATE TABLE t (id BIGINT, k BIGINT, v BIGINT)")
        .unwrap();
    for i in 0..4 {
        let lo = i * 250;
        db.sql(&format!(
            "INSERT INTO t VALUES {}",
            (lo..lo + 250)
                .map(|n| format!("({n},{},{} )", n % 4, n * 10))
                .collect::<Vec<_>>()
                .join(",")
        ))
        .unwrap();
    }
}

fn seed_d(db: &Qsdb) {
    db.sql("CREATE TABLE d (k BIGINT, cat STRING)").unwrap();
    db.sql("INSERT INTO d VALUES (0,'a'),(1,'b'),(2,'c'),(3,'d'),(9,'z')")
        .unwrap();
}

/// 与分片路径**同语义的串行基准**：把表包进 CTE（`LogicalPlan::Values`），
/// `try_shard_source` 到不了底 ⇒ 必走单节点 `HashAggExec`。
/// **同一台机上同一份数据**，是这条路径的天然对照组。
///
/// `table` 显式传入并整体替换 `FROM <table> `——早先按 `"FROM t "` 做子串替换，
/// 对 `t2` 根本不匹配（`FROM t2 ` 里没有 `FROM t `），于是"对照组"其实是
/// 又跑了一遍被测路径，断言变成自己跟自己比（**空转**）。名字要显式给。
fn via_cte(sql: &str, table: &str) -> String {
    sql.replace(
        &format!("FROM {table} "),
        &format!("FROM (WITH _s AS (SELECT * FROM {table}) SELECT * FROM _s) {table} "),
    )
}

/// INNER join + GROUP BY：分片结果必须与串行逐行一致，且值对。
#[test]
fn inner_join_agg_sharded_matches_serial() {
    let (db, root) = open("inner");
    seed_t(&db);
    seed_d(&db);
    let sql = "SELECT d.cat, count(*) c, sum(t.v) s FROM t JOIN d ON t.k = d.k \
               GROUP BY d.cat ORDER BY d.cat";
    let sharded = rows(&db, sql);
    let serial = rows(&db, &via_cte(sql, "t"));
    assert_eq!(sharded, serial, "分片与串行的结果必须逐行一致");
    // 1000 行按 k = id%4 均分：a/b/c/d 各 250 行；v = id*10
    assert_eq!(sharded.len(), 4, "d 里 k=9 无匹配 ⇒ 只有 4 组：{sharded:?}");
    for r in &sharded {
        let f: Vec<&str> = r.split('|').collect();
        assert_eq!(f[1], "250", "每组 250 行：{r}");
    }
    std::fs::remove_dir_all(&root).ok();
}

/// LEFT OUTER：未匹配的左行**每片各发射一次**，不许重复也不许丢。
#[test]
fn left_join_agg_sharded_matches_serial() {
    let (db, root) = open("left");
    seed_t(&db);
    // 右表只覆盖 k ∈ {0,1} ⇒ k ∈ {2,3} 的左行未匹配
    db.sql("CREATE TABLE d2 (k BIGINT, cat STRING)").unwrap();
    db.sql("INSERT INTO d2 VALUES (0,'a'),(1,'b')").unwrap();
    let sql = "SELECT d2.cat, count(*) c FROM t LEFT JOIN d2 ON t.k = d2.k \
               GROUP BY d2.cat ORDER BY d2.cat";
    let sharded = rows(&db, sql);
    let serial = rows(&db, &via_cte(sql, "t"));
    assert_eq!(sharded, serial, "LEFT 的未匹配行也必须逐行一致");
    // 未匹配组是 NULL（`d2.cat` 为 NULL），且行数 = 1000 - 500 = 500
    assert!(
        sharded.iter().any(|r| r.starts_with("NULL|")),
        "应有未匹配组：{sharded:?}"
    );
    let null_rows: i64 = sharded
        .iter()
        .find(|r| r.starts_with("NULL|"))
        .map(|r| r.split('|').nth(1).unwrap().parse().unwrap())
        .unwrap();
    assert_eq!(null_rows, 500, "k∈{{2,3}} 各 250 行未匹配：{sharded:?}");
    std::fs::remove_dir_all(&root).ok();
}

/// 三表链（`complex_three_way` 的形状）：跨两级 join 仍要与串行一致。
#[test]
fn three_way_chain_sharded_matches_serial() {
    let (db, root) = open("three");
    seed_t(&db);
    seed_d(&db);
    db.sql("CREATE TABLE d2 (cat STRING, alias STRING)")
        .unwrap();
    db.sql("INSERT INTO d2 VALUES ('a','A'),('b','B'),('c','C'),('d','D')")
        .unwrap();
    let sql = "SELECT d2.alias, count(*) c FROM t JOIN d ON t.k = d.k \
               JOIN d2 ON d.cat = d2.cat GROUP BY d2.alias ORDER BY d2.alias";
    let sharded = rows(&db, sql);
    let serial = rows(&db, &via_cte(sql, "t"));
    assert_eq!(sharded, serial, "两级 join 的分片必须与串行一致");
    assert_eq!(sharded.len(), 4, "四个别名各一组：{sharded:?}");
    std::fs::remove_dir_all(&root).ok();
}

/// **投影错位绊线**：join 键**不是首列**。
///
/// 若哪天有人给 INNER/LEFT 建臂也加上键列投影下推（很自然的优化），而分片
/// 没有按当前调用点的投影重建，键列序号就会落到外层列序的别的列上 ——
/// 连不上 ⇒ 结果**凭空变空，且不报错**。这条测试就是那根绊线。
///
/// ⚠️ 诚实说明：**今天它不会红**——INNER/LEFT 建臂对 probe 传的是 `None`（不做
/// 键列下推），故分片投影恒等于 `None`，两边天然一致。它的价值在"将来"，
/// 以及把"非首列键 + 分片"这条组合的正确性**钉死**。
/// 配套的兜底是 `Scan::with_projection`（那边注释里有反向验证的记录）。
#[test]
fn key_column_projection_does_not_shift() {
    let (db, root) = open("projfix");
    // 键 k 在**第二列**：错位后 index 0 会是 a（10/20/30，与 d.k 无交集）⇒ 空结果
    db.sql("CREATE TABLE t2 (a BIGINT, k BIGINT, v BIGINT)")
        .unwrap();
    for chunk in [0i64, 200] {
        db.sql(&format!(
            "INSERT INTO t2 VALUES {}",
            (chunk..chunk + 200)
                .map(|n| format!("({}, {}, {})", n + 10, n % 4, n))
                .collect::<Vec<_>>()
                .join(",")
        ))
        .unwrap();
    }
    db.sql("CREATE TABLE d (k BIGINT, cat STRING)").unwrap();
    db.sql("INSERT INTO d VALUES (0,'a'),(1,'b'),(2,'c'),(3,'d')")
        .unwrap();
    let sql = "SELECT d.cat, count(*) c, sum(t2.v) s FROM t2 JOIN d ON t2.k = d.k \
               GROUP BY d.cat ORDER BY d.cat";
    let got = rows(&db, sql);
    assert!(!got.is_empty(), "键列序号错位会连不上 ⇒ 结果为空：{got:?}");
    assert_eq!(got, rows(&db, &via_cte(sql, "t2")), "与串行一致");
    let total: i64 = got
        .iter()
        .map(|r| r.split('|').nth(1).unwrap().parse::<i64>().unwrap())
        .sum();
    assert_eq!(total, 400, "全部 400 行都要连上：{got:?}");
    std::fs::remove_dir_all(&root).ok();
}

/// 分片路径**不该**吞掉 `GROUP BY` 之外的语义：`HAVING`、聚合表达式照旧正确。
#[test]
fn sharded_path_keeps_having_and_expr_semantics() {
    let (db, root) = open("having");
    seed_t(&db);
    seed_d(&db);
    let sql = "SELECT d.cat, count(*) c FROM t JOIN d ON t.k = d.k \
               GROUP BY d.cat HAVING count(*) > 300 ORDER BY d.cat";
    assert_eq!(
        rows(&db, sql),
        Vec::<String>::new(),
        "每组 250 行，`HAVING > 300` 应全部滤掉"
    );
    let sql2 = "SELECT d.cat, count(*) c FROM t JOIN d ON t.k = d.k \
                GROUP BY d.cat HAVING count(*) >= 250 ORDER BY d.cat";
    assert_eq!(rows(&db, sql2).len(), 4, "`>= 250` 应留下 4 组");
    assert_eq!(rows(&db, sql2), rows(&db, &via_cte(sql2, "t")));
    std::fs::remove_dir_all(&root).ok();
}
