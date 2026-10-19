//! **J13 回归**（qsdb-jdbc 反馈）：`count(*)` 的 O(1) 元数据短路吞掉客户端投影。
//!
//! 短路（`sql::metadata_shortcut_count`）为了 O(1) 应答会**剥掉顶层 Project**，
//! 直接从 manifest 取行数——于是客户端投影里的三件事全被无视：
//!
//! 1. **别名**：`SELECT COUNT(*) AS n` 透出 `count(*)` 而非 `n`
//!    （JDBC `findColumn("n")` / DBeaver 列头 / 帆软字段绑定全按别名取值 → 全挂）；
//! 2. **多余列**：`SELECT COUNT(*), 1 AS k` **只回一列**——`k` 被静默吞掉；
//! 3. **表达式**：`SELECT COUNT(*) + 1 AS n` 回的是**原始 count**——`+1` 被静默忽略。
//!
//! 2/3 是"用户拿到错东西还不报错"，比别名丢失更严重（J13 只报了第 1 条）。
//! 修法：短路**只认「恰好一个裸列引用」的投影**，其余一律让开走常规路径；
//! 输出列名随客户端别名走。
//!
//! 覆盖范围：短路只在「无 GROUP BY + 唯一聚合 `count(*)` + 谓词仅引用分区列
//! （或无分区表且无谓词）」时生效——所以本文件的用例都刻意构造成**能命中短路**的形状，
//! 否则测的是别的路径（那是"用例空转"）。

use qsdb::{Qsdb, QsdbOptions};

/// 测试目录唯一序号：pid 挡跨进程碰撞，序号挡同进程内并发（Windows 时钟
/// 粒度 ~100ns，纳秒后缀在同 tick 内会重复——CI 上真炸过，见 constraints）。
fn _uniq_seq() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    SEQ.fetch_add(1, Ordering::Relaxed)
}

fn setup(tag: &str) -> (Qsdb, std::path::PathBuf) {
    let root = std::env::temp_dir()
        .join("qsdb_agg_alias_test")
        .join(format!("{tag}_{:x}_{}", std::process::id(), _uniq_seq()));
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    (db, root)
}

/// 列名 + 首行值（两样都要：别名是名字问题，吞列/吞表达式是**值**问题）。
fn cols_and_vals(db: &Qsdb, sql: &str) -> (Vec<String>, Vec<String>) {
    let bs = db
        .sql_collect(sql)
        .unwrap_or_else(|e| panic!("{sql} 失败: {e}"));
    let names: Vec<String> = bs
        .first()
        .map(|b| {
            b.schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect()
        })
        .unwrap_or_default();
    let mut vals = Vec::new();
    if let Some(b) = bs.first() {
        if b.num_rows() > 0 {
            for c in 0..b.num_columns() {
                vals.push(
                    arrow::util::display::array_value_to_string(b.column(c).as_ref(), 0).unwrap(),
                );
            }
        }
    }
    (names, vals)
}

fn seed(db: &Qsdb) {
    db.sql("CREATE TABLE t (id BIGINT, name STRING)").unwrap();
    db.sql("INSERT INTO t VALUES (1,'a'),(2,'b')").unwrap();
    // 分区表：谓词仅分区列 → 短路同样命中（另一条入口）
    db.sql("CREATE TABLE p (dt DATE, v BIGINT) PARTITIONED BY (dt)")
        .unwrap();
    db.sql("INSERT INTO p VALUES (DATE '2026-01-01', 1), (DATE '2026-01-02', 2)")
        .unwrap();
}

/// J13 第 1 条：聚合列别名必须透出。
#[test]
fn count_star_alias_is_visible() {
    let (db, _root) = setup("alias");
    seed(&db);

    let (names, vals) = cols_and_vals(&db, "SELECT COUNT(*) AS n FROM t");
    assert_eq!(names, vec!["n"], "聚合别名必须透出（J13）");
    assert_eq!(vals, vec!["2"]);

    // 无别名：列名回显客户端写法（与大写无关，两条路径口径一致）
    let (names, _) = cols_and_vals(&db, "SELECT count(*) FROM t");
    assert_eq!(names, vec!["count(*)"]);

    // 分区表 + 纯分区谓词（短路的另一入口）
    let (names, vals) = cols_and_vals(
        &db,
        "SELECT COUNT(*) AS n FROM p WHERE dt = DATE '2026-01-01'",
    );
    assert_eq!(names, vec!["n"]);
    assert_eq!(vals, vec!["1"]);

    // 别名同时是 ORDER BY/后续引用的名字时也要一致
    let (names, _) = cols_and_vals(&db, "SELECT COUNT(*) AS n, 1 AS k FROM t");
    assert_eq!(names, vec!["n", "k"]);
}

/// J13 第 2 条（**比反馈更严重**）：投影里的多余列不得被静默吞掉。
#[test]
fn extra_projected_columns_are_not_swallowed() {
    let (db, _root) = setup("extra");
    seed(&db);

    let (names, vals) = cols_and_vals(&db, "SELECT COUNT(*), 1 AS k FROM t");
    assert_eq!(names.len(), 2, "多列投影不能被短路吞成一列：{names:?}");
    assert_eq!(vals, vec!["2", "1"]);

    let (names, vals) = cols_and_vals(&db, "SELECT COUNT(*) AS n, 'x' AS tag FROM t");
    assert_eq!(names, vec!["n", "tag"]);
    assert_eq!(vals, vec!["2", "x"]);
}

/// J13 第 3 条（**最严重：静默错值**）：投影表达式必须真的被执行。
#[test]
fn projected_expression_over_count_is_evaluated() {
    let (db, _root) = setup("expr");
    seed(&db);

    let (names, vals) = cols_and_vals(&db, "SELECT COUNT(*) + 1 AS n FROM t");
    assert_eq!(names, vec!["n"]);
    assert_eq!(vals, vec!["3"], "`+1` 不能被短路忽略（曾回原始 2）");

    let (_, vals) = cols_and_vals(&db, "SELECT COUNT(*) * 10 AS n FROM t");
    assert_eq!(vals, vec!["20"]);

    // 分区表短路入口上同理
    let (_, vals) = cols_and_vals(
        &db,
        "SELECT COUNT(*) + 100 AS n FROM p WHERE dt = DATE '2026-01-01'",
    );
    assert_eq!(vals, vec!["101"]);
}

/// 短路该命中时仍要**命中**（别为了修别名把 O(1) 优化顺手关掉）。
///
/// 可观察证据：**分区表 + 纯分区谓词**的短路只读 `partitions.idx` / 分区 manifest，
/// **不碰 parquet 段文件**。把段文件改名藏起来——短路查询仍应答得出来，而需要真读
/// 数据的查询必须失败。（先前用"搬目录"做观察是错的：**无分区表**的短路走 parquet
/// footer，本来就碰文件；口径不同，观察方式就不能一样。）
#[test]
fn metadata_shortcut_still_fires_for_the_plain_shape() {
    let (db, root) = setup("shortcut");
    seed(&db);

    // 基线
    let (_, vals) = cols_and_vals(
        &db,
        "SELECT COUNT(*) AS n FROM p WHERE dt = DATE '2026-01-01'",
    );
    assert_eq!(vals, vec!["1"]);

    // 藏起所有 parquet 段文件（manifest / idx 留着）
    let pdir = root.join("default").join("p");
    let mut hidden = Vec::new();
    for entry in walk(&pdir) {
        if entry.extension().map(|e| e == "parquet").unwrap_or(false) {
            let bak = entry.with_extension("parquet.bak");
            if std::fs::rename(&entry, &bak).is_ok() {
                hidden.push((entry, bak));
            }
        }
    }
    assert!(!hidden.is_empty(), "前置：应能找到段文件（否则本用例空转）");

    // 短路：只读 manifest / idx → 仍回 1（且带别名）
    let (names, vals) = cols_and_vals(
        &db,
        "SELECT COUNT(*) AS n FROM p WHERE dt = DATE '2026-01-01'",
    );
    assert_eq!(
        (names, vals),
        (vec!["n".to_string()], vec!["1".to_string()]),
        "短路应只读 manifest / idx——段文件不在也应答得出来（顺带证明路径真被走到）"
    );
    // 对照：要真读数据的查询此时必须失败（证明上一条不是"缓存了结果"）
    assert!(
        db.sql_collect("SELECT sum(v) FROM p").is_err(),
        "读段文件的查询在段文件缺失时必须失败——否则上面的断言毫无意义"
    );

    for (orig, bak) in hidden {
        let _ = std::fs::rename(&bak, &orig);
    }
}

/// 递归列出目录下的文件。
fn walk(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    let Ok(rd) = std::fs::read_dir(dir) else {
        return out;
    };
    for e in rd.flatten() {
        let p = e.path();
        if p.is_dir() {
            out.extend(walk(&p));
        } else {
            out.push(p);
        }
    }
    out
}
