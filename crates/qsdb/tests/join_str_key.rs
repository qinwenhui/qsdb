//! **单 UTF8 join 键的零拷贝索引**（2026-10-17，`join 字符串键零拷贝设计稿`）。
//!
//! 建侧不复制键字节（索引只存「字节哈希 → 全局行号」），probe 命中后与
//! 建侧箭头缓冲原地比字节。回归钉住四条语义：
//! ① 哈希碰撞必须比真值（重复键命中全部对）；② NULL 键不匹配；
//! ③ 空串是合法键（参与匹配）；④ LEFT 的未匹配行补 NULL 保留。

use qsdb::{Qsdb, QsdbOptions};

fn setup(tag: &str) -> (Qsdb, std::path::PathBuf) {
    let root = std::env::temp_dir().join("qsdb_join_str").join(format!(
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

fn rows(db: &Qsdb, sql: &str) -> Vec<String> {
    let bs = db.sql_collect(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    let mut out = Vec::new();
    for b in &bs {
        for i in 0..b.num_rows() {
            let cells: Vec<String> = (0..b.num_columns())
                .map(|c| {
                    arrow::util::display::array_value_to_string(b.column(c).as_ref(), i).unwrap()
                })
                .collect();
            out.push(cells.join("|"));
        }
    }
    out.sort();
    out
}

/// 基础对撞：字符串键 INNER 与手写等值对撞 + 重复键命中**全部**对
/// （桶内多 id 逐个比真值 —— 哈希碰撞正确性的日常 exercising）。
#[test]
fn 字符串键_inner_与语义对撞() {
    let (db, _r) = setup("inner");
    db.sql("CREATE TABLE t (k VARCHAR, v INT)").unwrap();
    db.sql("CREATE TABLE d (k VARCHAR, w VARCHAR)").unwrap();
    // 重复键两侧都有：'a' 各 2 行 ⇒ 2×2 = 4 对
    db.sql("INSERT INTO t VALUES ('a', 1), ('a', 2), ('b', 3), (NULL, 9), ('', 8)")
        .unwrap();
    db.sql("INSERT INTO d VALUES ('a', 'x'), ('a', 'y'), ('c', 'z'), ('', 'e')")
        .unwrap();

    // INNER：a(2 行) × a(2 行) = 4 对；'' 匹配 '' = 1 对；NULL 不匹配
    let got = rows(&db, "SELECT t.v, d.w FROM t JOIN d ON t.k = d.k");
    assert_eq!(
        got,
        vec!["1|x", "1|y", "2|x", "2|y", "8|e"],
        "重复键要命中全部对；NULL 不匹配；空串要匹配"
    );
}

/// LEFT：未匹配行保留、build 段补 NULL；NULL 键同样保留。
#[test]
fn 字符串键_left_未匹配补_null() {
    let (db, _r) = setup("left");
    db.sql("CREATE TABLE t (k VARCHAR, v INT)").unwrap();
    db.sql("CREATE TABLE d (k VARCHAR, w VARCHAR)").unwrap();
    db.sql("INSERT INTO t VALUES ('a', 1), ('b', 2), (NULL, 3)")
        .unwrap();
    db.sql("INSERT INTO d VALUES ('a', 'x')").unwrap();
    let got = rows(&db, "SELECT t.v, d.w FROM t LEFT JOIN d ON t.k = d.k");
    assert_eq!(got, vec!["1|x", "2|", "3|"]);
}

/// **空串是合法键**：`'' = ''` 参与匹配（不能被当成 NULL/跳过）。
#[test]
fn 空串键参与匹配() {
    let (db, _r) = setup("empty");
    db.sql("CREATE TABLE t (k VARCHAR, v INT)").unwrap();
    db.sql("CREATE TABLE d (k VARCHAR, tag VARCHAR)").unwrap();
    db.sql("INSERT INTO t VALUES ('', 1), ('x', 2)").unwrap();
    db.sql("INSERT INTO d VALUES ('', 'E'), ('x', 'X')")
        .unwrap();
    let got = rows(&db, "SELECT t.v, d.tag FROM t JOIN d ON t.k = d.k");
    assert_eq!(got, vec!["1|E", "2|X"]);
}

/// **三表链**（complex_three_way 的形状）：第二段 join 也是字符串键 ——
/// 零拷贝索引在链式 join 上逐值正确。
#[test]
fn 三表链_第二段字符串键() {
    let (db, _r) = setup("chain");
    db.sql("CREATE TABLE t (g BIGINT, v INT)").unwrap();
    db.sql("CREATE TABLE d (g BIGINT, cat VARCHAR)").unwrap();
    db.sql("CREATE TABLE d2 (cat VARCHAR, tag VARCHAR)")
        .unwrap();
    db.sql("INSERT INTO t VALUES (1, 10), (1, 11), (2, 20), (3, 30)")
        .unwrap();
    db.sql("INSERT INTO d VALUES (1, 'A'), (2, 'B')").unwrap();
    db.sql("INSERT INTO d2 VALUES ('A', 'a1'), ('A', 'a2'), ('C', 'c')")
        .unwrap();
    let got = rows(
        &db,
        "SELECT d2.tag, count(*) c FROM t JOIN d ON t.g = d.g JOIN d2 ON d.cat = d2.cat \
         GROUP BY d2.tag ORDER BY d2.tag",
    );
    // g=1 两行 → cat A → A 有 2 个 d2 行 ⇒ tag a1×2、a2×2；g=2 → B 无 d2 行消失
    assert_eq!(got, vec!["a1|2", "a2|2"]);
}

/// 键值多样性大一点（1 万行不同串 + 少量重复）：哈希碰撞路径的日常 exercising。
#[test]
fn 高基数字符串键_计数正确() {
    let (db, _r) = setup("card");
    db.sql("CREATE TABLE t (k VARCHAR, v INT)").unwrap();
    db.sql("CREATE TABLE d (k VARCHAR, w INT)").unwrap();
    let mut vals = String::new();
    for i in 0..10000u32 {
        vals.push_str(&format!(",('k{}', {})", i, i));
    }
    db.sql(&format!(
        "INSERT INTO t VALUES {}",
        vals.trim_start_matches(',')
    ))
    .unwrap();
    let mut vals = String::new();
    for i in (0..10000u32).step_by(2) {
        vals.push_str(&format!(",('k{}', {})", i, i * 10));
    }
    db.sql(&format!(
        "INSERT INTO d VALUES {}",
        vals.trim_start_matches(',')
    ))
    .unwrap();
    let got = rows(&db, "SELECT count(*) FROM t JOIN d ON t.k = d.k");
    assert_eq!(got, vec!["5000"], "偶数键一一配对 = 5000 对");
}
