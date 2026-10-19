//! **多值 `IN` 的下推**（2026-10-17）。
//!
//! ## 为什么这条值得单独立回归
//!
//! `x IN (a, b, c)` 在绑定期被展开成 **`x=a OR x=b OR x=c`**。而谓词下推拆分
//! (`translate::split_pushdown`) 此前**只认 `Cmp` 和 `And`** —— `Or` 直接掉进兜底，
//! 于是**多值 IN 完全不下推**：分区裁剪、段 min/max 裁剪、row-group 裁剪
//! **三级全都拿不到它**。（单值 `IN ('x')` 因为只有一个元素、压根不生成 `Or`，
//! 退化成 `Eq`，所以一直是对的 —— 语料里绝大多数正是单值，把这个洞盖住了。）
//!
//! 真实语料那批算法叫"**多日**"：一次要算很多天 ⇒ `PDATE IN (d1, d2, …)` 是常态写法，
//! 所以这个洞很值得补。
//!
//! ## ⚠️ 这里最容易写错的地方：`Or` **不能半推**
//!
//! `And` 与 `Or` 的规则**刻意不同**：
//!
//! | | 规则 |
//! |---|---|
//! | `And(a, b)` | **一侧能推就推一侧**，另一侧留在行过滤（两个条件"都要满足"，推一个不改变结果）|
//! | `Or(a, b)`  | **必须两侧都能推、且都没有残余**才整体下推。只推一侧会**多滤掉行** —— 另一分支满足的那些行本该留下 |
//!
//! 所以下面的测试重点是**对撞**：带下推的写法与"把 IN 改写成等价的 `OR` 显式条件、
//! 逼它走行过滤"的写法，**逐值必须一致**。

use qsdb::{Qsdb, QsdbOptions};

/// 测试目录唯一序号：pid 挡跨进程碰撞，序号挡同进程内并发（Windows 时钟
/// 粒度 ~100ns，纳秒后缀在同 tick 内会重复——CI 上真炸过，见 constraints）。
fn _uniq_seq() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    SEQ.fetch_add(1, Ordering::Relaxed)
}

fn setup(tag: &str) -> (Qsdb, std::path::PathBuf) {
    let root = std::env::temp_dir().join("qsdb_in_pushdown").join(format!(
        "{tag}_{}_{}",
        std::process::id(),
        format!("{:x}_{}", std::process::id(), _uniq_seq())
    ));
    std::fs::create_dir_all(&root).unwrap();
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    (db, root)
}

fn rows(db: &Qsdb, sql: &str) -> Vec<String> {
    let bs = db.sql_collect(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    if bs.is_empty() {
        return vec![];
    }
    let b = &bs[0];
    (0..b.num_rows())
        .map(|i| {
            (0..b.num_columns())
                .map(|c| {
                    arrow::util::display::array_value_to_string(b.column(c).as_ref(), i).unwrap()
                })
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

/// 造一张**多段**表：1000 行、`k` 从 0..999 —— 段裁剪/row-group 裁剪才有东西可裁。
fn seed(db: &Qsdb) {
    db.sql("CREATE TABLE t (k BIGINT, s VARCHAR, d DATE)")
        .unwrap();
    // 分段批量插，制造多个 segment
    for chunk in 0..10 {
        let vals: Vec<String> = (0..100)
            .map(|i| {
                let k = chunk * 100 + i;
                format!("({k}, 'v{k}', DATE '2026-01-{:02}')", k % 28 + 1)
            })
            .collect();
        db.sql(&format!("INSERT INTO t VALUES {}", vals.join(", ")))
            .unwrap();
    }
}

/// ★ **本体**：多值 `IN` 的结果必须与"显式 OR"逐值相同。
/// 判据刻意选"两路对撞" —— 只测一路的话，**半推**（丢行）这种 bug 测不出来。
#[test]
fn 多值_in_与显式_or_逐值相同() {
    let (db, _r) = setup("multi");
    seed(&db);

    // 3 个值分散在不同段（0-99 / 300-399 / 900-999）
    let got = rows(&db, "SELECT k FROM t WHERE k IN (7, 350, 999) ORDER BY k");
    let want = rows(
        &db,
        "SELECT k FROM t WHERE k = 7 OR k = 350 OR k = 999 ORDER BY k",
    );
    assert_eq!(got, want);
    assert_eq!(got, vec!["7", "350", "999"]);

    // 大列表
    let got = rows(
        &db,
        "SELECT count(*) FROM t WHERE k IN (1,2,3,4,5,105,205,305)",
    );
    let want = rows(
        &db,
        "SELECT count(*) FROM t WHERE k=1 OR k=2 OR k=3 OR k=4 OR k=5 OR k=105 OR k=205 OR k=305",
    );
    assert_eq!(got, want);
    assert_eq!(got, vec!["8"]);

    // 全都不匹配
    assert_eq!(
        rows(&db, "SELECT count(*) FROM t WHERE k IN (-1, -2, -3)"),
        vec!["0"]
    );
    // 命中边界
    assert_eq!(
        rows(&db, "SELECT count(*) FROM t WHERE k IN (0, 999)"),
        vec!["2"]
    );
}

/// 字符串列的多值 `IN`（走同一套 `Or`）。
#[test]
fn 字符串多值_in() {
    let (db, _r) = setup("str");
    seed(&db);
    let got = rows(
        &db,
        "SELECT count(*) FROM t WHERE s IN ('v1','v500','v999')",
    );
    let want = rows(
        &db,
        "SELECT count(*) FROM t WHERE s='v1' OR s='v500' OR s='v999'",
    );
    assert_eq!(got, want);
    assert_eq!(got, vec!["3"]);
}

/// ★ **半推防线**：`Or` 一侧能下推、另一侧**推不动**（这里是函数调用）时，
/// **整体不许下推** —— 否则另一分支的行会被静默滤掉。
#[test]
fn or_一侧推不动时整体不下推() {
    let (db, _r) = setup("half");
    seed(&db);
    // `length(s) = 4` 不是"列 op 字面量" ⇒ 推不动 ⇒ 整个 Or 必须留在行过滤
    let got = rows(
        &db,
        "SELECT count(*) FROM t WHERE k IN (1, 2, 3) OR length(s) = 4",
    );
    let want = rows(
        &db,
        "SELECT count(*) FROM t WHERE k=1 OR k=2 OR k=3 OR length(s)=4",
    );
    assert_eq!(got, want);
    // s 是 'v0'..'v999'：length=4 的是 v100..v999 = **900** 行；
    // 而 k∈{1,2,3} 对应 s='v1','v2','v3'（长度 2）⇒ **不在**上面那 900 行里 ⇒ 并集 903。
    // （我第一版把它写成 900、漏加了这 3 行 —— 引擎是对的，错的是期望值。）
    assert_eq!(got, vec!["903"]);
}

/// `IN` 与 `AND` 混用：`And` 那一侧**可以**只推一半（与 `Or` 规则不同）。
#[test]
fn in_与_and_混用() {
    let (db, _r) = setup("andmix");
    seed(&db);
    let got = rows(
        &db,
        "SELECT count(*) FROM t WHERE k IN (1,2,3,4,5,6,7,8,9,10) AND k > 5",
    );
    assert_eq!(got, vec!["5"]); // 6..10
    // 下推与不下推对撞
    let want = rows(
        &db,
        "SELECT count(*) FROM t WHERE (k=1 OR k=2 OR k=3 OR k=4 OR k=5 OR k=6 OR k=7 OR k=8 OR k=9 OR k=10) AND k > 5",
    );
    assert_eq!(got, want);
}

/// 分区表上的多值 `IN` —— 这条直接对应"多日"算法：
/// `pdate IN (d1, d2)` 应该能**裁掉没提到的日期分区**，且结果正确。
#[test]
fn 分区列多值_in_结果正确() {
    let (db2, _r2) = setup("part2");
    db2.sql("CREATE TABLE p (v BIGINT, d DATE) PARTITIONED BY (d)")
        .unwrap();
    for day in 1..=5 {
        db2.sql(&format!(
            "INSERT INTO p VALUES (1, DATE '2026-01-{:02}'), (2, DATE '2026-01-{:02}')",
            day, day
        ))
        .unwrap();
    }
    // 多值 IN 命中 2 个分区
    assert_eq!(
        rows(
            &db2,
            "SELECT count(*) FROM p WHERE d IN (DATE '2026-01-02', DATE '2026-01-04')"
        ),
        vec!["4"]
    );
    // 与显式 OR 对撞
    assert_eq!(
        rows(
            &db2,
            "SELECT count(*) FROM p WHERE d = DATE '2026-01-02' OR d = DATE '2026-01-04'"
        ),
        vec!["4"]
    );
    // 不存在的日期 ⇒ 0 行（分区被裁光）
    assert_eq!(
        rows(
            &db2,
            "SELECT count(*) FROM p WHERE d IN (DATE '2026-09-09', DATE '2026-09-10')"
        ),
        vec!["0"]
    );
    // 全选
    assert_eq!(rows(&db2, "SELECT count(*) FROM p"), vec!["10"]);
}
