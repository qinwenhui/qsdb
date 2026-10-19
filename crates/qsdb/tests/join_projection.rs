//! **join 输出投影下推**（P1⑥）的正确性回归。
//!
//! 背景（2026-10-11 实测）：`join_batch` 对**每一行**都 `take` **全部 probe 列 +
//! 全部 build 列**。基准那句 `t×d + GROUP BY` 产出 **1000 万行 × 10 列**（含 4 个
//! 字符串列），而聚合**只读 2 列** —— 单线程 1.27s 里约 **0.85s（67%）** 白花。
//! 等价的列宽实验：输出 10 列 → 5 列，**时间直接减半**（1.33s → 0.63s）。
//!
//! 现在 `Aggregate` 之下的 join 链会被标注"**只需物化这几个位置**"，
//! 其余位置由 join **填 NULL**（列还在、schema 不变、索引一概不动）。
//!
//! ## 为什么这个优化特别危险
//!
//! **判错一列 = 那一列读出 NULL = 静默错值**，不报错、不打日志。
//! 所以每条断言都不是"跑通就行"，而是**逐值对照一条语义相同、但走不到这条优化
//! 路径的计划**：把 join 包进**派生表**（`FROM (SELECT …) y`），计划形状就成了
//! `Aggregate(Project(Join))` —— 标注遇 `Project` **就地停手**。同一条 SQL 的两种
//! 写法，一个优化一个不优化，结果必须逐行一致。
//!
//! ⚠️ 若哪天这个"对照"也走优化路径了，这些测试就退化成自己跟自己比（空转）。
//! 故每组都配**字面量期望值**，不单靠对照。
//! （`QSDB_JOIN_PROF=1` 会打出 `[join-proj] 输出列 10 → 只物化 N`，
//! 想确认"到底哪一边被优化了"就开它。）
//!
//! ⚠️ 写这些 SQL 时踩到的**既有绑定器缺口**（与本次优化无关，但会误导人）：
//! `GROUP BY` / `ORDER BY` 里**不认限定名**——`GROUP BY t.g`、`GROUP BY y.cat`
//! 都报 "expression CompoundIdentifier … over aggregates not supported"。
//! 左表以裸名注册（`裸名 = 左输入`），派生表的列同理。故下面 GROUP BY 一律裸名。

use qsdb::{Qsdb, QsdbOptions};

/// 测试目录唯一序号：pid 挡跨进程碰撞，序号挡同进程内并发（Windows 时钟
/// 粒度 ~100ns，纳秒后缀在同 tick 内会重复——CI 上真炸过，见 constraints）。
fn _uniq_seq() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    SEQ.fetch_add(1, Ordering::Relaxed)
}

fn tmp(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join("qsdb_join_proj").join(format!(
        "{tag}_{}_{}",
        std::process::id(),
        format!("{:x}_{}", std::process::id(), _uniq_seq())
    ))
}

fn open(tag: &str) -> (Qsdb, std::path::PathBuf) {
    let root = tmp(tag);
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    (db, root)
}

/// 结果按行渲染（列内 `|` 连，行间排序）。
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

/// 两条写照必须逐行一致：直写（走优化）vs 派生表（不走）。
fn both(db: &Qsdb, direct: &str, derived: &str) -> Vec<String> {
    let a = rows(db, direct);
    let b = rows(db, derived);
    assert_eq!(
        a, b,
        "优化路径与未优化路径结果不一致！\n 直写: {direct}\n 派生: {derived}"
    );
    a
}

/// 造事实表 `t`（7 列，**分 4 段** ⇒ 分片路径也生效）+ 维表 `d`（3 列，**每键两行**）。
fn seed(db: &Qsdb) {
    db.sql(
        "CREATE TABLE t (id BIGINT, g BIGINT, name STRING, v BIGINT, x DOUBLE, s STRING, dt DATE)",
    )
    .unwrap();
    for chunk in 0..4i64 {
        let lo = chunk * 50;
        db.sql(&format!(
            "INSERT INTO t VALUES {}",
            (lo..lo + 50)
                .map(|i| format!(
                    "({i},{},'n{i}',{}, {}.5,'s{}',DATE '2022-01-01')",
                    i % 5,
                    i * 10,
                    i,
                    i % 3
                ))
                .collect::<Vec<_>>()
                .join(",")
        ))
        .unwrap();
    }
    // d：每个 g 两行（复现基准里的扇出）
    db.sql("CREATE TABLE d (g BIGINT, nm STRING, cat STRING)")
        .unwrap();
    db.sql(
        "INSERT INTO d VALUES (0,'a0','c0'),(0,'a0b','c0'),\
         (1,'a1','c1'),(1,'a1b','c1'),(2,'a2','c2'),(2,'a2b','c2'),\
         (3,'a3','c3'),(3,'a3b','c3'),(4,'a4','c4'),(4,'a4b','c4'),(9,'x','c9')",
    )
    .unwrap();
}

/// 全聚合族只读 `t.v` 一列，其余 9 列全被掩掉。
#[test]
fn aggregates_over_one_column_match_unoptimized() {
    let (db, root) = open("agg1");
    seed(&db);
    let got = both(
        &db,
        "SELECT d.cat, count(*) c, sum(t.v) s, min(t.v) mn, max(t.v) mx, avg(t.v) a \
         FROM t JOIN d ON t.g = d.g GROUP BY d.cat",
        "SELECT cat, count(*) c, sum(v) s, min(v) mn, max(v) mx, avg(v) a \
         FROM (SELECT d.cat AS cat, t.v AS v FROM t JOIN d ON t.g = d.g) y GROUP BY cat",
    );
    assert_eq!(
        got.len(),
        5,
        "g ∈ 0..4 都匹配 ⇒ c0..c4 共 5 组（c9 无匹配）：{got:?}"
    );
    for r in &got {
        let f: Vec<&str> = r.split('|').collect();
        assert_eq!(f[1], "80", "每组 40 行 ×2 扇出 = 80：{r}");
        // min(v) = 该组最小 id*10；max = 最大
        // min = g*10，max = (g+195)*10 ⇒ 差 1950
        assert_eq!(
            f[3].parse::<i64>().unwrap() + 1950,
            f[4].parse::<i64>().unwrap(),
            "min/max 必须取到该组的真实极值：{r}"
        );
    }
    // Σ v = Σ id*10 (id ∈ [0,200)) = 199000；每键 2 行扇出 ×2 = 398000
    let total: f64 = got
        .iter()
        .map(|r| r.split('|').nth(2).unwrap().parse::<f64>().unwrap())
        .sum();
    assert_eq!(total, 398000.0, "sum 总和（含 2× 扇出）：{got:?}");
    std::fs::remove_dir_all(&root).ok();
}

/// **分组列在右表**：`t` 侧 7 列里只有连接键 `g` 是必要的。若掩码把连接键也掩掉，
/// 探测就全空 ⇒ 结果凭空消失（最典型的判错形态）。
#[test]
fn group_by_right_column_keeps_probe_key() {
    let (db, root) = open("grp_right");
    seed(&db);
    let got = both(
        &db,
        "SELECT d.cat, count(*) c FROM t JOIN d ON t.g = d.g GROUP BY d.cat",
        "SELECT cat, count(*) c FROM (SELECT d.cat AS cat FROM t JOIN d ON t.g = d.g) y \
         GROUP BY cat",
    );
    assert_eq!(got.len(), 5, "{got:?}");
    assert!(
        got.iter().all(|r| r.split('|').nth(1) == Some("80")),
        "{got:?}"
    );
    std::fs::remove_dir_all(&root).ok();
}

/// **分组列在左表**：此时 `d` 侧只有键列必要。
#[test]
fn group_by_left_column_masks_build_columns() {
    let (db, root) = open("grp_left");
    seed(&db);
    let got = both(
        &db,
        "SELECT g, count(*) c, sum(x) sx FROM t JOIN d ON t.g = d.g GROUP BY g",
        "SELECT g, count(*) c, sum(x) sx FROM \
         (SELECT t.g AS g, t.x AS x FROM t JOIN d ON t.g = d.g) y GROUP BY g",
    );
    assert_eq!(got.len(), 5, "g ∈ 0..4 有匹配：{got:?}");
    for r in &got {
        let f: Vec<&str> = r.split('|').collect();
        assert_eq!(f[1], "80", "每组 40 行 ×2 扇出：{r}");
        // sum(x)：该组 40 个 id 的 (id+0.5) 之和 ×2 扇出
        let g: f64 = f[0].parse().unwrap();
        assert_eq!(f[2].parse::<f64>().unwrap(), 80.0 * g + 7840.0, "{r}");
    }
    std::fs::remove_dir_all(&root).ok();
}

/// **表达式跨两侧**（`v*2 + id` 全在左、`cat` 在右）：两侧的列都必须在掩码里。
#[test]
fn expression_spanning_both_sides() {
    let (db, root) = open("both_sides");
    seed(&db);
    both(
        &db,
        "SELECT d.cat, sum(v * 2 + id) s FROM t JOIN d ON t.g = d.g GROUP BY d.cat",
        "SELECT cat, sum(v * 2 + id) s FROM \
         (SELECT d.cat AS cat, t.v AS v, t.id AS id FROM t JOIN d ON t.g = d.g) y \
         GROUP BY cat",
    );
    std::fs::remove_dir_all(&root).ok();
}

/// **WHERE 在 join 之上**：谓词读的列（`x`，它既不在 GROUP BY 也不在聚合里）
/// 只有 `annotate_walk` 的 Filter 分支会把它带上 —— 漏了就是静默错行数。
#[test]
fn filter_above_join_keeps_its_columns() {
    let (db, root) = open("filter");
    seed(&db);
    let got = both(
        &db,
        "SELECT d.cat, count(*) c FROM t JOIN d ON t.g = d.g WHERE x < 100.0 GROUP BY d.cat",
        "SELECT cat, count(*) c FROM \
         (SELECT d.cat AS cat, t.x AS x FROM t JOIN d ON t.g = d.g) y \
         WHERE x < 100.0 GROUP BY cat",
    );
    // x = id + 0.5 ⇒ x < 100 ⇔ id < 100；每 id 命中 2 个 d 行 ⇒ 200
    let total: i64 = got
        .iter()
        .map(|r| r.split('|').nth(1).unwrap().parse::<i64>().unwrap())
        .sum();
    assert_eq!(total, 200, "id<100 共 100 行 ×2 扇出：{got:?}");
    std::fs::remove_dir_all(&root).ok();
}

/// **LEFT OUTER**：未匹配行右列必须仍是 NULL（掩码不许把"该 NULL"变成别的）。
#[test]
fn left_outer_unmatched_rows_stay_null() {
    let (db, root) = open("left");
    seed(&db);
    db.sql("CREATE TABLE dl (g BIGINT, cat STRING)").unwrap();
    db.sql("INSERT INTO dl VALUES (0,'z0'),(1,'z1')").unwrap();
    let got = both(
        &db,
        "SELECT dl.cat, count(*) c, sum(t.v) s FROM t LEFT JOIN dl ON t.g = dl.g GROUP BY dl.cat",
        "SELECT cat, count(*) c, sum(v) s FROM \
         (SELECT dl.cat AS cat, t.v AS v FROM t LEFT JOIN dl ON t.g = dl.g) y GROUP BY cat",
    );
    let null_row = got
        .iter()
        .find(|r| r.starts_with("NULL|"))
        .unwrap_or_else(|| panic!("应有未匹配组：{got:?}"));
    // g ∈ {2,3,4} 各 40 行 ⇒ 120 行未匹配
    assert_eq!(
        null_row.split('|').nth(1).unwrap(),
        "120",
        "未匹配行数：{got:?}"
    );
    std::fs::remove_dir_all(&root).ok();
}

/// **三表链**：外层 join 的 probe 就是内层 join 的输出 —— 位置靠"左段原样对应"
/// 一路往下传；传错一层，内层就会掩掉外层还需要的列。
#[test]
fn three_way_chain_propagates_downward() {
    let (db, root) = open("chain");
    seed(&db);
    db.sql("CREATE TABLE d2 (cat STRING, alias STRING)")
        .unwrap();
    db.sql("INSERT INTO d2 VALUES ('c0','A'),('c1','B'),('c2','C'),('c3','D'),('c4','E')")
        .unwrap();
    let got = both(
        &db,
        "SELECT d2.alias, count(*) c, sum(t.v) s FROM t JOIN d ON t.g = d.g \
         JOIN d2 ON d.cat = d2.cat GROUP BY d2.alias",
        "SELECT alias, count(*) c, sum(v) s FROM \
         (SELECT d2.alias AS alias, t.v AS v FROM t JOIN d ON t.g = d.g \
          JOIN d2 ON d.cat = d2.cat) y GROUP BY alias",
    );
    assert_eq!(got.len(), 5, "{got:?}");
    assert!(
        got.iter().all(|r| r.split('|').nth(1) == Some("80")),
        "{got:?}"
    );
    std::fs::remove_dir_all(&root).ok();
}

/// **连接键不在输出里**：`count(*)` + 按左表列分组 ⇒ 右键列**一个都不需要**。
/// （右键是全局 idx，减左宽算错就会掩错列。）
#[test]
fn key_columns_not_in_output() {
    let (db, root) = open("keys");
    seed(&db);
    both(
        &db,
        "SELECT g, count(*) c FROM t JOIN d ON t.g = d.g GROUP BY g",
        "SELECT g, count(*) c FROM (SELECT t.g AS g FROM t JOIN d ON t.g = d.g) y GROUP BY g",
    );
    std::fs::remove_dir_all(&root).ok();
}

/// **HAVING** 读的是聚合输出，与 join 输出无关，不该影响掩码。
#[test]
fn having_above_aggregate_is_unaffected() {
    let (db, root) = open("having");
    seed(&db);
    let got = both(
        &db,
        "SELECT d.cat, count(*) c FROM t JOIN d ON t.g = d.g GROUP BY d.cat \
         HAVING count(*) > 150",
        "SELECT cat, count(*) c FROM \
         (SELECT d.cat AS cat FROM t JOIN d ON t.g = d.g) y GROUP BY cat \
         HAVING count(*) > 150",
    );
    assert!(got.is_empty(), "每组 100 行，>150 应全滤掉：{got:?}");
    std::fs::remove_dir_all(&root).ok();
}

/// **不该被优化的形状**：join 与聚合之间夹了 `Project`（派生表）⇒ 标注必须停手。
/// 这条是**对照组自身的守门**：若哪天它也被标注了，上面的对照就全成了空转。
#[test]
fn derived_table_shape_is_left_alone() {
    let (db, root) = open("shape");
    seed(&db);
    // 直接验证结果正确（值断言）；是否真"未被优化"由 doc 里说的 prof 旋钮人工核对
    let got = rows(
        &db,
        "SELECT cat, count(*) c FROM (SELECT d.cat AS cat FROM t JOIN d ON t.g = d.g) y \
         GROUP BY cat",
    );
    assert_eq!(got.len(), 5, "{got:?}");
    assert!(got.iter().all(|r| r.ends_with("|80")), "{got:?}");
    std::fs::remove_dir_all(&root).ok();
}
