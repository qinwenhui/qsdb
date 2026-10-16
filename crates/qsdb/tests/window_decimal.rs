//! **窗口聚合 over `DECIMAL`** 的验收（2026-10-16）。
//!
//! 起因：真实业务表（电力日清结算明细）的核心三列 —— **电量/电费/电价 ——
//! 全是 `DECIMAL(30,8)`**，而 `sum(d) OVER (…)` 此前直接报
//! `support: integer/float args`。窗口累计电量/电费是日清最常用的形状之一。
//!
//! 三条要钉住的：
//! 1. **值要对**（`i256` 精确累加，不是 f64 糊弄）；
//! 2. **类型要对**（`sum` → `Decimal(38,8)`；曾经因为 `is_numeric()` 挡在
//!    `Decimal` 前面而**静默变成 BIGINT** —— 那是个一直没暴露的既有 bug）；
//! 3. **与非窗口聚合逐值一致**（同一个函数两条路给出不同值是灾难）。

use qsdb::{Qsdb, QsdbOptions};

fn setup(tag: &str) -> (Qsdb, std::path::PathBuf) {
    let root = std::env::temp_dir().join("qsdb_wdec").join(format!(
        "{tag}_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    db.sql("CREATE TABLE t (i BIGINT, g VARCHAR, d DECIMAL(30,8))")
        .unwrap();
    db.sql("INSERT INTO t VALUES (1,'a',1.5),(2,'a',2.25),(3,'b',-3.125),(4,'b',NULL)")
        .unwrap();
    (db, root)
}

/// 逐行 `c0|c1|…`，**保持引擎给的行序**（有 ORDER BY 的必须用这个）。
fn rows(db: &Qsdb, sql: &str) -> Vec<String> {
    let bs = db.sql_collect(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    bs.iter()
        .flat_map(|b| {
            (0..b.num_rows()).map(move |i| {
                (0..b.num_columns())
                    .map(|c| {
                        let a = b.column(c);
                        if a.is_null(i) {
                            "NULL".into()
                        } else {
                            arrow::util::display::array_value_to_string(a.as_ref(), i).unwrap()
                        }
                    })
                    .collect::<Vec<_>>()
                    .join("|")
            })
        })
        .collect()
}

fn types(db: &Qsdb, sql: &str) -> Vec<String> {
    let b = db.sql_collect(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    b.first()
        .map(|b| {
            b.schema()
                .fields()
                .iter()
                .map(|f| f.data_type().to_string())
                .collect()
        })
        .unwrap_or_default()
}

// ══ ① 默认帧（整分区）═════════════════════════════════════════════════════

#[test]
fn 整分区_sum_avg_min_max() {
    let (db, root) = setup("whole");
    assert_eq!(
        rows(
            &db,
            "SELECT sum(d) AS s, avg(d) AS a, min(d) AS mn, max(d) AS mx FROM t LIMIT 1"
        ),
        vec!["0.62500000|0.20833333|-3.12500000|2.25000000"]
    );
    std::fs::remove_dir_all(&root).ok();
}

/// ★ 类型：`sum` 曾经静默变成 `BIGINT`（`is_numeric()` 挡在 `Decimal` 前），
/// 值会被当整数显示。这条钉死它。
#[test]
fn 输出类型() {
    let (db, root) = setup("ty");
    assert_eq!(
        types(
            &db,
            "SELECT sum(d) OVER () AS s, avg(d) OVER () AS a FROM t LIMIT 1"
        ),
        vec!["Decimal128(38, 8)", "Decimal128(38, 8)"],
        "sum/avg 的输出精度 = min(p+10, 38) = 38，scale 保持 8/4"
    );
    // min/max 沿用参数类型
    assert_eq!(
        types(
            &db,
            "SELECT min(d) OVER () AS mn, max(d) OVER () AS mx FROM t LIMIT 1"
        ),
        vec!["Decimal128(30, 8)", "Decimal128(30, 8)"],
    );
    std::fs::remove_dir_all(&root).ok();
}

// ══ ② 显式行帧（滑窗）═════════════════════════════════════════════════════

#[test]
fn 滑窗帧() {
    let (db, root) = setup("frame");
    // 期望：1.5 / 1.5+2.25=3.75 / 2.25-3.125=-0.875 / -3.125(NULL 不计)
    assert_eq!(
        rows(
            &db,
            "SELECT sum(d) OVER (ORDER BY i ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS s \
             FROM t ORDER BY i"
        ),
        vec!["1.50000000", "3.75000000", "-0.87500000", "-3.12500000"]
    );
    std::fs::remove_dir_all(&root).ok();
}

// ══ ③ PARTITION BY + 默认帧 ═══════════════════════════════════════════════

#[test]
fn 分组累计() {
    let (db, root) = setup("part");
    // a 组：1.5 / 3.75；b 组：-3.125 / -3.125（NULL 行沿用累计值）
    assert_eq!(
        rows(
            &db,
            "SELECT sum(d) OVER (PARTITION BY g ORDER BY i) AS s FROM t ORDER BY g, i"
        ),
        vec!["1.50000000", "3.75000000", "-3.12500000", "-3.12500000"]
    );
    std::fs::remove_dir_all(&root).ok();
}

// ══ ④ ★ 与非窗口聚合**逐值一致** ═══════════════════════════════════════════

/// 同一个函数两条路给出不同值是灾难。整分区窗口 ≡ 无分组的聚合。
#[test]
fn 整分区窗口与非窗口聚合逐值一致() {
    let (db, root) = setup("same");
    let agg = rows(
        &db,
        "SELECT sum(d) AS s, avg(d) AS a, min(d) AS mn, max(d) AS mx FROM t",
    );
    let win = rows(
        &db,
        "SELECT sum(d) OVER () AS s, avg(d) OVER () AS a, min(d) OVER () AS mn, \
         max(d) OVER () AS mx FROM t LIMIT 1",
    );
    assert_eq!(win, agg, "整分区窗口必须等于无分组聚合");
    std::fs::remove_dir_all(&root).ok();
}

// ══ ⑤ NULL 口径 ═══════════════════════════════════════════════════════════

#[test]
fn 帧内全_null_给_null() {
    let (db, root) = setup("nulls");
    db.sql("CREATE TABLE n (i BIGINT, d DECIMAL(10,4))")
        .unwrap();
    db.sql("INSERT INTO n VALUES (1,NULL),(2,NULL),(3,1.0)")
        .unwrap();
    // 前两行帧内无非 NULL ⇒ sum/avg 回 NULL（不是 0）
    let got = rows(
        &db,
        "SELECT sum(d) OVER (ORDER BY i ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS s \
         FROM n ORDER BY i",
    );
    assert_eq!(got[0], "NULL");
    assert_eq!(got[1], "NULL");
    assert_eq!(got[2], "1.0000");
    std::fs::remove_dir_all(&root).ok();
}

// ══ ⑥ 精度：不丢位 ════════════════════════════════════════════════════════

/// **大数**上验 `i256` 精确累加 —— 用 f64 糊弄的话这里必错。
#[test]
fn 大数不丢精度() {
    let (db, root) = setup("big");
    db.sql("CREATE TABLE b (i BIGINT, d DECIMAL(38,8))")
        .unwrap();
    // 这两个数放进 f64 会挤成同一个值（f64 只有 ~15-16 位有效数字）
    db.sql(
        "INSERT INTO b VALUES \
         (1, 1234567890123456789012345.12345678), \
         (2, 0.00000001)",
    )
    .unwrap();
    let got = rows(&db, "SELECT sum(d) OVER () AS s FROM b LIMIT 1");
    assert_eq!(
        got,
        vec!["1234567890123456789012345.12345679"],
        "分毫不能差"
    );
    std::fs::remove_dir_all(&root).ok();
}

// ══ ⑦ 溢出：**显式报错，不回 NULL** ═══════════════════════════════════════

/// 输出类型声明的是 `min(p+10, 38)`；装不下时此前会**静默变 NULL**
/// （「求和超范围」被说成「这组全空」）。这条钉住它改成报错。
#[test]
fn 求和溢出要报错不回_null() {
    let (db, root) = setup("ovf");
    db.sql("CREATE TABLE o (d DECIMAL(38,8))").unwrap();
    // `DECIMAL(38,8)` 单值最大 ≈ 10^38（raw i128）；**两个**这样的数相加
    // 就超出 i128 上界（1.7×10^38）⇒ 正好触发溢出那条路。
    db.sql(
        "INSERT INTO o VALUES (999999999999999999999999999999.00000000), \
         (999999999999999999999999999999.00000000)",
    )
    .unwrap();
    // ⚠️ **`avg` 不在此列**：`avg = sum / count` 会除回去，而每个值本身 ≤ 10^38
    //    ⇒ `avg` 对**合法输入**不可能溢出（输出 scale = max(s,4) ≥ 输入 scale）。
    //    实现里那条防御性报错保留，但**构造不出用例**。别把它写进断言。
    for q in ["SELECT sum(d) FROM o", "SELECT sum(d) OVER () FROM o"] {
        let e = db.sql_collect(q).unwrap_err().to_string();
        assert!(
            e.contains("超出 DECIMAL 能表示的范围"),
            "`{q}` 该显式报错，实得：{e}"
        );
    }
    std::fs::remove_dir_all(&root).ok();
}
