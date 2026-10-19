//! `PIVOT` 行转列（2026-10-17）—— **两路对撞**：`PIVOT` 写法 vs 手写 `CASE + GROUP BY`。
//!
//! `PIVOT` 被实现成**绑定期重写成条件聚合**（见 `列转行与行转列设计稿`），
//! 所以"它算对了"= "它算得跟手写的 `CASE WHEN … GROUP BY` 一样"。
//!
//! ⚠️ 这里最该盯住的不是"值对不对"，而是**「其余列」有没有算对**：
//! 少减一列 ⇒ 分组粒度变细 ⇒ 一组变多组 ⇒ **数字全错，但一条错都不报**。
//! 所以下面既比对撞，也**专门钉分组粒度**（同一组必须合成一行）。

use qsdb::{Qsdb, QsdbOptions};

/// 测试目录唯一序号：pid 挡跨进程碰撞，序号挡同进程内并发（Windows 时钟
/// 粒度 ~100ns，纳秒后缀在同 tick 内会重复——CI 上真炸过，见 constraints）。
fn _uniq_seq() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    SEQ.fetch_add(1, Ordering::Relaxed)
}

fn setup(tag: &str) -> (Qsdb, std::path::PathBuf) {
    let root = std::env::temp_dir().join("qsdb_pivot").join(format!(
        "{tag}_{}_{}",
        std::process::id(),
        format!("{:x}_{}", std::process::id(), _uniq_seq())
    ));
    std::fs::create_dir_all(&root).unwrap();
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    (db, root)
}

fn dump(db: &Qsdb, sql: &str) -> (Vec<String>, Vec<Vec<String>>) {
    let bs = db.sql_collect(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    let names: Vec<String> = match bs.first() {
        Some(b) => b
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect(),
        None => Vec::new(),
    };
    let mut rows = Vec::new();
    for b in &bs {
        for i in 0..b.num_rows() {
            rows.push(
                (0..b.num_columns())
                    .map(|c| {
                        arrow::util::display::array_value_to_string(b.column(c).as_ref(), i)
                            .unwrap()
                    })
                    .collect(),
            );
        }
    }
    (names, rows)
}

/// 输入表：`tpoint` 的取值是 `'1'..'3'`，且**故意缺一个**（`c2` 没有 `'2'`）
/// —— 空分组必须给 `NULL`，不能整行消失。
fn seed(db: &Qsdb) {
    db.sql("CREATE TABLE s (pdate VARCHAR, cust VARCHAR, tpoint VARCHAR, value DECIMAL(10,2))")
        .unwrap();
    db.sql(
        "INSERT INTO s VALUES \
         ('d1','c1','1', 10.00), \
         ('d1','c1','2', 20.00), \
         ('d1','c1','1',  5.00), \
         ('d1','c2','1', 30.00), \
         ('d2','c1','3', 40.00)",
    )
    .unwrap();
}

const PIVOT_SQL: &str = "SELECT pdate, cust, v1, v2, v3 FROM ( \
       SELECT pdate, cust, tpoint, value FROM s \
     ) t PIVOT ( \
       SUM(CAST(COALESCE(value, 0) AS DECIMAL(38,11))) AS value \
       FOR tpoint IN ('1' AS v1, '2' AS v2, '3' AS v3) \
     ) ORDER BY pdate, cust";

/// ⚠️ 手写版必须与 `PIVOT` 版**同形**：同样的 `SUM(COALESCE(...))`。
///
/// 注意这里**不能**天真地写 `SUM(CASE WHEN tpoint='1' THEN value END)` ——
/// 那会与 `SUM(CASE WHEN … THEN COALESCE(value,0) END)` 在"整组全空"时不同
/// （前者 NULL、后者 0）。真正的对照组是**把 `PIVOT` 的输出列名换成 CASE**，
/// 聚合体一字不改。
const HAND_SQL: &str = "SELECT pdate, cust, \
       SUM(CASE WHEN tpoint = '1' THEN CAST(COALESCE(value, 0) AS DECIMAL(38,11)) END) AS v1, \
       SUM(CASE WHEN tpoint = '2' THEN CAST(COALESCE(value, 0) AS DECIMAL(38,11)) END) AS v2, \
       SUM(CASE WHEN tpoint = '3' THEN CAST(COALESCE(value, 0) AS DECIMAL(38,11)) END) AS v3 \
     FROM s GROUP BY pdate, cust ORDER BY pdate, cust";

#[test]
fn 与手写_case_group_by_逐值逐列一致() {
    let (db, _r) = setup("basic");
    seed(&db);
    let (n1, r1) = dump(&db, PIVOT_SQL);
    let (n2, r2) = dump(&db, HAND_SQL);
    assert_eq!(n1, vec!["pdate", "cust", "v1", "v2", "v3"], "列名与列序");
    assert_eq!(n1, n2);
    assert_eq!(r1, r2);
    // 3 组：(d1,c1) (d1,c2) (d2,c1)。**分组粒度**这一钉：多减一列就会变 5 行。
    assert_eq!(r1.len(), 3, "{r1:?}");
    // 缺席的组合 ⇒ NULL（不是 0、也不是整行消失）
    let c2 = r1
        .iter()
        .find(|r| r[0] == "d1" && r[1] == "c2")
        .expect("缺 (d1,c2) 行");
    assert!(
        c2[3].is_empty(),
        "c2 没有 tpoint='2' ⇒ v2 应为 NULL: {c2:?}"
    );
}

/// 内层是**裸表**（没有派生表包一层）时同样成立。
#[test]
fn 内层直接是实表() {
    let (db, _r) = setup("rawtable");
    seed(&db);
    let (n, rows) = dump(
        &db,
        "SELECT pdate, cust, v1, v2 FROM s \
         PIVOT (SUM(value) AS value FOR tpoint IN ('1' AS v1, '2' AS v2)) \
         ORDER BY pdate, cust",
    );
    assert_eq!(n, vec!["pdate", "cust", "v1", "v2"]);
    assert_eq!(rows.len(), 3);
    // (d1,c1,'1') 两行 10+5 = 15
    let row = rows.iter().find(|r| r[0] == "d1" && r[1] == "c1").unwrap();
    assert_eq!(row[2], "15.00", "{rows:?}");
}

/// `FOR` 的列**不进**输出；聚合引用到的列也不进分组。
/// 这里内层有 4 列 `(pdate, cust, tpoint, value)`，聚合吃 `value` ⇒ 分组恰好是 2 列。
#[test]
fn 其余列不含_for_列与聚合引用的列() {
    let (db, _r) = setup("remain");
    seed(&db);
    let (names, _) = dump(
        &db,
        "SELECT * FROM (SELECT pdate, cust, tpoint, value FROM s) t \
         PIVOT (SUM(value) AS value FOR tpoint IN ('1' AS v1, '2' AS v2))",
    );
    assert_eq!(
        names,
        vec!["pdate", "cust", "v1", "v2"],
        "tpoint/value 都不该出现在输出里"
    );
}

/// **语料里的真实形状**：`stack`（列转行）→ `PIVOT`（行转列）串起来
/// （`work/daily/L0-17_01.sql` 的 t16 → t18 就是这个骨架）。
/// 两条重写要能接得上：`stack` 造出 `data_type` / `value` 两列，PIVOT 再按 `tpoint` 透开。
#[test]
fn stack_接_pivot_的语料骨架() {
    let (db, _r) = setup("chain");
    db.sql(
        "CREATE TABLE w (pdate VARCHAR, tpoint VARCHAR, energy DECIMAL(10,2), price DECIMAL(10,2))",
    )
    .unwrap();
    db.sql(
        "INSERT INTO w VALUES \
         ('d1','1', 10.00, 100.00), \
         ('d1','2', 20.00, 200.00), \
         ('d2','1', 30.00, 300.00)",
    )
    .unwrap();
    // ① 宽 → 长（stack），② 长 → 宽（PIVOT），按 data_type 分组
    let sql = "WITH t16 AS ( \
         SELECT pdate, tpoint, \
                stack(2, 'energy', energy, 'price', price) AS (data_type, value) \
         FROM w \
       ), t18 AS ( \
         SELECT pdate, data_type, v1, v2 FROM ( \
           SELECT pdate, tpoint, data_type, value FROM t16 \
         ) t PIVOT ( \
           SUM(CAST(COALESCE(value, 0) AS DECIMAL(38,11))) AS value \
           FOR tpoint IN ('1' AS v1, '2' AS v2) \
         ) \
       ) SELECT pdate, data_type, v1, v2 FROM t18 ORDER BY pdate, data_type";
    let (names, rows) = dump(&db, sql);
    assert_eq!(names, vec!["pdate", "data_type", "v1", "v2"]);
    // 3 行：(d1,energy) (d1,price) (d2,energy) (d2,price) —— 其实是 4 行
    assert_eq!(rows.len(), 4, "{rows:?}");
    let energy_d1 = rows
        .iter()
        .find(|r| r[0] == "d1" && r[1] == "energy")
        .expect("缺 (d1,energy)");
    assert_eq!(energy_d1[2], "10.00000000000", "{rows:?}");
    assert_eq!(energy_d1[3], "20.00000000000", "{rows:?}");

    // 与**全手写**版对撞（stack 展开成 UNION ALL、PIVOT 展开成 CASE+GROUP BY）
    let hand = "SELECT pdate, data_type, \
         SUM(CASE WHEN tpoint = '1' THEN CAST(COALESCE(value, 0) AS DECIMAL(38,11)) END) AS v1, \
         SUM(CASE WHEN tpoint = '2' THEN CAST(COALESCE(value, 0) AS DECIMAL(38,11)) END) AS v2 \
       FROM ( \
         SELECT pdate, tpoint, 'energy' AS data_type, energy AS value FROM w \
         UNION ALL SELECT pdate, tpoint, 'price', price FROM w \
       ) u GROUP BY pdate, data_type ORDER BY pdate, data_type";
    let (hn, hr) = dump(&db, hand);
    assert_eq!(names, hn);
    assert_eq!(rows, hr);
}

// ---------------------------------------------------------------------------
// 边界：全部**显式拒**（设计稿 §3.3）
// ---------------------------------------------------------------------------

fn err_of(db: &Qsdb, sql: &str) -> String {
    db.sql_collect(sql)
        .err()
        .unwrap_or_else(|| panic!("本该报错却跑通了: {sql}"))
        .to_string()
}

/// `IN ('1','2')` **不带 `AS`** ⇒ 列名按 Spark 的规则取**字面量的文本形式**。
///
/// 语料里 11 个块就是这个形态，而且**下游 SQL 就是按 `` `1` ``、`` `2` `` 引用的**
/// （`work/daily/L1-01_01.sql` 的 t14），所以这个名字不是我们编的、是下游的契约。
/// 这条同时钉住"下游能按那个名字取到列"——取不到就说明名字定错了。
#[test]
fn in_无别名时按字面量取名() {
    let (db, _r) = setup("noalias");
    seed(&db);
    let (names, rows) = dump(
        &db,
        "SELECT * FROM s PIVOT (SUM(value) AS v FOR tpoint IN ('1', '2')) ORDER BY pdate, cust",
    );
    assert_eq!(names, vec!["pdate", "cust", "1", "2"], "列名应取字面量文本");
    // 下游那种引用方式必须真的取得到（反引号标识符）
    let (n2, r2) = dump(
        &db,
        "SELECT t.`1` AS a1, t.`2` AS a2 FROM ( \
           SELECT * FROM s PIVOT (SUM(value) AS v FOR tpoint IN ('1', '2')) \
         ) t ORDER BY t.`1`",
    );
    assert_eq!(n2, vec!["a1", "a2"]);
    assert_eq!(r2.len(), rows.len());
    // 数字字面量同理（`IN (1, 2)`，不是字符串）
    let (names, _) = dump(
        &db,
        "SELECT * FROM (SELECT pdate, cust, CAST(tpoint AS INT) AS tp, value FROM s) x \
         PIVOT (SUM(value) AS v FOR tp IN (1, 2))",
    );
    assert_eq!(names, vec!["pdate", "cust", "1", "2"]);
}

/// 取不出确定名字的 `IN` 值 ⇒ **显式拒**（列引用 / 表达式），绝不猜。
#[test]
fn in_值非字面量时拒() {
    let (db, _r) = setup("nonlit");
    seed(&db);
    let e = err_of(
        &db,
        "SELECT * FROM s PIVOT (SUM(value) AS v FOR tpoint IN (cust))",
    );
    assert!(e.contains("write the alias explicitly"), "unexpected: {e}");
    let e = err_of(
        &db,
        "SELECT * FROM s PIVOT (SUM(value) AS v FOR tpoint IN (CAST('1' AS STRING)))",
    );
    assert!(e.contains("write the alias explicitly"), "unexpected: {e}");
}

/// **Spark 的 `BYTE` = `TINYINT`**：语料里 24 处 `CAST(x AS byte)`（8 个块，全在 `stack` 里）。
/// 写错就退回 `Custom("byte") is not supported`。
#[test]
fn cast_到_byte_等于_tinyint() {
    let (db, _r) = setup("byte");
    db.sql("CREATE TABLE b (a INT, v INT)").unwrap();
    db.sql("INSERT INTO b VALUES (1, 7)").unwrap();
    assert_eq!(dump(&db, "SELECT CAST(300 AS byte) FROM b").1[0][0], "");
    assert_eq!(dump(&db, "SELECT CAST(7 AS byte) FROM b").1[0][0], "7");
    // 语料里的真实写法：`byte` 出现在 `stack` 的参数里
    let (names, rows) = dump(
        &db,
        "SELECT stack(2, 'x', CAST(v AS byte), 'y', CAST(a AS byte)) AS (dt, val) FROM b",
    );
    assert_eq!(names, vec!["dt", "val"]);
    assert_eq!(rows.len(), 2);
}

#[test]
fn in_里含子查询或_any_就拒() {
    let (db, _r) = setup("dynamic");
    seed(&db);
    let e = err_of(
        &db,
        "SELECT * FROM s PIVOT (SUM(value) AS v FOR tpoint IN (SELECT tpoint FROM s))",
    );
    assert!(e.contains("depend on the data"), "unexpected: {e}");
    let e = err_of(
        &db,
        "SELECT * FROM s PIVOT (SUM(value) AS v FOR tpoint IN (ANY))",
    );
    assert!(e.contains("depend on the data"), "unexpected: {e}");
}

#[test]
fn 多聚合就拒() {
    let (db, _r) = setup("multiagg");
    seed(&db);
    let e = err_of(
        &db,
        "SELECT * FROM s PIVOT (SUM(value) AS a, MAX(value) AS b FOR tpoint IN ('1' AS v1))",
    );
    assert!(e.contains("aggregates"), "unexpected: {e}");
}

#[test]
fn 聚合不是聚合函数就拒() {
    let (db, _r) = setup("notagg");
    seed(&db);
    let e = err_of(
        &db,
        "SELECT * FROM s PIVOT (abs(value) AS a FOR tpoint IN ('1' AS v1))",
    );
    assert!(
        e.contains("must be one of count/sum/min/max/avg"),
        "unexpected: {e}"
    );
}

#[test]
fn for_不是裸列就拒() {
    let (db, _r) = setup("fornotcol");
    seed(&db);
    let e = err_of(
        &db,
        "SELECT * FROM s PIVOT (SUM(value) AS v FOR CAST(tpoint AS INT) IN ('1' AS v1))",
    );
    assert!(e.contains("plain column reference"), "unexpected: {e}");
}

#[test]
fn for_列不存在就拒() {
    let (db, _r) = setup("fornocol");
    seed(&db);
    let e = err_of(
        &db,
        "SELECT * FROM s PIVOT (SUM(value) AS v FOR nosuch IN ('1' AS v1))",
    );
    assert!(e.contains("no such column"), "unexpected: {e}");
}

/// `JOIN` 右侧的 `PIVOT` 显式拒，且**给出怎么改**（不是把语法树 Debug-print 出去）。
#[test]
fn join_右侧的_pivot_显式拒() {
    let (db, _r) = setup("joinpivot");
    seed(&db);
    let e = err_of(
        &db,
        "SELECT * FROM s a JOIN (SELECT pdate, tpoint, value FROM s) b \
         PIVOT (SUM(value) AS v FOR tpoint IN ('1' AS v1)) ON a.pdate = b.pdate",
    );
    assert!(e.contains("derived table"), "unexpected: {e}");
}
