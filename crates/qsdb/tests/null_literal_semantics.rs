//! **裸 `NULL` 字面量的类型**（2026-10-17）—— 三个语料块（L7-02_01 / L8-02_02）
//! 的共同根因。
//!
//! ## 洞在哪
//!
//! `SELECT NULL AS v1 … UNION ALL SELECT decimal_col AS v1 …`：
//! `Literal::Null` **没有类型**，`PhysExpr::data_type()` 只能兜底成 **Boolean**。
//! 集合操作按两侧声明类型归一 ⇒ NULL 分支把整列"变成" Boolean，下游类型世界全歪：
//!
//! - `V1 = ''` 报 `cannot compare BOOLEAN with STRING`（其实这列是小数）；
//! - 更阴的一例：NULL vs BIGINT 归一时 **BIGINT 侧被字符串化**（`common_supertype`
//!   见字符串就收编），下游 `V1 = 0` 报 `STRING vs BIGINT` —— **错的是归一，
//!   报错却指向用户没写过的比较**。
//!
//! ## 正确语义（Spark 3.5.2）
//!
//! `UNION` 里的裸 NULL 采纳**对侧的类型**（NullType 与任何类型归一 → 对方）。
//! 本仓没有 NullType，等价做法就是：归一时**裸 NULL 列直接取对侧类型**。

use qsdb::{Qsdb, QsdbOptions};

fn setup(tag: &str) -> (Qsdb, std::path::PathBuf) {
    let root = std::env::temp_dir().join("qsdb_null_lit").join(format!(
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

/// **NULL 分支 + DECIMAL 分支**：`v` 必须按 DECIMAL 归一，
/// 下游 `v = ''` 给 NULL（Spark 非 ANSI：转不动 ⇒ NULL），不再报 BOOLEAN vs STRING。
#[test]
fn union_裸_null_采纳对侧的_decimal_类型() {
    let (db, _r) = setup("dec");
    db.sql("CREATE TABLE d (v DECIMAL(10,2))").unwrap();
    db.sql("INSERT INTO d VALUES (1.50),(2.00)").unwrap();
    let (names, rows) = dump(
        &db,
        "SELECT v, tag FROM (SELECT NULL AS v, 'a' AS tag) x \
         UNION ALL SELECT v, 'b' FROM d",
    );
    assert_eq!(names, vec!["v", "tag"]);
    assert_eq!(rows.len(), 3);
    // 值要对：1.50 / 2.00 在，NULL 也在
    assert!(rows.iter().any(|r| r[0] == "1.50"), "{rows:?}");
    assert!(
        rows.iter().any(|r| r[0].is_empty()),
        "NULL 行丢了: {rows:?}"
    );
    // **下游类型**：`v = ''` 必须是 NULL（不是类型错、更不是 true/false）。
    // ⚠️ 比较要写在 **UNION 之外**：写在分支里，UNION 会把整个 SELECT 吞成分支，
    // 那比较面对的就是裸 NULL（Boolean 兜底）而不是归一后的列。
    let (_, r2) = dump(
        &db,
        "SELECT v = '' AS c FROM \
         ((SELECT NULL AS v, 'a' AS tag) UNION ALL (SELECT v, 'b' FROM d)) u",
    );
    // 若归一仍是 Boolean，这条在绑定就报错；到不了这里
    assert_eq!(r2.len(), 3);
    assert!(
        r2.iter().all(|r| r[0].is_empty() || r[0] == "NULL"),
        "{r2:?}"
    );
}

/// **NULL 分支 + BIGINT 分支**：BIGINT 不许被字符串化（曾经的 `STRING vs BIGINT`）。
#[test]
fn union_裸_null_不给_bigint_侧字符串化() {
    let (db, _r) = setup("int");
    db.sql("CREATE TABLE n (v BIGINT)").unwrap();
    db.sql("INSERT INTO n VALUES (7)").unwrap();
    let (_, rows) = dump(
        &db,
        "SELECT v FROM (SELECT NULL AS v) x UNION ALL SELECT v FROM n",
    );
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().any(|r| r[0] == "7"), "{rows:?}");
    // 下游与数字算术必须走数值路径（同样写在 UNION 外，理由同上）
    let (_, r2) = dump(
        &db,
        "SELECT v + 1 AS c FROM ((SELECT NULL AS v) UNION ALL (SELECT v FROM n)) u",
    );
    assert!(r2.iter().any(|r| r[0] == "8"), "{r2:?}");
}

/// **双向**：NULL 在右侧也一样采纳（对称性）。
#[test]
fn union_裸_null_在对侧也一样() {
    let (db, _r) = setup("sym");
    db.sql("CREATE TABLE n (v BIGINT)").unwrap();
    db.sql("INSERT INTO n VALUES (7)").unwrap();
    let (names, rows) = dump(
        &db,
        "SELECT v FROM n UNION ALL SELECT NULL AS v FROM (SELECT 1 AS k) x",
    );
    assert_eq!(names, vec!["v"]);
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().any(|r| r[0] == "7"));
}

/// **语料骨架**（L7-02_01 的形状）：NULL 占位列 UNION 真值列之后，
/// 外层还要过一遍 `CASE WHEN v IS NULL THEN 0 WHEN v = '' THEN 0 ELSE v END`。
/// 这条在修复前直接报 `cannot compare BOOLEAN with STRING`。
#[test]
fn 语料骨架_null_占位列过_case_空串兜底() {
    let (db, _r) = setup("corpus");
    db.sql("CREATE TABLE t (k VARCHAR, v DECIMAL(10,2))")
        .unwrap();
    db.sql("INSERT INTO t VALUES ('x', 3.50), ('y', NULL)")
        .unwrap();
    let sql = "SELECT k, CASE WHEN v IS NULL THEN 0 WHEN v = '' THEN 0 ELSE v END AS c \
               FROM ( \
                 (SELECT k, v FROM t) \
                 UNION ALL \
                 (SELECT 'z' AS k, NULL AS v) \
               ) u ORDER BY k";
    let (_, rows) = dump(&db, sql);
    assert_eq!(rows.len(), 3, "{rows:?}");
    let get = |k: &str| rows.iter().find(|r| r[0] == k).map(|r| r[1].clone());
    assert_eq!(get("x").as_deref(), Some("3.50"));
    // NULL 落 THEN 0；`v = ''` 分支是 NULL（不取）。
    // ⚠️ 显示为 "0.00" 而不是 "0"：CASE 分支类型统一到 DECIMAL，字面量 0 被
    // cast 成 0.00 —— Spark 同样如此（分支归一是 CASE 的规格，不是我们的发明）。
    assert_eq!(get("y").as_deref(), Some("0.00"));
    assert_eq!(get("z").as_deref(), Some("0.00"));
}

// ---------------------------------------------------------------------------
// **字符串参与算术**（L11-01_01）：Spark 把字符串转 double 再算
// ---------------------------------------------------------------------------

/// `decimal * CAST(numeric AS STRING)` —— 语料原样（`COALESCE(fee,0.0)*CAST(energy AS STRING)`）。
/// Spark 3.5.2 实测口径：结果 **double**；这里外层还有 CAST 归位 DECIMAL。
#[test]
fn 字符串参与算术_转_double_再算() {
    let (db, _r) = setup("strarith");
    db.sql("CREATE TABLE s (fee DECIMAL(10,2), energy INT)")
        .unwrap();
    db.sql("INSERT INTO s VALUES (2.50, 4), (1.00, 5)").unwrap();
    let (names, rows) = dump(
        &db,
        "SELECT CAST(fee * CAST(energy AS STRING) AS DECIMAL(10,2)) AS c FROM s ORDER BY c",
    );
    assert_eq!(names, vec!["c"]);
    // 2.5 * 4 = 10；1 * 5 = 5
    assert_eq!(rows[0][0], "5.00", "{rows:?}");
    assert_eq!(rows[1][0], "10.00", "{rows:?}");
    // 除法同族（语料里是乘完再除）
    let (_, r2) = dump(
        &db,
        "SELECT CAST(fee / CAST(energy AS STRING) AS DECIMAL(10,4)) AS c FROM s ORDER BY c",
    );
    assert_eq!(r2[0][0], "0.2000", "{r2:?}");
    // 字符串×字符串（Spark：'2' * '3' ⇒ 6.0）
    let (_, r3) = dump(&db, "SELECT CAST('2' * '3' AS INT) AS c FROM s");
    assert_eq!(r3[0][0], "6");
}

/// 字符串 × 非数值（日期）**不**在此列 —— 照旧显式报错，不许顺手放行。
#[test]
fn 字符串与日期算术仍显式拒() {
    let (db, _r) = setup("strdate");
    let e = db
        .sql_collect("SELECT DATE '2026-01-01' + CAST('x' AS STRING)")
        .err()
        .expect("本该报错却跑通了")
        .to_string();
    assert!(e.contains("not supported"), "unexpected: {e}");
}

// ---------------------------------------------------------------------------
// **集合操作归一的 cast 必须按「这一侧自己的类型」**（L4-01 三块的真凶）
// ---------------------------------------------------------------------------

/// `(38,8)` 分支 UNION `(30,8)` 分支（后者由 CASE 折叠整数 0 攒出来的形状）：
/// 归一到 `(38,8)` 时，右侧**必须真的包 Cast** —— 此前 `cast_setop_side` 拿
/// 左侧的类型当右列的现状类型，`common`==左侧类型时就不包，
/// 右侧 `Decimal(30,8)` 裸穿到 `(38,8)` 的 schema 下 ⇒
/// `project failed: expected Decimal128(38,8) found Decimal128(30,8)`。
#[test]
fn union_两侧_decimal_精度不同_必须真_cast() {
    let (db, _r) = setup("sidecast");
    db.sql("CREATE TABLE a (v DECIMAL(38,8))").unwrap();
    db.sql("CREATE TABLE b (v DECIMAL(30,8))").unwrap();
    db.sql("INSERT INTO a VALUES (1.00000001)").unwrap();
    db.sql("INSERT INTO b VALUES (2.00000002)").unwrap();
    // 左侧 (38,8) 是"赢"的形状：归一结果 == 左侧类型 ⇒ 右侧必须包 Cast
    let (_, rows) = dump(
        &db,
        "SELECT v FROM ((SELECT v FROM a) UNION ALL (SELECT v FROM b)) u ORDER BY v",
    );
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], "1.00000001", "{rows:?}");
    assert_eq!(rows[1][0], "2.00000002", "{rows:?}");
    // 换边：右侧 (38,8) "赢" —— 左侧这次要被 cast
    let (_, rows2) = dump(
        &db,
        "SELECT v FROM ((SELECT v FROM b) UNION ALL (SELECT v FROM a)) u ORDER BY v",
    );
    assert_eq!(rows2, rows);
}

/// 字符串收编数字的反向案例：数字侧不许被静默字符串化后还能"跑通"——
/// 要么归一成字符串且**值正确**，要么类型正确。这里钉"值正确"。
#[test]
fn union_字符串侧与_int_侧_值不丢() {
    let (db, _r) = setup("strconv");
    db.sql("CREATE TABLE s (v VARCHAR)").unwrap();
    db.sql("CREATE TABLE n (v INT)").unwrap();
    db.sql("INSERT INTO s VALUES ('7')").unwrap();
    db.sql("INSERT INTO n VALUES (9)").unwrap();
    let (_, rows) = dump(
        &db,
        "SELECT v FROM ((SELECT v FROM s) UNION ALL (SELECT v FROM n)) u ORDER BY v",
    );
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().any(|r| r[0] == "7"), "{rows:?}");
    assert!(rows.iter().any(|r| r[0] == "9"), "{rows:?}");
}

/// **字符串表达式 vs 数字**（L8-02_02 的 `CAST(VALUE AS STRING) = 1`）：
/// 运行期两侧转 double 再比（Spark 的 string-vs-double 路径）。
#[test]
fn 字符串表达式与数字比较() {
    let (db, _r) = setup("strexpr");
    db.sql("CREATE TABLE s (v DECIMAL(10,2))").unwrap();
    db.sql("INSERT INTO s VALUES (1.00),(2.50),(NULL)").unwrap();
    // 语料原样：decimal 先 cast 成 STRING 再与数字比
    let (_, rows) = dump(
        &db,
        "SELECT v, CAST(CASE WHEN CAST(v AS STRING) = 1 THEN '是' ELSE '否' END AS STRING) AS c \
         FROM s ORDER BY v",
    );
    let get = |v: &str| rows.iter().find(|r| r[0] == v).map(|r| r[1].clone());
    // 1.00 → 字符串 '1.00' 与 1 相等（两侧转 double 后 1.0 == 1.0）—— Spark 同判
    assert_eq!(get("1.00").as_deref(), Some("是"), "{rows:?}");
    assert_eq!(get("2.50").as_deref(), Some("否"));
    // NULL 比较恒 UNKNOWN → ELSE（显示空串）
    assert_eq!(get("").as_deref(), Some("否"));
    // 对称：数字在左
    let (_, r2) = dump(
        &db,
        "SELECT CASE WHEN 1 = CAST(v AS STRING) THEN 'y' ELSE 'n' END AS c \
         FROM s WHERE v = 1.00",
    );
    assert_eq!(r2[0][0], "y");
}

// ---------------------------------------------------------------------------
// **JOIN 键的类型归一**（L7-02_01 的真凶：静默 0 匹配）
// ---------------------------------------------------------------------------

/// `数值列 = 字符串列` 的 JOIN 键：此前两侧各绑各的、类型原样进哈希
/// ⇒ 编码不同 ⇒ **0 匹配、无报错**（Spark 同库同数 8136 行，我们 0 行）。
/// 修复后键走与比较路径同一套提升。
#[test]
fn join_键_数值列对字符串列_必须真匹配() {
    let (db, _r) = setup("joinkey");
    db.sql("CREATE TABLE l (k INT, v VARCHAR)").unwrap();
    db.sql("CREATE TABLE r (k VARCHAR, w VARCHAR)").unwrap();
    db.sql("INSERT INTO l VALUES (5, 'a'), (2, 'b'), (9, 'c')")
        .unwrap();
    db.sql("INSERT INTO r VALUES ('5', 'x'), ('2', 'y'), ('7', 'z')")
        .unwrap();
    let (_, rows) = dump(
        &db,
        "SELECT l.v, r.w FROM l INNER JOIN r ON l.k = r.k ORDER BY l.v",
    );
    // 5↔'5'、2↔'2' 配上；9 配不上
    assert_eq!(rows.len(), 2, "{rows:?}");
    assert!(
        rows.contains(&vec!["a".to_string(), "x".to_string()]),
        "{rows:?}"
    );
    assert!(
        rows.contains(&vec!["b".to_string(), "y".to_string()]),
        "{rows:?}"
    );
    // LEFT 同理：不匹配的左行保留、右段补 NULL
    let (_, r2) = dump(
        &db,
        "SELECT l.v, r.w FROM l LEFT JOIN r ON l.k = r.k ORDER BY l.v",
    );
    assert_eq!(r2.len(), 3, "{r2:?}");
    assert_eq!(r2[2], vec!["c", ""], "{r2:?}");
}

/// 字符串字面量在键上：`int_col = '5'`（绑定期就能转，走字面量提升）。
#[test]
fn join_键_列对字面量() {
    let (db, _r) = setup("joinlit");
    db.sql("CREATE TABLE l (k INT, v VARCHAR)").unwrap();
    db.sql("INSERT INTO l VALUES (5, 'a'), (6, 'b')").unwrap();
    let (_, rows) = dump(
        &db,
        "SELECT v FROM l INNER JOIN (SELECT 5 AS k) r ON l.k = r.k",
    );
    assert_eq!(rows, vec![vec!["a"]], "{rows:?}");
}

/// **RIGHT JOIN（换边）上的跨类型键**（L0-16_01 的形状）：
/// 换边要把键的 idx 从合并 schema 换算到 probe/build 局部序，
/// `CAST(列 AS …)` 包装里的列必须跟着换（漏了 = 越界 panic）。
#[test]
fn right_join_上的跨类型键() {
    let (db, _r) = setup("rightkey");
    db.sql("CREATE TABLE l (d VARCHAR, t INT, v VARCHAR)")
        .unwrap();
    db.sql("CREATE TABLE r (d VARCHAR, p INT, w VARCHAR)")
        .unwrap();
    db.sql("INSERT INTO l VALUES ('d1', 1, 'a'), ('d1', 2, 'b')")
        .unwrap();
    db.sql("INSERT INTO r VALUES ('d1', 1, 'x'), ('d1', 3, 'y')")
        .unwrap();
    // 数值列 t = 字符串列 p 的 RIGHT JOIN：l 有 2 行，r 有 2 行，匹配 1 对
    //（RIGHT 保留 r 的孤儿行）
    let (_, rows) = dump(
        &db,
        "SELECT l.v, r.w FROM l RIGHT JOIN r ON l.d = r.d AND l.t = r.p ORDER BY r.w",
    );
    assert_eq!(rows.len(), 2, "{rows:?}");
    // (1↔'1') 配上；(3) 是右孤儿，左段补 NULL
    assert_eq!(rows[0], vec!["a", "x"], "{rows:?}");
    assert_eq!(rows[1], vec!["", "y"], "{rows:?}");
}
