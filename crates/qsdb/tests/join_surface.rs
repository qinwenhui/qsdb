//! **join 支持面**的行为探针：每一种写法都要么**算出对的数**，要么**明确报错**。
//!
//! 为什么要有这个文件：`docs/SQL支持面.md` 是**对外声明**，而绑定器里是另一个真相
//! 源（`translate.rs` 的 `JoinOperator` 匹配只有 INNER/LEFT 两条臂，其余一律
//! `unsupported`）。两者一旦漂移，外部人就会按文档写出跑不通的 SQL——或者更糟，
//! 按文档以为能跑、实际**静默变成别的语义**。
//!
//! 所以这里对每条形态都**实测**，并把"实测到的那句话"钉进断言：
//!
//! - ✅ 的：断言**值**（不是"不报错"就算过——聚合恒一行，值错也全绿）；
//! - ✋ 的：断言**报错**，且文案里点明了原因（不是 "internal error" 那种猜谜）。
//!
//! 探针产出的表见 `docs/SQL支持面.md` §二。

use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use qsdb::{Qsdb, QsdbOptions};

fn tmp_root(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join("qsdb_join_surface").join(format!(
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

/// `a(k,x)` = (1,10) (2,20) (3,30)；`b(k,y)` = (2,200) (3,300) (4,400)。
///
/// 刻意**两侧都有不匹配的行**（a 的 k=1、b 的 k=4），这样 LEFT / RIGHT / FULL
/// 的差别在结果行数上立刻可见——只有一侧有孤儿的话，"补 NULL"那半边等于没测。
fn seeded(tag: &str) -> (Qsdb, std::path::PathBuf) {
    let root = tmp_root(tag);
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    db.sql("CREATE TABLE a (k BIGINT, x BIGINT)").unwrap();
    db.sql("CREATE TABLE b (k BIGINT, y BIGINT)").unwrap();
    let sa = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("x", DataType::Int64, true),
    ]));
    let sb = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("y", DataType::Int64, true),
    ]));
    db.append(
        "a",
        &[RecordBatch::try_new(
            Arc::clone(&sa),
            vec![
                Arc::new(Int64Array::from(vec![1i64, 2, 3])),
                Arc::new(Int64Array::from(vec![10i64, 20, 30])),
            ],
        )
        .unwrap()],
    )
    .unwrap();
    db.append(
        "b",
        &[RecordBatch::try_new(
            Arc::clone(&sb),
            vec![
                Arc::new(Int64Array::from(vec![2i64, 3, 4])),
                Arc::new(Int64Array::from(vec![200i64, 300, 400])),
            ],
        )
        .unwrap()],
    )
    .unwrap();
    (db, root)
}

/// `SELECT count(*)` 的标量结果。
fn count1(db: &Qsdb, sql: &str) -> i64 {
    match db
        .sql(sql)
        .unwrap_or_else(|e| panic!("`{sql}` 应当成功，实为：{e}"))
    {
        qsdb::sql::SqlResult::Query { batches, .. } => batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("count 应当是 Int64Array")
                    .values()
                    .to_vec()
            })
            .next()
            .expect("count 必须回恰好一行"),
        other => panic!("`{sql}` 应当是查询结果，实为 {other:?}"),
    }
}

/// 断言这条 SQL **被显式拒绝**（"未实现"那一类），并把引擎原话打出来
/// ——文案就是文档要写的东西。
///
/// ⚠️ 判据必须**挑明是哪种错**：只看"有没有报错"的话，一个**无关的错误**
/// 也能让用例绿（本文件第一版就栽在这——行元组那条报的其实是"列不存在"，
/// 是造数写错，探针却照收）。所以这里要求文案里出现 `unsupported`
/// （引擎对"语法认识、但没实现"的固定说法）。
fn rejected(db: &Qsdb, sql: &str) -> String {
    match db.sql(sql) {
        Ok(_) => panic!("`{sql}` 被接受了 —— 要么实现它，要么显式拒（不许静默）"),
        Err(e) => {
            let m = format!("{e}");
            eprintln!("[拒绝] {sql}\n        → {m}");
            assert!(
                m.contains("unsupported"),
                "`{sql}` 报的不是「未实现」而是别的错：{m}\n\
                 （无关错误混进来 = 这条探针没测到它想测的东西）"
            );
            // **文案里不许 dump 语法树**：`{expr:?}` 会把 `Ident { value: "a",
            // span: Span(Location(1,40)..Location(1,41)) }` 这种整棵打出来，
            // 一条几百字符，人和驱动都读不出重点。这是驱动面事故（J 系列同源：
            // 消费方按文案猜）。
            assert!(
                !m.contains("Ident {") && !m.contains("Span("),
                "拒绝文案里 dump 了 sqlparser 语法树：{m}"
            );
            assert!(m.len() < 300, "拒绝文案过长（{} 字符）：{m}", m.len());
            m
        }
    }
}

// ---------------------------------------------------------------------------
// ✅ 支持的
// ---------------------------------------------------------------------------

#[test]
fn 等值_inner_join() {
    let (db, root) = seeded("inner");
    // a 里 k=1 不匹配、b 里 k=4 不被匹配 ⇒ 命中 2 行 (2,3)
    assert_eq!(
        count1(&db, "SELECT count(*) FROM a JOIN b ON a.k = b.k"),
        2,
        "等值 INNER：只应命中 k=2,3"
    );
    assert_eq!(
        count1(&db, "SELECT count(*) FROM a INNER JOIN b ON a.k = b.k"),
        2,
        "`INNER JOIN` 与裸 `JOIN` 同义"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn 等值_left_join_未匹配补_null() {
    let (db, root) = seeded("left");
    // a 的 k=1 未被匹配 —— LEFT 必须**留住它**并补 NULL（只数行数会漏掉"补没补"）
    assert_eq!(
        count1(&db, "SELECT count(*) FROM a LEFT JOIN b ON a.k = b.k"),
        3
    );
    assert_eq!(
        count1(
            &db,
            "SELECT count(*) FROM a LEFT JOIN b ON a.k = b.k WHERE b.k IS NULL"
        ),
        1,
        "未匹配行必须补 NULL（而不是被丢掉）"
    );
    assert_eq!(
        count1(&db, "SELECT count(*) FROM a LEFT OUTER JOIN b ON a.k = b.k"),
        3,
        "`LEFT OUTER` 与 `LEFT` 同义"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn 非等值_on_的_inner_join() {
    let (db, root) = seeded("nonequi");
    // x ∈ {10,20,30} 全部 < y ∈ {200,300,400} ⇒ 9 对全中
    assert_eq!(
        count1(&db, "SELECT count(*) FROM a JOIN b ON a.x < b.y"),
        9,
        "非等值 INNER 走叉积+过滤，结果必须与逐对判定一致"
    );
    assert_eq!(
        count1(&db, "SELECT count(*) FROM a JOIN b ON a.x > b.y"),
        0,
        "反向谓词应当 0 行（不是「忽略谓词」那种静默兜底）"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn in_exists_族走_semi_anti() {
    let (db, root) = seeded("semi");
    assert_eq!(
        count1(&db, "SELECT count(*) FROM a WHERE k IN (SELECT k FROM b)"),
        2
    );
    assert_eq!(
        count1(
            &db,
            "SELECT count(*) FROM a WHERE k NOT IN (SELECT k FROM b)"
        ),
        1,
        "NOT IN：k=1 是唯一不在 b 里的"
    );
    assert_eq!(
        count1(
            &db,
            "SELECT count(*) FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.k = a.k)"
        ),
        2
    );
    assert_eq!(
        count1(
            &db,
            "SELECT count(*) FROM a WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.k = a.k)"
        ),
        1
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn 三表链式等值_join() {
    let (db, root) = seeded("chain3");
    db.sql("CREATE TABLE c (k BIGINT, tag STRING)").unwrap();
    db.append(
        "c",
        &[RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int64, true),
                Field::new("tag", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![2i64, 3])),
                Arc::new(StringArray::from(vec![Some("p"), Some("q")])),
            ],
        )
        .unwrap()],
    )
    .unwrap();
    // 三表链接（曾是 `complex_three_way` 113× 的病根：第二段起静默退化成叉积）
    assert_eq!(
        count1(
            &db,
            "SELECT count(*) FROM a JOIN b ON a.k = b.k JOIN c ON b.k = c.k"
        ),
        2
    );
    // 逆向书写 ON 的操作数顺序不该改变结果
    assert_eq!(
        count1(
            &db,
            "SELECT count(*) FROM a JOIN b ON a.k = b.k JOIN c ON c.k = b.k"
        ),
        2
    );
    let _ = std::fs::remove_dir_all(&root);
}

// ---------------------------------------------------------------------------
// ✋ 不支持的（必须**显式拒绝**，不许静默换语义）
// ---------------------------------------------------------------------------

#[test]
fn cross_join() {
    let (db, root) = seeded("cross");
    // `CROSS JOIN` 是 sqlparser 的独立算子（`JoinOperator::CrossJoin`），
    // **不经过** INNER 那条臂。文档曾把"非等值 ON 走叉积"写成了"CROSS JOIN ✅"
    // （`docs/SQL支持面.md` 已于 2026-10-15 按本探针纠正；`error-codes.md` 里
    //  `UNSUPPORTED` 一直拿它当例子——那边是对的）。
    rejected(&db, "SELECT count(*) FROM a CROSS JOIN b");
    // 想要笛卡尔积的替代写法：非等值 INNER 的恒真谓词 ⇒ 叉积 + 全通过
    assert_eq!(
        count1(&db, "SELECT count(*) FROM a JOIN b ON TRUE"),
        9,
        "`ON TRUE` 走非等值 INNER，等价于笛卡尔积"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn right_join_与_full_join_根本没有实现() {
    let (db, root) = seeded("rightfull");
    // 注意：`JoinType` 枚举里**没有** RightOuter/FullOuter 变体 ——
    // 这不是"没接上"，是结构性缺失（要做就得连执行层一起加）。
    //
    // 文案要说**用户写的那种写法**（"RIGHT JOIN"），不是 sqlparser 的变体名、
    // 更不是语法树 dump。修复前这里打的是：
    // `JOIN type Right(On(BinaryOp { left: CompoundIdentifier([Ident { value: "a", …`
    // （`m` 带 `unsupported: ` 类别前缀，判"第一句说的是不是这种写法"要先剥掉它）
    let m = rejected(&db, "SELECT count(*) FROM a RIGHT JOIN b ON a.k = b.k");
    let body = m.strip_prefix("unsupported: ").unwrap_or(&m);
    assert!(
        body.starts_with("RIGHT JOIN"),
        "文案该直接说 RIGHT JOIN：{m}"
    );
    let m = rejected(&db, "SELECT count(*) FROM a FULL JOIN b ON a.k = b.k");
    let body = m.strip_prefix("unsupported: ").unwrap_or(&m);
    assert!(body.starts_with("FULL"), "{m}");
    let m = rejected(&db, "SELECT count(*) FROM a FULL OUTER JOIN b ON a.k = b.k");
    let body = m.strip_prefix("unsupported: ").unwrap_or(&m);
    assert!(body.starts_with("FULL"), "{m}");
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn 非等值_left_join() {
    let (db, root) = seeded("nonequi_left");
    // 非等值只做了 INNER：LEFT 的非等值要么实现、要么明确拒（**绝不能**按 INNER 跑，
    // 那会静默丢掉未匹配行）
    rejected(&db, "SELECT count(*) FROM a LEFT JOIN b ON a.x < b.y");
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn using_与_natural() {
    let (db, root) = seeded("using");
    // 这两条**必须**拒：若被当成叉积，行数会从 3 炸成 9（静默错行数）
    rejected(&db, "SELECT count(*) FROM a JOIN b USING (k)");
    rejected(&db, "SELECT count(*) FROM a NATURAL JOIN b");
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn 逗号连接() {
    let (db, root) = seeded("comma");
    // `FROM a, b` 在标准 SQL 里是叉积。支持与否都可以，但**不能静默**：
    // 要么给出 9 行（真叉积），要么明确拒绝。
    match db.sql("SELECT count(*) FROM a, b") {
        Ok(_) => {
            assert_eq!(
                count1(&db, "SELECT count(*) FROM a, b"),
                9,
                "真叉积就该是 3×3"
            );
            eprintln!("[支持] `FROM a, b` 被当作叉积（9 行）");
        }
        Err(e) => eprintln!("[拒绝] `FROM a, b` → {e}"),
    }
    let _ = std::fs::remove_dir_all(&root);
}

/// `FULL [OUTER] JOIN` 没有实现，**但有等价配方**吗？
///
/// `RIGHT JOIN` 可以改写成 `LEFT JOIN`（交换两侧），FULL 不行——它要
/// "左未匹配 ∪ 右未匹配"，得自己拼。这条用例就是来**验**这个说法的：
/// 构件是 `LEFT JOIN` + `NOT EXISTS` + `UNION ALL` + `CAST(NULL AS …)`，
/// 四件我们都支持，但**支持不等于拼得通**（类型要对齐、UNION 两侧列数要一致），
/// 所以必须实测，不能写在文档里当传说。
///
/// 期望：a(1,2,3) 与 b(2,3,4) 全外连 = 4 行（k=1 只左、k=4 只右）。
#[test]
fn full_outer_join_的等价拼法() {
    let (db, root) = seeded("full_recipes");
    let sql = "SELECT count(*) FROM (
                 SELECT a.k AS k FROM a LEFT JOIN b ON a.k = b.k
                 UNION ALL
                 SELECT b.k AS k FROM b WHERE NOT EXISTS (SELECT 1 FROM a WHERE a.k = b.k)
               )";
    match db.sql(sql) {
        Ok(_) => assert_eq!(
            count1(&db, sql),
            4,
            "FULL OUTER 的等价拼法：左全 + 右未匹配，a(1,2,3)×b(2,3,4) 应当是 4 行"
        ),
        Err(e) => {
            // 拼不通也要**说清楚**：那"用 LEFT+NOT EXISTS 拼 FULL"这句建议就不成立
            eprintln!("[配方不成立] FULL OUTER 的等价拼法跑不通：{e}");
            panic!("FULL OUTER 没有实现、又没有可用配方 —— 这是真实的表达力缺口，应当进文档/队列");
        }
    }
    // 反向：真写 FULL 仍然必须是**明确拒绝**（不是静默给个错结果）
    rejected(&db, "SELECT count(*) FROM a FULL OUTER JOIN b ON a.k = b.k");
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn 行元组_in_与_any() {
    let (db, root) = seeded("tuple");
    // 两条都该归 `UNSUPPORTED`（"改写法"档），**不是** `INVALID_ARGUMENT`
    // （"修正参数"档）——驱动按码决定动作。
    //
    // 行元组这条曾经报 `invalid argument: IN subquery must select exactly one
    // column (got 2)`：把"我们没实现"说成了"你写错了"。2026-10-15 修
    // （`translate.rs::is_row_value`）。
    //
    // 注意两侧都用了**真实存在**的列：第一版这里写成 b 的 `x`，报的是
    // `column 'x' not found` —— 一个**无关错误**，而当时的断言只看"有没有报错"，
    // 照收（探针自己歪了；现在 `rejected` 要求文案里出现 `unsupported`）。
    let m = rejected(
        &db,
        "SELECT count(*) FROM a WHERE k = ANY (SELECT k FROM b)",
    );
    assert!(m.contains("ANY"), "文案该点出是哪种写法：{m}");
    let m = rejected(
        &db,
        "SELECT count(*) FROM a WHERE (k, x) IN (SELECT k, y FROM b)",
    );
    assert!(
        m.contains("row-value") || m.contains("row value"),
        "文案该点出「行值比较」这个真实原因：{m}"
    );
    let _ = std::fs::remove_dir_all(&root);
}
