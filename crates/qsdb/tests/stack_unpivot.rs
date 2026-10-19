//! `stack` 列转行（2026-10-17）—— **两路对撞**：`stack` 写法 vs 手写 `UNION ALL`。
//!
//! ## 为什么用"对撞"而不是钉死期望值
//!
//! `stack` 被实现成**绑定期重写成 `UNION ALL`**（见 `列转行与行转列设计稿`），
//! 所以"它算对了"这句话的全部内容就是"它算得跟手写的 `UNION ALL` 一样"。
//! 把两边跑出来**逐值逐列**比，比在测试里抄一份期望值更能锁住语义 ——
//! 期望值抄错了我不知道，两边对不上我一定知道。
//!
//! 期望值另有 `.14` 上真 Spark 3.5.2 的对账兜底（那是**跨引擎**的独立一票，见 §五）。

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
        .join("qsdb_stack_unpivot")
        .join(format!("{tag}_{:x}_{}", std::process::id(), _uniq_seq()));
    std::fs::create_dir_all(&root).unwrap();
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    (db, root)
}

/// 跑一句，返回 `(列名, 行 × 列的字符串矩阵)`。
///
/// **列名也要取**：`stack` 的输出列名只能来自用户的 `AS (c1, c2)`，
/// 语料的 PIVOT 是靠 `data_type` / `value` 这两个名字接下去的 ——
/// 名字错了下游整条链都错，而**数值还是对的**（那种错最难发现）。
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

/// 建一张小表：`v2` 那一列**故意留 NULL**（`stack` 会把 NULL 从竖直方向搬到水平方向，
/// 是最容易把"少一行"和"多一行 NULL"搞混的地方）。
fn seed(db: &Qsdb) {
    db.sql(
        "CREATE TABLE t (a INT, b VARCHAR, v1 DECIMAL(10,2), v2 DECIMAL(10,2), v3 DECIMAL(10,2))",
    )
    .unwrap();
    db.sql(
        "INSERT INTO t VALUES \
         (1, 'x', 1.50, 2.50, 3.50), \
         (2, 'y', 10.00, NULL, 30.00)",
    )
    .unwrap();
}

/// 三对：`stack` 写法与手写 `UNION ALL` **列名 + 每格逐值**一致。
#[test]
fn 三对_与手写_union_all_逐值一致() {
    let (db, _r) = setup("three");
    seed(&db);
    let (n1, r1) = dump(
        &db,
        "SELECT a, stack(3, 'k1', v1, 'k2', v2, 'k3', v3) AS (data_type, value) \
         FROM t ORDER BY a, data_type",
    );
    let (n2, r2) = dump(
        &db,
        "SELECT a, 'k1' AS data_type, v1 AS value FROM t \
         UNION ALL SELECT a, 'k2', v2 FROM t \
         UNION ALL SELECT a, 'k3', v3 FROM t ORDER BY a, data_type",
    );
    assert_eq!(n1, vec!["a", "data_type", "value"], "列名");
    assert_eq!(n1, n2);
    assert_eq!(r1, r2);
    // 钉一下确实是 6 行（2 行 × 3 对）—— 两边都错成同一个数时上面那比对不出来
    assert_eq!(r1.len(), 6);
    // NULL 原样搬过来，不当成"这一对没有"
    assert!(
        r1.iter().any(|row| row[2].is_empty()),
        "v2 的 NULL 应在结果里: {r1:?}"
    );
}

/// **分支顺序 = pair 顺序**（设计稿 §2.3 明写的一条）。下游全靠它对齐，
/// 而 `UNION ALL` 若被内部重排（去重/并行合并）这里就会红。
#[test]
fn 分支顺序就是_pair_顺序() {
    let (db, _r) = setup("order");
    seed(&db);
    let (_, rows) = dump(
        &db,
        "SELECT a, stack(3, 'k1', 1, 'k2', 2, 'k3', 3) AS (dt, v) FROM t WHERE a = 1",
    );
    let keys: Vec<&str> = rows.iter().map(|r| r[1].as_str()).collect();
    assert_eq!(keys, vec!["k1", "k2", "k3"]);
}

/// `stack` 的两项**落在原位**：`SELECT a, stack(…), b` ⇒ `(a, c1, c2, b)`。
/// 列序与 Spark 一致，`SELECT *` 出去的表才对得上。
#[test]
fn 两列落在原位而不是追加到末尾() {
    let (db, _r) = setup("inplace");
    seed(&db);
    let (names, _) = dump(
        &db,
        "SELECT a, stack(2, 'k1', v1, 'k2', v2) AS (dt, val), b FROM t",
    );
    assert_eq!(names, vec!["a", "dt", "val", "b"]);
}

/// `FROM` 是 **CTE** 时的形态（语料里 28 个块**全是**这样）：
/// CTE 物化一次、n 个分支读同一份数据，值与列名都要对。
#[test]
fn 输入是_cte_时同样成立() {
    let (db, _r) = setup("cte");
    seed(&db);
    let (n1, r1) = dump(
        &db,
        "WITH s AS (SELECT a, v1, v2 FROM t WHERE a >= 1) \
         SELECT a, stack(2, 'p', v1, 'q', v2) AS (dt, val) FROM s ORDER BY a, dt",
    );
    let (n2, r2) = dump(
        &db,
        "WITH s AS (SELECT a, v1, v2 FROM t WHERE a >= 1) \
         SELECT a, 'p' AS dt, v1 AS val FROM s \
         UNION ALL SELECT a, 'q', v2 FROM s ORDER BY a, dt",
    );
    assert_eq!(n1, vec!["a", "dt", "val"]);
    assert_eq!(n1, n2);
    assert_eq!(r1, r2);
    assert_eq!(r1.len(), 4);
}

/// `n = 12`（语料实测的最大值，见 `work/daily/L8-02_01.sql`）。
#[test]
fn 十二对的上界() {
    let (db, _r) = setup("twelve");
    seed(&db);
    let pairs: Vec<String> = (1..=12).map(|i| format!("'k{i}', {i}")).collect();
    let sql = format!(
        "SELECT a, stack(12, {}) AS (dt, val) FROM t WHERE a = 1",
        pairs.join(", ")
    );
    let (names, rows) = dump(&db, &sql);
    assert_eq!(names, vec!["a", "dt", "val"]);
    assert_eq!(rows.len(), 12);
    assert_eq!(rows[0][1], "k1");
    assert_eq!(rows[11][1], "k12");
    assert_eq!(rows[11][2], "12");
}

/// 括号包着 `stack`：`(stack(…)) AS (a, b)` —— 括号语义透明，不接受就太苛刻了。
/// （与"括号查询体"同一口径，见 `translate_select_inner` 里那段。）
#[test]
fn 外面套一层括号也认() {
    let (db, _r) = setup("paren");
    seed(&db);
    let (n1, r1) = dump(
        &db,
        "SELECT a, (stack(2, 'p', v1, 'q', v2)) AS (dt, val) FROM t WHERE a = 1 ORDER BY dt",
    );
    let (n2, r2) = dump(
        &db,
        "SELECT a, stack(2, 'p', v1, 'q', v2) AS (dt, val) FROM t WHERE a = 1 ORDER BY dt",
    );
    assert_eq!(n1, n2);
    assert_eq!(r1, r2);
}

/// 同一投影里写**两个** `stack` ⇒ 自然得到 2×2 = 4 行的**笛卡尔**展开（与 Spark 一致）。
/// 这条是递归重写的副产品，钉住它免得以后"优化"成只展开第一个。
#[test]
fn 两个_stack_是笛卡尔展开() {
    let (db, _r) = setup("twostack");
    seed(&db);
    let (names, rows) = dump(
        &db,
        "SELECT a, stack(2, 'p', v1, 'q', v2) AS (dt, val), \
                stack(2, 'c', 10, 'd', 20) AS (dt2, val2) \
         FROM t WHERE a = 1",
    );
    assert_eq!(names, vec!["a", "dt", "val", "dt2", "val2"]);
    assert_eq!(rows.len(), 4, "{rows:?}");
    let pairs: Vec<(&str, &str)> = rows
        .iter()
        .map(|r| (r[1].as_str(), r[3].as_str()))
        .collect();
    assert_eq!(
        pairs,
        vec![("p", "c"), ("p", "d"), ("q", "c"), ("q", "d")],
        "{rows:?}"
    );
}

/// `SELECT DISTINCT … stack …` ⇒ 去重范围是**整行**，等价于 `UNION`（非 ALL）。
/// 表里故意有重复行，看它是否真的被去掉。
#[test]
fn distinct_提升到集合算子上() {
    let (db, _r) = setup("distinct");
    db.sql("CREATE TABLE d (a INT, v1 INT, v2 INT)").unwrap();
    db.sql("INSERT INTO d VALUES (1, 7, 8), (1, 7, 8), (2, 7, 8)")
        .unwrap();
    let (_, rows) = dump(
        &db,
        "SELECT DISTINCT a, stack(2, 'p', v1, 'q', v2) AS (dt, val) FROM d",
    );
    // 行1、行2 展开后完全相同 ⇒ 去重成一份；行3 的 a 不同 ⇒ 另一份。共 4 行。
    assert_eq!(rows.len(), 4, "{rows:?}");
}

/// 兄弟投影项里有 `*` —— 原样复制进每个分支，展开与分支无关（不额外处理即正确）。
#[test]
fn 与_select_star_同用() {
    let (db, _r) = setup("star");
    db.sql("CREATE TABLE s (a INT, v1 INT, v2 INT)").unwrap();
    db.sql("INSERT INTO s VALUES (1, 7, 8)").unwrap();
    let (names, rows) = dump(
        &db,
        "SELECT *, stack(2, 'p', v1, 'q', v2) AS (dt, val) FROM s",
    );
    assert_eq!(names, vec!["a", "v1", "v2", "dt", "val"]);
    assert_eq!(rows.len(), 2, "{rows:?}");
    assert_eq!(rows[0], vec!["1", "7", "8", "p", "7"]);
    assert_eq!(rows[1], vec!["1", "7", "8", "q", "8"]);
}

// ---------------------------------------------------------------------------
// 边界：全部**显式拒**（设计稿 §2.3）。一条都不许静默换个做法。
// ---------------------------------------------------------------------------

fn err_of(db: &Qsdb, sql: &str) -> String {
    db.sql_collect(sql)
        .err()
        .unwrap_or_else(|| panic!("本该报错却跑通了: {sql}"))
        .to_string()
}

#[test]
fn 没有两列别名就拒() {
    let (db, _r) = setup("noalias");
    seed(&db);
    // 完全没有别名
    let e = err_of(&db, "SELECT a, stack(2, 'p', v1, 'q', v2) FROM t");
    assert!(e.contains("two-column alias"), "unexpected: {e}");
    // 只有一列别名（Spark 会在这儿给两列编 col1/col2，本仓不编）
    let e = err_of(&db, "SELECT a, stack(2, 'p', v1, 'q', v2) AS x FROM t");
    assert!(e.contains("two-column alias"), "unexpected: {e}");
    // 别名给了三个
    let e = err_of(
        &db,
        "SELECT a, stack(2, 'p', v1, 'q', v2) AS (x, y, z) FROM t",
    );
    assert!(
        e.contains("exactly 2 output column aliases"),
        "unexpected: {e}"
    );
}

#[test]
fn 被别的表达式包住就拒() {
    let (db, _r) = setup("wrapped");
    seed(&db);
    let e = err_of(
        &db,
        "SELECT a, stack(2, 'p', v1, 'q', v2) + 1 AS (x, y) FROM t",
    );
    assert!(e.contains("top-level SELECT item"), "unexpected: {e}");
    let e = err_of(
        &db,
        "SELECT a, abs(stack(2, 'p', v1, 'q', v2)) AS (x, y) FROM t",
    );
    assert!(e.contains("top-level SELECT item"), "unexpected: {e}");
}

#[test]
fn 参数个数与_n_对不上就拒() {
    let (db, _r) = setup("arity");
    seed(&db);
    // 说 2 对，只给了 1 对半
    let e = err_of(&db, "SELECT stack(2, 'p', v1, 'q') AS (x, y) FROM t");
    assert!(e.contains("needs exactly 5 arguments"), "unexpected: {e}");
    // 说 3 对，给了 4 对
    let e = err_of(
        &db,
        "SELECT stack(3, 'p', v1, 'q', v2, 'r', v3, 's', v1) AS (x, y) FROM t",
    );
    assert!(e.contains("needs exactly 7 arguments"), "unexpected: {e}");
}

#[test]
fn n_不是正整数字面量就拒() {
    let (db, _r) = setup("bign");
    seed(&db);
    // 字符串
    let e = err_of(&db, "SELECT stack('2', 'p', v1, 'q', v2) AS (x, y) FROM t");
    assert!(e.contains("positive integer literal"), "unexpected: {e}");
    // 动态列
    let e = err_of(&db, "SELECT stack(a, 'p', v1, 'q', v2) AS (x, y) FROM t");
    assert!(e.contains("positive integer literal"), "unexpected: {e}");
    // 0 对
    let e = err_of(&db, "SELECT stack(0) AS (x, y) FROM t");
    assert!(e.contains("positive integer literal"), "unexpected: {e}");
}

/// **聚合与 `stack` 混用**：重写会把聚合放进**每个分支各算一遍**，
/// 而 Spark 是"先分组、再对结果展开" —— 两者**不报错、只错值**。
/// 这是这一族里最该拦住的一条。
#[test]
fn 与聚合_group_by_混用就拒() {
    let (db, _r) = setup("agg");
    seed(&db);
    // 兄弟投影项里有聚合
    let e = err_of(
        &db,
        "SELECT sum(a), stack(2, 'p', 1, 'q', 2) AS (dt, v) FROM t",
    );
    assert!(
        e.contains("aggregate / window functions"),
        "unexpected: {e}"
    );
    // stack 参数里有聚合
    let e = err_of(
        &db,
        "SELECT a, stack(2, 'p', sum(v1), 'q', v2) AS (dt, v) FROM t",
    );
    assert!(
        e.contains("aggregate / window functions"),
        "unexpected: {e}"
    );
    // GROUP BY
    let e = err_of(
        &db,
        "SELECT a, stack(2, 'p', v1, 'q', v2) AS (dt, v) FROM t GROUP BY a",
    );
    assert!(e.contains("GROUP BY / HAVING"), "unexpected: {e}");
    // 窗口函数
    let e = err_of(
        &db,
        "SELECT row_number() OVER (ORDER BY a) AS rn, \
                stack(2, 'p', v1, 'q', v2) AS (dt, v) FROM t",
    );
    assert!(
        e.contains("aggregate / window functions"),
        "unexpected: {e}"
    );
}
