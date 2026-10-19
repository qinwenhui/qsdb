//! **常量数组必须广播到批长**——同一族坑的**四个**落点，一次性钉住。
//!
//! `eval` 对常量表达式返回**长度 1** 的数组，而下游处处按"批内行号"索引它。
//! 不广播就有两种下场，都很难看：
//! - `RecordBatch::try_new` 报各列长度不一致（**内部话术**，用户看不懂）；
//! - 或者**直接越界 panic**（被 panic 隔离层兜住，用户看到"查询 panic"）。
//!
//! 这四条**全都是 2026-10-17 实测挖出来的**，且都不是本次改动引入的 ——
//! 它们是"顺带"：改动 ② 把常量广播修好之后，才敢确认这一族到底有几个落点。
//! 之所以此前没人撞见，是因为最常见的形态（`SELECT 1, g FROM t`）报的是
//! 内部错误、看着像"引擎坏了"而不是"我写错了"，于是没人往这上面想。

use qsdb::{Qsdb, QsdbOptions};

/// 测试目录唯一序号：pid 挡跨进程碰撞，序号挡同进程内并发（Windows 时钟
/// 粒度 ~100ns，纳秒后缀在同 tick 内会重复——CI 上真炸过，见 constraints）。
fn _uniq_seq() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    SEQ.fetch_add(1, Ordering::Relaxed)
}

fn setup(tag: &str) -> (Qsdb, std::path::PathBuf) {
    let root = std::env::temp_dir().join("qsdb_constbc").join(format!(
        "{tag}_{:x}_{}",
        std::process::id(),
        _uniq_seq()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    db.sql("CREATE TABLE t (g BIGINT, v BIGINT)").unwrap();
    db.sql("INSERT INTO t VALUES (1,10),(2,20),(1,30)").unwrap();
    (db, root)
}

fn q(db: &Qsdb, sql: &str) -> Vec<String> {
    let batches = db.sql_collect(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    let mut out = Vec::new();
    for b in &batches {
        for i in 0..b.num_rows() {
            out.push(
                (0..b.num_columns())
                    .map(|c| {
                        arrow::util::display::array_value_to_string(b.column(c).as_ref(), i)
                            .unwrap()
                    })
                    .collect::<Vec<_>>()
                    .join("|"),
            );
        }
    }
    out.sort();
    out
}

// ── ① 投影：常量列 + 真列**混着写** ───────────────────────────────────────
//
// 这是最普通的一句 SQL，此前**必炸**（`all columns in a record batch must have
// the same length`）。全常量（`SELECT 1 FROM t`）靠事后那段广播侥幸过得去 ⇒
// 这个洞**只对"混合"这一种形态显形**。
#[test]
fn 投影里的常量列混真列() {
    let (db, root) = setup("proj");
    assert_eq!(q(&db, "SELECT 1, g FROM t"), vec!["1|1", "1|1", "1|2"]);
    assert_eq!(q(&db, "SELECT g, 1 FROM t"), vec!["1|1", "1|1", "2|1"]);
    assert_eq!(
        q(&db, "SELECT g, 'x' AS tag, v FROM t"),
        vec!["1|x|10", "1|x|30", "2|x|20"]
    );
    // 表达式折叠出来的常量同样算常量
    assert_eq!(q(&db, "SELECT g, 1 + 1 FROM t"), vec!["1|2", "1|2", "2|2"]);
    // 空结果也要对齐（常量广播到 0 行，不能留一条长度 1 的列）
    assert_eq!(
        q(&db, "SELECT 1, g FROM t WHERE g = 999"),
        Vec::<String>::new()
    );
    // 对照：全常量照旧
    assert_eq!(q(&db, "SELECT 1 FROM t"), vec!["1", "1", "1"]);
    let _ = std::fs::remove_dir_all(&root);
}

// ── ② 聚合的**分组键**是常量 ──────────────────────────────────────────────
//
// `GROUP BY 'k'` 此前**panic**（`Trying to access an element at index 1 from a
// StringArray of length 1`）。`GROUP BY 1` 指向常量时走的是同一条路。
#[test]
fn 常量分组键() {
    let (db, root) = setup("gkey");
    assert_eq!(
        q(&db, "SELECT g, sum(v) FROM t GROUP BY 'k', g ORDER BY g"),
        vec!["1|40", "2|20"]
    );
    assert_eq!(
        q(&db, "SELECT g, sum(v) FROM t GROUP BY g, 'k' ORDER BY g"),
        vec!["1|40", "2|20"]
    );
    // 经由序号指向常量投射列（`GROUP BY 1, 2`：1 是常量、2 是 g）
    assert_eq!(
        q(&db, "SELECT 1, g, sum(v) FROM t GROUP BY 1, 2 ORDER BY 2"),
        vec!["1|1|40", "1|2|20"]
    );
    let _ = std::fs::remove_dir_all(&root);
}

// ── ③ 聚合的**参数**是常量（`sum(1)`）───────────────────────────────────
#[test]
fn 常量聚合参数() {
    let (db, root) = setup("aarg");
    // sum(1) 逐行计 1 ⇒ 组内行数
    assert_eq!(
        q(&db, "SELECT g, sum(1) FROM t GROUP BY g ORDER BY g"),
        vec!["1|2", "2|1"]
    );
    assert_eq!(
        q(&db, "SELECT max(1), g FROM t GROUP BY g"),
        vec!["1|1", "1|2"]
    );
    assert_eq!(q(&db, "SELECT count(1) FROM t"), vec!["3"]);
    let _ = std::fs::remove_dir_all(&root);
}

// ── ④ JOIN 的**残留谓词**是常量（`ON a.g = b.g AND true`）────────────────
#[test]
fn 常量连接残留谓词() {
    let (db, root) = setup("jres");
    // 等值连接 = 5 对：a.g=1 配 b 的两个 g=1、a.g=2 配一个、a.g=1 再配两个
    assert_eq!(
        q(&db, "SELECT a.g FROM t a JOIN t b ON a.g = b.g AND true"),
        vec!["1", "1", "1", "1", "2"]
    );
    // 残留判否时 LEFT 的未匹配行要保留（不能因为残留是常量就整体丢行）
    assert_eq!(
        q(
            &db,
            "SELECT a.g FROM t a LEFT JOIN t b ON a.g = b.g AND b.v > 0"
        ),
        vec!["1", "1", "1", "1", "2"]
    );
    let _ = std::fs::remove_dir_all(&root);
}

// ── ⑤ 连带修的一处**静默无效**：`ORDER BY <越界序号>` ────────────────────
//
// 此前它是"借用 ① 那个 bug 才报错"的：越界序号退回"当常量排序键"，
// 常量列与真列对不齐 ⇒ 抛内部错误。① 修好后它就**静默变成不排序**了
//（用户以为排了、其实没有），`sql_gaps::order_by_ordinal` 当场抓到。
// 现在显式校验序号范围。
#[test]
fn 越界序号显式拒而不是静默不排() {
    let (db, root) = setup("ordr");
    let e = match db.sql("SELECT g FROM t ORDER BY 5") {
        Ok(_) => panic!("越界序号必须报错（否则等于不排序，静默无效）"),
        Err(e) => e.to_string(),
    };
    assert!(e.contains("超出输出列范围"), "{e}");
    assert!(db.sql("SELECT g FROM t ORDER BY 0").is_err());
    assert!(db.sql("SELECT g FROM t ORDER BY 1.5").is_err());
    // 对照：合法序号 / 常量排序键（合法但无意义）照旧可用
    assert_eq!(
        q(&db, "SELECT g, v FROM t ORDER BY 2"),
        vec!["1|10", "1|30", "2|20"]
    );
    assert_eq!(q(&db, "SELECT g FROM t ORDER BY 'k'").len(), 3);
    let _ = std::fs::remove_dir_all(&root);
}
