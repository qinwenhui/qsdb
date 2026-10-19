//! **支持面文档里的"自述"逐条实测**。
//!
//! 起因（2026-10-15，F29②）：`内部 SQL 支持面清单` 写着 `GROUP BY 1` 的报错文案
//! "已明确（此前是误导文案）"，实测**仍是误导文案** —— 修复根本没落地，
//! 而文档替它打了包票。
//!
//! ⇒ 凡文档里出现「**已明确 / 已修 / 报错文案已…**」这类**自述**，一律要能兑现：
//! 本文件逐条跑一遍、把**引擎原话**打出来。文档说错了比不说更坏（对接方按它
//! 写代码，到运行期才炸，还顺带怀疑整份支持面的可信度）。

use qsdb::{Qsdb, QsdbOptions};

fn fresh(tag: &str) -> (Qsdb, std::path::PathBuf) {
    let d = std::env::temp_dir().join("qsdb_claims").join(format!(
        "{tag}_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&d).unwrap();
    let db = Qsdb::open(&d, QsdbOptions::default()).unwrap();
    db.sql("CREATE TABLE t (g BIGINT, v BIGINT)").unwrap();
    db.sql("INSERT INTO t VALUES (1, 10), (2, 20)").unwrap();
    (db, d)
}

/// 跑一组 SQL，把**引擎原话**打出来（不预设结论）。
fn report(db: &Qsdb, title: &str, sqls: &[&str]) {
    eprintln!("\n=== {title} ===");
    for sql in sqls {
        match db.sql(sql) {
            Ok(_) => eprintln!("[OK ] {sql}"),
            Err(e) => eprintln!("[ERR] {sql}\n      → {e}"),
        }
    }
}

/// §一 第 46 行：`WITH w(q) AS …`（CTE 列别名）—— 文档说此前"别名被丢掉、
/// 之后报'列不存在'（误导）"，隐含**已改**。核实现在给的是什么。
#[test]
fn 自述核实_cte_列别名() {
    let (db, root) = fresh("cte_alias");
    report(
        &db,
        "CTE 列别名 WITH w(q) AS …",
        &[
            "WITH w(q) AS (SELECT g FROM t) SELECT q FROM w",
            "WITH w AS (SELECT g FROM t) SELECT g FROM w", // 对照：不带别名（应 OK）
        ],
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// §二 第 86 行：派生表列别名 `… AS d(a,b)` —— 同一条自述。
#[test]
fn 自述核实_派生表列别名() {
    let (db, root) = fresh("derived_alias");
    report(
        &db,
        "派生表列别名 … AS d(a,b)",
        &[
            "SELECT d.x FROM (SELECT g FROM t) AS d(x)",
            "SELECT d.g FROM (SELECT g FROM t) AS d", // 对照：不带别名（应 OK）
        ],
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// §五 第 137 行：CTAS —— 文档说"报错文案已明确（此前是 'table needs at least
/// one column'，误导）"。
#[test]
fn 自述核实_ctas() {
    let (db, root) = fresh("ctas");
    report(
        &db,
        "CREATE TABLE … AS SELECT",
        &["CREATE TABLE t2 AS SELECT g, v FROM t"],
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// §五 第 138 行：`CREATE OR REPLACE TABLE` —— 文档**自认**"目前报 already exists
/// （已知误导，排期中改明确文案）"。核实一下，并看看现在这条改没改。
#[test]
fn 自述核实_create_or_replace() {
    let (db, root) = fresh("cor");
    report(
        &db,
        "CREATE OR REPLACE TABLE",
        &[
            "CREATE OR REPLACE TABLE t (g BIGINT)",
            "CREATE OR REPLACE TABLE 新表 (g BIGINT)", // 表**不存在**时的写法
        ],
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// §三 第 111 行 / §四 第 121 行：文档说"报错会**列出支持清单**"。
/// 只写"不支持"而不列清单，等于把猜谜留给调用方 —— 这条自述也要兑现。
#[test]
fn 自述核实_报错列支持清单() {
    let (db, root) = fresh("lists");
    report(
        &db,
        "标量函数 / 窗口函数 的报错是否列清单",
        &[
            "SELECT greatest(g, v) FROM t",
            "SELECT array_agg(g) FROM t",
            "SELECT ntile(4) OVER (ORDER BY g) FROM t",
            "SELECT first_value(g) OVER (ORDER BY g) FROM t",
            "SELECT nonexistent_fn(g) FROM t",
        ],
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// §三 第 110 行：`位置参数 ?` 那行写着「本仓走 **HTTP/API 层参数化**，
/// 不做服务端占位符」——
/// **但我们根本没有参数化查询**（协议面是 `{"sql": …}` 纯文本）。这条自述
/// 是**假的**，而且正好会把驱动引到"客户端自己拼字符串"这条路上。
#[test]
fn 自述核实_位置参数那行的说法() {
    let (db, root) = fresh("params");
    report(
        &db,
        "占位符 `?`",
        &[
            "SELECT g FROM t WHERE v = ?",
            "PREPARE p AS SELECT g FROM t WHERE v = $1",
        ],
    );
    let _ = std::fs::remove_dir_all(&root);
}
