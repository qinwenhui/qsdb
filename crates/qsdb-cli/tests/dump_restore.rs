//! **逻辑备份的闭环**：`qsdb dump --data` → 一个新库 → `qsdb restore` → 逐值一致。
//!
//! 这是"备份"这个词**唯一有意义的验收** —— 导出的东西必须**真的能恢复**，
//! 且恢复出来的数据**逐值等于**原来那份。只测"文件生成了"等于没测。
//!
//! ⚠️ 重点在**分区表**：导出走 hive 布局（分区列只在目录名里），读回来时
//! `read_parquet` 给的列序是「数据列 ++ 分区列」，而表的列序是**建表时那个序**
//! （分区列可能夹在中间）⇒ 若按位置灌就是**静默错值**（值全在、全串位）。
//! 所以下面专门造一张"分区列夹在中间"的表。

use std::process::Command;

/// 测试目录唯一序号：pid 挡跨进程碰撞，序号挡同进程内并发（Windows 时钟
/// 粒度 ~100ns，纳秒后缀在同 tick 内会重复——CI 上真炸过，见 constraints）。
fn _uniq_seq() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    SEQ.fetch_add(1, Ordering::Relaxed)
}

fn tmp(tag: &str) -> std::path::PathBuf {
    let p = std::env::temp_dir().join("qsdb_dumprestore").join(format!(
        "{tag}_{:x}_{}",
        std::process::id(),
        _uniq_seq()
    ));
    std::fs::create_dir_all(&p).unwrap();
    p
}

/// 跑一次 `qsdb <args>`，失败就带上 stderr 炸。
fn qsdb(args: &[&str]) -> String {
    let out = Command::new(env!("CARGO_BIN_EXE_qsdb"))
        .args(args)
        .output()
        .expect("spawn qsdb");
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    assert!(
        out.status.success(),
        "qsdb {args:?} 失败\nstdout: {stdout}\nstderr: {stderr}"
    );
    stdout
}

fn sql(path: &std::path::Path, s: &str) -> String {
    qsdb(&["query", "--path", &path.to_string_lossy(), s])
}

#[test]
fn dump_restore_闭环逐值一致() {
    let src = tmp("src");
    let dst = tmp("dst");
    let bak = tmp("bak");
    let p = |d: &std::path::Path| d.to_string_lossy().into_owned();

    // ① 造数据。**含一张分区列夹在中间的表**（`dt` 在第一列、`region` 在第三列）
    sql(&src, "CREATE TABLE plain (id BIGINT, s STRING)");
    sql(&src, "INSERT INTO plain VALUES (1,'a'),(2,'b'),(3,NULL)");
    sql(
        &src,
        "CREATE TABLE part (dt DATE, id BIGINT, region STRING, v BIGINT) \
         PARTITIONED BY (dt, region)",
    );
    sql(
        &src,
        "INSERT INTO part VALUES \
         (DATE '2026-01-01', 1, 'cn', 10), (DATE '2026-01-01', 2, 'cn', 20), \
         (DATE '2026-01-02', 3, 'us', 30)",
    );
    sql(&src, "CREATE TABLE empty_t (a BIGINT, b STRING)");
    sql(&src, "CREATE VIEW v1 AS SELECT id FROM plain");

    // ② 导出（结构 + 数据）
    qsdb(&["dump", "--path", &p(&src), "--data", &p(&bak)]);
    assert!(bak.join("tables/plain.sql").is_file(), "缺 plain.sql");
    assert!(bak.join("tables/part.sql").is_file(), "缺 part.sql");
    assert!(bak.join("views/v1.sql").is_file(), "缺 v1.sql");
    assert!(bak.join("data/plain").is_dir(), "缺 data/plain");
    // ★ **备份不做 hive 分区布局**（与 `COPY … TO (PARTITION_BY …)` 那条刻意不同）：
    //   分区布局会把分区值写成**目录名文本**，读回来靠类型推断 —— 那是"湖导出"
    //   该关心的事（要让 Spark/pyarrow 读）；**备份要的是保真**：值留在数据文件里，
    //   类型原样、不经过任何推断。所以 `dump` 走的是**不带 PARTITION_BY** 的 COPY。
    assert!(
        bak.join("data/part").is_dir(),
        "备份的数据目录（平铺，不是 hive 布局）"
    );
    let n_parts = std::fs::read_dir(bak.join("data/part"))
        .unwrap()
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().ends_with(".parquet"))
        .count();
    assert!(n_parts > 0, "分区表的数据该有 part 文件");

    // ③ 恢复到一个**全新的空库**
    qsdb(&["restore", "--path", &p(&dst), "--from", &p(&bak)]);

    // ④ 逐值比（按**行序**比 —— 下面每条都有 ORDER BY）
    for (q, want) in [
        (
            "SELECT id, s FROM plain ORDER BY id",
            vec!["1|a", "2|b", "3|NULL"],
        ),
        (
            "SELECT dt, id, region, v FROM part ORDER BY id",
            vec![
                "2026-01-01|1|cn|10",
                "2026-01-01|2|cn|20",
                "2026-01-02|3|us|30",
            ],
        ),
        ("SELECT count(*) FROM empty_t", vec!["0"]),
        ("SELECT id FROM v1 ORDER BY id", vec!["1", "2", "3"]),
    ] {
        assert_eq!(rows(&dst, q), want, "查询：{q}");
    }

    for d in [src, dst, bak] {
        std::fs::remove_dir_all(d).ok();
    }
}

/// 目标库非空 ⇒ **拒**（不做静默覆盖：往半个库里灌备份，出来的东西既不是备份
/// 也不是原库，而没人看得出）。
#[test]
fn restore_到非空库要拒() {
    let src = tmp("src2");
    let bak = tmp("bak2");
    let dst = tmp("dst2");
    let p = |d: &std::path::Path| d.to_string_lossy().into_owned();
    sql(&src, "CREATE TABLE t (a BIGINT)");
    qsdb(&["dump", "--path", &p(&src), "--data", &p(&bak)]);
    sql(&dst, "CREATE TABLE already (a BIGINT)");

    let out = Command::new(env!("CARGO_BIN_EXE_qsdb"))
        .args(["restore", "--path", &p(&dst), "--from", &p(&bak)])
        .output()
        .unwrap();
    assert!(!out.status.success(), "非空库该被拒");
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("不是空的") && err.contains("already"), "{err}");

    for d in [src, dst, bak] {
        std::fs::remove_dir_all(d).ok();
    }
}

/// 只导结构（不带 `--data`）→ 打到 stdout，可直接重定向成 `.sql`。
#[test]
fn dump_只导结构到_stdout() {
    let src = tmp("src3");
    let p = src.to_string_lossy().into_owned();
    sql(
        &src,
        "CREATE TABLE t (a BIGINT NOT NULL, s STRING COMMENT 'x')",
    );
    sql(&src, "CREATE VIEW vv AS SELECT a FROM t");

    let out = qsdb(&["dump", "--path", &p]);
    assert!(out.contains("CREATE TABLE t ("), "{out}");
    assert!(out.contains("a BIGINT NOT NULL"), "{out}");
    assert!(out.contains("CREATE VIEW vv AS"), "{out}");
    // 只有指定的表
    let one = qsdb(&["dump", "--path", &p, "--table", "t"]);
    assert!(!one.contains("CREATE VIEW"), "指定单表时不该带视图：{one}");

    std::fs::remove_dir_all(&src).ok();
}

/// 跑一条查询，把 `qsdb query` 的**表格输出**剥成 `行内单元格用 | 连接` 的列表。
///
/// `print_result` 的形状：表头行 / 分隔线（含 `-`）/ 数据行 / `(N rows, T)` 尾行。
/// 有 ORDER BY 的查询按原序返回（**不做排序** —— 排序会掩盖"顺序错"那类 bug）。
fn rows(path: &std::path::Path, s: &str) -> Vec<String> {
    let out = sql(path, s);
    let lines: Vec<&str> = out.lines().collect();
    let mut out_rows = Vec::new();
    for (i, l) in lines.iter().enumerate() {
        // 跳过表头（第一行）、分隔线、以及尾部的 `(N rows, …)`
        if i == 0 || l.contains("---") || l.trim_start().starts_with('(') {
            continue;
        }
        if l.trim().is_empty() {
            continue;
        }
        let cells: Vec<&str> = l.split('|').map(str::trim).collect();
        out_rows.push(cells.join("|"));
    }
    out_rows
}
