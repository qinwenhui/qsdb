//! **无分区表**的单一真值来源 + 写者身份（F26 同族，内核自查发现）。
//!
//! 无分区表看起来是"退化特例"，其实 `write::pdir()` 对空分区键返回 `table_dir`
//! —— 它**有 manifest，而且就在表目录下**，与分区表是同一套提交协议。三处代码
//! 按"特例"处理，各自错开：
//!
//! ① `clear_unpartitioned`（TRUNCATE）直接搬 parquet 而**不动 manifest** ⇒
//!    manifest 继续引用已搬走的段，`SELECT` 从此永久报 `open failed`（表搞坏）。
//! ② `scan_metadata_count`（count(*)）按**列目录**求和 ⇒ 与按 manifest 的
//!    `SELECT` 是两个真值来源；写路径"延迟回收"留在原地的旧段被算进去 ⇒
//!    `INSERT OVERWRITE` 后 `SELECT *`=1 行而 `count(*)`=3 行（**静默错值**）。
//! ③ 清空/compact 不校验写者身份代（F26 同族），过期写者能清掉新代数据。
//!
//! 本文件锁住这三条：manifest 是唯一真值、count 与 select 恒等、破坏性写要判身份。

use qsdb::{Qsdb, QsdbOptions};

/// 测试目录唯一序号：pid 挡跨进程碰撞，序号挡同进程内并发（Windows 时钟
/// 粒度 ~100ns，纳秒后缀在同 tick 内会重复——CI 上真炸过，见 constraints）。
fn _uniq_seq() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    SEQ.fetch_add(1, Ordering::Relaxed)
}

fn tmp(tag: &str) -> std::path::PathBuf {
    let root = std::env::temp_dir()
        .join("qsdb_unpartitioned_test")
        .join(format!("{tag}_{:x}_{}", std::process::id(), _uniq_seq()));
    let _ = std::fs::remove_dir_all(&root);
    root
}

fn opts(epoch: u64) -> QsdbOptions {
    QsdbOptions {
        writer_epoch: epoch,
        ..Default::default()
    }
}

/// 单列单行结果渲染成字符串（`SELECT count(*)` / 单值用）。
fn scalar(db: &Qsdb, sql: &str) -> String {
    let bs = db
        .sql_collect(sql)
        .unwrap_or_else(|e| panic!("{sql} 应成功，实得 {e}"));
    bs.iter()
        .map(|b| {
            (0..b.num_rows())
                .map(|i| {
                    arrow::util::display::array_value_to_string(b.column(0).as_ref(), i).unwrap()
                })
                .collect::<Vec<_>>()
                .join(",")
        })
        .collect::<Vec<_>>()
        .join("|")
}

fn rows(db: &Qsdb, sql: &str) -> usize {
    db.sql_collect(sql)
        .unwrap_or_else(|e| panic!("{sql} 应成功，实得 {e}"))
        .iter()
        .map(|b| b.num_rows())
        .sum()
}

fn sql_err(db: &Qsdb, sql: &str) -> String {
    match db.sql(sql) {
        Ok(_) => panic!("本应被拒却成功了: {sql}"),
        Err(e) => format!("{e}"),
    }
}

/// 表目录下的 parquet 文件数（验"延迟回收"是否真的把旧段留在原地）。
fn parquet_files(dir: &std::path::Path) -> usize {
    std::fs::read_dir(dir)
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .filter(|e| e.file_name().to_string_lossy().ends_with(".parquet"))
                .count()
        })
        .unwrap_or(0)
}

/// **①+②：TRUNCATE 不能把表搞坏，且 count 与 select 恒等**。
#[test]
fn truncate_unpartitioned_keeps_table_usable() {
    let root = tmp("trunc");
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    db.sql("CREATE TABLE u (a BIGINT)").unwrap();
    db.sql("INSERT INTO u VALUES (1),(2)").unwrap();
    assert_eq!(scalar(&db, "SELECT count(*) FROM u"), "2");

    db.sql("TRUNCATE TABLE u").unwrap();
    assert_eq!(scalar(&db, "SELECT count(*) FROM u"), "0");
    // 关键：`SELECT` 不许报 open failed（旧实现把段搬走却留着引用它的 manifest）
    assert_eq!(rows(&db, "SELECT * FROM u"), 0, "清空后表必须仍可读");

    // 清空后还能继续写，且只看到新数据
    db.sql("INSERT INTO u VALUES (7)").unwrap();
    assert_eq!(scalar(&db, "SELECT count(*) FROM u"), "1");
    assert_eq!(scalar(&db, "SELECT * FROM u"), "7");

    std::fs::remove_dir_all(&root).ok();
}

/// **②：overwrite 之后 count(*) 与 SELECT 必须一致**——写路径把旧段留在原地
/// 等 GC（保护在途扫描），列目录求和就会把它们算进去（曾回 3 而行只有 1 行）。
#[test]
fn unpartitioned_count_agrees_with_select_despite_orphans() {
    let root = tmp("orphan");
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    let tdir = root.join("default").join("u");
    db.sql("CREATE TABLE u (a BIGINT)").unwrap();
    db.sql("INSERT INTO u VALUES (1),(2)").unwrap();

    db.sql("INSERT OVERWRITE u VALUES (9)").unwrap();
    // 前置：旧段确实还在盘上（延迟回收语义，不是被立刻删掉）
    assert!(
        parquet_files(&tdir) >= 2,
        "旧段应留在原地等 GC（实测 {} 个文件）",
        parquet_files(&tdir)
    );
    assert_eq!(scalar(&db, "SELECT * FROM u"), "9");
    assert_eq!(
        scalar(&db, "SELECT count(*) FROM u"),
        "1",
        "count(*) 必须与 SELECT 同源（manifest），孤儿段不算数"
    );

    // 再覆盖一次，仍然一致
    db.sql("INSERT OVERWRITE u VALUES (7),(8)").unwrap();
    assert_eq!(scalar(&db, "SELECT count(*) FROM u"), "2");
    assert_eq!(rows(&db, "SELECT * FROM u"), 2);

    std::fs::remove_dir_all(&root).ok();
}

/// **③：过期写者不许清空/compact**（F26 同族——破坏性写同样是写）。
#[test]
fn stale_writer_cannot_truncate_or_compact() {
    let root = tmp("stale");
    let tdir = root.join("default").join("t");
    {
        let db = Qsdb::open(&root, opts(4)).unwrap();
        db.sql("CREATE TABLE u (a BIGINT)").unwrap();
        db.sql("INSERT INTO u VALUES (1),(2)").unwrap();
        // 分区表，攒够 segment 好让 compact 有活干（默认 min_segments=4）
        db.sql("CREATE TABLE t (id BIGINT, p BIGINT) PARTITIONED BY (p)")
            .unwrap();
        for i in 0..4 {
            db.sql(&format!("INSERT INTO t VALUES ({i}, 1)")).unwrap();
        }
    }

    let stale = Qsdb::open(&root, opts(2)).unwrap();
    assert_eq!(
        scalar(&stale, "SELECT count(*) FROM u"),
        "2",
        "读不受身份代影响"
    );

    // ① 无分区表清空被拒
    let e = sql_err(&stale, "TRUNCATE TABLE u");
    assert!(e.contains("stale writer"), "实得 {e}");
    assert_eq!(
        scalar(&stale, "SELECT count(*) FROM u"),
        "2",
        "被拒的清空不能动数据"
    );

    // ② 分区表清空被拒
    let e = sql_err(&stale, "TRUNCATE TABLE t");
    assert!(e.contains("stale writer"), "实得 {e}");
    assert_eq!(rows(&stale, "SELECT * FROM t"), 4, "被拒的清空不能动数据");

    // ③ 本地 compaction 被拒（盖章会把自己的代写回去 ⇒ 身份降级）
    let e = stale
        .compact_table_with(
            "t",
            qsdb::compact::CompactOptions {
                min_segments: 2,
                ..Default::default()
            },
        )
        .err()
        .map(|e| format!("{e}"))
        .expect("过期写者 compact 必须被拒");
    assert!(e.contains("stale writer"), "实得 {e}");
    assert_eq!(
        rows(&stale, "SELECT * FROM t"),
        4,
        "被拒的 compact 不能动数据"
    );
    // 段文件仍在（没白写、也没白删）
    let pdir = std::fs::read_dir(&tdir)
        .unwrap()
        .filter_map(|e| e.ok())
        .find(|e| e.file_name().to_string_lossy().starts_with("p="))
        .map(|e| e.path())
        .expect("分区目录");
    assert_eq!(parquet_files(&pdir), 4, "被拒的 compact 不该产出新段");

    // ④ 正当接管（epoch 5）照常可清
    drop(stale);
    let db5 = Qsdb::open(&root, opts(5)).unwrap();
    db5.sql("TRUNCATE TABLE u").unwrap();
    assert_eq!(scalar(&db5, "SELECT count(*) FROM u"), "0");

    std::fs::remove_dir_all(&root).ok();
}
