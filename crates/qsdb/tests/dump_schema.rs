//! **`SHOW CREATE TABLE` / 库结构导出** 的验收。
//!
//! 唯一的硬要求是**往返**：
//!
//! ```text
//! 表 A ──SHOW CREATE TABLE──▶ DDL 文本 ──在新库里 CREATE──▶ 表 B
//!                              A 与 B 必须逐字段相等
//! ```
//!
//! 这条断了，导出的"备份"就是**不能恢复的** —— 那比没有备份更坏（你以为有）。
//! 所以下面用**真的建回去再比**，不比对字符串。

use qsdb::{Qsdb, QsdbOptions};

/// 测试目录唯一序号：pid 挡跨进程碰撞，序号挡同进程内并发（Windows 时钟
/// 粒度 ~100ns，纳秒后缀在同 tick 内会重复——CI 上真炸过，见 constraints）。
fn _uniq_seq() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    SEQ.fetch_add(1, Ordering::Relaxed)
}

fn db_at(tag: &str) -> (Qsdb, std::path::PathBuf) {
    let root = std::env::temp_dir().join("qsdb_dump").join(format!(
        "{tag}_{:x}_{}",
        std::process::id(),
        _uniq_seq()
    ));
    std::fs::create_dir_all(&root).unwrap();
    (Qsdb::open(&root, QsdbOptions::default()).unwrap(), root)
}

fn ddl(db: &Qsdb, table: &str) -> String {
    let b = db
        .sql_collect(&format!("SHOW CREATE TABLE {table}"))
        .unwrap_or_else(|e| panic!("{table}: {e}"));
    arrow::util::display::array_value_to_string(b[0].column(1).as_ref(), 0).unwrap()
}

/// 除 `created_at` 外逐字段相等（`created_at` 是建表时刻，天然不同）。
fn same(a: &qsdb::catalog::TableDef, b: &qsdb::catalog::TableDef) -> bool {
    a.name == b.name
        && a.version == b.version
        && a.columns == b.columns
        && a.partition_keys == b.partition_keys
        && a.properties == b.properties
}

/// ★ 核心：**建回去，逐字段比**。
#[test]
fn ddl_往返() {
    let (db, root) = db_at("rt");
    let cases = [
        // 最简单
        "CREATE TABLE a (id BIGINT, s STRING)",
        // NOT NULL / 分区
        "CREATE TABLE b (dt DATE, id BIGINT NOT NULL, s STRING) PARTITIONED BY (dt)",
        // CHECK / COMMENT
        "CREATE TABLE c (v BIGINT CHECK (v > 0), s STRING COMMENT '名字')",
        // 多分区键 + 表属性
        "CREATE TABLE d (dt DATE, region STRING, v BIGINT) PARTITIONED BY (dt, region) \
         WITH (compression = 'snappy')",
        // 各类型都过一遍
        "CREATE TABLE e (b BOOLEAN, i8 TINYINT, i16 SMALLINT, i32 INT, i64 BIGINT, \
         f32 FLOAT, f64 DOUBLE, dec DECIMAL(18,4), s STRING, s10 VARCHAR(10), \
         c5 CHAR(5), bin BINARY, d DATE, t TIME, ts TIMESTAMP)",
    ];
    for sql in cases {
        db.sql(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
    }

    // 换一个**全新的库**，只喂 DDL
    let (db2, root2) = db_at("rt2");
    for sql in cases {
        let name = sql.split_whitespace().nth(2).unwrap().to_string();
        let d = ddl(&db, &name);
        db2.sql(&d)
            .unwrap_or_else(|e| panic!("用导出的 DDL 建不回去：\n{d}\n错误：{e}"));
    }

    for sql in cases {
        let name = sql.split_whitespace().nth(2).unwrap().to_string();
        let a = db.table_def(&name).unwrap();
        let b = db2.table_def(&name).unwrap();
        assert!(
            same(&a, &b),
            "`{name}` 往返后结构不一致\n--- 原表 ---\n{a:#?}\n--- 重建 ---\n{b:#?}\n--- DDL ---\n{}",
            ddl(&db, &name)
        );
    }
    for r in [root, root2] {
        std::fs::remove_dir_all(r).ok();
    }
}

/// 同一张表**两次 dump 必须逐字节相同** —— `properties` 是 `HashMap`，
/// 迭代序每次进程都不同；不排序的话备份脚本会天天"变"（diff 全是噪声）。
#[test]
fn dump_是确定的() {
    let (db, root) = db_at("det");
    db.sql(
        "CREATE TABLE t (a BIGINT, b STRING, c DATE) \
         WITH (compression = 'snappy', owner = 'ops', tier = 'hot')",
    )
    .unwrap();
    let once = ddl(&db, "t");
    let twice = ddl(&db, "t");
    assert_eq!(once, twice, "两次 dump 不一致：\n{once}\n---\n{twice}");
    // 键是**排序**输出的（否则上面这条在单进程里可能偶然通过）
    let i = once.find("WITH (").expect("该有 WITH");
    let body = &once[i..];
    let mut keys: Vec<&str> = body
        .trim_start_matches("WITH (")
        .split('=')
        .filter_map(|s| s.split(',').next_back())
        .map(str::trim)
        .collect();
    keys.pop(); // 最后一段是 ')' 尾巴
    let mut sorted = keys.clone();
    sorted.sort();
    assert_eq!(keys, sorted, "属性键没排序：{once}");
    std::fs::remove_dir_all(&root).ok();
}

/// 注释里的单引号要转义，否则 DDL 解析不了（**这是个能静默生成坏备份的坑**）。
#[test]
fn 注释里的单引号要转义() {
    let (db, root) = db_at("esc");
    db.sql("CREATE TABLE t (v BIGINT COMMENT 'don''t panic')")
        .unwrap();
    let d = ddl(&db, "t");
    assert!(d.contains("don''t panic"), "转义后的 DDL：{d}");

    let (db2, root2) = db_at("esc2");
    db2.sql(&d).unwrap_or_else(|e| panic!("{d}\n{e}"));
    assert_eq!(
        db.table_def("t").unwrap().columns,
        db2.table_def("t").unwrap().columns
    );
    std::fs::remove_dir_all(&root).ok();
    std::fs::remove_dir_all(&root2).ok();
}

/// 不存在的表 → 明确报错（不许回一个空的 DDL）。
#[test]
fn 表不存在要报错() {
    let (db, root) = db_at("nf");
    let e = db.sql_collect("SHOW CREATE TABLE nope").unwrap_err();
    let m = e.to_string();
    assert!(m.contains("nope"), "{m}");
    std::fs::remove_dir_all(&root).ok();
}

/// `SHOW CREATE TABLE` 是**只读**语句 —— reader 角色能用（备份不该要写权限）。
#[test]
fn 只读角色可用() {
    let (db, root) = db_at("role");
    db.sql("CREATE TABLE t (a BIGINT)").unwrap();
    db.sql_with_role("SHOW CREATE TABLE t", qsdb::users::Role::Reader)
        .expect("reader 应当能导出结构");
    std::fs::remove_dir_all(&root).ok();
}

/// 导出的 DDL **带分号、可直接喂脚本**（一次一条）。
#[test]
fn ddl_带分号且单语句可执行() {
    let (db, root) = db_at("semi");
    db.sql("CREATE TABLE t (a BIGINT)").unwrap();
    let d = ddl(&db, "t");
    assert!(d.ends_with(';'), "{d}");
    // 本仓一次只收一条语句 —— 结尾分号必须能被吃掉
    let (db2, root2) = db_at("semi2");
    db2.sql(&d).unwrap();
    std::fs::remove_dir_all(&root).ok();
    std::fs::remove_dir_all(&root2).ok();
}
