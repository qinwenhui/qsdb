//! `read_csv('路径')` 外表：RFC4180 子集解析 + 类型推断 + 规划层裁剪。
//!
//! 覆盖：基本类型与表头、引号内逗号/换行/`""` 转义、CRLF、BOM、空字段 → NULL、
//! 列裁剪与 WHERE（走外表扫描）、glob 多文件、错误路径（文件不存在 / 表头为空 /
//! 行宽不符 / 非数字值 / 未知表函数）。

use qsdb::{Qsdb, QsdbOptions};

fn tmp_root(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir()
        .join("qsdb_read_csv")
        .join(format!("{tag}_{}", uuid::Uuid::now_v7().simple()));
    std::fs::create_dir_all(&d).unwrap();
    d
}

/// 执行并取错误文案（`SqlResult` 没实现 Debug，用不了 `unwrap_err`）。
fn err(db: &Qsdb, sql: &str) -> String {
    match db.sql(sql) {
        Ok(_) => panic!("expected error: {sql}"),
        Err(e) => e.to_string(),
    }
}

fn db_at(root: &std::path::Path) -> Qsdb {
    Qsdb::open(root, QsdbOptions::default()).unwrap()
}

fn write(path: &std::path::Path, content: &str) {
    std::fs::write(path, content).unwrap();
}

fn csv_path(p: &std::path::Path) -> String {
    p.to_string_lossy().replace('\\', "/")
}

/// 行集（`v1|v2|…`，NULL → `NULL`），保持输出顺序（加 ORDER BY 时稳定）。
fn rows(db: &Qsdb, sql: &str) -> Vec<String> {
    db.sql_collect(sql)
        .unwrap()
        .iter()
        .flat_map(|b| {
            (0..b.num_rows()).map(|i| {
                (0..b.num_columns())
                    .map(|c| {
                        let a = b.column(c);
                        if a.is_null(i) {
                            "NULL".to_string()
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

fn scalar(db: &Qsdb, sql: &str) -> String {
    let b = db.sql_collect(sql).unwrap();
    arrow::util::display::array_value_to_string(b[0].column(0).as_ref(), 0).unwrap()
}

/// ① 基本类型推断 + 表头 + 列名绑定。
#[test]
fn basic_types_and_header() {
    let root = tmp_root("basic");
    let f = root.join("t.csv");
    write(
        &f,
        "id,name,ratio,flag\n1,alice,1.5,true\n2,bob,2.5,false\n3,carol,,true\n",
    );

    let db = db_at(&root);
    // 类型：id=BIGINT / ratio=DOUBLE / flag=BOOLEAN / name=VARCHAR
    let r = rows(
        &db,
        &format!(
            "SELECT id, name, ratio, flag FROM read_csv('{}') ORDER BY id",
            csv_path(&f)
        ),
    );
    assert_eq!(
        r,
        vec![
            "1|alice|1.5|true",
            "2|bob|2.5|false",
            "3|carol|NULL|true", // 空字段 → NULL
        ]
    );
    // 类型推断的旁证：数值列可参与算术与聚合
    let sql = format!(
        "SELECT sum(id), avg(ratio) FROM read_csv('{}')",
        csv_path(&f)
    );
    assert!(scalar(&db, &sql).starts_with("6")); // sum(id) = 6
    // 列名即表头（可投影/别名/谓词）
    let sql = format!(
        "SELECT name AS n FROM read_csv('{}') WHERE id >= 2 ORDER BY n",
        csv_path(&f)
    );
    assert_eq!(rows(&db, &sql), vec!["bob", "carol"]);

    std::fs::remove_dir_all(&root).ok();
}

/// ② 引号：内含逗号 / 换行 / `""` 转义；CRLF 与 BOM。
#[test]
fn quoted_fields_crlf_bom() {
    let root = tmp_root("quote");
    let f = root.join("t.csv");
    // BOM + CRLF + 引号内逗号/换行/双引号转义
    write(
        &f,
        "\u{feff}id,text\r\n1,\"a,b\"\r\n2,\"line1\nline2\"\r\n3,\"say \"\"hi\"\"\"\r\n",
    );
    let db = db_at(&root);
    let r = rows(
        &db,
        &format!(
            "SELECT id, text FROM read_csv('{}') ORDER BY id",
            csv_path(&f)
        ),
    );
    assert_eq!(r.len(), 3);
    assert_eq!(r[0], "1|a,b");
    assert_eq!(r[1], "2|line1\nline2");
    assert_eq!(r[2], "3|say \"hi\"");

    std::fs::remove_dir_all(&root).ok();
}

/// ③ 空字段 → NULL（含 `""`），以及空列（无样本）→ utf8。
#[test]
fn empty_fields_are_null() {
    let root = tmp_root("null");
    let f = root.join("t.csv");
    // 第 3 列全空（含显式 ""）→ 推断为 VARCHAR；第 2 列部分空 → BIGINT 保留
    write(&f, "a,b,c\n1,,x\n2,7,\"\"\n3,8,\n");
    let db = db_at(&root);
    let r = rows(
        &db,
        &format!(
            "SELECT a, b, c FROM read_csv('{}') ORDER BY a",
            csv_path(&f)
        ),
    );
    assert_eq!(r, vec!["1|NULL|x", "2|7|NULL", "3|8|NULL"]);
    // b 是数值列：NULL 不参与 sum（2+3 行的 7+8）
    let sql = format!("SELECT sum(b) FROM read_csv('{}')", csv_path(&f));
    assert_eq!(scalar(&db, &sql), "15");

    std::fs::remove_dir_all(&root).ok();
}

/// ④ 列裁剪与 WHERE 走外表扫描（计划级投影下推 + FilterExec 行过滤）。
#[test]
fn projection_and_where() {
    let root = tmp_root("prune");
    let f = root.join("t.csv");
    let mut s = String::from("id,v,w\n");
    for i in 0..50 {
        s.push_str(&format!("{i},{},{}\n", i * 2, i * 3));
    }
    write(&f, &s);
    let db = db_at(&root);

    // 投影 1 列 + 谓词（谓词列不在投影里：投影下推必须保留谓词列）
    let sql = format!(
        "SELECT v FROM read_csv('{}') WHERE w > 140 ORDER BY v",
        csv_path(&f)
    );
    assert_eq!(rows(&db, &sql), vec!["94", "96", "98"]);
    // JOIN 里的右表（非 Scan 根 → 不走列裁剪下推，走同一执行器）
    db.sql("CREATE TABLE dim (v BIGINT, label VARCHAR)")
        .unwrap();
    db.sql("INSERT INTO dim VALUES (0, 'zero'), (2, 'two')")
        .unwrap();
    let sql = format!(
        "SELECT d.label FROM read_csv('{}') c JOIN dim d ON c.v = d.v ORDER BY d.label",
        csv_path(&f)
    );
    assert_eq!(rows(&db, &sql), vec!["two", "zero"]);
    // 覆盖 SELECT *（全列）
    assert_eq!(
        scalar(
            &db,
            &format!("SELECT count(*) FROM read_csv('{}')", csv_path(&f))
        ),
        "50"
    );
    // LIMIT 0 schema 契约（与 read_parquet 同口径）
    match db
        .sql(&format!(
            "SELECT id, w FROM read_csv('{}') LIMIT 0",
            csv_path(&f)
        ))
        .unwrap()
    {
        qsdb::sql::SqlResult::Query { schema, batches } => {
            let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
            assert_eq!(names, vec!["id", "w"]);
            assert!(batches.iter().all(|b| b.num_rows() == 0));
        }
        _ => panic!("expect query result"),
    }

    std::fs::remove_dir_all(&root).ok();
}

/// glob 多文件：每个文件自己的表头各自跳过，批跨文件连续。
#[test]
fn glob_multiple_files() {
    let root = tmp_root("glob");
    let dir = root.join("parts");
    std::fs::create_dir_all(&dir).unwrap();
    write(&dir.join("p1.csv"), "k,v\na,1\nb,2\n");
    write(&dir.join("p2.csv"), "k,v\nc,3\n");
    let db = db_at(&root);
    let sql = format!(
        "SELECT k, v FROM read_csv('{}/*.csv') ORDER BY k",
        csv_path(&dir)
    );
    assert_eq!(rows(&db, &sql), vec!["a|1", "b|2", "c|3"]);

    std::fs::remove_dir_all(&root).ok();
}

/// ⑤ 错误路径：全部要明确报错，不含糊。
#[test]
fn error_paths() {
    let root = tmp_root("err");
    let db = db_at(&root);

    // 文件不存在
    let e = err(&db, "SELECT * FROM read_csv('Z:/no/such/file.csv')");
    assert!(e.contains("read_csv"), "错误要点明是哪个表函数: {e}");

    // 目录存在但无匹配（glob）
    let e = err(
        &db,
        &format!("SELECT * FROM read_csv('{}/*.csv')", csv_path(&root)),
    );
    assert!(e.contains("no file matches"), "glob 未命中的文案: {e}");

    // 空文件（无表头）
    let empty = root.join("empty.csv");
    write(&empty, "");
    assert!(
        db.sql(&format!("SELECT * FROM read_csv('{}')", csv_path(&empty)))
            .is_err()
    );

    // 空列名（尾随逗号）
    let bad_header = root.join("bad_header.csv");
    write(&bad_header, "a,b,\n1,2,3\n");
    let e = err(
        &db,
        &format!("SELECT * FROM read_csv('{}')", csv_path(&bad_header)),
    );
    assert!(e.contains("empty column name"), "空列名文案: {e}");

    // 行宽不符
    let ragged = root.join("ragged.csv");
    write(&ragged, "a,b\n1,2\n3\n");
    let e = err(
        &db,
        &format!("SELECT * FROM read_csv('{}')", csv_path(&ragged)),
    );
    assert!(e.contains("header has"), "行宽不符文案: {e}");

    // 推断为数值的列里出现非数字：类型是绑定期定死的（前 1000 行都是数字 →
    // BIGINT），第 1001 行的 "abc" 必须报错而不是变成 NULL。
    // 反过来说——"abc" 若落在推断窗口内，列会被推断成 utf8 而不是报错（见
    // `basic_types_and_header` 的类型规则），这是刻意的：类型对全批稳定。
    let dirty = root.join("dirty.csv");
    let mut s = String::from("n\n");
    for _ in 0..1000 {
        s.push_str("1\n");
    }
    s.push_str("abc\n");
    write(&dirty, &s);
    let e = err(
        &db,
        &format!("SELECT * FROM read_csv('{}')", csv_path(&dirty)),
    );
    assert!(e.contains("cannot parse"), "脏值文案: {e}");

    // 未知表函数
    assert!(db.sql("SELECT * FROM read_json('x')").is_err());
    // s3:// 不给 feature 也不支持（read_csv 顺序流不做远端）
    let e = err(&db, "SELECT * FROM read_csv('s3://b/k.csv')");
    assert!(e.contains("s3"), "s3 边界要明说: {e}");

    std::fs::remove_dir_all(&root).ok();
}

/// read_csv 灌进管理表（与 INSERT … SELECT 组合，导入链路的完整形态）。
#[test]
fn import_csv_into_table() {
    let root = tmp_root("import");
    let f = root.join("t.csv");
    write(&f, "id,name\n1,a\n2,b\n3,c\n");
    let db = db_at(&root);
    db.sql("CREATE TABLE t (id BIGINT, name VARCHAR) PARTITIONED BY (id)")
        .unwrap();
    db.sql(&format!(
        "INSERT INTO t SELECT id, name FROM read_csv('{}')",
        csv_path(&f)
    ))
    .unwrap();
    assert_eq!(scalar(&db, "SELECT count(*) FROM t"), "3");
    assert_eq!(db.partition_stats("t").unwrap().len(), 3);
    std::fs::remove_dir_all(&root).ok();
}
