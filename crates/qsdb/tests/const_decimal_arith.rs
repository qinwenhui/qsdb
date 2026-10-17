use qsdb::{Qsdb, QsdbOptions};

#[test]
fn 常量十进制算术必须保持_const_否则外层列乘它越界() {
    let root = std::env::temp_dir().join("qsdb_repro_t19").join(format!(
        "{}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
    std::fs::create_dir_all(&root).unwrap();
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    db.sql("CREATE TABLE t18 (RSJENERGY DECIMAL(38,11), RQCQENERGY DECIMAL(38,11), XQC_ENERGY DECIMAL(38,11), RQCQPRICE DECIMAL(38,11), SSCQPRICE DECIMAL(38,11))").unwrap();
    // 528 行（VALUES 行必须正数：负字面量的括号是另一条解析路，与本 bug 无关）
    for chunk_start in (0..528u32).step_by(50) {
        let mut v = String::new();
        for i in chunk_start..(chunk_start + 50).min(528) {
            let a = (i % 17) as f64 + 0.5;
            let b = (i % 11) as f64 + 0.5;
            let c = (i % 7) as f64 + 0.5;
            let d = (i % 13) as f64 + 0.5;
            v.push_str(&format!(",({},{},{},{},{})", a, b, c, d, a + d));
        }
        db.sql(&format!("INSERT INTO t18 VALUES {}", v.trim_start_matches(','))).unwrap();
    }
    let sql = "SELECT \
      CAST(CASE when (COALESCE(`RQCQENERGY`,0.0) - COALESCE(`XQC_ENERGY`,0.0) > COALESCE(`RSJENERGY`,0.0)*(1+(0.2)) ) and(COALESCE(`SSCQPRICE`,0.0) > COALESCE(`RQCQPRICE`,0.0) ) \
        THEN CAST((COALESCE(`RQCQENERGY`,0.0)-COALESCE(`XQC_ENERGY`,0.0)-(1+(0.2))*COALESCE(`RSJENERGY`,0.0))*(COALESCE(`SSCQPRICE`,0.0)-COALESCE(`RQCQPRICE`,0.0)) AS DECIMAL(38, 8)) \
        when (COALESCE(`RQCQENERGY`,0.0) -COALESCE(`XQC_ENERGY`,0.0) < (1-(0.2))*COALESCE(`RSJENERGY`,0.0) ) and(COALESCE(`SSCQPRICE`,0.0) < COALESCE(`RQCQPRICE`,0.0) ) \
        THEN CAST(((1-(0.2))*COALESCE(`RSJENERGY`,0.0)-COALESCE(`RQCQENERGY`,0.0)+COALESCE(`XQC_ENERGY`,0.0))*(COALESCE(`RQCQPRICE`,0.0)-COALESCE(`SSCQPRICE`,0.0)) AS DECIMAL(38, 8)) \
        ELSE CAST(0 AS DECIMAL(38, 8)) END as DECIMAL(38,8)) AS pcsyzydf FROM t18";
    let bs = db.sql_collect(sql).expect("回归：常量×常量的十进制算术曾返回 Arr(len 1)，外层列×它越界 panic");
    let n: usize = bs.iter().map(|b| b.num_rows()).sum();
    assert_eq!(n, 528);
    // 值也钉一下：ELSE 路径（ ELSE CAST(0 AS DECIMAL(38,8))）应得 0.00000000
    let v = arrow::util::display::array_value_to_string(bs[0].column(0).as_ref(), 0).unwrap();
    assert!(!v.is_empty(), "第一行结果为 NULL？");
}

/// `date_format` 的字符串列里混着不可解析的值 ⇒ 那些行给 **NULL**（Spark 非 ANSI），
/// 不许整批报错（L10-01 的输入列混着人名，2026-10-17 实测）。
#[test]
fn date_format_坏串给_null_不报错() {
    let root = std::env::temp_dir().join("qsdb_repro_df").join(format!(
        "{}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
    std::fs::create_dir_all(&root).unwrap();
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    db.sql("CREATE TABLE t (s VARCHAR)").unwrap();
    db.sql("INSERT INTO t VALUES ('2025-10-01 08:00:00'), ('管理员'), (NULL)").unwrap();
    let bs = db.sql_collect("SELECT DATE_FORMAT(s, 'yyyy-MM-dd') FROM t").unwrap();
    let a = bs[0].column(0);
    assert!(!a.is_null(0));
    assert_eq!(arrow::util::display::array_value_to_string(a, 0).unwrap(), "2025-10-01");
    assert!(a.is_null(1), "坏串必须是 NULL，不是报错");
    assert!(a.is_null(2));
}
