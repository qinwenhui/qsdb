use qsdb::{Qsdb, QsdbOptions};
fn main() {
    let root = std::env::temp_dir().join("qsdb_ins_probe3");
    let _ = std::fs::remove_dir_all(&root);
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    db.sql("CREATE TABLE s (a BIGINT, s STRING)").unwrap();
    db.sql("CREATE TABLE d (a BIGINT, s STRING)").unwrap();
    let mut sql = String::from("INSERT INTO s VALUES ");
    for i in 0..100_000 {
        if i > 0 {
            sql.push(',');
        }
        sql.push_str(&format!("({i},'v{i}')"));
    }
    db.sql(&sql).unwrap();
    let bs = db.sql_collect("SELECT a, s FROM s").unwrap();
    println!(
        "管理表扫描批数={} 列类型={:?}",
        bs.len(),
        bs[0]
            .schema()
            .fields()
            .iter()
            .map(|f| f.data_type().clone())
            .collect::<Vec<_>>()
    );
    // 走 ExternalScan（read_parquet 外表）
    let dir = root.to_string_lossy().replace('\\', "/");
    let bs = db
        .sql_collect(&format!(
            "SELECT a, s FROM read_parquet('{dir}/default/s/*.parquet')"
        ))
        .unwrap_or_default();
    if let Some(b) = bs.first() {
        println!(
            "外表 read_parquet 批数={} 列类型={:?}",
            bs.len(),
            b.schema()
                .fields()
                .iter()
                .map(|f| f.data_type().clone())
                .collect::<Vec<_>>()
        );
        println!("  首批 batch_bytes={}", qsdb::memory::batch_bytes(b));
        println!(
            "  切片 8192 行 batch_bytes={}",
            qsdb::memory::batch_bytes(&b.slice(0, 8192))
        );
    } else {
        println!("外表查询无结果");
    }
}
