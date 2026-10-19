//! POC 导入：parquet 文件流式读 → QSDB append（外部引擎基准数据入 QSDB 表格式，
//! 走完整写路径：统计收集/zstd/manifest/原子提交 — 与生产写入同链路）。
//!
//! 用法：poc_import --db <qsdb根> --table <表名> <a.parquet> [b.parquet ...]
//! 前置：表已 CREATE（本工具不做 DDL）。

use std::path::PathBuf;

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

fn main() {
    let mut args = std::env::args().skip(1);
    let mut db_path: Option<PathBuf> = None;
    let mut table = String::new();
    let mut files: Vec<PathBuf> = Vec::new();
    while let Some(a) = args.next() {
        match a.as_str() {
            "--db" => db_path = Some(PathBuf::from(args.next().expect("--db 需值"))),
            "--table" => table = args.next().expect("--table 需值"),
            _ => files.push(PathBuf::from(a)),
        }
    }
    let db_path = db_path.expect("需 --db <qsdb根>");
    assert!(!table.is_empty(), "需 --table");
    assert!(!files.is_empty(), "需至少一个 parquet 文件");

    let db = qsdb::Qsdb::open(&db_path, qsdb::QsdbOptions::default()).expect("Qsdb::open");
    let t0 = std::time::Instant::now();
    let mut total: usize = 0;
    for f in &files {
        let file = std::fs::File::open(f).unwrap_or_else(|e| panic!("打开 {:?}: {}", f, e));
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .expect("parquet footer")
            .with_batch_size(65_536)
            .build()
            .expect("reader");
        for b in reader {
            let b = b.expect("parquet 批解码");
            total += b.num_rows();
            db.append(&table, &[b]).expect("append");
        }
        eprintln!(
            "[poc_import] {} 累计 {} 行 {:?}",
            f.display(),
            total,
            t0.elapsed()
        );
    }
    println!(
        "[poc_import] 完成：{} ← {} 行（{:?}）",
        table,
        total,
        t0.elapsed()
    );
}
