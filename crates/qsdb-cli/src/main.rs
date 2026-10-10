//! `qsdb` 单机二进制：shell（REPL）/ query（单条）/ admin（运维）。
//!
//! M2 起支持 SQL-0/1；server 子命令 M3 开放。

use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use clap::Parser;
use qsdb::sql::SqlResult;
mod server;

use qsdb::{Qsdb, QsdbOptions};

#[derive(Debug, Parser)]
#[command(
    name = "qsdb",
    version,
    about = "QSDB — embeddable columnar analytical store"
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Debug, clap::Subcommand)]
enum Cmd {
    /// 交互式 SQL shell
    Shell {
        /// 数据库根目录
        #[arg(long)]
        path: PathBuf,
    },
    /// 执行单条 SQL（DDL/DML/SELECT）
    Query {
        #[arg(long)]
        path: PathBuf,
        sql: String,
    },
    /// 运维：表/分区/索引/垃圾清理
    Admin {
        #[arg(long)]
        path: PathBuf,
        #[command(subcommand)]
        cmd: AdminCmd,
    },
    /// 启动 HTTP API 服务
    Server {
        #[arg(long)]
        path: PathBuf,
        #[arg(long, default_value = "0.0.0.0:9430")]
        addr: String,
        /// bearer token 鉴权（不设则开放）
        #[arg(long)]
        token: Option<String>,
        /// 自动 compaction 周期（秒）
        #[arg(long)]
        auto_compact_secs: Option<u64>,
        /// 热数据缓存预算（MB；0 = 关闭，默认 256）
        #[arg(long, default_value_t = 256)]
        cache_mb: usize,
        /// 并发查询上限（0 = 不限）
        #[arg(long, default_value_t = 0)]
        max_concurrent_queries: usize,
        /// 配置文件（JSON：query_timeout_ms / query_memory_limit_bytes）
        #[arg(long)]
        config: Option<PathBuf>,
    },
}

#[derive(Debug, clap::Subcommand)]
enum AdminCmd {
    /// 列出所有表
    Tables,
    /// 列出表分区
    Partitions { table: String },
    /// 重建表级分区索引
    RefreshIndex { table: String },
    /// trash/孤儿清理
    Gc,
    /// 小文件合并
    Compact { table: String },
}

fn main() {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Shell { path } => shell(path),
        Cmd::Query { path, sql } => {
            let db = open(&path);
            let t = std::time::Instant::now();
            match db.sql(&sql) {
                Ok(r) => print_result(&r, t.elapsed()),
                Err(e) => {
                    eprintln!("error: {e}");
                    std::process::exit(1);
                }
            }
        }
        Cmd::Admin { path, cmd } => {
            let db = open(&path);
            let r = match cmd {
                AdminCmd::Tables => db.sql("SHOW TABLES"),
                AdminCmd::Partitions { table } => db.sql(&format!("SHOW PARTITIONS {table}")),
                AdminCmd::RefreshIndex { table } => match db.refresh_index(&table) {
                    Ok(()) => {
                        println!("index refreshed: {table}");
                        return;
                    }
                    Err(e) => {
                        eprintln!("error: {e}");
                        std::process::exit(1);
                    }
                },
                AdminCmd::Compact { table } => match db.compact_table(&table) {
                    Ok(r) => {
                        println!(
                            "compacted: {} partitions, {} segments, {} rows, {} bytes",
                            r.partitions_compacted, r.segments_merged, r.rows, r.bytes_written
                        );
                        return;
                    }
                    Err(e) => {
                        eprintln!("error: {e}");
                        std::process::exit(1);
                    }
                },
                AdminCmd::Gc => match db.gc() {
                    Ok((purged, freed)) => {
                        println!("gc: {purged} entries, {} bytes freed", freed);
                        return;
                    }
                    Err(e) => {
                        eprintln!("error: {e}");
                        std::process::exit(1);
                    }
                },
            };
            match r {
                Ok(res) => print_result(&res, std::time::Duration::ZERO),
                Err(e) => {
                    eprintln!("error: {e}");
                    std::process::exit(1);
                }
            }
        }
        Cmd::Server {
            path,
            addr,
            token,
            auto_compact_secs,
            cache_mb,
            max_concurrent_queries,
            config,
        } => {
            let opts = QsdbOptions {
                scan_cache_bytes: cache_mb * 1024 * 1024,
                ..Default::default()
            };
            let db = match Qsdb::open(&path, opts) {
                Ok(db) => Arc::new(db),
                Err(e) => {
                    eprintln!("open {} failed: {e}", path.display());
                    std::process::exit(1);
                }
            };
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .expect("init tokio runtime");
            if let Err(e) = rt.block_on(crate::server::serve(
                db,
                &addr,
                token,
                auto_compact_secs,
                Some(max_concurrent_queries),
                config.as_deref(),
            )) {
                eprintln!("server error: {e}");
                std::process::exit(1);
            }
        }
    }
}

fn open(path: &std::path::Path) -> Qsdb {
    match Qsdb::open(path, QsdbOptions::default()) {
        Ok(db) => db,
        Err(e) => {
            eprintln!("open {} failed: {e}", path.display());
            std::process::exit(1);
        }
    }
}

fn shell(path: PathBuf) {
    let db = open(&path);
    println!("QSDB shell — path: {}", path.display());
    println!("commands end with ';'. dot-commands: .tables  .schema <t>  .exit");
    let mut rl = rustyline::DefaultEditor::new().expect("init readline");
    let mut buffer = String::new();
    loop {
        let prompt = if buffer.is_empty() {
            "qsdb> "
        } else {
            "  ... "
        };
        let line = match rl.readline(prompt) {
            Ok(l) => l,
            Err(_) => break, // EOF/Ctrl-D
        };
        rl.add_history_entry(&line).ok();
        let trimmed = line.trim();
        if buffer.is_empty() && trimmed.starts_with('.') {
            let dot = trimmed.to_ascii_lowercase();
            match dot.as_str() {
                ".exit" | ".quit" => break,
                ".tables" => run(&db, "SHOW TABLES"),
                _ => {
                    if let Some(t) = dot.strip_prefix(".schema ") {
                        run(&db, &format!("DESCRIBE {t}"));
                    } else {
                        eprintln!("unknown command {trimmed}");
                    }
                }
            }
            continue;
        }
        if trimmed.is_empty() {
            continue;
        }
        buffer.push_str(trimmed);
        buffer.push(' ');
        if trimmed.ends_with(';') {
            let sql = buffer.trim().trim_end_matches(';').to_string();
            buffer.clear();
            run(&db, &sql);
        }
    }
    println!("bye");
}

fn run(db: &Qsdb, sql: &str) {
    let t = std::time::Instant::now();
    match db.sql(sql) {
        Ok(r) => print_result(&r, t.elapsed()),
        Err(e) => eprintln!("error: {e}"),
    }
}

/// 结果打印：表格对齐（宽度上限截断）。
fn print_result(r: &SqlResult, elapsed: std::time::Duration) {
    match r {
        SqlResult::Ok { message } => println!("{message} ({elapsed:?})"),
        SqlResult::Query { schema, batches } => {
            let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            if rows == 0 {
                println!("(0 rows, {elapsed:?})");
                return;
            }
            let mut table: Vec<Vec<String>> = Vec::with_capacity(rows + 1);
            table.push(schema.fields().iter().map(|f| f.name().clone()).collect());
            for b in batches {
                push_rows(&mut table, b);
            }
            // 列宽（上限 24）
            let width: Vec<usize> = (0..schema.fields().len())
                .map(|c| {
                    table
                        .iter()
                        .map(|r| r.get(c).map(|s| s.len()).unwrap_or(0))
                        .max()
                        .unwrap_or(0)
                        .min(24)
                })
                .collect();
            let sep: String = width
                .iter()
                .map(|w| "-".repeat(w + 2))
                .collect::<Vec<_>>()
                .join("+");
            for (i, row) in table.iter().enumerate() {
                let cells: Vec<String> = row
                    .iter()
                    .enumerate()
                    .map(|(c, v)| {
                        let v = if v.len() > 24 {
                            format!("{}…", &v[..21])
                        } else {
                            v.clone()
                        };
                        format!(" {:<width$} ", v, width = width[c])
                    })
                    .collect();
                println!("{}", cells.join("|"));
                if i == 0 {
                    println!("{sep}");
                }
            }
            println!("({rows} rows, {elapsed:?})");
            let _ = std::io::stdout().flush();
        }
    }
}

fn push_rows(table: &mut Vec<Vec<String>>, b: &RecordBatch) {
    for i in 0..b.num_rows() {
        let mut cells = Vec::with_capacity(b.num_columns());
        for c in 0..b.num_columns() {
            let arr: &dyn Array = b.column(c).as_ref();
            if arr.is_null(i) {
                cells.push("NULL".into());
            } else {
                cells.push(
                    arrow::util::display::array_value_to_string(arr, i)
                        .unwrap_or_else(|_| "<err>".into()),
                );
            }
        }
        table.push(cells);
    }
}

// Arc 供未来 server 共享句柄
#[allow(dead_code)]
fn _arc_hint(_: Arc<Qsdb>) {}
