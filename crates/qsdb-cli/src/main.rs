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
mod error_code;
#[cfg(feature = "flight")]
mod flight;
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
#[allow(clippy::large_enum_variant)] // Server 参数最多、进程内单次构造
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
        /// 热数据缓存预算（MB；0 = 关闭，默认 256）。
        /// 保守默认守低占用定位；宽投影要快请显式调大（代价见
        /// `QsdbOptions::scan_cache_bytes` 文档的内存-性能权衡）。
        #[arg(long, default_value_t = 256)]
        cache_mb: usize,
        /// 并发查询上限（0 = 不限；默认 16）。**防 OOM 双闸之一**：
        /// 超限请求直接 503（不排队——闸门的职责是拒绝，不是缓冲）。
        /// **16 是过渡默认**：它 ≈ 机器内存 ÷ 单查询上限，是当前唯一的系统级
        /// 内存上界；但闸门数的是**条数**，分不清 µs 级轻查询与吃 4GB 的重查询，
        /// 故对轻查询并发有副作用。最终取定随「全局内存预算 + 溢出」准入设计
        /// 一并做（见 docs/configuration.md 内存模型一节）——在那之前，按机器
        /// 容量显式传参即可。
        #[arg(long, default_value_t = 16)]
        max_concurrent_queries: usize,
        /// 单查询执行内存上限（MB；**0 = 不限**；不给则默认 4096 = 4GB）。
        /// **防 OOM 双闸之二**，只约束显式物化算子的记账（join build /
        /// sort spill / window / udaf；流式扫描/聚合不驻留故不记账）。
        /// 显式给定时优先于 `--config` 里的同名项。
        #[arg(long)]
        query_memory_limit_mb: Option<u64>,
        /// 配置文件（JSON：query_timeout_ms / query_memory_limit_bytes）
        #[arg(long)]
        config: Option<PathBuf>,
        /// Arrow Flight 监听口（h2c；需 --features flight 编译）
        #[arg(long)]
        flight_bind: Option<String>,
        /// 远端外表（read_parquet s3://）段文件本地缓存（MB；0 = 关闭）
        #[arg(long, default_value_t = 0)]
        file_cache_mb: u64,
        /// 存储后端类型：local（默认）| s3（需 --features s3 编译）
        #[arg(long, default_value = "local")]
        storage_type: String,
        /// S3 兼容端点（AWS 官方留空；http:// 端点自动 allow_http）
        #[arg(long)]
        storage_endpoint: Option<String>,
        #[arg(long)]
        storage_region: Option<String>,
        /// S3 bucket（storage type = s3 时必填）
        #[arg(long)]
        storage_bucket: Option<String>,
        /// HDFS NameNode URL（storage type = hdfs 时必填，如 hdfs://host:9000）
        #[arg(long)]
        storage_hdfs_url: Option<String>,
        /// bucket 内 key 前缀（多租户/多库共存）
        #[arg(long, default_value = "")]
        storage_prefix: String,
        /// 远端写 staging 目录（M6-4：写盘即 ACK，后台上传；
        /// 需 --storage-type s3。建议放本地高速盘/与库根不同盘）
        #[arg(long)]
        staging_dir: Option<PathBuf>,
        /// staging 后台上传周期（秒；默认 5）
        #[arg(long, default_value_t = 5)]
        staging_upload_secs: u64,
        /// 数据耐久性：process-crash（默认，页缓存即持久）/ power-loss
        /// （segment/元数据 rename 前 fsync，掉电安全，写吞吐实测约 -25%）
        #[arg(long, default_value = "process-crash")]
        durability: String,
        /// 只读模式（standby/只读副本）：写/DDL 一律 503 `READ_ONLY`
        /// （读完全不受影响；客户端**永不重试**该码，换可写实例才对）
        #[arg(long, default_value_t = false)]
        read_only: bool,
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
    /// 添加用户（角色 admin|writer|reader；密码可经 --password 或交互输入）
    UserAdd {
        name: String,
        role: String,
        #[arg(long)]
        password: Option<String>,
    },
    /// 删除用户
    UserDrop { name: String },
    /// 改密码
    UserPasswd {
        name: String,
        #[arg(long)]
        password: Option<String>,
    },
    /// 用户清单
    UserList,
    /// 导出用户面（JSON 到 stdout；含角色+哈希，不含明文密码）
    UserExport,
    /// 导入用户面（JSON 从 stdin 或文件；--overwrite 整体替换 / 默认只补缺）
    UserImport {
        #[arg(long)]
        file: Option<PathBuf>,
        #[arg(long)]
        overwrite: bool,
    },
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
                AdminCmd::UserAdd {
                    name,
                    role,
                    password,
                } => {
                    let r = qsdb::users::Role::from_name(&role).unwrap_or_else(|e| {
                        eprintln!("error: {e}");
                        std::process::exit(1);
                    });
                    let pw = match password {
                        Some(p) => p,
                        None => {
                            eprint!("password for '{name}': ");
                            std::io::stdout().flush().unwrap();
                            let mut line = String::new();
                            std::io::stdin().read_line(&mut line).unwrap();
                            line.trim().to_string()
                        }
                    };
                    match qsdb::users::UserStore::add(db.store(), &name, &pw, r) {
                        Ok(()) => {
                            println!("user '{name}' added ({})", r.name());
                            return;
                        }
                        Err(e) => {
                            eprintln!("error: {e}");
                            std::process::exit(1);
                        }
                    }
                }
                AdminCmd::UserDrop { name } => {
                    match qsdb::users::UserStore::drop_user(db.store(), &name) {
                        Ok(()) => {
                            println!("user '{name}' dropped");
                            return;
                        }
                        Err(e) => {
                            eprintln!("error: {e}");
                            std::process::exit(1);
                        }
                    }
                }
                AdminCmd::UserPasswd { name, password } => {
                    let pw = match password {
                        Some(p) => p,
                        None => {
                            eprint!("new password for '{name}': ");
                            std::io::stdout().flush().unwrap();
                            let mut line = String::new();
                            std::io::stdin().read_line(&mut line).unwrap();
                            line.trim().to_string()
                        }
                    };
                    match qsdb::users::UserStore::set_password(db.store(), &name, &pw) {
                        Ok(()) => {
                            println!("password updated for '{name}'");
                            return;
                        }
                        Err(e) => {
                            eprintln!("error: {e}");
                            std::process::exit(1);
                        }
                    }
                }
                AdminCmd::UserExport => match qsdb::users::UserStore::export_json(db.store()) {
                    Ok(json) => {
                        println!("{json}");
                        return;
                    }
                    Err(e) => {
                        eprintln!("error: {e}");
                        std::process::exit(1);
                    }
                },
                AdminCmd::UserImport { file, overwrite } => {
                    let json = match file {
                        Some(p) => std::fs::read_to_string(&p).unwrap_or_else(|e| {
                            eprintln!("error: read {}: {e}", p.display());
                            std::process::exit(1);
                        }),
                        None => {
                            let mut buf = String::new();
                            use std::io::Read;
                            std::io::stdin().read_to_string(&mut buf).unwrap();
                            buf
                        }
                    };
                    match qsdb::users::UserStore::import_json(db.store(), &json, overwrite) {
                        Ok(n) => {
                            println!(
                                "imported {n} users ({})",
                                if overwrite { "overwrite" } else { "merge" }
                            );
                            return;
                        }
                        Err(e) => {
                            eprintln!("error: {e}");
                            std::process::exit(1);
                        }
                    }
                }
                AdminCmd::UserList => match qsdb::users::UserStore::list(db.store()) {
                    Ok(users) => {
                        if users.is_empty() {
                            println!("(no users)");
                        } else {
                            for (n, r) in users {
                                println!("{n:<20} {}", r.name());
                            }
                        }
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
            query_memory_limit_mb,
            flight_bind,
            file_cache_mb,
            storage_type,
            storage_endpoint,
            storage_region,
            storage_bucket,
            storage_hdfs_url,
            storage_prefix,
            staging_dir,
            staging_upload_secs,
            durability,
            read_only,
            config,
        } => {
            // 存储后端解析（cli 层校验，错误走统一 exit 风格）
            let storage = match storage_type.as_str() {
                "local" => qsdb::api::StorageConfig::Local,
                "hdfs" => {
                    #[cfg(feature = "hdfs")]
                    {
                        let Some(url) = storage_hdfs_url.clone() else {
                            eprintln!(
                                "error: --storage-hdfs-url is required for --storage-type hdfs"
                            );
                            std::process::exit(1);
                        };
                        qsdb::api::StorageConfig::Hdfs {
                            url,
                            prefix: storage_prefix,
                        }
                    }
                    #[cfg(not(feature = "hdfs"))]
                    {
                        let _ = (storage_hdfs_url, storage_prefix);
                        eprintln!(
                            "error: --storage-type hdfs requires building with --features hdfs"
                        );
                        std::process::exit(1);
                    }
                }
                "s3" => {
                    #[cfg(feature = "s3")]
                    {
                        let Some(bucket) = storage_bucket.clone() else {
                            eprintln!("error: --storage-bucket is required for --storage-type s3");
                            std::process::exit(1);
                        };
                        qsdb::api::StorageConfig::S3 {
                            endpoint: storage_endpoint,
                            region: storage_region,
                            bucket,
                            prefix: storage_prefix,
                        }
                    }
                    #[cfg(not(feature = "s3"))]
                    {
                        let _ = (
                            storage_endpoint,
                            storage_region,
                            storage_bucket,
                            storage_prefix,
                        );
                        eprintln!("error: --storage-type s3 requires building with --features s3");
                        std::process::exit(1);
                    }
                }
                other => {
                    eprintln!("error: unknown --storage-type '{other}' (local|s3|hdfs)");
                    std::process::exit(1);
                }
            };
            let dur = match durability.as_str() {
                "process-crash" => qsdb::api::Durability::ProcessCrash,
                "power-loss" => qsdb::api::Durability::PowerLoss,
                other => {
                    eprintln!("error: unknown --durability '{other}' (process-crash|power-loss)");
                    std::process::exit(1);
                }
            };
            let opts = QsdbOptions {
                storage,
                durability: dur,
                read_only,
                scan_cache_bytes: cache_mb * 1024 * 1024,
                file_cache_bytes: file_cache_mb * 1024 * 1024,
                staging_dir: staging_dir.clone(),
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
            let staging_secs = if staging_dir.is_some() {
                Some(staging_upload_secs)
            } else {
                None
            };
            if let Err(e) = rt.block_on(crate::server::serve(
                db,
                &addr,
                token,
                auto_compact_secs,
                Some(max_concurrent_queries),
                // MB → 字节；未显式给则 None（让配置文件优先，再由 serve 兜 4GB）
                query_memory_limit_mb.map(|mb| mb.saturating_mul(1024 * 1024)),
                config.as_deref(),
                flight_bind,
                staging_secs,
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
