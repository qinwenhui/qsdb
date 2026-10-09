//! `qsdb` 单机二进制入口。
//!
//! 子命令随里程碑开放：
//! - `server`（M3）：HTTP API 服务
//! - `shell`（M2）：本地 REPL
//! - `query`（M2）：一次性 SQL 执行
//! - `admin`（M1 起）：表/分区/治理运维

use clap::Parser;

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
    /// 启动 HTTP API 服务（M3 开放）
    Server {
        /// 数据库根目录
        #[arg(long)]
        path: std::path::PathBuf,
        /// 监听地址
        #[arg(long, default_value = "0.0.0.0:9430")]
        addr: String,
    },
    /// 交互式 shell（M2 开放）
    Shell {
        #[arg(long)]
        path: std::path::PathBuf,
    },
    /// 执行单条 SQL（M2 开放）
    Query {
        #[arg(long)]
        path: std::path::PathBuf,
        sql: String,
    },
    /// 运维：表/分区/垃圾清理检查（M1 起逐步开放）
    Admin {
        #[arg(long)]
        path: std::path::PathBuf,
        #[command(subcommand)]
        cmd: AdminCmd,
    },
}

#[derive(Debug, clap::Subcommand)]
enum AdminCmd {
    /// 列出所有表（M1）
    Tables,
    /// 列出表分区（M1）
    Partitions { table: String },
    /// 检查/清理 _trash 与孤儿文件（M1）
    GcCheck,
}

fn main() {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Server { path, addr } => {
            eprintln!(
                "qsdb server: planned for M3 (path={}, addr={addr}) — not yet implemented",
                path.display()
            );
        }
        Cmd::Shell { path } => {
            eprintln!(
                "qsdb shell: planned for M2 (path={}) — not yet implemented",
                path.display()
            );
        }
        Cmd::Query { path, sql } => {
            let _ = (path, sql);
            eprintln!("qsdb query: planned for M2 — not yet implemented");
        }
        Cmd::Admin { path, cmd } => {
            let _ = (path, cmd);
            eprintln!("qsdb admin: planned for M1 — not yet implemented");
        }
    }
}
