//! `qsdb` 单机二进制：shell（REPL）/ query（单条）/ admin（运维）。
//!
//! M2 起支持 SQL-0/1；server 子命令 M3 开放。

use std::io::{IsTerminal, Write};
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::Array;
use arrow::record_batch::RecordBatch;
use clap::Parser;
use qsdb::sql::SqlResult;
mod commit_log;
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
        /// 写者身份代（F25；默认 0）。接管后要经 CLI 写入时必须显式抬高，
        /// 否则会被 `STALE_WRITER` 拦下（见 `docs/configuration.md`）。
        #[arg(long, default_value_t = 0)]
        writer_epoch: u64,
    },
    /// 执行单条 SQL（DDL/DML/SELECT）
    Query {
        #[arg(long)]
        path: PathBuf,
        /// 写者身份代（F25；默认 0）。接管后要经 CLI 写入时必须显式抬高。
        #[arg(long, default_value_t = 0)]
        writer_epoch: u64,
        sql: String,
    },
    /// 运维：表/分区/索引/垃圾清理
    Admin {
        #[arg(long)]
        path: PathBuf,
        /// **回收站静默期（秒；默认 600 = 10 分钟）**。
        ///
        /// 它就是"**删了还能捞回来**"的窗口长度：`admin gc` 只清**早于**它的条目。
        /// ⇒ 想让回收站当误删保险（比如"昨天手滑删的表今天还能救"），
        /// 这里给大值（如 `86400` = 一天），**并且别把 `gc` 跑得比它勤**。
        /// 代价是删掉的数据在盘上多留那么久。
        #[arg(long, default_value_t = 600)]
        trash_grace_secs: u64,
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
        /// **写者身份代**（qsdb-sync F25；默认 0 = 不启用）。
        ///
        /// 主备/failover 时由接管方**显式抬高**（比原主库大）。之后：
        /// ① 每次提交把它盖进 manifest，生态（qsdb-sync replica）据此用
        /// `(epoch, generation)` 复合序判增量——撞值不再可能；
        /// ② 配着小 epoch 的旧进程再连回来会被拒（`STALE_WRITER`）。
        #[arg(long, default_value_t = 0)]
        writer_epoch: u64,
        /// 热数据缓存预算（MB；0 = 关闭，默认 256）。
        /// 保守默认守低占用定位；宽投影要快请显式调大（代价见
        /// `QsdbOptions::scan_cache_bytes` 文档的内存-性能权衡）。
        #[arg(long, default_value_t = 256)]
        cache_mb: usize,
        /// 并发查询上限（0 = 不限；**默认 0**）。超限请求直接 503
        /// `TOO_MANY_QUERIES`（不排队——闸门的职责是拒绝，不是缓冲）。
        ///
        /// **它只管「条数」，不承担内存职责**：内存由 `--memory-budget-mb`
        /// 的全局预算管（超预算 → 503 `MEMORY_PRESSURE`，可退避重试）。
        /// 原先默认 16 是因为"它是唯一的系统级内存上界"，预算落地后这个理由
        /// 不成立了；且按条数限流会误伤 µs 级轻查询。真需要限流（过载保护/
        /// 公平性）时按机器容量显式给。
        #[arg(long, default_value_t = 0)]
        max_concurrent_queries: usize,
        /// **会话空闲回收**（分钟；0 = 永不回收，默认关）。超时会话按"踢人"
        /// 路径清理（取消在跑查询 + 吊销 token，之后 401 `SESSION_REVOKED`）。
        /// 默认关的理由：踢错活跃会话的代价大于泄漏代价（会话对象 ~百字节级，
        /// 泄漏是审计噪音不是内存问题）。
        #[arg(long, default_value_t = 0)]
        session_idle_timeout_mins: u64,
        /// **溢出目录**（排序/聚合/join 落盘用；默认 `<库根>/_spill`）。
        ///
        /// 溢出量可以很大（5M 行排序就是几十上百 MB）——**默认与数据同盘**，
        /// 避免写满系统盘（`/` 这种小分区在生产上踩过）。只读句柄不写库根，
        /// 回落系统临时目录。每次查询在其下开 `qsdb_spill_<uuid>/`，结束即删；
        /// 启动时清理崩溃残留（超过静默期未动过的子目录）。
        #[arg(long)]
        spill_dir: Option<PathBuf>,
        /// **全局内存预算**（MB；0 = 不限；默认 4096 = 4GB）。
        ///
        /// 本进程内**所有**查询共享的执行内存上界 + 解码缓存的总上界
        /// （压力下先挤兑缓存——纯缓存丢了大不了重算）。超预算的查询 →
        /// 503 `MEMORY_PRESSURE`（可退避重试），**不是 OOM**。
        /// 与 `--query-memory-limit-mb` 的分工：那个是「单个查询的配额」
        /// （超了是 400 `QUERY_MEMORY_LIMIT`，改查询才有用）。
        #[arg(long, default_value_t = 4096)]
        memory_budget_mb: u64,
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
    /// **导出库结构**（DDL；打印到 stdout）—— 备份/迁移/交付。
    ///
    /// 数据不在这一步：用 `--data <目录>` 连数据一起导，或自己走
    /// `COPY (SELECT …) TO '<目录>' (FORMAT PARQUET)`（A3 的导出原语）。
    Dump {
        #[arg(long)]
        path: PathBuf,
        /// 写者身份代（F25；默认 0）
        #[arg(long, default_value_t = 0)]
        writer_epoch: u64,
        /// 只导这一张表（默认全部表 + 视图）
        #[arg(long)]
        table: Option<String>,
        /// 连**数据**一起导到该目录（产出 `tables/` + `views/` + `data/`）
        #[arg(long)]
        data: Option<PathBuf>,
    },
    /// **物理快照**：把整个库**原样**拷到 `<目录>`（含 generation / epoch /
    /// 段边界 / 磁盘格式版本）—— 主备、时间点回滚、整机搬迁用它。
    ///
    /// ⚠️ 与 `dump` 的区别：`dump` 出的是**给别人读的数据**（Spark/pyarrow 能读，
    /// 但恢复出来是"另一份等价的库"）；`backup` 出的是**这个库本身**
    /// （只有 QSDB 能读）。
    ///
    /// ⚠️ 备份期间**别做 DDL**（`CREATE`/`DROP`/`ALTER`）—— 本条会持 catalog 锁
    /// 挡住它们。并发**写入**是安全的。
    Backup {
        #[arg(long)]
        path: PathBuf,
        /// 快照落到这个目录（必须不存在或为空）
        #[arg(long)]
        to: PathBuf,
    },
    /// 从**物理快照**恢复（目标库必须为空）。
    Recover {
        #[arg(long)]
        path: PathBuf,
        #[arg(long)]
        from: PathBuf,
    },
    /// 从 `dump --data` 产出的目录**恢复**（结构 + 数据）。
    ///
    /// ⚠️ **目标库必须是空的**（一张表都没有）—— 不做静默覆盖：往半个库里灌备份，
    /// 出来的东西既不是备份也不是原库，而没人看得出。
    Restore {
        #[arg(long)]
        path: PathBuf,
        #[arg(long, default_value_t = 0)]
        writer_epoch: u64,
        #[arg(long)]
        from: PathBuf,
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
    /// trash/孤儿清理（**注意：这会真的删掉回收站里的东西**）
    Gc,
    /// 列出回收站（误删的东西在这儿；最近的在前）
    TrashList,
    /// 从回收站捞回一条（`ts` 见 trash-list 的第一列）
    TrashRestore { ts: u64 },
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
        Cmd::Shell { path, writer_epoch } => shell(path, writer_epoch),
        Cmd::Query {
            path,
            writer_epoch,
            sql,
        } => {
            let db = open_with_epoch(&path, writer_epoch);
            let t = std::time::Instant::now();
            match db.sql(&sql) {
                Ok(r) => print_result(&r, t.elapsed()),
                Err(e) => {
                    eprintln!("error: {e}");
                    std::process::exit(1);
                }
            }
        }
        Cmd::Dump {
            path,
            writer_epoch,
            table,
            data,
        } => dump(path, writer_epoch, table, data),
        Cmd::Restore {
            path,
            writer_epoch,
            from,
        } => restore(path, writer_epoch, &from),
        Cmd::Backup { path, to } => backup(&path, &to),
        Cmd::Recover { path, from } => recover(&path, &from),
        Cmd::Admin {
            path,
            trash_grace_secs,
            cmd,
        } => {
            let db = open_with_grace(&path, 0, trash_grace_secs);
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
                AdminCmd::TrashList => {
                    let list = match qsdb::fsutil::list_trash(db.root()) {
                        Ok(v) => v,
                        Err(e) => {
                            eprintln!("error: {e}");
                            std::process::exit(1);
                        }
                    };
                    if list.is_empty() {
                        println!("（回收站是空的）");
                    } else {
                        println!("{:<16} {:>10}  原来在哪", "ts（恢复用）", "大小");
                        for e in &list {
                            println!(
                                "{:<16} {:>9.1}MB  {}",
                                e.ts,
                                e.bytes as f64 / 1048576.0,
                                e.src.as_deref().unwrap_or("(旧遗留，认不出来)")
                            );
                        }
                    }
                    println!(
                        "\n捞回来：qsdb admin --path <库> trash-restore <ts>\n\
                         ⚠️ **真删是 `admin gc`** —— 它会把回收站清空。要当误删保险，\
                         别让它跑太勤，或用 `admin --trash-grace-secs <秒>` 把窗口调大。"
                    );
                    return;
                }
                AdminCmd::TrashRestore { ts } => match qsdb::fsutil::restore_trash(db.root(), ts) {
                    Ok(p) => {
                        // ★ **光把目录挪回来不够**：`drop_table` 是两件事 ——
                        //   目录进 trash **+** 从清单去掉名。少了后半件，库仍然
                        //   不认识它（`not found: table default.t`）——
                        //   这条是写这个功能时实测撞出来的。
                        if let Some((d, t)) = table_rel(db.root(), &p) {
                            if let Err(e) =
                                qsdb::catalog::register_existing_table(db.store(), &d, &t)
                            {
                                eprintln!(
                                    "error: 目录已挪回，但**没能登记进清单**（表仍不可见）：{e}"
                                );
                                std::process::exit(1);
                            }
                            println!("已恢复 → {}（表 {d}.{t} 已重新登记）", p.display());
                        } else {
                            println!("已恢复 → {}", p.display());
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
            writer_epoch,
            cache_mb,
            max_concurrent_queries,
            session_idle_timeout_mins,
            spill_dir,
            memory_budget_mb,
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
            // **提交事件缓冲 + 钩子（T2）**：缓冲要在开库**之前**建好，
            // 因为钩子得在 `Qsdb::open` 时一起装上去。
            let commits = crate::commit_log::CommitLog::new();
            let opts = QsdbOptions {
                storage,
                durability: dur,
                read_only,
                on_commit: read_only
                    .then_some(None)
                    .flatten()
                    .or_else(|| Some(commits.hook())),
                writer_epoch,
                // MB → 字节；0 = 不限（嵌入方/部署方自控）
                memory_budget_bytes: (memory_budget_mb > 0)
                    .then(|| memory_budget_mb.saturating_mul(1024 * 1024)),
                // 溢出目录：默认与数据同盘；只读句柄回落系统 temp
                spill_dir: if read_only {
                    None
                } else {
                    Some(spill_dir.clone().unwrap_or_else(|| path.join("_spill")))
                },
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
                commits,
                auto_compact_secs,
                Some(max_concurrent_queries),
                Some(session_idle_timeout_mins),
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

/// 带**回收站静默期**打开（`admin gc` 只清早于它的条目）。
fn open_with_grace(path: &std::path::Path, writer_epoch: u64, trash_grace_secs: u64) -> Qsdb {
    let opts = QsdbOptions {
        writer_epoch,
        trash_grace_ms: trash_grace_secs.saturating_mul(1000),
        ..Default::default()
    };
    Qsdb::open(path, opts).unwrap_or_else(|e| {
        eprintln!("error: open {} failed: {e}", path.display());
        std::process::exit(1);
    })
}

/// 带写者身份代打开（F25）。
fn open_with_epoch(path: &std::path::Path, writer_epoch: u64) -> Qsdb {
    let opts = QsdbOptions {
        writer_epoch,
        ..Default::default()
    };
    match Qsdb::open(path, opts) {
        Ok(db) => db,
        Err(e) => {
            eprintln!("open {} failed: {e}", path.display());
            std::process::exit(1);
        }
    }
}

/// `qsdb backup`：打一份物理快照。
fn backup(path: &std::path::Path, to: &std::path::Path) {
    let db = open_with_epoch(path, 0);
    match qsdb::backup::snapshot(db.store(), to) {
        Ok(info) => {
            eprintln!(
                "已备份 {} 张表 / {} 个文件 / {:.1}MB → {}
（磁盘格式 v{}，内核 {}）",
                info.tables.len(),
                info.files,
                info.bytes as f64 / 1048576.0,
                to.display(),
                info.manifest_format,
                info.kernel_version
            );
        }
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(1);
        }
    }
}

/// `qsdb recover`：从物理快照恢复。
fn recover(path: &std::path::Path, from: &std::path::Path) {
    let db = open_with_epoch(path, 0);
    match qsdb::backup::recover(db.store(), from) {
        Ok(info) => {
            eprintln!(
                "已恢复 {} 张表 / {} 个文件 / {:.1}MB → {}",
                info.tables.len(),
                info.files,
                info.bytes as f64 / 1048576.0,
                path.display()
            );
        }
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(1);
        }
    }
}

/// `qsdb dump`：导出库结构（可选连数据）。
///
/// **为什么数据不在这一步**：导出数据的原语是 SQL 的 `COPY (SELECT …) TO …`
/// （A3），有 HTTP / 驱动 / 嵌入方四条路在用；再在 CLI 里做一套是重复。
/// 这里 `--data` 只是**薄壳**：逐表调那条原语。
fn dump(path: PathBuf, writer_epoch: u64, only: Option<String>, data: Option<PathBuf>) {
    let db = open_with_epoch(&path, writer_epoch);
    let tables: Vec<String> = match &only {
        Some(t) => vec![t.to_ascii_lowercase()],
        None => match db.tables() {
            Ok(v) => v,
            Err(e) => {
                eprintln!("error: {e}");
                std::process::exit(1);
            }
        },
    };
    // 视图：`SqlResult` 直接给 DDL 文本？不 —— 视图存的是 SELECT 原文，这里拼。
    let views: Vec<(String, String)> = if only.is_none() {
        match qsdb::views::ViewStore::read_all(db.store()) {
            Ok(m) => {
                let mut v: Vec<(String, String)> = m.into_iter().collect();
                v.sort(); // 确定序（HashMap 迭代序每次进程都不同）
                v
            }
            Err(e) => {
                eprintln!("error: {e}");
                std::process::exit(1);
            }
        }
    } else {
        Vec::new()
    };

    let ddl_of = |t: &str| -> String {
        match db.sql(&format!("SHOW CREATE TABLE {t}")) {
            Ok(r) => {
                if let qsdb::sql::SqlResult::Query { batches, .. } = r {
                    batches
                        .first()
                        .map(|b| {
                            arrow::util::display::array_value_to_string(b.column(1).as_ref(), 0)
                                .unwrap_or_default()
                        })
                        .unwrap_or_default()
                } else {
                    String::new()
                }
            }
            Err(e) => {
                eprintln!("error: SHOW CREATE TABLE {t}: {e}");
                std::process::exit(1);
            }
        }
    };

    match data {
        // 只导结构 → 打到 stdout（人读 / 重定向进 .sql）
        None => {
            for t in &tables {
                println!("{}", ddl_of(t));
            }
            for (n, sql) in &views {
                println!("CREATE VIEW {n} AS {sql};");
            }
        }
        // 连数据 → 落目录。**一表一个 .sql 文件**：正好一条语句，
        // 恢复时不需要"切分 SQL 文本"（切分要处理字符串里的分号，是个静默出错面）
        Some(dir) => {
            let sub = |d: &str| -> PathBuf {
                let p = dir.join(d);
                if let Err(e) = std::fs::create_dir_all(&p) {
                    eprintln!("error: mkdir {}: {e}", p.display());
                    std::process::exit(1);
                }
                p
            };
            let tdir = sub("tables");
            let vdir = sub("views");
            let ddir = sub("data");
            for t in &tables {
                if let Err(e) = std::fs::write(tdir.join(format!("{t}.sql")), ddl_of(t)) {
                    eprintln!("error: write {t}.sql: {e}");
                    std::process::exit(1);
                }
                // 数据：走 A3 的原语，但**刻意不传 `PARTITION_BY`** ——
                // 「备份」与「湖导出」要的不是一回事：
                //   · 湖导出要**互操作**（Spark/pyarrow 读）⇒ 分区列进目录名；
                //   · 备份要**保真** ⇒ 值就留在数据文件里、类型原样，不经过
                //     "目录名文本 → 类型推断"那一跳（那跳还带类型白名单限制）。
                // 恢复侧按列名映射，两种布局都成立；平铺更稳。
                let target = ddir.join(t).to_string_lossy().replace('\\', "/");
                if let Err(e) = db.sql(&format!(
                    "COPY (SELECT * FROM {t}) TO '{target}' (FORMAT PARQUET)"
                )) {
                    eprintln!("error: 导出表 {t} 的数据：{e}");
                    std::process::exit(1);
                }
            }
            for (n, sql) in &views {
                let _ = std::fs::write(
                    vdir.join(format!("{n}.sql")),
                    format!("CREATE VIEW {n} AS {sql};"),
                );
            }
            eprintln!(
                "已导出 {} 张表 + {} 个视图 → {}",
                tables.len(),
                views.len(),
                dir.display()
            );
        }
    }
}

/// 恢复出来的路径若是 `<库>/<表>` 形态，返回 `(库, 表)`。
///
/// 判据收紧成"**正好两层、且都不以 `_` 开头**"——`_trash`/`_spill`/`_catalog`
/// 这类内部目录不在此列（它们不是表，别去登记）。是否**真是**表由
/// `catalog::register_existing_table` 用 `table.json` 的存在性最终判定。
fn table_rel(root: &std::path::Path, restored: &std::path::Path) -> Option<(String, String)> {
    let rel = restored.strip_prefix(root).ok()?;
    let parts: Vec<String> = rel
        .components()
        .map(|c| c.as_os_str().to_string_lossy().into_owned())
        .collect();
    if parts.len() != 2 {
        return None;
    }
    if parts.iter().any(|s| s.starts_with('_')) {
        return None;
    }
    Some((parts[0].clone(), parts[1].clone()))
}

/// 目录下（含子目录）有没有 parquet 文件 —— 用来区分"空表"与"导出了但读不到"。
fn has_parquet(dir: &std::path::Path) -> bool {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return false;
    };
    for e in rd.flatten() {
        let p = e.path();
        if p.is_dir() {
            if has_parquet(&p) {
                return true;
            }
        } else if p.extension().and_then(|s| s.to_str()) == Some("parquet") {
            return true;
        }
    }
    false
}

/// `qsdb restore`：从 `dump --data` 的目录恢复（结构 + 数据）。
fn restore(path: PathBuf, writer_epoch: u64, from: &std::path::Path) {
    let db = open_with_epoch(&path, writer_epoch);
    // **目标库必须空**（不做静默覆盖 —— 见子命令的文档）
    let existing = match db.tables() {
        Ok(v) => v,
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(1);
        }
    };
    if !existing.is_empty() {
        eprintln!(
            "error: 目标库不是空的（已有 {} 张表：{}）—— 本命令**不做静默覆盖**。\
             请换一个空目录，或先自己清掉",
            existing.len(),
            existing.join(", ")
        );
        std::process::exit(1);
    }

    let run = |sql: &str, what: &str| {
        if let Err(e) = db.sql(sql) {
            eprintln!("error: {what}: {e}\n--- SQL ---\n{sql}");
            std::process::exit(1);
        }
    };

    // ① 结构：一表一个文件 = 一条语句（不需要切分文本）
    let mut n_tab = 0usize;
    for (dir, sub) in [(from.join("tables"), "表"), (from.join("views"), "视图")] {
        let Ok(rd) = std::fs::read_dir(&dir) else {
            continue; // 没有视图目录很正常
        };
        let mut files: Vec<PathBuf> = rd.flatten().map(|e| e.path()).collect();
        files.sort(); // 确定序
        for f in files {
            if f.extension().and_then(|s| s.to_str()) != Some("sql") {
                continue;
            }
            let Ok(sql) = std::fs::read_to_string(&f) else {
                eprintln!("error: 读不了 {}", f.display());
                std::process::exit(1);
            };
            run(sql.trim(), &format!("建{sub} {}", f.display()));
            if sub == "表" {
                n_tab += 1;
            }
        }
    }

    // ② 数据：从 hive 目录读回来（A4 的 hive 语义正好接上）
    //
    // ⚠️ **必须显式列名，不能用 `SELECT *`**：`read_parquet` 读 hive 目录时给出的
    // 列序是「**数据文件列 ++ 分区列**」，而表的列序是**建表时写的那个序**
    // （分区列可能夹在中间）。按位置灌 = **静默错值的经典形状**（值全在、全串位）。
    // 按名则与顺序无关，列对不上时会**报错**而不是错位。
    let mut n_data = 0usize;
    for t in db.tables().unwrap_or_default() {
        let d = from.join("data").join(&t);
        if !d.is_dir() {
            continue;
        }
        let def = match db.table_def(&t) {
            Ok(d) => d,
            Err(e) => {
                eprintln!("error: 读表定义 {t}: {e}");
                std::process::exit(1);
            }
        };
        let list = def
            .columns
            .iter()
            .map(|c| c.name.clone())
            .collect::<Vec<_>>()
            .join(", ");
        // **空表没有 parquet 文件**（导出时不写零行段）⇒ 没有文件就跳过。
        // 不跳的话 `read_parquet` 会报 "no file matches" —— 而"空表恢复成空表"
        // 本来就是对的，那不是错误。
        if !has_parquet(&d) {
            continue;
        }
        let glob = format!("{}/**/*.parquet", d.to_string_lossy().replace('\\', "/"));
        run(
            &format!("INSERT INTO {t} ({list}) SELECT {list} FROM read_parquet('{glob}')"),
            &format!("灌表 {t} 的数据"),
        );
        n_data += 1;
    }
    eprintln!(
        "已恢复 {n_tab} 张表（{n_data} 张带数据）→ {}",
        path.display()
    );
}

fn shell(path: PathBuf, writer_epoch: u64) {
    let db = open_with_epoch(&path, writer_epoch);
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
        SqlResult::Ok { message, .. } => println!("{message} ({elapsed:?})"),
        SqlResult::Query {
            schema, batches, ..
        } => {
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
            // ------------------------------------------------------------------
            // 单元格截断 —— **只在往终端上打的时候做**（2026-10-17 修）
            //
            // ⚠️ 这段此前有三个问题，且**已经连坑三次**：
            //   ① 无条件截断 ⇒ 输出被程序消费时**静默丢数据**（`SHOW TABLES` 的长表名
            //      被拿去做 `DESCRIBE`，39 张表被静默跳过）；
            //   ② 长列名被截进 DDL，拼出语法错；
            //   ③ `&v[..21]` 是**按字节**切片 ⇒ 撞上多字节字符（中文列名）直接
            //      **panic**（`end byte index 21 is not a char boundary`），
            //      而前半屏已经打出去了，看上去像"查询失败"。
            //
            // 现在：**重定向/管道 ⇒ 一个字都不截**（程序拿到的是完整值）；
            // 终端 ⇒ 仍截断（那是给眼睛看的，`…` 是可见的，不是静默）。
            let tty = std::io::stdout().is_terminal();
            let head = |s: &str| -> String {
                if tty && s.chars().count() > 24 {
                    // 按**字符**取，不是按字节 —— 边界必须落在字符上
                    format!("{}…", s.chars().take(21).collect::<String>())
                } else {
                    s.to_string()
                }
            };
            // 列宽按**字符数**算（`{:<width$}` 的填充也是按字符），否则中文列错位
            let width: Vec<usize> = (0..schema.fields().len())
                .map(|c| {
                    table
                        .iter()
                        .map(|r| r.get(c).map(|s| head(s).chars().count()).unwrap_or(0))
                        .max()
                        .unwrap_or(0)
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
                        let v = head(v);
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
        // `SqlResult` 是 `#[non_exhaustive]`（F27）：内核加变体不该打断嵌入方
        // 编译，故外部匹配留通配臂（**放最后**，放前面会让上面的臂不可达）。
        _ => println!("(unhandled result variant, {elapsed:?})"),
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
