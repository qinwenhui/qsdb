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
        path: Option<PathBuf>,
        #[arg(long)]
        addr: Option<String>,
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
        #[arg(long)]
        writer_epoch: Option<u64>,
        /// 热数据缓存预算（MB；0 = 关闭，默认 256）。
        /// 保守默认守低占用定位；宽投影要快请显式调大（代价见
        /// `QsdbOptions::scan_cache_bytes` 文档的内存-性能权衡）。
        #[arg(long)]
        cache_mb: Option<usize>,
        /// 并发查询上限（0 = 不限；**默认 0**）。超限请求直接 503
        /// `TOO_MANY_QUERIES`（不排队——闸门的职责是拒绝，不是缓冲）。
        ///
        /// **它只管「条数」，不承担内存职责**：内存由 `--memory-budget-mb`
        /// 的全局预算管（超预算 → 503 `MEMORY_PRESSURE`，可退避重试）。
        /// 原先默认 16 是因为"它是唯一的系统级内存上界"，预算落地后这个理由
        /// 不成立了；且按条数限流会误伤 µs 级轻查询。真需要限流（过载保护/
        /// 公平性）时按机器容量显式给。
        #[arg(long)]
        max_concurrent_queries: Option<usize>,
        /// **会话空闲回收**（分钟；0 = 永不回收，默认关）。超时会话按"踢人"
        /// 路径清理（取消在跑查询 + 吊销 token，之后 401 `SESSION_REVOKED`）。
        /// 默认关的理由：踢错活跃会话的代价大于泄漏代价（会话对象 ~百字节级，
        /// 泄漏是审计噪音不是内存问题）。
        #[arg(long)]
        session_idle_timeout_mins: Option<u64>,
        /// **会话数上限**（0 = 不限，默认 0）。登录时超限 → 503
        /// `TOO_MANY_SESSIONS`（等会话释放/被踢）；已有会话不受影响。
        /// 会话=登录签发的进程态凭证（Basic/开放模式不建会话，不受此限）。
        #[arg(long)]
        max_sessions: Option<u64>,
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
        #[arg(long)]
        memory_budget_mb: Option<u64>,
        /// 单查询执行内存上限（MB；**0 = 不限**；不给则默认 4096 = 4GB）。
        /// **防 OOM 双闸之二**，只约束显式物化算子的记账（join build /
        /// sort spill / window / udaf；流式扫描/聚合不驻留故不记账）。
        /// 显式给定时优先于 `--config` 里的同名项。
        #[arg(long)]
        query_memory_limit_mb: Option<u64>,
        /// **结果护栏·行数**（十七批 R1；默认 0 = 关，升级零行为变更）。
        /// 单次查询结果行数超过它 → 400 `RESULT_TOO_LARGE`，**绝不返回部分
        /// 数据**。检查点在执行后、序列化前（不省计算，纯传输护栏，全格式
        /// 一致）；可被每请求 `maxRows`/`X-QSDB-Max-Rows` 覆盖，
        /// 可经 `PUT /api/v1/config` 热更。显式给定时优先于 `--config`。
        #[arg(long)]
        max_result_rows: Option<u64>,
        /// **结果护栏·体量**（MB，按 Arrow 内存估算；默认 0 = 关）。
        /// JSON 序列化后实际体积可达估算的 1.5~3×（转义/数字展开）——
        /// 要精确护栏用 `--max-result-rows`。热更键 `maxResultBytes`。
        #[arg(long)]
        max_result_mb: Option<u64>,
        /// 配置文件（JSON：query_timeout_ms / query_memory_limit_bytes）
        #[arg(long)]
        config: Option<PathBuf>,
        /// Arrow Flight 监听口（h2c；需 --features flight 编译）
        #[arg(long)]
        flight_bind: Option<String>,
        /// 远端外表（read_parquet s3://）段文件本地缓存（MB；0 = 关闭）
        #[arg(long)]
        file_cache_mb: Option<u64>,
        /// 存储后端类型：local（默认）| s3（需 --features s3 编译）
        #[arg(long)]
        storage_type: Option<String>,
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
        #[arg(long)]
        storage_prefix: Option<String>,
        /// 远端写 staging 目录（M6-4：写盘即 ACK，后台上传；
        /// 需 --storage-type s3。建议放本地高速盘/与库根不同盘）
        #[arg(long)]
        staging_dir: Option<PathBuf>,
        /// staging 后台上传周期（秒；默认 5）
        #[arg(long)]
        staging_upload_secs: Option<u64>,
        /// 数据耐久性：process-crash（默认，页缓存即持久）/ power-loss
        /// （segment/元数据 rename 前 fsync，掉电安全，写吞吐实测约 -25%）
        #[arg(long)]
        durability: Option<String>,
        /// 只读模式（standby/只读副本）：写/DDL 一律 503 `READ_ONLY`
        /// （读完全不受影响；客户端**永不重试**该码，换可写实例才对）
        #[arg(long, default_value_t = false)]
        read_only: bool,
        /// **多库挂载**（M-B，可重复）：`--mount 名=路径` 或带选项
        /// `--mount 名=路径,budget-mb=2048,read-only`。
        ///
        /// `default` 库固定来自 `--path`（`--mount default=…` 显式拒绝）。
        /// 名字进 SQL 限定符（`settle.t`）、`USE`、`SHOW DATABASES` 与
        /// grants 键——必须是小写 `[a-z_][a-z0-9_]*`。各挂载独立预算/缓存/
        /// 写缓冲（并发闸是实例级的——过载保护管整个进程）。
        #[arg(long = "mount")]
        mounts: Vec<String>,
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
        /// 全局角色（= `"*"` 槽位）。给了 `--grant` 后可省略（省略 = 白名单制，
        /// 未列出的库一律无权）。
        role: Option<String>,
        /// 库级授权（M-C）：`--grant 库名=角色`，可重复；角色省写（`db=`）=
        /// **显式无权**；库名可为 `*`（通配）。判定每请求现查，立即生效。
        #[arg(long = "grant")]
        grants: Vec<String>,
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
                    grants,
                    password,
                } => {
                    // --grant 解析（M-C）：`db=role`；`db=` = 显式无权；`*=role` = 通配
                    let mut map = std::collections::BTreeMap::new();
                    for g in &grants {
                        let Some((db, r)) = g.split_once('=') else {
                            eprintln!("error: --grant 需要 库名=角色 形态（got {g:?}）");
                            std::process::exit(1);
                        };
                        let db = db.trim();
                        if db.is_empty() {
                            eprintln!("error: --grant 的库名不能为空（got {g:?}）");
                            std::process::exit(1);
                        }
                        let rr = if r.trim().is_empty() {
                            None // `db=` = 显式无权
                        } else {
                            Some(qsdb::users::Role::from_name(r.trim()).unwrap_or_else(|e| {
                                eprintln!("error: {e}");
                                std::process::exit(1);
                            }))
                        };
                        map.insert(db.to_string(), rr);
                    }
                    let global = role
                        .as_deref()
                        .map(qsdb::users::Role::from_name)
                        .transpose()
                        .unwrap_or_else(|e| {
                            eprintln!("error: {e}");
                            std::process::exit(1);
                        });
                    if global.is_none() && map.is_empty() {
                        eprintln!("error: 需要全局角色（admin|writer|reader）或至少一个 --grant");
                        std::process::exit(1);
                    }
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
                    match (global, map.is_empty()) {
                        // 旧形态：只有全局角色 → 旧格式（逐位兼容）
                        (Some(r), true) => {
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
                        // grants 形态：全局角色（若有）落 "*" 槽位
                        (r, false) => {
                            if let Some(r) = r {
                                map.insert("*".to_string(), Some(r));
                            }
                            match qsdb::users::UserStore::add_with_grants(
                                db.store(),
                                &name,
                                &pw,
                                map,
                            ) {
                                Ok(()) => {
                                    println!("user '{name}' added (grants 模式)");
                                    return;
                                }
                                Err(e) => {
                                    eprintln!("error: {e}");
                                    std::process::exit(1);
                                }
                            }
                        }
                        (None, true) => unreachable!("上面已挡"),
                    };
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
                            for u in users {
                                let role = u
                                    .role
                                    .as_ref()
                                    .map(|r| r.name().to_string())
                                    .unwrap_or_else(|| "-".to_string());
                                match &u.grants {
                                    None => println!("{:<20} {}", u.name, role),
                                    Some(g) => {
                                        let gs = g
                                            .iter()
                                            .map(|(k, v)| match v {
                                                Some(r) => format!("{k}={}", r.name()),
                                                None => format!("{k}=-"),
                                            })
                                            .collect::<Vec<_>>()
                                            .join(",");
                                        println!("{:<20} {}  [{}]", u.name, role, gs);
                                    }
                                }
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
            max_sessions,
            spill_dir,
            memory_budget_mb,
            query_memory_limit_mb,
            max_result_rows,
            max_result_mb,
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
            mounts,
            config,
        } => {
            // ---- 配置文件（十九批Ⅶ）：部署档案；CLI 显式值 > 配置文件 > 默认 ----
            // deny_unknown_fields：键名拼错在这里显式失败（写了不生效的配置
            // 比没有配置更危险）
            let cfg: crate::server::ServerConfigFile = match &config {
                Some(cp) => {
                    let text = std::fs::read_to_string(cp).unwrap_or_else(|e| {
                        eprintln!("error: read config {}: {e}", cp.display());
                        std::process::exit(1);
                    });
                    serde_json::from_str(&text).unwrap_or_else(|e| {
                        eprintln!(
                            "error: parse config {}: {e}
（配置不接受未知键——键名拼错会在此报错）",
                            cp.display()
                        );
                        std::process::exit(1);
                    })
                }
                None => Default::default(),
            };
            let path = path.or_else(|| cfg.path.clone()).unwrap_or_else(|| {
                eprintln!("error: --path（或配置文件里的 path）必须提供其一");
                std::process::exit(1);
            });
            let addr = addr
                .or(cfg.addr.clone())
                .unwrap_or_else(|| "0.0.0.0:9430".to_string());
            let token = token.or(cfg.token.clone());
            let auto_compact_secs = auto_compact_secs.or(cfg.auto_compact_secs);
            let writer_epoch = writer_epoch.unwrap_or(cfg.writer_epoch.unwrap_or(0));
            let cache_mb = cache_mb.or(cfg.cache_mb).unwrap_or(256);
            let max_concurrent_queries = max_concurrent_queries
                .or(cfg.max_concurrent_queries)
                .unwrap_or(0);
            let session_idle_timeout_mins = session_idle_timeout_mins
                .or(cfg.session_idle_timeout_mins)
                .unwrap_or(0);
            let max_sessions = max_sessions.or(cfg.max_sessions).unwrap_or(0);
            let memory_budget_mb = memory_budget_mb.or(cfg.memory_budget_mb).unwrap_or(4096);
            let file_cache_mb = file_cache_mb.or(cfg.file_cache_mb).unwrap_or(0);
            let staging_upload_secs = staging_upload_secs.or(cfg.staging_upload_secs).unwrap_or(5);
            let durability = durability
                .or(cfg.durability.clone())
                .unwrap_or_else(|| "process-crash".to_string());
            let storage_type = storage_type
                .or(cfg.storage_type.clone())
                .unwrap_or_else(|| "local".to_string());
            let storage_endpoint = storage_endpoint.or(cfg.storage_endpoint.clone());
            let storage_region = storage_region.or(cfg.storage_region.clone());
            let storage_bucket = storage_bucket.or(cfg.storage_bucket.clone());
            let storage_hdfs_url = storage_hdfs_url.or(cfg.storage_hdfs_url.clone());
            let storage_prefix = storage_prefix.or(cfg.storage_prefix.clone());
            let read_only = read_only || cfg.read_only.unwrap_or(false);
            let spill_dir = spill_dir.or(cfg.spill_dir.clone());
            let flight_bind = flight_bind.or(cfg.flight_bind.clone());
            let staging_dir = staging_dir.or(cfg.staging_dir.clone());
            // 单查询内存：CLI MB > 配置 bytes > 配置 MB > serve 兜底 4GB
            let query_memory_limit_bytes = query_memory_limit_mb
                .map(|mb| mb.saturating_mul(1024 * 1024))
                .or(cfg.query_memory_limit_bytes)
                .or(cfg
                    .query_memory_limit_mb
                    .map(|mb| mb.saturating_mul(1024 * 1024)));
            let mut mounts = mounts;
            if mounts.is_empty() {
                if let Some(ms) = &cfg.mounts {
                    // 配置挂载转成与 --mount 相同的字符串形态——校验/语义共用一套
                    mounts = ms
                        .iter()
                        .map(|m| {
                            let mut x = format!("{}={}", m.name, m.path.display());
                            if let Some(b) = m.budget_mb {
                                x.push_str(&format!(",budget-mb={b}"));
                            }
                            if m.read_only {
                                x.push_str(",read-only");
                            }
                            x
                        })
                        .collect();
                }
            }
            let bootstrap = cfg.bootstrap_admin.clone();

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
            // ---- 多挂载解析（M-B）----
            // 名字小写标识符；选项 = 逗号尾注（budget-mb=N / read-only）。
            let mut extra_mounts: Vec<(String, PathBuf, Option<u64>, bool)> = Vec::new();
            for m in &mounts {
                let Some((name, rest)) = m.split_once('=') else {
                    eprintln!("error: --mount 需要 名字=路径 形态（got {m:?}）");
                    std::process::exit(1);
                };
                let name = name.trim().to_string();
                let ok = !name.is_empty()
                    && name
                        .bytes()
                        .next()
                        .is_some_and(|b| b.is_ascii_lowercase() || b == b'_')
                    && name
                        .bytes()
                        .all(|b| b.is_ascii_lowercase() || b == b'_' || b.is_ascii_digit());
                if !ok {
                    eprintln!(
                        "error: --mount 名字必须是小写 [a-z_][a-z0-9_]*（got {name:?}）——\\
                         它要进 SQL 限定符与 grants 键"
                    );
                    std::process::exit(1);
                }
                if name == "default" {
                    eprintln!("error: default 库固定来自 --path，不许 --mount default=…");
                    std::process::exit(1);
                }
                if extra_mounts.iter().any(|(n, _, _, _)| *n == name) {
                    eprintln!("error: --mount 名字重复：{name}");
                    std::process::exit(1);
                }
                let mut parts = rest.split(',');
                let dir = PathBuf::from(parts.next().unwrap_or_default().trim());
                if dir.as_os_str().is_empty() {
                    eprintln!("error: --mount {name} 缺路径");
                    std::process::exit(1);
                }
                let mut budget: Option<u64> = None;
                let mut ro = false;
                for opt in parts {
                    let opt = opt.trim();
                    if opt.is_empty() {
                        continue;
                    }
                    if opt == "read-only" {
                        ro = true;
                    } else if let Some(v) = opt.strip_prefix("budget-mb=") {
                        budget = Some(v.parse().unwrap_or_else(|_| {
                            eprintln!("error: --mount {name} 的 budget-mb 不是数字：{v}");
                            std::process::exit(1);
                        }));
                    } else {
                        eprintln!(
                            "error: --mount {name} 未知选项 {opt:?}（支持 budget-mb=N、read-only）"
                        );
                        std::process::exit(1);
                    }
                }
                extra_mounts.push((name, dir, budget, ro));
            }
            // 同路径双挂载 = 两套缓存/预算对着同一目录，显式拒绝
            {
                let mut seen = vec![path.clone()];
                for (n, d, _, _) in &extra_mounts {
                    if seen.contains(d) {
                        eprintln!("error: --mount {n} 的路径 {} 已被别的挂载使用", d.display());
                        std::process::exit(1);
                    }
                    seen.push(d.clone());
                }
            }
            let open_mount = |name: &str,
                              p: &std::path::Path,
                              ro: bool,
                              budget_override: Option<u64>|
             -> Arc<Qsdb> {
                let budget = budget_override.unwrap_or(memory_budget_mb);
                let opts = QsdbOptions {
                    storage: storage.clone(),
                    durability: dur,
                    read_only: ro,
                    on_commit: ro
                        .then_some(None)
                        .flatten()
                        .or_else(|| Some(commits.hook_for(name))),
                    writer_epoch,
                    // MB → 字节；0 = 不限（嵌入方/部署方自控）
                    memory_budget_bytes: (budget > 0).then(|| budget.saturating_mul(1024 * 1024)),
                    // 溢出目录：默认与数据同盘；只读句柄回落系统 temp
                    spill_dir: if ro {
                        None
                    } else {
                        Some(spill_dir.clone().unwrap_or_else(|| p.join("_spill")))
                    },
                    scan_cache_bytes: cache_mb * 1024 * 1024,
                    file_cache_bytes: file_cache_mb * 1024 * 1024,
                    staging_dir: staging_dir.clone(),
                    ..Default::default()
                };
                match Qsdb::open(p, opts) {
                    Ok(db) => Arc::new(db),
                    Err(e) => {
                        eprintln!("open {} (mount {name}) failed: {e}", p.display());
                        std::process::exit(1);
                    }
                }
            };
            let mut mount_list: Vec<(String, Arc<Qsdb>)> = vec![(
                "default".to_string(),
                open_mount("default", &path, read_only, None),
            )];
            for (name, dir, budget, ro) in &extra_mounts {
                let db = open_mount(name, dir, *ro, *budget);
                mount_list.push((name.clone(), db));
            }
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
                mount_list,
                &addr,
                token,
                commits,
                auto_compact_secs,
                Some(max_concurrent_queries),
                Some(session_idle_timeout_mins),
                query_memory_limit_bytes,
                max_result_rows,
                max_result_mb,
                cfg,
                max_sessions,
                bootstrap,
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
