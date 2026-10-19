# QSDB

**高性能列式分析数据库。一个二进制。Arrow 原生。Parquet 存储。零外部服务。**

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.85%2B-orange.svg)](https://www.rust-lang.org)
[![CI](https://github.com/qinwenhui/qsdb/actions/workflows/ci.yml/badge.svg)](https://github.com/qinwenhui/qsdb/actions/workflows/ci.yml)

官网：**[qsdb.qinwh.cn](http://qsdb.qinwh.cn)**

[English](README.md) | 简体中文

QSDB 是面向大数据量场景的分析数据库——GB 级表、分区化 Parquet 湖、
亿级行聚合。它只做三件事并做到极致：**读写速度、资源占用、并发能力**，
其余刻意不做。无需集群协调、无外部元数据服务、无 JVM：整个系统是
**一个静态二进制**，作为常规网络服务运行（HTTP API / JDBC / ODBC 驱动），
也可以作为 Rust 库同进程嵌入——**同一引擎、同一磁盘格式；这只是交付
形态的区别，不是两个产品**。

---

## 特性

- **自研向量化执行引擎** —— 哈希 join（字符串键零拷贝）、DECIMAL 窗口函数、
  排序/聚合落盘溢出、高基数 `count(DISTINCT)` 的密集直索引路径。
  每个版本都跑 19 例基准套件（带值断言）；方法论与数字全部公开。
- **深度裁剪的 Parquet 存储** —— 列统计 + 分区/行组两级裁剪直接做进扫描路径；
  单核扫描吞吐以数百 MB/s 计。
- **列式、不可变文件** —— append + 分区级原子 overwrite；无行级
  UPDATE/DELETE（分析场景不需要它，而不可变性正是其余一切便宜的前提）。
- **Arrow 全链路** —— 进程内 `RecordBatch` 零拷贝交接；需要时线上走 Arrow IPC。
- **宽 SQL 面** —— 对 Spark 方言友好：`stack` / `PIVOT`、CTE（含死 CTE 消除）、
  `least`/`greatest`、非 ANSI 双引号字符串、多值 `IN` 下推、参数化查询
  （`$1` / `?`——值从不进入 SQL 文本）。
- **真正的服务端** —— 恒定内存的流式结果、协作式取消（端到端亚 100ms）、
  会话、多库挂载、库级权限、运行期热改护栏（结果行数/体量、超时、内存）、
  客户端可运行期拉取的机器可读错误码目录。
- **元数据零外部组件** —— 本地文件 + 内存缓存 + 分区级文件锁。
  无 HMS、无 MySQL、无 ZooKeeper。
- **依赖刻意保持最小** —— arrow-rs、parquet-rs、sqlparser-rs、tokio/axum，
  基本就是全部。

## 快速开始

```bash
# 构建
cargo build --release -p qsdb-cli

# 启动服务（HTTP API，:9430）
./target/release/qsdb server --path /data/qsdb --addr 0.0.0.0:9430

# 或直接对库操作
./target/release/qsdb shell  --path /data/qsdb
./target/release/qsdb query  --path /data/qsdb "SELECT count(*) FROM t"
```

```sql
CREATE TABLE events (uid VARCHAR, amount DECIMAL(38,8), ts TIMESTAMP)
  PARTITIONED BY (dt DATE);

INSERT INTO events VALUES (...);

SELECT uid, sum(amount) AS total
FROM events
WHERE dt = '2026-10-03'
GROUP BY uid ORDER BY total DESC LIMIT 100;
```

## Rust 客户端：`qsdb-client`

```bash
cargo add qsdb-client
```

```rust
use qsdb_client::{Auth, Client};

let c = Client::connect("http://127.0.0.1:9430", Auth::Basic {
    user: "admin".into(),
    password: "…".into(),
})?;

// 读：默认列式批，零额外依赖
let rs = c.query("SELECT cat, count(*) FROM t GROUP BY cat")?;
let t = rs.into_bulk()?;
for row in t.rows() {
    let (cat, n) = (row.get(0)?, row.get(1)?);
    println!("{cat} {n}");
}

// 批量装载：不要逐行 INSERT（实测慢 37 倍）
c.append("t", &[
    ("id", vec![Some(1i64), Some(2)].into()),
    ("cat", vec![Some("x".to_string()), None].into()),
])?;

// 大结果：流式、内存恒定
let mut s = c.query_stream("SELECT * FROM big", &Default::default())?;
while let Some(chunk) = s.next_chunk()? { /* … */ }
```

客户端在手写 HTTP/1.1 之上只依赖 `serde`——**零网络依赖**，除非显式打开
Arrow IPC（`features = ["ipc"]`）。

## 嵌入式 API

```rust
use qsdb::{Qsdb, QsdbOptions};

let db = Qsdb::open("/data/qsdb", QsdbOptions::default())?;
db.append("events", &[batch])?;                 // 分区路由 + 攒批
db.overwrite_partition("events", &[(...)], &batches)?; // 分区原子替换
for batch in db.scan(Default::default())? { /* RecordBatch 流 */ }
```

嵌入形态拿到的是**同进程零序列化的 Arrow 直通**——工具、边缘部署、
测试夹具的标准姿势。它是同一引擎的进程内运行，不是阉割版：当库需要
活得比进程久、或有多个客户端要访问时，用服务端。

## 性能

基准套件在 `crates/qsdb-bench`——19 个用例（导入、扫描、聚合、join、
去重、导出），每个都带值断言，在专用基准机上跑：

```bash
cargo run -p qsdb-bench --bin bench_suite -- --dir <数据目录> --rows 5000000 --all
```

方法论、逐格数字、以及背后的剖析都随源码一起维护在工程笔记里。我们遵守
一条简单的纪律：**每个数字只有一个真源，每个回退都有根因，而不是耸肩。**

## 文档

| 文档 | 内容 |
|---|---|
| [`docs/error-codes.md`](docs/error-codes.md) | 全部错误码：HTTP 状态、分类、SQLSTATE、gRPC 映射 |
| [`docs/configuration.md`](docs/configuration.md) | 全量配置项，按生效层级组织 |
| [`docs/format-spec.md`](docs/format-spec.md) | 磁盘布局与提交协议 |
| [`docs/bulk-format.md`](docs/bulk-format.md) / [`docs/rows-format.md`](docs/rows-format.md) | 二进制批格式（写路径、流式读） |

## 仓库结构

```
crates/
  qsdb         # 内核：存储、执行、SQL → 计划 → 向量化执行
  qsdb-cli     # 二进制：server（HTTP）、shell、query、管理命令
  qsdb-client  # Rust 薄客户端（发布到 crates.io）
  qsdb-bench   # 基准套件 + 探针
```

## 开发

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

CI 在 Linux 与 Windows 上跑 fmt + clippy + 测试，外加 release 二进制体积闸。
行为变更必须带回归测试；错误码变更必须同步 `docs/error-codes.md`
（有测试强制它与运行期码表零漂移）。

## 路线图

| 领域 | 状态 |
|---|---|
| 类型系统、存储、提交协议、裁剪扫描 | ✅ |
| 向量化执行：join、窗口、溢出、去重 | ✅ |
| HTTP 服务、会话、多库、库级权限、护栏 | ✅ |
| COPY 导出（分区 parquet）、外表、S3/HDFS 后端 | ✅ |
| 参数化查询、取消、流式、错误码目录 | ✅ |
| Flight/gRPC 面 | ✅（增强中） |
| 配额、物化视图 | 规划中 |

## 生态

生态项目全部在 [qinwenhui](https://github.com/qinwenhui) 下。

| 项目 | 是什么 |
|---|---|
| [qsdb-studio](https://github.com/qinwenhui/qsdb-studio) | 官方桌面工作台——查询、浏览、管理（Tauri + Vue 3，基于 `qsdb-client`） |
| [qsdb-jdbc](https://github.com/qinwenhui/qsdb-jdbc) | JDBC 驱动——BI 工具与 JVM 应用 |
| [qsdb-odbc](https://github.com/qinwenhui/qsdb-odbc) | ODBC 驱动——Excel / Tableau / Power BI |
| [qsdb-sync](https://github.com/qinwenhui/qsdb-sync) | 大块数据搬运——QSDB ↔ 数据湖、S3/HDFS、parquet 目录 |

**官网**：[qsdb.qinwh.cn](http://qsdb.qinwh.cn)

## 许可证

[MIT](LICENSE)
