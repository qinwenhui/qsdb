# QSDB

**A single-node, embeddable, columnar analytical read/write store. Arrow-native. Parquet-backed. Zero external services.**

QSDB 为大数据计算引擎而生：作为 Rust 库嵌入时它是高吞吐读写层，编译为单二进制时它是可直接部署的分析服务。设计目标只有两个——**超高性能**与**超低占用**。

- 列式、不可变文件：append + 分区级原子 overwrite，无行级 UPDATE/DELETE
- Arrow `RecordBatch` 全链路零拷贝，嵌入式 API 是一等公民
- Parquet 存储 + 列统计 + 分区/行组两级裁剪
- 元数据 = 本地文件 + 内存缓存 + 分区级文件锁，**不依赖任何外部服务**（无 HMS/MySQL/ZooKeeper）
- 自研向量化执行引擎（依赖刻意保持最小：arrow-rs + parquet-rs + sqlparser-rs）

## 嵌入式使用（规划 API，随里程碑逐步可用）

```rust
use qsdb::{Qsdb, QsdbOptions, ScanRequest};

let db = Qsdb::open("/data/qsdb", QsdbOptions::default())?;

// 批量写：自动分区路由、攒批、统计收集
db.append("events", &[batch])?;

// 分区整体原子替换（计算引擎幂等回写）
db.overwrite_partition("events", &[("dt", lit_date(20664))], &batches)?;

// 扫描：列裁剪 + 分区裁剪 + row group 裁剪
for batch in db.scan(ScanRequest { table: "events", ..Default::default() })? {
    // RecordBatch 流式消费
}
```

## 单机服务

```bash
qsdb server --path /data/qsdb --addr 0.0.0.0:9430   # HTTP API (M3)
qsdb shell  --path /data/qsdb                        # 本地 REPL (M2)
qsdb query  --path /data/qsdb "SELECT ..."           # 一次性查询 (M2)
```

## 性能红线（验收口径）

| 指标 | 目标 |
|------|------|
| 元数据加载（万级分区，热缓存） | < 50ms |
| 扫描吞吐（热缓存、数值列） | 单核 ≥ 300 MB/s |
| 聚合（1 亿行 3 列 group by） | < 2s 单线程 / < 500ms 8 线程 |
| 嵌入态空闲内存 | < 16MB RSS |
| 服务态空闲内存 | < 48MB RSS |
| 二进制体积（release） | ≤ 20MB |

## 路线图

| 里程碑 | 内容 | 状态 |
|--------|------|------|
| M0 | 骨架 + 类型系统（Decimal 精度矩阵 / cast / compare） | 🚧 进行中 |
| M1 | 读写闭环：表格式 + 提交协议 + 裁剪扫描 + 嵌入式 API | ⬜ |
| M2 | 向量化执行 + SQL-0/1 + CLI | ⬜ |
| M3 | HTTP 服务 + compaction + 治理 | ⬜ |
| M4 | Join + spill + bloom + S3/HDFS | ⬜ |
| M5 | 窗口/CTE/UDF + Flight + SIMD/GPU | ⬜ |

## 开发

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo run -p qsdb-bench -- smoke          # 基准管线自检
```

设计文档见 [`docs/QSDB设计开发文档.md`](docs/QSDB设计开发文档.md)，磁盘格式规范见 [`docs/format-spec.md`](docs/format-spec.md)。

## License

Apache-2.0
