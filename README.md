# QSDB

**A standalone, single-node, embeddable columnar analytical database. Arrow-native. Parquet-backed. Zero external services.**

通用嵌入式分析数据库：作为 Rust 库嵌入任意应用/引擎，或编译为单二进制直接部署。不绑定任何上游，接口全部基于开放标准（Arrow / Parquet / HTTP / SQL）。三件事做到极致——**读写速度、资源占用、并发能力**。

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
| M0 | 骨架 + 类型系统（Decimal 精度矩阵 / cast / compare） | ✅ |
| M1 | 读写闭环：表格式 + 提交协议 + 三级裁剪扫描 + 嵌入式 API | ✅ |
| M2 | 向量化执行 + SQL-0/1 + CLI | ✅（聚合红线部分达成，M4 复测） |
| M3 | HTTP 服务 + compaction + WriteBuffer 攒批 | ✅ |
| M4 | Join + spill + bloom + S3/HDFS | ⬜ |
| M5 | 窗口/CTE/UDF + Flight + SIMD/GPU | ⬜ |

M1 实测（Windows 开发机，release）：写入 1.4M rows/s、扫描 855 MB/s 单线程、万级分区元数据 18ms。
M2 实测：1 亿行 3 列 group by 0.92s（18 线程）；SQL-0/1 全语义 e2e 106 测试。
M3 实测：HTTP 并发写入 522MB/s（8 流回环）；二进制 15.7MB；compaction/攒批就位。

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
