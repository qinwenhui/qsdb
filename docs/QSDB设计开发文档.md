# QSDB 设计开发文档

> 版本：v0.1（2026-10-09）
> 状态：设计定稿，开发起点
> 上游参考：`参考项目设计开发文档.md`（MiniDB，以下简称「参考文档」）。参考文档的第三/四/五章细节（类型规范、统计结构等）本文按需引用，不全文复抄。

---

## 一、项目定位

QSDB 是一个**单机、嵌入式优先、列式、Arrow-native、Parquet-backed 的分析型读写库**。

- **一等公民是嵌入式 Rust API**（Arrow RecordBatch 进出），服务于上游大数据计算引擎的读写。
- **同一份核心库编译为单二进制** `qsdb`（server / shell / query 子命令），可独立部署在机器上直接启动。
- 无外部服务依赖（无 HMS / MySQL / ZooKeeper），元数据与数据全部落在本地目录，分区级文件锁保证多进程/多线程互斥。

| 维度 | 决策 |
|------|------|
| 数据模型 | 列式，不可变文件（append + 分区级 overwrite），无行级 UPDATE/DELETE |
| 变更语义 | Append-only + `INSERT OVERWRITE`（分区原子替换），M5+ 再评估行级 delete |
| 事务 | 无跨分区事务；**分区级原子性 + 表级 schema 原子性**由文件协议保证 |
| SQL | 长期目标完整支持；按 SQL-0 → SQL-3 分级渐进引入 |
| 执行模型 | 自研：拉式向量化迭代器 + rayon 数据并行 |
| 内存模型 | Arrow RecordBatch 贯穿全程，零拷贝为默认 |
| 存储后端 | v1 本地文件系统；HDFS/S3 经 object_store 抽象后置 |
| 对外接口 | 嵌入式 API（主）→ CLI → HTTP → Arrow Flight（后） |
| 扩展路径 | SIMD（arrow 内核 + 定制热点）→ GPU（feature，后置） |

**与参考文档的定位差异**：参考文档是通用 SQL 引擎；QSDB 是**计算引擎的读写库**。因此：
1. 嵌入式 API 优先级最高，SQL 是分阶段的门面；
2. 写路径（批量 append、分区 overwrite、小文件治理）与参考文档同等重要甚至更重要；
3. 参考「HMS 教训」的解法完全继承：**元数据 = 本地文件 + 内存缓存 + 分区级文件锁，不走任何网络服务**。

---

## 二、性能红线（验收口径）

| 指标 | 目标 | 验证方式 |
|------|------|---------|
| 元数据加载（万级分区，热缓存） | < 50ms | bench: 冷/热 readdir + prune |
| 分区裁剪后单分区扫描启动延迟 | < 5ms | bench |
| 扫描吞吐（热缓存、数值列、解压后） | 单核 ≥ 300 MB/s（v1），长期 ≥ 500 MB/s | bench vs duckdb `read_parquet` |
| 批量写入（本地盘） | ≥ 100 MB/s 或 ≥ 1M rows/s 单线程 | bench |
| 聚合（1 亿行、3 列 group by） | < 2s 单线程 / < 500ms 8 线程 | M2 gate |
| 嵌入态空闲内存 | < 16MB RSS | smoke test |
| 服务态空闲内存 | < 48MB RSS | smoke test |
| 二进制大小（release，默认 feature） | ≤ 20MB | CI cargo-bloat 守门 |
| 崩溃一致性 | kill -9 中断写入后，重启无脏数据可见，残留 tmp 可清理 | fuzz 测试 |

红线是里程碑验收标准，不是愿望。不达标时优先缩范围/调架构，而不是放水指标。

---

## 三、总体架构

```
┌───────────────────────────────────────────────────────────────┐
│                            门面层                              │
│   Embedded API (主)  │  qsdb 二进制: server/shell/query       │
│                      │  HTTP API (axum)   │  Flight (M5)      │
└──────────┬───────────────────────┬────────────────────────────┘
           │                       │
           │              SqlFrontend（分级开放）
           │   sqlparser-rs → Binder → LogicalPlan → Optimizer
           │                       │
           ▼                       ▼
┌───────────────────────────────────────────────────────────────┐
│                        Session / API 层                        │
│   Qsdb::open / scan / write / overwrite_partition / sql       │
└───────────────────────────┬───────────────────────────────────┘
                            ▼
┌───────────────────────────────────────────────────────────────┐
│                       执行内核（自研）                          │
│   Pull-based: ExecNode → Iterator<Item=Result<RecordBatch>>   │
│   ScanExec(裁剪) → FilterExec → ProjectExec → HashAggExec     │
│   → SortExec/TopNExec → LimitExec → (M4) HashJoinExec ...     │
│   并行：rayon，文件级/分区级并行，两阶段聚合                     │
└───────────────────────────┬───────────────────────────────────┘
                            ▼
┌───────────────────────────────────────────────────────────────┐
│                     表格式与元数据层                            │
│   Catalog(内存) │ 分区 manifest(磁盘真值) │ 列统计/裁剪索引      │
│   提交协议：tmp + 原子 rename；分区 flock（fs2）                │
└───────────────────────────┬───────────────────────────────────┘
                            ▼
┌───────────────────────────────────────────────────────────────┐
│                          存储层                                │
│   StorageBackend trait: Local(v1, std::fs 同步) → HDFS/S3(后) │
│   数据：Parquet(zstd/snappy)  元数据：postcard(serde)          │
└───────────────────────────────────────────────────────────────┘
```

---

## 四、Workspace 与 Crate 结构

```text
qsdb/
├── Cargo.toml                    # [workspace]
├── crates/
│   ├── qsdb/                     # 核心库（可嵌入，无 tokio 依赖）
│   │   └── src/
│   │       ├── lib.rs            # pub use api::Qsdb 等门面
│   │       ├── types/            # 类型系统（继承参考文档 §3）
│   │       │   ├── mod.rs        #   LogicalType / PhysicalType
│   │       │   ├── literal.rs    #   Literal（含 postcard serde）
│   │       │   ├── cast.rs       #   显式/隐式转换规则
│   │       │   └── compare.rs    #   跨类型比较
│   │       ├── catalog/          # Catalog / TableMeta / 分区索引
│   │       ├── table/            # 表格式：manifest 读写、提交协议、GC
│   │       ├── storage/          # StorageBackend trait + LocalBackend
│   │       ├── expr/             # 物理表达式 + 求值
│   │       ├── exec/             # 算子（scan/filter/project/agg/sort...）
│   │       ├── sql/              # parser → binder → logical → optimizer
│   │       ├── write/            # 写缓冲、分区路由、parquet writer、统计收集
│   │       ├── api/              # Qsdb / Session / ScanRequest / WriteResult
│   │       └── error.rs          # QsdbError（thiserror）
│   ├── qsdb-cli/                 # qsdb 二进制：server / shell / query
│   └── qsdb-bench/               # 宏观基准（vs duckdb/datafusion 只读路径）
├── docs/                         # 本文档 + 格式规范 + 协议规范
└── .github/workflows/ci.yml      # fmt / clippy -D warnings / test / size 守门
```

**关键依赖（刻意保持最小）**：

| crate | 用途 | 备注 |
|-------|------|------|
| `arrow` / `parquet` | 内存模型 / 文件格式 | 同版本，唯二重依赖 |
| `object_store` | 存储抽象 | v1 仅启用 local 相关 feature |
| `sqlparser` | SQL 解析（只解析） | 优化与执行全自主 |
| `rayon` | 查询内并行 | |
| `hashbrown` | 聚合 hash 表 | |
| `fs2` | 跨平台文件锁 | Windows LockFileEx / POSIX flock |
| `serde` + `postcard` | 元数据二进制格式 | 与 JSON 一行切换，便于调试 |
| `uuid`(v7) | segment id | 时间有序，利于局部性 |
| `thiserror` / `tracing` | 错误 / 日志 | |
| `axum` + `tokio` | 仅 qsdb-cli 的 server 子命令 | 核心库不依赖 tokio |

**同步优先原则**：核心库 v1 全同步（`std::fs` + rayon），本地盘吞吐最优、嵌入式最简。远程后端引入时在 `StorageBackend` 上加异步实现，server 层用 `spawn_blocking` 桥接。

---

## 五、类型系统（继承参考文档 §3，按此执行）

完全继承参考文档第三章的规范，要点复述 + delta：

| 项 | 决策 |
|----|------|
| 逻辑类型集合 | BOOLEAN / INT8..64 / FLOAT32/64 / DECIMAL(p≤38,s) / STRING(VARCHAR,CHAR) / BINARY / DATE / TIME / TIMESTAMP(±TZ) / INTERVAL |
| DECIMAL | 主路径 i128；聚合 sum 用 i256 累加；精度/舍入规则按参考文档 §3.2 表格执行；JSON 输出为字符串 |
| 时间 | 内部统一 INT64 纳秒；Parquet `TIMESTAMP(isAdjustedToUTC, NANOS)`；时区用 `chrono-tz`，不做 DST 特判 |
| 类型层级 v1 落地 | M0/M1：全部标量类型；**后置**：ARRAY / MAP、超长字符串 overflow 存储 |
| 类型系统代码结构 | 按参考文档 §3.5 的 `types/` 布局 |

Delta：参考文档的 `encoding_hint`、`ConstraintMgr`（唯一/范围/CHECK 约束）整体后置到 M4 之后——读写库场景写入端（计算引擎）自己保证数据质量，v1 不为校验付性能代价。

---

## 六、表格式与目录布局

### 6.1 磁盘布局

```
{qsdb_root}/
├── _catalog/
│   └── {db}.postcard                  # db 内表名清单（默认 db = "default"）
├── {db}/{table}/
│   ├── table.json                     # 表定义：schema/分区键/属性/next_gen 等
│   ├── dt=2026-10-09/                 # 一个分区一个目录（Hive 风格）
│   │   ├── manifest.postcard          # ★ 分区清单 = 该分区唯一提交点
│   │   ├── seg_0192f3a1-….parquet     # 数据文件（uuid v7 命名）
│   │   └── .lock                      # 分区锁文件（fs2）
│   ├── dt=2026-10-10/…
│   └── _trash/…                       # 待清理文件（跨平台安全删除）
```

规则：
- 分区目录名 `k=v`，v 做 **percent-encoding**（`%`、`/`、`=`、控制字符必须转义），解析时反转义。
- `table.json` 用 JSON（人类可读、低频变更）；分区 manifest 用 postcard（高频、机器读）。
- 下划线开头的目录（`_trash`）与隐藏文件不参与分区发现。

### 6.2 分区 Manifest（单个文件 = 原子提交点）

```text
PartitionManifest
├── version: u32
├── generation: u64                  # 每次提交 +1，overwrite 判新旧
├── partition_values: Vec<(String, Literal)>
├── segments: Vec<SegmentEntry>
│   ├── file_name: String
│   ├── row_count / byte_size: u64
│   ├── created_at: i64
│   └── column_stats: Vec<ColumnStats>   # 每列 min/max/null_count
└── schema_fingerprint: u64          # 写入时表 schema 版本哈希，读时校验
```

**为什么是分区级单一 manifest，而不是参考文档的 segment 级 manifest**：
- append 提交 = 「读旧 manifest → 追加条目 → tmp + rename」一次原子替换；
- **overwrite 提交 = 写一个只含新 segments 的 manifest，一次原子替换** —— 分区级原子性天然成立；segment 级方案做 overwrite 需要删除多个文件，无法原子。
- 代价：同分区每次 append 重写整个 manifest（KB～MB 级，分区锁内本就串行）+ 单分区 segment 数靠 compaction 控制（目标单 segment 128–512MB）。这个 trade-off 在单机场景明显划算。
- 升级路径：若未来单分区 segment 数失控，引入 Iceberg 式 manifest-list 分层，协议向后兼容。

### 6.3 Parquet 写入参数

| 参数 | v1 默认 | 说明 |
|------|--------|------|
| Row Group | 128MB 或 1M 行（先到为准） | |
| Data Page | 1MB | |
| 压缩 | zstd(3) 默认 / snappy 可选 | 计算引擎场景压缩比优先 |
| 字典编码 | cardinality < 10^4 自动 | parquet-rs 默认策略 |
| 统计 | 每列 min/max/null_count 全开 | 裁剪的根基，不可关 |

---

## 七、提交协议与并发控制（★ 核心新设计）

### 7.1 Append（同分区追加）

```
1. flock 获取 {partition}/.lock（独占写锁）
2. 写数据文件：seg_{uuid7}.parquet.tmp → rename 去掉 .tmp
3. 从 parquet footer 收集列统计
4. 读旧 manifest → segments.push(new) → generation+1
5. 写 manifest.postcard.tmp → 原子 rename 覆盖 manifest.postcard   ← ★ 提交点
6. 释放锁；更新进程内 Catalog 缓存（ArcSwap）
```

### 7.2 Overwrite（分区原子替换）

```
1. flock 获取分区锁
2. 写全新数据文件（同 append 2-3）
3. 构建只含新 segments 的新 manifest，generation = old+1
4. 原子 rename 覆盖旧 manifest                                    ← ★ 提交点
5. 旧数据文件 move 进 _trash/{table}/{ts}/（不是 unlink）
6. 释放锁
7. 后台 GC：_trash 内超过静默期（默认 10min）的目录删除
```

**语义**：overwrite 时刻之后的扫描看到全新数据；正在进行的旧扫描继续读完旧文件（POSIX 下 rename/unlink 不影响已打开 fd）。10min 静默期兜底慢查询与 Windows 句柄。

### 7.3 读协议

```
1. 扫描开始时对每个目标分区读一次 manifest（单文件原子，天然快照）
2. 分区内 segment 列表在本次扫描内固定 —— 之后的写入不影响本扫描（快照隔离，分区粒度）
3. partition pruning：readdir 表目录 + 解析目录名（不读 manifest，O(1) per 分区）
4. row group pruning：manifest 内 column_stats + 谓词
```

跨分区不保证同一时刻快照（不同分区 manifest 独立提交）。对分析场景可接受，文档明示。

### 7.4 崩溃一致性

- manifest rename 之前的任何中断（kill -9 / 断电）：数据文件成为孤儿 → 启动时 `_trash` 与孤儿 seg 清理器回收（对 manifest 不可见的 seg 文件即孤儿）。
- rename 是原子操作（同文件系统），不存在半提交状态。
- tmp 文件一律可安全删除。

### 7.5 跨平台注意事项（开发在 Windows，部署多在 Linux）

| 问题 | Windows 行为 | 对策 |
|------|-------------|------|
| rename 覆盖已存在文件 | `std::fs::rename` 失败 | 封装 `atomic_replace()`：先 `MoveFileEx(REPLACE_EXISTING)`（windows-sys），失败退化为锁内 remove+rename |
| 删除已打开文件 | 失败（除非 FILE_SHARE_DELETE） | 一律先 move 到 `_trash`，静默期后删 |
| flock | LockFileEx（fs2 封装） | 语义对齐 POSIX |
| 文件名大小写 | 不敏感 | 表名/分区值统一 lowercase 规范化 or 禁止仅大小写差异的命名 |

CI 必须双平台跑（windows-latest + ubuntu-latest），锁与提交协议用多进程压测用例覆盖。

---

## 八、元数据层

| 原则 | 实现 |
|------|------|
| 内存优先 | `Catalog = RwLock<ArcSwap<CatalogInner>>`，读路径全程无锁拿快照 |
| 磁盘真值 | manifest / table.json / _catalog，任何时候可从磁盘全量重建内存 |
| 懒加载 | 分区 manifest 首次触达才读 + LRU（moka 可选，v1 用简单 `DashMap` + 容量上限） |
| 写不阻塞读 | 写只在锁内改磁盘 + 原子换内存快照 |
| 外部进程可见 | 同目录多进程（计算引擎嵌入 + 独立 qsdb server 并存）靠 flock + 磁盘真值协调；进程内缓存通过「锁获取后比对 manifest mtime/generation」失效 |

`table.json`：schema（字段/类型/nullable）、partition_keys、表属性、created_at。schema 变更 v1 只支持加列（向后兼容），`schema_fingerprint` 防止旧 reader 误读新格式。

---

## 九、执行引擎（自研）

### 9.1 执行模型

- 物理计划 = `ExecNode` 树；每个节点 `execute() -> Box<dyn Iterator<Item = Result<RecordBatch>> + Send>`。
- 拉式驱动，batch 默认 **8192 行**（自适应 1024–65536）。
- 扫描层并行：rayon 按「文件 × row group」切块；上层算子消费合并流。
- 两阶段聚合：分区任务内 partial agg（hashbrown 原始表，group key 序列化字节）→ 单线程 final merge。
- Sort：v1 内存排序（nomerge sort + arrow::compute::sort）；TopN 用堆；外排 spill 到 M4。
- Filter/Project：表达式求值出 `BooleanArray` → `arrow::compute::filter_record_batch`，内核天然向量化。

### 9.2 表达式系统

```rust
trait PhysExpr: Send + Sync {
    fn data_type(&self) -> &DataType;
    fn eval(&self, batch: &RecordBatch) -> Result<ArrayRef>;
}
// Column / Literal / BinaryOp / Compare / Cast / ScalarFn / IsNull / Not / And / Or
```

- 逻辑计划 → 物理表达式时做常量折叠与隐式 cast 注入。
- 标量函数 v1 集：比较/算术/逻辑 + `length/upper/lower/substr/trim` + `date_trunc/date_diff` + `abs/round/floor/ceil`。函数注册表 `HashMap<&str, Arc<ScalarFnImpl>>`，为 M5 UDF 预留。
- Decimal 运算精度规则按参考文档 §3.2 表格实现，乘除走 i256 中间量。

### 9.3 聚合函数

| 函数 | 状态 | 说明 |
|------|------|------|
| count / min / max | u64 / 类型化 Option | |
| sum(int) | i128 | |
| sum(decimal) | i256 | |
| avg | (i256, u64) | |
| sum/avg(float) | Kahan 或 pairwise | float 精度 |
| distinct_count | v1 HashSet，后置 HLL | M4 |

### 9.4 内存管理

- `MemoryPool`：每查询登记预留字节数（scan 解压 buffer + 聚合状态 + sort），超限先触发 spill（M4）/ 报错。
- 嵌入式场景暴露 `QsdbOptions { memory_limit, batch_rows, rayon_threads, … }`。
- 超低占用的关键：核心库无后台常驻线程（rayon 池按需创建）、无 tokio、元数据懒加载。

---

## 十、SQL 前端路线（长期完整，分级解锁）

`sqlparser-rs`（Generic 方言 + Hive 风格分区语法）只做解析；Binder/LogicalPlan/Optimizer/物理计划全自研。按里程碑开放语法，未开放的语法在 parse 后立即明确报「not supported in this version」。

| 级别 | 里程碑 | 语法范围 |
|------|--------|---------|
| SQL-0 | M2 | `CREATE/DROP TABLE`、`DESCRIBE`、`SHOW TABLES/PARTITIONS`、`SELECT … FROM … WHERE … LIMIT`（投影/过滤/字面量/比较/逻辑）、`INSERT INTO … VALUES`（小批量） |
| SQL-1 | M2 末–M3 | `GROUP BY` + count/sum/avg/min/max、`ORDER BY`、`DISTINCT`、隐式/显式 cast、标量函数 v1 集 |
| SQL-2 | M4 | `INNER/LEFT JOIN`（等值 hash join）、`IN`/`EXISTS`（相关子查询受限形态）、`HAVING` |
| SQL-3 | M5+ | 嵌套子查询、CTE、窗口函数、UDF/UDAF 注册、完整优化器规则集 |

**优化器**（规则式，够用即止）：谓词下推 → 列裁剪 → limit 下推 → partition/rowgroup 裁剪 → 投影合并。代价模型 M5 再议，不做过早优化。

---

## 十一、嵌入式 API（一等公民）

```rust
use arrow::record_batch::RecordBatch;
use qsdb::{Qsdb, QsdbOptions, ScanRequest, Predicate};

// 打开（同目录多实例靠文件锁互斥协调）
let db = Qsdb::open("/data/qsdb", QsdbOptions::default())?;

// DDL（API 与 SQL 双入口，同一实现）
db.execute_ddl(Ddl::CreateTable { .. })?;

// 批量写：自动按分区字段路由、攒批、写 parquet、提交 manifest
let result: WriteResult = db.append("events", &[batch1, batch2])?;
// result = { rows, bytes, partitions_touched, segments }

// 分区整体替换（计算引擎重算回写，幂等）
db.overwrite_partition("events", &[("dt", Literal::Date(20664))], &batches)?;

// 扫描：列裁剪 + 分区裁剪 + row group 裁剪 + 谓词求值
let scan: ScanResult = db.scan(ScanRequest {
    table: "events",
    projection: &["tenant_id", "amount"],
    predicate: Predicate::parse("dt = DATE '2026-10-09' AND amount > 0")?,
    limit: None,
})?;
for batch in scan { /* RecordBatch 流式消费，零拷贝 */ }

// SQL 门面（随里程碑逐步可用）
let stream = db.sql("SELECT tenant_id, sum(amount) FROM events GROUP BY 1")?;
```

设计约束：
- `Send + Sync`，`&self` 全接口（内部 ArcSwap/DashMap），计算引擎可随意共享/跨线程。
- 返回流式迭代器而非全量物化；上游可随时提前 drop。
- 写接口接受 `&[RecordBatch]`，攒批/分区路由/统计收集在库内完成。

---

## 十二、单机服务形态（qsdb 二进制）

```
qsdb server  --path /data/qsdb --addr 0.0.0.0:9430 [--token <bearer>]
qsdb shell   --path /data/qsdb          # 本地 REPL（rustyline）
qsdb query   --path /data/qsdb "SELECT ..."   # 一次性执行，Arrow IPC / JSON 输出
qsdb admin   tables|partitions|compact|gc-check --path …
```

HTTP API（M3）：

```
POST /api/v1/query        { "sql": "…", "format": "ipc|json" }   → 流式 Arrow IPC / JSON
POST /api/v1/write        body = Arrow IPC stream + ?table=&mode=append|overwrite
GET  /api/v1/tables | /tables/{t}/schema | /tables/{t}/partitions
GET  /health | /metrics   # prometheus 文本：行数/字节数/段数/缓存命中/查询时延分位
```

- 鉴权 v1：静态 bearer token；TLS 交给反向代理。
- server 是核心库的薄壳：tokio 只做 IO，查询执行进 `spawn_blocking`，不污染核心库。
- Arrow Flight（M5）：高吞吐摄入/取数通道，gRPC + IPC。

---

## 十三、Compaction 与小文件治理（M3）

计算引擎高频回写会产生小文件。策略：

- 写侧：`WriteBuffer` 按分区攒批（size/time 双阈值，默认 128MB 或 5s），上游显式 flush。
- 后台 compaction：分区锁内把 N 个小 seg 重写为 1 个大 seg，manifest 原子替换，旧文件走 `_trash`。触发条件：`segment 数 > 阈值 && 平均 seg < 64MB`。
- 在线执行：compaction 与读并发安全（manifest 快照语义），与写靠同一把分区锁串行。
- 全程可配置、可关闭（嵌入态由上游调度时，`QsdbOptions.background_compaction = false`）。

---

## 十四、开发计划与验收标准

单人节奏估算，每个里程碑有硬性验收 gate，不过不进下一个。

### M0（第 1 周）：骨架
- workspace 建立、CI（fmt/clippy -D warnings/test 双平台）、`qsdb-bench` 空跑 harness。
- types 全标量类型 + cast/compare + Literal serde，单测覆盖（含 Decimal 精度规则表驱动测试）。
- **Gate**：`cargo test` 全绿；类型测试含参考文档 §3.2 精度矩阵全组合。

### M1（第 2–4 周）：读写闭环（无 SQL）✅ 已完成
- 目录布局 + table.json + 分区 manifest + 提交协议（append/overwrite）+ flock + `_trash` GC。
- parquet 写（统计收集）与读（投影/row group 裁剪）。
- 嵌入式 API v0：open/append/overwrite_partition/scan/gc。
- **表级分区索引 `partitions.idx`**（§8 规划的 v2 缓存在 M1 提前落地）：万级分区
  逐 manifest open+read 在 Windows 上 ~0.2ms/文件 → 2s+ 超红线；单一索引文件
  （全分区 manifest 快照，commit 时增量 upsert，读侧乐观信任 + 损坏回退）
  → 实测 18ms/万分区。已知写放大：万级分区高频小批提交时索引全量重写 O(n²)，
  由 M3 WriteBuffer 攒批解决。
- **Gate（实测，Windows 开发机 release）**：
  - ✅ 随机写读回环 fuzz（4 线程并发 append + overwrite + 分区行数核算）无脏读；
  - ✅ 崩溃一致性：孤儿 segment 不可见 / tmp 残留无害 / overwrite 提交点前旧数据完整；
  - ✅ 多实例（模拟多进程）同分区 append 无丢失无重复；
  - ✅ 扫描吞吐 855 MB/s 单线程（红线 ≥ 300）；
  - ✅ 万级分区元数据 18ms 冷/19ms 热（红线 < 50ms）；
  - ✅ 写入 1.4M rows/s（红线 ≥ 1M rows/s）。

### M2（第 5–8 周）：向量化执行 + SQL-0/1
- 表达式系统、Filter/Project/Limit、两阶段 HashAgg、Sort/TopN、Distinct。
- SQL-0 + SQL-1、`qsdb shell/query`、错误信息体系。
- **Gate**：
  - 1 亿行 3 列 group by < 2s 单线程 / < 500ms 8 线程；
  - SQL e2e 测试 ≥ 100 用例（含类型边界/NULL 语义）；
  - 嵌入态空闲 RSS < 16MB。

### M3（第 9–11 周）：服务化与治理
- `qsdb server` + HTTP API + /metrics；compaction；GC 策略；崩溃恢复演练脚本。
- **Gate**：
  - HTTP 写入 ≥ 500MB/s（本机回环）；
  - 服务态空闲 RSS < 48MB、二进制 ≤ 20MB（CI 守门）；
  - compaction 在线运行时 P99 读延迟劣化 < 10%；
  - 公开 bench 报告：scan vs duckdb/datafusion read_parquet。

### M4（第 12 周起）：Join 与进阶
- HashJoin（build/probe、广播小表）、SQL-2、外排 spill、bloom filter（等值点查）、S3/HDFS backend、distinct_count HLL。
- **Gate**：TPC-H 子集（当时语法范围内）对比报告；spill 下 10× 内存超限查询可完成。

### M5（按需）：生态与极限
- SQL-3（窗口/CTE/UDF）、Arrow Flight、SIMD 定制热点内核、约束系统、GPU feature（Vulkan 优先）。

---

## 十五、风险与对策

| 风险 | 影响 | 对策 |
|------|------|------|
| 自研 SQL 范围蔓延 | 工期失控 | 语法分级硬边界；未开放语法立即报错，不留半成品 |
| Windows/POSIX 文件语义差异 | 数据损坏 | §7.5 对策 + 双平台 CI + 多进程压测；trash 延迟删除 |
| 小文件爆炸 | 元数据/扫描退化 | WriteBuffer 攒批 + M3 compaction，红线监控 segment 均值 |
| 单 manifest 重写放大 | 高频 append 退化 | 分区锁内本就串行 + compaction 控制 segment 数；必要时升级 manifest-list |
| arrow/parquet 大版本升级破坏 | 维护成本 | workspace 统一版本 pin，升级单独立 PR + 全量回归 |
| 性能红线不达标 | 定位失败 | bench 前置（M0 就有 harness）；不达标先缩范围/换实现，红线不放水 |
| 单人开发 | 节奏 | 里程碑 gate 制，随时可停在可用状态 |

---

## 十六、与参考文档的差异清单

| # | 参考文档 | QSDB | 原因 |
|---|---------|------|------|
| 1 | 通用 SQL 引擎定位 | 计算引擎读写库，嵌入式 API 一等公民 | 用户定位 |
| 2 | segment 级 manifest（存在即真值） | 分区级单一 manifest（rename 即提交） | overwrite 原子性 + 协议更简 |
| 3 | 无 overwrite | 分区级 INSERT OVERWRITE + trash GC | 计算引擎幂等回写刚需 |
| 4 | SQL Phase1 即较全 | SQL-0..3 分级，长期完整 | 自研执行引擎的节奏控制 |
| 5 | 约束系统（唯一/范围/CHECK）在元数据层 | 后置 M5 | 写入端保证数据质量，v1 不付性能税 |
| 6 | MessagePack 元数据 + mmap | postcard(serde)，可一行切 JSON 调试 | 工具链更简，性能同级 |
| 7 | HDFS 一等后端 | 本地优先，HDFS/S3 经 object_store 后置 | 先把单机做到极致 |
| 8 | 无 compaction | 写缓冲 + 在线 compaction + trash GC | 高频回写场景必需 |
| 9 | 无崩溃一致性细节 | §7.4 崩溃协议 + kill -9 fuzz 为 M1 gate | 读写库的底线要求 |

---

## 附：仓库初始化清单（M0 执行）

- [ ] `git init` + 推送 github.com/qinwenhui/qsdb
- [ ] README（中英）：定位、快速上手（嵌入式 5 行示例）、性能红线
- [ ] LICENSE：Apache-2.0（建议，生态兼容）
- [ ] CI：双平台 fmt/clippy/test + 二进制体积守门
- [ ] `docs/format-spec.md`：manifest/table.json 字段级规范（实现前冻结）
