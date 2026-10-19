# QSDB 设计开发文档

> 版本：v0.1（2026-10-04）
> 状态：设计定稿，开发起点
> 上游参考：早期原型（MiniDB）的设计笔记（内部资料，未随本仓发布）。其第三/四/五章细节（类型规范、统计结构等）本文按需引用，不全文复抄。

---

## 一、项目定位

> 2026-10-10 定位修正：QSDB 是**独立通用的单机分析数据库**，不绑定任何上游计算引擎。
> 唯一职责：极致的读写速度、极致低的资源占用、极致的高并发。生态工具（数据同步 /
> CDC / 各类导入导出生态）是未来**独立新项目**，QSDB 只保证接口开放与标准
> （Arrow / Parquet / HTTP），不在本仓库内实现。

QSDB 是一个**单机、嵌入式优先、列式、Arrow-native、Parquet-backed 的通用分析数据库**。

- **一等公民是嵌入式 Rust API**（Arrow RecordBatch 进出），任何应用、任何计算引擎、
  任何语言生态（经 HTTP/IPC）都可以作为客户端。
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

**与参考文档的定位差异**：参考文档是通用 SQL 引擎；QSDB 收敛为**通用高性能读写存储引擎**。因此：
1. 嵌入式 API 优先级最高，SQL 是分阶段的门面；
2. 读写路径（批量 append、分区 overwrite、小文件治理、并发扩展）是最核心的工程对象；
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
| 二进制大小（release，默认 feature） | ≤ 25MB（2026-10-11 定案放宽：SQL-3/约束/ALTER 进核心后实测 24.1MB；panic=unwind 表 +7.8MB 换会话隔离，abort 可回 ~16MB 但失去隔离） | CI size-guard 守门 |
| 崩溃一致性 | kill -9 中断写入后，重启无脏数据可见，残留 tmp 可清理 | fuzz 测试 |

红线是里程碑验收标准，不是愿望。不达标时优先缩范围/调架构，而不是放水指标。

### 2.1 对比口径（2026-10-10 定案，血泪教训）

早期 bench 用 `duckdb_vs`（QSDB 侧 4GB **解码后**缓存 + best-of-3、同进程交替）
得出"四项反超 DuckDB"，第三方 POC 用真实业务形态复测后不成立。两条口径问题：

1. **缓存不对称**：`scan_cache_bytes` 缓存**解码后**的 Arrow 批，第 2/3 轮零解码；
   DuckDB `read_parquet` 每查询现解码（其 buffer 只缓存压缩页）。不是同一场比赛。
2. **同进程互相干扰**：QSDB 受干扰后慢 2x、DuckDB 仅 1.3x——**抗干扰能力弱是真实
   特征**（生产是多负载共存），不能用"失真"解释，但会让比率偏离单查询真实水平。

**权威口径**：`crates/qsdb-bench/src/bin/fair_vs.rs`
- `scan_cache_bytes = 0`（冷读，每轮解码，与 DuckDB 同起点）
- `FAIR_SOLO=qsdb|duck` **分进程独立测量**（消除互相干扰）
- 交替 5 轮取**中位数**并报告离散度（旧协议"先跑完一方再跑另一方"会被负载漂移带偏）

**结论纪律**：对外性能声明必须写明「口径 + 数据形态 + 基数」；`scan_cache_bytes > 0`
的数字不得用于与每次现解码的引擎对比。已知我们的强项（架构性、任何口径成立）：
元数据短路的点查（>1000x）、四级裁剪的过滤扫描；弱项：高基数 group by、大结果集 join。

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
│   ├── dt=2026-10-04/                 # 一个分区一个目录（Hive 风格）
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
- M7-B 表达式补全：`CASE WHEN`（简单/搜索形态）/ `coalesce` / `nullif` / `||` 拼接；`abs/round/floor/ceil` 补 Decimal 入参（i128 定点取整，digits≥scale 零拷贝）；`%` 补 Decimal；数学函数 `sqrt/exp/ln/log/sin/cos/tan/atan/power(pow)`（f64 向量化，Decimal 入参按 scale 换算）。类型统一走 `common_supertype`（数值塔向宽处靠 / 字符串收编），宽 Decimal 除法超 38 截 scale 保整数位。
- M7-B2 日期算术：`DATE ± INTEGER`（天，Add 可换序）/ `DATE - DATE`（天数 BIGINT）；`date_diff(unit, a, b)`（DuckDB 边界跨越语义，year/quarter/month/week/day + TIMESTAMP/TIME 的 hour/minute/second，字符串日期字面量白名单提升、Date↔Timestamp 归一）；`EXTRACT(field FROM x)` / `extract(field, x)`（year/quarter/month/day/dow/doy/hour/minute/second）。**F16**：temporal 列 × 字符串**字面量**比较在绑定期提升（`dt = '2026-01-01'` → typed DATE 字面量，min/max 裁剪与下推不受影响；字符串列仍诚实报错，解析失败给 `DATE '...'` 指引）。
- M7-B3 缺口收口：`BETWEEN` / `NOT BETWEEN`（≡ `x >= lo AND x <= hi`，NOT 取反走 Kleene；谓词下推/min-max 裁剪自动受益，普通与聚合上路两处对称实现）；`date_trunc(unit, x)`（同型返回：Date→year/quarter/month/week/day，Timestamp→+hour/minute/second，Time→hour/minute/second；周一边界与 date_trunc/date_diff 一致）；**标量函数套聚合**（`round(sum(x))` / `ceil(avg(x))` / `upper(group_col)` 等——bind_scalar_fn_over 落地，参数递归 rebind_over_agg，尾段与普通路径共用 finish_scalar_fn）。**修复两个既有 bug**：HAVING 里不在 SELECT 的聚合从未收集（`.expect` panic → 置换后先收集，隐藏聚合列模式）；collect_aggs 不递归 Floor/Ceil AST 节点（`ceil(avg(x))` 被误判非聚合查询撞聚合闸门）。
- M7-B4 SQL 短板收口：**LIKE/ILIKE/NOT LIKE**（模式绑定期预编译——Exact/Prefix/Suffix/Contains 零回溯快路径 + 通用回溯；ESCAPE 显式；NULL 三值；字符串列操作数诚实报错）；**UNION [ALL] / INTERSECT / EXCEPT**（LogicalPlan::SetOp + SetOpExec——UNION 直连（distinct 包 DistinctExec），INTERSECT/EXCEPT 复用 semi-join 行键编码（encode_join_key）存在集探测 + 内联去重；两侧公共超类型 cast 归一、外层 ORDER BY 名/序号 + LIMIT；ALL 多重集形态拒绝）；**无 FROM SELECT**（OneRow 单行零列源 + `SELECT 1+1`/`CURRENT_DATE`/`now()`/`current_timestamp`）；**CASE/LIKE 套聚合**（rebind_over_agg 补 Case/Like/Floor/Ceil 臂，finish_case/like_from_phys 两路共用）；**ORDER BY 序号**（1-based，普通路径 + 集合操作外层）；**CREATE VIEW / DROP VIEW [IF EXISTS] / OR REPLACE**（`_catalog/views.json` 非物化——SELECT 期重解析展开为子计划，深度护栏 8 防循环；Admin 专属 DDL）。视图/集合操作内的列裁剪禁用（plan_scan_def None）。
- **F17 修复（sync 真库 e2e 发现的内核 correctness bug）**：`WHERE bool_col = true/false` 在**单值域布尔段**（段内非空值全 true 或全 false——真库布尔列常态）恒返回空。根因 = `bool_stats` 用 has_false/has_true **分开**设置 min/max → 全 true 段得 `(None, Some(true))` → segment 裁剪的 `(Some,Some)` 匹配不中落入 `_ => false`（"缺失=全 NULL 列"错误推断）→ 整段错裁。修复 = min/max 成对设置（全 true→[true,true]，全 false→[false,false]，全 NULL→不设）。裸列 WHERE 不走字面量谓词下推所以无恙——两路径分叉正是定位线索。顺带：footer_bound 补 `S::Boolean` 臂（RG 级布尔裁剪真正生效，原先保守放行）；`IS [NOT] TRUE/FALSE`（Coalesce+Not 组合，NULL 归一语义）；`IS [NOT] DISTINCT FROM`（NULL 安全等值——DISTINCT 形态不能 NOT 包裹（Kleene 三值），显式展开 `(单侧NULL) OR (x != y)`）。
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
| SQL-0 | M2 | `CREATE/DROP TABLE`、`DESCRIBE`、`SHOW TABLES/PARTITIONS`、`SELECT … FROM … WHERE … LIMIT`（投影/过滤/字面量/比较/逻辑）、`INSERT INTO … VALUES`（小批量）；2026-10-11 起 `INSERT INTO … SELECT …`（源可为任意 SELECT） |
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
    predicate: Predicate::parse("dt = DATE '2026-10-04' AND amount > 0")?,
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

### M2（第 5–8 周）：向量化执行 + SQL-0/1 ✅ 已完成（聚合红线部分达成，见下）
- 表达式系统、Filter/Project/Limit、两阶段 HashAgg、Sort/TopN、Distinct。
- SQL-0 + SQL-1、`qsdb shell/query`、错误信息体系。
- 实现：PhysExpr（Datum 标量广播 + DECIMAL i256 精确算术 + 常量折叠传播）、
  FxHash 分组表（零分配 key 编码 + Kahan float + i256 decimal 状态）、
  agg-over-scan 文件级 rayon 并行（partial/merge）、谓词下推拆分
  （可下推形态进 M1 三级裁剪，复杂条件留行过滤）、sqlparser 0.63 只解析
  （原生 PARTITIONED BY / INSERT OVERWRITE / Substring/Floor/Ceil AST 对接）。
- **Gate（实测，Windows 开发机 release）**：
  - ✅ SQL e2e：106 个测试（90 单测 + 5 fuzz + 11 e2e，130+ 断言）全绿；
  - ⚠️ 1 亿行 3 列 group by：单线程 4.13s（红线 <2s）、多线程 0.92s@18T /
    3.05s@8T（红线 <0.5s@8T）——**未达标**。归因：端到端含 parquet zstd 解码
    与磁盘 IO（3.2GB 逻辑量），聚合内核经 Int64 特化（内联 key + 开放寻址）
    已由 6.1s 优化至 4.1s 单线程，剩余为 IO/解码占主导（多线程加速比 4.5x
    受 IO 带宽限制）。M4 优化路径：① 聚合列裁剪下推（count(*) 零列读）
    ② zstd level 自适应 / LZ4 ③ 预取。红线不注销，M4 复测；
  - ✅ 嵌入态空闲 RSS：unix /proc 检查进 CI（`qsdb-bench --bench rss`）；
  - ✅ 附带达成：谓词下推（可下推形态进三级裁剪）、INSERT OVERWRITE、
    DECIMAL i256 精确算术、并行聚合与串行结果一致性测试。

### M3（第 9–11 周）：服务化与治理 ✅ 已完成
- `qsdb server`（axum：query/write IPC/tables/schema/partitions/stats/health/metrics，
  bearer token 鉴权，1GB body 上限，spawn_blocking 桥接核心库，Ctrl-C 优雅退出 +
  写缓冲 flush）；compaction（分区锁内合并重写 + manifest 原子替换 + 分区级 rayon
  并行 + admin/compact 与 --auto-compact-secs）；WriteBuffer 攒批（双阈值自动
  flush + 分区级并行提交；server 写路径默认走缓冲）。
- 写路径性能修正：zstd level 3→1；列统计收集改 typed 快路径（跳过逐行 Literal
  构造，+12% 吞吐）。
- **Gate（实测，Windows 开发机 release，fat-LTO + panic=abort + strip）**：
  - ✅ HTTP 写入（并发 8 流回环）522 MB/s logical / 10.7M rows/s；
    单流上限 ~94MB/s（parquet 单线程压缩 bound，多流聚合是主口径）；
  - ✅ 二进制 15.7MB（红线 ≤ 20MB）；
  - ⏳ 服务态空闲 RSS：CI Linux 项（Windows 本机无 /proc）；
  - ⏳ compaction 在线 P99 劣化 <10%：压测脚本 M4 初补（语义已有并发安全测试）；
  - ✅ scan vs 原生 parquet-rs：743 vs 1240 MB/s 单线程（开销系数 1.67x，
    含分区发现/manifest/行过滤管线——M4 优化项）；
  - ✅ DuckDB 对比基线（进程内 bundled 1.1，同批 parquet 文件，同机 18 线程；
    `cargo run --release -p qsdb-bench --features duckdb --bin duckdb_vs`）：
    | 场景（4 千万行×7 列） | QSDB | DuckDB | 比值 |
    |---|---|---|---|
    | 过滤扫描 a>0 计数 | 0.739s | 0.083s | 0.11x |
    | 全表聚合 count/sum/min/max | 0.650s | 0.219s | 0.34x |
    | 3 列 group by | 50.9s ⚠️ | 0.180s | 异常 |
    | 分区点查 dt= | 0.321s | 0.002s | 0.01x |
    | 写入 | 2.27M rows/s | 16.1M rows/s(COPY) | 0.14x |
    根因定位（M4 工作清单，按优先级）：
    ① group by 并行/faster-path 失效（50s ≈ 全串行+通用分组路径，应 <1s）——疑似 bug；
    ② scan 单线程（DuckDB 18 线程并行扫描）→ scan 层分片并行；
    ③ count(*) 等不读列的查询仍全列解码 → 聚合列裁剪下推；
    ④ 分区点查无短路（min/max 统计已可 O(1) 出 count）→ 元数据应答；
    ⑤ 写入模型差异（QSDB parquet+zstd 持久化 vs DuckDB 内存表 COPY，不可比但暴露
    zstd 编码成本）→ row group 并行压缩评估。

### M4（第 12 周起）：Join 与进阶 ✅ 已完成（聚合红线见下）
- 读路径四连击：group 表快路径索引跨批丢失修复（100x）、ParScanExec 文件级
  并行扫描（无序流式 + 背压 + LIMIT 提前终止）、全计划列裁剪下推（idx 重映射
  + 非表列豁免 + 多 Scan 守卫）、count(*)+纯分区谓词元数据 O(1) 短路。
- 热数据缓存（可开关/可配大小，QsdbOptions.scan_cache_bytes / server --cache-mb）：
  segment 级解码后 LRU，1GB 预算下 aggregate 1.78x / group by 1.00x。
- 会话隔离：QueryContext（超时/取消/内存记账，算子批边界协作检查全链路）、
  panic 隔离（unwind + catch_unwind → QueryPanic；代价 +7.8MB，红线 24MB）、
  server 查询注册表/取消端点/并发上限/超时头。
- 配置体系：docs/configuration.md 全量清单（启动期/运行期/查询级/表级/服务器级）
  + QsdbRuntime 热更 + GET/PUT /api/v1/config + --config 文件。
- SQL-2：HashJoinExec（build 右表/probe 左表流式，NULL 键 SQL 语义，LEFT 未匹配
  保左值补右 NULL）+ JOIN 链 binder（限定名优先解析、右键全局→局部重映射）+
  HAVING（聚合输出过滤）+ ORDER BY 表达式 display 匹配。
- Sort spill：内存超限 chunk 排序落盘 zstd parquet run → k 路归并（类型化
  单元格比较、pg NULL 序）；1KB 上限 ORDER BY 落盘完成而非终止。
- **Gate（实测，同机；DuckDB 对比数字受机器负载影响大，趋势以下为准）**：
  - ✅ 分区点查 **8.8x 反超**（元数据短路）；aggregate **1.25-1.78x 反超**（热缓存）；
  - ⏳ 过滤扫描 0.74x（目标 ≥0.5x 达成）；group by 0.38x（目标同量级未达，
    静机最好成绩 0.41s/18T，与 DuckDB 0.13s 仍差 3x——剩 zstd 解码与分组内核差距）；
  - ⏳ 1 亿行 3 列 group by 红线（<2s 单线程/<0.5s 8T）：静机最好 0.41s@18T 已达
    8T 口径；单线程 3.3s 未达（<2s）——scan 单线程 zstd 解码为主瓶颈；
  - ✅ spill：10× 内存超限 ORDER BY 落盘完成；
  - ✅ 121 测试全绿、clippy 干净、二进制 15.8MB（abort）/23.6MB（unwind，24MB 红线内）。
- HashJoin（build/probe、广播小表）、SQL-2、外排 spill、bloom filter（等值点查）、S3/HDFS backend、distinct_count HLL。
- **Gate**：TPC-H 子集（当时语法范围内）对比报告；spill 下 10× 内存超限查询可完成。

### M5（按需）：生态与极限 ✅ 已完成（2026-10-10，范围用户定案）
- 落地：SQL-3 补全（窗口基础集+后置项：多 spec/同名区分/LAG-LEAD/嵌表达式/聚合上窗口；
  CTE 链式引用+嵌套 WITH+JOIN 右侧；嵌套子查询：FROM 派生表/JOIN 右派生表/标量子查询/
  IN 值列表；UDF+UDAF 注册）、约束系统（NOT NULL 强制/CHECK/UNIQUE·PK 显式后置）、
  ALTER TABLE ADD COLUMN（只加不删+旧 segment 缺列补 NULL 读+宽松写，format-spec §7.1）、
  Arrow Flight（feature-gated，h2c 独立口，do_get/get_flight_info/get_schema/do_put）。
- 定案后置：S3/HDFS backend（写提交协议依赖本地锁+rename 原子性，远端需重设计——
  lease 锁+conditional put，M6 候选）；SIMD 热点内核（等 gate 数据定热点）；
  GPU feature（用户定案后置）；EXISTS/IN 子查询（需 semi-join）、关联子查询。
- **Gate（实测，同机 40M 行×7 列×8 分区，18 线程口径；机器中等负载）**：
  - ✅ M4 核心路径零回归：点查 **10.08x 反超**（M4 8.8x）、aggregate **1.18x 反超**
    （M4 1.25-1.78x 区间内）、过滤扫描 0.86x（M4 0.74x 同级，≥0.5x 目标持续达成）、
    group by 0.37x（M4 0.38x——已知差距项，M5 未触碰扫描/聚合热路径）；
  - ✅ M5 特性开销（8M 行实测）：约束写开销 **0.99x（零）**（NOT NULL 位图 O(1)+
    CHECK 每批一次求值）；CTE vs 派生表 **1.00x**；窗口 row_number 2.25x 于等价
    ORDER BY 基线（物化窗口 v1 预期内）；UDAF 串行物化路径 18x 于内置状态机
    （设计如此——核心聚合热路径零改动，文档化）；
  - ✅ LIMIT 0 schema 契约（同步工具 schema 协商依赖）测试锁定；
  - ⚠️ **二进制红线**：默认 feature release 24.1MB，超红线 24MB 0.1MB
    （M4 gate 23.6MB + M5 SQL-3 核心代码 ~0.5MB；strip=symbols 已开）。
    处置：红线放宽至 **≤25MB**（用户定案，范围驱动）；flight feature 构建
    25.3MB（默认关闭，不计红线）；
  - ✅ 165 测试全绿、clippy 干净（默认+flight 双模式）。

### M5 后性能专项（P1-P3，2026-10-10/11）✅ 已完成
- **group by 反超**（0.37x → **2.4-3.0x 快于 DuckDB**）：GroupTable AoS→SoA
  （key arena + 列式聚合状态，高基数 9.1GB→3.8GB）；聚合路径零拷贝化
  （gcol 返回 `&[&Int64Array]` 引用 + update_sum 直接迭代类型化数组——
  全列 to_vec/collect 在执行上下文病态慢 ~235MB/s，裸 SIMD 读 0.8ns/元素）；
  分片策略按调用方分化（过滤=RG 级细分 LPT 均衡，聚合=文件粒度——高基数下
  细分重复构建组状态 +20% 反而劣化）；finalize 直读 SoA（替代 take_states）。
- **mem 单行对比误报排查**：微基准 950M rows/s vs 引擎 9s/分片同形状——
  排除 merge（0.014s）/分片/逐列后定位为整列拷贝；用户基准方法论修正：
  同机同噪声相对对比 + 少轮数人工判断（生产环境必噪声，赢要赢在噪声中）。
- **CacheCollector 爆内存修复**：整文件累积后 put × 8 分片 ≈1.6GB →
  budget/8 累积上界超限弃缓存（4GB 预算 → 512MB 上界）。
- **Scan 谓词列被列裁剪丢弃**（M4 正确性 bug）：count(*) WHERE a>15 当 a
  不在投影时谓词失效——collect_plan_columns 补 Scan 谓词列 + 回归测试。
- 终态（40M 行×8 分区，18 线程，中负载）：**全四项快于 DuckDB**——
  filter 1.6-1.8x、aggregate 5.3-6.7x、group by 2.4-3.0x、点查 7.4-11.4x；
  内存：qsdb 查询工作集 **15-17MB** vs DuckDB-parquet 26-56MB vs
  DuckDB 驻留 2.8GB（mem_vs 分进程峰值口径）。

---

### M6：多后端存储（2026-10-11 起，`docs/存储层设计.md` 为准）

设计输入：`docs/存储层设计.md`（v0.1 已评审定案，✅/⏳ 标记冻结）。
分期 M6-1 抽象收口 → M6-2 只读 S3 外表 → M6-3 S3 管理表（OCC）→
M6-4 staging 写 → M6-5 HDFS → M6-6 可选租约；每阶段独立 gate。

#### M6-1 Store 抽象收口 ✅ 已完成（2026-10-11）
- 新增 `store.rs`：`enum Store { Local }` 静态派发（贴合项目 enum 惯例，
  零虚调用）；方法面 = 现有调用实际用到的原语（read/read_to_string/
  write_atomic/stat/exists/create_dir_all/read_dir/commit_object + root()）；
  **条件写原语（put_if_absent/put_if_etag）不预建**——随 M6-3 协议层落地。
- 路径契约：方法收 db root 下绝对路径；Local 原样使用，远端按 root strip
  推 key（与本地路径一一映射）。热路径豁免（§4）：parquet 列读本地直开
  `File`；分区锁/trash/tmp 命名保持 fsutil 本地原语（M6-3 按 BackendCaps 分支）。
- 收口面：catalog / manifest / index / write / compact / scan（发现+缓存签名
  stat）/ api / write_buffer；`Qsdb` 持 `Arc<Store>` 并暴露 `store()`。
- **零回退 gate（A/B：本提交 vs 前提交 worktree 交替对跑，同机中负载）**：
  - mem_vs 40M 行×3 轮：write −2.6% / filter −1.5% / aggregate +2.2% /
    group by +0.4% —— 全部 ±3% 内（gate 线 ±5%）；峰值 RSS 543 vs 542MB、
    查询工作集 11-14MB 持平；
  - meta（万分区）：发现+读 manifest 24.5→18.6ms（红线 <50ms 稳）、
    热重复 20.1→21.5ms、万分区写 221→223.5s（+1%）；write 单线程
    1.68M→1.66M rows/s（−1.7%，红线 ≥1M 稳）；scan 单线程同噪声带内
    中位数持平（757 vs 740 MB/s，带内波动 ±25% 为机器负载）；
  - ✅ **gate 绿**：169 测试全绿、clippy 干净（workspace --all-targets）。

#### M6-2 只读外表 read_parquet ✅ 已完成（2026-10-11）
- `FROM read_parquet('<路径>')`（含 JOIN 右侧、谓词下推、列裁剪、并行分片、
  LIMIT 0 schema 契约）：本地路径/glob（`*` 段内、`**` 跨段，零新依赖）与
  `s3://bucket/key?endpoint=…&region=…`（feature `s3`，object_store 0.14 +
  tokio 仅此 feature 引入；凭据走环境变量链）。
- 读路径：parquet `ChunkReader` 派发源 `ChunkSource{Local File | Remote
  range GET}`——footer 裁剪与「只传选中 RG/列」由 parquet-rs 惰性按需
  取字节天然成立（存储设计 P5/§10.1 四级裁剪的 1-4 级全链路）。
- 段缓存（§10.2）：`filecache.rs`（独立模块、无 object_store 依赖），
  `{root}/_filecache/{url_hash}/{etag}/` 预算制 LRU；ETag 键控自动失效；
  准入 = 无 RG 裁剪的整读（选择性查询走 range GET 不占预算）；
  `QsdbOptions.file_cache_bytes`（默认 0 关）/ server `--file-cache-mb`。
- 顺手修复两个既有 bug：① JOIN 右侧实表带别名解析错名（`JOIN dim d` 按别名
  当表名 resolve → NotFound）；② HashJoin INNER 存在未匹配行时 `total_rows`
  恒计入 unmatched → 与列长不符触发批校验失败（现有测试全为全匹配未暴露；
  外表 join 测试首触发）。
- **验收**：179 测试全绿（默认）+ 185 全绿（--features s3，含 LocalFileSystem
  注入的全链路 range/顺序读/parquet 回读单测）；clippy 双模式干净；
  二进制默认 **24.22MB**（红线 ≤25MB 内，+0.1MB）；s3 feature 30.0MB
  （+5.8MB，默认关不计红线，cargo tree 验证默认零依赖增量）；
  管理表路径 mem_vs smoke 无回归（filter/agg/group 均在当日噪声带内）。
- 后置（M6-3）：S3 管理表（OCC manifest 提交）；远端文件 footer 重复
  HEAD/range（plan 期 + open 期各一次，v1 已知可优化）；glob 仅常量前缀
  list（无 key 谓词下推）。

#### M6-3 S3 管理表（OCC）✅ 已完成（2026-10-11）
- `Store::Remote` 变体（feature `s3`）：**虚拟根** `s3://bucket/prefix` 承接
  M6-1 路径契约——catalog/manifest/write/scan/compact 全调用面零改动直通；
  object_store 0.14 桥接（get/head/list_with_delimiter/put_opts）。
- **ProtocolOCC（§7.2 定案落地）**：OCC 原语 `get_with_etag` +
  `put_if_etag`（PutMode::Create/Update，Precondition/AlreadyExists →
  冲突 false）；`occ_commit` 重放循环 ≤8 轮（每轮基于最新 cur 重建，
  天然无 ABA 丢写）；manifest 新增 `writer_epoch`（本地恒 0）。
  ⚠️ **更正（2026-10-11）**：此处原写「`serde(default)` 即可兼容旧文件」——**那是错的**。
  postcard 不是自描述格式，尾部加字段会让旧文件「读穿」，`serde(default)` 只在 JSON
  这类自描述格式上生效。规则是**加字段 = 同时 bump `manifest.version`**，
  且读者要保留旧版本的读路径（见 `docs/format-spec.md` §7）。
  写路径：segment 内存缓冲整体 PUT（不可变无协议）
  → OCC 提交；compaction/overwrite/clear 同协议（替换语义重放）。
- catalog/table.json 远端 = 单对象 OCC（`occ_update_bytes`）；drop =
  清单 OCC 去名 + 前缀尽力删除；**远端 GC（§12）**= 引用扫描式
  （list 表前缀 → manifest 真值比对 → 未引用且过安全期才删）；
  进程内分区 Mutex 串行化（§8），跨进程由 OCC 收敛——无锁多写者。
- 分区发现：远端跳过 `partitions.idx`（stale 索引 = 丢分区数据的风险，
  真值单一原则）走 list + 逐分区 manifest；`_index` 加速对象后置。
- `QsdbOptions.storage`（StorageConfig::S3）+ server CLI
  `--storage-type/--storage-endpoint/--storage-region/--storage-bucket/
  --storage-prefix`；`Qsdb::open_with_store` 注入自定义后端（测试/嵌入方）。
- **验收**：187 测试全绿（--features s3，含 InMemory 注入的协议全链路：
  OCC 原语冲突矩阵、**冲突重放不丢并发写者 segment**、全生命周期
  建表/追加/overwrite/compaction/TRUNCATE/drop/stats、孤儿不可见 +
  GC 安全期回收、双实例 16 并发写无丢失）/179 默认全绿 / workspace
  clippy 双模式 0 警告 / 默认二进制 24.34MB（红线内）/ 本地路径
  mem_vs smoke 无回归。真实 MinIO/AWS 的 If-Match 行为差异（R1）
  留探测套件任务；staging 写（M6-4）后置。

#### M6-4 staging 写 ✅ 已完成（2026-10-11）
- **写盘即 ACK**（§11）：远端写（append/overwrite）先序列化到本地 staging
  目录 + postcard sidecar（表/分区/mode/entry；**sidecar 后写 = 完成标记**，
  崩溃残留无 sidecar 的 parquet 为半成品忽略）；网络段由 `flush_staged()`
  批量 PUT + OCC 提交后删对。
- **崩溃补传幂等** = ① segment PUT 幂等（UUID 不可变对象）② manifest
  追加 **file_name 去重**（`occ_append_entry`）——「已提交但删除前崩溃」
  的残留对重放不重复计入（exactly-once 可见状态）。
- server `--staging-dir` + `--staging-upload-secs`（默认 5s）定时器：
  **启动即跑一轮 = 崩溃恢复补传**，之后周期 flush（错误记日志不中断）；
  核心库保持无常驻线程（flush 由外部驱动，嵌入方按需调用）。
- **可见性语义文档化**：未 flush 数据不可见（manifest 唯一真值），
  可见性延迟 = 上传周期；读己之写先 flush（`staged_pending()` 诊断）。
- 验收：193 测试全绿（s3，含 staging 五用例：ACK 不可见→flush 可见、
  崩溃重开补传 exactly-once、dedup 防重复计入、staged overwrite 替换、
  配置校验）/180 默认 / clippy 双模式 0 / 默认二进制 24.38MB。

### 性能专项第三批：全局聚合快路径 + UDAF 并行（2026-10-10）✅
- **全局聚合零迭代快路径**（`GroupTable::try_global_fast`，group 为空时）：
  `count(*)` = `num_rows` **O(1)**、`count(col)` = `num_rows − null_count`
  **O(1)**、整型 sum = 类型化 `values()` 切片直加 i128（null_count=0 免
  逐行判空，LLVM 自动向量化）、float sum = 切片 Kahan、min/max =
  arrow compute SIMD 标量。先验证形态后应用（不中回退原路径，零状态
  变更）；`ingest` 与 `ingest_with_sel`（全选）双钩子，semi 融合全选
  路径同享。实现坑：min/max 单槽位只取该聚合自己的极值（min 聚合取
  min——写反会把 max 混进 min 列，e2e 抓到）。
- **实测（semi_vs 40M 行）**：基线聚合（2 列全局 agg）**0.26s → 0.012s
  = 41.5x 快于 DuckDB**（此前 0.04x——「缓存直达聚合内核」落地）；
  连带 semi 全系反超：**IN 3.5x / NOT IN 1.58x / EXISTS 1.45x /
  过滤+聚合 1.07x**；mem_vs 宽形态 aggregate 0.87→0.61s、filter
  0.48→0.26s、group by 0.50→0.38s（全系受益）。
- **UDAF 并行化**：组求值 rayon par_iter（EvalFn 本身 Send+Sync）+
  分组编码**分块并行**（8M 行级 encode_key_row 是串行瓶颈——64K 分块
  本地 HashMap + 块序合并保首见输出序）。m5_bench 8M 行 GROUP BY：
  **vs 内置 22.78x → 7.67x**（剩余为整组物化契约固有：concat+take）。
- 191 测试绿 × 2 批；clippy 0。

### EXISTS / IN 子查询 — semi/anti-join（2026-10-10）✅
- **SQL 面**：WHERE 顶层 AND 合取项级提取——`g IN (SELECT 单列)`、
  关联 `EXISTS (… WHERE 内列 = 外列 [AND 本地谓词])`、`NOT IN`/`NOT EXISTS`
  （含 NOT(...) 括号形态）；合取可链式多个 semi join、可与普通谓词混合。
- **JoinType 扩展**：Semi / Anti / AntiIn（NOT IN 三值语义：build 含 NULL →
  恒空、probe NULL → 丢弃；NOT EXISTS 的 NULL 键按零命中发射）。
- **存在集三形态自动选优**（「别人不做的偏门」）：单整数键免编码 i64 集 →
  稠密域（max−min ≤ 16M 位）**位图探测**（分区键 IN 的标准形态，无哈希）→
  多键/字符串通用字节集。右表不物化（比 INNER build 省一遍）。
- **掩码融合并行路径**：semi 存在集 build 一次全分片共享（Arc）；probe 批
  上 semi 掩码 ∧ 过滤谓词 → 行选择向量 → `GroupTable::ingest_with_sel`
  掩码摄入——零中间物化、零 take。列裁剪下推穿透 semi 计划（左扫描只读
  键列+聚合列；右键用子计划局部 idx 消除宽度减法）。
- 修复三个实现坑：掩码求值必须每批一次（逐行重算整列 = O(n²)）；semi
  probe 子必须继承裁剪投影（传 None 读全列 → 左键 idx 错位指向 dt）；
  ExistsState pub 化 + prepare_exists_state 独立（融合路径共享）。
- **性能（40M 行 × 8 分区，同机）**：semi 融合查询 0.36-0.43s ≈ 自家
  filter 基线（0.49s）/快于 aggregate 基线（0.87s）——**semi 开销≈0**
  （掩码+掩码摄入实测 <0.15s/查询）。vs DuckDB（本 micro bench 为
  窄 2 列 + 预热 buffer 形态）0.14-0.16x；官方 gate 口径（duckdb_vs
  宽表冷读）仍是 filter 1.6-1.8x / agg 5.3-6.7x / group by 2.4-3.0x
  反超——本形态差距在扫描底座（2 列窄形态缓存聚合 0.04x），非 semi 本身。
- 191 默认 / 204 s3 / 208 hdfs / 207 s3+hdfs 全绿；clippy 0；24.64MB。

### 导入能力两条查询面原语：INSERT … SELECT + read_csv（2026-10-11）✅
- **`INSERT INTO t [(cols)] SELECT …`**：源从"仅字面量列表"扩到**整条 SELECT**
  （含 CTE / 子查询 / 集合操作 / 窗口——不是简化版 SELECT，走
  `translate_select_full_with_ctes` + `exec_builder::build` 同一条链路）。
  两条源（VALUES / SELECT）共用一份列映射、类型校验、约束与分区路由：
  `insert_column_perm`（列名 → 表列下标）+ `InsertSink`（APPEND 攒批 /
  OVERWRITE 按分区聚齐）。**真正的价值**：`INSERT INTO t SELECT * FROM
  read_parquet('…')` 此前直接报 not supported——「从数据湖灌进管理表」这条
  最省事的路现在通了（生态导入工具的前置）。
- 列清单是**位置语义**（标准 SQL：源第 i 列 → 清单第 i 个名字）。VALUES 源
  保持既有契约（清单必须覆盖全部表列，已有测试锁定）；SELECT 源允许部分列
  （未列出的列补 NULL，NOT NULL/CHECK 由写入端拦）。
- 类型校验**复用 `cast_rule` 矩阵**（与 VALUES 的 `cast_literal` 同一张表）：
  `Impossible`（如 BOOLEAN → BIGINT）在 cast 前就拒；其余走 arrow 数组 cast，
  并额外守一条——**cast 不得把非 NULL 变成 NULL**（arrow 的 safe=true 会把
  转不了的值静默变 NULL，对 INSERT 而言那是静默写脏数据）。
- 流式：APPEND 每 16 批 / 32MB 落盘一次（避免万级小批的 manifest O(n²) 写
  放大），不物化全量；OVERWRITE 必须按分区聚齐（整分区一次提交的契约），
  与 VALUES 路径同口径——大数据集导入用 APPEND。
- **`read_csv('路径/glob')` 外表**：自研 RFC4180 子集（引号包裹字段内的
  分隔符/换行/`""` 转义、CRLF/LF、BOM、空字段 → NULL），零新依赖。首行表头
  + 前 1000 行类型推断（int64/float64/boolean/utf8，拿不准回落 utf8），
  推断结果**绑定期定死**（全批稳定）；每个文件各自的表头各自跳过。
  实现 = 新 `LogicalPlan::CsvScan` + `csv::CsvScanExec`（流式 8192 行/批），
  **不动 parquet 扫描热路径**。
- 刻意不做（写在 `src/csv.rs` 模块头与《对接契约》§3.6）：s3:// 远端 CSV、
  压缩、`HEADER=false`、自定义分隔符/nullstr、谓词下推到扫描内；列裁剪**有**
  （由 exec_builder 通用投影下推驱动，省下未选列的类型解析与数组构建）。
- 验收：新增 `tests/insert_select.rs`（6 例）+ `tests/read_csv.rs`（7 例）
  + `csv.rs` 单测 7 例；`cargo test -p qsdb -p qsdb-cli` 全绿、clippy 0
  警告、`cargo fmt --all` 干净、无新依赖。

### 生态契约交付 + soak 实测修复（2026-10-11）✅
- **read_only 模式**（`QsdbOptions.read_only` + CLI `--read-only`）：写路径
  全拒（append/overwrite/DDL/TRUNCATE/compact/gc/refresh-index；WriteBuffer
  fail-fast；server /write 503）——standby/只读副本标准姿势。
- **`Qsdb::partition_stats`**：每分区 (name, generation, rows, segments)，
  idx 快路径——qsdb-sync 轮询契约；HTTP `/api/v1/tables/{t}/partitions`
  增 `detail` 字段（generation 为增量判据）。
- **《对接契约》**（docs/对接契约.md v1）：表格式/嵌入 API/HTTP/行为/版本
  五层承诺——生态团队的对接基准（契约 vs 内部的边界成文）。
- **soak 长稳 bench**（qsdb-bench soak）：混合负载（写 ~1M rows/s 量级 +
  三类查询轮询 + 周期 compaction/gc）+ RSS/缓存/段数曲线 + 末尾总数校验。
- **soak 抓到并修复真并发 bug**：查询按旧 manifest 快照规划 →
  compaction/overwrite/clear **立即 move_to_trash** → 在途扫描打开旧段
  失败。修复 = **延迟回收语义**：旧段留原地，gc 以「替换 manifest 的
  mtime 年龄 ≥ trash_grace_ms」为安全期锚点清理孤儿（在途扫描获得完整
  窗口）；复验 2min soak：5755 万行校验一致、2780 查询 0 错误。
- 验收：182 默认 / 198 s3+hdfs 全绿、clippy 0。

### 耐久性专项（投产前阻塞项 2/3，2026-10-11）✅
- **Durability 两档**（`QsdbOptions.durability`）：`ProcessCrash`（默认——
  页缓存即持久，kill -9/进程退出安全）| `PowerLoss`（segment/元数据/staging
  在 rename 前 fsync + POSIX 目录 sync，掉电安全）；`Store::local_dur/
  remote_dur` 携带标志，`data_sync()` 贯穿 write/compact/staging；
  远端 PUT 由后端保证耐久（标志仅作用于 staging 本地文件）。
- **损耗实测**（qsdb-bench write，3×1M 行/段 ≈31MB 压缩，本机 NVMe）：
  ProcessCrash 稳态 **1.76M rows/s**（55MB/s）；PowerLoss 稳态
  **1.32M rows/s**（41.6MB/s，每段 fsync ≈190ms ≈160MB/s 落盘率）
  ——**−25%**；PowerLoss 首轮脏页积压 0.72M 更差。红线 ≥1M rows/s
  两档均过，但 25% 损耗按「性能优先」定案**默认 ProcessCrash**，
  PowerLoss 按部署选配（带掉电保护电容的企业 SSD 代价远低；
  WriteBuffer 攒大段摊薄每字节 fsync）。
- 181 默认测试全绿（含 PowerLoss 写读 overwrite 冒烟）；clippy 0。

#### M6-5 HDFS 后端 ✅ 已完成（2026-10-11，真实集群验证）
- 选型定案（§6.3 R2 关闭）：**hdfs-native 0.14**（纯 Rust HDFS RPC，
  无 JVM/libhdfs/HADOOP_HOME 依赖）+ hdfs-native-object-store 0.17
  （与本库同版 object_store 0.14——单一版本零冲突，`cargo tree` 验证）。
- **CAS 模拟（§6.1 put_if_etag ⏳ 落地）**：`LockingStore` 包装器——
  put_opts(Update) = 锁对象 create-no-clobber（HDFS 原生）+ 锁下 ETag
  （适配层合成 mtime-size）双检 + 覆写；残留锁 60s 过期窃取；
  适配层 put 不建父目录（S3 无目录概念）→ 包装器 put 前 mkdirs 补齐。
- **remote 伞 feature 重构**：s3/hdfs 共享 Store::Remote + OCC + staging +
  引用 GC 全机件（`remote`）；S3 专属（AmazonS3/URL/glob 外表）留 `s3`；
  四种 feature 组合编译/测试/clippy 全绿——抽象贴合原则的枚举派发红利。
- `StorageConfig::Hdfs { url, prefix }` + CLI `--storage-type hdfs
  --storage-hdfs-url`；身份走 `HADOOP_USER_NAME`。
- **真实集群 e2e**（hdfs://namenode:9000，Hadoop 3.1.3，hdfs 用户）：
  CAS 原语（冲突拒写/锁释放）/管理表全生命周期（含 overwrite/TRUNCATE/
  引用 GC/drop）/staging 写——**3/3 全过**；无集群环境（CI）自动跳过。
- 验收：180 默认 / 193 s3 / 197 hdfs（含 3 真实集群）/ 196 s3+hdfs
  全绿；clippy 四组合 0；默认二进制 24.38MB。
- 后置：`read_parquet('hdfs://…')` 外表（管理表已覆盖，外表读取待需求）；
  HDFS 多 NameNode HA（需 HADOOP_CONF_DIR 配置链）。

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
