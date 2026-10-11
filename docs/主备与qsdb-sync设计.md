# QSDB 主备支持与 qsdb-sync 设计（v0.1 草案）

> 版本：v0.1（2026-10-11，M6 后主备专项设计输入）
> 状态：**草案**——定案项标 ✅，待定案项标 ⏳，评审后冻结
> 上位文档：`QSDB设计开发文档.md`、`format-spec.md`（表格式冻结——本设计的地基）
> 定位遵循项目总纲：**同步/主备属生态项目（qsdb-sync 独立仓库），QSDB 内核
> 只保证接口开放与标准**（Arrow/Parquet/HTTP/Flight + 冻结表格式）

---

## 一、架构洞察：为什么 QSDB 的主备是"便宜"的

QSDB 的既有设计决策使语义级复制几乎不需要内核新机制：

| 已有事实 | 对主备的含义 |
|---|---|
| segment 不可变（UUID v7 命名，单次写入） | 复制 = 拷文件；无变更追踪、无行级 CDC |
| 分区单一 manifest = 唯一真值，`generation` 单调递增 | manifest 即**复制日志**：比对 generation 即增量；应用 manifest 即原子提交 |
| 多进程一致性协议（分区 flock + manifest 原子替换） | **备库上的 sync 工具就是"另一个写者"**，与 standby server 读者共存——无需停写 |
| TableDefCache 按 mtime 失效；分区发现每查询重扫 | 外部进程写入的数据对 server **即时可见**，无需通知通道 |
| 表格式冻结（format-spec）+ LIMIT 0 schema 契约 | sync 工具的稳定跨版本接口 |
| M6 后全后端同格式（local/S3/HDFS 同 key 布局） | 备库可以是**任意后端**（本地备到对象存储、异地容灾） |

**结论**：主备 = 「以 manifest/generation 为日志的语义级复制」。内核新增面
很小（§四），主体是生态工具 qsdb-sync。

---

## 二、方案选型

| 方案 | 结论 |
|---|---|
| A. 存储级复制（rsync 目录 / 块设备 / 快照） | ❌ 粗粒度：无语义（不知哪些段有效）、全量扫描、无增量边界；仅作冷备兜底 |
| B. **语义级复制（manifest/generation 驱动）** | ✅ 定案：增量精确（只拷新段）、幂等（UUID + generation 拒回退）、备库可在线服务读 |
| C. 内核内建 WAL 流复制（binlog/raft 类） | ❌ 拒绝：违背「表格式即真值」的减法设计——WAL 引入重放/截断/双真值一整类复杂度，且 manifest 已是日志 |

---

## 三、qsdb-sync 设计（独立项目）

### 3.1 命令形态

```text
qsdb-sync snapshot --src <endpoint> --dst <path-or-url> [--table T]...
    # 全量初始化备库/冷备份（可断点续传：segment 存在即跳过）

qsdb-sync follow --src <endpoint> --dst <...> [--interval 5s] [--tables T,...]
    # 持续追平 = 主备复制（RPO = interval）

qsdb-sync check --src --dst
    # pre-flight 差异报告（schema 协商 + generation lag 明细；此前的生态评估结论）

qsdb-sync reverse --src --dst        # 旧主恢复后的差异回放（§五 failover）
```

`<endpoint>`：`http://src:9430`（server API）或直接表格式路径/URL
（`/data/qsdb`、`s3://b/prefix`、`hdfs://nn:9000/p`——sync 依赖 qsdb 库，
**全后端 Store 直接复用**）。

### 3.2 复制协议（单分区一次同步）

```text
① 发现：src 分区清单（partitions.idx / list；HTTP 走 /api/v1/tables/{t}/partitions）
② 比对：dst.manifest.generation < src.manifest.generation → 待同步
③ 数据先行：拷 src 新增 segments（UUID 文件；dst 已存在即跳过 = 幂等/续传；
   孤儿段无害——不被引用即不可见）
④ 应用真值：拷 manifest（dst 侧走内核同款 tmp+rename / 远端条件写）→ 原子可见
⑤ DDL：table.json 与 _catalog 清单按「先表定义后数据」顺序同步；
   ALTER ADD COLUMN 前缀兼容规则内核已备（§7.1）
```

### 3.3 一致性保证

- **快照一致性（不停源）**：segments 全拷（含孤儿）→ manifests 最后拷 →
  备库任意时刻处于某个一致代际（manifest 引用的段必然已在）。
- **exactly-once**：segment UUID 幂等 + **generation 拒绝回退**
  （dst.gen > src.gen → 停机告警，防脑裂回灌，见 §五）。
- **不依赖时钟**：generation 单调，跨机无时钟假设。
- 指标：lag = Σ(src.gen − dst.gen) / 分区明细；prometheus 暴露。

### 3.4 源/目标矩阵

| src \ dst | 本地 | S3 | HDFS | parquet 湖导出 |
|---|---|---|---|---|
| 本地/server | ✅ | ✅ | ✅ | ✅（快照形态，供外部引擎） |
| S3/HDFS | ✅ | ✅ | ✅ | ✅ |

（同格式互拷 = 纯对象复制；湖导出 = 只导 manifest 引用段 + 统一 schema。）

---

## 四、内核接入点

### 4.1 已具备（零改动）✅

1. `PartitionManifest` / `SegmentEntry` / `list_partitions` 公开 API——sync 的读写面
2. 分区锁 + manifest 原子替换——sync 与 standby server 共存协议
3. TableDefCache mtime 失效——外部写入即时可见
4. `read_parquet` 外表 + LIMIT 0 契约——sync 校验/协商工具面
5. 五后端 `Store`（local/S3/HDFS + 段缓存）——sync 直接复用，零新 IO 层

### 4.2 需新增（小量；建议排期 S2，约 2-3 天）

1. **`QsdbOptions.read_only: bool`** ★主备投产硬前置
   - 写路径（append/overwrite/DDL/compact/gc）全部拒绝；server /write 端点 503
   - standby 误连读写模式的双主风险由此消除（配合 §五纪律）
2. **提交事件钩子 `QsdbOptions.on_commit: Option<Arc<dyn Fn(&CommitEvent)>>`**
   - `CommitEvent { table, partition_values, generation, new_segments: Vec<SegmentEntry>, kind: Append|Overwrite|Compact|Clear }`
   - write/compact/clear 提交点后同步触发（锁外）；`None` 零开销（Option 分支）
   - 价值：server 内嵌 follow（push 模式免轮询，RPO 从秒级到毫秒级）
3. （⏳ v2）**db 级租约 `_catalog/lease`**（心跳文件，写路径周期校验）
   - 防双主脑裂的内核强校验；v1 以运维纪律（单写者 + generation 对账）替代

---

## 五、failover（主备切换）

| 场景 | 流程 |
|---|---|
| 计划内切换 | 停源写入 → follow 追平（lag=0）→ dst 以读写模式重启（read_only off）→ 切流 |
| 计划外切换 | dst 直接提升（接受 RPO 内丢失 = 最近 interval 的写入）；旧主冻结 |
| 旧主恢复 | **不得直接回写**。`qsdb-sync reverse`：逐分区 generation 对账—— 旧主 gen ≤ 新主：该分区丢弃；旧主 gen > 新主：差异段重放到新主（罕见窗口：新主已承接新写 → 人工裁决分区级合并） |
| 脑裂防护 | v1：纪律（切流后旧主进程隔离）+ generation 回退检测告警；v2 ⏳：lease 内核强校验 |

---

## 六、分期

| 阶段 | 内容 | 内核改动 |
|---|---|---|
| S1 | qsdb-sync snapshot/follow/check（读两端表格式 + 目标侧走多写者协议） | **零**——备库"外部写入者"模式今天即可工作 |
| S2 | read_only + on_commit 钩子；server 内嵌 follow | 2 项小改（§4.2） |
| S3 ⏳ | lease 防脑裂；sync 并行/限速/加密链路；lag 告警体系 | 按需 |

---

## 七、风险与对策

| # | 风险 | 对策 |
|---|---|---|
| R1 | 万级分区轮询发现成本 | 复用 partitions.idx / 远端 list 前缀；on_commit push 模式（S2）根治 |
| R2 | DDL 与数据竞争 | 顺序规则：table.json 先于新数据 manifest；schema 前缀兼容使旧段可读 |
| R3 | 大段重复拷贝（断点续传中途） | UUID 幂等跳过；checksum 可选校验 ⏳ |
| R4 | 双主脑裂 | v1 纪律 + generation 回退告警；v2 lease |
| R5 | sync 与 compaction 竞争（段被合并） | compaction 后旧段进 trash/待 GC——manifest 已不引用，sync 只认 manifest，天然解耦 |
