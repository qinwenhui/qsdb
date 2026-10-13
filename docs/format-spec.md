# QSDB 磁盘格式规范（v1 冻结）

> 状态：M0 冻结，M1 实现以此为准。任何字段变更必须 bump `version` 并写迁移。
> 上游设计：`QSDB设计开发文档.md` §6–7。

## 1. 总览

```
{qsdb_root}/
├── _catalog/
│   └── {db}.postcard              # db → 表名清单（默认 db：default）
├── _trash/{ts}/…                  # 待清理文件（root 级；静默期后删除）
├── _filecache/…                   # 远端段缓存（工具可忽略，不可删除）
├── {db}/{table}/
│   ├── table.json                 # 表定义（低频变更，人类可读）
│   ├── partitions.idx             # 表级分区索引（工具加速结构，可重建）
│   ├── {p1}={v1}[/{p2}={v2}/…]/   # 分区目录（Hive 风格，可嵌套）
│   │   ├── manifest.postcard      # ★ 分区唯一提交点
│   │   ├── seg_{uuid7}.parquet    # 数据文件
│   │   ├── .lock                  # 分区互斥锁（fs2）
│   │   └── {seg}.{uuid}.tmp       # 写入中的临时文件（崩溃残留，GC 清）
│   └── _index.lock                # partitions.idx 的互斥锁（可重建）
```

- 隐藏文件与 `_` 开头目录不参与分区发现。
- 元数据序列化：postcard（serde）；`i128` 一律 16 字节大端（postcard 不支持 128 位整数）。

## 2. 分区目录名

- 形式 `k=v`，多级分区为嵌套目录：`dt=2026-10-09/region=cn`。
- **v 必须 percent-encoding**：`%`→`%25`、`/`→`%2F`、`=`→`%3D`、控制字符（<0x20、0x7F）→`%XX`；其余字符原样。
- k 为列名原样（已保证不含 `/`、`=`、`%`——DDL 层校验）。
- 解析：按第一个 `=` 切分，v 反转义。

## 3. table.json

| 字段 | 类型 | 说明 |
|------|------|------|
| `version` | u32 | 当前 = 1（**各有各的版本，别混用**：manifest 已是 2） |
| `name` | string | 表名（`[a-zA-Z_][a-zA-Z0-9_]*`，≤64 字符，大小写不敏感环境禁止仅大小写差异） |
| `columns` | `ColumnDef[]` | 顺序即列序 |
| `partition_keys` | `string[]` | 必须是 `columns` 的子集，顺序即目录层级 |
| `created_at` | i64 | epoch 毫秒 |
| `properties` | map<string,string> | 表属性（压缩算法、segment 目标大小等） |

`ColumnDef`：`{ name: string, logical_type: LogicalType(Serde), nullable: bool, default: Option<Literal>, comment: Option<string> }`

- `logical_type` 按 `qsdb::types::LogicalType` 的 serde 表示存储。
- 写入：`.tmp` + 原子 rename。变更仅允许加列（nullable 或带 default）。

## 4. manifest.postcard（分区提交点）

| 字段 | 类型 | 说明 |
|------|------|------|
| `version` | u32 | 当前 = **2**（v1 = 无 `epoch`；读者须同时支持 v1，见 §7） |
| `generation` | u64 | 每次提交 +1；读者用于缓存失效判断 |
| `partition_values` | `Vec<(string, Literal)>` | 与目录层级一致 |
| `segments` | `Vec<SegmentEntry>` | 该分区当前全部活跃 segment |
| `schema_fingerprint` | u64 | 写入时表 schema 的 FxHash；读取时不匹配 → 报 schema 漂移 |

`SegmentEntry`：

| 字段 | 类型 | 说明 |
|------|------|------|
| `file_name` | string | `seg_{uuid7}.parquet`，相对分区目录 |
| `row_count` | u64 | |
| `byte_size` | u64 | 压缩后文件大小 |
| `created_at` | i64 | epoch 毫秒 |
| `column_stats` | `Vec<(string, ColumnStats)>` | 每列，按表列序 |

`ColumnStats`：`{ min: Option<Literal>, max: Option<Literal>, null_count: u64 }`
（min/max 不含 NULL 与 NaN；全 NULL 列 min=max=None）

**提交语义**：
- append：锁内 `旧 manifest + 新 segment` → generation+1 → `.tmp` + rename。
- overwrite：锁内写全新 segments 的新 manifest → rename。**旧数据文件留原地**（延迟回收：
  按 manifest 快照规划的在途扫描在安全期内仍可打开；GC 按「替换 manifest 的 mtime
  年龄 ≥ 静默期」清理孤儿——qsdb-sync soak 反馈修正，2026-10-12）。
- clear/TRUNCATE：锁内空 manifest 替换，**generation 继承 +1**（单调递增是同步工具
  的增量判据契约，不回退——qsdb-sync F2 修正，2026-10-12）。
- manifest rename 即提交点；此前任何中断留下的是可清理的孤儿（`seg_*.parquet` 不被任何 manifest 引用即孤儿）。

## 5. Parquet 数据文件

| 参数 | 值 |
|------|-----|
| Row Group | 128MB 或 1M 行（先到为准） |
| Data Page | 1MB |
| 压缩 | zstd(3) 默认（表属性可改 snappy） |
| 统计 | 每列 min/max/null_count 全开（必开） |
| schema | 与 table.json 的 `arrow_type()` 映射一致；DECIMAL 一律 Decimal128 物理列 |

## 6. 锁与跨平台

- 分区锁：`{partition}/.lock`，fs2 独占写锁（Windows LockFileEx / POSIX flock）。
- 原子替换：Windows 用 `MoveFileEx(REPLACE_EXISTING)`；POSIX rename 原生。
- 删除已打开文件在 Windows 失败 → 一律先移 `_trash`，默认 10 分钟静默期后清理。
- 每次持锁后比对 manifest 的 mtime/generation，实现跨进程内存缓存失效。

## 7. 兼容性规则

- 读者遇 `version` 大于自身支持值 → 明确报错（不猜）。
  ⚠️ **"明确报错"要求版本能在反序列化之前被单独读出**：`version` 是结构体第一个字段，
  postcard 顺序编码 ⇒ 它恒是前导变长整数。实现见 `manifest::leading_version`——
  **否则版本不匹配只会表现为 `Hit the end of buffer`（被报成"数据损坏"）**，
  读者会去查磁盘/GC，方向全错（2026-10-14 实测踩过）。
- **加字段 = 必须同时 bump `version`**（本文件开头的规则），**且读者要保留对旧版本的读路径**。
  ⚠️ **`#[serde(default)]` 在 postcard 上不提供任何兼容性**——postcard **不是自描述格式**
  （按字段声明序编码，无字段名、无长度前缀），尾部新增字段后**旧文件读到末尾就"读穿了"**。
  `serde(default)` 只对 JSON 那类自描述格式生效。
  *（本文件 2026-10-13 之前的措辞是"加字段只能加在尾部且带默认值"——**那是错的**，
  而且已经咬过一次：`epoch` 落盘后，旧数据被读成 `corrupt manifest`。）*
  现行做法：`MANIFEST_VERSION` 常量 + 前导版本闸门 + 对 **v1（尾部缺 `epoch`）**
  补一个 `0x00`（`epoch = 0`，正是 v1 时代"未启用身份代"的语义）后重解；
  **补零只在版本号 == 1 时发生**——截断的 v2 文件版本号仍是 2，走不到这条路，如实报损坏。
- `LogicalType` 枚举只允许新增 variant（旧数据可读）。

### 7.1 schema 演进（M5-B2 起生效；追加式修订）

**只加不删**：`ALTER TABLE ... ADD COLUMN` 仅允许追加 nullable 列（尾部）。

- **table.json**：新列 `check` 字段 `#[serde(default)]`，旧文件缺字段可读。
- **schema 指纹**：`fingerprint(columns[..k], partition_keys)`；分区 manifest 指纹
  ∈ 当前表 `prefix_fingerprints` ⇔ 旧 schema 是当前 schema 前缀 → 兼容。
  追加写入时分区 manifest 指纹升格为当前值；非前缀差异（改类型/删列/换分区键）
  仍拒绝（须 overwrite）。
- **读**：segment 文件列集为表数据列前缀；缺列在批级补 NULL
  （`scan::pad_missing_columns`；谓词/装配前完成）。
- **写**：新 segment 携带全列 schema；**宽松写**——批缺尾列且缺列均 nullable
  时自动补 NULL（NOT NULL 缺列不可补，报错）。
- **compaction**：合并时旧批补 NULL，产物自然迁移到新 schema。
- 后置：ADD COLUMN NOT NULL（需 DEFAULT）、DROP COLUMN、类型变更。
