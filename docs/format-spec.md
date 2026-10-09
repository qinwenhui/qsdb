# QSDB 磁盘格式规范（v1 冻结）

> 状态：M0 冻结，M1 实现以此为准。任何字段变更必须 bump `version` 并写迁移。
> 上游设计：`QSDB设计开发文档.md` §6–7。

## 1. 总览

```
{qsdb_root}/
├── _catalog/
│   └── {db}.postcard              # db → 表名清单（默认 db：default）
├── {db}/{table}/
│   ├── table.json                 # 表定义（低频变更，人类可读）
│   ├── {p1}={v1}[/{p2}={v2}/…]/   # 分区目录（Hive 风格，可嵌套）
│   │   ├── manifest.postcard      # ★ 分区唯一提交点
│   │   ├── seg_{uuid7}.parquet    # 数据文件
│   │   └── .lock                  # 分区互斥锁（fs2）
│   └── _trash/{ts}/…              # 待清理文件（静默期后删除）
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
| `version` | u32 | 当前 = 1 |
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
| `version` | u32 | 当前 = 1 |
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
- overwrite：锁内写全新 segments 的新 manifest → rename；旧数据文件移入 `_trash/{table}/{ts}/`。
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
- 加字段只能加在 postcard 结构尾部且带默认值（serde `#[serde(default)]`）。
- `LogicalType` 枚举只允许新增 variant（旧数据可读）。
