# QSDB 配置参考

> 全量配置清单。按**生效层级**组织：启动期（重开句柄/重启生效）、运行期（热更，
> 新请求即时生效）、查询级（单请求覆盖）、表级（建表时定，随表持久化）、服务器级。
> 最后附运行时查看/修改通道。

---

## 一、启动期配置（`QsdbOptions`，改后需重开句柄）

| 配置 | 类型 / 默认 | 说明 |
|------|------------|------|
| `storage` | StorageConfig / Local | 存储后端（M6-3）：`Local`（默认）或 `S3 { endpoint, region, bucket, prefix }` 管理表（需 feature `s3`；`QsdbOptions::s3(...)` 构造）。S3 模式下 manifest 提交走 OCC 条件写、回收走引用扫描 GC（`trash_grace_ms` 即孤儿安全期）；凭据走环境变量链（`AWS_ACCESS_KEY_ID` 等）。测试/嵌入方可经 `Qsdb::open_with_store` 注入任意 object_store 后端。 |
| `trash_grace_ms` | u64 / 10 分钟 | `_trash` 静默期：被换出的旧数据文件至少保留这么久才可被 `gc()` 清理。调小可加速空间回收，但会牺牲正在进行的旧扫描（跨进程长查询）。 |
| `scan_cache_bytes` | usize / 256MB | 热数据缓存预算（segment 级解码后 LRU）。**0 = 关闭**。预算应能装下热工作集且不超过机器可用内存（预算过大会触发换页反噬——实测 4GB 预算在被其他进程占内存的机器上反而变慢）。 |
| `query_timeout_ms` | Option\<u64\> / None | 默认查询超时（毫秒）。**这是运行期同名配置的初值**——open 后可经 [`Qsdb::runtime()`] 热更，见下节。 |
| `query_memory_limit_bytes` | Option\<u64\> / None | 默认单查询内存上限（字节）。同为运行期配置的初值。 |
| `file_cache_bytes` | u64 / 0 | 远端外表（`read_parquet('s3://…')`）段文件本地磁盘缓存预算，布局 `{root}/_filecache/{url_hash}/{etag}/…`（ETag 键控，对象覆盖自动失效；预算内按 mtime LRU 驱逐）。**0 = 关闭（默认）**。仅整读（无 row group 裁剪命中）的对象入缓存；选择性查询走 range GET 不占预算。需编译 feature `s3`。 |

**服务器 CLI 等价参数**（`qsdb server`）：`--cache-mb`（即 `scan_cache_bytes/1MB`）、`--file-cache-mb`（即 `file_cache_bytes/1MB`）、`--storage-type local|s3` + `--storage-endpoint/--storage-region/--storage-bucket/--storage-prefix`（即 `storage`；s3 需编译 feature）。

**feature `s3`（默认关闭，M6-2）**：编译开关 `cargo build -p qsdb-cli --features s3`（qsdb 核心为 `qsdb/s3`）。启用后 `read_parquet('s3://…')` 可查对象存储 parquet：URL 支持 `?endpoint=…&region=…` 查询参数（`http://` 端点自动 allow_http）与 glob 通配（`*` 段内 / `**` 跨段）；凭据走 object_store 环境变量链（`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`/`AWS_REGION` 等），不落配置文件明文。代价：二进制 +5.8MB（object_store/tokio/reqwest）——默认关闭时零依赖增量（`cargo tree` 验证）。

## 二、运行期配置（热更，无需重启；新查询/新请求即时生效）

经 `Qsdb::runtime()`（返回 `Arc<QsdbRuntime>`，直接调 setter）或 server 的
`PUT /api/v1/config`。**对进行中的查询不追溯**——只影响之后创建的查询上下文。

| 配置 | 类型 / 默认 | 运行时读法 | 说明 |
|------|------------|-----------|------|
| 默认查询超时 | u64 ms / 0（不超时） | `GET /api/v1/config` | 0 = 不限；单请求可用 `X-QSDB-Timeout` 头覆盖 |
| 默认查询内存上限 | u64 字节 / 0（不限） | 同上 | 超限查询被终止（`QueryMemoryLimit`），不会 OOM 全库 |

## 三、查询级配置（单请求覆盖，优先级最高）

| 配置 | 通道 | 说明 |
|------|------|------|
| 超时 | HTTP 头 `X-QSDB-Timeout: <ms>`；嵌入 API `Qsdb::new_query_context(Some(ms))` | 显式传值优先于运行期默认 |
| 取消 | `POST /api/v1/queries/{id}/cancel`；嵌入 API `ctx.cancel()` | 协作式：算子批边界生效（并行分片即刻可见） |
| 内存上限 | 嵌入 API `QueryContext::new(id, None, Some(bytes))` | 单查询独立限额 |

## 四、表级配置（建表时写入 `table.json` 的 `properties`，随表持久化）

| 配置 | 取值 / 默认 | 说明 |
|------|------------|------|
| `compression` | `zstd`（默认）/ `snappy` | segment 压缩算法。zstd(level 1) 压缩比好；snappy 解压更快、CPU 占用更低。 |

分区键、列类型、nullable 等也是表级（DDL 定义），不在此列。

## 五、服务器级配置（`qsdb server` 启动参数；部分热更）

| 配置 | 参数 / 默认 | 热更 | 说明 |
|------|------------|------|------|
| 数据目录 | `--path`（必填） | ❌ | 库根目录 |
| 监听地址 | `--addr` / 0.0.0.0:9430 | ❌ | 重启生效 |
| 鉴权 token | `--token` / 无 | ❌ | Bearer token；不设则开放（建议生产必设） |
| 热缓存预算 | `--cache-mb` / 256 | ❌（open 期定死） | 0 = 关闭 |
| 自动 compaction 周期 | `--auto-compact-secs` / 关 | ❌ | 周期遍历全表 compact |
| 并发查询上限 | `--max-concurrent-queries` / 0（不限） | ❌（当前版） | 超限返回 503 |
| 写缓冲阈值 | WriteBufferOptions（代码默认 64 批/128MB） | ❌ | 攒批自动 flush |
| compaction 触发 | CompactOptions（代码默认 ≥4 段且均值 <64MB） | ❌ | 见 `qsdb admin compact` |
| Flight 监听口 | `--flight-bind` / 关 | ❌ | Arrow Flight 数据面（h2c，独立于 HTTP）；需 `--features flight` 编译（默认关闭守二进制红线）。鉴权复用 `--token`（Bearer metadata）。do_get ticket=SQL；do_put 首帧 descriptor cmd=`table:<名>` |

## 六、运行时查看 / 修改通道

```
GET  /api/v1/config                 # 当前运行期配置 + 启动期快照（只读标注）
PUT  /api/v1/config                # 热更运行期项（body: {"queryTimeoutMs": 60000, ...}）
GET  /api/v1/queries                # 运行中查询（id/sql/elapsed/memory/cancelled）
POST /api/v1/queries/{id}/cancel    # 取消
GET  /api/v1/stats                  # 表数/存储字节/写缓冲状态
GET  /api/v1/tables / tables/{t}/schema / tables/{t}/partitions
```

CLI 等价：`qsdb admin tables|partitions|compact|refresh-index|gc`。

---

### 配置演进规则（工程约定）

1. 新配置项必须先归入上表某一层级并在本文登记，才能进代码。
2. 「运行期热更」是能力承诺：热更项不允许在 open 后被缓存进长生命周期结构。
3. 改默认值需同步更新本文、`QsdbOptions::default()`、CI 中相关断言。
