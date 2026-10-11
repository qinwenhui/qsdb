# QSDB 配置参考

> 全量配置清单。按**生效层级**组织：启动期（重开句柄/重启生效）、运行期（热更，
> 新请求即时生效）、查询级（单请求覆盖）、表级（建表时定，随表持久化）、服务器级。
> 最后附运行时查看/修改通道。

---

## 一、启动期配置（`QsdbOptions`，改后需重开句柄）

| 配置 | 类型 / 默认 | 说明 |
|------|------------|------|
| `storage` | StorageConfig / Local | 存储后端（M6-3）：`Local`（默认）或 `S3 { endpoint, region, bucket, prefix }` 管理表（需 feature `s3`；`QsdbOptions::s3(...)` 构造）。S3 模式下 manifest 提交走 OCC 条件写、回收走引用扫描 GC（`trash_grace_ms` 即孤儿安全期）；凭据走环境变量链（`AWS_ACCESS_KEY_ID` 等）。测试/嵌入方可经 `Qsdb::open_with_store` 注入任意 object_store 后端。 |
| `read_only` | bool / false | **只读模式**：拒绝一切写路径（append/overwrite/DDL/compact/gc/refresh-index；server /write 返回 503）。standby/只读副本标准姿势——只读句柄永不做写者，杜绝误连双主。CLI：`--read-only`。 |
| `durability` | Durability / ProcessCrash | 耐久性档位（M6 后追加）：`ProcessCrash`（默认，页缓存即持久——kill -9 安全，掉电可能丢最近段）/ `PowerLoss`（segment/元数据/staging 文件 rename 前 fsync + POSIX 目录 sync，掉电安全）。**损耗实测：写吞吐约 -25%**（本机 NVMe，31MB/段每段 fsync ≈190ms；首轮脏页积压时更多）——按性能优先定案默认关；带电容/掉电保护的企业级 SSD 上 fsync 代价远低；WriteBuffer 攒大段可摊薄每字节 fsync 次数。CLI：`--durability process-crash\|power-loss`。 |
| `trash_grace_ms` | u64 / 10 分钟 | `_trash` 静默期：被换出的旧数据文件至少保留这么久才可被 `gc()` 清理。调小可加速空间回收，但会牺牲正在进行的旧扫描（跨进程长查询）。 |
| `scan_cache_bytes` | usize / 256MB | 热数据缓存预算（segment 级解码后 LRU）。**0 = 关闭**。预算应能装下热工作集且不超过机器可用内存（预算过大会触发换页反噬——实测 4GB 预算在被其他进程占内存的机器上反而变慢）。 |
| `query_timeout_ms` | Option\<u64\> / None | 默认查询超时（毫秒）。**这是运行期同名配置的初值**——open 后可经 [`Qsdb::runtime()`] 热更，见下节。 |
| `query_memory_limit_bytes` | Option\<u64\> / None | 默认单查询内存上限（字节）。同为运行期配置的初值。 |
| `storage`（HDFS 形态） | `StorageConfig::Hdfs { url, prefix }` | HDFS 管理表（M6-5，feature `hdfs`）：`url` 如 `hdfs://192.168.1.12:9000`；CAS 由锁对象模拟（`hdfs_backend`）；身份走 `HADOOP_USER_NAME` 环境变量。CLI：`--storage-type hdfs --storage-hdfs-url hdfs://host:9000`。 |
| `staging_dir` | Option\<PathBuf\> / None | 远端写 staging 目录（M6-4）：Some = 启用——远端写先落本地盘（**写盘即 ACK**，写延迟与网络解耦），`flush_staged()` 批量 PUT + OCC 提交。需 S3 后端。**可见性语义**：未 flush 的数据对查询不可见（manifest 唯一真值），可见性延迟 = 上传周期；读己之写先 flush。崩溃恢复：残留对下次 flush 幂等补传（segment PUT 幂等 + manifest file_name 去重）。 |
| `file_cache_bytes` | u64 / 0 | 远端外表（`read_parquet('s3://…')`）段文件本地磁盘缓存预算，布局 `{root}/_filecache/{url_hash}/{etag}/…`（ETag 键控，对象覆盖自动失效；预算内按 mtime LRU 驱逐）。**0 = 关闭（默认）**。仅整读（无 row group 裁剪命中）的对象入缓存；选择性查询走 range GET 不占预算。需编译 feature `s3`。 |

**服务器 CLI 等价参数**（`qsdb server`）：`--cache-mb`（即 `scan_cache_bytes/1MB`）、`--file-cache-mb`（即 `file_cache_bytes/1MB`）、`--storage-type local|s3` + `--storage-endpoint/--storage-region/--storage-bucket/--storage-prefix`（即 `storage`；s3 需编译 feature）、`--staging-dir <目录>`（即 `staging_dir`，启用 M6-4 staging 写）+ `--staging-upload-secs`（后台上传周期，默认 5s；server 定时 flush_staged，启动即跑一轮=崩溃补传）。

**feature `hdfs`（默认关闭，M6-5）**：`cargo build -p qsdb-cli --features hdfs`。hdfs-native（纯 Rust HDFS RPC，无 JVM/libhdfs）+ object_store 适配层（与 feature `s3` 同版 object_store 0.14，单一版本）。S3/HDFS 共享 `remote` 机件（Store::Remote + OCC + staging + 引用 GC）；HDFS 的条件写由锁对象模拟（create-no-clobber 锁 + 锁下 ETag 双检 + 覆写；残留锁 60s 过期窃取）。真实集群验证：Hadoop 3.1.3 e2e 全过。

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

## 五·五、用户体系（M7-A）

三角色（`admin | writer | reader`）：
| 能力 | reader | writer | admin |
|---|---|---|---|
| SELECT / SHOW / DESCRIBE | ✓ | ✓ | ✓ |
| INSERT / POST /write | ✗ | ✓ | ✓ |
| DDL（CREATE/DROP/ALTER/TRUNCATE/DELETE） | ✗ | ✗ | ✓ |
| compact / gc / refresh-index / 用户管理 / PUT /config | ✗ | ✗ | ✓ |

**认证方式**（server）：
| 方式 | 形态 | 说明 |
|---|---|---|
| Bearer 用户 token | `Authorization: Bearer <token>` | `POST /api/v1/auth/login`（Basic）签发，进程内存态（重启重登） |
| Basic | `Authorization: Basic base64(user:pass)` | 每次请求校验（~5ms 密码哈希） |
| `--token` 超级凭证 | `Authorization: Bearer <--token值>` | Admin 等效（运维后门，保留） |

**用户存储**：`_catalog/users.json`（进程外可直接读不可改——密码哈希 + catalog 锁内原子写）。

**SQL 管理**（Admin 专属）：`CREATE USER name WITH PASSWORD 'pw' [ROLE r]`、`DROP USER name [IF EXISTS]`、`ALTER USER name SET PASSWORD 'pw' / SET ROLE r`、`SHOW USERS`。默认角色 = writer。

**CLI**：`qsdb admin user add <名> <角色> [--password <密码>]`（未传则交互输入）、`user drop <名>`、`user passwd <名>`、`user list`。

**零配置零变化**：不建用户 + 不设 `--token` = 完全开放（现状兼容）；建了用户即要求认证。嵌入模式不受影响（宿主自控，`sql_with_role()` 可选）。

**密码哈希**：SHA-256(password ‖ 16B salt) 迭代 10,000 次（内网部署口径）。

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
