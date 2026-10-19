# QSDB 配置参考

> 全量配置清单。按**生效层级**组织：启动期（重开句柄/重启生效）、运行期（热更，
> 新请求即时生效）、查询级（单请求覆盖）、表级（建表时定，随表持久化）、服务器级。
> 最后附运行时查看/修改通道。

---

## 一、启动期配置（`QsdbOptions`，改后需重开句柄）

| 配置 | 类型 / 默认 | 说明 |
|------|------------|------|
| `storage` | StorageConfig / Local | 存储后端（M6-3）：`Local`（默认）或 `S3 { endpoint, region, bucket, prefix }` 管理表（需 feature `s3`；`QsdbOptions::s3(...)` 构造）。S3 模式下 manifest 提交走 OCC 条件写、回收走引用扫描 GC（`trash_grace_ms` 即孤儿安全期）；凭据走环境变量链（`AWS_ACCESS_KEY_ID` 等）。测试/嵌入方可经 `Qsdb::open_with_store` 注入任意 object_store 后端。 |
| `read_only` | bool / false | **只读模式**：拒绝一切写路径（append/overwrite/DDL/compact/gc/refresh-index）。HTTP 面 → 503 **`READ_ONLY`**（**永不重试**：只读库对读完全可用，换实例才对；与并发闸的 503 是两个码）。standby/只读副本标准姿势——只读句柄永不做写者，杜绝误连双主。CLI：`--read-only`。 |
| `durability` | Durability / ProcessCrash | 耐久性档位（M6 后追加）：`ProcessCrash`（默认，页缓存即持久——kill -9 安全，掉电可能丢最近段）/ `PowerLoss`（segment/元数据/staging 文件 rename 前 fsync + POSIX 目录 sync，掉电安全）。**损耗实测：写吞吐约 -25%**（本机 NVMe，31MB/段每段 fsync ≈190ms；首轮脏页积压时更多）——按性能优先定案默认关；带电容/掉电保护的企业级 SSD 上 fsync 代价远低；WriteBuffer 攒大段可摊薄每字节 fsync 次数。CLI：`--durability process-crash\|power-loss`。 |
| `trash_grace_ms` | u64 / 10 分钟 | `_trash` 静默期：被换出的旧数据文件至少保留这么久才可被 `gc()` 清理。调小可加速空间回收，但会牺牲正在进行的旧扫描（跨进程长查询）。 |
| `scan_cache_bytes` | usize / **256MB** | 热数据缓存预算（segment 级**解码后** LRU）。**0 = 关闭**。预算应能装下热工作集且不超过机器可用内存（预算过大会触发换页反噬——实测 4GB 预算在被其他进程占内存的机器上反而变慢）。**默认取保守值守「极致低占用」定位**；宽投影要快的部署显式调大（CLI `--cache-mb`）。**权衡实测（2026-10-10，10M×4 列）**：单条累积上界是 `budget/8`（8 分片防爆内存系数），而单个分片片段约 32~64MB——**256MB（上界 32MB）恰好卡在片段大小边缘**：16 分片只进 11 个，付了缓存成本却几乎不命中（**0.182s，比关缓存 0.136s 还慢**）；**1GB（上界 128MB）** 全命中 → **0.011s（约 16x）**，代价是查询期 RSS 到 **1.2GB**。**根治方向是缓存粒度**（按批/RG 分块，使小预算也能部分命中），不是放大预算。**注意**：这是**缓存记账上限**，不是进程内存硬上限（在途 `Arc` 不计入，见内存模型一节）。 |
| `query_timeout_ms` | Option\<u64\> / None | 默认查询超时（毫秒）。**这是运行期同名配置的初值**——open 后可经 [`Qsdb::runtime()`] 热更，见下节。 |
| `query_memory_limit_bytes` | Option\<u64\> / None | 默认单查询内存上限（字节）；**None/0 = 不限**（库默认不限——嵌入方自控内存策略）。同为运行期配置的初值。**`qsdb server` 兜底 4GB**（`--query-memory-limit-mb`）：server 是唯一会叠加多查询的部署面，不给默认即「N 个查询各自无界」。只约束**物化算子**记账（join build / sort spill / window / udaf），流式扫描/聚合不记账——见内存模型一节。 |
| `memory_budget_bytes` | Option\<u64\> / None | **全局内存预算**（字节）：本句柄内**所有**查询共享的执行内存 + 解码缓存总上界；**None/0 = 不限**（库默认不限，嵌入方自控）。与上一行的分工：那个是「单查询配额」（400 `QUERY_MEMORY_LIMIT`），这个是「整个句柄的天花板」（超了是 503 `MEMORY_PRESSURE`，可退避重试）。压力下**先挤兑缓存**再拒查询。**只约束记账到的内存**（物化算子 + 解码缓存；流式在途批的残留上界见设计文档 §七）。`qsdb server` 兜底 4GB（`--memory-budget-mb`）。 |
| `spill_dir` | Option\<PathBuf\> / None | **溢出目录根**（排序/聚合/join 落盘用）；None = 系统临时目录。溢出量可以很大（5M 行排序就是几十上百 MB）——**生产建议与数据同盘**（`qsdb server` 默认 `<库根>/_spill`）。每次查询在其下开 `qsdb_spill_<uuid>/`，查询结束即删；`open` 时清理**超过静默期未动过**的残留子目录（崩溃遗留）。只读句柄不写库根（回落系统 temp）。CLI：`--spill-dir`。 |
| `storage`（HDFS 形态） | `StorageConfig::Hdfs { url, prefix }` | HDFS 管理表（M6-5，feature `hdfs`）：`url` 如 `hdfs://namenode:9000`；CAS 由锁对象模拟（`hdfs_backend`）；身份走 `HADOOP_USER_NAME` 环境变量。CLI：`--storage-type hdfs --storage-hdfs-url hdfs://host:9000`。 |
| `staging_dir` | Option\<PathBuf\> / None | 远端写 staging 目录（M6-4）：Some = 启用——远端写先落本地盘（**写盘即 ACK**，写延迟与网络解耦），`flush_staged()` 批量 PUT + OCC 提交。需 S3 后端。**可见性语义**：未 flush 的数据对查询不可见（manifest 唯一真值），可见性延迟 = 上传周期；读己之写先 flush。崩溃恢复：残留对下次 flush 幂等补传（segment PUT 幂等 + manifest file_name 去重）。 |
| `file_cache_bytes` | u64 / 0 | 远端外表（`read_parquet('s3://…')`）段文件本地磁盘缓存预算，布局 `{root}/_filecache/{url_hash}/{etag}/…`（ETag 键控，对象覆盖自动失效；预算内按 mtime LRU 驱逐）。**0 = 关闭（默认）**。仅整读（无 row group 裁剪命中）的对象入缓存；选择性查询走 range GET 不占预算。需编译 feature `s3`。 |

**服务器 CLI 等价参数**（`qsdb server`）：`--cache-mb`（即 `scan_cache_bytes/1MB`）、`--query-memory-limit-mb`（单查询内存闸，默认 4096=4GB）、`--max-concurrent-queries`（并发闸，**默认 0=不限**；多客户端部署的推荐值见 `多库与会话设计稿` §七）、`--file-cache-mb`（即 `file_cache_bytes/1MB`）、`--storage-type local|s3` + `--storage-endpoint/--storage-region/--storage-bucket/--storage-prefix`（即 `storage`；s3 需编译 feature）、`--staging-dir <目录>`（即 `staging_dir`，启用 M6-4 staging 写）+ `--staging-upload-secs`（后台上传周期，默认 5s；server 定时 flush_staged，启动即跑一轮=崩溃补传）。

**feature `hdfs`（默认关闭，M6-5）**：`cargo build -p qsdb-cli --features hdfs`。hdfs-native（纯 Rust HDFS RPC，无 JVM/libhdfs）+ object_store 适配层（与 feature `s3` 同版 object_store 0.14，单一版本）。S3/HDFS 共享 `remote` 机件（Store::Remote + OCC + staging + 引用 GC）；HDFS 的条件写由锁对象模拟（create-no-clobber 锁 + 锁下 ETag 双检 + 覆写；残留锁 60s 过期窃取）。真实集群验证：Hadoop 3.1.3 e2e 全过。

**feature `s3`（默认关闭，M6-2）**：编译开关 `cargo build -p qsdb-cli --features s3`（qsdb 核心为 `qsdb/s3`）。启用后 `read_parquet('s3://…')` 可查对象存储 parquet：URL 支持 `?endpoint=…&region=…` 查询参数（`http://` 端点自动 allow_http）与 glob 通配（`*` 段内 / `**` 跨段）；凭据走 object_store 环境变量链（`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`/`AWS_REGION` 等），不落配置文件明文。代价：二进制 +5.8MB（object_store/tokio/reqwest）——默认关闭时零依赖增量（`cargo tree` 验证）。

## 二、运行期配置（热更，无需重启；新查询/新请求即时生效）

经 `Qsdb::runtime()`（返回 `Arc<QsdbRuntime>`，直接调 setter）或 server 的
`PUT /api/v1/config`。**对进行中的查询不追溯**——只影响之后创建的查询上下文。

| 配置 | 类型 / 默认 | 运行时读法 | 说明 |
|------|------------|-----------|------|
| 默认查询超时 | u64 ms / 0（不超时） | `GET /api/v1/config` | 0 = 不限；单请求可用 `X-QSDB-Timeout` 头覆盖 |
| 默认查询内存上限 | u64 字节 / **server 启动置 4096MB**（嵌入库 None = 0 不限） | 同上 | 超限查询被终止（`QueryMemoryLimit`），不会 OOM 全库。server 的初值来自 `--query-memory-limit-mb`（见服务器级表） |

## 三、查询级配置（单请求覆盖，优先级最高）

| 配置 | 通道 | 说明 |
|------|------|------|
| 超时 | HTTP 头 `X-QSDB-Timeout: <ms>`；嵌入 API `Qsdb::new_query_context(Some(ms))` | 显式传值优先于运行期默认 |
| 取消 | `POST /api/v1/queries/{id}/cancel`；嵌入 API `ctx.cancel()` | 协作式：算子批边界生效（并行分片即刻可见） |
| 内存上限 | 嵌入 API `QueryContext::new(id, None, Some(bytes))` | 单查询独立限额 |
| **结果护栏** | HTTP body `"maxRows": N`（规范名；别名 `max_rows`）或头 `X-QSDB-Max-Rows: <n>` | **给出即接管本请求的护栏整体**（行数 = 此值，体量闸一并放开；`0` = 本请求不限）。body 优先于头，头优先于服务器值——**双向覆盖**（可放宽也可收紧）。仅可信客户端可用（查询面本就有 grants） |

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
| 热缓存预算 | `--cache-mb` / 256 | ❌（open 期定死） | 0 = 关闭；守恒低占用，宽投影可显式调大 |
| 单查询内存上限 | `--query-memory-limit-mb` / 4096 | ✅（`PUT /api/v1/config`） | 0 = 不限；超限该查询被终止（**防 OOM 双闸之二**） |
| 自动 compaction 周期 | `--auto-compact-secs` / 关 | ❌ | 周期遍历全表 compact |
| 溢出目录 | `--spill-dir` / **`<库根>/_spill`** | ❌（open 期定死） | 排序/聚合/join 落盘位置。默认与数据同盘；**不设就是系统临时目录** |
| 全局内存预算 | `--memory-budget-mb` / **4096** | ❌（open 期定死） | 全库执行内存 + 缓存总上界；0 = 不限。超预算 → 503 `MEMORY_PRESSURE`（**可退避重试**）。**这是「进程内存上界」的承诺落点** |
| 单查询内存上限 | `--query-memory-limit-mb` / **4096** | ✅（`PUT /api/v1/config`） | 单查询自己的配额；0 = 不限。超限 → 400 `QUERY_MEMORY_LIMIT`（改查询才有用） |
| 并发查询上限 | `--max-concurrent-queries` / **0（不限）** | ❌（当前版） | 0 = 不限；超限直接 503 `TOO_MANY_QUERIES`（不排队，可退避重试）。**只管条数、不承担内存职责**（内存归预算管）——先前默认 16 是因为"它是唯一的系统级内存上界"，预算落地后该理由不成立，且按条数限流会误伤轻查询 |
| **结果护栏·行数** | `--max-result-rows` / **0（关）** | ✅（`PUT /api/v1/config`，键 `maxResultRows`） | 单次查询结果行数超限 → 400 `RESULT_TOO_LARGE`，**绝不返回部分数据**。检查点在执行后、序列化前（不省计算，纯传输护栏；json/ipc/bulk/rows/csv 全格式一致）。默认关 ⇒ 升级零行为变更 |
| **结果护栏·体量** | `--max-result-mb` / **0（关）** | ✅（同上，键 `maxResultBytes`） | 结果体量上限，按 **Arrow 数组内存估算**——JSON 序列化后实际体积可达估算的 1.5~3×（转义/数字展开），要精确护栏用行数档。热更值单位是**字节**（CLI 是 MB） |
| 会话空闲回收 | `--session-idle-timeout-mins` / **0（关）** | ❌ | 超时会话按"踢人"路径清理（取消在跑查询 + 吊销 token，之后 401 `SESSION_REVOKED`）。默认关的理由：踢错活跃会话的代价大于泄漏代价（会话对象 ~百字节级，泄漏是审计噪音不是内存问题） |
| **多库挂载** | `--mount 名=路径`（可重复；带选项 `名=路径,budget-mb=2048,read-only`） | ❌ | **`default` 库固定来自 `--path`**（`--mount default=…` 显式拒绝）。名字（小写 `[a-z_][a-z0-9_]*`）进 SQL 限定符（`app.t`）、`USE`、`SHOW DATABASES` 与 grants 键。**各挂载独立**内存预算/解码缓存/写缓冲（缓存 `--cache-mb` 每挂载一份）；**并发闸是实例级的**（过载保护管整个进程）。跨库 JOIN/事务显式拒（501）。`--mount` 同路径双挂载拒绝 |
| 写缓冲阈值 | WriteBufferOptions（代码默认 64 批/128MB） | ❌ | 攒批自动 flush |
| compaction 触发 | CompactOptions（代码默认 ≥4 段且均值 <64MB） | ❌ | 见 `qsdb admin compact` |
| Flight 监听口 | `--flight-bind` / 关 | ❌ | Arrow Flight 数据面（h2c，独立于 HTTP）；需 `--features flight` 编译（默认关闭守二进制红线）。鉴权复用 `--token`（Bearer metadata）。do_get ticket=SQL；do_put 首帧 descriptor cmd=`table:<名>` |

## 五·五、用户体系（M7-A）与库级授权（M-C）

三角色（`admin | writer | reader`），**库级生效**（M-C 起同一用户在不同库可以不同角色）：
| 能力 | reader | writer | admin |
|---|---|---|---|
| SELECT / SHOW / DESCRIBE | ✓ | ✓ | ✓ |
| INSERT / POST /write | ✗ | ✓ | ✓ |
| DDL（CREATE/DROP/ALTER/TRUNCATE/DELETE） | ✗ | ✗ | ✓ |
| compact / gc / refresh-index / 用户管理 / `/config`（读与写都 admin——响应含挂载清单与启动期路径，非 admin 可从 `RESULT_TOO_LARGE` 等错误文案拿到运行期上限） | ✗ | ✗ | ✓ |
| `/sessions` 列表 | — | — | admin 全量；**非 admin 只见自己名下**的会话（十九批Ⅶ） |

**库级 grants（M-C，白名单制）**：
```jsonc
// _catalog/users.json 的用户条目（grants 字段存在时即取代 role 成为判定唯一依据）
{ "name": "op_app", "salt": "…", "hash": "…",
  "grants": { "app": "Reader", "sales": null, "*": "Writer" } }
```
- 键 = 库名或 `"*"`（通配）；值 `null` = **显式无权**；**键缺失 = 无权**（白名单制）
- 库级精确项优先于 `"*"`；无 `grants` 字段的旧条目 = `role` 管所有库（**逐位兼容**）
- 判定**每请求现查**（角色不缓存进会话）——`PUT grants` 返回即生效，用户无需重登
- 无授权（凭证有效但该库无 grant）→ **403 `FORBIDDEN`**，文案区分"未认证"（401）

**认证方式**（server）：
| 方式 | 形态 | 说明 |
|---|---|---|
| Bearer 用户 token | `Authorization: Bearer <token>` | `POST /api/v1/auth/login`（Basic）签发，进程内存态（重启重登）；`sess-` 前缀，绑定一个会话（见下） |
| Basic | `Authorization: Basic base64(user:pass)` | 每次请求校验（~5ms 密码哈希）；无会话 |
| `--token` 超级凭证 | `Authorization: Bearer <--token值>` | Admin 等效（运维后门，保留）；无会话 |

**会话**（十六批 M-A；`POST /api/v1/auth/login` 签发 token 时建立，进程内存态，重启即失效）：

- 会话对象：`id` / `user` / `role`（登录时快照，展示用）/ `client`（ip:port + UA）/
  `createdAtMs` / `lastActiveMs`（**每个认证请求刷新**）/ `defaultDb`（`USE` 的落点，
  初始 `default`）/ 累计统计（`queriesTotal`、`rowsWritten`）
- 端点：`GET /api/v1/sessions`（admin 列全量，含在跑查询）、
  `GET /api/v1/sessions/{id}`（admin 或本会话自查）、
  `DELETE /api/v1/sessions/{id}`（admin **踢人**：取消在跑查询 + 吊销 token，
  之后该 token 一律 401 `SESSION_REVOKED`）、
  `PUT /api/v1/sessions/current/database`（设默认库，库名按挂载白名单校验，
  未挂载 404）
- 空闲回收：`--session-idle-timeout-mins`（默认 0 = 永不回收，见服务器级表）
- 可观测：`GET /metrics` 的 `qsdb_sessions` gauge；在跑查询经
  `GET /api/v1/queries` 回显 `sessionId`
- Basic / 超级 token / 开放模式**不建会话**（不可被踢、无默认库可设）——会话
  只属于"登录"这个动作

**用户存储**：`_catalog/users.json`（进程外可直接读不可改——密码哈希 + catalog 锁内原子写）。

**SQL 管理**（Admin 专属）：`CREATE USER name WITH PASSWORD 'pw' [ROLE r]`、`DROP USER name [IF EXISTS]`、`ALTER USER name SET PASSWORD 'pw' / SET ROLE r`（= 写 `"*"` 槽位，库级精确项保留）、`SHOW USERS`（M-C 起第三列 `grants` 摘要，如 `app=reader,*=-`）。默认角色 = writer。

**grants 管理**（M-C，不做 SQL DDL）：
- `PUT /api/v1/users/{name}/grants`（admin 热改）：body `{"grants": {"app": "reader", "*": null}}`，**PUT = 全量替换**；角色名大小写不敏感，`null` = 显式无权；想全拒给 `{"*": null}`（空表 400）
- `qsdb admin user-add <名> [--password <密码>] [角色] [--grant 库=角色 …]`（可重复；`db=` = 显式无权；给了 `--grant` 不给角色 = 白名单制）
- `user export/import` 原样（**grants 随 JSON 走**，failover 无缝延伸）
- 登录响应回显 `grants` 摘要 + 默认库的当前角色（`null` = 无权）——客户端自查，不用撞 403 试探

**零配置零变化**：不建用户 + 不设 `--token` = 完全开放（现状兼容）；建了用户即要求认证。嵌入模式不受影响（宿主自控，`sql_with_role()` 可选）。

**用户面同步（F15，主备 failover 认证无缝）**：
- `qsdb admin user export` → JSON（含角色+密码哈希，**不含明文密码**）
- `qsdb admin user import [--file <p>] [--overwrite]`：stdin/文件导入；
  `--overwrite` = 整体替换（failover 标准姿势）；默认只补缺（merge）
- 嵌入 API：`UserStore::export_json / import_json`
- 部署文档：启用用户体系的库做主备时，failover 编排调用 export→import

**writer 角色 × schema drift（F14 回复）**：三角色矩阵是定案。需要
「源加列→sink 自动 ALTER」的管道用 **admin 凭证**；writer 凭证的管道
drift 时显式报错转人工 ALTER（最小权限原则）。此矩阵不改。

**密码哈希**：SHA-256(password ‖ 16B salt) 迭代 10,000 次（内网部署口径）。

## 六、内存模型（一个账本，三类租户）

设计见 [`内存预算与溢出设计稿`](内存预算与溢出设计稿)；**P0（账本合一）已落地 2026-10-11**。

**一个账本**：`MemoryPool`（每个 `Qsdb` 句柄一份，`Arc` 共享）持有
`budget` 与两个计数（`exec` 执行侧 / `cache` 缓存侧）。**准入只有一个入口**
`QueryContext::try_reserve` → RAII 的 `Reservation`（`Drop` 即归还，
「忘还」在类型上不可能）。

| 租户 | 谁 | 记账 | 压力下的行为 |
|---|---|---|---|
| **执行内存** | 显式物化算子：HashJoin build / CrossJoin / Sort / Window / HashAgg / Distinct / SetOp / JoinCount / semi-anti 存在集 / CTE 物化。**其中 Sort / HashAgg / Distinct / HashJoin(INNER·LEFT) 已能落盘**（内存要不到额度就溢出完成，见 [`内存预算与溢出设计稿`](内存预算与溢出设计稿) §四） | 按**分配点的实际保留量**增量预留（聚合按组表**每分片**实记——分片放大是真占用） | 有溢出能力的（**Sort / HashAgg / Distinct / HashJoin 的 INNER·LEFT**）→ 落盘完成；尚未有溢出的（Window / SetOp / semi-anti 存在集 / CTE 物化 / CrossJoin）→ `MEMORY_PRESSURE`（503 **可退避重试**） |
| **解码缓存** | `SegmentCache`（实例级共享 LRU） | 由缓存自己同步 `used` | **先被挤兑**（纯缓存，丢了大不了重算；只挤到够用，不一挤到底） |
| **写缓冲** | `WriteBuffer` | **不纳入预算**（自带上界 64 批 / 128MB） | 攒批阈值触发 flush |

**流式路径不记账**：扫描/过滤/聚合是拉式迭代（一批 8192 行用完即弃），不驻留——
故空闲进程 RSS 只有几 MB。只有「必须物化」的算子才吃账本。

### 四道闸（仅 server；嵌入模式由宿主自控）

| 闸 | 默认 | 超限行为 |
|---|------|---------|
| `--memory-budget-mb` | **4096**（4GB；0 = 不限） | **全库**执行内存 + 缓存的总上界。要不到额度 → 503 **`MEMORY_PRESSURE`**（**可退避重试**——多为他人占用）。这才是「进程内存上界」的落点 |
| `--query-memory-limit-mb` | **4096**（4GB；0 = 不限） | 单查询**自己的配额**超限 → 400 `QUERY_MEMORY_LIMIT`（**改查询才有用**，重试无用） |
| `--max-concurrent-queries` | **0**（不限） | 并发**条数**上限 → 503 `TOO_MANY_QUERIES`（可退避重试）。**它不再承担内存职责**（内存归预算管）；真需要过载保护/公平性时才显式给 |
| `--max-result-rows` / `--max-result-mb` | **0（关）** | **结果传输体量**上限（十七批 R1）→ 400 `RESULT_TOO_LARGE`（**绝不返回部分数据**；重试无用，加 LIMIT / 换流式格式 / `COPY` 导出）。与前三者正交：超时管计算时长、内存管执行、护栏管传输——一条查询可以算得快、省内存、但结果太大 |

**"内存不够"的两种码别混**（客户端动作相反）：`QUERY_MEMORY_LIMIT`（你的查询太大 →
改查询）/ `MEMORY_PRESSURE`（全库此刻吃紧 → 退避重试）。**三种 503 也别按状态码一刀切**。
生效优先级：**CLI 显式值 > `--config` 文件 > 兜底默认**。

> 错误码的含义、可重试性与 SQLState 映射见 [`error-codes.md`](error-codes.md)（权威码表）。
> 运行期观测：`GET /api/v1/config` 的 `memory` 节（budget/exec/cache/挤兑量）、
> `/metrics` 的 `qsdb_memory_used_bytes{pool=exec|cache}` 与
> `qsdb_queries_rejected_total{reason=concurrency|memory}`。

### 三条容易踩的认知（2026-10-10 实测）

1. **`scan_cache_bytes` 不是进程内存上限**。它是**缓存索引的记账上限**：已被 LRU 驱逐但仍在途被查询使用的批由 `Arc` 持有、不计入 `used`，故实际 RSS 可超预算（实测预算 1GB、峰值 1563MB）。
2. **驱动峰值的是池 ②，不是池 ①**。实测 `mem_vs`：filter 后 352MB → aggregate 后 1035MB（缓存接近填满）→ group by 后 1235MB → 峰值 1563MB。超出预算的部分全在物化算子的执行内存里。
3. **并发不会数据错乱，但会缓存抖动**。取出的 `Arc<Vec<RecordBatch>>` 是不可变快照，驱逐只摘 map 索引——正在用的查询完全不受影响（Arrow 数组本身只读，无原地修改路径）。但两个查询各需约半个预算的段时会互相驱逐，**两边命中率都归零、比关缓存还慢**（即 256MB 下 0.182s > 关缓存 0.136s 的成因）。缓解靠**缓存粒度**（按批/RG 分块），不是放大预算。

---

## 七、运行时查看 / 修改通道

```
GET  /api/v1/config                 # 当前运行期配置 + 启动期快照（**admin**）
PUT  /api/v1/config                # 热更运行期项（body: {"queryTimeoutMs": 60000}）
GET  /api/v1/queries                # 运行中查询（id/sql/elapsed/memory/cancelled）
POST /api/v1/queries/{id}/cancel    # 取消
GET  /api/v1/stats                  # 表数/存储字节/写缓冲状态
GET  /api/v1/tables / tables/{t}/schema / tables/{t}/partitions
GET  /api/v1/error-codes            # 错误码目录（静态、免鉴权）
```

**`PUT /api/v1/config` 的键形态**（2026-10-10 实测校正）：**camelCase 与
snake_case 都接受**——`queryTimeoutMs` / `queryMemoryLimitBytes`（与 GET 回显
同形，故「读-改-写」整轮可行）或 `query_timeout_ms` / `query_memory_limit_bytes`
（与 `--config` 文件同形）。两个键名同时出现 → **422**（重复字段，不猜）。
结果护栏两键同款（十七批 R1）：`maxResultRows` / `maxResultBytes` 或
`max_result_rows` / `max_result_bytes`。

- 应答 `applied` = **确实落了至少一个已知字段**：空 body 或键名拼错时为 `false`
  （此前恒返回 `true`，键名拼错会拿到"成功"的假信号——已修）
- 应答回显**生效后**的 `queryTimeoutMs` / `queryMemoryLimitBytes` /
  `maxResultRows` / `maxResultBytes`，客户端可直接比对

CLI 等价：`qsdb admin tables|partitions|compact|refresh-index|gc`。

---

### 配置演进规则（工程约定）

1. 新配置项必须先归入上表某一层级并在本文登记，才能进代码。
2. 「运行期热更」是能力承诺：热更项不允许在 open 后被缓存进长生命周期结构。
3. 改默认值需同步更新本文、`QsdbOptions::default()`、CI 中相关断言。

## `--writer-epoch`：写者身份代（qsdb-sync F25）

主备 / failover 场景下的**持久写者身份**，默认 `0`（不启用）。它与
`generation` 不是一回事：

| | `generation` | `writer_epoch` |
|---|---|---|
| 是什么 | **分区提交计数器** | **写者身份代** |
| 谁改 | 每次提交由内核 +1 | **只有运维动作**（接管时显式抬高） |
| 回答 | "这个分区被改过几次" | "这块数据属于哪一代写者" |
| 用途 | CAS（`expected_generation`）、同步增量 | `(epoch, generation)` 复合判据、防静默分歧 |

**为什么需要它**：`generation` 是计数器不是内容指纹，防脑裂只是**单向**保证
（`dst > src` 拒绝），而 `dst == src` 并不蕴含内容一致。脑裂后两边各自演进恰好
追平到同一个 generation 时，以 generation 为判据的同步会误判"已同步"而**静默分歧**。

**怎么用**：

```bash
# 主库：不配（默认 0）
qsdb server --path /data/db --addr 0.0.0.0:9430

# 备库接管（failover）：显式抬到比原主库大
qsdb server --path /data/db --addr 0.0.0.0:9430 --writer-epoch 1
```

两条保证：
1. **每次提交把 epoch 盖进 manifest**（`PartitionManifest::epoch`）——生态侧
   （qsdb-sync replica）据此用 `(epoch, generation)` 复合序判增量，撞值不再可能；
2. **内核自己也不许回退写**：盘上 `epoch` 比本进程配置大 → 写被拒，
   返回 `STALE_WRITER`（409，`retry=no`）——防止"接管后老进程又连回来"。

**覆盖范围（F26：这里曾经写"四条路径"，错了两条）**。凡是产生一次分区提交的
路径都**既校验又盖章**，一条不能少——漏盖章比漏校验更重：新 manifest 由
`PartitionManifest::new()` 建出、epoch 初值 0，一次放行的旧代写就把盘上身份
**降级**成既成事实，此后老进程全线畅通。

| 写路径 | 校验 | 盖章 |
|---|---|---|
| 本地 append / overwrite | ✅ | ✅（`PartitionManifest::commit`） |
| 远端 append（含 staged append） | ✅ | ✅ |
| 远端 overwrite（含 staged overwrite） | ✅ | ✅ |
| 本地 / 远端 compaction | ✅ | ✅ |
| 本地 / 远端清空分区、**TRUNCATE**（含**无分区表**） | ✅ | ✅ |

回归钉在 `tests/writer_epoch.rs` + `tests/unpartitioned.rs` + `tests/remote_managed.rs`。

默认全 0 时两项都是 no-op（与旧版行为完全一致）；只有运维显式抬高过 epoch 之后，
忘配 / 配小的进程才会被拦下。

---

## 附录 A：调试与剖面开关（`QSDB_*` 环境变量全量收编，十六批 M-D）

**命名纪律**：`QSDB_NO_*` = 关掉一项已落地的优化（A/B 对照与回归锚点用）；
`QSDB_*_PROF` / `*_TIMING` = 剖面计时（stderr 输出）；`QSDB_*_CAP`/`QSDB_*_PARTS` =
探针/调参上限。**存在性即生效**（`is_ok()` 判定，值不重要），除非另行标注。

### A.1 性能 A/B 开关（**生产不要设**——关掉的都是已验收的默认优化）

| 开关 | 关掉什么 | 出处 |
|---|---|---|
| `QSDB_NO_FUSED_JOIN` | join 融合执行（回逐算子物化） | join 优化批 |
| `QSDB_NO_JOIN_DENSE` | join build 侧的稠密直索引（回哈希表路） | join 批；`join_group` 形态 |
| `QSDB_NO_DISTINCT_V2` | `SELECT DISTINCT` utf8 单列 v2 路（回 v1） | distinct v2 批 |
| `QSDB_NO_DISTINCT_FUSE` | Distinct+聚合融合（回两算子串行） | 融合批 |
| `QSDB_JOIN_NO_PROJ` | join build 侧键列投影（回整宽物化） | 三表 join 实测批 |
| `QSDB_NO_DENSE_GROUP` | 整型 group-by 密集直索引（回槽表路） | 十五批 |
| `QSDB_SCAN_SERIAL` | 扫描并行化（强制单线程；"并行改变行为"的 A/B 用） | par_scan |

### A.2 探针 / 调参（有语义的值；生产默认即甜点，别乱动）

| 开关 | 默认 | 语义 |
|---|---|---|
| `QSDB_SHARDS` | 4（**封顶 16**） | 聚合/去重并行分片数。实测：16 分片内存 ~3× 只快 ~20%（5M 行口径）；**默认 4 即甜点**。计划级并行 merge 未做前，更高分片主要吃内存 |
| `QSDB_DENSE_GROUP_CAP` | 8_000_000 槽（=32MB） | 整型密集直索引的值域跨度上限；超过即坠回哈希槽表。调大 = 更宽的值域走密集路（内存线性涨：4B/槽）；进程内测试可改（每次进批读 env） |
| `QSDB_DISTINCT_PARTS` | 按基数自适应 | Distinct 去重的强制分区数（测试注入用） |

### A.3 剖面 / 计时（**stderr 打点**，性能调查用；开着会拖慢并污染日志）

| 开关 | 输出内容 |
|---|---|
| `QSDB_JOIN_PROF` | join 各阶段计时与行数（build/probe/补轮） |
| `QSDB_AGG_PROF` | 聚合各阶段计时 |
| `QSDB_AGG_TIMING` | 聚合语句级分段计时 |
| `QSDB_WRITE_PROF` | 写路径分段计时（对齐/落段） |
| `QSDB_INSERT_PROF` | INSERT 分段计时（对齐/落段/批数） |
| `QSDB_DISTINCT_PROF` | 去重各阶段计时（含溢出） |
| `QSDB_OCC_PROF` | 远端 OCC 提交的重试/竞争采样（hdfs/s3 后端） |
| `QSDB_JOIN_SHARD_PROF` | join 分片级剖面 |
| `QSDB_PLAN_DUMP` | 计划形状 dump（曾抓出多表 join 退化叉积 113× 的形状错） |

---

## 附录 B：生产部署模板（十六批 M-D）

### B.1 单库生产基线

```bash
qsdb server --path /data/qsdb   --addr 0.0.0.0:9430   --memory-budget-mb 4096   --query-memory-limit-mb 4096   --max-concurrent-queries 0   --cache-mb 256
# --memory-budget-mb 4096        进程执行+缓存总闸；按机器内存 ~1/4 起
# --query-memory-limit-mb 4096   单查询配额；大聚合单条超限时才调大
# --max-concurrent-queries 0     默认不限；过载保护需要时显式给（见 B.3）
# --cache-mb 256                 热数据缓存；宽投影快查显式调大（代价见启动期表）
# --max-result-rows 1000000      可选：结果护栏（默认关）。面向 BI/看板的部署
#                                建议开（手滑 SELECT * 的闸）；批量取数客户端
#                                走 ipc/rows/bulk 流式格式或本请求 maxRows 覆盖，
#                                全量导出走 COPY（护栏不适用）
```

安全基线二选一：`--token <运维密钥>`（运维后门 + 最简）**或**建用户
（`CREATE USER … ROLE …` / `user-add`，库级 grants 见 §五·五）——
**建了用户即要求认证**；两者都配时用户 token 优先。

### B.2 多库多租户形（M-B/M-C）

```bash
qsdb server --path /data/qsdb-default   --mount app=/data/qsdb-app,budget-mb=2048   --mount sales=/data/qsdb-sales,read-only   --token <运维密钥>
# 库级授权：管理面走 PUT /api/v1/users/{name}/grants（热改即生效）或
#   qsdb admin user-add op_app --grant app=writer
# 客户端：SQL 限定名 app.t 直达（授权同判）；或登录后 USE app
```

内存规划：**每挂载独立预算**（默认 4096MB/挂载，`budget-mb=` 覆写）——
N 个挂载的进程上界 ≈ Σ(各挂载预算) + 各挂载缓存（`--cache-mb` 每挂载一份）。
并发闸与用户面是**实例级**的。

### B.3 容量三角与并发闸推荐值（实测出处：`多库访问控制与会话设计.md` §七）

| 维度 | 实测口径（16 核，5M 行基准表） |
|---|---|
| 轻查询（count/点查） | **~3400 qps @128 并发**，p95<50ms，RSS 315MB——不构成瓶颈 |
| 重聚合（10 万行结果集） | server 容量 **~16 qps**（每查询 4 分片 ×16 核即饱和）；之后**加并发只加延迟** |
| 远程结果传输 | 10 万行结果集远程口径 ~5 qps——**结果集大小是远程吞吐第一决定因素** |

**公式**：`并发闸推荐值 ≈ 期望重聚合并发数 + 轻查询余量`。纯 BI 看板/点查
形态可保持 0（不限）；混合负载建议 **16~32**（超限 503 `TOO_MANY_QUERIES`
可退避重试，客户端做指数退避）。内存三角：全局预算（B.1）× 单查询配额 ×
并发条数——预算是真正的进程上界，闸只管条数。

### B.4 会话与观测

- 会话空闲回收：批处理/长连接场景保持默认关；面向不可信客户端的部署给
  `--session-idle-timeout-mins 60`（超时按踢人路径清理）。
- 观测三件：`GET /metrics`（prometheus：请求/错误/延迟/内存/拒绝/会话数）、
  `GET /api/v1/sessions`（谁连着、在跑什么、多久没动）、
  `GET /api/v1/queries`（在跑查询明细，可 cancel）。
- 提交事件订阅（qsdb-sync 增量）：`GET /api/v1/watch`（事件带 `database` 字段；
  多挂载共用一个游标序列）。

### B.5 已知边界（v1 定案，部署前须知）

- **跨库 JOIN/事务不支持**（501 显式拒）——需要关联的数据放同一个挂载。
- COPY 湖导出、Flight 数据面固定走 default 挂载（限定名路由不覆盖这两面）。
- `/files` 物理复制源固定 default 挂载（多挂载复制按各库独立起 sync 任务）。
- 会话不持久化：重启即失效，客户端重登即可（批处理场景零成本）。
