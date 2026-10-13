# QSDB 错误码目录（权威）

> **这是契约级文档**：`code` 字段是**稳定标识**，外部工具/驱动据此做精确分支——
> 判断该不该重试、该报什么给用户、该映射成哪个 SQLState。错误**文案**（`error`）
> 可随版本变化，**消费方不得依赖**。
>
> 本文**已落地并被测试守护**：码表由代码侧 `enum ErrorCode` 单一真源产出，
> 本文件 §三 的标记区与之**逐字对账**（见 §8.1）。改码不改文档 = `cargo test` 变红。

---

## 一、错误体形态

HTTP 错误响应**恒为**：

```json
{ "error": "<人读文案，可能变>", "code": "<机器码，稳定>" }
```

**包含**框架层抽取失败（非法 JSON、body 字段类型/缺失、query string 缺参）——
一律同形 + `INVALID_ARGUMENT`，不返回纯文本。

**没有目录外的码**：`code` 只可能取 §三 表格里的值。此前存在的"状态派生兜底"
（把没分类的错误按状态码反推成 `BAD_REQUEST`/`UNAVAILABLE` 之类）已删除，
现在由编译器保证每个错误都必须落到某个码上。

Flight 通道不走 `code`，按 gRPC status 映射（见 §七）。

## 二、字段约定

| 字段 | 含义 |
|---|---|
| **Code** | `SCREAMING_SNAKE_CASE`，ASCII，**一次发布永不变更语义**；不用命名空间前缀，族别由「类别」列表达 |
| **HTTP** | 与该 code 绑定的状态码（**同一 code 恒定同一状态码**） |
| **Retry** | `yes` = 瞬时故障，退避重试有意义；`no` = 重试无用（需改请求/改配置/换实例/上报） |
| 类别 | `query` \| `request` \| `resource` \| `auth` \| `capacity` \| `storage` \| `internal`——给人看的归组 |
| SQLState | **建议**映射（JDBC 用）。最终映射归驱动所有，此处给的是语义最接近的标准类 |

**运行期可查**：`GET /api/v1/error-codes`（免鉴权，静态目录）返回 `{code, http,
retry, category, sqlState}` 全表——驱动冷启动即可自配置映射，不必硬编码码表。

## 三、权威码表

下表被单测解析（`crates/qsdb-cli/src/error_code.rs` 的
`catalog_matches_authoritative_doc`）：**前后标记行不可删**，
**Code / HTTP / Retry / 类别 / SQLState 五列必须与代码侧枚举逐字一致**；
「触发条件」「客户端动作」两列是给人读的文档，可自由改写。

> 为什么是单测不是 `tests/` 下的集成测试：集成测试够不到私有枚举（除非为测试
> 加一个 lib target），而这条不变式恰恰是"代码侧枚举 ↔ 文档"的对账。

<!-- BEGIN CODE TABLE -->
| Code | HTTP | Retry | 类别 | 触发条件 | 客户端动作 | SQLState |
|---|---|---|---|---|---|---|
| `QUERY_TIMEOUT` | 408 | no | query | 查询超时（`X-QSDB-Timeout-Ms` 头或运行期默认） | 上报超时；长查询请显式调大 | `57014` |
| `QUERY_CANCELLED` | 400 | no | query | 被 `/queries/{id}/cancel` 或客户端断连取消 | 视为用户主动取消，不重试 | `HY008` |
| `QUERY_MEMORY_LIMIT` | 400 | no | query | 单查询执行内存超限（物化算子记账） | 报错；调大 `--query-memory-limit-mb` 或改查询 | `53200` |
| `QUERY_PANIC` | 500 | no | query | 算子 panic，被隔离边界捕获（进程无恙） | **上报内核 bug**；该查询不可重试 | `XX000` |
| `TABLE_NOT_FOUND` | 400 | no | query | 查询端点：表不存在（SQL 语义错，**非** HTTP 404） | 表名/DB 名写错？ | `42P01` |
| `COLUMN_NOT_FOUND` | 400 | no | query | 查询端点：列不存在 | 列名写错？ | `42703` |
| `PARSE_ERROR` | 400 | no | query | SQL 解析失败（语法/字面量） | 修 SQL，不重试 | `42601` |
| `TYPE_ERROR` | 400 | no | query | 类型不匹配 / 溢出 / 精度超限 | 改 SQL 或列类型 | `42804` |
| `UNSUPPORTED` | 501 | no | query | 当前版本未开放的能力（如 `CROSS JOIN`） | 改写法；确需则提需求 | `0A000` |
| `ALREADY_EXISTS` | 409 | no | resource | DDL 对象已存在 | 加 `IF NOT EXISTS` 或改名 | `42P07` |
| `INVALID_ARGUMENT` | 400 | no | request | 参数非法：请求体形状错、值不在白名单、抽取失败 | 修请求（**拼错会显式报错，不静默兜底**） | `42000` |
| `INVALID_IPC` | 400 | no | request | `/write` 的 Arrow IPC 流非法（缺 schema / 截断 / 列不匹配） | 修客户端编码 | `08P01` |
| `NOT_FOUND` | 404 | no | resource | **资源端点**（`/files`、`/tables/{t}/schema` 等）的对象不存在 | 资源名写错？ | `42704` |
| `UNAUTHORIZED` | 401 | no | auth | 缺凭证 / 凭证无效 / token 过期 | 重新登录换 token | `28000` |
| `FORBIDDEN` | 403 | no | auth | 凭证有效但角色不足（reader 写、非 admin 改配置） | 换角色或申请授权 | `42501` |
| `TOO_MANY_QUERIES` | 503 | yes | capacity | 并发查询超上限（`--max-concurrent-queries`） | **退避重试**（此时段并发已满） | `53300` |
| `MEMORY_PRESSURE` | 503 | yes | capacity | **全库内存预算**此刻耗尽（`--memory-budget-mb`；多为**他人占用**） | **退避重试**（别的查询跑完就好）；持续则调大预算 | `53000` |
| `READ_ONLY` | 503 | no | capacity | 只读库（`--read-only`）拒绝写 | **永不重试**；换可写实例 | `25006` |
| `METADATA_ERROR` | 500 | no | storage | manifest / 表格式元数据损坏或不一致 | 上报；可能需要 `refresh-index` 或人工介入 | `XX000` |
| `IO_ERROR` | 500 | yes | storage | 磁盘 / 远端对象存储 I/O 失败（瞬时居多） | **退避重试**；持续失败则上报 | `58030` |
| `INTERNAL` | 500 | no | internal | 未归类的内部错误（内核 bug） | **上报**，附 `queryId` | `XX000` |
<!-- END CODE TABLE -->

## 四、几个容易混的语义

- **503 有三种，别按状态码一刀切**：`TOO_MANY_QUERIES`（并发条数满，**可退避重试**）、
  `MEMORY_PRESSURE`（全库内存预算被他人占用，**可退避重试**）、`READ_ONLY`
  （只读库，**永不重试**——换实例才对）。
- **"内存不够"有两种**：`QUERY_MEMORY_LIMIT`（400，**你的查询超了自己的配额**——
  改查询或调 `--query-memory-limit-mb`）与 `MEMORY_PRESSURE`（503 可重试，
  **全库预算此刻被占用**——退避重试即可）。混成一个码会让驱动错误地重试/不重试。
- **400 与 404 的分工**：**查询端点**的 SQL 语义错误（表/列不存在）→ **400**
  （`TABLE_NOT_FOUND`/`COLUMN_NOT_FOUND`），避免网关把 SQL 错判成路由错；
  **资源端点**的对象不存在 → **404**（`NOT_FOUND`），保住 HTTP 语义。
  查询端点上的**其它**对象不存在（外表文件 `read_parquet('x')`、视图、函数名写错）
  → `INVALID_ARGUMENT`（400）——它们是"请求指向了不存在的东西"，不是"表/列不存在"，
  硬套 `TABLE_NOT_FOUND` 反而会让驱动报错报错地方。
  这条判定靠内核错误自带的**结构化类别**（`QsdbError::NotFoundKind`），
  不嗅探文案——`default.t_column` 这种表名曾因含 "column" 被误判成列不存在。
- **401 与 403**：401 = 你是谁不知道（重登）；403 = 知道你是谁但不够格（重试无用）。
- **`QUERY_PANIC` 与 `INTERNAL`**：都是内核 bug、都上报，但前者**进程无恙、
  只这一条查询挂了**——驱动可据此提示"该查询异常终止"而非"服务不可用"。
- **超时与取消的 SQLState 建议刻意不同**：`QUERY_TIMEOUT` → `57014`（Postgres 惯例），
  `QUERY_CANCELLED` → **`HY008`**（JDBC 标准 "operation canceled"）。
  早先两者都给 `57014`，qsdb-jdbc 反馈这在监控告警里必须分开算（"超时"是容量/性能
  问题，"取消"是用户/编排主动行为），驱动侧已改用 `HY008`——**内核的建议值随之对齐**，
  免得每个驱动各自去纠一次。语义上 57014 本就是"查询被终止"的合集，单独表示超时更准。

## 五、客户端动作矩阵

| Retry | 码 | 建议动作 |
|---|---|---|
| **yes** | `TOO_MANY_QUERIES`、`IO_ERROR` | 指数退避重试（带 jitter，上限几次）；持续失败则上报 |
| **no** | 其余全部 | 不重试。其中 `INVALID_ARGUMENT`/`PARSE_ERROR`/`TYPE_ERROR`/`*_NOT_FOUND`/`INVALID_IPC` 是**调用方的问题**（改请求），`UNAUTHORIZED` 重新认证，`FORBIDDEN`/`READ_ONLY` 换身份或实例，`QUERY_*` 按提示调整资源/查询，`INTERNAL`/`QUERY_PANIC`/`METADATA_ERROR` 上报 |

## 六、稳定性与演进政策

1. **只增不改**：已发布的 code 语义与 HTTP 状态码**永久冻结**；不允许复用/改义。
2. **新增**走 minor 版本；**改状态码/删码**属破坏性变更，须在
   [`对接契约.md`](对接契约.md) 的版本承诺一节登记并提前通知消费方。
3. **弃用**：某码不再产生时，在本文标注「已弃用（自 X 版本）」并保留条目——
   消费方的分支不能因为内核删了个码就崩。
4. **文案**（`error` 字段）任何时候都可能变，**且不做兼容承诺**。

## 七、非 HTTP 面

### 7.1 Flight（gRPC）

无 `code` 字段，按 gRPC status 映射。**映射不丢掉"可重试性"**：

| Code | gRPC status |
|---|---|
| `QUERY_TIMEOUT` | `DEADLINE_EXCEEDED` |
| `QUERY_CANCELLED` | `CANCELLED` |
| `TOO_MANY_QUERIES` | `RESOURCE_EXHAUSTED`（gRPC 客户端默认可重试） |
| `IO_ERROR` | `UNAVAILABLE`（同上） |
| `READ_ONLY` | `FAILED_PRECONDITION` |
| `UNAUTHORIZED` | `UNAUTHENTICATED` |
| `FORBIDDEN` | `PERMISSION_DENIED` |
| `UNSUPPORTED` | `UNIMPLEMENTED` |
| `NOT_FOUND` | `NOT_FOUND` |
| `ALREADY_EXISTS` | `ALREADY_EXISTS` |
| 其余（表/列不存在、解析、类型、参数、内存超限、panic、元数据、内部） | `INVALID_ARGUMENT` / `INTERNAL` |

**消息里附带码名**（`… [TABLE_NOT_FOUND]`）：gRPC 没有 `code` 字段，不带码名
调用方只能靠 status 猜——并发闸与只读库都是"资源类"失败，靠码才分得开。

### 7.2 嵌入 API（Rust）

直接返回 `QsdbError` 枚举（**类型化**，不是字符串码）。上层需要码时走
`ErrorCode::from_qsdb(e, query_ctx)`（`crates/qsdb-cli/src/error_code.rs`）——
这也是"码表只有一处定义"的落点。

两个机器可读字段是给上层映射用的，别去 match 文案：
`QsdbError::not_found_kind() -> Option<NotFoundKind>`、
`QsdbError::is_transient_io() -> bool`。

## 八、实现与演进

### 8.1 实现约定（防漂移）

1. **单一真源**：`crates/qsdb-cli/src/error_code.rs` 的 `enum ErrorCode`。
   码名、HTTP 状态、可重试性、类别、SQLState 全在这里，响应一律由
   `ErrorCode::response(msg)` 构造。
2. **禁止状态派生码**：`err_response(status, msg)` 与 `status_code_of()` 已删除。
   新增错误时不再有"顺手给个状态码"的出口——**编译器**保证每个错误都落到目录里的码。
3. **文档对账测试**：`catalog_matches_authoritative_doc`（同文件 `mod tests`）
   解析本文 §三 标记区，逐字比对**码集合 + HTTP + Retry + 类别 + SQLState**。
   加码/改状态码/改可重试性而没同步本文 → 测试红。
4. **映射只有一处**：`ErrorCode::from_qsdb(e, query_ctx)`。HTTP 与 Flight 都调它，
   Flight 再经 `ErrorCode::grpc()` 转 gRPC status。

### 8.2 落地记录（2026-10-12）

本节记录相对"方案提出前"改了什么，便于驱动侧对账：

| # | 此前 | 现在 | 对客户端的意义 |
|---|---|---|---|
| 1 | `QueryPanic` 落 `INTERNAL` | 独立 `QUERY_PANIC`（500） | 能区分"这一条查询挂了"与"服务挂了" |
| 2 | `Io` + Arrow（含其 `IoError`）全落 `INTERNAL` | 瞬时 I/O → `IO_ERROR`（500，**可重试**） | 磁盘/对象存储抖动不再被当成永久失败；其余 Arrow 编解码问题仍归 `INTERNAL` |
| 3 | `/write` 的 IPC 解码失败发**状态派生码** `BAD_REQUEST` | `INVALID_IPC`（400） | 拿到目录外的码的最后一个分支已消除 |
| 4 | 只读拒绝写 → `UNAVAILABLE` | `READ_ONLY`（503，**永不重试**） | 与并发闸的 503 分开；内核新增 `QsdbError::ReadOnly` 变体承载 |
| 5 | `COLUMN_NOT_FOUND` 靠 `msg.contains("column")` **嗅探文案** | 内核 `NotFound { kind, name }`（`NotFoundKind`） | 表名含 "column" 不再误判；查询端点上文件/视图/函数不存在归 `INVALID_ARGUMENT` |
| 6 | `status_code_of()` 的 `CONFLICT` 死分支 | 随函数一并删除 | 无行为变化（`ALREADY_EXISTS` 一直走显式码） |
| 7 | 码表只存在于契约文档一张表 | 本文为权威 + 契约文档摘要链接 + 单测对账 | 一处维护，两处不漂 |
| 8 | 运行期查不到目录 | 新增 `GET /api/v1/error-codes`（免鉴权） | 驱动可自配置映射，不必硬编码 |

**同批修正的相邻问题**（都不属于码表本身，但驱动会撞上）：

- 抽取器拒绝（非法 JSON / 缺字段 / 缺 Content-Type）此前回**纯文本**、无 `code`；
  现在一律 `{error, code}` + `INVALID_ARGUMENT`。**`422` 与 `415` 一并归一到 `400`**
  ——否则 `INVALID_ARGUMENT` 会同时对应三种状态码，破坏"一个码一个状态码"。
- Flight 面的查询/写入错误此前一律 `INVALID_ARGUMENT`（超时、内存超限、只读、
  内部错误全被压成"参数错"）；现在走同一张码表映射（见 §七）。
