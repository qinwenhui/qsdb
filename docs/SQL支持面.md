# SQL 支持面（对外声明 · 可执行）

> 版本：v1（2026-10-13）——起因是一次**系统扫描**：绑定器是「逐字段读」的，
> AST 里**没读的字段默认被忽略**，于是"语法合法但没实现"的子句会**安静地按没写执行**。
> 这类 bug 不报错、不打日志，只能靠人撞见（`count(DISTINCT)` 那条就是手滑撞见的）。
>
> 本文 + `crates/qsdb/tests/sql_silent_drop.rs`（回归）一起构成声明：
> **每条语法要么正确执行、要么显式拒绝——没有"静默忽略"这一档。**
>
> 判据落在 `sql::translate` 的 `audit_query` / `audit_select` / `audit_function` /
> `audit_window_spec` 四个闸门里（新加能力时要么在绑定器里读那个字段，要么在闸门里拒绝）。
> 读者：驱动/SDK 作者（不用猜"这条能不能写"）、生态工具、写 SQL 的人。

**状态含义**：✅ 支持 · ✋ **显式拒绝**（给了话术清晰的报错）· ⛔ 解析期拒绝（sqlparser 不认这个语法）

---

## 一、`SELECT` 子句

| 语法 | 状态 | 说明 |
|---|---|---|
| `SELECT` / 投影 / 别名 / 表达式 | ✅ | |
| `SELECT DISTINCT` | ✅ | |
| `SELECT DISTINCT ON (…)` | ✋ | 语义是"每键取一行"，与 `DISTINCT` 不同；静默当 `DISTINCT` 会**多返回行** |
| `SELECT *` | ✅ | |
| `SELECT t.*`（限定通配） | ✋ | |
| `SELECT * EXCLUDE/EXCEPT/REPLACE/RENAME/ILIKE` | ✋ | |
| `WHERE` / `HAVING` / `GROUP BY` / `ORDER BY` | ✅ | |
| `GROUP BY` 位置序号（`GROUP BY 1`） | ✋ | **明确拒绝**：`GROUP BY 位置序号（GROUP BY 1）暂不支持 —— 请写列名`（2026-10-15 修，F29②：此前误报"列 "g" 必须出现在 GROUP BY 里"，把"没实现"说成"你写错了"）。`ORDER BY 1` 是 ✅ —— 这个不对称**已知**，实现在生态功能批次 |
| `GROUP BY ROLLUP/CUBE/GROUPING SETS` | ✋ | |
| `GROUP BY ALL` | ✋ | |
| `ORDER BY` 位置序号 / 输出别名 | ✅ | |
| `ORDER BY … NULLS FIRST/LAST` | ✅ | **默认放置**：`ASC ⇒ NULLS LAST`、`DESC ⇒ NULLS FIRST`（与 PG 一致）；显式值覆盖默认。此前被**静默忽略** ⇒ 顺序错 |
| `ORDER BY ALL` / `ORDER BY … USING` | ✋ / ⛔ | |
| `TOP n`（SQL Server 形） | ✋ | 用 `LIMIT` |
| `LIMIT n` | ✅ | |
| `OFFSET n`（含 `LIMIT n OFFSET m`、`LIMIT n, m`、`OFFSET n ROWS`） | ✅ | 此前**静默丢掉 offset** ⇒ 少翻页，2026-10-13 实现 |
| `FETCH FIRST n ROWS ONLY` | ✅ | 等价 `LIMIT n` |
| `FETCH FIRST … PERCENT ROWS` / `… WITH TIES` | ✋ | **语义与 LIMIT 不同**（按比例 / 含并列），故不实现即拒绝——绝不当 LIMIT 执行 |
| `QUALIFY` / `PREWHERE` / `CLUSTER BY` / `DISTRIBUTE BY` / `SORT BY` | ✋ | |
| `SELECT … INTO` | ✋ | |
| `FOR UPDATE` / `FOR SHARE` | ✋ | |
| `TABLESAMPLE` | ✋ | 此前**静默当全表** ⇒ 多返回 |
| `LATERAL VIEW` / `CONNECT BY` | ✋ | |
| `WINDOW w AS (…)`（命名窗口） | ✋ | |
| CTE 列别名 `WITH w(q) AS …` | ✋ | 此前**别名被丢掉**，之后报"列不存在"（误导） |
| `WITH w AS MATERIALIZED/NOT MATERIALIZED` | ⛔ | sqlparser 不认这个写法 |

## 二、`FROM` / JOIN

| 语法 | 状态 | 说明 |
|---|---|---|
| 单表 / 派生表 `/ CTE` 引用 / 表函数（`read_parquet` / `read_csv`） | ✅ | |
> 本节每条都由 `crates/qsdb/tests/join_surface.rs` **实测**钉住（✅ 的断言**值**、
> ✋ 的断言**报错文案**）。正文里那些引号中的话就是引擎原话。

> ### ⚠️ 写 join 前先知道两件事（省一次"内存爆了"）
>
> **① 哈希 join 只把「右表」整张放进内存**，左表**逐批流式**。
> 所以 `A JOIN B` 的常驻内存 ≈ **B 的大小**，与 A 多大**无关**；
> 而我们**没有自动换边** ⇒ **书写顺序有实际代价**。
> **大表放左边**：`big JOIN small`（✔ 流式）而不是 `small JOIN big`（✘ 整张 big 进内存）。
>
> **② 没有"溢出阈值"旋钮**：溢出**不是**"超过某个 MB 就落盘"，
> 而是**内存额度要不到时才触发**（配额见 `memory_limit` / 全局池）。
> 要多少 = 右表 + **concat 瞬时 2×** + 键索引（`行数 × 16B`）—— 中间那笔 2×
> 最容易"看起来够、实际不够"。额度不够时：
> **INNER / LEFT / RIGHT 转分区块 hash join（落盘，跑完）**，
> **FULL 没有这条退路**（右孤儿会被分区重复发射）⇒ 明确报 `MEMORY_PRESSURE`。
> 完整口径见 `docs/内存预算与溢出设计.md` §4.4。

| 语法 | 状态 | 说明 |
|---|---|---|
| 多表逗号连接（`FROM a, b`） | ✋ | `unsupported: multi-FROM (comma join) not supported` |
| `INNER JOIN`（`JOIN` 同义）/ `LEFT [OUTER] JOIN` + `ON <等值>` | ✅ | 哈希 join；三表以上链式等值同样走它 |
| `RIGHT [OUTER] JOIN` | ✅ | 换边实现（`a RIGHT JOIN b` ≡ `b LEFT JOIN a`，输出列序换回 `a.* ++ b.*`）。仅支持等值 `ON`。值级验收 `tests/join_right_cross.rs` |
| `FULL [OUTER] JOIN` | ✅ | 值级验收 `tests/join_full.rs`。⚠️ 两条限制：**不参与并行分片**、**不支持落盘溢出**（内存不足时明确报 `MEMORY_PRESSURE`，不假装能跑）。实用判据：`2 × 右表大小 + 索引` 装得进内存配额 —— 细节见 `docs/内存预算与溢出设计.md` §4.4 |
| ↑ 这两种的**替代写法**（实测通过，见 `join_surface.rs`） | ✅ | `A RIGHT JOIN B` ≡ `B LEFT JOIN A`（列序自己重排）。`FULL OUTER` 拼出来是：<br>`SELECT a.k FROM a LEFT JOIN b ON a.k = b.k`<br>`UNION ALL`<br>`SELECT b.k FROM b WHERE NOT EXISTS (SELECT 1 FROM a WHERE a.k = b.k)`<br>（左全 + 右未匹配；**列数/类型要对齐**，右侧补的列写 `CAST(NULL AS <类型>)`） |
| `JOIN … USING (cols)` | ✅ | 键 = 两侧同名列**且输出把那一列合并成一份**（`SELECT *` 只出一份；取值 `COALESCE(l.c, r.c)`，四种 join 一套覆盖）。值级验收 `tests/join_using.rs`。<br>⚠️ **偏差**：合并之后 `l.c` / `r.c` 这类**限定名会回退到裸名**、指到合并列上（PostgreSQL 是两者并存）；这是本仓限定名解析的既有行为，已记队列 |
| `NATURAL JOIN` | ✋ | **明确不做**：按"同名同类型列"隐式匹配，两侧加一列就可能**静默改语义** —— 与"拼错就报错"的纪律相反。写 `ON`/`USING` |
| 非等值 `ON` + `INNER` | ✅ | 走 `CrossJoinFilter`（叉积 + 过滤）。代价 = **左行数 × 右行数**，只适合小表 |
| 非等值 `ON` + `LEFT` | ✋ | `non-equi LEFT JOIN not supported (only non-equi INNER is)`（**绝不能按 INNER 跑**——那会静默丢掉未匹配行） |
| `CROSS JOIN` | ✅ | 笛卡尔积（谓词恒真）。⚠️ **裸 `JOIN b`（无 `ON`/`USING`）仍然拒绝** —— 那是"没写约束"，静默当叉积会炸行数 |
| 行值比较 `(a, b) IN (子查询)` / `= ANY/ALL (…)` | ✋ | 见 §三（表达式面）。⚠️ 行值那条**此前归类在 `INVALID_ARGUMENT`**（"你写错了"），2026-10-15 改成 `UNSUPPORTED`（"没实现，改写法"）——驱动按码决定动作 |
| `IN (子查询)` / `EXISTS` / `NOT IN` / `NOT EXISTS` | ✅ | 走 semi/anti 族（存在集，非物化整行）。`NOT IN` 有专门的三值语义实现 |
| 派生表列别名 `… AS d(a,b)` | ✋ | 此前**别名被丢掉**，之后报"列不存在"（误导） |
| 表因子列别名 `FROM t AS x(a,b)` | ✋ | 同样被静默丢掉。⚠️ 真实踩坑：`SELECT * FROM t EXCLUDE (b)` 会被解析成「别名 `exclude` + 列清单」⇒ 拿到**全列**（Snowflake 用户会中招） |
| `TABLESAMPLE` | ✋ | |
| 表 PARTITION 选择器 / `AT VERSION`（时间旅行） | ⛔ | 本方言不解析；**静默忽略会读到别的数据**，故一旦能解析必须拦 |
| **裸列的解析范围** | ⚠️ | **裸名 = 左侧输入**；JOIN 右表的列**必须限定**（`u.c`）。与标准 SQL 不同（标准是"全输入搜索 + 歧义报错"），但本仓规则**确定性**且报错会指路（提示写成 `u.c`）。歧义不会静默二选一 |

## 三、表达式与函数

| 语法 | 状态 | 说明 |
|---|---|---|
| 算术 / 比较 / 布尔 / `IS [NOT] NULL` / `IS [NOT] DISTINCT FROM` | ✅ | |
| `BETWEEN` / `IN (…)` / `IN (子查询)` / `EXISTS` / 标量子查询 | ✅ | |
| `CASE` / `CAST` / `TRY_CAST` / `::` / 类型化字面量（`DATE '…'`） | ✅ | |
| 聚合 `count/sum/avg/min/max`（含 `count(*)`） | ✅ | |
| 聚合 `DISTINCT`（`count/sum/avg/min/max(DISTINCT x)`） | ✅ | 2026-10-14 实现（原生：绑定层等价改写 + 单遍融合计数）。**此前**静默当成不带 DISTINCT ⇒ 回错数字 |
| └ `DISTINCT` 与非 `DISTINCT` **混用**、或多个 DISTINCT 聚合**参数不同** | ✋ | 一次去重会改掉另外那些聚合的输入（`sum(y)` 会少算行），**显式拒绝**不硬猜 |
| 聚合 `FILTER (WHERE …)` | ✋ | 此前**静默忽略** ⇒ 回全量聚合（错值） |
| 函数参数内的 `ORDER BY` / `LIMIT` / `SEPARATOR` | ✋ | |
| `WITHIN GROUP (ORDER BY …)` | ✋ | |
| `IGNORE/RESPECT NULLS` | ✋ | 静默忽略会改窗口函数结果 |
| `INTERVAL` / `AT TIME ZONE` / `COLLATE` / `SIMILAR TO` | ✋ | |
| 行元组 `(a,b) IN (…)` | ✋ | `row-value comparison `(a, b) IN (subquery)` not supported — write it as EXISTS (SELECT 1 FROM … WHERE … AND …)`（**归类 `UNSUPPORTED`**，改写法即可） |
| `= ANY(...)` / `= ALL(...)` | ✋ | `` `= ANY/ALL (subquery)` not supported — use IN / EXISTS, or an explicit OR/AND chain `` |
| 位置参数 `?` | ✋ | 本仓走 HTTP/API 层参数化，不做服务端占位符 |
| 标量函数集 | ✅ | `SELECT abs(1)` 之类报错会**列出支持清单**，以之为准 |
| `GREATEST` / `LEAST` / `array_agg` 等 | ✋ | 未实现（报错列清单） |

## 四、窗口函数

| 语法 | 状态 | 说明 |
|---|---|---|
| `row_number` / `rank` / `dense_rank` / `lag` / `lead` / `sum`/`avg`/`count`/`min`/`max` `OVER (…)` | ✅ | 支持清单以报错文案为准（`support: …`） |
| `PARTITION BY` / `ORDER BY`（多列、混合方向） | ✅ | |
| **帧边界** `ROWS/RANGE BETWEEN …` | ✋ | 此前**静默用默认帧** ⇒ 换一个数字（累计）而不报错 |
| 窗口内 `ORDER BY … NULLS FIRST/LAST` | ✅ | 与外部 `ORDER BY` **同一套**规则（共用 `SortKey`）|
| `ntile` / `first_value` / `last_value` / `percent_rank` / `cume_dist` | ✋ | 未实现（报错列支持清单） |
| 命名窗口 | ✋ | 见 §一 |

## 五、DML / DDL

| 语法 | 状态 | 说明 |
|---|---|---|
| `INSERT INTO t VALUES …` / `INSERT INTO t (列前缀) VALUES …` | ✅ | 缺列补 NULL（NOT NULL 列照拦） |
| `INSERT INTO t SELECT …` / `INSERT OVERWRITE t SELECT …` | ✅ | |
| `INSERT … ON CONFLICT` | ✋ | 此前**静默忽略** ⇒ 直接插重复行 |
| `INSERT … RETURNING` / `IGNORE` / `REPLACE INTO` / `PARTITION (…)` | ✋ | |
| `DELETE FROM t [WHERE …]` | ⚠️ | **只支持分区键谓词**（分区粒度）：`DELETE FROM t WHERE dt = '…'` ✅；`WHERE <非分区列>` → `unsupported: DELETE WHERE references non-partition column …`。**非分区表上带任何 `WHERE` 一律拒**（没有分区列可裁剪）。整表清空（无 `WHERE`）✅。<br>⚠️ F29①（2026-10-15，qsdb-sync 实测）：此前本表写的是无条件 ✅，**方括号没限定**，读起来像"任意 WHERE 都行" —— 对 BI 的"删这一行"尤其误导。**行级 DELETE 未实现**（是独立特性） |
| `UPDATE` | ✋ | |
| `TRUNCATE TABLE` | ✅ | |
| `CREATE TABLE`（含 `PARTITIONED BY`、`NOT NULL`、`CHECK`、`COMMENT`） | ✅ | **分区表至少要有一个非分区列**（否则数据文件零列，见 `docs/进度与待办.md` §三之四） |
| 列 `DEFAULT` 值 | ✋ | 静默忽略会写入 NULL 而非默认值 |
| `CREATE TABLE … AS SELECT`（CTAS） | ✋ | 报错文案已明确（此前是"table needs at least one column"，误导） |
| `CREATE OR REPLACE TABLE` | ✋ | 目前报 `already exists`（已知误导，排期中改明确文案） |
| `ALTER TABLE … ADD COLUMN` | ✅ | |
| `ALTER TABLE … DROP COLUMN` | ✋ | |
| `CREATE INDEX` / 视图 / 用户管理 | ✅ | 见 `docs/对接契约.md` §三 |
| 多语句（分号分隔） | ✋ | 一次一条（防注入面） |

---

## 附：这份清单怎么维护

1. **加新语法**：要么在绑定器里真读那个字段，要么在四个 `audit_*` 里拒绝。
2. **闸门有回归**：`crates/qsdb/tests/sql_silent_drop.rs` 每条拒绝都配**对照组**
   （正例），防止"一刀切"把正常查询也拒了。
3. **别信"没人会这么写"**：这份清单里 **12 条**静默丢弃中，有 **5 条是常见写法**
   （`FILTER`、窗口帧、`OFFSET`、`NULLS FIRST`、`ON CONFLICT`）——其中 3 条
   （`OFFSET`/`FETCH`/`NULLS`）在 2026-10-13 已从"拒"实现为"能"。
4. **顺手挖到的真实踩坑**：`SELECT * FROM t EXCLUDE (b)` 会被解析成「表别名
   `exclude` + 列清单 `(b)`」⇒ **静默拿到全列**（Snowflake 用户会中招）。同类
   写法 `FROM t AS x(a,b)` 一并拦。
