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
| `GROUP BY` 位置序号（`GROUP BY 1`） | ✅ | 2026-10-15 实现（生态功能批次 #2）。裸整数 = **投射第 n 个表达式**（1 起，与 `ORDER BY 1` 同一套标准），不是常量。**显式拒**：越界（`GROUP BY 0` / 超出列数）、非整数（`GROUP BY 1.5`）、指向聚合（不能当分组键）、指向 `SELECT *`（通配展不成"第 n 个表达式"）。值级验收 `tests/group_by_ordinal.rs` |
| `GROUP BY <表达式>`（`upper(s)` / `v % 2` / `CAST(…)` / `CASE WHEN …` / `a ‖ b` …） | ✅ | 2026-10-15 **修**：此前**全都不工作**（报"列 's' 必须出现在 GROUP BY 里"，把"没实现"说成"你写错了"）。根因是判据拿 AST 的 `to_string()` 去比 `PhysExpr::display()`（后者带括号，`(g + 1)` vs `g + 1`）**永远比不中**。本表此前把 `GROUP BY` 整行标 ✅，**是虚的** |
| `GROUP BY ROLLUP/CUBE/GROUPING SETS` | ✋ | |
| `GROUP BY ALL` | ✋ | |
| `ORDER BY` 位置序号 / 输出别名 | ✅ | 序号越界 / 非整数**显式拒**（`ORDER BY 5` / `0` / `1.5`）。⚠️ 2026-10-15 修：此前越界**静默变成不排序**（退回"当常量排序键"）—— 它一直是靠"常量排序键让批内列长对不齐而报错"**借 bug 通过**的，`sql_gaps::order_by_ordinal` 那条断言实为"借 bug 通过" |
| `ORDER BY <常量>`（`ORDER BY 'k'`） | ✅ | 合法但无意义（不排序）；与"越界序号"是两回事，别混 |
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
| 聚合 `FILTER (WHERE …)` | ✅ | 2026-10-15 实现（生态功能批次 3）。此前**静默忽略** ⇒ 回全量聚合（错值）。<br>语义按 SQL 标准：**只有谓词为 TRUE 的行**喂给该聚合（NULL/UNKNOWN 不通过）；谓词**不影响分组** —— 一个全不匹配的组照样出现、回 `0`/`NULL`。谓词可引用任意列（不必是聚合参数）。<br>掩码**逐聚合**：同一条 SELECT 里两个聚合可各有各的谓词。<br>值级验收 `tests/agg_filter.rs` |
| └ 窗口函数上的 `FILTER` | ✋ | 未实现 ⇒ 显式拒（静默丢掉 = 回全量）。改写：`sum(CASE WHEN c THEN x END) OVER (…)` |
| └ `DISTINCT` + `FILTER` 同时写 | ✋ | 显式拒：去重改写会换掉输入，谓词引用的列就不在了。拆成子查询（先 `WHERE` 再 `DISTINCT`） |
| └ `FILTER` 用在标量函数上 / 谓词非布尔 / `WHERE NULL` | ✋ | 都是显式拒（后两者几乎必然是笔误） |
| 函数参数内的 `ORDER BY` / `LIMIT` / `SEPARATOR` | ✋ | |
| `WITHIN GROUP (ORDER BY …)` | ✋ | |
| `IGNORE/RESPECT NULLS` | ✋ | 静默忽略会改窗口函数结果 |
| `INTERVAL` / `AT TIME ZONE` / `COLLATE` / `SIMILAR TO` | ✋ | |
| 行元组 `(a,b) IN (…)` | ✋ | `row-value comparison `(a, b) IN (subquery)` not supported — write it as EXISTS (SELECT 1 FROM … WHERE … AND …)`（**归类 `UNSUPPORTED`**，改写法即可） |
| `= ANY(...)` / `= ALL(...)` | ✋ | `` `= ANY/ALL (subquery)` not supported — use IN / EXISTS, or an explicit OR/AND chain `` |
| 参数化查询：占位符 `$1…` / `?` | ✅ | 2026-10-15 实现（设计见 `docs/参数化查询设计.md`）。`$n` 显式位置、**可重复引用**；`?` 按出现顺序；**同一句不许混用**。值经协议 `params` 数组传入，在**绑定期变成 AST 字面量**（**从不进入 SQL 文本** ⇒ 注入无从谈起）。真值级验收 `tests/params.rs`。<br>⚠️ 生效范围：`WHERE`/`HAVING`/投影/`CASE`/`IN`/`LIKE`/`LIMIT`/`OFFSET`/窗口帧偏移/`lag-lead` 偏移都认；**表名位置不认**（那不是表达式位置） |
| `PREPARE` / `EXECUTE`（SQL 级预编译） | ✋ | 不做：**会话态语句句柄**在无状态 HTTP + 重连下是地雷（设计 §二）。要"解析复用"走服务端计划缓存，协议不变 |
| 标量函数集 | ✅ | `SELECT abs(1)` 之类报错会**列出支持清单**，以之为准 |
| `GREATEST` / `LEAST` / `array_agg` 等 | ✋ | 未实现（报错列清单） |

## 四、窗口函数

| 语法 | 状态 | 说明 |
|---|---|---|
| `row_number` / `rank` / `dense_rank` / `lag` / `lead` / `sum`/`avg`/`count`/`min`/`max` `OVER (…)` | ✅ | 支持清单以报错文案为准（`support: …`） |
| `count(*) OVER (…)` | ✅ | 2026-10-15 修：此前报 `needs an argument`（把"没实现"说成"你写错了"）。`count(*)` 数**行**、`count(x)` 数**非 NULL** —— 含 NULL 的列上结果不同 |
| `PARTITION BY` / `ORDER BY`（多列、混合方向） | ✅ | |
| **行帧** `ROWS BETWEEN a AND b` | ✅ | 2026-10-15 实现。边界 `UNBOUNDED PRECEDING/FOLLOWING`、`n PRECEDING/FOLLOWING`、`CURRENT ROW`；简写 `ROWS n PRECEDING` ≡ `… AND CURRENT ROW`。**帧不会跨分区**。<br>⚠️ **帧可能为空**（边界全落在当前行之外，如 `3 PRECEDING AND 1 PRECEDING` 在分区首行）⇒ `sum/avg/min/max` 给 **NULL**、`count` 给 **0**（与 PG/DuckDB 一致；**不是**夹到分区边）。<br>⚠️ 帧内一个非 NULL 都没有时 `sum/avg/min/max` 也是 **NULL**（此前是 0 / NaN，2026-10-15 一并修）。<br>值级验收 `tests/window_frame.rs` |
| 帧 + **无 `ORDER BY`** | ✋ | 显式拒：没有 `ORDER BY` 就没有确定行序（并行扫描下会变），按行分帧的值不可复现。**例外**：`ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`（整分区，与行序无关）放行 |
| **`RANGE` / `GROUPS BETWEEN …`** | ✋ | 按**值**分帧 / 按组计数，是另一件事；当 `ROWS` 跑会换个数字，故显式拒 |
| 起点在终点之后（`3 PRECEDING AND 5 PRECEDING`）、起点 `UNBOUNDED FOLLOWING`、终点 `UNBOUNDED PRECEDING` | ✋ | 帧恒空 / 标准不允许 —— 必是笔误，报错比回一列 NULL 好 |
| **排名 / lag / lead 带帧** | ✋ | 按标准它们**忽略**帧；本仓不静默忽略 ⇒ 显式拒并指路 |
| 窗口内 `ORDER BY … NULLS FIRST/LAST` | ✅ | 与外部 `ORDER BY` **同一套**规则（共用 `SortKey`）|
| `ntile` / `first_value` / `last_value` / `percent_rank` / `cume_dist` | ✋ | 未实现（报错列支持清单） |
| 窗口聚合 **NULL 口径** | ⚠️ | 帧内一个非 NULL 都没有 ⇒ `sum/avg/min/max` = **NULL**、`count` = **0**。**2026-10-15 改**：此前 `sum/min/max` 给 `0`、`avg` 给 `NaN` —— 那是**错值**（0 会被下游当真数字），已在 `tests/limit_offset_nulls.rs` 一并改正 |
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
| `CREATE TABLE … AS SELECT`（CTAS） | ✅ | 2026-10-15 实现（生态功能批次 4）。**列名与类型由 SELECT 的输出推导**（`SELECT *` / 表达式 / JOIN 都行）；也支持写显式列清单（`CREATE TABLE t (g BIGINT, n BIGINT) AS SELECT …`，列数须与查询一致）。支持 `PARTITIONED BY`。源可以是 CTE / 派生表 / JOIN。<br>⚠️ **表达式列名会被拒**：`SELECT g, count(*)` 推导出 `count(*)`，那不是合法表列名（限 `[A-Za-z_][A-Za-z0-9_]*`）⇒ 报错并指路（`… AS n` 别名，或写显式列清单）。**本仓不替用户改名** —— PG 会改成 `count`，那是另一种静默（表里出现你没写过的列名）。<br>⚠️ JOIN 的**限定名会剥掉表限定**（`a.g` ⇒ `g`，与 PG 一致）；剥完重名（`SELECT a.g, b.g`）会报出来。<br>⚠️ **失败会回滚**（把刚建的表删掉）—— PG 的 CTAS 是全有全无。但**表已存在**时是 `AlreadyExists`，**不会**动旧表。<br>⚠️ `WITH … CREATE TABLE …`（CTE 在前）sqlparser 不认，写成 `CREATE TABLE … AS WITH … SELECT …`。<br>值级验收 `tests/create_table_as.rs` |
| `CREATE OR REPLACE TABLE … AS SELECT` | ✋ | 与不带 `AS SELECT` 的形态一样拒（判断在 CTAS 分支**之前**）。先 `DROP TABLE` 再 `CREATE`；只想换数据用 `INSERT OVERWRITE` |
| `CREATE OR REPLACE TABLE` | ✋ | `unsupported: CREATE OR REPLACE TABLE 未实现 —— 先 \`DROP TABLE\` 再 \`CREATE\``（2026-10-15 修；此前报 `already exists`，误导） |
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
5. **"本表说 ✅，实测不 ✅"是这份清单最大的风险**（F29 家族，已经栽过三次：
   `count(DISTINCT)` 的推荐改写、`GROUP BY 1` 的报错文案、CTAS 的报错文案）。
   2026-10-15 又栽一次：`GROUP BY` 整行标 ✅，实际**表达式分组键全都不工作**。
   ⇒ 凡本表写"已实现/已修/文案已明确"，**都要有一条实测用例兜着**
   （`tests/support_claims.rs` 就是干这个的）。
6. **常量不是普通表达式**：`eval` 对常量返回**长度 1** 的数组，而下游按批内行号
   索引 ⇒ 不广播就有两个下场：`RecordBatch::try_new` 报**内部话术**
   （`all columns in a record batch must have the same length`）或**直接 panic**。
   2026-10-15 一次性清掉**四个**落点（投影 / 聚合分组键 / 聚合参数 / JOIN 残留），
   其中 `SELECT 1, g FROM t` 这种最普通的写法**此前必炸**。回归
   `tests/const_broadcast.rs`。**新写"按行索引数组"的代码时先问一句：它会不会是常量？**
7. **加一条"整表行数"式的捷径时，先问它绕过了什么**：`count(*)` 有**三条**独立捷径
   （元数据 footer / join 按键计数下推 / distinct 融合计数），每条都**不看行**。
   2026-10-15 加聚合 `FILTER` 时，三条**各自**让谓词静默失效（回全量大数）。
   与"有残留谓词就不能下推 join 计数"是同一个坑 —— 那次也栽过一遍。
   凡聚合的语义维度增加（`DISTINCT` / `FILTER` / 将来的 `ORDER BY`），
   都要**逐条**回去检查每个捷径的准入条件。
