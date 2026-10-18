# SQL 支持面补齐设计 —— USING 命名空间 · CTE 列别名 · RANGE 帧（2026-10-18，设计稿）

> **缘起**：F29 的"覆盖度 B-/C+"评估停在 2026-10-14；此后生态功能批次
> #2-#4（2026-10-15）与窗口批次已经落地了一大半——**本稿先把现状用探针
> 复核清楚，再给真实剩余缺口的范围与设计**。复核方式：22 条探针 SQL 逐条
> 打真引擎（`work/probe_surface.sh` / `work/probe_using.sh`，2026-10-18 实测），
> 与 `docs/SQL支持面.md`（2026-10-15 更新，含 F29 两处失实的修正注记）交叉对账。
>
> **定位**：日清分 154 语料已全绿——本批**不为业务 SQL**，为的是
> **BI/工具生成的 SQL**（帆软图表向导、DBeaver、JDBC 元数据驱动）——
> 这正是 qsdb-jdbc M3（工具验收）的直接前置。

---

## 一、现状复核（探针实测，2026-10-18）

### 1.1 已实现——F29 时代的"缺"大半不在了

| 形态 | 状态 | 证据 |
|---|---|---|
| `RIGHT JOIN` / `FULL [OUTER] JOIN` | ✅ | RIGHT = 换边的 LEFT（`translate.rs:3963`）；FULL 两侧孤儿都保留（`:3970`） |
| `JOIN … USING (g)` **本体** | ✅ | 输出合并列、取值 `COALESCE(l.g, r.g)`、四种 join 一套；值级验收 `tests/join_using.rs` |
| `count(DISTINCT)` / `sum(DISTINCT)` | ✅ | F28 的"推荐改写不等价"已由**原生重写**闭环（`translate.rs:7192`：`count(DISTINCT x)` → `Distinct{[x]} + count(*)`，NULL 语义正确） |
| `FILTER (WHERE …)` | ✅ | 探针过 |
| `GROUP BY 序号` / 表达式分组键 | ✅ | 生态功能批次 #2（序号越界/非整数显式拒；`to_string()` 比对 bug 修） |
| `ROWS BETWEEN` 帧 / `LAG`/`LEAD` | ✅ | `audit_window_frame` + 帧翻译（`translate.rs:440-509`） |
| `INTERSECT` / `EXCEPT` / `UNION [ALL]` | ✅ | 探针过 |
| `TRY_CAST` / `COALESCE` / `NULLIF` / `BETWEEN` / `LIKE` / `CASE WHEN` / `FETCH FIRST` / `NULLS FIRST` | ✅ | 探针过 |
| `DELETE FROM t WHERE dt=…`（分区键谓词） | ✅ | 支持面文档 ⚠️ 注记已带 F29① 修正 |
| `NATURAL JOIN` | **保持 ✋** | 显式拒的理由成立（隐式按同名列匹配，两侧加一列就可能静默改语义——与"拼错就报错"纪律相反） |

### 1.2 真实剩余缺口（本批 Scope，探针实锤）

| # | 缺口 | 探针证据 |
|---|---|---|
| **S1** | **USING 命名空间不完整**：`SELECT w FROM t JOIN u USING (g)` → `not found: column 'w' (…unqualified names resolve to the left input only)`。合并列的裸名 ✅（探针第 3 条过），但**右侧非合并列的裸名不可见**；且支持面文档 §107 已记"限定名 `l.c`/`r.c` 回退裸名"的偏差（PG 是两者并存） | `work/probe_using.sh` 全系 |
| **S2** | **CTE 列别名**：`WITH c (gg, vv) AS (SELECT g, v FROM t)` → `unsupported: CTE column alias list … refusing rather than silently ignoring`（此前别名被静默丢掉——现闸是 2026-10-15 补的，正确但没实现） | 探针 |
| **S3** | **RANGE 帧**：`sum(v) OVER (ORDER BY v RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)` → `unsupported: window frame RANGE BETWEEN … (only ROWS is implemented)`。**这正是帆软/BI 图表向导生成"运行总计"的默认形态** | 探针 |

三个缺口全部是**显式拒绝**状态（话术正确、不静默）——本批是把三条闸换成真实现，
不是修 bug。

## 二、设计

### 2.1 S1 USING 命名空间补齐（绑定层·名字解析表）

**标准口径**（PG/MySQL 同判）：`JOIN … USING (g)` 之后的可见命名空间 =

| 名字形态 | 指到哪 | 现状 |
|---|---|---|
| 裸合并列名 `g` | **合并列**（`COALESCE(l.g, r.g)`） | ✅ 已对 |
| 右侧非合并列裸名 `w` | 右表该列 | ✗ 被拒（本批修） |
| 左侧非合并列裸名 `v` | 左表该列 | ✅（左输入解析既有） |
| 两侧同名非合并列裸名 | **歧义 → 400**（不猜） | ✅ 语义保留 |
| 限定名 `t.g` / `u.g` | **都指合并列**（PG 同款并存） | ⚠️ 现回退裸名（§107 已记），本批一并转正 |

**实现**：JOIN 绑定产出的"输出 schema + 名字解析表"里，USING 形态追加右输入
的列注册 + 合并列的限定名别名两条；歧义判定沿用现有"重名报 `ambiguous`"的
口径（F30 定案，不静默取第一个）。**纯放宽**——此前报错的查询能跑了，已跑
通的查询一个字节不变 ⇒ 零回归风险。

**孤儿行验证（错数据级，实现时必须探针锁定）**：LEFT/RIGHT/FULL 的合并列
取非空侧在**裸名/限定名两条路径下**都要成立（`tests/join_using.rs` 已锁
COALESCE 本体，本批加裸名 + `u.g` 限定名变体）——四种 join × 三种名字形态
的值级矩阵，12 格全绿才算过。

### 2.2 S2 CTE 列别名（绑定层·位置重命名）

**形态**：`WITH c (gg, vv) AS (SELECT g, v FROM t) SELECT gg FROM c`。
sqlparser 里 CTE 与派生表共用 `TableAlias { name, columns }`——**同族语法
同治**：`FROM (SELECT …) AS x (a, b)` 的派生表列别名一并支持（探针没单测，
实现时同一套验收覆盖两形态）。

**语义**：列别名是**按位置重命名输出 schema**，不是再加投影：

- 别名个数 ≠ 查询输出列数 → **400**（带两侧数字，指路）；
- 别名重名 → **400**（标准要求唯一，静默去重是"改用户的名字"）；
- 重命名后 CTE 的所有引用共享同一 schema（**在 CTE 定义处包一层全列投影
  重命名**，多引用只做一次；列重命名是元数据操作，Arrow 层零拷贝）。

**为什么不是"引用处映射"**：CTE 可能被引用多次，每个引用各记一张映射表
= 三处真相源；定义处重命名一次，此后 CTE 就是一张普通表——与派生表
`AS x(a,b)` 的语义也天然一致。

### 2.3 S3 RANGE 帧（收窄：只收无值偏移的三形态）

**为什么不能拿 ROWS 冒充**（这也是它当初被显式拒的原因，话术保留）：RANGE
按**值**分帧——`ORDER BY` 键相等的行是 **peers，整组一起进帧**。键有重复时，
`RANGE UNBOUNDED PRECEDING..CURRENT ROW` 的累计值与 `ROWS` 同形态**不同**
（ROWS 只收到当前位置）。BI 工具默认生成 RANGE 恰恰因为它是"业务正确"的
运行总计。

**v1 范围**（三个无值偏移形态，覆盖运行总计的全部工具生成形态）：

| 形态 | 语义 | ORDER BY 要求 |
|---|---|---|
| `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` | 分区首到**当前 peer 组**止 | **恰一列** |
| `RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING` | 当前 peer 组到分区尾 | 恰一列 |
| `RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` | 整分区 | 可无（无序整分区） |

**实现**：窗口执行器加 **peer 分组**（排序键相等判定；NULL 视为相等 peer，
PG 同判）——帧边界落在 peer 组边界而非行位置。`ROWS` 路径不动（peer 组
大小为 1 时两者重合，现有测试即回归）。

**保持显式拒**：`RANGE` 带值偏移（`n PRECEDING` / `n FOLLOWING`）——那需要
排序键的值算术（数值加减 / 时间戳区间），且"相邻值间隔"语义与 `GROUPS`
纠缠；另一批议。拒时话术给两条路：改 `ROWS`（并说明键重复时数字会变）或等
实现。

## 三、契约变更清单（只增不改）

| 项 | 变更 | 兼容性 |
|---|---|---|
| 无新错误码 | 三项全部是"此前显式拒 → 现在能跑" | **纯放宽**，零回归面 |
| `docs/SQL支持面.md` | USING 行改 ✅ 并删 §107 偏差注记；CTE 列别名行改 ✅；RANGE 行改"三形态 ✅ / 值偏移 ✋"；派生表列别名入表 | 文档维护纪律（附节） |
| 错误话术 | RANGE 值偏移的拒文案改指路（"三形态已支持"） | 文案不做兼容承诺 |

## 四、分期与验收

| 期 | 内容 | 验收 |
|---|---|---|
| **Ⅰ S1+S2**（纯绑定层，同族名字语义） | USING 命名空间 + CTE/派生表列别名 | 值级：四 join × 三名字形态 12 格矩阵（含孤儿行 coalesce）；CTE/派生表两形态别名往返；个数不等/重名/歧义三个 400；`tests/join_using.rs`、`tests/` 新增 `cte_alias.rs` |
| **Ⅱ S3**（窗口执行器 peer 分组） | RANGE 三形态 | 值级：键重复表上 RANGE 与 ROWS **数字必须不同**（锁定 peer 语义）；三形态 × 键唯一/重复/含 NULL；RANGE 值偏移仍显式拒；`ROWS` 全量回归 |

回归关：19 基准 + 154 语料 + 既有支持面测试（`group_by_ordinal` / `join_using` /
`create_table_as` / 窗口系）全绿；每期独立提交。

## 五、不做清单（本批边界）

- **NATURAL JOIN**——保持显式拒（隐式匹配静默改语义，与"拼错就报错"纪律相反）
- **RANGE 值偏移 / `GROUPS` 帧**——值算术 + 另一套分组语义，另批议
- **`ROLLUP`/`CUBE`/`GROUPING SETS` / `GROUP BY ALL` / `ORDER BY ALL`**——
  工具生成频率低、展开语义与现有聚合器结构冲突面大；有真实消费方再立项
- **行级 `UPDATE`/`DELETE`**——定位声明（分析库），不是本批范围
- **`l.c`/`r.c` 指向合并列之外的东西**——PG 并存语义就是"都指合并列"，不造第三种含义
