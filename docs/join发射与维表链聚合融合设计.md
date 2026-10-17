# join 发射对症 + 维表链聚合融合 —— 设计稿（2026-10-17，第十四批）

> **缘起**：`complex_three_way` 0.30s vs DuckDB 0.121s（2.5×）、`complex_join_group`
> 0.26s vs 0.121s（2.2×）。上一批收尾时说"发射路径 40% + GROUP BY 字符串键 19.5%"
> —— **本批先剖，两个判断都被实测修正了一个**（详见 §一）。

## 一、剖面结论（.14，批 13 二进制 + 新增 `QSDB_JOIN_PROF` 段表计时）

基准形状：`t`(500 万) ⋈ `d`(20 万，g 整数键) ⋈ `d2`(20 行，cat 字符串键)
→ GROUP BY d2.cat。默认分片 4。

**1. 发射路径内部（段计时，多线程累计；墙钟 0.30s、段计 0.68s ≈ 2.4 有效并行）**

| 段 | 累计 | 占比 |
|---|---|---|
| **探测/配对循环**（哈希查找 + 行号对 push） | **536ms**（i64 283 / str 253） | **79%** |
| probe 侧 take | 52ms | 8% |
| build 侧 take | 93ms | 14% |
| 批构造 / 键求值 / 残留 | ~1ms | 0% |

**修正一**：上一批 perf 把 40.3% 记在 `join_batch` 名下当作"发射=取列贵"——
self 采样把**内联进来的 take** 也算进了函数本体。段计时证明 **take 只占 22%，
大头是配对循环本身**。i64 路 57ns/行（1 次哈希查找 + 2 次命中 push），
str 路 25ns/行（哈希 + 查找 + 比字节 + 1 push）。

**2. GROUP BY 字符串键无罪**

同一份 2000 万行 join 输出：整型键（10 万组）0.32s vs 字符串键（20 组）0.32s
——**墙钟一样**。低基数下字符串组表又小又热，不背锅。上批的"19.5%"是采样占比
不是墙钟账。**高基数字符串分组（W1b/unit_id 1.5×）另案，本批不动。**

**3. 真正的墙：join 管道逐行物化**

- 分片 1 → 0.77s；4 → 0.30s；8 → 0.24s；**16 → 0.24s 平台**（内存 87→276MB，
  3 倍换不来，不调默认分片）。每片串行管道 ≈ 0.77s，其中配对循环 + 2000 万行
  ×1-2 列的发射物化是主体。
- `count(*) FROM 三表join`（不分组）反而 **0.65s**：`列收窄批=0` —— count(*)
  的 needed 为空被 `annotate_join_needs` 整体放弃，子 join **整宽物化 1000 万行**。
- `Project 盖在 join 上`（`SELECT count(*) FROM (SELECT d2.cat FROM …) x`）
  **1.5s**：`annotate_walk` 不认 Project，就地停手，probe-take 顶到 683ms。

## 二、三刀（同一批，各自独立开关，坏了任何一刀不连坐）

### 刀 A —— 列收窄覆盖面（annotate_join_needs 补两个洞）

1. **空 needed 不再放弃**：count(*) 过 join 时 needed 为空 ⇒ 照样标注。
   join 键列本来就由 `annotate_walk` 的 Join 臂强制补进 needed（键必须真值才能
   匹配），所以实际效果 = "只物化键列，其余填 NULL"。原注释"整行 NULL 无实测
   支撑"作废：count-over-join 就是支持对象，行数才是唯一被读的东西。
2. **`annotate_walk` 学会 Project**：needed 语义 = "本子树输出必须真实产出的
   位置"。过 Project 时把"输出位置 i"映射到"exprs[i] 引用的输入位置集合"
   （裸列 → 那一列；计算表达式 → collect_columns 全集）。**空间换算纪律**：
   本子树输出空间 ≠ 输入空间，映射只许经 `exprs`（与 `allow_scan_read` 同款教训）。
3. 开关沿用既有 `QSDB_JOIN_NO_PROJ=1`（整函数一个闸，不新增）。

预期：A 0.65→~0.30s；A1 1.5→~0.30s；语料里 `SELECT 列 FROM join` / count-over-join
形态全部受益。

### 刀 B —— i64 探测密集直索引 + 类型化循环

`HashIndex` 增 `Direct { min: i64, rows: Vec<SmallRows> }` 变体：

- **建**：int_ok 且 span = max−min+1 ≤ **256k**（32B/槽 × 256k = 8MB 上界）⇒
  两趟：先收键算 span，再按 `v−min` 直 placement。免哈希、免桶。
- **探**：`rows[(v−min) as usize]` **一次随机访问拿到全部行号**（SmallRows
  首行内联，一对多也停在槽里）。基准里 d 的 20 万 g 键 span=10 万 ⇒
  3.2MB 数组，L2/L3 常温。
- span 超限 ⇒ 照旧 `HashIndex::I64`（哈希路保留，不是回退是双路并存）。
- 顺手把 I64 探测臂的 `Box<dyn Fn>` 逐行虚调用换成**类型化枚举视图**
  （`int_col`/`IntCol`，覆盖面与 `int_key_column` 严格一致：Int8/16/32/64）。
- 开关 `QSDB_NO_JOIN_DENSE=1`。
- ⚠️ 首版（旁表形态）是负优化，实测 ~5× 慢、已重造——见 §七 教训 1。

预期：i64 探测 283ms 累计 → ~150ms。融合刀落地后它继续加速幸存的 5M 次查找。

### 刀 C —— 维表链聚合融合（本批主菜）

**洞察**：`SELECT d2.cat, count(*) FROM t ⋈ d ⋈ d2 GROUP BY d2.cat` 这种
"事实表 → 维表 → 维表"链 + 纯计数聚合，**一行都不需要物化**：

- TOP join 的 probe 键 = BOT join 的 **build 侧列**（d.cat）⇒ "d 的每行能配到
  哪些 d2 行"是 **build 侧的纯函数**，可对 build 行**一次性预计算**
  （20 万行 × ~50ns ≈ 10ms），probe 阶段按 build 行号直查，**不再逐对取
  cat、哈希、匹配**；
- 分组键 = TOP join build 侧裸列 ⇒ 每个 build2 行的组号也可预计算 ⇒
  probe 阶段每对配对只剩 `counts[gid] += 1`（+ tier-2：`sums[gid] += v[p]`，
  v[p] 是 probe 侧**顺序**读）。

**门（全部满足才走，任何一条不满足零改动走现有路）**：

1. `Aggregate` 直接盖在 `Join(TOP)` 上，`Join(TOP)` 直接盖在 `Join(BOT)` 或
   Scan 上（深度 ≤ 2）；之间无 Filter/Project/Sort。
2. 两个 join 都 INNER、无残留谓词、无 semi 族。
3. aggs ⊆ { `count(*)`, `count(裸列)`, `sum(Int64|Float64 裸列)` }；
   列来源 = TOP 的 probe 侧（BOT 的 probe 输入）。DISTINCT 聚合一律门拒。
4. group = TOP build 侧**裸列**；TOP 的 probe 键 = BOT build 侧**裸列**；
   两 join 的键表达式都是裸列（计算表达式门拒）。
5. 组基数上界 = TOP build 行数；预估状态字节数超预算 ⇒ 门拒（现有路能溢出，
   融合路不建溢出，所以装不下时**必须**让给现有路）。
6. 融合 build 要不到内存 ⇒ 报错与现有路一致：融合路需要的内存**严格更少**
   （同样的 build 侧 + 少掉全部发射物化），现有路装得下融合路必然装得下，
   反之亦然——不存在"融合失败但现有路能过"的静默降级。

**结构**：

```
FusedDimAggExec（新 ExecNode，agg.rs 旁）
  build 阶段：BOT/TOP build 侧物化 + 键索引（复用 BuildSide/build_index 全套）
    → 预计算 matches[b] : Vec<SmallRows>（BOT build 行 → TOP build 行号列表）
    → 预计算 gid_of[m] : Vec<u32>（TOP build 行 → 组号；组表用 GroupTable
       对去重后的键 ingest 种子化，吃既有 merge/finalize 全套）
  probe 阶段：BOT probe 输入流式逐批
    每行 p：BOT 键索引查 → 命中 b 列表
      每对 (p,b)：每 m ∈ matches[b]：g = gid_of[m]
        counts[g] += 1;  sums[j][g] += v_j[p]（类型化直读）
  产出：GroupTable partial（分片路）/ finalize 批（非分片路）
```

- **分片路**（`build_aggregate` 里 `joins > 0` 分支）：每片
  `run_fused_join_partial(...) -> (GroupTable, Reservation)`，merge/finalize
  机制零改动（`finish_partitioned_merge` 原样吃）。`ShardScope` 与
  `scope.taken()` 检查原样保留。
- **非分片路**：同一核心 fn 包成 ExecNode，`next()` 首调 build+probe，随后吐
  finalize 批。
- GroupTable 需新增的口子：`lookup_gid(key 编码) -> Option<u32>`、
  `bump_count(gid, col, by)`、`add_sum_i64/add_sum_f64(gid, col, v)` ——
  **种子路与 ingest 路必须产出逐位一致的表**（单测钉死）。
- 开关 `QSDB_NO_FUSED_JOIN=1`。

**预期**（分片 4）：three_way 每片 ≈ 扫描 4ms + 125 万次密集查找 19ms +
250 万对 ×6ns 15ms + build ~10ms ≈ 50-60ms ⇒ 墙钟 **~0.10s**；
join_group（深度 1）**~0.07s**。两格都有翻赢的可能（DuckDB 0.121s）。

## 三、正确性钉子（融合刀）

1. **哈希碰撞必须比真字节**：预计算 matches 用既有 `StringIndex/RowIndex::find`
   （内部已比对真值），不新写查找。
2. **NULL 键三处**：BOT probe 键 NULL ⇒ 该行不配（现有索引语义：NULL 不入
   索引/不匹配）；BOT build 键 NULL ⇒ 该 build 行 matches 为空；TOP build 键
   NULL ⇒ 同上。空串是合法键。
3. **一对多乘法**：d2 同 cat 多行 ⇒ matches[b] 是列表，逐 m 计数（不是只取
   第一个——静默少计数）。
4. **零计数组不发射**：组从"配过对的输入行"长出来，预种子化的组若一个对都没
   摊上，finalize 时按 count=0 剔除（与 SQL 分组语义一致）。
5. **sum 的 NULL**：count(col)/sum(col) 逐对判 `v[p]` 非 NULL 才计入
   （`counts` 口径 = 非空计数，与 GroupTable::ingest 一致）。
6. **分片一致性**：每片独立融合，组键去重靠 merge_subset —— 与现有分片聚合
   同一合并语义，无新增跨片状态。
7. **三路对撞**：默认（融合）/ `QSDB_NO_FUSED_JOIN=1` / `QSDB_JOIN_NO_PROJ=1`
   + 物化改写基线，结果逐值一致。环境变量进程全局 ⇒ 测试内串行 Mutex。

## 四、验收

1. 基准（.14，5 轮中位，DuckDB 同轮）：`complex_three_way`、`complex_join_group`
   目标 ≤0.15s（现 0.30/0.26）；`A`（count 不分组）≤0.32s（现 0.65）；
   其余 17 用例零回归。
2. 语料 154/154，行数对 `dag_opt1.tsv` diff = 0。
3. 闸门全绿 EXIT=0。
4. 新单测：融合 vs 不融合对撞（多匹配 cat / 三处 NULL / 空 build / 构造碰撞串 /
   分片 2·4·8 一致 / sum NULL / count(col) 空值语义）；刀 A 的 Project 过 join
   与 count-over-join 值/行数对撞。
5. `QSDB_JOIN_PROF` 段表计时保留（本批顺手的观测资产）。

## 五、不做清单（本批明确不做）

- **高基数字符串 GROUP BY 快路径 / W1b 自适应**：实测证明低基数字符串分组不是
  这两格的账；unit_id 1.5× 另案（可复用批 13 排序去重）。
- **默认分片调大**：16 分片 0.24s 平台 + 内存 3 倍，低占用纪律否决。
- **sum over build 侧列 / min/max / avg / Decimal sum**：每对多一次 build 侧
  随机读或比较器，收益未证，门拒留待实测。
- **深度 ≥ 3 的链**：语料形状最深两跳，门按深度 ≤ 2 收口。
- **溢出**：融合路不建溢出路径，装不下的形状门拒走现有路。

## 六、风险

- **门判错一列 = 静默错值**（融合路不物化，错键/错列不会报错只会算错）⇒
  门全部用"裸列 + 显式类型"收口，PhysExpr 形状不匹配一律门拒；对撞单测覆盖
  每条钉子。
- **GroupTable 手工种子与 ingest 分叉** ⇒ 单测断言两条路 finalize 逐位一致；
  种子实现尽量走 ingest 本体（对去重键批量 ingest 一次）。
- **刀 A 空间换算错位**（`allow_scan_read` 同款坑）⇒ Project 映射只经 `exprs`，
  以既有 join_projection 测试扩两例钉住。

## 七、落地结果与实测教训（2026-10-17 追记）

**结果**（.14，5 轮中位，DuckDB 同轮）：

| 用例 | 改前 | 改后 | DuckDB 同轮 | 相对 |
|---|---|---|---|---|
| complex_three_way | 0.307s | **0.090s** | 0.1214s | **~1.35× 领先** |
| complex_join_group | 0.262s | 0.152s | 0.1418s | 1.07×（基本打平） |
| count(*) 三表 join（A） | 0.65s | **0.38s** | — | 1.7× |
| `count(*) FROM (SELECT 列 FROM join)`（A1） | 1.5s | ~0.2s 量级 | — | 列收窄生效 |

基准 19/19 PASS、语料 154/154 OK ERR 0 行数零漂移、闸门全绿。

**两条途中撞出来、已写回代码的教训**：

1. **密集直索引首版是负优化**（~5×）：把"多命中"放 `HashMap<u32, Vec>` 旁表
   ——本基准恰好每键 2 行，每次探测变成"数组读 + 哈希查找 + 解引用"，
   比纯哈希还慢。重造为 `Vec<SmallRows>` 单数组（多命中内联在槽里，
   span ≤ 256k ⇒ 8MB 上界），`first[i] + more.get()` 三连变成一次随机访问。
   **通用教训：直索引的任何"旁表"都会把哈希路最贵的随机追逐请回来，
   旁表形态不配叫直索引。**
2. **逐行预备态**：融合探测的 `for_each_match` 把 downcast/装箱放进单行里
   ——实测 414ns/行（整条查询 0.5s+）。改成 `prepare_probe`（每批一次）+
   `match_row`（行内零预备）后 70ns/行。**通用教训：跨行不变量（数组视图、
   缓冲）必须 hoist 到批级，任何 per-row 的分配/装箱都是量级错误。**

另：T3 计数下推的 probe 子 join 标注曾把外层 probe 键位填 NULL
（`annotate_walk` 的 Join 臂只把键下传给孩子、不保证出现在输出里）⇒
静默 0 行，被既有 `三表链式等值_join` 测试当场抓住——needed 必须显式
含父层要读的输出位。
