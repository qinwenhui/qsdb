# join 字符串键零拷贝索引 —— 设计稿（2026-10-17，第十二批）

> **缘起**：`complex_three_way` 对 DuckDB 3.4×（353ms vs ~105ms）。perf 剖面
> （12K 采样，`--no-children`）：`join_batch` 25.6%、`RowIndex::find` 12.5%、
> `encode_join_key` 5.8%、`memcmp` 2.3%、`memcpy/memset` 10%、`group_id` 12.5%
> + `encode_key_row` 5.5%、ZSTD 解压 ~5%、arrow take（字符串）2.8%。
>
> **归因一句话**：第二条 join（`d.cat = d2.cat`）是**字符串键**，走字节路 ——
> 每行"编码进 buf（一次拷贝+tag）→ 对 buf 哈希 → find（arena 随机访问 +
> memcmp 比真值）"。加上 GROUP BY 字符串键同族开销，**字符串键机器 ≈ 38%**。

## 一、目标与非目标

**目标**：单列 UTF8 join 键走**零拷贝字符串索引**：
- 建侧：**不复制键字节**（保住 build 批的 `StringArray`，索引只存 row id
  和预先算好的 hash）；
- probe 侧：每行直接对 arrow 字节切片哈希（`value(i)` 的 `&[u8]`，无 buf），
  命中后与建侧行字节**原地比较**（memcmp 只在此时发生，且比较的是箭头缓冲
  而非 arena 拷贝）。

**非目标**（本批不做）：多列字符串键、字符串×整数混合键、GROUP BY 字符串键的
同族改造（视本批收益再定）、并行 build。

## 二、结构

```rust
enum BuildKeyIndex {
    I64(I64Map<Vec<u32>>),     // 现行：单整数键
    Str(StringIndex),          // 新增：单 UTF8 键
    Bytes(RowIndex),           // 现行：其余（多键/混合/非整非串）
}

struct StringIndex {
    by_hash: HashMap<u64, SmallRows, BuildStringHasher>, // hash → 行
    col: StringArray 切片（保 Arc[ArrayRef]，含 value 偏移）,
    base: u32,               // 跨批的行号基址
}
```

- 建侧每行：取 `value(i)` 字节切片 → `hash_bytes`（与现行同族 FxHash）→
  `by_hash.entry(h)`（`One(row)/Many(vec)` 同现行 `RowIndex` 的桶形）。
  **不写 arena**。
- probe 每行：NULL → 无匹配；`value(i)` 切片 → 哈希 → 桶内逐 id 与
  **建侧箭头缓冲的字节**比较（`memcmp`，哈希碰撞必须比真值 ——
  静默错值纪律）→ 匹配则成对输出。
- **退化**：probe 键实到类型不是 UTF8（绑定期该 cast 已定，但运行期防御）
  → 整体回退字节路重灌（现行行为）。

## 三、正确性（钉死三条）

1. **哈希碰撞必须比真值**（只信哈希 = 静默错值）。
2. **NULL 键不匹配任何行**（SQL 三值；空串 `''` 是合法键、参与匹配）。
3. **跨批行号**：build 多批时 `base[bi] + row`，与现行 I64/Bytes 路同一约定。

## 四、顺手的两处白捡（同一批，纯删代码）

1. `join_batch` 的 `probe_idx` / `build_idx` **预留给量**（现在从 0 扩容，
   500 万匹配对的反复合并 ≈ memcpy 6.8% 的大头之一）。
2. INNER 路径 `all_probe = probe_idx.clone()` → 直接 move
   （`emit_unmatched=false` 时 clone 纯浪费，又是一次 20MB 级拷贝）。

## 五、验收

1. **闸门全绿**（join_surface / join_projection / join_spill / sql_e2e …）。
2. bench `complex_three_way` / `complex_join_group` 前后对照（同数据 5M，
   3 轮中位；DuckDB 同轮）。
3. **真实语料零回归**：154 块行数一格不变（join 改动动的是执行路径，
   行数/值必须逐块一致）。
4. **诊断开关**：`QSDB_JOIN_PROF=1` 打印各 join 选择的键索引形态
   （I64/Str/Bytes），**可断言**（对照"静默不触发"）。

## 六、风险

- **缓存行为**：字符串键哈希表本身是缓存敏感结构（第五批教训）。本批**不改
  表结构**（仍是 hash → 桶形），只去掉两次拷贝（encode 的 buf 写、arena 写），
  理论上只减不加；但以实测为准，变慢就回退。
- build 侧 `StringArray` 的存活期：索引持有 `Arc`，与现行 `state.table`
  同生命周期，无新增风险。
