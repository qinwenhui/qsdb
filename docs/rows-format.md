# `qsdb-rows`：行式无依赖批量格式（契约）

> **给谁用**：各类**驱动**（JDBC / ODBC / Python / 任何按行取值的客户端）。
> **用在哪**：`POST /api/v1/write?format=rows`（写）与 `POST /api/v1/query`（`"format":"rows"`，读）。
> **实现规模**：任何语言手写 **~80 行**（读一遍头，然后 `while !eof` 逐块）。
> **性质**：契约级稳定——**加类型只能追加编号**，已发布编号的语义永不变。
>
> **和 `bulk` 的分工**（[列式规范](bulk-format.md)）：
>
> | | `bulk`（列式） | **`rows`（行式，本文）** |
> |---|---|---|
> | 面向 | 列式生产者（Rust/Arrow 生态、嵌入方） | **驱动**（值是按行取出来的） |
> | 客户端实现量 | ~150 行（要**转置**） | **~300 行（边收边写，零转置）** |
> | 内存 | O(全量数据) | **O(一个 chunk)** |
> | 类型码表 | 1..7 | **1..7 语义完全相同**，8..14 为超集 |
>
> 为什么驱动要行式：JDBC 的取值是 `setLong(1,…); setString(2,…); addRow()`——
> 列式会逼驱动**先缓冲全量、再转置成列**，等于在业务进程里再造一份全量副本
> （5M 行 × 4 列 ≈ 200MB 堆 + 一次全量遍历），且无法流式。

## 一、布局（**全小端**；无对齐要求）

```text
头：magic "QSDBROWS" (8) | version u16 | flags u8 | n_cols u16
  flags bit0 = HAS_SCHEMA
    =1 → 列描述 × n_cols：
            name_len u16 | name(UTF-8) | type u8 | col_flags u8 | meta_len u16 | meta
            （col_flags bit0 = nullable；meta 见 §二）
    =0 → 无列描述（schema 由使用方提供，见 §四）
块 × N（直到 body 结束）：
    row_count u32 | chunk_flags u8
    chunk_flags bit0 = HAS_NULLS → n_cols × ceil(row_count/8) 字节位图（列序拼接）
    值：**行主序**（行 0 的 列 0..n_cols，再行 1 …）
```

**帧边界就是 `row_count`**：一个 body 里可以放任意多个块（驱动攒够一拨就发），
也可以一个请求一个块——两种都合法，内存恒定。

## 二、类型标签（`type` 字节）

| 值 | 名 | wire | meta |
|---|---|---|---|
| 1 | `Boolean` | 1 字节（0=假，非 0=真） | — |
| 2 | `Int32` | 4 字节 i32 | — |
| 3 | `Int64` | 8 字节 i64 | — |
| 4 | `Float64` | 8 字节 IEEE754 位模式 | — |
| 5 | `Utf8`（STRING/VARCHAR/CHAR） | 变长（§三.2） | 4 字节 `max_chars u32`（0 = 无上限） |
| 6 | `Date` | 4 字节 i32：自 1970-01-01 的**天数** | — |
| 7 | `Timestamp` | 8 字节 i64：自 epoch 的**纳秒** | `tz_len u16 + tz`（0 长度 = 无时区） |
| 8 | `Int8` | 1 字节 i8 | — |
| 9 | `Int16` | 2 字节 i16 | — |
| 10 | `Float32` | 4 字节 IEEE754 位模式 | — |
| 11 | `Binary` | 变长（§三.2） | — |
| 12 | `Decimal128` | 16 字节 i128 小端（**未缩放整数**） | `precision u8 + scale i8` |
| 13 | `Time` | 8 字节 i64：自午夜的纳秒 | — |
| 14 | `Interval` | 16 字节：`months i32 + days i32 + nanos i64` | — |

**编号 1..7 与列式 [`bulk`](bulk-format.md) 完全同义同宽**——两个格式只需记一套心智模型。

**不在表里的类型 → 报错**（显式拒绝，不猜；新类型按需追加编号）。
`meta_len` 不在上表类型的必须为 `0`（多给了字节一律报错）。

> `CHAR(n)` 与 `VARCHAR(n)` 都走 `type=5`（Arrow 层无区分）；定长语义以表定义为准。

## 三、值

### 3.1 NULL：位图为准，**占位照写**

- 位图：每列 `ceil(row_count/8)` 字节，**列序拼接**，LSB first
  （第 `i` 行 NULL ⇔ `bits[i/8] & (1<<(i%8)) != 0`）。
- **解析不依赖位图**：NULL 行也照常占位——
  - 定长列写全 `0`（值无意义，宽度照占）；
  - 变长列写 `len = 0`。
- 于是"空串"与"NULL"由位图区分，**驱动不需要猜**；解码端可以先把值读出来、
  再拿位图决定写值还是写 NULL（两件事解耦）。
- 本块**一个 NULL 都没有**时，`HAS_NULLS=0`，位图整段不出现（省掉每列 1/8 行的开销）。

### 3.2 变长（`Utf8` / `Binary`）

`u32 小端长度 + 字节`。`Utf8` 必须是合法 UTF-8（非法 → 报错，不做替换）。

### 3.3 示例（85 字节，可直接当一致性向量）

表：`id BIGINT`(tag 3) / `name STRING`(tag 5) / `dt DATE`(tag 6)；
两块：块 1 = 2 行（第 1 行整行 NULL），块 2 = 1 行。

```text
  0  51 53 44 42 52 4f 57 53   "QSDBROWS"
  8  01 00                     version 1
 10  00                        flags = 0（无列描述 = 写侧形态）
 11  03 00                     n_cols = 3
 13  02 00 00 00               chunk1: row_count = 2
 17  01                        chunk_flags = HAS_NULLS
 18  02 02 02                  3 列位图（第 1 行置位 → 0b10）
 21  01 00 00 00 00 00 00 00   id   = 1
 29  05 00 00 00 61 6c 69 63 65   name = "alice"（len 5 + 字节）
 38  38 4a 00 00               dt   = 19000 天（2022-01-08）
 42  00 00 00 00 00 00 00 00   NULL 行占位：id
 50  00 00 00 00               NULL 行占位：name（len 0）
 54  00 00 00 00               NULL 行占位：dt
 58  01 00 00 00               chunk2: row_count = 1
 62  00                        chunk_flags = 0（无位图）
 63  03 00 00 00 00 00 00 00   id   = 3
 71  06 00 00 00 e4 b8 ad e6 96 87   name = "中文"（6 字节 UTF-8）
 81  39 4a 00 00               dt   = 19001 天（2022-01-09）
```

内核侧有测试逐字节锁定这段（`crates/qsdb/src/rows.rs` 的 `spec_example_from_docs`）。

## 四、写侧与读侧的用法

```bash
# 写：body 按表列序输出值，**不带列描述**（flags=0），可含多块
curl -X POST 'http://host:9430/api/v1/write?table=t&mode=append&format=rows' \
     -H 'Content-Type: application/vnd.qsdb.rows' --data-binary @batch.rows

# 读：列名/类型随头一起发（flags=1），客户端逐块消费
curl -X POST 'http://host:9430/api/v1/query' -H 'Content-Type: application/json' \
     -d '{"sql":"SELECT * FROM t","format":"rows"}' -o result.rows
```

| 方向 | `flags` | 谁提供 schema |
|---|---|---|
| 写（`/write`） | **必须 0** | 端点按 `table=` 查表定义（`GET /api/v1/tables/{t}/schema` 可先取） |
| 读（`/query`） | **恒为 1** | payload 头里的列描述 |

- **写侧带 `HAS_SCHEMA=1` → 400**（`INVALID_ARGUMENT`）：表结构由 `table=` 决定，
  payload 里再放一份就是两个真相源。**显式拒绝，不静默忽略**。
- 写侧列序 = **表列序**（含分区列；与 `/write` 的 IPC/CSV 通道同一约定）。
  列数不符 → 400，**不按前缀凑合**。
- 块数不限：`mode=append` 下**一次请求 = 一次提交**，body 里放 1 块还是 100 块，
  落盘语义相同；`flush=false` 时先进写缓冲（由调用方后续 flush 决定提交时机）。

## 五、与其它通道的对照

| | `format=ipc` | `format=bulk` | **`format=rows`** | `format=csv` |
|---|---|---|---|---|
| 客户端依赖 | Arrow 库（Java: arrow-java，JDK 11+） | **零** | **零** | **零** |
| 面向 | 列式生产者 | 列式生产者 | **驱动**（按行取值） | 人/脚本 |
| 可流式（内存恒定） | 否（整批） | 否（整批） | **是（逐块）** | 是 |

> **JDBC `fetchSize` 的对应实现（十七批 R1 注记）**：驱动拿到 `rows` 响应后
> **按块增量消费、逐块喂给 `ResultSet.next()`**，不必等整个 body 落地——
> 这就是 PG/MySQL 驱动 fetch-size 流式在本库的对应物（结果护栏超限时驱动
> 应改用本通道，见 [对接契约 §3.7](对接契约.md)）。`json` 通道服务端虽已
> 逐批流式输出（R2），但客户端仍整段收——程序化大结果请走分块通道。
| 嵌套类型 | 支持 | 暂不支持（显式拒绝） | 暂不支持（显式拒绝） | 不支持 |

## 六、互操作已验证（2026-10-13）

三份**互相独立**的实现已按本文对死：

| 实现 | 语言 | 验证方式 |
|---|---|---|
| 内核编码器/解码器 | Rust | 单测（全类型往返 + 截断/坏 tag 显式报错） |
| 内核 HTTP 写侧契约测试 | Rust | body **照本文手写**（不调用内核 `encode`），喂真 server |
| **qsdb-jdbc 的 `QsdbBulkWriter`** | Java | `RowsEncoderTest` 用 §三.3 的 85 字节向量**逐字节**锁定；`BulkChannelIntegrationTest`（对真 server）**6/6 绿**：全类型往返（含 DECIMAL/BINARY/TIMESTAMP 与 NULL）、分块跨多次请求、并发装载、空写 no-op、缺表与类型不符的清晰报错 |

内核侧的权威实现：`crates/qsdb/src/rows.rs`（`encode` / `encode_values` / `decode` /
`decode_with_schema`），HTTP 契约测试在 `crates/qsdb-cli/tests/http_contract.rs`。
