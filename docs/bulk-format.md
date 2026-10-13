# `qsdb-bulk`：无依赖列式批量格式（契约）

> **给谁用**：生态工具 / 驱动（尤其 Java：不想引 `arrow-java` 的一串传递依赖）。
> **用在哪**：`POST /api/v1/write?format=bulk`（写）与 `POST /api/v1/query`（`"format":"bulk"`，读）。
> **实现规模**：任何语言手写 **~150 行**；定长列两侧都能零拷贝。
> **性质**：契约级稳定——**加类型只能追加编号**，已发布编号的语义永不变。
>
> 背景：IPC 的元数据是 FlatBuffers，Java 侧产出/解析它就要 JDK 11+ 与 FlatBuffers/Jackson。
> 本格式 = **Arrow body 去掉 FlatBuffers**：一遍顺序读、定长小端、显式长度、无 varint、无压缩。

## 一、布局（全小端；无对齐要求）

| 段 | 类型 | 说明 |
|---|---|---|
| `magic` | 8 字节 | ASCII `QSDBBULK` |
| `version` | u16 | 当前 `1` |
| `n_cols` | u16 | 列数 |
| `n_rows` | u64 | 总行数 |
| 列描述 × n_cols | | `name_len`:u16 + `name`:UTF-8 字节 + `type`:u8（见 §二） |
| 列数据 × n_cols | | 见 §三（**顺序与列描述一致**） |

## 二、类型标签（`type` 字节）

| 值 | 名 | 语义 | 宽度 |
|---|---|---|---|
| 1 | `Bool` | 布尔 | 1 字节（0=假，非 0=真） |
| 2 | `I32` | 32 位有符号整数 | 4 |
| 3 | `I64` | 64 位有符号整数 | 8 |
| 4 | `F64` | IEEE754 双精度（小端位模式） | 8 |
| 5 | `Utf8` | UTF-8 字符串 | 变长（§三.2） |
| 6 | `Date32` | 自 1970-01-01 的天数（i32） | 4 |
| 7 | `TimestampNs` | 自 epoch 的纳秒（i64） | 8 |

**不在表里的类型 → 报错**（显式拒绝，不猜；新类型按需追加编号）。

## 三、列数据

### 3.1 公共前缀（每列）

| 字段 | 类型 | 说明 |
|---|---|---|
| `null_bytes` | u64 | `0` = 本列无 NULL；否则 = `ceil(n_rows/8)`，随后跟**位图** |
| 位图 | `null_bytes` 字节 | **LSB first**：第 `i` 行 NULL ⇔ `bits[i/8] & (1<<(i%8)) != 0` |
| `data_len` | u64 | 定长列 = `n_rows × 宽度`（**必须**相符，否则报错）；`Utf8` = 数据段字节数 |

### 3.2 定长列（1/2/3/4/6/7）

紧跟 `data_len` 字节的裸值数组（小端）。**NULL 行仍占位**（值无意义，解码端按位图还原）——
这样解码端不需要"压缩行号"那套。

### 3.3 `Utf8`（5）

紧跟 `data_len` 字节的数据段，**前置** `(n_rows + 1)` 个 u32 小端 `offsets`：
第 `i` 行的字节区间 = `data[offsets[i] .. offsets[i+1]]`（`offsets[n_rows] == data_len`）。
NULL 行为空区间（并列的两 offset 相等）**或**由位图标示——**两侧都以位图为准**，
空区间只是让偏移量保持单调。

## 四、写侧与读侧的用法

```bash
# 写：body 就是上面的字节流（列序 = 表列序，与 /write 既有约定一致）
curl -X POST 'http://host:9430/api/v1/write?table=t&mode=append&format=bulk' \
     -H 'Content-Type: application/vnd.qsdb.bulk' --data-binary @batch.bulk

# 读：与 ipc/json 并列的一种 format
curl -X POST 'http://host:9430/api/v1/query' -H 'Content-Type: application/json' \
     -d '{"sql":"SELECT * FROM t","format":"bulk"}' -o result.bulk
```

另有无依赖的**文本**通道（更省事、更慢）：`format=csv`
- 写：body = CSV 文本，列序 = 表列序，首行若与表列名逐字相同则当表头跳过；
  空字段 = NULL；**类型按表列类型解析，不做推断**（推断会把 `001` 变成 `1`）。
- 读：结果是 CSV 文本（首行表头）。

## 五、与 IPC 的关系

| | `format=ipc` | `format=bulk` | `format=csv` |
|---|---|---|---|
| 客户端依赖 | Arrow 库（Java: arrow-java, JDK 11+） | **零** | **零** |
| 速度 | 最快 | 与 IPC 同档（同为列式二进制） | 慢 1.5-2× |
| 嵌套类型 | 支持 | 暂不支持（显式拒绝） | 不支持 |

内核侧的权威实现：`crates/qsdb/src/bulk.rs`（`encode` / `decode`），
协议测试在 `crates/qsdb-cli/tests/http_contract.rs`。
