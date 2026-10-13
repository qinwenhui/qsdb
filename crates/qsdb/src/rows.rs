//! **`qsdb-rows`：行式、无依赖、可流式的批量通道**（读写双向）。
//!
//! ## 为什么还要一个行式格式（已有列式 `bulk`）
//!
//! 生态工具的取值天生是**按行**的：JDBC `setLong(1,…); setString(2,…); addRow()`，
//! ODBC / Python 客户端同理。列式格式逼驱动**先缓冲全量、再转置成列**——
//! 这等于在业务进程里再造一份全量副本（5M 行 × 4 列 ≈ 200MB 堆 + 一次全量遍历），
//! 而且无法流式。行式则**边收边写**：攒够一个 chunk 就交给内核，内存恒定。
//!
//! | | `bulk`（[列式](crate::bulk)） | **`rows`（行式，本模块）** |
//! |---|---|---|
//! | 面向 | 列式生产者（Rust/Arrow 生态、嵌入方） | **各类驱动**（JDBC/ODBC/Python） |
//! | 客户端实现量 | ~150 行（要转置） | **~300 行（边收边写）** |
//! | 内存 | O(全量) | **O(一个 chunk)** |
//! | 类型码表 | 1..7 | **1..7 语义完全相同**，8..14 为超集 |
//!
//! ## 布局（全小端）
//!
//! ```text
//! 头：magic "QSDBROWS"(8) | version u16 | flags u8 | n_cols u16
//!   flags bit0 = HAS_SCHEMA
//!   HAS_SCHEMA=1 → 列描述 × n_cols：
//!       name_len u16 | name(UTF-8) | type u8 | col_flags u8 | meta_len u16 | meta
//!       （col_flags bit0 = nullable；meta 内容见 [`RowType`]）
//!   HAS_SCHEMA=0 → 无列描述（解码方自带 schema）
//! 块 × N（直到 EOF）：
//!   row_count u32 | chunk_flags u8
//!   chunk_flags bit0 = HAS_NULLS → n_cols × ceil(row_count/8) 字节位图（列序拼接）
//!   值：**行主序**（行 i 的 列 0..n_cols 依次）
//! ```
//!
//! ## 三条硬约定
//!
//! 1. **写侧不带 schema**（`flags=0`）：表结构由端点给（`/write?table=T`），
//!    payload 只带值。自描述 schema 会让**每个**客户端各写一遍序列化，是驱动侧
//!    最大的一笔成本。带 `HAS_SCHEMA=1` 的写请求**显式 400**（不静默忽略）。
//! 2. **NULL 显式**：块级位图，LSB first。定长列的 NULL 行**仍占位**（值无意义，
//!    写 0），变长列写 `len=0` —— 于是**解析不需要位图**，位图只负责还原空值。
//!    "空串" 与 "NULL" 由位图区分，驱动不必猜。
//! 3. **数值/时间一律定长二进制**（i32 天数、i64 纳秒、i128 十进制），不走文本——
//!    避免再走一遍字符串解析，也规避格式歧义。
//!
//! 解析实现量：任何语言 ~80 行（读一遍头，然后 `while !eof` 逐块）。规范见
//! `docs/rows-format.md`。

use arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryBuilder, BooleanArray, BooleanBuilder, Date32Array,
    Date32Builder, Decimal128Array, Decimal128Builder, Float32Array, Float32Builder, Float64Array,
    Float64Builder, Int16Array, Int16Builder, Int32Array, Int32Builder, Int64Array, Int64Builder,
    Int8Array, Int8Builder, IntervalMonthDayNanoArray, IntervalMonthDayNanoBuilder, StringArray,
    StringBuilder, Time64NanosecondArray, Time64NanosecondBuilder, TimestampNanosecondArray,
    TimestampNanosecondBuilder,
};
use arrow::array::types::IntervalMonthDayNanoType;
use arrow::datatypes::{DataType, Field, IntervalUnit, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

use crate::error::{QsdbError, Result};

const MAGIC: &[u8; 8] = b"QSDBROWS";
const VERSION: u16 = 1;
/// 头 flags：bit0 = 带列描述。
const FLAG_HAS_SCHEMA: u8 = 1;
/// 列 flags：bit0 = 可空。
const COL_FLAG_NULLABLE: u8 = 1;
/// 块 flags：bit0 = 本块含 NULL（随后跟位图）。
const CHUNK_FLAG_HAS_NULLS: u8 = 1;

/// 线上类型标签。**1..=7 与 [`crate::bulk::ColType`] 语义完全相同**（同宽同序），
/// `rows` 是列式的**超集**——两张表只需记一套心智模型。
///
/// **契约级稳定**：已发布编号的语义永不变，新类型只能追加编号。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RowType {
    Bool = 1,
    I32 = 2,
    I64 = 3,
    F64 = 4,
    /// VARCHAR / CHAR / STRING（Arrow 层无区分；定长语义以表定义为准）
    Utf8 = 5,
    Date32 = 6,
    /// i64 纳秒；meta = 时区串（0 长度 = naive）
    TimestampNs = 7,
    I8 = 8,
    I16 = 9,
    F32 = 10,
    Binary = 11,
    /// 16 字节 i128（小端）；meta = precision u8 + scale i8
    Decimal = 12,
    /// i64 纳秒（自午夜）
    Time64Ns = 13,
    /// 16 字节：months i32 + days i32 + nanos i64（= Arrow MonthDayNano 的物理宽度）
    Interval = 14,
}

impl RowType {
    /// 定长列的字节宽度；变长列（Utf8/Binary）返回 `None`。
    fn width(self) -> Option<usize> {
        Some(match self {
            Self::Bool | Self::I8 => 1,
            Self::I16 => 2,
            Self::I32 | Self::F32 | Self::Date32 => 4,
            Self::I64 | Self::F64 | Self::TimestampNs | Self::Time64Ns => 8,
            Self::Decimal => 16,
            Self::Interval => 16,
            Self::Utf8 | Self::Binary => return None,
        })
    }

    fn from_u8(v: u8) -> Result<Self> {
        Ok(match v {
            1 => Self::Bool,
            2 => Self::I32,
            3 => Self::I64,
            4 => Self::F64,
            5 => Self::Utf8,
            6 => Self::Date32,
            7 => Self::TimestampNs,
            8 => Self::I8,
            9 => Self::I16,
            10 => Self::F32,
            11 => Self::Binary,
            12 => Self::Decimal,
            13 => Self::Time64Ns,
            14 => Self::Interval,
            other => {
                return Err(QsdbError::invalid(format!(
                    "rows: unknown column type tag {other} (see docs/rows-format.md)"
                )));
            }
        })
    }
}

/// Arrow 类型 → 标签。不支持的 Arrow 类型**显式拒绝**（不猜、不降级）。
///
/// 无分配——逐值编码的热路径也走这里（宽度由 [`RowType::width`] 给）。
fn tag_of(dt: &DataType) -> Result<RowType> {
    Ok(match dt {
        DataType::Boolean => RowType::Bool,
        DataType::Int8 => RowType::I8,
        DataType::Int16 => RowType::I16,
        DataType::Int32 => RowType::I32,
        DataType::Int64 => RowType::I64,
        DataType::Float32 => RowType::F32,
        DataType::Float64 => RowType::F64,
        DataType::Utf8 => RowType::Utf8,
        DataType::Binary => RowType::Binary,
        DataType::Date32 => RowType::Date32,
        DataType::Time64(TimeUnit::Nanosecond) => RowType::Time64Ns,
        DataType::Timestamp(TimeUnit::Nanosecond, _) => RowType::TimestampNs,
        DataType::Decimal128(_, _) => RowType::Decimal,
        DataType::Interval(IntervalUnit::MonthDayNano) => RowType::Interval,
        other => {
            return Err(QsdbError::unsupported(format!(
                "rows: column type {other:?} not supported (see docs/rows-format.md §二)"
            )));
        },
    })
}

/// Arrow 类型 → (标签, meta)。只在写列描述时调用（一趟一批，非逐值）。
fn type_and_meta(dt: &DataType) -> Result<(RowType, Vec<u8>)> {
    let t = tag_of(dt)?;
    let meta = match dt {
        DataType::Timestamp(_, Some(tz)) => {
            let mut m = Vec::with_capacity(2 + tz.len());
            m.extend_from_slice(&(tz.len() as u16).to_le_bytes());
            m.extend_from_slice(tz.as_bytes());
            m
        },
        DataType::Timestamp(_, None) => 0u16.to_le_bytes().to_vec(),
        DataType::Decimal128(p, s) => vec![*p, *s as u8],
        DataType::Utf8 => 0u32.to_le_bytes().to_vec(), // max_chars：Arrow 层无此信息
        _ => Vec::new(),
    };
    Ok((t, meta))
}

/// (标签, meta) → Arrow 类型。与 [`type_and_meta`] 严格互逆。
fn type_from_meta(t: RowType, meta: &[u8], name: &str) -> Result<DataType> {
    let bad = |what: &str| {
        QsdbError::invalid(format!("rows: column '{name}' {what} (type tag {})", t as u8))
    };
    Ok(match t {
        RowType::Bool => expect_empty(meta, t, name, DataType::Boolean)?,
        RowType::I8 => expect_empty(meta, t, name, DataType::Int8)?,
        RowType::I16 => expect_empty(meta, t, name, DataType::Int16)?,
        RowType::I32 => expect_empty(meta, t, name, DataType::Int32)?,
        RowType::I64 => expect_empty(meta, t, name, DataType::Int64)?,
        RowType::F32 => expect_empty(meta, t, name, DataType::Float32)?,
        RowType::F64 => expect_empty(meta, t, name, DataType::Float64)?,
        RowType::Utf8 => {
            if meta.len() != 4 {
                return Err(bad("VARCHAR meta must be 4 bytes (max_chars u32)"));
            }
            DataType::Utf8
        },
        RowType::Binary => expect_empty(meta, t, name, DataType::Binary)?,
        RowType::Date32 => expect_empty(meta, t, name, DataType::Date32)?,
        RowType::Time64Ns => expect_empty(meta, t, name, DataType::Time64(TimeUnit::Nanosecond))?,
        RowType::TimestampNs => {
            if meta.len() < 2 {
                return Err(bad("TIMESTAMP meta must start with tz length u16"));
            }
            let l = u16::from_le_bytes([meta[0], meta[1]]) as usize;
            if meta.len() != 2 + l {
                return Err(bad("TIMESTAMP tz length mismatch"));
            }
            let tz = if l == 0 {
                None
            } else {
                Some(
                    std::str::from_utf8(&meta[2..])
                        .map_err(|_| bad("TIMESTAMP tz not valid UTF-8"))?
                        .to_string()
                        .into(),
                )
            };
            DataType::Timestamp(TimeUnit::Nanosecond, tz)
        },
        RowType::Decimal => {
            if meta.len() != 2 {
                return Err(bad("DECIMAL meta must be precision u8 + scale i8"));
            }
            DataType::Decimal128(meta[0], meta[1] as i8)
        },
        RowType::Interval => expect_empty(meta, t, name, DataType::Interval(IntervalUnit::MonthDayNano))?,
    })
}

fn expect_empty(meta: &[u8], t: RowType, name: &str, dt: DataType) -> Result<DataType> {
    if !meta.is_empty() {
        return Err(QsdbError::invalid(format!(
            "rows: column '{name}' type tag {} takes no meta ({} bytes given)",
            t as u8,
            meta.len()
        )));
    }
    Ok(dt)
}

// ---------------------------------------------------------------------------
// 编码（读侧：结果集 → rows）
// ---------------------------------------------------------------------------

/// 批 → `qsdb-rows` 负载。**每批一个块**（保留输入的自然分帧；客户端可逐块消费）。
///
/// 头里带列描述（`HAS_SCHEMA=1`）——读侧的列名/类型不是客户端能自己知道的。
pub fn encode(batches: &[RecordBatch]) -> Result<Vec<u8>> {
    let Some(first) = batches.first() else {
        return Err(QsdbError::invalid("rows: empty batches"));
    };
    let schema = first.schema();
    check(batches, &schema)?;
    let mut out: Vec<u8> = Vec::with_capacity(64 + batches.len() * 64);
    write_header(&mut out, schema.fields().len(), Some(&schema))?;
    for b in batches {
        encode_chunk(&mut out, b)?;
    }
    Ok(out)
}

/// 与 [`encode`] 同，但**不写列描述**（`flags=0`）——**写侧形态**：schema 由调用方
/// 与目标表对齐后给出（`/write` 用表定义；见 `docs/rows-format.md` §四）。
///
/// 列名不参与编码，故只按 `schema` 校验列数与列类型。空批列表 → 只有头的负载
/// （零个块，合法）。
pub fn encode_values(schema: &SchemaRef, batches: &[RecordBatch]) -> Result<Vec<u8>> {
    check(batches, schema)?;
    let mut out: Vec<u8> = Vec::with_capacity(64 + batches.len() * 64);
    write_header(&mut out, schema.fields().len(), None)?;
    for b in batches {
        encode_chunk(&mut out, b)?;
    }
    Ok(out)
}

fn check(batches: &[RecordBatch], schema: &SchemaRef) -> Result<()> {
    if schema.fields().is_empty() {
        return Err(QsdbError::invalid("rows: zero columns"));
    }
    for b in batches {
        if b.schema() != *schema {
            return Err(QsdbError::invalid("rows: batches have different schemas"));
        }
    }
    for f in schema.fields() {
        tag_of(f.data_type())
            .map_err(|e| QsdbError::unsupported(format!("rows: column '{}': {e}", f.name())))?;
    }
    Ok(())
}

/// 头 + （可选）列描述。`n_cols` **两种形态都要写**——解码方据此校验外部 schema。
fn write_header(out: &mut Vec<u8>, n_cols: usize, schema: Option<&SchemaRef>) -> Result<()> {
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&VERSION.to_le_bytes());
    out.push(if schema.is_some() { FLAG_HAS_SCHEMA } else { 0 });
    out.extend_from_slice(&(n_cols as u16).to_le_bytes());
    let Some(s) = schema else {
        return Ok(());
    };
    for f in s.fields() {
        let (t, meta) = type_and_meta(f.data_type())?;
        let name = f.name().as_bytes();
        out.extend_from_slice(&(name.len() as u16).to_le_bytes());
        out.extend_from_slice(name);
        out.push(t as u8);
        out.push(if f.is_nullable() { COL_FLAG_NULLABLE } else { 0 });
        out.extend_from_slice(&(meta.len() as u16).to_le_bytes());
        out.extend_from_slice(&meta);
    }
    Ok(())
}

fn encode_chunk(out: &mut Vec<u8>, b: &RecordBatch) -> Result<()> {
    let n = b.num_rows();
    let n_cols = b.num_columns();
    let n32 = u32::try_from(n)
        .map_err(|_| QsdbError::invalid(format!("rows: chunk of {n} rows exceeds u32")))?;
    out.extend_from_slice(&n32.to_le_bytes());
    let has_nulls = (0..n_cols).any(|c| b.column(c).null_count() > 0);
    out.push(if has_nulls { CHUNK_FLAG_HAS_NULLS } else { 0 });
    if has_nulls {
        let nb = n.div_ceil(8);
        let base = out.len();
        out.resize(base + nb * n_cols, 0);
        for c in 0..n_cols {
            let a = b.column(c);
            let col_base = base + c * nb;
            for i in 0..n {
                if a.is_null(i) {
                    out[col_base + i / 8] |= 1 << (i % 8);
                }
            }
        }
    }
    // 值：**行主序**（这是行式格式的全部意义）
    for i in 0..n {
        for c in 0..n_cols {
            write_value(out, b.column(c), i)?;
        }
    }
    Ok(())
}

fn write_value(out: &mut Vec<u8>, a: &ArrayRef, i: usize) -> Result<()> {
    let dt = a.data_type();
    let w = tag_of(dt)?.width(); // 顺带做类型校验（不支持 → 显式报错）
    if a.is_null(i) {
        // 定长：占位（写 0）；变长：len=0。位图才是 NULL 的真值。
        match w {
            Some(w) => out.resize(out.len() + w, 0),
            None => out.extend_from_slice(&0u32.to_le_bytes()),
        }
        return Ok(());
    }
    match dt {
        DataType::Boolean => out.push(u8::from(downcast::<BooleanArray>(a, dt)?.value(i))),
        DataType::Int8 => out.extend_from_slice(&downcast::<Int8Array>(a, dt)?.value(i).to_le_bytes()),
        DataType::Int16 => {
            out.extend_from_slice(&downcast::<Int16Array>(a, dt)?.value(i).to_le_bytes())
        },
        DataType::Int32 => {
            out.extend_from_slice(&downcast::<Int32Array>(a, dt)?.value(i).to_le_bytes())
        },
        DataType::Int64 => {
            out.extend_from_slice(&downcast::<Int64Array>(a, dt)?.value(i).to_le_bytes())
        },
        DataType::Float32 => {
            out.extend_from_slice(&downcast::<Float32Array>(a, dt)?.value(i).to_le_bytes())
        },
        DataType::Float64 => {
            out.extend_from_slice(&downcast::<Float64Array>(a, dt)?.value(i).to_le_bytes())
        },
        DataType::Utf8 => {
            let s = downcast::<StringArray>(a, dt)?.value(i).as_bytes();
            out.extend_from_slice(&(s.len() as u32).to_le_bytes());
            out.extend_from_slice(s);
        },
        DataType::Binary => {
            let s = downcast::<BinaryArray>(a, dt)?.value(i);
            out.extend_from_slice(&(s.len() as u32).to_le_bytes());
            out.extend_from_slice(s);
        },
        DataType::Date32 => {
            out.extend_from_slice(&downcast::<Date32Array>(a, dt)?.value(i).to_le_bytes())
        },
        DataType::Time64(TimeUnit::Nanosecond) => out.extend_from_slice(
            &downcast::<Time64NanosecondArray>(a, dt)?
                .value(i)
                .to_le_bytes(),
        ),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => out.extend_from_slice(
            &downcast::<TimestampNanosecondArray>(a, dt)?
                .value(i)
                .to_le_bytes(),
        ),
        DataType::Decimal128(_, _) => out.extend_from_slice(
            &downcast::<Decimal128Array>(a, dt)?.value(i).to_le_bytes(),
        ),
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            let (m, d, ns) =
                IntervalMonthDayNanoType::to_parts(downcast::<IntervalMonthDayNanoArray>(a, dt)?.value(i));
            out.extend_from_slice(&m.to_le_bytes());
            out.extend_from_slice(&d.to_le_bytes());
            out.extend_from_slice(&ns.to_le_bytes());
        },
        other => {
            return Err(QsdbError::unsupported(format!(
                "rows: column type {other:?} not supported"
            )));
        },
    }
    Ok(())
}

fn downcast<'a, T: 'static>(a: &'a ArrayRef, dt: &DataType) -> Result<&'a T> {
    a.as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| QsdbError::invalid(format!("rows: expected {dt:?} array")))
}

// ---------------------------------------------------------------------------
// 解码（写侧：rows → 批）
// ---------------------------------------------------------------------------

/// 解码**自描述**负载（`HAS_SCHEMA=1`）→ 块列表。
pub fn decode(bytes: &[u8]) -> Result<Vec<RecordBatch>> {
    let mut r = Reader { b: bytes, p: 0 };
    let (flags, n_cols) = read_header(&mut r)?;
    if flags & FLAG_HAS_SCHEMA == 0 {
        return Err(QsdbError::invalid(
            "rows: payload has no schema block — use decode_with_schema (the endpoint supplies it)",
        ));
    }
    let schema = read_schema(&mut r, n_cols)?;
    read_chunks(&mut r, &schema)
}

/// 解码**外部 schema** 负载（`HAS_SCHEMA=0`；写侧：表结构来自端点）。
///
/// `schema` 必须与表列序一致（`/write` 的既有约定：全列批）。
pub fn decode_with_schema(bytes: &[u8], schema: &SchemaRef) -> Result<Vec<RecordBatch>> {
    let mut r = Reader { b: bytes, p: 0 };
    let (flags, n_cols) = read_header(&mut r)?;
    if flags & FLAG_HAS_SCHEMA != 0 {
        return Err(QsdbError::invalid(
            "rows: payload carries a schema block, but /write takes the schema from the table \
             definition — emit flags=0 (see docs/rows-format.md §四)",
        ));
    }
    if n_cols != schema.fields().len() {
        return Err(QsdbError::invalid(format!(
            "rows: payload has {n_cols} columns, table has {}",
            schema.fields().len()
        )));
    }
    // 先校验每列类型受支持（别写到一半才发现）
    for f in schema.fields() {
        type_and_meta(f.data_type()).map_err(|e| {
            QsdbError::invalid(format!("rows: column '{}': {e}", f.name()))
        })?;
    }
    read_chunks(&mut r, schema)
}

fn read_header(r: &mut Reader<'_>) -> Result<(u8, usize)> {
    if r.take(8)? != MAGIC {
        return Err(QsdbError::invalid("rows: bad magic (expect QSDBROWS)"));
    }
    let ver = r.u16()?;
    if ver != VERSION {
        return Err(QsdbError::invalid(format!(
            "rows: unsupported version {ver} (this build speaks {VERSION})"
        )));
    }
    let flags = r.u8()?;
    if flags & !FLAG_HAS_SCHEMA != 0 {
        return Err(QsdbError::invalid(format!(
            "rows: unknown header flags {flags:#x}"
        )));
    }
    let n_cols = r.u16()? as usize;
    if n_cols == 0 {
        return Err(QsdbError::invalid("rows: zero columns"));
    }
    Ok((flags, n_cols))
}

fn read_schema(r: &mut Reader<'_>, n_cols: usize) -> Result<SchemaRef> {
    let mut fields = Vec::with_capacity(n_cols);
    for _ in 0..n_cols {
        let l = r.u16()? as usize;
        let name = String::from_utf8(r.take(l)?.to_vec())
            .map_err(|_| QsdbError::invalid("rows: column name not valid UTF-8"))?;
        let tag = r.u8()?;
        let col_flags = r.u8()?;
        if col_flags & !COL_FLAG_NULLABLE != 0 {
            return Err(QsdbError::invalid(format!(
                "rows: column '{name}' has unknown flags {col_flags:#x}"
            )));
        }
        let ml = r.u16()? as usize;
        let meta = r.take(ml)?;
        let dt = type_from_meta(RowType::from_u8(tag)?, meta, &name)?;
        fields.push(Field::new(name, dt, col_flags & COL_FLAG_NULLABLE != 0));
    }
    Ok(Arc::new(Schema::new(fields)))
}

fn read_chunks(r: &mut Reader<'_>, schema: &SchemaRef) -> Result<Vec<RecordBatch>> {
    let n_cols = schema.fields().len();
    let mut out = Vec::new();
    while !r.done() {
        let n = r.u32()? as usize;
        let cf = r.u8()?;
        if cf & !CHUNK_FLAG_HAS_NULLS != 0 {
            return Err(QsdbError::invalid(format!(
                "rows: unknown chunk flags {cf:#x}"
            )));
        }
        let nb = n.div_ceil(8);
        let bits = if cf & CHUNK_FLAG_HAS_NULLS != 0 {
            Some(r.take(nb * n_cols)?.to_vec())
        } else {
            None
        };
        let mut bs: Vec<ColBuilder> = schema
            .fields()
            .iter()
            .map(|f| ColBuilder::new(f.data_type()))
            .collect::<Result<_>>()?;
        for i in 0..n {
            for c in 0..n_cols {
                let is_null = bits
                    .as_ref()
                    .map(|b| b[c * nb + i / 8] & (1 << (i % 8)) != 0)
                    .unwrap_or(false);
                // **无论空否都要按宽度吃掉字节**——定长列的 NULL 行有占位，
                // "解析不依赖位图" 这条约定就落在这里。
                bs[c].read(r, is_null)?;
            }
        }
        let cols: Vec<ArrayRef> = bs
            .into_iter()
            .zip(schema.fields())
            .map(|(b, f)| b.finish(f.data_type()))
            .collect();
        out.push(
            RecordBatch::try_new_with_options(
                Arc::clone(schema),
                cols,
                &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(n)),
            )
            .map_err(|e| QsdbError::invalid(format!("rows: build batch failed: {e}")))?,
        );
    }
    Ok(out)
}

/// 逐列构造器（enum 静态派发——热路径上不做 `Box<dyn>` 虚调用）。
enum ColBuilder {
    Bool(BooleanBuilder),
    I8(Int8Builder),
    I16(Int16Builder),
    I32(Int32Builder),
    I64(Int64Builder),
    F32(Float32Builder),
    F64(Float64Builder),
    Utf8(StringBuilder),
    Bin(BinaryBuilder),
    Date32(Date32Builder),
    Time64(Time64NanosecondBuilder),
    Ts(TimestampNanosecondBuilder),
    Dec(Decimal128Builder),
    Interval(IntervalMonthDayNanoBuilder),
}

impl ColBuilder {
    fn new(dt: &DataType) -> Result<Self> {
        Ok(match dt {
            DataType::Boolean => Self::Bool(BooleanBuilder::new()),
            DataType::Int8 => Self::I8(Int8Builder::new()),
            DataType::Int16 => Self::I16(Int16Builder::new()),
            DataType::Int32 => Self::I32(Int32Builder::new()),
            DataType::Int64 => Self::I64(Int64Builder::new()),
            DataType::Float32 => Self::F32(Float32Builder::new()),
            DataType::Float64 => Self::F64(Float64Builder::new()),
            DataType::Utf8 => Self::Utf8(StringBuilder::new()),
            DataType::Binary => Self::Bin(BinaryBuilder::new()),
            DataType::Date32 => Self::Date32(Date32Builder::new()),
            DataType::Time64(TimeUnit::Nanosecond) => Self::Time64(Time64NanosecondBuilder::new()),
            DataType::Timestamp(TimeUnit::Nanosecond, _) => Self::Ts(TimestampNanosecondBuilder::new()),
            DataType::Decimal128(p, s) => {
                let mut b = Decimal128Builder::new();
                b = b.with_data_type(DataType::Decimal128(*p, *s));
                Self::Dec(b)
            },
            DataType::Interval(IntervalUnit::MonthDayNano) => {
                Self::Interval(IntervalMonthDayNanoBuilder::new())
            },
            other => {
                return Err(QsdbError::unsupported(format!(
                    "rows: column type {other:?} not supported"
                )));
            },
        })
    }

    /// 读一个值。**`is_null` 为真时仍然吃掉对应字节**（定长占位 / 变长 `len`）——
    /// 于是"解析不依赖位图"，位图只负责把值还原成 NULL。
    fn read(&mut self, r: &mut Reader<'_>, is_null: bool) -> Result<()> {
        macro_rules! fixed {
            ($b:expr, $w:expr, $t:ty) => {{
                let s = r.take($w)?;
                if is_null {
                    $b.append_null();
                } else {
                    let mut a = [0u8; std::mem::size_of::<$t>()];
                    a.copy_from_slice(s);
                    $b.append_value(<$t>::from_le_bytes(a));
                }
            }};
        }
        match self {
            Self::Bool(b) => {
                let v = r.u8()?;
                if is_null {
                    b.append_null();
                } else {
                    b.append_value(v != 0);
                }
            },
            Self::I8(b) => fixed!(b, 1, i8),
            Self::I16(b) => fixed!(b, 2, i16),
            Self::I32(b) => fixed!(b, 4, i32),
            Self::I64(b) => fixed!(b, 8, i64),
            Self::F32(b) => fixed!(b, 4, f32),
            Self::F64(b) => fixed!(b, 8, f64),
            Self::Utf8(b) => {
                let s = r.var()?;
                if is_null {
                    b.append_null();
                } else {
                    b.append_value(std::str::from_utf8(s).map_err(|_| {
                        QsdbError::invalid("rows: string value not valid UTF-8")
                    })?);
                }
            },
            Self::Bin(b) => {
                let s = r.var()?;
                if is_null {
                    b.append_null();
                } else {
                    b.append_value(s);
                }
            },
            Self::Date32(b) => fixed!(b, 4, i32),
            Self::Time64(b) => fixed!(b, 8, i64),
            Self::Ts(b) => fixed!(b, 8, i64),
            Self::Dec(b) => fixed!(b, 16, i128),
            Self::Interval(b) => {
                let s = r.take(16)?;
                if is_null {
                    b.append_null();
                } else {
                    let (m, d) = (
                        i32::from_le_bytes([s[0], s[1], s[2], s[3]]),
                        i32::from_le_bytes([s[4], s[5], s[6], s[7]]),
                    );
                    let ns = i64::from_le_bytes([s[8], s[9], s[10], s[11], s[12], s[13], s[14], s[15]]);
                    b.append_value(IntervalMonthDayNanoType::make_value(m, d, ns));
                }
            },
        }
        Ok(())
    }

    fn finish(self, dt: &DataType) -> ArrayRef {
        match self {
            Self::Bool(mut b) => Arc::new(b.finish()),
            Self::I8(mut b) => Arc::new(b.finish()),
            Self::I16(mut b) => Arc::new(b.finish()),
            Self::I32(mut b) => Arc::new(b.finish()),
            Self::I64(mut b) => Arc::new(b.finish()),
            Self::F32(mut b) => Arc::new(b.finish()),
            Self::F64(mut b) => Arc::new(b.finish()),
            Self::Utf8(mut b) => Arc::new(b.finish()),
            Self::Bin(mut b) => Arc::new(b.finish()),
            Self::Date32(mut b) => Arc::new(b.finish()),
            Self::Time64(mut b) => Arc::new(b.finish()),
            Self::Ts(mut b) => match dt {
                DataType::Timestamp(_, tz) => Arc::new(b.finish().with_timezone_opt(tz.clone())),
                _ => Arc::new(b.finish()),
            },
            Self::Dec(mut b) => Arc::new(b.finish()),
            Self::Interval(mut b) => Arc::new(b.finish()),
        }
    }
}

/// 顺序读的小工具（越界一律报错，不 panic）。
struct Reader<'a> {
    b: &'a [u8],
    p: usize,
}

impl<'a> Reader<'a> {
    fn done(&self) -> bool {
        self.p >= self.b.len()
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self
            .p
            .checked_add(n)
            .filter(|e| *e <= self.b.len())
            .ok_or_else(|| {
                QsdbError::invalid(format!(
                    "rows: truncated payload (need {n} more bytes at offset {})",
                    self.p
                ))
            })?;
        let s = &self.b[self.p..end];
        self.p = end;
        Ok(s)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16> {
        let s = self.take(2)?;
        Ok(u16::from_le_bytes([s[0], s[1]]))
    }

    fn u32(&mut self) -> Result<u32> {
        let s = self.take(4)?;
        Ok(u32::from_le_bytes([s[0], s[1], s[2], s[3]]))
    }

    /// 变长值：u32 长度 + 字节。
    fn var(&mut self) -> Result<&'a [u8]> {
        let n = self.u32()? as usize;
        self.take(n)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Decimal128Array, IntervalMonthDayNanoArray, Time64NanosecondArray};

    fn every_type() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("b", DataType::Boolean, true),
            Field::new("i8", DataType::Int8, true),
            Field::new("i16", DataType::Int16, true),
            Field::new("i32", DataType::Int32, true),
            Field::new("i64", DataType::Int64, true),
            Field::new("f32", DataType::Float32, true),
            Field::new("f64", DataType::Float64, true),
            Field::new("s", DataType::Utf8, true),
            Field::new("bin", DataType::Binary, true),
            Field::new("d", DataType::Date32, true),
            Field::new("t", DataType::Time64(TimeUnit::Nanosecond), true),
            Field::new(
                "ts",
                // 用具名时区（"UTC"）会让 arrow 的 display 要求 chrono-tz；用偏移量足够测 meta
                DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("+08:00"))),
                true,
            ),
            Field::new("dec", DataType::Decimal128(18, 4), true),
            Field::new("iv", DataType::Interval(IntervalUnit::MonthDayNano), true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])), // 0

                Arc::new(Int8Array::from(vec![Some(-1), None, Some(127)])),
                Arc::new(Int16Array::from(vec![Some(-2), Some(0), None])),
                Arc::new(Int32Array::from(vec![None, Some(7), Some(-9)])),
                Arc::new(Int64Array::from(vec![Some(1), None, Some(-3)])),
                Arc::new(Float32Array::from(vec![Some(1.5f32), None, Some(-0.25)])),
                Arc::new(Float64Array::from(vec![Some(1.5), Some(f64::NAN), None])),
                Arc::new(StringArray::from(vec![Some("x"), None, Some("中文-ok")])),
                Arc::new(BinaryArray::from(vec![
                    Some(&b"ab"[..]),
                    None,
                    Some(&[0u8, 255][..]),
                ])),
                Arc::new(Date32Array::from(vec![Some(19_000), None, Some(19_001)])),
                Arc::new(Time64NanosecondArray::from(vec![
                    Some(1_000),
                    None,
                    Some(86_399_999_999_999),
                ])),
                Arc::new(
                    TimestampNanosecondArray::from(vec![Some(1), None, Some(-5)])
                        .with_timezone("+08:00"),
                ),
                Arc::new(
                    Decimal128Array::from(vec![Some(1234567i128), None, Some(-1)])
                        .with_precision_and_scale(18, 4)
                        .unwrap(),
                ),
                Arc::new(IntervalMonthDayNanoArray::from(vec![
                    Some(IntervalMonthDayNanoType::make_value(1, 2, 3)),
                    None,
                    Some(IntervalMonthDayNanoType::make_value(-1, -2, -3)),
                ])),
            ],
        )
        .unwrap()
    }

    fn assert_same(a: &RecordBatch, b: &RecordBatch) {
        assert_eq!(a.schema(), b.schema(), "schema 不一致");
        assert_eq!(a.num_rows(), b.num_rows());
        for c in 0..a.num_columns() {
            for i in 0..a.num_rows() {
                assert_eq!(
                    a.column(c).is_null(i),
                    b.column(c).is_null(i),
                    "列{c} 行{i} NULL 不一致"
                );
                if !a.column(c).is_null(i) {
                    let va = arrow::util::display::array_value_to_string(a.column(c), i).unwrap();
                    let vb = arrow::util::display::array_value_to_string(b.column(c), i).unwrap();
                    assert_eq!(va, vb, "列{c} 行{i} 值不一致");
                }
            }
        }
    }

    #[test]
    fn roundtrip_all_types_with_nulls() {
        let b = every_type();
        let bytes = encode(std::slice::from_ref(&b)).unwrap();
        assert_eq!(&bytes[..8], MAGIC);
        let back = decode(&bytes).unwrap();
        assert_eq!(back.len(), 1);
        assert_same(&b, &back[0]);
    }

    /// 写侧形态：外部 schema + 不带列描述（驱动不产出 schema 块）。
    #[test]
    fn external_schema_write_path_roundtrip() {
        let b = every_type();
        let bytes = encode(std::slice::from_ref(&b)).unwrap();
        let schema_with = decode(&bytes).unwrap()[0].schema();
        // 手工剥掉列描述：重编一份 flags=0 的负载（模拟驱动行为）
        let no_schema = strip_schema(&bytes, &schema_with);
        let back = decode_with_schema(&no_schema, &b.schema()).unwrap();
        assert_same(&b, &back[0]);
    }

    /// 把 `HAS_SCHEMA=1` 的负载重编为 `HAS_SCHEMA=0`（仅测试用）。
    fn strip_schema(bytes: &[u8], schema: &SchemaRef) -> Vec<u8> {
        let mut r = Reader { b: bytes, p: 0 };
        let (_f, n_cols) = read_header(&mut r).unwrap();
        for _ in 0..n_cols {
            let l = r.u16().unwrap() as usize;
            r.take(l).unwrap();
            r.u8().unwrap(); // tag
            r.u8().unwrap(); // flags
            let ml = r.u16().unwrap() as usize;
            r.take(ml).unwrap();
        }
        let mut out = Vec::new();
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&VERSION.to_le_bytes());
        out.push(0);
        out.extend_from_slice(&(schema.fields().len() as u16).to_le_bytes());
        out.extend_from_slice(&bytes[r.p..]);
        out
    }

    #[test]
    fn schema_block_is_rejected_on_the_write_path() {
        let b = every_type();
        let bytes = encode(std::slice::from_ref(&b)).unwrap();
        let e = decode_with_schema(&bytes, &b.schema()).unwrap_err();
        assert!(format!("{e}").contains("schema block"), "实得 {e}");
    }

    #[test]
    fn column_count_mismatch_is_rejected() {
        let b = every_type();
        let bytes = strip_schema(&encode(std::slice::from_ref(&b)).unwrap(), &b.schema());
        let narrow = Arc::new(Schema::new(vec![Field::new("i64", DataType::Int64, true)]));
        let e = decode_with_schema(&bytes, &narrow).unwrap_err();
        assert!(format!("{e}").contains("columns"), "实得 {e}");
    }

    #[test]
    fn multi_chunk_framing_preserved() {
        let b = every_type();
        let b2 = b.slice(0, 2);
        let bytes = encode(&[b.clone(), b2.clone()]).unwrap();
        let back = decode(&bytes).unwrap();
        assert_eq!(back.len(), 2, "每批一个块");
        assert_same(&b, &back[0]);
        assert_same(&b2, &back[1]);
    }

    #[test]
    fn truncated_payload_errors_not_panic() {
        let b = every_type();
        let bytes = encode(std::slice::from_ref(&b)).unwrap();
        for cut in 1..8 {
            let e = decode(&bytes[..bytes.len() - cut]).unwrap_err();
            assert!(format!("{e}").contains("rows:"), "实得 {e}");
        }
        assert!(decode(b"NOTROWS!!").is_err());
    }

    #[test]
    fn unsupported_type_is_rejected_explicitly() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8View, true)]));
        let b = RecordBatch::try_new(
            schema,
            vec![Arc::new(
                arrow::array::StringViewArray::from(vec!["a"]),
            )],
        )
        .unwrap();
        let e = encode(std::slice::from_ref(&b)).unwrap_err();
        assert!(format!("{e}").contains("not supported"), "实得 {e}");
    }

    /// **把 `docs/rows-format.md` §三.3 的 85 字节示例逐字节钉住**——
    /// 规范里那段十六进制是给第三方照着手写实现的，必须与内核解码器一致
    /// （改格式先改这里，再改文档）。
    #[test]
    fn spec_example_from_docs() {
        let hex = "\
            51 53 44 42 52 4f 57 53 01 00 00 03 00 02 00 00 \
            00 01 02 02 02 01 00 00 00 00 00 00 00 05 00 00 \
            00 61 6c 69 63 65 38 4a 00 00 00 00 00 00 00 00 \
            00 00 00 00 00 00 00 00 00 00 01 00 00 00 00 03 \
            00 00 00 00 00 00 00 06 00 00 00 e4 b8 ad e6 96 \
            87 39 4a 00 00";
        let bytes: Vec<u8> = hex
            .split_whitespace()
            .map(|h| u8::from_str_radix(h, 16).unwrap())
            .collect();
        assert_eq!(bytes.len(), 85, "文档里写的是 85 字节");

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("dt", DataType::Date32, true),
        ]));
        let chunks = decode_with_schema(&bytes, &schema).unwrap();
        assert_eq!(chunks.len(), 2, "两个块");
        assert_eq!(chunks[0].num_rows(), 2);
        assert_eq!(chunks[1].num_rows(), 1);
        let col = |b: &RecordBatch, c: usize, i: usize| {
            if b.column(c).is_null(i) {
                "NULL".to_string()
            } else {
                arrow::util::display::array_value_to_string(b.column(c), i).unwrap()
            }
        };
        assert_eq!(col(&chunks[0], 0, 0), "1");
        assert_eq!(col(&chunks[0], 1, 0), "alice");
        assert_eq!(col(&chunks[0], 2, 0), "2022-01-08");
        for c in 0..3 {
            assert_eq!(col(&chunks[0], c, 1), "NULL", "块1 第1行应整行 NULL");
        }
        assert_eq!(col(&chunks[1], 0, 0), "3");
        assert_eq!(col(&chunks[1], 1, 0), "中文");
        assert_eq!(col(&chunks[1], 2, 0), "2022-01-09");
    }

    #[test]
    fn decimal_scale_survives_the_roundtrip() {
        // scale=4 的 1234567 → 123.4567（若把 meta 读错成 i8 无符号会整体换值）
        let b = every_type();
        let back = decode(&encode(std::slice::from_ref(&b)).unwrap()).unwrap();
        let s = arrow::util::display::array_value_to_string(back[0].column(12), 0).unwrap();
        assert_eq!(s, "123.4567", "实得 {s}");
    }
}
