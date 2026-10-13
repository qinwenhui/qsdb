//! **`qsdb-bulk`：无依赖列式批量格式**（读写双向）。
//!
//! ## 为什么要有它
//!
//! `/api/v1/write` 与 `/api/v1/query` 原本只认 **Arrow IPC**。IPC 的元数据是
//! FlatBuffers——Java 客户端要产出/解析它就得引 **arrow-java（JDK 11+、FlatBuffers、
//! Jackson 一串传递依赖）**，这正是生态工具（qsdb-jdbc）拒 Flight/gRPC 的同一个理由。
//! 结果是：**驱动拿不到"快通道"，只能把 N 条 SQL 语句发过来**（实测 5M 行 81s，
//! 而二进制通道 2.18s——**37×**）。
//!
//! 结论：把"二进制批量"与"Arrow 库"解绑。本格式就是**Arrow body 去掉 FlatBuffers**：
//! 一遍顺序读、定长小端、显式长度、无 varint、无压缩——**任何语言手写 ~150 行即可**，
//! 且定长列在两侧都能零拷贝。
//!
//! ## 布局（全小端）
//!
//! ```text
//! magic     8 字节  "QSDBBULK"
//! version   u16     当前 1
//! n_cols    u16
//! n_rows    u64
//! 列描述 × n_cols（顺序即列序）：
//!   name_len u16 + name(UTF-8)
//!   type     u8      见 [`ColType`]
//! 列数据 × n_cols（顺序同上）：
//!   null_len u64     0 = 本列无 NULL；否则 n_rows 位（LSB first）的位图
//!   data_len u64     定长列 = n_rows × 宽度（**必须**相符）；Utf8 = 数据段字节数
//!   offsets  u32 × (n_rows + 1)   仅 Utf8：相对 data 段的起止（最后一个 = data_len）
//!   data     data_len 字节        定长：小端定值；Utf8：UTF-8 串接
//! ```
//!
//! 约定：**同一列的 NULL 行在定长列里仍有占位字节**（值无意义），字符串则为空区间
//! （`offsets[i] == offsets[i+1]`）——这样解码端不需要"压缩行号"这类花活。
//!
//! 类型映射见 [`ColType`]；不在这张表里的 Arrow 类型 → 报错（显式拒绝，不猜）。

use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Float64Array, Int32Array, Int64Array, StringArray,
    TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

use crate::error::{QsdbError, Result};

const MAGIC: &[u8; 8] = b"QSDBBULK";
const VERSION: u16 = 1;

/// 线上类型标签（**契约级稳定**：加类型只能追加编号）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ColType {
    Bool = 1,
    I32 = 2,
    I64 = 3,
    F64 = 4,
    Utf8 = 5,
    Date32 = 6,
    TimestampNs = 7,
}

impl ColType {
    fn from_arrow(dt: &DataType) -> Option<Self> {
        Some(match dt {
            DataType::Boolean => Self::Bool,
            DataType::Int32 => Self::I32,
            DataType::Int64 => Self::I64,
            DataType::Float64 => Self::F64,
            DataType::Utf8 => Self::Utf8,
            DataType::Date32 => Self::Date32,
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, _) => Self::TimestampNs,
            _ => return None,
        })
    }

    fn to_arrow(self) -> DataType {
        match self {
            Self::Bool => DataType::Boolean,
            Self::I32 => DataType::Int32,
            Self::I64 => DataType::Int64,
            Self::F64 => DataType::Float64,
            Self::Utf8 => DataType::Utf8,
            Self::Date32 => DataType::Date32,
            Self::TimestampNs => DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
        }
    }

    fn width(self) -> Option<u64> {
        Some(match self {
            Self::Bool => 1,
            Self::I32 | Self::Date32 => 4,
            Self::I64 | Self::F64 | Self::TimestampNs => 8,
            Self::Utf8 => return None,
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
            other => {
                return Err(QsdbError::invalid(format!(
                    "bulk: unknown column type {other}"
                )));
            }
        })
    }
}

// ---------------------------------------------------------------------------
// 编码
// ---------------------------------------------------------------------------

/// 全部批编码成一份 `qsdb-bulk` 负载（各批必须同 schema）。
///
/// `schema` 由调用方给出（**零行结果**也是合法输入：列描述照写、`n_rows = 0`）——
/// 与 [`crate::rows::encode`] 同一口径，见那里的说明。
pub fn encode(schema: &SchemaRef, batches: &[RecordBatch]) -> Result<Vec<u8>> {
    let n_cols = schema.fields().len();
    let n_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

    // 列类型（先全量校验，别写一半才发现不支持）
    let types: Vec<ColType> = schema
        .fields()
        .iter()
        .map(|f| {
            ColType::from_arrow(f.data_type()).ok_or_else(|| {
                QsdbError::unsupported(format!(
                    "bulk: column '{}' type {:?} not supported (see qsdb-bulk spec)",
                    f.name(),
                    f.data_type()
                ))
            })
        })
        .collect::<Result<_>>()?;
    for b in batches {
        if b.schema() != *schema {
            return Err(QsdbError::invalid("bulk: batches have different schemas"));
        }
    }

    let mut out: Vec<u8> = Vec::with_capacity(64 + n_rows * n_cols * 8);
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&VERSION.to_le_bytes());
    out.extend_from_slice(&(n_cols as u16).to_le_bytes());
    out.extend_from_slice(&(n_rows as u64).to_le_bytes());
    for (f, t) in schema.fields().iter().zip(&types) {
        let name = f.name().as_bytes();
        out.extend_from_slice(&(name.len() as u16).to_le_bytes());
        out.extend_from_slice(name);
        out.push(*t as u8);
    }
    for (c, t) in (0..n_cols).zip(&types) {
        encode_column(&mut out, batches, c, *t, n_rows)?;
    }
    Ok(out)
}

fn encode_column(
    out: &mut Vec<u8>,
    batches: &[RecordBatch],
    c: usize,
    ty: ColType,
    n_rows: usize,
) -> Result<()> {
    // NULL 位图（任一批有 null 才写）
    let any_null = batches.iter().any(|b| b.column(c).null_count() > 0);
    if any_null {
        let mut bits = vec![0u8; n_rows.div_ceil(8)];
        // 字段是**位图字节数**（0 = 无 NULL）——不是行数；两侧口径必须一致
        out.extend_from_slice(&(bits.len() as u64).to_le_bytes());
        let mut row = 0usize;
        for b in batches {
            let a = b.column(c);
            for i in 0..b.num_rows() {
                if a.is_null(i) {
                    bits[row / 8] |= 1 << (row % 8);
                }
                row += 1;
            }
        }
        out.extend_from_slice(&bits);
    } else {
        out.extend_from_slice(&0u64.to_le_bytes());
    }

    match ty {
        ColType::Utf8 => {
            // offsets + data
            let mut data: Vec<u8> = Vec::new();
            let mut offsets: Vec<u32> = Vec::with_capacity(n_rows + 1);
            offsets.push(0);
            for b in batches {
                let a = b
                    .column(c)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| QsdbError::invalid("bulk: expected Utf8"))?;
                for i in 0..a.len() {
                    if !a.is_null(i) {
                        data.extend_from_slice(a.value(i).as_bytes());
                    }
                    offsets.push(data.len() as u32);
                }
            }
            out.extend_from_slice(&(data.len() as u64).to_le_bytes());
            for o in &offsets {
                out.extend_from_slice(&o.to_le_bytes());
            }
            out.extend_from_slice(&data);
        }
        _ => {
            let w = ty.width().expect("fixed width");
            out.extend_from_slice(&(n_rows as u64 * w).to_le_bytes());
            for b in batches {
                let a = b.column(c);
                for i in 0..b.num_rows() {
                    // NULL 行也占位（值无意义）——解码端按位图还原
                    if a.is_null(i) {
                        out.extend_from_slice(&vec![0u8; w as usize]);
                        continue;
                    }
                    match ty {
                        ColType::Bool => out.push(u8::from(
                            a.as_any().downcast_ref::<BooleanArray>().unwrap().value(i),
                        )),
                        ColType::I32 => out.extend_from_slice(
                            &a.as_any()
                                .downcast_ref::<Int32Array>()
                                .unwrap()
                                .value(i)
                                .to_le_bytes(),
                        ),
                        ColType::I64 => out.extend_from_slice(
                            &a.as_any()
                                .downcast_ref::<Int64Array>()
                                .unwrap()
                                .value(i)
                                .to_le_bytes(),
                        ),
                        ColType::F64 => out.extend_from_slice(
                            &a.as_any()
                                .downcast_ref::<Float64Array>()
                                .unwrap()
                                .value(i)
                                .to_le_bytes(),
                        ),
                        ColType::Date32 => out.extend_from_slice(
                            &a.as_any()
                                .downcast_ref::<Date32Array>()
                                .unwrap()
                                .value(i)
                                .to_le_bytes(),
                        ),
                        ColType::TimestampNs => out.extend_from_slice(
                            &a.as_any()
                                .downcast_ref::<TimestampNanosecondArray>()
                                .unwrap()
                                .value(i)
                                .to_le_bytes(),
                        ),
                        ColType::Utf8 => unreachable!(),
                    }
                }
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// 解码
// ---------------------------------------------------------------------------

/// 解码 `qsdb-bulk` 负载 → 批（**单批**：整份负载一批；调用方按需再切）。
pub fn decode(bytes: &[u8]) -> Result<RecordBatch> {
    let mut r = Reader { b: bytes, p: 0 };
    if r.take(8)? != MAGIC {
        return Err(QsdbError::invalid("bulk: bad magic"));
    }
    let ver = r.u16()?;
    if ver != VERSION {
        return Err(QsdbError::invalid(format!(
            "bulk: unsupported version {ver}"
        )));
    }
    let n_cols = r.u16()? as usize;
    let n_rows = r.u64()? as usize;
    let mut names: Vec<String> = Vec::with_capacity(n_cols);
    let mut types: Vec<ColType> = Vec::with_capacity(n_cols);
    for _ in 0..n_cols {
        let l = r.u16()? as usize;
        names.push(
            String::from_utf8(r.take(l)?.to_vec())
                .map_err(|_| QsdbError::invalid("bulk: column name not valid UTF-8"))?,
        );
        types.push(ColType::from_u8(r.u8()?)?);
    }
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(n_cols);
    for (name, ty) in names.iter().zip(&types) {
        cols.push(decode_column(&mut r, *ty, n_rows, name)?);
    }
    let fields: Vec<Field> = names
        .iter()
        .zip(&types)
        .map(|(n, t)| Field::new(n, t.to_arrow(), true))
        .collect();
    let schema: SchemaRef = Arc::new(Schema::new(fields));
    RecordBatch::try_new_with_options(
        schema,
        cols,
        &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(n_rows)),
    )
    .map_err(|e| QsdbError::invalid(format!("bulk: build batch failed: {e}")))
}

fn decode_column(r: &mut Reader<'_>, ty: ColType, n_rows: usize, name: &str) -> Result<ArrayRef> {
    let null_len = r.u64()? as usize;
    let has_null = null_len > 0;
    let bits = if has_null {
        if null_len != n_rows.div_ceil(8) {
            return Err(QsdbError::invalid(format!(
                "bulk: column '{name}' null bitmap length {null_len} != {}",
                n_rows.div_ceil(8)
            )));
        }
        Some(r.take(null_len)?.to_vec())
    } else {
        None
    };
    let data_len = r.u64()? as usize;

    let is_null = |i: usize| -> bool {
        bits.as_ref()
            .map(|b| b[i / 8] & (1 << (i % 8)) != 0)
            .unwrap_or(false)
    };

    Ok(match ty {
        ColType::Utf8 => {
            let off_bytes = r.take((n_rows + 1) * 4)?;
            let mut offsets: Vec<i32> = Vec::with_capacity(n_rows + 1);
            for k in 0..=n_rows {
                offsets.push(i32::from_le_bytes([
                    off_bytes[k * 4],
                    off_bytes[k * 4 + 1],
                    off_bytes[k * 4 + 2],
                    off_bytes[k * 4 + 3],
                ]));
            }
            let data = r.take(data_len)?;
            let mut b = arrow::array::StringBuilder::with_capacity(n_rows, data_len);
            for i in 0..n_rows {
                if is_null(i) {
                    b.append_null();
                } else {
                    let (s, e) = (offsets[i] as usize, offsets[i + 1] as usize);
                    let slice = data.get(s..e).ok_or_else(|| {
                        QsdbError::invalid(format!("bulk: column '{name}' offsets out of range"))
                    })?;
                    b.append_value(std::str::from_utf8(slice).map_err(|_| {
                        QsdbError::invalid(format!("bulk: column '{name}' not valid UTF-8"))
                    })?);
                }
            }
            Arc::new(b.finish())
        }
        _ => {
            let w = ty.width().expect("fixed width") as usize;
            if data_len != n_rows * w {
                return Err(QsdbError::invalid(format!(
                    "bulk: column '{name}' data length {data_len} != n_rows*w {}",
                    n_rows * w
                )));
            }
            let data = r.take(data_len)?;
            macro_rules! fixed {
                ($arr:ty, $conv:expr) => {{
                    let mut b = <$arr>::with_capacity(n_rows);
                    for i in 0..n_rows {
                        if is_null(i) {
                            b.append_null();
                        } else {
                            #[allow(clippy::redundant_closure_call)]
                            b.append_value($conv(&data[i * w..(i + 1) * w]));
                        }
                    }
                    Arc::new(b.finish()) as ArrayRef
                }};
            }
            match ty {
                ColType::Bool => {
                    let mut b = arrow::array::BooleanBuilder::with_capacity(n_rows);
                    for (i, byte) in data.iter().enumerate().take(n_rows) {
                        if is_null(i) {
                            b.append_null();
                        } else {
                            b.append_value(*byte != 0);
                        }
                    }
                    Arc::new(b.finish())
                }
                ColType::I32 => {
                    fixed!(arrow::array::Int32Builder, |s: &[u8]| i32::from_le_bytes([
                        s[0], s[1], s[2], s[3]
                    ]))
                }
                ColType::Date32 => {
                    fixed!(arrow::array::Date32Builder, |s: &[u8]| i32::from_le_bytes(
                        [s[0], s[1], s[2], s[3]]
                    ))
                }
                ColType::I64 => {
                    fixed!(arrow::array::Int64Builder, |s: &[u8]| i64::from_le_bytes([
                        s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7]
                    ]))
                }
                ColType::F64 => fixed!(
                    arrow::array::Float64Builder,
                    |s: &[u8]| f64::from_le_bytes([s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7]])
                ),
                ColType::TimestampNs => {
                    fixed!(arrow::array::TimestampNanosecondBuilder, |s: &[u8]| {
                        i64::from_le_bytes([s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7]])
                    })
                }
                ColType::Utf8 => unreachable!(),
            }
        }
    })
}

/// 顺序读的小工具（越界一律报错，不 panic）。
struct Reader<'a> {
    b: &'a [u8],
    p: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self
            .p
            .checked_add(n)
            .filter(|e| *e <= self.b.len())
            .ok_or_else(|| QsdbError::invalid("bulk: truncated payload"))?;
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
    fn u64(&mut self) -> Result<u64> {
        let s = self.take(8)?;
        Ok(u64::from_le_bytes([
            s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7],
        ]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};

    fn sample() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("s", DataType::Utf8, true),
            Field::new("d", DataType::Date32, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![Some(1), None, Some(-3)])),
                Arc::new(StringArray::from(vec![Some("x"), None, Some("中文-ok")])),
                Arc::new(Date32Array::from(vec![Some(19_000), None, Some(19_001)])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn roundtrip_preserves_values_and_nulls() {
        let b = sample();
        let bytes = encode(&b.schema(), std::slice::from_ref(&b)).unwrap();
        assert_eq!(&bytes[..8], MAGIC);
        let back = decode(&bytes).unwrap();
        assert_eq!(back.num_rows(), 3);
        assert_eq!(back.schema(), b.schema());
        for c in 0..b.num_columns() {
            for i in 0..b.num_rows() {
                assert_eq!(
                    b.column(c).is_null(i),
                    back.column(c).is_null(i),
                    "列{c} 行{i}"
                );
                if !b.column(c).is_null(i) {
                    assert_eq!(
                        arrow::util::display::array_value_to_string(b.column(c), i).unwrap(),
                        arrow::util::display::array_value_to_string(back.column(c), i).unwrap()
                    );
                }
            }
        }
    }

    #[test]
    fn unsupported_type_is_rejected_explicitly() {
        let schema = Arc::new(Schema::new(vec![Field::new("f", DataType::Float32, true)]));
        let b = RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow::array::Float32Array::from(vec![1.0f32]))],
        )
        .unwrap();
        let e = encode(&b.schema(), std::slice::from_ref(&b)).unwrap_err();
        assert!(format!("{e}").contains("not supported"), "实得 {e}");
    }

    /// **J15 同族**：零行结果（列描述照写、`n_rows = 0`）必须能往返。
    #[test]
    fn zero_row_result_roundtrip() {
        let b = sample();
        let schema = b.schema();
        let bytes = encode(&schema, &[]).unwrap();
        let back = decode(&bytes).unwrap();
        assert_eq!(back.num_rows(), 0);
        assert_eq!(back.schema().fields().len(), schema.fields().len());
        assert_eq!(
            back.schema()
                .fields()
                .iter()
                .map(|f| f.name())
                .collect::<Vec<_>>(),
            schema.fields().iter().map(|f| f.name()).collect::<Vec<_>>()
        );
    }

    #[test]
    fn truncated_payload_errors_not_panic() {
        let b = sample();
        let bytes = encode(&b.schema(), std::slice::from_ref(&b)).unwrap();
        let e = decode(&bytes[..bytes.len() - 3]).unwrap_err();
        assert!(format!("{e}").contains("bulk:"), "实得 {e}");
        assert!(decode(b"NOTBULK!!").is_err());
    }
}
