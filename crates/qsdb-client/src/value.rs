//! 线上类型标签与值。
//!
//! 标签编号是**契约级稳定**的（加类型只能追加编号，已发布编号的语义永不变）：
//! `bulk` 用 1..7，`rows` 用 1..14 且 **1..7 与 `bulk` 完全同义同宽**——
//! 两个格式只需记一套心智模型（见 `docs/bulk-format.md` / `docs/rows-format.md`）。
//!
//! [`ValueRef`] 是**零拷贝**视图（借自响应体），[`Value`] 是物化版本。
//! 定长列取值走 `from_le_bytes`，不做对齐假设（响应体里的数据段没有对齐承诺）。

use crate::error::{Error, Result, decode_err};

// ---------------------------------------------------------------------------
// 标签
// ---------------------------------------------------------------------------

/// 线上类型标签。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Tag {
    /// 1：布尔（1 字节，非 0 为真）。
    Bool = 1,
    /// 2：32 位有符号整数。
    I32 = 2,
    /// 3：64 位有符号整数。
    I64 = 3,
    /// 4：IEEE754 双精度。
    F64 = 4,
    /// 5：UTF-8 字符串。
    Utf8 = 5,
    /// 6：自 1970-01-01 的**天数**（i32）。
    Date32 = 6,
    /// 7：自 epoch 的**纳秒**（i64）。
    TimestampNs = 7,
    /// 8：8 位有符号整数（bulk/rows 共用编号）。
    I8 = 8,
    /// 9：16 位有符号整数（bulk/rows 共用编号）。
    I16 = 9,
    /// 10：IEEE754 单精度（bulk/rows 共用编号）。
    F32 = 10,
    /// 11：二进制（bulk/rows 共用编号）。
    Binary = 11,
    /// 12：十进制（未缩放 i128 + 列级 precision/scale；bulk/rows 共用编号）。
    Decimal128 = 12,
    /// 13：自午夜的纳秒（bulk/rows 共用编号）。
    Time64Ns = 13,
    /// 14：`months i32 + days i32 + nanos i64`（rows；bulk 无对应内核列类型）。
    Interval = 14,
}

impl Tag {
    /// 线上字节值。
    pub fn code(self) -> u8 {
        self as u8
    }

    /// 从线上字节值解析（**不认识的编号显式报错，不猜**）。
    pub fn from_code(b: u8) -> Result<Self> {
        Ok(match b {
            1 => Tag::Bool,
            2 => Tag::I32,
            3 => Tag::I64,
            4 => Tag::F64,
            5 => Tag::Utf8,
            6 => Tag::Date32,
            7 => Tag::TimestampNs,
            8 => Tag::I8,
            9 => Tag::I16,
            10 => Tag::F32,
            11 => Tag::Binary,
            12 => Tag::Decimal128,
            13 => Tag::Time64Ns,
            14 => Tag::Interval,
            other => {
                return Err(Error::Decode(format!(
                    "未知类型标签 {other}（码表见 docs/rows-format.md §二）"
                )));
            }
        })
    }

    /// 人读名（**仅用于显示**；判分支请用枚举本身）。
    pub fn name(self) -> &'static str {
        match self {
            Tag::Bool => "BOOLEAN",
            Tag::I8 => "TINYINT",
            Tag::I16 => "SMALLINT",
            Tag::I32 => "INTEGER",
            Tag::I64 => "BIGINT",
            Tag::F32 => "FLOAT",
            Tag::F64 => "DOUBLE",
            Tag::Utf8 => "VARCHAR",
            Tag::Binary => "BLOB",
            Tag::Date32 => "DATE",
            Tag::Time64Ns => "TIME",
            Tag::TimestampNs => "TIMESTAMP",
            Tag::Decimal128 => "DECIMAL",
            Tag::Interval => "INTERVAL",
        }
    }

    /// 变长（`Utf8` / `Binary`）——线上是 `u32 长度 + 字节`。
    pub fn is_variable(self) -> bool {
        matches!(self, Tag::Utf8 | Tag::Binary)
    }

    /// 定长宽度（变长列返回 `None`）。
    pub fn fixed_width(self) -> Option<usize> {
        Some(match self {
            Tag::Bool | Tag::I8 => 1,
            Tag::I16 => 2,
            Tag::I32 | Tag::F32 | Tag::Date32 => 4,
            Tag::I64 | Tag::F64 | Tag::TimestampNs | Tag::Time64Ns => 8,
            Tag::Decimal128 | Tag::Interval => 16,
            Tag::Utf8 | Tag::Binary => return None,
        })
    }
}

// ---------------------------------------------------------------------------
// 列描述
// ---------------------------------------------------------------------------

/// 列描述（响应头里的列元数据）。
///
/// `bulk` 只带名字与标签；`rows` 另带 `nullable` 与三类列级元数据
/// （字符串上限 / 时区 / 十进制精度）。这里用同一结构承载，缺省为 `None`/`false`。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDesc {
    /// 列名。
    pub name: String,
    /// 线上类型。
    pub tag: Tag,
    /// 是否可空（`rows` 有；`bulk` 恒 `false`——可空性由各列的 `null_bytes` 表达）。
    pub nullable: bool,
    /// `Utf8`：字符数上限（`0` = 无上限）。
    pub max_chars: Option<u32>,
    /// `TimestampNs`：时区名（`None` = 无时区）。
    pub tz: Option<String>,
    /// `Decimal128`：`(precision, scale)`。
    pub decimal: Option<(u8, i8)>,
}

impl ColumnDesc {
    /// 只有名字与标签的列描述（`bulk` 形态）。
    pub fn new(name: impl Into<String>, tag: Tag) -> Self {
        Self {
            name: name.into(),
            tag,
            nullable: false,
            max_chars: None,
            tz: None,
            decimal: None,
        }
    }
}

/// `/api/v1/tables/{t}/schema` 与 `?withSchema=1` 返回的列信息（**元数据面**）。
///
/// 与 [`ColumnDesc`] 的区别：这里 `type_name` 是服务端给的 Arrow 类型名
/// （如 `Int64`、`Timestamp(Nanosecond, None)`），**不做映射**——
/// 元数据面就该原样透出，猜成标签反而会丢信息。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnInfo {
    /// 列名。
    pub name: String,
    /// 服务端类型名。
    pub type_name: String,
    /// 是否可空。
    pub nullable: bool,
    /// 是否分区列（分区列的值在目录名里，不在数据文件里）。
    pub partition: bool,
}

// ---------------------------------------------------------------------------
// 值
// ---------------------------------------------------------------------------

/// 物化的值。
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// NULL。
    Null,
    /// 布尔。
    Bool(bool),
    /// 8 位整数。
    I8(i8),
    /// 16 位整数。
    I16(i16),
    /// 32 位整数。
    I32(i32),
    /// 64 位整数。
    I64(i64),
    /// 单精度。
    F32(f32),
    /// 双精度。
    F64(f64),
    /// 字符串。
    Utf8(String),
    /// 二进制。
    Binary(Vec<u8>),
    /// 自 1970-01-01 的天数。
    Date32(i32),
    /// 自午夜的纳秒。
    Time64Ns(i64),
    /// 自 epoch 的纳秒（时区在列描述里）。
    TimestampNs(i64),
    /// 未缩放整数（`precision`/`scale` 在列描述里）。
    Decimal128(i128),
    /// 区间。
    Interval {
        /// 月数。
        months: i32,
        /// 天数。
        days: i32,
        /// 纳秒数。
        nanos: i64,
    },
}

impl Value {
    /// 是否 NULL。
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// 取整数视图（整数族与布尔；**不做浮点截断**——那类"看着能跑"的转换
    /// 会把 `1.9` 静默变成 `1`）。
    pub fn as_i64(&self) -> Option<i64> {
        Some(match self {
            Value::I8(v) => i64::from(*v),
            Value::I16(v) => i64::from(*v),
            Value::I32(v) => i64::from(*v),
            Value::I64(v) => *v,
            Value::Bool(b) => i64::from(*b),
            Value::Date32(v) => i64::from(*v),
            Value::Time64Ns(v) | Value::TimestampNs(v) => *v,
            _ => return None,
        })
    }

    /// 取浮点视图（浮点族与整数族；整数到浮点是精确的**可能**丢精度，
    /// 由调用方决定，见 `as_i64` 的反向不做）。
    pub fn as_f64(&self) -> Option<f64> {
        Some(match self {
            Value::F32(v) => f64::from(*v),
            Value::F64(v) => *v,
            Value::I8(v) => f64::from(*v),
            Value::I16(v) => f64::from(*v),
            Value::I32(v) => f64::from(*v),
            Value::I64(v) => *v as f64,
            _ => return None,
        })
    }

    /// 取字符串（**只有 `Utf8`**；二进制请用 [`Value::as_bytes`]）。
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::Utf8(s) => Some(s),
            _ => None,
        }
    }

    /// 取二进制。
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Value::Binary(b) => Some(b),
            Value::Utf8(s) => Some(s.as_bytes()),
            _ => None,
        }
    }

    /// 取布尔。
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            _ => None,
        }
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => f.write_str("NULL"),
            Value::Bool(b) => write!(f, "{b}"),
            Value::I8(v) => write!(f, "{v}"),
            Value::I16(v) => write!(f, "{v}"),
            Value::I32(v) => write!(f, "{v}"),
            Value::I64(v) => write!(f, "{v}"),
            Value::F32(v) => write!(f, "{v}"),
            Value::F64(v) => write!(f, "{v}"),
            Value::Utf8(s) => f.write_str(s),
            Value::Binary(b) => {
                for x in b {
                    write!(f, "{x:02x}")?;
                }
                Ok(())
            }
            Value::Date32(v) => write!(f, "{v}"),
            Value::Time64Ns(v) | Value::TimestampNs(v) => write!(f, "{v}"),
            Value::Decimal128(v) => write!(f, "{v}"),
            Value::Interval {
                months,
                days,
                nanos,
            } => write!(f, "{months}mo {days}d {nanos}ns"),
        }
    }
}

/// **零拷贝**值视图（借自响应体）。
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ValueRef<'a> {
    /// NULL。
    Null,
    /// 布尔。
    Bool(bool),
    /// 8 位整数。
    I8(i8),
    /// 16 位整数。
    I16(i16),
    /// 32 位整数。
    I32(i32),
    /// 64 位整数。
    I64(i64),
    /// 单精度。
    F32(f32),
    /// 双精度。
    F64(f64),
    /// 字符串（已验证 UTF-8）。
    Utf8(&'a str),
    /// 二进制。
    Binary(&'a [u8]),
    /// 自 1970-01-01 的天数。
    Date32(i32),
    /// 自午夜的纳秒。
    Time64Ns(i64),
    /// 自 epoch 的纳秒。
    TimestampNs(i64),
    /// 未缩放整数。
    Decimal128(i128),
    /// 区间。
    Interval {
        /// 月数。
        months: i32,
        /// 天数。
        days: i32,
        /// 纳秒数。
        nanos: i64,
    },
}

impl std::fmt::Display for ValueRef<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // 与 `Value` 的显示逐字一致（"零拷贝视图"不该有另一套打印规则）
        match self {
            ValueRef::Null => f.write_str("NULL"),
            ValueRef::Bool(b) => write!(f, "{b}"),
            ValueRef::I8(v) => write!(f, "{v}"),
            ValueRef::I16(v) => write!(f, "{v}"),
            ValueRef::I32(v) => write!(f, "{v}"),
            ValueRef::I64(v) => write!(f, "{v}"),
            ValueRef::F32(v) => write!(f, "{v}"),
            ValueRef::F64(v) => write!(f, "{v}"),
            ValueRef::Utf8(s) => f.write_str(s),
            ValueRef::Binary(b) => {
                for x in *b {
                    write!(f, "{x:02x}")?;
                }
                Ok(())
            }
            ValueRef::Date32(v) => write!(f, "{v}"),
            ValueRef::Time64Ns(v) | ValueRef::TimestampNs(v) => write!(f, "{v}"),
            ValueRef::Decimal128(v) => write!(f, "{v}"),
            ValueRef::Interval {
                months,
                days,
                nanos,
            } => write!(f, "{months}mo {days}d {nanos}ns"),
        }
    }
}

impl ValueRef<'_> {
    /// 物化成 [`Value`]。
    pub fn to_value(self) -> Value {
        match self {
            ValueRef::Null => Value::Null,
            ValueRef::Bool(v) => Value::Bool(v),
            ValueRef::I8(v) => Value::I8(v),
            ValueRef::I16(v) => Value::I16(v),
            ValueRef::I32(v) => Value::I32(v),
            ValueRef::I64(v) => Value::I64(v),
            ValueRef::F32(v) => Value::F32(v),
            ValueRef::F64(v) => Value::F64(v),
            ValueRef::Utf8(s) => Value::Utf8(s.to_string()),
            ValueRef::Binary(b) => Value::Binary(b.to_vec()),
            ValueRef::Date32(v) => Value::Date32(v),
            ValueRef::Time64Ns(v) => Value::Time64Ns(v),
            ValueRef::TimestampNs(v) => Value::TimestampNs(v),
            ValueRef::Decimal128(v) => Value::Decimal128(v),
            ValueRef::Interval {
                months,
                days,
                nanos,
            } => Value::Interval {
                months,
                days,
                nanos,
            },
        }
    }

    /// 是否 NULL。
    pub fn is_null(&self) -> bool {
        matches!(self, ValueRef::Null)
    }

    /// 取整数视图（同 [`Value::as_i64`]）。
    pub fn as_i64(&self) -> Option<i64> {
        self.to_value().as_i64()
    }

    /// 取浮点视图（同 [`Value::as_f64`]）。
    pub fn as_f64(&self) -> Option<f64> {
        self.to_value().as_f64()
    }

    /// 取字符串（**只有 `Utf8`**）。
    pub fn as_str(&self) -> Option<&str> {
        match self {
            ValueRef::Utf8(s) => Some(s),
            _ => None,
        }
    }
}

// ---------------------------------------------------------------------------
// 解码原语（bulk / rows 共用）
// ---------------------------------------------------------------------------

/// 从字节里读一个值（定长按宽度小端；变长读 `u32 长度 + 字节`）。
///
/// `Decimal128` 的 `precision`/`scale` 在列描述里，故这里只产出未缩放整数。
pub(crate) fn decode_value<'a>(tag: Tag, bytes: &'a [u8], fmt_name: &str) -> Result<ValueRef<'a>> {
    let need = |n: usize| -> Result<()> {
        if bytes.len() < n {
            return Err(decode_err(
                fmt_name,
                format!("{} 需要 {n} 字节，只有 {}", tag.name(), bytes.len()),
            ));
        }
        Ok(())
    };
    let i = |n: usize| -> Result<i128> {
        // i128::from_le_bytes 是唯一的 16 字节读法，其余宽度走同一个 helper
        let mut b = [0u8; 16];
        b[..n].copy_from_slice(&bytes[..n]);
        Ok(i128::from_le_bytes(b))
    };
    Ok(match tag {
        Tag::Bool => {
            need(1)?;
            ValueRef::Bool(bytes[0] != 0)
        }
        Tag::I8 => {
            need(1)?;
            ValueRef::I8(bytes[0] as i8)
        }
        Tag::I16 => {
            need(2)?;
            ValueRef::I16(i16::from_le_bytes(bytes[..2].try_into().expect("长度已查")))
        }
        Tag::I32 => {
            need(4)?;
            ValueRef::I32(i32::from_le_bytes(bytes[..4].try_into().expect("长度已查")))
        }
        Tag::I64 => {
            need(8)?;
            ValueRef::I64(i64::from_le_bytes(bytes[..8].try_into().expect("长度已查")))
        }
        Tag::F32 => {
            need(4)?;
            ValueRef::F32(f32::from_le_bytes(bytes[..4].try_into().expect("长度已查")))
        }
        Tag::F64 => {
            need(8)?;
            ValueRef::F64(f64::from_le_bytes(bytes[..8].try_into().expect("长度已查")))
        }
        Tag::Date32 => {
            need(4)?;
            ValueRef::Date32(i32::from_le_bytes(bytes[..4].try_into().expect("长度已查")))
        }
        Tag::Time64Ns => {
            need(8)?;
            ValueRef::Time64Ns(i64::from_le_bytes(bytes[..8].try_into().expect("长度已查")))
        }
        Tag::TimestampNs => {
            need(8)?;
            ValueRef::TimestampNs(i64::from_le_bytes(bytes[..8].try_into().expect("长度已查")))
        }
        Tag::Decimal128 => {
            need(16)?;
            ValueRef::Decimal128(i(16)?)
        }
        Tag::Interval => {
            need(16)?;
            let months = i32::from_le_bytes(bytes[..4].try_into().expect("长度已查"));
            let days = i32::from_le_bytes(bytes[4..8].try_into().expect("长度已查"));
            let nanos = i64::from_le_bytes(bytes[8..16].try_into().expect("长度已查"));
            ValueRef::Interval {
                months,
                days,
                nanos,
            }
        }
        Tag::Utf8 => ValueRef::Utf8(
            std::str::from_utf8(bytes)
                .map_err(|e| decode_err(fmt_name, format!("Utf8 列不是合法 UTF-8: {e}")))?,
        ),
        Tag::Binary => ValueRef::Binary(bytes),
    })
}

/// 十进制字符串 → 未缩放 i128（**精确，不经 f64**）。小数位超过 `scale` 报错
/// （四舍五入是编造值）；不足右补零。`"+12.5"`/`"-0.05"`/`"42"` 均可。
pub(crate) fn parse_decimal_unscaled(t: &str, scale: i8) -> std::result::Result<i128, String> {
    let (neg, body) = match t.strip_prefix('-') {
        Some(b) => (true, b),
        None => (false, t.strip_prefix('+').unwrap_or(t)),
    };
    let (int_part, frac_part) = match body.split_once('.') {
        Some((a, b)) => (a, b),
        None => (body, ""),
    };
    if int_part.is_empty() && frac_part.is_empty() {
        return Err("没有数字".into());
    }
    if !int_part.chars().all(|c| c.is_ascii_digit())
        || !frac_part.chars().all(|c| c.is_ascii_digit())
    {
        return Err("含非数字字符".into());
    }
    if frac_part.len() as i8 > scale {
        return Err(format!(
            "小数位 {} 超出 scale {scale}（多出的位拒绝——四舍五入是编造值）",
            frac_part.len()
        ));
    }
    let digits = format!(
        "{int_part}{frac_part}{}",
        "0".repeat((scale as usize) - frac_part.len())
    );
    let v: i128 = digits.parse().map_err(|_| "超出 i128".to_string())?;
    Ok(if neg { -v } else { v })
}
