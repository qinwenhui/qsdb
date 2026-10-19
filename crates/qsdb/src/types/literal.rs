//! [`Literal`]：带类型的标量值，用于列统计（min/max）、谓词、默认值与
//! 分区值等场景。序列化目标为 postcard（分区 manifest 内嵌）。
//!
//! 注意：postcard 不支持 128 位整数，`Decimal::unscaled` 通过
//! [`i128_be`]（16 字节大端）自定义 serde 编解码。

use std::cmp::Ordering;
use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::LogicalType;
use super::decimal::{DecimalMeta, format_decimal};
use super::temporal;

/// 带类型的标量值。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Literal {
    Null,
    Boolean(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    /// DECIMAL：unscaled 值 + 精度元数据
    Decimal {
        #[serde(with = "i128_be")]
        unscaled: i128,
        meta: DecimalMeta,
    },
    Utf8(String),
    Binary(Vec<u8>),
    /// 自 1970-01-01 的天数
    Date(i32),
    /// 自午夜纳秒
    Time(i64),
    /// 自 epoch 纳秒（naive UTC）
    Timestamp(i64),
    /// 12 字节 interval（months, days, nanos）
    Interval {
        months: i32,
        days: i32,
        nanos: i64,
    },
}

impl Literal {
    /// 值所属的逻辑类型；`Null` 无类型。
    pub fn logical_type(&self) -> Option<LogicalType> {
        Some(match self {
            Self::Null => return None,
            Self::Boolean(_) => LogicalType::Boolean,
            Self::Int8(_) => LogicalType::Int8,
            Self::Int16(_) => LogicalType::Int16,
            Self::Int32(_) => LogicalType::Int32,
            Self::Int64(_) => LogicalType::Int64,
            Self::Float32(_) => LogicalType::Float32,
            Self::Float64(_) => LogicalType::Float64,
            Self::Decimal { meta, .. } => LogicalType::Decimal(*meta),
            Self::Utf8(_) => LogicalType::Varchar { max_chars: None },
            Self::Binary(_) => LogicalType::Binary,
            Self::Date(_) => LogicalType::Date,
            Self::Time(_) => LogicalType::Time,
            Self::Timestamp(_) => LogicalType::Timestamp { timezone: None },
            Self::Interval { .. } => LogicalType::Interval,
        })
    }

    #[inline]
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    /// 构造 DECIMAL 字面量（校验 |unscaled| ≤ 10^precision - 1）。
    pub fn decimal(unscaled: i128, meta: DecimalMeta) -> crate::error::Result<Self> {
        if !meta.fits(unscaled) {
            return Err(crate::error::QsdbError::type_err(format!(
                "unscaled value {unscaled} overflows {meta}"
            )));
        }
        Ok(Self::Decimal { unscaled, meta })
    }

    /// SQL 文本输出（时间按 §3.4 格式，Decimal 保留尾随零，字符串带引号）。
    pub fn to_sql_string(&self) -> String {
        match self {
            Self::Null => "NULL".into(),
            Self::Boolean(b) => b.to_string(),
            Self::Int8(v) => v.to_string(),
            Self::Int16(v) => v.to_string(),
            Self::Int32(v) => v.to_string(),
            Self::Int64(v) => v.to_string(),
            Self::Float32(v) => format_float(f64::from(*v)),
            Self::Float64(v) => format_float(*v),
            Self::Decimal { unscaled, meta } => format_decimal(*unscaled, meta.scale),
            Self::Utf8(s) => format!("'{}'", s.replace('\'', "''")),
            Self::Binary(b) => {
                let hex: String = b.iter().map(|x| format!("{x:02X}")).collect();
                format!("X'{hex}'")
            }
            Self::Date(d) => temporal::format_date(*d),
            Self::Time(ns) => temporal::format_time_ns(*ns),
            Self::Timestamp(ns) => temporal::format_timestamp_ns(*ns),
            Self::Interval {
                months,
                days,
                nanos,
            } => {
                format!("INTERVAL '{months}-{days}-{nanos}' (MONTH-DAY-NANO)")
            }
        }
    }

    /// 跨类型比较（NULL 与 NaN 无序；类型族不匹配报错）。
    /// 规则见 `types/compare.rs`。
    pub fn partial_cmp_literal(&self, other: &Literal) -> crate::error::Result<Option<Ordering>> {
        super::compare::cmp_literals(self, other)
    }
}

impl fmt::Display for Literal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_sql_string())
    }
}

/// 浮点输出：有限值用最短往返表示，特殊值按 SQL 习惯。
fn format_float(v: f64) -> String {
    if v.is_nan() {
        "NaN".into()
    } else if v.is_infinite() {
        if v > 0.0 {
            "Infinity".into()
        } else {
            "-Infinity".into()
        }
    } else {
        format!("{v}")
    }
}

// ---------------------------------------------------------------------------
// i128 ↔ 16 字节大端（postcard 不支持 128 位整数）
// ---------------------------------------------------------------------------

mod i128_be {
    use super::*;

    pub(super) fn serialize<S: Serializer>(v: &i128, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_bytes(&v.to_be_bytes())
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<i128, D::Error> {
        let bytes = Vec::<u8>::deserialize(d)?;
        let arr: [u8; 16] = bytes
            .as_slice()
            .try_into()
            .map_err(|_| serde::de::Error::custom("i128 expects exactly 16 bytes"))?;
        Ok(i128::from_be_bytes(arr))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(p: u8, s: i8) -> DecimalMeta {
        DecimalMeta::try_new(p, s).unwrap()
    }

    #[test]
    fn sql_string_output() {
        assert_eq!(Literal::Null.to_sql_string(), "NULL");
        assert_eq!(Literal::Boolean(true).to_sql_string(), "true");
        assert_eq!(Literal::Int32(-42).to_sql_string(), "-42");
        assert_eq!(Literal::Float64(1.5).to_sql_string(), "1.5");
        assert_eq!(
            Literal::Decimal {
                unscaled: 123456789,
                meta: d(18, 5)
            }
            .to_sql_string(),
            "1234.56789"
        );
        assert_eq!(
            Literal::Decimal {
                unscaled: 100,
                meta: d(18, 2)
            }
            .to_sql_string(),
            "1.00"
        );
        assert_eq!(Literal::Utf8("a'b".into()).to_sql_string(), "'a''b'");
        assert_eq!(
            Literal::Binary(vec![0xAB, 0x00, 0xCD]).to_sql_string(),
            "X'AB00CD'"
        );
        assert_eq!(Literal::Date(20_735).to_sql_string(), "2026-10-04");
        assert_eq!(
            Literal::Time(45 * 1_000_000_000).to_sql_string(),
            "00:00:45"
        );
        assert_eq!(Literal::Timestamp(0).to_sql_string(), "1970-01-01 00:00:00");
        assert_eq!(Literal::Float64(f64::NAN).to_sql_string(), "NaN");
        assert_eq!(Literal::Float64(f64::INFINITY).to_sql_string(), "Infinity");
    }

    #[test]
    fn logical_type_mapping() {
        assert_eq!(Literal::Int8(1).logical_type(), Some(LogicalType::Int8));
        assert_eq!(
            Literal::Decimal {
                unscaled: 1,
                meta: d(10, 2)
            }
            .logical_type(),
            Some(LogicalType::Decimal(d(10, 2)))
        );
        assert_eq!(Literal::Null.logical_type(), None);
    }

    #[test]
    fn decimal_constructor_validates() {
        assert!(Literal::decimal(99_999, d(5, 0)).is_ok());
        assert!(Literal::decimal(100_000, d(5, 0)).is_err());
    }

    #[test]
    fn postcard_roundtrip_all_variants() {
        let all = vec![
            Literal::Null,
            Literal::Boolean(false),
            Literal::Int8(-1),
            Literal::Int16(300),
            Literal::Int32(-70_000),
            Literal::Int64(i64::MIN),
            Literal::Float32(3.5),
            Literal::Float64(-2.25),
            Literal::Decimal {
                unscaled: -12345678901234567890123456789012345678i128,
                meta: d(38, 12),
            },
            Literal::Decimal {
                unscaled: i128::MIN,
                meta: d(38, 0),
            }, // 不合法值也应无损序列化
            Literal::Utf8("你好 qsdb".into()),
            Literal::Binary(vec![0u8, 255, 7]),
            Literal::Date(-719_162),
            Literal::Time(86_399_000_000_000),
            Literal::Timestamp(i64::MIN),
            Literal::Interval {
                months: -14,
                days: 3,
                nanos: 123_456_789,
            },
        ];
        for lit in all {
            let bytes = postcard::to_allocvec(&lit).expect("serialize");
            let back: Literal = postcard::from_bytes(&bytes).expect("deserialize");
            assert_eq!(
                back,
                lit,
                "roundtrip failed for {lit:?} ({} bytes)",
                bytes.len()
            );
        }
    }

    #[test]
    fn postcard_rejects_truncated_input() {
        let lit = Literal::Decimal {
            unscaled: 1,
            meta: d(5, 0),
        };
        let mut bytes = postcard::to_allocvec(&lit).unwrap();
        bytes.truncate(bytes.len() - 4); // 破坏尾部
        assert!(postcard::from_bytes::<Literal>(&bytes).is_err());
        bytes.clear(); // 空输入
        assert!(postcard::from_bytes::<Literal>(&bytes).is_err());
    }
}
