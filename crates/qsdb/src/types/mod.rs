//! QSDB 类型系统：逻辑类型（DDL/元数据层）与 Arrow 物理类型（执行层）的映射。
//!
//! 逻辑类型集合对齐参考文档 §3.1；v1 落地全部标量类型，
//! ARRAY / MAP / 超长字符串 overflow 存储后置。

pub mod cast;
pub mod compare;
pub mod decimal;
pub mod literal;
pub mod temporal;

use std::fmt;
use std::sync::Arc;

use arrow::datatypes::{DataType, IntervalUnit, TimeUnit};
use serde::{Deserialize, Serialize};

pub use decimal::DecimalMeta;
pub use literal::Literal;

use crate::error::{QsdbError, Result};

/// 逻辑类型。DDL、表 schema（table.json）、分区 manifest 全部以此为准；
/// 执行与 Parquet 层通过 [`LogicalType::arrow_type`] 映射。
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum LogicalType {
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    Decimal(DecimalMeta),
    /// STRING（`max_chars: None`）/ VARCHAR(n)
    Varchar {
        max_chars: Option<u32>,
    },
    /// CHAR(n)：定长，语义层补空格比较；物理存储同 Utf8
    Char {
        length: u32,
    },
    Binary,
    /// i32 天数（1970-01-01 起）
    Date,
    /// i64 纳秒（自午夜）
    Time,
    /// i64 纳秒（naive UTC）；带时区时由 session tz 解释
    Timestamp {
        timezone: Option<String>,
    },
    /// 16 字节 (months i32, days i32, nanos i64)
    Interval,
}

impl LogicalType {
    // -- 构造校验 ----------------------------------------------------------

    pub fn try_decimal(precision: u8, scale: i8) -> Result<Self> {
        Ok(Self::Decimal(DecimalMeta::try_new(precision, scale)?))
    }

    pub fn try_varchar(max_chars: Option<u32>) -> Result<Self> {
        if let Some(n) = max_chars {
            if n == 0 || n > 65_535 {
                return Err(QsdbError::invalid(format!(
                    "VARCHAR length {n} out of range 1..=65535"
                )));
            }
        }
        Ok(Self::Varchar { max_chars })
    }

    pub fn try_char(length: u32) -> Result<Self> {
        if length == 0 || length > 65_535 {
            return Err(QsdbError::invalid(format!(
                "CHAR length {length} out of range 1..=65535"
            )));
        }
        Ok(Self::Char { length })
    }

    // -- 分类 --------------------------------------------------------------

    #[inline]
    pub fn is_numeric(&self) -> bool {
        self.is_integer() || self.is_floating() || self.is_decimal()
    }

    #[inline]
    pub fn is_integer(&self) -> bool {
        matches!(self, Self::Int8 | Self::Int16 | Self::Int32 | Self::Int64)
    }

    #[inline]
    pub fn is_floating(&self) -> bool {
        matches!(self, Self::Float32 | Self::Float64)
    }

    #[inline]
    pub fn is_decimal(&self) -> bool {
        matches!(self, Self::Decimal(_))
    }

    #[inline]
    pub fn is_string(&self) -> bool {
        matches!(self, Self::Varchar { .. } | Self::Char { .. })
    }

    #[inline]
    pub fn is_temporal(&self) -> bool {
        matches!(self, Self::Date | Self::Time | Self::Timestamp { .. })
    }

    /// 数值塔层级（见 compare.rs）；非数值返回 0。
    #[inline]
    pub fn numeric_rank(&self) -> u8 {
        match self {
            Self::Int8 => 1,
            Self::Int16 => 2,
            Self::Int32 => 3,
            Self::Int64 => 4,
            Self::Float32 => 5,
            Self::Float64 => 6,
            Self::Decimal(_) => 7,
            _ => 0,
        }
    }

    // -- Arrow 映射（§3.1） -------------------------------------------------

    /// 物理存储/执行层类型。Char 与 Varchar(n) 的长度语义只存在于逻辑层。
    pub fn arrow_type(&self) -> DataType {
        match self {
            Self::Boolean => DataType::Boolean,
            Self::Int8 => DataType::Int8,
            Self::Int16 => DataType::Int16,
            Self::Int32 => DataType::Int32,
            Self::Int64 => DataType::Int64,
            Self::Float32 => DataType::Float32,
            Self::Float64 => DataType::Float64,
            Self::Decimal(m) => DataType::Decimal128(m.precision, m.scale),
            Self::Varchar { .. } | Self::Char { .. } => DataType::Utf8,
            Self::Binary => DataType::Binary,
            Self::Date => DataType::Date32,
            Self::Time => DataType::Time64(TimeUnit::Nanosecond),
            Self::Timestamp { timezone } => {
                DataType::Timestamp(TimeUnit::Nanosecond, timezone.as_deref().map(Arc::from))
            }
            Self::Interval => DataType::Interval(IntervalUnit::MonthDayNano),
        }
    }

    /// 从 Arrow 物理类型反推逻辑类型（写路径校验 RecordBatch schema 用）。
    ///
    /// 注意：Utf8 反推为 STRING（VARCHAR(n)/CHAR 长度语义由 table.json 保留）。
    pub fn try_from_arrow(dt: &DataType) -> Result<Self> {
        let t = match dt {
            DataType::Boolean => Self::Boolean,
            DataType::Int8 => Self::Int8,
            DataType::Int16 => Self::Int16,
            DataType::Int32 => Self::Int32,
            DataType::Int64 => Self::Int64,
            DataType::Float32 => Self::Float32,
            DataType::Float64 => Self::Float64,
            DataType::Decimal128(p, s) => Self::Decimal(DecimalMeta::try_new(*p, *s)?),
            DataType::Utf8 => Self::Varchar { max_chars: None },
            // F3（qsdb-sync 反馈）：parquet 生态的 us/ms/s 时间戳是事实标准
            // （Spark/DuckDB/pandas 默认）——收纳并归一到 ns（内部只存 ns；
            // 数据列在 scan 路径 cast，见 scan::cast_external_columns）
            DataType::Timestamp(_, tz) => Self::Timestamp {
                timezone: tz.as_ref().map(|s| s.to_string()),
            },
            // 同理：字符串视图/大字符串归一 Utf8；Date64 归一 Date32
            DataType::Utf8View | DataType::LargeUtf8 => Self::Varchar { max_chars: None },
            DataType::Date64 => Self::Date,
            DataType::Binary => Self::Binary,
            DataType::Date32 => Self::Date,
            DataType::Time64(TimeUnit::Nanosecond) => Self::Time,
            DataType::Interval(IntervalUnit::MonthDayNano) => Self::Interval,
            other => {
                return Err(QsdbError::unsupported(format!(
                    "arrow type {other:?} has no QSDB logical mapping (hint: cast timestamp to ns, string to utf8)"
                )));
            }
        };
        Ok(t)
    }

    // -- SQL 名解析 ----------------------------------------------------------

    /// 解析 DDL 类型名（大小写不敏感），支持：
    /// `INT`、`BIGINT`、`DECIMAL(18,4)`、`VARCHAR(255)`、`CHAR(8)`、
    /// `STRING`、`TIMESTAMP`、`TIMESTAMP WITH TIME ZONE` 等。
    pub fn from_sql(sql: &str) -> Result<Self> {
        let upper = sql.trim().to_ascii_uppercase();
        let tz = upper.ends_with("WITH TIME ZONE");
        let core = if tz {
            upper[..upper.len() - "WITH TIME ZONE".len()].trim()
        } else {
            upper.as_str()
        };
        let (name, params) = match core.split_once('(') {
            Some((n, rest)) => {
                let p = rest.strip_suffix(')').ok_or_else(|| {
                    QsdbError::parse_err(format!("invalid type '{sql}': unclosed '('"))
                })?;
                (n.trim(), Some(p.trim()))
            }
            None => (core, None),
        };

        match name {
            "BOOLEAN" | "BOOL" => Ok(Self::Boolean),
            "TINYINT" | "INT8" => Ok(Self::Int8),
            "SMALLINT" | "INT16" => Ok(Self::Int16),
            "INT" | "INTEGER" | "INT32" => Ok(Self::Int32),
            "BIGINT" | "INT64" => Ok(Self::Int64),
            "FLOAT" | "REAL" | "FLOAT32" => Ok(Self::Float32),
            "DOUBLE" | "DOUBLE PRECISION" | "FLOAT64" => Ok(Self::Float64),
            "DECIMAL" | "NUMERIC" => {
                let (p, s) = match params {
                    None => (38, 0),
                    Some(spec) => {
                        let (a, b) = spec.split_once(',').ok_or_else(|| {
                            QsdbError::parse_err(format!(
                                "invalid DECIMAL spec '{spec}', expect DECIMAL(p, s)"
                            ))
                        })?;
                        (
                            a.trim().parse::<u8>().map_err(|_| {
                                QsdbError::parse_err(format!("bad precision '{a}'"))
                            })?,
                            b.trim()
                                .parse::<i8>()
                                .map_err(|_| QsdbError::parse_err(format!("bad scale '{b}'")))?,
                        )
                    }
                };
                Self::try_decimal(p, s)
            }
            "STRING" | "TEXT" => Ok(Self::Varchar { max_chars: None }),
            "VARCHAR" => {
                let n = match params {
                    None => None,
                    Some(spec) => Some(spec.parse::<u32>().map_err(|_| {
                        QsdbError::parse_err(format!("bad VARCHAR length '{spec}'"))
                    })?),
                };
                Self::try_varchar(n)
            }
            "CHAR" | "CHARACTER" => {
                let spec = params.ok_or_else(|| {
                    QsdbError::parse_err(format!("type '{name}' requires parameters"))
                })?;
                let n = spec
                    .parse::<u32>()
                    .map_err(|_| QsdbError::parse_err(format!("bad CHAR length '{spec}'")))?;
                Self::try_char(n)
            }
            "BINARY" | "VARBINARY" | "BLOB" | "BYTES" => Ok(Self::Binary),
            "DATE" => Ok(Self::Date),
            "TIME" => Ok(Self::Time),
            "TIMESTAMP" => Ok(Self::Timestamp {
                timezone: if tz { Some("UTC".into()) } else { None },
            }),
            "INTERVAL" => Ok(Self::Interval),
            other => Err(QsdbError::unsupported(format!("unknown type '{other}'"))),
        }
    }
}

impl fmt::Display for LogicalType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Boolean => write!(f, "BOOLEAN"),
            Self::Int8 => write!(f, "TINYINT"),
            Self::Int16 => write!(f, "SMALLINT"),
            Self::Int32 => write!(f, "INT"),
            Self::Int64 => write!(f, "BIGINT"),
            Self::Float32 => write!(f, "FLOAT"),
            Self::Float64 => write!(f, "DOUBLE"),
            Self::Decimal(m) => write!(f, "{m}"),
            Self::Varchar { max_chars: None } => write!(f, "STRING"),
            Self::Varchar { max_chars: Some(n) } => write!(f, "VARCHAR({n})"),
            Self::Char { length } => write!(f, "CHAR({length})"),
            Self::Binary => write!(f, "BINARY"),
            Self::Date => write!(f, "DATE"),
            Self::Time => write!(f, "TIME"),
            // v1 时区仅 UTC；非 UTC 显示原名
            Self::Timestamp { timezone: None } => write!(f, "TIMESTAMP"),
            Self::Timestamp { timezone: Some(tz) } => write!(f, "TIMESTAMP WITH TIME ZONE ({tz})"),
            Self::Interval => write!(f, "INTERVAL"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(sql: &str) -> LogicalType {
        LogicalType::from_sql(sql).unwrap()
    }

    #[test]
    fn from_sql_names() {
        assert_eq!(t("boolean"), LogicalType::Boolean);
        assert_eq!(t("BOOL"), LogicalType::Boolean);
        assert_eq!(t("tinyint"), LogicalType::Int8);
        assert_eq!(t("smallint"), LogicalType::Int16);
        assert_eq!(t("int"), LogicalType::Int32);
        assert_eq!(t("INTEGER"), LogicalType::Int32);
        assert_eq!(t("bigint"), LogicalType::Int64);
        assert_eq!(t("float"), LogicalType::Float32);
        assert_eq!(t("real"), LogicalType::Float32);
        assert_eq!(t("double"), LogicalType::Float64);
        assert_eq!(t("double precision"), LogicalType::Float64);
        assert_eq!(
            t("decimal(18, 4)"),
            LogicalType::Decimal(DecimalMeta::new(18, 4))
        );
        assert_eq!(
            t("numeric(5,2)"),
            LogicalType::Decimal(DecimalMeta::new(5, 2))
        );
        assert_eq!(t("string"), LogicalType::Varchar { max_chars: None });
        assert_eq!(t("text"), LogicalType::Varchar { max_chars: None });
        assert_eq!(
            t("varchar(255)"),
            LogicalType::Varchar {
                max_chars: Some(255)
            }
        );
        assert_eq!(t("varchar"), LogicalType::Varchar { max_chars: None });
        assert_eq!(t("char(8)"), LogicalType::Char { length: 8 });
        assert_eq!(t("binary"), LogicalType::Binary);
        assert_eq!(t("varbinary"), LogicalType::Binary);
        assert_eq!(t("date"), LogicalType::Date);
        assert_eq!(t("time"), LogicalType::Time);
        assert_eq!(t("timestamp"), LogicalType::Timestamp { timezone: None });
        assert_eq!(
            t("timestamp with time zone"),
            LogicalType::Timestamp {
                timezone: Some("UTC".into())
            }
        );
        assert_eq!(t("interval"), LogicalType::Interval);
    }

    #[test]
    fn from_sql_rejects() {
        for s in [
            "",
            "int4x",
            "decimal(40, 0)",
            "decimal(5, 6)",
            "varchar(0)",
            "varchar(70000)",
            "char(0)",
            "decimal(5)",
            "varchar(x)",
            "char",
        ] {
            assert!(LogicalType::from_sql(s).is_err(), "should reject {s:?}");
        }
    }

    #[test]
    fn display_matches_canonical() {
        assert_eq!(LogicalType::Int32.to_string(), "INT");
        assert_eq!(t("numeric(9,3)").to_string(), "DECIMAL(9, 3)");
        assert_eq!(
            LogicalType::Varchar { max_chars: None }.to_string(),
            "STRING"
        );
        assert_eq!(t("varchar(9)").to_string(), "VARCHAR(9)");
        assert_eq!(t("timestamp").to_string(), "TIMESTAMP");
        assert_eq!(
            t("timestamp with time zone").to_string(),
            "TIMESTAMP WITH TIME ZONE (UTC)"
        );
    }

    #[test]
    fn arrow_roundtrip_physical() {
        let all = vec![
            LogicalType::Boolean,
            LogicalType::Int8,
            LogicalType::Int16,
            LogicalType::Int32,
            LogicalType::Int64,
            LogicalType::Float32,
            LogicalType::Float64,
            LogicalType::Decimal(DecimalMeta::new(18, 4)),
            LogicalType::Varchar { max_chars: None },
            LogicalType::Binary,
            LogicalType::Date,
            LogicalType::Time,
            LogicalType::Timestamp { timezone: None },
            LogicalType::Timestamp {
                timezone: Some("UTC".into()),
            },
            LogicalType::Interval,
        ];
        for lt in all {
            let back = LogicalType::try_from_arrow(&lt.arrow_type()).unwrap();
            // 字符串长度语义（VARCHAR(n)/CHAR）只存在于逻辑层，物理层往返归一为 STRING
            let expected = match &lt {
                LogicalType::Varchar { .. } | LogicalType::Char { .. } => {
                    LogicalType::Varchar { max_chars: None }
                }
                other => other.clone(),
            };
            assert_eq!(back, expected, "arrow roundtrip for {lt}");
        }
    }

    #[test]
    fn arrow_unsupported_mappings() {
        // F3 后：us/ms/s 时间戳、Utf8View/Date64 收纳归一（不再是 unsupported）
        assert_eq!(
            LogicalType::try_from_arrow(&DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap(),
            LogicalType::Timestamp { timezone: None }
        );
        assert_eq!(
            LogicalType::try_from_arrow(&DataType::Date64).unwrap(),
            LogicalType::Date
        );
        assert!(LogicalType::try_from_arrow(&DataType::Utf8View).is_ok());
        // 仍不收纳的例子
        assert!(LogicalType::try_from_arrow(&DataType::Float16).is_err());
        assert!(LogicalType::try_from_arrow(&DataType::Time64(TimeUnit::Microsecond)).is_err());
    }

    #[test]
    fn classifications() {
        assert!(LogicalType::Int8.is_numeric());
        assert!(LogicalType::Decimal(DecimalMeta::new(10, 2)).is_numeric());
        assert!(!LogicalType::Varchar { max_chars: None }.is_numeric());
        assert!(LogicalType::Date.is_temporal());
        assert!(LogicalType::Varchar { max_chars: Some(3) }.is_string());
        assert!(LogicalType::Char { length: 3 }.is_string());
    }

    #[test]
    fn constructors_validate() {
        assert!(LogicalType::try_varchar(Some(0)).is_err());
        assert!(LogicalType::try_varchar(Some(65_536)).is_err());
        assert!(LogicalType::try_char(65_535).is_ok());
        assert!(LogicalType::try_decimal(18, 4).is_ok());
        assert!(LogicalType::try_decimal(18, 19).is_err());
    }
}
