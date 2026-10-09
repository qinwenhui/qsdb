//! 类型转换：规则矩阵（[`cast_rule`]）与字面量值转换（[`cast_literal`]）。
//!
//! 规则总则：
//! - 同类型转换恒为 `Implicit`（无操作）
//! - 数值塔内放大（窄整数→宽整数、整数→浮点、整数→DECIMAL、
//!   DATE→TIMESTAMP）为 `Implicit`；塔内缩小与其余合法转换为 `Explicit`
//! - BOOLEAN 与数值/时间互转 `Impossible`；INTERVAL 不与任何其他类型互转
//! - 运行时语义：缩小整数溢出报错；FLOAT→INT 截断（非有限值报错）；
//!   DECIMAL 缩小 scale 用 HALF_UP；FLOAT→DECIMAL 经 scale+1 位文本中转
//!   （双重舍入仅在病态边界出现，M0 接受并记录）

use super::LogicalType;
use super::decimal::{DecimalMeta, parse_decimal, pow10, rescale};
use super::literal::Literal;
use super::temporal::{self, NANOS_PER_DAY};
use crate::error::{QsdbError, Result};

/// 转换级别。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CastRule {
    /// 无损放大，可自动注入
    Implicit,
    /// 合法但可能有损/可失败，需显式 CAST 或上下文允许
    Explicit,
    /// 不允许
    Impossible,
}

pub fn cast_rule(from: &LogicalType, to: &LogicalType) -> CastRule {
    use CastRule::{Explicit, Implicit, Impossible};
    use LogicalType as Lt;

    if from == to {
        return Implicit;
    }
    match (from, to) {
        // BOOLEAN 与字符串族（含 CHAR）
        (Lt::Boolean, Lt::Varchar { .. } | Lt::Char { .. })
        | (Lt::Varchar { .. } | Lt::Char { .. }, Lt::Boolean) => Explicit,
        (Lt::Boolean, _) | (_, Lt::Boolean) => Impossible,

        // 数值 → 数值：塔内放大 Implicit，其余 Explicit
        _ if from.is_numeric() && to.is_numeric() => {
            if numeric_upcast(from, to) {
                Implicit
            } else {
                Explicit
            }
        }

        // 数值 ↔ 字符串
        _ if from.is_numeric() && to.is_string() => Explicit,
        _ if from.is_string() && to.is_numeric() => Explicit,

        // 字符串族内部（长度语义变化）
        _ if from.is_string() && to.is_string() => Implicit,

        // 字符串 ↔ 时间
        _ if from.is_string() && to.is_temporal() => Explicit,
        _ if from.is_temporal() && to.is_string() => Explicit,

        // 字符串 ↔ BINARY
        (Lt::Varchar { .. }, Lt::Binary) | (Lt::Char { .. }, Lt::Binary) => Explicit,
        (Lt::Binary, Lt::Varchar { .. }) | (Lt::Binary, Lt::Char { .. }) => Explicit,
        (Lt::Binary, _) | (_, Lt::Binary) => Impossible,

        // 时间族：DATE → TIMESTAMP 无损；反向截断；tz 语义变化算显式
        (Lt::Date, Lt::Timestamp { timezone: None }) => Implicit,
        (Lt::Date, Lt::Timestamp { timezone: Some(_) }) => Explicit,
        (Lt::Timestamp { .. }, Lt::Date) => Explicit,
        (Lt::Timestamp { .. }, Lt::Timestamp { .. }) => Explicit,
        (Lt::Time, _) | (_, Lt::Time) => Impossible,

        // INTERVAL 只与自身（同类型已在开头返回）
        (Lt::Interval, _) | (_, Lt::Interval) => Impossible,

        _ => Impossible,
    }
}

/// `from` 数值是否可无损放入 `to`。
fn numeric_upcast(from: &LogicalType, to: &LogicalType) -> bool {
    use LogicalType as Lt;
    match (from, to) {
        // decimal → decimal：整数位不减且 scale 不减
        (Lt::Decimal(a), Lt::Decimal(b)) => b.int_digits() >= a.int_digits() && b.scale >= a.scale,
        // 整数 → decimal：目标整数位 ≥ 该整型实际十进制位宽
        (_, Lt::Decimal(m)) => from.is_integer() && m.int_digits() >= int_width(from),
        (Lt::Decimal(_), _) => false,
        _ => to.numeric_rank() > from.numeric_rank(),
    }
}

/// 整型的十进制位宽（最大值位数）。
fn int_width(t: &LogicalType) -> u8 {
    match t {
        LogicalType::Int8 => 3,   // 127
        LogicalType::Int16 => 5,  // 32767
        LogicalType::Int32 => 10, // 2147483647
        _ => 19,                  // Int64
    }
}

// ---------------------------------------------------------------------------
// 值转换
// ---------------------------------------------------------------------------

/// 字面量值转换（Implicit/Explicit 语义统一运行时校验；NULL 透传）。
pub fn cast_literal(lit: &Literal, to: &LogicalType) -> Result<Literal> {
    if lit.is_null() {
        return Ok(Literal::Null);
    }
    let from = lit
        .logical_type()
        .ok_or_else(|| QsdbError::type_err("cannot cast NULL"))?;
    match cast_rule(&from, to) {
        CastRule::Impossible => Err(QsdbError::type_err(format!("cannot cast {from} to {to}"))),
        CastRule::Implicit | CastRule::Explicit => cast_value(lit, to),
    }
}

fn cast_value(lit: &Literal, to: &LogicalType) -> Result<Literal> {
    match to {
        LogicalType::Boolean => match lit {
            Literal::Boolean(_) => Ok(lit.clone()),
            Literal::Utf8(s) => match s.to_ascii_lowercase().as_str() {
                "true" | "t" | "1" => Ok(Literal::Boolean(true)),
                "false" | "f" | "0" => Ok(Literal::Boolean(false)),
                _ => Err(QsdbError::parse_err(format!(
                    "cannot parse '{s}' as BOOLEAN"
                ))),
            },
            other => Err(cast_err(other, to)),
        },
        LogicalType::Int8 | LogicalType::Int16 | LogicalType::Int32 | LogicalType::Int64 => {
            let v = to_i64(lit)?;
            narrow_i64(v, to)
        }
        LogicalType::Float32 => Ok(Literal::Float32(to_f64(lit)? as f32)),
        LogicalType::Float64 => Ok(Literal::Float64(to_f64(lit)?)),
        LogicalType::Decimal(meta) => to_decimal(lit, *meta),
        LogicalType::Varchar { .. } | LogicalType::Char { .. } => match lit {
            Literal::Utf8(_) => Ok(lit.clone()),
            other => Ok(Literal::Utf8(plain_string(other)?)),
        },
        LogicalType::Binary => match lit {
            Literal::Binary(_) => Ok(lit.clone()),
            Literal::Utf8(s) => Ok(Literal::Binary(s.as_bytes().to_vec())),
            other => Err(cast_err(other, to)),
        },
        LogicalType::Date => match lit {
            Literal::Date(_) => Ok(lit.clone()),
            Literal::Timestamp(ns) => {
                let days = ns.div_euclid(NANOS_PER_DAY);
                i32::try_from(days)
                    .map(Literal::Date)
                    .map_err(|_| QsdbError::type_err("timestamp out of DATE range"))
            }
            Literal::Utf8(s) => temporal::parse_date(s).map(Literal::Date),
            other => Err(cast_err(other, to)),
        },
        LogicalType::Time => match lit {
            Literal::Time(_) => Ok(lit.clone()),
            Literal::Utf8(s) => temporal::parse_time_ns(s).map(Literal::Time),
            other => Err(cast_err(other, to)),
        },
        LogicalType::Timestamp { .. } => match lit {
            Literal::Timestamp(_) => Ok(lit.clone()),
            Literal::Date(d) => i64::from(*d)
                .checked_mul(NANOS_PER_DAY)
                .map(Literal::Timestamp)
                .ok_or_else(|| {
                    QsdbError::type_err(format!(
                        "DATE {} out of nanosecond TIMESTAMP range (max 2262-04-11)",
                        temporal::format_date(*d)
                    ))
                }),
            Literal::Utf8(s) => temporal::parse_timestamp_ns(s).map(Literal::Timestamp),
            other => Err(cast_err(other, to)),
        },
        LogicalType::Interval => match lit {
            Literal::Interval { .. } => Ok(lit.clone()),
            other => Err(cast_err(other, to)),
        },
    }
}

fn cast_err(lit: &Literal, to: &LogicalType) -> QsdbError {
    let from = lit
        .logical_type()
        .map(|t| t.to_string())
        .unwrap_or_else(|| "NULL".into());
    QsdbError::type_err(format!("cannot cast {from} value to {to}"))
}

/// 任意数值/字符串字面量 → i64（FLOAT 截断；DECIMAL HALF_UP 到整数）。
fn to_i64(lit: &Literal) -> Result<i64> {
    let v: i128 = match lit {
        Literal::Int8(v) => i128::from(*v),
        Literal::Int16(v) => i128::from(*v),
        Literal::Int32(v) => i128::from(*v),
        Literal::Int64(v) => i128::from(*v),
        Literal::Decimal { unscaled, meta } => rescale(*unscaled, meta.scale, 0)?,
        Literal::Float32(v) => trunc_f64(f64::from(*v))?,
        Literal::Float64(v) => trunc_f64(*v)?,
        Literal::Utf8(s) => s
            .parse::<i64>()
            .map_err(|_| QsdbError::parse_err(format!("cannot parse '{s}' as integer")))?
            as i128,
        other => return Err(cast_err(other, &LogicalType::Int64)),
    };
    i64::try_from(v).map_err(|_| QsdbError::type_err(format!("value {v} overflows BIGINT")))
}

fn trunc_f64(v: f64) -> Result<i128> {
    if !v.is_finite() {
        return Err(QsdbError::type_err(
            "cannot cast non-finite value to integer",
        ));
    }
    let t = v.trunc();
    if !(-9.3e18..=9.3e18).contains(&t) {
        return Err(QsdbError::type_err(format!("value {t} overflows BIGINT")));
    }
    Ok(t as i128)
}

/// 任意数值/字符串字面量 → f64。
fn to_f64(lit: &Literal) -> Result<f64> {
    match lit {
        Literal::Int8(v) => Ok(f64::from(*v)),
        Literal::Int16(v) => Ok(f64::from(*v)),
        Literal::Int32(v) => Ok(f64::from(*v)),
        Literal::Int64(v) => Ok(*v as f64),
        Literal::Float32(v) => Ok(f64::from(*v)),
        Literal::Float64(v) => Ok(*v),
        Literal::Decimal { unscaled, meta } => {
            Ok(*unscaled as f64 / pow10(meta.scale as u32) as f64)
        }
        Literal::Utf8(s) => s
            .parse::<f64>()
            .map_err(|_| QsdbError::parse_err(format!("cannot parse '{s}' as float"))),
        other => Err(cast_err(other, &LogicalType::Float64)),
    }
}

/// 任意数值/字符串字面量 → 目标 DECIMAL。
fn to_decimal(lit: &Literal, meta: DecimalMeta) -> Result<Literal> {
    let unscaled = match lit {
        Literal::Decimal { unscaled, meta: m } => {
            let v = rescale(*unscaled, m.scale, meta.scale)?;
            if !meta.fits(v) {
                return Err(QsdbError::type_err(format!(
                    "decimal value overflows {meta}"
                )));
            }
            return Ok(Literal::Decimal { unscaled: v, meta });
        }
        Literal::Int8(v) => rescale(i128::from(*v), 0, meta.scale)?,
        Literal::Int16(v) => rescale(i128::from(*v), 0, meta.scale)?,
        Literal::Int32(v) => rescale(i128::from(*v), 0, meta.scale)?,
        Literal::Int64(v) => rescale(i128::from(*v), 0, meta.scale)?,
        // 以下两条路径产出已在目标 scale 上
        Literal::Float32(v) => float_to_decimal(f64::from(*v), meta)?,
        Literal::Float64(v) => float_to_decimal(*v, meta)?,
        Literal::Utf8(s) => parse_decimal(s, meta)?,
        other => return Err(cast_err(other, &LogicalType::Decimal(meta))),
    };
    if !meta.fits(unscaled) {
        return Err(QsdbError::type_err(format!(
            "value {} overflows {meta}",
            super::decimal::format_decimal(unscaled, meta.scale)
        )));
    }
    Ok(Literal::Decimal { unscaled, meta })
}

/// FLOAT → DECIMAL：按 scale+1 位文本中转再 HALF_UP（见模块注释）。
fn float_to_decimal(v: f64, meta: DecimalMeta) -> Result<i128> {
    if !v.is_finite() {
        return Err(QsdbError::type_err(
            "cannot cast non-finite float to DECIMAL",
        ));
    }
    let text = format!("{:.*}", meta.scale as usize + 1, v);
    parse_decimal(&text, meta)
}

/// i64 → 指定整数宽度（溢出报错）。
fn narrow_i64(v: i64, to: &LogicalType) -> Result<Literal> {
    let range_err = || QsdbError::type_err(format!("value {v} overflows {to}"));
    Ok(match to {
        LogicalType::Int8 => Literal::Int8(i8::try_from(v).map_err(|_| range_err())?),
        LogicalType::Int16 => Literal::Int16(i16::try_from(v).map_err(|_| range_err())?),
        LogicalType::Int32 => Literal::Int32(i32::try_from(v).map_err(|_| range_err())?),
        LogicalType::Int64 => Literal::Int64(v),
        _ => unreachable!("narrow_i64 to non-int type"),
    })
}

/// 非字符串字面量 → 无引号文本（CAST 到字符串用）。
fn plain_string(lit: &Literal) -> Result<String> {
    match lit {
        Literal::Binary(b) => String::from_utf8(b.clone())
            .map_err(|_| QsdbError::type_err("binary is not valid UTF-8")),
        // 数值/布尔/时间/Decimal/Interval 的 to_sql_string 均为无引号文本
        _ => Ok(lit.to_sql_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::super::decimal::DecimalMeta;
    use super::*;

    fn d(p: u8, s: i8) -> DecimalMeta {
        DecimalMeta::try_new(p, s).unwrap()
    }

    fn lt(sql: &str) -> LogicalType {
        LogicalType::from_sql(sql).unwrap()
    }

    #[test]
    fn rule_matrix() {
        use CastRule::{Explicit, Implicit};
        let cases: Vec<(&str, &str, CastRule)> = vec![
            // 同类型
            ("INT", "INT", Implicit),
            // 整数放大 / 缩小
            ("TINYINT", "BIGINT", Implicit),
            ("BIGINT", "TINYINT", Explicit),
            // 整数 → 浮点 / 浮点放大
            ("INT", "DOUBLE", Implicit),
            ("FLOAT", "DOUBLE", Implicit),
            ("DOUBLE", "FLOAT", Explicit),
            // 整数 ↔ decimal
            ("BIGINT", "DECIMAL(23, 4)", Implicit), // 整数位 19
            ("BIGINT", "DECIMAL(20, 4)", Explicit), // 整数位 16 < 19
            ("INT", "DECIMAL(10, 0)", Implicit),    // 整数位 10 = INT 宽度
            ("DECIMAL(10, 2)", "DECIMAL(12, 4)", Implicit),
            ("DECIMAL(10, 4)", "DECIMAL(12, 2)", Explicit), // scale 缩小
            ("DOUBLE", "DECIMAL(18, 4)", Explicit),
            // 数值 ↔ 字符串
            ("INT", "STRING", Explicit),
            ("STRING", "INT", Explicit),
            ("STRING", "TIMESTAMP", Explicit),
            ("DATE", "STRING", Explicit),
            // 字符串族
            ("VARCHAR(10)", "STRING", Implicit),
            ("CHAR(4)", "VARCHAR(10)", Implicit),
            ("STRING", "BINARY", Explicit),
            ("BINARY", "STRING", Explicit),
            // 时间
            ("DATE", "TIMESTAMP", Implicit),
            ("TIMESTAMP", "DATE", Explicit),
            ("TIMESTAMP", "TIMESTAMP WITH TIME ZONE", Explicit),
            ("TIME", "DATE", CastRule::Impossible),
            // 布尔只与字符串互转
            ("BOOLEAN", "STRING", Explicit),
            ("STRING", "BOOLEAN", Explicit),
            ("BOOLEAN", "INT", CastRule::Impossible),
            ("INT", "BOOLEAN", CastRule::Impossible),
            // interval / binary 封闭
            ("INTERVAL", "STRING", CastRule::Impossible),
            ("BINARY", "INT", CastRule::Impossible),
        ];
        for (from, to, expect) in cases {
            assert_eq!(
                cast_rule(&lt(from), &lt(to)),
                expect,
                "cast_rule({from}, {to})"
            );
        }
    }

    #[test]
    fn cast_null_passthrough() {
        assert_eq!(
            cast_literal(&Literal::Null, &lt("INT")).unwrap(),
            Literal::Null
        );
    }

    #[test]
    fn cast_int_narrowing_checked() {
        assert!(cast_literal(&Literal::Int64(300), &lt("TINYINT")).is_err());
        assert_eq!(
            cast_literal(&Literal::Int64(100), &lt("TINYINT")).unwrap(),
            Literal::Int8(100)
        );
        assert_eq!(
            cast_literal(&Literal::Int32(-5), &lt("BIGINT")).unwrap(),
            Literal::Int64(-5)
        );
    }

    #[test]
    fn cast_string_numeric_roundtrip() {
        assert_eq!(
            cast_literal(&Literal::Utf8("-42".into()), &lt("INT")).unwrap(),
            Literal::Int32(-42)
        );
        assert_eq!(
            cast_literal(&Literal::Int32(-42), &lt("STRING")).unwrap(),
            Literal::Utf8("-42".into())
        );
        assert_eq!(
            cast_literal(&Literal::Utf8("2.5".into()), &lt("DOUBLE")).unwrap(),
            Literal::Float64(2.5)
        );
        assert!(cast_literal(&Literal::Utf8("abc".into()), &lt("INT")).is_err());
        assert!(cast_literal(&Literal::Utf8("1.5".into()), &lt("INT")).is_err()); // 不做静默截断
    }

    #[test]
    fn cast_float_int_truncates() {
        assert_eq!(
            cast_literal(&Literal::Float64(2.7), &lt("INT")).unwrap(),
            Literal::Int32(2)
        );
        assert_eq!(
            cast_literal(&Literal::Float64(-2.7), &lt("INT")).unwrap(),
            Literal::Int32(-2)
        );
        assert!(cast_literal(&Literal::Float64(f64::NAN), &lt("INT")).is_err());
        assert!(cast_literal(&Literal::Float64(f64::INFINITY), &lt("INT")).is_err());
        assert!(cast_literal(&Literal::Float64(1e19), &lt("BIGINT")).is_err()); // 超 i64
    }

    #[test]
    fn cast_decimal_paths() {
        let m18_4 = d(18, 4);
        // 整数 → decimal
        assert_eq!(
            cast_literal(&Literal::Int64(5), &LogicalType::Decimal(m18_4)).unwrap(),
            Literal::Decimal {
                unscaled: 50_000,
                meta: m18_4
            }
        );
        // decimal HALF_UP 到整数
        assert_eq!(
            cast_literal(
                &Literal::Decimal {
                    unscaled: 15,
                    meta: d(5, 1)
                },
                &lt("INT")
            )
            .unwrap(),
            Literal::Int32(2) // 1.5 → 2
        );
        // decimal scale 缩小 HALF_UP
        assert_eq!(
            cast_literal(
                &Literal::Decimal {
                    unscaled: 1235,
                    meta: d(6, 3)
                },
                &LogicalType::Decimal(d(6, 2))
            )
            .unwrap(),
            Literal::Decimal {
                unscaled: 124,
                meta: d(6, 2)
            } // 1.235 → 1.24
        );
        // 字符串 → decimal
        assert_eq!(
            cast_literal(
                &Literal::Utf8("0.125".into()),
                &LogicalType::Decimal(d(5, 2))
            )
            .unwrap(),
            Literal::Decimal {
                unscaled: 13,
                meta: d(5, 2)
            }
        );
        // float → decimal（scale+1 文本中转 HALF_UP）
        assert_eq!(
            cast_literal(&Literal::Float64(123.456), &LogicalType::Decimal(d(10, 2))).unwrap(),
            Literal::Decimal {
                unscaled: 12346,
                meta: d(10, 2)
            }
        );
        // decimal → float
        assert_eq!(
            cast_literal(
                &Literal::Decimal {
                    unscaled: 25,
                    meta: d(5, 2)
                },
                &lt("DOUBLE")
            )
            .unwrap(),
            Literal::Float64(0.25)
        );
        // 溢出
        assert!(cast_literal(&Literal::Int64(100_000), &LogicalType::Decimal(d(5, 0))).is_err());
    }

    #[test]
    fn cast_string_bool() {
        assert_eq!(
            cast_literal(&Literal::Utf8("TRUE".into()), &lt("BOOLEAN")).unwrap(),
            Literal::Boolean(true)
        );
        assert_eq!(
            cast_literal(&Literal::Utf8("f".into()), &lt("BOOLEAN")).unwrap(),
            Literal::Boolean(false)
        );
        assert_eq!(
            cast_literal(&Literal::Boolean(false), &lt("STRING")).unwrap(),
            Literal::Utf8("false".into())
        );
        assert!(cast_literal(&Literal::Utf8("yes".into()), &lt("BOOLEAN")).is_err());
    }

    #[test]
    fn cast_temporal_paths() {
        let date = Literal::Date(20_735); // 2026-10-09
        let ts = cast_literal(&date, &lt("TIMESTAMP")).unwrap();
        assert_eq!(ts, Literal::Timestamp(20_735 * NANOS_PER_DAY));
        // 反向截断
        assert_eq!(
            cast_literal(
                &Literal::Timestamp(20_735 * NANOS_PER_DAY + 123),
                &lt("DATE")
            )
            .unwrap(),
            date
        );
        // 字符串解析
        assert_eq!(
            cast_literal(&Literal::Utf8("2026-10-09".into()), &lt("DATE")).unwrap(),
            date
        );
        assert_eq!(
            cast_literal(&Literal::Utf8("12:30:45".into()), &lt("TIME")).unwrap(),
            Literal::Time(12 * 3_600_000_000_000 + 30 * 60_000_000_000 + 45_000_000_000)
        );
        // 输出
        assert_eq!(
            cast_literal(&date, &lt("STRING")).unwrap(),
            Literal::Utf8("2026-10-09".into())
        );
        // DATE 超出 ns 表示范围
        assert!(cast_literal(&Literal::Date(2_932_896), &lt("TIMESTAMP")).is_err());
        assert!(cast_literal(&Literal::Utf8("2026-13-01".into()), &lt("DATE")).is_err());
    }

    #[test]
    fn cast_string_binary() {
        assert_eq!(
            cast_literal(&Literal::Utf8("qsdb".into()), &lt("BINARY")).unwrap(),
            Literal::Binary(b"qsdb".to_vec())
        );
        assert_eq!(
            cast_literal(&Literal::Binary(b"qsdb".to_vec()), &lt("STRING")).unwrap(),
            Literal::Utf8("qsdb".into())
        );
        assert!(cast_literal(&Literal::Binary(vec![0xFF, 0xFE]), &lt("STRING")).is_err());
    }

    #[test]
    fn impossible_casts_error() {
        assert!(cast_literal(&Literal::Boolean(true), &lt("INT")).is_err());
        assert!(cast_literal(&Literal::Int64(1), &lt("BOOLEAN")).is_err());
        assert!(cast_literal(&Literal::Int64(1), &lt("DATE")).is_err());
        assert!(
            cast_literal(
                &Literal::Interval {
                    months: 1,
                    days: 0,
                    nanos: 0
                },
                &lt("INT")
            )
            .is_err()
        );
    }
}
