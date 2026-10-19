//! 跨类型比较：列统计裁剪（min/max vs 谓词字面量）与表达式比较共用的语义。
//!
//! 规则：
//! - 任意一方为 NULL → 无序（`None`）
//! - 数值族统一比较：整数族提升 i64；浮点族提升 f64（NaN 无序）；
//!   DECIMAL 与整数/DECIMAL 对齐到公共 scale 后用 i256 精确比较；
//!   DECIMAL vs FLOAT 走 f64（有损，仅用于统计裁剪级判断，文档明示）
//! - 字符串按字节序；BINARY 按字节序；BOOLEAN false < true
//! - DATE vs TIMESTAMP 按 epoch 纳秒比较；TIME 独立
//! - INTERVAL 仅支持相等性（分量全等 → Equal，否则无序）
//! - 类型族不匹配 → 报错

use std::cmp::Ordering;

use arrow::datatypes::i256;

use super::LogicalType;
use super::decimal::pow10;
use super::literal::Literal;
use super::temporal::NANOS_PER_DAY;
use crate::error::{QsdbError, Result};

/// 跨类型比较。`Ok(None)` 表示无序（NULL / NaN / interval 分量不等）。
pub fn cmp_literals(a: &Literal, b: &Literal) -> Result<Option<Ordering>> {
    if a.is_null() || b.is_null() {
        return Ok(None);
    }
    match (a, b) {
        (Literal::Boolean(x), Literal::Boolean(y)) => Ok(Some(x.cmp(y))),
        // 整数族内部 → i64
        _ if is_int(a) && is_int(b) => Ok(Some(as_i64(a).cmp(&as_i64(b)))),
        // 浮点参与（int/float/decimal 混合中的 float）→ f64
        _ if is_float(a) && is_numeric(b) => float_cmp(a, b),
        _ if is_float(b) && is_numeric(a) => Ok(float_cmp(b, a)?.map(Ordering::reverse)),
        // decimal vs decimal / decimal vs int → i256 精确
        _ if is_decimal(a) && (is_decimal(b) || is_int(b)) => decimal_cmp(a, b),
        _ if is_decimal(b) && (is_decimal(a) || is_int(a)) => {
            Ok(decimal_cmp(b, a)?.map(Ordering::reverse))
        }
        (Literal::Utf8(x), Literal::Utf8(y)) => Ok(Some(x.cmp(y))),
        (Literal::Binary(x), Literal::Binary(y)) => Ok(Some(x.cmp(y))),
        (Literal::Date(x), Literal::Date(y)) => Ok(Some(x.cmp(y))),
        (Literal::Time(x), Literal::Time(y)) => Ok(Some(x.cmp(y))),
        (Literal::Timestamp(x), Literal::Timestamp(y)) => Ok(Some(x.cmp(y))),
        (Literal::Date(d), Literal::Timestamp(ns)) => {
            let Some(dns) = i64::from(*d).checked_mul(NANOS_PER_DAY) else {
                return Ok(None); // DATE 超出 TIMESTAMP ns 表示范围 → 视为无序
            };
            Ok(Some(dns.cmp(ns)))
        }
        (Literal::Timestamp(ns), Literal::Date(d)) => {
            let Some(dns) = i64::from(*d).checked_mul(NANOS_PER_DAY) else {
                return Ok(None);
            };
            Ok(Some(ns.cmp(&dns)))
        }
        (
            Literal::Interval {
                months: m1,
                days: d1,
                nanos: n1,
            },
            Literal::Interval {
                months: m2,
                days: d2,
                nanos: n2,
            },
        ) => {
            if m1 == m2 && d1 == d2 && n1 == n2 {
                Ok(Some(Ordering::Equal))
            } else {
                Ok(None)
            }
        }
        _ => Err(QsdbError::type_err(format!(
            "cannot compare {} with {}",
            a.logical_type()
                .map(|t| t.to_string())
                .unwrap_or_else(|| "NULL".into()),
            b.logical_type()
                .map(|t| t.to_string())
                .unwrap_or_else(|| "NULL".into())
        ))),
    }
}

/// 两个操作数的公共类型（表达式 coercion 用）；`Ok(None)` = 不兼容。
///
/// 数值合并：整数取宽、浮点升 Double、DECIMAL 对齐公共 scale
/// （超 38 位时退化 Double）；DATE + TIMESTAMP → TIMESTAMP。
pub fn common_type(a: &LogicalType, b: &LogicalType) -> Result<Option<LogicalType>> {
    if a == b {
        return Ok(Some(a.clone()));
    }
    let out = match (a, b) {
        // 整数族：取宽
        _ if a.is_integer() && b.is_integer() => {
            if a.numeric_rank() >= b.numeric_rank() {
                a.clone()
            } else {
                b.clone()
            }
        }
        // 浮点族 / 浮点与整数 / 浮点与 decimal
        (LogicalType::Float32, LogicalType::Float32) => LogicalType::Float32,
        _ if a.is_floating() && b.is_numeric() => LogicalType::Float64,
        _ if b.is_floating() && a.is_numeric() => LogicalType::Float64,
        // decimal vs decimal / decimal vs int：公共 scale 对齐
        _ if (a.is_decimal() && b.is_numeric() && !b.is_floating())
            || (b.is_decimal() && a.is_numeric() && !a.is_floating()) =>
        {
            let da = to_decimal_meta(a);
            let db = to_decimal_meta(b);
            let s = da.scale.max(db.scale);
            let ip = (i16::from(da.precision) - i16::from(da.scale))
                .max(i16::from(db.precision) - i16::from(db.scale));
            if ip + i16::from(s) > 38 {
                LogicalType::Float64 // 精度超限退化
            } else {
                LogicalType::Decimal(
                    super::decimal::DecimalMeta::try_new((ip + i16::from(s)) as u8, s)
                        .expect("guarded above"),
                )
            }
        }
        // 字符串族合并为 STRING（长度语义交给目标列）
        _ if a.is_string() && b.is_string() => LogicalType::Varchar { max_chars: None },
        // 时间：DATE + TIMESTAMP → TIMESTAMP（保留 tz，若有）
        (LogicalType::Date, LogicalType::Timestamp { timezone }) => LogicalType::Timestamp {
            timezone: timezone.clone(),
        },
        (LogicalType::Timestamp { timezone }, LogicalType::Date) => LogicalType::Timestamp {
            timezone: timezone.clone(),
        },
        (LogicalType::Binary, LogicalType::Binary)
        | (LogicalType::Boolean, LogicalType::Boolean)
        | (LogicalType::Interval, LogicalType::Interval) => a.clone(),
        _ => return Ok(None),
    };
    Ok(Some(out))
}

fn to_decimal_meta(t: &LogicalType) -> super::decimal::DecimalMeta {
    match t {
        LogicalType::Decimal(m) => *m,
        // 整数按其实际可表示的十进制位数处理
        LogicalType::Int8 => super::decimal::DecimalMeta::new(3, 0),
        LogicalType::Int16 => super::decimal::DecimalMeta::new(5, 0),
        LogicalType::Int32 => super::decimal::DecimalMeta::new(10, 0),
        _ => super::decimal::DecimalMeta::new(19, 0),
    }
}

// ---------------------------------------------------------------------------
// 内部提升辅助
// ---------------------------------------------------------------------------

fn is_int(l: &Literal) -> bool {
    matches!(
        l,
        Literal::Int8(_) | Literal::Int16(_) | Literal::Int32(_) | Literal::Int64(_)
    )
}

fn is_float(l: &Literal) -> bool {
    matches!(l, Literal::Float32(_) | Literal::Float64(_))
}

fn is_decimal(l: &Literal) -> bool {
    matches!(l, Literal::Decimal { .. })
}

fn is_numeric(l: &Literal) -> bool {
    is_int(l) || is_float(l) || is_decimal(l)
}

fn as_i64(l: &Literal) -> i64 {
    match l {
        Literal::Int8(v) => i64::from(*v),
        Literal::Int16(v) => i64::from(*v),
        Literal::Int32(v) => i64::from(*v),
        Literal::Int64(v) => *v,
        _ => unreachable!("as_i64 on non-int literal"),
    }
}

fn as_f64(l: &Literal) -> f64 {
    match l {
        Literal::Int8(v) => f64::from(*v),
        Literal::Int16(v) => f64::from(*v),
        Literal::Int32(v) => f64::from(*v),
        Literal::Int64(v) => *v as f64,
        Literal::Float32(v) => f64::from(*v),
        Literal::Float64(v) => *v,
        Literal::Decimal { unscaled, meta } => *unscaled as f64 / pow10(meta.scale as u32) as f64,
        _ => unreachable!("as_f64 on non-float literal"),
    }
}

/// float 一方在前，与任意数值比较；NaN → 无序。
fn float_cmp(f: &Literal, other: &Literal) -> Result<Option<Ordering>> {
    let x = as_f64(f);
    let y = as_f64(other);
    Ok(x.partial_cmp(&y))
}

/// decimal 一方在前（decimal vs decimal/int），公共 scale + i256 精确比较。
fn decimal_cmp(d: &Literal, other: &Literal) -> Result<Option<Ordering>> {
    let scale = match (d, other) {
        (Literal::Decimal { meta: m1, .. }, Literal::Decimal { meta: m2, .. }) => {
            m1.scale.max(m2.scale)
        }
        (Literal::Decimal { meta, .. }, _) => meta.scale,
        _ => unreachable!(),
    };
    let x = to_i256_scaled(d, scale);
    let y = to_i256_scaled(other, scale);
    Ok(Some(x.cmp(&y)))
}

/// 数值字面量对齐到指定 scale 的 i256（decimal / 整数）。
fn to_i256_scaled(l: &Literal, scale: i8) -> i256 {
    let (base, from_scale) = match l {
        Literal::Decimal { unscaled, meta } => (i256::from_i128(*unscaled), meta.scale),
        Literal::Int8(v) => (i256::from_i128(i128::from(*v)), 0),
        Literal::Int16(v) => (i256::from_i128(i128::from(*v)), 0),
        Literal::Int32(v) => (i256::from_i128(i128::from(*v)), 0),
        Literal::Int64(v) => (i256::from_i128(i128::from(*v)), 0),
        _ => unreachable!("to_i256_scaled on non-decimal/int literal"),
    };
    if scale == from_scale {
        base
    } else {
        // scale ≥ from_scale（调用方保证公共 scale 对齐）
        base * i256::from_i128(pow10((scale - from_scale) as u32))
    }
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use super::super::decimal::DecimalMeta;
    use super::*;

    fn d(p: u8, s: i8) -> DecimalMeta {
        DecimalMeta::try_new(p, s).unwrap()
    }

    #[test]
    fn null_is_unordered() {
        assert_eq!(
            cmp_literals(&Literal::Null, &Literal::Int64(1)).unwrap(),
            None
        );
        assert_eq!(
            cmp_literals(&Literal::Int64(1), &Literal::Null).unwrap(),
            None
        );
        assert_eq!(cmp_literals(&Literal::Null, &Literal::Null).unwrap(), None);
    }

    #[test]
    fn int_family_promotes_to_i64() {
        let a = Literal::Int8(-5);
        let b = Literal::Int64(-3);
        assert_eq!(cmp_literals(&a, &b).unwrap(), Some(Ordering::Less));
        let c = Literal::Int32(2_000_000_000);
        assert_eq!(cmp_literals(&c, &a).unwrap(), Some(Ordering::Greater));
        // i64 极值
        assert_eq!(
            cmp_literals(&Literal::Int64(i64::MIN), &Literal::Int64(i64::MAX)).unwrap(),
            Some(Ordering::Less)
        );
    }

    #[test]
    fn float_mixing_and_nan() {
        let f = Literal::Float64(2.5);
        let i = Literal::Int64(2);
        assert_eq!(cmp_literals(&f, &i).unwrap(), Some(Ordering::Greater));
        assert_eq!(cmp_literals(&i, &f).unwrap(), Some(Ordering::Less));
        let f32l = Literal::Float32(1.5);
        assert_eq!(
            cmp_literals(&f32l, &Literal::Int8(1)).unwrap(),
            Some(Ordering::Greater)
        );
        let nan = Literal::Float64(f64::NAN);
        assert_eq!(cmp_literals(&nan, &f).unwrap(), None);
        assert_eq!(cmp_literals(&f, &nan).unwrap(), None);
    }

    #[test]
    fn decimal_exact_compare_i256() {
        // 0.10 (s=2) vs 0.1 (s=1)：公共 scale 对齐后相等
        let a = Literal::Decimal {
            unscaled: 10,
            meta: d(5, 2),
        };
        let b = Literal::Decimal {
            unscaled: 1,
            meta: d(5, 1),
        };
        assert_eq!(cmp_literals(&a, &b).unwrap(), Some(Ordering::Equal));
        // 大精度：38 位整数 vs i64
        let big = Literal::Decimal {
            unscaled: pow10(38) - 1,
            meta: d(38, 0),
        };
        assert_eq!(
            cmp_literals(&big, &Literal::Int64(i64::MAX)).unwrap(),
            Some(Ordering::Greater)
        );
        // decimal vs int
        let dec = Literal::Decimal {
            unscaled: 42,
            meta: d(10, 0),
        };
        assert_eq!(
            cmp_literals(&dec, &Literal::Int64(42)).unwrap(),
            Some(Ordering::Equal)
        );
        // 负数比较
        let neg = Literal::Decimal {
            unscaled: -1,
            meta: d(5, 2),
        };
        assert_eq!(
            cmp_literals(&neg, &Literal::Int8(0)).unwrap(),
            Some(Ordering::Less)
        );
    }

    #[test]
    fn decimal_vs_float_is_lossy_f64() {
        let dec = Literal::Decimal {
            unscaled: 1,
            meta: d(5, 1),
        }; // 0.1
        let f = Literal::Float64(0.1);
        assert_eq!(cmp_literals(&dec, &f).unwrap(), Some(Ordering::Equal));
    }

    #[test]
    fn string_binary_bool_order() {
        assert_eq!(
            cmp_literals(&Literal::Utf8("abc".into()), &Literal::Utf8("abd".into())).unwrap(),
            Some(Ordering::Less)
        );
        // 中文字节序
        assert_eq!(
            cmp_literals(&Literal::Utf8("你".into()), &Literal::Utf8("好".into())).unwrap(),
            Some(Ordering::Less)
        );
        assert_eq!(
            cmp_literals(&Literal::Binary(vec![1, 2]), &Literal::Binary(vec![1, 3])).unwrap(),
            Some(Ordering::Less)
        );
        assert_eq!(
            cmp_literals(&Literal::Boolean(false), &Literal::Boolean(true)).unwrap(),
            Some(Ordering::Less)
        );
    }

    #[test]
    fn temporal_compare() {
        let date = Literal::Date(20_730); // 2026-10-04
        let ts = Literal::Timestamp(20_730 * NANOS_PER_DAY + 1);
        assert_eq!(cmp_literals(&date, &ts).unwrap(), Some(Ordering::Less));
        assert_eq!(cmp_literals(&ts, &date).unwrap(), Some(Ordering::Greater));
        let ts0 = Literal::Timestamp(20_730 * NANOS_PER_DAY);
        assert_eq!(cmp_literals(&date, &ts0).unwrap(), Some(Ordering::Equal));
        assert_eq!(
            cmp_literals(&Literal::Time(1), &Literal::Time(2)).unwrap(),
            Some(Ordering::Less)
        );
    }

    #[test]
    fn interval_equality_only() {
        let i1 = Literal::Interval {
            months: 1,
            days: 2,
            nanos: 3,
        };
        let i2 = Literal::Interval {
            months: 1,
            days: 2,
            nanos: 3,
        };
        let i3 = Literal::Interval {
            months: 1,
            days: 2,
            nanos: 4,
        };
        assert_eq!(cmp_literals(&i1, &i2).unwrap(), Some(Ordering::Equal));
        assert_eq!(cmp_literals(&i1, &i3).unwrap(), None);
    }

    #[test]
    fn family_mismatch_errors() {
        assert!(cmp_literals(&Literal::Int64(1), &Literal::Utf8("x".into())).is_err());
        assert!(cmp_literals(&Literal::Boolean(true), &Literal::Int64(1)).is_err());
        assert!(cmp_literals(&Literal::Date(0), &Literal::Time(0)).is_err());
        assert!(cmp_literals(&Literal::Utf8("a".into()), &Literal::Binary(b"a".to_vec())).is_err());
    }

    #[test]
    fn common_type_rules() {
        use LogicalType as Lt;
        let c = |a: &Lt, b: &Lt| common_type(a, b).unwrap();
        // 整数取宽
        assert_eq!(c(&Lt::Int8, &Lt::Int32), Some(Lt::Int32));
        assert_eq!(c(&Lt::Int64, &Lt::Int16), Some(Lt::Int64));
        // 浮点
        assert_eq!(c(&Lt::Float32, &Lt::Int64), Some(Lt::Float64));
        assert_eq!(c(&Lt::Float32, &Lt::Float32), Some(Lt::Float32));
        assert_eq!(c(&Lt::Float64, &Lt::Float32), Some(Lt::Float64));
        // decimal
        assert_eq!(
            c(&Lt::Decimal(d(10, 2)), &Lt::Int32),
            Some(Lt::Decimal(d(12, 2)))
        );
        assert_eq!(
            c(&Lt::Decimal(d(38, 5)), &Lt::Decimal(d(38, 6))),
            Some(Lt::Float64) // ip 33 + 6 > 38 → 退化
        );
        // 字符串
        assert_eq!(
            c(&Lt::Varchar { max_chars: Some(3) }, &Lt::Char { length: 2 }),
            Some(Lt::Varchar { max_chars: None })
        );
        // 时间
        assert_eq!(
            c(&Lt::Date, &Lt::Timestamp { timezone: None }),
            Some(Lt::Timestamp { timezone: None })
        );
    }

    #[test]
    fn common_type_incompatible() {
        use LogicalType as Lt;
        assert_eq!(
            common_type(&Lt::Int32, &Lt::Varchar { max_chars: None }).unwrap(),
            None
        );
        assert_eq!(common_type(&Lt::Boolean, &Lt::Int8).unwrap(), None);
        assert_eq!(common_type(&Lt::Time, &Lt::Date).unwrap(), None);
    }
}
