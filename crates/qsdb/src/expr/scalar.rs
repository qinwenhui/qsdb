//! 标量函数注册表（SQL-1 内置集 + UDF 回退）。
//!
//! 内置：length / upper / lower / trim / substr / abs / round / floor / ceil /
//! sqrt / exp / ln / log / sin / cos / tan / atan / power(pow)。
//! 全部行级函数：NULL 输入 → NULL 输出；数值函数覆盖 Int64/Float64/Decimal 入参。
//! 未命中内置表时查全局 UDF 注册表（见 [`crate::expr::udf`]）。

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, Date32Array, Float64Array, GenericStringArray, Int64Array,
    Time64NanosecondArray, TimestampNanosecondArray,
};
use arrow::datatypes::{Float64Type, Int64Type};

use crate::error::{QsdbError, Result};
use crate::types::LogicalType;
use crate::types::temporal::{NANOS_PER_DAY, NANOS_PER_HOUR, NANOS_PER_MIN, NANOS_PER_SEC};

use super::eval::{RoundDir, decimal_abs, decimal_round_dir};

pub type RetTyFn = Arc<dyn Fn(&[LogicalType]) -> Result<LogicalType> + Send + Sync>;
pub type EvalFn = Arc<dyn Fn(&[ArrayRef]) -> Result<ArrayRef> + Send + Sync>;

/// 标量函数定义（内置与 UDF 共用）。
pub struct ScalarFnDef {
    /// 入参类型 → 返回类型（binder 类型检查）
    pub ret_ty: RetTyFn,
    /// 向量化求值（整批进整批出；NULL 处理由实现负责）
    pub eval: EvalFn,
}

fn primitive_map<T, F>(args: &[ArrayRef], f: F) -> Result<ArrayRef>
where
    T: arrow::datatypes::ArrowPrimitiveType,
    F: Fn(T::Native) -> T::Native,
{
    let a = args[0]
        .as_any()
        .downcast_ref::<arrow::array::PrimitiveArray<T>>()
        .ok_or_else(|| QsdbError::type_err("abs: bad arg type"))?;
    let out: arrow::array::PrimitiveArray<T> = a.iter().map(|v| v.map(&f)).collect();
    Ok(Arc::new(out))
}

fn registry() -> &'static HashMap<&'static str, Arc<ScalarFnDef>> {
    use std::sync::OnceLock;
    static REG: OnceLock<HashMap<&'static str, Arc<ScalarFnDef>>> = OnceLock::new();
    REG.get_or_init(build)
}

fn build() -> HashMap<&'static str, Arc<ScalarFnDef>> {
    let mut m: HashMap<&'static str, Arc<ScalarFnDef>> = HashMap::new();

    // length(string) -> bigint
    m.insert(
        "length",
        Arc::new(ScalarFnDef {
            ret_ty: Arc::new(|args| match args {
                [t] if t.is_string() => Ok(LogicalType::Int64),
                _ => Err(QsdbError::type_err("length(string) expected")),
            }),
            eval: Arc::new(|args| {
                let s = string_arg(args, 0, "length")?;
                let out: Int64Array = s
                    .iter()
                    .map(|v| v.map(|x| x.chars().count() as i64))
                    .collect();
                Ok(Arc::new(out))
            }),
        }),
    );

    // upper / lower / trim(string) -> string
    for name in ["upper", "lower", "trim"] {
        let mode: &'static str = name;
        m.insert(
            name,
            Arc::new(ScalarFnDef {
                ret_ty: Arc::new(move |args| match args {
                    [t] if t.is_string() => Ok(t.clone()),
                    _ => Err(QsdbError::type_err(format!("{mode}(string) expected"))),
                }),
                eval: Arc::new(move |args| {
                    let s = string_arg(args, 0, mode)?;
                    let out: GenericStringArray<i32> = s
                        .iter()
                        .map(|v| {
                            v.map(|x| match mode {
                                "upper" => x.to_uppercase(),
                                "lower" => x.to_lowercase(),
                                _ => x.trim().to_string(),
                            })
                        })
                        .collect();
                    Ok(Arc::new(out))
                }),
            }),
        );
    }

    // substr(string, start[, len]) -> string（1-based）
    m.insert(
        "substr",
        Arc::new(ScalarFnDef {
            ret_ty: Arc::new(|args| match args {
                [t, LogicalType::Int64] | [t, LogicalType::Int64, LogicalType::Int64]
                    if t.is_string() =>
                {
                    Ok(t.clone())
                }
                _ => Err(QsdbError::type_err("substr(string, start[, len]) expected")),
            }),
            eval: Arc::new(|args| {
                let s = string_arg(args, 0, "substr")?;
                let start = int_arg(args, 1, "substr")?;
                let len_arr: Option<&Int64Array> = if args.len() > 2 {
                    Some(int_arg(args, 2, "substr")?)
                } else {
                    None
                };
                let mut out = arrow::array::StringBuilder::new();
                for i in 0..s.len() {
                    if s.is_null(i) || start.is_null(i) {
                        out.append_null();
                        continue;
                    }
                    if let Some(l) = len_arr {
                        if l.is_null(i) {
                            out.append_null();
                            continue;
                        }
                    }
                    let sv = s.value(i);
                    let chars: Vec<char> = sv.chars().collect();
                    let start0 = (start.value(i) - 1).max(0) as usize;
                    if start0 >= chars.len() {
                        out.append_value("");
                        continue;
                    }
                    let take = match len_arr {
                        None => chars.len() - start0,
                        Some(l) => {
                            let lv = l.value(i);
                            if lv <= 0 {
                                0
                            } else {
                                (lv as usize).min(chars.len() - start0)
                            }
                        }
                    };
                    out.append_value(chars[start0..start0 + take].iter().collect::<String>());
                }
                Ok(Arc::new(out.finish()))
            }),
        }),
    );

    // abs(numeric) -> numeric（BIGINT/DOUBLE/DECIMAL）
    m.insert(
        "abs",
        Arc::new(ScalarFnDef {
            ret_ty: Arc::new(|args| match args {
                [t] if t.is_numeric() => Ok(t.clone()),
                _ => Err(QsdbError::type_err("abs(numeric) expected")),
            }),
            eval: Arc::new(|args| match args[0].data_type() {
                arrow::datatypes::DataType::Int64 => {
                    primitive_map::<Int64Type, _>(args, |v: i64| v.wrapping_abs())
                }
                arrow::datatypes::DataType::Float64 => {
                    primitive_map::<Float64Type, _>(args, f64::abs)
                }
                arrow::datatypes::DataType::Decimal128(..) => decimal_abs(&args[0]),
                other => Err(QsdbError::unsupported(format!(
                    "abs on {other:?} (支持 BIGINT/DOUBLE/DECIMAL)"
                ))),
            }),
        }),
    );

    // round / floor / ceil(numeric[, digits]) -> 同型
    // （DOUBLE 浮点路径 / DECIMAL i256 精确路径 / BIGINT 恒等）
    for (name, dir) in [
        ("round", RoundDir::HalfUp),
        ("floor", RoundDir::Floor),
        ("ceil", RoundDir::Ceil),
    ] {
        let nm: &'static str = name;
        m.insert(
            name,
            Arc::new(ScalarFnDef {
                ret_ty: Arc::new(move |args| match args {
                    [LogicalType::Float64] | [LogicalType::Float64, LogicalType::Int64] => {
                        Ok(LogicalType::Float64)
                    }
                    [t @ LogicalType::Decimal(_)]
                    | [t @ LogicalType::Decimal(_), LogicalType::Int64] => Ok(t.clone()),
                    [LogicalType::Int64] | [LogicalType::Int64, LogicalType::Int64] => {
                        Ok(LogicalType::Int64)
                    }
                    _ => Err(QsdbError::type_err(format!(
                        "{nm}(numeric[, digits]) expected"
                    ))),
                }),
                eval: Arc::new(move |args| {
                    let digits: i64 = if args.len() > 1 {
                        int_arg(args, 1, nm)?.value(0)
                    } else {
                        0
                    };
                    match args[0].data_type() {
                        arrow::datatypes::DataType::Float64 => {
                            let v = args[0].as_any().downcast_ref::<Float64Array>().ok_or_else(
                                || QsdbError::type_err(format!("{nm}: bad arg type")),
                            )?;
                            let f = |x: f64| match dir {
                                RoundDir::HalfUp => {
                                    let m = 10f64.powi(digits as i32);
                                    (x * m + if x >= 0.0 { 0.5 } else { -0.5 }).trunc() / m
                                }
                                RoundDir::Floor => x.floor(),
                                RoundDir::Ceil => x.ceil(),
                            };
                            let out: Float64Array = v.iter().map(|x| x.map(f)).collect();
                            Ok(Arc::new(out))
                        }
                        arrow::datatypes::DataType::Decimal128(..) => {
                            decimal_round_dir(&args[0], digits, dir)
                        }
                        // 整数入参：digits >= 0 恒等（digits < 0 的十位取整不支持）
                        _ => Ok(Arc::clone(&args[0])),
                    }
                }),
            }),
        );
    }

    // ── 数学函数（numeric → double；f64 向量化，域错误逐行报错）─────────
    type UnaryMath = (&'static str, fn(f64) -> Result<f64>);
    let unary_math: &[UnaryMath] = &[
        ("sqrt", |x| {
            if x < 0.0 {
                Err(QsdbError::type_err(
                    "cannot take square root of a negative number",
                ))
            } else {
                Ok(x.sqrt())
            }
        }),
        ("exp", |x| Ok(x.exp())),
        ("ln", |x| {
            if x <= 0.0 {
                Err(QsdbError::type_err(
                    "cannot take logarithm of a non-positive number",
                ))
            } else {
                Ok(x.ln())
            }
        }),
        ("log", |x| {
            if x <= 0.0 {
                Err(QsdbError::type_err(
                    "cannot take logarithm of a non-positive number",
                ))
            } else {
                Ok(x.log10())
            }
        }),
        ("sin", |x| Ok(x.sin())),
        ("cos", |x| Ok(x.cos())),
        ("tan", |x| Ok(x.tan())),
        ("atan", |x| Ok(x.atan())),
    ];
    for (name, f) in unary_math {
        let nm: &'static str = name;
        m.insert(
            name,
            Arc::new(ScalarFnDef {
                ret_ty: Arc::new(move |args| match args {
                    [t] if t.is_numeric() => Ok(LogicalType::Float64),
                    _ => Err(QsdbError::type_err(format!("{nm}(numeric) expected"))),
                }),
                eval: Arc::new(move |args| {
                    let xs = f64_values(&args[0])?;
                    let mut out: Vec<Option<f64>> = Vec::with_capacity(xs.len());
                    for x in &xs {
                        out.push(match x {
                            Some(v) => Some(f(*v)?),
                            None => None,
                        });
                    }
                    Ok(Arc::new(Float64Array::from(out)))
                }),
            }),
        );
    }

    // power(a, b) / pow 别名：0 的负幂 / 溢出报错
    for name in ["power", "pow"] {
        let nm: &'static str = name;
        m.insert(
            name,
            Arc::new(ScalarFnDef {
                ret_ty: Arc::new(move |args| match args {
                    [a, b] if a.is_numeric() && b.is_numeric() => Ok(LogicalType::Float64),
                    _ => Err(QsdbError::type_err(format!("{nm}(a, b) expected"))),
                }),
                eval: Arc::new(move |args| {
                    let xs = f64_values(&args[0])?;
                    let ys = f64_values(&args[1])?;
                    let n = xs.len().max(ys.len());
                    let mut out: Vec<Option<f64>> = Vec::with_capacity(n);
                    for i in 0..n {
                        let x = xs.get(i).copied().flatten();
                        let y = ys.get(i).copied().flatten();
                        match (x, y) {
                            (Some(x), Some(y)) => {
                                if x == 0.0 && y < 0.0 {
                                    return Err(QsdbError::type_err(
                                        "zero raised to a negative power is undefined",
                                    ));
                                }
                                let r = x.powf(y);
                                if !r.is_finite() {
                                    return Err(QsdbError::type_err(format!(
                                        "{nm} result out of range"
                                    )));
                                }
                                out.push(Some(r));
                            }
                            _ => out.push(None),
                        }
                    }
                    Ok(Arc::new(Float64Array::from(out)))
                }),
            }),
        );
    }

    // ── 日期函数：extract(field, x) → BIGINT；date_diff(unit, a, b) → BIGINT ──
    m.insert(
        "extract",
        Arc::new(ScalarFnDef {
            ret_ty: Arc::new(|args| match args {
                [
                    _,
                    LogicalType::Date | LogicalType::Timestamp { .. } | LogicalType::Time,
                ] => Ok(LogicalType::Int64),
                _ => Err(QsdbError::type_err(
                    "extract(field, DATE/TIMESTAMP/TIME) expected",
                )),
            }),
            eval: Arc::new(|args| {
                let f = string_arg(args, 0, "extract")?;
                let field = f.value(0).to_ascii_lowercase();
                match args[1].data_type() {
                    arrow::datatypes::DataType::Date32 => {
                        let a = args[1]
                            .as_any()
                            .downcast_ref::<Date32Array>()
                            .ok_or_else(|| QsdbError::type_err("extract: bad date arg"))?;
                        map_rows(a.iter(), |d| extract_field(&field, d, 0))
                    }
                    arrow::datatypes::DataType::Timestamp(
                        arrow::datatypes::TimeUnit::Nanosecond,
                        _,
                    ) => {
                        let a = args[1]
                            .as_any()
                            .downcast_ref::<TimestampNanosecondArray>()
                            .ok_or_else(|| QsdbError::type_err("extract: bad timestamp arg"))?;
                        map_rows(a.iter(), |ns| {
                            extract_field(
                                &field,
                                ns.div_euclid(NANOS_PER_DAY) as i32,
                                ns.rem_euclid(NANOS_PER_DAY),
                            )
                        })
                    }
                    arrow::datatypes::DataType::Time64(arrow::datatypes::TimeUnit::Nanosecond) => {
                        let a = args[1]
                            .as_any()
                            .downcast_ref::<Time64NanosecondArray>()
                            .ok_or_else(|| QsdbError::type_err("extract: bad time arg"))?;
                        map_rows(a.iter(), |ns| extract_field(&field, 0, ns))
                    }
                    other => Err(QsdbError::unsupported(format!("extract on {other:?}"))),
                }
            }),
        }),
    );

    m.insert(
        "date_diff",
        Arc::new(ScalarFnDef {
            ret_ty: Arc::new(|args| match args {
                [_, t, u]
                    if t == u
                        && matches!(
                            t,
                            LogicalType::Date | LogicalType::Timestamp { .. } | LogicalType::Time
                        ) =>
                {
                    Ok(LogicalType::Int64)
                }
                _ => Err(QsdbError::type_err(
                    "date_diff(unit, a, b) expected（a/b 同为 DATE/TIMESTAMP/TIME）",
                )),
            }),
            eval: Arc::new(|args| {
                let f = string_arg(args, 0, "date_diff")?;
                let unit = normalize_unit(f.value(0));
                let date_rows = |a: &dyn Array, b: &dyn Array| -> Result<ArrayRef> {
                    let (a, b) = (
                        a.as_any().downcast_ref::<Date32Array>().unwrap(),
                        b.as_any().downcast_ref::<Date32Array>().unwrap(),
                    );
                    map_rows(
                        a.iter().zip(b.iter()).map(|(x, y)| match (x, y) {
                            (Some(x), Some(y)) => Some((x, y)),
                            _ => None,
                        }),
                        |(x, y)| date_diff_days(&unit, x, y).map(i64::from),
                    )
                };
                match args[1].data_type() {
                    arrow::datatypes::DataType::Date32 => {
                        date_rows(args[1].as_ref(), args[2].as_ref())
                    }
                    arrow::datatypes::DataType::Timestamp(
                        arrow::datatypes::TimeUnit::Nanosecond,
                        _,
                    ) => {
                        let (a, b) = (
                            args[1]
                                .as_any()
                                .downcast_ref::<TimestampNanosecondArray>()
                                .ok_or_else(|| QsdbError::type_err("date_diff: bad arg"))?,
                            args[2]
                                .as_any()
                                .downcast_ref::<TimestampNanosecondArray>()
                                .ok_or_else(|| QsdbError::type_err("date_diff: bad arg"))?,
                        );
                        map_rows(
                            a.iter().zip(b.iter()).map(|(x, y)| match (x, y) {
                                (Some(x), Some(y)) => Some((x, y)),
                                _ => None,
                            }),
                            |(x, y)| timestamp_diff_ns(&unit, x, y),
                        )
                    }
                    arrow::datatypes::DataType::Time64(arrow::datatypes::TimeUnit::Nanosecond) => {
                        let (a, b) = (
                            args[1]
                                .as_any()
                                .downcast_ref::<Time64NanosecondArray>()
                                .ok_or_else(|| QsdbError::type_err("date_diff: bad arg"))?,
                            args[2]
                                .as_any()
                                .downcast_ref::<Time64NanosecondArray>()
                                .ok_or_else(|| QsdbError::type_err("date_diff: bad arg"))?,
                        );
                        map_rows(
                            a.iter().zip(b.iter()).map(|(x, y)| match (x, y) {
                                (Some(x), Some(y)) => Some((x, y)),
                                _ => None,
                            }),
                            |(x, y)| timestamp_diff_ns(&unit, x, y),
                        )
                    }
                    other => Err(QsdbError::unsupported(format!("date_diff on {other:?}"))),
                }
            }),
        }),
    );

    // date_trunc(unit, x) → 截断到单位边界（同型返回）
    m.insert(
        "date_trunc",
        Arc::new(ScalarFnDef {
            ret_ty: Arc::new(|args| match args {
                [_, t @ LogicalType::Date]
                | [_, t @ LogicalType::Timestamp { .. }]
                | [_, t @ LogicalType::Time] => Ok(t.clone()),
                _ => Err(QsdbError::type_err(
                    "date_trunc(unit, DATE/TIMESTAMP/TIME) expected",
                )),
            }),
            eval: Arc::new(|args| {
                let f = string_arg(args, 0, "date_trunc")?;
                let unit = normalize_unit(f.value(0));
                match args[1].data_type() {
                    arrow::datatypes::DataType::Date32 => {
                        let a = args[1]
                            .as_any()
                            .downcast_ref::<Date32Array>()
                            .ok_or_else(|| QsdbError::type_err("date_trunc: bad date arg"))?;
                        let mut out: Vec<Option<i32>> = Vec::with_capacity(a.len());
                        for v in a.iter() {
                            out.push(match v {
                                Some(d) => Some(trunc_days(&unit, d)?),
                                None => None,
                            });
                        }
                        Ok(Arc::new(Date32Array::from(out)))
                    }
                    arrow::datatypes::DataType::Timestamp(
                        arrow::datatypes::TimeUnit::Nanosecond,
                        _,
                    ) => {
                        let a = args[1]
                            .as_any()
                            .downcast_ref::<TimestampNanosecondArray>()
                            .ok_or_else(|| QsdbError::type_err("date_trunc: bad timestamp arg"))?;
                        let mut out: Vec<Option<i64>> = Vec::with_capacity(a.len());
                        for v in a.iter() {
                            out.push(match v {
                                Some(ns) => Some(trunc_ns(&unit, ns)?),
                                None => None,
                            });
                        }
                        Ok(Arc::new(TimestampNanosecondArray::from(out)))
                    }
                    arrow::datatypes::DataType::Time64(arrow::datatypes::TimeUnit::Nanosecond) => {
                        let a = args[1]
                            .as_any()
                            .downcast_ref::<Time64NanosecondArray>()
                            .ok_or_else(|| QsdbError::type_err("date_trunc: bad time arg"))?;
                        let mut out: Vec<Option<i64>> = Vec::with_capacity(a.len());
                        for v in a.iter() {
                            out.push(match v {
                                Some(ns) => Some(trunc_time_ns(&unit, ns)?),
                                None => None,
                            });
                        }
                        Ok(Arc::new(Time64NanosecondArray::from(out)))
                    }
                    other => Err(QsdbError::unsupported(format!("date_trunc on {other:?}"))),
                }
            }),
        }),
    );

    // ── 当前时间（无参：current_date / current_timestamp / now）──────
    // UTC（无时区会话概念；Date32 天 = secs/86400 直取）
    m.insert(
        "current_date",
        Arc::new(ScalarFnDef {
            ret_ty: Arc::new(|args| match args {
                [] => Ok(LogicalType::Date),
                _ => Err(QsdbError::type_err("current_date() takes no arguments")),
            }),
            eval: Arc::new(|_args| {
                let days = now_epoch_secs() / 86_400;
                Ok(Arc::new(Date32Array::from(vec![Some(days as i32)])))
            }),
        }),
    );
    for name in ["current_timestamp", "now"] {
        m.insert(
            name,
            Arc::new(ScalarFnDef {
                ret_ty: Arc::new(|args| match args {
                    [] => Ok(LogicalType::Timestamp { timezone: None }),
                    _ => Err(QsdbError::type_err(
                        "current_timestamp() takes no arguments",
                    )),
                }),
                eval: Arc::new(|_args| {
                    use std::time::{SystemTime, UNIX_EPOCH};
                    let d = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map_err(|e| QsdbError::invalid(format!("clock error: {e}")))?;
                    let ns = d.as_secs() as i64 * 1_000_000_000 + i64::from(d.subsec_nanos());
                    Ok(Arc::new(TimestampNanosecondArray::from(vec![Some(ns)])))
                }),
            }),
        );
    }

    m
}

/// epoch 秒（current_date 用）。
fn now_epoch_secs() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// DATE 级单位截断（周一边界同 date_diff；时间单位不适用于 DATE）。
fn trunc_days(unit: &str, d: i32) -> Result<i32> {
    use crate::types::temporal::{civil_from_days, days_from_civil};
    let (y, m, _) = civil_from_days(d);
    Ok(match unit {
        "day" => d,
        "week" => d - (d - 4).rem_euclid(7), // 回到周一（epoch 天 4）
        "month" => days_from_civil(y, m, 1),
        "quarter" => days_from_civil(y, (m - 1) / 3 * 3 + 1, 1),
        "year" => days_from_civil(y, 1, 1),
        other => {
            return Err(QsdbError::invalid(format!(
                "date_trunc unit '{other}' not supported on DATE (year/quarter/month/week/day)"
            )));
        }
    })
}

/// TIMESTAMP 纳秒截断（时间单位按纳秒边界；日期单位归约到天后置零点）。
fn trunc_ns(unit: &str, ns: i64) -> Result<i64> {
    Ok(match unit {
        "second" => ns.div_euclid(NANOS_PER_SEC) * NANOS_PER_SEC,
        "minute" => ns.div_euclid(NANOS_PER_MIN) * NANOS_PER_MIN,
        "hour" => ns.div_euclid(NANOS_PER_HOUR) * NANOS_PER_HOUR,
        _ => i64::from(trunc_days(unit, ns.div_euclid(NANOS_PER_DAY) as i32)?) * NANOS_PER_DAY,
    })
}

/// TIME 纳秒截断（仅时间单位）。
fn trunc_time_ns(unit: &str, ns: i64) -> Result<i64> {
    Ok(match unit {
        "second" => ns.div_euclid(NANOS_PER_SEC) * NANOS_PER_SEC,
        "minute" => ns.div_euclid(NANOS_PER_MIN) * NANOS_PER_MIN,
        "hour" => ns.div_euclid(NANOS_PER_HOUR) * NANOS_PER_HOUR,
        other => {
            return Err(QsdbError::invalid(format!(
                "date_trunc unit '{other}' not supported on TIME (hour/minute/second)"
            )));
        }
    })
}

/// 单位名归一（复数收单数：days→day）。
fn normalize_unit(u: &str) -> String {
    let u = u.to_ascii_lowercase();
    match u.as_str() {
        "days" | "weeks" | "months" | "quarters" | "years" | "hours" | "minutes" | "seconds" => {
            u.trim_end_matches('s').to_string()
        }
        _ => u,
    }
}

/// 逐行 map（NULL 传播）→ Int64Array。
fn map_rows<I, F, T>(rows: I, f: F) -> Result<ArrayRef>
where
    I: Iterator<Item = Option<T>>,
    F: Fn(T) -> Result<i64>,
{
    let mut out: Vec<Option<i64>> = Vec::new();
    for v in rows {
        out.push(match v {
            Some(x) => Some(f(x)?),
            None => None,
        });
    }
    Ok(Arc::new(Int64Array::from(out)))
}

/// EXTRACT 字段求值（days=epoch 天；ns_in_day=日内纳秒）。
fn extract_field(field: &str, days: i32, ns_in_day: i64) -> Result<i64> {
    use crate::types::temporal::{civil_from_days, days_from_civil};
    let (y, m, d) = civil_from_days(days);
    Ok(match field {
        "year" => i64::from(y),
        "quarter" => i64::from((m - 1) / 3 + 1),
        "month" => i64::from(m),
        "day" => i64::from(d),
        "doy" => i64::from(days - days_from_civil(y, 1, 1) + 1),
        "dow" => i64::from((days + 4).rem_euclid(7)), // 0=周日（1970-01-01=周四）
        "hour" => ns_in_day / NANOS_PER_HOUR,
        "minute" => (ns_in_day % NANOS_PER_HOUR) / NANOS_PER_MIN,
        "second" => (ns_in_day % NANOS_PER_MIN) / NANOS_PER_SEC,
        other => {
            return Err(QsdbError::unsupported(format!(
                "extract field '{other}' not supported"
            )));
        }
    })
}

/// DATE 级单位差（DuckDB 边界跨越语义：跨过几个该单位边界）。
fn date_diff_days(unit: &str, d1: i32, d2: i32) -> Result<i32> {
    use crate::types::temporal::civil_from_days;
    let bad_unit = || {
        QsdbError::invalid(format!(
            "date_diff unit '{unit}' not supported on DATE（year/quarter/month/week/day）"
        ))
    };
    let (y1, m1, _) = civil_from_days(d1);
    let (y2, m2, _) = civil_from_days(d2);
    Ok(match unit {
        "day" => d2 - d1,
        // 周一为周边界（epoch 天 4 = 1970-01-05 首个周一）
        "week" => {
            (i64::from(d2) + 3).div_euclid(7) as i32 - (i64::from(d1) + 3).div_euclid(7) as i32
        }
        "month" => (y2 - y1) * 12 + (m2 as i32 - m1 as i32),
        "quarter" => (y2 - y1) * 4 + (((m2 - 1) / 3) as i32 - ((m1 - 1) / 3) as i32),
        "year" => y2 - y1,
        _ => return Err(bad_unit()),
    })
}

/// TIMESTAMP/TIME 纳秒差（时间单位按边界；日期单位先归约到天）。
fn timestamp_diff_ns(unit: &str, ns1: i64, ns2: i64) -> Result<i64> {
    Ok(match unit {
        "hour" => ns2.div_euclid(NANOS_PER_HOUR) - ns1.div_euclid(NANOS_PER_HOUR),
        "minute" => ns2.div_euclid(NANOS_PER_MIN) - ns1.div_euclid(NANOS_PER_MIN),
        "second" => ns2.div_euclid(NANOS_PER_SEC) - ns1.div_euclid(NANOS_PER_SEC),
        _ => {
            let (d1, d2) = (
                ns1.div_euclid(NANOS_PER_DAY) as i32,
                ns2.div_euclid(NANOS_PER_DAY) as i32,
            );
            date_diff_days(unit, d1, d2).map(i64::from)?
        }
    })
}

/// 数组 → 逐行 f64（Int64/Float64/Decimal128；NULL → None）。
fn f64_values(a: &ArrayRef) -> Result<Vec<Option<f64>>> {
    use arrow::datatypes::DataType;
    match a.data_type() {
        DataType::Int64 => {
            let arr = a
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| QsdbError::type_err("math: arg is not bigint"))?;
            Ok(arr.iter().map(|v| v.map(|x| x as f64)).collect())
        }
        DataType::Float64 => {
            let arr = a
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| QsdbError::type_err("math: arg is not double"))?;
            Ok(arr.iter().collect())
        }
        DataType::Decimal128(_, s) => {
            let arr = a
                .as_any()
                .downcast_ref::<arrow::array::Decimal128Array>()
                .ok_or_else(|| QsdbError::type_err("math: arg is not decimal"))?;
            let div = 10f64.powi(i32::from(*s));
            Ok(arr.iter().map(|v| v.map(|x| x as f64 / div)).collect())
        }
        other => Err(QsdbError::unsupported(format!(
            "math function on {other:?}"
        ))),
    }
}

/// 按名查找：内置表优先，未命中查 UDF 注册表。
pub fn lookup(name: &str) -> Result<Arc<ScalarFnDef>> {
    if let Some(def) = registry().get(name) {
        return Ok(Arc::clone(def));
    }
    super::udf::lookup(name).ok_or_else(|| {
        QsdbError::not_found(format!(
            "unknown function '{name}' (支持: {:?})",
            known_functions()
        ))
    })
}

/// 是否为内置函数名（UDF 注册防冲突用）。
pub fn is_builtin(name: &str) -> bool {
    registry().contains_key(name)
}

/// 已注册函数名（错误提示用）。
pub fn known_functions() -> Vec<&'static str> {
    let mut v: Vec<&'static str> = registry().keys().copied().collect();
    v.sort();
    v
}

fn string_arg<'a>(
    args: &'a [ArrayRef],
    i: usize,
    fname: &str,
) -> Result<&'a GenericStringArray<i32>> {
    args.get(i)
        .and_then(|a| a.as_any().downcast_ref::<GenericStringArray<i32>>())
        .ok_or_else(|| QsdbError::type_err(format!("{fname}: arg[{i}] is not utf8")))
}

fn int_arg<'a>(args: &'a [ArrayRef], i: usize, fname: &str) -> Result<&'a Int64Array> {
    args.get(i)
        .and_then(|a| a.as_any().downcast_ref::<Int64Array>())
        .ok_or_else(|| QsdbError::type_err(format!("{fname}: arg[{i}] is not bigint")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;

    fn utf8(v: Vec<&str>) -> ArrayRef {
        Arc::new(StringArray::from(v))
    }

    #[test]
    fn string_functions() {
        let inp = utf8(vec!["Hello", "  QS  "]);
        let out = (lookup("upper").unwrap().eval)(&[Arc::clone(&inp)]).unwrap();
        let a = out
            .as_any()
            .downcast_ref::<GenericStringArray<i32>>()
            .unwrap();
        assert_eq!(a.value(0), "HELLO");

        let out = (lookup("trim").unwrap().eval)(&[Arc::clone(&inp)]).unwrap();
        let a = out
            .as_any()
            .downcast_ref::<GenericStringArray<i32>>()
            .unwrap();
        assert_eq!(a.value(1), "QS");

        let out = (lookup("length").unwrap().eval)(&[Arc::clone(&inp)]).unwrap();
        let a = out.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(a.value(0), 5);
    }

    #[test]
    fn substr_semantics() {
        let inp = utf8(vec!["hello world"]);
        let start = Arc::new(Int64Array::from(vec![7i64]));
        let out = (lookup("substr").unwrap().eval)(&[inp, start]).unwrap();
        let a = out
            .as_any()
            .downcast_ref::<GenericStringArray<i32>>()
            .unwrap();
        assert_eq!(a.value(0), "world");
        let start = Arc::new(Int64Array::from(vec![1i64]));
        let len = Arc::new(Int64Array::from(vec![5i64]));
        let out =
            (lookup("substr").unwrap().eval)(&[utf8(vec!["hello world"]), start, len]).unwrap();
        let a = out
            .as_any()
            .downcast_ref::<GenericStringArray<i32>>()
            .unwrap();
        assert_eq!(a.value(0), "hello");
        let start = Arc::new(Int64Array::from(vec![99i64]));
        let out = (lookup("substr").unwrap().eval)(&[utf8(vec!["hi"]), start]).unwrap();
        let a = out
            .as_any()
            .downcast_ref::<GenericStringArray<i32>>()
            .unwrap();
        assert_eq!(a.value(0), "");
    }

    #[test]
    fn numeric_functions() {
        let v: ArrayRef = Arc::new(Float64Array::from(vec![1.5, -1.5, 2.4]));
        let out = (lookup("round").unwrap().eval)(&[Arc::clone(&v)]).unwrap();
        let a = out.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(a.value(0), 2.0); // HALF_UP 远离零
        assert_eq!(a.value(1), -2.0);
        assert_eq!(a.value(2), 2.0);

        let out = (lookup("floor").unwrap().eval)(&[Arc::clone(&v)]).unwrap();
        let a = out.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(a.value(0), 1.0);

        let i: ArrayRef = Arc::new(Int64Array::from(vec![-5i64]));
        let out = (lookup("abs").unwrap().eval)(&[i]).unwrap();
        let a = out.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(a.value(0), 5);
    }
}
