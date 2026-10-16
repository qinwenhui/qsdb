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
                    // ⚠️ **ASCII 快路**：`to_uppercase()` / `to_lowercase()` /
                    // `trim().to_string()` **每个都是一次堆分配**。实测（`.14`，
                    // 500 万行）`upper` 比同列的 `count(gs)` 贵 **+20ns/行**、
                    // `lower` **+14ns/行**。
                    //
                    // 在 **ASCII 域**上这三个与标准库**逐字等价**（判据见
                    // `ascii_trim_bounds`；ASCII 的大小写映射全是 1:1，没有
                    // "一个字符展开成多个"的情形）⇒ 可以在一段**复用缓冲**上
                    // 就地做，一次分配都不要。非 ASCII 才回退标准库那条。
                    let mut out = arrow::array::StringBuilder::new();
                    let mut buf: Vec<u8> = Vec::new();
                    for i in 0..s.len() {
                        if s.is_null(i) {
                            out.append_null();
                            continue;
                        }
                        let x = s.value(i);
                        if x.is_ascii() {
                            buf.clear();
                            buf.extend_from_slice(x.as_bytes());
                            let r: &str = match mode {
                                "upper" => {
                                    buf.make_ascii_uppercase();
                                    std::str::from_utf8(&buf).unwrap_or_default()
                                }
                                "lower" => {
                                    buf.make_ascii_lowercase();
                                    std::str::from_utf8(&buf).unwrap_or_default()
                                }
                                _ => {
                                    let (a, b) = ascii_trim_bounds(&buf);
                                    std::str::from_utf8(&buf[a..b]).unwrap_or_default()
                                }
                            };
                            out.append_value(r);
                        } else {
                            let owned = match mode {
                                "upper" => x.to_uppercase(),
                                "lower" => x.to_lowercase(),
                                _ => x.trim().to_string(),
                            };
                            out.append_value(&owned);
                        }
                    }
                    Ok(Arc::new(out.finish()))
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
                // ⚠️ **一行一次分配都不要**。原实现每行两发：`Vec<char>`（为了按
                // **字符**数取下标）+ `collect::<String>()`（为了喂 `append_value`）。
                // 实测（`.14`，500 万行）`substr` 比同列的 `count(gs)` 贵 **+43ns/行**，
                // 而剖针里 **≈56% 在分配器**（`_int_malloc`/`realloc`/`_free` +
                // `Vec::from_iter<Chars>`）。改成**算字节区间、直接切片**之后
                // 两发分配全消失 —— 切片本身就是合法 UTF-8（边界按整字符推进）。
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
                    let start0 = (start.value(i) - 1).max(0) as usize;
                    let lv = len_arr.map(|l| l.value(i));
                    let (b0, b1) = substr_byte_range(sv, start0, lv);
                    out.append_value(&sv[b0..b1]);
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

    // ── `uuid()`（别名 `sys_guid()`）───────────────────────────────
    // 这里只登记**类型**（binder 要查得到才能过）；真正的求值在 `expr::eval` 里
    // **特判**——因为要**逐行**给不同的值，而注册表的约定是"1 个元素 ⇒ 常量广播"。
    // 详见 `uuid_strings` 的说明。这条 `eval` 不会在主路径上被调用。
    for name in ["uuid", "sys_guid"] {
        let fname: &'static str = name;
        m.insert(
            name,
            Arc::new(ScalarFnDef {
                ret_ty: Arc::new(move |args| match args {
                    [] => Ok(LogicalType::Varchar { max_chars: None }),
                    _ => Err(QsdbError::type_err(format!("{fname}() takes no arguments"))),
                }),
                eval: Arc::new(|_args| Ok(uuid_strings(1))),
            }),
        );
    }

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

/// **逐行**生成 UUID v4 字符串（`uuid()`，以及 `sys_guid()` 这个别名）。
///
/// ## 为什么它不按普通标量函数注册
///
/// 注册表的约定是「**返回 1 个元素 ⇒ 当常量广播**」（见 `expr::eval` 的
/// `PhysExpr::Fn`）。而 `uuid()` 必须**每行不同** —— 它在这批日清结算 SQL 里是拿去
/// 当**主键**的（`CAST(SYS_GUID() AS VARCHAR(64)) AS PHID`）：整批同一个 UUID
/// 等于主键全撞，**数据坏了还不报错**。所以求值处对它**特判**，走这一支。
///
/// ## 语义（对齐 Spark，实测 `.14` 上的 Spark 3.5.2）
///
/// - Spark 的 `uuid()` 返回**规范 36 字符**（`8-4-4-4-12`）的 UUID v4，**非确定性**；
/// - ⚠️ **Spark 里没有 `sys_guid()`**（实测 `[UNRESOLVED_ROUTINE] Cannot resolve
///   function 'sys_guid'`）—— 它是 Oracle 的函数名。这里**收作别名**，
///   为的是让那批既有 SQL 能原样跑；真正的 Spark 名字是 `uuid()`。
///
/// ## 零新依赖
///
/// 本仓不许加依赖（无 `uuid` crate）⇒ 128 位由 `(纳秒时间, 进程内计数器)` 两轮
/// splitmix64 混合得到。**够"不重复"**（计数器保证同进程内单调），不是密码学随机 ——
/// 与 Spark 自己"每分区一个 RandomUUIDGenerator"的量级相当。
pub(crate) fn uuid_strings(n: usize) -> ArrayRef {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    /// splitmix64 的混合函数（零依赖、雪崩性好）。
    fn mix(mut z: u64) -> u64 {
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    let mut out = arrow::array::StringBuilder::new();
    for _ in 0..n {
        // 时间源用**纳秒全分辨率**（一次系统调用拿全 128 位再折成 64）
        let t = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| {
                let n = d.as_nanos();
                (n as u64) ^ ((n >> 64) as u64)
            })
            .unwrap_or(0);
        let hi = mix(t ^ SEQ
            .fetch_add(1, Ordering::Relaxed)
            .wrapping_mul(0x9E37_79B9_7F4A_7C15));
        let lo = mix(hi ^ 0x94D0_49BB_1331_11EB);
        // 36 字节固定布局，直接写、不经过 `format!`（每行一次堆分配不值得）
        let mut buf = [0u8; 36];
        write_uuid(buf.as_mut(), hi, lo);
        out.append_value(std::str::from_utf8(&buf).unwrap_or_default());
    }
    Arc::new(out.finish())
}

/// 把两个 u64 写成 `8-4-4-4-12` 的 UUID 文本（版本位 = 4、variant 位 = 10xx）。
fn write_uuid(buf: &mut [u8], hi: u64, lo: u64) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut nib = [0u8; 32];
    for i in 0..16 {
        nib[i] = ((hi >> (60 - 4 * i)) & 0xf) as u8;
        nib[16 + i] = ((lo >> (60 - 4 * i)) & 0xf) as u8;
    }
    nib[12] = 4; // 版本 4
    nib[16] = (nib[16] & 0x3) | 0x8; // RFC 4122 variant
    let mut j = 0usize;
    for (i, v) in nib.iter().enumerate() {
        if i == 8 || i == 12 || i == 16 || i == 20 {
            buf[j] = b'-';
            j += 1;
        }
        buf[j] = HEX[*v as usize];
        j += 1;
    }
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

/// `trim` 在 **ASCII 域**上的有效区间（去掉两端的空白）。
///
/// **为什么可以用它替掉标准库的 `str::trim()`**：`char::is_whitespace` 在 ASCII
/// 上认的正好是 `u8::is_ascii_whitespace` 那一组（U+0009–U+000D + U+0020），
/// 于是对**纯 ASCII** 输入两者逐字等价；非 ASCII 的输入走标准库那条（见调用点）。
fn ascii_trim_bounds(b: &[u8]) -> (usize, usize) {
    let mut a = 0usize;
    while a < b.len() && b[a].is_ascii_whitespace() {
        a += 1;
    }
    let mut e = b.len();
    while e > a && b[e - 1].is_ascii_whitespace() {
        e -= 1;
    }
    (a, e)
}

/// `substr(s, start0, len)` 结果的**字节区间**（`start0` 已转 0-based，按**字符**数计）。
///
/// **为什么算字节而不是收 `Vec<char>`**：语义确实是"按字符数取"，但**不需要**
/// 把整串摊成 `Vec<char>` 才能做到 —— 字符边界只要扫一遍前导字节就能定出来。
/// 原实现每行为此付两发堆分配（见 `substr` 的注释）。
///
/// - **ASCII 快路**：字符下标 == 字节下标，直接算，**一次扫描都不用**（`is_ascii`
///   是 SIMD 友好的）。
/// - 非 ASCII：按 UTF-8 前导字节推进。区间两端都落在**字符边界**上 ⇒
///   调用方拿到的切片一定是合法 UTF-8。
fn substr_byte_range(s: &str, start0: usize, len: Option<i64>) -> (usize, usize) {
    let b = s.as_bytes();
    let take = |avail: usize| -> usize {
        match len {
            None => avail,
            Some(l) if l <= 0 => 0,
            Some(l) => (l as usize).min(avail),
        }
    };
    if s.is_ascii() {
        if start0 >= b.len() {
            return (b.len(), b.len());
        }
        return (start0, start0 + take(b.len() - start0));
    }
    let mut idx = 0usize; // 字节位置
    let mut c = 0usize; // 已越过的字符数
    while c < start0 && idx < b.len() {
        idx += utf8_seq_len(b[idx]);
        c += 1;
    }
    if idx >= b.len() {
        return (b.len(), b.len());
    }
    let start_b = idx;
    let n = take(usize::MAX);
    let mut end_b = idx;
    let mut k = 0usize;
    while k < n && end_b < b.len() {
        end_b += utf8_seq_len(b[end_b]);
        k += 1;
    }
    (start_b, end_b)
}

/// 一个 UTF-8 序列从 `lead` 开始的字节数。非法/续字节按 1 走（不 panic、不越界）。
fn utf8_seq_len(lead: u8) -> usize {
    match lead {
        0x00..=0x7F => 1,
        0xC0..=0xDF => 2,
        0xE0..=0xEF => 3,
        0xF0..=0xF7 => 4,
        _ => 1,
    }
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

    /// ★★ `substr` 现在**算字节区间**再切片，必须与**朴素实现**（摊成 `Vec<char>`
    /// 再按字符切片 —— 也就是原实现的语义）**逐字相等**。
    ///
    /// ⚠️ **只测 ASCII 是测不到它的**：`is_ascii` 快路在那个域里把字符下标和字节
    /// 下标**当同一件事**，字符边界那一支压根不执行。所以这里的用例**绝大多数是
    /// 多字节**（`é` 是 2 字节、`世` 是 3、`🙂` 是 4），并且逐例断言两端都落在
    /// `is_char_boundary` 上 —— 越界/切半个字符都会当场炸。
    #[test]
    fn substr_byte_range_matches_naive_impl() {
        let cases = [
            "hello world",
            "",
            "a",
            "héllo",
            "世界和平",
            "aé世界b",
            "🙂🙂🙂",
            "naïve café",
            "日本語テキスト",
        ];
        let mut n = 0;
        for s in cases {
            for start in [-3i64, 0, 1, 2, 3, 5, 99] {
                for len in [
                    None,
                    Some(-1i64),
                    Some(0),
                    Some(1),
                    Some(2),
                    Some(4),
                    Some(99),
                ] {
                    // 期望值 = **朴素实现**（原 `substr` 的做法）现算
                    let chars: Vec<char> = s.chars().collect();
                    let start0 = (start - 1).max(0) as usize;
                    let want: String = if start0 >= chars.len() {
                        String::new()
                    } else {
                        let take = match len {
                            None => chars.len() - start0,
                            Some(l) if l <= 0 => 0,
                            Some(l) => (l as usize).min(chars.len() - start0),
                        };
                        chars[start0..start0 + take].iter().collect()
                    };
                    let (b0, b1) = substr_byte_range(s, start0, len);
                    assert_eq!(&s[b0..b1], want, "s={s:?} start={start} len={len:?}");
                    assert!(
                        s.is_char_boundary(b0) && s.is_char_boundary(b1),
                        "切在了半个字符上：s={s:?} start={start} len={len:?} → {b0}..{b1}"
                    );
                    n += 1;
                }
            }
        }
        assert_eq!(n, 9 * 7 * 7, "用例数不对说明有循环被改没了");
    }

    /// ★ `upper`/`lower`/`trim` 的 **ASCII 快路**（就地折返 + 复用缓冲）必须与
    /// **标准库**（也就是非 ASCII 那条回退路）**逐字相等**。
    ///
    /// 用例里**必须混非 ASCII**：纯 ASCII 的输入压根走不到回退那条，
    /// 两边就没有对照物了（"快路对不对"是在跟"标准库同不同"比）。
    #[test]
    fn case_and_trim_fast_path_matches_stdlib() {
        let cases = [
            "",
            "a",
            "Hello World",
            "  QS  ",
            "\t\nMiXeD\r ",
            "abc123",
            "   ",
            "ÉÀÜ",
            "世 界",
            "ß",
        ];
        let inp = utf8(cases.to_vec());
        for name in ["upper", "lower", "trim"] {
            let out = (lookup(name).unwrap().eval)(&[Arc::clone(&inp)]).unwrap();
            let a = out
                .as_any()
                .downcast_ref::<GenericStringArray<i32>>()
                .unwrap();
            for (i, c) in cases.iter().enumerate() {
                let want = match name {
                    "upper" => c.to_uppercase(),
                    "lower" => c.to_lowercase(),
                    _ => c.trim().to_string(),
                };
                assert_eq!(a.value(i), want, "{name}({c:?})");
            }
        }
    }

    /// 数组级：三参形态的 NULL 传播 + **按字符而不是按字节**取下标。
    ///
    /// ⚠️ 断 NULL 必须用 `is_null`，**不能用 `value() == ""`** —— 空槽上
    /// `value()` 读回 `""`，两者会被混成一件（我第一次就把期望写错了）。
    #[test]
    fn substr_array_null_and_multibyte() {
        let inp = utf8(vec!["hello", "世界和平", "aé", "", "x"]);
        let start = Arc::new(Int64Array::from(vec![
            Some(1i64),
            Some(2),
            None,
            Some(1),
            Some(1),
        ]));
        let len = Arc::new(Int64Array::from(vec![
            Some(3i64),
            Some(2),
            Some(1),
            None,
            Some(0),
        ]));
        let out = (lookup("substr").unwrap().eval)(&[inp, start, len]).unwrap();
        let a = out
            .as_any()
            .downcast_ref::<GenericStringArray<i32>>()
            .unwrap();
        assert_eq!(a.value(0), "hel");
        assert_eq!(
            a.value(1),
            "界和",
            "从第 2 个**字符**起（不是第 2 个字节 —— 第 2 字节是「界」的中间）"
        );
        assert!(a.is_null(2), "起点 NULL ⇒ 结果 NULL");
        assert!(a.is_null(3), "长度 NULL ⇒ 结果 NULL");
        assert_eq!(a.value(4), "", "长度 0 ⇒ 空串（不是 NULL）");

        // 两参形态（没有长度参数 ⇒ 取到末尾）：非 ASCII 同样按字符数
        let start = Arc::new(Int64Array::from(vec![3i64]));
        let out = (lookup("substr").unwrap().eval)(&[utf8(vec!["世界和平"]), start]).unwrap();
        let a = out
            .as_any()
            .downcast_ref::<GenericStringArray<i32>>()
            .unwrap();
        assert_eq!(a.value(0), "和平");
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
