//! 标量函数注册表（SQL-1 集）。
//!
//! length / upper / lower / trim / substr / abs / round / floor / ceil。
//! 全部行级函数：NULL 输入 → NULL 输出。

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float64Array, GenericStringArray, Int64Array};
use arrow::datatypes::{Float64Type, Int64Type};

use crate::error::{QsdbError, Result};
use crate::types::LogicalType;

type RetTyFn = Box<dyn Fn(&[LogicalType]) -> Result<LogicalType> + Send + Sync>;
type EvalFn = Box<dyn Fn(&[ArrayRef]) -> Result<ArrayRef> + Send + Sync>;

/// 标量函数定义。
pub struct ScalarFnDef {
    /// 入参类型 → 返回类型（binder 类型检查）
    pub ret_ty: RetTyFn,
    /// 行级求值
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

fn registry() -> &'static HashMap<&'static str, ScalarFnDef> {
    use std::sync::OnceLock;
    static REG: OnceLock<HashMap<&'static str, ScalarFnDef>> = OnceLock::new();
    REG.get_or_init(build)
}

fn build() -> HashMap<&'static str, ScalarFnDef> {
    let mut m: HashMap<&'static str, ScalarFnDef> = HashMap::new();

    // length(string) -> bigint
    m.insert(
        "length",
        ScalarFnDef {
            ret_ty: Box::new(|args| match args {
                [t] if t.is_string() => Ok(LogicalType::Int64),
                _ => Err(QsdbError::type_err("length(string) expected")),
            }),
            eval: Box::new(|args| {
                let s = string_arg(args, 0, "length")?;
                let out: Int64Array = s
                    .iter()
                    .map(|v| v.map(|x| x.chars().count() as i64))
                    .collect();
                Ok(Arc::new(out))
            }),
        },
    );

    // upper / lower / trim(string) -> string
    for name in ["upper", "lower", "trim"] {
        let mode: &'static str = name;
        m.insert(
            name,
            ScalarFnDef {
                ret_ty: Box::new(move |args| match args {
                    [t] if t.is_string() => Ok(t.clone()),
                    _ => Err(QsdbError::type_err(format!("{mode}(string) expected"))),
                }),
                eval: Box::new(move |args| {
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
            },
        );
    }

    // substr(string, start[, len]) -> string（1-based）
    m.insert(
        "substr",
        ScalarFnDef {
            ret_ty: Box::new(|args| match args {
                [t, LogicalType::Int64] | [t, LogicalType::Int64, LogicalType::Int64]
                    if t.is_string() =>
                {
                    Ok(t.clone())
                }
                _ => Err(QsdbError::type_err("substr(string, start[, len]) expected")),
            }),
            eval: Box::new(|args| {
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
        },
    );

    // abs(numeric) -> numeric（当前支持 BIGINT/DOUBLE）
    m.insert(
        "abs",
        ScalarFnDef {
            ret_ty: Box::new(|args| match args {
                [t] if t.is_numeric() => Ok(t.clone()),
                _ => Err(QsdbError::type_err("abs(numeric) expected")),
            }),
            eval: Box::new(|args| match args[0].data_type() {
                arrow::datatypes::DataType::Int64 => {
                    primitive_map::<Int64Type, _>(args, |v: i64| v.wrapping_abs())
                }
                arrow::datatypes::DataType::Float64 => {
                    primitive_map::<Float64Type, _>(args, f64::abs)
                }
                other => Err(QsdbError::unsupported(format!(
                    "abs on {other:?} (支持 BIGINT/DOUBLE)"
                ))),
            }),
        },
    );

    // round / floor / ceil(double[, digits]) -> double
    for (name, mode) in [("round", 0u8), ("floor", 1u8), ("ceil", 2u8)] {
        let nm: &'static str = name;
        m.insert(
            name,
            ScalarFnDef {
                ret_ty: Box::new(move |args| match args {
                    [LogicalType::Float64] | [LogicalType::Float64, LogicalType::Int64] => {
                        Ok(LogicalType::Float64)
                    }
                    _ => Err(QsdbError::type_err(format!(
                        "{nm}(double[, digits]) expected"
                    ))),
                }),
                eval: Box::new(move |args| {
                    let v = args[0]
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .ok_or_else(|| QsdbError::type_err(format!("{nm}: bad arg type")))?;
                    let digits: i64 = if args.len() > 1 {
                        int_arg(args, 1, nm)?.value(0)
                    } else {
                        0
                    };
                    let f = |x: f64| match mode {
                        0 => {
                            let m = 10f64.powi(digits as i32);
                            (x * m + if x >= 0.0 { 0.5 } else { -0.5 }).trunc() / m
                        }
                        1 => x.floor(),
                        _ => x.ceil(),
                    };
                    let out: Float64Array = v.iter().map(|x| x.map(f)).collect();
                    Ok(Arc::new(out))
                }),
            },
        );
    }

    m
}

/// 按名查找。
pub fn lookup(name: &str) -> Result<&'static ScalarFnDef> {
    registry()
        .get(name)
        .ok_or_else(|| QsdbError::not_found(format!("unknown function '{name}'")))
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
