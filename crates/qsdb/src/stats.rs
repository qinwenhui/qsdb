//! 列统计收集：arrow 数组 → [`ColumnStats`]（min/max 不含 NULL 与 NaN）。

use std::cmp::Ordering;

use arrow::array::{
    Array, ArrowPrimitiveType, BooleanArray, Decimal128Array, GenericBinaryArray,
    GenericStringArray, PrimitiveArray,
};
use arrow::datatypes::{
    DataType, Date32Type, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type,
    Time64NanosecondType, TimestampNanosecondType,
};

use crate::error::Result;
use crate::manifest::ColumnStats;
use crate::types::{DecimalMeta, Literal};

/// arrow 原生值 → Literal 的类型级映射（避免 i32 被 Date32/Int32 二义）。
trait NativeLit: ArrowPrimitiveType {
    fn to_lit(v: Self::Native) -> Literal;
    fn is_nan(_v: &Self::Native) -> bool {
        false
    }
}

impl NativeLit for Int8Type {
    fn to_lit(v: i8) -> Literal {
        Literal::Int8(v)
    }
}
impl NativeLit for Int16Type {
    fn to_lit(v: i16) -> Literal {
        Literal::Int16(v)
    }
}
impl NativeLit for Int32Type {
    fn to_lit(v: i32) -> Literal {
        Literal::Int32(v)
    }
}
impl NativeLit for Int64Type {
    fn to_lit(v: i64) -> Literal {
        Literal::Int64(v)
    }
}
impl NativeLit for Float32Type {
    fn to_lit(v: f32) -> Literal {
        Literal::Float32(v)
    }
    fn is_nan(v: &f32) -> bool {
        v.is_nan()
    }
}
impl NativeLit for Float64Type {
    fn to_lit(v: f64) -> Literal {
        Literal::Float64(v)
    }
    fn is_nan(v: &f64) -> bool {
        v.is_nan()
    }
}
impl NativeLit for Date32Type {
    fn to_lit(v: i32) -> Literal {
        Literal::Date(v)
    }
}
impl NativeLit for Time64NanosecondType {
    fn to_lit(v: i64) -> Literal {
        Literal::Time(v)
    }
}
impl NativeLit for TimestampNanosecondType {
    fn to_lit(v: i64) -> Literal {
        Literal::Timestamp(v)
    }
}

// Decimal128 单独处理（需要 DecimalMeta），见 decimal_stats

/// 计算单列统计。不支持的类型（Interval 等）返回 null_count-only 统计。
pub fn column_stats(array: &dyn Array) -> Result<ColumnStats> {
    let mut stats = ColumnStats {
        min: None,
        max: None,
        null_count: array.null_count() as u64,
    };
    if array.len() == array.null_count() {
        return Ok(stats); // 全 NULL（或空数组）
    }
    match array.data_type() {
        DataType::Int8 => primitive_stats::<Int8Type>(array, &mut stats),
        DataType::Int16 => primitive_stats::<Int16Type>(array, &mut stats),
        DataType::Int32 => primitive_stats::<Int32Type>(array, &mut stats),
        DataType::Int64 => primitive_stats::<Int64Type>(array, &mut stats),
        DataType::Float32 => primitive_stats::<Float32Type>(array, &mut stats),
        DataType::Float64 => primitive_stats::<Float64Type>(array, &mut stats),
        DataType::Date32 => primitive_stats::<Date32Type>(array, &mut stats),
        DataType::Time64(arrow::datatypes::TimeUnit::Nanosecond) => {
            primitive_stats::<Time64NanosecondType>(array, &mut stats)
        }
        DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, _) => {
            primitive_stats::<TimestampNanosecondType>(array, &mut stats)
        }
        DataType::Decimal128(p, s) => {
            decimal_stats(array, &mut stats, DecimalMeta::try_new(*p, *s)?)
        }
        DataType::Boolean => bool_stats(array, &mut stats),
        DataType::Utf8 => string_stats::<i32>(array, &mut stats),
        DataType::LargeUtf8 => string_stats::<i64>(array, &mut stats),
        DataType::Binary => binary_stats::<i32>(array, &mut stats),
        DataType::LargeBinary => binary_stats::<i64>(array, &mut stats),
        _ => {} // Interval 等：仅 null_count
    }
    Ok(stats)
}

/// 用 Literal 偏序更新 min/max。
fn lit_update(cur: &mut Option<Literal>, v: Literal, keep_min: bool) {
    match cur {
        None => *cur = Some(v),
        Some(old) => {
            if let Ok(Some(o)) = v.partial_cmp_literal(old) {
                let better = if keep_min {
                    o == Ordering::Less
                } else {
                    o == Ordering::Greater
                };
                if better {
                    *cur = Some(v);
                }
            }
        }
    }
}

fn primitive_stats<T>(array: &dyn Array, stats: &mut ColumnStats)
where
    T: NativeLit,
{
    let arr = array.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
    for i in 0..arr.len() {
        if arr.is_null(i) || T::is_nan(&arr.value(i)) {
            continue;
        }
        let v = T::to_lit(arr.value(i));
        lit_update(&mut stats.min, v.clone(), true);
        lit_update(&mut stats.max, v, false);
    }
}

fn decimal_stats(array: &dyn Array, stats: &mut ColumnStats, meta: DecimalMeta) {
    let arr = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
    for i in 0..arr.len() {
        if arr.is_null(i) {
            continue;
        }
        let v = Literal::Decimal {
            unscaled: arr.value(i),
            meta,
        };
        lit_update(&mut stats.min, v.clone(), true);
        lit_update(&mut stats.max, v, false);
    }
}

fn bool_stats(array: &dyn Array, stats: &mut ColumnStats) {
    let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
    let mut has_true = false;
    let mut has_false = false;
    for i in 0..arr.len() {
        if arr.is_null(i) {
            continue;
        }
        if arr.value(i) {
            has_true = true;
        } else {
            has_false = true;
        }
    }
    if has_false {
        stats.min = Some(Literal::Boolean(false));
    }
    if has_true {
        stats.max = Some(Literal::Boolean(true));
    }
}

fn string_stats<O: arrow::array::OffsetSizeTrait>(array: &dyn Array, stats: &mut ColumnStats) {
    let arr = array
        .as_any()
        .downcast_ref::<GenericStringArray<O>>()
        .unwrap();
    for i in 0..arr.len() {
        if arr.is_null(i) {
            continue;
        }
        let v = Literal::Utf8(arr.value(i).to_string());
        lit_update(&mut stats.min, v.clone(), true);
        lit_update(&mut stats.max, v, false);
    }
}

fn binary_stats<O: arrow::array::OffsetSizeTrait>(array: &dyn Array, stats: &mut ColumnStats) {
    let arr = array
        .as_any()
        .downcast_ref::<GenericBinaryArray<O>>()
        .unwrap();
    for i in 0..arr.len() {
        if arr.is_null(i) {
            continue;
        }
        let v = Literal::Binary(arr.value(i).to_vec());
        lit_update(&mut stats.min, v.clone(), true);
        lit_update(&mut stats.max, v, false);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        BooleanArray, Date32Array, Decimal128Array, Float64Array, Int32Array, Int64Array,
        StringArray, TimestampNanosecondArray,
    };

    #[test]
    fn int_min_max_nulls() {
        let arr = Int32Array::from(vec![Some(5), None, Some(-3), Some(9)]);
        let st = column_stats(&arr).unwrap();
        assert_eq!(st.min, Some(Literal::Int32(-3)));
        assert_eq!(st.max, Some(Literal::Int32(9)));
        assert_eq!(st.null_count, 1);
    }

    #[test]
    fn all_null_or_empty() {
        let arr = Int64Array::from(vec![None::<i64>, None]);
        let st = column_stats(&arr).unwrap();
        assert_eq!(st.min, None);
        assert_eq!(st.max, None);
        assert_eq!(st.null_count, 2);
        let empty = Int64Array::from(Vec::<Option<i64>>::new());
        let st = column_stats(&empty).unwrap();
        assert_eq!(st.min, None);
        assert_eq!(st.null_count, 0);
    }

    #[test]
    fn float_nan_skipped() {
        let arr = Float64Array::from(vec![Some(1.5), Some(f64::NAN), Some(-2.0)]);
        let st = column_stats(&arr).unwrap();
        assert_eq!(st.min, Some(Literal::Float64(-2.0)));
        assert_eq!(st.max, Some(Literal::Float64(1.5)));
        let all_nan = Float64Array::from(vec![Some(f64::NAN)]);
        let st = column_stats(&all_nan).unwrap();
        assert_eq!(st.min, None);
    }

    #[test]
    fn string_bool_decimal_date_ts() {
        let arr = StringArray::from(vec![Some("b"), Some("a"), None, Some("c")]);
        let st = column_stats(&arr).unwrap();
        assert_eq!(st.min, Some(Literal::Utf8("a".into())));
        assert_eq!(st.max, Some(Literal::Utf8("c".into())));

        let arr = BooleanArray::from(vec![Some(true), Some(false), Some(true)]);
        let st = column_stats(&arr).unwrap();
        assert_eq!(st.min, Some(Literal::Boolean(false)));
        assert_eq!(st.max, Some(Literal::Boolean(true)));

        let arr = Decimal128Array::from(vec![Some(150), Some(-25), None])
            .with_data_type(DataType::Decimal128(10, 2));
        let st = column_stats(&arr).unwrap();
        assert_eq!(
            st.min,
            Some(Literal::Decimal {
                unscaled: -25,
                meta: DecimalMeta::new(10, 2)
            })
        );
        assert_eq!(
            st.max,
            Some(Literal::Decimal {
                unscaled: 150,
                meta: DecimalMeta::new(10, 2)
            })
        );

        let arr = Date32Array::from(vec![Some(1), Some(3)]);
        let st = column_stats(&arr).unwrap();
        assert_eq!(st.min, Some(Literal::Date(1)));

        let arr = TimestampNanosecondArray::from(vec![Some(5), Some(2)]);
        let st = column_stats(&arr).unwrap();
        assert_eq!(st.min, Some(Literal::Timestamp(2)));
        assert_eq!(st.max, Some(Literal::Timestamp(5)));
    }

    #[test]
    fn interval_stats_null_only() {
        use arrow::array::IntervalMonthDayNanoArray;
        let arr =
            IntervalMonthDayNanoArray::from(vec![arrow::datatypes::IntervalMonthDayNano::new(
                1, 2, 3,
            )]);
        let st = column_stats(&arr).unwrap();
        assert_eq!(st.min, None);
        assert_eq!(st.null_count, 0);
    }
}
