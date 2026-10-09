//! 向量化求值：叶子（列/常量）→ 算子（arrow kernels + Datum 广播）。
//!
//! DECIMAL 算术为 i256 自实现精确路径（arrow 数值 kernel 不覆盖 decimal）。

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Decimal128Array, Int8Array, Int16Array, Int32Array, Int64Array,
};
use arrow::compute::kernels::cmp;
use arrow::compute::kernels::numeric;
use arrow::compute::{
    CastOptions, and_kleene, cast_with_options, is_not_null, is_null, not, or_kleene,
};
use arrow::datatypes::DataType;
use arrow::datatypes::i256 as I256;
use arrow::record_batch::RecordBatch;

use super::{ArithOp, CmpOp, PhysExpr};
use crate::error::{QsdbError, Result};
use crate::types::{Literal, LogicalType, decimal};

/// 叶子求值结果：常量（1 行）或满长数组。
#[derive(Debug, Clone)]
enum EvalVal {
    Arr(ArrayRef),
    Const(ArrayRef),
}

impl EvalVal {
    fn len_hint(&self) -> usize {
        match self {
            Self::Arr(a) => a.len(),
            Self::Const(a) => a.len().max(1),
        }
    }
    /// 物化为满长数组（标量函数参数/输出用）。
    fn materialize(&self, n: usize) -> Result<ArrayRef> {
        match self {
            EvalVal::Arr(a) => Ok(Arc::clone(a)),
            EvalVal::Const(one) => {
                let indices: Vec<(usize, usize)> = vec![(0, 0); n];
                arrow::compute::interleave(&[one.as_ref()], &indices)
                    .map_err(|e| QsdbError::invalid(format!("broadcast const failed: {e}")))
            }
        }
    }
}

/// 表达式求值：返回满长或 1 行（常量）数组。
pub fn eval(expr: &PhysExpr, batch: &RecordBatch) -> Result<ArrayRef> {
    match eval_val(expr, batch)? {
        EvalVal::Arr(a) => Ok(a),
        EvalVal::Const(a) => Ok(a),
    }
}

/// 求值为布尔掩码（Filter/Join 条件用；要求表达式为 Boolean 类型）。
pub fn eval_mask(expr: &PhysExpr, batch: &RecordBatch) -> Result<BooleanArray> {
    if !expr.is_boolean() {
        return Err(QsdbError::type_err(format!(
            "expected boolean expression, got {}",
            expr.data_type()
        )));
    }
    let v = eval_val(expr, batch)?;
    let arr = match v {
        EvalVal::Arr(a) => a,
        EvalVal::Const(one) => {
            let indices: Vec<(usize, usize)> = vec![(0, 0); batch.num_rows()];
            arrow::compute::interleave(&[one.as_ref()], &indices)
                .map_err(|e| QsdbError::invalid(format!("broadcast const failed: {e}")))?
        }
    };
    arr.as_any()
        .downcast_ref::<BooleanArray>()
        .cloned()
        .ok_or_else(|| QsdbError::type_err("expression did not evaluate to boolean"))
}

fn eval_val(expr: &PhysExpr, batch: &RecordBatch) -> Result<EvalVal> {
    match expr {
        PhysExpr::Column { idx, .. } => Ok(EvalVal::Arr(Arc::clone(batch.column(*idx)))),
        PhysExpr::Lit(l) => Ok(EvalVal::Const(literal_array(l)?)),
        PhysExpr::Arith { op, l, r, ty } => {
            let lv = eval_val(l, batch)?;
            let rv = eval_val(r, batch)?;
            if matches!(ty, LogicalType::Decimal(_)) {
                decimal_arith(*op, &lv, &rv, ty)
            } else {
                let arr = arith_arrow(*op, &lv, &rv)?;
                // 两标量入 → 单行结果保持 Const 语义（避免上层按非标量定长比较）
                let both_const = matches!(lv, EvalVal::Const(_)) && matches!(rv, EvalVal::Const(_));
                if both_const && arr.len() == 1 {
                    Ok(EvalVal::Const(arr))
                } else {
                    Ok(EvalVal::Arr(arr))
                }
            }
        }
        PhysExpr::Cmp { op, l, r } => {
            let lv = eval_val(l, batch)?;
            let rv = eval_val(r, batch)?;
            Ok(EvalVal::Arr(cmp_arrays(*op, &lv, &rv)?))
        }
        PhysExpr::And(a, b) => {
            let l = eval_mask(a, batch)?;
            let r = eval_mask(b, batch)?;
            Ok(EvalVal::Arr(Arc::new(and_kleene(&l, &r).map_err(|e| {
                QsdbError::type_err(format!("AND failed: {e}"))
            })?)))
        }
        PhysExpr::Or(a, b) => {
            let l = eval_mask(a, batch)?;
            let r = eval_mask(b, batch)?;
            Ok(EvalVal::Arr(Arc::new(or_kleene(&l, &r).map_err(|e| {
                QsdbError::type_err(format!("OR failed: {e}"))
            })?)))
        }
        PhysExpr::Not(a) => {
            let m = eval_mask(a, batch)?;
            Ok(EvalVal::Arr(Arc::new(not(&m).map_err(|e| {
                QsdbError::type_err(format!("NOT failed: {e}"))
            })?)))
        }
        PhysExpr::IsNull(e, negated) => {
            let v = eval_val(e, batch)?;
            let arr = match v {
                EvalVal::Arr(a) => a,
                EvalVal::Const(one) => materialize_const(&one, batch.num_rows())?,
            };
            let mask = if *negated {
                is_not_null(&arr)
            } else {
                is_null(&arr)
            };
            Ok(EvalVal::Arr(Arc::new(mask.map_err(|e| {
                QsdbError::invalid(format!("is_null failed: {e}"))
            })?)))
        }
        PhysExpr::Cast { e, ty } => {
            let v = eval_val(e, batch)?;
            let target = ty.arrow_type();
            match v {
                EvalVal::Const(one) => {
                    // 常量转换后仍是常量（1 行）
                    let casted = cast_with_options(one.as_ref(), &target, &CastOptions::default())
                        .map_err(|err| {
                            QsdbError::type_err(format!(
                                "cast {} to {ty} failed: {err}",
                                e.display()
                            ))
                        })?;
                    Ok(EvalVal::Const(casted))
                }
                EvalVal::Arr(a) => Ok(EvalVal::Arr(
                    cast_with_options(a.as_ref(), &target, &CastOptions::default()).map_err(
                        |err| {
                            QsdbError::type_err(format!(
                                "cast {} to {ty} failed: {err}",
                                e.display()
                            ))
                        },
                    )?,
                )),
            }
        }
        PhysExpr::Fn { name, args, .. } => {
            let def = super::scalar::lookup(name)?;
            let n = batch.num_rows();
            let mut arg_vals: Vec<ArrayRef> = Vec::with_capacity(args.len());
            for a in args {
                arg_vals.push(eval_val(a, batch)?.materialize(n)?);
            }
            let out = (def.eval)(&arg_vals)?;
            if out.len() == n {
                Ok(EvalVal::Arr(out))
            } else {
                Ok(EvalVal::Const(out))
            }
        }
    }
}

fn materialize_const(one: &ArrayRef, n: usize) -> Result<ArrayRef> {
    let indices: Vec<(usize, usize)> = vec![(0, 0); n];
    arrow::compute::interleave(&[one.as_ref()], &indices)
        .map_err(|e| QsdbError::invalid(format!("broadcast const failed: {e}")))
}

// ---------------------------------------------------------------------------
// Datum 适配（常量广播）
// ---------------------------------------------------------------------------

struct WrapArr<'a>(&'a dyn Array);
struct WrapConst<'a>(&'a dyn Array);

impl arrow::array::Datum for WrapArr<'_> {
    fn get(&self) -> (&dyn Array, bool) {
        (self.0, false)
    }
}
impl arrow::array::Datum for WrapConst<'_> {
    fn get(&self) -> (&dyn Array, bool) {
        (self.0, true)
    }
}

/// 以 Datum 形式把两个操作数交给 kernel（自动标量广播）。
fn with_datum<T, F>(l: &EvalVal, r: &EvalVal, f: F) -> Result<T>
where
    F: FnOnce(
        &dyn arrow::array::Datum,
        &dyn arrow::array::Datum,
    ) -> std::result::Result<T, arrow::error::ArrowError>,
{
    match (l, r) {
        (EvalVal::Arr(a), EvalVal::Arr(b)) => f(&WrapArr(a.as_ref()), &WrapArr(b.as_ref())),
        (EvalVal::Const(a), EvalVal::Arr(b)) => f(&WrapConst(a.as_ref()), &WrapArr(b.as_ref())),
        (EvalVal::Arr(a), EvalVal::Const(b)) => f(&WrapArr(a.as_ref()), &WrapConst(b.as_ref())),
        (EvalVal::Const(a), EvalVal::Const(b)) => f(&WrapConst(a.as_ref()), &WrapConst(b.as_ref())),
    }
    .map_err(|e| QsdbError::type_err(format!("kernel failed: {e}")))
}

// ---------------------------------------------------------------------------
// 算术 / 比较
// ---------------------------------------------------------------------------

fn arith_arrow(op: ArithOp, l: &EvalVal, r: &EvalVal) -> Result<ArrayRef> {
    match op {
        ArithOp::Add => with_datum(l, r, numeric::add),
        ArithOp::Sub => with_datum(l, r, numeric::sub),
        ArithOp::Mul => with_datum(l, r, numeric::mul),
        ArithOp::Div => with_datum(l, r, numeric::div),
        ArithOp::Mod => with_datum(l, r, numeric::rem),
    }
}

fn cmp_arrays(op: CmpOp, l: &EvalVal, r: &EvalVal) -> Result<ArrayRef> {
    let out: BooleanArray = match op {
        CmpOp::Eq => with_datum(l, r, cmp::eq)?,
        CmpOp::NotEq => with_datum(l, r, cmp::neq)?,
        CmpOp::Lt => with_datum(l, r, cmp::lt)?,
        CmpOp::LtEq => with_datum(l, r, cmp::lt_eq)?,
        CmpOp::Gt => with_datum(l, r, cmp::gt)?,
        CmpOp::GtEq => with_datum(l, r, cmp::gt_eq)?,
    };
    Ok(Arc::new(out))
}

// ---------------------------------------------------------------------------
// DECIMAL 精确算术（i256）
// ---------------------------------------------------------------------------

fn decimal_arith(op: ArithOp, l: &EvalVal, r: &EvalVal, ty: &LogicalType) -> Result<EvalVal> {
    let LogicalType::Decimal(result_meta) = ty else {
        unreachable!()
    };
    let n = l.len_hint().max(r.len_hint());
    let la = match l {
        EvalVal::Arr(a) => Arc::clone(a),
        EvalVal::Const(one) => materialize_const(one, n)?,
    };
    let ra = match r {
        EvalVal::Arr(a) => Arc::clone(a),
        EvalVal::Const(one) => materialize_const(one, n)?,
    };
    let ld = la
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .ok_or_else(|| QsdbError::type_err("decimal arith on non-decimal operand"))?;
    let rd = ra
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .ok_or_else(|| QsdbError::type_err("decimal arith on non-decimal operand"))?;
    let (lsa, lpa) = decimal_meta_of(ld.data_type());
    let (lsb, _rpb) = decimal_meta_of(rd.data_type());
    let rs = result_meta.scale;

    let mut out: Vec<Option<i128>> = Vec::with_capacity(n);
    for i in 0..n {
        if ld.is_null(i) || rd.is_null(i) {
            out.push(None);
            continue;
        }
        if b_is_zero(rd, i) && matches!(op, ArithOp::Div | ArithOp::Mod) {
            return Err(QsdbError::type_err("divide by zero"));
        }
        let a = I256::from_i128(ld.value(i));
        let b = I256::from_i128(rd.value(i));
        let v: I256 = match op {
            ArithOp::Add => align_scale(a, lsa, rs)? + align_scale(b, lsb, rs)?,
            ArithOp::Sub => align_scale(a, lsa, rs)? - align_scale(b, lsb, rs)?,
            ArithOp::Mul => {
                // 结果 scale = sa + sb（binder 保证 rs == sa + sb）
                let raw = a * b;
                rescale_i256(raw, i32::from(lsa) + i32::from(lsb), i32::from(rs))?
            }
            ArithOp::Div => {
                // unscaled = HALF_UP(a * 10^(rs + sb - sa) / b)
                let shift = i32::from(rs) + i32::from(lsb) - i32::from(lsa);
                if shift < 0 {
                    return Err(QsdbError::unsupported(
                        "decimal divide with negative scale shift",
                    ));
                }
                let scaled = a
                    .checked_mul(I256::from_i128(decimal::pow10(shift as u32)))
                    .ok_or_else(|| QsdbError::type_err("decimal divide overflow"))?;
                let q = scaled / b; // b != 0 已检查
                q
            }
            ArithOp::Mod => return Err(QsdbError::unsupported("decimal % operator")),
        };
        let _ = lpa;
        let Some(i128v) = v.to_i128() else {
            return Err(QsdbError::type_err(format!(
                "decimal arithmetic overflows {result_meta}"
            )));
        };
        if !result_meta.fits(i128v) {
            return Err(QsdbError::type_err(format!(
                "decimal arithmetic overflows {result_meta}"
            )));
        }
        out.push(Some(i128v));
    }
    let arr = Decimal128Array::from(out).with_data_type(DataType::Decimal128(
        result_meta.precision,
        result_meta.scale,
    ));
    Ok(EvalVal::Arr(Arc::new(arr)))
}

fn b_is_zero(arr: &Decimal128Array, i: usize) -> bool {
    arr.value(i) == 0
}

fn decimal_meta_of(dt: &DataType) -> (i8, u8) {
    match dt {
        DataType::Decimal128(p, s) => (*s, *p),
        _ => (0, 38),
    }
}

/// 操作数对齐到公共 scale。
fn align_scale(v: I256, from: i8, to: i8) -> Result<I256> {
    if from == to {
        return Ok(v);
    }
    let d = i32::from(to) - i32::from(from);
    if d >= 0 {
        v.checked_mul(I256::from_i128(decimal::pow10(d as u32)))
            .ok_or_else(|| QsdbError::type_err("decimal scale align overflow"))
    } else {
        // 缩小（HALF_UP）
        let p = I256::from_i128(decimal::pow10((-d) as u32));
        let q = v.wrapping_div(p);
        let rem = v.wrapping_rem(p);
        // |rem|*2 >= p → 远离零进位
        let round_up =
            rem != I256::from_i128(0) && rem.wrapping_mul(I256::from_i128(2)).wrapping_abs() >= p;
        Ok(if round_up {
            if v >= I256::from_i128(0) {
                q + I256::from_i128(1)
            } else {
                q - I256::from_i128(1)
            }
        } else {
            q
        })
    }
}

/// i256 rescale（放大；缩小 HALF_UP）。
fn rescale_i256(v: I256, from: i32, to: i32) -> Result<I256> {
    if from == to {
        return Ok(v);
    }
    if from < to {
        v.checked_mul(I256::from_i128(decimal::pow10((to - from) as u32)))
            .ok_or_else(|| QsdbError::type_err("decimal rescale overflow"))
    } else {
        let p = I256::from_i128(decimal::pow10((from - to) as u32));
        let q = v.wrapping_div(p);
        let rem = v.wrapping_rem(p);
        let round_up =
            rem != I256::from_i128(0) && rem.wrapping_mul(I256::from_i128(2)).wrapping_abs() >= p;
        Ok(if round_up {
            if v >= I256::from_i128(0) {
                q + I256::from_i128(1)
            } else {
                q - I256::from_i128(1)
            }
        } else {
            q
        })
    }
}

// ---------------------------------------------------------------------------
// Literal → 1 行数组
// ---------------------------------------------------------------------------

/// Literal → 单行 arrow 数组（类型由 Literal 自身决定）。
pub fn literal_array(lit: &Literal) -> Result<ArrayRef> {
    use arrow::array::{
        BooleanArray, Date32Array, Float32Array, Float64Array, StringArray, Time64NanosecondArray,
        TimestampNanosecondArray,
    };
    Ok(match lit {
        // NULL 以「null 布尔常量」承载：在 Kleene and/or/not 与 cmp 下得到
        // SQL 三值语义（NULL 参与比较 → 全 UNKNOWN）；投影场景上层应避免裸 NULL
        Literal::Null => Arc::new(BooleanArray::from(vec![Option::<bool>::None])),
        Literal::Boolean(v) => Arc::new(BooleanArray::from(vec![Some(*v)])),
        Literal::Int8(v) => Arc::new(Int8Array::from(vec![Some(*v)])),
        Literal::Int16(v) => Arc::new(Int16Array::from(vec![Some(*v)])),
        Literal::Int32(v) => Arc::new(Int32Array::from(vec![Some(*v)])),
        Literal::Int64(v) => Arc::new(Int64Array::from(vec![Some(*v)])),
        Literal::Float32(v) => Arc::new(Float32Array::from(vec![Some(*v)])),
        Literal::Float64(v) => Arc::new(Float64Array::from(vec![Some(*v)])),
        Literal::Decimal { unscaled, meta } => Arc::new(
            Decimal128Array::from(vec![Some(*unscaled)])
                .with_data_type(DataType::Decimal128(meta.precision, meta.scale)),
        ),
        Literal::Utf8(s) => Arc::new(StringArray::from(vec![Some(s.as_str())])),
        Literal::Binary(_) => return Err(QsdbError::unsupported("binary literals in expressions")),
        Literal::Date(v) => Arc::new(Date32Array::from(vec![Some(*v)])),
        Literal::Time(v) => Arc::new(Time64NanosecondArray::from(vec![Some(*v)])),
        Literal::Timestamp(v) => Arc::new(TimestampNanosecondArray::from(vec![Some(*v)])),
        Literal::Interval { .. } => {
            return Err(QsdbError::unsupported("interval literals in expressions"));
        }
    })
}
