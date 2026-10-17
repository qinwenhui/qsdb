//! 向量化求值：叶子（列/常量）→ 算子（arrow kernels + Datum 广播）。
//!
//! DECIMAL 算术为 i256 自实现精确路径（arrow 数值 kernel 不覆盖 decimal）。

use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, Int8Array, Int16Array, Int32Array,
    Int64Array, StringArray,
};
use arrow::compute::kernels::cmp;
use arrow::compute::kernels::numeric;
use arrow::compute::{
    CastOptions, and_kleene, cast_with_options, is_not_null, is_null, not, or_kleene,
};
use arrow::datatypes::DataType;
use arrow::datatypes::i256 as I256;
use arrow::record_batch::RecordBatch;

use super::{ArithOp, CmpOp, FoldKind, PhysExpr};
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
    let arr = eval_to_n(expr, batch, batch.num_rows())?;
    arr.as_any()
        .downcast_ref::<BooleanArray>()
        .cloned()
        .ok_or_else(|| QsdbError::type_err("expression did not evaluate to boolean"))
}

/// CASE 向量化求值：布尔掩码从后往前覆盖（首个真条件的值存活——
/// 后覆盖者只会落在先前为 false/NULL 的行上，语义 = 按序短路）。
fn eval_case(
    conds: &[(PhysExpr, PhysExpr)],
    else_expr: &PhysExpr,
    batch: &RecordBatch,
) -> Result<ArrayRef> {
    let n = batch.num_rows();
    let mut current = eval_to_n(else_expr, batch, n)?;
    for (cond, result) in conds.iter().rev() {
        let mask = eval_mask(cond, batch)?;
        let result_arr = eval_to_n(result, batch, n)?;
        current = select_arrays(&mask, &result_arr, &current)?;
    }
    Ok(current)
}

fn broadcast_const(one: &ArrayRef, n: usize) -> Result<ArrayRef> {
    let indices: Vec<(usize, usize)> = vec![(0, 0); n];
    arrow::compute::interleave(&[one.as_ref()], &indices)
        .map_err(|e| QsdbError::invalid(format!("broadcast const failed: {e}")))
}

/// 求值并对齐到 n 行（Const / 常量折叠后的 1 行 Arr → 广播；
/// 其余长度不符报错——防静默错位）。
fn eval_to_n(expr: &PhysExpr, batch: &RecordBatch, n: usize) -> Result<ArrayRef> {
    match eval_val(expr, batch)? {
        EvalVal::Const(one) => broadcast_const(&one, n),
        EvalVal::Arr(a) if a.len() == n => Ok(a),
        EvalVal::Arr(a) if a.len() == 1 => broadcast_const(&a, n),
        EvalVal::Arr(a) => Err(QsdbError::invalid(format!(
            "expression length {} != batch rows {n}",
            a.len()
        ))),
    }
}

/// 布尔掩码选择：mask=true 取 a，false/NULL 取 b（arrow zip kernel 向量化）。
fn select_arrays(mask: &BooleanArray, a: &ArrayRef, b: &ArrayRef) -> Result<ArrayRef> {
    arrow::compute::kernels::zip::zip(mask, a, b)
        .map_err(|e| QsdbError::invalid(format!("select failed: {e}")))
}

/// COALESCE / LEAST / GREATEST 向量化 —— **跳过 NULL 的折叠**。
///
/// 参数在绑定期已 cast 到公共类型，此处逐参数扫一遍：
/// - `acc` 为 NULL 而本参非 NULL ⇒ 本参接上（这一步 = 「忽略 NULL」，全 NULL 的位置
///   自然保持 NULL）；
/// - 两者都非 NULL ⇒ 按 `ord` 取舍（`None` = 取**首个**，即 COALESCE；`Some(max)`
///   = 取最小/最大，即 LEAST/GREATEST）。
fn eval_fold(args: &[PhysExpr], batch: &RecordBatch, ord: Option<bool>) -> Result<ArrayRef> {
    let n = batch.num_rows();
    let mut acc = arrow::array::new_null_array(&args[0].data_type().arrow_type(), n);
    for e in args {
        let arr = eval_to_n(e, batch, n)?;
        let acc_nn = is_not_null(&acc)
            .map_err(|er| QsdbError::invalid(format!("is_not_null failed: {er}")))?;
        let arr_nn = is_not_null(&arr)
            .map_err(|er| QsdbError::invalid(format!("is_not_null failed: {er}")))?;
        // acc 尚空、本参有值 ⇒ 接上
        let fill = and_kleene(&not(&acc_nn)?, &arr_nn)?;
        let take = match ord {
            None => fill,
            Some(max) => {
                let both = and_kleene(&acc_nn, &arr_nn)?;
                // `take` 的语义是「用本参替换 acc」，所以判据写成 acc 站在**输**的一侧：
                // 取最大 —— 本参更大（acc < arr）时让位；取最小 —— 本参更小（acc > arr）。
                let better = if max {
                    cmp::lt(&acc, &arr)
                } else {
                    cmp::gt(&acc, &arr)
                }
                .map_err(|er| QsdbError::invalid(format!("compare failed: {er}")))?;
                // 两者都非 NULL 时才谈得上取舍；`both`/`fill` 均无 NULL 位
                or_kleene(&and_kleene(&both, &better)?, &fill)?
            }
        };
        acc = select_arrays(&take, &arr, &acc)?;
    }
    Ok(acc)
}

/// 字符串拼接 ||：NULL || x = NULL（SQL 语义）。
/// StringBuilder 单缓冲复用——无每行堆分配。
fn eval_concat(l: &PhysExpr, r: &PhysExpr, batch: &RecordBatch) -> Result<ArrayRef> {
    let n = batch.num_rows();
    let la = eval_to_n(l, batch, n)?;
    let ra = eval_to_n(r, batch, n)?;
    let ls = la
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| QsdbError::type_err("|| left not Utf8"))?;
    let rs = ra
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| QsdbError::type_err("|| right not Utf8"))?;
    let mut b = arrow::array::StringBuilder::new();
    use std::fmt::Write as _;
    for i in 0..ls.len() {
        if ls.is_null(i) || rs.is_null(i) {
            b.append_null();
        } else {
            // 两段直写 builder 缓冲（fmt::Write 追加不收值），append_value("") 收值
            b.write_str(ls.value(i)).ok();
            b.write_str(rs.value(i)).ok();
            b.append_value("");
        }
    }
    Ok(Arc::new(b.finish()))
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
            } else if matches!(l.data_type(), LogicalType::Date)
                || matches!(r.data_type(), LogicalType::Date)
            {
                date_arith(*op, &lv, &rv)
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
                    // NULL 字面量兜底类型是 Boolean，arrow 无 Boolean→数值 cast；
                    // 全 NULL 短路（NULL cast 恒 NULL，语义等价）
                    if one.null_count() == one.len() {
                        return Ok(EvalVal::Const(arrow::array::new_null_array(
                            &target,
                            one.len(),
                        )));
                    }
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
                EvalVal::Arr(a) => Ok(EvalVal::Arr(if a.null_count() == a.len() {
                    arrow::array::new_null_array(&target, a.len())
                } else {
                    cast_with_options(a.as_ref(), &target, &CastOptions::default()).map_err(
                        |err| {
                            QsdbError::type_err(format!(
                                "cast {} to {ty} failed: {err}",
                                e.display()
                            ))
                        },
                    )?
                })),
            }
        }
        PhysExpr::Case {
            conds, else_expr, ..
        } => Ok(EvalVal::Arr(eval_case(conds, else_expr, batch)?)),
        PhysExpr::Fold { kind, args, .. } => {
            let ord = match kind {
                FoldKind::Coalesce => None,
                FoldKind::Least => Some(false),
                FoldKind::Greatest => Some(true),
            };
            Ok(EvalVal::Arr(eval_fold(args, batch, ord)?))
        }
        PhysExpr::Concat(l, r) => Ok(EvalVal::Arr(eval_concat(l, r, batch)?)),
        PhysExpr::Like {
            e,
            pattern,
            negated,
            ci,
        } => {
            // NULL LIKE x → NULL（三值；Filter 掩码 null 按 false 处理）
            let arr = eval_to_n(e, batch, batch.num_rows())?;
            let s = arr
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QsdbError::type_err("LIKE requires string operand"))?;
            let mut b = arrow::array::BooleanBuilder::with_capacity(s.len());
            for i in 0..s.len() {
                if s.is_null(i) {
                    b.append_null();
                } else {
                    let m = pattern.matches(s.value(i), *ci);
                    b.append_value(if *negated { !m } else { m });
                }
            }
            Ok(EvalVal::Arr(Arc::new(b.finish())))
        }
        PhysExpr::Fn { name, args, .. } => {
            let def = super::scalar::lookup(name)?;
            let n = batch.num_rows();
            // ⚠️ **逐行非确定性函数要在这里特判**：`uuid()` / `sys_guid()` 必须
            // 每行不同（它是当**主键**用的），而下面那条约定是"输出 1 个元素 ⇒
            // 当常量广播" ⇒ 不特判的话**整批会拿到同一个 UUID**，主键全撞还
            // 一声不响。详见 `scalar::uuid_strings`。
            if args.is_empty()
                && (name.eq_ignore_ascii_case("uuid") || name.eq_ignore_ascii_case("sys_guid"))
            {
                return Ok(EvalVal::Arr(super::scalar::uuid_strings(n)));
            }
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
        // **整数除零 ⇒ NULL**（Spark 语义）。arrow 的整数内核**直接报错**
        // （`Divide by zero error`），与 Spark 的 `NULL` 不同。
        //
        // 走**冷路径**：先照常调内核（热路径**零开销** —— 绝大多数批里没有 0），
        // 只有当它确实因除零失败时，才重算一遍并把那些位置改成 NULL。
        // （DECIMAL 不走这里，见 `decimal_arith`，那边同样是 NULL。）
        ArithOp::Div => div_mod_null_on_zero(true, l, r),
        ArithOp::Mod => div_mod_null_on_zero(false, l, r),
    }
}

/// 整数除/模的**除零 ⇒ NULL**：内核报除零时，把分母的 0 换成 1 重算，
/// 再把那些位置盖成 NULL。只为**错误路径**付代价。
fn div_mod_null_on_zero(is_div: bool, l: &EvalVal, r: &EvalVal) -> Result<ArrayRef> {
    let first = if is_div {
        with_datum(l, r, numeric::div)
    } else {
        with_datum(l, r, numeric::rem)
    };
    match first {
        Ok(v) => Ok(v),
        Err(e) if e.to_string().to_lowercase().contains("divide by zero") => {
            let n = l.len_hint().max(r.len_hint());
            let op_name = if is_div { "div" } else { "rem" };
            let ra = match r {
                EvalVal::Arr(a) => Arc::clone(a),
                EvalVal::Const(one) => materialize_const(one, n)?,
            };
            let rdt = ra.data_type().clone();
            let scalar = |v: i64| -> Result<ArrayRef> {
                cast_with_options(&Int64Array::from(vec![v]), &rdt, &CastOptions::default())
                    .map_err(|er| QsdbError::invalid(format!("{op_name}: 构造常量失败: {er}")))
            };
            let zero = scalar(0)?;
            let one = scalar(1)?;
            // 分母非零 → 真；0 或 NULL → 假
            let nz: BooleanArray = cmp::neq(&ra, &WrapConst(zero.as_ref()))
                .map_err(|er| QsdbError::invalid(format!("{op_name}: 比较失败: {er}")))?;
            let safe_r = select_arrays(&nz, &ra, &one)?;
            let out: ArrayRef = if is_div {
                with_datum(l, &EvalVal::Arr(safe_r), numeric::div)?
            } else {
                with_datum(l, &EvalVal::Arr(safe_r), numeric::rem)?
            };
            let nulls = arrow::array::new_null_array(&out.data_type().clone(), n);
            select_arrays(&nz, &out, &nulls)
        }
        Err(e) => Err(e),
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
        // **除零 ⇒ NULL**（Spark 语义，实测真 Spark 3.5.2：`1/0`、小数除零、`1%0`
        // 全是 `NULL`）。
        //
        // 与下面 `Mul` 的**溢出**报错口径不同，是**刻意**的：
        // - 溢出是"这个值装不下"，报错能逼出模型/精度问题（本仓一贯口径）；
        // - 除零是**数据相关**的运行期状况（某一行分母恰好是 0），
        //   报错会让整批结算因为一个坏行全挂，而 Spark 的答案本来就是 NULL。
        if b_is_zero(rd, i) && matches!(op, ArithOp::Div | ArithOp::Mod) {
            out.push(None);
            continue;
        }
        let a = I256::from_i128(ld.value(i));
        let b = I256::from_i128(rd.value(i));
        let v: I256 = match op {
            ArithOp::Add => align_scale(a, lsa, rs)? + align_scale(b, lsb, rs)?,
            ArithOp::Sub => align_scale(a, lsa, rs)? - align_scale(b, lsb, rs)?,
            ArithOp::Mul => {
                // 结果 scale = sa + sb（binder 保证 rs == sa + sb）
                //
                // ⚠️ **必须 checked**：`mul_result` 按 Spark 的规则把精度**封顶到 38**
                // （(30,8)×(30,8) ⇒ (38,6)，见 `types::decimal::mul_result`），
                // 于是"真装不下"的值只能靠这里报错兜住 —— 用裸 `*` 会**静默回绕**，
                // 那就是本仓一路在消灭的静默错值。（Spark 非 ANSI 模式下这类溢出回
                // NULL；本仓的口径是**报错**，与 `sum` 溢出同一条纪律。）
                let raw = a
                    .checked_mul(b)
                    .ok_or_else(|| QsdbError::type_err("decimal multiply overflow"))?;
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
            ArithOp::Mod => {
                // 余数符号随被除数；两侧对齐到结果 scale 后取模
                align_scale(a, lsa, rs)? % align_scale(b, lsb, rs)?
            }
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
    // **两侧都是 Const ⇒ 结果保持 Const（长度 1）** —— 与非十进制分支的
    // `both_const → Const` 同一条纪律。
    //
    // ⚠️ 漏了这一收口的后果不是"多占一行"：`(1+(0.2))` 这类**常量×常量**返回
    // `Arr(len 1)`，外层 `列 × 它` 时 `len_hint` 取到 N、而 Arr 侧**不会**再
    // `materialize_const` ⇒ `value(1)` 越界 panic。语料 L1-01/L1-16 在旧切片上
    // 输入恰好为空（0 行循环不到 i=1）一直没炸，换到有数据的切片当场爆。
    if matches!(l, EvalVal::Const(_)) && matches!(r, EvalVal::Const(_)) {
        return Ok(EvalVal::Const(Arc::new(arr)));
    }
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
// 日期算术：DATE ± INTEGER（天）/ DATE - DATE（天数）
// ---------------------------------------------------------------------------

/// 日期算术（binder 保证一侧 DATE、int 侧 Int64）：i64 中间量防 i32 溢出。
fn date_arith(op: ArithOp, l: &EvalVal, r: &EvalVal) -> Result<EvalVal> {
    let n = l.len_hint().max(r.len_hint());
    let la = match l {
        EvalVal::Arr(a) => Arc::clone(a),
        EvalVal::Const(one) => materialize_const(one, n)?,
    };
    let ra = match r {
        EvalVal::Arr(a) => Arc::clone(a),
        EvalVal::Const(one) => materialize_const(one, n)?,
    };
    let ld = la.as_any().downcast_ref::<Date32Array>();
    let rd = ra.as_any().downcast_ref::<Date32Array>();

    // DATE − DATE → 天数（Int64）
    if let (Some(l), Some(r)) = (ld, rd) {
        if !matches!(op, ArithOp::Sub) {
            return Err(QsdbError::type_err("DATE + DATE is not defined"));
        }
        let mut out: Vec<Option<i64>> = Vec::with_capacity(n);
        for i in 0..n {
            if l.is_null(i) || r.is_null(i) {
                out.push(None);
            } else {
                out.push(Some(i64::from(l.value(i)) - i64::from(r.value(i))));
            }
        }
        return Ok(EvalVal::Arr(Arc::new(Int64Array::from(out))));
    }

    // DATE ± INTEGER → DATE（Add 双侧可换；Sub 仅 Date−Int）
    let bad = || QsdbError::type_err("date arithmetic expects DATE and INTEGER operands");
    let (da, ia, int_first) = match (ld, rd) {
        (Some(d), None) => (
            d,
            ra.as_any().downcast_ref::<Int64Array>().ok_or_else(bad)?,
            false,
        ),
        (None, Some(d)) => (
            d,
            la.as_any().downcast_ref::<Int64Array>().ok_or_else(bad)?,
            true,
        ),
        _ => return Err(bad()),
    };
    if int_first && matches!(op, ArithOp::Sub) {
        return Err(QsdbError::type_err("INTEGER - DATE is not defined"));
    }
    let mut out: Vec<Option<i32>> = Vec::with_capacity(n);
    for i in 0..n {
        if da.is_null(i) || ia.is_null(i) {
            out.push(None);
            continue;
        }
        let d = i64::from(da.value(i));
        let k = ia.value(i);
        let v = match op {
            ArithOp::Add => d + k,
            _ => d - k, // Sub（其余运算 binder 已拒）
        };
        let Some(v32) = i32::try_from(v).ok() else {
            return Err(QsdbError::type_err("date arithmetic overflow"));
        };
        out.push(Some(v32));
    }
    Ok(EvalVal::Arr(Arc::new(Date32Array::from(out))))
}

// ---------------------------------------------------------------------------
// DECIMAL 标量数值函数（abs / round / floor / ceil）
// ---------------------------------------------------------------------------

/// 取整方向（round=HALF_UP 远离零；floor/ceil=向 ±∞）。
#[derive(Clone, Copy)]
pub(crate) enum RoundDir {
    HalfUp,
    Floor,
    Ceil,
}

/// decimal abs：scale 不变（i128 原生；i128::MIN 溢出即报错——
/// 其绝对值超出 38 位精度，DECIMAL 域内不可表示）。
pub(crate) fn decimal_abs(arr: &ArrayRef) -> Result<ArrayRef> {
    let d = arr
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .ok_or_else(|| QsdbError::type_err("abs: arg is not decimal"))?;
    let (s, p) = decimal_meta_of(d.data_type());
    let meta = crate::types::DecimalMeta::new(p, s);
    let mut out: Vec<Option<i128>> = Vec::with_capacity(d.len());
    for i in 0..d.len() {
        match d.is_null(i) {
            true => out.push(None),
            false => {
                let v = d
                    .value(i)
                    .checked_abs()
                    .filter(|v| meta.fits(*v))
                    .ok_or_else(|| QsdbError::type_err(format!("abs overflows {meta}")))?;
                out.push(Some(v));
            }
        }
    }
    Ok(Arc::new(
        Decimal128Array::from(out).with_data_type(DataType::Decimal128(p, s)),
    ))
}

/// decimal 定点取整：在 digit 位取整后放大回原 scale（类型不变）。
/// digits >= scale 时零拷贝返回原数组。i128 原生除法
///（10^38 ≤ i128 域；|q·pw| ≤ |v| + pw，checked 兜底极端值）。
pub(crate) fn decimal_round_dir(arr: &ArrayRef, digits: i64, dir: RoundDir) -> Result<ArrayRef> {
    let d = arr
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .ok_or_else(|| QsdbError::type_err("round: arg is not decimal"))?;
    let (s, p) = decimal_meta_of(d.data_type());
    if digits >= i64::from(s) {
        return Ok(Arc::clone(arr)); // 已在该精度内，无需取整
    }
    let di = digits.clamp(-38, 38) as i32; // |digits|>38 时结果恒 0，钳制不失语义
    let k = (i32::from(s) - di).clamp(0, 38) as u32; // 缩小位数（≤ 38：pow10 表界）
    let pw = decimal::pow10(k); // 10^38 < i128::MAX
    let half = (pw + 1) / 2; // HALF_UP 阈值（无溢出形式：2|rem| >= pw ⟺ |rem| >= half）
    let mut out: Vec<Option<i128>> = Vec::with_capacity(d.len());
    for i in 0..d.len() {
        if d.is_null(i) {
            out.push(None);
            continue;
        }
        let v = d.value(i);
        let q = v / pw; // Rust 除法向零截断
        let rem = v % pw;
        // 方向修正：HALF_UP 远离零 / floor 向 -∞ / ceil 向 +∞
        let q = match dir {
            RoundDir::HalfUp => {
                if rem.abs() >= half && rem != 0 {
                    if v >= 0 { q + 1 } else { q - 1 }
                } else {
                    q
                }
            }
            RoundDir::Floor => {
                if v < 0 && rem != 0 {
                    q - 1
                } else {
                    q
                }
            }
            RoundDir::Ceil => {
                if v > 0 && rem != 0 {
                    q + 1
                } else {
                    q
                }
            }
        };
        let Some(r) = q.checked_mul(pw) else {
            return Err(QsdbError::type_err("decimal round overflow"));
        };
        out.push(Some(r));
    }
    Ok(Arc::new(
        Decimal128Array::from(out).with_data_type(DataType::Decimal128(p, s)),
    ))
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
        // BINARY 字面量**可以**当常量：`INSERT … VALUES (X'6162')` 与
        // `CAST(… AS BINARY)` 都落到这里。此前一律拒绝，等于 BINARY 列
        // 经 SQL 无法写入（qsdb-jdbc 实测踩到）。
        Literal::Binary(b) => Arc::new(arrow::array::BinaryArray::from(vec![Some(b.as_slice())])),
        Literal::Date(v) => Arc::new(Date32Array::from(vec![Some(*v)])),
        Literal::Time(v) => Arc::new(Time64NanosecondArray::from(vec![Some(*v)])),
        Literal::Timestamp(v) => Arc::new(TimestampNanosecondArray::from(vec![Some(*v)])),
        Literal::Interval { .. } => {
            return Err(QsdbError::unsupported("interval literals in expressions"));
        }
    })
}
