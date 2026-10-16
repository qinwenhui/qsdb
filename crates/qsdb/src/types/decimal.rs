//! DECIMAL 内核：精度矩阵、HALF_UP 舍入、格式化与解析。
//!
//! 规则对齐参考文档 §3.2：
//! - `precision` 1..=38，`scale` 0..=precision
//! - 主路径 i128（unscaled 值）；聚合与中间量在执行层用 i256
//! - 默认舍入 HALF_UP（`.5` 远离零）；溢出即报错（不做饱和）
//! - SQL 文本输出保留按 scale 的尾随零（`123456789.12345` → 精确 5 位小数）

use std::cmp::Ordering;

use serde::{Deserialize, Serialize};

use crate::error::{QsdbError, Result};

/// 10^0 .. 10^38（i128 可容纳的上限）
const POW10: [i128; 39] = {
    let mut t = [1i128; 39];
    let mut i = 1;
    while i < 39 {
        t[i] = t[i - 1] * 10;
        i += 1;
    }
    t
};

/// 10^n（n ≤ 38）
#[inline]
pub const fn pow10(n: u32) -> i128 {
    POW10[n as usize]
}

/// 校验过的 DECIMAL(precision, scale) 元数据。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DecimalMeta {
    pub precision: u8,
    pub scale: i8,
}

impl DecimalMeta {
    pub const MAX_PRECISION: u8 = 38;

    pub fn new(precision: u8, scale: i8) -> Self {
        debug_assert!(
            (1..=Self::MAX_PRECISION).contains(&precision),
            "precision must be in 1..=38"
        );
        debug_assert!(
            scale >= 0 && (scale as i16) <= (precision as i16),
            "scale must be in 0..=precision"
        );
        Self { precision, scale }
    }

    /// 带校验构造（运行时输入路径使用）。
    pub fn try_new(precision: u8, scale: i8) -> Result<Self> {
        if !(1..=Self::MAX_PRECISION).contains(&precision) {
            return Err(QsdbError::type_err(format!(
                "DECIMAL precision {precision} out of range 1..=38"
            )));
        }
        if scale < 0 || i16::from(scale) > i16::from(precision) {
            return Err(QsdbError::type_err(format!(
                "DECIMAL scale {scale} out of range 0..={precision}"
            )));
        }
        Ok(Self { precision, scale })
    }

    /// 最大 unscaled 值：10^precision - 1。
    #[inline]
    pub fn max_unscaled(&self) -> i128 {
        pow10(u32::from(self.precision)) - 1
    }

    /// unscaled 值是否可容纳于该精度（|v| ≤ 10^p - 1）。
    #[inline]
    pub fn fits(&self, unscaled: i128) -> bool {
        unscaled.unsigned_abs() <= self.max_unscaled() as u128
    }

    /// 整数部分可用位数 = precision - scale。
    #[inline]
    pub fn int_digits(&self) -> u8 {
        self.precision - self.scale as u8
    }
}

impl std::fmt::Display for DecimalMeta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DECIMAL({}, {})", self.precision, self.scale)
    }
}

// ---------------------------------------------------------------------------
// 运算结果精度矩阵（参考文档 §3.2）
// ---------------------------------------------------------------------------

impl DecimalMeta {
    /// `x + y` / `x - y`：max(p1-s1, p2-s2) + max(s1,s2) + 1, max(s1,s2)
    pub fn add_result(a: Self, b: Self) -> Result<Self> {
        let s = a.scale.max(b.scale);
        let ip = (i16::from(a.precision) - i16::from(a.scale))
            .max(i16::from(b.precision) - i16::from(b.scale));
        Self::try_new((ip + i16::from(s) + 1) as u8, s)
    }

    /// `x - y` 与加法同矩阵。
    pub fn sub_result(a: Self, b: Self) -> Result<Self> {
        Self::add_result(a, b)
    }

    /// `x * y`：标准矩阵 `p1+p2+1, s1+s2`；**超 38 时按 Spark 的规则封顶**：
    ///
    /// ```text
    /// p ≤ 38  ⇒ (p, s)
    /// 否则    ⇒ (38, max(38-(p-s), min(s, 6)))
    /// ```
    ///
    /// **这条公式是拿真 Spark 3.5.2 反推、六组实测逐一对上的**（`.14` 上跑的）：
    ///
    /// | 表达式 | 计算值 | Spark `typeof` |
    /// |---|---|---|
    /// | `(10,2)×(10,2)` | (21,4) | `decimal(21,4)` |
    /// | `(30,8)×(30,8)` | (61,16) | `decimal(38,6)` |
    /// | `(25,5)×(25,5)` | (51,10) | `decimal(38,6)` |
    /// | `(38,0)×(38,0)` | (77,0) | `decimal(38,0)` |
    /// | `(30,20)×(30,20)` | (61,40) | `decimal(38,17)` |
    /// | `(38,10)×(38,16)` | (77,26) | `decimal(38,6)` |
    ///
    /// ⚠️ **为什么不用 DuckDB 的规则**（"保 scale 截精度" ⇒ `(30,8)×(30,8)` 给 `(38,16)`）：
    /// 用户那批日清结算 SQL 是 **Spark SQL** 写的，口径按 Spark 对齐。两者只在
    /// "需要封顶"时才分叉。
    ///
    /// ⚠️ 封顶**会丢精度**（Spark 的已知行为，见 SPARK-22036）——所以真正的溢出
    /// 必须由**运行期**的 `checked_mul` 报错兜住，**不能静默回绕**
    /// （见 `expr::eval` 的 `ArithOp::Mul`）。
    pub fn mul_result(a: Self, b: Self) -> Result<Self> {
        Self::adjust(
            i16::from(a.precision) + i16::from(b.precision) + 1,
            i16::from(a.scale) + i16::from(b.scale),
        )
    }

    /// 把**算出来的** `(p, s)` 按 Spark 的规则封顶到 38：`p ≤ 38` 原样，
    /// 否则 `(38, max(38-(p-s), min(s, 6)))`。
    ///
    /// 这个调整式是**拿真 Spark 3.5.2 反推、十三个用例逐一对上**的
    /// （乘 6 + 除 4 + avg 3，见 `mul_result` / `div_result` / `avg_result` 的表）。
    fn adjust(p: i16, s: i16) -> Result<Self> {
        if p <= 38 {
            Self::try_new(p as u8, s as i8)
        } else {
            let ns = (38 - (p - s)).max(s.min(6));
            Self::try_new(Self::MAX_PRECISION, ns as i8)
        }
    }

    /// `x / y`：Spark 的规则 `p1-s1+s2+R, R`，其中 `R = max(6, s1+p2+1)`，再封顶。
    ///
    /// ⚠️ **此前这条是错的**（用的是"p1+s2+max(s1+s2,4)"），实测真 Spark：
    /// `(10,2)/(10,3)` ⇒ `decimal(24,13)`（我们旧规则给 (18,5)）、
    /// `(5,1)/(10,2)` ⇒ `(18,12)`、`(10,2)/(5,1)` ⇒ `(17,8)`、
    /// `(30,8)/(30,8)` ⇒ `(38,8)`（旧规则给 (38,16)）。
    /// **除法口径错 = 结算金额错**，所以这条按 Spark 逐格对齐。
    pub fn div_result(a: Self, b: Self) -> Result<Self> {
        let r = (i16::from(a.scale) + i16::from(b.precision) + 1).max(6);
        let p = i16::from(a.precision) - i16::from(a.scale) + i16::from(b.scale) + r;
        Self::adjust(p, r)
    }

    /// `x % y`：Spark 的规则 `min(p1-s1, p2-s2) + max(s1,s2), max(s1,s2)`。
    ///
    /// ⚠️ **整数位取的是 `min` 不是 `max`**（余数的绝对值 < 两侧较小者的绝对值），
    /// 此前写成 `max` ⇒ `(30,8)%(10,2)` 给 `(30,8)`，Spark 是 **`(16,8)`**。
    pub fn mod_result(a: Self, b: Self) -> Result<Self> {
        let s = a.scale.max(b.scale);
        let ip = (i16::from(a.precision) - i16::from(a.scale))
            .min(i16::from(b.precision) - i16::from(b.scale));
        Self::adjust(ip + i16::from(s), i16::from(s))
    }

    /// `sum(x)`：min(p+10, 38), s
    pub fn sum_result(a: Self) -> Self {
        Self {
            precision: a.precision.saturating_add(10).min(Self::MAX_PRECISION),
            scale: a.scale,
        }
    }

    /// `avg(x)`：Spark 的规则 `min(38, p+4), s+4`。
    ///
    /// ⚠️ **此前这条也是错的**（用的是 `min(38,p+10), max(s,4)`）。实测真 Spark：
    /// `avg((30,8))` ⇒ `decimal(34,12)`、`avg((10,2))` ⇒ `(14,6)`、
    /// `avg((38,10))` ⇒ `(38,14)` —— 三条都是 `(min(38,p+4), s+4)` ✓。
    pub fn avg_result(a: Self) -> Self {
        Self {
            precision: a.precision.saturating_add(4).min(Self::MAX_PRECISION),
            scale: a.scale.saturating_add(4),
        }
    }
}

// ---------------------------------------------------------------------------
// 舍入与 rescale
// ---------------------------------------------------------------------------

/// HALF_UP 整数除法：`n / d`，当余数绝对值 ≥ d/2 时远离零进位。
///
/// 用于小数位缩减。`d` 必须为正；结果幅值不会超过 `|n/d| + 1`，无溢出。
pub fn round_half_up_div(n: i128, d: i128) -> i128 {
    debug_assert!(d > 0);
    let q = n.wrapping_div(d);
    let r = n.wrapping_rem(d);
    if r != 0 && r.unsigned_abs() * 2 >= d as u128 {
        if n >= 0 { q + 1 } else { q - 1 }
    } else {
        q
    }
}

/// unscaled 值在两个 scale 之间换算；放大用 checked 乘（溢出报错），
/// 缩小用 HALF_UP。
pub fn rescale(unscaled: i128, from: i8, to: i8) -> Result<i128> {
    match to.cmp(&from) {
        Ordering::Equal => Ok(unscaled),
        Ordering::Greater => {
            let d = (to - from) as u32;
            if d > 38 {
                return Err(QsdbError::type_err(format!(
                    "decimal rescale {from} -> {to} exceeds precision limit"
                )));
            }
            unscaled
                .checked_mul(pow10(d))
                .ok_or_else(|| QsdbError::type_err("decimal rescale overflow (i128)"))
        }
        Ordering::Less => {
            let d = (from - to) as u32;
            debug_assert!(d <= 38);
            Ok(round_half_up_div(unscaled, pow10(d)))
        }
    }
}

// ---------------------------------------------------------------------------
// 格式化与解析
// ---------------------------------------------------------------------------

/// unscaled → SQL 文本，保留按 scale 的尾随零。
///
/// 例：`(123456789, 5)` → `"1234.56789"`；`(-5, 2)` → `"-0.05"`；`(100, 2)` → `"1.00"`。
pub fn format_decimal(unscaled: i128, scale: i8) -> String {
    debug_assert!(scale >= 0);
    if scale == 0 {
        return unscaled.to_string();
    }
    let scale = scale as usize;
    let neg = unscaled < 0;
    let mag = unscaled.unsigned_abs().to_string(); // u128 路径，i128::MIN 安全
    let digits = if mag.len() <= scale {
        format!("{}{mag}", "0".repeat(scale + 1 - mag.len()))
    } else {
        mag
    };
    let split = digits.len() - scale;
    let body = format!("{}.{}", &digits[..split], &digits[split..]);
    if neg { format!("-{body}") } else { body }
}

/// SQL 数字文本 → unscaled（HALF_UP 处理超出 scale 的部分；超出 precision 报错）。
///
/// 接受 `[+-]?digits[.digits]`，至少一位数字；不接受指数记法与空白（不 trim）。
pub fn parse_decimal(s: &str, meta: DecimalMeta) -> Result<i128> {
    let invalid = || QsdbError::parse_err(format!("invalid DECIMAL literal '{s}'"));
    let (neg, body) = match s.as_bytes().first() {
        Some(b'+') => (false, &s[1..]),
        Some(b'-') => (true, &s[1..]),
        _ => (false, s),
    };
    let (int_part, frac_part) = match body.split_once('.') {
        Some((i, f)) => (i, f),
        None => (body, ""),
    };
    if int_part.is_empty() && frac_part.is_empty() {
        return Err(invalid());
    }
    if !int_part.bytes().all(|b| b.is_ascii_digit())
        || !frac_part.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(invalid());
    }
    let scale = meta.scale as usize;
    let int_digits = int_part.trim_start_matches('0');
    // 有效整数位 + 目标 scale 决定了最终位数
    if int_digits.len() + scale > usize::from(DecimalMeta::MAX_PRECISION) {
        return Err(QsdbError::type_err(format!("value '{s}' overflows {meta}")));
    }
    let int_val: i128 = if int_digits.is_empty() {
        0
    } else {
        int_digits.parse().map_err(|_| invalid())?
    };
    let mut v = int_val
        .checked_mul(pow10(scale as u32))
        .ok_or_else(|| QsdbError::type_err(format!("value '{s}' overflows {meta}")))?;
    let kept = frac_part.len().min(scale);
    if kept > 0 {
        let frac_val: i128 = frac_part[..kept].parse().map_err(|_| invalid())?;
        v = v
            .checked_add(
                frac_val
                    .checked_mul(pow10((scale - kept) as u32))
                    .ok_or_else(|| QsdbError::type_err(format!("value '{s}' overflows {meta}")))?,
            )
            .ok_or_else(|| QsdbError::type_err(format!("value '{s}' overflows {meta}")))?;
    }
    // 超出 scale 的首位数字决定 HALF_UP 进位
    if frac_part.len() > scale && frac_part.as_bytes()[scale] >= b'5' {
        v = v
            .checked_add(1)
            .ok_or_else(|| QsdbError::type_err(format!("value '{s}' overflows {meta}")))?;
    }
    let v = if neg { -v } else { v };
    if !meta.fits(v) {
        return Err(QsdbError::type_err(format!("value '{s}' overflows {meta}")));
    }
    Ok(v)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(p: u8, s: i8) -> DecimalMeta {
        DecimalMeta::try_new(p, s).unwrap()
    }

    // -- 元数据校验 -----------------------------------------------------------

    #[test]
    fn meta_validation() {
        assert!(DecimalMeta::try_new(0, 0).is_err());
        assert!(DecimalMeta::try_new(39, 0).is_err());
        assert!(DecimalMeta::try_new(5, 6).is_err());
        assert!(DecimalMeta::try_new(5, -1).is_err());
        assert_eq!(m(1, 0), DecimalMeta::new(1, 0));
        assert_eq!(m(38, 38).precision, 38);
        assert_eq!(m(18, 4).int_digits(), 14);
    }

    #[test]
    fn max_unscaled_and_fits() {
        assert_eq!(m(38, 0).max_unscaled(), pow10(38) - 1);
        assert!(m(5, 0).fits(99_999));
        assert!(!m(5, 0).fits(100_000));
        assert!(m(5, 0).fits(-99_999));
        // |i128::MIN| = 1.7e38 > 10^38 - 1，超出 DECIMAL(38) 上界
        assert!(!m(38, 0).fits(i128::MIN));
    }

    // -- 精度矩阵（表驱动） ---------------------------------------------------

    #[test]
    /// ⚠️ **2026-10-16 按真 Spark 3.5.2 重新对过一遍** —— 这条测试原来钉的
    /// **除 / 取模 / avg / 乘**四组期望是**旧口径**（乘的 +1、除的整套公式、
    /// 取模的 max→min、avg 的 +10/+4），拿真 Spark 逐格比对时发现**全是错的**。
    /// 现在这条只留"我们自己的口径自洽"，**与 Spark 的对齐钉在
    /// `spark_precision_matrix`**（那里每一行都是实测值）。
    fn precision_matrix_explicit() {
        // 加/减：与 Spark 一致（本就没改）
        assert_eq!(
            DecimalMeta::add_result(m(18, 4), m(18, 4)).unwrap(),
            m(19, 4)
        );
        assert_eq!(DecimalMeta::add_result(m(9, 0), m(5, 2)).unwrap(), m(12, 2));
        assert_eq!(
            DecimalMeta::sub_result(m(18, 4), m(18, 4)).unwrap(),
            m(19, 4)
        );
        // 乘：**+1**（Spark：(10,2)×(10,2) ⇒ (21,4)）；不再"超 38 就报错"，
        // 而是**封顶**（Spark：(38,0)×(38,0) ⇒ (38,0)）
        assert_eq!(
            DecimalMeta::mul_result(m(18, 4), m(18, 4)).unwrap(),
            m(37, 8)
        );
        assert_eq!(
            DecimalMeta::mul_result(m(5, 2), m(3, 1)).unwrap(),
            m(9, 3),
            "p = 5+3+1 = 9, s = 2+1 = 3"
        );
        assert_eq!(
            DecimalMeta::mul_result(m(20, 0), m(20, 0)).unwrap(),
            m(38, 0),
            "封顶到 38、scale 0（Spark 实测）"
        );
        // 除：Spark 的 `p1-s1+s2+R, R`，R = max(6, s1+p2+1)
        assert_eq!(
            DecimalMeta::div_result(m(18, 4), m(18, 4)).unwrap(),
            m(38, 20),
            "R = max(6, 4+18+1) = 23 ⇒ p = 18-4+4+23 = 41 > 38 ⇒ 封顶 (38, max(38-18, 6)) = (38,20)"
        );
        assert_eq!(
            DecimalMeta::div_result(m(5, 0), m(2, 0)).unwrap(),
            m(11, 6),
            "R = max(6, 0+2+1) = 6 ⇒ p = 5-0+0+6 = 11, s = 6（≤38，不封顶）"
        );
        assert_eq!(
            DecimalMeta::div_result(m(38, 20), m(38, 20)).unwrap(),
            m(38, 6)
        );
        assert_eq!(
            DecimalMeta::div_result(m(38, 0), m(38, 0)).unwrap(),
            m(38, 6)
        );
        // 聚合
        assert_eq!(DecimalMeta::sum_result(m(28, 6)), m(38, 6));
        assert_eq!(DecimalMeta::sum_result(m(30, 0)), m(38, 0));
        assert_eq!(DecimalMeta::avg_result(m(10, 2)), m(14, 6));
        assert_eq!(DecimalMeta::avg_result(m(10, 6)), m(14, 10));
    }

    #[test]
    fn precision_matrix_property() {
        // 加法结果必能容纳两操作数的放大对齐
        for p1 in [1u8, 9, 18, 28, 38] {
            for p2 in [1u8, 9, 18, 28, 38] {
                for s in [0i8, 2, 4] {
                    let (s1, s2) = (s.min(p1 as i8), s.min(p2 as i8));
                    let a = m(p1, s1);
                    let b = m(p2, s2);
                    if let Ok(r) = DecimalMeta::add_result(a, b) {
                        assert!(r.scale >= a.scale && r.scale >= b.scale);
                        assert!(r.int_digits() >= a.int_digits().max(b.int_digits()));
                    } else {
                        // 溢出仅当整数位 + 公共 scale + 1 > 38
                        let expect_ip = a.int_digits().max(b.int_digits());
                        assert!(i32::from(expect_ip) + i32::from(r_expected_scale(a, b)) + 1 > 38);
                    }
                }
            }
        }
    }

    fn r_expected_scale(a: DecimalMeta, b: DecimalMeta) -> i8 {
        a.scale.max(b.scale)
    }

    // -- 舍入 -----------------------------------------------------------------

    #[test]
    fn half_up_division() {
        assert_eq!(round_half_up_div(5, 2), 3);
        assert_eq!(round_half_up_div(-5, 2), -3);
        assert_eq!(round_half_up_div(4, 2), 2);
        assert_eq!(round_half_up_div(-4, 2), -2);
        assert_eq!(round_half_up_div(15, 10), 2);
        assert_eq!(round_half_up_div(-15, 10), -2);
        assert_eq!(round_half_up_div(14, 10), 1);
        assert_eq!(round_half_up_div(25, 10), 3);
        assert_eq!(round_half_up_div(0, 10), 0);
    }

    #[test]
    fn rescale_both_ways() {
        assert_eq!(rescale(12345, 2, 4).unwrap(), 1_234_500);
        assert_eq!(rescale(12345, 4, 2).unwrap(), 123); // 1.2345 -> 1.23
        assert_eq!(rescale(12495, 4, 2).unwrap(), 125); // 1.2495 -> 1.25 (HALF_UP)
        assert_eq!(rescale(12449, 4, 2).unwrap(), 124); // 1.2449 -> 1.24
        assert_eq!(rescale(-12495, 4, 2).unwrap(), -125); // -1.2495 -> -1.25
        assert_eq!(rescale(7, 0, 0).unwrap(), 7);
        assert!(rescale(i128::MAX - 1, 0, 2).is_err()); // 放大溢出
    }

    // -- 格式化 ---------------------------------------------------------------

    #[test]
    fn format_matches_reference_cases() {
        assert_eq!(format_decimal(12345678912345, 5), "123456789.12345");
        assert_eq!(format_decimal(-5, 2), "-0.05");
        assert_eq!(format_decimal(100, 2), "1.00");
        assert_eq!(format_decimal(0, 4), "0.0000");
        assert_eq!(format_decimal(123, 0), "123");
        assert_eq!(format_decimal(-123, 0), "-123");
        assert_eq!(format_decimal(1, 9), "0.000000001");
        // i128::MIN 路径（u128 取绝对值）
        assert_eq!(
            format_decimal(i128::MIN, 0),
            "-170141183460469231731687303715884105728"
        );
    }

    // -- 解析 -----------------------------------------------------------------

    #[test]
    fn parse_ok() {
        assert_eq!(parse_decimal("123.45", m(5, 2)).unwrap(), 12345);
        assert_eq!(parse_decimal("+1.23", m(5, 2)).unwrap(), 123);
        assert_eq!(parse_decimal("-2.5", m(5, 0)).unwrap(), -3); // HALF_UP
        assert_eq!(parse_decimal("2.5", m(5, 0)).unwrap(), 3);
        assert_eq!(parse_decimal("0.125", m(5, 2)).unwrap(), 13); // 0.13
        assert_eq!(parse_decimal(".5", m(5, 1)).unwrap(), 5);
        assert_eq!(parse_decimal("-.5", m(5, 1)).unwrap(), -5);
        assert_eq!(parse_decimal("00123.450", m(8, 3)).unwrap(), 123450);
        assert_eq!(parse_decimal("1.999", m(4, 2)).unwrap(), 200); // 进位 2.00
        assert_eq!(parse_decimal("12345", m(5, 0)).unwrap(), 12345);
        assert_eq!(parse_decimal("0", m(1, 0)).unwrap(), 0);
        // 38 位边界
        let max38 = pow10(38) - 1;
        assert_eq!(
            parse_decimal("99999999999999999999999999999999999999", m(38, 0)).unwrap(),
            max38
        );
    }

    #[test]
    fn parse_rejects() {
        let meta = m(5, 2);
        for s in [
            "", ".", "+", "-", "abc", "1a", "1.2.3", "1e5", " 1.5", "1.5 ", "1_000",
        ] {
            assert!(parse_decimal(s, meta).is_err(), "should reject {s:?}");
        }
        assert!(parse_decimal("999999", m(5, 0)).is_err()); // 6 位 > 5
        assert!(parse_decimal("1000.00", m(5, 2)).is_err()); // 整数位 4 > 3
        // DECIMAL(38,38) 整数位为 0，任何非零整数都溢出
        assert!(
            parse_decimal("1.23456789012345678901234567890123456789012345", m(38, 38)).is_err()
        );
        assert!(parse_decimal("12", m(38, 38)).is_err()); // 整数位 2 + scale 38 > 38
    }

    #[test]
    fn parse_format_roundtrip() {
        for &(v, s) in &[
            (0i128, 0i8),
            (1, 0),
            (-1, 0),
            (123456789, 4),
            (-98765, 3),
            (5, 9),
        ] {
            let meta = DecimalMeta::try_new(
                (v.unsigned_abs().to_string().len() as u8)
                    .max(s as u8)
                    .min(38),
                s,
            )
            .unwrap();
            let text = format_decimal(v, s);
            assert_eq!(parse_decimal(&text, meta).unwrap(), v, "text={text}");
        }
    }

    /// ★ **运算结果的精度矩阵必须与 Spark 逐格一致** —— 2026-10-16 拿 `.14` 上的
    /// **真 Spark 3.5.2** 把这些格子全跑了一遍（`SELECT typeof(CAST(1 AS DECIMAL(p,s)) OP ...)`），
    /// 发现我们的 **除 / 取模 / avg 三条一直是错的**。
    ///
    /// 为什么这条测试格外要紧：**精度口径错 = 结算金额错**。而且这类错**不报错** ——
    /// 形状对、值也能算出来，只是**位数不对**（多截一位小数、或少一位整数），
    /// 只有拿另一个引擎逐格对才看得见。
    ///
    /// ⚠️ 下面每一行都是**实测值**，不是从文档抄的。改这张表之前先拿真 Spark 复核。
    #[test]
    fn spark_precision_matrix() {
        let d = |p: u8, s: i8| m(p, s);
        // (表达式, 期望)
        let cases: Vec<(&str, DecimalMeta, DecimalMeta)> = vec![
            // ── 加 / 减：Spark decimal(31,8) —— 我们本来就对
            (
                "(30,8)+(30,8)",
                d(31, 8),
                DecimalMeta::add_result(d(30, 8), d(30, 8)).unwrap(),
            ),
            (
                "(30,8)-(10,2)",
                d(31, 8),
                DecimalMeta::sub_result(d(30, 8), d(10, 2)).unwrap(),
            ),
            // ── 乘
            (
                "(10,2)*(10,2)",
                d(21, 4),
                DecimalMeta::mul_result(d(10, 2), d(10, 2)).unwrap(),
            ),
            (
                "(30,8)*(30,8)",
                d(38, 6),
                DecimalMeta::mul_result(d(30, 8), d(30, 8)).unwrap(),
            ),
            (
                "(25,5)*(25,5)",
                d(38, 6),
                DecimalMeta::mul_result(d(25, 5), d(25, 5)).unwrap(),
            ),
            (
                "(38,0)*(38,0)",
                d(38, 0),
                DecimalMeta::mul_result(d(38, 0), d(38, 0)).unwrap(),
            ),
            (
                "(30,20)*(30,20)",
                d(38, 17),
                DecimalMeta::mul_result(d(30, 20), d(30, 20)).unwrap(),
            ),
            (
                "(38,10)*(38,16)",
                d(38, 6),
                DecimalMeta::mul_result(d(38, 10), d(38, 16)).unwrap(),
            ),
            (
                "(38,8)*(38,8)",
                d(38, 6),
                DecimalMeta::mul_result(d(38, 8), d(38, 8)).unwrap(),
            ),
            // ── 除：**此前全错**
            (
                "(10,2)/(10,3)",
                d(24, 13),
                DecimalMeta::div_result(d(10, 2), d(10, 3)).unwrap(),
            ),
            (
                "(10,2)/(5,1)",
                d(17, 8),
                DecimalMeta::div_result(d(10, 2), d(5, 1)).unwrap(),
            ),
            (
                "(5,1)/(10,2)",
                d(18, 12),
                DecimalMeta::div_result(d(5, 1), d(10, 2)).unwrap(),
            ),
            (
                "(30,8)/(30,8)",
                d(38, 8),
                DecimalMeta::div_result(d(30, 8), d(30, 8)).unwrap(),
            ),
            // ── 取模：整数位是 **min**（此前写成 max）
            (
                "(30,8)%(10,2)",
                d(16, 8),
                DecimalMeta::mod_result(d(30, 8), d(10, 2)).unwrap(),
            ),
            (
                "(9,0)%(9,0)",
                d(9, 0),
                DecimalMeta::mod_result(d(9, 0), d(9, 0)).unwrap(),
            ),
        ];
        for (what, want, got) in cases {
            assert_eq!(
                (got.precision, got.scale),
                (want.precision, want.scale),
                "DEFIMAL(p,s) 口径与 Spark 不一致：{what}"
            );
        }
        // ── avg / sum（返回类型不是 Result）
        for (p, s, wp, ws) in [(30u8, 8i8, 34u8, 12i8), (10, 2, 14, 6), (38, 10, 38, 14)] {
            let g = DecimalMeta::avg_result(d(p, s));
            assert_eq!(
                (g.precision, g.scale),
                (wp, ws),
                "avg(({p},{s})) 与 Spark 不一致"
            );
        }
        let g = DecimalMeta::sum_result(d(30, 8));
        assert_eq!(
            (g.precision, g.scale),
            (38u8, 8i8),
            "sum((30,8)) 与 Spark 不一致"
        );
    }
}
