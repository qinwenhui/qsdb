//! 时间类型算法（无外部时间库依赖）。
//!
//! 内部表示（对齐设计文档 §3.4）：
//! - `DATE`：i32，自 1970-01-01 起的天数，可表示 0001-01-01 ~ 9999-12-31
//! - `TIME`：i64，自午夜起的纳秒，`[0, 86_400_000_000_000)`
//! - `TIMESTAMP`：i64，自 1970-01-01T00:00:00 起的纳秒（naive，UTC 语义）
//!
//! 文本格式约束：
//! - 输入：ISO 8601（`2026-10-04`、`14:30:00.123`、`2026-10-04T14:30:00.123Z`，
//!   日期时间分隔符允许 `T` 或空格，尾缀 `Z` 接受并视为 naive UTC）
//! - 输出：`YYYY-MM-DD`、`HH:MM:SS[.fff|.ffffff|.fffffffff]`、
//!   `YYYY-MM-DD HH:MM:SS[.fff|...]`（小数部分按 3/6/9 位分组省略尾零）

use crate::error::{QsdbError, Result};

pub const NANOS_PER_SEC: i64 = 1_000_000_000;
pub const NANOS_PER_MIN: i64 = 60 * NANOS_PER_SEC;
pub const NANOS_PER_HOUR: i64 = 60 * NANOS_PER_MIN;
pub const NANOS_PER_DAY: i64 = 24 * NANOS_PER_HOUR;

/// 0001-01-01
pub const MIN_DATE_DAYS: i32 = -719_162;
/// 9999-12-31
pub const MAX_DATE_DAYS: i32 = 2_932_896;

#[inline]
pub fn is_leap_year(y: i32) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

/// 月份天数；`m` 非法时返回 0（由校验层拒绝）。
#[inline]
pub fn days_in_month(y: i32, m: u32) -> u32 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(y) => 29,
        2 => 28,
        _ => 0,
    }
}

/// 校验公历日期（年份限于 0001~9999）。
pub fn is_valid_date(y: i32, m: u32, d: u32) -> bool {
    (1..=9999).contains(&y) && (1..=12).contains(&m) && (1..=days_in_month(y, m)).contains(&d)
}

/// Howard Hinnant 的 days_from_civil 算法（i64 版本，供 timestamp 格式化使用）。
pub fn days_from_civil_i64(y: i64, m: u32, d: u32) -> i64 {
    let m0 = m as i64;
    let y0 = if m0 <= 2 { y - 1 } else { y };
    let era = if y0 >= 0 { y0 } else { y0 - 399 } / 400;
    let yoe = y0 - era * 400;
    let doy = (153 * (if m0 > 2 { m0 - 3 } else { m0 + 9 }) + 2) / 5 + i64::from(d) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// Howard Hinnant 的 civil_from_days 算法（i64 版本）。
pub fn civil_from_days_i64(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// `DATE` 内部值：自 epoch 的天数（配合 [`is_valid_date`] 保证的合法日期）。
pub fn days_from_civil(y: i32, m: u32, d: u32) -> i32 {
    days_from_civil_i64(i64::from(y), m, d) as i32
}

/// `DATE` 内部值 → (年, 月, 日)。
pub fn civil_from_days(days: i32) -> (i32, u32, u32) {
    let (y, m, d) = civil_from_days_i64(i64::from(days));
    (y as i32, m, d)
}

// ---------------------------------------------------------------------------
// DATE
// ---------------------------------------------------------------------------

/// 解析 `YYYY-MM-DD` → 天数。
pub fn parse_date(s: &str) -> Result<i32> {
    let invalid = || QsdbError::parse_err(format!("invalid DATE '{s}', expected 'YYYY-MM-DD'"));
    let b = s.as_bytes();
    if b.len() != 10 || b[4] != b'-' || b[7] != b'-' {
        return Err(invalid());
    }
    let y = digits(s, 0, 4).ok_or_else(invalid)? as i32;
    let m = digits(s, 5, 2).ok_or_else(invalid)? as u32;
    let d = digits(s, 8, 2).ok_or_else(invalid)? as u32;
    if !is_valid_date(y, m, d) {
        return Err(invalid());
    }
    Ok(days_from_civil(y, m, d))
}

pub fn format_date(days: i32) -> String {
    let (y, m, d) = civil_from_days(days);
    format!("{y:04}-{m:02}-{d:02}")
}

// ---------------------------------------------------------------------------
// TIME
// ---------------------------------------------------------------------------

/// 解析 `HH:MM[:SS[.f{1,9}]]` → 自午夜纳秒。
pub fn parse_time_ns(s: &str) -> Result<i64> {
    let invalid = || QsdbError::parse_err(format!("invalid TIME '{s}'"));
    let (hms, frac) = match s.split_once('.') {
        Some((a, b)) => (a, Some(b)),
        None => (s, None),
    };
    let parts: Vec<&str> = hms.split(':').collect();
    if parts.len() != 2 && parts.len() != 3 {
        return Err(invalid());
    }
    let h = parse_field(parts[0])
        .filter(|v| *v < 24)
        .ok_or_else(invalid)?;
    let m = parse_field(parts[1])
        .filter(|v| *v < 60)
        .ok_or_else(invalid)?;
    let (sec, nanos) = if parts.len() == 3 {
        let sec = parse_field(parts[2])
            .filter(|v| *v < 60)
            .ok_or_else(invalid)?;
        let ns = match frac {
            None => 0,
            Some(f) if (1..=9).contains(&f.len()) && f.bytes().all(|b| b.is_ascii_digit()) => {
                digits(f, 0, f.len()).ok_or_else(invalid)? * pow10(9 - f.len())
            }
            Some(_) => return Err(invalid()),
        };
        (sec, ns)
    } else if frac.is_some() {
        return Err(invalid());
    } else {
        (0, 0)
    };
    Ok(h * NANOS_PER_HOUR + m * NANOS_PER_MIN + sec * NANOS_PER_SEC + nanos)
}

/// 自午夜纳秒 → `HH:MM:SS[.fff]`。超范围值按一天取模（保证全函数）。
pub fn format_time_ns(mut ns: i64) -> String {
    ns = ns.rem_euclid(NANOS_PER_DAY);
    let h = ns / NANOS_PER_HOUR;
    let m = (ns % NANOS_PER_HOUR) / NANOS_PER_MIN;
    let sec = (ns % NANOS_PER_MIN) / NANOS_PER_SEC;
    let frac = ns % NANOS_PER_SEC;
    format!("{h:02}:{m:02}:{sec:02}{}", format_frac(frac))
}

// ---------------------------------------------------------------------------
// TIMESTAMP
// ---------------------------------------------------------------------------

/// 解析 `YYYY-MM-DD[T| HH:MM[:SS[.f{1,9}]]][Z]` → 自 epoch 纳秒。
///
/// 纯日期等价于当天 00:00:00。结果必须落在 i64 纳秒可表示范围内
/// （约 1677-09-21 ~ 2262-04-11），否则报错。
pub fn parse_timestamp_ns(s: &str) -> Result<i64> {
    let invalid = || QsdbError::parse_err(format!("invalid TIMESTAMP '{s}'"));
    if s.len() < 10 {
        return Err(invalid());
    }
    let date_part = &s[..10];
    let days = parse_date(date_part).map_err(|_| invalid())?;
    let rest = &s[10..];
    let time_ns = if rest.is_empty() || rest == "Z" {
        0
    } else {
        let body = rest.strip_prefix(['T', ' ']).ok_or_else(invalid)?;
        let body = body.strip_suffix('Z').unwrap_or(body);
        parse_time_ns(body).map_err(|_| invalid())?
    };
    i64::from(days)
        .checked_mul(NANOS_PER_DAY)
        .and_then(|d| d.checked_add(time_ns))
        .ok_or_else(|| {
            QsdbError::parse_err(format!(
                "TIMESTAMP '{s}' out of range (max 2262-04-11 at nanosecond precision)"
            ))
        })
}

/// 自 epoch 纳秒 → `YYYY-MM-DD HH:MM:SS[.fff]`（naive UTC）。
pub fn format_timestamp_ns(ns: i64) -> String {
    let days = ns.div_euclid(NANOS_PER_DAY);
    let rem = ns.rem_euclid(NANOS_PER_DAY);
    let h = rem / NANOS_PER_HOUR;
    let m = (rem % NANOS_PER_HOUR) / NANOS_PER_MIN;
    let sec = (rem % NANOS_PER_MIN) / NANOS_PER_SEC;
    let frac = rem % NANOS_PER_SEC;
    let (y, mo, d) = civil_from_days_i64(days);
    format!(
        "{y:04}-{mo:02}-{d:02} {h:02}:{m:02}:{sec:02}{}",
        format_frac(frac)
    )
}

// ---------------------------------------------------------------------------
// 内部工具
// ---------------------------------------------------------------------------

/// 小数部分：0 → 空；按 3/6/9 位分组省略尾零。
fn format_frac(nanos: i64) -> String {
    debug_assert!((0..NANOS_PER_SEC).contains(&nanos));
    if nanos == 0 {
        String::new()
    } else if nanos % 1_000_000 == 0 {
        format!(".{:03}", nanos / 1_000_000)
    } else if nanos % 1_000 == 0 {
        format!(".{:06}", nanos / 1_000)
    } else {
        format!(".{nanos:09}")
    }
}

/// 从 `s[start..start+len)` 解析非负 ASCII 数字（全数字才通过）。
fn digits(s: &str, start: usize, len: usize) -> Option<i64> {
    let sub = s.get(start..start + len)?;
    if !sub.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    sub.parse().ok()
}

/// 解析时间字段（严格两位数字）。
fn parse_field(s: &str) -> Option<i64> {
    if s.len() != 2 || !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.parse().ok()
}

/// 10^n（n ≤ 9），时间小数位补零用。
fn pow10(n: usize) -> i64 {
    const P: [i64; 10] = [
        1,
        10,
        100,
        1_000,
        10_000,
        100_000,
        1_000_000,
        10_000_000,
        100_000_000,
        1_000_000_000,
    ];
    P[n]
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- civil 算法 -----------------------------------------------------------

    #[test]
    fn epoch_anchors() {
        assert_eq!(days_from_civil(1970, 1, 1), 0);
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(days_from_civil(2000, 1, 1), 10_957);
        assert_eq!(civil_from_days(10_957), (2000, 1, 1));
        assert_eq!(days_from_civil(1, 1, 1), MIN_DATE_DAYS);
        assert_eq!(days_from_civil(9999, 12, 31), MAX_DATE_DAYS);
        assert_eq!(civil_from_days(MIN_DATE_DAYS), (1, 1, 1));
        assert_eq!(civil_from_days(MAX_DATE_DAYS), (9999, 12, 31));
        // 设计文档当日
        assert_eq!(days_from_civil(2026, 10, 9), 20_735);
    }

    #[test]
    fn leap_years() {
        assert!(is_leap_year(2000));
        assert!(is_leap_year(2024));
        assert!(!is_leap_year(1900));
        assert!(!is_leap_year(2023));
        assert_eq!(days_in_month(2024, 2), 29);
        assert_eq!(days_in_month(2023, 2), 28);
        assert_eq!(days_in_month(1900, 2), 28);
    }

    #[test]
    fn civil_roundtrip_samples() {
        let samples = [
            (1, 1, 1),
            (1, 12, 31),
            (1582, 10, 15),
            (1900, 2, 28),
            (1970, 12, 31),
            (2000, 2, 29),
            (2024, 2, 29),
            (2026, 10, 9),
            (9999, 12, 31),
        ];
        for &(y, m, d) in &samples {
            let z = days_from_civil(y, m, d);
            assert_eq!(civil_from_days(z), (y, m, d), "{y:04}-{m:02}-{d:02}");
        }
        // 连续往返：1969-12-30 ~ 1970-01-03
        for z in -2..=3 {
            let (y, m, d) = civil_from_days(z);
            assert_eq!(days_from_civil(y, m, d), z);
        }
    }

    // -- DATE -----------------------------------------------------------------

    #[test]
    fn parse_date_ok() {
        assert_eq!(parse_date("1970-01-01").unwrap(), 0);
        assert_eq!(parse_date("2026-10-04").unwrap(), 20_730);
        assert_eq!(parse_date("0001-01-01").unwrap(), MIN_DATE_DAYS);
        assert_eq!(parse_date("9999-12-31").unwrap(), MAX_DATE_DAYS);
        assert_eq!(
            parse_date("2024-02-29").unwrap(),
            days_from_civil(2024, 2, 29)
        );
    }

    #[test]
    fn parse_date_rejects() {
        for s in [
            "2023-02-29",
            "1900-02-29",
            "2026-13-01",
            "2026-00-10",
            "2026-10-00",
            "2026-10-32",
            "2026-1-09",
            "2026/10/09",
            "2026-10-04T00",
            "",
            "10000-01-01",
            "0000-01-01",
        ] {
            assert!(parse_date(s).is_err(), "should reject {s:?}");
        }
    }

    #[test]
    fn format_date_roundtrip() {
        assert_eq!(format_date(0), "1970-01-01");
        assert_eq!(format_date(20_730), "2026-10-04");
        assert_eq!(format_date(MIN_DATE_DAYS), "0001-01-01");
        assert_eq!(format_date(MAX_DATE_DAYS), "9999-12-31");
        for days in [MIN_DATE_DAYS, -1, 0, 1, 10_957, 20_730, MAX_DATE_DAYS] {
            assert_eq!(parse_date(&format_date(days)).unwrap(), days);
        }
    }

    // -- TIME -----------------------------------------------------------------

    #[test]
    fn parse_time_ok() {
        assert_eq!(parse_time_ns("00:00:00").unwrap(), 0);
        assert_eq!(
            parse_time_ns("23:59:59.999999999").unwrap(),
            NANOS_PER_DAY - 1
        );
        assert_eq!(
            parse_time_ns("12:30").unwrap(),
            12 * NANOS_PER_HOUR + 30 * NANOS_PER_MIN
        );
        assert_eq!(
            parse_time_ns("12:30:45.123").unwrap(),
            12 * NANOS_PER_HOUR + 30 * NANOS_PER_MIN + 45 * NANOS_PER_SEC + 123_000_000
        );
        assert_eq!(parse_time_ns("00:00:00.000000001").unwrap(), 1);
        // 1~9 位小数均合法，右侧补零对齐纳秒
        assert_eq!(
            parse_time_ns("12:30:45.1234").unwrap(),
            12 * NANOS_PER_HOUR + 30 * NANOS_PER_MIN + 45 * NANOS_PER_SEC + 123_400_000
        );
        assert_eq!(parse_time_ns("00:00:00.000000010").unwrap(), 10);
    }

    #[test]
    fn parse_time_rejects() {
        for s in [
            "24:00:00",
            "12:60",
            "12:30:60",
            "1:2:3",
            "12:30:45.",
            "12:30:45.1234567890",
            "12345678901",
            "",
            "12",
            "ab:cd:ef",
        ] {
            assert!(parse_time_ns(s).is_err(), "should reject {s:?}");
        }
    }

    #[test]
    fn format_time_matches() {
        assert_eq!(format_time_ns(0), "00:00:00");
        assert_eq!(format_time_ns(NANOS_PER_DAY - 1), "23:59:59.999999999");
        assert_eq!(
            format_time_ns(
                12 * NANOS_PER_HOUR + 30 * NANOS_PER_MIN + 45 * NANOS_PER_SEC + 123_000_000
            ),
            "12:30:45.123"
        );
        assert_eq!(format_time_ns(500_000_000), "00:00:00.500");
        assert_eq!(format_time_ns(123_400_000), "00:00:00.123400");
        // 负数按一天取模
        assert_eq!(format_time_ns(-1), "23:59:59.999999999");
    }

    // -- TIMESTAMP ------------------------------------------------------------

    #[test]
    fn parse_timestamp_ok() {
        assert_eq!(parse_timestamp_ns("1970-01-01T00:00:00").unwrap(), 0);
        assert_eq!(parse_timestamp_ns("1970-01-01 00:00:00").unwrap(), 0);
        assert_eq!(parse_timestamp_ns("1970-01-01").unwrap(), 0);
        assert_eq!(
            parse_timestamp_ns("2026-10-04 14:30:00").unwrap(),
            20_730 * NANOS_PER_DAY + 14 * NANOS_PER_HOUR + 30 * NANOS_PER_MIN
        );
        assert_eq!(
            parse_timestamp_ns("2026-10-04T14:30:00.123Z").unwrap(),
            20_730 * NANOS_PER_DAY + 14 * NANOS_PER_HOUR + 30 * NANOS_PER_MIN + 123_000_000
        );
        // i64 纳秒可表示上界附近
        assert!(parse_timestamp_ns("2262-04-11T23:47:16.854775807").is_ok());
    }

    #[test]
    fn parse_timestamp_rejects() {
        for s in [
            "",
            "2026-10-04X12:00:00",
            "2026-10-04 25:00:00",
            "9999-12-31T00:00:00",
            "2026-10-04T12:00:00.1234567890",
        ] {
            assert!(parse_timestamp_ns(s).is_err(), "should reject {s:?}");
        }
    }

    #[test]
    fn format_timestamp_matches() {
        assert_eq!(format_timestamp_ns(0), "1970-01-01 00:00:00");
        assert_eq!(format_timestamp_ns(-1), "1969-12-31 23:59:59.999999999");
        assert_eq!(
            format_timestamp_ns(20_730 * NANOS_PER_DAY + 14 * NANOS_PER_HOUR + 123_456_789),
            "2026-10-04 14:00:00.123456789"
        );
        assert_eq!(
            format_timestamp_ns(20_730 * NANOS_PER_DAY + 500_000_000),
            "2026-10-04 00:00:00.500"
        );
        let ns = 20_730 * NANOS_PER_DAY + 14 * NANOS_PER_HOUR + 30 * NANOS_PER_MIN + 7;
        let s = format_timestamp_ns(ns);
        assert_eq!(parse_timestamp_ns(&s).unwrap(), ns);
    }
}
