//! `COPY … TO` 的 **`PARTITION_BY` 词法桩**（F9 / A3）。
//!
//! 设计见 `提交事件订阅设计稿` §2.2。
//!
//! # 为什么要有这个模块
//!
//! sqlparser 0.63 的 `CopyOption` 枚举**没有** `PARTITION_BY`（实测：写成
//! `(FORMAT PARQUET, PARTITION_BY (dt))` 会在 `PARTITION_BY` 上直接报
//! `Expected: ), found: PARTITION_BY`）。而 `COPY … TO` 这个形状本身是对的
//! （DuckDB / Spark / Hive 都是它，也是本仓唯一的对标物）。
//!
//! ⇒ 在**送进 `Parser::parse_sql` 之前**把这一小段从句子里摘出来，让剩下的
//! 文本能被 sqlparser 正常解析。
//!
//! # ⚠️ 为什么用 tokenizer，不用文本 `find`
//!
//! 这正是 `params::normalize_placeholders` 已经踩过并被证明对的那条路。文本
//! `find("partition_by")` 在下面这句上会**静默给出错的分区列**：
//!
//! ```sql
//! COPY (SELECT id, 'partition_by' AS note FROM t) TO '/x' (FORMAT PARQUET)
//! ```
//!
//! （字符串字面量里那一份被当成子句了。）tokenizer 天然分得清字面量、带引号
//! 标识符、注释 —— 手写扫描器在这些边界上极容易漏。
//!
//! # ⚠️ 又不是"看到 `PARTITION_BY` 就认"
//!
//! `SELECT partition_by FROM t` 里那是**列名**。所以判据收紧成：
//! **`COPY` 打头 → 顶层 `TO` 之后 → 选项括号组内、处于选项级**的 `PARTITION_BY`。
//! 组内别处的同名 token（比如某个选项的括号里）一律不认。
//!
//! 摘掉之后剩下的文本仍然原样交给 sqlparser —— 本模块**只做这一件事**，
//! 分区列的语义校验（列在不在输出里、重不重名、能不能当目录名）在
//! `crate::export` 里做（那需要看到计划）。

use std::borrow::Cow;

use sqlparser::dialect::GenericDialect;
use sqlparser::keywords::Keyword;
use sqlparser::tokenizer::{Token, TokenWithSpan, Tokenizer};

use crate::error::{QsdbError, Result};

use super::params::byte_offset;

/// 从 `COPY … TO …` 文本里抽出 `PARTITION_BY (a, b)`，**并把它从文本中摘掉**。
///
/// 返回 `(可送进 sqlparser 的 SQL, Some(列名列表))`。
///
/// **没有这个子句时返回借用原文**（`Cow::Borrowed`）—— 绝大多数语句走这条路，
/// 代价只有一次 tokenize（`normalize_placeholders` 已经做过一次，量级相同）。
pub(crate) fn extract_partition_by(sql: &str) -> Result<(Cow<'_, str>, Option<Vec<String>>)> {
    let mut tk = Tokenizer::new(&GenericDialect {}, sql);
    let raw = tk
        .tokenize_with_location()
        .map_err(|e| QsdbError::parse_err(format!("SQL tokenize 失败：{e}")))?;
    // ⚠️ **`tokenize_with_location` 不跳过空白**（实测；解析器内部用的是另一条
    //    会跳过的路）。不去掉的话 `PARTITION_BY` 后面跟的是 `Whitespace` 而不是
    //    `(` —— 第一版就栽在这上面，十二条用例红了六条。
    //    去掉之后 token 下标才等于"下一个语法单元"，相邻判断与字节区间才对得上。
    let tokens: Vec<TokenWithSpan> = raw
        .into_iter()
        .filter(|t| !matches!(t.token, Token::Whitespace(_)))
        .collect();

    // ① 只认 `COPY` 打头的语句。其余一律原样放行（本模块不该影响任何别的语句）。
    match tokens.first() {
        Some(t) if is_word(&t.token, "COPY") => {}
        _ => return Ok((Cow::Borrowed(sql), None)),
    }

    // ② 找**顶层**的 `TO` / `FROM`。
    //
    // `COPY (SELECT …) TO '…'` 里 SELECT 的那些括号都在 depth ≥ 1，所以顶层 `TO`
    // 一定在源子句之后 —— 不需要额外判断"这个 TO 是不是 SELECT 里的"。
    //
    // `FROM` 也在这里判：那是**导入**方向（`COPY t FROM '…'`），本轮不做。
    // 这里只是"没有 PARTITION_BY 可取"，报错留给翻译层（那儿才知道上下文）。
    //
    // ⚠️ `skip(1)` 跳过开头那个 `COPY` 自己 —— `COPY` 也是个顶层 Word，
    //    第一版漏了这一步，于是循环在它身上就 `break` 了（全部用例当场红）。
    // ⚠️ 顶层的**别的** Word（表名 `COPY t TO …` 里那个 `t`）**不能 break**，
    //    只有 `TO`/`FROM` 才是分界。
    let mut depth: i32 = 0;
    let mut to_idx: Option<usize> = None;
    let mut saw_from = false;
    for (i, t) in tokens.iter().enumerate().skip(1) {
        match &t.token {
            Token::LParen => depth += 1,
            Token::RParen => depth -= 1,
            Token::Word(w) if depth == 0 => {
                if w.keyword == Keyword::TO {
                    to_idx = Some(i);
                    break;
                }
                if w.keyword == Keyword::FROM {
                    saw_from = true;
                    break;
                }
            }
            _ => {}
        }
    }
    let Some(to_idx) = to_idx else {
        let _ = saw_from; // 导入方向：本模块不管，留给翻译层报"不支持 COPY … FROM"
        return Ok((Cow::Borrowed(sql), None));
    };

    // ③ `TO <目标>` 之后**第一个顶层的 `(`** = 选项组。
    //    没有它 ⇒ 没有选项 ⇒ 更不可能有 PARTITION_BY。
    let mut depth: i32 = 0;
    let mut open = None;
    for (i, t) in tokens.iter().enumerate().skip(to_idx + 1) {
        match &t.token {
            Token::LParen => {
                if depth == 0 {
                    open = Some(i);
                    break;
                }
                depth += 1;
            }
            Token::RParen => depth -= 1,
            _ => {}
        }
    }
    let Some(open) = open else {
        return Ok((Cow::Borrowed(sql), None));
    };

    // ④ 在选项组内（相对语句 depth == 1）找 `PARTITION_BY`。
    //    进到更深一层的同名 token 不认（那不是选项）。
    let mut depth: i32 = 1;
    let mut pb: Option<usize> = None;
    let mut split_form: Option<usize> = None;
    let mut i = open + 1;
    while i < tokens.len() {
        match &tokens[i].token {
            Token::RParen => {
                depth -= 1;
                if depth == 0 {
                    break; // 选项组结束
                }
            }
            Token::LParen => depth += 1,
            Token::Word(w) if depth == 1 && w.quote_style.is_none() => {
                if w.value.eq_ignore_ascii_case("PARTITION_BY") {
                    pb = Some(i);
                    break;
                }
                // `PARTITION BY`（空格）是窗口函数的写法、不是这里的语法。
                // 单独认出来**只为把报错说清楚** —— 否则用户看到的是
                // sqlparser 在 `PARTITION` 上报的 `Expected: ), found: PARTITION`。
                if w.keyword == Keyword::PARTITION
                    && matches!(
                        tokens.get(i + 1).map(|t| &t.token),
                        Some(Token::Word(b)) if b.keyword == Keyword::BY
                    )
                {
                    split_form = Some(i);
                    break;
                }
            }
            _ => {}
        }
        i += 1;
    }
    if split_form.is_some() {
        return Err(QsdbError::parse_err(
            "COPY … TO 的分区子句要写成 `PARTITION_BY (列, …)`（**下划线**）；\
             `PARTITION BY` 是窗口函数的写法，不是这里的",
        ));
    }
    let Some(w) = pb else {
        return Ok((Cow::Borrowed(sql), None));
    };

    // ⑤ 紧跟的必须是 `(` + 逗号分隔的**列名** + `)`。
    match tokens.get(w + 1).map(|t| &t.token) {
        Some(Token::LParen) => {}
        _ => {
            return Err(QsdbError::parse_err(
                "`PARTITION_BY` 后面要跟列清单：`PARTITION_BY (dt, region)`",
            ));
        }
    }
    let (cols, close) = parse_col_list(&tokens, w + 1)?;

    // ⑥ 算出要从原文里摘掉的**字节区间**。
    //
    // 用"下一个 token 的起点"当上界（而不是自己算 token 长度）：吃掉的只是
    // token 之间的空白，无副作用，且不必分别处理 `Word` / `Comma` / `)` 的长度。
    let start_of = |i: usize| -> Result<usize> {
        let t = &tokens[i];
        byte_offset(sql, t.span.start.line, t.span.start.column)
    };
    let end_of = |i: usize| -> Result<usize> {
        match i + 1 {
            n if n < tokens.len() => start_of(n),
            _ => Ok(sql.len()),
        }
    };
    // ⚠️ 判"唯一选项"要**两条都成立**：它前面紧挨着选项组的 `(` **且**它后面
    //    紧挨着选项组的 `)`。只判前半条的话，`(PARTITION_BY (dt), FORMAT …)`
    //    会被误当成唯一选项、把后面的 FORMAT 一起摘掉（实测就是这么红的）。
    let is_first = w - 1 == open;
    let is_last = tokens.get(close + 1).map(|t| &t.token) == Some(&Token::RParen);
    let (from_tok, to_tok) = if is_first && is_last {
        // ⑥a **唯一选项**：连选项括号一起去。
        //     只摘 `PARTITION_BY (dt)` 会留下 `()`，而 sqlparser 的
        //     `parse_comma_separated` 在空括号上会报错 —— 那是个"我们造的"
        //     假错误，绝不能留给用户。
        //     顺带把紧邻的 `WITH` 也带上（`… TO 'x' WITH (...)` 摘完剩 `WITH`
        //     虽然能解析，但留着没有意义）。
        let start = match open.checked_sub(1).and_then(|p| tokens.get(p)) {
            Some(t) => match &t.token {
                Token::Word(k) if k.keyword == Keyword::WITH => open - 1,
                _ => open,
            },
            None => open,
        };
        (start, close + 1) // close+1 = 选项组的 `)`
    } else if tokens.get(w - 1).map(|t| &t.token) == Some(&Token::Comma) {
        // ⑥b 前面有逗号 ⇒ 从逗号起（否则留下 `(FORMAT PARQUET, )`）
        (w - 1, close)
    } else if tokens.get(close + 1).map(|t| &t.token) == Some(&Token::Comma) {
        // ⑥c 后面有逗号 ⇒ 连它一起（否则留下 `(, FORMAT PARQUET)`）
        (w, close + 1)
    } else {
        // ⑥d 兜底：两侧都没有分隔符。正常语法下到不了这里，但**不许猜**。
        (w, close)
    };
    // 防御：区间必须真的有序，否则 `replace_range` 会 panic。
    if from_tok >= to_tok {
        return Err(QsdbError::parse_err(
            "`PARTITION_BY` 的文本区间异常（内部错误）",
        ));
    }
    let (mut a, mut b) = (start_of(from_tok)?, end_of(to_tok)?);
    if a > b {
        (a, b) = (b, a);
    }

    let mut out = String::with_capacity(sql.len());
    out.push_str(&sql[..a]);
    out.push_str(&sql[b..]);
    Ok((Cow::Owned(out), Some(cols)))
}

/// 解析 `( a, b, c )`：只接受裸列名，**拒绝一切别的东西**（宁可报错，不猜）。
///
/// 返回 `(列名, 右括号的 token 下标)` —— 下标是**数出来的、不是算出来的**
/// （`列数 × 2 + 1` 这种算式在畸形输入上会指到别处，然后 `replace_range` 就
/// 摘错了字节区间）。
fn parse_col_list(tokens: &[TokenWithSpan], lp: usize) -> Result<(Vec<String>, usize)> {
    let mut cols: Vec<String> = Vec::new();
    let mut want_ident = true;
    let mut i = lp + 1;
    let close = loop {
        let Some(t) = tokens.get(i) else {
            return Err(QsdbError::parse_err("`PARTITION_BY (…)` 的括号没有闭合"));
        };
        match &t.token {
            Token::RParen => break i,
            Token::Comma => {
                if want_ident {
                    return Err(QsdbError::parse_err(
                        "`PARTITION_BY (…)` 里有空的一项（多写了一个逗号？）",
                    ));
                }
                want_ident = true;
            }
            Token::Word(w) => {
                if !want_ident {
                    return Err(QsdbError::parse_err(format!(
                        "`PARTITION_BY (…)` 里 `{}` 前面少了逗号",
                        w.value
                    )));
                }
                if w.quote_style.is_some() {
                    return Err(QsdbError::parse_err(format!(
                        "`PARTITION_BY (…)` 里不接受带引号的列名 `{}`",
                        w.value
                    )));
                }
                cols.push(w.value.clone());
                want_ident = false;
            }
            other => {
                return Err(QsdbError::parse_err(format!(
                    "`PARTITION_BY (…)` 里只接受列名，遇到 `{other}`"
                )));
            }
        }
        i += 1;
    };
    if want_ident {
        return Err(QsdbError::parse_err(
            "`PARTITION_BY (…)` 里最后一项后面多了个逗号",
        ));
    }
    if cols.is_empty() {
        return Err(QsdbError::parse_err(
            "`PARTITION_BY ()` 是空的 —— 要么写列名，要么整个子句去掉",
        ));
    }
    // 重名：`PARTITION_BY (dt, dt)` 建出来的目录是 `dt=a/dt=a`，读回来两列都叫 dt。
    // 这种"写得出、读不对"的形态一律当场拒。
    let mut seen = std::collections::HashSet::new();
    if let Some(dup) = cols.iter().find(|c| !seen.insert(c.to_ascii_lowercase())) {
        return Err(QsdbError::parse_err(format!(
            "`PARTITION_BY (…)` 里 `{dup}` 出现了两次"
        )));
    }
    Ok((cols, close))
}

fn is_word(t: &Token, kw: &str) -> bool {
    matches!(t, Token::Word(w) if w.value.eq_ignore_ascii_case(kw))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pb(sql: &str) -> (String, Option<Vec<String>>) {
        let (s, v) = extract_partition_by(sql).unwrap();
        (s.into_owned(), v)
    }

    /// ★ 本模块存在的理由：sqlparser **自己解析不了** `PARTITION_BY`。
    ///    这条断言把"为什么要写这个桩"钉死在测试里 —— 将来 sqlparser 支持了它，
    ///    这条会红，那时才知道可以删掉本模块。
    #[test]
    fn sqlparser_自己解析不了_partition_by() {
        let raw = "COPY (SELECT 1) TO '/tmp/x' (FORMAT PARQUET, PARTITION_BY (dt))";
        let e = sqlparser::parser::Parser::parse_sql(&GenericDialect {}, raw);
        assert!(
            e.is_err(),
            "sqlparser 竟然能解析 PARTITION_BY 了 —— 本模块可以删了：{e:?}"
        );
    }

    #[test]
    fn 摘掉之后_sqlparser_能解析() {
        let raw = "COPY (SELECT 1) TO '/tmp/x' (FORMAT PARQUET, PARTITION_BY (dt, region))";
        let (s, cols) = pb(raw);
        assert_eq!(cols, Some(vec!["dt".into(), "region".into()]));
        assert!(!s.contains("partition_by") && !s.to_lowercase().contains("partition_by"));
        let ast = sqlparser::parser::Parser::parse_sql(&GenericDialect {}, &s);
        assert!(ast.is_ok(), "摘完还得能解析：{s:?} → {ast:?}");
    }

    #[test]
    fn 没有这个子句时零改动() {
        let raw = "COPY (SELECT 1) TO '/tmp/x' (FORMAT PARQUET)";
        let (s, cols) = extract_partition_by(raw).unwrap();
        assert!(cols.is_none());
        assert!(matches!(s, Cow::Borrowed(_)), "应当是借用（零成本那条路）");
    }

    #[test]
    fn 唯一选项时连括号一起去() {
        let (s, cols) = pb("COPY (SELECT 1) TO '/x' (PARTITION_BY (dt))");
        assert_eq!(cols, Some(vec!["dt".into()]));
        assert!(
            !s.contains("()"),
            "留下空括号会让 sqlparser 报假错误：{s:?}"
        );
        assert!(sqlparser::parser::Parser::parse_sql(&GenericDialect {}, &s).is_ok());
    }

    #[test]
    fn 唯一选项且带_with() {
        let (s, cols) = pb("COPY (SELECT 1) TO '/x' WITH (PARTITION_BY (dt))");
        assert_eq!(cols, Some(vec!["dt".into()]));
        assert!(
            !s.to_lowercase().contains("with"),
            "WITH 也该一并摘掉：{s:?}"
        );
        assert!(sqlparser::parser::Parser::parse_sql(&GenericDialect {}, &s).is_ok());
    }

    #[test]
    fn 分区子句写在前面() {
        let (s, cols) = pb("COPY (SELECT 1) TO '/x' (PARTITION_BY (dt), FORMAT PARQUET)");
        assert_eq!(cols, Some(vec!["dt".into()]));
        assert!(sqlparser::parser::Parser::parse_sql(&GenericDialect {}, &s).is_ok());
    }

    /// ★ 这条是"用 tokenizer 而不是文本 find"的全部理由。
    #[test]
    fn 字符串字面量里的同名不认() {
        let raw = "COPY (SELECT 'partition_by (x)' AS s) TO '/x' (FORMAT PARQUET)";
        let (s, cols) = extract_partition_by(raw).unwrap();
        assert!(cols.is_none(), "那是字符串内容，不是子句");
        assert!(matches!(s, Cow::Borrowed(_)));
    }

    /// 列名叫 `partition_by` 也不该被当成子句（它在 SELECT 里，depth ≥ 1）。
    #[test]
    fn select_里的同名列不认() {
        let raw = "COPY (SELECT partition_by FROM t) TO '/x' (FORMAT PARQUET)";
        let (s, cols) = extract_partition_by(raw).unwrap();
        assert!(cols.is_none());
        assert!(matches!(s, Cow::Borrowed(_)));
    }

    #[test]
    fn 带引号的同名列不认() {
        let raw = "COPY (SELECT 1) TO '/x' (\"PARTITION_BY\" (dt))";
        let (s, cols) = extract_partition_by(raw).unwrap();
        assert!(cols.is_none(), "带引号的是标识符，不是关键字");
        assert!(matches!(s, Cow::Borrowed(_)));
    }

    #[test]
    fn 大小写不敏感() {
        let (_, cols) = pb("copy (select 1) to '/x' (format parquet, partition_by (dt))");
        assert_eq!(cols, Some(vec!["dt".into()]));
    }

    /// `PARTITION BY`（空格）是窗口函数的写法 —— 报错要**说清是哪个**。
    #[test]
    fn 空格写法报错要指路() {
        let e = extract_partition_by("COPY (SELECT 1) TO '/x' (FORMAT PARQUET, PARTITION BY (dt))")
            .unwrap_err();
        let m = e.to_string();
        assert!(m.contains("PARTITION_BY") && m.contains("下划线"), "{m}");
    }

    #[test]
    fn from_方向不抽() {
        let (_, cols) = extract_partition_by("COPY t FROM '/x' (FORMAT PARQUET)").unwrap();
        assert!(cols.is_none(), "FROM 是导入方向，本轮不做");
    }

    #[test]
    fn 各种畸形都报错不猜() {
        for bad in [
            "COPY (SELECT 1) TO '/x' (PARTITION_BY)",
            "COPY (SELECT 1) TO '/x' (PARTITION_BY ())",
            "COPY (SELECT 1) TO '/x' (PARTITION_BY (dt,))",
            "COPY (SELECT 1) TO '/x' (PARTITION_BY (dt dt))",
            "COPY (SELECT 1) TO '/x' (PARTITION_BY (dt, dt))",
            "COPY (SELECT 1) TO '/x' (PARTITION_BY (dt + 1))",
        ] {
            assert!(extract_partition_by(bad).is_err(), "这条该报错：{bad}");
        }
    }
}
