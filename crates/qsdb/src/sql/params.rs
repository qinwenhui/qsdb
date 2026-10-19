//! 参数化查询：**值从不进入 SQL 文本**。
//!
//! 设计见 `参数化查询设计稿`。三句话概括：
//! - **绑在内核的绑定器里**（不是驱动、也不是 HTTP 层）—— 只有它同时握着 AST 与
//!   类型系统。驱动侧拼串 = 注入且无类型检查；HTTP 层替换 = 注入，且
//!   `'2026-01-01'` 该是 DATE 还是 Varchar 无从判断。
//! - 值的落点是 **AST 里的字面量节点**（`PhysExpr::Lit`），所以下游（类型检查、
//!   常量折叠、下推、溢出）**一个字都不用改**。
//! - 占位符两份写法：`$1…`（显式、可重复引用）与 `?`（按源码顺序），**不许混用**。

use std::cell::RefCell;

use crate::error::{QsdbError, Result};
use crate::types::{Literal, LogicalType};

thread_local! {
    /// 当前语句的参数（值已**类型化**）。
    ///
    /// 绑定发生在查询自己的线程上（`run_isolated` 之内），嵌套子查询共享同一份
    /// —— 本来就该共享（同一条语句的参数）。
    static PARAMS: RefCell<Vec<Literal>> = const { RefCell::new(Vec::new()) };
}

/// 参数作用域（RAII）：`new` 装上、`Drop` 还原。
///
/// 为什么用作用域而不是把参数穿下去：绑定器是"一路 `&Expr` 往下走"的，再穿一层
/// 要动 `translate.rs` 里十几处 `SchemaCtx` 构造点；而参数是**整条语句一份**的
/// 上下文 —— 作用域语义正好。写法照抄本仓已有的 `CteScope`（thread-local + RAII）。
#[must_use = "作用域必须活着，否则绑定期看不到参数"]
pub struct ParamScope {
    prev: Vec<Literal>,
}

impl ParamScope {
    pub fn new(values: Vec<Literal>) -> Self {
        Self {
            prev: PARAMS.with(|p| p.replace(values)),
        }
    }
}

impl Drop for ParamScope {
    fn drop(&mut self) {
        PARAMS.with(|p| {
            *p.borrow_mut() = std::mem::take(&mut self.prev);
        });
    }
}

/// 取第 `idx` 个参数（1-based）。**值在这里变成字面量** —— 整个设计的安全支点。
pub(crate) fn lookup(idx: usize) -> Result<Literal> {
    let v = PARAMS.with(|p| p.borrow().get(idx - 1).cloned());
    v.ok_or_else(|| {
        QsdbError::invalid(format!(
            "第 {idx} 个参数没有提供（SQL 里用了 ${idx}，但 params 只有 {} 个）",
            PARAMS.with(|p| p.borrow().len())
        ))
    })
}

/// 解析占位符文本 → 序号（1-based）。
pub(crate) fn placeholder_index(p: &str) -> Result<usize> {
    let Some(num) = p.strip_prefix('$') else {
        // `?` 本该在**解析之前**被 `number_question_marks` 固化成 `$n`（设计 §三）。
        // 走到这里说明有人绕过了那条路 —— 报出来，别当成别的什么。
        return Err(QsdbError::invalid(format!(
            "占位符 `{p}` 未经预处理 —— `?` 应在解析前固化成 `$n`（内部错误）"
        )));
    };
    let idx: usize = num.parse().map_err(|_| {
        QsdbError::invalid(format!("占位符 `${num}` 不是合法序号（要写成 $1、$2…）"))
    })?;
    if idx == 0 {
        return Err(QsdbError::invalid(
            "占位符 `$0` 非法 —— 参数序号从 **1** 开始（$1）",
        ));
    }
    Ok(idx)
}

// ---------------------------------------------------------------------------
// `?` 的固化：按**源码顺序**编号，改的是**原文本的精确字节区间**
// ---------------------------------------------------------------------------

/// 把 `?` 按出现顺序固化成 `$1…$n`，**并报出这条 SQL 用到几个参数**。
///
/// 返回 `(规范化后的 SQL, 用到的参数个数)`；个数 = `$n` 的最大序号（没有占位符 = 0）。
///
/// ⚠️ **个数是在这里数出来的，不是绑定期数出来的** —— 这是刻意的：绑定是**懒**的
/// （`SELECT` 的绑定要等 `execute` 那一步才做，因为它需要 `db` 拿表定义），
/// 若把"参数个数对不对"留到那时查，就会变成"语句已经跑完了才报参数给多了"
/// （对 `INSERT` 是**写完了才报**）。**在文本上数** ⇒ 任何翻译/副作用之前就能拒。
/// 而且文本层面的判据与绑定层面的完全一致（同一个 tokenizer 认出来的占位符）。
///
/// ⚠️ **为什么不写成"绑定期用计数器编号"**：本仓有"把同一个表达式再走一遍"的路径
/// （`rebind_over_agg` 的 HAVING/窗口重绑、`group_key_pos` 的试探绑定）——
/// 计数器重走一次就错位，值会**静默绑到别的占位符上**。所以必须**在绑定之前**
/// 把序号写死在文本里。
///
/// ⚠️ **为什么用 sqlparser 自己的 tokenizer 而不是手写扫描器**：字符串字面量、
/// 注释、带引号标识符里的 `?` 都不能算占位符 —— 这些边界条件手写极容易漏
/// （驱动侧改写器就常在这里出 bug）。tokenizer 已经把它们处理对了（实测）。
pub fn normalize_placeholders(sql: &str) -> Result<(std::borrow::Cow<'_, str>, usize)> {
    use sqlparser::dialect::GenericDialect;
    use sqlparser::tokenizer::{Token, Tokenizer};

    let mut tk = Tokenizer::new(&GenericDialect {}, sql);
    let tokens = tk
        .tokenize_with_location()
        .map_err(|e| QsdbError::parse_err(format!("SQL tokenize 失败：{e}")))?;

    // ① 混用检查 + 收集 `?` 的字节起点 + 数出最大序号
    let mut marks: Vec<usize> = Vec::new();
    let mut has_dollar = false;
    let mut max_idx = 0usize;
    for t in &tokens {
        match &t.token {
            Token::Placeholder(p) if p == "?" => {
                marks.push(byte_offset(sql, t.span.start.line, t.span.start.column)?);
            }
            Token::Placeholder(p) => {
                has_dollar = true;
                max_idx = max_idx.max(placeholder_index(p)?);
            }
            _ => {}
        }
    }
    if marks.is_empty() {
        return Ok((std::borrow::Cow::Borrowed(sql), max_idx));
    }
    if has_dollar {
        return Err(QsdbError::invalid(
            "同一条 SQL 里不能混用 `?` 与 `$n` 两种占位符 —— 请只用一种\
             （`?` 按出现顺序编号，`$n` 显式指定）",
        ));
    }

    // ② 从后往前替换（不改动前面的偏移）
    let mut out = sql.to_string();
    for (i, off) in marks.iter().enumerate().rev() {
        out.replace_range(*off..*off + 1, &format!("${}", i + 1));
    }
    Ok((std::borrow::Cow::Owned(out), marks.len()))
}

/// `params` 的个数与 SQL 里用到的占位符个数必须**正好相等**。
///
/// - 少给 ⇒ 后面的占位符绑不上（点名缺第几个）；
/// - 多给 ⇒ **报错**。静默忽略多出来的参数 = 调用方以为自己防住了注入，其实有个值
///   压根没绑上 —— 那正是本仓最忌讳的"默默按没写执行"。
pub fn check_param_count(expected: usize, given: usize) -> Result<()> {
    if given < expected {
        return Err(QsdbError::invalid(format!(
            "参数不够：SQL 里用到了 {expected} 个占位符，只给了 {given} 个"
        )));
    }
    if given > expected {
        return Err(QsdbError::invalid(format!(
            "参数多了：SQL 里只用了 {expected} 个占位符，给了 {given} 个 —— \
             多出来的不会被绑定（宁可报错，也不让调用方以为绑上了）"
        )));
    }
    Ok(())
}

/// `(行, 列)` → 字节偏移。
///
/// ⚠️ sqlparser 的 `Location.column` 数的是**字符**、不是字节
/// （实测：`SELECT '含中文内容', ?` 里那个 `?` 报 `C17` —— 按字符数才对得上）。
/// 所以必须**逐字符走一遍**；直接拿 column 当字节用，会在含中文的 SQL（字符串
/// 字面量、注释）上错位。
pub(crate) fn byte_offset(sql: &str, line: u64, col: u64) -> Result<usize> {
    let (mut l, mut c) = (1u64, 1u64);
    for (i, ch) in sql.char_indices() {
        if l == line && c == col {
            return Ok(i);
        }
        if ch == '\n' {
            l += 1;
            c = 1;
        } else {
            c += 1;
        }
    }
    if l == line && c == col {
        return Ok(sql.len());
    }
    Err(QsdbError::invalid(format!(
        "tokenizer 报的位置 L{line}C{col} 落不到 SQL 文本上（内部错误）"
    )))
}

// ---------------------------------------------------------------------------
// JSON 参数 → 已类型化的 Literal
// ---------------------------------------------------------------------------

/// 一个参数元素的 JSON 形态：裸值（推类型）或 `{"type": …, "value": …}`（显式）。
///
/// 显式是**可选**的：手写 HTTP 好用、驱动照 `setInt/setDate` 直传也不用多想；
/// 同时它是"必填类型"的**超集** ⇒ 将来不会因为要加类型而改协议。
#[derive(serde::Deserialize)]
#[serde(untagged)]
enum ParamBody {
    Typed {
        #[serde(rename = "type")]
        ty: String,
        value: serde_json::Value,
    },
    Bare(serde_json::Value),
}

/// 协议面的 `params` 数组 → 内核的字面量数组。
pub fn params_from_json(v: Option<&serde_json::Value>) -> Result<Vec<Literal>> {
    let Some(v) = v else {
        return Ok(Vec::new());
    };
    let arr = v
        .as_array()
        .ok_or_else(|| QsdbError::invalid("`params` 必须是数组"))?;
    arr.iter()
        .enumerate()
        .map(|(i, e)| {
            let body: ParamBody = serde_json::from_value(e.clone())
                .map_err(|err| QsdbError::invalid(format!("第 {} 个参数格式不对：{err}", i + 1)))?;
            match body {
                ParamBody::Typed { ty, value } => {
                    let t = parse_param_type(&ty).map_err(|err| {
                        QsdbError::invalid(format!(
                            "第 {} 个参数的类型 `{ty}` 不认识：{err}",
                            i + 1
                        ))
                    })?;
                    json_to_literal(&value, Some(&t)).map_err(|err| {
                        QsdbError::invalid(format!("第 {} 个参数（{ty}）：{err}", i + 1))
                    })
                }
                ParamBody::Bare(value) => json_to_literal(&value, None)
                    .map_err(|err| QsdbError::invalid(format!("第 {} 个参数：{err}", i + 1))),
            }
        })
        .collect()
}

/// 显式 `type` 的名字：**与 DDL 用同一个解析器**（`BIGINT` / `DATE` /
/// `DECIMAL(18,2)` …）⇒ 两边名字不会漂。
pub fn parse_param_type(name: &str) -> Result<LogicalType> {
    let ast = sqlparser::parser::Parser::parse_sql(
        &sqlparser::dialect::GenericDialect {},
        &format!("SELECT CAST(NULL AS {name})"),
    )
    .map_err(|e| QsdbError::invalid(format!("{e}")))?;
    let sqlparser::ast::Statement::Query(q) = &ast[0] else {
        return Err(QsdbError::invalid("内部：类型名解析形状不对"));
    };
    let sqlparser::ast::SetExpr::Select(s) = q.body.as_ref() else {
        return Err(QsdbError::invalid("内部：类型名解析形状不对"));
    };
    let sqlparser::ast::SelectItem::UnnamedExpr(sqlparser::ast::Expr::Cast { data_type, .. }) =
        &s.projection[0]
    else {
        return Err(QsdbError::invalid("内部：类型名解析形状不对"));
    };
    crate::sql::translate::ast_type_to_logical(data_type)
}

/// JSON 值 → 字面量。`ty = None` 时按 JSON 自身推断（见设计 §五）。
fn json_to_literal(v: &serde_json::Value, ty: Option<&LogicalType>) -> Result<Literal> {
    use serde_json::Value as J;
    // 没写类型 ⇒ 按 JSON 推。**字符串推不出日期** ⇒ DATE/TIMESTAMP 必须显式写 type。
    let ty = match ty {
        Some(t) => t.clone(),
        None => match v {
            J::Null => return Ok(Literal::Null),
            J::Bool(_) => LogicalType::Boolean,
            J::Number(n) if n.is_i64() => LogicalType::Int64,
            J::Number(n) if n.is_f64() => LogicalType::Float64,
            J::Number(_) => {
                return Err(QsdbError::invalid(
                    "整数超出 i64 范围 —— 请显式写类型（如 {\"type\":\"DECIMAL(38,0)\",…}）",
                ));
            }
            J::String(_) => LogicalType::Varchar { max_chars: None },
            J::Array(_) | J::Object(_) => {
                return Err(QsdbError::invalid(
                    "不支持把 JSON 数组/对象当参数（本库没有数组/结构体类型）",
                ));
            }
        },
    };
    // 取文本形态（数字/布尔也允许写成字符串，便于大数与精确小数）
    let text = match v {
        J::Null => return Ok(Literal::Null),
        J::String(s) => s.clone(),
        J::Bool(b) => b.to_string(),
        J::Number(n) => n.to_string(),
        _ => return Err(QsdbError::invalid("参数值形态不支持")),
    };
    crate::sql::translate::parse_literal_text(&text, &ty)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn 编号与位置换算() {
        let sql = "SELECT ? FROM t WHERE a = ?";
        assert_eq!(
            normalize_placeholders(sql).unwrap().0,
            "SELECT $1 FROM t WHERE a = $2"
        );
        // 字符串字面量 / 注释 / 带引号标识符里的 `?` 不算占位符
        assert_eq!(
            normalize_placeholders("SELECT '$1?', ? FROM t").unwrap().0,
            "SELECT '$1?', $1 FROM t"
        );
        assert_eq!(
            normalize_placeholders("SELECT 1 -- ?\n, ? FROM t")
                .unwrap()
                .0,
            "SELECT 1 -- ?\n, $1 FROM t"
        );
        // ⚠️ 含中文：column 数字符不数字节，换算必须逐字符走
        assert_eq!(
            normalize_placeholders("SELECT '含中文内容', ? FROM t")
                .unwrap()
                .0,
            "SELECT '含中文内容', $1 FROM t"
        );
        // 没有 `?` ⇒ 原样（借用，不复制）；顺带报出用到的参数个数
        assert!(matches!(
            normalize_placeholders("SELECT $1 FROM t").unwrap().0,
            std::borrow::Cow::Borrowed(_)
        ));
        assert_eq!(normalize_placeholders("SELECT ? , ? FROM t").unwrap().1, 2);
        assert_eq!(normalize_placeholders("SELECT $3 FROM t").unwrap().1, 3);
        assert_eq!(normalize_placeholders("SELECT 1 FROM t").unwrap().1, 0);
    }

    #[test]
    fn 混用被拒() {
        let e = normalize_placeholders("SELECT ? , $2 FROM t").unwrap_err();
        assert!(format!("{e}").contains("不能混用"), "{e}");
    }
}
