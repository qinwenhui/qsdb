//! 物理表达式系统：绑定列 idx、自带结果类型、向量化求值。
//!
//! 类型规则（binder 负责构造合法表达式）：
//! - 算术：两侧 cast 到公共类型（数值塔 / DECIMAL 精度矩阵），结果即该类型
//! - DECIMAL 算术走 i256 自实现精确路径（add/sub/mul/div + HALF_UP）
//! - 比较：两侧 cast 到公共类型后按序比较；结果 Boolean
//! - And/Or/Not：Kleene 三值；IsNull/IsNotNull 恒 Boolean
//! - Cast：arrow cast（safe=false，失败即错误）

pub mod eval;
pub mod scalar;
pub mod udaf;
pub mod udf;

use crate::types::{Literal, LogicalType};

/// 算术运算符。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ArithOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
}

impl ArithOp {
    pub fn symbol(self) -> &'static str {
        match self {
            Self::Add => "+",
            Self::Sub => "-",
            Self::Mul => "*",
            Self::Div => "/",
            Self::Mod => "%",
        }
    }
}

/// 比较运算符。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CmpOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

impl CmpOp {
    pub fn symbol(self) -> &'static str {
        match self {
            Self::Eq => "=",
            Self::NotEq => "!=",
            Self::Lt => "<",
            Self::LtEq => "<=",
            Self::Gt => ">",
            Self::GtEq => ">=",
        }
    }
}

/// 物理表达式（绑定完成：列已解析为 idx，节点自带结果类型）。
#[derive(Debug, Clone, PartialEq)]
pub enum PhysExpr {
    Column {
        idx: usize,
        name: String,
        ty: LogicalType,
    },
    Lit(Literal),
    Arith {
        op: ArithOp,
        l: Box<PhysExpr>,
        r: Box<PhysExpr>,
        ty: LogicalType,
    },
    Cmp {
        op: CmpOp,
        l: Box<PhysExpr>,
        r: Box<PhysExpr>,
    },
    And(Box<PhysExpr>, Box<PhysExpr>),
    Or(Box<PhysExpr>, Box<PhysExpr>),
    Not(Box<PhysExpr>),
    /// IsNull(e, false) = IS NULL；IsNull(e, true) = IS NOT NULL
    IsNull(Box<PhysExpr>, bool),
    Cast {
        e: Box<PhysExpr>,
        ty: LogicalType,
    },
    /// 标量函数（`scalar::registry`）
    Fn {
        name: String,
        args: Vec<PhysExpr>,
        ty: LogicalType,
    },
    /// CASE WHEN（M7-B）：conds 按序评估（短路），else 兜底。向量化：
    /// 布尔掩码从后往前覆盖（首个真条件的值存活）
    Case {
        conds: Vec<(PhysExpr, PhysExpr)>,
        else_expr: Box<PhysExpr>,
        ty: LogicalType,
    },
    /// 多参折叠（COALESCE / LEAST / GREATEST）：**跳过 NULL**，按 `kind` 的选择器
    /// 在其余值里取一个；**全 NULL ⇒ NULL**。三者是同一个形状，故共用一个变体。
    /// 参数已 cast 到 `ty`（公共超类型），求值时无需再转换。
    Fold {
        kind: FoldKind,
        args: Vec<PhysExpr>,
        ty: LogicalType,
    },
    /// 字符串拼接 `||`（M7-B）：两侧已 cast 为 Utf8
    Concat(Box<PhysExpr>, Box<PhysExpr>),
    /// LIKE/ILIKE（M7-B4）：模式绑定期预编译；ci=大小写不敏感。
    Like {
        e: Box<PhysExpr>,
        pattern: LikePattern,
        negated: bool,
        ci: bool,
    },
}

/// `Fold` 的选择器：跳过 NULL 之后，在剩下的值里选谁。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FoldKind {
    /// COALESCE：取**首个**（按参数顺序）。
    Coalesce,
    /// LEAST：取**最小**（Spark 语义：忽略 NULL，全 NULL ⇒ NULL）。
    Least,
    /// GREATEST：取**最大**（同上）。
    Greatest,
}

impl FoldKind {
    pub fn name(self) -> &'static str {
        match self {
            Self::Coalesce => "coalesce",
            Self::Least => "least",
            Self::Greatest => "greatest",
        }
    }
}

/// LIKE 模式词元（`%` 任意序列 / `_` 单字符；字面字符按 char）。
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LikeTok {
    Lit(char),
    One,
    Any,
}

/// 预编译 LIKE 模式：常见形态走零回溯快路径，其余通用回溯匹配。
#[derive(Debug, Clone, PartialEq)]
pub enum LikePattern {
    /// 无通配符
    Exact(String),
    /// `abc%`
    Prefix(String),
    /// `%abc`
    Suffix(String),
    /// `%abc%`
    Contains(String),
    /// 通用（连续 % 已折叠）
    General(Vec<LikeTok>),
}

impl LikePattern {
    /// 编译模式。escape=None 时 `\` 为普通字符（Postgres 默认，无隐式转义）。
    /// ci 时字面段预小写（行值匹配时同步小写）。
    pub fn compile(pat: &str, escape: Option<char>, ci: bool) -> crate::error::Result<Self> {
        let mut toks: Vec<LikeTok> = Vec::new();
        let mut lit = String::new();
        let mut chars = pat.chars().peekable();
        while let Some(raw) = chars.next() {
            let (c, escaped) = if Some(raw) == escape {
                // 转义：下一字符取字面（含通配符本身）
                match chars.next() {
                    Some(n) => (n, true),
                    None => {
                        return Err(crate::error::QsdbError::parse_err(
                            "LIKE pattern ends with escape character",
                        ));
                    }
                }
            } else {
                (raw, false)
            };
            match c {
                '%' if !escaped => {
                    // 连续 % 折叠：仅当中间无未 flush 的字面量（lit 空）且末尾已是 Any
                    if !(lit.is_empty() && matches!(toks.last(), Some(LikeTok::Any))) {
                        flush_lit(&mut lit, &mut toks);
                        toks.push(LikeTok::Any);
                    }
                }
                '_' if !escaped => {
                    flush_lit(&mut lit, &mut toks);
                    toks.push(LikeTok::One);
                }
                c => {
                    if ci {
                        // 多字符小写展开（如 ß→ss）完整保留
                        for lc in c.to_lowercase() {
                            lit.push(lc);
                        }
                    } else {
                        lit.push(c);
                    }
                }
            }
        }
        flush_lit(&mut lit, &mut toks);
        // 快路径归类
        let is_lit = |t: &LikeTok| matches!(t, LikeTok::Lit(_));
        let all_lit = |ts: &[LikeTok]| ts.iter().all(is_lit);
        let lit_join = |ts: &[LikeTok]| {
            ts.iter()
                .map(|t| match t {
                    LikeTok::Lit(c) => *c,
                    _ => unreachable!(),
                })
                .collect::<String>()
        };
        Ok(match toks.as_slice() {
            [] => LikePattern::Exact(String::new()),
            t if all_lit(t) => LikePattern::Exact(lit_join(t)),
            [t @ .., LikeTok::Any] if all_lit(t) => LikePattern::Prefix(lit_join(t)),
            [LikeTok::Any, t @ ..] if all_lit(t) => LikePattern::Suffix(lit_join(t)),
            [LikeTok::Any, t @ .., LikeTok::Any] if all_lit(t) => {
                LikePattern::Contains(lit_join(t))
            }
            _ => LikePattern::General(toks),
        })
    }

    /// 行值匹配（ci 时行值小写后比较——与编译期小写规则一致）。
    pub fn matches(&self, s: &str, ci: bool) -> bool {
        let lowered;
        let s = if ci {
            lowered = s.to_lowercase();
            lowered.as_str()
        } else {
            s
        };
        match self {
            LikePattern::Exact(x) => s == x,
            LikePattern::Prefix(x) => s.starts_with(x),
            LikePattern::Suffix(x) => s.ends_with(x),
            LikePattern::Contains(x) => s.contains(x),
            LikePattern::General(toks) => like_match(&s.chars().collect::<Vec<_>>(), toks),
        }
    }
}

fn flush_lit(lit: &mut String, toks: &mut Vec<LikeTok>) {
    for c in lit.drain(..) {
        toks.push(LikeTok::Lit(c));
    }
}

impl LikePattern {
    /// 模式回显（display/EXPLAIN 用；快路径即原样语义）。
    pub fn display(&self) -> String {
        match self {
            LikePattern::Exact(s) => format!("'{s}'"),
            LikePattern::Prefix(s) => format!("'{s}%'"),
            LikePattern::Suffix(s) => format!("'%{s}'"),
            LikePattern::Contains(s) => format!("'%{s}%'"),
            LikePattern::General(toks) => {
                let mut s = String::new();
                for t in toks {
                    match t {
                        LikeTok::Lit(c) => s.push(*c),
                        LikeTok::One => s.push('_'),
                        LikeTok::Any => s.push('%'),
                    }
                }
                format!("'{s}'")
            }
        }
    }
}

/// 通用回溯匹配（模式短，回溯量级可控）。
fn like_match(s: &[char], toks: &[LikeTok]) -> bool {
    match toks.split_first() {
        None => s.is_empty(),
        Some((LikeTok::Any, rest)) => {
            if rest.is_empty() {
                return true;
            }
            (0..=s.len()).rev().any(|k| like_match(&s[k..], rest))
        }
        Some((LikeTok::One, rest)) => !s.is_empty() && like_match(&s[1..], rest),
        Some((LikeTok::Lit(c), rest)) => !s.is_empty() && s[0] == *c && like_match(&s[1..], rest),
    }
}

impl PhysExpr {
    /// 公开版 cast_to（诊断/测试用）。
    pub fn cast_to_pub(self, ty: LogicalType) -> PhysExpr {
        crate::sql::translate::cast_to_impl(self, ty)
    }

    /// 结果类型。
    pub fn data_type(&self) -> LogicalType {
        match self {
            Self::Column { ty, .. } => ty.clone(),
            Self::Lit(l) => l.logical_type().unwrap_or(LogicalType::Boolean),
            Self::Arith { ty, .. } | Self::Cast { ty, .. } | Self::Fn { ty, .. } => ty.clone(),
            Self::Case { ty, .. } | Self::Fold { ty, .. } => ty.clone(),
            Self::Concat(..) => LogicalType::Varchar { max_chars: None },
            Self::Cmp { .. }
            | Self::And(..)
            | Self::Or(..)
            | Self::Not(_)
            | Self::IsNull(..)
            | Self::Like { .. } => LogicalType::Boolean,
        }
    }

    /// 是否为布尔类型（Filter/And/Or 合法输入）。
    pub fn is_boolean(&self) -> bool {
        matches!(self.data_type(), LogicalType::Boolean)
    }

    /// 收集引用列 idx。
    pub fn collect_columns(&self, out: &mut Vec<usize>) {
        match self {
            Self::Column { idx, .. } => {
                if !out.contains(idx) {
                    out.push(*idx);
                }
            }
            Self::Lit(_) => {}
            Self::Arith { l, r, .. } | Self::Cmp { l, r, .. } => {
                l.collect_columns(out);
                r.collect_columns(out);
            }
            Self::And(a, b) | Self::Or(a, b) => {
                a.collect_columns(out);
                b.collect_columns(out);
            }
            Self::Not(a) | Self::IsNull(a, _) => a.collect_columns(out),
            Self::Cast { e, .. } => e.collect_columns(out),
            Self::Fn { args, .. } => args.iter().for_each(|a| a.collect_columns(out)),
            Self::Case {
                conds, else_expr, ..
            } => {
                for (c, r) in conds {
                    c.collect_columns(out);
                    r.collect_columns(out);
                }
                else_expr.collect_columns(out);
            }
            Self::Fold { args, .. } => args.iter().for_each(|e| e.collect_columns(out)),
            Self::Concat(l, r) => {
                l.collect_columns(out);
                r.collect_columns(out);
            }
            Self::Like { e, .. } => e.collect_columns(out),
        }
    }

    /// 显示（诊断/EXPLAIN 用）。
    pub fn display(&self) -> String {
        match self {
            Self::Column { name, .. } => name.clone(),
            Self::Lit(l) => l.to_sql_string(),
            Self::Arith { op, l, r, .. } => {
                format!("({} {} {})", l.display(), op.symbol(), r.display())
            }
            Self::Cmp { op, l, r } => format!("({} {} {})", l.display(), op.symbol(), r.display()),
            Self::And(a, b) => format!("({} AND {})", a.display(), b.display()),
            Self::Or(a, b) => format!("({} OR {})", a.display(), b.display()),
            Self::Not(a) => format!("(NOT {})", a.display()),
            Self::IsNull(a, false) => format!("({} IS NULL)", a.display()),
            Self::IsNull(a, true) => format!("({} IS NOT NULL)", a.display()),
            Self::Cast { e, ty } => format!("CAST({} AS {ty})", e.display()),
            Self::Fn { name, args, .. } => {
                let inner: Vec<String> = args.iter().map(|a| a.display()).collect();
                format!("{name}({})", inner.join(", "))
            }
            Self::Case {
                conds, else_expr, ..
            } => {
                let mut s = String::from("CASE");
                for (c, r) in conds {
                    s.push_str(&format!(" WHEN {} THEN {}", c.display(), r.display()));
                }
                if !matches!(**else_expr, Self::Lit(Literal::Null)) {
                    s.push_str(&format!(" ELSE {}", else_expr.display()));
                }
                s.push_str(" END");
                s
            }
            Self::Fold { kind, args, .. } => {
                let inner: Vec<String> = args.iter().map(|e| e.display()).collect();
                format!("{}({})", kind.name(), inner.join(", "))
            }
            Self::Concat(l, r) => format!("({} || {})", l.display(), r.display()),
            Self::Like {
                e,
                pattern,
                negated,
                ci,
            } => format!(
                "({} {}{} {})",
                e.display(),
                if *negated { "NOT " } else { "" },
                if *ci { "ILIKE" } else { "LIKE" },
                pattern.display()
            ),
        }
    }
}

/// 聚合函数。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AggFunc {
    Count,
    Sum,
    Min,
    Max,
    Avg,
    /// 用户注册的 UDAF（定义在 AggExpr.udaf；GroupTable 状态路径不处理）
    Udaf,
}

impl AggFunc {
    pub fn name(self) -> &'static str {
        match self {
            Self::Count => "count",
            Self::Sum => "sum",
            Self::Min => "min",
            Self::Max => "max",
            Self::Avg => "avg",
            Self::Udaf => "udaf",
        }
    }

    /// 聚合结果类型（输入类型 → 输出类型）。Udaf 不经此路径（ret_ty 决定）。
    pub fn output_type(self, input: &LogicalType) -> crate::error::Result<LogicalType> {
        use crate::types::{DecimalMeta, LogicalType as Lt};
        Ok(match self {
            Self::Count | Self::Udaf => Lt::Int64,
            Self::Sum => match input {
                Lt::Int8 | Lt::Int16 | Lt::Int32 | Lt::Int64 => Lt::Int64,
                Lt::Float32 | Lt::Float64 => Lt::Float64,
                Lt::Decimal(m) => Lt::Decimal(DecimalMeta::sum_result(*m)),
                other => {
                    return Err(crate::error::QsdbError::type_err(format!(
                        "sum({other}) not supported"
                    )));
                }
            },
            Self::Avg => match input {
                Lt::Int8 | Lt::Int16 | Lt::Int32 | Lt::Int64 | Lt::Float32 | Lt::Float64 => {
                    Lt::Float64
                }
                Lt::Decimal(m) => Lt::Decimal(DecimalMeta::avg_result(*m)),
                other => {
                    return Err(crate::error::QsdbError::type_err(format!(
                        "avg({other}) not supported"
                    )));
                }
            },
            Self::Min | Self::Max => match input {
                t if t.is_numeric() || t.is_string() || t.is_temporal() => input.clone(),
                other => {
                    return Err(crate::error::QsdbError::type_err(format!(
                        "{}({other}) not supported",
                        self.name()
                    )));
                }
            },
        })
    }
}

/// 聚合表达式。
#[derive(Debug, Clone, PartialEq)]
pub struct AggExpr {
    pub func: AggFunc,
    /// None = COUNT(*)
    pub arg: Option<PhysExpr>,
    /// 输出列名（SELECT 里的显示名或别名）
    pub out_name: String,
    pub out_ty: LogicalType,
    /// UDAF 定义（func == Udaf 时必有；其余 None）
    pub udaf: Option<std::sync::Arc<udaf::UdafDef>>,
    /// 参数上写了 `DISTINCT`（`count(DISTINCT x)`）。
    ///
    /// **绑定层必须把它消费掉**：等价改写（`Distinct` 去重 + 普通聚合）在
    /// `translate` 里完成，落到计划节点上的 `AggExpr` 一律是 `false`。
    /// 执行层不认识这个语义，所以 `build_aggregate` 见到 `true` 会**显式报错**
    /// ——宁可炸，也不能把 `count(DISTINCT x)` 当成 `count(x)` 跑出个错数字
    /// （这正是 2026-10-11 之前的老行为）。
    pub distinct: bool,
    /// `FILTER (WHERE …)`：**只有谓词为 TRUE 的行**才喂给这个聚合
    /// （NULL/UNKNOWN 不通过 —— SQL 三值语义）。
    ///
    /// 与 `distinct` 不同，这个语义**落到了执行层**（`exec::agg` 在更新每个聚合
    /// 之前按自己的掩码筛行）。⚠️ 掩码是**逐聚合**的：同一条 SELECT 里两个聚合
    /// 可以各有各的 FILTER，所以**不能**在批次层面统一预筛（那会让它们互相污染）。
    pub filter: Option<PhysExpr>,
}

impl AggExpr {
    pub fn new(
        func: AggFunc,
        arg: Option<PhysExpr>,
        out_name: impl Into<String>,
    ) -> crate::error::Result<Self> {
        let input_ty = arg
            .as_ref()
            .map(|e| e.data_type())
            .unwrap_or(LogicalType::Boolean);
        let out_ty = func.output_type(&input_ty)?;
        Ok(Self {
            func,
            arg,
            out_name: out_name.into(),
            out_ty,
            udaf: None,
            distinct: false,
            filter: None,
        })
    }

    /// UDAF 聚合（单参数；输出类型由 ret_ty 决定）。
    pub fn new_udaf(
        def: std::sync::Arc<udaf::UdafDef>,
        arg: PhysExpr,
        out_name: impl Into<String>,
    ) -> crate::error::Result<Self> {
        let out_ty = (def.ret_ty)(&[arg.data_type()])?;
        Ok(Self {
            func: AggFunc::Udaf,
            arg: Some(arg),
            out_name: out_name.into(),
            out_ty,
            udaf: Some(def),
            distinct: false,
            filter: None,
        })
    }

    pub fn display(&self) -> String {
        match &self.arg {
            None => format!("{}(*)", self.func.name()),
            Some(a) => format!("{}({})", self.func.name(), a.display()),
        }
    }
}

#[cfg(test)]
mod like_tests {
    use super::*;

    #[test]
    fn compile_fast_paths() {
        assert!(matches!(
            LikePattern::compile("abc", None, false).unwrap(),
            LikePattern::Exact(s) if s == "abc"
        ));
        assert!(matches!(
            LikePattern::compile("abc%", None, false).unwrap(),
            LikePattern::Prefix(s) if s == "abc"
        ));
        assert!(matches!(
            LikePattern::compile("%abc", None, false).unwrap(),
            LikePattern::Suffix(s) if s == "abc"
        ));
        assert!(matches!(
            LikePattern::compile("%a%", None, false).unwrap(),
            LikePattern::Contains(s) if s == "a"
        ));
        assert!(matches!(
            LikePattern::compile("%z-a%", None, false).unwrap(),
            LikePattern::Contains(s) if s == "z-a"
        ));
    }

    #[test]
    fn match_semantics() {
        let c = LikePattern::compile("%z-a%", None, false).unwrap();
        assert!(c.matches("zz-abc", false));
        let g = LikePattern::compile("z%-a__", None, false).unwrap();
        assert!(g.matches("zz-abc", false));
        let one = LikePattern::compile("_x", None, false).unwrap();
        assert!(one.matches("ax", false));
        assert!(!one.matches("abx", false));
        // 转义：ESCAPE '!' 时 !% 为字面 %
        let e = LikePattern::compile("50!%", Some('!'), false).unwrap();
        assert!(e.matches("50%", false));
        assert!(!e.matches("50x", false));
        // 连续 % 折叠
        let f = LikePattern::compile("a%%b", None, false).unwrap();
        assert!(f.matches("axxb", false));
    }
}
