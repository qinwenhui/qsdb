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
            Self::Cmp { .. } | Self::And(..) | Self::Or(..) | Self::Not(_) | Self::IsNull(..) => {
                LogicalType::Boolean
            }
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
}

impl AggFunc {
    pub fn name(self) -> &'static str {
        match self {
            Self::Count => "count",
            Self::Sum => "sum",
            Self::Min => "min",
            Self::Max => "max",
            Self::Avg => "avg",
        }
    }

    /// 聚合结果类型（输入类型 → 输出类型）。
    pub fn output_type(self, input: &LogicalType) -> crate::error::Result<LogicalType> {
        use crate::types::{DecimalMeta, LogicalType as Lt};
        Ok(match self {
            Self::Count => Lt::Int64,
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
        })
    }

    pub fn display(&self) -> String {
        match &self.arg {
            None => format!("{}(*)", self.func.name()),
            Some(a) => format!("{}({})", self.func.name(), a.display()),
        }
    }
}
