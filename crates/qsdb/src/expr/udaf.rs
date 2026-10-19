//! UDAF 聚合函数注册（M5-A4）。
//!
//! 语义：**整组向量化求值**——`eval` 收到该组全部输入列（数组长度 = 组行数），
//! 返回长度 1 的数组。不带部分状态 merge，因此 UDAF 查询走串行物化分组路径
//! （UdafAggExec），不参与并行 partial/merge——核心聚合热路径零改动零开销。
//!
//! 约定：
//! - 名字小写归一 + ascii [a-z0-9_]；与内置聚合/内置标量/UDF/已注册 UDAF 重名报错
//! - `ret_ty` 在 binder 做入参类型检查；`eval` 的 NULL 处理、确定性由 UDAF 负责
//! - UDAF panic 由查询级 catch_unwind 隔离
//! - 混合查询（UDAF + 内置聚合同 SELECT）：内置聚合在该路径按组重算
//!   （avg(decimal) 混用显式报错后置）；纯内置查询不受影响
//!
//! ```no_run
//! use std::sync::Arc;
//! use arrow::array::{Array, ArrayRef, Float64Array};
//! use qsdb::types::LogicalType;
//!
//! qsdb::udaf::register(
//!     "my_second_largest",
//!     Arc::new(|args: &[LogicalType]| match args {
//!         [LogicalType::Float64] => Ok(LogicalType::Float64),
//!         _ => Err(qsdb::QsdbError::type_err("my_second_largest(double) expected")),
//!     }),
//!     Arc::new(|args: &[ArrayRef]| {
//!         let a = args[0].as_any().downcast_ref::<Float64Array>().unwrap();
//!         let mut m = [f64::NEG_INFINITY; 2];
//!         for v in a.iter().flatten() {
//!             if v > m[0] { m[1] = m[0]; m[0] = v; }
//!             else if v > m[1] { m[1] = v; }
//!         }
//!         Ok(Arc::new(Float64Array::from(vec![Some(m[1])])) as ArrayRef)
//!     }),
//! ).unwrap();
//! // SELECT g, my_second_largest(v) FROM t GROUP BY g
//! ```

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, LazyLock, RwLock};

use crate::error::{QsdbError, Result};

pub use super::scalar::RetTyFn;
pub use super::udf::EvalFn;

/// UDAF 定义。
pub struct UdafDef {
    /// 注册名（小写；身份判据）
    pub name: String,
    /// 入参类型 → 返回类型（binder 类型检查）
    pub ret_ty: RetTyFn,
    /// 整组求值：该组全部输入列 → 长度 1 数组
    pub eval: EvalFn,
}

impl fmt::Debug for UdafDef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UdafDef").field("name", &self.name).finish()
    }
}

impl PartialEq for UdafDef {
    /// 注册名唯一 → 名字即身份
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

static UDAFS: LazyLock<RwLock<HashMap<String, Arc<UdafDef>>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

/// 内置聚合名（防冲突）。
fn is_builtin_agg(name: &str) -> bool {
    matches!(name, "count" | "sum" | "min" | "max" | "avg")
}

/// 注册 UDAF。重名（内置聚合/内置标量/UDF/已注册 UDAF）报错；替换先 [`unregister`]。
pub fn register(name: &str, ret_ty: RetTyFn, eval: EvalFn) -> Result<()> {
    let name = super::udf::normalize(name)?;
    if is_builtin_agg(&name)
        || super::scalar::is_builtin(&name)
        || super::udf::lookup(&name).is_some()
    {
        return Err(QsdbError::invalid(format!(
            "UDAF name '{name}' conflicts with an existing function"
        )));
    }
    let mut g = UDAFS
        .write()
        .map_err(|_| QsdbError::meta("udaf registry poisoned"))?;
    if g.contains_key(&name) {
        return Err(QsdbError::already_exists(format!(
            "UDAF '{name}' already registered (unregister first to replace)"
        )));
    }
    g.insert(name.clone(), Arc::new(UdafDef { name, ret_ty, eval }));
    Ok(())
}

/// 注销 UDAF；返回是否存在。
pub fn unregister(name: &str) -> bool {
    match UDAFS.write() {
        Ok(mut g) => g.remove(&name.to_ascii_lowercase()).is_some(),
        Err(_) => false,
    }
}

/// 已注册 UDAF 名（小写、排序）。
pub fn names() -> Vec<String> {
    UDAFS
        .read()
        .map(|g| {
            let mut v: Vec<String> = g.keys().cloned().collect();
            v.sort();
            v
        })
        .unwrap_or_default()
}

/// 按名查找（binder 用；入参小写归一）。
pub fn lookup(name: &str) -> Option<Arc<UdafDef>> {
    UDAFS
        .read()
        .ok()
        .and_then(|g| g.get(&name.to_ascii_lowercase()).cloned())
}
