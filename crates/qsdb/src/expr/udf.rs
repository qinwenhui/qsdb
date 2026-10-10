//! UDF 标量函数注册（M5-5）。
//!
//! 全局注册表：进程内所有 [`crate::Qsdb`] 实例共享（执行层表达式求值无
//! 实例上下文，按实例隔离需重构 eval 链路——后置项）。
//!
//! 语义约定：
//! - 名字小写归一（SQL 大小写不敏感）；不能与内置函数或已注册 UDF 重名
//! - `ret_ty` 在 binder 阶段做类型检查（错误 = 查询拒绝）
//! - `eval` 是向量化求值：整批 `ArrayRef` 进、同长度 `ArrayRef` 出；
//!   NULL 传播、长度校验由 UDF 自己负责
//! - UDF panic 由查询级 catch_unwind 隔离（QueryPanic），不拖垮进程
//! - 数值入参在 binder 隐式拓宽（int→BIGINT / float→DOUBLE）后到达 eval
//!
//! ```no_run
//! use std::sync::Arc;
//! use arrow::array::{ArrayRef, Int64Array};
//! use qsdb::error::Result;
//! use qsdb::types::LogicalType;
//!
//! qsdb::udf::register(
//!     "my_double",
//!     Arc::new(|args: &[LogicalType]| match args {
//!         [LogicalType::Int64] => Ok(LogicalType::Int64),
//!         _ => Err(qsdb::QsdbError::type_err("my_double(bigint) expected")),
//!     }),
//!     Arc::new(|args: &[ArrayRef]| {
//!         let a = args[0].as_any().downcast_ref::<Int64Array>().unwrap();
//!         Ok(Arc::new(a.iter().map(|v| v.map(|x| x * 2)).collect::<Int64Array>()) as ArrayRef)
//!     }),
//! ).unwrap();
//! // SELECT my_double(v) FROM t  ← 可用于投影 / WHERE / ORDER BY
//! ```

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, RwLock};

use super::scalar::{ScalarFnDef, is_builtin};
use crate::error::{QsdbError, Result};

pub use super::scalar::{EvalFn, RetTyFn};

static UDFS: LazyLock<RwLock<HashMap<String, Arc<ScalarFnDef>>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

/// 注册 UDF。名字小写归一；与内置函数或已注册 UDF 重名报错（防语义意外覆盖，
/// 需替换先 [`unregister`]）。
pub fn register(name: &str, ret_ty: RetTyFn, eval: EvalFn) -> Result<()> {
    let name = normalize(name)?;
    if is_builtin(&name) {
        return Err(QsdbError::invalid(format!(
            "UDF name '{name}' conflicts with a builtin function"
        )));
    }
    let mut g = UDFS
        .write()
        .map_err(|_| QsdbError::meta("udf registry poisoned"))?;
    if g.contains_key(&name) {
        return Err(QsdbError::already_exists(format!(
            "UDF '{name}' already registered (unregister first to replace)"
        )));
    }
    g.insert(name, Arc::new(ScalarFnDef { ret_ty, eval }));
    Ok(())
}

/// 注销 UDF；返回是否存在。
pub fn unregister(name: &str) -> bool {
    match UDFS.write() {
        Ok(mut g) => g.remove(&name.to_ascii_lowercase()).is_some(),
        Err(_) => false,
    }
}

/// 已注册 UDF 名（小写、排序）。
pub fn names() -> Vec<String> {
    UDFS.read()
        .map(|g| {
            let mut v: Vec<String> = g.keys().cloned().collect();
            v.sort();
            v
        })
        .unwrap_or_default()
}

/// 内置表未命中后的回退查找（scalar::lookup 调用；入参小写归一）。
pub(super) fn lookup(name: &str) -> Option<Arc<ScalarFnDef>> {
    UDFS.read()
        .ok()
        .and_then(|g| g.get(&name.to_ascii_lowercase()).cloned())
}

fn normalize(name: &str) -> Result<String> {
    let n = name.trim().to_ascii_lowercase();
    if n.is_empty() || !n.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(QsdbError::invalid(format!(
            "UDF name '{name}' invalid (ascii [a-z0-9_] required)"
        )));
    }
    Ok(n)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;

    fn noop_eval() -> EvalFn {
        Arc::new(|args: &[arrow::array::ArrayRef]| {
            let a = args[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| QsdbError::type_err("bigint expected"))?;
            Ok(
                Arc::new(a.iter().map(|v| v.map(|x| x + 1)).collect::<Int64Array>())
                    as arrow::array::ArrayRef,
            )
        })
    }

    #[test]
    fn register_lookup_unregister() {
        let ret: RetTyFn = Arc::new(|args: &[crate::types::LogicalType]| match args {
            [crate::types::LogicalType::Int64] => Ok(crate::types::LogicalType::Int64),
            _ => Err(QsdbError::type_err("bigint expected")),
        });
        let name = "udf_reg_test";
        register(name, ret, noop_eval()).unwrap();
        // 大小写归一命中
        assert!(lookup("UDF_REG_TEST").is_some());
        // 重复注册拒绝
        assert!(register(name, noop_ret(), noop_eval()).is_err());
        // 内置冲突拒绝
        assert!(register("upper", noop_ret(), noop_eval()).is_err());
        // 非法名拒绝
        assert!(register("bad-name!", noop_ret(), noop_eval()).is_err());
        // 注销后未命中
        assert!(unregister(name));
        assert!(lookup(name).is_none());
    }

    fn noop_ret() -> RetTyFn {
        Arc::new(|_| Ok(crate::types::LogicalType::Int64))
    }
}
