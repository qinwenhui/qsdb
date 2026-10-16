//! LogicalPlan → ExecNode。聚合遇「Scan(/Filter) 无破坏算子」时做
//! 文件级 rayon 并行（分片 partial agg → merge → finalize）。

use std::sync::Arc;

use arrow::record_batch::RecordBatch;

use crate::api::{Qsdb, ScanRequest};
use crate::error::Result;
use crate::exec::agg::GroupTable;
use crate::exec::window::WindowFn;
use crate::exec::{ExecNode, FilterExec, HashAggExec, LimitExec, ProjectExec, ScanExec, SortExec};
use crate::expr::{AggExpr, PhysExpr};

use super::translate::LogicalPlan;

// 分片重写：本线程上"某个实表名 → 该分片"的一次性覆盖。
//
// **为什么需要**（2026-10-14：join 之后的分聚合）：并行分片只切**最底下那个 Scan**，
// 但它上面可能还有 join —— 那些层必须**每片各建一遍**（与既有 semi 分片同款权衡：
// 墙钟不变、内存 × 分片数）。所以分片不能只喂给 `ScanExec`，得让 `build_inner`
// **在正常建计划的过程中**把最底下那个 Scan 换成分片。
//
// **取用一次即失效**（`take`）：`build_inner` 建 join 时**先建 probe（左）再建 build（右）**，
// 而 `try_shard_source` 是沿**左脊柱**剥到底的 ⇒ 第一个被建的 Scan 必是那个分片。
// 万一哪天顺序变了，`ShardScope::taken()` 会为 false，调用方**显式报错**
// （宁可报错，也不许静默变成"全表扫 + 结果错"）。
thread_local! {
    static SHARD_OVERRIDE: std::cell::RefCell<Option<(String, crate::scan::Scan)>> =
        const { std::cell::RefCell::new(None) };
}

/// 作用域守卫：生效期间 `build_inner` 把该表的 Scan 换成分片；drop 即失效（含 panic 展开）。
pub struct ShardScope;

impl ShardScope {
    /// 进入"表 `table` 的分片 = `shard`"的作用域。
    pub fn set(table: &str, shard: crate::scan::Scan) -> Self {
        SHARD_OVERRIDE.with(|c| *c.borrow_mut() = Some((table.to_string(), shard)));
        Self
    }

    /// 该作用域内的分片**是否真的被 `build_inner` 取走了**。
    ///
    /// 为 false 说明"剥出来的左脊柱"与"`build_inner` 实际建的树"对不上
    /// （计划形状变了 / 建序变了）——此时若继续，跑的就是**全表扫**：
    /// 结果仍对但白算一遍，更糟的是它**不报错**。调用方据此显式失败。
    pub fn taken(&self) -> bool {
        SHARD_OVERRIDE.with(|c| c.borrow().is_none())
    }
}

impl Drop for ShardScope {
    fn drop(&mut self) {
        SHARD_OVERRIDE.with(|c| *c.borrow_mut() = None);
    }
}

/// 取用分片覆盖（表名匹配才取；取走即失效）。
fn take_shard_override(table: &str) -> Option<crate::scan::Scan> {
    SHARD_OVERRIDE.with(|c| {
        let mut b = c.borrow_mut();
        match b.as_ref() {
            Some((name, _)) if name == table => b.take().map(|(_, s)| s),
            _ => None,
        }
    })
}

// CTE 批缓存（execute_sql 物化后注入；build 期间读取）。
//
// **生命周期由 `CteScope` 管**（RAII）：查询开始 `set_cte_batches` 重置、
// 结束（含提前 return / panic 展开）`CteScope::drop` 清空。此前只在"下一个
// WITH 查询开始时"才被替换，物化批会**跨查询驻留**在这条线程上
// ——与注释自称的"无跨查询泄漏"不符，是实打实的滞留。
thread_local! {
    static CTE_BATCHES: std::cell::RefCell<CteStore> = std::cell::RefCell::new(CteStore::default());
}

/// CTE 批存储 + 它们的内存预留（批被清掉，预留也一起归还）。
#[derive(Default)]
pub struct CteStore {
    map: std::collections::HashMap<u32, Vec<RecordBatch>>,
    reserved: Vec<crate::memory::Reservation>,
}

/// 查询级作用域：drop 即清空 CTE 批 + 归还内存预留。
#[derive(Default)]
pub struct CteScope;

impl CteScope {
    /// 开始一个新查询的 CTE 作用域（清掉上一条查询可能残留的物化批）。
    pub fn new() -> Self {
        set_cte_batches(std::collections::HashMap::new());
        Self
    }
}

impl Drop for CteScope {
    fn drop(&mut self) {
        set_cte_batches(std::collections::HashMap::new());
    }
}

/// 注入 CTE 批缓存（execute_sql 调用；build 前设置）。
pub fn set_cte_batches(map: std::collections::HashMap<u32, Vec<RecordBatch>>) {
    CTE_BATCHES.with(|c| {
        let mut s = c.borrow_mut();
        s.map = map;
        s.reserved.clear(); // 旧的批已丢弃 → 预留一并归还
    });
}

/// 增量写入单个 CTE 批（链式 CTE：后物化者读取先物化者的缓存），
/// 并把该批的内存预留挂进存储（存活到作用域结束）。
pub fn add_cte_batches(cte_id: u32, batches: Vec<RecordBatch>, res: crate::memory::Reservation) {
    CTE_BATCHES.with(|c| {
        let mut s = c.borrow_mut();
        s.map.insert(cte_id, batches);
        s.reserved.push(res);
    });
}

/// 当前 CTE 批占用的内存（估算：批的 Arrow buffer 字节）。
pub fn cte_bytes(map: &std::collections::HashMap<u32, Vec<RecordBatch>>) -> u64 {
    map.values().flatten().map(crate::memory::batch_bytes).sum()
}

/// 一次性批执行节点（并行聚合结果）。
struct OneShotExec {
    batch: Option<RecordBatch>,
    schema: Arc<arrow::datatypes::Schema>,
}

impl ExecNode for OneShotExec {
    fn schema(&self) -> Arc<arrow::datatypes::Schema> {
        Arc::clone(&self.schema)
    }
    fn next(&mut self) -> Option<Result<RecordBatch>> {
        self.batch.take().map(Ok)
    }
}

pub fn build(
    db: &Qsdb,
    plan: &LogicalPlan,
    ctx: &Arc<crate::query::QueryContext>,
) -> Result<Box<dyn ExecNode>> {
    // M4-3 列裁剪下推：收集全计划引用列，Scan 只读所需列（含谓词列）。
    // 含窗计划**默认**仍然跳过（窗口列 idx 假设全列序）——只放行
    // `phase1_window` 那一种形状，见 `prune_decision`。
    if let Some((all, proj)) = prune_decision(plan) {
        if proj.len() < all.len() {
            let mapping = build_mapping(&all, &proj);
            // 裁掉的列数 = 各派生空间相对原计划的**左移量**（见 `output_shift`）
            let k = all.len() - proj.len();
            let rewritten = rewrite_plan(plan, &mapping, &all, k);
            return build_inner(db, &rewritten, Some(proj), ctx);
        }
    }
    build_inner(db, plan, None, ctx)
}

/// 列裁剪的**决策**：`(表全列, 实际要读的列)`；`None` = 这条计划不走裁剪。
///
/// 单独拎出来是为了让它**可断言**——这个优化已经三次因为「表达式活在哪个
/// 索引空间」静默出错（见 `exprs_are_in_table_space`、`Sort` 臂的注释），
/// 而"结果对不对"根本看不出它有没有白跑（少裁一列只是变慢，不是变错）。
/// 回归：`tests/projection_space.rs`。
pub fn prune_decision(plan: &LogicalPlan) -> Option<(Vec<String>, Vec<String>)> {
    // 含窗计划：**只放行**第一阶段那一种形状，其余照旧一刀切关掉（见 `phase1_window`）。
    if has_window_node(plan) && !phase1_window(plan) {
        return None;
    }
    let mut used: Vec<String> = Vec::new();
    collect_plan_columns(plan, &mut used);
    let def = plan_scan_def(plan)?;
    let all: Vec<String> = def.columns.iter().map(|c| c.name.clone()).collect();
    let mut proj: Vec<String> = all
        .iter()
        .filter(|n| {
            // ⚠️ `used` 里可能是**限定名**（`b.k`）、`all` 里是**裸名** ⇒ 比之前先剥
            // （同 `bare_col_name`；不剥的话带表限定的键列会被当成"没人读"**裁掉**，
            // 而它在 JOIN 里恰恰是必须留下的那一列）。
            used.iter()
                .any(|u| bare_col_name(u).eq_ignore_ascii_case(n))
        })
        .cloned()
        .collect();
    if proj.is_empty() {
        proj = all.first().cloned().into_iter().collect();
    }
    Some((all, proj))
}

/// 第一阶段放行的窗口形状。**其余含窗计划一律不裁。**
///
/// ```text
/// ( Limit | Project | Sort(Project(…)) )*  Window( Aggregate?( Filter?( Scan ) ) )
/// ```
///
/// ## 为什么只放行一种形状
///
/// `Window` 的输出 = **输入 ++ 窗口列**（它**追加**列）。裁剪落在窗口**之下**时，
/// 窗口**之上**的表达式会整体左移，而它们的 `Column.idx` 是按原宽度绑定的 ——
/// `rewrite_plan` 必须知道"左移了多少"才能把它们一起平移，而那个量**只能从下往上算**
/// （要到扫描处才定得下来）。本仓在这个"表达式活在哪个索引空间"的坑上已栽过三次
/// （回归就是 `tests/projection_space.rs`），故第一阶段只收这一种：
///
/// - **之上**只有 `Project`/`Limit`/`Sort(Project(…))` ⇒ 左移量由**结构**决定
///   （见 [`output_shift`]），不必按列名判 —— 按名判会撞车：窗口输出里的 `pdate`
///   与表列 `pdate` 同名却活在两个空间里（2026-10-16 那次"删掉 guard"的尝试就是
///   死在这里：`decimal window arg downcast failed`）；
/// - **之下**是 `Scan`/`Filter`/`Aggregate` ⇒ 三者的表达式都能精确定位；
/// - 全计划**恰好一个** `Window`（下面的链与 [`under_window_ok`] 已排除嵌套情形）。
///
/// ### `Sort` 那条为什么要写成 `Sort(Project(…))` 而不是裸 `Sort`
///
/// `rewrite_plan` 的 `Sort` 臂把排序键**原样保留**（键是"输入的输出序"，不是表列序，
/// 见那条注释与 `tests/order_by_prune.rs`）—— 而那条约定成立的前提是
/// "`Sort` 输入的空间**没被挪动过**"。`Project` 恰好满足：它的投影式被重写了，
/// 但**产出列的位置与含义一个都没变** ⇒ 键仍然指得对。
///
/// ⚠️ 反过来说 `Sort(Window(…))`（键直接活在窗口输出空间里）就**不安全**：
/// 那时键要按 [`rewrite_above_window_expr`] 改，而 `Sort` 臂是不改的。
/// 于是这里只认"`Sort` 紧贴在一个 `Project` 之上"这一种。
///
/// （`ORDER BY` 落在窗口查询外面是最常见的写法 —— `.14` 真表上正是它：
/// 裸 `Sort` 时 223 ms 一动不动，放行后 41 ms。）
///
/// 其余形状（之上还有 `Aggregate`/join、多个 `Window`、窗口接 join）
/// **继续关着**：每一个都得单独论证它的列空间。少裁一列只是变慢，
/// **裁错一列是静默错值**。
///
/// 回归：`tests/window_prune.rs`（反例必须仍然回 `None`）。
fn phase1_window(plan: &LogicalPlan) -> bool {
    let mut cur = plan;
    loop {
        match cur {
            LogicalPlan::Window { input, .. } => return under_window_ok(input),
            LogicalPlan::Project { input, .. } | LogicalPlan::Limit { input, .. } => cur = input,
            // 见上面"`Sort` 那条"：只收紧贴 `Project` 的 `Sort`
            LogicalPlan::Sort { input, .. } if matches!(**input, LogicalPlan::Project { .. }) => {
                cur = input
            }
            _ => return false,
        }
    }
}

/// 窗口**之下**允许的链。见 [`phase1_window`]。
///
/// **`Sort` 刻意不在内**：`rewrite_plan` 的 `Sort` 臂把排序键**原样保留**
/// （键是"输入的输出序"而非表列序，见那条注释）—— 那条约定成立的前提是
/// "`Sort` 的输入空间没被裁剪过"，窗口夹在中间会让它失效。
fn under_window_ok(p: &LogicalPlan) -> bool {
    match p {
        LogicalPlan::Scan { .. }
        | LogicalPlan::ExternalScan { .. }
        | LogicalPlan::CsvScan { .. } => true,
        // `Filter` 恒等 schema：它的谓词与 Scan 同处一个列空间
        LogicalPlan::Filter { input, .. } => under_window_ok(input),
        // 聚合**另起**一个列空间（输出宽度 = group ++ aggs，与裁剪无关）
        // ⇒ 它之上的窗口表达式一个字都不用改
        LogicalPlan::Aggregate { input, .. } => under_window_ok(input),
        _ => false,
    }
}

/// 全计划引用列名（Column 节点）。
fn collect_plan_columns(plan: &LogicalPlan, out: &mut Vec<String>) {
    match plan {
        // 下推进 Scan 的谓词也引用数据列——必须收集，否则投影被裁到只剩分区列
        // （M4 期 bug：数据列谓词静默失效——WHERE a>0 读不到 a 列按恒真处理）
        LogicalPlan::Scan { predicate, .. } | LogicalPlan::ExternalScan { predicate, .. } => {
            if let Some(p) = predicate {
                let mut cols = std::collections::HashSet::new();
                p.referenced_columns(&mut cols);
                for c in cols {
                    if !out.iter().any(|u| u.eq_ignore_ascii_case(&c)) {
                        out.push(c);
                    }
                }
            }
        }
        LogicalPlan::Filter { input, predicate } => {
            collect_expr_columns(predicate, out);
            collect_plan_columns(input, out);
        }
        LogicalPlan::Project { input, exprs, .. } => {
            for e in exprs {
                collect_expr_columns(e, out);
            }
            collect_plan_columns(input, out);
        }
        LogicalPlan::Aggregate { input, group, aggs } => {
            for g in group {
                collect_expr_columns(g, out);
            }
            for a in aggs {
                if let Some(arg) = &a.arg {
                    collect_expr_columns(arg, out);
                }
                // ⚠️ **FILTER 谓词引用的列也要算**（2026-10-15）：漏了它，扫描投影
                // 会把谓词要的列裁掉/换位 ⇒ 执行期按**旧索引**取列，越界 panic
                // （实测 `Trying to access ... len is 1 but the index is 1`）。
                if let Some(f) = &a.filter {
                    collect_expr_columns(f, out);
                }
            }
            collect_plan_columns(input, out);
        }
        LogicalPlan::Sort { input, keys } => {
            for k in keys {
                collect_expr_columns(&k.expr, out);
            }
            collect_plan_columns(input, out);
        }
        LogicalPlan::Limit { input, .. } | LogicalPlan::Distinct { input } => {
            collect_plan_columns(input, out);
        }
        LogicalPlan::Join {
            left,
            left_keys,
            join_type,
            ..
        } if join_type.is_semi_family() => {
            // semi 族输出 = 左 schema：可安全裁剪左扫描；连接键列必须保留
            for k in left_keys {
                collect_expr_columns(k, out);
            }
            collect_plan_columns(left, out);
        }
        LogicalPlan::Join { .. } | LogicalPlan::CrossJoinFilter { .. } => {
            // INNER/LEFT 多表计划不走列裁剪下推（build 的 count_scans guard 兜底）
        }
        LogicalPlan::Values { .. } | LogicalPlan::OneRow => {} // 内存源无需裁剪
        // ⚠️ `Window` **有 input**（此前和上面那两兄弟混在同一条"无输入"臂里，
        // 因为含窗计划一律不裁 ⇒ 那条臂是**死代码**）。现在要裁了，两处必须收全：
        // 1. 它输入里的列；
        // 2. 它自己的 `partition_by`/`order_by`/窗口函数参数 —— `Project(Window(Scan))`
        //    上层的投影只引用 `sum(x)` 这个**输出名**（不是表列 ⇒ 会被下面的过滤丢掉），
        //    窗口参数是**唯一**提到 `x` 的地方；漏了它 ⇒ 扫描裁剪过头 ⇒ 窗口读到 NULL。
        LogicalPlan::Window {
            input,
            partition_by,
            order_by,
            window_fns,
        } => {
            for p in partition_by {
                collect_expr_columns(p, out);
            }
            for k in order_by {
                collect_expr_columns(&k.expr, out);
            }
            for w in window_fns {
                for a in window_fn_args(w) {
                    collect_expr_columns(a, out);
                }
            }
            collect_plan_columns(input, out);
        }
        // CSV 外表：叶节点且无谓词字段（谓词不下推进扫描，见 CsvScan 注释）
        LogicalPlan::CsvScan { .. } => {}
        LogicalPlan::SetOp { left, right, .. } => {
            collect_plan_columns(left, out);
            collect_plan_columns(right, out);
        }
    }
}

fn collect_expr_columns(e: &PhysExpr, out: &mut Vec<String>) {
    match e {
        PhysExpr::Column { name, .. } => {
            if !out.iter().any(|n| n.eq_ignore_ascii_case(name.as_str())) {
                out.push(name.clone());
            }
        }
        PhysExpr::Lit(_) => {}
        PhysExpr::Arith { l, r, .. } | PhysExpr::Cmp { l, r, .. } => {
            collect_expr_columns(l, out);
            collect_expr_columns(r, out);
        }
        PhysExpr::And(a, b) | PhysExpr::Or(a, b) => {
            collect_expr_columns(a, out);
            collect_expr_columns(b, out);
        }
        PhysExpr::Not(a) | PhysExpr::IsNull(a, _) => collect_expr_columns(a, out),
        PhysExpr::Cast { e, .. } => collect_expr_columns(e, out),
        PhysExpr::Fn { args, .. } => args.iter().for_each(|a| collect_expr_columns(a, out)),
        PhysExpr::Case {
            conds, else_expr, ..
        } => {
            for (c, r) in conds {
                collect_expr_columns(c, out);
                collect_expr_columns(r, out);
            }
            collect_expr_columns(else_expr, out);
        }
        PhysExpr::Fold { args, .. } => args.iter().for_each(|e| collect_expr_columns(e, out)),
        PhysExpr::Concat(l, r) => {
            collect_expr_columns(l, out);
            collect_expr_columns(r, out);
        }
        PhysExpr::Like { e, .. } => collect_expr_columns(e, out),
    }
}

/// 计划内是否含 Window 节点（列裁剪跳过：窗口列 idx 假设全列序）。
fn has_window_node(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Window { .. } => true,
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Distinct { input } => has_window_node(input),
        _ => false,
    }
}

/// 计划根到叶的 Scan 表定义。
fn plan_scan_def(plan: &LogicalPlan) -> Option<Arc<crate::catalog::TableDef>> {
    match plan {
        LogicalPlan::Scan { def, .. }
        | LogicalPlan::ExternalScan { def, .. }
        | LogicalPlan::CsvScan { def, .. } => Some(Arc::clone(def)),
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Distinct { input } => plan_scan_def(input),
        LogicalPlan::Join {
            left, join_type, ..
        } if join_type.is_semi_family() => plan_scan_def(left),
        LogicalPlan::Join { .. }
        | LogicalPlan::CrossJoinFilter { .. }
        | LogicalPlan::Values { .. }
        | LogicalPlan::OneRow
        | LogicalPlan::SetOp { .. } => None,
        LogicalPlan::Window { input, .. } => plan_scan_def(input),
    }
}

/// 全列名 → 投影列名 的 idx 映射（大小写不敏感）。
fn build_mapping(all: &[String], proj: &[String]) -> Vec<Option<usize>> {
    all.iter()
        .map(|a| proj.iter().position(|p| p.eq_ignore_ascii_case(a.as_str())))
        .collect()
}

/// 计划的**输出 schema 是不是就是被裁剪那张表的原始 schema**。
///
/// 只有 `Scan` 自己，以及它上面一串**恒等 schema** 的节点（Filter/Sort/Limit/
/// Distinct —— 它们只改行、不改列）才是；`Project`/`Aggregate`/`Window` 会
/// 重定义列空间，从那里往下就不再是表空间。
fn out_schema_is_table(p: &LogicalPlan) -> bool {
    match p {
        // **三个扫描叶都在此列**——`plan_scan_def` 认它们、裁剪也会落在它们身上，
        // 漏掉一个就等于把它上面的表达式**全部跳过重映射**（这次差点漏掉外表两兄弟，
        // `tests/external_parquet.rs` 立刻炸了）。加新的扫描叶时**必须同步加到这里**。
        LogicalPlan::Scan { .. }
        | LogicalPlan::ExternalScan { .. }
        | LogicalPlan::CsvScan { .. } => true,
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Distinct { input } => out_schema_is_table(input),
        // **semi/anti 族的输出就是左输入的 schema**（右表只出掩码，不出列）
        // ⇒ 也是恒等。漏了这条，`SELECT v FROM t WHERE g IN (…)` 的投影就不重映射，
        // 而扫描已被裁到 2 列 ⇒ `index out of bounds: len 2 index 2`。
        LogicalPlan::Join {
            left, join_type, ..
        } if join_type.is_semi_family() => out_schema_is_table(left),
        _ => false,
    }
}

/// 本节点的**直接输入**（叶返回 `None`）。
///
/// 与 [`exprs_are_in_table_space`] 同一条前提：表达式是按**自己输入的输出序**编号的，
/// 所以"表达式活在哪个空间"这个问题问的就是它的输入是谁。
fn direct_input(p: &LogicalPlan) -> Option<&LogicalPlan> {
    match p {
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Distinct { input }
        | LogicalPlan::Window { input, .. } => Some(input),
        LogicalPlan::Join {
            left, join_type, ..
        } if join_type.is_semi_family() => Some(left),
        _ => None,
    }
}

/// 本节点**输出**的列序相对原计划左移了几列（列裁剪的产物）。
///
/// 只有"原样透传输入的列"的节点会把左移量传下去：
///
/// | 节点 | 输出左移量 |
/// |---|---|
/// | 扫描叶 | `k`（= 被裁掉的列数） |
/// | `Filter`/`Sort`/`Limit`/`Distinct` | 继承输入 |
/// | `Window` | 继承输入（窗口列是**追加在尾部**的，不挪动已有列） |
/// | semi/anti `Join` | 继承左子（输出 = 左 schema） |
/// | `Aggregate`/`Project`/其它 `Join` | **0** —— 它们**另起**列空间，宽度与列序都与裁剪无关 |
///
/// 这个量就是 `rewrite_plan` 判断"表达式的 idx 该怎么改"的依据：表达式活在自己
/// 输入的输出空间里 ⇒ 问左移量就是问 [`direct_input`]。注意它与
/// [`out_schema_is_table`] 差在 `Window` 上：`Window(Scan)` 的左移量是 `k`，
/// 但它的输出**不是**表空间 —— 那个差值正是要走 [`rewrite_above_window_expr`]
/// 那条"分两段"的路（而不是一个统一平移，见那里）。
fn output_shift(p: &LogicalPlan, k: usize) -> usize {
    match p {
        LogicalPlan::Scan { .. }
        | LogicalPlan::ExternalScan { .. }
        | LogicalPlan::CsvScan { .. } => k,
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Distinct { input }
        | LogicalPlan::Window { input, .. } => output_shift(input, k),
        LogicalPlan::Join {
            left, join_type, ..
        } if join_type.is_semi_family() => output_shift(left, k),
        _ => 0,
    }
}

/// 该节点**自身的表达式**（谓词 / 投影式 / 分组键 / 聚合参数）是否按**表列序**编号。
///
/// 表达式永远是对**自己输入的输出**求值 ⇒ 当且仅当那个输出就是表空间。
///
/// 这个判定替代了原先"**按列名猜**"的做法（`all.contains(name)`），后者在
/// 「上下两层列名相同、但列空间不同」的形状上会**拿外层的位置去查表空间映射**，
/// 静默取到隔壁那一列。实测（2026-10-14，先于本次改动就存在）：
///
/// ```sql
/// SELECT g, x FROM (SELECT DISTINCT g, x FROM t)   -- t(id,g,x)
/// -- 实际回 (1,1),(1,1),(2,2),(3,3),(3,3)：x 列变成了 g 列（mapping[1] = g 的投影位）
/// SELECT g, count(x) FROM (SELECT DISTINCT g, x FROM t) GROUP BY g
/// -- 实际回 2|1、3|2（正确是 2|0、3|1）——NULL 组被算进了 count
/// ```
///
/// 同类前科：`Sort` 的键也踩过（见 `rewrite_plan` 的 `Sort` 臂注释，
/// 回归 `tests/order_by_prune.rs`）。回归：`tests/projection_space.rs`。
fn exprs_are_in_table_space(plan: &LogicalPlan) -> bool {
    match plan {
        // 扫描谓词是对**自己**的列求值（而且按列名寻址，见 `scan::Predicate`）
        LogicalPlan::Scan { .. }
        | LogicalPlan::ExternalScan { .. }
        | LogicalPlan::CsvScan { .. } => true,
        LogicalPlan::Filter { input, .. }
        | LogicalPlan::Project { input, .. }
        | LogicalPlan::Aggregate { input, .. }
        | LogicalPlan::Sort { input, .. }
        | LogicalPlan::Limit { input, .. }
        | LogicalPlan::Distinct { input } => out_schema_is_table(input),
        // 窗口**自己不读**窗口列：`partition_by`/`order_by`/函数参数都是对它**输入的列**
        // 求值（窗口列是它**追加**到输出尾部的）⇒ 与 `Filter` 同判。
        // ⚠️ 这条只对"输入就是表空间"成立：`Window(Aggregate(Scan))` 里窗口的
        // `ORDER BY pdate` 引用的是**聚合输出**的 `pdate`，而 `pdate` 碰巧也是表列名
        // ⇒ 必须靠这条结构性判据挡住按名映射（那就是 H1）。
        LogicalPlan::Window { input, .. } => out_schema_is_table(input),
        _ => false,
    }
}

/// 重写计划中所有 Column.idx（全表列序 → 投影序）。
/// `all`：表全列名——非表列（聚合输出列如 sum(x)）保持原 idx。
fn rewrite_plan(
    plan: &LogicalPlan,
    mapping: &[Option<usize>],
    all: &[String],
    k: usize,
) -> LogicalPlan {
    // 表达式的 idx 活在**自己输入的输出空间**里（同 `exprs_are_in_table_space` 的前提），
    // 所以"该怎么改"由那个空间的左移量三选一：
    //   · 没动（0）⇒ 一个字都不用改；
    //   · 仍是**表空间** ⇒ 按 `mapping` 换算（含重排）；
    //   · 是**窗口输出空间** ⇒ 分两段，见 `rewrite_above_window_expr`。
    //
    // 第三种是本文件最容易错的一处（见 `output_shift` 的表格）：它**不是**一个统一平移。
    // 旧 `Window` 臂在这里无条件按名重写，2026-10-16 实测炸出
    // `decimal window arg downcast failed`。宁可少裁一列，也不能把表达式指到隔壁列上去。
    let space = direct_input(plan).map_or(0, |i| output_shift(i, k));
    let tbl = space > 0 && exprs_are_in_table_space(plan);
    let re = |e: &PhysExpr| -> PhysExpr {
        if space == 0 {
            e.clone()
        } else if tbl {
            rewrite_expr(e, mapping, all)
        } else {
            rewrite_above_window_expr(e, mapping, all, space)
        }
    };
    match plan {
        LogicalPlan::Scan {
            def,
            predicate,
            read,
        } => LogicalPlan::Scan {
            def: Arc::clone(def),
            predicate: predicate.clone(),
            read: read.clone(),
        },
        LogicalPlan::Join {
            left,
            right,
            left_keys,
            right_keys,
            join_type,
            needed,
            residual,
        } if join_type.is_semi_family() => LogicalPlan::Join {
            left: Box::new(rewrite_plan(left, mapping, all, k)),
            // 右键为右子计划局部 idx（绑定期约定），裁剪不触碰
            right: Box::new((**right).clone()),
            // ⚠️ 左键**无条件**按表列序重写（不要按 `out_schema_is_table(left)` 加闸门：
            // 实测 4 条 subquery 用例立刻 `index out of bounds` / 回空 —— semi 的左键
            // 在 plan_scan_def 认的那些形状下就是表列序，加了闸门反而漏重映射）。
            left_keys: left_keys
                .iter()
                .map(|k| rewrite_expr(k, mapping, all))
                .collect(),
            right_keys: right_keys.clone(),
            join_type: *join_type,
            needed: needed.clone(),
            // 残留与左键同一索引空间 ⇒ 同样无条件重写（semi 族恒为 None，留着是给
            // 将来复用这条分支时不留坑）
            residual: residual.as_ref().map(|r| rewrite_expr(r, mapping, all)),
        },
        LogicalPlan::ExternalScan {
            def,
            files,
            predicate,
        } => LogicalPlan::ExternalScan {
            def: Arc::clone(def),
            files: Arc::clone(files),
            predicate: predicate.clone(),
        },
        // CSV 外表：同 ExternalScan——裁剪只重排上层表达式的列 idx，
        // 扫描节点自身保持全表 def（schema 由 def 决定）
        LogicalPlan::CsvScan { def, files } => LogicalPlan::CsvScan {
            def: Arc::clone(def),
            files: Arc::clone(files),
        },
        LogicalPlan::Filter { input, predicate } => LogicalPlan::Filter {
            input: Box::new(rewrite_plan(input, mapping, all, k)),
            predicate: re(predicate),
        },
        LogicalPlan::Project {
            input,
            exprs,
            names,
        } => LogicalPlan::Project {
            input: Box::new(rewrite_plan(input, mapping, all, k)),
            exprs: exprs.iter().map(&re).collect(),
            names: names.clone(),
        },
        LogicalPlan::Aggregate { input, group, aggs } => LogicalPlan::Aggregate {
            input: Box::new(rewrite_plan(input, mapping, all, k)),
            group: group.iter().map(&re).collect(),
            aggs: aggs
                .iter()
                .map(|a| crate::expr::AggExpr {
                    func: a.func,
                    arg: a.arg.as_ref().map(&re),
                    out_name: a.out_name.clone(),
                    out_ty: a.out_ty.clone(),
                    udaf: a.udaf.clone(),
                    distinct: a.distinct,
                    // FILTER 谓词引用的是**表列**，与 arg 同一个索引空间，一起重写
                    filter: a.filter.as_ref().map(&re),
                })
                .collect(),
        },
        LogicalPlan::Sort { input, keys } => LogicalPlan::Sort {
            input: Box::new(rewrite_plan(input, mapping, all, k)),
            // **排序键不重写**——它们是**输入自身的输出序**，不是表列序。
            //
            // 两个索引空间不能混：`mapping` 是「表列序 → 扫描投影序」（列裁剪用），
            // 而排序键的 idx 指的是 `Sort` 输入的输出位置（translate 里按顶层投影的
            // 输出名/序号绑定；集合操作路径按 `lc` 的输出序）。此前这里照 `mapping`
            // 重写，等于拿输出号去查表列映射，**静默排错**：
            //   `SELECT v, w FROM t ORDER BY v, w`（t 有 g,v,w）——键 `w` 的输出号 1
            //   被查成「表列 g 映射到的投影位置」→ 两键都变成同一列 → v 的并列**完全
            //   不按 w 排**（无报错、顺序错）。
            // 回归：`tests/order_by_prune.rs`。
            keys: keys.clone(),
        },
        LogicalPlan::Limit { input, n, offset } => LogicalPlan::Limit {
            input: Box::new(rewrite_plan(input, mapping, all, k)),
            n: *n,
            offset: *offset,
        },
        LogicalPlan::Distinct { input } => LogicalPlan::Distinct {
            input: Box::new(rewrite_plan(input, mapping, all, k)),
        },
        LogicalPlan::Join { .. }
        | LogicalPlan::CrossJoinFilter { .. }
        | LogicalPlan::Values { .. }
        | LogicalPlan::OneRow => plan.clone(),
        LogicalPlan::SetOp {
            left,
            right,
            op,
            all: all_flag,
        } => LogicalPlan::SetOp {
            left: Box::new(rewrite_plan(left, mapping, all, k)),
            right: Box::new(rewrite_plan(right, mapping, all, k)),
            op: *op,
            all: *all_flag,
        },
        LogicalPlan::Window {
            input,
            partition_by,
            order_by,
            window_fns,
        } => LogicalPlan::Window {
            input: Box::new(rewrite_plan(input, mapping, all, k)),
            // 窗口的表达式活在**它输入**的空间里 ⇒ 与上面同一套判据（`re`）。
            // 此前这里无条件按名重写（H1：`Window(Aggregate(…))` 的 `ORDER BY pdate`
            // 会拿聚合空间的位置去查表映射），而 `window_fns` 更是直接 `clone()`
            // （`Window(Scan)` 的参数列会悬空）。
            partition_by: partition_by.iter().map(&re).collect(),
            order_by: order_by
                .iter()
                .map(|sk| crate::exec::SortKey {
                    expr: re(&sk.expr),
                    descending: sk.descending,
                    nulls_first: sk.nulls_first,
                })
                .collect(),
            window_fns: window_fns.iter().map(|w| map_window_fn(w, &re)).collect(),
        },
    }
}

fn rewrite_expr(e: &PhysExpr, mapping: &[Option<usize>], all: &[String]) -> PhysExpr {
    map_expr_idx(e, &|idx, name| {
        // 非表列（聚合输出列等）：idx 语义属上层算子，保持不变
        if all
            .iter()
            .any(|a| a.eq_ignore_ascii_case(bare_col_name(name)))
        {
            mapping[idx].unwrap_or(idx)
        } else {
            idx
        }
    })
}

/// 列名的**裸名**（去掉表限定：`b.k` ⇒ `k`）。
///
/// ⚠️ **按列名判"这一列是不是表列"之前必须先剥限定符** —— JOIN 右表的列在计划里
/// 是**限定名**（`translate` 绑 `CompoundIdentifier` 时把 `b.k` 原样存进 `Column.name`），
/// 而 `def.columns` / `mapping` 用的都是**裸名**。不剥就永远判"不是表列"，于是
/// `unwrap_or(idx)` **把旧索引原样留下**，静默指到隔壁列。
///
/// 实测（2026-10-16，两张同构真表 —— 电力日清的"日前 vs 实际"）：
/// `SELECT a.k, b.v FROM a JOIN b ON a.k=b.k` **静默回 0 行**、
/// `SELECT a.k, b.k FROM …` 直接 `index out of bounds: len 1 index 1`（panic）。
///
/// 剥是安全的：本仓列名**不许带 `.`**（见 `ident_ok`），所以"带点"必然是限定引用。
/// （`build_aggregate` 的键投影里早就在剥了 —— 见那句 `n.rsplit('.').next()` ——
/// 只是 `rewrite_expr` 这条路上漏了。))
fn bare_col_name(name: &str) -> &str {
    name.rsplit('.').next().unwrap_or(name)
}

/// "窗口**之上**的表达式"的 idx 改写：它们活在**窗口输出空间**里
/// （`Window` 输出 = 输入 ++ 窗口列），而那个空间分两段，**两段的下场不同**：
///
/// | 位置 | 是什么 | 怎么改 |
/// |---|---|---|
/// | `[0, all.len())` | **透传列**：窗口原样带下来的输入列 | 位置仍是表列序 ⇒ 按 `mapping` |
/// | `[all.len(), …)` | **追加的窗口列**（`sum(x)` 这类输出名） | 在**尾部**，扫描裁掉几列就整体左移几列 |
///
/// ## 两处容易想错的地方
///
/// 1. **不能一律平移**。第一版按"整段 −k"写，结果透传列 `i` 也被减了 —— 读成隔壁
///    `g` 列（`window_frame` 的 `count_星号数行而_count_列数非空` 当场变红：
///    首列变成恒定的 1）。分界线是**位置**，不是"整个空间挪了多少"。
/// 2. **分界线用 `all.len()` 是精确的，不是近似**：能走到这一支 ⟺ 窗口的输入是一条
///    **表空间链**（`output_shift` > 0 且 `out_schema_is_table` 为假，只可能是
///    `Window(表空间链)`）⇒ 窗口输入的宽度**就是**表宽，前一段正是表列序。
///    （输入是聚合时左移量为 0，整条走 `clone`，压根到不了这里。）
///
/// 也因此这里**不需要**按列名判"这一列是不是表列"（那正是 H1 撞车的地方）——
/// 位置本身就是判据。
fn rewrite_above_window_expr(
    e: &PhysExpr,
    mapping: &[Option<usize>],
    all: &[String],
    shift: usize,
) -> PhysExpr {
    map_expr_idx(e, &|idx, _| {
        if idx < all.len() {
            mapping[idx].unwrap_or(idx)
        } else {
            idx.saturating_sub(shift)
        }
    })
}

/// `WindowFn` 的**表达式参数**（`row_number()`/`count(*)` 这类没有参数）。
fn window_fn_args(w: &WindowFn) -> Vec<&PhysExpr> {
    match w {
        WindowFn::RowNumber | WindowFn::Rank | WindowFn::DenseRank | WindowFn::Count(None, _) => {
            Vec::new()
        }
        WindowFn::Sum(e, _) | WindowFn::Min(e, _) | WindowFn::Max(e, _) | WindowFn::Avg(e, _) => {
            vec![e]
        }
        WindowFn::Count(Some(e), _) => vec![e],
        WindowFn::Lag(e, _, d) | WindowFn::Lead(e, _, d) => match d {
            Some(d) => vec![e, d.as_ref()],
            None => vec![e],
        },
    }
}

/// 按 `f` 改写 `WindowFn` 的表达式参数（函数本身、帧一概不动）。
fn map_window_fn(w: &WindowFn, f: &dyn Fn(&PhysExpr) -> PhysExpr) -> WindowFn {
    match w {
        WindowFn::RowNumber | WindowFn::Rank | WindowFn::DenseRank => w.clone(),
        WindowFn::Sum(e, fr) => WindowFn::Sum(f(e), *fr),
        WindowFn::Count(a, fr) => WindowFn::Count(a.as_ref().map(f), *fr),
        WindowFn::Min(e, fr) => WindowFn::Min(f(e), *fr),
        WindowFn::Max(e, fr) => WindowFn::Max(f(e), *fr),
        WindowFn::Avg(e, fr) => WindowFn::Avg(f(e), *fr),
        WindowFn::Lag(e, o, d) => WindowFn::Lag(f(e), *o, d.as_ref().map(|d| Box::new(f(d)))),
        WindowFn::Lead(e, o, d) => WindowFn::Lead(f(e), *o, d.as_ref().map(|d| Box::new(f(d)))),
    }
}

/// 把表达式里所有 `Column.idx` 按 `f(idx, name)` 改写（结构 / 列名 / 类型一概不动）。
///
/// 抽出来是因为"改 idx"在本文件里有**两套判据**（[`rewrite_expr`] / [`shift_expr`]），
/// 而它们对表达式的遍历必须一字不差 —— 漏掉一个变体就是**静默漏改一列**。
fn map_expr_idx(e: &PhysExpr, f: &dyn Fn(usize, &str) -> usize) -> PhysExpr {
    match e {
        PhysExpr::Column { idx, name, ty } => PhysExpr::Column {
            idx: f(*idx, name),
            name: name.clone(),
            ty: ty.clone(),
        },
        PhysExpr::Lit(l) => PhysExpr::Lit(l.clone()),
        PhysExpr::Arith { op, l, r, ty } => PhysExpr::Arith {
            op: *op,
            l: Box::new(map_expr_idx(l, f)),
            r: Box::new(map_expr_idx(r, f)),
            ty: ty.clone(),
        },
        PhysExpr::Cmp { op, l, r } => PhysExpr::Cmp {
            op: *op,
            l: Box::new(map_expr_idx(l, f)),
            r: Box::new(map_expr_idx(r, f)),
        },
        PhysExpr::And(a, b) => {
            PhysExpr::And(Box::new(map_expr_idx(a, f)), Box::new(map_expr_idx(b, f)))
        }
        PhysExpr::Or(a, b) => {
            PhysExpr::Or(Box::new(map_expr_idx(a, f)), Box::new(map_expr_idx(b, f)))
        }
        PhysExpr::Not(a) => PhysExpr::Not(Box::new(map_expr_idx(a, f))),
        PhysExpr::IsNull(a, neg) => PhysExpr::IsNull(Box::new(map_expr_idx(a, f)), *neg),
        PhysExpr::Cast { e, ty } => PhysExpr::Cast {
            e: Box::new(map_expr_idx(e, f)),
            ty: ty.clone(),
        },
        PhysExpr::Fn { name, args, ty } => PhysExpr::Fn {
            name: name.clone(),
            args: args.iter().map(|a| map_expr_idx(a, f)).collect(),
            ty: ty.clone(),
        },
        PhysExpr::Case {
            conds,
            else_expr,
            ty,
        } => PhysExpr::Case {
            conds: conds
                .iter()
                .map(|(c, r)| (map_expr_idx(c, f), map_expr_idx(r, f)))
                .collect(),
            else_expr: Box::new(map_expr_idx(else_expr, f)),
            ty: ty.clone(),
        },
        PhysExpr::Fold { kind, args, ty } => PhysExpr::Fold {
            kind: *kind,
            args: args.iter().map(|e| map_expr_idx(e, f)).collect(),
            ty: ty.clone(),
        },
        PhysExpr::Concat(l, r) => {
            PhysExpr::Concat(Box::new(map_expr_idx(l, f)), Box::new(map_expr_idx(r, f)))
        }
        PhysExpr::Like {
            e,
            pattern,
            negated,
            ci,
        } => PhysExpr::Like {
            e: Box::new(map_expr_idx(e, f)),
            pattern: pattern.clone(),
            negated: *negated,
            ci: *ci,
        },
    }
}

fn build_inner(
    db: &Qsdb,
    plan: &LogicalPlan,
    scan_proj: Option<Vec<String>>,
    ctx: &Arc<crate::query::QueryContext>,
) -> Result<Box<dyn ExecNode>> {
    match plan {
        LogicalPlan::Scan {
            def,
            predicate,
            read,
        } => {
            let _ = read;
            // 分片重写（见 `ShardScope`）：命中就用分片，**按当前调用点的投影重建**
            // （join 的键列投影下推会让同一 Scan 在不同调用点要不同的投影；
            // 直接复用会列错位——`Scan::with_projection` 的注释里有详细说明）。
            // 分片任务内**串行**拉取（ScanExec，不用 ParScanExec）：嵌套并行会与
            // 分片任务互锁同一 rayon 池（M4 教训）。
            if let Some(shard) = take_shard_override(&def.name) {
                // 分片沿用分片的文件划分，**投影按当前调用点重建**，再叠"只读这些列"
                let mut scan = shard.with_projection(scan_proj)?;
                if let Some(keep) = read {
                    scan = scan.restrict_read(keep);
                }
                return Ok(Box::new(ScanExec::new(scan)));
            }
            let mut scan = db.scan(ScanRequest {
                table: def.name.clone(),
                predicate: predicate.clone(),
                projection: scan_proj,
                ..Default::default()
            })?;
            // ★ 只解码下游真读的列（见 `annotate_join_needs` / `Scan::restrict_read`）：
            // schema 与列序不变，未列出的列产出 NULL ⇒ 上层索引一个字都不用改。
            if let Some(keep) = read {
                scan = scan.restrict_read(keep);
            }
            Ok(crate::exec::par_scan::make_scan(scan, ctx))
        }
        LogicalPlan::ExternalScan {
            def,
            files,
            predicate,
        } => {
            let scan = db.external_scan(
                Arc::clone(def),
                files.as_ref().clone(),
                scan_proj,
                predicate.clone(),
                Arc::clone(ctx),
            )?;
            Ok(crate::exec::par_scan::make_scan(scan, ctx))
        }
        LogicalPlan::CsvScan { def, files } => Ok(Box::new(crate::csv::CsvScanExec::new(
            Arc::clone(def),
            files.as_ref().clone(),
            scan_proj,
            Arc::clone(ctx),
        )?)),
        LogicalPlan::Filter { input, predicate } => Ok(Box::new(FilterExec::new(
            build_inner(db, input, scan_proj.clone(), ctx)?,
            predicate.clone(),
            Arc::clone(ctx),
        ))),
        LogicalPlan::Project {
            input,
            exprs,
            names,
        } => {
            // 投影下推到 Join 两侧：物化 join 是冷读口径下最大差距项
            // （5 列表比 2 列表慢 1.8x，实测）。仅两侧均为无谓词 Scan 时启用。
            if let LogicalPlan::Join {
                left,
                right,
                left_keys,
                right_keys,
                join_type,
                residual,
                ..
            } = input.as_ref()
            {
                if let Some(node) = try_project_join(
                    db,
                    left,
                    right,
                    left_keys,
                    right_keys,
                    *join_type,
                    exprs,
                    names,
                    residual.as_ref(),
                    ctx,
                )? {
                    return Ok(node);
                }
            }
            Ok(Box::new(ProjectExec::new(
                build_inner(db, input, scan_proj.clone(), ctx)?,
                exprs.clone(),
                names.clone(),
            )))
        }
        LogicalPlan::Aggregate { input, group, aggs } => {
            // ★ join 输出投影下推（P1⑥）：**聚合是 join 输出唯一的读者**，
            // 且它的表达式可枚举 ⇒ "哪几列没人读"是**可证的**（不是猜的）。
            // 标注结果只影响"join 少物化那几列"（填 NULL），schema 与索引**一概不动**。
            // `scan_proj.is_none()` = 计划没被外层裁剪过 ⇒ 位置在全表空间，可下推到 Scan
            let narrowed = annotate_join_needs(input, group, aggs, scan_proj.is_none());
            build_aggregate(db, &narrowed, group, aggs, &scan_proj, ctx)
        }
        LogicalPlan::Sort { input, keys } => Ok(Box::new(SortExec::new(
            build_inner(db, input, scan_proj.clone(), ctx)?,
            keys.clone(),
            None,
            Arc::clone(ctx),
        ))),
        LogicalPlan::Limit { input, n, offset } => {
            // `ORDER BY … LIMIT n [OFFSET m]` → **TopN 接线**：把 `n + m` 交给 `SortExec`
            // （前 m 行反正要丢掉，故上界是 n+m 而非 n），再由 `LimitExec` 跳过 m。
            // 此前恒传 `None`（见上面 Sort 分支）→ 全量物化 + 全排序后才截断，
            // 而 `SortExec` 的 limit 一直有能力做"部分排序 + 有界缓冲"（早先没人接）。
            //
            // 两种形状都要认：`Sort`，以及 `Project{Sort}`——后者是 ORDER BY 引用了
            // **未投影**列时，translate 在 Sort 之上补的"收回隐藏排序列"投影
            // （`SELECT v FROM t ORDER BY w`）。
            //
            // ⚠️ **offset 非 0 时不能省掉 Limit 节点**（那会多返回前 m 行——
            // 这正是"OFFSET 被静默丢掉"那个 bug 的形态）；`n` 为 `None`（只写
            // `OFFSET m`）时 TopN 没有上界，退化为全排序 + 跳过。
            let (n, offset) = (*n, *offset);
            let topn = n.map(|k| k.saturating_add(offset));
            let wrap = |sorted: Box<dyn ExecNode>| -> Box<dyn ExecNode> {
                if offset == 0 {
                    sorted
                } else {
                    Box::new(LimitExec {
                        input: sorted,
                        remaining: n,
                        skip: offset,
                    })
                }
            };
            match input.as_ref() {
                LogicalPlan::Sort { input: inner, keys } => {
                    return Ok(wrap(Box::new(SortExec::new(
                        build_inner(db, inner, scan_proj.clone(), ctx)?,
                        keys.clone(),
                        topn,
                        Arc::clone(ctx),
                    ))));
                }
                LogicalPlan::Project {
                    input: p_in,
                    exprs,
                    names,
                } => {
                    if let LogicalPlan::Sort { input: inner, keys } = p_in.as_ref() {
                        let sorted = SortExec::new(
                            build_inner(db, inner, scan_proj.clone(), ctx)?,
                            keys.clone(),
                            topn,
                            Arc::clone(ctx),
                        );
                        return Ok(wrap(Box::new(ProjectExec::new(
                            Box::new(sorted),
                            exprs.clone(),
                            names.clone(),
                        ))));
                    }
                }
                _ => {}
            }
            Ok(Box::new(LimitExec {
                input: build_inner(db, input, scan_proj.clone(), ctx)?,
                remaining: n,
                skip: offset,
            }))
        }
        LogicalPlan::Distinct { input } => Ok(Box::new(crate::exec::DistinctExec::new(
            build_inner(db, input, scan_proj.clone(), ctx)?,
            Arc::clone(ctx),
        )?)),
        LogicalPlan::Window {
            input,
            partition_by,
            order_by,
            window_fns,
        } => {
            // ⚠️ **必须把裁剪投影传下去**（此前硬编码 `None`）。窗口自己在窗口列之上
            // 的表达式已经按裁剪后的空间重写好了（`rewrite_plan` 的 `Window` 臂），
            // 这里若传 `None`，扫描会读回全宽批 ⇒ 重写过的 idx（如 `pdate` → 0）
            // 会指到**另一个列**上 —— 而且不报错。窗口输出 = 输入 ++ 窗口列，
            // 输入窄了，上面那层 Project 也已经在同一个窄空间里，两边自洽。
            let input_exec = build_inner(db, input, scan_proj.clone(), ctx)?;
            Ok(Box::new(crate::exec::window::WindowExec::new(
                input_exec,
                partition_by.clone(),
                order_by.clone(),
                window_fns.clone(),
                Arc::clone(ctx),
            )))
        }
        LogicalPlan::Values { columns, cte_id } => {
            let schema = Arc::new(arrow::datatypes::Schema::new(
                columns
                    .iter()
                    .map(|(n, t)| arrow::datatypes::Field::new(n, t.arrow_type(), true))
                    .collect::<Vec<_>>(),
            ));
            let batches = CTE_BATCHES
                .with(|c| c.borrow().map.get(cte_id).cloned())
                .unwrap_or_default();
            Ok(Box::new(crate::exec::ValuesExec::new(schema, batches)))
        }
        LogicalPlan::OneRow => Ok(Box::new(crate::exec::OneRowExec::new())),
        LogicalPlan::SetOp {
            left,
            right,
            op,
            all,
        } => {
            // 集合操作内含多 Scan：不走列裁剪（build 侧 scan_proj=None 传全列）
            let l = build_inner(db, left, None, ctx)?;
            let r = build_inner(db, right, None, ctx)?;
            let exec = crate::exec::SetOpExec::new(l, r, *op, ctx)?;
            match (*op, *all) {
                // UNION distinct → DistinctExec 包装去重
                (crate::exec::SetOpKind::Union, false) => Ok(Box::new(
                    crate::exec::DistinctExec::new(Box::new(exec), Arc::clone(ctx))?,
                )),
                _ => Ok(Box::new(exec)),
            }
        }
        LogicalPlan::CrossJoinFilter {
            left,
            right,
            predicate,
        } => {
            let probe = build_inner(db, left, None, ctx)?;
            let build = build_inner(db, right, None, ctx)?;
            // cross：probe 批 × build 全表，再按谓词过滤
            // 实现为：FilterExec { input: CrossJoinExec } —— CrossJoinExec 内联在此
            let cross = crate::exec::join::CrossJoinExec::new(probe, build, Arc::clone(ctx));
            Ok(Box::new(FilterExec::new(
                Box::new(cross),
                predicate.clone(),
                Arc::clone(ctx),
            )))
        }
        LogicalPlan::Join {
            left,
            right,
            left_keys,
            right_keys,
            join_type,
            needed,
            residual,
        } => {
            // semi 族：左子继承裁剪投影（build() 对 semi 计划做了列裁剪——
            // 传 None 会读全列使左键 idx 错位；INNER/LEFT 无上游裁剪，None 等价）
            let probe = build_inner(
                db,
                left,
                if join_type.is_semi_family() {
                    scan_proj.clone()
                } else {
                    None
                },
                ctx,
            )?;
            let build = build_inner(db, right, None, ctx)?;
            // 右键 idx：INNER/LEFT 为全局 ctx 序（减 probe 宽度得右局部序）；
            // semi 族绑定期即用右子计划局部序（列裁剪改写左宽后仍正确）
            let local_keys: Vec<PhysExpr> = if join_type.is_semi_family() {
                right_keys.clone()
            } else {
                let left_width = probe.schema().fields().len();
                right_keys
                    .iter()
                    .map(|k| match k {
                        PhysExpr::Column { idx, name, ty } => PhysExpr::Column {
                            idx: idx.saturating_sub(left_width),
                            name: name.clone(),
                            ty: ty.clone(),
                        },
                        other => other.clone(),
                    })
                    .collect()
            };
            if needed.is_some() && std::env::var("QSDB_JOIN_PROF").is_ok() {
                eprintln!(
                    "[join-proj] 输出列 {} → 只物化 {}（其余填 NULL）",
                    probe.schema().fields().len() + build.schema().fields().len(),
                    needed.as_ref().map_or(0, |n| n.len())
                );
            }
            Ok(Box::new(
                crate::exec::join::HashJoinExec::new(
                    probe,
                    build,
                    left_keys.clone(),
                    local_keys,
                    *join_type,
                    Arc::clone(ctx),
                )
                // ★ join 输出投影下推（P1⑥）：只物化下游真读的列（见 `annotate_join_needs`）
                .with_needed(needed.clone())
                // 键之外的 ON 条件：**必须在 join 内部对匹配对求值**
                .with_residual(residual.clone()),
            ))
        }
    }
}

/// Join 输出（左 ++ 右）的 idx → 投影后序（左投影 ++ 右投影）。
/// `lw` = 原左宽，`nl` = 新左宽（左投影列数）。
fn remap_join_expr(
    e: &PhysExpr,
    lw: usize,
    nl: usize,
    lmap: &[Option<usize>],
    rmap: &[Option<usize>],
) -> PhysExpr {
    match e {
        PhysExpr::Column { idx, name, ty } => {
            let new_idx = if *idx < lw {
                lmap.get(*idx).copied().flatten().unwrap_or(*idx)
            } else {
                let r = *idx - lw;
                nl + rmap.get(r).copied().flatten().unwrap_or(r)
            };
            PhysExpr::Column {
                idx: new_idx,
                name: name.clone(),
                ty: ty.clone(),
            }
        }
        PhysExpr::Lit(l) => PhysExpr::Lit(l.clone()),
        PhysExpr::Arith { op, l, r, ty } => PhysExpr::Arith {
            op: *op,
            l: Box::new(remap_join_expr(l, lw, nl, lmap, rmap)),
            r: Box::new(remap_join_expr(r, lw, nl, lmap, rmap)),
            ty: ty.clone(),
        },
        PhysExpr::Cmp { op, l, r } => PhysExpr::Cmp {
            op: *op,
            l: Box::new(remap_join_expr(l, lw, nl, lmap, rmap)),
            r: Box::new(remap_join_expr(r, lw, nl, lmap, rmap)),
        },
        PhysExpr::And(a, b) => PhysExpr::And(
            Box::new(remap_join_expr(a, lw, nl, lmap, rmap)),
            Box::new(remap_join_expr(b, lw, nl, lmap, rmap)),
        ),
        PhysExpr::Or(a, b) => PhysExpr::Or(
            Box::new(remap_join_expr(a, lw, nl, lmap, rmap)),
            Box::new(remap_join_expr(b, lw, nl, lmap, rmap)),
        ),
        PhysExpr::Not(a) => PhysExpr::Not(Box::new(remap_join_expr(a, lw, nl, lmap, rmap))),
        PhysExpr::IsNull(a, n) => {
            PhysExpr::IsNull(Box::new(remap_join_expr(a, lw, nl, lmap, rmap)), *n)
        }
        PhysExpr::Cast { e, ty } => PhysExpr::Cast {
            e: Box::new(remap_join_expr(e, lw, nl, lmap, rmap)),
            ty: ty.clone(),
        },
        PhysExpr::Fn { name, args, ty } => PhysExpr::Fn {
            name: name.clone(),
            args: args
                .iter()
                .map(|a| remap_join_expr(a, lw, nl, lmap, rmap))
                .collect(),
            ty: ty.clone(),
        },
        PhysExpr::Case {
            conds,
            else_expr,
            ty,
        } => PhysExpr::Case {
            conds: conds
                .iter()
                .map(|(c, r)| {
                    (
                        remap_join_expr(c, lw, nl, lmap, rmap),
                        remap_join_expr(r, lw, nl, lmap, rmap),
                    )
                })
                .collect(),
            else_expr: Box::new(remap_join_expr(else_expr, lw, nl, lmap, rmap)),
            ty: ty.clone(),
        },
        PhysExpr::Fold { kind, args, ty } => PhysExpr::Fold {
            kind: *kind,
            args: args
                .iter()
                .map(|e| remap_join_expr(e, lw, nl, lmap, rmap))
                .collect(),
            ty: ty.clone(),
        },
        PhysExpr::Concat(l, r) => PhysExpr::Concat(
            Box::new(remap_join_expr(l, lw, nl, lmap, rmap)),
            Box::new(remap_join_expr(r, lw, nl, lmap, rmap)),
        ),
        PhysExpr::Like {
            e,
            pattern,
            negated,
            ci,
        } => PhysExpr::Like {
            e: Box::new(remap_join_expr(e, lw, nl, lmap, rmap)),
            pattern: pattern.clone(),
            negated: *negated,
            ci: *ci,
        },
    }
}

/// Join 两侧列裁剪下推。**两个已知坑**（首次尝试均踩中）：
/// ① 投影列必须按**表定义序**排列——`Scan` 的 read_columns 会按 def.columns
///    重排，而 build_mapping 按给定序索引，序不一致则 idx 错位（越界/结果空）。
/// ② 给 HashJoinExec 的右键必须是 **build 侧局部序**（其内部按 build 表求值），
///    只有 Join 之上的 Project exprs 才带 `nl`（新左宽）偏移。
#[allow(clippy::too_many_arguments)]
fn try_project_join(
    db: &Qsdb,
    left: &LogicalPlan,
    right: &LogicalPlan,
    left_keys: &[PhysExpr],
    right_keys: &[PhysExpr],
    join_type: crate::exec::join::JoinType,
    exprs: &[PhysExpr],
    names: &[String],
    residual: Option<&PhysExpr>,
    ctx: &Arc<crate::query::QueryContext>,
) -> Result<Option<Box<dyn ExecNode>>> {
    let (
        LogicalPlan::Scan {
            def: ldef,
            predicate: None,
            ..
        },
        LogicalPlan::Scan {
            def: rdef,
            predicate: None,
            ..
        },
    ) = (left, right)
    else {
        return Ok(None);
    };
    let lall: Vec<String> = ldef.columns.iter().map(|c| c.name.clone()).collect();
    let rall: Vec<String> = rdef.columns.iter().map(|c| c.name.clone()).collect();
    let lw = lall.len();

    let mut used: Vec<usize> = Vec::new();
    for e in exprs {
        e.collect_columns(&mut used);
    }
    for k in left_keys.iter().chain(right_keys.iter()) {
        k.collect_columns(&mut used);
    }
    // ⚠️ **残留谓词引用的列也要留下**：它是对匹配对求值的，被裁掉就会读到 NULL
    // ⇒ 谓词恒 UNKNOWN ⇒ 静默少行。（同 `annotate_join_needs` 的那条说明。）
    if let Some(r) = residual {
        r.collect_columns(&mut used);
    }
    let mut lproj: Vec<String> = Vec::new();
    let mut rproj: Vec<String> = Vec::new();
    for &i in &used {
        if i < lw {
            let n = &lall[i];
            if !lproj.contains(n) {
                lproj.push(n.clone());
            }
        } else {
            let n = &rall[i - lw];
            if !rproj.contains(n) {
                rproj.push(n.clone());
            }
        }
    }
    for pk in &ldef.partition_keys {
        if !lproj.iter().any(|c| c.eq_ignore_ascii_case(pk)) {
            lproj.push(pk.clone());
        }
    }
    for pk in &rdef.partition_keys {
        if !rproj.iter().any(|c| c.eq_ignore_ascii_case(pk)) {
            rproj.push(pk.clone());
        }
    }
    if lproj.len() >= lall.len() && rproj.len() >= rall.len() {
        return Ok(None); // 无收益
    }
    // ★ 坑 ①：按表定义序排序（与 Scan 的 read_columns 对齐）
    lproj.sort_by_key(|n| lall.iter().position(|c| c == n).unwrap_or(usize::MAX));
    rproj.sort_by_key(|n| rall.iter().position(|c| c == n).unwrap_or(usize::MAX));
    let lmap = build_mapping(&lall, &lproj);
    let rmap = build_mapping(&rall, &rproj);
    // 这两条子树是**裸 Scan**（本函数的入口就断言了）⇒ 不含窗口，`k` 只喂给
    // `output_shift` 的"扫描叶"那条臂，怎么算都对。
    let lp = rewrite_plan(left, &lmap, &lall, lall.len() - lproj.len());
    let rp = rewrite_plan(right, &rmap, &rall, rall.len() - rproj.len());
    let probe = build_inner(db, &lp, Some(lproj), ctx)?;
    let build = build_inner(db, &rp, Some(rproj), ctx)?;
    let nl = probe.schema().fields().len();

    let lk: Vec<PhysExpr> = left_keys
        .iter()
        .map(|k| rewrite_expr(k, &lmap, &lall))
        .collect();
    // ★ 坑 ②：右键 → 右表局部序（减 lw）再映射到右投影序，**不带 nl**
    let rk: Vec<PhysExpr> = right_keys
        .iter()
        .map(|k| match k {
            PhysExpr::Column { idx, name, ty } => rewrite_expr(
                &PhysExpr::Column {
                    idx: idx.saturating_sub(lw),
                    name: name.clone(),
                    ty: ty.clone(),
                },
                &rmap,
                &rall,
            ),
            other => other.clone(),
        })
        .collect();
    // 残留与 `exprs` 同在"join 输出（左全列 ++ 右全列）"这个索引空间里，
    // 故用同一个重映射器换算到裁剪后的空间。
    let new_residual = residual.map(|r| remap_join_expr(r, lw, nl, &lmap, &rmap));
    let join =
        crate::exec::join::HashJoinExec::new(probe, build, lk, rk, join_type, Arc::clone(ctx))
            .with_residual(new_residual);
    // exprs 在 Join 之上 → 带 nl 偏移（输出 = 左投影 ++ 右投影）
    let new_exprs: Vec<PhysExpr> = exprs
        .iter()
        .map(|e| remap_join_expr(e, lw, nl, &lmap, &rmap))
        .collect();
    Ok(Some(Box::new(ProjectExec::new(
        Box::new(join),
        new_exprs,
        names.to_vec(),
    ))))
}

/// 把"**只需物化这些输出位置**"标注到 `Aggregate` 之下的 join 链上（P1⑥）。
///
/// ## 为什么敢说"这几列没人读"
///
/// 只在 `Aggregate` 的输入链上做，而这条链**只有一个读者**：聚合自己。
/// 它要读的位置 = `group` ∪ 各 `agg.arg` 里的列，**可枚举**；链上若有 `Filter`
/// （WHERE 落在 join 之上），它是**恒等 schema 的透传**，把谓词引用的位置一并算上。
///
/// 一遇到别的形状（`Project` / `Sort` / `Window` / `SetOp` / semi join / CTE…）
/// **就地停手**——那些节点的读取集合不是"几条表达式"能枚举干净的，
/// 而**判错一列就是静默错值**（那一列会被 join 填成 NULL，且不报错）。
/// 停手 = 今天的全列行为，只是不优化。**宁可慢，不许悄悄错。**
///
/// ## 子 join 怎么往下传
///
/// `((t⋈d)⋈d2)` 里外层 join 的 probe **就是**内层 join 的输出，而 join 输出 = 左 ++ 右
/// ⇒ 外层要的位置 `p < 左宽` **原样对应**内层输出的位置 `p`。再加上外层用到的连接键
/// 位置，就是内层要产出的集合。右键同理（全局位置减左宽转局部）。
///
/// ⚠️ 实测依据（2026-10-14，.14）：join 输出宽 10 列 → 5 列，**时间减半**
/// （1.33s → 0.63s，峰值 RSS 300MB → 111MB）。基准那句聚合只读 2 列。
fn annotate_join_needs(
    plan: &LogicalPlan,
    group: &[PhysExpr],
    aggs: &[AggExpr],
    allow_scan_read: bool,
) -> LogicalPlan {
    let mut needed: Vec<usize> = Vec::new();
    for g in group {
        g.collect_columns(&mut needed);
    }
    for a in aggs {
        if let Some(arg) = &a.arg {
            arg.collect_columns(&mut needed);
        }
        // FILTER 谓词引用的列同样要标（漏了它 = 那些列被填 NULL ⇒ 谓词恒 UNKNOWN
        // ⇒ 聚合悄悄回空值；与 join 残留谓词那条是同一个坑）
        if let Some(f) = &a.filter {
            f.collect_columns(&mut needed);
        }
    }
    // 一列都不读（如 `count(*)` + 纯过滤）：**不标**。空掩码会把所有列填 NULL，
    // 虽然按定义也没人读，但"整行都是 NULL"这种状态没有实测支撑，不值得为它冒险。
    if needed.is_empty() || std::env::var("QSDB_JOIN_NO_PROJ").is_ok() {
        return plan.clone();
    }
    let needed: Vec<u32> = needed.into_iter().map(|i| i as u32).collect();
    annotate_walk(plan, &needed, allow_scan_read)
}

/// 见 [`annotate_join_needs`]。`needed` = **本子树输出**里必须真实产出的位置。
///
/// ⚠️ `allow_scan_read` 是**必需**的闸门，不是保守选项：`build()` 的单表列裁剪会
/// 把整棵计划的索引**重映射到裁剪后的空间**（`rewrite_plan`），而那个空间里的
/// 位置 **不能**拿去查 `def.columns`（全表序）——查出来是**别的列**。
/// 实测（2026-10-14）：`SELECT round(sum(score)) FROM t` 因此把 `cnt` 当成了
/// `score`，扫描读了个空集 ⇒ 聚合回 NULL。**同一个 plan 里位置属于哪个空间，
/// 是这个优化最容易错的地方**（这次是既有测试抓住的）。
/// 判据：`scan_proj.is_some()` ⟺ 已被外层裁剪（位置在裁剪空间）。
fn annotate_walk(plan: &LogicalPlan, needed: &[u32], allow_scan_read: bool) -> LogicalPlan {
    match plan {
        // Filter 是恒等 schema 的透传：它读的位置也必须在
        LogicalPlan::Filter { input, predicate } => {
            let mut n = needed.to_vec();
            let mut cols = Vec::new();
            predicate.collect_columns(&mut cols);
            for c in cols {
                let c = c as u32;
                if !n.contains(&c) {
                    n.push(c);
                }
            }
            LogicalPlan::Filter {
                input: Box::new(annotate_walk(input, &n, allow_scan_read)),
                predicate: predicate.clone(),
            }
        }
        LogicalPlan::Join {
            left,
            right,
            left_keys,
            right_keys,
            join_type,
            needed: _,
            residual,
        } if !join_type.is_semi_family() => {
            let Some(lw) = super::translate::plan_output_columns(left)
                .ok()
                .map(|c| c.len())
            else {
                return plan.clone(); // 宽度拿不到 ⇒ 不标（保守）
            };
            let mut ln: Vec<u32> = needed
                .iter()
                .copied()
                .filter(|&p| (p as usize) < lw)
                .collect();
            let mut lk = Vec::new();
            for k in left_keys {
                k.collect_columns(&mut lk);
            }
            for c in lk {
                let c = c as u32;
                if !ln.contains(&c) {
                    ln.push(c);
                }
            }
            let mut rn: Vec<u32> = needed
                .iter()
                .filter(|&&p| p as usize >= lw)
                .map(|&p| p - lw as u32)
                .collect();
            let mut rk = Vec::new();
            for k in right_keys {
                k.collect_columns(&mut rk);
            }
            for c in rk {
                let c = (c.saturating_sub(lw)) as u32; // 全局序 → 右局部序
                if !rn.contains(&c) {
                    rn.push(c);
                }
            }
            // ⚠️ **残留谓词引用的列也要留住**：它是对"匹配对"求值的，若它读的列
            // 被掩码填成 NULL，判出来就是错的（而且错得很安静——谓词为 NULL ⇒
            // 该对不发射 ⇒ **少行**）。与键列同一套索引空间换算。
            if let Some(r) = residual {
                let mut rc = Vec::new();
                r.collect_columns(&mut rc);
                for c in rc {
                    if c < lw {
                        if !ln.contains(&(c as u32)) {
                            ln.push(c as u32);
                        }
                    } else {
                        let lc = (c - lw) as u32;
                        if !rn.contains(&lc) {
                            rn.push(lc);
                        }
                    }
                }
            }
            LogicalPlan::Join {
                left: Box::new(annotate_walk(left, &ln, allow_scan_read)),
                right: Box::new(annotate_walk(right, &rn, allow_scan_read)),
                left_keys: left_keys.clone(),
                right_keys: right_keys.clone(),
                join_type: *join_type,
                needed: Some(Arc::new(needed.to_vec())),
                residual: residual.clone(),
            }
        }
        // **裸表**：只解码"下游真要的那几列"（列还在、列序不变，其余产出 NULL）。
        // 这是实测最大的一块：扫描代价几乎线性于解码列宽，而基准那句 join 里
        // 扫描占一半以上、聚合却只用 2 列。见 `Scan::restrict_read`。
        LogicalPlan::Scan {
            def,
            predicate,
            read: _,
        } => {
            // 位置不在全表空间（外层做过列裁剪）⇒ 不能按 `def.columns` 查，停手
            if !allow_scan_read {
                return plan.clone();
            }
            let mut cols: Vec<String> = Vec::new();
            for &p in needed {
                if let Some(c) = def.columns.get(p as usize) {
                    if !cols.iter().any(|x| x.eq_ignore_ascii_case(&c.name)) {
                        cols.push(c.name.clone());
                    }
                }
            }
            // 空（没人读）或没省下东西 ⇒ 维持原样
            if cols.is_empty() || cols.len() >= def.columns.len() {
                return plan.clone();
            }
            LogicalPlan::Scan {
                def: Arc::clone(def),
                predicate: predicate.clone(),
                read: Some(Arc::new(cols)),
            }
        }
        // 其它形状：读取集合无法枚举 ⇒ 停手（= 全列 = 今天的行为）
        other => other.clone(),
    }
}

/// 每行键字节的**保守**估计（分区缓冲）：编码后是 `tag + 长度 + 字节`。
/// 取 32 是给常见字典键（十几字节）留足余量 —— 估小了会在相 1 中途超额。
const DISTINCT_SHUFFLE_BYTES_PER_ROW: u64 = 32;

/// 分区数。缺省按核数（去重是**内存延迟受限**的，并行靠多路未命中藏延迟），
/// 但**封顶 16**：再多只是把每张表切得更碎、合并开销上升。
fn distinct_parts() -> usize {
    let forced = std::env::var("QSDB_DISTINCT_PARTS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok());
    match forced {
        Some(n) => n.clamp(1, 64),
        None => rayon::current_num_threads().clamp(1, 16),
    }
}

/// 计划上的行数上界（管理表 `Scan` 的元数据行数）。
///
/// 只认能直接拿到元数据行数的形状：`Scan` 及其上**只加不减行**的逐行节点
/// （`Project`/`Filter`）。`Filter` 会让上界**偏高** —— 估高只是多要一点额度，
/// 估低才会让相 1 中途超额，所以这里刻意取**保守方向**。
/// 拿不到（join/派生表/集合操作…）就返回 `None` ⇒ 走单线程路径。
fn distinct_row_upper_bound(db: &Qsdb, plan: &LogicalPlan) -> Option<u64> {
    let def = plan_scan_def(plan)?;
    db.scan_metadata_count(&def.name, None).ok()?
}

/// `count(*) over DISTINCT` 是否走**融合计数**（[`crate::exec::DistinctCountExec`]）。
///
/// 单独成函数是为了让它**可断言**：融合**静默不触发**只会让性能数字变难看，
/// 正确性测试一条都不会红 —— 优化白写了也没人知道。回归：`tests/distinct_count_fused.rs`。
pub fn distinct_fuse_applies(group: &[PhysExpr], aggs: &[AggExpr], input: &LogicalPlan) -> bool {
    group.is_empty()
        && aggs.len() == 1
        && matches!(aggs[0].func, crate::expr::AggFunc::Count)
        && aggs[0].arg.is_none()
        // 融合计数是"整行计数"，没有逐行谓词这一步 ⇒ 有 FILTER 就回常规路径
        // （`SELECT count(*) FILTER (WHERE …) FROM (SELECT DISTINCT …)`）
        && aggs[0].filter.is_none()
        && matches!(input, LogicalPlan::Distinct { .. })
}

/// 聚合构建：含 UDAF → 串行物化路径；可分片源 → 并行 partial/merge；否则串行。
fn build_aggregate(
    db: &Qsdb,
    input: &LogicalPlan,
    group: &[PhysExpr],
    aggs: &[AggExpr],
    scan_proj: &Option<Vec<String>>,
    ctx: &Arc<crate::query::QueryContext>,
) -> Result<Box<dyn ExecNode>> {
    // 聚合参数上的 `DISTINCT` 必须已在**绑定期**被 `rewrite_distinct_aggs` 消费掉。
    // 执行层不认识这个语义：放过去 = 把 `count(DISTINCT x)` 当成 `count(x)` 跑出
    // 一个**静默的错误数字**（2026-10-13 之前的真实老行为）。宁可炸。
    if let Some(a) = aggs.iter().find(|a| a.distinct) {
        return Err(crate::error::QsdbError::unsupported(format!(
            "aggregate `{}` still carries DISTINCT at execution time — the binder rewrite \
             (rewrite_distinct_aggs) did not run",
            a.out_name
        )));
    }
    // ── count(*) over DISTINCT：**融合计数**（不物化去重结果）──
    //
    //   Aggregate{ group: [], aggs: [count(*)] } ← Distinct ← X
    //
    // ⇒ 语义就是"X 的不同行数"，一个节点里数完。基准那句
    // `SELECT count(*) FROM (SELECT DISTINCT gs FROM t)` 与绑定层从
    // `count(DISTINCT x)` 改写出来的计划都是这个形状。
    //
    // 必须**先**于下面的分片/join 融合判断：它们不认识 `Distinct`，会各自落空。
    // 开关 `QSDB_NO_DISTINCT_FUSE=1`（A/B 对照；缺省开）。
    if distinct_fuse_applies(group, aggs, input) && std::env::var("QSDB_NO_DISTINCT_FUSE").is_err()
    {
        if let LogicalPlan::Distinct { input } = input {
            let mut fields: Vec<arrow::datatypes::Field> = Vec::with_capacity(aggs.len());
            for a in aggs {
                fields.push(arrow::datatypes::Field::new(
                    &a.out_name,
                    a.out_ty.arrow_type(),
                    true,
                ));
            }
            let out_schema = Arc::new(arrow::datatypes::Schema::new(fields));
            // ── 哈希分区并行（P1⑪，见 `exec::PartitionedDistinctCountExec`）──
            //
            // ⚠️ **判定必须在开读之前**：相 1 会把行数据变成分区缓冲，
            // 一旦开读就回不去了（不像 `DistinctCountExec` 能"读到一半转溢出"）。
            // 所以这里用**计划上的行数上界**×每行键字节的保守估计先要额度，
            // 要不到就走原来的单线程路径 —— 预算紧的查询行为与今天完全一致，
            // **不引入新的失败模式**。
            let node = build_inner(db, input, scan_proj.clone(), ctx)?;
            // ⚠️ **单定长键（整数 / 日期）不走分区**。
            //
            // 两条独立的依据，都实测过：
            // 1. **Int64** 单线程下已有一条专用快路（`GroupTable::int_slots`，
            //    键存在槽里 ⇒ 命中不碰 arena，实测 6–10×），而分区路径只能走
            //    `insert_raw_key`（通用编码）⇒ 反而慢。2026-10-15 实测：没这道闸门时
            //    `simple_distinct_int` 从 0.132s 退到 **0.444s**（3.4×）。
            // 2. **整数 / 日期**这类定长键**整体**不该分区（2026-10-16 补的）
            //    —— `.14` 真表 5 轮中位：`tpoint`(INT32) 49→**97**ms、
            //    `pdate`(DATE32，15 组) 44→**92**ms、`month`(DATE32，3 组) 43→**92**ms
            //    ⇒ 分区**一律亏 ~2×**。基准表上单线程也一路赢到 **250 万组**
            //    （`docs/去重计数性能设计.md` §4.1）。
            //    原因就是分区路径的**相 1 是顺序的、且它自己就是一次完整的逐行编码+哈希**
            //    （≈ 单线程路径的全部成本），相 2 只是再做一遍 ⇒ 只有"相 2 的插入成本"
            //    远大于"相 1 的一次顺序遍"时才划算，而**定长键的插入本来就便宜**。
            //
            // ⚠️ **变长键（VARCHAR）不在此列**：它在高基数下分区确实赢 2–3×
            //    （同上 §4.1），判据要按基数走 —— **还没做**（设计稿 §6.1 的 W1b）。
            // ⚠️ 这里只放**实测过**的类型：浮点 / 小数 / 时间戳**没有数据**，
            //    保持原样继续分区（宁可少改一列）。
            let sc = node.schema();
            let fs = sc.fields();
            let fixed_width_key = fs.len() == 1
                && matches!(
                    fs[0].data_type(),
                    arrow::datatypes::DataType::Int8
                        | arrow::datatypes::DataType::Int16
                        | arrow::datatypes::DataType::Int32
                        | arrow::datatypes::DataType::Int64
                        | arrow::datatypes::DataType::Date32
                );
            if !fixed_width_key {
                if let Some(rows) = distinct_row_upper_bound(db, input) {
                    let nparts = distinct_parts();
                    let reserve = rows.saturating_mul(DISTINCT_SHUFFLE_BYTES_PER_ROW);
                    if nparts > 1 {
                        if let Ok(res) = ctx.try_reserve(reserve) {
                            return Ok(Box::new(crate::exec::PartitionedDistinctCountExec::new(
                                node,
                                out_schema,
                                nparts,
                                res,
                                Arc::clone(ctx),
                            )?));
                        }
                    }
                }
            }
            return Ok(Box::new(crate::exec::DistinctCountExec::new(
                node,
                out_schema,
                Arc::clone(ctx),
            )?));
        }
    }

    // ── count(*) over INNER 等值 JOIN：计数下推（T3 优化）──
    // 低基数键下 JOIN 输出行数 = Σ左键匹配数，物化代价 n²/keys。
    // 恒等变换为「右键计数 + 左键累加」→ O(n)、build 侧只物化键列。
    if group.is_empty()
        && aggs.len() == 1
        && matches!(aggs[0].func, crate::expr::AggFunc::Count)
        && aggs[0].arg.is_none()
        // ⚠️ **写了 FILTER 就不能走这条下推**（2026-10-15，与上面"有残留谓词"
        // 同源）：`JoinCountExec` 只按键计数，压根没有"逐行谓词"这一步 ⇒
        // `count(*) FILTER (WHERE …)` 会被当成 `count(*)` 回**全量匹配数**。
        // 交给常规路径（结果正确、只是慢）。
        && aggs[0].filter.is_none()
    {
        if std::env::var("QSDB_JOIN_PROF").is_ok() {
            eprintln!(
                "[join-prof] count(*) agg, input = {}",
                match input {
                    LogicalPlan::Join { .. } => "Join",
                    LogicalPlan::Project { .. } => "Project",
                    LogicalPlan::Filter { .. } => "Filter",
                    LogicalPlan::Values { .. } => "Values",
                    LogicalPlan::Scan { .. } => "Scan",
                    _ => "other",
                }
            );
        }
        if let LogicalPlan::Join {
            left,
            right,
            left_keys,
            right_keys,
            join_type,
            residual,
            ..
        } = input
        {
            // INNER：Σ匹配数；LEFT：Σ max(匹配数, 1)（未匹配行补 NULL 仍计 1）
            let left_outer = matches!(join_type, crate::exec::join::JoinType::LeftOuter);
            if residual.is_some() {
                // ⚠️ **有残留谓词就不能走这条下推**：`JoinCountExec` 只按键计数，
                // 而残留会**否掉一部分匹配对**（`ON k=k AND r.v>250` 里 v≤250 的对
                // 不算匹配）⇒ 计数会**偏大**。实测：期望 1、回 2（静默错值）。
                // 要支持它就得让计数节点自己也求残留；在那之前**宁可放弃这条下推**
                // —— 像下面那句一样"交给常规路径"（join + 聚合，结果正确、只是慢）。
            } else if !left_outer && !matches!(join_type, crate::exec::join::JoinType::Inner) {
                // semi/anti 族等不适用（输出语义不同）——交给常规路径
            } else {
                // 键列投影：join 只需键列（扫描量 5 列 → 1 列）。
                // idx 两层转换：① right_keys 全局序 → 右表序（减左表**全**宽）
                // ② 两侧全表序 → 投影序（rewrite_expr，仅纯 Scan 无谓词时生效）。
                // 左表**原始**输出宽度（不限 Scan——CTE/派生表/Join 左侧同样适用）
                let l_full = crate::sql::translate::plan_output_columns(left)?.len();
                let r_local: Vec<PhysExpr> = right_keys
                    .iter()
                    .map(|k| match k {
                        PhysExpr::Column { idx, name, ty } => PhysExpr::Column {
                            idx: idx.saturating_sub(l_full),
                            name: name.clone(),
                            ty: ty.clone(),
                        },
                        other => other.clone(),
                    })
                    .collect();
                // 纯 Scan 无谓词 → 键列投影（否则回退全列）
                let project =
                    |pl: &LogicalPlan,
                     keys: &[PhysExpr]|
                     -> (LogicalPlan, Vec<PhysExpr>, Option<Vec<String>>) {
                        let LogicalPlan::Scan {
                            def,
                            predicate: None,
                            ..
                        } = pl
                        else {
                            return (pl.clone(), keys.to_vec(), None);
                        };
                        let all: Vec<String> = def.columns.iter().map(|c| c.name.clone()).collect();
                        let mut cols: Vec<String> = Vec::new();
                        for k in keys {
                            let mut raw: Vec<String> = Vec::new();
                            collect_expr_columns(k, &mut raw);
                            for n in raw {
                                // 去表限定（`rr.k` → `k`）：def.columns 存非限定名
                                let unq = n.rsplit('.').next().unwrap_or(&n).to_string();
                                if !cols.iter().any(|c| c.eq_ignore_ascii_case(&unq)) {
                                    cols.push(unq);
                                }
                            }
                        }
                        // 分区列必须保留（Scan 从目录重建）
                        for pk in &def.partition_keys {
                            if !cols.iter().any(|c| c.eq_ignore_ascii_case(pk)) {
                                cols.push(pk.clone());
                            }
                        }
                        if std::env::var("QSDB_JOIN_PROF").is_ok() {
                            eprintln!(
                                "[join-prof] scan all={} keycols={:?} project={}",
                                all.len(),
                                cols,
                                cols.len() < all.len()
                            );
                        }
                        if cols.len() >= all.len() {
                            return (pl.clone(), keys.to_vec(), None);
                        }
                        let mapping = build_mapping(&all, &cols);
                        // 同上：这条子树是裸 Scan，不含窗口
                        let lp = rewrite_plan(pl, &mapping, &all, all.len() - cols.len());
                        let k2: Vec<PhysExpr> = keys
                            .iter()
                            .map(|k| rewrite_expr(k, &mapping, &all))
                            .collect();
                        (lp, k2, Some(cols))
                    };
                let (lp, lkeys2, lproj) = project(left, left_keys);
                let (rp, rkeys2, rproj) = project(right, &r_local);
                let probe = build_inner(db, &lp, lproj, ctx)?;
                let build = build_inner(db, &rp, rproj, ctx)?;
                let left_width = probe.schema().fields().len();
                let _ = left_width; // 投影已含 idx 转换，此处不再减宽
                return Ok(Box::new(crate::exec::join::JoinCountExec::new(
                    probe,
                    build,
                    lkeys2,
                    rkeys2,
                    left_outer,
                    Arc::clone(ctx),
                )?));
            }
        }
    }
    if aggs
        .iter()
        .any(|a| matches!(a.func, crate::expr::AggFunc::Udaf))
    {
        return Ok(Box::new(crate::exec::udaf_agg::UdafAggExec::new(
            build_inner(db, input, scan_proj.clone(), ctx)?,
            group.to_vec(),
            aggs.to_vec(),
            Arc::clone(ctx),
        )));
    }
    let Some(ShardSource {
        shards,
        semis,
        filters,
        joins,
        table,
    }) = try_shard_source(db, input, scan_proj, ctx)
    else {
        return Ok(Box::new(HashAggExec::new(
            build_inner(db, input, scan_proj.clone(), ctx)?,
            group.to_vec(),
            aggs.to_vec(),
            Arc::clone(ctx),
        )?));
    };

    let mut fields: Vec<arrow::datatypes::Field> = group
        .iter()
        .map(|g| arrow::datatypes::Field::new(g.display(), g.data_type().arrow_type(), true))
        .collect();
    for a in aggs {
        fields.push(arrow::datatypes::Field::new(
            &a.out_name,
            a.out_ty.arrow_type(),
            true,
        ));
    }
    let schema = Arc::new(arrow::datatypes::Schema::new(fields));

    let group = group.to_vec();
    let aggs = aggs.to_vec();

    // ── semi/anti 融合路径：存在集 build 一次全分片共享；probe 批上
    // 直接求掩码 → 行选择 → 掩码摄入聚合（零中间物化、零 take）──
    //
    // ⚠️ **只在没穿过 join 时可用**（`joins == 0`）：融合的前提是"剥到的
    // filters/semis 都能压到扫描批上"，而穿过 INNER/LEFT 之后，上层的谓词
    // 可能引用 join 之后才存在的列。那种情况走下面的 join 分片路径。
    if joins == 0 && !semis.is_empty() {
        let probes: Vec<SemiProbe> = semis
            .iter()
            .map(|st| {
                let build = build_inner(db, &st.right, None, ctx)?;
                // semi 右键 = 右子计划局部 idx（绑定期约定）
                let state = crate::exec::join::prepare_exists_state(build, &st.right_keys, ctx)?;
                Ok(SemiProbe {
                    left_keys: st.left_keys.clone(),
                    state,
                    join_type: st.join_type,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let partials: Vec<(GroupTable, crate::memory::Reservation)> = shards
            .into_par_iter()
            .map(|s| run_partial_fused(s, &probes, &filters, &group, &aggs, ctx))
            .collect::<Result<Vec<_>>>()?;
        let (tables, hold): (Vec<_>, Vec<_>) = partials.into_iter().unzip();
        // 分片表的内存预留活到 merge 结束（merge 期间 N 张表同时在内存 = 峰值）
        return finish_partitioned_merge(tables, group, aggs, schema, hold);
    }

    use rayon::prelude::*;
    type Partial = (
        GroupTable,
        crate::memory::Reservation,
        Option<crate::exec::agg_spill::AggSpill>,
    );
    // 诊断（与 `QSDB_JOIN_PROF` 同风格）：证明**分片路径真被走到**——
    // 否则"测了并行"与"测了串行"从外面看不出来，回归会变成空转。
    if joins > 0 && std::env::var("QSDB_JOIN_SHARD_PROF").is_ok() {
        eprintln!(
            "[join-shard] table={table} joins={joins} shards={} group_by={} aggs={}",
            shards.len(),
            group.len(),
            aggs.len()
        );
    }
    let partials: Vec<Partial> = if joins > 0 {
        // ★ **穿过了 join 的分片**：每片把"整棵子树（含 join）"在 `ShardScope`
        // 下建一遍，最底下那个 Scan 被换成分片。这一层 join 是此前整条管线
        // 退回单线程的原因（`complex_three_way` 9.9× / `complex_join_group` 5.8×）。
        // `semis`/`filters` **传空**：它们已经在那棵子树里，再叠一次就是重复过滤。
        shards
            .into_par_iter()
            .map(|s| {
                let scope = ShardScope::set(&table, s);
                let node = build_inner(db, input, scan_proj.clone(), ctx)?;
                if !scope.taken() {
                    // 分片没被取走 ⇒ 剥出来的路径与 build_inner 建的树对不上。
                    // 继续跑会**静默变成全表扫**（结果对、白算一遍），故显式失败。
                    return Err(crate::error::QsdbError::invalid(format!(
                        "join 分片未生效：{} 的 Scan 未出现在重建的计划里（计划形状与分片剥离不一致）",
                        table
                    )));
                }
                run_partial(db, node, &[], &[], &group, &aggs, ctx)
            })
            .collect::<Result<Vec<_>>>()?
    } else {
        shards
            .into_par_iter()
            .map(|s| {
                run_partial(
                    db,
                    Box::new(ScanExec::new(s)),
                    &semis,
                    &filters,
                    &group,
                    &aggs,
                    ctx,
                )
            })
            .collect::<Result<Vec<_>>>()?
    };
    // 分片表的内存预留活到 merge 结束（峰值 = N 张分片表 + 合并中的分区）
    let mut tables = Vec::with_capacity(partials.len());
    let mut _hold = Vec::with_capacity(partials.len());
    let mut spills = Vec::with_capacity(partials.len());
    for (t, r, sp) in partials {
        tables.push(t);
        _hold.push(r);
        spills.push(sp);
    }

    // merge：按组 key hash 分区并行（每分区独立合并全部 shard 的对应组——
    // 分区表小缓存友好；原本单线程串行 merge 是高组数下的瓶颈），
    // 各分区各自 finalize 后按列 concat。
    let p = rayon::current_num_threads().clamp(1, 32);
    // 分区主序桶：parts[pi][ti] = 第 ti 张表中 hash 落 pi 分区的组下标
    // **有溢出** → 逐分区（顺序）：分区按 `partition_of_lits` 划，与溢出路由一致，
    // 于是每个分区把"各分片表里属于它的组"与"它落盘的行"合到一起算完。
    // 顺序执行（不并行）是刻意的：溢出路径本就 I/O 受限，简单 > 快。
    let batches: Vec<Option<RecordBatch>> = if spills.iter().any(|s| s.is_some()) {
        spill_aware_merge(tables, spills, &group, &aggs, &schema, &mut _hold)?
    } else {
        let mut parts: Vec<Vec<Vec<usize>>> = vec![vec![Vec::new(); tables.len()]; p];
        for (ti, t) in tables.iter().enumerate() {
            for g in 0..t.len() {
                parts[(t.group_hash(g) as usize) % p][ti].push(g);
            }
        }
        let group_ref = &group;
        let aggs_ref = &aggs;
        let schema_ref = &schema;
        parts
            .into_par_iter()
            .map(|mut table_buckets| {
                let mut merged = GroupTable::new();
                for (ti, t) in tables.iter().enumerate() {
                    let ogs = std::mem::take(&mut table_buckets[ti]);
                    if !ogs.is_empty() {
                        merged.merge_subset(t, ogs, aggs_ref.len());
                    }
                }
                // 本片空 → 本片无输出（含无 GROUP BY 的空输入：那一行由外层补，
                // 见下面 `some_batches.is_empty()` 分支）。
                crate::exec::agg::finalize_or_none(&merged, aggs_ref, group_ref, schema_ref)
            })
            .collect::<Result<Vec<_>>>()?
    };
    let some_batches: Vec<RecordBatch> = batches.into_iter().flatten().collect();
    if some_batches.is_empty() {
        // 全分片都空：无 GROUP BY 时**在这里补唯一那一行**——不能放到分片内补，
        // 全局聚合只落在一个分片上，各片各补一行会拼出 P 行（P = 分片数）；
        // 有 GROUP BY 才是真的零行。
        let batch = if group.is_empty() {
            Some(crate::exec::agg::finalize_global_empty(&aggs, &schema)?)
        } else {
            None
        };
        return Ok(Box::new(OneShotExec { batch, schema }));
    }
    let batch = if some_batches.len() == 1 {
        some_batches.into_iter().next().expect("len checked")
    } else {
        let cols = some_batches[0].num_columns();
        let mut out_cols = Vec::with_capacity(cols);
        for c in 0..cols {
            let slices: Vec<&dyn arrow::array::Array> =
                some_batches.iter().map(|b| b.column(c).as_ref()).collect();
            out_cols.push(
                arrow::compute::concat(&slices)
                    .map_err(|e| crate::error::QsdbError::invalid(format!("agg concat: {e}")))?,
            );
        }
        let n: usize = some_batches.iter().map(|b| b.num_rows()).sum();
        arrow::record_batch::RecordBatch::try_new_with_options(
            Arc::clone(&schema),
            out_cols,
            &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(n)),
        )
        .map_err(|e| crate::error::QsdbError::invalid(format!("agg output concat: {e}")))?
    };
    Ok(Box::new(OneShotExec {
        batch: Some(batch),
        schema,
    }))
}

/// 单分片 partial 聚合（当前线程执行；由 rayon 并行调度调用方决定）。
/// 预构建的存在集探针（融合路径全分片共享 Arc）。
struct SemiProbe {
    left_keys: Vec<PhysExpr>,
    state: std::sync::Arc<crate::exec::join::ExistsState>,
    join_type: crate::exec::join::JoinType,
}

/// 融合 partial：scan 批 → semi 掩码（AND 组合）→ 过滤谓词掩码 →
/// 掩码摄入聚合。无中间批物化——与 DuckDB 的 filter-into-scan 聚合
/// 融合同构，且存在集探测是免哈希的（i64 集或位图）。
fn run_partial_fused(
    scan: crate::scan::Scan,
    probes: &[SemiProbe],
    filters: &[PhysExpr],
    group: &[PhysExpr],
    aggs: &[AggExpr],
    ctx: &Arc<crate::query::QueryContext>,
) -> Result<(GroupTable, crate::memory::Reservation)> {
    use crate::exec::ExecNode;
    use arrow::array::Array;
    // 分片任务内串行拉取（ScanExec）——不用 ParScanExec：嵌套并行会与
    // 分片任务互锁同一 rayon 池（M4 教训；mask+ingest 实测仅 0.07s，
    // 时间全耗在嵌套并行等待）
    let mut node: Box<dyn ExecNode> = Box::new(crate::exec::ScanExec::new(scan));
    let mut table = GroupTable::default();
    // 融合路径与 HashAggExec 同口径记账（组表随摄入增长 → 增量预留）
    let mut res = ctx.try_reserve(0)?;
    while let Some(b) = node.next() {
        ctx.check()?;
        let batch = b?;
        if batch.num_rows() == 0 {
            continue;
        }
        // 行选择 = semi 掩码 ∧ 过滤谓词（**每批一次求值**再逐行组合——
        // 掩码求值是整列向量化，逐行重算整列 = O(n²) 灾难）
        let mut masks: Vec<arrow::array::BooleanArray> =
            Vec::with_capacity(probes.len() + filters.len());
        for p in probes {
            masks.push(crate::exec::join::semi_mask(
                &p.state,
                &p.left_keys,
                &batch,
                p.join_type,
                ctx,
            )?);
        }
        for f in filters {
            let m = crate::expr::eval::eval(f, &batch)?;
            let ba = m
                .as_any()
                .downcast_ref::<arrow::array::BooleanArray>()
                .ok_or_else(|| crate::error::QsdbError::type_err("filter must be boolean"))?;
            masks.push(ba.clone());
        }
        let mut sel: Vec<u32> = Vec::with_capacity(batch.num_rows());
        'outer: for row in 0..batch.num_rows() {
            for m in &masks {
                if m.is_null(row) || !m.value(row) {
                    continue 'outer;
                }
            }
            sel.push(row as u32);
        }
        table.ingest_with_sel(group, aggs, &batch, &sel)?;
        // 组表增长 → 增量预留（同 HashAggExec 口径：按容器容量增量）
        let est = table.estimated_bytes();
        if est > res.bytes() {
            res.grow(est - res.bytes())?;
        }
    }
    Ok((table, res))
}

/// **溢出感知的合并**：逐分区把「各分片表里属于该分区的组」与「该分区落盘的
/// 原始行」合到一起，再聚合/去重。
///
/// 为什么分区口径必须与溢出路由一致：落盘的行是按 `partition_of_lits` 分的，
/// 分片表里的组也必须按**同一个**函数归位——否则同一个组的"内存部分"与
/// "落盘部分"会被分到不同分区，各自聚合出半个结果而**静默算错**。
/// （这也是不能用 `group_hash`（表内部 arena 编码）来划分的原因。）
fn spill_aware_merge(
    tables: Vec<GroupTable>,
    mut spills: Vec<Option<crate::exec::agg_spill::AggSpill>>,
    group: &[PhysExpr],
    aggs: &[AggExpr],
    schema: &Arc<arrow::datatypes::Schema>,
    _hold: &mut [crate::memory::Reservation],
) -> Result<Vec<Option<RecordBatch>>> {
    let nparts = spills
        .iter()
        .flatten()
        .map(|s| s.nparts())
        .next()
        .unwrap_or(1);
    let mut out = Vec::with_capacity(nparts);
    for pi in 0..nparts {
        let mut merged = GroupTable::new();
        // ① 各分片表里属于本分区的组
        for t in tables.iter() {
            let lits = t.group_lits();
            let ogs: Vec<usize> = (0..t.len())
                .filter(|&g| {
                    let l: Vec<crate::types::Literal> = lits.iter().map(|c| c[g].clone()).collect();
                    crate::exec::agg_spill::partition_of_lits(&l, nparts) == pi
                })
                .collect();
            if !ogs.is_empty() {
                merged.merge_subset(t, ogs, aggs.len());
            }
        }
        // ② 本分区落盘的原始行（继续聚合进同一张表）
        for sp in spills.iter_mut().flatten() {
            // AggSpill 自持预留（走 reserve_spill：只受全局预算约束）。
            // **流式**逐 run 吃（峰值 = 一个 run）——整个分区的行一起物化要求
            // "单分区放得下"，小预算下做不到（聚合逐批可吃，没必要）。
            sp.for_each_partition(pi, |b| merged.ingest(group, aggs, b))?;
        }
        if merged.is_empty() {
            out.push(None);
            continue;
        }
        out.push(Some(crate::exec::agg::finalize_table(
            &merged, aggs, group, schema,
        )?));
    }
    Ok(out)
}

/// 分片合并 + 分区并行 finalize（融合路径）：与主路径 merge 块同款。
fn finish_partitioned_merge(
    tables: Vec<GroupTable>,
    group: Vec<PhysExpr>,
    aggs: Vec<AggExpr>,
    schema: Arc<arrow::datatypes::Schema>,
    _hold: Vec<crate::memory::Reservation>,
) -> Result<Box<dyn ExecNode>> {
    use rayon::prelude::*;
    let p = rayon::current_num_threads().clamp(1, 32);
    let mut parts: Vec<Vec<Vec<usize>>> = vec![vec![Vec::new(); tables.len()]; p];
    for (ti, t) in tables.iter().enumerate() {
        for g in 0..t.len() {
            parts[(t.group_hash(g) as usize) % p][ti].push(g);
        }
    }
    let aggs_ref = &aggs;
    let group_ref = &group;
    let schema_ref = &schema;
    let batches: Vec<Option<RecordBatch>> = parts
        .into_par_iter()
        .map(|mut table_buckets| {
            let mut merged = GroupTable::new();
            for (ti, t) in tables.iter().enumerate() {
                let ogs = std::mem::take(&mut table_buckets[ti]);
                if !ogs.is_empty() {
                    merged.merge_subset(t, ogs, aggs_ref.len());
                }
            }
            if merged.is_empty() {
                return Ok(None);
            }
            crate::exec::agg::finalize_table(&merged, aggs_ref, group_ref, schema_ref).map(Some)
        })
        .collect::<Result<Vec<_>>>()?;
    let some_batches: Vec<RecordBatch> = batches.into_iter().flatten().collect();
    if some_batches.is_empty() {
        // 全分片都空：无 GROUP BY 时**在这里补唯一那一行**——不能放到分片内补，
        // 全局聚合只落在一个分片上，各片各补一行会拼出 P 行（P = 分片数）；
        // 有 GROUP BY 才是真的零行。
        let batch = if group.is_empty() {
            Some(crate::exec::agg::finalize_global_empty(&aggs, &schema)?)
        } else {
            None
        };
        return Ok(Box::new(OneShotExec { batch, schema }));
    }
    let batch = if some_batches.len() == 1 {
        some_batches.into_iter().next().expect("len checked")
    } else {
        crate::exec::spill::concat_batches(&some_batches, &schema)?
    };
    Ok(Box::new(OneShotExec {
        batch: Some(batch),
        schema,
    }))
}

/// 分片管线中的 semi/anti 阶段（build 侧计划持引用，每分片重建 ExecNode
/// ——dims 级小表重建成本可忽略）。
struct SemiStage {
    left_keys: Vec<PhysExpr>,
    right: std::sync::Arc<LogicalPlan>,
    /// 全局 ctx 序（构建时减 probe 宽度）
    right_keys: Vec<PhysExpr>,
    join_type: crate::exec::join::JoinType,
}

/// 单个分片的 partial 聚合。
///
/// `node` 是**该分片的输入节点**，由调用方决定怎么造：
/// - 纯 `Scan(/Filter)` 链 → `ScanExec(分片)`（既有路径）；
/// - 穿过了 join 的分片 → `build_inner` 在 `ShardScope` 下建出的整棵子树
///   （此时 `semis`/`filters` 传空：它们已经在子树里了，再叠一次就是重复过滤）。
fn run_partial(
    db: &Qsdb,
    mut node: Box<dyn ExecNode>,
    semis: &[SemiStage],
    filters: &[PhysExpr],
    group: &[PhysExpr],
    aggs: &[AggExpr],
    ctx: &Arc<crate::query::QueryContext>,
) -> Result<(
    GroupTable,
    crate::memory::Reservation,
    Option<crate::exec::agg_spill::AggSpill>,
)> {
    // semi 阶段按「自底向上」应用（try_shard_source 剥离序为自顶向下）
    for st in semis.iter().rev() {
        let build = build_inner(db, &st.right, None, ctx)?;
        let left_width = node.schema().fields().len();
        let local_keys: Vec<PhysExpr> = st
            .right_keys
            .iter()
            .map(|k| match k {
                PhysExpr::Column { idx, name, ty } => PhysExpr::Column {
                    idx: idx.saturating_sub(left_width),
                    name: name.clone(),
                    ty: ty.clone(),
                },
                other => other.clone(),
            })
            .collect();
        node = Box::new(crate::exec::join::HashJoinExec::new(
            node,
            build,
            st.left_keys.clone(),
            local_keys,
            st.join_type,
            Arc::clone(ctx),
        ));
    }
    for f in filters {
        node = Box::new(FilterExec::new(node, f.clone(), Arc::clone(ctx)));
    }
    let mut exec = HashAggExec::new(node, group.to_vec(), aggs.to_vec(), Arc::clone(ctx))?;
    // 分片路径：溢出由调用方接管（分区结果要走 merge，不能在此当作最终行 emit）
    exec.set_defer_spill(true);
    while let Some(b) = exec.next() {
        b?; // 取消/超时/内存超限在此冒泡
    }
    // 预留随组表一起交回：分片表在 merge 期间仍然活着（N 张表同时在内存
    // 正是峰值），账不能在这里提前放掉
    let (table, res, spill) = exec.into_parts();
    Ok((table, res, spill))
}

/// 分片源：剥出来的分片 + 沿途的过滤/semi 阶段 + **穿过的 INNER/LEFT join 层数**。
struct ShardSource {
    shards: Vec<crate::scan::Scan>,
    /// 自顶向下剥到的 Filter 谓词（只有 `joins == 0` 时才可直接压到扫描批上）
    filters: Vec<PhysExpr>,
    /// 自顶向下剥到的 semi/anti 阶段（同上）
    semis: Vec<SemiStage>,
    /// **穿过**的 INNER/LEFT OUTER 层数。>0 = 分片里必须带上 join，
    /// 此时 `filters`/`semis` 不可单独压扫描批（它们的列可能来自 join 结果）。
    joins: usize,
    /// 被分片的实表名（`ShardScope` 据此认领）
    table: String,
}

/// 分片时"建侧"允许的最大行数。
///
/// **为什么有这道闸**：穿过 join 的分片会让**每一片各建一次 build 侧 hash 索引**
/// （与既有 semi 分片同款权衡：N 片并行建 ⇒ 墙钟 ≈ 建一次，但**内存 × N**）。
/// 大表互连时 × N 的内存可能把小预算的查询从"慢慢跑完"变成 `MEMORY_PRESSURE`，
/// 那是实打实的退步。故只对**小维表**分片；拿不到行数就不分（保守）。
const JOIN_SHARD_MAX_BUILD_ROWS: u64 = 2_000_000;

/// 分片数。**上限 4**，2026-10-14 在 .14 重测确认（下面有数）。
///
/// 旧注释给的上限 4 依据是"89 万组：4 片 0.953s / 8 片 1.407s / 16 片 2.212s"，
/// 但那段还写着"原本**单线程串行 merge** 是高组数下的瓶颈"——**那份测量早于
/// 分区并行 merge**，结论本可能过期。故加了 `QSDB_SHARDS` 旋钮重量一遍（5M 行）：
///
/// | 分片 | `t×d` 聚合 墙钟/CPU/峰值RSS | 三表链 墙钟/CPU | 10 万组聚合 墙钟/CPU |
/// |---|---|---|---|
/// | 1 | 1.29s / 1.29s / 307MB | 2.12s / 2.12s | 0.34s / 0.39s |
/// | 4 | **0.53s** / 1.74s / 419MB | **0.81s** / 2.97s | **0.20s** / 0.72s |
/// | 8 | 0.47s / 2.63s / 530MB | 0.65s / 3.83s | 0.21s / 1.23s |
/// | 16 | 0.45s / **5.78s** / **746MB** | 0.61s / 7.33s | **0.26s（回退）** / 2.69s |
///
/// **4 片是拐点**，旧结论在并行 merge 之后**依然成立**：4→16 只再快 1.2×，
/// 代价是 CPU ×3.3、峰值 RSS ×1.8，而**高基数聚合在 16 片反而变慢**。
///
/// 墙钟模型：`墙钟 ≈ P/N + B`（P = 可并行的扫描+探测+聚合，B = **每片固定成本**）。
/// 实测 CPU 随 N 明显上涨（1.29→5.78）说明 **B 主要是"每片各建一次建侧 hash 索引"**
/// ——这正是下一步（P0③ 后半）要拿掉的东西：**共享建侧**。拿掉之后 N 才能往 16 走，
/// 那时墙钟才可能从 0.53s 量级掉到 0.2s 量级。
fn shard_count() -> usize {
    if let Ok(v) = std::env::var("QSDB_SHARDS") {
        if let Ok(n) = v.parse::<usize>() {
            return n.clamp(1, 64);
        }
    }
    rayon::current_num_threads().clamp(1, 4)
}

/// 源分片：`(Scan | Filter | Join)+` 链且最底下的 scan 可切多片 → Some。
#[allow(clippy::too_many_arguments)]
fn try_shard_source(
    db: &Qsdb,
    plan: &LogicalPlan,
    scan_proj: &Option<Vec<String>>,
    ctx: &Arc<crate::query::QueryContext>,
) -> Option<ShardSource> {
    let mut filters: Vec<PhysExpr> = Vec::new();
    let mut semis: Vec<SemiStage> = Vec::new();
    let mut joins = 0usize;
    let mut cur = plan;
    loop {
        match cur {
            LogicalPlan::Filter { input, predicate } => {
                filters.push(predicate.clone());
                cur = input;
            }
            // semi/anti 族穿透：左链继续分片，右侧计划记为 build 阶段
            LogicalPlan::Join {
                left,
                right,
                left_keys,
                right_keys,
                join_type,
                ..
            } if join_type.is_semi_family() => {
                semis.push(SemiStage {
                    left_keys: left_keys.clone(),
                    right: std::sync::Arc::new((**right).clone()),
                    right_keys: right_keys.clone(),
                    join_type: *join_type,
                });
                cur = left;
            }
            // ⚠️ **FULL OUTER 不许分片**（正确性，不是保守）：分片按**行**切左输入，
            // 而 FULL 的"右孤儿"要等 probe 跑完、看过**全量** build 匹配标记才知道
            // 是谁。每片各发各的 ⇒ 同一个右孤儿会被**发射 N 次**（静默多行）。
            // 要支持得把标记做成跨分片全局位图 + 单独一轮收尾 —— 那是另一件事，
            // 别在这里半吊子地做。所以到此为止：不分片，让整条管线走单线程。
            LogicalPlan::Join {
                join_type: crate::exec::join::JoinType::FullOuter,
                ..
            } => break None,
            // ★ INNER/LEFT：左链继续分片，这一层**随分片一起重建**。
            //
            // 正确性：分片是左输入的不相交划分 ⇒ 每一行左数据只出现在一片里，
            // INNER 与 LEFT OUTER 的结果按片求并 = 整体结果（LEFT 的未匹配行
            // 也各自只在自己那片发射一次）。semi 族同理（既有路径已这么做）。
            LogicalPlan::Join {
                left,
                right,
                join_type:
                    crate::exec::join::JoinType::Inner | crate::exec::join::JoinType::LeftOuter,
                ..
            } => {
                // 建侧必须能"估得出且不太大"（见 JOIN_SHARD_MAX_BUILD_ROWS）。
                // 限定无谓词实表：一是能拿 metadata 行数，二是这类维表连接正是目标形态。
                let LogicalPlan::Scan {
                    def: rdef,
                    predicate: None,
                    ..
                } = &**right
                else {
                    return None;
                };
                let rows = db.scan_metadata_count(&rdef.name, None).ok()??;
                if rows > JOIN_SHARD_MAX_BUILD_ROWS {
                    return None;
                }
                joins += 1;
                cur = left;
            }
            LogicalPlan::Scan {
                def,
                predicate,
                read: _,
            } => {
                let scan = db
                    .scan_with_context(
                        ScanRequest {
                            table: def.name.clone(),
                            predicate: predicate.clone(),
                            projection: scan_proj.clone(),
                            ..Default::default()
                        },
                        Arc::clone(ctx),
                    )
                    .ok()?;
                let threads = shard_count();
                let shards = scan.shard(threads, false)?;
                return Some(ShardSource {
                    shards,
                    filters,
                    semis,
                    joins,
                    table: def.name.clone(),
                });
            }
            LogicalPlan::ExternalScan {
                def,
                files,
                predicate,
            } => {
                let scan = db
                    .external_scan(
                        Arc::clone(def),
                        files.as_ref().clone(),
                        scan_proj.clone(),
                        predicate.clone(),
                        Arc::clone(ctx),
                    )
                    .ok()?;
                let threads = shard_count();
                let shards = scan.shard(threads, false)?;
                return Some(ShardSource {
                    shards,
                    filters,
                    semis,
                    joins,
                    table: def.name.clone(),
                });
            }
            _ => return None,
        }
    }
}
