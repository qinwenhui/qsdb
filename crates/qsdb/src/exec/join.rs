//! HashJoin：build 侧（右表）建哈希表 → probe 侧（左表）流式探测。
//!
//! - 等值条件（多键 AND）；INNER / LEFT OUTER
//! - NULL 键不匹配（SQL 语义：NULL = NULL 非 TRUE）
//! - build 侧全量物化（ctx 内存记账）；probe 侧流式
//! - 输出列序：左表列 ++ 右表列

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int8Array, Int16Array, Int32Array, Int64Array, StringArray, TimestampNanosecondArray,
    UInt32Array,
};
use arrow::compute::{concat, take};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};

use crate::error::{QsdbError, Result};
use crate::expr::PhysExpr;
use crate::expr::eval;
use crate::query::QueryContext;

use super::ExecNode;

/// 连接类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    LeftOuter,
    /// Semi（IN / EXISTS）：左行命中 build 键集 ≥1 次则发射（至多一次，
    /// 不重复）；NULL 键不命中（SQL：NULL IN set = UNKNOWN）
    Semi,
    /// Anti（NOT EXISTS）：左行零命中则发射；NULL 键零命中 → 发射
    Anti,
    /// AntiIn（NOT IN，三值语义）：build 含 NULL 键 → 结果恒空；
    /// probe NULL 键 → 该行丢弃（NOT IN 对 NULL 的 SQL 语义）
    AntiIn,
}

impl JoinType {
    /// semi/anti 族：输出 = 左 schema；build 侧只需键存在集。
    pub fn is_semi_family(self) -> bool {
        matches!(self, Self::Semi | Self::Anti | Self::AntiIn)
    }
}

/// 笛卡尔积（非等值 INNER JOIN 的底座）：右表首拉时物化，左批逐批 × 右表展开。
pub struct CrossJoinExec {
    probe: Box<dyn ExecNode>,
    build: Option<Box<dyn ExecNode>>,
    build_state: Option<RecordBatch>,
    schema: SchemaRef,
    ctx: Arc<QueryContext>,
}

impl CrossJoinExec {
    pub fn new(probe: Box<dyn ExecNode>, build: Box<dyn ExecNode>, ctx: Arc<QueryContext>) -> Self {
        let fields: Vec<arrow::datatypes::FieldRef> = probe
            .schema()
            .fields()
            .iter()
            .cloned()
            .chain(build.schema().fields().iter().cloned())
            .collect();
        let schema = Arc::new(arrow::datatypes::Schema::new(fields));
        Self {
            probe,
            build: Some(build),
            build_state: None,
            schema,
            ctx,
        }
    }

    fn materialize_build(&mut self) -> Result<()> {
        if self.build_state.is_some() {
            return Ok(());
        }
        let Some(mut build) = self.build.take() else {
            return Ok(());
        };
        // 右 schema 从 build 节点自身（首批出现前用它 schema 的空批次形状不可行——
        // 直接收集批，再以首批 schema concat；无批则空表）
        let mut batches = Vec::new();
        let mut charged = 0u64;
        while let Some(b) = build.next() {
            self.ctx.check()?;
            let x = b?;
            let bytes = x.get_array_memory_size() as u64;
            self.ctx.charge_memory(bytes)?;
            charged += bytes;
            batches.push(x);
        }
        self.ctx.release_memory(charged);
        let table = match batches.split_first() {
            Some((first, _)) => crate::exec::spill::concat_batches(&batches, &first.schema())?,
            None => RecordBatch::new_empty(build.schema()),
        };
        self.build_state = Some(table);
        Ok(())
    }
}

impl ExecNode for CrossJoinExec {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn next(&mut self) -> Option<Result<RecordBatch>> {
        if let Err(e) = self.ctx.check() {
            return Some(Err(e));
        }
        if let Err(e) = self.materialize_build() {
            return Some(Err(e));
        }
        let batch = self.probe.next()?;
        let batch = match batch {
            Ok(b) => b,
            Err(e) => return Some(Err(e)),
        };
        if batch.num_rows() == 0 {
            return self.next();
        }
        // 展开：左批每行 × 右表每行
        let n_left = batch.num_rows();
        let n_right = self.build_state.as_ref().map(|b| b.num_rows()).unwrap_or(0);
        let total = n_left * n_right;
        let mut left_indices: Vec<u32> = Vec::with_capacity(total);
        let mut right_indices: Vec<u32> = Vec::with_capacity(total);
        for l in 0..n_left {
            for r in 0..n_right {
                left_indices.push(l as u32);
                right_indices.push(r as u32);
            }
        }
        let li = UInt32Array::from(left_indices);
        let ri = UInt32Array::from(right_indices);
        let mut cols: Vec<ArrayRef> = Vec::with_capacity(self.schema.fields().len());
        for c in 0..batch.num_columns() {
            let taken = take(batch.column(c), &li, None)
                .map_err(|e| QsdbError::invalid(format!("cross take: {e}")));
            match taken {
                Ok(a) => cols.push(a),
                Err(e) => return Some(Err(e)),
            }
        }
        if let Some(build) = &self.build_state {
            for c in 0..build.num_columns() {
                let taken = take(build.column(c), &ri, None)
                    .map_err(|e| QsdbError::invalid(format!("cross take: {e}")));
                match taken {
                    Ok(a) => cols.push(a),
                    Err(e) => return Some(Err(e)),
                }
            }
        } else {
            // 右表为空批：补全空列
            for f in self.schema.fields().iter().skip(batch.num_columns()) {
                cols.push(arrow::array::new_null_array(f.data_type(), total));
            }
        }
        let out = RecordBatch::try_new_with_options(
            Arc::clone(&self.schema),
            cols,
            &RecordBatchOptions::new().with_row_count(Some(total)),
        )
        .map_err(|e| QsdbError::invalid(format!("cross output: {e}")));
        Some(out)
    }
}

/// 哈希连接执行节点。
pub struct HashJoinExec {
    probe: Box<dyn ExecNode>,
    build: Box<dyn ExecNode>,
    probe_keys: Vec<PhysExpr>,
    build_keys: Vec<PhysExpr>,
    join_type: JoinType,
    schema: SchemaRef,
    ctx: Arc<QueryContext>,
    /// build 阶段产物（首拉时构建）
    build_state: Option<BuildState>,
    /// semi/anti 族 build 产物（与 build_state 互斥，按 join_type 选）
    exists_state: Option<ExistsState>,
    probe_done: bool,
}

struct BuildState {
    /// 右表物化批（单批 concat 后）
    table: RecordBatch,
    /// key bytes → 行号列表
    index: HashMap<Vec<u8>, Vec<u32>>,
}

/// semi/anti 族 build 产物：存在性三形态（按键形状自动选优——「别人不做
/// 的偏门」：IN/EXISTS 的键 90% 是单个整数列，走免编码快路径；稠密整数
/// 域再退化为位图探测）。
pub struct ExistsState {
    keys: ExistsKeys,
    /// build 侧出现过 NULL 键（NOT IN 三值语义用）
    has_null: bool,
}

enum ExistsKeys {
    /// 通用：多键/字符串/浮点 → 键字节编码存在集
    Bytes(std::collections::HashSet<Vec<u8>>),
    /// 单整数键（Int8..64/Date32/Boolean）→ 免编码 i64 存在集
    I64(std::collections::HashSet<i64>),
    /// 单整数键且域稠密（max−min ≤ 16M 位 = 2MB）→ 位图探测：
    /// probe = 单 bit 测试（无哈希/无分配/缓存友好；分区键 IN 的标准形态）
    Bitmap { min: i64, bits: Vec<u64> },
}

/// 单整数键取值（Int8..64 / Date32 / Boolean → i64）；其他类型 None。
fn int_key_value(a: &ArrayRef, row: usize) -> Option<i64> {
    use arrow::datatypes::DataType;
    macro_rules! as_i64 {
        ($t:ident) => {{
            a.as_any().downcast_ref::<$t>().unwrap().value(row) as i64
        }};
    }
    Some(match a.data_type() {
        DataType::Int8 => as_i64!(Int8Array),
        DataType::Int16 => as_i64!(Int16Array),
        DataType::Int32 => as_i64!(Int32Array),
        DataType::Int64 => as_i64!(Int64Array),
        // Date32/Timestamp 物理上 i32/i64 但 arrow 数组类型不同——各自 downcast
        DataType::Date32 => a.as_any().downcast_ref::<Date32Array>().unwrap().value(row) as i64,
        DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, _) => {
            a.as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap()
                .value(row)
        }
        DataType::Boolean => i64::from(
            a.as_any().downcast_ref::<BooleanArray>().unwrap().value(row),
        ),
        _ => return None,
    })
}

impl ExistsState {
    fn contains(&self, v: i64) -> bool {
        match &self.keys {
            ExistsKeys::I64(set) => set.contains(&v),
            ExistsKeys::Bitmap { min, bits } => {
                let b = v.wrapping_sub(*min);
                if b < 0 || b as u64 >= bits.len() as u64 * 64 {
                    return false;
                }
                bits[(b >> 6) as usize] & (1u64 << (b & 63)) != 0
            }
            ExistsKeys::Bytes(_) => unreachable!("int path on bytes state"),
        }
    }

    fn contains_bytes(&self, k: &[u8]) -> bool {
        match &self.keys {
            ExistsKeys::Bytes(set) => set.contains(k),
            _ => unreachable!("bytes path on int state"),
        }
    }
}

impl HashJoinExec {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        probe: Box<dyn ExecNode>,
        build: Box<dyn ExecNode>,
        probe_keys: Vec<PhysExpr>,
        build_keys: Vec<PhysExpr>,
        join_type: JoinType,
        ctx: Arc<QueryContext>,
    ) -> Self {
        // 输出 schema：semi/anti 族 = 仅左（probe）；INNER/LEFT = 左 ++ 右
        let fields: Vec<arrow::datatypes::FieldRef> = if join_type.is_semi_family() {
            probe.schema().fields().iter().cloned().collect()
        } else {
            probe
                .schema()
                .fields()
                .iter()
                .cloned()
                .chain(build.schema().fields().iter().cloned())
                .collect()
        };
        let schema = Arc::new(arrow::datatypes::Schema::new(fields));
        Self {
            probe,
            build,
            probe_keys,
            build_keys,
            join_type,
            schema,
            ctx,
            build_state: None,
            exists_state: None,
            probe_done: false,
        }
    }

    fn build_phase(&mut self) -> Result<()> {
        if self.join_type.is_semi_family() {
            return self.build_phase_exists();
        }
        if self.build_state.is_some() {
            return Ok(());
        }
        // 物化右表
        let mut batches = Vec::new();
        let mut charged = 0u64;
        while let Some(b) = self.build.next() {
            self.ctx.check()?;
            let x = b?;
            let bytes = x.get_array_memory_size() as u64;
            self.ctx.charge_memory(bytes)?;
            charged += bytes;
            batches.push(x);
        }
        self.ctx.release_memory(charged);
        let table = concat_batches(&batches, &self.build.schema())?;

        // 键哈希索引
        let mut key_cols: Vec<ArrayRef> = Vec::with_capacity(self.build_keys.len());
        for k in &self.build_keys {
            key_cols.push(eval::eval(k, &table)?);
        }
        let mut index: HashMap<Vec<u8>, Vec<u32>> = HashMap::new();
        let mut buf: Vec<u8> = Vec::new();
        for row in 0..table.num_rows() {
            encode_join_key(&key_cols, row, &mut buf);
            if buf.first() == Some(&0) {
                continue; // 任一键 NULL → 不入索引（SQL 语义）
            }
            index.entry(buf.clone()).or_default().push(row as u32);
        }
        self.build_state = Some(BuildState { table, index });
        Ok(())
    }
}

impl HashJoinExec {
    /// semi/anti 族 build：流式消费右表逐批求键 → 存在集。
    fn build_phase_exists(&mut self) -> Result<()> {
        if self.exists_state.is_some() {
            return Ok(());
        }
        let schema = self.build.schema();
        let node = std::mem::replace(&mut self.build, Box::new(EmptyExec::new(schema)));
        let keys = std::mem::take(&mut self.build_keys);
        let state = prepare_exists_state(node, &keys, &self.ctx)?;
        self.build_keys = keys;
        self.exists_state = Some(Arc::try_unwrap(state).unwrap_or_else(|arc| {
            let _ = arc;
            unreachable!("hash join owns the only ref")
        }));
        Ok(())
    }
}

/// 空源（build 节点被 move 走后的占位）。
struct EmptyExec {
    schema: SchemaRef,
}
impl EmptyExec {
    fn new(schema: SchemaRef) -> Self {
        Self { schema }
    }
}
impl ExecNode for EmptyExec {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
    fn next(&mut self) -> Option<Result<RecordBatch>> {
        None
    }
}

/// 独立准备存在集（聚合融合路径共享：build 一次、多分片探测）。
/// 右表不物化、不 concat；单整数键先走 i64 累积，扫完按域稠密度
/// 选 HashSet 或位图（2MB 位图 = 稠密整数域的 O(1) 无哈希探测）。
pub fn prepare_exists_state(
    mut build: Box<dyn ExecNode>,
    build_keys: &[PhysExpr],
    ctx: &Arc<QueryContext>,
) -> Result<Arc<ExistsState>> {
    let mut has_null = false;
    // 单整数键快路径累积（min/max 供位图判定）
    let mut ints: Option<Vec<i64>> = if build_keys.len() == 1 { Some(Vec::new()) } else { None };
    let mut bytes: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
    let mut buf: Vec<u8> = Vec::new();
    while let Some(b) = build.next() {
        ctx.check()?;
        let batch = b?;
        let mut key_cols: Vec<ArrayRef> = Vec::with_capacity(build_keys.len());
        for k in build_keys {
            key_cols.push(eval::eval(k, &batch)?);
        }
        if let (Some(acc), [only]) = (&mut ints, key_cols.as_slice()) {
            let mut fallback = false;
            for row in 0..batch.num_rows() {
                if only.is_null(row) {
                    has_null = true;
                    continue;
                }
                match int_key_value(only, row) {
                    Some(v) => acc.push(v),
                    // 非整数键（字符串/浮点等）→ 整体退回通用字节路径
                    None => {
                        fallback = true;
                        break;
                    }
                }
            }
            if fallback {
                ints = None;
                for r in 0..batch.num_rows() {
                    encode_join_key(&key_cols, r, &mut buf);
                    if buf.first() == Some(&0) {
                        has_null = true;
                    } else {
                        bytes.insert(buf.clone());
                    }
                }
            }
        } else {
            for row in 0..batch.num_rows() {
                encode_join_key(&key_cols, row, &mut buf);
                if buf.first() == Some(&0) {
                    has_null = true;
                } else {
                    bytes.insert(buf.clone());
                }
            }
        }
    }
    let keys = match ints {
        Some(vals) if !vals.is_empty() => {
            let min = *vals.iter().min().unwrap();
            let max = *vals.iter().max().unwrap();
            let range = (max as i128 - min as i128) as u128;
            if range <= 16 * 1024 * 1024 {
                // 稠密域 → 位图（2MB 上限）
                let nbits = range as usize + 1;
                let mut bits = vec![0u64; nbits.div_ceil(64)];
                for v in vals {
                    let b = (v - min) as usize;
                    bits[b / 64] |= 1u64 << (b % 64);
                }
                ExistsKeys::Bitmap { min, bits }
            } else {
                ExistsKeys::I64(vals.into_iter().collect())
            }
        }
        Some(_) => ExistsKeys::I64(Default::default()), // 空集
        None => ExistsKeys::Bytes(bytes),
    };
    Ok(Arc::new(ExistsState { keys, has_null }))
}

/// 掩码探测（聚合融合路径）：probe 批 → 半连接布尔掩码（true = 行入选）。
/// 与 join_batch_semi 同语义：Semi 命中入选；Anti 零命中入选（NULL 键入选）；
/// AntiIn 零命中入选（NULL 键剔除；build 含 NULL → 全 false）。
pub fn semi_mask(
    state: &ExistsState,
    probe_keys: &[PhysExpr],
    batch: &RecordBatch,
    join_type: JoinType,
    ctx: &Arc<QueryContext>,
) -> Result<BooleanArray> {
    let _ = ctx;
    if join_type == JoinType::AntiIn && state.has_null {
        return Ok(BooleanArray::from(vec![Some(false); batch.num_rows()]));
    }
    let mut key_cols: Vec<ArrayRef> = Vec::with_capacity(probe_keys.len());
    for k in probe_keys {
        key_cols.push(eval::eval(k, batch)?);
    }
    let n = batch.num_rows();
    let mut mask: Vec<bool> = Vec::with_capacity(n);
    if let (ExistsKeys::I64(_) | ExistsKeys::Bitmap { .. }, [only]) =
        (&state.keys, key_cols.as_slice())
    {
        // 零拷贝：类型分派一次（downcast 出循环——逐行 downcast 在 40M 行
        // 级 probe 上是主要成本，同 P3 教训）
        macro_rules! probe_int {
            ($arr:ident) => {{
                let v = only.as_any().downcast_ref::<$arr>().unwrap();
                for row in 0..n {
                    let selected = if v.is_null(row) {
                        join_type == JoinType::Anti
                    } else {
                        let hit = state.contains(v.value(row) as i64);
                        match join_type {
                            JoinType::Semi => hit,
                            JoinType::Anti | JoinType::AntiIn => !hit,
                            _ => false,
                        }
                    };
                    mask.push(selected);
                }
            }};
        }
        match only.data_type() {
            arrow::datatypes::DataType::Int8 => probe_int!(Int8Array),
            arrow::datatypes::DataType::Int16 => probe_int!(Int16Array),
            arrow::datatypes::DataType::Int32 => probe_int!(Int32Array),
            arrow::datatypes::DataType::Int64 => probe_int!(Int64Array),
            arrow::datatypes::DataType::Date32 => probe_int!(Date32Array),
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, _) => {
                probe_int!(TimestampNanosecondArray)
            }
            arrow::datatypes::DataType::Boolean => probe_int!(BooleanArray),
            _ => {
                return Err(QsdbError::meta("semi mask: mixed key types"));
            }
        }
    } else {
        let mut buf: Vec<u8> = Vec::new();
        for row in 0..n {
            encode_join_key(&key_cols, row, &mut buf);
            let selected = if buf.first() == Some(&0) {
                join_type == JoinType::Anti
            } else {
                let hit = state.contains_bytes(buf.as_slice());
                match join_type {
                    JoinType::Semi => hit,
                    JoinType::Anti | JoinType::AntiIn => !hit,
                    _ => false,
                }
            };
            mask.push(selected);
        }
    }
    Ok(BooleanArray::from(mask))
}

impl ExecNode for HashJoinExec {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn next(&mut self) -> Option<Result<RecordBatch>> {
        if self.probe_done {
            return None;
        }
        if let Err(e) = self.build_phase() {
            return Some(Err(e));
        }
        let join_type = self.join_type;
        let out_schema = Arc::clone(&self.schema);
        let probe_keys = self.probe_keys.clone();
        if join_type.is_semi_family() {
            let exists = self.exists_state.as_ref().unwrap();
            loop {
                if let Err(e) = self.ctx.check() {
                    return Some(Err(e));
                }
                let batch = self.probe.next()?;
                let batch = match batch {
                    Ok(b) => b,
                    Err(e) => return Some(Err(e)),
                };
                if batch.num_rows() == 0 {
                    continue;
                }
                match join_batch_semi(&batch, exists, &probe_keys, &out_schema, join_type) {
                    Ok(Some(b)) => return Some(Ok(b)),
                    Ok(None) => continue,
                    Err(e) => return Some(Err(e)),
                }
            }
        }
        let build = self.build_state.as_ref().unwrap();
        loop {
            if let Err(e) = self.ctx.check() {
                return Some(Err(e));
            }
            let batch = self.probe.next()?;
            let batch = match batch {
                Ok(b) => b,
                Err(e) => return Some(Err(e)),
            };
            if batch.num_rows() == 0 {
                continue;
            }
            let out = match join_batch(&batch, build, &probe_keys, &out_schema, join_type) {
                Ok(Some(b)) => b,
                Ok(None) => continue, // 无匹配且非 LEFT
                Err(e) => return Some(Err(e)),
            };
            return Some(Ok(out));
        }
    }
}

/// 单个 probe 批的连接。
fn join_batch(
    probe_batch: &RecordBatch,
    state: &BuildState,
    probe_keys: &[PhysExpr],
    out_schema: &SchemaRef,
    join_type: JoinType,
) -> Result<Option<RecordBatch>> {
    let n = probe_batch.num_rows();
    // probe 键求值
    let mut key_cols: Vec<ArrayRef> = Vec::with_capacity(probe_keys.len());
    for k in probe_keys {
        key_cols.push(eval::eval(k, probe_batch)?);
    }
    // 每行：build 行号列表（NULL 键 → 空）
    let mut probe_idx: Vec<u32> = Vec::new();
    let mut build_idx: Vec<u32> = Vec::new();
    let mut unmatched: Vec<u32> = Vec::new();
    let mut buf: Vec<u8> = Vec::new();
    for row in 0..n {
        encode_join_key(&key_cols, row, &mut buf);
        if buf.first() == Some(&0) {
            unmatched.push(row as u32);
            continue;
        }
        match state.index.get(buf.as_slice()) {
            Some(rows) => {
                for &b in rows {
                    probe_idx.push(row as u32);
                    build_idx.push(b);
                }
            }
            None => unmatched.push(row as u32),
        }
    }
    // LEFT：未匹配行补 NULL build 侧
    let emit_unmatched = join_type == JoinType::LeftOuter && !unmatched.is_empty();
    if probe_idx.is_empty() && !emit_unmatched {
        return Ok(None);
    }
    let n_probe_cols = probe_batch.num_columns();
    let n_build_cols = state.table.num_columns();
    // 声明行数 = 实际输出行数：INNER 丢弃未匹配行（M6-2 修复——
    // 原先恒计入 unmatched，未匹配行存在时与列长不符触发 batch 校验失败）
    let total_rows = probe_idx.len() + usize::from(emit_unmatched) * unmatched.len();
    // 左（probe）列索引 = 匹配行 ++ 未匹配行（保留左值）
    let mut all_probe = probe_idx.clone();
    if emit_unmatched {
        all_probe.extend_from_slice(&unmatched);
    }
    let indices = UInt32Array::from(all_probe);
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(n_probe_cols + n_build_cols);
    if !probe_idx.is_empty() || emit_unmatched {
        for c in 0..n_probe_cols {
            let taken = take(probe_batch.column(c), &indices, None)
                .map_err(|e| QsdbError::invalid(format!("join take: {e}")))?;
            cols.push(taken);
        }
    } else {
        for c in 0..n_probe_cols {
            cols.push(new_null_like(probe_batch.column(c).as_ref(), 0));
        }
    }
    // 右（build）列
    if total_rows > 0 {
        let build_indices = UInt32Array::from(build_idx);
        for c in 0..n_build_cols {
            let taken = take(state.table.column(c), &build_indices, None)
                .map_err(|e| QsdbError::invalid(format!("join take: {e}")))?;
            if emit_unmatched {
                // 未匹配段补 NULL：先取 build 段，再拼接 NULL 段
                let nulls = new_null_like(taken.as_ref(), unmatched.len());
                let parts: Vec<&dyn Array> = vec![taken.as_ref(), nulls.as_ref()];
                cols.push(
                    concat(&parts).map_err(|e| QsdbError::invalid(format!("join concat: {e}")))?,
                );
            } else {
                cols.push(taken);
            }
        }
    } else {
        for c in 0..n_build_cols {
            cols.push(new_null_like(state.table.column(c).as_ref(), 0));
        }
    }
    let _ = n_probe_cols;
    RecordBatch::try_new_with_options(
        Arc::clone(out_schema),
        cols,
        &RecordBatchOptions::new().with_row_count(Some(total_rows)),
    )
    .map(Some)
    .map_err(|e| QsdbError::invalid(format!("join output: {e}")))
}

/// semi/anti 族单 probe 批：存在集探测 → 选中左行 take（仅左列，
/// 无右列展开、无重复——semi 语义天然免 distinct）。
fn join_batch_semi(
    probe_batch: &RecordBatch,
    state: &ExistsState,
    probe_keys: &[PhysExpr],
    out_schema: &SchemaRef,
    join_type: JoinType,
) -> Result<Option<RecordBatch>> {
    // NOT IN 三值语义：build 含 NULL → 全查询恒空
    if join_type == JoinType::AntiIn && state.has_null {
        return Ok(None);
    }
    let n = probe_batch.num_rows();
    let mut key_cols: Vec<ArrayRef> = Vec::with_capacity(probe_keys.len());
    for k in probe_keys {
        key_cols.push(eval::eval(k, probe_batch)?);
    }
    let mut sel: Vec<u32> = Vec::with_capacity(n);
    // 单整数键快路径（与 build 侧形态一致才有效）
    if let (ExistsKeys::I64(_) | ExistsKeys::Bitmap { .. }, [only]) = (&state.keys, key_cols.as_slice())
    {
        for row in 0..n {
            if only.is_null(row) {
                // probe NULL：Semi 不发射；Anti 发射（零命中）；AntiIn 不发射
                if join_type == JoinType::Anti {
                    sel.push(row as u32);
                }
                continue;
            }
            if let Some(v) = int_key_value(only, row) {
                let hit = state.contains(v);
                match (join_type, hit) {
                    (JoinType::Semi, true)
                    | (JoinType::Anti, false)
                    | (JoinType::AntiIn, false) => sel.push(row as u32),
                    _ => {}
                }
            } else {
                return Err(QsdbError::meta("semi join: mixed key types"));
            }
        }
    } else {
        let mut buf: Vec<u8> = Vec::new();
        for row in 0..n {
            encode_join_key(&key_cols, row, &mut buf);
            let hit = buf.first() != Some(&0) && state.contains_bytes(buf.as_slice());
            if buf.first() == Some(&0) && join_type == JoinType::Anti {
                sel.push(row as u32); // NULL 键零命中 → NOT EXISTS 发射
                continue;
            }
            match (join_type, hit) {
                (JoinType::Semi, true)
                | (JoinType::Anti, false)
                | (JoinType::AntiIn, false) => sel.push(row as u32),
                _ => {}
            }
        }
    }
    if sel.is_empty() {
        return Ok(None);
    }
    let indices = UInt32Array::from(sel);
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(probe_batch.num_columns());
    for c in 0..probe_batch.num_columns() {
        let taken = take(probe_batch.column(c), &indices, None)
            .map_err(|e| QsdbError::invalid(format!("semi take: {e}")))?;
        cols.push(taken);
    }
    let n_out = cols.first().map(|c| c.len()).unwrap_or(0);
    RecordBatch::try_new_with_options(
        Arc::clone(out_schema),
        cols,
        &RecordBatchOptions::new().with_row_count(Some(n_out)),
    )
    .map(Some)
    .map_err(|e| QsdbError::invalid(format!("semi output: {e}")))
}

fn concat_batches(batches: &[RecordBatch], schema: &SchemaRef) -> Result<RecordBatch> {
    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::clone(schema)));
    }
    if batches.len() == 1 {
        return Ok(batches[0].clone());
    }
    let n_cols = schema.fields().len();
    let mut cols = Vec::with_capacity(n_cols);
    for c in 0..n_cols {
        let parts: Vec<&dyn Array> = batches.iter().map(|b| b.column(c).as_ref()).collect();
        cols.push(concat(&parts).map_err(|e| QsdbError::invalid(format!("concat: {e}")))?);
    }
    let n: usize = batches.iter().map(|b| b.num_rows()).sum();
    RecordBatch::try_new_with_options(
        Arc::clone(schema),
        cols,
        &RecordBatchOptions::new().with_row_count(Some(n)),
    )
    .map_err(|e| QsdbError::invalid(format!("concat batch: {e}")))
}

fn new_null_like(arr: &dyn Array, n: usize) -> ArrayRef {
    arrow::array::new_null_array(arr.data_type(), n)
}

/// 连接键编码：与 agg 的 encode_key_row 同构（null 前缀 tag + 类型化值）。
fn encode_join_key(arrays: &[ArrayRef], row: usize, buf: &mut Vec<u8>) {
    buf.clear();
    for a in arrays {
        if a.is_null(row) {
            buf.push(0);
        } else {
            buf.push(1);
            encode_value(a, row, buf);
        }
    }
}

fn encode_value(a: &dyn Array, i: usize, buf: &mut Vec<u8>) {
    use arrow::datatypes::*;
    match a.data_type() {
        DataType::Int8 => buf.extend_from_slice(
            &a.as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .value(i)
                .to_le_bytes(),
        ),
        DataType::Int16 => buf.extend_from_slice(
            &a.as_any()
                .downcast_ref::<Int16Array>()
                .unwrap()
                .value(i)
                .to_le_bytes(),
        ),
        DataType::Int32 => buf.extend_from_slice(
            &a.as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(i)
                .to_le_bytes(),
        ),
        DataType::Int64 => buf.extend_from_slice(
            &a.as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(i)
                .to_le_bytes(),
        ),
        DataType::Float32 => buf.extend_from_slice(
            &a.as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(i)
                .to_le_bytes(),
        ),
        DataType::Float64 => buf.extend_from_slice(
            &a.as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(i)
                .to_le_bytes(),
        ),
        DataType::Date32 => buf.extend_from_slice(
            &a.as_any()
                .downcast_ref::<Date32Array>()
                .unwrap()
                .value(i)
                .to_le_bytes(),
        ),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => buf.extend_from_slice(
            &a.as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap()
                .value(i)
                .to_le_bytes(),
        ),
        DataType::Boolean => buf.push(u8::from(
            a.as_any().downcast_ref::<BooleanArray>().unwrap().value(i),
        )),
        DataType::Decimal128(_, _) => buf.extend_from_slice(
            &a.as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .value(i)
                .to_le_bytes(),
        ),
        DataType::Utf8 => {
            let s = a.as_any().downcast_ref::<StringArray>().unwrap().value(i);
            buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
            buf.extend_from_slice(s.as_bytes());
        }
        other => {
            let s = format!("{other:?}");
            buf.extend_from_slice(s.as_bytes());
        }
    }
}
