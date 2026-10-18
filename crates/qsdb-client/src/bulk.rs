//! **`qsdb-bulk`**（列式，零依赖）：查询默认通道 + 批量装载通道。
//!
//! 布局与语义以 `docs/bulk-format.md` 为准（**契约级稳定**，本模块是它的
//! **第二份独立实现**——内核那份在 `crates/qsdb/src/bulk.rs`，两份互不引用，
//! 靠测试对死）。选它当默认通道的理由：
//!
//! - **列式**：定长列取值零拷贝（[`ColumnView`] 借自响应体，不复制）；
//! - **零依赖**：不拉 arrow（`ipc` 通道才要）；
//! - 与 `ipc` 同档速度（同为列式二进制）。
//!
//! 代价（**明说**）：整份负载一次性在内存里——要恒定内存请走 `rows` 通道
//! （[`crate::RowStream`]）。
//!
//! ## 解码严格到什么程度
//!
//! 坏字节一律**显式报错**，绝不"能读多少算多少"：magic/版本、定长列
//! `data_len` 与宽度是否相符、`Utf8` 的 `offsets` 是否单调且落在数据段内。
//! 静默截断在协议面 = 把半份数据当全份（写进库里更糟）。

use crate::error::{Error, Result, decode_err};
use crate::value::{ColumnDesc, Tag, ValueRef, decode_value, parse_decimal_unscaled};

const MAGIC: &[u8; 8] = b"QSDBBULK";
const VERSION: u16 = 1;
const FMT: &str = "bulk";

// ---------------------------------------------------------------------------
// 解码
// ---------------------------------------------------------------------------

/// 一笔批量结果（列式）。`raw` 是响应体，列视图全部借它。
#[derive(Debug)]
pub struct BulkTable {
    schema: Vec<ColumnDesc>,
    n_rows: usize,
    raw: Vec<u8>,
    cols: Vec<ColLayout>,
}

#[derive(Debug, Clone, Copy)]
struct ColLayout {
    /// `(起点, 长度)`；`None` = 本列无 NULL 位图。
    nulls: Option<(usize, usize)>,
    /// 数据段 `(起点, 长度)`。定长列 = `n_rows × 宽度`。
    data: (usize, usize),
    /// `Utf8` 的偏移段 `(起点, 长度)`——`(n_rows + 1)` 个 u32。
    offsets: Option<(usize, usize)>,
}

impl BulkTable {
    /// 解析一份 `qsdb-bulk` 负载。
    pub fn decode(bytes: Vec<u8>) -> Result<Self> {
        let mut r = Cur { b: &bytes, p: 0 };
        if r.take(8)? != MAGIC {
            return Err(decode_err(FMT, "magic 不是 QSDBBULK"));
        }
        let ver = r.u16()?;
        if ver != VERSION {
            return Err(decode_err(
                FMT,
                format!("版本 {ver} 不认识（本客户端支持 {VERSION}）"),
            ));
        }
        let n_cols = r.u16()? as usize;
        let n_rows = r.u64()? as usize;
        let mut schema = Vec::with_capacity(n_cols);
        for _ in 0..n_cols {
            let l = r.u16()? as usize;
            let name = std::str::from_utf8(r.take(l)?)
                .map_err(|e| decode_err(FMT, format!("列名不是合法 UTF-8: {e}")))?
                .to_string();
            let tag = Tag::from_code(r.u8()?)?;
            let mut desc = ColumnDesc::new(name, tag);
            // Decimal 的列描述多两个字节 (precision, scale)（格式文档 §类型表；
            // 与内核 bulk 编码器对齐）——值本身只是未缩放 i128
            if tag == Tag::Decimal128 {
                let p = r.u8()?;
                let s = r.u8()?;
                desc.decimal = Some((p, s as i8));
            }
            schema.push(desc);
        }
        let mut cols = Vec::with_capacity(n_cols);
        for d in &schema {
            let null_len = r.u64()? as usize;
            let nulls = if null_len == 0 {
                None
            } else {
                let want = n_rows.div_ceil(8);
                if null_len != want {
                    return Err(decode_err(
                        FMT,
                        format!(
                            "列 '{}' 的 NULL 位图应为 {want} 字节（{n_rows} 行），实为 {null_len}",
                            d.name
                        ),
                    ));
                }
                let s = r.p;
                r.take(null_len)?;
                Some((s, null_len))
            };
            let data_len = r.u64()? as usize;
            let offsets = if d.tag.is_variable() {
                // ⚠️ 偏移段的长度由**行数**决定（`n_rows + 1` 个 u32），
                // **不是** `data_len`——后者是数据段（UTF-8 串接）的字节数。
                // 这两者混淆过一次，是 `bulk_编码器输出与手推字节逐字节一致`
                // 那条测试抓出来的（解码器错、编码器对）。
                let want = (n_rows + 1) * 4;
                let s = r.p;
                r.take(want)?;
                Some((s, want))
            } else {
                None
            };
            let w = d.tag.fixed_width().unwrap_or(0);
            if w > 0 {
                let want = n_rows
                    .checked_mul(w)
                    .ok_or_else(|| decode_err(FMT, "行数 × 宽度溢出"))?;
                if data_len != want {
                    return Err(decode_err(
                        FMT,
                        format!(
                            "列 '{}' 定长数据应为 {want} 字节（{n_rows} × {w}），实为 {data_len}",
                            d.name
                        ),
                    ));
                }
            }
            let s = r.p;
            r.take(data_len)?;
            cols.push(ColLayout {
                nulls,
                data: (s, data_len),
                offsets,
            });
        }
        // 尾巴上多出来的字节 = 两侧对格式的理解已经分叉，报出来
        if r.p != bytes.len() {
            return Err(decode_err(
                FMT,
                format!("负载末尾多出 {} 字节", bytes.len() - r.p),
            ));
        }
        let t = Self {
            schema,
            n_rows,
            raw: bytes,
            cols,
        };
        // 偏移量的合法性在解析期就查完（别等到取值时才 panic）
        for (i, c) in t.cols.iter().enumerate() {
            if c.offsets.is_some() {
                t.check_offsets(i)?;
            }
        }
        Ok(t)
    }

    fn check_offsets(&self, i: usize) -> Result<()> {
        let c = &self.cols[i];
        let (os, ol) = c.offsets.expect("仅变长列调用");
        let dl = c.data.1;
        let offs = &self.raw[os..os + ol];
        let mut prev = 0u32;
        for k in 0..=self.n_rows {
            let v = u32::from_le_bytes(offs[k * 4..k * 4 + 4].try_into().expect("长度已查"));
            if v < prev {
                return Err(decode_err(
                    FMT,
                    format!("列 '{}' 的偏移量倒退（第 {k} 项）", self.schema[i].name),
                ));
            }
            if v as usize > dl {
                return Err(decode_err(
                    FMT,
                    format!(
                        "列 '{}' 的偏移量 {v} 超出数据段（{dl} 字节）",
                        self.schema[i].name
                    ),
                ));
            }
            prev = v;
        }
        Ok(())
    }

    /// 列描述。
    pub fn schema(&self) -> &[ColumnDesc] {
        &self.schema
    }

    /// 行数。
    pub fn num_rows(&self) -> usize {
        self.n_rows
    }

    /// 列数。
    pub fn num_columns(&self) -> usize {
        self.schema.len()
    }

    /// 是否零行（**零行结果是合法的**：`LIMIT 0` 会回一个空表 + 完整 schema，
    /// 见 `docs/对接契约.md` §二之一）。
    pub fn is_empty(&self) -> bool {
        self.n_rows == 0
    }

    /// 按序号取列。
    pub fn column(&self, i: usize) -> Result<ColumnView<'_>> {
        let c = self
            .cols
            .get(i)
            .ok_or_else(|| Error::Usage(format!("列序号 {i} 越界（共 {} 列）", self.cols.len())))?;
        let d = &self.schema[i];
        Ok(make_view(d.tag, c, &self.raw, self.n_rows))
    }

    /// 按名取列（重名取第一个）。
    pub fn column_by_name(&self, name: &str) -> Result<ColumnView<'_>> {
        let i = self
            .schema
            .iter()
            .position(|c| c.name == name)
            .ok_or_else(|| Error::Usage(format!("结果里没有列 '{name}'")))?;
        self.column(i)
    }

    /// 第 `row` 行第 `i` 列的值（零拷贝）。逐行扫用 [`BulkTable::rows`]。
    pub fn value(&self, row: usize, i: usize) -> Result<ValueRef<'_>> {
        if row >= self.n_rows {
            return Err(Error::Usage(format!(
                "行号 {row} 越界（共 {} 行）",
                self.n_rows
            )));
        }
        self.column(i)?.get(row)
    }

    /// 逐行视图（行式消费；列式消费请直接用 [`BulkTable::column`]）。
    pub fn rows(&self) -> Rows<'_> {
        Rows {
            table: self,
            next: 0,
        }
    }
}

fn make_view<'a>(tag: Tag, c: &ColLayout, raw: &'a [u8], n_rows: usize) -> ColumnView<'a> {
    let (ds, dl) = c.data;
    let data = &raw[ds..ds + dl];
    let nulls = c.nulls.map(|(s, l)| &raw[s..s + l]);
    match c.offsets {
        Some((os, ol)) => ColumnView::Var {
            tag,
            offsets: &raw[os..os + ol],
            data,
            nulls,
            n_rows,
        },
        None => ColumnView::Fixed {
            tag,
            data,
            nulls,
            n_rows,
        },
    }
}

/// 一列的字面视图（**零拷贝**）。
#[derive(Debug, Clone, Copy)]
pub enum ColumnView<'a> {
    /// 定长列：`data` 是 `n_rows × 宽度` 的裸值（小端）。
    Fixed {
        /// 类型标签。
        tag: Tag,
        /// 数据段。
        data: &'a [u8],
        /// NULL 位图（`None` = 本列无 NULL）。
        nulls: Option<&'a [u8]>,
        /// 行数。
        n_rows: usize,
    },
    /// 变长列（`Utf8`）：`offsets` 是 `n_rows + 1` 个 u32，`data` 是串接字节。
    Var {
        /// 类型标签。
        tag: Tag,
        /// 偏移段（`(n_rows + 1)` 个小端 u32）。
        offsets: &'a [u8],
        /// 数据段。
        data: &'a [u8],
        /// NULL 位图。
        nulls: Option<&'a [u8]>,
        /// 行数。
        n_rows: usize,
    },
}

impl<'a> ColumnView<'a> {
    /// 类型标签。
    pub fn tag(&self) -> Tag {
        match self {
            ColumnView::Fixed { tag, .. } | ColumnView::Var { tag, .. } => *tag,
        }
    }

    /// 行数。
    pub fn len(&self) -> usize {
        match self {
            ColumnView::Fixed { n_rows, .. } | ColumnView::Var { n_rows, .. } => *n_rows,
        }
    }

    /// 是否零行。
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// 第 `i` 行是否 NULL（位图为准，LSB first）。
    pub fn is_null(&self, i: usize) -> bool {
        let nulls = match self {
            ColumnView::Fixed { nulls, .. } | ColumnView::Var { nulls, .. } => *nulls,
        };
        nulls.is_some_and(|b| b[i / 8] & (1 << (i % 8)) != 0)
    }

    /// 第 `i` 行的值（**零拷贝**）。
    pub fn get(&self, i: usize) -> Result<ValueRef<'a>> {
        if i >= self.len() {
            return Err(Error::Usage(format!(
                "行号 {i} 越界（共 {} 行）",
                self.len()
            )));
        }
        if self.is_null(i) {
            return Ok(ValueRef::Null);
        }
        match *self {
            ColumnView::Fixed { tag, data, .. } => {
                let w = tag.fixed_width().expect("定长列");
                decode_value(tag, &data[i * w..(i + 1) * w], FMT)
            }
            ColumnView::Var {
                tag, offsets, data, ..
            } => {
                let off = |k: usize| {
                    u32::from_le_bytes(offsets[k * 4..k * 4 + 4].try_into().expect("长度已查"))
                        as usize
                };
                decode_value(tag, &data[off(i)..off(i + 1)], FMT)
            }
        }
    }

    /// 逐值迭代（`(行号, 值)`）。
    pub fn iter(&self) -> ColumnIter<'a, '_> {
        ColumnIter { c: self, next: 0 }
    }
}

/// [`ColumnView::iter`] 的迭代器。
#[derive(Debug)]
pub struct ColumnIter<'a, 'c> {
    c: &'c ColumnView<'a>,
    next: usize,
}

impl<'a, 'c> Iterator for ColumnIter<'a, 'c> {
    type Item = Result<ValueRef<'a>>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.next >= self.c.len() {
            return None;
        }
        let i = self.next;
        self.next += 1;
        Some(self.c.get(i))
    }
}

/// [`BulkTable::rows`] 的行迭代器。
#[derive(Debug)]
pub struct Rows<'a> {
    table: &'a BulkTable,
    next: usize,
}

impl<'a> Iterator for Rows<'a> {
    /// 一行（按列序取值，零拷贝）。
    type Item = Row<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.next >= self.table.n_rows {
            return None;
        }
        let i = self.next;
        self.next += 1;
        Some(Row {
            table: self.table,
            i,
        })
    }
}

/// 一行（借自 [`BulkTable`]）。
#[derive(Debug, Clone, Copy)]
pub struct Row<'a> {
    table: &'a BulkTable,
    i: usize,
}

impl<'a> Row<'a> {
    /// 行号。
    pub fn index(&self) -> usize {
        self.i
    }

    /// 按列序取值。
    pub fn get(&self, col: usize) -> Result<ValueRef<'a>> {
        self.table.value(self.i, col)
    }

    /// 按列名取值。
    pub fn get_by_name(&self, name: &str) -> Result<ValueRef<'a>> {
        let c = self.table.column_by_name(name)?;
        c.get(self.i)
    }

    /// 列数。
    pub fn len(&self) -> usize {
        self.table.num_columns()
    }

    /// 是否零列（分区表理论上可能，实际不会）。
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// ---------------------------------------------------------------------------
// 写入（批量装载）
// ---------------------------------------------------------------------------

/// 一列输入数据（**写侧**）。
///
/// 为什么用 `Vec<Option<T>>` 而不是 Arrow 数组：本 crate 默认不拉 arrow。
/// 手上已经有 Arrow 批的，请开 `ipc` feature 用
/// [`Client::append_batches`](crate::Client::append_batches)。
#[derive(Debug, Clone)]
pub enum Column {
    /// 布尔列。
    Bool(Vec<Option<bool>>),
    /// 32 位整数列。
    I32(Vec<Option<i32>>),
    /// 64 位整数列。
    I64(Vec<Option<i64>>),
    /// 双精度列。
    F64(Vec<Option<f64>>),
    /// 字符串列。
    Utf8(Vec<Option<String>>),
    /// 日期列（自 1970-01-01 的天数）。
    Date32(Vec<Option<i32>>),
    /// 时间戳列（自 epoch 的纳秒）。
    TimestampNs(Vec<Option<i64>>),
    /// 8 位整数列（十九批Ⅳ：bulk 类型扩展，与 rows 同标签集）。
    I8(Vec<Option<i8>>),
    /// 16 位整数列。
    I16(Vec<Option<i16>>),
    /// 单精度列。
    F32(Vec<Option<f32>>),
    /// 二进制列（原始字节）。
    Binary(Vec<Option<Vec<u8>>>),
    /// **DECIMAL 列（未缩放 i128）**：`precision`/`scale` 必须与表定义一致
    /// （`/schema` 的 `DECIMAL(p,s)`）——payload 的列描述会携带它们，值本身
    /// 只是缩放后的整数。字符串 → 未缩放的精确转换见
    /// [`Column::decimals_from_str`]（不经 f64）。
    Decimal128 {
        /// 未缩放值（`30.80` @ scale=2 ⇒ `3080`）。
        unscaled: Vec<Option<i128>>,
        /// 精度（总有效位数，≤38）。
        precision: u8,
        /// 小数位。
        scale: i8,
    },
    /// 时间列（自午夜的纳秒）。
    Time64Ns(Vec<Option<i64>>),
}

impl Column {
    /// 行数。
    pub fn len(&self) -> usize {
        match self {
            Column::Bool(v) => v.len(),
            Column::I32(v) => v.len(),
            Column::I64(v) => v.len(),
            Column::F64(v) => v.len(),
            Column::Utf8(v) => v.len(),
            Column::Date32(v) => v.len(),
            Column::TimestampNs(v) => v.len(),
            Column::I8(v) => v.len(),
            Column::I16(v) => v.len(),
            Column::F32(v) => v.len(),
            Column::Binary(v) => v.len(),
            Column::Decimal128 { unscaled, .. } => unscaled.len(),
            Column::Time64Ns(v) => v.len(),
        }
    }

    /// 是否零行。
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// 线上类型标签。
    pub fn tag(&self) -> Tag {
        match self {
            Column::Bool(_) => Tag::Bool,
            Column::I32(_) => Tag::I32,
            Column::I64(_) => Tag::I64,
            Column::F64(_) => Tag::F64,
            Column::Utf8(_) => Tag::Utf8,
            Column::Date32(_) => Tag::Date32,
            Column::TimestampNs(_) => Tag::TimestampNs,
            Column::I8(_) => Tag::I8,
            Column::I16(_) => Tag::I16,
            Column::F32(_) => Tag::F32,
            Column::Binary(_) => Tag::Binary,
            Column::Decimal128 { .. } => Tag::Decimal128,
            Column::Time64Ns(_) => Tag::Time64Ns,
        }
    }

    /// 日期列（`DATE`）——`From` 做不到（`Vec<Option<i32>>` 已是 `I32`）。
    pub fn dates<I: IntoIterator<Item = Option<i32>>>(it: I) -> Column {
        Column::Date32(it.into_iter().collect())
    }

    /// 时间戳列（纳秒）。
    pub fn timestamps<I: IntoIterator<Item = Option<i64>>>(it: I) -> Column {
        Column::TimestampNs(it.into_iter().collect())
    }

    /// **DECIMAL 列：从十进制字符串精确转未缩放 i128**（`"30.8"` @ scale=1 ⇒
    /// `308`）——**不经 f64**（精度红线：`0.1` 类小数经浮点会带脏尾巴）。
    ///
    /// `precision`/`scale` 来自表定义（`/schema` 的 `DECIMAL(p,s)`）。规则：
    /// 小数位**超过** `scale` ⇒ 报错（四舍五入是编造值）；总有效位数超过
    /// `precision` ⇒ 报错（写进去也会被服务端拒，早fail带原值更好定位）。
    /// 空串/`NULL` ⇒ None。
    pub fn decimals_from_str<I, S>(
        it: I,
        precision: u8,
        scale: i8,
    ) -> std::result::Result<Column, String>
    where
        I: IntoIterator<Item = Option<S>>,
        S: AsRef<str>,
    {
        let scale = scale.max(0);
        let mut unscaled = Vec::new();
        for (i, x) in it.into_iter().enumerate() {
            match x {
                None => unscaled.push(None),
                Some(raw) => {
                    let t = raw.as_ref().trim();
                    let v = parse_decimal_unscaled(t, scale)
                        .map_err(|e| format!("DECIMAL 列第 {} 行值 {t:?} 不合法: {e}", i + 1))?;
                    // 位数 ≤ precision（负号不算位）
                    let digits = v.unsigned_abs().to_string().len() as u32;
                    if digits > precision as u32 {
                        return Err(format!(
                            "DECIMAL 列第 {} 行值 {t:?} 超出精度 {precision}（缩放后 {v} 有 {digits} 位）",
                            i + 1
                        ));
                    }
                    unscaled.push(Some(v));
                }
            }
        }
        Ok(Column::Decimal128 {
            unscaled,
            precision,
            scale,
        })
    }

    /// 字符串列（从任意迭代器建，省得先攒 `Vec<Option<String>>`）。
    pub fn utf8<I, S>(it: I) -> Column
    where
        I: IntoIterator<Item = Option<S>>,
        S: Into<String>,
    {
        Column::Utf8(
            it.into_iter()
                .map(|x| x.map(Into::into))
                .collect::<Vec<Option<String>>>(),
        )
    }

    fn null_bitmap(&self) -> Option<Vec<u8>> {
        let n = self.len();
        let mut any = false;
        let mut bits = vec![0u8; n.div_ceil(8)];
        let mut mark = |i: usize, is_null: bool| {
            if is_null {
                any = true;
                bits[i / 8] |= 1 << (i % 8);
            }
        };
        match self {
            Column::Bool(v) => {
                for (i, x) in v.iter().enumerate() {
                    mark(i, x.is_none())
                }
            }
            Column::I32(v) | Column::Date32(v) => {
                for (i, x) in v.iter().enumerate() {
                    mark(i, x.is_none())
                }
            }
            Column::I64(v) | Column::TimestampNs(v) => {
                for (i, x) in v.iter().enumerate() {
                    mark(i, x.is_none())
                }
            }
            Column::F64(v) => {
                for (i, x) in v.iter().enumerate() {
                    mark(i, x.is_none())
                }
            }
            Column::Utf8(v) => {
                for (i, x) in v.iter().enumerate() {
                    mark(i, x.is_none())
                }
            }
            Column::I8(v) => {
                for (i, x) in v.iter().enumerate() {
                    mark(i, x.is_none())
                }
            }
            Column::I16(v) => {
                for (i, x) in v.iter().enumerate() {
                    mark(i, x.is_none())
                }
            }
            Column::F32(v) => {
                for (i, x) in v.iter().enumerate() {
                    mark(i, x.is_none())
                }
            }
            Column::Time64Ns(v) => {
                for (i, x) in v.iter().enumerate() {
                    mark(i, x.is_none())
                }
            }
            Column::Binary(v) => {
                for (i, x) in v.iter().enumerate() {
                    mark(i, x.is_none())
                }
            }
            Column::Decimal128 { unscaled, .. } => {
                for (i, x) in unscaled.iter().enumerate() {
                    mark(i, x.is_none())
                }
            }
        }
        if any { Some(bits) } else { None }
    }

    fn encode(&self, out: &mut Vec<u8>) {
        let n = self.len();
        match self.null_bitmap() {
            Some(b) => {
                out.extend_from_slice(&(b.len() as u64).to_le_bytes());
                out.extend_from_slice(&b);
            }
            None => out.extend_from_slice(&0u64.to_le_bytes()),
        }
        match self {
            Column::Bool(v) => {
                out.extend_from_slice(&(n as u64).to_le_bytes());
                for x in v {
                    // NULL 行占位（值无意义）——解码端按位图还原
                    out.push(match x {
                        Some(true) => 1,
                        _ => 0,
                    });
                }
            }
            Column::I32(v) | Column::Date32(v) => {
                out.extend_from_slice(&((n * 4) as u64).to_le_bytes());
                for x in v {
                    out.extend_from_slice(&x.unwrap_or(0).to_le_bytes());
                }
            }
            Column::I64(v) | Column::TimestampNs(v) => {
                out.extend_from_slice(&((n * 8) as u64).to_le_bytes());
                for x in v {
                    out.extend_from_slice(&x.unwrap_or(0).to_le_bytes());
                }
            }
            Column::F64(v) => {
                out.extend_from_slice(&((n * 8) as u64).to_le_bytes());
                for x in v {
                    out.extend_from_slice(&x.unwrap_or(0.0).to_le_bytes());
                }
            }
            Column::I8(v) => {
                out.extend_from_slice(&(n as u64).to_le_bytes());
                for x in v {
                    out.push(x.unwrap_or(0) as u8);
                }
            }
            Column::I16(v) => {
                out.extend_from_slice(&((n * 2) as u64).to_le_bytes());
                for x in v {
                    out.extend_from_slice(&x.unwrap_or(0).to_le_bytes());
                }
            }
            Column::F32(v) => {
                out.extend_from_slice(&((n * 4) as u64).to_le_bytes());
                for x in v {
                    out.extend_from_slice(&x.unwrap_or(0.0).to_le_bytes());
                }
            }
            Column::Time64Ns(v) => {
                out.extend_from_slice(&((n * 8) as u64).to_le_bytes());
                for x in v {
                    out.extend_from_slice(&x.unwrap_or(0).to_le_bytes());
                }
            }
            Column::Binary(v) => {
                let mut data = Vec::new();
                let mut offs = Vec::with_capacity(n + 1);
                offs.push(0u32);
                for x in v {
                    if let Some(b) = x {
                        data.extend_from_slice(b);
                    }
                    offs.push(data.len() as u32);
                }
                out.extend_from_slice(&(data.len() as u64).to_le_bytes());
                for o in &offs {
                    out.extend_from_slice(&o.to_le_bytes());
                }
                out.extend_from_slice(&data);
            }
            Column::Decimal128 {
                unscaled,
                precision,
                scale,
            } => {
                // 值 = 未缩放 i128（16 字节小端）；precision/scale 由**列描述**
                // 携带（encode 的描述符写入处按 tag 写）——这里只管值
                let _ = (precision, scale);
                out.extend_from_slice(&((n * 16) as u64).to_le_bytes());
                for x in unscaled {
                    out.extend_from_slice(&x.unwrap_or(0).to_le_bytes());
                }
            }
            Column::Utf8(v) => {
                let mut data = Vec::new();
                let mut offs = Vec::with_capacity(n + 1);
                offs.push(0u32);
                for x in v {
                    if let Some(s) = x {
                        data.extend_from_slice(s.as_bytes());
                    }
                    offs.push(data.len() as u32);
                }
                out.extend_from_slice(&(data.len() as u64).to_le_bytes());
                for o in &offs {
                    out.extend_from_slice(&o.to_le_bytes());
                }
                out.extend_from_slice(&data);
            }
        }
    }
}

impl From<Vec<Option<bool>>> for Column {
    fn from(v: Vec<Option<bool>>) -> Self {
        Column::Bool(v)
    }
}
impl From<Vec<Option<i32>>> for Column {
    fn from(v: Vec<Option<i32>>) -> Self {
        Column::I32(v)
    }
}
impl From<Vec<Option<i64>>> for Column {
    fn from(v: Vec<Option<i64>>) -> Self {
        Column::I64(v)
    }
}
impl From<Vec<Option<f64>>> for Column {
    fn from(v: Vec<Option<f64>>) -> Self {
        Column::F64(v)
    }
}
impl From<Vec<Option<String>>> for Column {
    fn from(v: Vec<Option<String>>) -> Self {
        Column::Utf8(v)
    }
}

/// 把若干列编码成一份 `qsdb-bulk` 负载（**列序必须 = 表列序**，含分区列）。
///
/// 列数/列名是否与表相符由**服务端**判定（客户端不知道表结构，不猜）——
/// 不符时回的是带 code 的错误体（`INVALID_ARGUMENT` / `COLUMN_NOT_FOUND`）。
pub fn encode(cols: &[(&str, Column)]) -> Result<Vec<u8>> {
    if cols.is_empty() {
        return Err(Error::Usage("append 至少要有一列".into()));
    }
    let n_rows = cols[0].1.len();
    for (name, c) in cols {
        if c.len() != n_rows {
            return Err(Error::Usage(format!(
                "列 '{name}' 有 {} 行，与首列 {n_rows} 行不符——批量装载不接受参差不齐的列",
                c.len()
            )));
        }
    }
    let mut out = Vec::with_capacity(64 + n_rows * cols.len() * 8);
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&VERSION.to_le_bytes());
    out.extend_from_slice(&(cols.len() as u16).to_le_bytes());
    out.extend_from_slice(&(n_rows as u64).to_le_bytes());
    for (name, c) in cols {
        let b = name.as_bytes();
        if b.len() > u16::MAX as usize {
            return Err(Error::Usage(format!("列名过长（{} 字节）", b.len())));
        }
        out.extend_from_slice(&(b.len() as u16).to_le_bytes());
        out.extend_from_slice(b);
        out.push(c.tag().code());
        // Decimal 的列描述多两个字节 (precision, scale)——与内核解码器对齐
        if let Column::Decimal128 {
            precision, scale, ..
        } = c
        {
            out.push(*precision);
            out.push(*scale as u8);
        }
    }
    for (_, c) in cols {
        c.encode(&mut out);
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// 字节游标
// ---------------------------------------------------------------------------

struct Cur<'a> {
    b: &'a [u8],
    p: usize,
}

impl<'a> Cur<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self
            .p
            .checked_add(n)
            .ok_or_else(|| decode_err(FMT, "偏移溢出"))?;
        if end > self.b.len() {
            return Err(decode_err(
                FMT,
                format!("负载在第 {} 字节处结束，还要 {n} 字节", self.b.len()),
            ));
        }
        let s = &self.b[self.p..end];
        self.p = end;
        Ok(s)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(
            self.take(2)?.try_into().expect("长度已查"),
        ))
    }

    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(
            self.take(8)?.try_into().expect("长度已查"),
        ))
    }
}
