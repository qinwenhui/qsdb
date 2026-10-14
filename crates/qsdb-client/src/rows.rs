//! **`qsdb-rows`**（行式、零依赖、**可流式**）：读侧解码。
//!
//! 布局以 `docs/rows-format.md` 为准（契约级稳定，本模块是内核那份之外的
//! **第二份独立实现**）。它是本客户端**唯一内存恒定**的读通道：
//!
//! | | `bulk`（默认） | **`rows`** |
//! |---|---|---|
//! | 内存 | O(全量结果) | **O(一个块)** |
//! | 取值 | 列式、零拷贝 | 行主序（每值一次 memcpy 进块 arena） |
//!
//! 所以：**结果可能很大 / 要边读边处理 → 用 [`crate::Client::query_stream`]**；
//! 结果不大又要列式零拷贝 → 默认的 `bulk`。
//!
//! **帧边界就是 `row_count`**：一个 body 里可以有任意多块，攒够一拨就处理，
//! 不必等整个结果到齐。
//!
//! ⚠️ 读侧 `flags` 恒为 1（列描述随头一起发）。`flags = 0`（写侧形态）在本模块
//! **显式报错**——那意味着"schema 由使用方提供"，客户端读路径没有这个前提。

use std::io::Read;
use std::sync::Arc;

use crate::error::{Error, Result, decode_err};
use crate::value::{ColumnDesc, Tag, ValueRef, decode_value};

const MAGIC: &[u8; 8] = b"QSDBROWS";
const VERSION: u16 = 1;
const FMT: &str = "rows";
const BUF: usize = 64 * 1024;

// ---------------------------------------------------------------------------
// 缓冲读（跨块不丢字节）
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct Reader<R> {
    r: R,
    buf: Vec<u8>,
    pos: usize,
    filled: usize,
}

impl<R: Read> Reader<R> {
    fn new(r: R) -> Self {
        Self {
            r,
            buf: vec![0u8; BUF],
            pos: 0,
            filled: 0,
        }
    }

    /// 保证缓冲里有 ≥1 字节可用；返回可用字节数（0 = 到流尾）。
    fn ensure(&mut self) -> Result<usize> {
        if self.pos < self.filled {
            return Ok(self.filled - self.pos);
        }
        self.pos = 0;
        self.filled = 0;
        loop {
            match self.r.read(&mut self.buf) {
                Ok(0) => return Ok(0),
                Ok(n) => {
                    self.filled = n;
                    return Ok(n);
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(Error::Io(e)),
            }
        }
    }

    fn exact(&mut self, n: usize, out: &mut [u8]) -> Result<()> {
        let mut got = 0;
        while got < n {
            if self.ensure()? == 0 {
                return Err(decode_err(FMT, format!("流在读到第 {got}/{n} 字节时结束")));
            }
            let take = (n - got).min(self.filled - self.pos);
            out[got..got + take].copy_from_slice(&self.buf[self.pos..self.pos + take]);
            self.pos += take;
            got += take;
        }
        Ok(())
    }

    /// 读 `n` 字节**追加**到 `out`（跨缓冲分片拷贝）。
    fn take_into(&mut self, n: usize, out: &mut Vec<u8>) -> Result<()> {
        let mut left = n;
        while left > 0 {
            if self.ensure()? == 0 {
                return Err(decode_err(FMT, format!("流在还差 {left} 字节时结束")));
            }
            let take = left.min(self.filled - self.pos);
            out.extend_from_slice(&self.buf[self.pos..self.pos + take]);
            self.pos += take;
            left -= take;
        }
        Ok(())
    }

    fn u8(&mut self) -> Result<u8> {
        let mut b = [0u8; 1];
        self.exact(1, &mut b)?;
        Ok(b[0])
    }

    fn u16(&mut self) -> Result<u16> {
        let mut b = [0u8; 2];
        self.exact(2, &mut b)?;
        Ok(u16::from_le_bytes(b))
    }

    fn u32(&mut self) -> Result<u32> {
        let mut b = [0u8; 4];
        self.exact(4, &mut b)?;
        Ok(u32::from_le_bytes(b))
    }

    /// 读到流尾（中途放弃大结果时把 body 读干净，让连接能干净收尾）。
    fn drain(&mut self) -> Result<()> {
        while self.ensure()? > 0 {
            self.pos = self.filled;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// 头部
// ---------------------------------------------------------------------------

/// 读列描述（**读侧**：`flags` 必须带 schema）。
fn read_header<R: Read>(rd: &mut Reader<R>) -> Result<Arc<Vec<ColumnDesc>>> {
    let mut magic = [0u8; 8];
    rd.exact(8, &mut magic)?;
    if &magic != MAGIC {
        return Err(decode_err(FMT, "magic 不是 QSDBROWS"));
    }
    let ver = rd.u16()?;
    if ver != VERSION {
        return Err(decode_err(
            FMT,
            format!("版本 {ver} 不认识（本客户端支持 {VERSION}）"),
        ));
    }
    let flags = rd.u8()?;
    if flags & 1 == 0 {
        return Err(decode_err(
            FMT,
            "读侧 payload 必须带列描述（flags bit0 = 1）；flags = 0 是写侧形态",
        ));
    }
    let n_cols = rd.u16()? as usize;
    let mut schema = Vec::with_capacity(n_cols);
    for _ in 0..n_cols {
        let l = rd.u16()? as usize;
        let mut name = Vec::new();
        rd.take_into(l, &mut name)?;
        let name = String::from_utf8(name)
            .map_err(|e| decode_err(FMT, format!("列名不是合法 UTF-8: {e}")))?;
        let tag = Tag::from_code(rd.u8()?)?;
        let col_flags = rd.u8()?;
        let meta_len = rd.u16()? as usize;
        let mut meta = Vec::new();
        rd.take_into(meta_len, &mut meta)?;
        let mut d = ColumnDesc::new(name, tag);
        d.nullable = col_flags & 1 != 0;
        parse_meta(&mut d, &meta)?;
        schema.push(d);
    }
    Ok(Arc::new(schema))
}

/// 列级元数据（`meta_len` 为 0 的列必须恰好不写字节——**多给了就报错**）。
fn parse_meta(d: &mut ColumnDesc, meta: &[u8]) -> Result<()> {
    let bad = |want: &str| {
        decode_err(
            FMT,
            format!(
                "列 '{}' 的 meta 应为 {want}，实为 {} 字节",
                d.name,
                meta.len()
            ),
        )
    };
    match d.tag {
        Tag::Utf8 => {
            if meta.len() != 4 {
                return Err(bad("4 字节 max_chars"));
            }
            d.max_chars = Some(u32::from_le_bytes(meta.try_into().expect("长度已查")));
        }
        Tag::TimestampNs => {
            if meta.len() < 2 {
                return Err(bad("tz_len u16 + tz"));
            }
            let tl = u16::from_le_bytes(meta[..2].try_into().expect("长度已查")) as usize;
            if meta.len() != 2 + tl {
                return Err(bad("tz_len u16 + tz"));
            }
            if tl > 0 {
                d.tz = Some(
                    String::from_utf8(meta[2..].to_vec())
                        .map_err(|e| decode_err(FMT, format!("时区名不是合法 UTF-8: {e}")))?,
                );
            }
        }
        Tag::Decimal128 => {
            if meta.len() != 2 {
                return Err(bad("precision u8 + scale i8"));
            }
            d.decimal = Some((meta[0], meta[1] as i8));
        }
        _ => {
            if !meta.is_empty() {
                return Err(bad("0 字节（该类型无 meta）"));
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// 块
// ---------------------------------------------------------------------------

/// 一个数据块（行主序）。**自持** arena，可与流解耦（`query_stream` 拿到的块
/// 在下一块读出后依然有效）。
#[derive(Debug)]
pub struct Chunk {
    schema: Arc<Vec<ColumnDesc>>,
    n_rows: usize,
    /// 值字节池；`spans` 指进来。
    arena: Vec<u8>,
    /// 每格 `(起点, 长度)`，下标 = `行号 × 列数 + 列号`。
    spans: Vec<(u32, u32)>,
    /// 每列的 NULL 位图（空 = 该列本块无 NULL）。**位图为准**，值区照占位。
    nulls: Vec<Vec<u8>>,
}

impl Chunk {
    /// 列描述。
    pub fn schema(&self) -> &[ColumnDesc] {
        &self.schema
    }

    /// 本块行数。
    pub fn num_rows(&self) -> usize {
        self.n_rows
    }

    /// 列数。
    pub fn num_columns(&self) -> usize {
        self.schema.len()
    }

    /// 本块是否零行。
    pub fn is_empty(&self) -> bool {
        self.n_rows == 0
    }

    /// 第 `row` 行第 `col` 列（零拷贝借自块 arena）。
    pub fn get(&self, row: usize, col: usize) -> Result<ValueRef<'_>> {
        let n_cols = self.schema.len();
        if row >= self.n_rows {
            return Err(Error::Usage(format!(
                "行号 {row} 越界（本块 {} 行）",
                self.n_rows
            )));
        }
        let d = self
            .schema
            .get(col)
            .ok_or_else(|| Error::Usage(format!("列序号 {col} 越界（共 {n_cols} 列）")))?;
        let nb = &self.nulls[col];
        if !nb.is_empty() && nb[row / 8] & (1 << (row % 8)) != 0 {
            return Ok(ValueRef::Null);
        }
        let (o, l) = self.spans[row * n_cols + col];
        decode_value(d.tag, &self.arena[o as usize..(o + l) as usize], FMT)
    }

    /// 按列名取值（重名取第一个）。
    pub fn get_by_name(&self, row: usize, name: &str) -> Result<ValueRef<'_>> {
        let c = self
            .schema
            .iter()
            .position(|d| d.name == name)
            .ok_or_else(|| Error::Usage(format!("结果里没有列 '{name}'")))?;
        self.get(row, c)
    }

    /// 逐行视图。
    pub fn rows(&self) -> ChunkRows<'_> {
        ChunkRows { c: self, next: 0 }
    }
}

/// [`Chunk::rows`] 的行迭代器。
#[derive(Debug)]
pub struct ChunkRows<'a> {
    c: &'a Chunk,
    next: usize,
}

impl<'a> Iterator for ChunkRows<'a> {
    /// 一行。
    type Item = ChunkRow<'a>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.next >= self.c.n_rows {
            return None;
        }
        let i = self.next;
        self.next += 1;
        Some(ChunkRow { c: self.c, i })
    }
}

/// 一行（借自 [`Chunk`]）。
#[derive(Debug, Clone, Copy)]
pub struct ChunkRow<'a> {
    c: &'a Chunk,
    i: usize,
}

impl<'a> ChunkRow<'a> {
    /// 行号（块内）。
    pub fn index(&self) -> usize {
        self.i
    }

    /// 按列序取值。
    pub fn get(&self, col: usize) -> Result<ValueRef<'a>> {
        self.c.get(self.i, col)
    }

    /// 按列名取值。
    pub fn get_by_name(&self, name: &str) -> Result<ValueRef<'a>> {
        self.c.get_by_name(self.i, name)
    }

    /// 列数。
    pub fn len(&self) -> usize {
        self.c.schema.len()
    }

    /// 是否零列。
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// ---------------------------------------------------------------------------
// 流
// ---------------------------------------------------------------------------

/// `rows` 通道的**流式**读取器（内存恒定）：
///
/// ```no_run
/// # fn main() -> Result<(), qsdb_client::Error> {
/// # let c = qsdb_client::Client::connect("http://127.0.0.1:9430", qsdb_client::Auth::None)?;
/// let mut s = c.query_stream("SELECT * FROM big", &Default::default())?;
/// while let Some(chunk) = s.next_chunk()? {
///     for row in chunk.rows() {
///         let _id = row.get(0)?;
///     }
/// }
/// # Ok(()) }
/// ```
#[derive(Debug)]
pub struct RowStream {
    rd: Reader<crate::http::Body>,
    schema: Arc<Vec<ColumnDesc>>,
    done: bool,
    /// 已读块数 / 行数（诊断用）。
    chunks: u64,
    rows: u64,
}

impl RowStream {
    pub(crate) fn new(body: crate::http::Body) -> Result<Self> {
        let mut rd = Reader::new(body);
        let schema = read_header(&mut rd)?;
        Ok(Self {
            rd,
            schema,
            done: false,
            chunks: 0,
            rows: 0,
        })
    }

    /// 列描述（**流一开始就有**，不必等第一块）。
    pub fn schema(&self) -> &[ColumnDesc] {
        &self.schema
    }

    /// 读下一块；`None` = 结果流结束。
    pub fn next_chunk(&mut self) -> Result<Option<Chunk>> {
        if self.done {
            return Ok(None);
        }
        match read_chunk(&mut self.rd, &self.schema) {
            Ok(Some(c)) => {
                self.chunks += 1;
                self.rows += c.num_rows() as u64;
                Ok(Some(c))
            }
            Ok(None) => {
                self.done = true;
                Ok(None)
            }
            Err(e) => {
                self.done = true; // 坏流不再尝试
                Err(e)
            }
        }
    }

    /// 已读块数。
    pub fn chunks_read(&self) -> u64 {
        self.chunks
    }

    /// 已读行数。
    pub fn rows_read(&self) -> u64 {
        self.rows
    }

    /// 把剩下没读的字节丢掉（**中途放弃大结果时调它**，好让连接能干净收尾）。
    pub fn finish(&mut self) -> Result<()> {
        if !self.done {
            self.rd.drain()?;
            self.done = true;
        }
        Ok(())
    }
}

impl Iterator for RowStream {
    type Item = Result<Chunk>;
    fn next(&mut self) -> Option<Self::Item> {
        match self.next_chunk() {
            Ok(Some(c)) => Some(Ok(c)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

/// 读一块；`Ok(None)` = 干净收尾。
fn read_chunk<R: Read>(rd: &mut Reader<R>, schema: &Arc<Vec<ColumnDesc>>) -> Result<Option<Chunk>> {
    // 帧边界 = row_count。流尾干净结束 → None（而不是报错）。
    let row_count = match rd.ensure()? {
        0 => return Ok(None),
        _ => rd.u32()? as usize,
    };
    let chunk_flags = rd.u8()?;
    let n_cols = schema.len();
    let mut nulls: Vec<Vec<u8>> = vec![Vec::new(); n_cols];
    if chunk_flags & 1 != 0 {
        let nb = row_count.div_ceil(8);
        for slot in nulls.iter_mut() {
            let mut b = Vec::new();
            rd.take_into(nb, &mut b)?;
            *slot = b;
        }
    }
    if chunk_flags & !1 != 0 {
        return Err(decode_err(
            FMT,
            format!("chunk_flags 有未知位：{chunk_flags:#04x}"),
        ));
    }
    let mut arena: Vec<u8> = Vec::new();
    let mut spans: Vec<(u32, u32)> = Vec::with_capacity(row_count * n_cols);
    for _row in 0..row_count {
        for d in schema.iter() {
            // NULL 与否**由位图决定**，值区照读照占位（格式文档 §三.1：
            // 解析不依赖位图 —— 两件事解耦，故这里根本不看位图）
            let start = arena.len();
            let len = if d.tag.is_variable() {
                // 变长：u32 长度 + 字节（**NULL 行也照写**，见格式文档 §三.1）
                let l = rd.u32()? as usize;
                rd.take_into(l, &mut arena)?;
                l
            } else {
                let w = d.tag.fixed_width().expect("定长列");
                rd.take_into(w, &mut arena)?;
                w
            };
            let off = u32::try_from(start).map_err(|_| decode_err(FMT, "单块 arena 超过 4GB"))?;
            let l = u32::try_from(len).map_err(|_| decode_err(FMT, "单值超过 4GB"))?;
            spans.push((off, l));
        }
    }
    Ok(Some(Chunk {
        schema: Arc::clone(schema),
        n_rows: row_count,
        arena,
        spans,
        nulls,
    }))
}

// ---------------------------------------------------------------------------
// 一次性解码（`format=rows` 的普通 `query`）
// ---------------------------------------------------------------------------

/// 把整份 `qsdb-rows` 负载解成若干块。
///
/// 普通 [`crate::Client::query`] 走这条（body 已整读进内存）；
/// 要恒定内存请用 [`crate::Client::query_stream`]。
pub fn decode_all(bytes: &[u8]) -> Result<RowsData> {
    let mut rd = Reader::new(std::io::Cursor::new(bytes));
    let schema = read_header(&mut rd)?;
    let mut chunks = Vec::new();
    let mut starts = Vec::new();
    let mut n = 0usize;
    while let Some(c) = read_chunk(&mut rd, &schema)? {
        starts.push(n);
        n += c.num_rows();
        chunks.push(c);
    }
    rd.drain()?;
    Ok(RowsData {
        schema,
        chunks,
        starts,
        n_rows: n,
    })
}

/// 一次性解码出来的全部块。
#[derive(Debug)]
pub struct RowsData {
    schema: Arc<Vec<ColumnDesc>>,
    chunks: Vec<Chunk>,
    /// 每块的全局起始行号。
    starts: Vec<usize>,
    n_rows: usize,
}

impl RowsData {
    /// 列描述。
    pub fn schema(&self) -> &[ColumnDesc] {
        &self.schema
    }

    /// 总行数。
    pub fn num_rows(&self) -> usize {
        self.n_rows
    }

    /// 块数。
    pub fn num_chunks(&self) -> usize {
        self.chunks.len()
    }

    /// 块。
    pub fn chunks(&self) -> &[Chunk] {
        &self.chunks
    }

    /// 是否零行。
    pub fn is_empty(&self) -> bool {
        self.n_rows == 0
    }

    /// 全局第 `row` 行第 `col` 列（跨块寻址）。
    pub fn get(&self, row: usize, col: usize) -> Result<ValueRef<'_>> {
        let (k, local) = self.locate(row)?;
        self.chunks[k].get(local, col)
    }

    /// 按列名取值（跨块寻址）。
    pub fn get_by_name(&self, row: usize, name: &str) -> Result<ValueRef<'_>> {
        let (k, local) = self.locate(row)?;
        self.chunks[k].get_by_name(local, name)
    }

    fn locate(&self, row: usize) -> Result<(usize, usize)> {
        if row >= self.n_rows {
            return Err(Error::Usage(format!(
                "行号 {row} 越界（共 {} 行）",
                self.n_rows
            )));
        }
        // 块数不多，线性找即可（`starts` 单调）
        let k = match self.starts.binary_search(&row) {
            Ok(k) => k,
            Err(k) => k - 1,
        };
        Ok((k, row - self.starts[k]))
    }

    /// 逐行（跨块）——`(块号, 块内行)` 视图统一成 [`ValueRef`] 取值。
    pub fn iter_rows(&self) -> impl Iterator<Item = (usize, ChunkRow<'_>)> {
        self.chunks
            .iter()
            .enumerate()
            .flat_map(|(k, c)| c.rows().map(move |r| (k, r)))
    }
}
