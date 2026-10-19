//! `read_csv` 外表：自研 CSV 解析 + 类型推断（零新依赖）。
//!
//! 为什么自己写而不是引 `csv`/`arrow-csv`：本仓红线是「Arrow/parquet 之外
//! 无重依赖」（设计文档 §14），而 CSV 需要的只是 RFC4180 的一个子集——
//! 引号包裹字段（内含分隔符/换行/`""` 转义）、CRLF 与 LF、BOM、表头。
//! 这点代码量不值得多一个 crate（还要多一份版本/安全问题面）。
//!
//! **支持到哪里（能力边界，超出即报错或按字面处理，不静默兜底）**：
//! - 分隔符固定 `,`；引号固定 `"`；行终止 `\n` / `\r\n`（孤立 `\r` 也当终止）；
//! - 文件首 3 字节为 UTF-8 BOM 则跳过；字段必须是合法 UTF-8；
//! - 首行 = 表头（无表头文件不支持：列名是 SQL 绑定的前提）；
//! - 空字段 → NULL（含显式 `""`——与 DuckDB 默认 `nullstr=''` 同口径）；
//!   不支持自定义 nullstr；
//! - 引号内换行/逗号是字面量，`""` → `"`；引号内的 `\r\n` **不归一**为 `\n`；
//! - 非空字段不 trim 空白（RFC 无语义，猜错的代价比多一个函数大）；
//! - 引号出现在字段中间（`a"b`）按字面字符收，不报错（真实数据里常见于
//!   未转义的英寸符号等）。
//!
//! **不做**：s3:// 远端（CSV 是顺序流，没有 parquet 的 range/footer 可省；
//! 由搬运工具先落本地）；压缩（.gz 等）；`HEADER=false`；分隔符/引号参数；
//! 谓词下推到扫描内（无 footer/RG 元数据可裁——谓词留在 FilterExec 上，
//! 逐行解析本来就要读整条记录才能定位字段边界）。列裁剪**有**：由
//! exec_builder 的通用投影下推驱动，省下未选列的类型解析与数组构建。

use std::fs::File;
use std::io::Read;
use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder};
use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use crate::catalog::{ColumnDef, TableDef};
use crate::error::{QsdbError, Result};
use crate::exec::ExecNode;
use crate::scan::FileLoc;
use crate::types::LogicalType;

/// 类型推断采样行数（表头之后的数据行）。
///
/// 取 1000：常见数据列在前千行内已暴露取值形态；再多读不改变结论却拖慢绑定。
/// 反过来说——第 1001 行才出现的"非数字"会在读取期报错而不是改写列类型，
/// 这是**刻意**的：类型一旦推断就必须对全批稳定（否则同列跨批两种类型，
/// 下游算子全部要处理 schema 漂移）。
const INFER_ROWS: usize = 1000;

/// 每批行数（与 parquet 扫描的 `scan::BATCH_SIZE` 同量级）。
const BATCH_ROWS: usize = 8192;

/// 绑定期：路径 → 文件列表 + 推断出的表定义（列名 = 首文件表头）。
pub(crate) fn resolve_csv_table(path: &str) -> Result<(Vec<FileLoc>, Arc<TableDef>)> {
    if path.starts_with("s3://") {
        return Err(QsdbError::unsupported(
            "read_csv: s3:// not supported (copy the object to a local path first)",
        ));
    }
    let files = crate::scan::resolve_file_locs(path, "read_csv")?;
    let def = infer_table_def(&files)?;
    Ok((files, def))
}

/// 表头 + 前 N 行 → 表定义。
///
/// 只采样**首文件**：同一 glob 下的多个 CSV 按"同构"约定处理（同表头、同列序、
/// 同类型域）——这是数据湖里 CSV 分片的通行前提；违反它就该先做 ETL 而不是
/// 让读取端猜。每个文件自己的表头各自跳过（见 `CsvFileReader`）。
fn infer_table_def(files: &[FileLoc]) -> Result<Arc<TableDef>> {
    let first = files
        .first()
        .ok_or_else(|| QsdbError::file_not_found("read_csv: no input file"))?;
    let path = first.display_path();
    let rd = open_local(&path)?;
    let mut rec = RecordReader::new(rd)?;

    let header = rec
        .next_record()?
        .ok_or_else(|| QsdbError::parse_err(format!("read_csv: empty file {}", path.display())))?;
    if header.is_empty() {
        return Err(QsdbError::parse_err(format!(
            "read_csv: empty header line in {}",
            path.display()
        )));
    }
    let mut names: Vec<String> = Vec::with_capacity(header.len());
    for (i, cell) in header.iter().enumerate() {
        match cell {
            Some(n) if !n.is_empty() => names.push(n.clone()),
            // 空列名（常见于尾随逗号）报错而不是自动命名：自动命名会让
            // `SELECT *` 的列名与文件内容对不上，导入工具的列映射静默错位。
            _ => {
                return Err(QsdbError::invalid(format!(
                    "read_csv: empty column name at position {} in {}",
                    i + 1,
                    path.display()
                )));
            }
        }
    }

    // 逐列累计观测到的取值类别（位或），最后归一到一种类型。
    let mut kinds = vec![0u8; names.len()];
    for _ in 0..INFER_ROWS {
        let Some(row) = rec.next_record()? else {
            break;
        };
        if row.len() != names.len() {
            return Err(width_err(&path, rec.line_no(), row.len(), names.len()));
        }
        for (i, cell) in row.iter().enumerate() {
            if let Some(v) = cell {
                kinds[i] |= classify(v);
            }
        }
    }

    let columns: Vec<ColumnDef> = names
        .iter()
        .zip(kinds.iter())
        .map(|(n, k)| ColumnDef {
            name: n.clone(),
            logical_type: resolve_kind(*k),
            // CSV 没有 NOT NULL 概念：空字段即 NULL，列一律 nullable
            nullable: true,
            check: None,
            default: None,
            comment: None,
        })
        .collect();
    Ok(Arc::new(TableDef {
        version: 1,
        name: "read_csv".into(),
        columns,
        // 不做 hive 目录分区推断：CSV 路径里的 k=v 段语义与 parquet 外表一致时
        // 才有意义，而 read_csv v1 定位是"把导出文件查起来"，分区交给写入端。
        partition_keys: Vec::new(),
        created_at: 0,
        properties: std::collections::HashMap::new(),
    }))
}

// ---------------------------------------------------------------------------
// 类型推断
// ---------------------------------------------------------------------------

const K_INT: u8 = 1;
const K_FLOAT: u8 = 2;
const K_BOOL: u8 = 4;
const K_STR: u8 = 8;

/// 单值类别。判序 = INT → FLOAT → BOOL → STR（"1" 是整数而不是布尔，
/// 与 SQL 的类型提升直觉一致；布尔列要求整列都是 true/false 字面量）。
fn classify(v: &str) -> u8 {
    if v.parse::<i64>().is_ok() {
        return K_INT;
    }
    if v.parse::<f64>().is_ok() {
        return K_FLOAT;
    }
    if v.eq_ignore_ascii_case("true") || v.eq_ignore_ascii_case("false") {
        return K_BOOL;
    }
    K_STR
}

/// 列类别并集 → 类型。任一值识别为文本 → utf8（"拿不准就 utf8"）；
/// 整数与浮点混合 → float64；全空列（无样本）→ utf8。
fn resolve_kind(k: u8) -> LogicalType {
    if k & K_STR != 0 || k == 0 {
        return LogicalType::Varchar { max_chars: None };
    }
    if k & K_FLOAT != 0 {
        return LogicalType::Float64;
    }
    if k & K_INT != 0 {
        return LogicalType::Int64;
    }
    LogicalType::Boolean
}

// ---------------------------------------------------------------------------
// 字节源 + 记录切分
// ---------------------------------------------------------------------------

/// 读缓冲大小（与原来的 `BufReader::with_capacity(64*1024)` 同量级）。
const CSV_CHUNK: usize = 64 * 1024;

/// 字节源：**自有缓冲 + 游标**。
///
/// 为什么不用 `BufRead::fill_buf()` + `consume(1)`（原来的写法）：那是
/// **每个字节一次 trait 虚调用**。535MB 的导入文件 ⇒ 约 **5.35 亿次**，
/// 实测解析 3.26s 里约 **2.7s** 在这里（`基准对账笔记` §三之二）。
/// 改成"按住一块缓冲 + 游标"之后 `bump` 就是一次数组索引，
/// 只在游标走到块尾时 `refill`。
struct ByteSrc<R: Read> {
    r: R,
    buf: Vec<u8>,
    /// 已消费到的位置（`buf[pos..len]` 是尚未消费的数据）
    pos: usize,
    len: usize,
}

impl<R: Read> ByteSrc<R> {
    fn new(r: R) -> Self {
        Self {
            r,
            buf: vec![0u8; CSV_CHUNK],
            pos: 0,
            len: 0,
        }
    }

    /// 补充缓冲：**把未消费的残留搬到块首**，再把块读满。
    ///
    /// ⚠️ 跨块记录（引号内换行、超长字段、CR 落在块尾而 LF 在下一块）全靠这一步 ——
    /// 残留**一字节都不能丢**。`pos == 0` 时 `copy_within` 是空操作。
    fn refill(&mut self) -> Result<()> {
        if self.pos > 0 {
            self.buf.copy_within(self.pos..self.len, 0);
            self.len -= self.pos;
            self.pos = 0;
        }
        while self.len < self.buf.len() {
            match self.r.read(&mut self.buf[self.len..]) {
                Ok(0) => break,
                Ok(n) => self.len += n,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => {
                    return Err(QsdbError::parse_err(format!("read_csv: read failed: {e}")));
                }
            }
        }
        Ok(())
    }

    #[inline]
    fn peek(&mut self) -> Result<Option<u8>> {
        if self.pos >= self.len {
            self.refill()?;
        }
        Ok(self.buf.get(self.pos).copied())
    }

    #[inline]
    fn bump(&mut self) -> Result<Option<u8>> {
        if self.pos >= self.len {
            self.refill()?;
            if self.pos >= self.len {
                return Ok(None); // 真的读完了
            }
        }
        let c = self.buf[self.pos];
        self.pos += 1;
        Ok(Some(c))
    }

    /// 跳过文件首的 UTF-8 BOM（Excel/PowerShell 导出的常见前缀）。
    fn skip_bom(&mut self) -> Result<()> {
        if self.pos >= self.len {
            self.refill()?; // 块内至少可见 3 字节（不足 3 字节的文件本来也不可能带 BOM）
        }
        if self.buf[self.pos..self.len].starts_with(&[0xEF, 0xBB, 0xBF]) {
            self.pos += 3;
        }
        Ok(())
    }
}

/// 记录切分器：一个 `Option<String>` 字段序列 = 一条记录。
/// `None` = 空字段（NULL）；引号内可含分隔符与换行。
struct RecordReader<R: Read> {
    src: ByteSrc<R>,
    /// 已产出的记录数（1-based 行号 = 该值 + 1，仅对不含引号内换行的文件精确；
    /// 报错文案用它定位"第几条记录"，比字节偏移对人有用）
    recs: u64,
    /// 当前记录的字段字节（**已去转义**；同一记录内连续存放，**跨记录复用**）。
    ///
    /// 为什么要有它：原实现是"每字段一个 `Vec<u8>` + `String::from_utf8`"
    /// ⇒ 5M 行 × 7 列 = **约 7000 万次堆分配**，那是解析 2.86s 里的主要残留。
    /// 现在字段字节直接写进这块记录级缓冲，字段只记 `(offset, len)` 跨度 ⇒
    /// **每记录 0 次分配**（缓冲复用）。
    arena: Vec<u8>,
    /// 每个字段在 `arena` 里的 `(offset, len)`。
    /// **`len == 0` 即 NULL** —— 与旧 `field()` 的口径一致（空字段与 `""` 都是 NULL）。
    spans: Vec<(u32, u32)>,
}

impl<R: Read> RecordReader<R> {
    fn new(r: R) -> Result<Self> {
        let mut src = ByteSrc::new(r);
        src.skip_bom()?;
        Ok(Self {
            src,
            recs: 0,
            arena: Vec::with_capacity(256),
            spans: Vec::with_capacity(16),
        })
    }

    fn line_no(&self) -> u64 {
        self.recs
    }

    /// 字段 `i` 的 UTF-8 视图；`None` = 空字段（NULL）。
    ///
    /// **非法 UTF-8 在这里报错**（与旧 `field()` 同文案）——不做 lossy 替换：
    /// 推断结果里没有 BINARY 列，脏字节只会变成一排 U+FFFD 混进字符串列，
    /// 用户看到的是"查得到但内容是乱码"，比一条明确的报错难排查得多。
    fn field(&self, i: usize) -> Result<Option<&str>> {
        let (o, l) = self.spans[i];
        if l == 0 {
            return Ok(None);
        }
        let bytes = &self.arena[o as usize..(o + l) as usize];
        std::str::from_utf8(bytes)
            .map(Some)
            .map_err(|e| QsdbError::parse_err(format!("read_csv: field is not valid UTF-8: {e}")))
    }

    fn nfields(&self) -> usize {
        self.spans.len()
    }

    /// 解析下一条记录到 [`Self::spans`]/[`Self::arena`]；`false` = 文件读完。
    /// 空行（整行无内容）**跳过**——尾随空行在真实导出文件里极常见，
    /// 按"1 列空字段"收会撞行宽校验。
    ///
    /// ⚠️ **这段状态机与逐字节版本逐条对应**（引号态、字段中部引号按字面收、
    /// CRLF、空行跳过、EOF 处收尾），改动它必须过 `tests/read_csv.rs`
    /// 的跨块边界用例（`records_straddle_read_buffer_boundaries` 等）。
    fn next_record_spans(&mut self) -> Result<bool> {
        self.spans.clear();
        self.arena.clear();
        // 本字段是否以引号开头（防 `""` 之后再进引号态）
        let mut quoted = false;
        // 当前字段在 arena 里的起点
        let mut start = 0usize;
        loop {
            match self.src.bump()? {
                None => {
                    if self.spans.is_empty() && self.arena.len() == start {
                        return Ok(false); // 干净的 EOF（末行有换行）
                    }
                    self.push_span(start);
                    self.recs += 1;
                    return Ok(true);
                }
                Some(b'"') => {
                    if self.arena.len() == start && !quoted {
                        quoted = true;
                        Self::read_quoted(&mut self.src, &mut self.arena, self.recs)?;
                    } else {
                        self.arena.push(b'"'); // 字段中部的引号：按字面收（见模块头边界）
                    }
                }
                Some(b',') => {
                    self.push_span(start);
                    start = self.arena.len();
                    quoted = false;
                }
                Some(b'\n') => {
                    if self.spans.is_empty() && self.arena.len() == start {
                        continue; // 空行
                    }
                    self.push_span(start);
                    self.recs += 1;
                    return Ok(true);
                }
                Some(b'\r') => {
                    if self.src.peek()? == Some(b'\n') {
                        self.src.bump()?;
                    }
                    if self.spans.is_empty() && self.arena.len() == start {
                        continue; // 空行
                    }
                    self.push_span(start);
                    self.recs += 1;
                    return Ok(true);
                }
                Some(c) => self.arena.push(c),
            }
        }
    }

    #[inline]
    fn push_span(&mut self, start: usize) {
        self.spans
            .push((start as u32, (self.arena.len() - start) as u32));
    }

    /// **冷路径**（表头 / 类型推断）：把当前记录物化成 `Vec<Option<String>>`。
    /// 热路径（`next_batch`）不走这里 —— 它直接从 `spans`/`arena` 取 `&str`，
    /// 免得为每个字段造一个 `String`。
    fn next_record(&mut self) -> Result<Option<Vec<Option<String>>>> {
        if !self.next_record_spans()? {
            return Ok(None);
        }
        let mut out = Vec::with_capacity(self.spans.len());
        for i in 0..self.spans.len() {
            out.push(self.field(i)?.map(str::to_string));
        }
        Ok(Some(out))
    }

    /// 引号态内容：`""` → 字面 `"`，其余（含换行）原样收。
    fn read_quoted(src: &mut ByteSrc<R>, out: &mut Vec<u8>, recs: u64) -> Result<()> {
        loop {
            match src.bump()? {
                None => {
                    return Err(QsdbError::parse_err(format!(
                        "read_csv: unterminated quoted field at record {}",
                        recs + 1
                    )));
                }
                Some(b'"') => {
                    if src.peek()? == Some(b'"') {
                        src.bump()?;
                        out.push(b'"');
                    } else {
                        return Ok(());
                    }
                }
                Some(c) => out.push(c),
            }
        }
    }
}

/// 打开本地文件。**不再外套 `BufReader`** —— 缓冲已经由 [`ByteSrc`] 自己管，
/// 再套一层只是多一次拷贝。
fn open_local(path: &std::path::Path) -> Result<File> {
    File::open(path).map_err(|e| {
        QsdbError::file_not_found(format!("read_csv: open {} failed: {e}", path.display()))
    })
}

fn width_err(path: &std::path::Path, line: u64, got: usize, want: usize) -> QsdbError {
    QsdbError::invalid(format!(
        "read_csv: record {line} in {} has {got} fields, header has {want}",
        path.display()
    ))
}

// ---------------------------------------------------------------------------
// 逐文件流式读取 → RecordBatch
// ---------------------------------------------------------------------------

/// 单文件的批流：表头跳过一次，此后按 `BATCH_ROWS` 攒批。
struct CsvFileReader<R: Read> {
    rec: RecordReader<R>,
    path: std::path::PathBuf,
    /// 表定义列数（全宽，用于行宽校验与列名报错）
    names: Vec<String>,
    /// 输出列 = 表列的哪些下标（列裁剪后的顺序）
    keep: Vec<usize>,
    builders: Vec<Option<ColBuilder>>,
    schema: SchemaRef,
    header_done: bool,
}

impl<R: Read> CsvFileReader<R> {
    fn new(
        r: R,
        path: std::path::PathBuf,
        names: Vec<String>,
        types: Vec<LogicalType>,
        keep: Vec<usize>,
        schema: SchemaRef,
    ) -> Result<Self> {
        let builders = types
            .iter()
            .enumerate()
            .map(|(i, t)| keep.contains(&i).then(|| ColBuilder::new(t)))
            .collect();
        Ok(Self {
            rec: RecordReader::new(r)?,
            path,
            names,
            keep,
            builders,
            schema,
            header_done: false,
        })
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if !self.header_done {
            // 每个文件都有自己的表头：跳过。宽度与推断表头不一致 → 明确报错，
            // 不静默错位（同构约定被违反时，用户必须知道）。
            let h = self.rec.next_record()?.ok_or_else(|| {
                QsdbError::parse_err(format!("read_csv: {} has no header", self.path.display()))
            })?;
            if h.len() != self.names.len() {
                return Err(QsdbError::invalid(format!(
                    "read_csv: header of {} has {} columns, expected {} (schema comes from the first file)",
                    self.path.display(),
                    h.len(),
                    self.names.len()
                )));
            }
            self.header_done = true;
        }
        let mut rows = 0usize;
        while rows < BATCH_ROWS {
            // **热路径不物化**：字段直接以 `&str` 从记录级 arena 取，
            // 不再为每个字段造 `String`（见 `RecordReader::arena`）。
            if !self.rec.next_record_spans()? {
                break;
            }
            let nf = self.rec.nfields();
            if nf != self.names.len() {
                return Err(width_err(
                    &self.path,
                    self.rec.line_no(),
                    nf,
                    self.names.len(),
                ));
            }
            let line = self.rec.line_no();
            for i in 0..nf {
                if self.builders[i].is_none() {
                    continue; // 未被选中的列：不解析、也不校验
                }
                // `cell` 借的是 `self.rec` 的 arena，`b` 借的是 `self.builders`
                // —— 两个不相交的字段，借用检查器放行（别改成经由 `self` 的方法取）。
                let cell = self.rec.field(i)?;
                let b = self.builders[i].as_mut().expect("上面刚判过 Some");
                b.push(cell).map_err(|e| {
                    QsdbError::type_err(format!(
                        "read_csv: {} record {} column '{}': {e}",
                        self.path.display(),
                        line,
                        self.names[i]
                    ))
                })?;
            }
            rows += 1;
        }
        if rows == 0 {
            return Ok(None);
        }
        // 只 finish 选中的列：未选列的 builder 保持空（从一开始就没 push）
        let mut out_cols: Vec<ArrayRef> = Vec::with_capacity(self.keep.len());
        for &i in &self.keep {
            let b = self.builders[i]
                .as_mut()
                .expect("keep 中的列必有 builder（构造期同一份 keep）");
            out_cols.push(b.finish());
        }
        RecordBatch::try_new(Arc::clone(&self.schema), out_cols)
            .map_err(|e| QsdbError::invalid(format!("read_csv: build batch failed: {e}")))
            .map(Some)
    }
}

/// 单列的按类型 builder（enum 派发：一次 match 决定后续全部行的解析路径）。
enum ColBuilder {
    I64(Int64Builder),
    F64(Float64Builder),
    Bool(BooleanBuilder),
    Str(StringBuilder),
}

impl ColBuilder {
    fn new(ty: &LogicalType) -> Self {
        match ty {
            LogicalType::Int64 => Self::I64(Int64Builder::new()),
            LogicalType::Float64 => Self::F64(Float64Builder::new()),
            LogicalType::Boolean => Self::Bool(BooleanBuilder::new()),
            // 推断只产出上面三种数值/布尔类型，其余一律按文本收
            _ => Self::Str(StringBuilder::new()),
        }
    }

    /// 推入一个字段（`None` = NULL）。解析失败 → Err（由调用方补文件/行/列上下文）。
    fn push(&mut self, v: Option<&str>) -> Result<()> {
        match self {
            Self::I64(b) => {
                match v {
                    None => b.append_null(),
                    Some(s) => b.append_value(s.parse::<i64>().map_err(|_| {
                        QsdbError::type_err(format!("cannot parse '{s}' as BIGINT"))
                    })?),
                }
            }
            Self::F64(b) => {
                match v {
                    None => b.append_null(),
                    Some(s) => b.append_value(s.parse::<f64>().map_err(|_| {
                        QsdbError::type_err(format!("cannot parse '{s}' as DOUBLE"))
                    })?),
                }
            }
            Self::Bool(b) => match v {
                None => b.append_null(),
                Some(s) if s.eq_ignore_ascii_case("true") => b.append_value(true),
                Some(s) if s.eq_ignore_ascii_case("false") => b.append_value(false),
                Some(s) => {
                    return Err(QsdbError::type_err(format!(
                        "cannot parse '{s}' as BOOLEAN"
                    )));
                }
            },
            Self::Str(b) => match v {
                None => b.append_null(),
                Some(s) => b.append_value(s),
            },
        }
        Ok(())
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            Self::I64(b) => Arc::new(b.finish()),
            Self::F64(b) => Arc::new(b.finish()),
            Self::Bool(b) => Arc::new(b.finish()),
            Self::Str(b) => Arc::new(b.finish()),
        }
    }
}

// ---------------------------------------------------------------------------
// 执行节点
// ---------------------------------------------------------------------------

/// `read_csv` 扫描节点：逐文件顺序流式（不做并行分片——CSV 解析是纯 CPU、
/// 无 footer 可跳，多文件并行是后续优化项，v1 边界见模块头）。
pub(crate) struct CsvScanExec {
    def: Arc<TableDef>,
    files: Vec<FileLoc>,
    /// 表列下标（列裁剪后顺序）
    keep: Vec<usize>,
    schema: SchemaRef,
    cur: Option<CsvFileReader<File>>,
    next_file: usize,
    ctx: Arc<crate::query::QueryContext>,
}

impl CsvScanExec {
    pub(crate) fn new(
        def: Arc<TableDef>,
        files: Vec<FileLoc>,
        projection: Option<Vec<String>>,
        ctx: Arc<crate::query::QueryContext>,
    ) -> Result<Self> {
        let keep: Vec<usize> = match &projection {
            None => (0..def.columns.len()).collect(),
            Some(names) => names
                .iter()
                .map(|n| {
                    def.columns
                        .iter()
                        .position(|c| c.name.eq_ignore_ascii_case(n))
                        .ok_or_else(|| {
                            QsdbError::column_not_found(format!("column '{n}' not in read_csv"))
                        })
                })
                .collect::<Result<_>>()?,
        };
        let schema = Arc::new(Schema::new(
            keep.iter()
                .map(|&i| {
                    Field::new(
                        &def.columns[i].name,
                        def.columns[i].logical_type.arrow_type(),
                        true,
                    )
                })
                .collect::<Vec<_>>(),
        ));
        Ok(Self {
            def,
            files,
            keep,
            schema,
            cur: None,
            next_file: 0,
            ctx,
        })
    }

    /// 打开下一个文件；`false` = 文件读完。
    /// 单文件打不开/无表头 → 整条查询报错（**不跳过**：glob 下静默少数据比报错难查）。
    fn open_next(&mut self) -> Result<bool> {
        if self.next_file < self.files.len() {
            let path = self.files[self.next_file].display_path();
            self.next_file += 1;
            let r = open_local(&path)?;
            let names: Vec<String> = self.def.columns.iter().map(|c| c.name.clone()).collect();
            let types: Vec<LogicalType> = self
                .def
                .columns
                .iter()
                .map(|c| c.logical_type.clone())
                .collect();
            self.cur = Some(CsvFileReader::new(
                r,
                path,
                names,
                types,
                self.keep.clone(),
                Arc::clone(&self.schema),
            )?);
            return Ok(true);
        }
        Ok(false)
    }
}

impl ExecNode for CsvScanExec {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn next(&mut self) -> Option<Result<RecordBatch>> {
        loop {
            if let Err(e) = self.ctx.check() {
                return Some(Err(e));
            }
            let Some(cur) = self.cur.as_mut() else {
                match self.open_next() {
                    Ok(true) => continue,
                    Ok(false) => return None,
                    Err(e) => return Some(Err(e)),
                }
            };
            match cur.next_batch() {
                Ok(Some(b)) => return Some(Ok(b)),
                Ok(None) => self.cur = None,
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// 单测：切分器（引号/转义/换行/BOM/空字段）
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn records(data: &str) -> Vec<Vec<Option<String>>> {
        let mut r = RecordReader::new(data.as_bytes()).unwrap();
        let mut out = Vec::new();
        while let Some(rec) = r.next_record().unwrap() {
            out.push(rec);
        }
        out
    }

    fn cell(v: &str) -> Option<String> {
        Some(v.to_string())
    }

    #[test]
    fn plain_and_empty_fields() {
        let r = records("a,b,c\n1,,3\n");
        assert_eq!(r.len(), 2);
        assert_eq!(r[0], vec![cell("a"), cell("b"), cell("c")]);
        assert_eq!(r[1], vec![cell("1"), None, cell("3")]);
    }

    #[test]
    fn crlf_and_missing_trailing_newline() {
        let r = records("a,b\r\n1,2\r\n3,4");
        assert_eq!(r.len(), 3);
        assert_eq!(r[2], vec![cell("3"), cell("4")]);
    }

    #[test]
    fn quoted_comma_newline_and_escape() {
        let r = records("\"a,b\",\"line1\nline2\",\"say \"\"hi\"\"\"\n");
        assert_eq!(r.len(), 1);
        assert_eq!(
            r[0],
            vec![cell("a,b"), cell("line1\nline2"), cell("say \"hi\"")]
        );
    }

    #[test]
    fn quoted_empty_is_null() {
        // `""` 与空字段同口径 → NULL（见模块头边界）
        let r = records("\"\",x\n");
        assert_eq!(r[0], vec![None, cell("x")]);
    }

    #[test]
    fn bom_skipped() {
        let r = records("\u{feff}a,b\n1,2\n");
        assert_eq!(r[0], vec![cell("a"), cell("b")]);
        assert_eq!(r[1], vec![cell("1"), cell("2")]);
    }

    #[test]
    fn unterminated_quote_errors() {
        let mut r = RecordReader::new("\"abc\n".as_bytes()).unwrap();
        assert!(r.next_record().is_err());
    }

    #[test]
    fn kind_resolution() {
        assert_eq!(resolve_kind(K_INT), LogicalType::Int64);
        assert_eq!(resolve_kind(K_INT | K_FLOAT), LogicalType::Float64);
        assert_eq!(resolve_kind(K_BOOL), LogicalType::Boolean);
        assert_eq!(
            resolve_kind(K_STR),
            LogicalType::Varchar { max_chars: None }
        );
        assert_eq!(resolve_kind(0), LogicalType::Varchar { max_chars: None });
        assert_eq!(classify("true"), K_BOOL);
        assert_eq!(classify("1"), K_INT);
        assert_eq!(classify("1.5"), K_FLOAT);
        assert_eq!(classify("x"), K_STR);
    }
}
