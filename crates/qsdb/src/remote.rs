//! S3 及兼容对象存储的只读访问（M6-2 外表，feature `s3`）。
//!
//! - URL：`s3://bucket/key.parquet`（可带 `?endpoint=…&region=…` 查询参数；
//!   `http://` 端点自动 allow_http）；glob 通配 `*`（段内）/`**`（跨段）
//! - 凭据走 object_store 环境变量链（AWS_ACCESS_KEY_ID 等），不落配置明文
//! - 读 = range GET [`RemoteReader`]（parquet ChunkReader）：footer 与数据页
//!   都按需取字节区间——「先裁剪后传输」由 parquet 惰性读天然成立
//! - 段缓存（[`crate::filecache`]）：命中零网络；未命中且整读（无 RG 裁剪）
//!   时流式下载入缓存后读本地
//! - 同步调用：专用 tokio Runtime `block_on`（object_store 客户端需要反应器；
//!   本模块是核心库中唯一的 tokio 触点，且仅 feature s3 编入）

use std::collections::HashMap;
use std::io::Read;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex};

use bytes::{Buf, Bytes};
use futures_util::StreamExt;
use futures_util::stream::BoxStream;
use object_store::path::Path as StorePath;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt};
use parquet::errors::ParquetError;
use parquet::file::reader::{ChunkReader, Length};

use crate::error::{QsdbError, Result};

/// 共享上下文：tokio 反应器 + 按 (endpoint, region, bucket) 复用的 store 句柄。
struct Ctx {
    rt: tokio::runtime::Runtime,
    stores: Mutex<HashMap<String, Arc<dyn ObjectStore>>>,
}

static CTX: LazyLock<Ctx> = LazyLock::new(|| Ctx {
    rt: tokio::runtime::Runtime::new().expect("init tokio runtime for object_store"),
    stores: Mutex::new(HashMap::new()),
});

/// 解析后的 S3 定位（单对象）。
pub struct S3Locator {
    pub store: Arc<dyn ObjectStore>,
    pub location: StorePath,
}

/// URL 拆分结果：(bucket, key, 小写查询参数)。
type SplitUrl = (String, StorePath, Vec<(String, String)>);

/// URL 查询参数拆分：`s3://bucket/key?endpoint=…&region=…`
fn split_url(url: &str) -> Result<SplitUrl> {
    let rest = url
        .strip_prefix("s3://")
        .ok_or_else(|| QsdbError::invalid(format!("not an s3:// url: {url}")))?;
    let (path, query) = match rest.split_once('?') {
        Some((p, q)) => (p, q),
        None => (rest, ""),
    };
    let (bucket, key) = path
        .split_once('/')
        .ok_or_else(|| QsdbError::invalid(format!("s3 url needs bucket/key: {url}")))?;
    if bucket.is_empty() || key.is_empty() {
        return Err(QsdbError::invalid(format!("s3 url needs bucket/key: {url}")));
    }
    let params: Vec<(String, String)> = query
        .split('&')
        .filter(|s| !s.is_empty())
        .map(|kv| match kv.split_once('=') {
            Some((k, v)) => (k.to_ascii_lowercase(), urldecode(v)),
            None => (kv.to_ascii_lowercase(), String::new()),
        })
        .collect();
    Ok((bucket.to_string(), StorePath::from(key), params))
}

fn urldecode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let b = s.as_bytes();
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'%' if i + 2 < b.len() => {
                let hex = std::str::from_utf8(&b[i + 1..i + 3]).ok();
                match hex.and_then(|h| u8::from_str_radix(h, 16).ok()) {
                    Some(byte) => {
                        out.push(byte as char);
                        i += 3;
                    }
                    None => {
                        out.push(b[i] as char);
                        i += 1;
                    }
                }
            }
            b'+' => {
                out.push(' ');
                i += 1;
            }
            c => {
                out.push(c as char);
                i += 1;
            }
        }
    }
    out
}

fn param<'a>(params: &'a [(String, String)], k: &str) -> Option<&'a str> {
    params
        .iter()
        .find(|(n, _)| n == k)
        .map(|(_, v)| v.as_str())
}

/// 定位单对象（构建/复用 store 句柄）。
pub(crate) fn locate(url: &str) -> Result<S3Locator> {
    let (bucket, location, params) = split_url(url)?;
    let endpoint = param(&params, "endpoint");
    let region = param(&params, "region");
    let key = format!("{}|{}|{}", endpoint.unwrap_or(""), region.unwrap_or(""), bucket);
    if let Some(store) = CTX.stores.lock().ok().and_then(|m| m.get(&key).cloned()) {
        return Ok(S3Locator { store, location });
    }
    let mut b = object_store::aws::AmazonS3Builder::from_env().with_bucket_name(&bucket);
    if let Some(ep) = endpoint {
        if ep.starts_with("http://") {
            b = b.with_allow_http(true);
        }
        b = b.with_endpoint(ep);
    }
    if let Some(r) = region {
        b = b.with_region(r);
    }
    let store: Arc<dyn ObjectStore> = Arc::new(
        b.build()
            .map_err(|e| QsdbError::meta(format!("s3 client for {url}: {e}")))?,
    );
    if let Ok(mut m) = CTX.stores.lock() {
        m.insert(key, Arc::clone(&store));
    }
    Ok(S3Locator { store, location })
}

/// HEAD：ETag + size。
pub(crate) fn head(loc: &S3Locator) -> Result<ObjectMeta> {
    CTX.rt
        .block_on(loc.store.head(&loc.location))
        .map_err(|e| QsdbError::meta(format!("s3 head {}: {e}", loc.location)))
}

/// 流式整读（段缓存回填用）。
pub(crate) fn stream_reader(loc: &S3Locator) -> Result<impl Read> {
    let result = CTX
        .rt
        .block_on(loc.store.get(&loc.location))
        .map_err(|e| QsdbError::meta(format!("s3 get {}: {e}", loc.location)))?;
    Ok(StreamRead {
        stream: result.into_stream(),
        buf: Bytes::new(),
        done: false,
    })
}

/// futures 字节流 → 同步 Read 适配（逐块 block_on 拉取）。
struct StreamRead {
    stream: BoxStream<'static, object_store::Result<Bytes>>,
    buf: Bytes,
    done: bool,
}

impl Read for StreamRead {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        while self.buf.is_empty() && !self.done {
            match CTX.rt.block_on(self.stream.next()) {
                Some(Ok(b)) => self.buf = b,
                Some(Err(e)) => {
                    return Err(std::io::Error::other(format!("object stream: {e}")))
                }
                None => self.done = true,
            }
        }
        let n = self.buf.len().min(out.len());
        out[..n].copy_from_slice(&self.buf[..n]);
        self.buf.advance(n);
        Ok(n)
    }
}

/// 远端对象的 parquet ChunkReader：`get_bytes` = range GET；`get_read` =
/// 从偏移起按块拉取的顺序读（footer/元数据段用，长度有界）。
pub struct RemoteReader {
    store: Arc<dyn ObjectStore>,
    location: StorePath,
    len: AtomicU64,
}

impl RemoteReader {
    pub fn open(url: &str) -> Result<Self> {
        let loc = locate(url)?;
        let len = head(&loc)?.size;
        Ok(Self {
            store: loc.store,
            location: loc.location,
            len: AtomicU64::new(len),
        })
    }

    pub(crate) fn from_parts(store: Arc<dyn ObjectStore>, location: StorePath, len: u64) -> Self {
        Self {
            store,
            location,
            len: AtomicU64::new(len),
        }
    }
}

impl Length for RemoteReader {
    fn len(&self) -> u64 {
        self.len.load(Ordering::Relaxed)
    }
}

impl ChunkReader for RemoteReader {
    type T = ChunkedRangeRead;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        Ok(ChunkedRangeRead {
            store: Arc::clone(&self.store),
            location: self.location.clone(),
            pos: start,
            buf: Bytes::new(),
        })
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        if length == 0 {
            return Ok(Bytes::new());
        }
        CTX.rt
            .block_on(
                self.store
                    .get_range(&self.location, start..start + length as u64),
            )
            .map_err(|e| ParquetError::External(Box::new(std::io::Error::other(format!(
                "s3 range {}..{}: {e}",
                self.location,
                start + length as u64
            )))))
    }
}

/// 顺序块拉取读（1MB 块；footer 段长度有界，不会整文件拉取）。
pub struct ChunkedRangeRead {
    store: Arc<dyn ObjectStore>,
    location: StorePath,
    pos: u64,
    buf: Bytes,
}

const READ_CHUNK: u64 = 1024 * 1024;

impl Read for ChunkedRangeRead {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        while self.buf.is_empty() {
            let b = CTX
                .rt
                .block_on(self.store.get_range(&self.location, self.pos..self.pos + READ_CHUNK))
                .map_err(|e| std::io::Error::other(format!("s3 range read: {e}")))?;
            let n = b.len() as u64;
            self.buf = b;
            self.pos += n;
            if n < READ_CHUNK {
                break; // 尾块（对象尽头）
            }
        }
        let n = self.buf.len().min(out.len());
        out[..n].copy_from_slice(&self.buf[..n]);
        self.buf.advance(n);
        Ok(n)
    }
}

/// glob 展开：`s3://bucket/prefix/**/*.parquet` → 匹配对象的全 URL 列表。
/// 常量前缀 list（分页由 object_store 处理）+ 简单 glob 匹配。
pub fn expand_s3_glob(pattern: &str) -> Result<Vec<String>> {
    let (bucket, location, params) = split_url(pattern)?;
    let key_pat = location.as_ref();
    let endpoint = param(&params, "endpoint");
    let region = param(&params, "region");
    // 复用 locate 的 store（key 仅用于解析，store 只按 bucket/endpoint/region 配置）
    let prefix_raw = key_pat.split('*').next().unwrap_or_default();
    let probe_key = if prefix_raw.is_empty() { "x" } else { prefix_raw };
    let probe = format!(
        "s3://{}/{}{}",
        bucket,
        probe_key,
        build_query(endpoint, region)
    );
    let loc = locate(&probe)?;
    // 常量前缀（首个通配符前的目录部分）
    let prefix = prefix_raw
        .rsplit_once('/')
        .map(|(dir, _)| dir.to_string())
        .unwrap_or_default();
    let prefix = if prefix.is_empty() {
        None
    } else {
        Some(StorePath::from(prefix.as_str()))
    };
    let stream = loc.store.list(prefix.as_ref());
    let metas: Vec<ObjectMeta> = CTX.rt.block_on(async {
        let mut out = Vec::new();
        let mut s = stream;
        while let Some(m) = s.next().await {
            match m {
                Ok(meta) => out.push(meta),
                Err(e) => {
                    return Err(QsdbError::meta(format!("s3 list {pattern}: {e}")));
                }
            }
        }
        Ok(out)
    })?;
    let mut urls: Vec<String> = metas
        .into_iter()
        .map(|m| m.location)
        .filter(|k| crate::scan::glob_match(key_pat, k.as_ref()))
        .map(|k| format!("s3://{}/{}{}", bucket, k.as_ref(), build_query(endpoint, region)))
        .collect();
    urls.sort();
    if urls.is_empty() {
        return Err(QsdbError::not_found(format!(
            "read_parquet: no object matches {pattern}"
        )));
    }
    Ok(urls)
}

fn build_query(endpoint: Option<&str>, region: Option<&str>) -> String {
    let mut q = String::new();
    if let Some(e) = endpoint {
        q.push_str("endpoint=");
        q.push_str(&e.replace(',', "%2C"));
    }
    if let Some(r) = region {
        if !q.is_empty() {
            q.push('&');
        }
        q.push_str("region=");
        q.push_str(r);
    }
    if q.is_empty() {
        String::new()
    } else {
        format!("?{q}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::local::LocalFileSystem;

    /// 用 LocalFileSystem 注入 store，全链路（range/顺序读/parquet）无网络验证。
    fn write_parquet(dir: &std::path::Path, name: &str, vals: &[i64]) -> std::path::PathBuf {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use parquet::arrow::arrow_writer::ArrowWriter;
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
        let batch = arrow::record_batch::RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vals.to_vec()))],
        )
        .unwrap();
        let p = dir.join(name);
        let f = std::fs::File::create(&p).unwrap();
        let mut w = ArrowWriter::try_new(f, schema, None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        p
    }

    fn reader_over_local(path: &std::path::Path) -> RemoteReader {
        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::default());
        let location = StorePath::from_absolute_path(path).unwrap();
        let len = std::fs::metadata(path).unwrap().len();
        RemoteReader::from_parts(store, location, len)
    }

    #[test]
    fn url_parsing() {
        let (b, k, p) = split_url("s3://bucket/a/b.parquet?endpoint=http://minio:9000&region=r1")
            .unwrap();
        assert_eq!(b, "bucket");
        assert_eq!(k.as_ref(), "a/b.parquet");
        assert_eq!(param(&p, "endpoint"), Some("http://minio:9000"));
        assert_eq!(param(&p, "region"), Some("r1"));
        assert!(split_url("s3://bucket").is_err());
        assert!(split_url("http://x/y").is_err());
    }

    #[test]
    fn chunk_reader_ranges_and_parquet_roundtrip() {
        let dir = std::env::temp_dir()
            .join(format!("qsdb_remote_test_{}", uuid::Uuid::now_v7().simple()));
        std::fs::create_dir_all(&dir).unwrap();
        let p = write_parquet(&dir, "f.parquet", &[1, 2, 3, 4, 5]);
        let r = reader_over_local(&p);
        assert_eq!(Length::len(&r), std::fs::metadata(&p).unwrap().len());
        // 区间读
        let head = ChunkReader::get_bytes(&r, 0, 4).unwrap();
        assert_eq!(&head[..], b"PAR1");
        // 顺序读（footer 路径）：parquet 魔数在尾部
        let total = Length::len(&r) as usize;
        let tail = ChunkReader::get_bytes(&r, (total - 4) as u64, 4).unwrap();
        assert_eq!(&tail[..], b"PAR1");
        // 全 parquet 读链路（footer + 数据页全走 ChunkReader）
        let builder =
            parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(r).unwrap();
        let reader = builder.with_batch_size(8192).build().unwrap();
        let rows: i64 = reader
            .map(|b| {
                let b = b.unwrap();
                let a = b
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .unwrap();
                a.iter().map(|v| v.unwrap()).sum::<i64>()
            })
            .sum();
        assert_eq!(rows, 15);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn stream_read_adapter() {
        let dir = std::env::temp_dir()
            .join(format!("qsdb_remote_stream_{}", uuid::Uuid::now_v7().simple()));
        std::fs::create_dir_all(&dir).unwrap();
        let p = write_parquet(&dir, "s.parquet", &[7]);
        let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::default());
        let location = StorePath::from_absolute_path(&p).unwrap();
        let mut rd = StreamRead {
            stream: {
                let result = CTX.rt.block_on(store.get(&location)).unwrap();
                result.into_stream()
            },
            buf: Bytes::new(),
            done: false,
        };
        let mut all = Vec::new();
        rd.read_to_end(&mut all).unwrap();
        assert_eq!(all.len() as u64, std::fs::metadata(&p).unwrap().len());
        assert_eq!(&all[..4], b"PAR1");
        std::fs::remove_dir_all(&dir).ok();
    }
}
