//! WriteBuffer：写入攒批（设计文档 §13）。
//!
//! 动机：万级分区 × 高频小批提交时，每批都走「分区锁 + manifest + 索引
//! 全量重写」，O(n²) 写放大。攒批把 N 次小提交合成每分区一次大提交。
//!
//! 用法：`db.writer()` 获得缓冲句柄；`push()` 累积，达到阈值自动 flush
//! 对应表；显式 `flush(table)` / `flush_all()` 落盘。句柄 Clone（Arc 共享），
//! 跨线程安全。

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use arrow::record_batch::RecordBatch;

use crate::api::Qsdb;
use crate::catalog::table_dir;
use crate::error::{QsdbError, Result};
use crate::partition::split_by_partition;
use crate::types::Literal;
use crate::write::{self, WriteResult};

/// 攒批阈值。
#[derive(Debug, Clone)]
pub struct WriteBufferOptions {
    /// 单表累积批数达到该值自动 flush
    pub flush_batches: usize,
    /// 单表累积逻辑字节（估算）达到该值自动 flush
    pub flush_bytes: usize,
}

impl Default for WriteBufferOptions {
    fn default() -> Self {
        Self {
            flush_batches: 64,
            flush_bytes: 128 * 1024 * 1024,
        }
    }
}

/// 单表缓冲。
#[derive(Default)]
struct TableBuffer {
    /// (分区值, 批) 累积
    parts: Vec<(Vec<Literal>, RecordBatch)>,
    batches: usize,
    bytes: usize,
}

/// 写缓冲句柄（Clone 共享同一缓冲）。
pub struct WriteBuffer {
    db: Arc<Qsdb>,
    opts: WriteBufferOptions,
    inner: Mutex<HashMap<String, TableBuffer>>,
}

impl Qsdb {
    /// 创建写缓冲（绑定本句柄）。
    pub fn writer(self: &Arc<Self>, opts: WriteBufferOptions) -> WriteBuffer {
        WriteBuffer {
            db: Arc::clone(self),
            opts,
            inner: Mutex::new(HashMap::new()),
        }
    }
}

impl WriteBuffer {
    /// 追加写入（自动按分区分组累积；触发阈值时自动 flush 该表）。
    pub fn push(&self, table: &str, batches: &[RecordBatch]) -> Result<()> {
        if self.db.read_only() {
            return Err(QsdbError::read_only(
                "database is opened read-only; write rejected",
            ));
        }
        let def = self.db.table_def(table)?;
        let mut groups: Vec<(Vec<Literal>, RecordBatch)> = Vec::new();
        for b in batches {
            groups.extend(split_by_partition(b, &def)?);
        }
        if groups.is_empty() {
            return Ok(());
        }
        let bytes: usize = groups
            .iter()
            .map(|(_, b)| crate::memory::batch_bytes(b) as usize)
            .sum();
        let batches_n = groups.len();

        let should_flush = {
            let mut guard = self.lock(table)?;
            let buf = guard.entry(table.to_string()).or_default();
            buf.parts.extend(groups);
            buf.batches += batches_n;
            buf.bytes += bytes;
            buf.batches >= self.opts.flush_batches || buf.bytes >= self.opts.flush_bytes
        };
        if should_flush {
            self.flush(table)?;
        }
        Ok(())
    }

    /// flush 单表（无缓冲则 no-op）。
    pub fn flush(&self, table: &str) -> Result<WriteResult> {
        let def = self.db.table_def(table)?;
        let taken = {
            let mut guard = self.lock(table)?;
            match guard.remove(table) {
                Some(buf) => buf.parts,
                None => return Ok(WriteResult::default()),
            }
        };
        if taken.is_empty() {
            return Ok(WriteResult::default());
        }
        // 合并同分区批 → 每分区一次提交
        let mut merged: HashMap<Vec<String>, (Vec<Literal>, Vec<RecordBatch>)> = HashMap::new();
        for (vals, batch) in taken {
            let key: Vec<String> = vals
                .iter()
                .map(crate::partition::partition_value_string)
                .collect();
            match merged.get_mut(&key) {
                Some((v, list)) => {
                    list.push(batch);
                    let _ = v;
                }
                None => {
                    merged.insert(key, (vals, vec![batch]));
                }
            }
        }
        // 分区级并行提交（不同分区互不阻塞；分区锁保证正确性）
        let tdir = table_dir(self.db.root(), crate::catalog::DEFAULT_DB, table);
        let store = Arc::clone(self.db.store());
        let staging = self.db.staging(); // Option<&Staging>：Copy，直接进闭包
        use rayon::prelude::*;
        let results: Vec<Result<WriteResult>> = merged
            .into_par_iter()
            .map(|(_key, (vals, list))| {
                write::append_partition(&store, staging, &tdir, &def, &vals, &list)
            })
            .collect();
        let mut total = WriteResult::default();
        for r in results {
            total = merge_write(total, r?);
        }
        Ok(total)
    }

    /// flush 全部缓冲表。
    pub fn flush_all(&self) -> Result<WriteResult> {
        let tables: Vec<String> = {
            let guard = self
                .inner
                .lock()
                .map_err(|_| QsdbError::meta("write buffer poisoned"))?;
            guard.keys().cloned().collect()
        };
        let mut total = WriteResult::default();
        for t in tables {
            total = merge_write(total, self.flush(&t)?);
        }
        Ok(total)
    }

    /// 各表当前缓冲概况（表名 → (批数, 字节)）。
    pub fn buffered(&self) -> Vec<(String, usize, usize)> {
        let guard = match self.inner.lock() {
            Ok(g) => g,
            Err(_) => return Vec::new(),
        };
        let mut out: Vec<(String, usize, usize)> = guard
            .iter()
            .map(|(t, b)| (t.clone(), b.batches, b.bytes))
            .collect();
        out.sort();
        out
    }

    fn lock(
        &self,
        _table: &str,
    ) -> Result<std::sync::MutexGuard<'_, HashMap<String, TableBuffer>>> {
        self.inner
            .lock()
            .map_err(|_| QsdbError::meta("write buffer poisoned"))
    }
}

fn merge_write(a: WriteResult, b: WriteResult) -> WriteResult {
    WriteResult {
        rows: a.rows + b.rows,
        bytes: a.bytes + b.bytes,
        segments: a.segments + b.segments,
    }
}

// PathBuf 供 table_dir 签名可见
#[allow(unused)]
fn _pb(_: PathBuf) {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::{QsdbOptions, ScanRequest};
    use arrow::array::{Date32Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};

    fn tmp_root(tag: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir()
            .join("qsdb_wb_test")
            .join(format!("{tag}_{}", uuid::Uuid::now_v7().simple()));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn batch(dt: i32, v: i64) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("dt", DataType::Date32, true),
            Field::new("v", DataType::Int64, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Date32Array::from(vec![dt])),
                Arc::new(Int64Array::from(vec![v])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn buffers_until_flush() {
        let root = tmp_root("buf");
        let db = Arc::new(Qsdb::open(&root, QsdbOptions::default()).unwrap());
        db.sql("CREATE TABLE t (dt DATE, v BIGINT) PARTITIONED BY (dt)")
            .unwrap();
        let wb = db.writer(WriteBufferOptions {
            flush_batches: 100, // 不自动触发
            flush_bytes: usize::MAX,
        });
        for i in 0..8 {
            wb.push("t", &[batch(19_000 + i % 3, i64::from(i))])
                .unwrap();
        }
        // 未 flush：磁盘无数据
        let n = db
            .sql_collect("SELECT v FROM t")
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum::<usize>();
        assert_eq!(n, 0, "buffered rows must not be visible before flush");
        assert_eq!(wb.buffered().len(), 1);

        let r = wb.flush("t").unwrap();
        assert_eq!(r.rows, 8);
        let n = db
            .sql_collect("SELECT count(*) FROM t")
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum::<usize>();
        assert!(n > 0);
        // 3 个分区 → 3 个 segment
        let batches = db.sql_collect("SELECT count(*) FROM t").unwrap();
        let cnt: i64 = batches
            .iter()
            .flat_map(|b| {
                (0..b.num_rows()).map(|i| {
                    arrow::util::display::array_value_to_string(b.column(0).as_ref(), i)
                        .unwrap()
                        .parse::<i64>()
                        .unwrap()
                })
            })
            .sum();
        assert_eq!(cnt, 8);
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn auto_flush_on_threshold() {
        let root = tmp_root("auto");
        let db = Arc::new(Qsdb::open(&root, QsdbOptions::default()).unwrap());
        db.sql("CREATE TABLE t (dt DATE, v BIGINT) PARTITIONED BY (dt)")
            .unwrap();
        let wb = db.writer(WriteBufferOptions {
            flush_batches: 3,
            flush_bytes: usize::MAX,
        });
        for i in 0..3 {
            wb.push("t", &[batch(19_000, i64::from(i))]).unwrap();
        }
        // 第 3 批触发自动 flush
        let n: usize = db
            .scan_collect(ScanRequest {
                table: "t".into(),
                ..Default::default()
            })
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(n, 3);
        // 只有一个 segment（攒批合并）
        let pdir = root
            .join(crate::catalog::DEFAULT_DB)
            .join("t")
            .join("dt=2022-01-08");
        let store = crate::store::Store::local(&root);
        let m = crate::manifest::PartitionManifest::read(&store, &pdir)
            .unwrap()
            .unwrap();
        assert_eq!(m.segments.len(), 1);
        std::fs::remove_dir_all(&root).ok();
    }
}
