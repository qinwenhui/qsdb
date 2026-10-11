//! 热数据缓存：segment 级解码后 RecordBatch 的 LRU 缓存。
//!
//! 动机（M4 DuckDB 对比结论）：热查询每次重新解 zstd 是与 DuckDB 内存
//! buffer manager 的主要差距。缓存解码后 arrow 批，命中路径零解码零拷贝
//! （RecordBatch 内部全是 Arc）。
//!
//! 语义：
//! - key =（文件路径, mtime, size, 投影签名, row-group 签名）——append 产生
//!   新文件不影响旧缓存；compaction/overwrite 改文件 → mtime 变 → 自动失效
//! - 字节预算 LRU 驱逐；单 segment 超预算不缓存（防抖动）
//! - `budget = 0` 全部 API 退化为 no-op（关闭）
//! - miss 回填在流式读取完成后进行，无额外内存峰值（Arc clone 收集）

use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use arrow::record_batch::RecordBatch;

/// 缓存键签名（不含路径，路径做 HashMap key 的一部分）。
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct CacheSig {
    path: PathBuf,
    mtime_ms: u64,
    size: u64,
    /// 投影列名（已规范化为文件内列序）
    projection: Vec<String>,
    /// row group 子集签名（None = 全部）
    row_groups: Option<Vec<usize>>,
}

struct Entry {
    batches: Arc<Vec<RecordBatch>>,
    bytes: usize,
    tick: u64,
}

struct Inner {
    map: HashMap<CacheSig, Entry>,
    /// (tick, sig) 近似 LRU
    lru: BTreeSet<(u64, CacheSig)>,
    used: usize,
    hits: u64,
    misses: u64,
}

/// segment 缓存（Clone 即共享）。
#[derive(Clone)]
pub struct SegmentCache {
    budget: usize,
    inner: Option<Arc<Mutex<Inner>>>,
    tick_ctr: Arc<AtomicU64>,
}

impl SegmentCache {
    /// `budget_bytes = 0` 创建禁用实例。
    pub fn new(budget_bytes: usize) -> Self {
        if budget_bytes == 0 {
            return Self {
                budget: 0,
                inner: None,
                tick_ctr: Arc::new(AtomicU64::new(0)),
            };
        }
        Self {
            budget: budget_bytes,
            inner: Some(Arc::new(Mutex::new(Inner {
                map: HashMap::new(),
                lru: BTreeSet::new(),
                used: 0,
                hits: 0,
                misses: 0,
            }))),
            tick_ctr: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.inner.is_some()
    }

    pub fn budget(&self) -> usize {
        self.budget
    }

    fn next_tick(&self) -> u64 {
        self.tick_ctr.fetch_add(1, Ordering::Relaxed)
    }

    /// 命中查询；命中即刷新 LRU。
    pub fn get(
        &self,
        path: &std::path::Path,
        mtime_ms: u64,
        size: u64,
        projection: &[String],
        row_groups: &Option<Vec<usize>>,
    ) -> Option<Arc<Vec<RecordBatch>>> {
        let inner_arc = self.inner.as_ref()?;
        let mut inner = inner_arc.lock().ok()?;
        let sig = CacheSig {
            path: path.to_path_buf(),
            mtime_ms,
            size,
            projection: projection.to_vec(),
            row_groups: row_groups.clone(),
        };
        let tick = self.next_tick();
        let hit = inner.map.get(&sig).map(|e| {
            let old_tick = e.tick;
            (Arc::clone(&e.batches), old_tick)
        });
        match hit {
            Some((batches, old_tick)) => {
                // 刷新 LRU（借用结束后）
                inner.lru.remove(&(old_tick, sig.clone()));
                if let Some(e) = inner.map.get_mut(&sig) {
                    e.tick = tick;
                }
                inner.lru.insert((tick, sig));
                inner.hits += 1;
                Some(batches)
            }
            None => {
                inner.misses += 1;
                None
            }
        }
    }

    /// 回填（超过预算自动驱逐；单条放不下则跳过）。
    pub fn put(
        &self,
        path: &std::path::Path,
        mtime_ms: u64,
        size: u64,
        projection: &[String],
        row_groups: &Option<Vec<usize>>,
        batches: Vec<RecordBatch>,
    ) {
        let Some(inner_arc) = &self.inner else { return };
        let bytes: usize = batches.iter().map(|b| b.get_array_memory_size()).sum();
        if bytes > self.budget {
            return; // 单 segment 超预算：不缓存（避免逐抖）
        }
        let Ok(mut inner) = inner_arc.lock() else {
            return;
        };
        // 驱逐至容纳
        while inner.used + bytes > self.budget {
            let Some((_, old_sig)) = inner.lru.iter().next().cloned() else {
                break;
            };
            if let Some(e) = inner.map.remove(&old_sig) {
                inner.used -= e.bytes;
            }
            inner.lru.retain(|(_, s)| s != &old_sig);
        }
        let sig = CacheSig {
            path: path.to_path_buf(),
            mtime_ms,
            size,
            projection: projection.to_vec(),
            row_groups: row_groups.clone(),
        };
        let tick = self.next_tick();
        // 同键覆盖：先移除旧值
        if let Some(old) = inner.map.remove(&sig) {
            inner.used -= old.bytes;
            inner.lru.retain(|(_, s)| s != &sig);
        }
        inner.used += bytes;
        inner.lru.insert((tick, sig.clone()));
        inner.map.insert(
            sig,
            Entry {
                batches: Arc::new(batches),
                bytes,
                tick,
            },
        );
    }

    /// 清空。
    pub fn clear(&self) {
        if let Some(inner_arc) = &self.inner {
            if let Ok(mut inner) = inner_arc.lock() {
                inner.map.clear();
                inner.lru.clear();
                inner.used = 0;
            }
        }
    }

    /// (entries, bytes, hits, misses)。
    pub fn stats(&self) -> (usize, usize, u64, u64) {
        let Some(inner_arc) = &self.inner else {
            return (0, 0, 0, 0);
        };
        let Ok(inner) = inner_arc.lock() else {
            return (0, 0, 0, 0);
        };
        (inner.map.len(), inner.used, inner.hits, inner.misses)
    }

    /// 惰性收集器：miss 路径逐批 yield 的同时收集（Arc clone 零拷贝），
    /// Drop 或 finish 时回填。累积上界 = 预算/8（并行分片各持一个收集器，
    /// 超界弃缓存——防「整文件累积后一次性 put」在多分片下爆内存）。
    pub fn collector(
        self: &Arc<Self>,
        path: PathBuf,
        mtime_ms: u64,
        size: u64,
        projection: Vec<String>,
        row_groups: Option<Vec<usize>>,
    ) -> CacheCollector {
        let cap = (self.budget / 8).max(16 * 1024 * 1024);
        CacheCollector {
            cache: Arc::clone(self),
            sig: (path, mtime_ms, size, projection, row_groups),
            batches: Vec::new(),
            acc_bytes: 0,
            cap_bytes: cap,
            aborted: false,
        }
    }
}

/// 收集器：读取完成时显式 `finish()` 回填（或 Drop 自动回填）。
pub struct CacheCollector {
    cache: Arc<SegmentCache>,
    sig: (PathBuf, u64, u64, Vec<String>, Option<Vec<usize>>),
    batches: Vec<RecordBatch>,
    acc_bytes: usize,
    cap_bytes: usize,
    aborted: bool,
}

impl CacheCollector {
    pub fn push(&mut self, b: RecordBatch) {
        if self.aborted {
            return;
        }
        self.acc_bytes += b.get_array_memory_size();
        if self.acc_bytes > self.cap_bytes {
            // 超上界：放弃本条目缓存（大文件不进缓存，扫描内存有界）
            self.batches.clear();
            self.aborted = true;
            return;
        }
        self.batches.push(b);
    }

    pub fn finish(self) {
        if self.aborted {
            return;
        }
        let (path, mt, sz, proj, rgs) = self.sig;
        self.cache.put(&path, mt, sz, &proj, &rgs, self.batches);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::path::Path;
    use std::sync::Arc as StdArc;

    fn batch(n: i64) -> RecordBatch {
        let schema = StdArc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
        RecordBatch::try_new(schema, vec![StdArc::new(Int64Array::from(vec![Some(n)]))]).unwrap()
    }

    #[test]
    fn disabled_is_noop() {
        let c = SegmentCache::new(0);
        assert!(!c.is_enabled());
        c.put(Path::new("x"), 1, 1, &[], &None, vec![batch(1)]);
        assert!(c.get(Path::new("x"), 1, 1, &[], &None).is_none());
        assert_eq!(c.stats(), (0, 0, 0, 0));
    }

    #[test]
    fn hit_miss_and_invalidation() {
        let c = SegmentCache::new(1 << 20);
        let p = Path::new("seg_a.parquet");
        assert!(c.get(p, 10, 100, &["v".into()], &None).is_none());
        c.put(p, 10, 100, &["v".into()], &None, vec![batch(7)]);
        assert!(c.get(p, 10, 100, &["v".into()], &None).is_some());
        // mtime 变（compaction）→ 失效
        assert!(c.get(p, 11, 100, &["v".into()], &None).is_none());
        // 投影不同 → miss
        assert!(c.get(p, 10, 100, &[], &None).is_none());
        let (_, _, hits, misses) = c.stats();
        assert_eq!((hits, misses), (1, 3));
    }

    #[test]
    fn lru_eviction_and_oversize() {
        let unit = batch(1).get_array_memory_size();
        let c = SegmentCache::new(unit * 2 + 1); // 容纳 2 个小批
        for (i, mt) in [(1u64, 1u64), (2, 2), (3, 3)] {
            let p = PathBuf::from(format!("s{i}.parquet"));
            c.get(&p, mt, 1, &[], &None);
            c.put(&p, mt, 1, &[], &None, vec![batch(i as i64)]);
        }
        let (entries, _, _, _) = c.stats();
        assert!(entries <= 2, "budget 只容 2 条：{entries}");
        // 最新命中（s3 保留）
        assert!(c.get(Path::new("s3.parquet"), 3, 1, &[], &None).is_some());
        // 单条超预算不缓存
        let big = SegmentCache::new(8);
        big.put(Path::new("big"), 1, 1, &[], &None, vec![batch(1)]);
        assert_eq!(big.stats().0, 0);
    }
}
