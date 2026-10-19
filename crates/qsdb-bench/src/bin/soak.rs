//! soak：长稳混合负载（投产前验证项 #4）。
//!
//! 写线程（WriteBuffer 持续追加滚动分区）× 查询线程（count/过滤/分组轮询）
//! × 周期 compaction/gc——报告吞吐、查询时延、RSS、缓存与段数曲线。
//! 检测目标：内存泄漏（RSS 爬升）、句柄/段数失控、吞吐衰减、错误。
//!
//! 运行：cargo run --release -p qsdb-bench --bin soak -- [分钟] [写线程数] [稳态行数上限(百万)]
//!   默认 30 分钟 / 2 写线程 / 不设上限（短跑验证用）。
//!   **8h 长跑必须设上限**（如 400 = 4 亿行后停写，查询/compaction/gc
//!   继续到时长结束——稳态考察内存/句柄/缓存，不写满磁盘）。
//!   数据落 %TEMP%/qsdb_soak，结束清理。

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use arrow::array::{Date32Array, Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use qsdb::{Qsdb, QsdbOptions};

static STOP: AtomicBool = AtomicBool::new(false);
static WRITTEN: AtomicU64 = AtomicU64::new(0);
static QUERIES: AtomicU64 = AtomicU64::new(0);
static Q_NANOS: AtomicU64 = AtomicU64::new(0);
static ERRORS: AtomicU64 = AtomicU64::new(0);

/// 当前进程 RSS / 峰值 RSS（MB）。Windows 走 Win32，Linux 走 /proc/self/status
/// （`VmRSS` / `VmHWM`，kB 取整 ⇒ 亚 MB 粒度丢失，对 soak 的 MB 级曲线无碍）。
fn mem_mb() -> (f64, f64) {
    #[cfg(windows)]
    {
        use windows_sys::Win32::System::ProcessStatus::{
            GetProcessMemoryInfo, PROCESS_MEMORY_COUNTERS,
        };
        use windows_sys::Win32::System::Threading::GetCurrentProcess;
        let mut pmc = PROCESS_MEMORY_COUNTERS::default();
        unsafe {
            let h = GetCurrentProcess();
            if GetProcessMemoryInfo(
                h,
                &mut pmc,
                std::mem::size_of::<PROCESS_MEMORY_COUNTERS>() as u32,
            ) != 0
            {
                (
                    pmc.WorkingSetSize as f64 / 1048576.0,
                    pmc.PeakWorkingSetSize as f64 / 1048576.0,
                )
            } else {
                (0.0, 0.0)
            }
        }
    }
    #[cfg(not(windows))]
    {
        let (mut cur, mut peak) = (0.0f64, 0.0f64);
        if let Ok(s) = std::fs::read_to_string("/proc/self/status") {
            for line in s.lines() {
                let parse = |v: &str| -> f64 {
                    v.split_whitespace().next().and_then(|x| x.parse().ok()).unwrap_or(0.0)
                };
                if let Some(v) = line.strip_prefix("VmRSS:") {
                    cur = parse(v) / 1024.0;
                } else if let Some(v) = line.strip_prefix("VmHWM:") {
                    peak = parse(v) / 1024.0;
                }
            }
        }
        (cur, peak)
    }
}

fn synth(rows: usize, dt: i32, seed: u64) -> RecordBatch {
    let mut x = seed | 0x9e37_79b9_7f4a_7c15;
    let mut g1 = Vec::with_capacity(rows);
    let mut a = Vec::with_capacity(rows);
    let mut score = Vec::with_capacity(rows);
    for _ in 0..rows {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        g1.push((x % 64) as i64);
        a.push((x % 1_000_000) as i64);
        score.push(x as f64 / 65536.0);
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("dt", DataType::Date32, true),
        Field::new("g1", DataType::Int64, true),
        Field::new("a", DataType::Int64, true),
        Field::new("score", DataType::Float64, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Date32Array::from(vec![dt; rows])),
            Arc::new(Int64Array::from(g1)),
            Arc::new(Int64Array::from(a)),
            Arc::new(Float64Array::from(score)),
        ],
    )
    .unwrap()
}

fn main() {
    let mins: u64 = std::env::args()
        .nth(1)
        .and_then(|v| v.parse().ok())
        .unwrap_or(30);
    let writers: usize = std::env::args()
        .nth(2)
        .and_then(|v| v.parse().ok())
        .unwrap_or(2);
    let cap_millions: u64 = std::env::args()
        .nth(3)
        .and_then(|v| v.parse().ok())
        .unwrap_or(0); // 0 = 不设上限（短跑）
    let cap_rows: u64 = cap_millions * 1_000_000;
    let root = std::env::temp_dir().join(format!(
        "qsdb_soak_{:x}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).unwrap();
    println!(
        "== soak: {mins}min, {writers} writers, root {} ==",
        root.display()
    );

    let db = Arc::new(Qsdb::open(&root, QsdbOptions::default()).unwrap());
    db.sql("CREATE TABLE t (dt DATE, g1 BIGINT, a BIGINT, score DOUBLE) PARTITIONED BY (dt)")
        .unwrap();
    let writer = std::sync::Arc::new(db.writer(qsdb::write_buffer::WriteBufferOptions {
        flush_batches: 32,
        flush_bytes: 64 * 1024 * 1024,
    }));

    // 写线程：滚动分区（每天 ~200 批 → 200 段/分区，触发 compaction 阈值路径）
    // WriteBuffer 非 Clone：单写线程聚合（多写者场景用 HTTP 多连接压）
    let mut handles = Vec::new();
    {
        let writer = Arc::clone(&writer);
        let db = Arc::clone(&db);
        let writers = writers.max(1);
        handles.push(std::thread::spawn(move || {
            let mut day = 19_000i32;
            let mut n = 0usize;
            while !STOP.load(Ordering::Relaxed) {
                // 稳态上限：写满即停（查询/compaction/gc 继续跑到结束）
                if cap_rows > 0 && WRITTEN.load(Ordering::Relaxed) >= cap_rows {
                    eprintln!(
                        "[soak] steady-state: {cap_millions}M rows reached, writer exits (queries continue)"
                    );
                    break;
                }
                // 每轮 writers 个批（等效并发写量）
                let batch = synth(5_000, day, 0x1000 + n as u64 * 7919);
                let mut bs = Vec::with_capacity(writers);
                for w in 0..writers {
                    let _ = w;
                    bs.push(batch.clone());
                }
                if let Err(e) = writer.push("t", &bs) {
                    ERRORS.fetch_add(1, Ordering::Relaxed);
                    eprintln!("write error: {e}");
                }
                WRITTEN.fetch_add(5_000 * writers as u64, Ordering::Relaxed);
                n += 1;
                if n % 200 == 0 {
                    day += 1; // 滚动到新分区
                }
                if n % 40 == 0 {
                    std::thread::sleep(Duration::from_millis(5)); // 留 CPU 给读
                }
            }
            let _ = db;
        }));
    }

    // 查询线程：三种负载轮询
    for q in 0..2 {
        let db = Arc::clone(&db);
        handles.push(std::thread::spawn(move || {
            let sqls = [
                "SELECT count(*) FROM t",
                "SELECT count(*) FROM t WHERE a > 500000",
                "SELECT g1, count(*), sum(a) FROM t GROUP BY g1",
            ];
            let mut i = q;
            while !STOP.load(Ordering::Relaxed) {
                let sql = sqls[i % sqls.len()];
                i += 1;
                let t = Instant::now();
                match db.sql_collect(sql) {
                    Ok(_) => {
                        QUERIES.fetch_add(1, Ordering::Relaxed);
                        Q_NANOS.fetch_add(t.elapsed().as_nanos() as u64, Ordering::Relaxed);
                    }
                    Err(e) => {
                        ERRORS.fetch_add(1, Ordering::Relaxed);
                        // 读写并发的瞬时段可见性错误不应发生——如实打印
                        eprintln!("query error ({sql}): {e}");
                    }
                }
            }
        }));
    }

    // 监控 + 周期 compaction/gc
    let total = Duration::from_secs(mins * 60);
    let start = Instant::now();
    let report_every = Duration::from_secs(30);
    let mut next_report = start + report_every;
    let mut last_written = 0u64;
    let mut last_t = start;
    println!("time  rows/s  queries  q_avg_ms  rss_mb  peak_mb  cache_MB  segs/part(avg)");
    while start.elapsed() < total {
        std::thread::sleep(Duration::from_millis(500));
        if Instant::now() >= next_report {
            next_report += report_every;
            let now_w = WRITTEN.load(Ordering::Relaxed);
            let dt = last_t.elapsed().as_secs_f64().max(0.001);
            let (cur, peak) = mem_mb();
            let q = QUERIES.load(Ordering::Relaxed);
            let q_avg = Q_NANOS.load(Ordering::Relaxed) as f64 / q.max(1) as f64 / 1e6;
            let (centries, cbytes, chits, cmiss) = db.cache_stats();
            let stats = db.partition_stats("t").unwrap_or_default();
            let segs: f64 = if stats.is_empty() {
                0.0
            } else {
                stats.iter().map(|s| s.segments).sum::<usize>() as f64 / stats.len() as f64
            };
            println!(
                "{:5.0}m {:7.0} {:8} {:8.1} {:7.0} {:8.0} {:8.0}  {:5.1}  (cache n={centries} hit={chits} miss={cmiss})",
                start.elapsed().as_secs_f64() / 60.0,
                (now_w - last_written) as f64 / dt,
                q,
                q_avg,
                cur,
                peak,
                cbytes as f64 / 1048576.0,
                segs,
            );
            last_written = now_w;
            last_t = Instant::now();
            // 周期运维动作（与读写并发——soak 的核心考察面）
            if let Err(e) = db.compact_table("t") {
                ERRORS.fetch_add(1, Ordering::Relaxed);
                eprintln!("compact error: {e}");
            }
            let _ = db.gc();
        }
    }
    STOP.store(true, Ordering::Relaxed);
    for h in handles {
        let _ = h.join();
    }
    // 上限模式：flush 攒批余量（计数以 WRITTEN 为准，flush 只落盘已计批）
    let _ = writer.flush_all();
    let total_rows: i64 = db
        .sql_collect("SELECT count(*) FROM t")
        .unwrap()
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
    let (cur, peak) = mem_mb();
    println!(
        "== done: 写入 {} 行 / 校验总数 {} / 查询 {} 次 / 错误 {} / RSS 峰值 {:.0}MB ==",
        WRITTEN.load(Ordering::Relaxed),
        total_rows,
        QUERIES.load(Ordering::Relaxed),
        ERRORS.load(Ordering::Relaxed),
        peak
    );
    assert_eq!(
        total_rows as u64,
        WRITTEN.load(Ordering::Relaxed),
        "读写并发下数据不得丢失"
    );
    let _ = cur;
    std::fs::remove_dir_all(&root).ok();
}
