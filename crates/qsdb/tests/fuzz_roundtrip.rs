//! M1 gate：随机写读回环 fuzz + 崩溃一致性 + 多实例并发。
//!
//! 随机源为固定种子的 LCG（可复现，无 rand 依赖）。

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Barrier;

use arrow::array::{ArrayRef, Date32Array, Decimal128Array, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use qsdb::catalog::ColumnDef;
use qsdb::types::{DecimalMeta, Literal, LogicalType};
use qsdb::{Predicate, Qsdb, QsdbOptions, ScanRequest, TableDef};

// -- 可复现随机源 -----------------------------------------------------------

struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 33
    }
    fn range(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// 测试目录唯一序号：pid 挡跨进程碰撞，序号挡同进程内并发（Windows 时钟
/// 粒度 ~100ns，纳秒后缀在同 tick 内会重复——CI 上真炸过，见 constraints）。
fn _uniq_seq() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    SEQ.fetch_add(1, Ordering::Relaxed)
}

fn col(name: &str, ty: LogicalType) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        logical_type: ty,
        nullable: true,
        check: None,
        default: None,
        comment: None,
    }
}

fn events_def() -> TableDef {
    TableDef::new(
        "events",
        vec![
            col("dt", LogicalType::Date),
            col("tenant", LogicalType::Varchar { max_chars: None }),
            col("amount", LogicalType::Decimal(DecimalMeta::new(18, 4))),
            col("cnt", LogicalType::Int64),
            col("score", LogicalType::Float64),
        ],
        vec!["dt".into()],
    )
    .unwrap()
}

/// 生成一批随机数据（行内跨 dt 分区、含 NULL）。
fn rand_batch(rng: &mut Lcg, rows: usize) -> RecordBatch {
    let mut dts: Vec<i32> = Vec::with_capacity(rows);
    let mut tenants: Vec<Option<String>> = Vec::with_capacity(rows);
    let mut amounts: Vec<Option<i128>> = Vec::with_capacity(rows);
    let mut cnts: Vec<Option<i64>> = Vec::with_capacity(rows);
    let mut scores: Vec<Option<f64>> = Vec::with_capacity(rows);
    for _ in 0..rows {
        dts.push(19_000 + (rng.range(16) as i32)); // 16 个分区
        tenants.push((rng.range(10) != 0).then(|| format!("t{}", rng.range(50))));
        amounts.push((rng.range(8) != 0).then_some(rng.range(1_000_000) as i128));
        cnts.push((rng.range(12) != 0).then_some(rng.next() as i64));
        scores.push((rng.range(6) != 0).then_some(rng.next() as f64 / 1e15));
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("dt", DataType::Date32, true),
        Field::new("tenant", DataType::Utf8, true),
        Field::new("amount", DataType::Decimal128(18, 4), true),
        Field::new("cnt", DataType::Int64, true),
        Field::new("score", DataType::Float64, true),
    ]));
    let amount_arr: ArrayRef =
        Arc::new(Decimal128Array::from(amounts).with_data_type(DataType::Decimal128(18, 4)));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Date32Array::from(dts)),
            Arc::new(StringArray::from(tenants)),
            amount_arr,
            Arc::new(Int64Array::from(cnts)),
            Arc::new(Float64Array::from(scores)),
        ],
    )
    .unwrap()
}

fn tmp_root(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir()
        .join("qsdb_fuzz_test")
        .join(format!("{tag}_{}", uuid_v7_hex()));
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn uuid_v7_hex() -> String {
    // 避免引入 uuid 到测试 dev-deps 之外的路径——退而求其次用
    // 「pid + 线程内序号」拼唯一十六进制 id（纳秒后缀在 Windows 同 tick
    // 内会重复，CI 上真炸过）。
    format!("{:x}_{}", std::process::id(), _uniq_seq())
}

fn count_rows(db: &Qsdb, table: &str) -> usize {
    db.scan_collect(ScanRequest {
        table: table.into(),
        ..Default::default()
    })
    .unwrap()
    .iter()
    .map(|b| b.num_rows())
    .sum()
}

// -- fuzz：并发 append + overwrite + 校验 ------------------------------------

#[test]
fn fuzz_concurrent_append_overwrite_roundtrip() {
    let root = tmp_root("fuzz");
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    db.create_table(events_def()).unwrap();

    const WRITERS: usize = 4;
    const APPENDS_PER_WRITER: usize = 8;
    const ROWS_PER_APPEND: usize = 500;

    let barrier = Arc::new(Barrier::new(WRITERS));
    let db = Arc::new(db);
    let mut handles = Vec::new();
    for w in 0..WRITERS {
        let db = Arc::clone(&db);
        let barrier = Arc::clone(&barrier);
        handles.push(std::thread::spawn(move || {
            let mut rng = Lcg(0x5eed_0000 + w as u64);
            barrier.wait();
            for _ in 0..APPENDS_PER_WRITER {
                db.append("events", &[rand_batch(&mut rng, ROWS_PER_APPEND)])
                    .unwrap();
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    let expect_total = WRITERS * APPENDS_PER_WRITER * ROWS_PER_APPEND;
    assert_eq!(count_rows(&db, "events"), expect_total, "并发 append 行数");

    // overwrite 三个确定的不同分区：每个写入已知行数 222
    for &dt in &[19_003i32, 19_009, 19_015] {
        let full = const_dt_batch(dt, 222);
        db.overwrite_partition("events", &[("dt", Literal::Date(dt))], &[full])
            .unwrap();
    }

    // 覆盖后的行数重新核算：非覆盖分区不变 + 3×222
    let mut per_part: HashMap<i32, usize> = HashMap::new();
    for b in db
        .scan_collect(ScanRequest {
            table: "events".into(),
            ..Default::default()
        })
        .unwrap()
    {
        let dt = b.column(0).as_any().downcast_ref::<Date32Array>().unwrap();
        for i in 0..b.num_rows() {
            *per_part.entry(dt.value(i)).or_default() += 1;
        }
    }
    let total: usize = per_part.values().sum();
    // 被 overwrite 的分区：16 个里覆盖了至多 3 个（可能重复覆盖同分区）
    assert!(total <= expect_total, "overwrite 只会减少行数");
    assert_eq!(per_part.values().filter(|&&n| n == 222).count(), 3);

    // 谓词扫描正确性：单分区行数与 map 一致
    for (dt, n) in &per_part {
        let got = db
            .scan_collect(ScanRequest {
                table: "events".into(),
                predicate: Some(Predicate::eq("dt", Literal::Date(*dt))),
                ..Default::default()
            })
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum::<usize>();
        assert_eq!(got, *n, "分区 {dt} 谓词扫描行数");
    }
    std::fs::remove_dir_all(&root).ok();
}

fn const_dt_batch(dt: i32, rows: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("dt", DataType::Date32, true),
        Field::new("tenant", DataType::Utf8, true),
        Field::new("amount", DataType::Decimal128(18, 4), true),
        Field::new("cnt", DataType::Int64, true),
        Field::new("score", DataType::Float64, true),
    ]));
    let amount = Decimal128Array::from(vec![Some(100i128); rows])
        .with_data_type(DataType::Decimal128(18, 4));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Date32Array::from(vec![dt; rows])),
            Arc::new(StringArray::from(vec![Some("ow"); rows])),
            Arc::new(amount),
            Arc::new(Int64Array::from(vec![Some(7); rows])),
            Arc::new(Float64Array::from(vec![Some(0.5); rows])),
        ],
    )
    .unwrap()
}

// -- 崩溃一致性：提交点语义 ---------------------------------------------------

#[test]
fn crash_orphan_segment_invisible() {
    let root = tmp_root("orphan");
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    db.create_table(events_def()).unwrap();
    let full = const_dt_batch(19_001, 10);
    db.append("events", &[full]).unwrap();
    assert_eq!(count_rows(&db, "events"), 10);

    // 模拟「数据文件已写、manifest 未提交」：往分区目录塞一个不受引用的 seg 文件
    let pdir = root.join("default").join("events").join("dt=2022-01-09");
    let manifest = qsdb::manifest::PartitionManifest::read(db.store(), &pdir)
        .unwrap()
        .unwrap();
    let live: Vec<String> = manifest
        .segments
        .iter()
        .map(|s| s.file_name.clone())
        .collect();
    let orphan = pdir.join("seg_deadbeef00000000deadbeef00000000.parquet");
    std::fs::copy(pdir.join(&live[0]), &orphan).unwrap();
    // 孤儿不可见
    assert_eq!(count_rows(&db, "events"), 10);
    // manifest 恢复原状（copy 只为制造孤儿）
    let _ = std::fs::remove_file(&orphan);
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn crash_tmp_manifest_leftover_is_harmless() {
    let root = tmp_root("tmpman");
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    db.create_table(events_def()).unwrap();
    db.append("events", &[const_dt_batch(19_002, 5)]).unwrap();

    // 模拟崩溃残留：假 tmp 文件躺在分区目录
    let pdir = root.join("default").join("events").join("dt=2022-01-10");
    std::fs::write(pdir.join("manifest.postcard.ABCD.tmp"), b"garbage").unwrap();
    // 读取不受影响
    assert_eq!(count_rows(&db, "events"), 5);
    // gc 清理
    let opts = QsdbOptions {
        trash_grace_ms: 0,
        scan_cache_bytes: 0,
        ..Default::default()
    };
    let db2 = Qsdb::open(&root, opts).unwrap();
    db2.gc().unwrap();
    assert_eq!(count_rows(&db, "events"), 5);
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn crash_between_overwrite_steps_keeps_old_data_visible() {
    // overwrite 的提交点是 manifest 原子替换；替换前任何时刻旧数据必须完整可读
    let root = tmp_root("owcrash");
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    db.create_table(events_def()).unwrap();
    db.append("events", &[const_dt_batch(19_003, 100)]).unwrap();

    // 手工执行 overwrite 的前半段：写新数据文件，但不动 manifest
    let pdir = root.join("default").join("events").join("dt=2022-01-11");
    let manifest_before = qsdb::manifest::PartitionManifest::read(db.store(), &pdir)
        .unwrap()
        .unwrap();
    let ghost = pdir.join("seg_ghost00000000000000000000000000.parquet");
    {
        let first = pdir.join(&manifest_before.segments[0].file_name);
        std::fs::copy(first, &ghost).unwrap();
    }
    // 旧数据仍然完整可读（manifest 未变）
    assert_eq!(count_rows(&db, "events"), 100);
    let manifest_after = qsdb::manifest::PartitionManifest::read(db.store(), &pdir)
        .unwrap()
        .unwrap();
    assert_eq!(manifest_after, manifest_before, "manifest 未被触碰");
    std::fs::remove_dir_all(&root).ok();
}

// -- 多实例（模拟多进程）同分区互斥 ------------------------------------------

#[test]
fn multi_instance_same_partition_appends_are_serialized() {
    let root = tmp_root("multi");
    // 两个独立句柄 = 模拟两个进程
    let db1 = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    db1.create_table(events_def()).unwrap();
    let db2 = Qsdb::open(&root, QsdbOptions::default()).unwrap();

    const N: usize = 20;
    const ROWS: usize = 50;
    let barrier = Arc::new(Barrier::new(2));
    let d1 = Arc::new(db1);
    let d2 = Arc::new(db2);
    let h1 = {
        let d = Arc::clone(&d1);
        let b = Arc::clone(&barrier);
        std::thread::spawn(move || {
            b.wait();
            for _ in 0..N {
                d.append("events", &[const_dt_batch(19_004, ROWS)]).unwrap();
            }
        })
    };
    let h2 = {
        let d = Arc::clone(&d2);
        let b = Arc::clone(&barrier);
        std::thread::spawn(move || {
            b.wait();
            for _ in 0..N {
                d.append("events", &[const_dt_batch(19_004, ROWS)]).unwrap();
            }
        })
    };
    h1.join().unwrap();
    h2.join().unwrap();

    let total = count_rows(&d1, "events");
    assert_eq!(total, 2 * N * ROWS, "两实例同分区 append 无丢失无重复");
    // manifest 行数一致
    let pdir = root.join("default").join("events").join("dt=2022-01-12");
    let m = qsdb::manifest::PartitionManifest::read(d1.store(), &pdir)
        .unwrap()
        .unwrap();
    assert_eq!(m.total_rows(), (2 * N * ROWS) as u64);
    assert_eq!(m.segments.len(), 2 * N);
    std::fs::remove_dir_all(&root).ok();
}
