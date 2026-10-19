//! 并行扫描互锁回归（2026-10-11 修）。
//!
//! **症状**：`SELECT k, v FROM p UNION ALL SELECT k, v FROM p` 在多段的表上
//! **永久挂死**（`qsdb-bench/src/bin/hang_repro.rs` 里那句"UNION ALL … 挂点"
//! 记的就是它）。
//!
//! **机制**：`ParScanExec` 的生产分片在**有界 channel** 上 `send`（满即阻塞）。
//! 计划构建期会同时建出多个扫描节点（UNION 左右、JOIN build/probe）。若生产端
//! 在构建期就启动，**没被抽的那一侧**会先把 channel 填满、全部分片任务阻塞在
//! `send` 上把池线程占住 → 被抽的那一侧分片排不上线程 → 消费端 `recv` 永不返回。
//!
//! **修法**：① 生产端**懒启动**（首次 `next` 才提交分片）——让"正在被抽的扫描"
//! 独占池线程；② 生产端改走**专用池**（`exec::par_scan::scan_pool`），不再占用
//! 全局 rayon 池，从结构上消除与查询自身并行（聚合分片/UDAF）的互锁。
//!
//! 本测试用"多段表 + 两侧全扫"复现原形，并**重复多次**——它是竞态，
//! 单次通过说明不了问题。

use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use qsdb::{Qsdb, QsdbOptions};

/// 建一张**非分区**表并逐批 append（每批一个 segment）——段数必须明显超过
/// channel 容量（`threads.clamp(2,16)*2 ≤ 32`），否则生产端塞得下、不阻塞，
/// 也就复现不出死锁。
fn many_segment_db(tag: &str, segs: usize) -> (Qsdb, std::path::PathBuf) {
    let root = std::env::temp_dir()
        .join("qsdb_parscan_test")
        .join(format!("{tag}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    db.sql("CREATE TABLE p (k BIGINT, v BIGINT)").unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("v", DataType::Int64, true),
    ]));
    for i in 0..segs as i64 {
        let b = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![i % 16])),
                Arc::new(Int64Array::from(vec![i])),
            ],
        )
        .unwrap();
        db.append("p", &[b]).unwrap();
    }
    (db, root)
}

#[test]
fn union_all_two_parallel_scans_does_not_deadlock() {
    let (db, root) = many_segment_db("union", 200);
    for i in 0..5 {
        let out = db
            .sql_collect("SELECT k, v FROM p UNION ALL SELECT k, v FROM p")
            .unwrap_or_else(|e| panic!("第 {i} 轮失败: {e}"));
        let rows: usize = out.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 400, "第 {i} 轮行数");
    }
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn union_distinct_two_parallel_scans_does_not_deadlock() {
    let (db, root) = many_segment_db("uniond", 200);
    for i in 0..5 {
        let out = db
            .sql_collect("SELECT k FROM p UNION SELECT k FROM p")
            .unwrap_or_else(|e| panic!("第 {i} 轮失败: {e}"));
        let rows: usize = out.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 16, "第 {i} 轮去重后行数");
    }
    let _ = std::fs::remove_dir_all(&root);
}

/// 两侧都有谓词的形态（`hang_repro` 的 bench D 形状）。
#[test]
fn join_two_parallel_scans_does_not_deadlock() {
    let (db, root) = many_segment_db("join", 200);
    for i in 0..5 {
        let out = db
            .sql_collect(
                "SELECT count(*) FROM p a JOIN p b ON a.k = b.k WHERE a.v < 100 AND b.v < 100",
            )
            .unwrap_or_else(|e| panic!("第 {i} 轮失败: {e}"));
        assert!(!out.is_empty(), "第 {i} 轮应有结果");
    }
    let _ = std::fs::remove_dir_all(&root);
}

/// 三个扫描同存（链式 UNION），覆盖"多个未抽侧"的情形。
#[test]
fn chained_union_does_not_deadlock() {
    let (db, root) = many_segment_db("chain", 120);
    let out = db
        .sql_collect("SELECT k FROM p UNION ALL SELECT k FROM p UNION ALL SELECT k FROM p")
        .unwrap();
    let rows: usize = out.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 360);
    let _ = std::fs::remove_dir_all(&root);
}
