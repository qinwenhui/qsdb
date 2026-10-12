//! 公平口径 DuckDB 对比基准（替代 `duckdb_vs` 的权威口径）。
//!
//! 设计原则（吸取 POC 教训）：
//! 1. **冷读口径**：QSDB `scan_cache_bytes = 0`——每轮查询都从 parquet 解码，
//!    与 DuckDB `read_parquet`（每查询重新解码）同起点。旧 bench 给 QSDB 4GB
//!    解码后缓存 + best-of-3，等于让它免解码比 DuckDB 解码——不是同一场比赛。
//! 2. **基数扫描**：group by 从 1K 到 5M 组逐档测，画出交叉点。旧 bench 只测
//!    1.6K 组（低基数=并行 partial agg 最占优形态）。
//! 3. **覆盖缺口**：join 首次纳入常规对比（旧 gate 只有 filter/agg/groupby/点查）。
//! 4. **口径自证**：所有非默认设置打印在表头，读数前先读口径。
//!
//! 运行：cargo run --release -p qsdb-bench --features duckdb --bin fair_vs -- [rows]

#[cfg(feature = "duckdb")]
mod imp {
    use std::sync::Arc;
    use std::time::Instant;

    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use duckdb::Connection;
    use qsdb::api::{Qsdb, QsdbOptions};

    pub fn run() {
        let rows: usize = std::env::args()
            .nth(1)
            .and_then(|v| v.parse().ok())
            .unwrap_or(10_000_000);
        let root = std::env::temp_dir().join("qsdb_fair_vs");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();

        // FAIR_SOLO=qsdb|duck：只测一方（分进程独立测量）。
        // 同进程交替会让双方争 CPU/内存（实测 agg 独立 0.011s vs 同进程 0.048s），
        // 使比率失真；SOLO 用于交叉验证真实差距。
        let solo = std::env::var("FAIR_SOLO").unwrap_or_default();
        let threads = rayon::current_num_threads().min(16).max(1);
        let mem_mb: usize = std::env::var("FAIR_MEM_MB")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(4096);

        // ── 数据准备：QSDB 写出 parquet，双方共用同一批文件 ──
        let db = Qsdb::open(
            &root,
            QsdbOptions {
                scan_cache_bytes: 0, // ★ 冷读：每轮解码（公平起点）
                ..Default::default()
            },
        )
        .unwrap();
        db.sql(
            "CREATE TABLE t (g BIGINT, v BIGINT, x DOUBLE, s STRING, dp BIGINT) PARTITIONED BY (dp)",
        )
        .unwrap();
        let t0 = Instant::now();
        // 分区列 dp 与分组键 g 正交；16 个分区模拟真实布局
        let parts = 16usize;
        let per = rows / parts;
        for p in 0..parts {
            let schema = Arc::new(Schema::new(vec![
                Field::new("g", DataType::Int64, false),
                Field::new("v", DataType::Int64, false),
                Field::new("x", DataType::Float64, false),
                Field::new("s", DataType::Utf8, false),
                Field::new("dp", DataType::Int64, false),
            ]));
            let g: Int64Array = (0..per)
                .map(|i| (i as i64 * 7919 + p as i64) % 5_000_000)
                .collect();
            let v: Int64Array = (0..per).map(|i| (i as i64) % 10_000).collect();
            let x: Float64Array = (0..per).map(|i| ((i % 1000) as f64) * 1.5).collect();
            let s: Vec<String> = (0..per).map(|i| format!("s-{}", i % 250_000)).collect();
            let dp: Int64Array = vec![p as i64; per].into();
            let b = RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(g),
                    Arc::new(v),
                    Arc::new(x),
                    Arc::new(StringArray::from(s)),
                    Arc::new(dp),
                ],
            )
            .unwrap();
            db.append("t", &[b]).unwrap();
        }
        // 维表（join 用）：键域与主表部分重叠
        db.sql("CREATE TABLE dim (g BIGINT, name STRING)").unwrap();
        let dn = per / 4;
        let dschema = Arc::new(Schema::new(vec![
            Field::new("g", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        for p in 0..4 {
            let dg: Int64Array = (0..dn)
                .map(|i| ((i as i64 * 7919 + p as i64 * 11) % 5_000_000))
                .collect();
            let name: Vec<String> = (0..dn).map(|i| format!("n-{}", i % 100_000)).collect();
            let b = RecordBatch::try_new(
                dschema.clone(),
                vec![Arc::new(dg), Arc::new(StringArray::from(name))],
            )
            .unwrap();
            db.append("dim", &[b]).unwrap();
        }
        let tdir = root.join("default").join("t");
        let glob = tdir
            .join("**")
            .join("*.parquet")
            .to_string_lossy()
            .replace('\\', "/");
        let ddir = root.join("default").join("dim");
        let dglob = ddir
            .join("**")
            .join("*.parquet")
            .to_string_lossy()
            .replace('\\', "/");

        println!("=== 口径（读数前先读这里）===");
        println!(
            "  数据：{} 行主表 + {} 行维表（同一批 parquet，QSDB 写出）",
            parts * per,
            4 * dn
        );
        println!("  QSDB：scan_cache_bytes=0（★冷读，每轮解码）+ threads={threads}");
        println!(
            "  DuckDB：read_parquet（每查询解码）+ memory_limit={mem_mb}MB + threads={threads}"
        );
        println!(
            "  计时：★交替测量 5 轮取中位数（含 1 轮预热），±x/y% = QSDB/DuckDB 的 (max−min)/中位；写入 {:.1}s（不计入）",
            t0.elapsed().as_secs_f64()
        );

        // ── DuckDB 就绪：与 QSDB 同等资源 ──
        let duck = Connection::open_in_memory().unwrap();
        duck.execute_batch(&format!(
            "SET memory_limit='{mem_mb}MB'; SET threads={threads};"
        ))
        .unwrap();

        let mut results = Results::default();

        // ══ A. group by 基数扫描（核心：POC T2 打的就是这里）══
        println!("\n=== A. group by 基数扫描（{} 行）===", parts * per);
        println!(
            "{:<12} {:>10} {:>10} {:>8}   说明",
            "基数", "QSDB", "DuckDB", "比率"
        );
        // 每档重建分组表（g 值域 = 基数）——QSDB 不支持 GROUP BY 表达式，
        // 只能用裸列分组；各档数据量相同、仅值域不同，可比。
        let gcard_per = per;
        for card in [1_000i64, 100_000, 1_000_000, 5_000_000] {
            db.sql("DROP TABLE IF EXISTS gt").unwrap();
            db.sql("CREATE TABLE gt (g BIGINT, v BIGINT, dp BIGINT) PARTITIONED BY (dp)")
                .unwrap();
            for p in 0..parts {
                let sch = Arc::new(Schema::new(vec![
                    Field::new("g", DataType::Int64, false),
                    Field::new("v", DataType::Int64, false),
                    Field::new("dp", DataType::Int64, false),
                ]));
                let g: Int64Array = (0..gcard_per)
                    .map(|i| (i as i64 * 7919 + p as i64) % card)
                    .collect();
                let v: Int64Array = (0..gcard_per).map(|i| (i as i64) % 10_000).collect();
                let dp: Int64Array = vec![p as i64; gcard_per].into();
                db.append(
                    "gt",
                    &[
                        RecordBatch::try_new(sch, vec![Arc::new(g), Arc::new(v), Arc::new(dp)])
                            .unwrap(),
                    ],
                )
                .unwrap();
            }
            let gg = tdir
                .join("..")
                .join("gt")
                .join("**")
                .join("*.parquet")
                .to_string_lossy()
                .replace(std::path::MAIN_SEPARATOR, "/");
            let q = bench_pair(
                || {
                    db.sql_collect("SELECT g, count(*), sum(v) FROM gt GROUP BY g")
                        .map(|b| b.iter().map(|x| x.num_rows()).sum::<usize>() as i64)
                },
                || {
                    duck.query_row(
                        &format!("SELECT count(*) FROM (SELECT g, count(*), sum(v) FROM read_parquet('{gg}') AS t GROUP BY g)"),
                        [], |r| r.get::<_, i64>(0))
                },
            );
            let (tq, td, sq, sd) = q;
            results.push("groupby", card, tq, td);
            println!(
                "{:<12} {:>9.3}s {:>9.3}s {:>7.2}x  ±{:.0}/{:.0}%  {}",
                card,
                tq,
                td,
                td / tq,
                sq * 100.0,
                sd * 100.0,
                verdict(td / tq)
            );
        }

        // ══ B. 全局聚合 ══
        println!("\n=== B. 全局聚合（{} 行）===", parts * per);
        let q = bench_pair(
            || {
                db.sql_collect("SELECT count(*), sum(v), sum(x), min(v), max(v) FROM t")
                    .map(|b| b.iter().map(|x| x.num_rows()).sum::<usize>() as i64)
            },
            || {
                duck.query_row(
                    &format!("SELECT count(*) FROM (SELECT count(*), sum(v), sum(x), min(v), max(v) FROM read_parquet('{glob}'))"),
                    [], |r| r.get::<_, i64>(0))
            },
        );
        results.push("aggregate", 0, q.0, q.1);
        println!(
            "{:<12} {:>9.3}s {:>9.3}s {:>7.2}x  ±{:.0}/{:.0}%  {}",
            "agg",
            q.0,
            q.1,
            q.1 / q.0,
            q.2 * 100.0,
            q.3 * 100.0,
            verdict(q.1 / q.0)
        );

        // ══ C. 过滤扫描 ══
        println!("\n=== C. 过滤扫描 ===");
        for (name, pred) in [
            ("num 选择率 1%", "v < 100"),
            ("str 前缀", "s LIKE 's-1%'"),
            ("数值范围", "g BETWEEN 1000000 AND 2000000"),
        ] {
            let q = bench_pair(
                || {
                    db.sql_collect(&format!("SELECT count(*) FROM t WHERE {pred}"))
                        .map(|b| b.iter().map(|x| x.num_rows()).sum::<usize>() as i64)
                },
                || {
                    duck.query_row(
                        &format!("SELECT count(*) FROM read_parquet('{glob}') WHERE {pred}"),
                        [],
                        |r| r.get::<_, i64>(0),
                    )
                },
            );
            results.push("filter", 0, q.0, q.1);
            println!(
                "{:<14} {:>9.3}s {:>9.3}s {:>7.2}x  ±{:.0}/{:.0}%  {}",
                name,
                q.0,
                q.1,
                q.1 / q.0,
                q.2 * 100.0,
                q.3 * 100.0,
                verdict(q.1 / q.0)
            );
        }

        // ══ D. JOIN（旧 gate 的缺口）══
        println!("\n=== D. JOIN（旧 gate 从未覆盖）===");
        for (name, sql_q, sql_d) in [
            (
                "count(*) 等值",
                format!("SELECT count(*) FROM t JOIN dim d ON t.g = d.g"),
                format!(
                    "SELECT count(*) FROM read_parquet('{glob}') AS t JOIN read_parquet('{dglob}') AS d ON t.g = d.g"
                ),
            ),
            (
                "LEFT 等值",
                format!("SELECT count(*) FROM t LEFT JOIN dim d ON t.g = d.g"),
                format!(
                    "SELECT count(*) FROM read_parquet('{glob}') AS t LEFT JOIN read_parquet('{dglob}') AS d ON t.g = d.g"
                ),
            ),
            (
                "物化 1000 行",
                format!("SELECT t.v, d.name FROM t JOIN dim d ON t.g = d.g LIMIT 1000"),
                format!(
                    "SELECT t.v, d.name FROM read_parquet('{glob}') AS t JOIN read_parquet('{dglob}') AS d ON t.g = d.g LIMIT 1000"
                ),
            ),
        ] {
            let q = bench_pair(
                || {
                    db.sql_collect(&sql_q)
                        .map(|b| b.iter().map(|x| x.num_rows()).sum::<usize>() as i64)
                },
                || {
                    duck.query_row(&format!("SELECT count(*) FROM ({sql_d})"), [], |r| {
                        r.get::<_, i64>(0)
                    })
                },
            );
            results.push("join", 0, q.0, q.1);
            println!(
                "{:<14} {:>9.3}s {:>9.3}s {:>7.2}x  ±{:.0}/{:.0}%  {}",
                name,
                q.0,
                q.1,
                q.1 / q.0,
                q.2 * 100.0,
                q.3 * 100.0,
                verdict(q.1 / q.0)
            );
        }

        // ══ E. 点查（分区裁剪）══
        println!("\n=== E. 分区点查 ===");
        let q = bench_pair(
            || {
                db.sql_collect("SELECT count(*) FROM t WHERE dp = 7")
                    .map(|b| b.iter().map(|x| x.num_rows()).sum::<usize>() as i64)
            },
            || {
                duck.query_row(
                    &format!("SELECT count(*) FROM read_parquet('{glob}') WHERE dp = 7"),
                    [],
                    |r| r.get::<_, i64>(0),
                )
            },
        );
        results.push("point", 0, q.0, q.1);
        println!(
            "{:<14} {:>9.3}s {:>9.3}s {:>7.2}x  ±{:.0}/{:.0}%  {}",
            "点查",
            q.0,
            q.1,
            q.1 / q.0,
            q.2 * 100.0,
            q.3 * 100.0,
            verdict(q.1 / q.0)
        );

        // ── 汇总：诚实统计 ──
        let (win, lose) = results.tally();
        println!("\n=== 汇总 ===");
        println!(
            "  胜（≥1.0x）：{win} 项   负：{lose} 项   共 {} 项",
            win + lose
        );
        println!("  注：本表为冷读口径。旧 bench 的 QSDB 4GB 解码后缓存 + best-of-3");
        println!("      使第 2/3 轮零解码，与 DuckDB 每轮解码不可比——不作为对外口径。");
    }

    #[derive(Default)]
    struct Results {
        rows: Vec<(String, i64, f64, f64)>,
    }
    impl Results {
        fn push(&mut self, kind: &str, card: i64, tq: f64, td: f64) {
            self.rows.push((kind.into(), card, tq, td));
        }
        fn tally(&self) -> (usize, usize) {
            let win = self.rows.iter().filter(|r| r.3 / r.2 >= 1.0).count();
            (win, self.rows.len() - win)
        }
    }

    fn verdict(ratio: f64) -> &'static str {
        if ratio >= 2.0 {
            "✓✓ QSDB 大幅领先"
        } else if ratio >= 1.15 {
            "✓ QSDB 领先"
        } else if ratio >= 0.87 {
            "≈ 同量级"
        } else if ratio >= 0.5 {
            "✗ DuckDB 领先"
        } else {
            "✗✗ DuckDB 大幅领先"
        }
    }

    /// 交替测量 5 轮取**中位数**（不是 best）——旧协议先跑完 QSDB×3 再跑
    /// DuckDB×3，机器负载漂移会系统性偏向一方（实测 DuckDB 同查询跨轮
    /// 波动 1.7x）。交替 + 中位数压制漂移；离散度一并报告。
    /// 返回 (qsdb_median, duck_median, qsdb_spread, duck_spread)。
    fn bench_pair<F, G>(mut f: F, mut g: G) -> (f64, f64, f64, f64)
    where
        F: FnMut() -> qsdb::error::Result<i64>,
        G: FnMut() -> Result<i64, duckdb::Error>,
    {
        let solo = std::env::var("FAIR_SOLO").unwrap_or_default();
        // SOLO 模式：对方一侧跳过（返回 0 占位，比率无意义——用绝对时间比较）
        if solo == "qsdb" {
            let mut best = f64::INFINITY;
            for _ in 0..6 {
                let t = Instant::now();
                if f().is_err() {
                    return (f64::NAN, f64::NAN, f64::NAN, f64::NAN);
                }
                best = best.min(t.elapsed().as_secs_f64());
            }
            return (best, 0.0, 0.0, 0.0);
        }
        if solo == "duck" {
            let mut best = f64::INFINITY;
            for _ in 0..6 {
                let t = Instant::now();
                if g().is_err() {
                    return (f64::NAN, f64::NAN, f64::NAN, f64::NAN);
                }
                best = best.min(t.elapsed().as_secs_f64());
            }
            return (0.0, best, 0.0, 0.0);
        }
        const ROUNDS: usize = 5;
        let mut qs: Vec<f64> = Vec::with_capacity(ROUNDS);
        let mut ds: Vec<f64> = Vec::with_capacity(ROUNDS);
        let mut qr = 0i64;
        let mut dr = 0i64;
        // 预热一轮（不计入）：消除首轮冷启动（页缓存/分配器）
        if f().is_err() {
            return (f64::NAN, f64::NAN, f64::NAN, f64::NAN);
        }
        if g().is_err() {
            return (f64::NAN, f64::NAN, f64::NAN, f64::NAN);
        }
        for _ in 0..ROUNDS {
            // ★ 交替：两引擎在同一时间窗口内各测一次，负载漂移同向影响
            let t = Instant::now();
            match f() {
                Ok(v) => qr = v,
                Err(e) => {
                    eprintln!("qsdb error: {e}");
                    return (f64::NAN, f64::NAN, f64::NAN, f64::NAN);
                }
            }
            qs.push(t.elapsed().as_secs_f64());
            let t = Instant::now();
            match g() {
                Ok(v) => dr = v,
                Err(e) => {
                    eprintln!("duckdb error: {e}");
                    return (f64::NAN, f64::NAN, f64::NAN, f64::NAN);
                }
            }
            ds.push(t.elapsed().as_secs_f64());
        }
        let _ = (qr, dr); // 结果对账在专项脚本做；此处只计时
        let qm = median(&qs);
        let dm = median(&ds);
        let spread = |v: &[f64], m: f64| {
            if m > 0.0 {
                (v.iter().cloned().fold(f64::MIN, f64::max)
                    - v.iter().cloned().fold(f64::MAX, f64::min))
                    / m
            } else {
                0.0
            }
        };
        (qm, dm, spread(&qs, qm), spread(&ds, dm))
    }

    fn median(v: &[f64]) -> f64 {
        let mut s = v.to_vec();
        s.sort_by(|a, b| a.partial_cmp(b).unwrap());
        s[s.len() / 2]
    }
}

#[cfg(feature = "duckdb")]
fn main() {
    imp::run();
}

#[cfg(not(feature = "duckdb"))]
fn main() {
    println!("需要 --features duckdb");
}
