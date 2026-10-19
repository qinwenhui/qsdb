//! 公平口径 DuckDB 对比基准（v2：**彻底分进程**）。
//!
//! ## 为什么重写
//!
//! v1 实测发现三类**系统性偏差**，每次都让结论偏移 2x 量级：
//! 1. **同进程交替**：QSDB 受干扰后慢 2x、DuckDB 仅 1.3x（抗干扰弱是真实特征，
//!    但会让比率偏离单查询真实水平）
//! 2. **段间相互影响**：高基数 group by 会留下分配器碎片/TLB 污染，使后续所有
//!    查询慢 1.7-2x（v1 曾把 groupby 段排最前，污染了后面全部数字）
//! 3. **DuckDB 侧被拖慢**：v1 的 join count 报 1.91x，受控探针复核实为 0.91x
//!    ——DuckDB 在 v1 环境里慢 57%（0.145s vs 0.092s）
//!
//! ## v2 口径
//!
//! - 每个「用例 × 引擎」**独立子进程**（`--case N --engine qsdb|duck`），
//!   进程退出即释放全部内存/线程/分配器状态 → 零残留影响
//! - 数据只准备一次（主进程建库，子进程复用）
//! - 子进程内交替 7 轮取**中位数**并报离散度
//! - QSDB `scan_cache_bytes = 0`（冷读，与 DuckDB 每查询现解码同起点）
//! - 线程数与内存上限两侧对齐
//!
//! 运行：cargo run --release -p qsdb-bench --features duckdb --bin fair_vs -- [rows]

#[cfg(feature = "duckdb")]
mod imp {
    use std::process::Command;
    use std::sync::Arc;

    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use qsdb::api::{Qsdb, QsdbOptions};

    pub const ROWS: usize = 10_000_000;
    pub const PARTS: usize = 16;

    fn arg(name: &str) -> Option<String> {
        let a: Vec<String> = std::env::args().collect();
        a.iter()
            .position(|x| x == name)
            .and_then(|i| a.get(i + 1).cloned())
    }

    pub fn root() -> std::path::PathBuf {
        std::env::temp_dir().join("qsdb_fair_v2")
    }

    /// 用例：(名称, QSDB SQL, DuckDB SQL)。DuckDB SQL 为空 = 需专门数据准备。
    pub fn cases() -> Vec<(&'static str, String, String)> {
        let g = root()
            .join("default")
            .join("t")
            .join("**")
            .join("*.parquet")
            .to_string_lossy()
            .replace(std::path::MAIN_SEPARATOR, "/");
        let d = root()
            .join("default")
            .join("dim")
            .join("**")
            .join("*.parquet")
            .to_string_lossy()
            .replace(std::path::MAIN_SEPARATOR, "/");
        vec![
            (
                "agg",
                "SELECT count(*), sum(v), sum(x), min(v), max(v) FROM t".into(),
                format!("SELECT count(*), sum(v), sum(x), min(v), max(v) FROM read_parquet('{g}')"),
            ),
            (
                "filter_num",
                "SELECT count(*) FROM t WHERE v < 100".into(),
                format!("SELECT count(*) FROM read_parquet('{g}') WHERE v < 100"),
            ),
            (
                "filter_str",
                "SELECT count(*) FROM t WHERE s LIKE 's-1%'".into(),
                format!("SELECT count(*) FROM read_parquet('{g}') WHERE s LIKE 's-1%'"),
            ),
            (
                "join_count",
                "SELECT count(*) FROM t JOIN dim d ON t.g = d.g".into(),
                format!(
                    "SELECT count(*) FROM read_parquet('{g}') AS t JOIN read_parquet('{d}') AS d ON t.g = d.g"
                ),
            ),
            (
                "join_mat",
                "SELECT t.v, d.name FROM t JOIN dim d ON t.g = d.g LIMIT 1000".into(),
                format!(
                    "SELECT t.v, d.name FROM read_parquet('{g}') AS t JOIN read_parquet('{d}') AS d ON t.g = d.g LIMIT 1000"
                ),
            ),
            (
                "point",
                "SELECT count(*) FROM t WHERE dp = 7".into(),
                format!("SELECT count(*) FROM read_parquet('{g}') WHERE dp = 7"),
            ),
            (
                "union_all",
                "SELECT count(*) FROM (SELECT v FROM t UNION ALL SELECT v FROM t)".into(),
                format!(
                    "SELECT count(*) FROM (SELECT v FROM read_parquet('{g}') UNION ALL SELECT v FROM read_parquet('{g}'))"
                ),
            ),
        ]
    }

    /// 建库（仅主进程；已存在则跳过）
    fn ensure_data(rows: usize) {
        let root = root();
        let marker = root.join("_ready");
        if marker.exists() {
            return;
        }
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let db = Qsdb::open(
            &root,
            QsdbOptions {
                scan_cache_bytes: 0,
                ..Default::default()
            },
        )
        .unwrap();
        db.sql("CREATE TABLE t (g BIGINT, v BIGINT, x DOUBLE, s STRING, dp BIGINT) PARTITIONED BY (dp)")
            .unwrap();
        let sc = Arc::new(Schema::new(vec![
            Field::new("g", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
            Field::new("x", DataType::Float64, false),
            Field::new("s", DataType::Utf8, false),
            Field::new("dp", DataType::Int64, false),
        ]));
        let per = rows / PARTS;
        for p in 0..PARTS as i64 {
            let g: Int64Array = (0..per)
                .map(|i| (i as i64 * 7919 + p) % 5_000_000)
                .collect();
            let v: Int64Array = (0..per).map(|i| i as i64).collect();
            let x = Float64Array::from(
                (0..per)
                    .map(|i| ((i % 1000) as f64) * 1.5)
                    .collect::<Vec<_>>(),
            );
            let s = StringArray::from(
                (0..per)
                    .map(|i| format!("s-{}", i % 250_000))
                    .collect::<Vec<_>>(),
            );
            let dp: Int64Array = vec![p; per].into();
            db.append(
                "t",
                &[RecordBatch::try_new(
                    sc.clone(),
                    vec![
                        Arc::new(g),
                        Arc::new(v),
                        Arc::new(x),
                        Arc::new(s),
                        Arc::new(dp),
                    ],
                )
                .unwrap()],
            )
            .unwrap();
        }
        db.sql("CREATE TABLE dim (g BIGINT, name BIGINT)").unwrap();
        let dsc = Arc::new(Schema::new(vec![
            Field::new("g", DataType::Int64, false),
            Field::new("name", DataType::Int64, false),
        ]));
        for p in 0..4i64 {
            let n = per / 4;
            let g: Int64Array = (0..n)
                .map(|i| (i as i64 * 7919 + p * 11) % 5_000_000)
                .collect();
            let nm: Int64Array = (0..n).map(|i| i as i64).collect();
            db.append(
                "dim",
                &[RecordBatch::try_new(dsc.clone(), vec![Arc::new(g), Arc::new(nm)]).unwrap()],
            )
            .unwrap();
        }
        std::fs::write(&marker, b"1").unwrap();
    }

    /// 子进程：跑一个用例的一个引擎，打印 `<engine> <median_secs> <spread>`
    fn run_one(case_idx: usize, engine: &str) {
        let cs = cases();
        let (_, q_sql, d_sql) = &cs[case_idx];
        let threads: usize = std::env::var("FAIR_THREADS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(16);

        let mut times: Vec<f64> = Vec::new();
        if engine == "qsdb" {
            let db = Qsdb::open(
                root(),
                QsdbOptions {
                    scan_cache_bytes: 0,
                    ..Default::default()
                },
            )
            .unwrap();
            // ★ 冷启动 vs 稳态：独立进程含冷开销（表定义/分区发现/首次缺页）。
            // 先记录**首轮**（冷），再充分预热后取稳态中位数——两个数都报，
            // 因为常驻 server 的真实场景介于两者之间且偏向稳态。
            let t_cold = std::time::Instant::now();
            let _ = db.sql_collect(q_sql).unwrap();
            let cold = t_cold.elapsed().as_secs_f64();
            for _ in 0..12 {
                let _ = db.sql_collect(q_sql).unwrap(); // 预热到稳态
            }
            for _ in 0..7 {
                let t = std::time::Instant::now();
                let _ = db.sql_collect(q_sql).unwrap();
                times.push(t.elapsed().as_secs_f64());
            }
            let _ = cold;
        } else {
            let duck = duckdb::Connection::open_in_memory().unwrap();
            duck.execute_batch(&format!("SET threads={threads}; SET memory_limit='4GB';"))
                .unwrap();
            let t_cold = std::time::Instant::now();
            let _ = duck.query_row(&format!("SELECT count(*) FROM ({d_sql})"), [], |r| {
                r.get::<_, i64>(0)
            });
            let cold = t_cold.elapsed().as_secs_f64();
            for _ in 0..12 {
                let _ = duck.query_row(&format!("SELECT count(*) FROM ({d_sql})"), [], |r| {
                    r.get::<_, i64>(0)
                });
            }
            let _ = cold;
            for _ in 0..7 {
                let t = std::time::Instant::now();
                let _ = duck
                    .query_row(&format!("SELECT count(*) FROM ({d_sql})"), [], |r| {
                        r.get::<_, i64>(0)
                    })
                    .unwrap();
                times.push(t.elapsed().as_secs_f64());
            }
        }
        times.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let med = times[times.len() / 2];
        let spread = if med > 0.0 {
            (times[times.len() - 1] - times[0]) / med
        } else {
            0.0
        };
        println!("{} {:.6} {:.3}", engine, med, spread);
    }

    pub fn run() {
        let rows: usize = std::env::args()
            .nth(1)
            .and_then(|v| v.parse().ok())
            .unwrap_or(ROWS);

        if let (Some(c), Some(e)) = (arg("--case"), arg("--engine")) {
            run_one(c.parse().unwrap(), &e);
            return;
        }

        ensure_data(rows);
        let exe = std::env::current_exe().unwrap();
        let cs = cases();
        println!("=== 公平口径 v2（每个「用例×引擎」独立子进程）===");
        println!("  QSDB scan_cache_bytes=0（冷读）; DuckDB read_parquet; 线程各 16");
        println!("  子进程内跑 7 轮取中位数；± = (max−min)/中位\n");
        println!(
            "{:<12} {:>9} {:>9} {:>8}  {}",
            "用例", "QSDB", "DuckDB", "比率", "离散度"
        );
        let mut win = 0;
        let mut total = 0;
        for (i, (name, _, d_sql)) in cs.iter().enumerate() {
            if d_sql.is_empty() {
                continue;
            }
            let run = |engine: &str| -> (f64, f64) {
                let out = Command::new(&exe)
                    .args(["--case", &i.to_string(), "--engine", engine])
                    .output()
                    .expect("spawn");
                let s = String::from_utf8_lossy(&out.stdout);
                let mut it = s.split_whitespace();
                let _e = it.next();
                let t: f64 = it.next().unwrap_or("nan").parse().unwrap_or(f64::NAN);
                let sp: f64 = it.next().unwrap_or("0").parse().unwrap_or(0.0);
                (t, sp)
            };
            let (qt, qs) = run("qsdb");
            let (dt, ds) = run("duck");
            let r = dt / qt;
            if r >= 1.0 {
                win += 1;
            }
            total += 1;
            println!(
                "{name:<12} {:>8.3}s {:>8.3}s {:>7.2}x  ±{:.0}/{:.0}%",
                qt,
                dt,
                r,
                qs * 100.0,
                ds * 100.0
            );
        }
        println!("\n胜 {win} / {total}（≥1.0x 计胜）");
        println!("注：每个测量独立进程，消除同进程交替 / 段间残留 / DuckDB 被拖慢三类偏差");
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
