//! QSDB 综合基准套件（`bench_suite`）：**导入四路 / 导出 / 简单与复杂分析 SQL /
//! 每任务耗时与峰值 RSS / DuckDB 对照**。
//!
//! 与 `ab_gate` 的分工：`ab_gate` 是**回归闸门**（A/B 两棵树比版本，用例固定、跑得快）；
//! 本工具是**对外基准**（面铺开：导入导出 + 分析 SQL + 对手对齐，跑得久）。
//!
//! ## 为什么每个用例单独起进程
//! 峰值 RSS（Linux `/proc/self/status` 的 `VmHWM`）是**进程级单调量**——同进程里跑
//! 第二个用例只会读到第一个的峰值。故 `--all` 是父进程，逐用例 `spawn` 自己 +
//! `--case X`，子进程退出即拿到干净读数。
//!
//! ## DuckDB 对齐口径
//! DuckDB 是**独立进程**（`/root/bench/duckdb`）：CentOS 7 的 gcc 4.8 编不了 bundled
//! C++，官方新版二进制又要 GLIBC 2.23（`.14` 是 GLIBC 2.17）。故走 **CLI v1.1.3**，
//! 与本工具读**同一份源文件**（CSV / Parquet），SQL 逐条对应（`--gen-duckdb-sql` 导出）。
//! 峰值 RSS 由远端 `runner.py` 轮询 `/proc/<pid>/status` 得到（同口径）。
//!
//! 用法：
//! ```text
//! bench_suite --dir DIR --rows N --gen          # 造数据（表 + src/*.csv + src/*.parquet）
//! bench_suite --dir DIR --list                  # 列用例
//! bench_suite --dir DIR --case NAME --rounds R  # 单用例（子进程口径）
//! bench_suite --dir DIR --all --rows N          # 全跑（父进程驱动，出汇总表）
//! bench_suite --dir DIR --gen-duckdb-sql        # 写 DuckDB 对照 SQL
//! ```

use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Instant;

use arrow::array::{Date32Array, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use qsdb::{Qsdb, QsdbOptions};

// ---------------------------------------------------------------------------
// 数据形状
// ---------------------------------------------------------------------------

/// 事实表列（导入目标 = 无分区表；分析表 = 同一批数据的 `t`，也做无分区，
/// 免得分区裁剪把"简单分析"变成"读一个分区"而失真）。
const FACT_COLS: &str = "id BIGINT, g BIGINT, gs STRING, v BIGINT, x DOUBLE, s STRING, dt DATE";

/// 造数：事实表 `t`（N 行）+ 维表 `d`（200k）+ 小维表 `d2`（1k）。
///
/// 列域刻意覆盖常见分析形态：
/// - `g` ∈ [0, 100k)：**中基数**分组键（join 键，与 d.g 对应）
/// - `gs`：**高基数**字符串（`k-{i}`，distinct 的主战场）
/// - `s`：字符串谓词（`region-{0..49}` 前缀 + 填充，LIKE 用）
/// - `x`：双精度（avg/sum）
/// - `dt`：日期（时间维）
/// 逐批生成事实表数据（**流式**）。
///
/// **为什么必须流式**：导入用例测的是"导入算子的内存"，若先把 5M 行全建在内存里
/// 再喂，峰值 RSS 记的是**造数器**的占用（实测把 append 的峰值抬到 583MB，真值被淹没）。
fn for_each_batch(rows: i64, per: i64, mut f: impl FnMut(RecordBatch)) {
    let schema = fact_schema();
    let mut done = 0i64;
    while done < rows {
        let n = per.min(rows - done);
        let b = (0..n).map(|i| done + i).collect::<Vec<i64>>();
        let id = Int64Array::from(b.clone());
        let g = Int64Array::from(b.iter().map(|i| i % 100_000).collect::<Vec<i64>>());
        let gs = StringArray::from(
            b.iter()
                .map(|i| format!("k-{:012}", i))
                .collect::<Vec<String>>(),
        );
        let v = Int64Array::from(
            b.iter()
                .map(|i| (i * 2654435761) % 1_000_000)
                .collect::<Vec<i64>>(),
        );
        let x = Float64Array::from(b.iter().map(|i| (*i as f64) * 0.25).collect::<Vec<f64>>());
        let s = StringArray::from(
            b.iter()
                .map(|i| format!("region-{:02}-{:040}", i % 50, i))
                .collect::<Vec<String>>(),
        );
        let dt = Date32Array::from(
            b.iter()
                .map(|i| 19_000 + (i % 365) as i32)
                .collect::<Vec<i32>>(),
        );
        f(RecordBatch::try_new(
            std::sync::Arc::clone(&schema),
            vec![
                std::sync::Arc::new(id),
                std::sync::Arc::new(g),
                std::sync::Arc::new(gs),
                std::sync::Arc::new(v),
                std::sync::Arc::new(x),
                std::sync::Arc::new(s),
                std::sync::Arc::new(dt),
            ],
        )
        .expect("build batch"));
        done += n;
    }
}

/// 全部批（仅造数/写源文件用；导入用例走 [`for_each_batch`] 流式）。
fn make_batches(rows: i64, per: i64) -> Vec<RecordBatch> {
    let mut out = Vec::new();
    for_each_batch(rows, per, |b| out.push(b));
    out
}

fn fact_schema() -> arrow::datatypes::SchemaRef {
    std::sync::Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("g", DataType::Int64, true),
        Field::new("gs", DataType::Utf8, true),
        Field::new("v", DataType::Int64, true),
        Field::new("x", DataType::Float64, true),
        Field::new("s", DataType::Utf8, true),
        Field::new("dt", DataType::Date32, true),
    ]))
}

// ---------------------------------------------------------------------------
// 用例清单
// ---------------------------------------------------------------------------

/// (名字, 简单/复杂分组, 说明)
fn cases() -> Vec<(&'static str, &'static str, &'static str)> {
    vec![
        (
            "import_csv",
            "导入",
            "CSV → 管理表（read_csv + INSERT SELECT）",
        ),
        (
            "import_parquet",
            "导入",
            "Parquet → 管理表（read_parquet + INSERT SELECT）",
        ),
        (
            "import_insert",
            "导入",
            "批量 INSERT（多行 VALUES，1000 行/语句）",
        ),
        ("import_append", "导入", "嵌入 append（内存批直写）"),
        (
            "export_parquet",
            "导出",
            "全表扫描 → Parquet 文件（内核暂无导出原语，见 docs §F9）",
        ),
        ("export_csv", "导出", "全表扫描 → CSV 文件"),
        ("simple_count", "简单", "count(*)"),
        ("simple_filter", "简单", "过滤 + 聚合（g < 1000）"),
        (
            "simple_group",
            "简单",
            "GROUP BY g（10 万组，count/sum/avg）",
        ),
        ("simple_topn", "简单", "ORDER BY v DESC LIMIT 100"),
        ("simple_distinct", "简单", "COUNT(DISTINCT gs)（高基数）"),
        (
            "complex_join_group",
            "复杂",
            "事实 × 维表 join + GROUP BY cat + ORDER BY",
        ),
        ("complex_cte", "复杂", "CTE 聚合 + HAVING + 二次 join"),
        (
            "complex_window",
            "复杂",
            "窗口函数（PARTITION BY g ORDER BY v）",
        ),
        ("complex_three_way", "复杂", "三表 join（t × d × d2）+ 分组"),
    ]
}

/// DuckDB 对照 SQL（与上面逐条对应；读**同一份源文件**，公平）。
fn duckdb_sql(dir: &Path) -> String {
    let pq = dir
        .join("src/t.parquet")
        .to_string_lossy()
        .replace('\\', "/");
    let cs = dir.join("src/t.csv").to_string_lossy().replace('\\', "/");
    let d = dir
        .join("src/d.parquet")
        .to_string_lossy()
        .replace('\\', "/");
    let d2 = dir
        .join("src/d2.parquet")
        .to_string_lossy()
        .replace('\\', "/");
    let out_dir = dir.to_string_lossy().replace('\\', "/");
    // 逐用例一个 `-- CASE <名> rounds=N` 块：runner.py 按块执行 + 计时 + 采峰值 RSS。
    // 口径：DuckDB 侧**每次现读 parquet**（不建持久表）；本仓侧是「管理表 + 段缓存」。
    // 这个口径对 DuckDB 更不利（它要重新解析/解码，本仓命中缓存），故下方结果里
    // DuckDB 的数字偏保守——刻意如此，宁可低估自己。
    let cases: Vec<(&str, String)> = vec![
        (
            "import_csv",
            format!(
                "DROP TABLE IF EXISTS t_csv;
CREATE TABLE t_csv AS SELECT * FROM read_csv_auto('{cs}');"
            ),
        ),
        (
            "import_parquet",
            format!(
                "DROP TABLE IF EXISTS t_pq;
CREATE TABLE t_pq AS SELECT * FROM read_parquet('{pq}');"
            ),
        ),
        (
            "export_parquet",
            format!(
                "COPY (SELECT * FROM read_parquet('{pq}')) TO '{out_dir}/out_duckdb.parquet' (FORMAT PARQUET);"
            ),
        ),
        (
            "export_csv",
            format!(
                "COPY (SELECT * FROM read_parquet('{pq}')) TO '{out_dir}/out_duckdb.csv' (FORMAT CSV, HEADER);"
            ),
        ),
        (
            "simple_count",
            format!("SELECT count(*) FROM read_parquet('{pq}');"),
        ),
        (
            "simple_filter",
            format!("SELECT count(*), sum(v) FROM read_parquet('{pq}') WHERE g < 1000;"),
        ),
        (
            "simple_group",
            format!(
                "SELECT g, count(*) c, sum(v) s, avg(x) a FROM read_parquet('{pq}') GROUP BY g;"
            ),
        ),
        (
            "simple_topn",
            format!("SELECT id, v FROM read_parquet('{pq}') ORDER BY v DESC LIMIT 100;"),
        ),
        (
            "simple_distinct",
            format!("SELECT count(DISTINCT gs) FROM read_parquet('{pq}');"),
        ),
        (
            "complex_join_group",
            format!(
                "SELECT d.cat, count(*) c, sum(t.v) s FROM read_parquet('{pq}') t JOIN read_parquet('{d}') d ON t.g = d.g GROUP BY d.cat ORDER BY s DESC LIMIT 20;"
            ),
        ),
        (
            "complex_cte",
            format!(
                "WITH big AS (SELECT g, sum(v) s FROM read_parquet('{pq}') GROUP BY g HAVING sum(v) > 1000) SELECT count(*) FROM big JOIN read_parquet('{d}') d ON big.g = d.g;"
            ),
        ),
        (
            "complex_window",
            format!(
                "SELECT g, v, row_number() OVER (PARTITION BY g ORDER BY v DESC) rn FROM read_parquet('{pq}') WHERE g < 50 LIMIT 200;"
            ),
        ),
        (
            "complex_three_way",
            format!(
                "SELECT d2.cat, count(*) c FROM read_parquet('{pq}') t JOIN read_parquet('{d}') d ON t.g = d.g JOIN read_parquet('{d2}') d2 ON d.cat = d2.cat GROUP BY d2.cat;"
            ),
        ),
    ];
    let mut out = String::from(
        "-- DuckDB 对照：读同一份源文件，逐 CASE 块计时 + 采峰值 RSS（runner.py）
-- 口径：DuckDB 每次现读 parquet；本仓侧是管理表 + 段缓存（对 DuckDB 不利）
",
    );
    for (name, sql) in cases {
        out.push_str(&format!(
            "
-- CASE {name} rounds=3
{sql}
"
        ));
    }
    out
}

// ---------------------------------------------------------------------------
// 峰值 RSS（Linux VmHWM；其他平台恒 0——本工具的真身跑在 Linux）
// ---------------------------------------------------------------------------

fn peak_rss_kb() -> u64 {
    if let Ok(s) = std::fs::read_to_string("/proc/self/status") {
        for line in s.lines() {
            if let Some(rest) = line.strip_prefix("VmHWM:") {
                if let Some(kb) = rest.split_whitespace().next() {
                    return kb.parse().unwrap_or(0);
                }
            }
        }
    }
    0
}

// ---------------------------------------------------------------------------
// 造数 / 落盘
// ---------------------------------------------------------------------------

fn open(dir: &Path) -> Qsdb {
    Qsdb::open(dir, QsdbOptions::default()).expect("open db")
}

/// 源文件写出（CSV + Parquet）：DuckDB 与本仓读**同一份**。
fn write_sources(dir: &Path, batches: &[RecordBatch]) -> std::io::Result<()> {
    let src = dir.join("src");
    std::fs::create_dir_all(&src)?;

    // Parquet（arrow writer；与内核段文件同格式族）
    let pq = src.join("t.parquet");
    let f = std::fs::File::create(&pq)?;
    let mut w = ArrowWriter::try_new(f, fact_schema(), None).expect("parquet writer");
    for b in batches {
        w.write(b).expect("parquet write");
    }
    w.close().expect("parquet close");

    // CSV（手写：本工具的源文件，不走内核）
    let mut f = std::io::BufWriter::new(std::fs::File::create(src.join("t.csv"))?);
    writeln!(f, "id,g,gs,v,x,s,dt")?;
    for b in batches {
        let id = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let g = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        let gs = b.column(2).as_any().downcast_ref::<StringArray>().unwrap();
        let v = b.column(3).as_any().downcast_ref::<Int64Array>().unwrap();
        let x = b.column(4).as_any().downcast_ref::<Float64Array>().unwrap();
        let s = b.column(5).as_any().downcast_ref::<StringArray>().unwrap();
        let dt = b.column(6).as_any().downcast_ref::<Date32Array>().unwrap();
        for i in 0..b.num_rows() {
            // 日期按 §3.4 文本形态（内核 read_csv 应能解析）
            let (y, m, d) = civil_from_days(dt.value(i));
            // 列序必须与 `FACT_COLS` 一致：id,g,gs,v,x,s,dt
            writeln!(
                f,
                "{},{},{},{},{},{},{:04}-{:02}-{:02}",
                id.value(i),
                g.value(i),
                gs.value(i),
                v.value(i),
                x.value(i),
                s.value(i),
                y,
                m,
                d
            )?;
        }
    }
    Ok(())
}

fn civil_from_days(days: i32) -> (i32, u32, u32) {
    let mut days = days;
    let mut y = 1970i32;
    loop {
        let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
        let len = if leap { 366 } else { 365 };
        if days < len {
            break;
        }
        days -= len;
        y += 1;
    }
    let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
    let ml = [
        31,
        if leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    let mut m = 0usize;
    while days >= ml[m] {
        days -= ml[m];
        m += 1;
    }
    (y, (m + 1) as u32, (days + 1) as u32)
}

/// 维表：`d`（g → name/cat）+ `d2`（cat → 别名），落 parquet 供 DuckDB 读。
fn write_dims(dir: &Path) -> std::io::Result<()> {
    let src = dir.join("src");
    std::fs::create_dir_all(&src)?;
    // d2 **每个 cat 恰好一行**（20 行）：多行会让 `JOIN d2 ON d.cat = d2.cat`
    // 变成多对多扇出——实测把三表 join 的中间结果撑到 5 亿行，.14 上烧掉 30 分钟 CPU。
    // 基准用例要测"正常分析查询"，不是病态 join。
    for (name, rows, is_d2) in [("d", 200_000i64, false), ("d2", 20i64, true)] {
        let (schema, batch) = if is_d2 {
            let schema = std::sync::Arc::new(Schema::new(vec![
                Field::new("cat", DataType::Utf8, true),
                Field::new("alias", DataType::Utf8, true),
            ]));
            let cat = StringArray::from(
                (0..rows)
                    .map(|i| format!("cat-{:02}", i % 20))
                    .collect::<Vec<String>>(),
            );
            let alias = StringArray::from(
                (0..rows)
                    .map(|i| format!("alias-{i}"))
                    .collect::<Vec<String>>(),
            );
            (
                schema.clone(),
                RecordBatch::try_new(
                    schema,
                    vec![std::sync::Arc::new(cat), std::sync::Arc::new(alias)],
                )
                .unwrap(),
            )
        } else {
            let schema = std::sync::Arc::new(Schema::new(vec![
                Field::new("g", DataType::Int64, true),
                Field::new("name", DataType::Utf8, true),
                Field::new("cat", DataType::Utf8, true),
            ]));
            let g = Int64Array::from((0..rows).map(|i| i % 100_000).collect::<Vec<i64>>());
            let nm = StringArray::from(
                (0..rows)
                    .map(|i| format!("name-{:06}", i))
                    .collect::<Vec<String>>(),
            );
            let cat = StringArray::from(
                (0..rows)
                    .map(|i| format!("cat-{:02}", i % 20))
                    .collect::<Vec<String>>(),
            );
            (
                schema.clone(),
                RecordBatch::try_new(
                    schema,
                    vec![
                        std::sync::Arc::new(g),
                        std::sync::Arc::new(nm),
                        std::sync::Arc::new(cat),
                    ],
                )
                .unwrap(),
            )
        };
        let f = std::fs::File::create(src.join(format!("{name}.parquet")))?;
        let mut w = ArrowWriter::try_new(f, schema, None).expect("parquet writer");
        w.write(&batch).expect("write dim");
        w.close().expect("close dim");
    }
    Ok(())
}

/// 造数据：管理表 `t`（分析用）+ `d`/`d2`（join 用）+ 源文件（导入/DuckDB 用）。
fn generate(dir: &Path, rows: i64, per: i64) {
    let db = open(dir);
    if db.sql("SELECT count(*) FROM t").is_ok() {
        println!("管理表已存在，跳过建表（源文件仍会补写）");
    } else {
        let t0 = Instant::now();
        db.sql(&format!("CREATE TABLE t ({FACT_COLS})")).unwrap();
        db.sql("CREATE TABLE d (g BIGINT, name STRING, cat STRING)")
            .unwrap();
        db.sql("CREATE TABLE d2 (cat STRING, alias STRING)")
            .unwrap();
        let batches = make_batches(rows, per);
        for b in &batches {
            db.append("t", std::slice::from_ref(b)).unwrap();
        }
        println!("t 灌入 {rows} 行，耗时 {:.1}s", t0.elapsed().as_secs_f64());
    }
    // 维表灌入（分析用例用）
    let d_rows = 200_000i64;
    // 逐表判断（**别用"d 有数据就跳过 d2"**：形状改过之后 d2 需要重建）
    if !has_rows(&db, "d") {
        let g = Int64Array::from((0..d_rows).map(|i| i % 100_000).collect::<Vec<i64>>());
        let nm = StringArray::from(
            (0..d_rows)
                .map(|i| format!("name-{i:06}"))
                .collect::<Vec<String>>(),
        );
        let cat = StringArray::from(
            (0..d_rows)
                .map(|i| format!("cat-{:02}", i % 20))
                .collect::<Vec<String>>(),
        );
        let schema = std::sync::Arc::new(Schema::new(vec![
            Field::new("g", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("cat", DataType::Utf8, true),
        ]));
        let b = RecordBatch::try_new(
            schema,
            vec![
                std::sync::Arc::new(g),
                std::sync::Arc::new(nm),
                std::sync::Arc::new(cat),
            ],
        )
        .unwrap();
        for off in (0..d_rows).step_by(50_000) {
            let n = 50_000.min(d_rows - off) as usize;
            db.append("d", &[b.slice(off as usize, n)]).unwrap();
        }
    }
    if !has_rows(&db, "d2") {
        let cats = StringArray::from(
            (0..20i64)
                .map(|i| format!("cat-{i:02}"))
                .collect::<Vec<String>>(),
        );
        let alias = StringArray::from(
            (0..20i64)
                .map(|i| format!("alias-{i}"))
                .collect::<Vec<String>>(),
        );
        let schema2 = std::sync::Arc::new(Schema::new(vec![
            Field::new("cat", DataType::Utf8, true),
            Field::new("alias", DataType::Utf8, true),
        ]));
        db.append(
            "d2",
            &[RecordBatch::try_new(
                schema2,
                vec![std::sync::Arc::new(cats), std::sync::Arc::new(alias)],
            )
            .unwrap()],
        )
        .unwrap();
        println!("维表 d/d2 灌入完成");
    }
    let batches = make_batches(rows, per);
    write_sources(dir, &batches).expect("write sources");
    write_dims(dir).expect("write dims");
    println!(
        "源文件就绪：{}/src/{{t.csv,t.parquet,d.parquet,d2.parquet}}",
        dir.display()
    );
}

// ---------------------------------------------------------------------------
// 用例执行
// ---------------------------------------------------------------------------

/// 取单个标量（`count(*)` 之类）——`produced` 要的是**行数**，不是"结果集有几行"。
fn scalar_i64(db: &Qsdb, sql: &str) -> i64 {
    let bs = db
        .sql_collect(sql)
        .unwrap_or_else(|e| panic!("{sql} 失败: {e}"));
    bs.iter()
        .find(|b| b.num_rows() > 0)
        .and_then(|b| {
            arrow::util::display::array_value_to_string(b.column(0).as_ref(), 0)
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
        })
        .unwrap_or(0)
}

/// 表是否已有数据（造数幂等）。
fn has_rows(db: &Qsdb, table: &str) -> bool {
    db.sql_collect(&format!("SELECT count(*) FROM {table}"))
        .map(|bs| {
            bs.iter().any(|b| {
                (0..b.num_rows()).any(|i| {
                    arrow::util::display::array_value_to_string(b.column(0).as_ref(), i)
                        .map(|s| s != "0")
                        .unwrap_or(false)
                })
            })
        })
        .unwrap_or(false)
}

fn rows_of(db: &Qsdb, sql: &str) -> u64 {
    match db.sql_collect(sql) {
        Ok(bs) => bs.iter().map(|b| b.num_rows() as u64).sum(),
        Err(e) => panic!("{sql} 失败: {e}"),
    }
}

/// 跑一个用例一轮（返回"产出规模"用于自检：防止用例空转）。
fn run_once(dir: &Path, case: &str, rows: i64, per: i64) -> (u64, u64) {
    let db = open(dir);
    let src = dir.to_string_lossy().replace('\\', "/");
    match case {
        // ---- 导入 ----
        "import_csv" => {
            db.sql("DROP TABLE IF EXISTS t_csv").ok();
            db.sql(&format!("CREATE TABLE t_csv ({FACT_COLS})"))
                .unwrap();
            let t0 = Instant::now();
            db.sql(&format!(
                "INSERT INTO t_csv SELECT id, g, gs, v, x, s, dt FROM read_csv('{src}/src/t.csv')"
            ))
            .expect("import csv");
            let dt = t0.elapsed();
            let n = scalar_i64(&db, "SELECT count(*) FROM t_csv") as u64;
            assert!(n > 0, "import_csv 灌了 0 行");
            (n, dt.as_millis() as u64)
        }
        "import_parquet" => {
            db.sql("DROP TABLE IF EXISTS t_pq").ok();
            db.sql(&format!("CREATE TABLE t_pq ({FACT_COLS})")).unwrap();
            let t0 = Instant::now();
            db.sql(&format!(
                "INSERT INTO t_pq SELECT id, g, gs, v, x, s, dt FROM read_parquet('{src}/src/t.parquet')"
            ))
            .expect("import parquet");
            let dt = t0.elapsed();
            let n = scalar_i64(&db, "SELECT count(*) FROM t_pq") as u64;
            assert!(n > 0, "import_parquet 灌了 0 行");
            (n, dt.as_millis() as u64)
        }
        "import_insert" => {
            db.sql("DROP TABLE IF EXISTS t_ins").ok();
            db.sql(&format!("CREATE TABLE t_ins ({FACT_COLS})"))
                .unwrap();
            let t0 = Instant::now();
            // 1000 行/语句（贴近"客户端批量 INSERT"的真实形态：解析 + 绑定 + 执行都算）
            for_each_batch(rows, per, |b| {
                for off in (0..b.num_rows()).step_by(1000) {
                    let batch = b.slice(off, 1000.min(b.num_rows() - off));
                    let mut sql = String::from("INSERT INTO t_ins VALUES ");
                    for i in 0..batch.num_rows() {
                        if i > 0 {
                            sql.push(',');
                        }
                        sql.push_str(&format!(
                            "({},{},'{}',{},{},'{}',DATE '2022-01-01')",
                            int_at(&batch, 0, i),
                            int_at(&batch, 1, i),
                            str_at(&batch, 2, i),
                            int_at(&batch, 3, i),
                            float_at(&batch, 4, i),
                            str_at(&batch, 5, i),
                        ));
                    }
                    db.sql(&sql).expect("insert");
                }
            });
            let dt = t0.elapsed();
            let n = scalar_i64(&db, "SELECT count(*) FROM t_ins") as u64;
            assert_eq!(
                n, rows as u64,
                "import_insert 必须真的插满（别只插每批的头一段）"
            );
            (n, dt.as_millis() as u64)
        }
        "import_append" => {
            db.sql("DROP TABLE IF EXISTS t_app").ok();
            db.sql(&format!("CREATE TABLE t_app ({FACT_COLS})"))
                .unwrap();
            let t0 = Instant::now();
            for_each_batch(rows, per, |b| {
                db.append("t_app", std::slice::from_ref(&b)).unwrap();
            });
            let dt = t0.elapsed();
            let n = scalar_i64(&db, "SELECT count(*) FROM t_app") as u64;
            assert_eq!(n, rows as u64, "import_append 必须真的灌满");
            (n, dt.as_millis() as u64)
        }
        // ---- 导出 ----
        "export_parquet" => {
            let out = dir.join("out_qsdb.parquet");
            let t0 = Instant::now();
            // 返回值的第二项是**毫秒**（`run_once` 的约定）。写错过一次——把"写入字节数"
            // 当成耗时返回，报出 577330s（160 小时）这种一眼假的数；顺手去掉那个误导量。
            let (n, _bytes) =
                scan_to_parquet(&db, &out, "SELECT * FROM t", FACT_COLS, fact_schema());
            let dt = t0.elapsed();
            (n, dt.as_millis() as u64)
        }
        "export_csv" => {
            let out = dir.join("out_qsdb.csv");
            let t0 = Instant::now();
            let n = scan_to_csv(&db, &out, "SELECT * FROM t");
            let dt = t0.elapsed();
            (n, dt.as_millis() as u64)
        }
        // ---- 简单分析 ----
        "simple_count" => {
            let t0 = Instant::now();
            let n = rows_of(&db, "SELECT count(*) FROM t");
            (n, t0.elapsed().as_millis() as u64)
        }
        "simple_filter" => {
            let t0 = Instant::now();
            let n = rows_of(&db, "SELECT count(*), sum(v) FROM t WHERE g < 1000");
            (n, t0.elapsed().as_millis() as u64)
        }
        "simple_group" => {
            let t0 = Instant::now();
            let n = rows_of(
                &db,
                "SELECT g, count(*) c, sum(v) s, avg(x) a FROM t GROUP BY g",
            );
            (n, t0.elapsed().as_millis() as u64)
        }
        "simple_topn" => {
            let t0 = Instant::now();
            let n = rows_of(&db, "SELECT id, v FROM t ORDER BY v DESC LIMIT 100");
            (n, t0.elapsed().as_millis() as u64)
        }
        "simple_distinct" => {
            let t0 = Instant::now();
            let n = rows_of(&db, "SELECT count(DISTINCT gs) FROM t");
            (n, t0.elapsed().as_millis() as u64)
        }
        // ---- 复杂分析 ----
        "complex_join_group" => {
            let t0 = Instant::now();
            let n = rows_of(
                &db,
                "SELECT d.cat, count(*) c, sum(t.v) s FROM t JOIN d ON t.g = d.g \
                 GROUP BY d.cat ORDER BY s DESC LIMIT 20",
            );
            (n, t0.elapsed().as_millis() as u64)
        }
        "complex_cte" => {
            let t0 = Instant::now();
            let n = rows_of(
                &db,
                "WITH big AS (SELECT g, sum(v) s FROM t GROUP BY g HAVING sum(v) > 1000) \
                 SELECT count(*) FROM big JOIN d ON big.g = d.g",
            );
            (n, t0.elapsed().as_millis() as u64)
        }
        "complex_window" => {
            let t0 = Instant::now();
            let n = rows_of(
                &db,
                "SELECT g, v, row_number() OVER (PARTITION BY g ORDER BY v DESC) rn \
                 FROM t WHERE g < 50 LIMIT 200",
            );
            (n, t0.elapsed().as_millis() as u64)
        }
        "complex_three_way" => {
            let t0 = Instant::now();
            let n = rows_of(
                &db,
                "SELECT d2.cat, count(*) c FROM t JOIN d ON t.g = d.g \
                 JOIN d2 ON d.cat = d2.cat GROUP BY d2.cat",
            );
            (n, t0.elapsed().as_millis() as u64)
        }
        other => panic!("未知用例 {other}"),
    }
}

fn int_at(b: &RecordBatch, c: usize, i: usize) -> i64 {
    b.column(c)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(i)
}
fn float_at(b: &RecordBatch, c: usize, i: usize) -> f64 {
    b.column(c)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .value(i)
}
fn str_at(b: &RecordBatch, c: usize, i: usize) -> String {
    b.column(c)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(i)
        .to_string()
}

/// 全表扫描 → Parquet（内核暂无导出原语，这里是"用内核读 + 标准写"的基线）。
fn scan_to_parquet(
    db: &Qsdb,
    out: &Path,
    sql: &str,
    _cols: &str,
    schema: arrow::datatypes::SchemaRef,
) -> (u64, u64) {
    let f = std::fs::File::create(out).expect("create out");
    let mut w = ArrowWriter::try_new(f, schema, None).expect("writer");
    let mut n = 0u64;
    let mut bytes = 0u64;
    for b in db.sql_collect(sql).expect("scan") {
        n += b.num_rows() as u64;
        bytes += b.get_array_memory_size() as u64;
        w.write(&b).expect("write");
    }
    w.close().expect("close");
    (n, bytes)
}

/// 全表扫描 → CSV（同上；文本形态另算一路）。
fn scan_to_csv(db: &Qsdb, out: &Path, sql: &str) -> u64 {
    let mut f = std::io::BufWriter::new(std::fs::File::create(out).expect("create out"));
    let mut n = 0u64;
    for b in db.sql_collect(sql).expect("scan") {
        for i in 0..b.num_rows() {
            let mut cells: Vec<String> = Vec::with_capacity(b.num_columns());
            for c in 0..b.num_columns() {
                let a = b.column(c);
                cells.push(if a.is_null(i) {
                    String::new()
                } else {
                    arrow::util::display::array_value_to_string(a.as_ref(), i).unwrap()
                });
            }
            writeln!(f, "{}", cells.join(",")).unwrap();
        }
        n += b.num_rows() as u64;
    }
    n
}

// ---------------------------------------------------------------------------
// 主流程
// ---------------------------------------------------------------------------

fn arg(name: &str) -> Option<String> {
    let a: Vec<String> = std::env::args().collect();
    a.iter()
        .position(|x| x == name)
        .and_then(|i| a.get(i + 1).cloned())
}

fn has(flag: &str) -> bool {
    std::env::args().any(|x| x == flag)
}

fn main() {
    let dir = PathBuf::from(arg("--dir").unwrap_or_else(|| "/root/bench/suite-data".into()));
    let rows: i64 = arg("--rows")
        .and_then(|v| v.parse().ok())
        .unwrap_or(3_000_000);
    let per: i64 = arg("--per").and_then(|v| v.parse().ok()).unwrap_or(65_536);
    let rounds: usize = arg("--rounds").and_then(|v| v.parse().ok()).unwrap_or(3);
    std::fs::create_dir_all(&dir).expect("mkdir");

    if has("--list") {
        println!("{:<20} {:<6} 说明", "用例", "分组");
        for (n, g, d) in cases() {
            println!("{n:<20} {g:<6} {d}");
        }
        return;
    }
    if has("--gen-duckdb-sql") {
        let p = dir.join("results");
        std::fs::create_dir_all(&p).unwrap();
        let f = p.join("duckdb.sql");
        std::fs::write(&f, duckdb_sql(&dir)).expect("write sql");
        println!("{}", f.display());
        return;
    }
    if has("--gen") {
        generate(&dir, rows, per);
        return;
    }
    // 诊断用：跑一条 SQL 并报「耗时 + 扫描行数」（各路径分段计时靠它）
    if let Some(sql) = arg("--sql") {
        let db = open(&dir);
        let t0 = Instant::now();
        match db.sql_collect(&sql) {
            Ok(bs) => {
                let n: usize = bs.iter().map(|b| b.num_rows()).sum();
                println!(
                    "SQL ok: {:.3}s rows={} rss_mb={:.1}",
                    t0.elapsed().as_secs_f64(),
                    n,
                    peak_rss_kb() as f64 / 1024.0
                );
            }
            Err(e) => println!("SQL ERR: {e}"),
        }
        return;
    }
    if let Some(case) = arg("--case") {
        let mut times = Vec::new();
        let mut produced = 0u64;
        for _ in 0..rounds {
            let (n, ms) = run_once(&dir, &case, rows, per);
            produced = n;
            times.push(ms as f64 / 1000.0);
        }
        times.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let med = times[times.len() / 2];
        let spread = (times[times.len() - 1] - times[0]) / med.max(1e-9) * 100.0;
        // 机器可读一行（父进程解析）+ 人读一行
        println!(
            "CASE {} median={:.4} min={:.4} spread={:.1}% produced={} rss_kb={}",
            case,
            med,
            times[0],
            spread,
            produced,
            peak_rss_kb()
        );
        return;
    }
    // --all：父进程逐用例起子进程（峰值 RSS 要干净的进程）
    let me = std::env::current_exe().expect("exe");
    let out_dir = dir.join("results");
    std::fs::create_dir_all(&out_dir).unwrap();
    let tag = arg("--tag").unwrap_or_else(|| "qsdb".into());
    let mut lines = vec!["case,group,median_s,spread_pct,produced,rss_kb".to_string()];
    println!(
        "{:<20} {:<6} {:>10} {:>8} {:>12} {:>10}",
        "用例", "分组", "中位(s)", "离散", "产出", "峰值RSS(MB)"
    );
    for (name, group, _) in cases() {
        let out = std::process::Command::new(&me)
            .args([
                "--dir",
                &dir.to_string_lossy(),
                "--rows",
                &rows.to_string(),
                "--per",
                &per.to_string(),
                "--rounds",
                &rounds.to_string(),
                "--case",
                name,
            ])
            .output()
            .expect("spawn child");
        let text = String::from_utf8_lossy(&out.stdout);
        let line = text
            .lines()
            .find(|l| l.starts_with("CASE "))
            .unwrap_or_else(|| {
                println!("{name:<20} 失败：{}", String::from_utf8_lossy(&out.stderr));
                ""
            });
        if line.is_empty() {
            continue;
        }
        let get = |k: &str| -> String {
            line.split_whitespace()
                .find(|t| t.starts_with(&format!("{k}=")))
                .map(|t| t[k.len() + 1..].to_string())
                .unwrap_or_default()
        };
        let med: f64 = get("median").parse().unwrap_or(0.0);
        let spread: f64 = get("spread").trim_end_matches('%').parse().unwrap_or(0.0);
        let produced: u64 = get("produced").parse().unwrap_or(0);
        let rss: u64 = get("rss_kb").parse().unwrap_or(0);
        println!(
            "{name:<20} {group:<6} {med:>10.4} {spread:>7.1}% {produced:>12} {:>10.1}",
            rss as f64 / 1024.0
        );
        lines.push(format!(
            "{name},{group},{med:.4},{spread:.1},{produced},{rss}"
        ));
    }
    let f = out_dir.join(format!("{tag}-results.csv"));
    std::fs::write(&f, lines.join("\n")).expect("write results");
    println!("\n结果已写入 {}", f.display());
}
