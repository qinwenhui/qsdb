//! **`qsdb-client` ↔ 真 server** 的往返测试（跨实现一致性）。
//!
//! 为什么放在 `qsdb-cli` 而不是 `qsdb-client`：只有这里能起**真的**
//! `qsdb server` 子进程（`CARGO_BIN_EXE_qsdb`）。它验的是客户端最要命的那件事：
//! **两份独立实现（内核的编解码 / `qsdb-client` 的编解码）对同一份字节的理解一致**。
//! 客户端的单测只能证明"自己编自己解"（循环论证），这里才是对撞。
//!
//! 覆盖：装载 → 查询（bulk/rows/ipc/json 四条通道）→ 流式 → 元数据 →
//! 覆盖写与 CAS → query id 透传与取消 → 错误码 → 列名对不上的行为。

use std::io::Read;
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use qsdb_client::{
    Auth, Client, Column, Error, Format, Mode, Param, QueryOptions, Value, WriteOptions,
};

// ---------------------------------------------------------------------------
// 起真 server（同 `http_contract.rs` 的做法）
// ---------------------------------------------------------------------------

struct Server {
    child: Child,
    port: u16,
    root: std::path::PathBuf,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

impl Server {
    fn start() -> Server {
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let uniq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let root =
            std::env::temp_dir().join(format!("qsdb_client_rt_{}_{}", std::process::id(), uniq));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("mkdir");
        let mut child = Command::new(env!("CARGO_BIN_EXE_qsdb"))
            .args(["server", "--path"])
            .arg(&root)
            .args(["--addr", "127.0.0.1:0"])
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn qsdb server");
        let mut stderr = child.stderr.take().expect("stderr");
        let mut buf = Vec::new();
        let deadline = Instant::now() + Duration::from_secs(30);
        let port = loop {
            if Instant::now() > deadline {
                let _ = child.kill();
                panic!("server 未就绪: {}", String::from_utf8_lossy(&buf));
            }
            let mut chunk = [0u8; 512];
            match stderr.read(&mut chunk) {
                Ok(0) | Err(_) => {
                    let _ = child.kill();
                    panic!("server 提前退出: {}", String::from_utf8_lossy(&buf));
                }
                Ok(n) => {
                    buf.extend_from_slice(&chunk[..n]);
                    let text = String::from_utf8_lossy(&buf);
                    if let Some(p) = parse_port(&text) {
                        break p;
                    }
                }
            }
        };
        // 端口开了不等于能服务（axum 起监听在 server 打印之后），探一下更稳
        let addr = format!("127.0.0.1:{port}");
        let dl = Instant::now() + Duration::from_secs(10);
        loop {
            if TcpStream::connect(&addr).is_ok() {
                break;
            }
            if Instant::now() > dl {
                panic!("端口 {port} 连不上");
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        Server { child, port, root }
    }

    fn client(&self) -> Client {
        Client::connect(&format!("http://127.0.0.1:{}", self.port), Auth::None).expect("connect")
    }
}

fn parse_port(text: &str) -> Option<u16> {
    let idx = text.find("listening on http://127.0.0.1:")?;
    let rest = &text[idx + "listening on http://127.0.0.1:".len()..];
    let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
    digits.parse().ok()
}

/// 建一张测试表（全类型）。
fn setup(c: &Client) {
    c.execute(
        "CREATE TABLE t (
            id BIGINT,
            name VARCHAR,
            score DOUBLE,
            ok BOOLEAN,
            d DATE,
            ts TIMESTAMP
        )",
    )
    .expect("create table");
}

fn sample() -> Vec<(&'static str, Column)> {
    vec![
        ("id", vec![Some(1i64), Some(2), None].into()),
        ("name", Column::utf8([Some("alice"), Some("中文"), None])),
        ("score", vec![Some(1.5f64), Some(-0.25), None].into()),
        ("ok", vec![Some(true), Some(false), None].into()),
        ("d", Column::dates([Some(19000), None, Some(-1)])),
        (
            "ts",
            Column::timestamps([Some(1_700_000_000_000_000_000), None, Some(0)]),
        ),
    ]
}

// ---------------------------------------------------------------------------
// 用例
// ---------------------------------------------------------------------------

#[test]
fn 装载与查询_全类型往返() {
    let s = Server::start();
    let c = s.client();
    setup(&c);

    let ack = c.append("t", &sample()).expect("append");
    assert_eq!(ack.rows, 3);
    assert_eq!(ack.mode.as_deref(), Some("append"));

    // 走 bulk（默认通道）：本客户端的解码器 vs 内核的编码器
    let t = c
        .query("SELECT id, name, score, ok, d, ts FROM t ORDER BY id NULLS LAST")
        .expect("query")
        .into_bulk()
        .expect("bulk");
    assert_eq!(t.num_rows(), 3);
    assert_eq!(t.value(0, 0).unwrap().to_value(), Value::I64(1));
    assert_eq!(
        t.value(0, 1).unwrap().to_value(),
        Value::Utf8("alice".into())
    );
    assert_eq!(t.value(0, 2).unwrap().to_value(), Value::F64(1.5));
    assert_eq!(t.value(0, 3).unwrap().to_value(), Value::Bool(true));
    assert_eq!(t.value(0, 4).unwrap().to_value(), Value::Date32(19000));
    assert_eq!(
        t.value(0, 5).unwrap().to_value(),
        Value::TimestampNs(1_700_000_000_000_000_000)
    );
    assert_eq!(
        t.value(1, 1).unwrap().to_value(),
        Value::Utf8("中文".into())
    );
    assert_eq!(t.value(1, 2).unwrap().to_value(), Value::F64(-0.25));
    // 第 2 行（0 基 1）的 d / ts 是 NULL；第 3 行的前四列是 NULL、
    // d = -1、ts = 0 —— **按列各自的 NULL 位图**断言，别整行一把抓
    assert_eq!(t.value(1, 4).unwrap().to_value(), Value::Null, "d 第 2 行");
    assert_eq!(t.value(1, 5).unwrap().to_value(), Value::Null, "ts 第 2 行");
    for col in 0..4 {
        assert_eq!(
            t.value(2, col).unwrap().to_value(),
            Value::Null,
            "第 3 行第 {col} 列应为 NULL"
        );
    }
    assert_eq!(t.value(2, 4).unwrap().to_value(), Value::Date32(-1));
    assert_eq!(t.value(2, 5).unwrap().to_value(), Value::TimestampNs(0));
}

#[test]
fn 行式通道与流式_值一致() {
    let s = Server::start();
    let c = s.client();
    setup(&c);
    c.append("t", &sample()).expect("append");

    // 一次性 rows
    let r = c
        .query_with(
            "SELECT id, name FROM t ORDER BY id NULLS LAST",
            &QueryOptions {
                format: Format::Rows,
                ..Default::default()
            },
        )
        .expect("query rows")
        .into_rows()
        .expect("rows");
    assert_eq!(r.num_rows(), 3);
    assert_eq!(r.get(0, 1).unwrap().to_value(), Value::Utf8("alice".into()));
    assert_eq!(r.get(2, 0).unwrap().to_value(), Value::Null);

    // 流式：同一份数据的另一条实现路径
    let mut st = c
        .query_stream(
            "SELECT id, name FROM t ORDER BY id NULLS LAST",
            &Default::default(),
        )
        .expect("stream");
    assert_eq!(st.schema().len(), 2);
    let mut rows: Vec<(Value, Value)> = Vec::new();
    while let Some(chunk) = st.next_chunk().expect("chunk") {
        for row in chunk.rows() {
            rows.push((
                row.get(0).unwrap().to_value(),
                row.get(1).unwrap().to_value(),
            ));
        }
    }
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].0, Value::I64(1));
    assert_eq!(rows[0].1, Value::Utf8("alice".into()));
    assert_eq!(rows[2].0, Value::Null);
    assert_eq!(st.rows_read(), 3);
}

#[test]
fn 大结果_流式内存恒定且行数正确() {
    let s = Server::start();
    let c = s.client();
    c.execute("CREATE TABLE big (id BIGINT, s VARCHAR)")
        .unwrap();
    // 造 5 万行，分几次装载（**批量装载**，不是逐行 INSERT）
    let n: i64 = 50_000;
    let part: i64 = 10_000;
    let mut i = 0;
    while i < n {
        let ids: Vec<Option<i64>> = (i..i + part).map(Some).collect();
        let ss: Vec<Option<String>> = (i..i + part).map(|v| Some(format!("s-{v}"))).collect();
        let ack = c
            .append("big", &[("id", ids.into()), ("s", Column::utf8(ss))])
            .unwrap();
        assert_eq!(ack.rows as i64, part);
        i += part;
    }
    let mut st = c
        .query_stream("SELECT id, s FROM big", &Default::default())
        .unwrap();
    let mut seen = 0usize;
    let mut chunks = 0usize;
    while let Some(chunk) = st.next_chunk().unwrap() {
        chunks += 1;
        for row in chunk.rows() {
            let id = row.get(0).unwrap().as_i64().unwrap();
            assert_eq!(
                row.get(1).unwrap().as_str().unwrap(),
                format!("s-{id}"),
                "第 {id} 行的字符串对不上"
            );
            seen += 1;
        }
    }
    assert_eq!(seen as i64, n);
    assert!(chunks >= 1, "至少一块");
    // 聚合也在服务端对一遍（别把数据搬回来再数）
    let t = c
        .query("SELECT count(*) FROM big")
        .unwrap()
        .into_bulk()
        .unwrap();
    assert_eq!(t.value(0, 0).unwrap().as_i64(), Some(n));
}

#[test]
fn 零行结果保留_schema() {
    let s = Server::start();
    let c = s.client();
    setup(&c);
    let t = c
        .query("SELECT id, name FROM t WHERE 1 = 0")
        .unwrap()
        .into_bulk()
        .unwrap();
    assert_eq!(t.num_rows(), 0);
    assert_eq!(t.num_columns(), 2);
    assert_eq!(t.schema()[0].name, "id");
    assert_eq!(t.schema()[1].name, "name");
}

#[test]
fn ddl_回执走_json_通道() {
    let s = Server::start();
    let c = s.client();
    // 即使请求 bulk，DDL 也回 JSON 回执（按 Content-Type 分派）
    let rs = c.query("CREATE TABLE d (a BIGINT)").unwrap();
    assert!(rs.is_exec(), "应当是回执，实为 {}", rs.kind());
    let j = rs.into_json().unwrap();
    assert!(j.command.is_some() || j.message.is_some());

    // execute() 是拿到回执的省事写法
    let j = c.execute("INSERT INTO d VALUES (1), (2)").unwrap();
    assert_eq!(j.command.as_deref(), Some("INSERT"));
    assert_eq!(j.rows_affected, Some(2));
    let t = c
        .query("SELECT count(*) FROM d")
        .unwrap()
        .into_bulk()
        .unwrap();
    assert_eq!(t.value(0, 0).unwrap().as_i64(), Some(2));
}

#[test]
fn 元数据端点() {
    let s = Server::start();
    let c = s.client();
    setup(&c);
    c.append("t", &sample()).unwrap();

    let tables = c.tables().unwrap();
    assert!(tables.contains(&"t".to_string()), "{tables:?}");
    let detailed = c.tables_with_schema().unwrap();
    let t = detailed.iter().find(|x| x.name == "t").expect("表 t");
    let names: Vec<&str> = t.columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, vec!["id", "name", "score", "ok", "d", "ts"]);

    let cols = c.schema("t").unwrap();
    assert_eq!(cols.len(), 6);
    // 元数据面透出的是**服务端的类型名**（原样，不做映射——猜成标签反而丢信息）
    assert_eq!(cols[0].type_name, "BIGINT");
    assert_eq!(cols[1].type_name, "STRING");
    assert!(cols[0].nullable);

    let parts = c.partitions("t").unwrap();
    assert!(!parts.is_empty());
    assert!(parts[0].rows >= 3);

    let st = c.stats().unwrap();
    assert!(st.tables >= 1);
    assert!(st.store_bytes > 0);
}

#[test]
fn 覆盖写与cas() {
    let s = Server::start();
    let c = s.client();
    setup(&c);
    c.append("t", &sample()).unwrap();

    // overwrite 是分区级原子替换：整体换掉（幂等）
    c.execute("CREATE TABLE t2 (a BIGINT)").unwrap();
    let first = c
        .append_with(
            "t2",
            &[("a", vec![Some(1i64), Some(2)].into())],
            &WriteOptions::overwrite(),
        )
        .unwrap();
    assert_eq!(first.rows, 2);
    let t = c
        .query("SELECT count(*) FROM t2")
        .unwrap()
        .into_bulk()
        .unwrap();
    assert_eq!(t.value(0, 0).unwrap().as_i64(), Some(2));

    // 再覆盖一次：还是 2 行（不是 4 行）
    c.append_with(
        "t2",
        &[("a", vec![Some(9i64)].into())],
        &WriteOptions::overwrite(),
    )
    .unwrap();
    let t = c
        .query("SELECT count(*) FROM t2")
        .unwrap()
        .into_bulk()
        .unwrap();
    assert_eq!(t.value(0, 0).unwrap().as_i64(), Some(1));

    // CAS：期望 generation 错误 → 拒绝写（且不动数据）
    let err = c
        .append_with(
            "t2",
            &[("a", vec![Some(7i64)].into())],
            &WriteOptions {
                mode: Mode::OverwriteCas(999_999),
                flush: true,
            },
        )
        .unwrap_err();
    assert_eq!(err.code(), Some("WRITE_CONFLICT"), "{err}");
    let t = c
        .query("SELECT count(*) FROM t2")
        .unwrap()
        .into_bulk()
        .unwrap();
    assert_eq!(
        t.value(0, 0).unwrap().as_i64(),
        Some(1),
        "被拒的写不能动数据"
    );
}

#[test]
fn 错误码_表不存在() {
    let s = Server::start();
    let c = s.client();
    let e = c.query("SELECT * FROM no_such_table").unwrap_err();
    assert_eq!(e.code(), Some("TABLE_NOT_FOUND"), "{e}");
    assert_eq!(e.code(), Some("TABLE_NOT_FOUND"));
    assert!(!e.retriable());

    // 元数据端点的对象不存在 → 404 NOT_FOUND（保住 HTTP 语义）
    let e = c.schema("no_such_table").unwrap_err();
    match e {
        Error::Api { status, .. } => assert_eq!(status, 404, "{e}"),
        other => panic!("应当是 Api 错误，实为 {other:?}"),
    }
}

#[test]
fn 错误码目录可运行期拉取() {
    let s = Server::start();
    let c = s.client();
    let v = c.error_codes().unwrap();
    let codes = v["codes"].as_array().expect("codes 数组");
    assert!(!codes.is_empty());
    // 驱动该能据此自配置"码 → 可重试性"，而不是硬编码
    let one = codes
        .iter()
        .find(|x| x["code"] == "TOO_MANY_QUERIES")
        .expect("码表里应有 TOO_MANY_QUERIES");
    assert_eq!(one["retry"], serde_json::Value::Bool(true), "{one}");
    let ro = codes
        .iter()
        .find(|x| x["code"] == "READ_ONLY")
        .expect("码表里应有 READ_ONLY");
    assert_eq!(
        ro["retry"],
        serde_json::Value::Bool(false),
        "只读库永不重试：{ro}"
    );
}

#[test]
fn query_id_透传与以回显为准() {
    let s = Server::start();
    let c = s.client();
    setup(&c);

    // 合法 id：原样采用（JSON 通道在体里回显）
    let j = c
        .query_with("SELECT 1", &QueryOptions::json().with_query_id("cli-abc-1"))
        .unwrap()
        .into_json()
        .unwrap();
    assert_eq!(j.query_id.as_deref(), Some("cli-abc-1"));

    // 非 JSON 通道：id 在**响应头**里（客户端把它塞进 ResultSet::query_id）
    let rs = c
        .query_with(
            "SELECT 1",
            &QueryOptions::default().with_query_id("cli-abc-2"),
        )
        .unwrap();
    assert_eq!(rs.query_id(), Some("cli-abc-2"));

    // 非法 id：显式 400（不静默忽略）
    let e = c
        .query_with("SELECT 1", &QueryOptions::json().with_query_id("bad id!"))
        .unwrap_err();
    assert_eq!(e.code(), Some("INVALID_ARGUMENT"), "{e}");
}

#[test]
fn 取消运行中的查询() {
    let s = Server::start();
    let c = s.client();
    let runner = s.client(); // 另一条连接：取消不能被自己的查询堵住
    c.execute("CREATE TABLE big (id BIGINT, s VARCHAR)")
        .unwrap();
    let n = 5_000i64;
    let ids: Vec<Option<i64>> = (0..n).map(Some).collect();
    let ss: Vec<Option<String>> = (0..n).map(|v| Some(format!("s-{v}"))).collect();
    c.append("big", &[("id", ids.into()), ("s", Column::utf8(ss))])
        .unwrap();

    // 非等值 join = 天然长查询（同 http_contract.rs 的用法）
    let sql = "SELECT count(*) FROM big a JOIN big b ON a.id < b.id";

    // 先量一遍**不取消**的基线：没有它就没法说"取消到底有没有用"
    // （只报"取消用了 14s"是**歪尺子**——那条查询本来可能就要 14s）
    let t0 = Instant::now();
    let full = c.query(sql).expect("基线跑完").into_bulk().expect("bulk");
    let full_secs = t0.elapsed();
    eprintln!(
        "[cancel] 不取消的完整耗时：{full_secs:?}（结果 {}",
        full.value(0, 0).unwrap()
    );

    let qid = "cancel-me-1";
    let handle = std::thread::spawn(move || {
        runner.query_with(sql, &QueryOptions::json().with_query_id(qid))
    });
    // 等它进注册表再取消
    let mut ok = false;
    for _ in 0..100 {
        std::thread::sleep(Duration::from_millis(50));
        let q = c.running_queries().unwrap();
        if q.to_string().contains(qid) {
            ok = true;
            break;
        }
    }
    assert!(ok, "查询没进注册表，取消用例没测到东西");
    let t0 = Instant::now();
    c.cancel(qid).expect("取消");
    let r = handle.join().unwrap();
    let stopped = t0.elapsed();
    // 这条 eprintln 是**测量**，不是装饰：取消的"生效延迟"取决于执行器在哪一层
    // 轮询取消标志。把它与基线放一起才有意义（见 `docs/真机基准对账.md` 的对应条目）
    eprintln!("[cancel] 从发出取消到查询线程收到响应：{stopped:?}（基线 {full_secs:?}）");
    // 判据是**比值**：取消必须比跑完快一个数量级。绝对秒数写死会变成 flaky 的尺子，
    // 而"取消只用了几百毫秒"这种绝对数在慢机器上本来也可能成立却毫无意义。
    // （修复前实测 19.74s：请求被受理、也回了 QUERY_CANCELLED，但执行器要跑到
    //  该形状收尾才轮询到标志 —— 见 `crates/qsdb/tests/cancel_latency.rs`。）
    if full_secs > Duration::from_secs(2) {
        assert!(
            stopped * 10 < full_secs,
            "取消生效太慢：{stopped:?} vs 基线 {full_secs:?}（应快一个数量级）"
        );
    } else {
        eprintln!("跳过比值断言：基线只有 {full_secs:?}（< 2s），比不出快慢");
    }
    match r {
        Err(e) => {
            let code = e.code().unwrap_or("");
            assert!(
                code == "QUERY_CANCELLED" || code == "QUERY_TIMEOUT" || code == "TOO_MANY_QUERIES",
                "取消应当回专属码，实为 {e}"
            );
        }
        // 抢在取消前跑完也算通过（机器太快时会发生）——但要**明确说明**
        Ok(_) => eprintln!("注意：查询在取消生效前已跑完，本用例退化为「不报错」分支"),
    }
}

#[test]
fn 列名对不上_必须有明确反馈() {
    // 这条用例盯的是**静默错值**：批量装载的 body 里带列名，
    // 若内核按位置吞下去而不校验名字，客户端写错列序就会**悄悄写进错的列**。
    let s = Server::start();
    let c = s.client();
    setup(&c);
    let r = c.append(
        "t",
        &[
            ("nope", vec![Some(1i64)].into()),
            ("name", Column::utf8([Some("x")])),
        ],
    );
    match r {
        // 明确报错 = 可以接受
        Err(e) => {
            let code = e.code().unwrap_or("");
            assert!(
                code == "INVALID_ARGUMENT" || code == "COLUMN_NOT_FOUND",
                "错误码应当是「输入有问题」那一类，实为 {e}"
            );
        }
        // 接受了 → 追查写进去的是什么（**不能静默错列**）
        Ok(ack) => {
            let t = c
                .query("SELECT id, name FROM t")
                .unwrap()
                .into_bulk()
                .unwrap();
            assert_eq!(ack.rows, 1);
            // 若列名不校验而按位置写入，`name` 列会拿到 "x"、`id` 会是 NULL
            let id = t.value(0, 0).unwrap().to_value();
            let name = t.value(0, 1).unwrap().to_value();
            panic!(
                "装了列名对不上的批却没报错：id={id:?} name={name:?} —— \
                 若这里能看到值，说明内核**按位置**吞了带错名的批（静默错值）"
            );
        }
    }
}

#[test]
fn ipc_通道往返() {
    use arrow::array::{Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    let s = Server::start();
    let c = s.client();
    c.execute("CREATE TABLE it (id BIGINT, s VARCHAR)").unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("s", DataType::Utf8, true),
    ]));
    let b = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])),
            Arc::new(StringArray::from(vec![Some("a"), Some("b"), None])),
        ],
    )
    .unwrap();
    let ack = c
        .append_batches("it", &[b], &WriteOptions::default())
        .expect("ipc append");
    assert_eq!(ack.rows, 3);

    // 读回（ipc 通道）——本客户端的 IPC 解码 vs 内核的编码
    let rs = c
        .query_with(
            "SELECT id, s FROM it ORDER BY id NULLS LAST",
            &QueryOptions {
                format: Format::Ipc,
                ..Default::default()
            },
        )
        .unwrap();
    let batches = rs.into_ipc().expect("ipc");
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 3);
    assert_eq!(batches[0].num_columns(), 2);
    let ids = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("Int64Array");
    // `ORDER BY id NULLS LAST` ⇒ 1, 3, NULL（**不是**插入序）
    assert_eq!(ids.value(0), 1);
    assert_eq!(ids.value(1), 3);
    assert!(ids.is_null(2), "NULL 必须还是 NULL（IPC 通道也要保住）");
    let ss = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("StringArray");
    // 输入是 (1,"a") / (NULL,"b") / (3,NULL)；`ORDER BY id NULLS LAST` ⇒
    // 1→"a"、3→NULL、NULL→"b"（字符串列与 id 列的 NULL 分布**不同**）
    assert_eq!(ss.value(0), "a");
    assert!(ss.is_null(1));
    assert_eq!(ss.value(2), "b");
}

#[test]
fn 认证失败与成功() {
    // 起一个带静态 token 的 server
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let uniq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let root =
        std::env::temp_dir().join(format!("qsdb_client_auth_{}_{}", std::process::id(), uniq));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_qsdb"))
        .args(["server", "--path"])
        .arg(&root)
        .args(["--addr", "127.0.0.1:0", "--token", "s3cret-token"])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stderr = child.stderr.take().unwrap();
    let mut buf = Vec::new();
    let port = loop {
        let mut chunk = [0u8; 512];
        match stderr.read(&mut chunk) {
            Ok(0) | Err(_) => panic!("server 提前退出"),
            Ok(n) => {
                buf.extend_from_slice(&chunk[..n]);
                let text = String::from_utf8_lossy(&buf);
                if let Some(p) = parse_port(&text) {
                    break p;
                }
            }
        }
    };
    let url = format!("http://127.0.0.1:{port}");
    // 匿名 → 401
    let anon = Client::connect(&url, Auth::None).unwrap();
    let e = anon.tables().unwrap_err();
    match e {
        Error::Api { status, code, .. } => {
            assert_eq!(status, 401);
            assert_eq!(code, "UNAUTHORIZED");
        }
        other => panic!("应当 401，实为 {other:?}"),
    }
    // 带上 token → 通过
    let ok = Client::connect(&url, Auth::Bearer("s3cret-token".into())).unwrap();
    ok.tables().expect("带 token 应当通过");
    // 错 token → 仍然 401
    let bad = Client::connect(&url, Auth::Bearer("wrong".into())).unwrap();
    assert!(bad.tables().is_err());

    let _ = child.kill();
    let _ = child.wait();
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn 健康检查不需要认证() {
    let s = Server::start();
    let c = s.client();
    let v = c.health().unwrap();
    assert!(!v.is_empty(), "版本号不该是空的");
}

/// **参数化查询**（`docs/参数化查询设计.md`）：客户端只负责把值塞进 `params` 数组 ——
/// 值在**内核绑定器**里变成 AST 字面量，**从不进入 SQL 文本**。
///
/// 这条同时钉住三件事，缺一条都不算数：
/// ① 三种形态都能过（裸值推类型 / 显式类型 / `?` 占位符）；
/// ② 参数**个数不对就是 400**（少给、多给都拒 —— 多给尤其要紧：静默忽略会让
///    调用方以为自己防住了注入，其实有个值压根没绑上）；
/// ③ ★ **注入串只是数据**：塞一段 `'; DROP TABLE t; --` 进去，表还在、值就是那串文本。
#[test]
fn 参数化查询往返() {
    let s = Server::start();
    let c = s.client();
    setup(&c);
    c.append("t", &sample()).expect("append");

    // ① 裸值推类型（`1` 推成 BIGINT，与 id 列同类型 ⇒ 能比）
    let t = c
        .query_params("SELECT id, name FROM t WHERE id = $1", [Param::infer(1i64)])
        .expect("query_params")
        .into_bulk()
        .expect("bulk");
    assert_eq!(t.num_rows(), 1);
    assert_eq!(
        t.value(0, 1).unwrap().to_value(),
        Value::Utf8("alice".into())
    );

    // ② 显式类型（`"19000"` 裸值会被推成 STRING，比不过 DATE 列 ⇒ 必须显式）
    let t = c
        .query_params(
            "SELECT id FROM t WHERE d = $1",
            [Param::typed("DATE", "2022-01-08")],
        )
        .expect("typed DATE")
        .into_bulk()
        .expect("bulk");
    assert_eq!(t.num_rows(), 1, "d = 19000 那一行");

    // ③ `?` 占位符（服务端认，驱动**不用改写 SQL**）+ 两个参数
    let t = c
        .query_with(
            "SELECT id FROM t WHERE id >= ? AND id <= ? ORDER BY id",
            &QueryOptions::default().with_params([Param::infer(1i64), Param::infer(2i64)]),
        )
        .expect("? placeholders")
        .into_bulk()
        .expect("bulk");
    assert_eq!(t.num_rows(), 2);

    // ④ 个数对不上 ⇒ 400（少给 / 多给都拒）
    for params in [
        vec![],                                       // 少给
        vec![Param::infer(1i64), Param::infer(2i64)], // 多给
    ] {
        let e = c
            .query_with(
                "SELECT id FROM t WHERE id = $1",
                &QueryOptions::default().with_params(params),
            )
            .expect_err("参数个数不对该报错");
        assert!(
            matches!(e, Error::Api { .. } | Error::Usage(_)),
            "应当是带 code 的错误，实得 {e:?}"
        );
    }

    // ⑤ ★ 注入串**只是数据**：表还在，值就是那串文本本身
    let evil = "x'; DROP TABLE t; --";
    let t = c
        .query_params("SELECT name FROM t WHERE name = $1", [Param::infer(evil)])
        .expect("注入串该被当数据")
        .into_bulk()
        .expect("bulk");
    assert_eq!(
        t.num_rows(),
        0,
        "没有一行叫这个 —— 但它**没有报错**，也没改任何东西"
    );
    let n = c
        .query("SELECT count(*) AS n FROM t")
        .expect("表还在")
        .into_bulk()
        .expect("bulk");
    assert_eq!(
        n.value(0, 0).unwrap().to_value(),
        Value::I64(3),
        "表必须还在、且还是 3 行（`DROP` 只是那串文本）"
    );
}
