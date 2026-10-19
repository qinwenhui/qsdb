//! 文件管理面（二十批，`docs/文件管理面设计.md`）：
//! 浏览（list）→ 详情（detail）→ 删除安全（trash：引用禁删 / 孤儿进回收站）
//! + 权限分档（读授权可浏览下载、删除恒 admin、非 admin 虚拟根）。
//!
//! 起**真** `qsdb server` 子进程 + 手写 HTTP/1.1（同 `http_contract.rs` 的做法）。

use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// 起真 server + 手写 HTTP 客户端（http_contract.rs 同款）
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
    fn start(extra_args: &[&str]) -> Server {
        let root = next_root("qsdb_files_test");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("mkdir temp root");
        let args: Vec<String> = extra_args.iter().map(|s| s.to_string()).collect();
        spawn_server(&root, &args)
    }

    /// 带一个额外挂载（挂在数据根下：`<root>/<mount>`——list 的根视图会看到它）。
    fn start_mounted(mount: &str, extra_args: &[&str]) -> Server {
        let root = next_root("qsdb_files_test");
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).expect("mkdir temp root");
        let mount_dir = root.join(mount);
        let args: Vec<String> = extra_args
            .iter()
            .map(|s| s.to_string())
            .chain([
                "--mount".to_string(),
                format!("{mount}={}", mount_dir.display()),
            ])
            .collect();
        spawn_server(&root, &args)
    }
}

fn next_root(tag: &str) -> std::path::PathBuf {
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let uniq = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!("{tag}_{}_{}", std::process::id(), uniq))
}

fn spawn_server(root: &std::path::Path, extra_args: &[String]) -> Server {
    let mut child = Command::new(env!("CARGO_BIN_EXE_qsdb"))
        .args(["server", "--path"])
        .arg(root)
        .args(["--addr", "127.0.0.1:0"])
        .args(extra_args)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn qsdb server");
    let mut stderr = child.stderr.take().expect("stderr piped");
    let mut buf = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(30);
    let port = loop {
        if Instant::now() > deadline {
            let _ = child.kill();
            panic!("server 未在 30s 内就绪: {}", String::from_utf8_lossy(&buf));
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
                if let Some(p) = parse_listening_port(&text) {
                    break p;
                }
            }
        }
    };
    Server {
        child,
        port,
        root: root.to_path_buf(),
    }
}

fn parse_listening_port(text: &str) -> Option<u16> {
    let idx = text.find("listening on http://127.0.0.1:")?;
    let rest = &text[idx + "listening on http://127.0.0.1:".len()..];
    let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
    digits.parse().ok()
}

struct Resp {
    status: u16,
    body: Vec<u8>,
}

impl Resp {
    fn json(&self) -> serde_json::Value {
        serde_json::from_slice(&self.body).unwrap_or_else(|e| {
            panic!(
                "响应不是 JSON（{}）: {}",
                e,
                String::from_utf8_lossy(&self.body)
            )
        })
    }
    fn code(&self) -> String {
        self.json()
            .get("code")
            .and_then(|c| c.as_str())
            .unwrap_or("<no code>")
            .to_string()
    }
    fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }
}

fn http(port: u16, method: &str, path: &str, headers: &[(&str, &str)], body: &[u8]) -> Resp {
    let mut s = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    s.set_read_timeout(Some(Duration::from_secs(60))).unwrap();
    let mut req = format!("{method} {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n");
    req.push_str(&format!("Content-Length: {}\r\n", body.len()));
    for (k, v) in headers {
        req.push_str(&format!("{k}: {v}\r\n"));
    }
    req.push_str("\r\n");
    s.write_all(req.as_bytes()).unwrap();
    s.write_all(body).unwrap();
    let mut raw = Vec::new();
    s.read_to_end(&mut raw).unwrap();
    let sep = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("no header/body separator");
    let head = String::from_utf8_lossy(&raw[..sep]).to_string();
    let status: u16 = head
        .lines()
        .next()
        .and_then(|l| l.split(' ').nth(1))
        .and_then(|c| c.parse().ok())
        .unwrap_or(0);
    let mut body = raw[sep + 4..].to_vec();
    // JSON 响应也可能是 chunked（流式中间件）——统一解掉
    if head
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked")
    {
        body = dechunk(&body);
    }
    Resp { status, body }
}

fn dechunk(mut b: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    while let Some(pos) = b.windows(2).position(|w| w == b"\r\n") {
        let size_str = String::from_utf8_lossy(&b[..pos]);
        let Ok(size) = usize::from_str_radix(size_str.split(';').next().unwrap_or("0").trim(), 16)
        else {
            break;
        };
        if size == 0 {
            break;
        }
        let start = pos + 2;
        out.extend_from_slice(&b[start..start + size]);
        b = &b[start + size + 2..];
    }
    out
}

fn auth_header(tok: &str) -> String {
    format!("Bearer {tok}")
}

fn sql(port: u16, tok: &str, statement: &str) -> Resp {
    let body = serde_json::json!({ "sql": statement }).to_string();
    let auth = auth_header(tok);
    http(
        port,
        "POST",
        "/api/v1/query",
        &[
            ("Content-Type", "application/json"),
            ("Authorization", &auth),
        ],
        body.as_bytes(),
    )
}

fn get(port: u16, tok: &str, path: &str) -> Resp {
    let auth = auth_header(tok);
    http(port, "GET", path, &[("Authorization", &auth)], b"")
}

fn post(port: u16, tok: &str, path: &str) -> Resp {
    let auth = auth_header(tok);
    http(
        port,
        "POST",
        path,
        &[
            ("Content-Type", "application/json"),
            ("Authorization", &auth),
        ],
        b"",
    )
}

fn login(port: u16, user: &str, pass: &str) -> String {
    use base64::Engine;
    let b64 = base64::engine::general_purpose::STANDARD.encode(format!("{user}:{pass}"));
    let r = http(
        port,
        "POST",
        "/api/v1/auth/login",
        &[("Authorization", &format!("Basic {b64}"))],
        b"",
    );
    assert_eq!(r.status, 200, "login {user}: {}", r.text());
    r.json()["token"].as_str().expect("token").to_string()
}

fn set_grants(port: u16, admin_tok: &str, user: &str, grants: serde_json::Value) {
    let body = serde_json::json!({ "grants": grants }).to_string();
    let auth = auth_header(admin_tok);
    let r = http(
        port,
        "PUT",
        &format!("/api/v1/users/{user}/grants"),
        &[
            ("Content-Type", "application/json"),
            ("Authorization", &auth),
        ],
        body.as_bytes(),
    );
    assert_eq!(r.status, 200, "grants {user}: {}", r.text());
}

/// 分区表 + 无分区表各一张。
fn seed(port: u16, tok: &str) {
    assert_eq!(
        sql(
            port,
            tok,
            "CREATE TABLE p (dt DATE, id BIGINT, s STRING) PARTITIONED BY (dt)"
        )
        .status,
        200,
        "{}",
        sql(port, tok, "SELECT 1").text()
    );
    assert_eq!(
        sql(
            port,
            tok,
            "INSERT INTO p VALUES (DATE '2026-01-01',1,'a'),(DATE '2026-01-01',2,'b'),\
             (DATE '2026-01-02',3,NULL)"
        )
        .status,
        200
    );
    assert_eq!(sql(port, tok, "CREATE TABLE q (a BIGINT)").status, 200);
    assert_eq!(sql(port, tok, "INSERT INTO q VALUES (7),(8)").status, 200);
}

/// 从目录清单里取第一个段文件名。
fn first_seg(listing: &serde_json::Value) -> String {
    listing["entries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["type"] == "file" && e["name"].as_str().unwrap_or("").starts_with("seg_"))
        .expect("该有段文件")["name"]
        .as_str()
        .unwrap()
        .to_string()
}

// ---------------------------------------------------------------------------
// 测试
// ---------------------------------------------------------------------------

/// admin 浏览：根 → 表 → 分区 → 段；详情带归属/列统计/readSql。
#[test]
fn 浏览_列表与详情() {
    let s = Server::start(&["--token", "t0k"]);
    let (p, t) = (s.port, "t0k");
    seed(p, t);

    // ① 根清单：有 default 目录；内部目录（_ 开头）不回显
    let r = get(p, t, "/api/v1/files/list");
    assert_eq!(r.status, 200, "{}", r.text());
    let j = r.json();
    assert_eq!(j["dir"], "");
    let names: Vec<&str> = j["entries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["name"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"default"), "{names:?}");
    assert!(
        names.iter().all(|n| !n.starts_with('_')),
        "内部目录不回显: {names:?}"
    );
    assert!(
        j["mounts"]
            .as_array()
            .unwrap()
            .iter()
            .any(|m| m["name"] == "default" && m["isDefault"] == true),
        "{:?}",
        j["mounts"]
    );

    // ② 表目录：两张表
    let r = get(p, t, "/api/v1/files/list?dir=default");
    assert_eq!(r.status, 200);
    let j = r.json();
    let names: Vec<&str> = j["entries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["name"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"p") && names.contains(&"q"), "{names:?}");

    // ③ 分区清单：段文件免读 parquet 就带归属摘要
    let r = get(p, t, "/api/v1/files/list?dir=default/p/dt=2026-01-01");
    assert_eq!(r.status, 200, "{}", r.text());
    let j = r.json();
    assert_eq!(j["dir"], "default/p/dt=2026-01-01");
    let seg = first_seg(&j);
    let entry = j["entries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["name"] == seg.as_str())
        .unwrap();
    assert_eq!(entry["segment"]["referenced"], true);
    assert_eq!(entry["segment"]["rows"], 2);
    assert!(entry["size"].as_u64().unwrap() > 0);

    // ④ 段详情：归属 + 列统计 + readSql
    let r = get(
        p,
        t,
        &format!("/api/v1/files/detail?path=default/p/dt=2026-01-01/{seg}"),
    );
    assert_eq!(r.status, 200, "{}", r.text());
    let j = r.json();
    assert_eq!(j["kind"], "segment");
    assert_eq!(j["db"], "default");
    assert_eq!(j["table"], "p");
    assert_eq!(j["partition"], "dt=2026-01-01");
    assert_eq!(j["segment"]["referenced"], true);
    assert_eq!(j["segment"]["rows"], 2);
    assert_eq!(j["segment"]["generation"], 1);
    let cols = j["segment"]["columns"].as_array().unwrap();
    let id = cols.iter().find(|c| c["name"] == "id").expect("id 列统计");
    assert_eq!(id["min"], "1");
    assert_eq!(id["max"], "2");
    assert_eq!(id["nullCount"], 0);
    let read_sql = j["readSql"].as_str().expect("readSql");
    assert!(
        read_sql.starts_with("SELECT * FROM read_parquet("),
        "{read_sql}"
    );

    // ⑤ manifest 详情：generation/行数/段数
    let r = get(
        p,
        t,
        "/api/v1/files/detail?path=default/p/dt=2026-01-01/manifest.postcard",
    );
    assert_eq!(r.status, 200, "{}", r.text());
    let j = r.json();
    assert_eq!(j["kind"], "manifest");
    assert!(j["manifest"]["generation"].as_u64().unwrap() >= 1);
    assert_eq!(j["manifest"]["rows"], 2);
    assert_eq!(j["manifest"]["partitionValues"]["dt"], "2026-01-01");

    // ⑥ tabledef 详情：内嵌原样 JSON
    let r = get(p, t, "/api/v1/files/detail?path=default/p/table.json");
    assert_eq!(r.status, 200, "{}", r.text());
    assert!(r.json()["tableDef"].is_object(), "tableDef 该内嵌");

    // ⑦ 目录详情：浅层汇总
    let r = get(p, t, "/api/v1/files/detail?path=default/p");
    assert_eq!(r.status, 200);
    let j = r.json();
    assert_eq!(j["type"], "dir");
    assert!(j["summary"]["files"].as_u64().unwrap() >= 1);

    // ⑧ 不存在的目录 → 404（不是 500）
    let r = get(p, t, "/api/v1/files/list?dir=default/nope");
    assert_eq!(r.status, 404, "{}", r.text());
}

/// 删除安全：引用段 409 拒删；孤儿段进回收站；元数据/目录拒删。
#[test]
fn 引用段禁删_孤儿段进回收站() {
    let s = Server::start(&["--token", "t0k"]);
    let (p, t) = (s.port, "t0k");
    seed(p, t);

    let seg = {
        let r = get(p, t, "/api/v1/files/list?dir=default/p/dt=2026-01-01");
        first_seg(&r.json())
    };
    let addr = format!("default/p/dt=2026-01-01/{seg}");

    // ① 引用段 → 409 SEGMENT_REFERENCED，文案指路正确动作
    let r = post(p, t, &format!("/api/v1/files/trash?path={addr}"));
    assert_eq!(r.status, 409, "{}", r.text());
    assert_eq!(r.code(), "SEGMENT_REFERENCED", "{}", r.text());
    let msg = r.text();
    assert!(
        msg.contains("DELETE WHERE") || msg.contains("INSERT OVERWRITE"),
        "{msg}"
    );
    // 文件还在
    let r = get(p, t, &format!("/api/v1/files/detail?path={addr}"));
    assert_eq!(r.status, 200);

    // ② 手工造孤儿（复制引用段）→ list 标 referenced=false → trash 200
    let pdir = s.root.join("default").join("p").join("dt=2026-01-01");
    std::fs::copy(pdir.join(&seg), pdir.join("seg_orphan_test.parquet")).unwrap();
    let r = get(p, t, "/api/v1/files/list?dir=default/p/dt=2026-01-01");
    let orphan = r.json()["entries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["name"] == "seg_orphan_test.parquet")
        .expect("孤儿该在清单里")
        .clone();
    assert_eq!(orphan["segment"]["referenced"], false, "{orphan}");
    let r = post(
        p,
        t,
        "/api/v1/files/trash?path=default/p/dt=2026-01-01/seg_orphan_test.parquet",
    );
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(r.json()["trashed"], true);
    // 原位消失、回收站有货
    let r = get(p, t, "/api/v1/files/list?dir=default/p/dt=2026-01-01");
    let names: Vec<String> = r.json()["entries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["name"].as_str().unwrap().to_string())
        .collect();
    assert!(!names.iter().any(|n| n.contains("orphan")), "{names:?}");
    assert!(s.root.join("_trash").is_dir(), "_trash 该有货");

    // ③ 元数据拒删（manifest / table.json），目录拒删
    for bad in [
        "default/p/dt=2026-01-01/manifest.postcard",
        "default/p/table.json",
        "default/p",
    ] {
        let r = post(p, t, &format!("/api/v1/files/trash?path={bad}"));
        assert_eq!(r.status, 400, "{bad}: {}", r.text());
        assert_eq!(r.code(), "INVALID_ARGUMENT", "{bad}");
    }
    // ④ 不存在 → 404
    let r = post(
        p,
        t,
        "/api/v1/files/trash?path=default/p/dt=2026-01-01/seg_missing.parquet",
    );
    assert_eq!(r.status, 404);

    // ⑤ 数据完好（孤儿误伤零可能）
    let r = sql(p, t, "SELECT count(*) FROM p");
    assert_eq!(r.status, 200, "{}", r.text());
    assert!(r.text().contains("3"), "3 行还在");
}

/// 权限分档：app 写者可浏览/下载 app 文件；摸 default → 403；
/// trash 恒 admin（writer 也 403）；非 admin 列根 = 虚拟视图；无 grant → 403。
#[test]
fn 权限分档_读授权可浏览_删除恒admin() {
    let s = Server::start_mounted("app", &["--token", "t0k"]);
    let (p, t) = (s.port, "t0k");
    seed(p, t);

    // 三个用户：u1=writer@app、u2=reader@app、u3 无任何 grant。
    // ⚠️ CREATE USER 的 ROLE 是**旧格式全局角色**（管所有库）——必须用
    // PUT grants 转成库级模型（role 置 None）；"哪里都不给" 用 `{"*": null}`。
    for (u, pw) in [("u1", "pass1"), ("u2", "pass2"), ("u3", "pass3")] {
        let r = sql(
            p,
            t,
            &format!("CREATE USER {u} WITH PASSWORD '{pw}' ROLE reader"),
        );
        assert_eq!(r.status, 200, "建 {u}: {}", r.text());
    }
    set_grants(p, t, "u1", serde_json::json!({"app": "writer"}));
    set_grants(p, t, "u2", serde_json::json!({"app": "reader"}));
    set_grants(p, t, "u3", serde_json::json!({"*": null}));

    // app 里的数据：DDL 恒 admin（writer 也不能 DDL）→ admin 限定名建表；
    // u1（writer@app）限定名 INSERT——按**目标库**判权，正是 M-B 的路由规则
    assert_eq!(sql(p, t, "CREATE TABLE app.st2 (a BIGINT)").status, 200);
    let t1 = login(p, "u1", "pass1");
    assert_eq!(
        sql(p, &t1, "INSERT INTO app.st2 VALUES (9),(10)").status,
        200,
        "{}",
        sql(p, &t1, "INSERT INTO app.st2 VALUES (9),(10)").text()
    );

    // 段文件地址（app 挂载：`app/default/st2/<分区less>/seg_*.parquet`）
    let r = get(p, &t1, "/api/v1/files/list?dir=app/default/st2");
    assert_eq!(r.status, 200, "app 写者该能浏览自己库: {}", r.text());
    let seg = first_seg(&r.json());

    // ① 虚拟根：u1 只看到 app，看不到 default
    let r = get(p, &t1, "/api/v1/files/list");
    assert_eq!(r.status, 200);
    let j = r.json();
    let names: Vec<&str> = j["entries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["name"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"app"), "{names:?}");
    assert!(
        !names.contains(&"default"),
        "无 default grant 不该看到: {names:?}"
    );

    // ② 摸 default → 403（list / detail / download 全挡）
    let r = get(p, &t1, "/api/v1/files/list?dir=default");
    assert_eq!(r.status, 403, "{}", r.text());
    assert_eq!(r.code(), "FORBIDDEN");
    let r = get(p, &t1, "/api/v1/files/detail?path=default/p/table.json");
    assert_eq!(r.status, 403);
    let r = get(p, &t1, "/api/v1/files?path=default/p/table.json");
    assert_eq!(r.status, 403, "download 也按读授权: {}", r.text());

    // ③ app 面读路径放行：detail / download / 段摘要
    let r = get(
        p,
        &t1,
        &format!("/api/v1/files/detail?path=app/default/st2/{seg}"),
    );
    assert_eq!(r.status, 200, "{}", r.text());
    let j = r.json();
    assert_eq!(j["db"], "app");
    assert_eq!(j["table"], "st2");
    assert_eq!(j["segment"]["referenced"], true);
    assert_eq!(j["segment"]["rows"], 2);
    let r = get(
        p,
        &t1,
        &format!("/api/v1/files?path=app/default/st2/{seg}"),
    );
    assert_eq!(r.status, 200, "app 读者该能下载: {}", r.text());
    assert!(!r.body.is_empty());

    // ④ trash 恒 admin：app 的 writer 也 403
    let r = post(
        p,
        &t1,
        &format!("/api/v1/files/trash?path=app/default/st2/{seg}"),
    );
    assert_eq!(r.status, 403, "{}", r.text());
    assert_eq!(r.code(), "FORBIDDEN");

    // ⑤ 无 grant 用户：app 403、根视图全空
    let t3 = login(p, "u3", "pass3");
    let r = get(p, &t3, "/api/v1/files/list?dir=app");
    assert_eq!(r.status, 403);
    let r = get(p, &t3, "/api/v1/files/list");
    assert_eq!(r.status, 200);
    let j = r.json();
    assert!(j["entries"].as_array().unwrap().is_empty(), "{}", r.text());
    assert!(j["mounts"].as_array().unwrap().is_empty(), "{}", r.text());

    // ⑥ reader（u2）也能浏览 app
    let t2 = login(p, "u2", "pass2");
    let r = get(p, &t2, "/api/v1/files/list?dir=app/default/st2");
    assert_eq!(r.status, 200, "reader 该能浏览: {}", r.text());

    // ⑦ 未认证 → 401
    let r = http(p, "GET", "/api/v1/files/list", &[], b"");
    assert_eq!(r.status, 401);
}
