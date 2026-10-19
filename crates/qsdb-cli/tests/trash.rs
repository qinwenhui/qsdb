//! **误删能捞回来**：`DROP TABLE` → `admin trash-list` → `admin trash-restore`。
//!
//! 这条是"备份"之外**最常真正用上**的那一层 —— 复制防机器故障，**这一层防人删错**。
//! 而在此之前，删掉的东西虽然在 `_trash` 里躺着，**用户没有任何命令能拿回来**
//! （只有 `gc` 会真删）。

use std::process::Command;

/// 测试目录唯一序号：pid 挡跨进程碰撞，序号挡同进程内并发（Windows 时钟
/// 粒度 ~100ns，纳秒后缀在同 tick 内会重复——CI 上真炸过，见 constraints）。
fn _uniq_seq() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    SEQ.fetch_add(1, Ordering::Relaxed)
}

fn tmp(tag: &str) -> std::path::PathBuf {
    let p = std::env::temp_dir().join("qsdb_trash").join(format!(
        "{tag}_{}_{}",
        std::process::id(),
        format!("{:x}_{}", std::process::id(), _uniq_seq())
    ));
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn run(args: &[&str]) -> (bool, String, String) {
    let out = Command::new(env!("CARGO_BIN_EXE_qsdb"))
        .args(args)
        .output()
        .expect("spawn qsdb");
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

fn ok(args: &[&str]) -> String {
    let (good, o, e) = run(args);
    assert!(good, "qsdb {args:?} 失败\nout: {o}\nerr: {e}");
    o
}

/// 从 `trash-list` 的输出里取第一条的 ts。
fn first_ts(out: &str) -> u64 {
    out.lines()
        .skip(1) // 表头
        .find_map(|l| l.split_whitespace().next()?.parse::<u64>().ok())
        .unwrap_or_else(|| panic!("trash-list 里没有条目：\n{out}"))
}

#[test]
fn 删掉的表能捞回来() {
    let db = tmp("db");
    let p = db.to_string_lossy().into_owned();
    ok(&[
        "query",
        "--path",
        &p,
        "CREATE TABLE t (id BIGINT, s STRING)",
    ]);
    ok(&[
        "query",
        "--path",
        &p,
        "INSERT INTO t VALUES (1,'a'),(2,'b')",
    ]);
    ok(&["query", "--path", &p, "DROP TABLE t"]);

    // ① 列出来：能认出"原来在哪"
    let list = ok(&["admin", "--path", &p, "trash-list"]);
    assert!(list.contains("default/t"), "该认出原路径：\n{list}");
    let ts = first_ts(&list);

    // ② 捞回来
    let out = ok(&["admin", "--path", &p, "trash-restore", &ts.to_string()]);
    assert!(out.contains("已恢复"), "{out}");

    // ③ **数据也在**（不只是目录回来了）
    let rows = ok(&["query", "--path", &p, "SELECT id, s FROM t ORDER BY id"]);
    assert!(
        rows.contains("1") && rows.contains('a'),
        "数据该还在：\n{rows}"
    );

    // ④ 捞完之后回收站该空了
    let after = ok(&["admin", "--path", &p, "trash-list"]);
    assert!(after.contains("回收站是空的"), "{after}");

    std::fs::remove_dir_all(&db).ok();
}

/// 恢复目标已存在 ⇒ **拒**（删掉之后又建了同名的 —— 覆盖它等于用一次撤销毁另一份数据）。
#[test]
fn 同名已存在时拒绝恢复() {
    let db = tmp("db2");
    let p = db.to_string_lossy().into_owned();
    ok(&["query", "--path", &p, "CREATE TABLE t (id BIGINT)"]);
    ok(&["query", "--path", &p, "DROP TABLE t"]);
    let ts = first_ts(&ok(&["admin", "--path", &p, "trash-list"]));
    // 又建了同名的
    ok(&["query", "--path", &p, "CREATE TABLE t (id BIGINT)"]);

    let (good, _, err) = run(&["admin", "--path", &p, "trash-restore", &ts.to_string()]);
    assert!(!good, "该被拒");
    assert!(err.contains("已经存在"), "{err}");
    std::fs::remove_dir_all(&db).ok();
}

/// **旧遗留**（加来源标记之前删的）⇒ 列表里标出来、恢复明确报错 —— **不猜**。
#[test]
fn 没有来源标记的旧条目要如实说() {
    let db = tmp("db3");
    let p = db.to_string_lossy().into_owned();
    ok(&["query", "--path", &p, "CREATE TABLE t (id BIGINT)"]);
    // 手工造一个"旧格式"条目（有数据、没标记）
    let old = db.join("_trash").join("1").join("default").join("t");
    std::fs::create_dir_all(&old).unwrap();
    std::fs::write(old.join("whatever"), b"x").unwrap();

    let list = ok(&["admin", "--path", &p, "trash-list"]);
    assert!(list.contains("认不出来"), "旧的该如实标出：\n{list}");

    let (good, _, err) = run(&["admin", "--path", &p, "trash-restore", "1"]);
    assert!(!good, "认不出来就不许猜着恢复");
    assert!(err.contains("来源标记"), "{err}");
    std::fs::remove_dir_all(&db).ok();
}

/// ★ **真删是 `admin gc`** —— 这条钉住"回收站会不会被清掉"。
/// 它也是 `trash-list` 输出里那句警告的依据。
#[test]
fn gc_才是真删() {
    let db = tmp("db4");
    let p = db.to_string_lossy().into_owned();
    ok(&["query", "--path", &p, "CREATE TABLE t (id BIGINT)"]);
    ok(&["query", "--path", &p, "DROP TABLE t"]);
    assert!(ok(&["admin", "--path", &p, "trash-list"]).contains("default/t"));

    // ⚠️ 默认静默期 10 分钟 ⇒ 刚删的**不会**被 gc 清掉（安全期内的保护）。
    // 这正是"要当误删保险得把 grace 调大"的由来：gc 跑得勤 + grace 小 = 捞不回来。
    let out = ok(&["admin", "--path", &p, "gc"]);
    assert!(
        out.contains("0 entries"),
        "安全期内的条目不该被 gc 清掉：{out}"
    );
    assert!(
        ok(&["admin", "--path", &p, "trash-list"]).contains("default/t"),
        "gc 之后条目该还在"
    );
    std::fs::remove_dir_all(&db).ok();
}
