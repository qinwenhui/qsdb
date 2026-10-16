//! **物理快照**的验收：`qsdb backup` → `qsdb recover` → **库根逐字节相同**。
//!
//! 「物理快照」这个词的分量全在这一条上：恢复出来的必须是**同一个库**，
//! 不是"一份等价的库"。所以判据不是"查出来一样"，而是**盘上逐文件逐字节相同**
//! —— generation / epoch / 段边界 / 磁盘格式版本，全在这个判据里。
//!
//! （逻辑备份那条路比的是**值**，见 `dump_restore.rs`；两条路的判据故意不同，
//! 因为它们承诺的东西不同。）

use std::collections::BTreeMap;
use std::process::Command;

fn tmp(tag: &str) -> std::path::PathBuf {
    let p = std::env::temp_dir().join("qsdb_bak").join(format!(
        "{tag}_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
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

fn sql(path: &std::path::Path, s: &str) -> String {
    ok(&["query", "--path", &path.to_string_lossy(), s])
}

/// 库根下所有文件的 `相对路径 → 字节`。**跳过锁文件**（进程态，不进快照，
/// 恢复后也不会重建 —— 它们按需创建）。
fn snapshot_of(root: &std::path::Path) -> BTreeMap<String, Vec<u8>> {
    let mut out = BTreeMap::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(d) = stack.pop() {
        let Ok(rd) = std::fs::read_dir(&d) else {
            continue;
        };
        for e in rd.flatten() {
            let p = e.path();
            let rel = p
                .strip_prefix(root)
                .unwrap()
                .to_string_lossy()
                .replace('\\', "/");
            if p.is_dir() {
                stack.push(p.clone());
            } else if !rel.ends_with(".lock") {
                out.insert(rel, std::fs::read(&p).unwrap());
            }
        }
    }
    out
}

fn seed(db: &std::path::Path) {
    sql(
        db,
        "CREATE TABLE p (dt DATE, id BIGINT, s STRING) PARTITIONED BY (dt)",
    );
    sql(
        db,
        "INSERT INTO p VALUES (DATE '2026-01-01',1,'a'),(DATE '2026-01-01',2,'b'),\
         (DATE '2026-01-02',3,NULL)",
    );
    sql(db, "CREATE TABLE q (a BIGINT)");
    sql(db, "INSERT INTO q VALUES (7),(8)");
    sql(db, "CREATE VIEW vv AS SELECT a FROM q");
}

/// ★ 核心：恢复出来的库根与源库**逐文件逐字节相同**。
#[test]
fn 物理快照逐字节还原() {
    let src = tmp("src");
    let bak = tmp("bak");
    let dst = tmp("dst");
    let p = |d: &std::path::Path| d.to_string_lossy().into_owned();
    seed(&src);

    ok(&["backup", "--path", &p(&src), "--to", &p(&bak)]);
    assert!(
        bak.join("qsdb_snapshot.json").is_file(),
        "快照该带元信息（否则 recover 认不出来）"
    );

    ok(&["recover", "--path", &p(&dst), "--from", &p(&bak)]);

    // ① 逐字节比（**这就是"物理"的全部含义**）
    let a = snapshot_of(&src);
    let b = snapshot_of(&dst);
    let keys_a: Vec<&String> = a.keys().collect();
    let keys_b: Vec<&String> = b.keys().collect();
    assert_eq!(keys_a, keys_b, "文件清单不一致");
    for k in a.keys() {
        assert!(
            a[k] == b[k],
            "`{k}` 的内容不一致（物理快照要求逐字节相同）：源 {} 字节 / 恢复 {} 字节",
            a[k].len(),
            b[k].len()
        );
    }

    // ② 快照元信息**不该**被留在恢复出来的库里（它不是库的一部分）
    assert!(
        !dst.join("qsdb_snapshot.json").exists(),
        "元信息不该混进恢复出来的库"
    );

    // ③ 数据可查（防止"文件对但库读不出来"）
    let out = sql(&dst, "SELECT dt, id FROM p ORDER BY id");
    assert!(out.contains('a') || out.contains("2026-01-01"), "{out}");

    for d in [src, dst, bak] {
        std::fs::remove_dir_all(d).ok();
    }
}

/// 目标非空 ⇒ 拒（半个旧快照 + 半个新快照，没人看得出）。
#[test]
fn 目标非空要拒() {
    let src = tmp("src2");
    let bak = tmp("bak2");
    let p = |d: &std::path::Path| d.to_string_lossy().into_owned();
    seed(&src);
    std::fs::write(bak.join("junk"), b"x").unwrap();

    let (good, _, err) = run(&["backup", "--path", &p(&src), "--to", &p(&bak)]);
    assert!(!good, "非空目标该被拒");
    assert!(err.contains("已经有东西"), "{err}");

    for d in [src, bak] {
        std::fs::remove_dir_all(d).ok();
    }
}

/// 快照目录在库根里 ⇒ 拒（遍历会把自己吃进去）。
#[test]
fn 快照不能落在库根里() {
    let src = tmp("src3");
    let p = src.to_string_lossy().into_owned();
    seed(&src);
    let inside = src.join("bak");
    let (good, _, err) = run(&["backup", "--path", &p, "--to", &inside.to_string_lossy()]);
    assert!(!good, "该被拒");
    assert!(err.contains("不能在库根里面"), "{err}");
    std::fs::remove_dir_all(&src).ok();
}

/// 从**不是快照**的目录恢复 ⇒ 明确报错并**指路逻辑备份**（这是最容易搞混的一对）。
#[test]
fn 从非快照目录恢复要指路() {
    let dst = tmp("dst4");
    let junk = tmp("junk4");
    let (good, _, err) = run(&[
        "recover",
        "--path",
        &dst.to_string_lossy(),
        "--from",
        &junk.to_string_lossy(),
    ]);
    assert!(!good);
    assert!(
        err.contains("qsdb_snapshot.json") && err.contains("qsdb restore"),
        "该指路到逻辑备份：{err}"
    );
    for d in [dst, junk] {
        std::fs::remove_dir_all(d).ok();
    }
}

/// 恢复目标非空 ⇒ 拒。
#[test]
fn 恢复到非空库要拒() {
    let src = tmp("src5");
    let bak = tmp("bak5");
    let dst = tmp("dst5");
    let p = |d: &std::path::Path| d.to_string_lossy().into_owned();
    seed(&src);
    ok(&["backup", "--path", &p(&src), "--to", &p(&bak)]);
    sql(&dst, "CREATE TABLE already (a BIGINT)");

    let (good, _, err) = run(&["recover", "--path", &p(&dst), "--from", &p(&bak)]);
    assert!(!good);
    assert!(err.contains("已经有东西"), "{err}");
    for d in [src, dst, bak] {
        std::fs::remove_dir_all(d).ok();
    }
}

/// 快照**不带走**回收站与溢出（它们不是活库的一部分）。
#[test]
fn 快照不带回收站与溢出() {
    let src = tmp("src6");
    let bak = tmp("bak6");
    let p = |d: &std::path::Path| d.to_string_lossy().into_owned();
    seed(&src);
    sql(&src, "DROP TABLE q"); // 进 _trash
    std::fs::create_dir_all(src.join("_spill").join("qsdb_spill_x")).unwrap();
    std::fs::write(src.join("_spill/qsdb_spill_x/junk"), b"x").unwrap();

    ok(&["backup", "--path", &p(&src), "--to", &p(&bak)]);
    assert!(!bak.join("_trash").exists(), "回收站不该进快照");
    assert!(!bak.join("_spill").exists(), "溢出目录不该进快照");
    // 但**表还在**（q 已被 DROP，别指望它在）
    assert!(bak.join("default/p/table.json").is_file());

    for d in [src, bak] {
        std::fs::remove_dir_all(d).ok();
    }
}
