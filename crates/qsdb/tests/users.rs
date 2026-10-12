//! M7-A 用户体系 e2e：SQL 管理 + 角色矩阵 + 密码校验。

use qsdb::users::{Role, UserStore};
use qsdb::{Qsdb, QsdbOptions};

fn setup(tag: &str) -> (Qsdb, std::path::PathBuf) {
    let root = std::env::temp_dir().join("qsdb_m7_users").join(format!(
        "{tag}_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let db = Qsdb::open(&root, QsdbOptions::default()).unwrap();
    (db, root)
}

#[test]
fn sql_user_management() {
    let (db, root) = setup("sql");
    // 建表备用（测试角色闸门）
    db.sql("CREATE TABLE t (v BIGINT)").unwrap();
    db.sql("INSERT INTO t VALUES (1), (2)").unwrap();

    // CREATE USER
    db.sql("CREATE USER alice WITH PASSWORD 'pass123' ROLE admin")
        .unwrap();
    db.sql("CREATE USER bob WITH PASSWORD 'pass456' ROLE reader")
        .unwrap();
    db.sql("CREATE USER carol WITH PASSWORD 'pass789'").unwrap(); // 默认 writer

    // 重复拒绝
    assert!(db.sql("CREATE USER alice WITH PASSWORD 'x1234'").is_err());

    // SHOW USERS
    let batches = db.sql_collect("SHOW USERS").unwrap();
    let users: Vec<String> = batches
        .iter()
        .flat_map(|b| {
            (0..b.num_rows()).map(|i| {
                let n =
                    arrow::util::display::array_value_to_string(b.column(0).as_ref(), i).unwrap();
                let r =
                    arrow::util::display::array_value_to_string(b.column(1).as_ref(), i).unwrap();
                format!("{n}|{r}")
            })
        })
        .collect();
    assert!(users.contains(&"alice|admin".to_string()));
    assert!(users.contains(&"bob|reader".to_string()));
    assert!(users.contains(&"carol|writer".to_string()));

    // 校验
    assert_eq!(
        UserStore::verify(db.store(), "alice", "pass123"),
        Some(Role::Admin)
    );
    assert_eq!(
        UserStore::verify(db.store(), "bob", "pass456"),
        Some(Role::Reader)
    );
    assert_eq!(
        UserStore::verify(db.store(), "carol", "pass789"),
        Some(Role::Writer)
    );

    // ALTER USER SET PASSWORD / SET ROLE
    db.sql("ALTER USER bob SET PASSWORD 'new789'").unwrap();
    assert_eq!(UserStore::verify(db.store(), "bob", "pass456"), None);
    assert_eq!(
        UserStore::verify(db.store(), "bob", "new789"),
        Some(Role::Reader)
    );
    db.sql("ALTER USER bob SET ROLE writer").unwrap();
    assert_eq!(
        UserStore::verify(db.store(), "bob", "new789"),
        Some(Role::Writer)
    );

    // DROP USER
    db.sql("DROP USER carol").unwrap();
    assert!(db.sql("DROP USER carol").is_err());
    db.sql("DROP USER IF EXISTS carol").unwrap(); // 幂等
    assert_eq!(UserStore::verify(db.store(), "carol", "pass789"), None);
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn role_matrix_gates() {
    let (db, root) = setup("roles");
    db.sql("CREATE TABLE t (v BIGINT)").unwrap();
    UserStore::add(db.store(), "r", "pass1", Role::Reader).unwrap();
    UserStore::add(db.store(), "w", "pass2", Role::Writer).unwrap();
    UserStore::add(db.store(), "a", "pass3", Role::Admin).unwrap();

    // Reader：SELECT/SHOW ✓；INSERT/DDL ✗
    assert!(
        db.sql_with_role("SELECT count(*) FROM t", Role::Reader)
            .is_ok()
    );
    assert!(db.sql_with_role("SHOW USERS", Role::Reader).is_ok());
    assert!(
        db.sql_with_role("INSERT INTO t VALUES (1)", Role::Reader)
            .is_err()
    );
    assert!(
        db.sql_with_role("CREATE TABLE x (v BIGINT)", Role::Reader)
            .is_err()
    );
    assert!(db.sql_with_role("DROP TABLE t", Role::Reader).is_err());

    // Writer：SELECT/INSERT ✓；DDL ✗
    assert!(
        db.sql_with_role("SELECT count(*) FROM t", Role::Writer)
            .is_ok()
    );
    assert!(
        db.sql_with_role("INSERT INTO t VALUES (1)", Role::Writer)
            .is_ok()
    );
    assert!(
        db.sql_with_role("CREATE TABLE x (v BIGINT)", Role::Writer)
            .is_err()
    );
    assert!(
        db.sql_with_role("CREATE USER u1 WITH PASSWORD 'p1234'", Role::Writer)
            .is_err()
    );

    // Admin：全 ✓
    assert!(
        db.sql_with_role("SELECT count(*) FROM t", Role::Admin)
            .is_ok()
    );
    assert!(
        db.sql_with_role("INSERT INTO t VALUES (1)", Role::Admin)
            .is_ok()
    );
    assert!(
        db.sql_with_role("CREATE TABLE x (v BIGINT)", Role::Admin)
            .is_ok()
    );
    assert!(
        db.sql_with_role("CREATE USER u1 WITH PASSWORD 'p1234'", Role::Admin)
            .is_ok()
    );
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn password_security_basics() {
    let (db, root) = setup("pwsec");
    UserStore::add(db.store(), "u", "secret", Role::Writer).unwrap();
    // 错误密码
    assert_eq!(UserStore::verify(db.store(), "u", "wrong"), None);
    // 不存在用户
    assert_eq!(UserStore::verify(db.store(), "ghost", "secret"), None);
    // 太短密码拒绝
    assert!(UserStore::add(db.store(), "v", "abc", Role::Reader).is_err());
    // 非法用户名拒绝
    assert!(UserStore::add(db.store(), "9bad", "abcd", Role::Reader).is_err());
    assert!(UserStore::add(db.store(), "has.dot", "abcd", Role::Reader).is_err());
    // 密码大小写敏感
    UserStore::add(db.store(), "case", "PassWord", Role::Reader).unwrap();
    assert_eq!(UserStore::verify(db.store(), "case", "password"), None);
    assert_eq!(
        UserStore::verify(db.store(), "case", "PassWord"),
        Some(Role::Reader)
    );
    std::fs::remove_dir_all(&root).ok();
}

#[test]
fn f15_user_export_import() {
    let (db, root) = setup("f15");
    UserStore::add(db.store(), "alice", "pass123", Role::Admin).unwrap();
    UserStore::add(db.store(), "bob", "pass456", Role::Reader).unwrap();

    // 导出（JSON；含哈希不含明文；Role 按 serde 枚举变体名序列化为 "Admin" 等）
    let json = UserStore::export_json(db.store()).unwrap();
    assert!(json.contains("\"alice\""));
    assert!(json.contains("Admin") || json.contains("admin"));
    assert!(!json.contains("pass123")); // 不含明文密码
    assert!(json.contains("\"hash\"")); // 含哈希

    // 目标库导入（failover 模拟）
    let root2 = root.with_extension("target");
    std::fs::create_dir_all(root2.join("_catalog")).unwrap();
    let db2 = Qsdb::open(&root2, QsdbOptions::default()).unwrap();

    // merge 模式：只补缺
    UserStore::add(db2.store(), "carol", "pass789", Role::Writer).unwrap();
    let n = UserStore::import_json(db2.store(), &json, false).unwrap();
    assert_eq!(n, 2); // alice + bob
    // carol 保留
    assert_eq!(
        UserStore::verify(db2.store(), "carol", "pass789"),
        Some(Role::Writer)
    );
    // alice 凭证无缝
    assert_eq!(
        UserStore::verify(db2.store(), "alice", "pass123"),
        Some(Role::Admin)
    );

    // overwrite 模式：整体替换
    let n = UserStore::import_json(db2.store(), &json, true).unwrap();
    assert_eq!(n, 2);
    // carol 被清（不在导出集）
    assert_eq!(UserStore::verify(db2.store(), "carol", "pass789"), None);
    assert_eq!(
        UserStore::verify(db2.store(), "bob", "pass456"),
        Some(Role::Reader)
    );

    // 非法 JSON 拒绝
    assert!(UserStore::import_json(db2.store(), "not json", true).is_err());
    std::fs::remove_dir_all(&root).ok();
    std::fs::remove_dir_all(&root2).ok();
}
