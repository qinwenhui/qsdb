//! 用户体系（M7-A）：三角色 + 密码凭据 + `_catalog/users.json` 存储。
//!
//! 设计（减法原则）：
//! - **嵌入模式不管认证**——宿主引擎自己控制访问（现状不变）；
//!   `sql_with_role()` 提供编程式角色闸门供宿主选用
//! - **server 模式**：HTTP Basic → Bearer token（qsdb-cli 层）；
//!   `--token` 保留为超级凭证（Admin 等效，向后兼容）
//! - **零新协议**：用户存储走 catalog 同款锁 + 原子写；密码哈希
//!   SHA-256(password ‖ salt) 迭代（内网部署口径；不是 argon2 但
//!   加盐+迭代足以抗离线字典的 casual 攻击）
//! - **角色不持久化在会话**——每请求校验（token 表内存态）

use std::collections::HashMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::{QsdbError, Result};
use crate::store::Store;

/// 用户角色（enum 派发；无 RBAC 表——v1 定案不做表级 ACL）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Role {
    /// 全能力：读写 + DDL + 运维（compact/gc/用户管理/配置）
    Admin,
    /// 读写：查询 + 写入（INSERT/INSERT OVERWRITE）
    Writer,
    /// 只读：查询 + SHOW/DESCRIBE
    Reader,
}

impl Role {
    pub fn from_name(s: &str) -> Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "admin" => Ok(Self::Admin),
            "writer" => Ok(Self::Writer),
            "reader" => Ok(Self::Reader),
            other => Err(QsdbError::invalid(format!(
                "unknown role '{other}' (admin|writer|reader)"
            ))),
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::Admin => "admin",
            Self::Writer => "writer",
            Self::Reader => "reader",
        }
    }

    /// 写权限（INSERT / WRITE 端点）。
    pub fn can_write(&self) -> bool {
        !matches!(self, Self::Reader)
    }

    /// 管理权限（DDL / compact / gc / 用户管理 / 配置热更）。
    pub fn is_admin(&self) -> bool {
        matches!(self, Self::Admin)
    }
}

/// 用户定义（`_catalog/users.json` 条目）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserDef {
    pub name: String,
    pub role: Role,
    /// 16 字节随机盐
    pub salt: String,
    /// SHA-256(password ‖ salt) 迭代 `ITERATIONS` 次的 hex
    pub hash: String,
}

/// 迭代次数（~5ms/次校验——服务器登录频率下无感知，离线爆破的最低门槛）。
const ITERATIONS: u32 = 10_000;

// ---------------------------------------------------------------------------
// 密码哈希（SHA-256 加盐迭代）
// ---------------------------------------------------------------------------

fn hash_password(password: &str, salt: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(password.as_bytes());
    hasher.update(salt);
    let mut digest = hasher.finalize_reset();
    for _ in 1..ITERATIONS {
        hasher.update(digest);
        hasher.update(salt);
        digest = hasher.finalize_reset();
    }
    let mut out = String::with_capacity(64);
    for b in digest {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

fn random_salt() -> [u8; 16] {
    let mut salt = [0u8; 16];
    let uuid = uuid::Uuid::now_v7();
    salt.copy_from_slice(&uuid.as_bytes()[..16]);
    salt
}

/// 校验密码（常量时间比较）。
fn verify_password(password: &str, salt_hex: &str, hash_hex: &str) -> bool {
    let salt = hex_or_empty(salt_hex);
    let computed = hash_password(password, &salt);
    constant_time_eq(computed.as_bytes(), hash_hex.as_bytes())
}

fn hex_or_empty(s: &str) -> Vec<u8> {
    (0..s.len() / 2)
        .filter_map(|i| u8::from_str_radix(&s[i * 2..i * 2 + 2], 16).ok())
        .collect()
}

fn bytes_to_hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

// ---------------------------------------------------------------------------
// 用户存储
// ---------------------------------------------------------------------------

/// 用户存储（`_catalog/users.json`；catalog 锁内原子写）。
pub struct UserStore;

#[derive(Debug, Serialize, Deserialize, Default)]
struct UsersFile {
    users: Vec<UserDef>,
}

impl UserStore {
    /// 存储路径。
    pub fn path(store: &Store) -> PathBuf {
        store.root().join("_catalog").join("users.json")
    }

    /// 读取全部用户（无文件 = 空集）。
    pub fn read_all(store: &Store) -> Result<HashMap<String, UserDef>> {
        let path = Self::path(store);
        match store.read_to_string(&path)? {
            Some(text) => {
                let f: UsersFile = serde_json::from_str(&text)
                    .map_err(|e| QsdbError::meta(format!("corrupt users.json: {e}")))?;
                Ok(f.users.into_iter().map(|u| (u.name.clone(), u)).collect())
            }
            None => Ok(HashMap::new()),
        }
    }

    /// catalog 锁内写回（整体替换）。
    fn write_all(store: &Store, users: &HashMap<String, UserDef>) -> Result<()> {
        let mut list: Vec<&UserDef> = users.values().collect();
        list.sort_by(|a, b| a.name.cmp(&b.name));
        let f = UsersFile {
            users: list.into_iter().cloned().collect(),
        };
        let text = serde_json::to_string_pretty(&f)
            .map_err(|e| QsdbError::meta(format!("serialize users failed: {e}")))?;
        // **经 with_dir_lock 分派**（F23）：此前无条件走本地 flock，远端库
        // （`--storage-type s3/hdfs`）的 `CREATE USER` 会去 `create_dir_all("s3://…")`
        // 而整体不可用。远端臂 = 进程内互斥 + 对象存储条件写。
        let lock = store.root().join("_catalog").join(".lock");
        crate::fsutil::with_dir_lock(store, &lock, || {
            store.write_atomic(&Self::path(store), text.as_bytes())
        })
    }

    /// 校验用户名密码 → 角色（不匹配/不存在 → None）。
    pub fn verify(store: &Store, name: &str, password: &str) -> Option<Role> {
        let users = Self::read_all(store).ok()?;
        let u = users.get(name)?;
        verify_password(password, &u.salt, &u.hash).then_some(u.role)
    }

    /// 添加用户（已存在报错）。
    pub fn add(store: &Store, name: &str, password: &str, role: Role) -> Result<()> {
        validate_username(name)?;
        validate_password(password)?;
        let mut users = Self::read_all(store)?;
        if users.contains_key(name) {
            return Err(QsdbError::already_exists(format!("user '{name}'")));
        }
        let salt = random_salt();
        users.insert(
            name.to_string(),
            UserDef {
                name: name.to_string(),
                role,
                salt: bytes_to_hex(&salt),
                hash: hash_password(password, &salt),
            },
        );
        Self::write_all(store, &users)
    }

    /// 删除用户。
    pub fn drop_user(store: &Store, name: &str) -> Result<()> {
        let mut users = Self::read_all(store)?;
        if users.remove(name).is_none() {
            return Err(QsdbError::not_found(format!("user '{name}'")));
        }
        Self::write_all(store, &users)
    }

    /// 改密码。
    pub fn set_password(store: &Store, name: &str, password: &str) -> Result<()> {
        validate_password(password)?;
        let mut users = Self::read_all(store)?;
        let u = users
            .get_mut(name)
            .ok_or_else(|| QsdbError::not_found(format!("user '{name}'")))?;
        let salt = random_salt();
        u.salt = bytes_to_hex(&salt);
        u.hash = hash_password(password, &salt);
        Self::write_all(store, &users)
    }

    /// 改角色。
    pub fn set_role(store: &Store, name: &str, role: Role) -> Result<()> {
        let mut users = Self::read_all(store)?;
        let u = users
            .get_mut(name)
            .ok_or_else(|| QsdbError::not_found(format!("user '{name}'")))?;
        u.role = role;
        Self::write_all(store, &users)
    }

    /// 导出用户面（F15）：(用户名, 角色, salt_hex, hash_hex) 序列化——
    /// **不含明文密码**（哈希可直接导入目标库，凭证无缝）。JSON 格式供
    /// 工具编排（qsdb-sync failover 调用 → 目标库 import）。
    pub fn export_json(store: &Store) -> Result<String> {
        let users = Self::read_all(store)?;
        let mut list: Vec<&UserDef> = users.values().collect();
        list.sort_by(|a, b| a.name.cmp(&b.name));
        #[derive(Serialize)]
        struct ExportEntry<'a> {
            name: &'a str,
            role: &'a Role,
            salt: &'a str,
            hash: &'a str,
        }
        let entries: Vec<ExportEntry> = list
            .iter()
            .map(|u| ExportEntry {
                name: &u.name,
                role: &u.role,
                salt: &u.salt,
                hash: &u.hash,
            })
            .collect();
        serde_json::to_string_pretty(&serde_json::json!({ "users": entries }))
            .map_err(|e| QsdbError::meta(format!("export users failed: {e}")))
    }

    /// 导入用户面（F15）：export_json 的输出 → 整体替换或合并。
    /// `overwrite = true` = 整体替换（failover 标准姿势）；false = 只补缺。
    pub fn import_json(store: &Store, json: &str, overwrite: bool) -> Result<usize> {
        #[derive(Deserialize)]
        struct ImportEntry {
            name: String,
            role: Role,
            salt: String,
            hash: String,
        }
        #[derive(Deserialize)]
        struct ImportFile {
            users: Vec<ImportEntry>,
        }
        let f: ImportFile = serde_json::from_str(json)
            .map_err(|e| QsdbError::invalid(format!("import users parse failed: {e}")))?;
        let mut users = if overwrite {
            HashMap::new()
        } else {
            Self::read_all(store)?
        };
        let count = f.users.len();
        for e in f.users {
            validate_username(&e.name)?;
            users.insert(
                e.name.clone(),
                UserDef {
                    name: e.name,
                    role: e.role,
                    salt: e.salt,
                    hash: e.hash,
                },
            );
        }
        Self::write_all(store, &users)?;
        Ok(count)
    }

    /// 用户名清单（SHOW USERS）。
    pub fn list(store: &Store) -> Result<Vec<(String, Role)>> {
        let users = Self::read_all(store)?;
        let mut out: Vec<(String, Role)> =
            users.values().map(|u| (u.name.clone(), u.role)).collect();
        out.sort();
        Ok(out)
    }
}

fn validate_username(name: &str) -> Result<()> {
    let ok = !name.is_empty()
        && name.len() <= 64
        && name
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
    if ok {
        Ok(())
    } else {
        Err(QsdbError::invalid(format!(
            "invalid username '{name}' (expect [A-Za-z_][A-Za-z0-9_]*, <=64 chars)"
        )))
    }
}

fn validate_password(pw: &str) -> Result<()> {
    if pw.len() < 4 {
        return Err(QsdbError::invalid(
            "password too short (minimum 4 characters)",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmpdir(tag: &str) -> PathBuf {
        let d = std::env::temp_dir()
            .join("qsdb_users_test")
            .join(format!("{tag}_{}", uuid::Uuid::now_v7().simple()));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    #[test]
    fn password_hash_roundtrip() {
        let salt = random_salt();
        let hash = hash_password("secret", &salt);
        assert_eq!(hash.len(), 64); // SHA-256 hex
        assert!(verify_password("secret", &bytes_to_hex(&salt), &hash));
        assert!(!verify_password("wrong", &bytes_to_hex(&salt), &hash));
        // 不同盐不同哈希
        let salt2 = random_salt();
        assert_ne!(hash, hash_password("secret", &salt2));
    }

    #[test]
    fn user_crud_and_verify() {
        let dir = tmpdir("crud");
        let store = Store::local(&dir);
        std::fs::create_dir_all(dir.join("_catalog")).unwrap();

        // 空 = 无用户
        assert!(UserStore::read_all(&store).unwrap().is_empty());

        // 添加
        UserStore::add(&store, "alice", "pw123", Role::Admin).unwrap();
        UserStore::add(&store, "bob", "pw456", Role::Reader).unwrap();
        // 重复
        assert!(UserStore::add(&store, "alice", "x", Role::Writer).is_err());

        // 校验
        assert_eq!(
            UserStore::verify(&store, "alice", "pw123"),
            Some(Role::Admin)
        );
        assert_eq!(
            UserStore::verify(&store, "bob", "pw456"),
            Some(Role::Reader)
        );
        assert_eq!(UserStore::verify(&store, "alice", "wrong"), None);
        assert_eq!(UserStore::verify(&store, "carol", "pw123"), None);

        // 改密码
        UserStore::set_password(&store, "alice", "new789").unwrap();
        assert_eq!(UserStore::verify(&store, "alice", "pw123"), None);
        assert_eq!(
            UserStore::verify(&store, "alice", "new789"),
            Some(Role::Admin)
        );

        // 改角色
        UserStore::set_role(&store, "bob", Role::Writer).unwrap();
        assert_eq!(
            UserStore::verify(&store, "bob", "pw456"),
            Some(Role::Writer)
        );

        // 删除
        UserStore::drop_user(&store, "bob").unwrap();
        assert!(UserStore::drop_user(&store, "bob").is_err());
        assert_eq!(UserStore::verify(&store, "bob", "pw456"), None);

        // 清单
        let list = UserStore::list(&store).unwrap();
        assert_eq!(list, vec![("alice".into(), Role::Admin)]);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn role_permissions() {
        assert!(Role::Admin.can_write() && Role::Admin.is_admin());
        assert!(Role::Writer.can_write() && !Role::Writer.is_admin());
        assert!(!Role::Reader.can_write() && !Role::Reader.is_admin());
        assert_eq!(Role::from_name("READER").unwrap(), Role::Reader);
        assert!(Role::from_name("root").is_err());
    }

    #[test]
    fn validation_rules() {
        assert!(validate_username("valid_name").is_ok());
        assert!(validate_username("9bad").is_err());
        assert!(validate_username("has-dot").is_err());
        assert!(validate_username("").is_err());
        assert!(validate_password("abc").is_err());
        assert!(validate_password("abcd").is_ok());
    }
}
