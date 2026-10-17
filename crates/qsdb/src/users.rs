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

use std::collections::{BTreeMap, HashMap};
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
///
/// 十六批 M-C 起**双轨**：`grants` 缺省（旧格式 / 从未做过库级授权）时 `role`
/// 就是全部授权（逐位兼容）；`grants` 一旦存在即**取代** `role` 成为判定唯一
/// 依据（白名单制——不在表里的库一律无权），`role` 退化为展示残留。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserDef {
    pub name: String,
    /// 全局角色（旧格式字段；`grants` 存在时判定忽略它，仅作展示同步）。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<Role>,
    /// 16 字节随机盐
    pub salt: String,
    /// SHA-256(password ‖ salt) 迭代 `ITERATIONS` 次的 hex
    pub hash: String,
    /// 库级授权（M-C）：键 = 库名或 `"*"`（通配）；值 `None` = **显式无权**。
    /// **白名单制**：键缺失 = 无权——`grants` 存在时不在表里的库一律拒绝。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grants: Option<BTreeMap<String, Option<Role>>>,
}

impl UserDef {
    /// 判定某库的生效角色（**每请求现查的落点**）。`None` = 无权（403）。
    pub fn role_for(&self, db: &str) -> Option<Role> {
        match &self.grants {
            // 旧格式：全局角色管所有库
            None => self.role,
            // 库级精确项优先，其次 "*" 通配；显式 null 与缺键同为无权
            Some(g) => g.get(db).or_else(|| g.get("*")).and_then(|r| *r),
        }
    }

    /// grants 摘要（REST 回显 / 展示）：`{"*":"reader"}`、
    /// `{"default":"writer","settle":null}`。值统一小写角色名 / null。
    pub fn grants_json(&self) -> serde_json::Value {
        match &self.grants {
            None => match self.role {
                Some(r) => serde_json::json!({ "*": r.name() }),
                None => serde_json::json!({}),
            },
            Some(g) => g
                .iter()
                .map(|(k, v)| {
                    (
                        k.clone(),
                        match v {
                            Some(r) => serde_json::json!(r.name()),
                            None => serde_json::Value::Null,
                        },
                    )
                })
                .collect::<serde_json::Map<String, serde_json::Value>>()
                .into(),
        }
    }
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

    /// 校验用户名密码 → 用户定义（M-C：判定走 [`UserDef::role_for`]，不再只回
    /// 全局角色——同一个用户在不同库可以不同角色）。
    pub fn verify_def(store: &Store, name: &str, password: &str) -> Option<UserDef> {
        let users = Self::read_all(store).ok()?;
        let u = users.get(name)?;
        verify_password(password, &u.salt, &u.hash).then(|| u.clone())
    }

    /// 校验密码 → 全局角色（兼容壳：等于 `"*"` 槽位；白名单制用户可能为 None）。
    pub fn verify(store: &Store, name: &str, password: &str) -> Option<Role> {
        Self::verify_def(store, name, password).and_then(|u| u.role)
    }

    /// 添加用户（已存在报错）。全局角色形态 = 旧格式（`role` 字段），
    /// 逐位兼容：判定上等价于 `grants = {"*": role}`。
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
                role: Some(role),
                salt: bytes_to_hex(&salt),
                hash: hash_password(password, &salt),
                grants: None,
            },
        );
        Self::write_all(store, &users)
    }

    /// 添加用户（grants 形态，M-C）：CLI `user add --grant` 的落点。
    /// 映射可含 `"*"`；**不含** = 白名单制（未列出的库一律无权）。
    pub fn add_with_grants(
        store: &Store,
        name: &str,
        password: &str,
        mut grants: BTreeMap<String, Option<Role>>,
    ) -> Result<()> {
        validate_username(name)?;
        validate_password(password)?;
        if grants.is_empty() {
            return Err(QsdbError::invalid(
                "empty grants (give at least one --grant, or use add() for a global role)",
            ));
        }
        let mut users = Self::read_all(store)?;
        if users.contains_key(name) {
            return Err(QsdbError::already_exists(format!("user '{name}'")));
        }
        let salt = random_salt();
        users.insert(
            name.to_string(),
            UserDef {
                name: name.to_string(),
                role: None,
                salt: bytes_to_hex(&salt),
                hash: hash_password(password, &salt),
                grants: Some(std::mem::take(&mut grants)),
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

    /// 改角色 = 写 `"*"` 槽位（M-C）：无 grants 的用户照旧写 `role` 字段；
    /// 有 grants 的用户覆盖其 `"*"` 授权（库级精确项原样保留）。
    pub fn set_role(store: &Store, name: &str, role: Role) -> Result<()> {
        let mut users = Self::read_all(store)?;
        let u = users
            .get_mut(name)
            .ok_or_else(|| QsdbError::not_found(format!("user '{name}'")))?;
        match &mut u.grants {
            None => u.role = Some(role),
            Some(g) => {
                g.insert("*".to_string(), Some(role));
                u.role = Some(role); // 展示残留同步（判定不看它）
            }
        }
        Self::write_all(store, &users)
    }

    /// 整体替换某用户的库级授权（M-C；REST `PUT /users/{name}/grants` 的落点）。
    /// PUT 语义 = 全量替换：想"全部拒绝"给 `{"*": null}`，不是空表。
    pub fn set_grants(
        store: &Store,
        name: &str,
        grants: BTreeMap<String, Option<Role>>,
    ) -> Result<()> {
        if grants.is_empty() {
            return Err(QsdbError::invalid(
                "empty grants (give {\"*\": null} to deny everything, not an empty map)",
            ));
        }
        let mut users = Self::read_all(store)?;
        let u = users
            .get_mut(name)
            .ok_or_else(|| QsdbError::not_found(format!("user '{name}'")))?;
        u.grants = Some(grants);
        Self::write_all(store, &users)
    }

    /// 导出用户面（F15）：(用户名, 角色, salt_hex, hash_hex, grants?) 序列化——
    /// **不含明文密码**（哈希可直接导入目标库，凭证无缝）。M-C 起 grants 随行
    /// （旧格式用户导出体保持旧形状，`role`/`grants` 均按缺省跳过）。
    pub fn export_json(store: &Store) -> Result<String> {
        let users = Self::read_all(store)?;
        let mut list: Vec<&UserDef> = users.values().collect();
        list.sort_by(|a, b| a.name.cmp(&b.name));
        #[derive(Serialize)]
        struct ExportEntry<'a> {
            name: &'a str,
            #[serde(skip_serializing_if = "Option::is_none")]
            role: &'a Option<Role>,
            salt: &'a str,
            hash: &'a str,
            #[serde(skip_serializing_if = "Option::is_none")]
            grants: &'a Option<BTreeMap<String, Option<Role>>>,
        }
        let entries: Vec<ExportEntry> = list
            .iter()
            .map(|u| ExportEntry {
                name: &u.name,
                role: &u.role,
                salt: &u.salt,
                hash: &u.hash,
                grants: &u.grants,
            })
            .collect();
        serde_json::to_string_pretty(&serde_json::json!({ "users": entries }))
            .map_err(|e| QsdbError::meta(format!("export users failed: {e}")))
    }

    /// 导入用户面（F15）：export_json 的输出 → 整体替换或合并。
    /// `overwrite = true` = 整体替换（failover 标准姿势）；false = 只补缺。
    /// M-C 起同时认 grants；`role` 与 `grants` 至少要有一个。
    pub fn import_json(store: &Store, json: &str, overwrite: bool) -> Result<usize> {
        #[derive(Deserialize)]
        struct ImportEntry {
            name: String,
            #[serde(default)]
            role: Option<Role>,
            salt: String,
            hash: String,
            #[serde(default)]
            grants: Option<BTreeMap<String, Option<Role>>>,
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
            if e.role.is_none() && e.grants.is_none() {
                return Err(QsdbError::invalid(format!(
                    "user entry '{}' needs a role or grants",
                    e.name
                )));
            }
            users.insert(
                e.name.clone(),
                UserDef {
                    name: e.name,
                    role: e.role,
                    salt: e.salt,
                    hash: e.hash,
                    grants: e.grants,
                },
            );
        }
        Self::write_all(store, &users)?;
        Ok(count)
    }

    /// 用户清单（SHOW USERS / CLI `user list`），按名字排序。
    pub fn list(store: &Store) -> Result<Vec<UserDef>> {
        let mut out: Vec<UserDef> = Self::read_all(store)?.into_values().collect();
        out.sort_by(|a, b| a.name.cmp(&b.name));
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
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].name, "alice");
        assert_eq!(list[0].role, Some(Role::Admin));
        std::fs::remove_dir_all(&dir).ok();
    }

    /// grants 判定矩阵（M-C 核心）：旧格式回落 / 精确项优先 / 通配 /
    /// 显式 null / **白名单制缺键 = 无权**。
    #[test]
    fn grants_role_for_matrix() {
        // 旧格式（无 grants）：全局角色管所有库
        let legacy = UserDef {
            name: "old".into(),
            role: Some(Role::Reader),
            salt: String::new(),
            hash: String::new(),
            grants: None,
        };
        assert_eq!(legacy.role_for("default"), Some(Role::Reader));
        assert_eq!(legacy.role_for("settle"), Some(Role::Reader));
        // 旧格式但 role 字段缺失（不该出现，防御性）：一律无权
        let empty = UserDef {
            role: None,
            grants: None,
            ..legacy.clone()
        };
        assert_eq!(empty.role_for("default"), None);

        // 白名单制：只授 settle，其它库（含未挂载名）一律无权
        let wl = UserDef {
            name: "wl".into(),
            role: None,
            salt: String::new(),
            hash: String::new(),
            grants: Some(BTreeMap::from([("settle".to_string(), Some(Role::Writer))])),
        };
        assert_eq!(wl.role_for("settle"), Some(Role::Writer));
        assert_eq!(wl.role_for("default"), None);
        assert_eq!(wl.role_for("*"), None, "缺 '*' 键 = 通配也无权");

        // "*" 通配 + 库级精确项优先；显式 null = 拒绝该库
        let mixed = UserDef {
            name: "mix".into(),
            role: None,
            salt: String::new(),
            hash: String::new(),
            grants: Some(BTreeMap::from([
                ("settle".to_string(), Some(Role::Reader)),
                ("sales".to_string(), None),
                ("*".to_string(), Some(Role::Writer)),
            ])),
        };
        assert_eq!(mixed.role_for("settle"), Some(Role::Reader), "精确项优先");
        assert_eq!(mixed.role_for("sales"), None, "显式 null = 拒绝");
        assert_eq!(mixed.role_for("other"), Some(Role::Writer), "通配兜底");
        // 全拒：{"*": null}
        let deny_all = UserDef {
            grants: Some(BTreeMap::from([("*".to_string(), None)])),
            ..mixed
        };
        assert_eq!(deny_all.role_for("anything"), None);
    }

    /// grants 管理面：add_with_grants / set_grants / set_role（"*" 槽位同步）
    /// + 每请求现查的持久化（写盘即生效）。
    #[test]
    fn grants_store_roundtrip() {
        let dir = tmpdir("grants");
        let store = Store::local(&dir);
        std::fs::create_dir_all(dir.join("_catalog")).unwrap();

        // add_with_grants：白名单制（只有 settle）
        let mut g = BTreeMap::new();
        g.insert("settle".to_string(), Some(Role::Writer));
        UserStore::add_with_grants(&store, "op", "pw123", g).unwrap();
        let def = UserStore::verify_def(&store, "op", "pw123").unwrap();
        assert_eq!(def.role_for("settle"), Some(Role::Writer));
        assert_eq!(def.role_for("default"), None, "白名单制：未列出 = 无权");
        assert!(
            UserStore::add_with_grants(&store, "x2", "pw123", BTreeMap::new()).is_err(),
            "空 grants 显式拒绝"
        );

        // set_grants：整体替换（PUT 语义）——settle 收权、通配给 reader
        let mut g2 = BTreeMap::new();
        g2.insert("*".to_string(), Some(Role::Reader));
        UserStore::set_grants(&store, "op", g2.clone()).unwrap();
        let def = UserStore::verify_def(&store, "op", "pw123").unwrap();
        assert_eq!(def.role_for("settle"), Some(Role::Reader), "通配生效");
        assert_eq!(def.role_for("default"), Some(Role::Reader));
        assert!(UserStore::set_grants(&store, "ghost", g2).is_err());

        // set_role：写 "*" 槽位（writer），库级精确项保留
        let mut g3 = BTreeMap::new();
        g3.insert("settle".to_string(), Some(Role::Reader));
        g3.insert("*".to_string(), None);
        UserStore::add_with_grants(&store, "mix", "pw123", g3).unwrap();
        UserStore::set_role(&store, "mix", Role::Writer).unwrap();
        let def = UserStore::verify_def(&store, "mix", "pw123").unwrap();
        assert_eq!(
            def.role_for("settle"),
            Some(Role::Reader),
            "精确项不被 set_role 覆盖"
        );
        assert_eq!(
            def.role_for("default"),
            Some(Role::Writer),
            "set_role 写入通配"
        );
        // grants 用户密码正确但无全局角色的 verify() 兼容壳
        assert_eq!(
            UserStore::verify(&store, "mix", "pw123"),
            Some(Role::Writer)
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// export/import 往返：grants 随行（F15 承诺的 failover 无缝延伸到 M-C）。
    #[test]
    fn grants_survive_export_import() {
        let dir = tmpdir("gexp");
        let store = Store::local(&dir);
        std::fs::create_dir_all(dir.join("_catalog")).unwrap();
        UserStore::add(&store, "legacy", "pw123", Role::Admin).unwrap();
        let g = BTreeMap::from([
            ("settle".to_string(), Some(Role::Reader)),
            ("*".to_string(), None),
        ]);
        UserStore::add_with_grants(&store, "op", "pw456", g).unwrap();

        let json = UserStore::export_json(&store).unwrap();
        assert!(json.contains("\"grants\""), "grants 要进导出体");
        assert!(!json.contains("pw123") && !json.contains("pw456"));

        // 目标库导入（overwrite）
        let dir2 = tmpdir("gimp");
        let store2 = Store::local(&dir2);
        std::fs::create_dir_all(dir2.join("_catalog")).unwrap();
        let n = UserStore::import_json(&store2, &json, true).unwrap();
        assert_eq!(n, 2);
        let op = UserStore::verify_def(&store2, "op", "pw456").unwrap();
        assert_eq!(op.role_for("settle"), Some(Role::Reader));
        assert_eq!(op.role_for("default"), None, "白名单制跨库保真");
        assert_eq!(
            UserStore::verify(&store2, "legacy", "pw123"),
            Some(Role::Admin),
            "旧格式用户往返不变"
        );

        // role 与 grants 都没有的条目 → 显式拒绝（不产出"幽灵用户"）
        let bad = r#"{"users":[{"name":"ghosty","salt":"aa","hash":"bb"}]}"#;
        assert!(UserStore::import_json(&store2, bad, true).is_err());
        std::fs::remove_dir_all(&dir).ok();
        std::fs::remove_dir_all(&dir2).ok();
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
