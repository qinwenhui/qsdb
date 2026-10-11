//! HDFS 后端（M6-5，feature `hdfs`，存储设计 §6.3 选型定案：
//! `hdfs-native`（纯 Rust，无 JVM/libhdfs 依赖）+ 其 object_store 适配层）。
//!
//! 能力映射（§6.1 表）：
//! - get/get_range/head/list：native ✓；ETag 由适配层合成（`mtime-size`，
//!   HTTP 惯例）→ [`crate::store::Store::get_with_etag`] 直接可用
//! - put / put_if_absent（`PutMode::Create` = create-no-clobber）native ✓
//! - **put_if_etag（CAS）HDFS 无原生支持**（适配层 NotImplemented）——
//!   本模块用 [`LockingStore`] 包装模拟：锁对象 create-no-clobber 为
//!   互斥点 + 锁下 ETag 双检 + 覆写（§6.1 "put_if_etag ⏳ 模拟"落地）。
//!   锁残留（写者崩溃）按过期时间窃取；OCC 重放语义不受影响。
//!
//! 版本对齐：hdfs-native-object-store 0.17 与本库同用 object_store 0.14
//! （单一版本，无重复依赖；`cargo tree` 验证）。

use std::sync::Arc;

use async_trait::async_trait;
use futures_util::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    Error as OsError, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMultipartOptions, PutMode, PutOptions, PutPayload, PutResult,
    Result as OsResult,
};

/// 锁过期（写者崩溃残留）：超过即窃取。远端 OCC 循环 ≤8 轮 * 网络延迟，
/// 分钟级过期只影响死锁场景的恢复速度。
const LOCK_STALE_MS: i64 = 60_000;
/// 获取锁的重试（每次 50ms 退避）。
const LOCK_RETRY_MS: u64 = 50;
const LOCK_MAX_WAIT_ROUNDS: usize = 400; // ≈20s 上限，交给上层 OCC 重试

/// CAS 模拟包装：委托内层 store，仅 `put_opts(PutMode::Update)` 走锁协议。
///
/// 协议（对 `put_if_etag(path, data, expected)`）：
/// ```text
/// ① head(path)：etag ≠ expected → false（快路径拒绝）
/// ② 锁：put(lock, Create)——已存在且未过期 → 退避重试；过期 → 删锁重试
/// ③ 锁内 head(path) 复核：etag ≠ expected → 弃锁 → false
/// ④ put(path, Overwrite)；⑤ 删锁 → true
/// ```
/// 多写者在 ② 串行；③ 双检保证锁等待期间被抢先的场景不盲写。
pub struct LockingStore {
    /// 客户端直用：put 前建父目录（适配层 put 走 tmp+rename 但不建目录——
    /// S3 无目录概念，HDFS rename 要求父目录存在）
    client: hdfs_native::Client,
    inner: Arc<dyn ObjectStore>,
}

impl LockingStore {
    pub fn new(client: hdfs_native::Client, inner: Arc<dyn ObjectStore>) -> Self {
        Self { client, inner }
    }

    /// 确保父目录存在（幂等 mkdirs；失败由后续 put 报真实错误）。
    async fn ensure_parent(&self, location: &Path) {
        let mut parts: Vec<_> = location.parts().collect();
        parts.pop();
        if parts.is_empty() {
            return;
        }
        let parent: Path = parts.into_iter().collect();
        let abs = format!("/{parent}");
        let _ = self.client.mkdirs(&abs, 0o755, true).await;
    }

    fn lock_path(location: &Path) -> Path {
        Path::parse(format!("{location}.__cas_lock")).expect("valid lock path")
    }

    async fn etag_of(&self, location: &Path) -> OsResult<Option<String>> {
        match self.inner.head(location).await {
            Ok(m) => Ok(m.e_tag),
            Err(OsError::NotFound { .. }) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// 获取锁（返回 true = 持有；false = 应放弃——目标 etag 已变）。
    async fn acquire_lock(&self, location: &Path, expected: Option<&str>) -> OsResult<bool> {
        let lock = Self::lock_path(location);
        for round in 0..LOCK_MAX_WAIT_ROUNDS {
            // 先决条件快检：目标已变则无需争锁
            if self.etag_of(location).await?.as_deref() != expected {
                return Ok(false);
            }
            let create = PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            };
            match self
                .inner
                .put_opts(&lock, PutPayload::default(), create)
                .await
            {
                Ok(_) => {
                    // 拿到锁：复核目标未变（等待期间可能被抢先）
                    if self.etag_of(location).await?.as_deref() != expected {
                        let _ = self.inner.delete(&lock).await;
                        return Ok(false);
                    }
                    return Ok(true);
                }
                Err(OsError::AlreadyExists { .. }) => {
                    // 残留锁：过期则窃取（写者崩溃残留）
                    if let Ok(meta) = self.inner.head(&lock).await {
                        let age_ms =
                            crate::fsutil::now_ms() as i64 - meta.last_modified.timestamp_millis();
                        if age_ms > LOCK_STALE_MS {
                            let _ = self.inner.delete(&lock).await;
                            continue;
                        }
                    }
                }
                Err(e) => return Err(e),
            }
            if round % 4 == 3 {
                tokio::time::sleep(std::time::Duration::from_millis(LOCK_RETRY_MS)).await;
            }
        }
        Err(OsError::Generic {
            store: "LockingStore",
            source: format!("CAS lock contention timeout on {location}").into(),
        })
    }

    async fn cas_put(
        &self,
        location: &Path,
        payload: PutPayload,
        expected: Option<&str>,
    ) -> OsResult<PutResult> {
        if !self.acquire_lock(location, expected).await? {
            return Err(OsError::Precondition {
                path: location.to_string(),
                source: format!("etag mismatch: expected {expected:?}").into(),
            });
        }
        let result = self
            .inner
            .put_opts(
                location,
                payload,
                PutOptions {
                    mode: PutMode::Overwrite,
                    ..Default::default()
                },
            )
            .await;
        let _ = self.inner.delete(&Self::lock_path(location)).await;
        result
    }
}

impl std::fmt::Display for LockingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LockingStore({})", self.inner)
    }
}

impl std::fmt::Debug for LockingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LockingStore").finish()
    }
}

#[async_trait]
impl ObjectStore for LockingStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        self.ensure_parent(location).await;
        match opts.mode {
            PutMode::Update(version) => {
                // Update 的 ETag 凭证来自外层 get_with_etag（合成 etag）；
                // 锁协议负责互斥 + 双检
                let expected = version.e_tag.clone();
                self.cas_put(location, payload, expected.as_deref()).await
            }
            _ => self.inner.put_opts(location, payload, opts).await,
        }
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    fn delete_stream(
        &self,
        locations: futures_util::stream::BoxStream<'static, OsResult<Path>>,
    ) -> futures_util::stream::BoxStream<'static, OsResult<Path>> {
        self.inner.delete_stream(locations)
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        opts: object_store::CopyOptions,
    ) -> OsResult<()> {
        self.inner.copy_opts(from, to, opts).await
    }

    async fn get_opts(&self, location: &Path, opts: GetOptions) -> OsResult<GetResult> {
        self.inner.get_opts(location, opts).await
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, OsResult<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OsResult<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }
}

/// 构建 HDFS 后端句柄（URL 形如 `hdfs://host:9000`；凭据/身份走
/// `HADOOP_USER_NAME` 环境变量与 hdfs-native 配置链）。
pub fn build_hdfs_store(url: &str) -> crate::error::Result<Arc<dyn ObjectStore>> {
    let client = hdfs_native::ClientBuilder::new()
        .with_url(url)
        .build()
        .map_err(|e| {
            crate::error::QsdbError::meta(format!("hdfs client for {url}: {e}"))
        })?;
    Ok(Arc::new(LockingStore::new(
        client.clone(),
        Arc::new(hdfs_native_object_store::HdfsObjectStore::new(client)),
    )))
}
