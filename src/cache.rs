//! In-memory cache + SQLite-backed distributed lock module.
//!
//! This module is the Redis replacement for the broker. It exposes the two
//! primitives the CIS stack used Redis for:
//!
//! * [`CacheStore`] — a binary-safe string KV store with TTL and atomic
//!   increment, kept in a process-local `HashMap` so reads and writes never
//!   touch SQLite. An LRU cap bounds memory growth.
//! * [`LockStore`] — a distributed lock with owner token, expiry and renewal,
//!   persisted in the `sys_lock` table. Locks stay durable so a broker restart
//!   cannot silently drop a lease mid-job.
//!
//! Semantics mirror Redis where it matters (`TTL` returns `-2` for a missing
//! key and `-1` for a key without expiry; `SET NX` is the acquire primitive).

use std::collections::{hash_map::Entry, HashMap};
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Duration, Utc};
use sqlx::Row;
use tonic::{Request, Response, Status};

use crate::db::Database;
use crate::proto::{
    cache_service_server::CacheService as CacheServiceTrait, CacheDeleteRequest,
    CacheDeleteResponse, CacheExpireRequest, CacheExpireResponse, CacheGetRequest,
    CacheGetResponse, CacheIncrRequest, CacheIncrResponse, CacheSetRequest, CacheSetResponse,
    CacheTtlRequest, CacheTtlResponse, LockAcquireRequest, LockAcquireResponse, LockReleaseRequest,
    LockReleaseResponse, LockRenewRequest, LockRenewResponse, LockIsLockedRequest, LockIsLockedResponse,
};

const TYPE_STRING: &str = "string";
const TYPE_LONG: &str = "long";

/// Default cache capacity when not overridden. 100k small entries (tokens,
/// counters, captcha) are a few MiB at most, far below typical broker RAM.
pub const DEFAULT_CACHE_MAX_ENTRIES: usize = 100_000;

fn now_rfc3339() -> String {
    Utc::now().to_rfc3339()
}

fn expire_at_utc(ttl_secs: i64) -> Option<DateTime<Utc>> {
    if ttl_secs <= 0 {
        None
    } else {
        Some(Utc::now() + Duration::seconds(ttl_secs))
    }
}

fn parse_rfc3339(value: &str) -> Option<DateTime<Utc>> {
    chrono::DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|value| value.with_timezone(&Utc))
}

fn normalize_value_type(value_type: &str) -> &str {
    let value_type = value_type.trim();
    if value_type.is_empty() {
        TYPE_STRING
    } else {
        value_type
    }
}

// ============================== CacheStore ==============================

#[derive(Clone)]
struct CacheEntry {
    value: Vec<u8>,
    value_type: String,
    expire_at: Option<DateTime<Utc>>,
    last_accessed: DateTime<Utc>,
}

impl CacheEntry {
    fn new(value: Vec<u8>, value_type: String, expire_at: Option<DateTime<Utc>>) -> Self {
        Self {
            value,
            value_type,
            expire_at,
            last_accessed: Utc::now(),
        }
    }

    fn is_expired(&self, now: DateTime<Utc>) -> bool {
        self.expire_at.is_some_and(|deadline| deadline <= now)
    }
}

#[derive(Clone)]
pub struct CacheStore {
    inner: Arc<Mutex<HashMap<String, CacheEntry>>>,
    max_entries: usize,
}

impl CacheStore {
    pub fn new(max_entries: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            max_entries: max_entries.max(1),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, CacheEntry>> {
        self.inner.lock().expect("cache lock poisoned")
    }

    /// Store `value` under `key` with an optional type hint. `ttl_secs <= 0` means no expiry.
    pub async fn set(
        &self,
        key: &str,
        value: &[u8],
        value_type: &str,
        ttl_secs: i64,
    ) -> anyhow::Result<()> {
        let entry = CacheEntry::new(
            value.to_vec(),
            normalize_value_type(value_type).to_string(),
            expire_at_utc(ttl_secs),
        );
        let mut map = self.lock();
        map.insert(key.to_string(), entry);
        if map.len() > self.max_entries {
            let now = Utc::now();
            map.retain(|_, entry| !entry.is_expired(now));
            while map.len() > self.max_entries {
                let lru = map
                    .iter()
                    .min_by_key(|(_, entry)| entry.last_accessed)
                    .map(|(key, _)| key.clone());
                let Some(lru) = lru else { break };
                map.remove(&lru);
            }
        }
        Ok(())
    }

    /// Read a value and its type hint, lazily deleting expired entries.
    pub async fn get(&self, key: &str) -> anyhow::Result<Option<(Vec<u8>, String)>> {
        let mut map = self.lock();
        let now = Utc::now();
        if map.get(key).is_some_and(|entry| entry.is_expired(now)) {
            map.remove(key);
            return Ok(None);
        }
        let Some(entry) = map.get_mut(key) else {
            return Ok(None);
        };
        entry.last_accessed = now;
        Ok(Some((entry.value.clone(), entry.value_type.clone())))
    }

    pub async fn delete(&self, key: &str) -> anyhow::Result<bool> {
        Ok(self.lock().remove(key).is_some())
    }

    /// Atomically add `delta` to the value of `key`, creating it with `ttl_secs`
    /// (applied only on creation) when it does not exist.
    pub async fn incr(&self, key: &str, delta: i64, ttl_secs: i64) -> anyhow::Result<i64> {
        let mut map = self.lock();
        let now = Utc::now();
        if map.get(key).is_some_and(|entry| entry.is_expired(now)) {
            map.remove(key);
        }
        match map.entry(key.to_string()) {
            Entry::Occupied(mut slot) => {
                let current = parse_i64(&slot.get().value).ok_or_else(|| {
                    anyhow::anyhow!("cache value is not a number for key {key}")
                })?;
                let next = current + delta;
                slot.get_mut().value = next.to_string().into_bytes();
                slot.get_mut().value_type = TYPE_LONG.to_string();
                slot.get_mut().last_accessed = now;
                Ok(next)
            }
            Entry::Vacant(slot) => {
                slot.insert(CacheEntry::new(
                    delta.to_string().into_bytes(),
                    TYPE_LONG.to_string(),
                    expire_at_utc(ttl_secs),
                ));
                Ok(delta)
            }
        }
    }

    /// Set the expiry of an existing key. Returns false when the key is absent.
    pub async fn expire(&self, key: &str, ttl_secs: i64) -> anyhow::Result<bool> {
        let mut map = self.lock();
        let now = Utc::now();
        if map.get(key).is_some_and(|entry| entry.is_expired(now)) {
            map.remove(key);
            return Ok(false);
        }
        let Some(entry) = map.get_mut(key) else {
            return Ok(false);
        };
        entry.expire_at = expire_at_utc(ttl_secs);
        Ok(true)
    }

    /// Redis-style TTL: `-2` missing, `-1` no expiry, otherwise seconds left.
    pub async fn ttl(&self, key: &str) -> anyhow::Result<i64> {
        let mut map = self.lock();
        let Some(expire) = map.get(key).map(|entry| entry.expire_at) else {
            return Ok(-2);
        };
        let Some(deadline) = expire else {
            return Ok(-1);
        };
        if deadline <= Utc::now() {
            map.remove(key);
            return Ok(-2);
        }
        Ok((deadline - Utc::now()).num_seconds().max(0))
    }

    /// Whether the key exists and has not expired (cheap TTL check).
    #[allow(dead_code)]
    pub async fn exists(&self, key: &str) -> anyhow::Result<bool> {
        Ok(self.ttl(key).await? != -2)
    }

    /// Delete expired keys. Returns the number of deleted keys.
    pub async fn purge_expired(&self) -> anyhow::Result<u64> {
        let mut map = self.lock();
        let now = Utc::now();
        let before = map.len();
        map.retain(|_, entry| !entry.is_expired(now));
        Ok((before - map.len()) as u64)
    }

    /// Snapshot all live entries after purging expired ones. Returns
    /// `(key, value, value_type, ttl_seconds)` sorted by key, where
    /// `ttl_seconds` follows the Redis-style convention of [`CacheStore::ttl`].
    pub async fn entries(&self) -> anyhow::Result<Vec<(String, Vec<u8>, String, i64)>> {
        let mut map = self.lock();
        let now = Utc::now();
        map.retain(|_, entry| !entry.is_expired(now));
        let mut entries = Vec::with_capacity(map.len());
        for (key, entry) in map.iter() {
            let ttl_seconds = entry
                .expire_at
                .map(|deadline| (deadline - now).num_seconds().max(0))
                .unwrap_or(-1);
            entries.push((
                key.clone(),
                entry.value.clone(),
                entry.value_type.clone(),
                ttl_seconds,
            ));
        }
        entries.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(entries)
    }
}

// ============================== LockStore ===============================

#[derive(Clone)]
pub struct LockStore {
    db: Database,
}

impl LockStore {
    pub fn new(db: Database) -> Self {
        Self { db }
    }

    /// Try to acquire `lock_key` for `owner` with a `ttl_secs` lease.
    pub async fn acquire(&self, lock_key: &str, owner: &str, ttl_secs: i64) -> anyhow::Result<bool> {
        // Clear a lease that already expired so a dead holder never blocks.
        let _ = sqlx::query("DELETE FROM sys_lock WHERE lock_key = ? AND expire_at <= ?")
            .bind(lock_key)
            .bind(now_rfc3339())
            .execute(&self.db.0)
            .await?;
        let result = sqlx::query(
            "INSERT INTO sys_lock (lock_key, owner, expire_at, create_time)
             VALUES (?, ?, ?, ?)
             ON CONFLICT(lock_key) DO NOTHING",
        )
        .bind(lock_key)
        .bind(owner)
        .bind((Utc::now() + Duration::seconds(ttl_secs)).to_rfc3339())
        .bind(now_rfc3339())
        .execute(&self.db.0)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    /// Extend the lease of a lock held by `owner`.
    pub async fn renew(&self, lock_key: &str, owner: &str, ttl_secs: i64) -> anyhow::Result<bool> {
        let result = sqlx::query(
            "UPDATE sys_lock SET expire_at = ? WHERE lock_key = ? AND owner = ?",
        )
        .bind((Utc::now() + Duration::seconds(ttl_secs)).to_rfc3339())
        .bind(lock_key)
        .bind(owner)
        .execute(&self.db.0)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    /// Release a lock only if it is still held by `owner`.
    pub async fn release(&self, lock_key: &str, owner: &str) -> anyhow::Result<bool> {
        let result = sqlx::query("DELETE FROM sys_lock WHERE lock_key = ? AND owner = ?")
            .bind(lock_key)
            .bind(owner)
            .execute(&self.db.0)
            .await?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn is_locked(&self, lock_key: &str) -> anyhow::Result<bool> {
        let row = sqlx::query("SELECT expire_at FROM sys_lock WHERE lock_key = ?")
            .bind(lock_key)
            .fetch_optional(&self.db.0)
            .await?;
        let Some(row) = row else {
            return Ok(false);
        };
        Ok(!is_expired(Some(row.get::<String, _>("expire_at").as_str())))
    }

    pub async fn purge_expired(&self) -> anyhow::Result<u64> {
        let result = sqlx::query("DELETE FROM sys_lock WHERE expire_at <= ?")
            .bind(now_rfc3339())
            .execute(&self.db.0)
            .await?;
        Ok(result.rows_affected())
    }

    /// List all live lock rows after deleting expired ones. Returns
    /// `(lock_key, owner, expire_at, create_time)` sorted by lock key.
    pub async fn list(&self) -> anyhow::Result<Vec<(String, String, String, String)>> {
        sqlx::query("DELETE FROM sys_lock WHERE expire_at <= ?")
            .bind(now_rfc3339())
            .execute(&self.db.0)
            .await?;
        let rows = sqlx::query(
            "SELECT lock_key, owner, expire_at, create_time FROM sys_lock ORDER BY lock_key",
        )
        .fetch_all(&self.db.0)
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| {
                (
                    row.get("lock_key"),
                    row.get("owner"),
                    row.get("expire_at"),
                    row.get("create_time"),
                )
            })
            .collect())
    }
}

fn is_expired(expire: Option<&str>) -> bool {
    match expire {
        None => false,
        Some(value) => match parse_rfc3339(value) {
            Some(deadline) => deadline <= Utc::now(),
            None => true,
        },
    }
}

fn parse_i64(bytes: &[u8]) -> Option<i64> {
    std::str::from_utf8(bytes).ok()?.trim().parse().ok()
}

// =========================== gRPC service ===============================

#[derive(Clone)]
pub struct CacheService {
    cache: CacheStore,
    lock: LockStore,
}

impl CacheService {
    pub fn new(db: Database, cache_max_entries: usize) -> Self {
        Self {
            cache: CacheStore::new(cache_max_entries),
            lock: LockStore::new(db),
        }
    }
}

fn require_key(value: &str) -> Result<&str, Status> {
    let value = value.trim();
    if value.is_empty() {
        Err(Status::invalid_argument("key is required"))
    } else {
        Ok(value)
    }
}

#[tonic::async_trait]
impl CacheServiceTrait for CacheService {
    async fn set(
        &self,
        request: Request<CacheSetRequest>,
    ) -> Result<Response<CacheSetResponse>, Status> {
        let request = request.into_inner();
        let key = require_key(&request.key)?;
        self.cache
            .set(key, &request.value, &request.value_type, request.ttl_seconds)
            .await
            .map_err(|error| {
                tracing::error!(%error, key, "cache set failed");
                Status::internal("cache set failed")
            })?;
        Ok(Response::new(CacheSetResponse { ok: true }))
    }

    async fn get(
        &self,
        request: Request<CacheGetRequest>,
    ) -> Result<Response<CacheGetResponse>, Status> {
        let request = request.into_inner();
        let key = require_key(&request.key)?;
        let entry = self.cache.get(key).await.map_err(|error| {
            tracing::error!(%error, key, "cache get failed");
            Status::internal("cache get failed")
        })?;
        let found = entry.is_some();
        let (value, value_type) = entry.unwrap_or_default();
        Ok(Response::new(CacheGetResponse {
            found,
            value,
            value_type,
        }))
    }

    async fn delete(
        &self,
        request: Request<CacheDeleteRequest>,
    ) -> Result<Response<CacheDeleteResponse>, Status> {
        let request = request.into_inner();
        let key = require_key(&request.key)?;
        let deleted = self.cache.delete(key).await.map_err(|error| {
            tracing::error!(%error, key, "cache delete failed");
            Status::internal("cache delete failed")
        })?;
        Ok(Response::new(CacheDeleteResponse { deleted }))
    }

    async fn incr(
        &self,
        request: Request<CacheIncrRequest>,
    ) -> Result<Response<CacheIncrResponse>, Status> {
        let request = request.into_inner();
        let key = require_key(&request.key)?;
        let value = self
            .cache
            .incr(key, request.delta, request.ttl_seconds)
            .await
            .map_err(|error| {
                tracing::error!(%error, key, "cache incr failed");
                Status::invalid_argument(format!("cache incr failed: {error}"))
            })?;
        Ok(Response::new(CacheIncrResponse { value }))
    }

    async fn expire(
        &self,
        request: Request<CacheExpireRequest>,
    ) -> Result<Response<CacheExpireResponse>, Status> {
        let request = request.into_inner();
        let key = require_key(&request.key)?;
        let ok = self.cache.expire(key, request.ttl_seconds).await.map_err(|error| {
            tracing::error!(%error, key, "cache expire failed");
            Status::internal("cache expire failed")
        })?;
        Ok(Response::new(CacheExpireResponse { ok }))
    }

    async fn ttl(
        &self,
        request: Request<CacheTtlRequest>,
    ) -> Result<Response<CacheTtlResponse>, Status> {
        let request = request.into_inner();
        let key = require_key(&request.key)?;
        let ttl_seconds = self.cache.ttl(key).await.map_err(|error| {
            tracing::error!(%error, key, "cache ttl failed");
            Status::internal("cache ttl failed")
        })?;
        Ok(Response::new(CacheTtlResponse { ttl_seconds }))
    }

    async fn acquire_lock(
        &self,
        request: Request<LockAcquireRequest>,
    ) -> Result<Response<LockAcquireResponse>, Status> {
        let request = request.into_inner();
        let lock_key = require_key(&request.lock_key)?;
        let owner = request.owner.trim();
        if owner.is_empty() {
            return Err(Status::invalid_argument("owner is required"));
        }
        if request.ttl_seconds <= 0 {
            return Err(Status::invalid_argument("ttl_seconds must be > 0"));
        }
        let acquired = self
            .lock
            .acquire(lock_key, owner, request.ttl_seconds)
            .await
            .map_err(|error| {
                tracing::error!(%error, lock_key, "lock acquire failed");
                Status::internal("lock acquire failed")
            })?;
        Ok(Response::new(LockAcquireResponse { acquired }))
    }

    async fn renew_lock(
        &self,
        request: Request<LockRenewRequest>,
    ) -> Result<Response<LockRenewResponse>, Status> {
        let request = request.into_inner();
        let lock_key = require_key(&request.lock_key)?;
        let owner = request.owner.trim();
        if owner.is_empty() {
            return Err(Status::invalid_argument("owner is required"));
        }
        if request.ttl_seconds <= 0 {
            return Err(Status::invalid_argument("ttl_seconds must be > 0"));
        }
        let renewed = self
            .lock
            .renew(lock_key, owner, request.ttl_seconds)
            .await
            .map_err(|error| {
                tracing::error!(%error, lock_key, "lock renew failed");
                Status::internal("lock renew failed")
            })?;
        Ok(Response::new(LockRenewResponse { renewed }))
    }

    async fn release_lock(
        &self,
        request: Request<LockReleaseRequest>,
    ) -> Result<Response<LockReleaseResponse>, Status> {
        let request = request.into_inner();
        let lock_key = require_key(&request.lock_key)?;
        let owner = request.owner.trim();
        if owner.is_empty() {
            return Err(Status::invalid_argument("owner is required"));
        }
        let released = self
            .lock
            .release(lock_key, owner)
            .await
            .map_err(|error| {
                tracing::error!(%error, lock_key, "lock release failed");
                Status::internal("lock release failed")
            })?;
        Ok(Response::new(LockReleaseResponse { released }))
    }

    async fn is_locked(
        &self,
        request: Request<LockIsLockedRequest>,
    ) -> Result<Response<LockIsLockedResponse>, Status> {
        let request = request.into_inner();
        let lock_key = require_key(&request.lock_key)?;
        let locked = self.lock.is_locked(lock_key).await.map_err(|error| {
            tracing::error!(%error, lock_key, "lock is_locked failed");
            Status::internal("lock is_locked failed")
        })?;
        Ok(Response::new(LockIsLockedResponse { locked }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration as StdDuration;
    use uuid::Uuid;

    async fn stores() -> (CacheStore, LockStore, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!("cangling-cache-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let db = Database::connect(&format!("sqlite:{}/queue.db", dir.display()))
            .await
            .unwrap();
        (
            CacheStore::new(DEFAULT_CACHE_MAX_ENTRIES),
            LockStore::new(db),
            dir,
        )
    }

    #[tokio::test]
    async fn set_get_delete_roundtrip() {
        let (cache, _lock, dir) = stores().await;
        cache.set("k", b"hello", "string", 0).await.unwrap();
        let entry = cache.get("k").await.unwrap();
        assert_eq!(entry.as_ref().map(|(v, _)| v.as_slice()), Some(&b"hello"[..]));
        assert_eq!(entry.as_ref().map(|(_, t)| t.as_str()), Some("string"));
        assert!(cache.delete("k").await.unwrap());
        assert!(!cache.delete("k").await.unwrap());
        assert!(cache.get("k").await.unwrap().is_none());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn ttl_semantics_match_redis() {
        let (cache, _lock, dir) = stores().await;
        assert_eq!(cache.ttl("missing").await.unwrap(), -2);
        cache.set("forever", b"1", "string", 0).await.unwrap();
        assert_eq!(cache.ttl("forever").await.unwrap(), -1);
        cache.set("temp", b"1", "string", 60).await.unwrap();
        assert!(cache.ttl("temp").await.unwrap() > 0);
        cache.expire("temp", 0).await.unwrap();
        assert_eq!(cache.ttl("temp").await.unwrap(), -1);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn expired_value_is_lazily_deleted() {
        let (cache, _lock, dir) = stores().await;
        cache.set("k", b"v", "string", 1).await.unwrap();
        tokio::time::sleep(StdDuration::from_millis(1100)).await;
        assert!(cache.get("k").await.unwrap().is_none());
        assert!(!cache.exists("k").await.unwrap());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn incr_is_atomic_and_creates_key() {
        let (cache, _lock, dir) = stores().await;
        assert_eq!(cache.incr("n", 5, 60).await.unwrap(), 5);
        assert_eq!(cache.incr("n", 7, 60).await.unwrap(), 12);
        let (value, value_type) = cache.get("n").await.unwrap().unwrap();
        assert_eq!(value, b"12");
        assert_eq!(value_type, "long");
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn lru_evicts_least_recently_used_when_full() {
        let cache = CacheStore::new(2);
        cache.set("a", b"1", "string", 0).await.unwrap();
        cache.set("b", b"2", "string", 0).await.unwrap();
        // Touch `a` so `b` becomes the least recently used entry.
        assert!(cache.get("a").await.unwrap().is_some());
        cache.set("c", b"3", "string", 0).await.unwrap();
        assert!(cache.get("b").await.unwrap().is_none());
        assert!(cache.get("a").await.unwrap().is_some());
        assert!(cache.get("c").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn lock_acquire_release_and_renew() {
        let (_cache, lock, dir) = stores().await;
        assert!(lock.acquire("job", "a", 60).await.unwrap());
        assert!(!lock.acquire("job", "b", 60).await.unwrap());
        assert!(lock.is_locked("job").await.unwrap());
        assert!(lock.renew("job", "a", 60).await.unwrap());
        assert!(!lock.release("job", "b").await.unwrap());
        assert!(lock.release("job", "a").await.unwrap());
        assert!(!lock.is_locked("job").await.unwrap());
        assert!(lock.acquire("job", "b", 60).await.unwrap());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn expired_lock_can_be_reacquired() {
        let (_cache, lock, dir) = stores().await;
        assert!(lock.acquire("job", "a", 1).await.unwrap());
        tokio::time::sleep(StdDuration::from_millis(1100)).await;
        assert!(!lock.is_locked("job").await.unwrap());
        assert!(lock.acquire("job", "b", 60).await.unwrap());
        let _ = std::fs::remove_dir_all(dir);
    }
}
