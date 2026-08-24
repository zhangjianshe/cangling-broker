# cangling-broker 内存缓存与分布式锁模块 — 调查报告

> 目标：在 `cangling-broker`（Rust 编写的消息 broker，SQLite 持久化）中新增一个模块，
> 提供 **内存缓存（KV + TTL + 原子自增）** 与 **分布式锁（acquire / renew / release）** 能力，
> 作为 Redis 的数据库化替代品，供 CIS 等系统通过 gRPC / HTTP 调用。

---

## 1. 项目现状

`cangling-broker` 是一个“类 Kafka 的积木”式消息 broker：

| 维度 | 现状 |
|------|------|
| 语言/运行时 | Rust 2021 + Tokio + tonic (gRPC) + axum (HTTP) |
| 存储 | SQLite（`sqlx`，WAL 模式），单文件 `<data>/queue.db` |
| 对外协议 | gRPC（`proto/queue.proto`，package `dispatcher.v1`）+ HTTP 状态口 + MQTT 3.1.1 |
| 客户端 SDK | Java（`cn.mapway:cangling-broker`）、Python（`cangling-broker`） |
| 认证 | `authorization: Bearer <CL_BROKER_AUTH_TOKEN>`（gRPC/HTTP/MQTT 共用） |
| 数据表 | `messages`、`consumers`、`topic_stats`（同一 SQLite 文件） |

关键代码位置：

- `src/db.rs` — `Database`（`SqlitePool` 包装），建表 + 全部 SQL
- `src/main.rs` — 装配 gRPC 服务（`MessageQueue`）、HTTP 状态口、后台循环
- `src/status.rs` — axum HTTP 路由（`/`、`/health`、`/status`、`/topics`、`/messages`）
- `proto/queue.proto` — gRPC 契约；`build.rs` 用 `tonic_build` 编译生成 `crate::proto`
- `src/auth.rs` — token 校验拦截器（gRPC/HTTP 共用）

**结论**：broker 已具备“SQLite 代替 Redis”的全部基础设施（连接池、时间戳、原子 upsert、
后台清理循环），缓存/锁模块可直接复用，无需引入新依赖。

---

## 2. 需求映射（Redis → SQLite 模块）

| Redis 能力 | 新增模块接口 | 持久化表 |
|-----------|-------------|---------|
| `SET key value [EX ttl]` | `CacheStore::set` / `CacheService.Set` | `sys_kv` |
| `GET key` | `CacheStore::get` / `CacheService.Get` | `sys_kv` |
| `DEL key` | `CacheStore::delete` / `CacheService.Delete` | `sys_kv` |
| `INCR key [by delta]` | `CacheStore::incr` / `CacheService.Incr` | `sys_kv` |
| `EXPIRE key ttl` | `CacheStore::expire` / `CacheService.Expire` | `sys_kv` |
| `TTL key` | `CacheStore::ttl` / `CacheService.Ttl`（-2 不存在 / -1 永不过期） | `sys_kv` |
| `EXISTS key` | `CacheStore::exists` | `sys_kv` |
| `SET NX`（锁） | `LockStore::acquire` / `CacheService.AcquireLock` | `sys_lock` |
| 锁续期 | `LockStore::renew` / `CacheService.RenewLock` | `sys_lock` |
| 安全释放（Lua 对比 owner） | `LockStore::release` / `CacheService.ReleaseLock` | `sys_lock` |
| 锁状态查询 | `LockStore::is_locked` | `sys_lock` |

语义对齐 Redis 的关键点：

- `TTL` 返回值：`-2` 键不存在 / `-1` 永不过期 / `>=0` 剩余秒数。
- 锁释放只允许 `owner` 匹配者执行（等价 Redis `if get == owner then del`）。
- 过期采用**惰性删除**（读时判断）+ 后台定时清理，防止表膨胀与死锁。

---

## 3. 设计决策

### 3.1 存储：SQLite 两张新表（与 CIS 的 PostgreSQL 方案对齐）

```sql
CREATE TABLE IF NOT EXISTS sys_kv (
    key         TEXT PRIMARY KEY NOT NULL,
    value       BLOB NOT NULL,            -- 二进制安全，等价 Redis string
    value_type  TEXT NOT NULL DEFAULT 'string',  -- string / long，用于 incr 语义
    expire_at   TEXT,                      -- RFC3339，NULL = 永不过期
    update_time TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_sys_kv_expire ON sys_kv(expire_at);

CREATE TABLE IF NOT EXISTS sys_lock (
    lock_key    TEXT PRIMARY KEY NOT NULL,
    owner       TEXT NOT NULL,             -- 持有者 UUID
    expire_at   TEXT NOT NULL,             -- 锁过期时间（防死锁）
    create_time TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_sys_lock_expire ON sys_lock(expire_at);
```

与 CIS `doc/remove-redis.md` 中的 `sys_kv` / `sys_lock` 命名一致；时间戳沿用项目
既有的 RFC3339 字符串约定（SQLite 下字典序 == 时间序，无需存储专用类型）。

### 3.2 原子性

- 缓存写：`INSERT ... ON CONFLICT(key) DO UPDATE`（单条原子 upsert）。
- 自增：`ON CONFLICT ... DO UPDATE SET value = CAST(CAST(sys_kv.value AS INTEGER) + ? AS BLOB)`
  单条原子语句，首次插入时写入 TTL，冲突时**不覆盖**已有 TTL（与 CIS `CacheService.incr` 一致）。
- 锁获取：先 `DELETE` 已过期租约，再 `INSERT ... ON CONFLICT(lock_key) DO NOTHING`，
  以主键冲突为互斥事实源（等价 `SET NX`）。
- 锁释放/续期：`WHERE lock_key=? AND owner=?`，保证只有持有者可操作。

### 3.3 传输与接入面

1. **gRPC**：在 `proto/queue.proto` 中新增独立 `service CacheService`（同 package），
   不修改既有 `MessageQueue`，向后兼容，老客户端不受影响。
2. **HTTP**：在状态口（`7501`）新增 `/cache*`、`/lock*` 路由，复用同一鉴权中间件，
   方便 curl / 浏览器调试与无 gRPC 环境的接入。
3. **SDK**：Java / Python 客户端各新增缓存 + 锁方法（详见第 5 节）。

### 3.4 生命周期

- 过期 KV 与锁：读时惰性删除 + `retention_loop` 60 秒清扫一次（`purge_expired`）。
- 锁默认不会自动续期，由持有者调用 `renew`（与 Redis SET NX + EX 语义一致）。

---

## 4. 已实现文件清单

| 文件 | 动作 | 说明 |
|------|------|------|
| `proto/queue.proto` | 修改 | 新增 `CacheService` + 9 个 RPC + 对应 message |
| `src/db.rs` | 修改 | `connect()` 新增 `sys_kv` / `sys_lock` 建表与索引 |
| `src/cache.rs` | **新增** | `CacheStore`、`LockStore`、gRPC `CacheService` 实现 + 单元测试 |
| `src/main.rs` | 修改 | `mod cache`；注册 `CacheServiceServer`（共用鉴权拦截器）；retention 循环接入过期清理 |
| `src/status.rs` | 修改 | 新增 `/cache`、`/cache/incr`、`/lock`、`/lock/acquire`、`/lock/renew` HTTP 路由与处理器 |
| `python/cangling_broker/proto/*` | 重新生成 | `queue_pb2.py` / `queue_pb2_grpc.py`（grpcio-tools 1.83.0） |
| `python/cangling_broker/client.py` | 修改 | `cache_*` / `acquire_lock` 方法 + `Lock` 句柄类 |
| `java/.../SatwayClient.java` | 修改 | `cache*` / `acquireLock` 方法 |
| `java/.../LockHandle.java` | **新增** | 锁句柄（`AutoCloseable`，支持 try-with-resources） |

---

## 5. 对外 API

### 5.1 gRPC（`dispatcher.v1.CacheService`）

| RPC | 请求 | 响应 |
|-----|------|------|
| `Set` | `key, value(bytes), ttl_seconds` | `ok` |
| `Get` | `key` | `found, value(bytes)` |
| `Delete` | `key` | `deleted` |
| `Incr` | `key, delta, ttl_seconds` | `value` |
| `Expire` | `key, ttl_seconds` | `ok` |
| `Ttl` | `key` | `ttl_seconds`（-2 / -1 / >=0） |
| `AcquireLock` | `lock_key, owner, ttl_seconds` | `acquired` |
| `RenewLock` | `lock_key, owner, ttl_seconds` | `renewed` |
| `ReleaseLock` | `lock_key, owner` | `released` |

### 5.2 HTTP（状态口，Bearer 鉴权）

```bash
curl -s -H 'authorization: Bearer <token>' 'http://127.0.0.1:7501/cache?key=jobs:count'
curl -s -X PUT -H 'authorization: Bearer <token>' -H 'content-type: application/json' \
  -d '{"key":"session:u1","value":"{\"role\":\"admin\"}","ttl_seconds":300}' \
  http://127.0.0.1:7501/cache
curl -s -X DELETE -H 'authorization: Bearer <token>' 'http://127.0.0.1:7501/cache?key=jobs:count'
curl -s -X POST -H 'authorization: Bearer <token>' -H 'content-type: application/json' \
  -d '{"key":"jobs:count","delta":1,"ttl_seconds":60}' http://127.0.0.1:7501/cache/incr

curl -s -X POST -H 'authorization: Bearer <token>' -H 'content-type: application/json' \
  -d '{"lock_key":"import:dataset","owner":"uuid","ttl_seconds":30}' \
  http://127.0.0.1:7501/lock/acquire
curl -s -X POST -H 'authorization: Bearer <token>' -H 'content-type: application/json' \
  -d '{"lock_key":"import:dataset","owner":"uuid","ttl_seconds":30}' \
  http://127.0.0.1:7501/lock/renew
curl -s -X DELETE -H 'authorization: Bearer <token>' \
  'http://127.0.0.1:7501/lock?lock_key=import:dataset&owner=uuid'
```

### 5.3 Java SDK

```java
try (SatwayClient client = SatwayClient.connect("127.0.0.1:7500", "change-me")) {
    client.cacheSet("session:u1", "{\"role\":\"admin\"}", 300);
    String value = client.cacheGetString("session:u1");
    long n = client.cacheIncr("jobs:count", 1, 60);
    long ttl = client.cacheTtl("jobs:count");     // -2 / -1 / >=0

    try (LockHandle lock = client.acquireLock("import:dataset", 30)) {
        if (lock != null) {
            lock.renew(30);
            // critical section
        }
    }
}
```

### 5.4 Python SDK

```python
from cangling_broker import SatwayClient

with SatwayClient.connect("127.0.0.1:7500", "change-me") as client:
    client.cache_set("session:u1", '{"role": "admin"}', ttl_seconds=300)
    value = client.cache_get_string("session:u1")
    n = client.cache_incr("jobs:count", 1, ttl_seconds=60)
    ttl = client.cache_ttl("jobs:count")          # -2 / -1 / >=0

    lock = client.acquire_lock("import:dataset", 30)
    if lock:
        with lock:
            lock.renew(30)
            # critical section
```

---

## 6. 验证结果

- `cargo check` / `cargo build`：通过（无新警告，除 1 处有意 `#[allow(dead_code)]` 的 `exists`）。
- 新增单元测试 6 个全部通过（`cache::tests`）：
  - `set/get/delete` 往返、TTL Redis 语义、过期惰性删除、`incr` 原子自增、
    锁互斥 + 释放 + 续期、过期锁可重入。
- `db::tests` 12 个全部通过（建表/迁移兼容）。
- `cargo test` 总结果：**65 passed / 1 failed**，唯一失败为**预先存在**的
  `status::tests::dashboard_html_injects_base_href`（`status.html` 在历史提交中已移除
  `client-page-size`/`topic-page-size`，但测试断言未同步更新；与本次改动无关）。
- Java：`mvn -q -o compile` 通过（离线，proto 由 `../proto` 重新生成）。
- Python：grpcio-tools 1.83.0 重新生成 stub，SDK 导入与属性自检通过。

---

## 7. 风险与注意事项

| 风险 | 说明 / 缓解 |
|------|------------|
| `incr` 对已过期键 | 自增前先删除过期行再 upsert，避免在脏值上累加 |
| 锁时间单位 | 本模块统一为**秒**（与 CIS `doc/remove-redis.md` Phase 0 审计结论一致），HTTP/gRPC 均传秒 |
| SQLite 单写者 | 缓存/锁与消息队列共用同一 WAL 库，写吞吐受单文件限制；高频计数场景建议本地合并后再写 |
| `keys(pattern)` 全表扫 | 未提供模糊 `KEYS`，只提供精确键；需要时用 `key LIKE 'prefix%'` + 索引 |
| 表膨胀 | `retention_loop` 每 60 秒清理过期 KV 与锁行 |
| 认证 | 缓存/锁 HTTP 路由走与 `/status` 相同的 token 中间件，`/health` 保持开放 |

---

## 8. 建议后续步骤

1. 在 CIS 侧新增一个薄客户端（或直接复用 Java/Python SDK 的 `cache*` / `acquireLock`），
   把 `CacheService` / `DbLocker` 的 SQL 后端切换为对 broker 的 gRPC 调用（可选开关，便于回滚）。
2. 补充集成测试：多实例并发抢锁、TTL 过期、`incr` 并发自增（可基于现有 `.test/` Python 脚本扩展）。
3. 视需要为高频进度/API 计数增加本地合并批量写，降低 SQLite 写放大。
