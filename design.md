# cangling-broker 设计文档

> 一份“类 Kafka 的积木”式消息组件：**gRPC 流消息队列 + 内存缓存 + SQLite 分布式锁**，
> 同时兼容 MQTT 3.1.1（TCP / WebSocket），用单文件 SQLite 代替 Redis / ZooKeeper 等外部依赖。

---

## 1. 项目定位

| 维度 | 说明 |
|------|------|
| 语言 / 运行时 | Rust 2021 + Tokio（async） |
| 对外协议 | gRPC（`dispatcher.v1`）、HTTP 状态口、MQTT 3.1.1（TCP / WebSocket） |
| 持久化 | SQLite（`sqlx`，WAL 模式），单文件 `<data>/queue.db` |
| 客户端 SDK | Java（`cn.mapway:cangling-broker`）、Python（`cangling_broker`）、Rust（`examples/receiver.rs`） |
| 认证 | 统一 `CL_BROKER_AUTH_TOKEN`（`authorization: Bearer <token>`） |
| 部署形态 | 单二进制 / Docker 容器，客户端通过 gRPC 主动拨出，只需暴露端口 |

**要解决的核心问题**：为 CIS 等系统提供一个**轻量、单机可部署、无外部依赖**的基础设施，
同时覆盖三类能力——消息投递、KV 缓存、分布式锁——从而在去 Redis 后仍能共享同一份缓存/锁。

---

## 2. 设计目标与原则

1. **单文件持久化**：所有状态（消息、消费者、主题统计、锁）落在一个 SQLite 文件中，备份/迁移/排查都极简。
2. **客户端主动拨出**：消费者通过 gRPC 长连接主动连接 broker，因此 Docker 只需发布端口，无需打通反向连接。
3. **MQTT 式默认语义**：未配置主题默认 `broadcast`（广播）+ `ephemeral`（即弃），即每个在线流各收一份、无人监听即丢弃；按需显式切换为 `single`（单投/竞争消费）与 `persistent`（持久排队）。
4. **热路径内存化、可靠性 SQLite 化**：缓存走内存（高频、可丢）；锁和消息走 SQLite（低频、必须跨重启可靠）。
5. **Redis 语义对齐**：`TTL` 返回 `-2`（不存在）/`-1`（永不过期）/`>=0`（剩余秒）；锁等价 `SET NX` + owner 校验释放。
6. **统一鉴权**：gRPC / HTTP / MQTT 复用同一个 token 校验逻辑。
7. **可观测性内置**：HTTP 状态口提供仪表盘（主题、客户端、消息、缓存、锁）与调试接口。

---

## 3. 总体架构

```
                        ┌──────────────────────────────────────────────┐
                        │                  Clients                     │
                        │  Java SDK │ Python SDK │ MQTT 3.1.1 客户端    │
                        └───────┬──────────┬──────────────┬─────────────┘
                                │ gRPC     │ gRPC/HTTP    │ MQTT(TCP/WS)
        ┌───────────────────────▼──────────▼──────────────▼──────────────┐
        │                        接入层 (Ingress)                        │
        │  tonic gRPC Server         axum HTTP Status       mqtt module  │
        │  (MessageQueue/CacheService) (dashboard + APIs)  (codec/session)│
        └───────────────────────────────┬────────────────────────────────┘
                                        │
        ┌───────────────────────────────▼────────────────────────────────┐
        │                      核心逻辑层 (Core)                         │
        │  delivery::ingest / fanout_ephemeral / run_subscribe_loop      │
        │  subscribers::TopicSubscribers (内存订阅表)                     │
        │  subscribers::InflightAcks    (in-flight 消息 ack 等待表)       │
        │  topic::filter_matches        (MQTT `+`/`#` 主题匹配)           │
        └───────────────────────────────┬────────────────────────────────┘
                                        │
        ┌───────────────────────────────▼────────────────────────────────┐
        │                      存储层 (Storage)                          │
        │  db::Database (SqlitePool, WAL)  ── messages / consumers /      │
        │                                     topic_stats / sys_lock     │
        │  cache::CacheStore (Arc<Mutex<HashMap>>, 内存 + TTL + LRU)     │
        │  cache::LockStore  (SQLite sys_lock)                           │
        └───────────────────────────────┬────────────────────────────────┘
                                        │
        ┌───────────────────────────────▼────────────────────────────────┐
        │                    后台任务 (Background tasks)                  │
        │  dispatch_loop   (persistent 主题无在线流时 HTTP 回退)          │
        │  retention_loop  (过期消息/消费者/缓存/锁 清理)                 │
        │  subscribe loops (每订阅一条，负责 claim → 投递 → ack)          │
        └────────────────────────────────────────────────────────────────┘
```

### 3.1 消息生命周期与状态机

消息在 SQLite `messages` 表中的状态迁移：

```
 AcceptMessages
      │  (enqueue: idempotency_key 去重)
      ▼
  pending ──claim(事务: 置 processing + lease + next_attempt_at)──▶ processing
      ▲                                                              │
      │   reclaim_stale() 超时未 ack 回收                              │ AckMessage(success)
      │  (processing 且 next_attempt_at 过期 → pending)              │ 或超时
      └──────────────────────────────────────────────────────────────┤
                                             ┌────────────────────────┴─────────────┐
                                             ▼                                      ▼
                                        delivered                               failed
                                        (成功, 可清理)                     (attempts+1, 达上限)
                                                                                   │
                                                                                   ▼
                                              next_attempt_at 到期 → 重新进入 pending（重试）
```

关键点：

- **claim 原子性**：`UPDATE ... WHERE status='pending'` + 事务，保证并发消费者不会抢到同一条。
- **lease**：每条投递携带随机 `lease`（UUID），`AckMessage` 必须回传 `message_id + lease` 才生效，防止串号确认。
- **reclaim**：`reclaim_stale()` 把超时未确认的 `processing` 消息重置为 `pending`，实现“至少一次”投递。
- **ephemeral（即弃）**：不落 `messages` 队列，直接内存 fanout 给在线订阅者；无人接收则只更新 `topic_stats` 的 `dropped` 计数。

### 3.2 投递模式 × 持久模式

`delivery`（投递）与 `persistence`（持久）是两个正交维度：

| | `single`（单投） | `broadcast`（广播） |
|---|---|---|
| **语义** | 竞争消费，每条只发给一个在线流 | 每个匹配的在线流各收一份 |
| **等价** | Kafka 消费组 / 队列 | MQTT pub/sub / fanout |

| | `persistent`（持久） | `ephemeral`（即弃） |
|---|---|---|
| **语义** | 先入队、后投递，可稍后/重试 | 只投给在线订阅者，无人即丢 |
| **回退** | 无在线流时走 `DOWNSTREAM_URL` HTTP 回退 | 无 |

未调用 `ConfigureTopics` 的主题默认 **broadcast + ephemeral**（MQTT 风格）。

### 3.3 主题匹配（MQTT 3.1.1）

`topic::filter_matches` 实现标准 MQTT 主题过滤器：

- `+` 匹配单层；`#` 匹配剩余所有层（且只能出现在末尾）。
- `$` 开头的系统主题不被根级通配符（`#`、`+/...`）匹配。
- 发布主题禁止包含 `+` / `#`。

订阅表 `TopicSubscribers` 以“过滤器”为键，投递时用 `matching_senders()` 找到所有匹配会话。

### 3.4 缓存与分布式锁（Redis 替代）

| 能力 | 实现 | 存储 | 理由 |
|------|------|------|------|
| KV 缓存 | `CacheStore` | **内存** `Arc<Mutex<HashMap>>` + TTL + LRU | 热路径、高频、可丢 |
| 分布式锁 | `LockStore` | **SQLite** `sys_lock` | 低频、跨重启必须可靠 |

- 缓存值二进制安全，携带 `value_type`（string/long/int/double/bool）以便客户端还原类型。
- `incr` 在同一临界区内完成“读 + 加 delta + 写回”，首次创建才写 TTL。
- 超上限（`CL_BROKER_CACHE_MAX_ENTRIES`，默认 100000）时先清过期项，再逐出 LRU。
- 锁获取 = 先删已过期租约，再 `INSERT ... ON CONFLICT(lock_key) DO NOTHING`；释放/续期必须 `WHERE owner = ?`。
- 锁过期采用惰性删除 + `retention_loop` 每 60 秒清扫。

> 为什么锁不内存化：broker 重启会导致租约丢失，两个 worker 可能同时拿到同一把锁。
> 持久化锁保证“协调者重启不丢租约”，这是分布式锁的底线（详见 `doc/cache-lock-module.md`）。

### 3.5 鉴权

- gRPC：`AuthInterceptor`（tonic interceptor），校验 `authorization: Bearer <token>` 或 `x-auth-token`。
- HTTP：`require_token` 中间件，同样支持 `authorization` 头或 `?token=` 查询参数。
- `/health` 保持开放（存活探针）。
- token 比较使用恒定时间比较（`tokens_match`）防时序侧信道。
- 客户端元数据 `x-client-version` / `x-client-host` 被捕获为消费者属性，展示在仪表盘。

---

## 4. 模块分布（`src/`）

| 文件 | 行数 | 职责 |
|------|------|------|
| `main.rs` | 685 | 装配与生命周期：连接 DB、启动 gRPC/HTTP/MQTT、后台任务；优雅停机（WAL checkpoint + 关池） |
| `config.rs` | 199 | 配置：`clap` + 环境变量，端口/认证/保留策略/缓存上限等 |
| `auth.rs` | 170 | token 鉴权（gRPC interceptor + HTTP 辅助）、客户端元数据读取 |
| `db.rs` | 1650 | `Database`（`SqlitePool` 包装）：建表/迁移/索引，全部 SQL（入队、claim、ack、统计、清理） |
| `delivery.rs` | 426 | 消息入口 `ingest`、ephemeral 内存 fanout、`run_subscribe_loop`（claim→投递→ack 主循环） |
| `subscribers.rs` | 263 | `TopicSubscribers`（内存订阅表）、`InflightAcks`（in-flight ack 等待表）、`SubscriptionGuard` |
| `topic.rs` | 129 | MQTT 3.1.1 主题过滤器匹配与合法性校验 |
| `model.rs` | 209 | 领域模型：`DeliveryMode`/`PersistenceMode`/`TopicConfig`/`TopicSnapshot`/`ClaimedMessage` 等 |
| `grpc_conn.rs` | 212 | `TrackingIncoming` + `GrpcClientRegistry`：跟踪 gRPC 连接的建立/断开与身份 |
| `cache.rs` | 684 | `CacheStore`（内存缓存）+ `LockStore`（SQLite 锁）+ gRPC `CacheService` 实现与测试 |
| `status.rs` | 1337 | axum HTTP 状态服务：`/status`、`/topics`、`/messages`、`/cache*`、`/lock*` 路由与聚合 |
| `status.html` | 1566 | 内置仪表盘（单页应用）：消息/缓存/锁视图、主题/客户端表格、分页、主题过滤器 |
| `logging.rs` | 82 | `tracing` 初始化：stdout + 按大小滚动文件日志 |
| `mqtt/mod.rs` | 720 | MQTT TCP/WS 服务入口、路由、`ClientRegistry`（在线客户端注册表） |
| `mqtt/codec.rs` | 600 | MQTT 3.1.1 报文编解码 |
| `mqtt/session.rs` | 629 | MQTT 会话（TCP/WS）：CONNECT/PUBLISH/SUBSCRIBE/UNSUBSCRIBE，与订阅表桥接 |

**SDK 与示例：**

- `java/` — Java SDK（`cn.mapway.broker`）：`SatwayClient`、`LockHandle`（try-with-resources）、`CacheEntry`、`SubscribeOptions`、`MessageHandler` 及 `ProducerMain`/`ConsumerMain` 示例。
- `python/cangling_broker/` — Python SDK：`client.py`、`models.py`、`compat.py` 及生成的 proto stub。
- `examples/receiver.rs` — Rust 消费端示例。

**协议：**

- `proto/queue.proto` — gRPC 契约（package `dispatcher.v1`），含 `MessageQueue` 与 `CacheService` 两个 service；`build.rs` 用 `tonic-build` 生成 `crate::proto`。

---

## 5. 数据模型（SQLite）

| 表 | 用途 | 关键字段 |
|----|------|---------|
| `messages` | 持久消息队列 | `id`, `idempotency_key`, `topic`, `payload`, `attributes`, `status`, `attempts`, `next_attempt_at`, `lease`, `created_at`, `delivered_at`, `last_error` |
| `consumers` | 消费者元数据 | `id`, `topic`, `name`, `attributes`, `last_seen_at`, `created_at` |
| `topic_stats` | 主题统计与配置 | `topic`, `accepted`, `duplicates`, `delivered`, `failed`, `dropped`, `delivery`, `persistence`, `configured`, `last_seen_at` |
| `sys_lock` | 分布式锁 | `lock_key`, `owner`, `expire_at`, `create_time` |
| `sys_kv` | （历史遗留）KV 缓存表 | `key`, `value`, `value_type`, `expire_at`, `update_time` |

> 说明：缓存已改为纯内存实现，`sys_kv` 表仅在 `connect()` 中建表以保持旧库兼容，当前不再读写。

- 时间戳统一使用 RFC3339 字符串（SQLite 下字典序 == 时间序，无需专用时间类型）。
- `journal_mode = WAL`，启动时 `wal_checkpoint(RESTART)`，关停时 `wal_checkpoint(TRUNCATE)` + `pool.close()`。

---

## 6. 对外接口

### 6.1 gRPC（`dispatcher.v1`）

**MessageQueue**：`AcceptMessages`（流式发布）、`Register`/`Unregister`、`Subscribe`（流式消费）、`AckMessage`、`ConfigureTopics`、`ListTopics`。

**CacheService**：`Set`/`Get`/`Delete`/`Incr`/`Expire`/`Ttl`，以及 `AcquireLock`/`RenewLock`/`ReleaseLock`/`IsLocked`。

### 6.2 HTTP 状态口（默认 `7501`）

| 路由 | 说明 |
|------|------|
| `GET /` | 仪表盘（`status.html`） |
| `GET /health` | 存活探针（免鉴权） |
| `GET /status` | 聚合状态 JSON |
| `GET/POST /topics` | 查询/配置主题 |
| `GET/DELETE /messages` | 查看/清空主题消息 |
| `GET/PUT/DELETE /cache`、`GET /cache/keys`、`POST /cache/incr` | 缓存操作 |
| `GET/DELETE /lock`、`GET /lock/list`、`POST /lock/acquire`、`POST /lock/renew` | 锁操作 |

---

## 7. 后台任务与生命周期

| 任务 | 触发/周期 | 职责 |
|------|-----------|------|
| `dispatch_loop` | `WORKER_POLL_MS`（默认 500ms） | 持久主题无在线流时，把消息 POST 到 `DOWNSTREAM_URL` |
| `retention_loop` | 每 60s | 清理过期消息/消费者/缓存/锁，执行各类保留策略 |
| `subscribe loop` | 每订阅一条 | claim → 投递 → 等 ack → delivered/failed |

**优雅停机**：`Ctrl-C`/`SIGTERM` → 取消 `CancellationToken` → gRPC/HTTP/MQTT 停止接受新连接并排空在途请求 → 等待所有后台任务退出 → `wal_checkpoint(TRUNCATE)` + 关闭连接池（确保即使某任务报错也会执行关库）。

---

## 8. 配置（环境变量）

| 变量 | 默认 | 说明 |
|------|------|------|
| `CL_BROKER_PORT` | `7500` | gRPC 端口 |
| `CL_BROKER_WEBPORT` | `7501` | HTTP 状态口 |
| `CL_BROKER_MQTT_PORT` | `7883` | MQTT TCP（`0` 关闭） |
| `CL_BROKER_MQTT_WSPORT` | `8083` | MQTT WebSocket（`0` 挂到状态口） |
| `CL_BROKER_AUTH_TOKEN` | 无 | 认证 token（不设则开放） |
| `CL_BROKER_DATA` | `./queue.db` | 数据目录（SQLite + 日志） |
| `CL_BROKER_WEB_BASE` | 空 | 反代路径前缀（如 `/msg`） |
| `CL_BROKER_CACHE_MAX_ENTRIES` | `100000` | 内存缓存 LRU 上限 |
| `DOWNSTREAM_URL` | 无 | 持久主题无在线流时的 HTTP 回退 |
| `ACK_TIMEOUT_SECS` | `30` | 消息可见性超时（超时回收） |
| `MAX_DELIVERY_ATTEMPTS` | `10` | 最大投递次数 |
| `MESSAGE_RETENTION_DAYS` | `10` | 消息保留天数 |
| `CL_BROKER_DELIVERED_RETENTION_HOURS` | `24` | 已投递消息保留小时 |
| `CL_BROKER_LOG_MESSAGES` | `false` | 是否打印消息内容 |

完整列表见 `src/config.rs`。

---

## 9. 关键设计决策回顾

1. **SQLite 而非外部 MQ**：单文件、零运维、易备份，配合 WAL 满足本项目吞吐；牺牲跨机横向扩展，换取部署极简。
2. **缓存内存化、锁/消息持久化**：按“可丢失性”划分存储，兼顾吞吐与可靠性。
3. **默认广播+即弃**：对齐 MQTT 心智，显式配置才切单投/持久，避免隐式排队带来的堆积。
4. **`lease` + `InflightAcks`**：以“消息级租约 + 内存 ack 等待表”实现至少一次投递，不引入独立的 ack 通道。
5. **HTTP 仪表盘内置**：无额外监控组件，开箱即可观察主题、客户端、消息、缓存、锁。
6. **单一 token 三协议共用**：鉴权逻辑集中一处，降低误配风险。
