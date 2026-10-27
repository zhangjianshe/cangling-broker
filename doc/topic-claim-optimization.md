# 单 topic 大量积压时的消息认领优化

2026-10-26，本地工作区版本 0.1.69。现场截图为 0.1.64，本次修改尚未部署到现场。

## 修改

原认领 SQL 按 `next_attempt_at <= now` 筛选，按 `created_at` 取最早的一条。原索引
`(topic, status, next_attempt_at, created_at)` 中，范围条件位于排序字段之前，实际查询计划
出现 `USE TEMP B-TREE FOR ORDER BY`。单 topic 积压较大时，每次认领反复扫描并排序候选记录。

在全部 16 个消息分片启动时自动执行：

```sql
CREATE INDEX IF NOT EXISTS idx_messages_pending_topic_created
ON messages(topic, created_at, next_attempt_at, id)
WHERE status = 'pending';
```

这个索引只包含待处理消息，覆盖候选查询需要的全部字段，并按创建时间排列。
精确 topic 的认领子查询通过 `INDEXED BY` 固定使用该索引，避免统计信息变化后重新选择
原来的范围索引。该路径保留原子 `UPDATE ... RETURNING`、`status = 'pending'` 条件、
创建时间顺序、尝试次数累加和租约更新。

已有分片自动补建索引，重复启动不会重复创建。管理库中的历史回滚表不参与认领，未增加该索引。
首次启动会产生索引构建时间和磁盘占用。通配订阅及 HTTP 回退查询保持现有实现。

维持创建时间顺序意味着：若较早的消息大量处于未来重试时间，查询仍需要跳过这些索引记录。
本次性能对比的负载是单 topic 中全部积压消息均已到可投递时间。

## 验证方法

```powershell
python .test/benchmark_topic_claim.py --messages 143654 --claims 64 --drain
```

脚本直接提取 `src/db.rs` 中的分片表结构、新索引和生产认领 SQL；旧 SQL 由新 SQL 移除
索引提示得到，在未创建新索引的数据库中执行。数据通过 SQLite backup 复制，使前后测试
使用相同的初始消息集。所有数据库均为本机临时文件，结束后自动清理，不连接任何运行中的 broker。

环境：Python 3.12、SQLite 3.45.1、磁盘文件、WAL、`synchronous=NORMAL`，单 topic
143654 条消息，每条 payload 为 256 字节。每组采样 64 次完整的原子认领，包括隐式事务提交。

## 实测结果

| 项目 | 修改前 | 修改后 |
| --- | ---: | ---: |
| 平均每次认领 | 185.3894ms | 0.2372ms |
| 中位数 | 175.9380ms | 0.1212ms |
| 查询计划临时排序 | 有 | 无 |

平均认领耗时降低约 782 倍。新索引构建耗时约 0.789 秒。

优化后的查询计划包含：

```text
SEARCH messages USING COVERING INDEX idx_messages_pending_topic_created (topic=?)
```

采样认领并确认 64 条后，继续处理剩余 143590 条，每 32 条批量更新确认状态：

- 清空剩余消息用时 70.687 秒，约 2031 条/秒。
- 最终 `delivered = 143654`，`pending = 0`，`processing = 0`。
- 每条认领结果均核对了预期消息 ID 和创建时间顺序。

这是直接执行数据库 SQL 的本地结果，不是包含 gRPC、Tokio 调度及真实消费者的端到端吞吐。
磁盘、缓存和运行负载会影响数值；部署后的实际吞吐仍需在目标存储上复测。

## 正确性回归

Python 验证已通过：已有数据库补索引、重复建索引、覆盖索引与无临时排序、创建时间顺序、
未来重试排除及重新到期、非 pending 状态排除、topic 隔离、attempts/lease 更新、旧 lease
确认拒绝、空队列，以及 8 个独立数据库连接竞争一条消息时只有一个成功。

`src/db.rs` 还增加了 Rust 回归测试，覆盖 SQLx 查询计划、16 个已有分片补索引，以及认领顺序和
重试时间语义。原有并发认领测试继续保留。

## Rust 环境与检查命令

已在 `Ubuntu-Preview` WSL（Ubuntu 24.04、x86_64）中通过
[官方 rustup](https://rust-lang.org/tools/install/) 安装 Rust/Cargo 1.88.0，与项目 CI 一致；
同时安装了 Clippy、rustfmt、GCC/G++、make 和 pkg-config。
工具链位于 WSL 用户的 `~/.cargo` 和 `~/.rustup`，新登录的 Bash 会话可直接调用。

从 Windows PowerShell 进入项目：

```powershell
wsl -d Ubuntu-Preview --cd /mnt/f/workSpace/air/cis/git/cangling-broker
```

在 WSL 中执行：

```bash
export CARGO_TARGET_DIR="$HOME/.cache/cangling-broker-target"
cargo test --locked -j 8 -- --test-threads=4
cargo check --locked --all-targets -j 8
cargo clippy --locked --all-targets -j 8 -- -D warnings
cargo build --release --locked --bin cangling-broker --example receiver -j 8
rustfmt --edition 2021 --check src/db.rs
```

测试并行数限制为 4，以控制每个测试创建 16 个 SQLite 分片时的资源使用；并发认领测试内部
仍保留原有并发任务。

构建缓存放在 WSL 本地磁盘，避免 Windows 挂载目录的大量编译/链接 I/O。
Release 产物另复制到项目中的 `target/release/cangling-broker` 和
`target/release/examples/receiver`；两者均为 Linux x86_64 可执行文件。
检查日志位于项目的 `target/verification/`。

## Rust 实际验证结果

2026-10-26，以上工具链下的验证均已完成：

| 检查 | 结果 | 日志 |
| --- | --- | --- |
| 完整回归测试 | 81 passed，0 failed，0 ignored；测试执行 32.08 秒 | `target/verification/tests.log` |
| `cargo check --locked --all-targets` | 通过 | `target/verification/check.log` |
| `cargo clippy --locked --all-targets -- -D warnings` | 通过，无警告 | `target/verification/clippy.log` |
| broker 与 receiver 的 Release 构建 | 通过 | `target/verification/release.log` |
| `rustfmt --edition 2021 --check src/db.rs` | 通过 | `target/verification/format.log` |
| 两个 Release 程序的 `--help` 启动检查 | 均以退出码 0 结束 | `target/verification/*-help.txt` |

本次新增的三个 Rust 回归测试均通过：

- `topic_claim_uses_covering_index_without_sorting`：SQLx 实际查询计划使用覆盖索引，没有临时排序。
- `reopening_existing_shards_adds_topic_claim_index`：16 个已有分片均自动补建索引，原有消息仍可认领。
- `topic_claim_keeps_creation_order_and_skips_unready_messages`：保持创建时间顺序，排除未来重试及非
  pending 消息，并验证重试重新到期后可以认领。

原有的多任务竞争认领、批量写入与确认、幂等、MQTT、状态接口和认证测试也全部通过。
编译与检查阶段只修正了本次 Rust 改动的四处格式差异，没有发现需要修改业务逻辑的错误。
以上结果确认了本地 Linux x86_64 环境中的回归与构建状态，尚未部署或进行现场端到端压测。

## Docker 镜像与离线包

2026-10-26，已在 WSL Docker Engine 中按仓库 `Dockerfile` 完成多阶段构建。
编译阶段使用 `rust:1.88-bookworm`，运行阶段使用 `debian:bookworm-slim`。
最终镜像为 `hub.cangling.cn/cangling/cangling-broker:latest`，另有 `:0.1.70` 标签，
平台为 `linux/amd64`，程序内部版本为 `0.1.70`。首次打包曾使用 `0.1.69`，现已同步递增
Rust、Cargo.lock、Java、Python 的项目版本并重新编译，通过容器 `/status` 核实新版本。
由于本目录不含 `.git`，构建标识设置为 `local-topic-claim`，并非 Git 提交号。

在 PowerShell 中重新构建：

```powershell
wsl -d Ubuntu-Preview -u root --cd /mnt/f/workSpace/air/cis/git/cangling-broker --exec docker build -f Dockerfile --build-arg GIT_HASH=local-topic-claim -t hub.cangling.cn/cangling/cangling-broker:latest -t hub.cangling.cn/cangling/cangling-broker:0.1.70 .
```

本次重建遇到镜像加速源对 Dockerfile 外部解析器返回 403，因此实际构建使用
`target/verification/Dockerfile.cached`：保留原构建指令，固定上次使用的基础镜像摘要，
使用 Docker 内置解析器。原始 Dockerfile 和 Docker 服务配置未改动。

已导出的离线包：

```text
target/images/cangling-broker-0.1.70-linux-amd64.tar.gz
SHA256: 765c0c41b96f1968c05374197a36c067d44613d4ea46e1eb55d4b3e9506516d0
```

同一文件已复制到 `C:\Users\Baoshuai\Desktop\cangling-broker-0.1.70-linux-amd64.tar.gz`，
并核对复制前后的 SHA256 一致。镜像包包含用户指定的 `hub.cangling.cn/cangling/cangling-broker:latest` 标签。

将文件复制到 Linux amd64 服务器后导入：

```bash
docker load -i cangling-broker-0.1.70-linux-amd64.tar.gz
docker image inspect hub.cangling.cn/cangling/cangling-broker:latest
```

导入后可在现有部署配置中使用上述镜像标签；实际部署时保留原有数据卷、端口和认证配置。

镜像中的程序已通过 `--help` 和实际启动验证。另用独立临时容器完成以下检查：

- 以 UID 10001 运行，健康接口返回 `{"ok":true}`。
- 通过 gRPC 将 128 条消息写入一个 persistent/single topic；无消费者时待处理为 128。
- 重启容器后待处理仍为 128；订阅后逐条核对消息 ID、payload 和 lease，分批 ACK。
- 最终 delivered 为 128，pending、processing 均为 0；16 个分片均有新索引。
- 临时容器已删除；离线包通过 gzip 完整性检查与实际 `docker load` 导入验证，镜像 ID 一致。

0.1.70 完整 Rust 回归重跑结果为 81 passed、0 failed，日志为 `target/verification/tests-0.1.70.log`。
构建日志、容器验证结果和镜像元数据位于 `target/verification/docker-*0.1.70*`，
校验文件位于 `target/images/cangling-broker-0.1.70-linux-amd64.tar.gz.sha256`。
这是镜像功能验证，非 14 万条消息的端到端压测。

## GitHub 自动编译的触发方式

当前 `.github/workflows/ci.yml` 仅监听 `push.tags: ["v*"]`，普通分支提交、分支推送和 PR
不会触发此工作流，也未配置手动运行入口。

推送新的 `v*` 标签后，工作流会测试、Clippy 检查并编译 amd64/arm64 broker，使用
`Dockerfile.ci` 打包双架构镜像，推送 Docker Hub 和 Harbor，并发布 Java/Python SDK。
发布是否成功还取决于仓库 Actions 是否启用以及所需 Secrets 是否配置有效，当前本地无法确认。

`release.sh` 会先执行检查、同步递增 Rust/Java/Python 版本，再创建提交和标签并推送。
当前工作目录没有 Git 元数据；若采用该发布途径，需要先将本次源码改动合入实际 Git 克隆仓库，
并核对发布版本。仅把源码普通提交到 GitHub，按现有配置不会自动编译。
