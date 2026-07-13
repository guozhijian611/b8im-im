# b8im-im

b8im IM 运行时仓库，承载 register、gateway、business 等通信层进程。

## 目录

```text
im-register/   RegisterWorker 服务注册发现
im-gateway/    GatewayWorker WebSocket 接入层
im-business/   BusinessWorker 消息、鉴权、ACK、撤回等业务层
```

三个服务都通过 Composer path repository 依赖同级子模块仓库：

```text
../b8im-im-shared
```

从单个服务目录执行 Composer 命令时，相对路径是：

```json
{ "type": "path", "url": "../../b8im-im-shared" }
```

## 本机配置

各服务复制自己的示例配置：

```bash
cp im-register/.env.example im-register/.env
cp im-gateway/.env.example im-gateway/.env
cp im-business/.env.example im-business/.env
```

`im-business` 默认连接本机数据库：

```text
DB_NAME = nb8im
DB_USER = root
DB_PASSWORD = root
```

三个进程统一使用 `IM_TIMEZONE=Asia/Shanghai`。`SECRET_KEY` 是 GatewayWorker
内部传输信任根，必须三端一致、至少 32 字节且不能使用示例占位值；
不符合时进程直接失败关闭。Register 默认只监听 `127.0.0.1:1238`，只有明确的
多机私网部署才应改为其他地址。

命令行显式传入的环境变量优先于 `.env`，因此可以安全使用临时数据库运行迁移和集成测试。

## Docker 构建

IM 三个运行单元共用一个参数化 `Dockerfile`，依赖安装层和
Composer 下载缓存可以重用。建议在 `b8im-dev-workspace` 根目录使用统一脚本：

```bash
./scripts/build-images.sh im
./scripts/build-images.sh --push im
```

如果需要直接调试单个镜像，必须为 Composer path 依赖提供 named context：

```bash
docker buildx build \
  --build-context im-shared=../b8im-im-shared \
  --build-arg IM_SERVICE=im-gateway \
  -t b8im/im-gateway:local .
```

## IM 数据库迁移

IM 表和消息分片由本仓库独立管理，不混入 Server migration。完整部署仍应先执行
`b8im-server` migration，以提供机构、模块和租户授权等 IM 会读取的控制面表；随后执行：

```bash
cd im-business
composer install
composer migration-status
composer migrate
composer migration-status
```

回滚当前 IM migration：

```bash
cd im-business
composer rollback
```

迁移会按首次部署的 `IM_MESSAGE_SHARD_BUCKETS` 创建当月和下月全部分片，并把桶数写入 `im_runtime_config` 作为不可变部署配置。运行时或预建任务发现环境变量与已迁移桶数不一致会失败关闭。跨月前由运维或定时任务执行：

```bash
cd im-business
composer prebuild-shards
```

`im-business` 启动时只做 schema 与当月/下月分片预检。`SEND` / `ACK` / `SYNC` 请求路径绝不执行 `CREATE TABLE` / `ALTER TABLE`；分片缺失时失败关闭并返回 `IM_SHARD_NOT_PREBUILT`。

本 migration 同时拥有第一阶段 Web/IM 真链路所需的用户凭证与资料、好友关系/申请、设备与登录 IP 审计、会话/群资料/成员历史可见周期、会话自定义分组、消息索引、变更流和 outbox 表。开发版只使用新字段口径，例如 `password_hash`、`member_role`、`conversation_remark`，不保留 `password`、`role`、`platform` 等旧字段别名。

## 数据库集成测试隔离

会执行 migration `down` 或写入测试数据的命令不得指向正在被其他服务使用的 `nb8im`。先创建独立临时库、导入 SaiMulti 基线并在该库执行 Server migration，再显式设置并断言目标库：

```bash
cd im-business
DB_NAME=nb8im_im_test IM_EXPECT_DATABASE=nb8im_im_test composer assert-database-target
DB_NAME=nb8im_im_test IM_EXPECT_DATABASE=nb8im_im_test composer migrate
DB_NAME=nb8im_im_test IM_EXPECT_DATABASE=nb8im_im_test composer test-integration
DB_NAME=nb8im_im_test IM_EXPECT_DATABASE=nb8im_im_test composer rollback
```

`assert-database-target` 会分别通过 Business PDO 和 Phinx PDO 执行 `SELECT DATABASE()`；任一连接不是 `IM_EXPECT_DATABASE` 都立即失败。MySQL/WebSocket 集成测试同样强制要求该变量，避免误写默认开发库。

私有附件历史 URL 归一化迁移的契约测试必须使用名为
`nb8im_private_asset_migration_<random>_test` 的独立库，先只迁移到
`20260710030000`，再执行 `composer test-private-asset-migration`。该测试会写入
URL-only 附件、嵌套合并转发和待重试 outbox，然后执行
`20260710050000` 并验证旧 URL 全部清除。

## AUTH 信任域

- IM 只接受 HS256 JWT，`iss` 必须是 `IM_TOKEN_TRUSTED_ISSUERS` 中的稳定 `deployment_id`，`aud` 必须包含 `IM_TOKEN_AUDIENCE=im`。
- token 必须包含非空 `deployment_id`、`exp`、`nbf`、`organization`、`user_id`、`device_id`、`client_id`、`session_id`、`client_family`、`os`；`deployment_id` 必须与 `iss` 一致。Web 唯一口径为 `client_family=web, os=browser`。
- HTTP 控制面签发 token 前需在 `im_auth_session` 写入同一 credential session；IM AUTH 会重新校验机构、用户、设备和会话状态。
- AUTH 后每条已鉴权 cmd 都会重验同一身份链路；仅把成功结果短暂缓存到 `im:auth:active:{organization}:{credential_session_id}`，最长由 `IM_AUTH_REVALIDATE_TTL_SECONDS` 控制。撤销会话时删除该 key 可立即失败关闭，未主动删除时也会在有界 TTL 内生效。
- `im:events:realtime` 中的 session/device 撤销事件只会关闭 `organization + user + device + credential session` 与事件中已提供的 client/connection session 同时匹配的连接；机构停用先写 `im:auth:organization_inactive:{organization}` 强制阻断，再仅遍历关闭该机构的规范 session。缺字段、重复租户位置或旧格式事件直接丢弃，不猜测范围。
- 鉴权成功后 BusinessWorker 生成独立 128-bit 连接 `session_id`，客户端帧里的 `organization` 不参与授权、路由或持久化。
- `im_user_device` 保留当前设备/IP 快照；每次成功 AUTH 和断开都会写入或闭合 `im_user_login_audit`，旧连接断开使用 client/session compare-and-delete，不能清掉新连接。
- 浏览器 WebSocket 握手 Origin 必须命中 `im-gateway/.env` 的 `IM_TRUSTED_ORIGINS`。原生 App/Desktop 可不携带 Origin，但仍必须通过 JWT 鉴权。

## 租户 IM 运行策略

- AUTH 从 `sm_tenant_im_policy` 校验客户端形态、帐号席位、同设备/跨设备登录策略和最大在线设备数；同账号并发 AUTH 使用 5 秒 Redis reservation 串行决策。
- SEND 在写库前获取 Redis Lua QPS/并发租约，无论成功或异常都释放；策略、MySQL 或 Redis 不可用时失败关闭。
- 撤回/编辑时间窗和撤回提醒只读 canonical `sm_tenant_im_policy`，不再从旧 `message_config` 口径取值。
- `tenant.policy.changed` 先失效策略缓存，再仅断开当前 organization 的规范 session，客户端重连后必须重新通过策略决策。

## 模块授权执行边界

- 模块扩展 cmd 在执行 handler 前同时要求 `sm_module.status=ENABLED`、`sm_tenant_module_license.status=ENABLED`、授权未过期且 manifest 包含 `im` 平台；任何数据库或缓存异常都失败关闭。
- Server 与 IM 共用无框架前缀的物理 Redis key `module_license:{organization}:{module_key}` 和 JSON 快照字段；缓存 TTL 最长 300 秒且不越过 `effective_until`。控制面提交后写入权威新快照，Redis 只接受单调递增的 `(module_lock_version, license version)`，提交前读到旧状态的迟到写入不能重新放开权限。

## 消息同步

- 用户级离线同步只接受十进制字符串 `after_global_seq`，返回字符串 `next_after_global_seq`。
- 会话同步同时使用 `after_seq` 与 `after_change_seq`，分别返回 `messages_has_more` 与 `changes_has_more`。
- 全局和会话消息读取都按 `im_conversation_membership_period` 的历史可见区间过滤，不以当前成员状态代替历史授权。
- `edit`、`recall`、`delete_both`、`delete_self` 在修改主体的同一事务内写 `im_message_change` 和可靠 outbox；目标用户事件被过滤时扫描游标仍推进。
- Outbox claim 绑定 `worker_id + claim_token + locked_until`；只有当前 claim 能写发布结果，RabbitMQ publisher confirm 成功后才置为已发布，过期 claim 可回收并按至少一次语义重投。
- 图片/文件/语音/视频消息只接受 Server 签发的 `file_id`。Business 会以 `organization + user_id + file_id + kind` 回查 `im_upload_asset`，并用数据库中的 URL、MIME 和大小覆盖客户端内容，不接受任意外链。

## RabbitMQ 实时投递

`ImRealtimeDelivery` 是与 BusinessWorker 隔离的独立 Workerman 进程，消费 durable `im.message.after`。它只接受
`message.created`、`message.recalled`、`message.edited`、`message.deleted_both` 和
`message.deleted_self`，并且要求 JSON `event_type` 与 RabbitMQ routing key 一致。持久化消息事件
不再由 BusinessWorker 直接向收件人二次广播：发起连接只收到请求 ACK，
RabbitMQ 消费者是唯一实时业务事件投递路径。

- created 的收件人取 payload 与当前活跃、且在原消息 `message_seq` 可见周期内成员的交集；普通发送事件同时投递给发送者的其他在线连接，但排除本次发起连接。
- `deleted_self` 只发给 `target_user_id`，不会扩散给其他会话成员。
- 独立进程显式设置 `Gateway::$registerAddress` 和内部密钥，先按 `organization:user_id` 查询当前 client，每个 Gateway 写入成功后在 Redis 记录 `event_id + client_id` 投递检查点，全部成功后才 ACK RabbitMQ。
- 投递异常以 `organization + message_id + event_type + change_seq` 在 Redis 原子计数，超过 `MQ_REALTIME_MAX_RETRY` 后 reject 至既有 DLX/DLQ；坏 JSON、不支持的事件和 schema 冲突不会被静默 ACK。
- packet 显式携带稳定 `event_id`、`message_seq` 和 `change_seq`，客户端必须以事件与序号幂等去重。RabbitMQ 仍是至少一次语义；进程恰好在 Gateway 写入成功后、检查点持久化前崩溃时仍可能重复，但不会因预先标记而丢失事件。

## OpenTelemetry Trace

- WebSocket JSON 帧顶层可选携带 W3C `traceparent` / `tracestate`；只接受严格的 version 00 格式，不支持 baggage，不从业务 `data` 读取 trace 上下文。
- 每个 AUTH/SEND/ACK/SYNC 等客户端命令创建独立 server span；ACK/ERROR 使用同一 trace 的新 span context 回传。
- 消息主体、`im_message_index` 与 `im_message_outbox` 仍在同一 MySQL 事务内写入。Outbox 只用独立 `traceparent` / `tracestate` 列保存因果上下文，不从 payload 兼容读取。
- Publisher 每次尝试都创建新 PRODUCER span，并注入 RabbitMQ `application_headers`；Consumer 提取后创建新 CONSUMER span，RealtimeDelivery 与每次 Gateway PUSH 继续生成新 span id。
- 业务索引属性统一使用 `b8im.organization` / `b8im.message_id` / `b8im.conversation_id` / `b8im.client_msg_id` / `b8im.outbox_id` / `b8im.event_id`，不双写旧字段。资源中 `service.name` 由进程受信代码固定，`OTEL_SERVICE_NAME` 不能覆盖；`OTEL_SERVICE_VERSION` 只接受 1..64 位安全标识字符。
- OTLP 固定使用 HTTP/protobuf，默认只上报 `otel-collector`；超时统一使用 OTel 标准毫秒环境变量 `OTEL_EXPORTER_OTLP_TRACES_TIMEOUT`。长驻进程使用有界 batch queue 和定时 flush；Exporter/Collector 超时或不可用时丢弃 telemetry 并限频告警，不回滚事务、不 NACK 正常消息、不阻断 PUSH。
- Span 和 exception event 不采集 Authorization、Cookie、密码、token、消息正文、完整请求/响应体、带参 SQL、附件 URL 或 secret。

## IM 可靠性真链路回归

`im-business/tests/run_live_reliability.sh` 是本机与测试环境共用的统一入口，不使用
mock。每次运行使用 `qa-im-<QA_RUN_ID>-*` 幂等 ID 和 `[QA:<QA_RUN_ID>]` 内容标记，
并生成只包含 QA 用户和本次消息 ID 的 JSON manifest。

真实 WebSocket 部分覆盖：

- A 发送、`SEND_ACK`、RabbitMQ `PUSH`，且发起连接不自回声。
- 同一 `client_msg_id` 重发返回原消息且不产生第二次 PUSH。
- B 送达 `ACK / ACK_ACK`，重复 ACK 不增加回执行且状态不回退。
- B 断线期间连续消息的会话序号连续、机构全局序号单调，重连 AUTH 后依靠
  SYNC 无缺口恢复。
- 客户端伪造 organization 不影响服务端身份，跨 organization 收件人被拒绝。

数据和基础设施审计覆盖：消息全局索引与分片行一致、幂等唯一索引存在、
回执单行、outbox 已发布且 claim 已释放、RabbitMQ ready/DLQ/消费者阈值和指定日志的
严重错误特征。

本机全链路（默认成功后精确清理本次 QA 消息）：

```bash
cd im-business
API=http://127.0.0.1:18888 \
WS_URL=ws://127.0.0.1:18787 \
ORIGIN=http://127.0.0.1:16988 \
ORGANIZATION=1 OTHER_ORGANIZATION=2 \
A_ACCOUNT=qa_im_a A_PASSWORD='<qa-password>' \
B_ACCOUNT=qa_im_b B_PASSWORD='<qa-password>' \
X_ACCOUNT=qa_im_x X_PASSWORD='<qa-password>' \
QA_LOG_FILES=/path/to/im-business.log,/path/to/im-realtime.log \
composer test-live-reliability
```

测试环境先在可访问公网 API/WebSocket 的机器运行协议回归，再把 manifest 复制到
SSH `b8im` 中的 `im-business` 容器执行数据库/RabbitMQ/日志审计和清理：

```bash
# 公网协议链路
QA_CLEANUP_AFTER=0 API=https://api.example.test WS_URL=wss://api.example.test/_wukongim_ws \
  ORIGIN=https://web.example.test ORGANIZATION=1 OTHER_ORGANIZATION=2 \
  A_ACCOUNT=qa_im_a A_PASSWORD='<qa-password>' B_ACCOUNT=qa_im_b B_PASSWORD='<qa-password>' \
  X_ACCOUNT=qa_im_x X_PASSWORD='<qa-password>' \
  composer test-live-reliability-websocket

# 在 IM 运行环境中使用上一步输出的绝对 manifest 路径
QA_MANIFEST=/tmp/b8im-im-reliability-<run-id>.json composer test-live-reliability-audit
QA_MANIFEST=/tmp/b8im-im-reliability-<run-id>.json composer test-live-reliability-cleanup
```

审计默认要求 `im.message.after` ready=0、DLQ=0 且至少一个消费者。有已知基线积压时可显式设置
`QA_RABBIT_MAX_READY`、`QA_RABBIT_MAX_DLX`、`QA_RABBIT_MIN_CONSUMERS`，不允许脚本自动忽略。
清理命令只接受 manifest 中的精确 `message_id` 且会二次核对 `qa-im-<QA_RUN_ID>-`
前缀；对话中存在非本次 manifest 消息或非 QA 成员时将拒绝执行，因此每次回归前要重置
专用 QA 账号。该命令不删除测试账号，账号创建/重置与删除由控制面 QA 命令负责。
