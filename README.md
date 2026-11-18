# 机房虚拟机地址租约服务

机房虚拟机启动器从共享的内网 IPv4 地址池领取地址。本服务提供租约 API：
**FastAPI + SQLite（WAL）**，Docker Compose 启动 **两个实例**，挂载同一命名卷
**共享持久数据**（同一个 SQLite 数据库文件）。Swagger UI 可直接完成申请、续租、
释放与归属查询。

## 快速开始

```bash
docker compose up --build
# node1: http://127.0.0.1:8001/docs
# node2: http://127.0.0.1:8002/docs   （两个实例共享同一份租约数据）
```

无 Docker 的本地等价验证（两个进程共享同一个 SQLite 文件）：

```bash
python3 -m pip install -r app/requirements.txt
bash scripts/dev_run_local.sh        # 前台启动 8001/8002 两个实例，Ctrl-C 停止
```

地址池由 `POOL_CIDR` 配置，默认 `10.99.0.0/29`（6 个可用地址，便于演示池满与争抢）。

## API 一览（Swagger: /docs）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/leases` | 申请地址。提交 `vm_id`、`request_id`（申请号）、`ttl_seconds`（1~60 秒）。幂等：同申请号重试返回原租约当前状态（200, `replayed=true`），过期后也不会借旧号再领地址 |
| POST | `/api/leases/{lease_id}/renew` | 续租（需令牌，从当前时刻延长 1~60 秒） |
| POST | `/api/leases/{lease_id}/release` | 释放（需令牌，重复释放幂等） |
| GET | `/api/leases/{lease_id}` | 按租约 ID 查状态（active/expired/released） |
| GET | `/api/requests/{request_id}` | 按申请号查租约与全部申请记录（审计） |
| GET | `/api/addresses/{address}` | 查询地址当前归属与历史 |
| GET | `/api/leases?vm_id=&status=` | 租约台账 |
| GET | `/api/pool` | 地址池总览 |
| GET | `/api/health` | 健康检查 |

错误统一为 `{"detail": {"error": <代码>, "message": <中文说明>, ...}}`：
`pool_exhausted`（地址不足）、`vm_already_leased`（一机一址）、
`token_mismatch`（令牌不符）、`lease_expired` / `lease_released`、`request_id_conflict`。

## 设计要点

- **分配原子性**：所有写操作走 `BEGIN IMMEDIATE` 事务。两个实例共用同一数据库文件，
  写事务在 SQLite 层串行化，“查空闲地址 + 写租约”是原子操作——两个实例争抢最后一个
  地址时只有一方成功，另一方收到 `pool_exhausted`（地址不足）。
- **幂等申请**：`request_id` 有唯一约束，一个申请号终身只对应一条租约。同号重试
  （包括打到另一个实例）返回原租约的当前状态；原租约过期后重试只会看到 `expired`，
  不会借旧号再领地址。
- **一机一址**：申请时在事务内检查该虚拟机是否已持有有效租约，已持有则拒绝并返回现有租约。
- **懒过期**：`expires_at <= now` 即视为过期，地址立即可回收，无需后台清理线程；
  状态（active/expired/released）由字段实时推导。
- **令牌防串**：续租/释放必须携带申请时返回的随机令牌。地址过期易主后，旧令牌迟到的
  续租/释放只命中已终止的旧租约行，不影响新归属。
- **持久化与审计**：`leases`（租约全量历史）与 `request_log`（每次申请的受理结果）
  都落在共享卷的 SQLite 文件中，实例重启后仍可核对。

## 自测

```bash
python3 scripts/selftest.py
```

覆盖：参数校验（1~60 秒）、基本申请、跨实例同号重试、一机一址、池满“地址不足”、
续租（含错误令牌）、释放幂等、过期交错（旧号重试 / 旧令牌迟到 / 地址易主）、
双实例争抢最后一个地址（2 抢 1）、满池争抢（12 抢 6）、申请记录审计。

## 已知限制

- 为便于演示，租约视图（含令牌）对查询接口可见；生产环境应对 GET 接口隐藏令牌并加访问控制。
- `ALLOW_RESET=1` 仅在自测时开启（compose 中默认开启，生产部署请移除）。
- 两个实例须运行在同一台宿主机共享卷上（SQLite 文件锁依赖本地文件系统；
  不支持 NFS 等网络文件系统上的多机共享）。
