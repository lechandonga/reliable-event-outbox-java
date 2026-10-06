# reliable-event-outbox-java

基于 **Transactional Outbox** 模式的可靠事件投递链路（Spring Boot 3 / Java 21）。
业务数据写入时产出的领域事件被异步、最终一致地消费，全程仅依赖内嵌 H2 数据库与内存队列，
不依赖任何外部服务。

## 架构与投递语义

```
业务写入 ──同事务──> orders 表 + outbox_event 表 (H2, 本地文件持久化)
                            │  OutboxDispatcher 定时轮询 (NEW/RETRY 到期事件)
                            ▼
                  InMemoryMessageBroker (内存队列, 模拟消息中间件)
                            │  OutboxConsumer 多线程消费 (按聚合键串行)
                            ▼
                     DomainEventHandler (业务处理, 幂等)
```

- **原子提交**：业务行与事件在同一数据库事务中写入（`OrderService.createOrder`），
  不存在"业务已存事件丢失"或"事件已发业务未提交"的中间态。
- **投递语义**：at-least-once。事件可能重复到达，消费端通过 `processed_event` 表
  （event_id 主键）识别并跳过重复投递，业务逻辑只执行一次。
- **崩溃恢复**：进程退出时停留在 `DISPATCHED` 的事件在启动时（或手工触发）被重置为
  `NEW` 重新派发，配合消费端幂等保证不丢不重。

## 事件状态机

| 状态 | 含义 |
|---|---|
| `NEW` | 已写入，等待派发 |
| `DISPATCHED` | 已被派发器乐观认领，投递途中 |
| `RETRY` | 投递失败，等待按退避时间重试（`next_retry_at` 到期后重新派发） |
| `DONE` | 消费成功，终态，不可重放 |
| `DEAD` | 超过 `outbox.max-attempts` 进入死信，仅支持人工重放（`DEAD -> NEW`） |

每条事件的 `attempts`、`last_error`、`next_retry_at`、时间戳均落库可查。

## 重试与死信策略

- 失败后按指数退避重试：`delay = min(backoff-initial-ms * multiplier^(attempts-1), backoff-max-ms)`。
- 达到 `max-attempts`（含首次）后进入 `DEAD`，不再自动投递，等待人工重放。
- 重放仅允许 `DEAD -> NEW` 且清零尝试次数；`DONE` 事件拒绝重放（返回 409），
  死信重放后重新走完整状态机，不会被静默丢弃。

## 幂等与顺序保证的边界

- **幂等**：以 event_id 为粒度。处理前先插入 `processed_event`，主键冲突即跳过；
  处理失败会撤销该记录以便重试真正重跑业务逻辑。处理器实现自身也应尽量幂等。
- **顺序**：仅保证**同一聚合键**内按产生顺序（`seq` 单调递增）消费——派发器对同一键
  只派发最靠前的一条未完成事件，消费端按聚合键加条纹锁串行执行。
  不同聚合键之间不保证顺序、并行推进；某键的事件重试/退避不会拖住其他键。
  注意：同键内前序事件进入死信后，后续事件会被阻塞，直到死信被重放或人工处理。

## 配置项（`outbox.*`）

| 配置 | 默认值 | 说明 |
|---|---|---|
| `outbox.poll-interval-ms` | 500 | 派发轮询间隔 |
| `outbox.batch-size` | 50 | 单次轮询最大捞取数 |
| `outbox.consumer-threads` | 4 | 消费工作线程数 |
| `outbox.max-attempts` | 5 | 最大尝试次数（含首次），超过进入死信 |
| `outbox.backoff-initial-ms` | 1000 | 首次重试退避 |
| `outbox.backoff-multiplier` | 2.0 | 退避倍数 |
| `outbox.backoff-max-ms` | 30000 | 退避上限 |

数据库默认使用本地文件 H2：`./data/outbox`（`spring.datasource.url` 可改）。

## 本地运行与手工入口

```bash
mvn spring-boot:run
```

| 方法/路径 | 说明 |
|---|---|
| `POST /orders` | 业务写入（body: `{"aggregateKey":"k1","eventType":"ORDER_CREATED","payload":"{}"}`） |
| `GET /outbox/events` | 查询全部事件及状态 |
| `GET /outbox/events/{id}` | 查询单条事件状态流转 |
| `GET /outbox/dead` | 死信列表 |
| `GET /outbox/stats` | 各状态计数 |
| `POST /outbox/dispatch` | 手工触发一次派发 |
| `POST /outbox/events/{id}/replay` | 人工重放死信事件 |
| `POST /outbox/recover` | 手工触发崩溃恢复（DISPATCHED -> NEW） |

故障注入（演示/测试用事件类型）：`FLAKY:<n>` 前 n 次处理失败后成功；`FAIL_ALWAYS` 永远失败。

## 测试

```bash
mvn test
```

`OutboxIntegrationTest` 覆盖 7 个业务场景：原子提交+异步投递、并发投递、重复投递幂等、
异常退出恢复、失败重试与退避、死信与人工重放、按聚合键顺序保持与故障隔离。
测试日志以 `=== 场景N ===` 与 `用例: ... [通过]` 标注各场景覆盖点与结果。
