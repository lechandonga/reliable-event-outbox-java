# 可靠事件投递链路

本项目用嵌入式 H2 文件数据库实现事务发件箱，并用进程内 `ConcurrentLinkedQueue` 模拟消息中间件，不依赖 Kafka、RabbitMQ、Redis 或外部数据库。

## 端到端流程

1. 业务服务在同一个数据库事务中写入 `business_orders` 和 `outbox_events`，并为每个聚合键分配连续序号。
2. `OutboxRelay` 轮询待投递事件，用乐观锁领取后写入本地内存队列。
3. `OrderEventConsumer` 的工作线程跨聚合并行消费；同一聚合键通过本地互斥锁和 `aggregate_progress` 位点串行处理。
4. 业务副作用、消费去重记录、聚合位点和事件成功状态在同一个新事务中提交。
5. 消费失败后事件回到 `PENDING` 并按指数退避重试；达到上限进入 `DEAD_LETTER`。

## 投递语义

- 业务写入与事件产出是事务原子提交：业务失败则事件回滚，事件失败则业务回滚。
- 事件进入内存队列后采用至少一次投递，崩溃、重复领取或重复入队都可能让同一事件再次到达消费端。
- 消费端以 `consumed_events.event_id` 做事务内幂等，并按聚合位点保证副作用只提交一次。
- 进程在事件投递、消费或状态回写中途退出后，重启时发件箱中仍是 `PENDING` 或超时的 `IN_PROGRESS` 事件，会被重新领取并推进。
- 内存队列只用于模拟 broker，不是事实来源；事实来源始终是持久化发件箱。

## 状态流转

- `PENDING`：业务事务已提交，等待投递或等待退避时间到达。
- `IN_PROGRESS`：投递器已领取，正在投递或等待消费结果。
- `SUCCEEDED`：消费事务提交成功，终态，不允许再次处理。
- `DEAD_LETTER`：成功尝试次数达到配置上限，终态但可人工重放。

每次状态变化都写入 `event_status_history`，包含原状态、目标状态、尝试次数、原因和时间。

## 重试与死信

退避时间为：

```text
min(backoff-max-ms, backoff-initial-ms * backoff-multiplier ^ (attempts - 1))
```

达到 `max-attempts` 后事件进入 `DEAD_LETTER`，记录最后一次错误原因。死信不会被自动静默删除。

- `POST /events/{eventId}/replay`：只允许重放死信；成功事件会返回冲突，不能重复执行业务副作用。
- `POST /events/{eventId}/repair-and-replay`：先修复死信载荷，再重放，适用于演示中载荷持续导致消费失败的场景。
- 重放后事件从 `DEAD_LETTER` 变为 `PENDING`，尝试次数清零，状态历史保留完整轨迹。
- 同一事件重放与正常投递并发时，消费事务依赖事件 ID 幂等和成功终态，已成功事件不会再次应用副作用。

## 顺序保证与边界

- 同一 `aggregate_key` 的事件按 `sequence_no` 升序提交。
- 前一个事件未成功时，后续事件即使先到达消费端，也会被重新放回本地队列等待。
- 一个聚合键正在重试不会阻塞其他聚合键，工作线程按不同聚合键并行推进。
- 顺序保证范围是单实例、单个聚合键；不保证不同聚合键之间的全局顺序。
- 当前内存锁只覆盖单进程。多实例部署时需要把领取锁和聚合串行锁下沉到数据库或真实消息中间件的分区/键顺序消费者。

## 手工接口

- `POST /orders`：创建订单并原子产出事件。
- `POST /test/rollback`：模拟业务事务提交前失败，用于验证业务和事件同时回滚。
- `POST /relay/run`：手工执行一次发件箱投递。
- `POST /consumer/dispatch`：手工把本地内存队列消息交给消费工作线程。
- `GET /events`：查询所有事件状态。
- `GET /events/{eventId}`：查询单个事件。
- `GET /events/{eventId}/history`：查询事件状态流转解释。
- `POST /events/{eventId}/replay`：重放死信。
- `POST /events/{eventId}/repair-and-replay`：修复载荷并重放。
- `GET /projections`：查询消费端投影结果。
- `GET /progress`：查询每个聚合键的最后成功位点。

创建订单请求示例：

```json
{
  "orderId": "order-001",
  "title": "示例订单",
  "failConsumption": false
}
```

将 `failConsumption` 设为 `true` 可制造消费失败，观察退避重试和死信。

## 配置项

| 配置 | 默认值 | 说明 |
| --- | --- | --- |
| `outbox.relay.fixed-delay-ms` | `200` | 自动投递和消费调度间隔 |
| `outbox.relay.batch-size` | `100` | 单轮最多领取/分发的事件数 |
| `outbox.relay.max-attempts` | `5` | 最大尝试次数，达到后进入死信 |
| `outbox.relay.backoff-initial-ms` | `200` | 第一次失败后的退避时间 |
| `outbox.relay.backoff-multiplier` | `2` | 指数退避乘数 |
| `outbox.relay.backoff-max-ms` | `5000` | 单次退避上限 |
| `outbox.relay.worker-threads` | `4` | 跨聚合并行消费线程数 |
| `outbox.relay.in-progress-timeout-ms` | `5000` | 崩溃恢复时间，超时的 `IN_PROGRESS` 可被重新领取 |
| `spring.sql.init.mode` | `always` | 启动时执行 `schema.sql` 建表 |

## 本地验证

```bash
mvn clean test
```

测试覆盖：

- 业务与事件原子提交、事务回滚和异常退出恢复。
- 12 个不同聚合键的并发投递与并行推进。
- 同一事件重复到达后的幂等跳过，以及失败聚合不阻塞其他聚合。
- 退避后进入死信、人工修复载荷并重放。
- 同一聚合键前序事件失败时后序事件等待，重放成功后按序号消费。

测试日志会输出每个“业务”的用例开始信息和通过/失败结果。
