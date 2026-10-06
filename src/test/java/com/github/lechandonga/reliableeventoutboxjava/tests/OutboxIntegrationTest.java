package com.github.lechandonga.reliableeventoutboxjava.tests;

import com.github.lechandonga.reliableeventoutboxjava.order.OrderService;
import com.github.lechandonga.reliableeventoutboxjava.outbox.OrderEventHandler;
import com.github.lechandonga.reliableeventoutboxjava.outbox.OutboxConsumer;
import com.github.lechandonga.reliableeventoutboxjava.outbox.OutboxEvent;
import com.github.lechandonga.reliableeventoutboxjava.outbox.OutboxEvent.OutboxStatus;
import com.github.lechandonga.reliableeventoutboxjava.outbox.OutboxEventRepository;
import com.github.lechandonga.reliableeventoutboxjava.outbox.OutboxRecovery;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 可靠事件投递链路集成测试，覆盖：
 * 1. 业务写入与事件产出的原子提交 + 异步投递
 * 2. 并发投递
 * 3. 重复投递的幂等识别
 * 4. 异常退出后的恢复
 * 5. 失败重试与退避
 * 6. 死信与人工重放
 * 7. 按聚合键的顺序保持与故障隔离
 */
@SpringBootTest(properties = {
        "spring.datasource.url=jdbc:h2:mem:outboxtest;DB_CLOSE_DELAY=-1",
        "outbox.poll-interval-ms=50",
        "outbox.consumer-threads=4",
        "outbox.max-attempts=3",
        "outbox.backoff-initial-ms=50",
        "outbox.backoff-multiplier=2.0",
        "outbox.backoff-max-ms=500"
})
class OutboxIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(OutboxIntegrationTest.class);

    @Autowired OrderService orderService;
    @Autowired OutboxEventRepository repository;
    @Autowired OutboxConsumer consumer;
    @Autowired OutboxRecovery recovery;
    @Autowired OrderEventHandler handler;

    @Test
    @DisplayName("业务写入与事件原子提交，事件被异步消费至 DONE")
    void atomicCommitAndAsyncDelivery() throws Exception {
        log.info("=== 场景1: 原子提交 + 异步投递 ===");
        String key = "order-" + UUID.randomUUID();
        OrderService.CreatedOrder created = orderService.createOrder(key, "ORDER_CREATED", "{\"amount\":100}");

        OutboxEvent saved = repository.findById(created.eventId()).orElseThrow();
        assertEquals(OutboxStatus.NEW, saved.status());
        log.info("用例: 业务与事件同事务提交 -> 事件落库状态={} [通过]", saved.status());

        await(() -> statusOf(created.eventId()) == OutboxStatus.DONE, 10000);
        log.info("用例: 异步投递完成 -> 状态=DONE, handler 已处理 [通过]");
    }

    @Test
    @DisplayName("并发投递：多聚合键批量写入全部投递成功")
    void concurrentDelivery() throws Exception {
        log.info("=== 场景2: 并发投递 ===");
        List<String> eventIds = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            eventIds.add(orderService.createOrder("key-" + (i % 5), "ORDER_CREATED", "{}").eventId());
        }
        for (String id : eventIds) {
            await(() -> statusOf(id) == OutboxStatus.DONE, 10000);
        }
        log.info("用例: 20 条事件跨 5 个聚合键并发投递全部 DONE [通过]");
    }

    @Test
    @DisplayName("重复投递：同一事件到达两次，业务逻辑只执行一次")
    void duplicateDeliveryIsIdempotent() throws Exception {
        log.info("=== 场景3: 重复投递幂等 ===");
        String key = "dup-" + UUID.randomUUID();
        String eventId = orderService.createOrder(key, "ORDER_CREATED", "{}").eventId();
        await(() -> statusOf(eventId) == OutboxStatus.DONE, 10000);

        long handledBefore = handler.handledEvents().stream().filter(e -> e.id().equals(eventId)).count();
        // 模拟消息中间件重复投递：把同一事件再次交给消费者
        consumer.process(repository.findById(eventId).orElseThrow());

        long handledAfter = handler.handledEvents().stream().filter(e -> e.id().equals(eventId)).count();
        assertEquals(1, handledBefore);
        assertEquals(1, handledAfter);
        assertEquals(OutboxStatus.DONE, statusOf(eventId));
        log.info("用例: 重复投递被识别并跳过, handler 执行次数=1 [通过]");
    }

    @Test
    @DisplayName("异常退出恢复：卡在 DISPATCHED 的事件重启后重新推进")
    void crashRecovery() throws Exception {
        log.info("=== 场景4: 异常退出后恢复 ===");
        String key = "crash-" + UUID.randomUUID();
        String eventId = orderService.createOrder(key, "ORDER_CREATED", "{}").eventId();

        // 模拟进程在事件被认领后、完成前退出
        assertTrue(repository.claim(eventId, System.currentTimeMillis()));
        assertEquals(OutboxStatus.DISPATCHED, statusOf(eventId));
        log.info("用例: 模拟崩溃, 事件卡在 DISPATCHED [就绪]");

        int recovered = recovery.recoverStuckEvents();
        assertTrue(recovered >= 1);
        assertEquals(OutboxStatus.NEW, statusOf(eventId));
        log.info("用例: 启动恢复, DISPATCHED -> NEW [通过]");

        await(() -> statusOf(eventId) == OutboxStatus.DONE, 10000);
        log.info("用例: 恢复后事件继续推进至 DONE [通过]");
    }

    @Test
    @DisplayName("失败重试：瞬时故障按退避重试后成功")
    void retryWithBackoff() throws Exception {
        log.info("=== 场景5: 失败重试与退避 ===");
        String key = "flaky-" + UUID.randomUUID();
        String eventId = orderService.createOrder(key, "FLAKY:2", "{}").eventId();

        await(() -> statusOf(eventId) == OutboxStatus.DONE, 10000);
        OutboxEvent done = repository.findById(eventId).orElseThrow();
        assertEquals(2, done.attempts());
        log.info("用例: 前 2 次失败按退避重试, 第 3 次成功, attempts={} [通过]", done.attempts());
    }

    @Test
    @DisplayName("死信与重放：超过次数上限进入 DEAD，人工重放后成功且不被静默丢弃")
    void deadLetterAndReplay() throws Exception {
        log.info("=== 场景6: 死信与人工重放 ===");
        String key = "dead-" + UUID.randomUUID();
        // FLAKY:5 超过 maxAttempts=3，必然进入死信
        String eventId = orderService.createOrder(key, "FLAKY:5", "{}").eventId();
        await(() -> statusOf(eventId) == OutboxStatus.DEAD, 15000);
        OutboxEvent dead = repository.findById(eventId).orElseThrow();
        assertEquals(3, dead.attempts());
        assertTrue(repository.findByStatus(OutboxStatus.DEAD).stream().anyMatch(e -> e.id().equals(eventId)));
        log.info("用例: 超过 maxAttempts=3 进入 DEAD, 死信列表可查 [通过]");

        // 人工重放：DEAD -> NEW，故障预算已耗尽，重放后应成功
        assertTrue(repository.replay(eventId, System.currentTimeMillis()));
        assertEquals(OutboxStatus.NEW, statusOf(eventId));
        await(() -> statusOf(eventId) == OutboxStatus.DONE, 10000);
        log.info("用例: 人工重放 DEAD -> NEW -> DONE, 未被静默丢弃 [通过]");

        // 已成功的事件不得被重放
        assertFalse(repository.replay(eventId, System.currentTimeMillis()));
        assertEquals(OutboxStatus.DONE, statusOf(eventId));
        log.info("用例: DONE 事件拒绝重放, 状态保持 DONE [通过]");
    }

    @Test
    @DisplayName("顺序保持与故障隔离：同键按序消费，单键重试不阻塞其他键")
    void orderingAndIsolation() throws Exception {
        log.info("=== 场景7: 顺序保持与故障隔离 ===");
        String orderedKey = "ordered-" + UUID.randomUUID();
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            ids.add(orderService.createOrder(orderedKey, "ORDER_CREATED", "{\"n\":" + i + "}").eventId());
        }
        for (String id : ids) {
            await(() -> statusOf(id) == OutboxStatus.DONE, 10000);
        }
        List<Long> handledSeqs = handler.handledEvents().stream()
                .filter(e -> e.aggregateKey().equals(orderedKey))
                .map(OutboxEvent::seq).toList();
        assertEquals(List.of(1L, 2L, 3L, 4L, 5L), handledSeqs);
        log.info("用例: 同一聚合键 5 条事件按 seq 顺序 {} 被消费 [通过]", handledSeqs);

        // 故障键持续重试时，健康键照常推进
        String badKey = "bad-" + UUID.randomUUID();
        String goodKey = "good-" + UUID.randomUUID();
        String badId = orderService.createOrder(badKey, "FLAKY:2", "{}").eventId();
        String goodId = orderService.createOrder(goodKey, "ORDER_CREATED", "{}").eventId();
        await(() -> statusOf(goodId) == OutboxStatus.DONE, 10000);
        log.info("用例: 故障键退避期间, 健康键事件已 DONE (bad 状态={}) [通过]", statusOf(badId));
        await(() -> statusOf(badId) == OutboxStatus.DONE, 10000);
    }

    private OutboxStatus statusOf(String eventId) {
        return repository.findById(eventId).orElseThrow().status();
    }

    private void await(Supplier<Boolean> condition, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.get()) return;
            Thread.sleep(25);
        }
        assertTrue(condition.get(), "condition not met within " + timeoutMs + "ms");
    }
}
