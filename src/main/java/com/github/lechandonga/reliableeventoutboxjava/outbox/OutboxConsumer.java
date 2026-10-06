package com.github.lechandonga.reliableeventoutboxjava.outbox;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * 消费工作器：从内存消息中间件取事件并处理。
 *
 * 幂等：处理前先向 processed_event 表插入 event_id，主键冲突说明是重复投递，直接跳过。
 * 顺序：按聚合键加条纹锁，同一键的事件串行处理；不同键并行，单键重试不会拖住其他键。
 */
@Component
public class OutboxConsumer {

    private static final Logger log = LoggerFactory.getLogger(OutboxConsumer.class);
    private static final int LOCK_STRIPES = 64;

    private final InMemoryMessageBroker broker;
    private final OutboxEventRepository repository;
    private final JdbcTemplate jdbc;
    private final OutboxProperties properties;
    private final List<DomainEventHandler> handlers;
    private final Object[] keyLocks = new Object[LOCK_STRIPES];

    private ExecutorService executor;
    private volatile boolean running;

    public OutboxConsumer(InMemoryMessageBroker broker,
                          OutboxEventRepository repository,
                          JdbcTemplate jdbc,
                          OutboxProperties properties,
                          List<DomainEventHandler> handlers) {
        this.broker = broker;
        this.repository = repository;
        this.jdbc = jdbc;
        this.properties = properties;
        this.handlers = handlers;
        for (int i = 0; i < LOCK_STRIPES; i++) {
            keyLocks[i] = new Object();
        }
    }

    @PostConstruct
    public void start() {
        running = true;
        executor = Executors.newFixedThreadPool(properties.getConsumerThreads());
        for (int i = 0; i < properties.getConsumerThreads(); i++) {
            executor.submit(this::consumeLoop);
        }
    }

    @PreDestroy
    public void stop() {
        running = false;
        if (executor != null) {
            executor.shutdownNow();
            try {
                executor.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void consumeLoop() {
        while (running) {
            try {
                OutboxEvent event = broker.take(200);
                if (event != null) {
                    process(event);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                log.error("unexpected consumer error", e);
            }
        }
    }

    public void process(OutboxEvent event) {
        Object lock = keyLocks[Math.floorMod(event.aggregateKey().hashCode(), LOCK_STRIPES)];
        synchronized (lock) {
            long now = System.currentTimeMillis();
            if (!markProcessedIfNew(event.id(), now)) {
                log.info("duplicate event id={} detected, skipping handler", event.id());
                repository.markDone(event.id(), now);
                return;
            }
            try {
                DomainEventHandler handler = handlers.stream()
                        .filter(h -> h.supports(event.eventType()))
                        .findFirst()
                        .orElseThrow(() -> new IllegalStateException(
                                "no handler for event type " + event.eventType()));
                handler.handle(event);
                repository.markDone(event.id(), System.currentTimeMillis());
            } catch (Exception e) {
                // 处理失败：撤销幂等记录，让重试/重放能真正再次执行业务逻辑
                removeProcessed(event.id());
                int attempts = event.attempts() + 1;
                if (attempts >= properties.getMaxAttempts()) {
                    repository.markDead(event.id(), attempts, e.getMessage(), System.currentTimeMillis());
                    log.warn("event id={} key={} moved to DEAD after {} attempts: {}",
                            event.id(), event.aggregateKey(), attempts, e.getMessage());
                } else {
                    long backoff = properties.backoffForAttempt(attempts);
                    repository.markRetry(event.id(), attempts,
                            System.currentTimeMillis() + backoff, e.getMessage(), System.currentTimeMillis());
                    log.info("event id={} key={} failed (attempt {}), retry in {}ms: {}",
                            event.id(), event.aggregateKey(), attempts, backoff, e.getMessage());
                }
            }
        }
    }

    private boolean markProcessedIfNew(String eventId, long now) {
        try {
            jdbc.update("INSERT INTO processed_event (event_id, processed_at) VALUES (?,?)",
                    eventId, now);
            return true;
        } catch (DuplicateKeyException e) {
            return false;
        }
    }

    private void removeProcessed(String eventId) {
        jdbc.update("DELETE FROM processed_event WHERE event_id=?", eventId);
    }
}
