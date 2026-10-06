package com.github.lechandonga.reliableeventoutboxjava.outbox;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 订单领域事件处理器。
 *
 * 事件类型约定（同时作为本地演示/测试的故障注入点）：
 *   ORDER_CREATED        正常处理
 *   FLAKY:<n>            前 n 次处理抛异常，之后成功（验证重试与退避）
 *   FAIL_ALWAYS          永远抛异常（验证死信）
 */
@Component
public class OrderEventHandler implements DomainEventHandler {

    private static final Logger log = LoggerFactory.getLogger(OrderEventHandler.class);

    private final ConcurrentHashMap<String, AtomicInteger> flakyBudget = new ConcurrentHashMap<>();
    private final List<OutboxEvent> handledEvents = new CopyOnWriteArrayList<>();

    @Override
    public boolean supports(String eventType) {
        return eventType.equals("ORDER_CREATED")
                || eventType.startsWith("FLAKY:")
                || eventType.equals("FAIL_ALWAYS");
    }

    @Override
    public void handle(OutboxEvent event) {
        String type = event.eventType();
        if (type.equals("FAIL_ALWAYS")) {
            throw new IllegalStateException("simulated permanent failure for event " + event.id());
        }
        if (type.startsWith("FLAKY:")) {
            int times = Integer.parseInt(type.substring("FLAKY:".length()));
            AtomicInteger budget = flakyBudget.computeIfAbsent(event.id(), k -> new AtomicInteger(times));
            if (budget.getAndDecrement() > 0) {
                throw new IllegalStateException("simulated transient failure for event " + event.id());
            }
        }
        handledEvents.add(event);
        log.info("handled event id={} key={} seq={} type={} payload={}",
                event.id(), event.aggregateKey(), event.seq(), event.eventType(), event.payload());
    }

    /** 测试/演示用：已成功处理的事件（按处理顺序） */
    public List<OutboxEvent> handledEvents() {
        return handledEvents;
    }
}
