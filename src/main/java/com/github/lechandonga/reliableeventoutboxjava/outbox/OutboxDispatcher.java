package com.github.lechandonga.reliableeventoutboxjava.outbox;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 派发器：周期性从 outbox 表捞取到期事件，乐观认领后投递到内存消息中间件。
 * 同一聚合键同一时刻只会派发最靠前的一条未完成事件，保证按键有序。
 */
@Component
public class OutboxDispatcher {

    private static final Logger log = LoggerFactory.getLogger(OutboxDispatcher.class);

    private final OutboxEventRepository repository;
    private final InMemoryMessageBroker broker;
    private final OutboxProperties properties;

    public OutboxDispatcher(OutboxEventRepository repository,
                            InMemoryMessageBroker broker,
                            OutboxProperties properties) {
        this.repository = repository;
        this.broker = broker;
        this.properties = properties;
    }

    @Scheduled(fixedDelayString = "${outbox.poll-interval-ms:500}")
    public void scheduledPoll() {
        pollNow();
    }

    /** 手工触发一次派发（REST 入口与定时任务共用） */
    public int pollNow() {
        long now = System.currentTimeMillis();
        List<OutboxEvent> events = repository.findDispatchable(properties.getBatchSize(), now);
        int dispatched = 0;
        for (OutboxEvent e : events) {
            if (repository.claim(e.id(), now)) {
                broker.publish(e);
                dispatched++;
                log.debug("dispatched event id={} key={} seq={} type={}",
                        e.id(), e.aggregateKey(), e.seq(), e.eventType());
            }
        }
        if (dispatched > 0) {
            log.info("dispatched {} event(s) to broker", dispatched);
        }
        return dispatched;
    }
}
