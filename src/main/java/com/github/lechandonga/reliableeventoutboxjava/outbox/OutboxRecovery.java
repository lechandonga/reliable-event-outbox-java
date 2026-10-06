package com.github.lechandonga.reliableeventoutboxjava.outbox;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * 崩溃恢复：进程异常退出时，已认领但未完成的事件停留在 DISPATCHED。
 * 启动后统一重置为 NEW，由派发器重新投递（消费端幂等保证不重复执行）。
 */
@Component
public class OutboxRecovery {

    private static final Logger log = LoggerFactory.getLogger(OutboxRecovery.class);

    private final OutboxEventRepository repository;

    public OutboxRecovery(OutboxEventRepository repository) {
        this.repository = repository;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void recoverOnStartup() {
        int recovered = recoverStuckEvents();
        if (recovered > 0) {
            log.info("recovered {} stuck DISPATCHED event(s) back to NEW", recovered);
        }
    }

    public int recoverStuckEvents() {
        return repository.resetStuckDispatched(System.currentTimeMillis());
    }
}
