package com.github.lechandonga.reliableeventoutboxjava.outbox;

/**
 * 领域事件处理器。实现类必须幂等：同一事件可能被重复调用。
 */
public interface DomainEventHandler {

    boolean supports(String eventType);

    void handle(OutboxEvent event);
}
