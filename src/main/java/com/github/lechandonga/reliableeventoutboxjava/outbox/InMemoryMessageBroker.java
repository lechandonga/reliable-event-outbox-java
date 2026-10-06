package com.github.lechandonga.reliableeventoutboxjava.outbox;

import org.springframework.stereotype.Component;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * 内存消息中间件模拟：派发器生产、消费工作线程消费。
 * 进程退出后队列内容丢失，但事件本体持久化在 outbox_event 表中，
 * 恢复流程会把 DISPATCHED 状态的事件重新置为 NEW 再次派发，因此不丢消息。
 */
@Component
public class InMemoryMessageBroker {

    private final BlockingQueue<OutboxEvent> queue = new LinkedBlockingQueue<>();

    public void publish(OutboxEvent event) {
        queue.offer(event);
    }

    public OutboxEvent take(long timeoutMs) throws InterruptedException {
        return queue.poll(timeoutMs, TimeUnit.MILLISECONDS);
    }

    public int pendingCount() {
        return queue.size();
    }
}
