package com.example.outbox.consumer;

import com.example.outbox.broker.LocalMessageBroker;
import com.example.outbox.config.OutboxProperties;
import com.example.outbox.outbox.EventStatus;
import com.example.outbox.outbox.OutboxEvent;
import com.example.outbox.outbox.OutboxEventRepository;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
public class OrderEventConsumer implements DisposableBean {

    private final OutboxEventRepository eventRepository;
    private final LocalMessageBroker broker;
    private final OutboxProperties properties;
    private final OrderEventConsumptionService consumptionService;
    private final ExecutorService executor;
    private final LinkedBlockingQueue<LocalMessageBroker.Message> readyQueue = new LinkedBlockingQueue<>();
    private final Map<String, ReentrantLock> aggregateLocks = new ConcurrentHashMap<>();
    private final AtomicInteger inflight = new AtomicInteger();

    public OrderEventConsumer(OutboxEventRepository eventRepository,
                              LocalMessageBroker broker,
                              OutboxProperties properties,
                              OrderEventConsumptionService consumptionService) {
        this.eventRepository = eventRepository;
        this.broker = broker;
        this.properties = properties;
        this.consumptionService = consumptionService;
        this.executor = Executors.newFixedThreadPool(Math.max(1, properties.workerThreads()));
        for (int index = 0; index < Math.max(1, properties.workerThreads()); index++) {
            this.executor.submit(this::processLoop);
        }
    }

    @Scheduled(fixedDelayString = "${outbox.relay.fixed-delay-ms:200}")
    public void dispatch() {
        for (int index = 0; index < Math.max(1, properties.batchSize()); index++) {
            LocalMessageBroker.Message message = broker.poll("domain-events");
            if (message == null) {
                return;
            }
            if (!readyQueue.offer(message)) {
                broker.publish("domain-events", message);
                return;
            }
        }
    }

    private void processLoop() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                LocalMessageBroker.Message message = readyQueue.take();
                ReentrantLock lock = aggregateLocks.computeIfAbsent(
                        message.aggregateKey(), ignored -> new ReentrantLock());
                if (!lock.tryLock()) {
                    broker.publish("domain-events", message);
                    continue;
                }
                inflight.incrementAndGet();
                boolean failed = false;
                String failureMessage = "事件消费失败";
                try {
                    try {
                        handle(message);
                    } catch (Exception exception) {
                        failed = true;
                        failureMessage = exception.getMessage();
                    }
                } finally {
                    lock.unlock();
                    synchronized (inflight) {
                        inflight.decrementAndGet();
                        inflight.notifyAll();
                    }
                }
                if (failed) {
                    eventRepository.scheduleFailure(message.eventId(), failureMessage, properties, Instant.now());
                }
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public void waitForInflight(long timeout, TimeUnit unit) throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        synchronized (inflight) {
            while (inflight.get() > 0) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    throw new IllegalStateException("等待消费中的事件完成超时");
                }
                inflight.wait(TimeUnit.NANOSECONDS.toMillis(remaining) + 1);
            }
        }
    }

    private void handle(LocalMessageBroker.Message message) {
        OutboxEvent event = eventRepository.findById(message.eventId()).orElse(null);
        if (event == null || event.status() == EventStatus.SUCCEEDED) {
            return;
        }
        consumptionService.consume(event);
    }

    @Override
    public void destroy() {
        executor.shutdownNow();
    }
}
