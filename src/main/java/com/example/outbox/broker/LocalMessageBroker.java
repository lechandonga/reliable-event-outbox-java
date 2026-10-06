package com.example.outbox.broker;

import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.springframework.stereotype.Component;

@Component
public class LocalMessageBroker {

    private final ConcurrentHashMap<String, ConcurrentLinkedQueue<Message>> queues = new ConcurrentHashMap<>();

    public void publish(String queue, Message message) {
        queues.computeIfAbsent(queue, ignored -> new ConcurrentLinkedQueue<>()).add(message);
    }

    public Message poll(String queue) {
        return queues.computeIfAbsent(queue, ignored -> new ConcurrentLinkedQueue<>()).poll();
    }

    public record Message(String eventId, String aggregateKey, long sequenceNo, Instant publishedAt) {
    }
}
