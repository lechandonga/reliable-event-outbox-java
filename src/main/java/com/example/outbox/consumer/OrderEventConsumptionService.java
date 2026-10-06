package com.example.outbox.consumer;

import com.example.outbox.broker.LocalMessageBroker;
import com.example.outbox.order.OrderEventPayload;
import com.example.outbox.outbox.OutboxEvent;
import com.example.outbox.outbox.OutboxEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.List;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

@Service
public class OrderEventConsumptionService {

    private final JdbcTemplate jdbcTemplate;
    private final OutboxEventRepository eventRepository;
    private final LocalMessageBroker broker;
    private final ObjectMapper objectMapper;
    private final TransactionTemplate transactionTemplate;

    public OrderEventConsumptionService(JdbcTemplate jdbcTemplate,
                                        OutboxEventRepository eventRepository,
                                        LocalMessageBroker broker,
                                        ObjectMapper objectMapper,
                                        PlatformTransactionManager transactionManager) {
        this.jdbcTemplate = jdbcTemplate;
        this.eventRepository = eventRepository;
        this.broker = broker;
        this.objectMapper = objectMapper;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
    }

    public void consume(OutboxEvent event) {
        transactionTemplate.executeWithoutResult(status -> consumeInNewTransaction(event));
    }

    private void consumeInNewTransaction(OutboxEvent event) {
        Integer consumed = jdbcTemplate.queryForObject(
                "SELECT COUNT(1) FROM consumed_events WHERE event_id = ?", Integer.class, event.id());
        if (consumed != null && consumed > 0) {
            eventRepository.markSucceeded(event.id(), Instant.now());
            return;
        }

        long expected = expectedSequence(event.aggregateKey());
        if (event.sequenceNo() < expected) {
            recordDuplicate(event);
            return;
        }
        if (event.sequenceNo() > expected) {
            broker.publish("domain-events", new LocalMessageBroker.Message(
                    event.id(), event.aggregateKey(), event.sequenceNo(), Instant.now()));
            return;
        }

        OrderEventPayload payload = readPayload(event.payload());
        if (payload.failConsumption()) {
            throw new IllegalStateException("模拟订单事件消费失败，等待退避重试");
        }

        Instant now = Instant.now();
        jdbcTemplate.update("""
                MERGE INTO order_projection (
                    aggregate_key, order_id, title, status, applied_event_count, updated_at
                ) KEY (aggregate_key)
                VALUES (?, ?, ?, 'CREATED', 1, ?)
                """, event.aggregateKey(), payload.orderId(), payload.title(), now);
        jdbcTemplate.update("""
                INSERT INTO consumed_events (event_id, aggregate_key, sequence_no, consumed_at)
                VALUES (?, ?, ?, ?)
                """, event.id(), event.aggregateKey(), event.sequenceNo(), now);
        upsertProgress(event.aggregateKey(), event.sequenceNo(), event.id(), now);
        eventRepository.markSucceeded(event.id(), now);
    }

    private long expectedSequence(String aggregateKey) {
        List<Long> sequenceValues = jdbcTemplate.queryForList(
                "SELECT last_sequence_no FROM aggregate_progress WHERE aggregate_key = ?",
                Long.class, aggregateKey);
        return sequenceValues.isEmpty() ? 1 : sequenceValues.get(0) + 1;
    }

    private void recordDuplicate(OutboxEvent event) {
        Instant now = Instant.now();
        jdbcTemplate.update("""
                INSERT INTO consumed_events (event_id, aggregate_key, sequence_no, consumed_at)
                VALUES (?, ?, ?, ?)
                """, event.id(), event.aggregateKey(), event.sequenceNo(), now);
        eventRepository.markSucceeded(event.id(), now);
    }

    private void upsertProgress(String aggregateKey, long sequenceNo, String eventId, Instant now) {
        jdbcTemplate.update("""
                MERGE INTO aggregate_progress (aggregate_key, last_sequence_no, last_event_id, updated_at)
                KEY (aggregate_key)
                VALUES (?, ?, ?, ?)
                """, aggregateKey, sequenceNo, eventId, now);
    }

    private OrderEventPayload readPayload(String json) {
        try {
            return objectMapper.readValue(json, OrderEventPayload.class);
        } catch (Exception exception) {
            throw new IllegalArgumentException("事件 payload 无法反序列化", exception);
        }
    }
}
