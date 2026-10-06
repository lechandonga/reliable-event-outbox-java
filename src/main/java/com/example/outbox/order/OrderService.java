package com.example.outbox.order;

import com.example.outbox.outbox.EventStatus;
import com.example.outbox.outbox.OutboxEvent;
import com.example.outbox.outbox.OutboxEventRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OrderService {

    private final JdbcTemplate jdbcTemplate;
    private final OutboxEventRepository eventRepository;
    private final ObjectMapper objectMapper;
    private final int maxAttempts;

    public OrderService(JdbcTemplate jdbcTemplate, OutboxEventRepository eventRepository,
                        ObjectMapper objectMapper,
                        @Value("${outbox.relay.max-attempts:5}") int maxAttempts) {
        this.jdbcTemplate = jdbcTemplate;
        this.eventRepository = eventRepository;
        this.objectMapper = objectMapper;
        this.maxAttempts = maxAttempts;
    }

    @Transactional
    public BusinessOrder create(CreateOrderRequest request) {
        Instant now = Instant.now();
        String orderId = request.orderId() == null || request.orderId().isBlank()
                ? UUID.randomUUID().toString() : request.orderId();
        String title = request.title() == null || request.title().isBlank() ? "未命名订单" : request.title();
        BusinessOrder order = new BusinessOrder(orderId, title, "CREATED", now, now);

        jdbcTemplate.update("""
                MERGE INTO business_orders (id, title, status, created_at, updated_at)
                KEY (id)
                VALUES (?, ?, ?, ?, ?)
                """, order.id(), order.title(), order.status(), order.createdAt(), order.updatedAt());

        long sequenceNo = eventRepository.nextSequence(orderId);
        OutboxEvent event = new OutboxEvent(
                UUID.randomUUID().toString(), orderId, sequenceNo, "ORDER_CREATED",
                writePayload(new OrderEventPayload(orderId, title, request.failConsumption())),
                EventStatus.PENDING, 0, maxAttempts, now, null, null, null, null, null,
                null, now, now, 0);
        eventRepository.insert(event);
        return order;
    }

    @Transactional
    public void createWithRollback(CreateOrderRequest request) {
        create(request);
        throw new IllegalStateException("模拟业务事务在写入业务和事件后失败，两者必须一起回滚");
    }

    private String writePayload(OrderEventPayload payload) {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException exception) {
            throw new IllegalArgumentException("事件 payload 无法序列化", exception);
        }
    }
}
