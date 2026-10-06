package com.github.lechandonga.reliableeventoutboxjava.order;

import com.github.lechandonga.reliableeventoutboxjava.outbox.OutboxEvent;
import com.github.lechandonga.reliableeventoutboxjava.outbox.OutboxEventRepository;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

/**
 * 业务写入入口：订单行与领域事件在同一事务中提交，
 * 要么都成功、要么都回滚，杜绝"业务已存事件丢失"或反之。
 */
@Service
public class OrderService {

    private final JdbcTemplate jdbc;
    private final OutboxEventRepository outboxRepository;

    public OrderService(JdbcTemplate jdbc, OutboxEventRepository outboxRepository) {
        this.jdbc = jdbc;
        this.outboxRepository = outboxRepository;
    }

    public record CreatedOrder(String orderId, String eventId) {}

    @Transactional
    public CreatedOrder createOrder(String aggregateKey, String eventType, String payload) {
        long now = System.currentTimeMillis();
        String orderId = UUID.randomUUID().toString();
        jdbc.update("INSERT INTO orders (id, order_no, aggregate_key, created_at) VALUES (?,?,?,?)",
                orderId, "NO-" + orderId.substring(0, 8), aggregateKey, now);

        String eventId = UUID.randomUUID().toString();
        long seq = outboxRepository.nextSeq(aggregateKey);
        outboxRepository.insert(new OutboxEvent(
                eventId, aggregateKey, seq, eventType, payload,
                OutboxEvent.OutboxStatus.NEW, 0, 0, null, now, now));
        return new CreatedOrder(orderId, eventId);
    }
}
