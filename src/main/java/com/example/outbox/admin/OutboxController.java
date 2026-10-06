package com.example.outbox.admin;

import com.example.outbox.consumer.OrderEventConsumer;
import com.example.outbox.order.BusinessOrder;
import com.example.outbox.order.CreateOrderRequest;
import com.example.outbox.order.OrderEventPayload;
import com.example.outbox.order.OrderService;
import com.example.outbox.outbox.OutboxEvent;
import com.example.outbox.outbox.OutboxEventRepository;
import com.example.outbox.relay.OutboxRelay;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping
public class OutboxController {

    private final OrderService orderService;
    private final OutboxEventRepository eventRepository;
    private final OutboxRelay relay;
    private final OrderEventConsumer consumer;
    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    public OutboxController(OrderService orderService, OutboxEventRepository eventRepository,
                            OutboxRelay relay, OrderEventConsumer consumer,
                            JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this.orderService = orderService;
        this.eventRepository = eventRepository;
        this.relay = relay;
        this.consumer = consumer;
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = objectMapper;
    }

    @PostMapping("/orders")
    @ResponseStatus(HttpStatus.CREATED)
    public BusinessOrder createOrder(@RequestBody CreateOrderRequest request) {
        return orderService.create(request);
    }

    @PostMapping("/test/rollback")
    @ResponseStatus(HttpStatus.CONFLICT)
    public Map<String, Object> rolledBackOrder(@RequestBody CreateOrderRequest request) {
        try {
            orderService.createWithRollback(request);
        } catch (IllegalStateException exception) {
            return Map.of("rolledBack", true, "reason", exception.getMessage());
        }
        throw new ResponseStatusException(HttpStatus.EXPECTATION_FAILED, "业务回滚没有按预期发生");
    }

    @GetMapping("/events")
    public List<OutboxEvent> events() {
        return eventRepository.findAll();
    }

    @GetMapping("/events/{eventId}")
    public OutboxEvent event(@PathVariable String eventId) {
        return eventRepository.findById(eventId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "事件不存在"));
    }

    @GetMapping("/events/{eventId}/history")
    public Object eventHistory(@PathVariable String eventId) {
        eventRepository.findById(eventId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "事件不存在"));
        return eventRepository.findHistory(eventId);
    }

    @PostMapping("/events/{eventId}/replay")
    public Map<String, Object> replay(@PathVariable String eventId) {
        boolean replayed = eventRepository.replay(eventId, Instant.now());
        if (!replayed) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "只有死信事件可以重放，已成功事件不会重复处理");
        }
        return Map.of("eventId", eventId, "replayed", true);
    }

    @PostMapping("/events/{eventId}/repair-and-replay")
    public Map<String, Object> repairAndReplay(@PathVariable String eventId,
                                               @RequestBody(required = false) OrderEventPayload payload) {
        OutboxEvent event = eventRepository.findById(eventId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "事件不存在"));
        OrderEventPayload repair = payload == null
                ? readPayload(event.payload()).withFailConsumption(false)
                : payload;
        String json = writePayload(repair);
        if (!eventRepository.repairPayload(eventId, json, Instant.now())) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "只有死信事件允许修复载荷");
        }
        if (!eventRepository.replay(eventId, Instant.now())) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "死信事件重放失败");
        }
        return Map.of("eventId", eventId, "replayed", true, "payload", repair);
    }

    @PostMapping("/relay/run")
    public Map<String, Object> runRelay() {
        relay.relayDueEvents();
        return Map.of("triggered", true);
    }

    @PostMapping("/consumer/dispatch")
    public Map<String, Object> dispatchConsumer() {
        consumer.dispatch();
        return Map.of("triggered", true);
    }

    @GetMapping("/projections")
    public List<Map<String, Object>> projections() {
        return jdbcTemplate.queryForList("SELECT * FROM order_projection ORDER BY aggregate_key");
    }

    @GetMapping("/progress")
    public List<Map<String, Object>> progress() {
        return jdbcTemplate.queryForList("SELECT * FROM aggregate_progress ORDER BY aggregate_key");
    }

    private OrderEventPayload readPayload(String json) {
        try {
            return objectMapper.readValue(json, OrderEventPayload.class);
        } catch (Exception exception) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "事件载荷无法解析");
        }
    }

    private String writePayload(OrderEventPayload payload) {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (Exception exception) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "事件载荷无法序列化");
        }
    }
}
