package com.example.outbox;

import com.example.outbox.broker.LocalMessageBroker;
import com.example.outbox.consumer.OrderEventConsumer;
import com.example.outbox.order.CreateOrderRequest;
import com.example.outbox.order.OrderEventPayload;
import com.example.outbox.order.OrderService;
import com.example.outbox.relay.OutboxRelay;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.boot.builder.SpringApplicationBuilder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ReliableOutboxIntegrationTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Map<String, Boolean> RESULTS = new LinkedHashMap<>();

    @Autowired
    MockMvc mockMvc;
    @Autowired
    JdbcTemplate jdbcTemplate;
    @Autowired
    ObjectMapper objectMapper;
    @Autowired
    OrderService orderService;
    @Autowired
    LocalMessageBroker broker;
    @Autowired
    OrderEventConsumer consumer;

    @BeforeAll
    static void prepareDatabase() throws IOException {
        Files.walk(Path.of("target"), 1)
                .filter(path -> path.getFileName().toString().startsWith("reliable-outbox-test"))
                .forEach(path -> {
                    try {
                        Files.deleteIfExists(path);
                    } catch (IOException ignored) {
                    }
                });
    }

    @AfterAll
    static void printCoverage() {
        System.out.println("================ 可靠事件投递测试覆盖 ================");
        RESULTS.forEach((name, passed) -> System.out.printf("业务[%s] 结果[%s]%n", name, passed ? "通过" : "失败"));
        System.out.println("====================================================");
    }

    @Test
    @Order(1)
    void atomicCommitAndRecovery() throws Exception {
        runCase("原子提交与异常退出恢复", () -> {
            CreateOrderRequest request = new CreateOrderRequest("order-atomic", "原子订单", false);
            String id = createOrder(request);
            relay();
            consumeOnce();

            int orders = count("business_orders", "id = 'order-atomic'");
            int events = count("outbox_events", "aggregate_key = 'order-atomic'");
            assertThat(orders).isEqualTo(1);
            assertThat(events).isEqualTo(1);

            mockMvc.perform(post("/test/rollback")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(objectMapper.writeValueAsString(
                                    new CreateOrderRequest("order-rollback", "回滚订单", false))))
                    .andExpect(status().isConflict());
            assertThat(count("business_orders", "id = 'order-rollback'")).isZero();
            assertThat(count("outbox_events", "aggregate_key = 'order-rollback'")).isZero();

            jdbcTemplate.update("INSERT INTO business_orders (id, title, status, created_at, updated_at) " +
                    "VALUES ('order-recover', '恢复订单', 'CREATED', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)");
            jdbcTemplate.update("INSERT INTO outbox_events (" +
                    "id, aggregate_key, sequence_no, event_type, payload, status, attempts, max_attempts, " +
                    "next_attempt_at, created_at, updated_at, version" +
                    ") VALUES ('event-recover', 'order-recover', 1, 'ORDER_CREATED', ?, 'IN_PROGRESS', 1, 3, " +
                    "CURRENT_TIMESTAMP - INTERVAL '10' SECOND, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 0)",
                    objectMapper.writeValueAsString(new OrderEventPayload("order-recover", "恢复订单", false)));
            Awaitility.await().atMost(Duration.ofSeconds(5)).pollInterval(Duration.ofMillis(50)).untilAsserted(() -> {
                relay();
                consumeOnce();
                assertThat(statusOf("event-recover")).isEqualTo("SUCCEEDED");
            });
            assertThat(id).isNotBlank();
        });
    }

    @Test
    @Order(2)
    void concurrentDeliveryAcrossAggregates() throws Exception {
        runCase("并发投递与跨聚合推进", () -> {
            int total = 12;
            ExecutorService pool = Executors.newFixedThreadPool(total);
            CountDownLatch ready = new CountDownLatch(total);
            CountDownLatch start = new CountDownLatch(1);
            ConcurrentHashMap<String, Boolean> created = new ConcurrentHashMap<>();
            for (int index = 1; index <= total; index++) {
                int eventIndex = index;
                pool.submit(() -> {
                    ready.countDown();
                    start.await();
                    String orderId = "order-parallel-" + eventIndex;
                    orderService.create(new CreateOrderRequest(orderId, "并发订单 " + eventIndex, false));
                    created.put(orderId, true);
                    return null;
                });
            }
            ready.await(5, TimeUnit.SECONDS);
            start.countDown();
            pool.shutdown();
            assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
            assertThat(created).hasSize(total);

            Awaitility.await().atMost(Duration.ofSeconds(8)).pollInterval(Duration.ofMillis(50)).untilAsserted(() -> {
                relay();
                consumeOnce();
                assertThat(jdbcTemplate.queryForObject(
                        "SELECT COUNT(1) FROM outbox_events WHERE aggregate_key LIKE 'order-parallel-%' AND status = 'SUCCEEDED'",
                        Integer.class)).isEqualTo(total);
            });
        });
    }

    @Test
    @Order(3)
    void duplicateDeliveryIsIdempotentAndFailingAggregateDoesNotBlockOthers() throws Exception {
        runCase("重复投递幂等且失败聚合不阻塞其他聚合", () -> {
            createOrder(new CreateOrderRequest("order-dup", "幂等订单", false));
            createOrder(new CreateOrderRequest("order-stuck", "失败订单", true));
            relay();
            consumeOnce();
            consumeOnce();

            String eventId = singleEventId("order-dup");
            broker.publish("domain-events", new LocalMessageBroker.Message(
                    eventId, "order-dup", 1, java.time.Instant.now()));
            consumeOnce();
            Thread.sleep(100);

            assertThat(statusOf(eventId)).isEqualTo("SUCCEEDED");
            Integer appliedCount = jdbcTemplate.queryForObject(
                    "SELECT applied_event_count FROM order_projection WHERE aggregate_key = 'order-dup'",
                    Integer.class);
            assertThat(appliedCount).isEqualTo(1);

            createOrder(new CreateOrderRequest("order-independent", "独立订单", false));
            Awaitility.await().atMost(Duration.ofSeconds(5)).pollInterval(Duration.ofMillis(50)).untilAsserted(() -> {
                relay();
                consumeOnce();
                assertThat(statusOf(singleEventId("order-independent"))).isEqualTo("SUCCEEDED");
            });
            assertThat(statusOf(singleEventId("order-stuck"))).isIn("PENDING", "DEAD_LETTER");
        });
    }

    @Test
    @Order(4)
    void deadLetterReplayAndOrdering() throws Exception {
        runCase("死信重放与聚合顺序", () -> {
            createOrder(new CreateOrderRequest("order-seq", "顺序订单 A", true));
            relay();
            consumeOnce();
            String first = singleEventId("order-seq");
            Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                relay();
                consumeOnce();
                assertThat(statusOf(first)).isEqualTo("DEAD_LETTER");
            });

            createOrder(new CreateOrderRequest("order-seq", "顺序订单 B", false));
            String second = singleEventId("order-seq", 2L);
            relay();
            Thread.sleep(100);
            assertThat(statusOf(second)).isIn("PENDING", "IN_PROGRESS");

            mockMvc.perform(post("/events/" + first + "/repair-and-replay")
                            .contentType(MediaType.APPLICATION_JSON)
                            .content(objectMapper.writeValueAsString(
                                    new OrderEventPayload("order-seq", "顺序订单 A", false))))
                    .andExpect(status().isOk());
            Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                relay();
                consumeOnce();
                assertThat(statusOf(first)).isEqualTo("SUCCEEDED");
                assertThat(statusOf(second)).isEqualTo("SUCCEEDED");
            });

            mockMvc.perform(post("/events/" + first + "/replay")).andExpect(status().isConflict());
            JsonNode history = getJson("/events/" + first + "/history");
            List<String> reasons = history.findValuesAsText("reason");
            assertThat(reasons).anyMatch(reason -> reason.contains("人工重放死信"));
            assertThat(jdbcTemplate.queryForObject(
                    "SELECT last_sequence_no FROM aggregate_progress WHERE aggregate_key = 'order-seq'",
                    Long.class)).isEqualTo(2L);
        });
    }

    @Test
    @Order(5)
    void restartApplicationContextAndContinuePendingEvent() throws Exception {
        runCase("应用重启后继续推进未完成事件", () -> {
            String orderId = "order-restart";
            createOrder(new CreateOrderRequest(orderId, "重启订单", false));
            relay();
            Thread.sleep(1200);

            try (ConfigurableApplicationContext restarted = new SpringApplicationBuilder(
                    ReliableOutboxApplication.class)
                    .run("--server.port=0", "--outbox.relay.in-progress-timeout-ms=1000")) {
                OutboxRelay restartedRelay = restarted.getBean(OutboxRelay.class);
                OrderEventConsumer restartedConsumer = restarted.getBean(OrderEventConsumer.class);
                JdbcTemplate restartedJdbc = restarted.getBean(JdbcTemplate.class);
                restartedRelay.relayDueEvents();
                restartedConsumer.dispatch();
                restartedConsumer.waitForInflight(5, TimeUnit.SECONDS);

                Integer succeeded = restartedJdbc.queryForObject(
                        "SELECT COUNT(1) FROM outbox_events WHERE aggregate_key = ? AND status = 'SUCCEEDED'",
                        Integer.class, orderId);
                assertThat(succeeded).isEqualTo(1);
            }
        });
    }

    private String createOrder(CreateOrderRequest request) throws Exception {
        MvcResult result = mockMvc.perform(post("/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isCreated())
                .andReturn();
        JsonNode body = JSON.readTree(result.getResponse().getContentAsString());
        return body.path("id").asText();
    }

    private void relay() throws Exception {
        mockMvc.perform(post("/relay/run")).andExpect(status().isOk());
    }

    private void consumeOnce() throws Exception {
        mockMvc.perform(post("/consumer/dispatch")).andExpect(status().isOk());
        consumer.waitForInflight(2, TimeUnit.SECONDS);
        Thread.sleep(10);
    }

    private JsonNode getJson(String path) throws Exception {
        MvcResult result = mockMvc.perform(get(path)).andExpect(status().isOk()).andReturn();
        return JSON.readTree(result.getResponse().getContentAsString());
    }

    private int count(String table, String predicate) {
        Integer value = jdbcTemplate.queryForObject("SELECT COUNT(1) FROM " + table + " WHERE " + predicate,
                Integer.class);
        return value == null ? 0 : value;
    }

    private String statusOf(String eventId) {
        return jdbcTemplate.queryForObject("SELECT status FROM outbox_events WHERE id = ?",
                String.class, eventId);
    }

    private String singleEventId(String aggregateKey) {
        return singleEventId(aggregateKey, 1L);
    }

    private String singleEventId(String aggregateKey, long sequenceNo) {
        return jdbcTemplate.queryForObject(
                "SELECT id FROM outbox_events WHERE aggregate_key = ? AND sequence_no = ?",
                String.class, aggregateKey, sequenceNo);
    }

    private void runCase(String business, ThrowingTest test) throws Exception {
        System.out.printf("开始业务[%s]%n", business);
        try {
            test.run();
            RESULTS.put(business, true);
            System.out.printf("业务[%s] 用例结果[通过]%n", business);
        } catch (Exception | AssertionError error) {
            RESULTS.put(business, false);
            System.out.printf("业务[%s] 用例结果[失败] 原因[%s]%n", business, error.getMessage());
            throw error;
        }
    }

    @FunctionalInterface
    private interface ThrowingTest {
        void run() throws Exception;
    }
}
