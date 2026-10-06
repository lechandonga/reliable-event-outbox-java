package com.example.outbox.relay;

import com.example.outbox.broker.LocalMessageBroker;
import com.example.outbox.config.OutboxProperties;
import com.example.outbox.outbox.OutboxEvent;
import com.example.outbox.outbox.OutboxEventRepository;
import java.lang.management.ManagementFactory;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class OutboxRelay {

    private final OutboxEventRepository repository;
    private final LocalMessageBroker broker;
    private final OutboxProperties properties;
    private final String workerId = ManagementFactory.getRuntimeMXBean().getName() + "-" + ProcessHandle.current().pid();

    public OutboxRelay(OutboxEventRepository repository, LocalMessageBroker broker,
                       OutboxProperties properties) {
        this.repository = repository;
        this.broker = broker;
        this.properties = properties;
    }

    @Scheduled(fixedDelayString = "${outbox.relay.fixed-delay-ms:200}")
    public void relayDueEvents() {
        Instant now = Instant.now();
        List<OutboxEvent> events = repository.claimDue(
                properties.batchSize(), workerId, now.minus(properties.inProgressTimeoutMs(), ChronoUnit.MILLIS), now);
        for (OutboxEvent event : events) {
            broker.publish("domain-events", new LocalMessageBroker.Message(
                    event.id(), event.aggregateKey(), event.sequenceNo(), now));
            repository.markPublished(event.id(), Instant.now());
        }
    }
}
