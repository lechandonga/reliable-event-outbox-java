package com.example.outbox.outbox;

import java.time.Instant;

public record OutboxEvent(
        String id,
        String aggregateKey,
        long sequenceNo,
        String eventType,
        String payload,
        EventStatus status,
        int attempts,
        int maxAttempts,
        Instant nextAttemptAt,
        Instant lockedAt,
        String lockedBy,
        Instant sentAt,
        Instant succeededAt,
        Instant deadAt,
        String lastError,
        Instant createdAt,
        Instant updatedAt,
        long version
) {
}
