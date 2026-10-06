package com.example.outbox.outbox;

import java.time.Instant;

public record StatusHistory(
        long id,
        String eventId,
        String aggregateKey,
        long sequenceNo,
        EventStatus fromStatus,
        EventStatus toStatus,
        int attempts,
        String reason,
        Instant createdAt
) {
}
