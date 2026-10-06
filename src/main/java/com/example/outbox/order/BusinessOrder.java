package com.example.outbox.order;

import java.time.Instant;

public record BusinessOrder(String id, String title, String status, Instant createdAt, Instant updatedAt) {
}
