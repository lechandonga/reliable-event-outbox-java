package com.example.outbox.outbox;

public enum EventStatus {
    PENDING,
    IN_PROGRESS,
    SUCCEEDED,
    DEAD_LETTER
}
