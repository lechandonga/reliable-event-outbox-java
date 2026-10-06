package com.example.outbox.order;

public record OrderEventPayload(String orderId, String title, boolean failConsumption) {
    public OrderEventPayload withFailConsumption(boolean value) {
        return new OrderEventPayload(orderId, title, value);
    }
}
