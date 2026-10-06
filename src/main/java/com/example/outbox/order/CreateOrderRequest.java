package com.example.outbox.order;

public record CreateOrderRequest(String orderId, String title, boolean failConsumption) {
}
