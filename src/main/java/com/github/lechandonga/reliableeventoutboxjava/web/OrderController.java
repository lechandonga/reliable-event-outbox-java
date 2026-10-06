package com.github.lechandonga.reliableeventoutboxjava.web;

import com.github.lechandonga.reliableeventoutboxjava.order.OrderService;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    public record CreateOrderRequest(String aggregateKey, String eventType, String payload) {}

    @PostMapping
    public OrderService.CreatedOrder create(@RequestBody CreateOrderRequest request) {
        String eventType = request.eventType() == null ? "ORDER_CREATED" : request.eventType();
        String payload = request.payload() == null ? "{}" : request.payload();
        return orderService.createOrder(request.aggregateKey(), eventType, payload);
    }
}
