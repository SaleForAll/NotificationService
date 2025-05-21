package com.demo.KafkaDemo.controller;

import com.demo.KafkaDemo.event.OrderEvent;
import com.demo.KafkaDemo.service.OrderEventPublisher;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/orders")
public class OrderController {

    @Autowired
    private OrderEventPublisher publisher;

    @PostMapping
    public ResponseEntity<String> placeOrder(@RequestBody OrderEvent orderEvent) throws JsonProcessingException {
        publisher.publishOrderEvent(orderEvent);
        return ResponseEntity.ok("Order Placed and Event Published");
    }

}