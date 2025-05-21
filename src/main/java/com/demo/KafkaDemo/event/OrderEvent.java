package com.demo.KafkaDemo.event;

import lombok.Data;

@Data
public class OrderEvent {
    private String orderId;
    private String customerName;
    private String customerEmail;

}