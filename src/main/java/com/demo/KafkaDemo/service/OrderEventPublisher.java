package com.demo.KafkaDemo.service;

import com.demo.KafkaDemo.event.OrderEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class OrderEventPublisher {
	private static final Logger logger = LoggerFactory.getLogger(OrderEventPublisher.class);

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	@Autowired
	private ObjectMapper objectMapper;

	public void publishOrderEvent(OrderEvent orderEvent) {
		try {
			String json = objectMapper.writeValueAsString(orderEvent);
			kafkaTemplate.send("order.created", json);
			System.out.println("Order event published successfully.");
		} catch (JsonProcessingException e) {
			logger.error("Failed to serialize OrderEvent: {}", e.getMessage(), e);
			// Optionally alert or send to dead-letter queue (DLQ)
		} catch (Exception e) {
			logger.error("Unexpected error during Kafka publishing: {}", e.getMessage(), e);
		}
	}
}
