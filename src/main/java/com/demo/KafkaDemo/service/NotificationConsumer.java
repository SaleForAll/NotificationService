package com.demo.KafkaDemo.service;

import com.demo.KafkaDemo.event.OrderEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Service;

@Service
public class NotificationConsumer {

	private static final Logger log = LoggerFactory.getLogger(NotificationConsumer.class);

	@Autowired
	private ObjectMapper objectMapper;

	@Autowired
	private JavaMailSender mailSender;

	@KafkaListener(topics = "order.created", groupId = "notification-service")
	public void consumeOrderCreated(String message) {
		log.info("Received order event: {}", message);
		try {
			OrderEvent event = objectMapper.readValue(message, OrderEvent.class);
			sendEmailNotification(event);
		} catch (JsonProcessingException e) {
			log.error("Failed to parse message: {}", message, e);
		}
	}

	private void sendEmailNotification(OrderEvent event) {
		SimpleMailMessage message = new SimpleMailMessage();
		message.setFrom("shruthikeerthi4997@gmail.com");
		message.setTo(event.getCustomerEmail()); // Ensure OrderEvent has this field
		message.setSubject("Your Order Has Been Created!");
		message.setText("Dear " + event.getCustomerName() + ",\n\n"
				+ "Your order with ID " + event.getOrderId() + " has been successfully created.\n\n"
				+ "Thank you for shopping with us!");

		mailSender.send(message);
	}
}
