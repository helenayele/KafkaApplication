package org.example.kafka.service;

import org.example.kafka.model.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerService.class);

    @KafkaListener(topics = "${app.kafka.topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void consumeMessage(User user) {
        logger.info("Received message from Kafka: {}", user);

        // Process the message based on action
        switch (user.getAction()) {
            case "CREATED":
                logger.info("Processing user creation: {}", user.getName());
                // Add your business logic here
                break;
            case "UPDATED":
                logger.info("Processing user update: {}", user.getName());
                // Add your business logic here
                break;
            case "DELETED":
                logger.info("Processing user deletion: {}", user.getId());
                // Add your business logic here
                break;
            default:
                logger.warn("Unknown action: {}", user.getAction());
        }
    }
}
