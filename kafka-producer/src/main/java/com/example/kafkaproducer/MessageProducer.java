package com.example.kafkaproducer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

@Service
@Slf4j
public class MessageProducer {

    @Value("${app.kafka.topic}")
    private String topicName;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    
    private final AtomicLong messageCounter = new AtomicLong(0);

    // Method to send a single message (can be kept for other uses or removed if not needed)
    public void sendMessage(String message) {
        this.kafkaTemplate.send(topicName, message);
        log.info("Sent single message: {} to topic: {}", message, topicName);
    }

    @Scheduled(fixedRate = 1000) // Run every second (faster than consumer processes)
    public void sendBatchMessages() {
        int batchSize = 20; // Increased batch size
        log.info("Sending batch of {} messages to topic {}...", batchSize, topicName);
        
        for (int i = 0; i < batchSize; i++) {
            long count = messageCounter.incrementAndGet();
            String randomMessage = "Message " + count + ": " + UUID.randomUUID().toString();
            this.kafkaTemplate.send(topicName, randomMessage);
            log.debug("Sent message: {}", randomMessage); // Log individual messages at DEBUG level
        }
        
        log.info("Successfully sent batch of {} messages to topic: {}", batchSize, topicName);
    }
} 