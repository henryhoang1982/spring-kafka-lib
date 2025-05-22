package com.example.kafkaproducer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

@Service
@Slf4j
public class MessageProducer {

    private final List<String> topics;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final AtomicLong messageCounter = new AtomicLong(0);

    public MessageProducer(
            @Value("${app.kafka.topics}") String topicsString,
            KafkaTemplate<String, String> kafkaTemplate) {
        this.topics = Arrays.asList(topicsString.split(","));
        this.kafkaTemplate = kafkaTemplate;
        log.info("Initialized MessageProducer with topics: {}", this.topics);
    }

    // Method to send a single message to a specific topic
    public void sendMessage(String topic, String message) {
        this.kafkaTemplate.send(topic, message);
        log.info("Sent single message: {} to topic: {}", message, topic);
    }

    @Scheduled(fixedRate = 1000) // Run every second (faster than consumer processes)
    public void sendBatchMessages() {
        int batchSize = 100; // Increased batch size
        
        for (String topic : topics) {
            log.info("Sending batch of {} messages to topic {}...", batchSize, topic);
            
            for (int i = 0; i < batchSize; i++) {
                long count = messageCounter.incrementAndGet();
                String randomMessage = "Message " + count + ": " + UUID.randomUUID().toString();
                this.kafkaTemplate.send(topic, randomMessage);
                log.debug("Sent message: {} to topic: {}", randomMessage, topic); // Log individual messages at DEBUG level
            }
            
            log.info("Successfully sent batch of {} messages to topic: {}", batchSize, topic);
        }
    }
} 