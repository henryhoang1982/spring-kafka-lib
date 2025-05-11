package com.example.kafkaconsumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@Slf4j
public class MessageConsumer {

    @Value("${app.kafka.topic}")
    private String topic;
    
    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @KafkaListener(topics = "${app.kafka.topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void listen(List<String> messages) {
        log.info("Received batch of {} messages from topic: {} in group: {}", 
                messages.size(), topic, groupId);
        
        // Process messages with a slight delay to create some lag
        for (String message : messages) {
            log.debug("  - Processing message: {}", message);
            
            // Simulate processing time (200ms per message)
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Message processing interrupted", e);
            }
        }
        
        log.info("Batch processing complete for {} messages.", messages.size());
    }
} 