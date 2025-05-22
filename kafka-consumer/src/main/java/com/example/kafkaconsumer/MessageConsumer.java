package com.example.kafkaconsumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@Slf4j
public class MessageConsumer {

    @KafkaListener(topics = "${app.kafka.group-ids.topic-x.topic}", groupId = "${app.kafka.group-ids.topic-x.group-id}")
    public void listenTopicX(List<String> messages) {
        log.info("Received batch of {} messages from topic.x", messages.size());
        
        // Process messages with a slight delay to create some lag
        for (String message : messages) {
            log.debug("  - Processing message from topic.x: {}", message);
            
            // Simulate processing time (200ms per message)
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Message processing interrupted", e);
            }
        }
        
        log.info("Batch processing complete for {} messages from topic.x", messages.size());
    }

    @KafkaListener(topics = "${app.kafka.group-ids.topic-y.topic}", groupId = "${app.kafka.group-ids.topic-y.group-id}")
    public void listenTopicY(List<String> messages) {
        log.info("Received batch of {} messages from topic-y", messages.size());
        
        // Process messages with a slight delay to create some lag
        for (String message : messages) {
            log.debug("  - Processing message from topic-y: {}", message);
            
            // Simulate processing time (200ms per message)
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Message processing interrupted", e);
            }
        }
        
        log.info("Batch processing complete for {} messages from topic-y", messages.size());
    }
} 