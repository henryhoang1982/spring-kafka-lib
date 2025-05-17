package com.example.kafkaconsumer.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.stereotype.Service;

import java.util.Collections;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaOffsetService {

    private final KafkaListenerEndpointRegistry registry;
    private final ConsumerFactory<?, ?> consumerFactory;

    public void resetOffset(String topic, int partition, long offset) {
        log.info("Resetting offset for topic={}, partition={} to offset={}", topic, partition, offset);
        
        // Get all listener containers
        registry.getListenerContainers().forEach(container -> {
            if (container instanceof AbstractMessageListenerContainer) {
                AbstractMessageListenerContainer<?, ?> listenerContainer = 
                    (AbstractMessageListenerContainer<?, ?>) container;
                
                // Create a TopicPartition for the specified topic and partition
                TopicPartition topicPartition = new TopicPartition(topic, partition);
                
                // Pause the container to prevent new messages from being processed
                listenerContainer.pause();
                
                try {
                    // Create a new consumer instance
                    Consumer<?, ?> consumer = consumerFactory.createConsumer();
                    if (consumer != null) {
                        // Assign the topic partition to the consumer
                        consumer.assign(Collections.singleton(topicPartition));
                        // Seek to the specified offset
                        consumer.seek(topicPartition, offset);
                        log.info("Successfully reset offset for topic={}, partition={} to offset={}", 
                                topic, partition, offset);
                    } else {
                        log.warn("Consumer is null for container {}", listenerContainer.getListenerId());
                    }
                } catch (Exception e) {
                    log.error("Error resetting offset: {}", e.getMessage(), e);
                    throw new RuntimeException("Failed to reset offset: " + e.getMessage(), e);
                } finally {
                    // Resume the container
                    listenerContainer.resume();
                }
            }
        });
    }
} 