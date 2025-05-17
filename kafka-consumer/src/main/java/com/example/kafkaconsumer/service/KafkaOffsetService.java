package com.example.kafkaconsumer.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaOffsetService {

    private final KafkaListenerEndpointRegistry registry;

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
                    // Get the consumer group ID
                    String groupId = listenerContainer.getContainerProperties().getGroupId();
                    log.info("Resetting offset for consumer group: {}", groupId);

                    // Create a rebalance listener that will handle the offset reset
                    ConsumerAwareRebalanceListener rebalanceListener = new ConsumerAwareRebalanceListener() {
                        @Override
                        public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
                            if (partitions.contains(topicPartition)) {
                                consumer.seek(topicPartition, offset);
                                log.info("Successfully reset offset for topic={}, partition={} to offset={}", 
                                        topic, partition, offset);
                            }
                        }
                    };

                    // Set the rebalance listener
                    listenerContainer.getContainerProperties().setConsumerRebalanceListener(rebalanceListener);
                    
                    // Trigger a rebalance to apply the offset reset
                    listenerContainer.stop();
                    listenerContainer.start();
                    
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