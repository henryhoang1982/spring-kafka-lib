package com.example.kafkametrics.service;

import com.example.kafkametrics.config.TotalLagMetric.LagValueSupplier;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Fallback service for calculating consumer lag when JMX is disabled.
 * This implementation uses the KafkaAdmin client to calculate lag,
 * which can be more resource-intensive but doesn't require JMX.
 */
@Slf4j
@Service
@RequiredArgsConstructor
@ConditionalOnProperty(name = "spring.jmx.enabled", havingValue = "false", matchIfMissing = true)
@Primary
public class KafkaLagService implements LagValueSupplier {
    private final KafkaAdmin kafkaAdmin;
    private final String consumerGroupId;
    
    private AdminClient adminClient;
    private final AtomicLong currentLag = new AtomicLong(0);

    @PostConstruct
    public void init() {
        this.adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties());
        log.info("Initialized KafkaLagService for consumer group: {} (JMX disabled fallback)", consumerGroupId);
        
        // Initial calculation
        refreshLag();
    }
    
    @Override
    public long getLag() {
        return currentLag.get();
    }
    
    @Scheduled(fixedRateString = "${management.metrics.export.cloudwatch.step:60000}")
    public void refreshLag() {
        long lag = calculateTotalLag();
        if (lag >= 0) { // Only update if calculation was successful
            currentLag.set(lag);
            log.debug("Updated current lag to: {}", lag);
        }
    }
    
    public long calculateTotalLag() {
        try {
            log.debug("Starting lag calculation for consumer group: {}", consumerGroupId);
            
            // List all consumer groups to verify our group exists
            var consumerGroups = adminClient.listConsumerGroups().all().get();
            log.trace("Available consumer groups: {}", 
                  consumerGroups.stream().map(g -> g.groupId()).collect(Collectors.toList()));
            
            // Get consumer offsets
            ListConsumerGroupOffsetsResult groupOffsetsResult = adminClient.listConsumerGroupOffsets(consumerGroupId);
            Map<TopicPartition, OffsetAndMetadata> consumerOffsets = groupOffsetsResult.partitionsToOffsetAndMetadata().get();

            if (consumerOffsets.isEmpty()) {
                log.debug("No offsets found for consumer group: {}. Group may not have consumed any messages yet.", consumerGroupId);
                return 0;
            }

            // Log all the topic partitions we're calculating lag for
            if (log.isTraceEnabled()) {
                log.trace("Found {} partition(s) with offsets for group {}: {}", 
                        consumerOffsets.size(), 
                        consumerGroupId,
                        consumerOffsets.keySet().stream()
                            .map(tp -> tp.topic() + "-" + tp.partition())
                            .collect(Collectors.joining(", ")));
            } else {
                log.debug("Found {} partition(s) with offsets for group {}", 
                        consumerOffsets.size(), consumerGroupId);
            }

            // Get end offsets for all partitions - process in smaller batches if many partitions
            final int BATCH_SIZE = 10; // Reduced batch size to limit memory pressure
            long totalLag = 0;
            
            // Break the offsets into batches if there are many partitions
            List<Map.Entry<TopicPartition, OffsetAndMetadata>> entries = new ArrayList<>(consumerOffsets.entrySet());
            for (int i = 0; i < entries.size(); i += BATCH_SIZE) {
                int end = Math.min(i + BATCH_SIZE, entries.size());
                List<Map.Entry<TopicPartition, OffsetAndMetadata>> batch = entries.subList(i, end);
                
                // Create a map of only the current batch of partitions
                Map<TopicPartition, org.apache.kafka.clients.admin.OffsetSpec> offsetSpecMap = 
                    batch.stream().collect(Collectors.toMap(
                        Map.Entry::getKey, 
                        tp -> org.apache.kafka.clients.admin.OffsetSpec.latest()
                    ));
                
                // Get end offsets for the current batch
                Map<TopicPartition, Long> logEndOffsets = adminClient.listOffsets(offsetSpecMap)
                    .all().get().entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().offset()));
                
                // Process each partition in the batch
                for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : batch) {
                    TopicPartition tp = entry.getKey();
                    long currentOffset = entry.getValue().offset();
                    Long endOffset = logEndOffsets.get(tp);
    
                    if (endOffset != null) {
                        long lag = Math.max(0, endOffset - currentOffset); // Lag can't be negative
                        totalLag += lag;
                        log.trace("Lag for partition {}-{}: {} (Current: {}, End: {})", 
                               tp.topic(), tp.partition(), lag, currentOffset, endOffset);
                    } else {
                        log.warn("No log end offset found for partition {}, skipping for lag calculation.", tp);
                    }
                }
                
                // Clear batch variables to release memory
                offsetSpecMap.clear();
                logEndOffsets.clear();
                
                // Force garbage collection more frequently
                if (entries.size() > BATCH_SIZE && i > 0 && i % (BATCH_SIZE * 2) == 0) {
                    System.gc();
                }
                
                // Refresh admin client for very large partition sets (helps with SSL connections)
                if (entries.size() > BATCH_SIZE * 5 && i > 0 && i % (BATCH_SIZE * 5) == 0) {
                    refreshAdminClient();
                    log.debug("AdminClient refreshed after processing {} of {} partitions", i, entries.size());
                }
            }
            
            log.debug("Calculated total lag for group {}: {}", consumerGroupId, totalLag);
            return totalLag;
        } catch (InterruptedException | ExecutionException e) {
            log.error("Error calculating total lag for consumer group {}: {}", consumerGroupId, e.getMessage());
            // Try to refresh AdminClient on error
            refreshAdminClient();
            Thread.currentThread().interrupt(); // Restore interruption status
            return -1; // Indicate error
        } catch (Exception e) { // Catch any other unexpected exceptions
            log.error("Unexpected error calculating total lag for consumer group {}: {}", consumerGroupId, e.getMessage(), e);
            // Try to refresh AdminClient on error
            refreshAdminClient();
            return -1; // Indicate error
        }
    }
    
    // Method to force cleanup of resources when needed
    private void refreshAdminClient() {
        log.debug("Refreshing Kafka AdminClient to free resources");
        
        // Close existing client if available
        if (this.adminClient != null) {
            try {
                this.adminClient.close();
                log.debug("Closed existing AdminClient");
            } catch (Exception e) {
                log.warn("Error closing existing AdminClient: {}", e.getMessage());
            }
        }
        
        // Create a new client
        this.adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties());
        // Force garbage collection to clean up resources
        System.gc();
    }

    @PreDestroy
    public void destroy() {
        log.info("Closing Kafka AdminClient for consumer group: {}", consumerGroupId);
        try {
            if (adminClient != null) {
                adminClient.close();
                log.info("Successfully closed Kafka AdminClient");
            }
        } catch (Exception e) {
            log.error("Error closing Kafka AdminClient", e);
        }
    }
} 