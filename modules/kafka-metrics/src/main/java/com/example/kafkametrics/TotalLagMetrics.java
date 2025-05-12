package com.example.kafkametrics;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Slf4j
@Component
@ConditionalOnProperty(name = "spring.kafka.consumer.group-id") // Only activate if group-id is set
@EnableScheduling
public class TotalLagMetrics implements MeterBinder, ApplicationListener<ApplicationReadyEvent> {

    private final AdminClient adminClient;
    private MeterRegistry meterRegistry;
    
    @Value("${spring.kafka.consumer.group-id}")
    private String consumerGroupId;
    
    private final AtomicLong currentLag = new AtomicLong(0);

    public TotalLagMetrics(KafkaAdmin kafkaAdmin) {
        this.adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties());
    }

    @PostConstruct
    public void init() {
        log.info("Initialized TotalLagMetrics for consumer group: {}", consumerGroupId);
    }

    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {
        log.info("Application context is ready, TotalLagMetrics can now safely use properties");
    }

    @Override
    public void bindTo(MeterRegistry registry) {
        Gauge.builder("kafka.consumer.totalLag", currentLag, AtomicLong::get)
                .description("Approximate total lag for the consumer group")
                .tag("consumerGroupId", consumerGroupId)
                .register(registry);
        log.info("Registered kafka.consumer.totalLag gauge for group: {}", consumerGroupId);
        
        // Initial calculation
        refreshLag();
    }
    
    @Scheduled(fixedRateString = "${management.cloudwatch.metrics.export.step}")
    public void refreshLag() {
        // Don't calculate lag too frequently to avoid resource contention
        try {
            long lag = calculateTotalLag();
            if (lag >= 0) { // Only update if calculation was successful
                currentLag.set(lag);
                log.debug("Updated current lag to: {}", lag);
            }
        } catch (OutOfMemoryError e) {
            // Handle OOM more gracefully
            log.error("Out of memory during lag calculation, skipping this cycle", e);
            System.gc(); // Request garbage collection
        }
    }

    private long calculateTotalLag() {
        try {
            log.debug("Starting lag calculation for consumer group: {}", consumerGroupId);
            
            // Limit logging to reduce memory pressure
            boolean verboseLogging = log.isTraceEnabled();
            
            // List all consumer groups to verify our group exists
            var consumerGroups = adminClient.listConsumerGroups().all().get();
            if (verboseLogging) {
                log.trace("Available consumer groups: {}", 
                    consumerGroups.stream().map(g -> g.groupId()).collect(Collectors.toList()));
            }
            
            // Get consumer offsets
            ListConsumerGroupOffsetsResult groupOffsetsResult = adminClient.listConsumerGroupOffsets(consumerGroupId);
            Map<TopicPartition, OffsetAndMetadata> consumerOffsets = groupOffsetsResult.partitionsToOffsetAndMetadata().get();

            if (consumerOffsets.isEmpty()) {
                log.debug("No offsets found for consumer group: {}. Group may not have consumed any messages yet.", consumerGroupId);
                return 0;
            }

            // Log all the topic partitions we're calculating lag for - only in trace mode
            if (verboseLogging) {
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
            final int BATCH_SIZE = 20; // Process offsets in batches to reduce memory pressure
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
                        if (verboseLogging) {
                            log.trace("Lag for partition {}-{}: {} (Current: {}, End: {})", 
                                   tp.topic(), tp.partition(), lag, currentOffset, endOffset);
                        }
                    } else {
                        log.warn("No log end offset found for partition {}, skipping for lag calculation.", tp);
                    }
                }
                
                // Force intermediary garbage collection if processing many partitions
                if (entries.size() > BATCH_SIZE * 2 && i > 0 && i % (BATCH_SIZE * 5) == 0) {
                    System.gc();
                }
            }
            
            log.debug("Calculated total lag for group {}: {}", consumerGroupId, totalLag);
            return totalLag;
        } catch (InterruptedException | ExecutionException e) {
            log.error("Error calculating total lag for consumer group {}: {}", consumerGroupId, e.getMessage());
            Thread.currentThread().interrupt(); // Restore interruption status
            return -1; // Indicate error
        } catch (Exception e) { // Catch any other unexpected exceptions
            log.error("Unexpected error calculating total lag for consumer group {}: {}", consumerGroupId, e.getMessage(), e);
            return -1; // Indicate error
        }
    }
} 