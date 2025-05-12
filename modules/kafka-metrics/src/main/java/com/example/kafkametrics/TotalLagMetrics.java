package com.example.kafkametrics;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.binder.MeterBinder;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Component
@ConditionalOnProperty(name = "spring.kafka.consumer.group-id") // Only activate if group-id is set
@EnableScheduling
public class TotalLagMetrics implements MeterBinder {

    private static final Logger logger = LoggerFactory.getLogger(TotalLagMetrics.class);
    private final AdminClient adminClient;
    private final String consumerGroupId;
    private final String cloudwatchNamespace;
    private final AtomicLong currentLag = new AtomicLong(0);

    public TotalLagMetrics(KafkaAdmin kafkaAdmin, 
                          @Value("${spring.kafka.consumer.group-id}") String consumerGroupId,
                          @Value("${management.cloudwatch.metrics.export.namespace}") String cloudwatchNamespace) {
        this.adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties());
        this.consumerGroupId = consumerGroupId;
        this.cloudwatchNamespace = cloudwatchNamespace;
        logger.info("Initialized TotalLagMetrics for consumer group: {}", consumerGroupId);
    }

    @Override
    public void bindTo(MeterRegistry registry) {
        Gauge.builder("kafka.consumer.totalLag", currentLag, AtomicLong::get)
                .description("Approximate total lag for the consumer group")
                .tag("consumerGroupId", consumerGroupId)
                .tag("aws.namespace", cloudwatchNamespace)
                .register(registry);
        logger.info("Registered kafka.consumer.totalLag gauge for group: {}", consumerGroupId);
        
        // Initial calculation
        refreshLag();
    }
    
    @Scheduled(fixedRateString = "#{@metricsFilterConfig.refreshIntervalMs}")
    public void refreshLag() {
        long lag = calculateTotalLag();
        if (lag >= 0) { // Only update if calculation was successful
            currentLag.set(lag);
            logger.debug("Updated current lag to: {}", lag);
        }
    }

    private long calculateTotalLag() {
        try {
            logger.debug("Starting lag calculation for consumer group: {}", consumerGroupId);
            
            // List all consumer groups to verify our group exists
            var consumerGroups = adminClient.listConsumerGroups().all().get();
            logger.debug("Available consumer groups: {}", 
                consumerGroups.stream().map(g -> g.groupId()).collect(Collectors.toList()));
            
            // Get consumer offsets
            ListConsumerGroupOffsetsResult groupOffsetsResult = adminClient.listConsumerGroupOffsets(consumerGroupId);
            Map<TopicPartition, OffsetAndMetadata> consumerOffsets = groupOffsetsResult.partitionsToOffsetAndMetadata().get();

            if (consumerOffsets.isEmpty()) {
                logger.debug("No offsets found for consumer group: {}. Group may not have consumed any messages yet.", consumerGroupId);
                return 0;
            }

            // Log all the topic partitions we're calculating lag for
            logger.debug("Found {} partition(s) with offsets for group {}: {}", 
                    consumerOffsets.size(), 
                    consumerGroupId,
                    consumerOffsets.keySet().stream()
                        .map(tp -> tp.topic() + "-" + tp.partition())
                        .collect(Collectors.joining(", ")));

            // Get end offsets for all partitions
            Map<TopicPartition, Long> logEndOffsets = adminClient.listOffsets(
                    consumerOffsets.keySet().stream()
                        .collect(Collectors.toMap(tp -> tp, tp -> org.apache.kafka.clients.admin.OffsetSpec.latest()))
            ).all().get().entrySet().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().offset()));
            
            // Calculate lag for each partition
            long totalLag = 0;
            for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : consumerOffsets.entrySet()) {
                TopicPartition tp = entry.getKey();
                long currentOffset = entry.getValue().offset();
                Long endOffset = logEndOffsets.get(tp);

                if (endOffset != null) {
                    long lag = Math.max(0, endOffset - currentOffset); // Lag can't be negative
                    totalLag += lag;
                    logger.debug("Lag for partition {}-{}: {} (Current: {}, End: {})", 
                               tp.topic(), tp.partition(), lag, currentOffset, endOffset);
                } else {
                    logger.warn("No log end offset found for partition {}, skipping for lag calculation.", tp);
                }
            }
            logger.debug("Calculated total lag for group {}: {}", consumerGroupId, totalLag);
            return totalLag;

        } catch (InterruptedException | ExecutionException e) {
            logger.error("Error calculating total lag for consumer group {}: {}", consumerGroupId, e.getMessage());
            Thread.currentThread().interrupt(); // Restore interruption status
            return -1; // Indicate error
        } catch (Exception e) { // Catch any other unexpected exceptions
             logger.error("Unexpected error calculating total lag for consumer group {}: {}", consumerGroupId, e.getMessage(), e);
             return -1; // Indicate error
        }
    }
} 