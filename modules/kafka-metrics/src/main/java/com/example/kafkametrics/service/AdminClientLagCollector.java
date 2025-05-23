package com.example.kafkametrics.service;

import com.example.kafkametrics.config.TotalLagMetric.LagValueSupplier;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Service
public class AdminClientLagCollector implements LagValueSupplier {

    private final KafkaAdmin kafkaAdmin;
    private final MeterRegistry meterRegistry;
    private final Map<String, Map<String, AtomicLong>> groupTopicLags;
    private static final String METRIC_NAME = "kafka.consumer.group.lag";

    @Autowired
    public AdminClientLagCollector(KafkaAdmin kafkaAdmin, MeterRegistry meterRegistry) {
        this.kafkaAdmin = kafkaAdmin;
        this.meterRegistry = meterRegistry;
        this.groupTopicLags = new ConcurrentHashMap<>();
    }

    @Scheduled(fixedRateString = "${kafka.metrics.step:PT10S}")
    public void updateLags() {
        try (AdminClient adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            // Get all consumer groups
            ListConsumerGroupsResult groups = adminClient.listConsumerGroups();
            groups.valid().get().forEach(group -> {
                String groupId = group.groupId();
                try {
                    // Get group offsets
                    Map<TopicPartition, Long> groupOffsets = new HashMap<>();
                    adminClient.listConsumerGroupOffsets(groupId)
                            .partitionsToOffsetAndMetadata().get()
                            .forEach((tp, offsetAndMetadata) -> 
                                groupOffsets.put(tp, offsetAndMetadata.offset()));

                    // Get end offsets for each partition
                    Map<TopicPartition, OffsetSpec> offsetSpecs = new HashMap<>();
                    groupOffsets.keySet().forEach(tp -> offsetSpecs.put(tp, OffsetSpec.latest()));
                    
                    Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endOffsets = 
                        adminClient.listOffsets(offsetSpecs).all().get();

                    // Calculate lag per topic
                    Map<String, Long> topicLags = new HashMap<>();
                    groupOffsets.forEach((tp, consumerOffset) -> {
                        String topic = tp.topic();
                        long endOffset = endOffsets.get(tp).offset();
                        long lag = Math.max(0, endOffset - consumerOffset);
                        topicLags.merge(topic, lag, Long::sum);
                    });

                    // Update metrics
                    topicLags.forEach((topic, lag) -> {
                        groupTopicLags.computeIfAbsent(groupId, k -> new ConcurrentHashMap<>())
                                    .computeIfAbsent(topic, k -> new AtomicLong())
                                    .set(lag);

                        // Update or create the meter
                        meterRegistry.gauge(METRIC_NAME,
                            Tags.of(
                                "consumer_group", groupId,
                                "topic", topic
                            ),
                            lag);
                        
                        log.debug("Updated lag for group: {}, topic: {}, lag: {}", groupId, topic, lag);
                    });

                } catch (InterruptedException | ExecutionException e) {
                    log.error("Error collecting lag for consumer group: " + groupId, e);
                }
            });
        } catch (Exception e) {
            log.error("Error collecting consumer group lags", e);
        }
    }

    @Override
    public long getLag() {
        // Return total lag across all groups and topics
        return groupTopicLags.values().stream()
                .flatMap(m -> m.values().stream())
                .mapToLong(AtomicLong::get)
                .sum();
    }
} 