package com.example.kafkametrics.config;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.binder.MeterBinder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicReference;

@Slf4j
@Component
public class CustomLagMetric implements MeterBinder {

    private final String consumerGroupId;
    private final String applicationName;
    private static final String SOURCE_METRIC_NAME = "kafka.consumer.fetch.manager.records.lag.max";
    private static final String CUSTOM_METRIC_NAME = "custom.kafka.consumer.lag";
    
    // Store the last known valid lag value
    private final AtomicReference<Double> lastKnownLag = new AtomicReference<>(0.0);

    public CustomLagMetric(
            @Value("${spring.kafka.consumer.group-id}") String consumerGroupId,
            @Value("${spring.application.name:unknown}") String applicationName,
            KafkaListenerEndpointRegistry kafkaRegistry) {
        this.consumerGroupId = consumerGroupId;
        this.applicationName = applicationName;
    }

    @Override
    public void bindTo(MeterRegistry registry) {
        Gauge.builder(CUSTOM_METRIC_NAME, registry, this::fetchLagValue)
                .tags(Tags.of(
                        "consumer_group", consumerGroupId,
                        "application", applicationName,
                        "metric_type", "consumer_lag"
                ))
                .description("Custom consumer lag metric derived from records-lag-max")
                .register(registry);
    }

    private Double fetchLagValue(MeterRegistry meterRegistry) {
        // Get the current lag value
        Double currentLag = meterRegistry.find(SOURCE_METRIC_NAME)
                .meters()
                .stream()
                .filter(meter -> meter instanceof io.micrometer.core.instrument.Gauge)
                .mapToDouble(meter -> ((io.micrometer.core.instrument.Gauge) meter).value())
                .sum();

        // Update the last known lag value if we got a valid value
        if (!currentLag.isNaN() && currentLag >= 0) {
            lastKnownLag.set(currentLag);
            log.debug("Updated last known lag value to: {}", currentLag);
        }

        return lastKnownLag.get();
    }
} 