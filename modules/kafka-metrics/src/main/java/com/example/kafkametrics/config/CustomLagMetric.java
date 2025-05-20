package com.example.kafkametrics.config;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.binder.MeterBinder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class CustomLagMetric implements MeterBinder {

    private final String consumerGroupId;
    private final String applicationName;
    private static final String SOURCE_METRIC_NAME = "kafka.consumer.fetch.manager.records.lag.max";
    private static final String CUSTOM_METRIC_NAME = "custom.kafka.consumer.lag";

    public CustomLagMetric(
            @Value("${spring.kafka.consumer.group-id}") String consumerGroupId,
            @Value("${spring.application.name:unknown}") String applicationName) {
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

    private Double fetchLagValue(MeterRegistry registry) {
        return registry.find(SOURCE_METRIC_NAME)
                .meters()
                .stream()
                .filter(meter -> meter instanceof io.micrometer.core.instrument.Gauge)
                .mapToDouble(meter -> ((io.micrometer.core.instrument.Gauge) meter).value())
                .sum();
    }
} 