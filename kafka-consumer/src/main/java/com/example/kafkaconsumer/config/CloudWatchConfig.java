package com.example.kafkaconsumer.config;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.config.MeterFilter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.actuate.autoconfigure.metrics.MeterRegistryCustomizer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
@ConditionalOnProperty(value = "management.metrics.export.cloudwatch.enabled", havingValue = "true")
public class CloudWatchConfig {

    /**
     * Customizes the CloudWatch MeterRegistry to limit metrics and reduce memory usage.
     */
    @Bean
    public MeterRegistryCustomizer<MeterRegistry> cloudWatchMeterRegistryCustomizer() {
        log.info("Configuring CloudWatch MeterRegistry with tags");
        
        return registry -> {
            // Add common tags for CloudWatch metrics
            registry.config().commonTags("service", "kafka-consumer");
            log.info("CloudWatch MeterRegistry configured with tags");
        };
    }
} 