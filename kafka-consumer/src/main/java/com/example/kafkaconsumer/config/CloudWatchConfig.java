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
        log.info("Configuring CloudWatch MeterRegistry with CPU/memory optimizations");
        
        return registry -> {
            // 1. Add common tags
            registry.config().commonTags("service", "kafka-consumer");
            
            // 2. Filter out non-essential metrics to reduce volume
            registry.config().meterFilter(MeterFilter.denyUnless(id -> {
                String name = id.getName();
                
                // Only allow essential metrics
                return name.equals("kafka.consumer.totalLag") ||
                       name.startsWith("jvm.memory") ||
                       name.startsWith("system.cpu") ||
                       name.startsWith("kafka.consumer.bytes-consumed") ||
                       name.equals("system.load.average.1m");
            }));
            
            // 3. Limit high cardinality metrics like timers
            registry.config().meterFilter(MeterFilter.deny(id -> {
                String name = id.getName();
                // Exclude metrics that generate many time series
                return name.contains("hystrix") || 
                       name.startsWith("tomcat.") ||
                       name.contains("percentile");
            }));
            
            log.info("CloudWatch MeterRegistry configured with optimizations");
        };
    }
} 