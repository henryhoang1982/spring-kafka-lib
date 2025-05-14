package com.example.kafkametrics.config;

import io.micrometer.cloudwatch2.CloudWatchConfig;
import io.micrometer.cloudwatch2.CloudWatchMeterRegistry;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.config.MeterFilterReply;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

@Slf4j
@Configuration
@EnableConfigurationProperties(MetricsProperties.class)
public class MetricsFilterConfig {
    private final MetricsProperties metricsProperties;

    public MetricsFilterConfig(MetricsProperties metricsProperties) {
        this.metricsProperties = metricsProperties;
        log.info("Initialized MetricsFilterConfig with properties: {}", metricsProperties.getAllowed());
    }

    @Bean
    public MeterFilter meterFilter() {
        log.info("Creating meter filter with allowed metrics: {}", metricsProperties.getAllowed());
        
        // Convert to a Set for faster lookups
        Set<String> allowedMetrics = new HashSet<>(metricsProperties.getAllowed());
        
        return new MeterFilter() {
            @Override
            public MeterFilterReply accept(Meter.Id id) {
                String metricName = id.getName();
                
                // Check if this metric is in our allowed list
                if (allowedMetrics.contains(metricName)) {
                    return MeterFilterReply.ACCEPT;
                }
                
                // Handle pattern matching for patterns ending with .*
                for (String prefix : allowedMetrics) {
                    if (prefix.endsWith(".*") && metricName.startsWith(prefix.substring(0, prefix.length() - 2))) {
                        return MeterFilterReply.ACCEPT;
                    }
                }
                
                // Deny all other metrics
                return MeterFilterReply.DENY;
            }
        };
    }
    
    // CloudWatch Configuration Section
    @Configuration
    @ConditionalOnProperty(value = "kafka.metrics.cloudwatch.enabled", havingValue = "true", matchIfMissing = false)
    public static class CloudWatchConfiguration {
        
        @Bean
        public CloudWatchAsyncClient cloudWatchAsyncClient() {
            log.info("Creating CloudWatchAsyncClient");
            return CloudWatchAsyncClient.create();
        }
        
        @Bean
        public CloudWatchConfig cloudWatchConfig() {
            return new CloudWatchConfig() {
                @Override
                public String get(String key) {
                    return null; // Accept defaults for properties not explicitly set
                }
                
                @Override
                public String namespace() {
                    return "KafkaMetrics"; // Custom namespace for the metrics
                }
                
                @Override
                public Duration step() {
                    return Duration.ofSeconds(60); // 1 minute reporting interval
                }
                
                @Override
                public boolean enabled() {
                    return true;
                }
                
                @Override
                public int batchSize() {
                    return 10; // Reduced batch size to control memory usage
                }
            };
        }
        
        @Bean
        public MeterRegistry cloudWatchMeterRegistry(CloudWatchConfig config, CloudWatchAsyncClient client) {
            log.info("Creating CloudWatch meter registry with namespace: {}", config.namespace());
            CloudWatchMeterRegistry registry = new CloudWatchMeterRegistry(config, Clock.SYSTEM, client);
            
            // Add common tags
            registry.config().commonTags("module", "kafka-metrics");
            
            return registry;
        }
    }
} 