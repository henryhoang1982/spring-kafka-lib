package com.example.kafkametrics.config;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.config.MeterFilterReply;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import lombok.extern.slf4j.Slf4j;

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
} 