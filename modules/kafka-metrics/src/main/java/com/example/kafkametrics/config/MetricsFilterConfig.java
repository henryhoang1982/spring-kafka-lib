package com.example.kafkametrics.config;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.config.MeterFilterReply;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Configuration
public class MetricsFilterConfig {

    // Default to only allowing kafka.consumer.totalLag if no configuration provided
    @Value("${metrics.filter.allowed:}")
    private List<String> allowedMetricsList;

    @Value("${metrics.updateIntervalMs:5000}")
    public long refreshIntervalMs;

    @Bean
    public MeterFilter meterFilter() {
        // Convert to a Set for faster lookups
        Set<String> allowedMetrics = new HashSet<>(
                allowedMetricsList.isEmpty() ? 
                Collections.singletonList("kafka.consumer.totalLag") : 
                allowedMetricsList
        );
        
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