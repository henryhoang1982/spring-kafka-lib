package com.example.kafkametrics.config;

import com.example.kafkametrics.config.TotalLagMetric.LagValueSupplier;
import com.example.kafkametrics.service.JmxMetricsCollector;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.config.MeterFilterReply;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Slf4j
@Configuration
@EnableScheduling
@EnableConfigurationProperties(MetricsProperties.class)
public class MetricsFilterConfig {

    private final MetricsProperties metricsProperties;
    
    @Value("${spring.kafka.consumer.group-id}")
    private String consumerGroupId;

    public MetricsFilterConfig(MetricsProperties metricsProperties) {
        this.metricsProperties = metricsProperties;
    }

    @Bean
    public MeterFilter metricsFilter() {
        Set<String> allowedMetrics = new HashSet<>(metricsProperties.getAllowed());
        log.info("Configuring metrics filter with allowed metrics: {}", allowedMetrics);

        return new MeterFilter() {
            @Override
            public MeterFilterReply accept(Meter.Id id) {
                String metricName = id.getName();
                // Check if metric name matches any of our allowed patterns
                boolean allowed = allowedMetrics.stream()
                        .anyMatch(pattern -> 
                            pattern.endsWith(".*") 
                                ? metricName.startsWith(pattern.substring(0, pattern.length() - 2))
                                : metricName.equals(pattern)
                        );
                
                MeterFilterReply reply = allowed ? MeterFilterReply.ACCEPT : MeterFilterReply.DENY;
                log.debug("Metric {} is {}", metricName, reply);
                return reply;
            }

            @Override
            public Meter.Id map(Meter.Id id) {
                // Add custom tags to Kafka lag metrics
                if (id.getName().startsWith("kafka.consumer.fetch.manager.records.lag")) {
                    // Log existing tags for debugging
                    id.getTags().forEach(tag -> 
                        log.debug("Existing tag for metric {}: {}={}", id.getName(), tag.getKey(), tag.getValue())
                    );

                    // Create new tags list, excluding unwanted default tags
                    List<Tag> tags = id.getTags().stream()
                        .filter(tag -> !shouldRemoveTag(tag.getKey()))
                        .collect(java.util.stream.Collectors.toList());

                    // Add our custom tags
                    tags.add(Tag.of("consumer_group", consumerGroupId));
                    tags.add(Tag.of("metric_type", "consumer_lag"));
                    
                    // Also add application name if available
                    String appName = System.getProperty("spring.application.name");
                    if (appName != null) {
                        tags.add(Tag.of("application", appName));
                    }
                    
                    return id.withTags(tags);
                }
                return id;
            }

            private boolean shouldRemoveTag(String tagKey) {
                // List of tag keys to remove
                return tagKey.equals("client-id") ||      // Remove client-id tag
                       tagKey.equals("kafka-version") ||  // Remove kafka version tag
                       tagKey.equals("spring.id");        // Remove spring id tag
            }
        };
    }

    @Bean
    @ConditionalOnProperty(name = "spring.kafka.consumer.group-id") // Only activate if group-id is set
    public TotalLagMetric totalLagMetric(LagValueSupplier lagValueSupplier,
                                         @Value("${spring.kafka.consumer.group-id}") String consumerGroupId) {
        String serviceType = lagValueSupplier instanceof JmxMetricsCollector ? "JMX" : "AdminClient";
        log.info("Creating TotalLagMetric bean for consumer group: {} using {} metrics", consumerGroupId, serviceType);
        return new TotalLagMetric(consumerGroupId, lagValueSupplier);
    }
} 