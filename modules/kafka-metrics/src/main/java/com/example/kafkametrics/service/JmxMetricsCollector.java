package com.example.kafkametrics.service;

import com.example.kafkametrics.config.TotalLagMetric.LagValueSupplier;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.scheduling.annotation.Scheduled;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import javax.management.*;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Set;

/**
 * Service that collects consumer lag metrics directly from JMX.
 * This approach utilizes the built-in Kafka Consumer metrics instead of
 * calculating lag manually through the AdminClient API.
 */
@Slf4j
@Service
public class JmxMetricsCollector implements LagValueSupplier {
    
    private final String consumerGroupId;
    private final AtomicLong currentLag = new AtomicLong(0);
    private MBeanServer mbeanServer;
    private ObjectName lagMetricPattern;

    public JmxMetricsCollector(@Value("${spring.kafka.consumer.group-id}") String consumerGroupId) {
        this.consumerGroupId = consumerGroupId;
    }
    
    @PostConstruct
    public void init() {
        try {
            mbeanServer = ManagementFactory.getPlatformMBeanServer();
            
            // The pattern will match all consumer instances for our group-id
            String pattern = "kafka.consumer:type=consumer-fetch-manager-metrics,client-id=*,consumer-id=*";
            lagMetricPattern = new ObjectName(pattern);
            
            log.info("Initialized JMX metrics collector for consumer group: {}", consumerGroupId);
            
            // Initial collection
            refreshLag();
        } catch (MalformedObjectNameException e) {
            log.error("Error creating JMX ObjectName pattern: {}", e.getMessage(), e);
        }
    }
    
    @Override
    public long getLag() {
        return currentLag.get();
    }
    
    @Scheduled(fixedRateString = "${management.metrics.export.cloudwatch.step:60000}")
    public void refreshLag() {
        try {
            long maxLag = collectMaxLag();
            if (maxLag >= 0) {
                currentLag.set(maxLag);
                log.debug("Updated current lag to: {}", maxLag);
            }
        } catch (Exception e) {
            log.error("Error collecting lag metrics from JMX: {}", e.getMessage(), e);
        }
    }
    
    private long collectMaxLag() {
        try {
            long maxLag = 0;
            
            // Query all MBeans matching our pattern
            Set<ObjectName> mbeans = mbeanServer.queryNames(lagMetricPattern, null);
            
            if (mbeans.isEmpty()) {
                log.warn("No consumer fetch manager metrics found for consumer group: {}", consumerGroupId);
                return 0;
            }
            
            // Find the maximum lag across all consumer instances
            for (ObjectName mbean : mbeans) {
                String clientId = mbean.getKeyProperty("client-id");
                String consumerId = mbean.getKeyProperty("consumer-id");
                
                // Only process metrics for our consumer group
                if (consumerId != null && consumerId.contains(consumerGroupId)) {
                    try {
                        // Get the records-lag-max attribute
                        Object value = mbeanServer.getAttribute(mbean, "records-lag-max");
                        if (value instanceof Number) {
                            long lag = ((Number) value).longValue();
                            maxLag = Math.max(maxLag, lag);
                            log.trace("Consumer {} has max lag: {}", clientId, lag);
                        }
                    } catch (AttributeNotFoundException | InstanceNotFoundException |
                             MBeanException | ReflectionException e) {
                        log.warn("Error retrieving records-lag-max for consumer {}: {}", 
                                 clientId, e.getMessage());
                    }
                }
            }
            
            log.debug("Collected maximum lag for group {}: {}", consumerGroupId, maxLag);
            return maxLag;
        } catch (Exception e) {
            log.error("Error collecting maximum lag via JMX: {}", e.getMessage(), e);
            return -1;
        }
    }
    
    @PreDestroy
    public void destroy() {
        log.info("Destroying JMX metrics collector for consumer group: {}", consumerGroupId);
    }
} 