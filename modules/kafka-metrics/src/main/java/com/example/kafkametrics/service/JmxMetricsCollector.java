package com.example.kafkametrics.service;

import com.example.kafkametrics.config.TotalLagMetric.LagValueSupplier;
import io.micrometer.common.lang.NonNullApi;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.event.ConsumerStartingEvent;
import org.springframework.kafka.event.ListenerContainerIdleEvent;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.management.AttributeNotFoundException;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Service that collects consumer lag metrics directly from JMX.
 * This approach utilizes the built-in Kafka Consumer metrics instead of
 * calculating lag manually through the AdminClient API.
 * Note: Requires only spring.jmx.enabled=true
 * (management.jmx.enabled is not required for this collector)
 */
@Slf4j
@NonNullApi
@Service
@ConditionalOnProperty(name = {"spring.jmx.enabled"}, havingValue = "true")
public class JmxMetricsCollector implements LagValueSupplier, SmartLifecycle {
    
    private final String consumerGroupId;
    private final AtomicLong currentLag = new AtomicLong(0);
    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final AtomicBoolean running = new AtomicBoolean(false);
    private MBeanServer mbeanServer;
    private ObjectName lagMetricPattern;
    private final KafkaListenerEndpointRegistry registry;
    private final ScheduledExecutorService initializationScheduler = Executors.newSingleThreadScheduledExecutor();

    // Attribute Patterns to look for, from most specific to most generic
    private final String JMX_RECORDS_LAG_ATTR = "records-lag-max";

    public JmxMetricsCollector(
            @Value("${spring.kafka.consumer.group-id}") String consumerGroupId,
            KafkaListenerEndpointRegistry registry) {
        this.consumerGroupId = consumerGroupId;
        this.registry = registry;
        log.info("Created JmxMetricsCollector bean, will initialize after Kafka consumer starts");
    }
    
    @Override
    public void start() {
        log.info("Starting JmxMetricsCollector");
        running.set(true);
        
        // Schedule initialization check to run after a short delay to ensure the consumer had a chance to start
        initializationScheduler.schedule(() -> {
            if (!initialized.get()) {
                log.info("Checking consumer status after startup delay...");
                checkAndRegisterConsumerListeners();
                
                // Schedule a fallback initialization after 30 seconds if still not initialized
                initializationScheduler.schedule(() -> {
                    if (!initialized.get()) {
                        log.info("Fallback initialization triggered as consumer events haven't been received");
                        initializeMetricsCollector();
                    }
                }, 30, TimeUnit.SECONDS);
            }
        }, 5, TimeUnit.SECONDS);
    }
    
    /**
     * Check if consumers are running and register listeners if needed
     */
    private void checkAndRegisterConsumerListeners() {
        log.info("Checking for running Kafka consumers to register listeners");
        if (registry.getListenerContainers().isEmpty()) {
            log.warn("No Kafka listener containers found - metrics initialization may be delayed");
            return;
        }
        
        log.info("Found {} listener containers", registry.getListenerContainers().size());
        registry.getListenerContainers().forEach(container -> {
            log.info("Container: {}, running: {}, paused: {}", 
                    container.getListenerId(), container.isRunning(), container.isPauseRequested());
            
            // If consumers are already running, initialize metrics directly
            if (container.isRunning() && !initialized.get()) {
                log.info("Found running consumer - initializing metrics collector immediately");
                initializeMetricsCollector();
            }
        });
    }
    
    @EventListener
    public void onConsumerStarting(ConsumerStartingEvent event) {
        log.info("Detected Kafka consumer starting event: {}", event);
        
        // Register our rebalance listener to be notified when partitions are assigned
        AbstractMessageListenerContainer<?, ?> container = (AbstractMessageListenerContainer<?, ?>) event.getSource();
        log.info("Kafka container starting: {}", container.getListenerId());
        
        // Get the existing rebalance listener and chain with ours
        ConsumerAwareRebalanceListener ourListener = new ConsumerAwareRebalanceListener() {
            @Override
            public void onPartitionsRevokedBeforeCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
                log.debug("Partitions revoked before commit: {}", partitions);
            }
            
            @Override
            public void onPartitionsRevokedAfterCommit(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
                log.debug("Partitions revoked after commit: {}", partitions);
            }
            
            @Override
            public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
                log.info("Partitions assigned to consumer: {}", partitions);
                
                if (!partitions.isEmpty() && !initialized.getAndSet(true)) {
                    log.info("First partition assignment detected for consumer. Initializing JMX metrics collector.");
                    initializeMetricsCollector();
                }
            }
        };
        
        // Replace with our listener
        container.getContainerProperties().setConsumerRebalanceListener(ourListener);
        log.info("Successfully registered rebalance listener for container: {}", container.getListenerId());
    }
    
    /**
     * As a fallback, also listen for idle events which indicate the consumer is running
     */
    @EventListener
    public void onListenerContainerIdle(ListenerContainerIdleEvent event) {
        log.info("Container idle event received: {}", event);
        if (!initialized.get()) {
            log.info("Consumer appears to be idle, initializing JMX metrics collector as fallback");
            initializeMetricsCollector();
        }
    }
    
    private void initializeMetricsCollector() {
        try {
            log.info("Initializing JMX metrics collector for consumer group: {}", consumerGroupId);
            mbeanServer = ManagementFactory.getPlatformMBeanServer();
            
            // Use a very generic pattern to match any Kafka metrics
            // We'll search for the right attributes in collectMaxLag()
            String pattern = "*:*";
            lagMetricPattern = new ObjectName(pattern);
            
            initialized.set(true);  // Mark as initialized
            
            // List all available consumer metrics for debugging
            logAvailableConsumerMetrics();
            
            // Initial collection
            refreshLag();
            
            log.info("JMX metrics collector initialized successfully");
        } catch (MalformedObjectNameException e) {
            log.error("Error creating JMX ObjectName pattern: {}", e.getMessage(), e);
        } catch (Exception e) {
            log.error("Error initializing JMX metrics collector: {}", e.getMessage(), e);
        }
    }
    
    /**
     * Log all available consumer metrics to help with debugging
     */
    private void logAvailableConsumerMetrics() {
        try {
            Set<ObjectName> beans = mbeanServer.queryNames(lagMetricPattern, null);
            log.info("Found {} JMX beans matching pattern *:*", beans.size());
            
            // Filter to only consumer/kafka related beans
            Set<ObjectName> consumerBeans = beans.stream()
                    .filter(bean -> bean.toString().contains("consumer") || bean.toString().contains("kafka"))
                    .collect(java.util.stream.Collectors.toSet());
            
            log.info("Found {} consumer-related beans", consumerBeans.size());
            
            // Count and log beans with our specific lag attribute
            long lagAttributeCount = consumerBeans.stream()
                    .filter(bean -> {
                        try {
                            MBeanInfo info = mbeanServer.getMBeanInfo(bean);
                            return java.util.Arrays.stream(info.getAttributes())
                                    .anyMatch(attr -> attr.getName().equals(JMX_RECORDS_LAG_ATTR));
                        } catch (Exception e) {
                            log.trace("Error checking attributes for bean {}: {}", bean, e.getMessage());
                            return false;
                        }
                    })
                    .peek(bean -> log.info("Found lag metric: {} in bean {}", JMX_RECORDS_LAG_ATTR, bean))
                    .count();
            
            log.info("Found {} consumer-related beans, {} lag-related attributes",
                    consumerBeans.size(), lagAttributeCount);
        } catch (Exception e) {
            log.error("Error listing consumer metrics: {}", e.getMessage(), e);
        }
    }
    
    @Override
    public long getLag() {
        return currentLag.get();
    }
    
    /**
     * Refresh lag metrics every minute (or as configured in application.yml)
     */
    @Scheduled(fixedRateString = "${kafka.metrics.step:PT10S}")
    public void refreshLag() {
        if (!initialized.get()) {
            log.info("JMX metrics collector not yet initialized, skipping lag refresh");
            return;
        }
        
        collectMaxLag();
    }
    
    private void collectMaxLag() {
        if (mbeanServer == null) {
            log.warn("MBeanServer not initialized yet, please enable it in app configuration");
            return;
        }

        try {
            // Pattern to match JMX object names for Kafka consumer lag metrics
            // Note: For records-lag-max, we don't need partition in the pattern
            String objectNamePattern = "kafka.consumer:type=consumer-fetch-manager-metrics,client-id=*";
            ObjectName objNamePattern = new ObjectName(objectNamePattern);
            
            // Get all beans matching our pattern
            Set<ObjectName> beans = mbeanServer.queryNames(objNamePattern, null);

            if (beans.isEmpty()) {
                log.debug("No JMX beans found matching pattern: {}", objectNamePattern);
                return;
            }

            log.debug("Found {} JMX beans matching pattern: {}", beans.size(), objectNamePattern);

            // Calculate maximum lag across all topics using streams
            long totalMaxLag = beans.stream()
                .map(bean -> {
                    try {
                        Object value = mbeanServer.getAttribute(bean, JMX_RECORDS_LAG_ATTR);
                        if (value instanceof Number) {
                            long lagValue = ((Number) value).longValue();
                            log.debug("Found max lag metric: {} = {} in {}", JMX_RECORDS_LAG_ATTR, lagValue, bean);
                            return lagValue;
                        }
                    } catch (AttributeNotFoundException e) {
                        log.trace("Attribute {} not found in bean {}", JMX_RECORDS_LAG_ATTR, bean);
                    } catch (Exception e) {
                        log.error("Error reading attribute {} from bean {}: {}", 
                                JMX_RECORDS_LAG_ATTR, bean, e.getMessage());
                    }
                    return 0L;
                })
                .mapToLong(Long::longValue)
                .sum();

            log.info("Calculated total lag is {}", String.format("%d", totalMaxLag));
            
            currentLag.set(totalMaxLag);
            
        } catch (Exception e) {
            log.error("Error collecting lag metrics from JMX: {}", e.getMessage(), e);
        }
    }

    @Override
    public void stop(Runnable callback) {
        stop();
        callback.run();
    }

    @Override
    public void stop() {
        log.info("Stopping JmxMetricsCollector");
        running.set(false);
        initializationScheduler.shutdownNow();
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE; // Start last, stop first
    }

    @PreDestroy
    public void destroy() {
        log.info("Shutting down JMX metrics collector");
        initializationScheduler.shutdownNow();
    }
} 