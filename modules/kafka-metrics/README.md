# Kafka Metrics Module

This module provides metrics collection and monitoring capabilities for Kafka consumers and producers.

## Integration Checklist

To integrate this module into your service (e.g., kafka-consumer), follow these steps:

1. **Add Module Dependency**
   ```xml
   <dependency>
       <groupId>com.example</groupId>
       <artifactId>kafka-metrics</artifactId>
       <version>${project.version}</version>
   </dependency>
   ```

2. **Enable Required Spring Features**
   - Add `@EnableScheduling` to your main application class
   - Ensure component scanning includes `com.example.kafkametrics` package

3. **Configuration Requirements**
   ```yaml
   spring:
     jmx:
       enabled: true  # Required for JMX metrics collection
   
   # Optional: Configure metrics refresh rate (default: 10 seconds)
   kafka:
     metrics:
       step: PT10S

   spring:
     kafka:
       consumer:
         properties:
           # Enable JMX reporting for Kafka consumer metrics
           metrics.recording.level: INFO
           # Set metrics sampling window (how often Kafka updates JMX metrics)
           metrics.sample.window.ms: 5000  # Default is 30000ms (30 seconds)
   
   # Configure which metrics to expose
   metrics:
     filter:
       allowed:
         - kafka.consumer.totalLag  # Our custom aggregated lag metric
         - "kafka.consumer:type=consumer-fetch-manager-metrics,client-id=*:records-lag"
         - "kafka.consumer:type=consumer-fetch-manager-metrics,client-id=*:records-lag-max"
         - "kafka.consumer:type=consumer-fetch-manager-metrics,client-id=*:records-lag-avg"
   ```

4. **Actuator Configuration**
   ```yaml
   management:
     endpoints:
       web:
         exposure:
           include: "metrics,prometheus"
     metrics:
       tags:
         application: ${spring.application.name}
   ```

## Available Metrics

The module provides the following Kafka consumer lag metrics:
- `kafka.consumer.totalLag`: Custom aggregated lag metric across all partitions
- `records-lag`: Current lag per partition
- `records-lag-max`: Maximum lag across all partitions
- `records-lag-avg`: Average lag across all partitions

### Custom Tags
All lag metrics include the following tags:
- `consumer_group`: The Kafka consumer group ID
- `metric_type`: Set to "consumer_lag" for lag-related metrics
- Any additional tags from the original Kafka metric

You can add custom tags by configuring additional `MeterFilter` beans in your application.

## Components

### KafkaAdmin Configuration

The module provides an optimized `KafkaAdmin` bean that automatically detects whether SSL or PLAINTEXT security protocol is in use:

- For SSL environments (typically production), it applies memory and connection optimizations to prevent OutOfMemory issues
- For PLAINTEXT environments (typically local development), it uses minimal configuration

### TotalLagMetric

A `MeterBinder` implementation that registers the `kafka.consumer.totalLag` metric, which tracks consumer lag. It follows the Single Responsibility Principle by doing only one thing - registering the metric.

### JmxMetricsCollector

Primary component that collects Kafka consumer lag metrics via JMX:
- Uses Kafka's built-in JMX metrics
- Automatically initializes when consumer starts
- Refreshes metrics at configurable intervals (default: 10 seconds)
- Requires only `spring.jmx.enabled=true`

### KafkaLagService

A fallback service that calculates consumer lag using AdminClient when JMX is disabled:
- Calculates consumer lag by comparing current offsets with end offsets
- Handles memory optimization for large numbers of partitions
- Provides the same interface as JmxMetricsCollector
- Used automatically when `spring.jmx.enabled=false`

### MetricsFilterConfig

Configures which metrics are exposed through Actuator/Prometheus:
- Allows fine-grained control over exposed metrics
- Supports pattern-based metric inclusion
- Automatically picks up configuration from `metrics.filter.allowed`

## Monitoring

Access metrics through:
1. Actuator endpoint: `/actuator/metrics`
2. Prometheus endpoint: `/actuator/prometheus`
3. Specific metric: `/actuator/metrics/kafka.consumer.totalLag`

## Troubleshooting

1. Verify JMX is enabled:
   ```yaml
   spring.jmx.enabled: true
   ```

2. Check logs for:
   - "JMX metrics collector initialized successfully"
   - "Found lag metric: records-lag-max"

3. Common issues:
   - JMX not enabled → Enable with `spring.jmx.enabled=true`
   - Metrics not visible → Check `metrics.filter.allowed` configuration
   - Lag not updating → Check both:
     - `kafka.metrics.step` (how often we collect from JMX)
     - `metrics.sample.window.ms` (how often Kafka updates JMX metrics)

## Architecture

The module follows a clean separation of concerns:

1. `TotalLagMetric` - Purely registers the metric with Micrometer and defines the LagValueSupplier interface
2. `JmxMetricsCollector` - Used when JMX is enabled to efficiently collect metrics
3. `KafkaLagService` - Used as fallback when JMX is disabled
4. `KafkaConfig` - Provides optimized Kafka client configuration
5. `MetricsFilterConfig` - Controls which metrics are exposed

## Advantages of JMX-based Lag Monitoring

The `records-lag-max` metric is exposed by Kafka Consumer clients and provides several advantages:

1. **Built-in Functionality**: Uses Kafka's native consumer lag tracking, which is more efficient than custom calculations
2. **Memory Efficient**: Avoids the heap-intensive operations of AdminClient-based lag calculation
3. **Real-time Accuracy**: Directly reflects the actual consumer's view of lag
4. **SSL Compatibility**: Works reliably with both PLAINTEXT and SSL connections without the memory issues 