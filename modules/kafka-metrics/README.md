# Kafka Metrics Module

This module provides metrics collection and monitoring capabilities for Kafka consumers and producers.

## Components

### KafkaAdmin Configuration

The module provides an optimized `KafkaAdmin` bean that automatically detects whether SSL or PLAINTEXT security protocol is in use:

- For SSL environments (typically production), it applies memory and connection optimizations to prevent OutOfMemory issues
- For PLAINTEXT environments (typically local development), it uses minimal configuration

### TotalLagMetric

A `MeterBinder` implementation that registers the `kafka.consumer.totalLag` metric, which tracks consumer lag. It follows the Single Responsibility Principle by doing only one thing - registering the metric.

### JmxMetricsCollector

Provides lag monitoring by leveraging Kafka's built-in JMX metrics:
- Uses the `records-lag-max` JMX metric exposed by Kafka Consumer clients
- Avoids the need for complex AdminClient operations to calculate lag
- Reduces the resource usage compared to manual lag calculation
- Efficiently collects metrics from all consumer instances in the group

### Metrics Filtering

Provides configuration to specify which metrics should be exposed.

## Usage

Include this module in your Spring Boot application and ensure your `application.yml` includes:

```yaml
metrics:
  filter:
    allowed:
      - kafka.consumer.totalLag
      - jvm.memory.used
      - system.cpu.usage
      # Add other metrics you want to expose
```

Additionally, configure your Kafka consumer group ID:

```yaml
spring:
  kafka:
    consumer:
      group-id: your-consumer-group
```

## Architecture

The module follows a clean separation of concerns:

1. `TotalLagMetric` - Purely registers the metric with Micrometer and defines the LagValueSupplier interface
2. `JmxMetricsCollector` - Implements LagValueSupplier by collecting the records-lag-max metric from JMX
3. `KafkaConfig` - Provides optimized Kafka client configuration
4. `MetricsFilterConfig` - Controls which metrics are exposed

## Advantages of JMX-based Lag Monitoring

The `records-lag-max` metric is exposed by Kafka Consumer clients and provides several advantages:

1. **Built-in Functionality**: Uses Kafka's native consumer lag tracking, which is more efficient than custom calculations
2. **Memory Efficient**: Avoids the heap-intensive operations of AdminClient-based lag calculation
3. **Real-time Accuracy**: Directly reflects the actual consumer's view of lag
4. **SSL Compatibility**: Works reliably with both PLAINTEXT and SSL connections without the memory issues 