# Kafka Metrics Module

This module provides metrics collection and monitoring capabilities for Kafka consumers and producers.

## Components

### KafkaAdmin Configuration

The module provides an optimized `KafkaAdmin` bean that automatically detects whether SSL or PLAINTEXT security protocol is in use:

- For SSL environments (typically production), it applies memory and connection optimizations to prevent OutOfMemory issues
- For PLAINTEXT environments (typically local development), it uses minimal configuration

### TotalLagMetric

A `MeterBinder` implementation that registers the `kafka.consumer.totalLag` metric, which tracks consumer lag. It follows the Single Responsibility Principle by doing only one thing - registering the metric.

### KafkaLagService

Provides the core functionality for:
- Calculating consumer lag by tracking the difference between current consumer offsets and log end offsets
- Maintaining the current lag value in memory
- Scheduling regular lag calculation updates
- Using batch processing to prevent memory issues with large topics
- Managing Kafka AdminClient resources efficiently

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
2. `KafkaLagService` - Implements LagValueSupplier and handles all the lag calculation business logic and scheduling
3. `KafkaConfig` - Provides optimized Kafka client configuration
4. `MetricsFilterConfig` - Controls which metrics are exposed 