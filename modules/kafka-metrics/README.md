# Kafka Metrics Module

This module provides metrics collection and monitoring capabilities for Kafka consumers and producers.

## Components

### KafkaAdmin Configuration

The module provides an optimized `KafkaAdmin` bean that automatically detects whether SSL or PLAINTEXT security protocol is in use:

- For SSL environments (typically production), it applies memory and connection optimizations to prevent OutOfMemory issues
- For PLAINTEXT environments (typically local development), it uses minimal configuration

### TotalLagMetrics

Monitors consumer lag for Kafka topics by:
- Tracking the difference between current consumer offsets and log end offsets
- Exposing metrics through Micrometer/Prometheus
- Using batch processing to prevent memory issues with large topics

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