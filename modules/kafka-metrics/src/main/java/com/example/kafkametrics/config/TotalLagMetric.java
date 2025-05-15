package com.example.kafkametrics.config;

import io.micrometer.common.lang.NonNullApi;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * A MeterBinder implementation that registers the kafka.consumer.totalLag metric.
 * The actual lag value is calculated and maintained by KafkaLagService.
 */
@Slf4j
@NonNullApi
@RequiredArgsConstructor
public class TotalLagMetric implements MeterBinder {

    private final String consumerGroupId;
    // The supplier of the current lag value
    private final LagValueSupplier lagValueSupplier;

    @Override
    public void bindTo(MeterRegistry registry) {
        Gauge.builder("kafka.consumer.totalLag", lagValueSupplier::getLag)
                .description("Approximate total lag for the consumer group")
                .tag("consumerGroupId", consumerGroupId)
                .register(registry);
        log.info("Registered kafka.consumer.totalLag gauge for group: {}", consumerGroupId);
    }
    
    /**
     * Interface to supply the current lag value.
     * This decouples the metric registration from the lag calculation logic.
     */
    public interface LagValueSupplier {
        long getLag();
    }
} 