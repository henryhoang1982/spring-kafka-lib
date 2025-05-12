package com.example.kafkametrics.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Slf4j
@Component
@ConfigurationProperties(prefix = "metrics.filter")
public class MetricsProperties {
    private List<String> allowed = new ArrayList<>(Collections.singletonList("kafka.consumer.totalLag"));

    @PostConstruct
    public void init() {
        log.info("Loaded metrics properties with allowed metrics: {}", allowed);
    }

    public List<String> getAllowed() {
        return allowed;
    }

    public void setAllowed(List<String> allowed) {
        log.info("Setting allowed metrics: {}", allowed);
        this.allowed = allowed;
    }
} 