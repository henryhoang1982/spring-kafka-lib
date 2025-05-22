package com.example.kafkaconsumer.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import java.util.Map;

@Configuration
@ConfigurationProperties(prefix = "app.kafka")
public class KafkaConfig {
    private Map<String, String> groupIds;

    public Map<String, String> getGroupIds() {
        return groupIds;
    }

    public void setGroupIds(Map<String, String> groupIds) {
        this.groupIds = groupIds;
    }
} 