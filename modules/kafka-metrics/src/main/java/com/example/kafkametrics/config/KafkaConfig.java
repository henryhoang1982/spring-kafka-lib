package com.example.kafkametrics.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

@Configuration
@Slf4j
public class KafkaConfig {

    @Bean
    @ConditionalOnMissingBean(KafkaAdmin.class)
    public KafkaAdmin kafkaAdmin(KafkaProperties kafkaProperties) {
        Map<String, Object> configs = new HashMap<>(kafkaProperties.buildAdminProperties());
        
        // Get the security protocol (default to PLAINTEXT if not specified)
        String securityProtocol = (String) configs.getOrDefault("security.protocol", "PLAINTEXT");
        
        // Check if using SSL
        if (securityProtocol.contains("SSL")) {
            // Add SSL-specific optimizations for environments with limited resources
            configs.put("request.timeout.ms", 30000);
            configs.put("reconnect.backoff.ms", 1000);
            configs.put("connections.max.idle.ms", 180000); // 3 minutes for SSL
            
            // Memory optimization for SSL connections
            configs.put("receive.buffer.bytes", 65536);
            configs.put("send.buffer.bytes", 65536);
            
            log.info("KafkaAdmin configured with SSL security protocol");
        } else {
            // Basic config for PLAINTEXT mode (local development)
            configs.put("request.timeout.ms", 15000);
            log.info("KafkaAdmin configured with PLAINTEXT security protocol");
        }
        
        return new KafkaAdmin(configs);
    }
} 