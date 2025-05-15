package com.example.kafkaconsumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

@SpringBootApplication
@ComponentScan(basePackages = {"com.example"})
@Slf4j
public class KafkaConsumerApplication {

    @Autowired
    private KafkaProperties kafkaProperties;

    @Bean
    public KafkaAdmin kafkaAdmin() {
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
            
            // Make sure all relevant SSL properties are included from KafkaProperties
            // No need to add them manually as they're already in the buildAdminProperties
            
            log.info("KafkaAdmin configured with SSL security protocol");
        } else {
            // Basic config for PLAINTEXT mode (local development)
            configs.put("request.timeout.ms", 15000);
            log.info("KafkaAdmin configured with PLAINTEXT security protocol");
        }
        
        return new KafkaAdmin(configs);
    }

    public static void main(String[] args) {
        SpringApplication.run(KafkaConsumerApplication.class, args);
    }

} 