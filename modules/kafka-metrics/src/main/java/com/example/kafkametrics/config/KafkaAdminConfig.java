package com.example.kafkametrics.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.util.StringUtils;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Configuration
public class KafkaAdminConfig {

    @Value("${spring.kafka.consumer.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.security.protocol:PLAINTEXT}")
    private String securityProtocol;

    @Value("${spring.kafka.consumer.ssl.trust-store-location:}")
    private String trustStoreLocation;

    @Value("${spring.kafka.consumer.ssl.trust-store-password:}")
    private String trustStorePassword;

    @Value("${spring.kafka.consumer.ssl.key-store-location:}")
    private String keyStoreLocation;

    @Value("${spring.kafka.consumer.ssl.key-store-password:}")
    private String keyStorePassword;

    @Value("${spring.kafka.consumer.ssl.key-password:}")
    private String keyPassword;

    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configs.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000");
        configs.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "10000");

        // Set security protocol
        configs.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, securityProtocol);

        // Configure SSL if protocol is SSL or SASL_SSL
        if (securityProtocol.contains("SSL")) {
            log.info("Configuring SSL for KafkaAdmin");
            
            if (StringUtils.hasText(trustStoreLocation)) {
                configs.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, trustStoreLocation);
                configs.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, trustStorePassword);
                log.debug("Configured truststore location: {}", trustStoreLocation);
            }

            if (StringUtils.hasText(keyStoreLocation)) {
                configs.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keyStoreLocation);
                configs.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, keyStorePassword);
                
                // Key password defaults to keystore password if not specified
                String actualKeyPassword = StringUtils.hasText(keyPassword) ? keyPassword : keyStorePassword;
                configs.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, actualKeyPassword);
                
                log.debug("Configured keystore location: {}", keyStoreLocation);
            }

            // Additional SSL configurations
            configs.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, ""); // Disable hostname verification
        }

        log.info("Creating KafkaAdmin with security protocol: {}", securityProtocol);
        return new KafkaAdmin(configs);
    }
} 