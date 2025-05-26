package com.example.kafkametrics.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Configuration
public class KafkaAdminConfig {

    @Value("${spring.kafka.consumer.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.properties.security.protocol:PLAINTEXT}")
    private String securityProtocol;

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
            log.info("Configuring SSL for KafkaAdmin using JVM system properties");
            
            // Use the same JVM system properties that are set in docker-entrypoint.sh
            String trustStore = System.getProperty("javax.net.ssl.trustStore");
            String trustStorePassword = System.getProperty("javax.net.ssl.trustStorePassword");
            String keyStore = System.getProperty("javax.net.ssl.keyStore");
            String keyStorePassword = System.getProperty("javax.net.ssl.keyStorePassword");

            if (trustStore != null) {
                configs.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, trustStore);
                configs.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, trustStorePassword);
                log.debug("Configured truststore location from JVM property: {}", trustStore);
            }

            if (keyStore != null) {
                configs.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keyStore);
                configs.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, keyStorePassword);
                configs.put(SslConfigs.SSL_KEY_PASSWORD_CONFIG, keyStorePassword);
                log.debug("Configured keystore location from JVM property: {}", keyStore);
            }

            // Additional SSL configurations
            configs.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, ""); // Disable hostname verification
        }

        log.info("Creating KafkaAdmin with security protocol: {}", securityProtocol);
        return new KafkaAdmin(configs);
    }
} 