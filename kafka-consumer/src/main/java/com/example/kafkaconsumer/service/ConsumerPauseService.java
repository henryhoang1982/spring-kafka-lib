package com.example.kafkaconsumer.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Service
public class ConsumerPauseService {
    
    private final KafkaListenerEndpointRegistry registry;

    public ConsumerPauseService(KafkaListenerEndpointRegistry registry) {
        this.registry = registry;
    }

    public void pauseConsumer(String listenerId) {
        MessageListenerContainer container = registry.getListenerContainer(listenerId);
        if (container != null && container.isRunning()) {
            log.info("Pausing consumer for listener ID: {}", listenerId);
            container.pause();
            log.info("Consumer paused successfully. Metrics will continue to be reported.");
        } else {
            log.warn("Container not found or not running for listener ID: {}", listenerId);
        }
    }

    public void resumeConsumer(String listenerId) {
        MessageListenerContainer container = registry.getListenerContainer(listenerId);
        if (container != null && container.isRunning() && container.isPauseRequested()) {
            log.info("Resuming consumer for listener ID: {}", listenerId);
            container.resume();
            log.info("Consumer resumed successfully");
        } else {
            log.warn("Container not found, not running, or not paused for listener ID: {}", listenerId);
        }
    }

    public Collection<String> getListenerIds() {
        return registry.getListenerContainerIds();
    }

    public Map<String, String> pauseAllConsumers() {
        Map<String, String> results = new HashMap<>();
        registry.getListenerContainers().forEach(container -> {
            String listenerId = container.getListenerId();
            try {
                if (container.isRunning() && !container.isPauseRequested()) {
                    container.pause();
                    results.put(listenerId, "Paused successfully");
                    log.info("Paused consumer for listener ID: {}", listenerId);
                } else if (container.isPauseRequested()) {
                    results.put(listenerId, "Already paused");
                    log.debug("Consumer already paused for listener ID: {}", listenerId);
                } else {
                    results.put(listenerId, "Not running");
                    log.warn("Container not running for listener ID: {}", listenerId);
                }
            } catch (Exception e) {
                results.put(listenerId, "Error: " + e.getMessage());
                log.error("Error pausing consumer for listener ID {}: {}", listenerId, e.getMessage());
            }
        });
        return results;
    }

    public Map<String, String> resumeAllConsumers() {
        Map<String, String> results = new HashMap<>();
        registry.getListenerContainers().forEach(container -> {
            String listenerId = container.getListenerId();
            try {
                if (container.isRunning() && container.isPauseRequested()) {
                    container.resume();
                    results.put(listenerId, "Resumed successfully");
                    log.info("Resumed consumer for listener ID: {}", listenerId);
                } else if (!container.isPauseRequested()) {
                    results.put(listenerId, "Not paused");
                    log.debug("Consumer not paused for listener ID: {}", listenerId);
                } else {
                    results.put(listenerId, "Not running");
                    log.warn("Container not running for listener ID: {}", listenerId);
                }
            } catch (Exception e) {
                results.put(listenerId, "Error: " + e.getMessage());
                log.error("Error resuming consumer for listener ID {}: {}", listenerId, e.getMessage());
            }
        });
        return results;
    }
} 