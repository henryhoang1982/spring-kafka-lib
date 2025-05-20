package com.example.kafkaconsumer.controller;

import com.example.kafkaconsumer.service.ConsumerPauseService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Collection;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/kafka/consumer")
public class ConsumerController {

    private final ConsumerPauseService pauseService;

    public ConsumerController(ConsumerPauseService pauseService) {
        this.pauseService = pauseService;
    }

    @PostMapping("/pause")
    public ResponseEntity<?> pauseConsumer(@RequestParam String listenerId) {
        try {
            pauseService.pauseConsumer(listenerId);
            return ResponseEntity.ok(Map.of(
                "status", "success",
                "message", "Consumer paused successfully"
            ));
        } catch (Exception e) {
            log.error("Error pausing consumer: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(Map.of(
                "status", "error",
                "message", "Failed to pause consumer: " + e.getMessage()
            ));
        }
    }

    @PostMapping("/resume")
    public ResponseEntity<?> resumeConsumer(@RequestParam String listenerId) {
        try {
            pauseService.resumeConsumer(listenerId);
            return ResponseEntity.ok(Map.of(
                "status", "success",
                "message", "Consumer resumed successfully"
            ));
        } catch (Exception e) {
            log.error("Error resuming consumer: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(Map.of(
                "status", "error",
                "message", "Failed to resume consumer: " + e.getMessage()
            ));
        }
    }

    @GetMapping("/listeners")
    public ResponseEntity<Collection<String>> getListenerIds() {
        return ResponseEntity.ok(pauseService.getListenerIds());
    }

    @PostMapping("/pause/all")
    public ResponseEntity<Map<String, String>> pauseAllConsumers() {
        try {
            Map<String, String> results = pauseService.pauseAllConsumers();
            log.info("Paused all consumers with results: {}", results);
            return ResponseEntity.ok(results);
        } catch (Exception e) {
            log.error("Error pausing all consumers: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(Map.of(
                "error", "Failed to pause all consumers: " + e.getMessage()
            ));
        }
    }

    @PostMapping("/resume/all")
    public ResponseEntity<Map<String, String>> resumeAllConsumers() {
        try {
            Map<String, String> results = pauseService.resumeAllConsumers();
            log.info("Resumed all consumers with results: {}", results);
            return ResponseEntity.ok(results);
        } catch (Exception e) {
            log.error("Error resuming all consumers: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(Map.of(
                "error", "Failed to resume all consumers: " + e.getMessage()
            ));
        }
    }
} 