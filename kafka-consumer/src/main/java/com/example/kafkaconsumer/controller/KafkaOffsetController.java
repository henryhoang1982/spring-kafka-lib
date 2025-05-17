package com.example.kafkaconsumer.controller;

import com.example.kafkaconsumer.dto.OffsetResetRequest;
import com.example.kafkaconsumer.service.KafkaOffsetService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/kafka")
@RequiredArgsConstructor
public class KafkaOffsetController {

    private final KafkaOffsetService kafkaOffsetService;

    @PostMapping("/offset/reset")
    public ResponseEntity<Map<String, Object>> resetOffset(@Valid @RequestBody OffsetResetRequest request) {
        log.info("Received request to reset offset for topic={}, partition={} to offset={}", 
                request.getTopic(), request.getPartition(), request.getOffset());
        
        try {
            kafkaOffsetService.resetOffset(request.getTopic(), request.getPartition(), request.getOffset());
            return ResponseEntity.ok(Map.of(
                "status", "success",
                "message", String.format("Successfully reset offset for topic=%s, partition=%d to offset=%d", 
                        request.getTopic(), request.getPartition(), request.getOffset())
            ));
        } catch (Exception e) {
            log.error("Error resetting offset: {}", e.getMessage(), e);
            return ResponseEntity.badRequest().body(Map.of(
                "status", "error",
                "message", "Failed to reset offset: " + e.getMessage()
            ));
        }
    }
} 