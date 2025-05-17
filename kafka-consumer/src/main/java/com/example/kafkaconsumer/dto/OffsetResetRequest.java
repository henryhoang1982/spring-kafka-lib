package com.example.kafkaconsumer.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;

@Data
public class OffsetResetRequest {
    @NotBlank(message = "Topic name is required")
    private String topic;
    
    @Min(value = 0, message = "Partition must be a non-negative number")
    private int partition;
    
    @Min(value = 0, message = "Offset must be a non-negative number")
    private long offset;
} 