package com.taskflow.api;

import java.util.Map;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

public record SubmitJobRequest(
        @NotBlank String type,
        Map<String, Object> payload,
        Integer priority,
        @Min(1) @Max(50) Integer maxAttempts,
        @Min(0) Long delaySeconds) {
}
