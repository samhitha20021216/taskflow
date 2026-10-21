package com.taskflow.job;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public record Job(
        UUID id,
        String type,
        Map<String, Object> payload,
        JobStatus status,
        int priority,
        String idempotencyKey,
        int attempt,
        int maxAttempts,
        Instant nextRunAt,
        String leaseOwner,
        Instant leaseExpiresAt,
        long fence,
        boolean cancelRequested,
        Map<String, Object> checkpoint,
        int progress,
        Map<String, Object> result,
        String lastError,
        Instant createdAt,
        Instant startedAt,
        Instant finishedAt,
        Instant updatedAt) {
}
