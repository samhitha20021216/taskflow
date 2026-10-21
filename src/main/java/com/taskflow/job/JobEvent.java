package com.taskflow.job;

import java.time.Instant;
import java.util.UUID;

public record JobEvent(long id, UUID jobId, String type, int attempt, String worker, String message, Instant createdAt) {

    public static final String SUBMITTED = "SUBMITTED";
    public static final String DISPATCHED = "DISPATCHED";
    public static final String CLAIMED = "CLAIMED";
    public static final String SUCCEEDED = "SUCCEEDED";
    public static final String RETRY_SCHEDULED = "RETRY_SCHEDULED";
    public static final String DEAD = "DEAD";
    public static final String LEASE_EXPIRED = "LEASE_EXPIRED";
    public static final String LEASE_LOST = "LEASE_LOST";
    public static final String REDELIVERED = "REDELIVERED";
    public static final String CANCEL_REQUESTED = "CANCEL_REQUESTED";
    public static final String CANCELLED = "CANCELLED";
    public static final String RELEASED = "RELEASED";
    public static final String MANUAL_RETRY = "MANUAL_RETRY";
}
