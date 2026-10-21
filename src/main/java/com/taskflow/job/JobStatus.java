package com.taskflow.job;

/**
 * SCHEDULED -> QUEUED -> RUNNING -> SUCCEEDED
 *                ^          |
 *                |          +-> SCHEDULED (retry with backoff, or lease expired)
 *                |          +-> DEAD (attempts exhausted / non-retryable)
 *                +----------+-> CANCELLED
 */
public enum JobStatus {
    SCHEDULED, QUEUED, RUNNING, SUCCEEDED, DEAD, CANCELLED;

    public boolean isTerminal() {
        return this == SUCCEEDED || this == DEAD || this == CANCELLED;
    }
}
