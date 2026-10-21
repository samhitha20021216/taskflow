package com.taskflow.worker;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;

/** What a {@link JobHandler} sees while it runs. */
public interface JobContext {

    UUID jobId();

    String type();

    /** 1-based attempt number. */
    int attempt();

    Map<String, Object> payload();

    /** State saved by a previous attempt via {@link #checkpoint}, or an empty map on a fresh start. */
    Map<String, Object> checkpoint();

    /**
     * Durably saves progress so a later attempt (on any worker) can resume from here.
     *
     * @throws LeaseLostException if another worker now owns the job
     * @throws JobCancelledException if cancellation was requested
     */
    void checkpoint(Map<String, Object> state, int progressPercent);

    /** Throws if the job was cancelled, its lease was lost, or the worker is shutting down. */
    void throwIfStopped();

    /** Sleeps in small slices so cancellation is noticed promptly. */
    void sleep(Duration duration);

    default int intParam(String name, int defaultValue) {
        Object value = payload().get(name);
        return value instanceof Number n ? n.intValue() : defaultValue;
    }
}
