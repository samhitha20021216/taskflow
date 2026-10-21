package com.taskflow.worker;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import com.taskflow.job.Job;
import com.taskflow.job.JobRepository;

/** A job currently executing on this worker; doubles as the handler's {@link JobContext}. */
class RunningJob implements JobContext {

    enum StopReason { NONE, CANCEL_REQUESTED, LEASE_LOST, SHUTDOWN }

    private final Job job;
    private final JobRepository repository;
    private volatile StopReason stopReason = StopReason.NONE;

    RunningJob(Job job, JobRepository repository) {
        this.job = job;
        this.repository = repository;
    }

    Job job() {
        return job;
    }

    long fence() {
        return job.fence();
    }

    StopReason stopReason() {
        return stopReason;
    }

    void stop(StopReason reason) {
        // Lease loss wins over everything: once lost we must not write anything.
        if (stopReason != StopReason.LEASE_LOST) {
            stopReason = reason;
        }
    }

    @Override
    public UUID jobId() {
        return job.id();
    }

    @Override
    public String type() {
        return job.type();
    }

    @Override
    public int attempt() {
        return job.attempt();
    }

    @Override
    public Map<String, Object> payload() {
        return job.payload() == null ? Map.of() : job.payload();
    }

    @Override
    public Map<String, Object> checkpoint() {
        return job.checkpoint() == null ? Map.of() : job.checkpoint();
    }

    @Override
    public void checkpoint(Map<String, Object> state, int progressPercent) {
        throwIfStopped();
        if (!repository.saveCheckpoint(job.id(), job.fence(), state, progressPercent)) {
            stop(StopReason.LEASE_LOST);
            throwIfStopped();
        }
    }

    @Override
    public void throwIfStopped() {
        switch (stopReason) {
            case NONE -> {
            }
            case LEASE_LOST -> throw new LeaseLostException("Lease lost for job " + job.id());
            case CANCEL_REQUESTED -> throw new JobCancelledException("Cancellation requested");
            case SHUTDOWN -> throw new JobCancelledException("Worker shutting down");
        }
    }

    @Override
    public void sleep(Duration duration) {
        long deadline = System.nanoTime() + duration.toNanos();
        while (true) {
            throwIfStopped();
            long remainingMs = (deadline - System.nanoTime()) / 1_000_000;
            if (remainingMs <= 0) {
                return;
            }
            try {
                Thread.sleep(Math.min(remainingMs, 50));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                stop(StopReason.SHUTDOWN);
                throwIfStopped();
            }
        }
    }
}
