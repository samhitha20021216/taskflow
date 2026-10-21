package com.taskflow.job;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.taskflow.worker.HandlerRegistry;

@Service
public class JobService {

    public record SubmitCommand(String type, Map<String, Object> payload, int priority, int maxAttempts,
                                long delaySeconds, String idempotencyKey) {
    }

    public record SubmitResult(Job job, boolean created) {
    }

    private final JobRepository repository;
    private final HandlerRegistry handlers;

    public JobService(JobRepository repository, HandlerRegistry handlers) {
        this.repository = repository;
        this.handlers = handlers;
    }

    @Transactional
    public SubmitResult submit(SubmitCommand cmd) {
        if (handlers.find(cmd.type()).isEmpty()) {
            throw new IllegalArgumentException("Unknown job type '" + cmd.type() + "'. Known: " + handlers.types());
        }
        UUID id = UUID.randomUUID();
        Instant runAt = Instant.now().plusSeconds(Math.max(0, cmd.delaySeconds()));
        boolean created = repository.insert(id, cmd.type(), cmd.payload() == null ? Map.of() : cmd.payload(),
                cmd.priority(), cmd.idempotencyKey(), cmd.maxAttempts(), runAt);
        if (!created) {
            // Same idempotency key seen before: return the original job instead of creating a duplicate.
            return new SubmitResult(repository.findByIdempotencyKey(cmd.idempotencyKey()).orElseThrow(), false);
        }
        repository.addEvent(id, JobEvent.SUBMITTED, 0, null, null);
        return new SubmitResult(get(id), true);
    }

    public Job get(UUID id) {
        return repository.findById(id).orElseThrow(() -> new JobNotFoundException(id));
    }

    public List<Job> list(JobStatus status, String type, int limit, int offset) {
        return repository.list(status, type, Math.clamp(limit, 1, 500), Math.max(0, offset));
    }

    public List<JobEvent> events(UUID id) {
        get(id);
        return repository.events(id);
    }

    @Transactional
    public Job cancel(UUID id) {
        Job job = get(id);
        if (repository.cancelIfNotStarted(id)) {
            repository.addEvent(id, JobEvent.CANCELLED, job.attempt(), null, "Cancelled before start");
        } else if (repository.requestCancel(id)) {
            repository.addEvent(id, JobEvent.CANCEL_REQUESTED, job.attempt(), null, "Worker will stop at next heartbeat");
        } else {
            throw new IllegalStateException("Job " + id + " is already " + job.status());
        }
        return get(id);
    }

    @Transactional
    public Job retry(UUID id) {
        Job job = get(id);
        if (!repository.revive(id)) {
            throw new IllegalStateException("Only DEAD or CANCELLED jobs can be retried; job is " + job.status());
        }
        repository.addEvent(id, JobEvent.MANUAL_RETRY, 0, null, "Revived by operator");
        return get(id);
    }

    public Map<JobStatus, Long> counts() {
        return repository.countByStatus();
    }
}
