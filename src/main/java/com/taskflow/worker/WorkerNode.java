package com.taskflow.worker;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.taskflow.config.NodeIdentity;
import com.taskflow.config.TaskFlowProperties;
import com.taskflow.coordination.WorkerInfo;
import com.taskflow.coordination.WorkerRegistry;
import com.taskflow.job.BackoffPolicy;
import com.taskflow.job.Job;
import com.taskflow.job.JobEvent;
import com.taskflow.job.JobRepository;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

/**
 * Consumes job ids from Kafka, claims each job with a lease, runs its handler on a bounded pool and
 * keeps the lease alive with heartbeats.
 *
 * <p>Kafka offsets are committed as soon as a job is claimed: from then on Postgres owns the job's fate.
 * If this process dies, the lease expires and the sweeper reschedules the job for another worker.
 */
@Component
public class WorkerNode implements SmartLifecycle {

    static final String LISTENER_ID = "taskflow-worker";
    private static final Logger log = LoggerFactory.getLogger(WorkerNode.class);

    private final JobRepository repository;
    private final HandlerRegistry handlers;
    private final BackoffPolicy backoff;
    private final WorkerRegistry workerRegistry;
    private final KafkaListenerEndpointRegistry listeners;
    private final TaskFlowProperties.Worker config;
    private final NodeIdentity node;
    private final MeterRegistry meters;

    private final Map<UUID, RunningJob> running = new ConcurrentHashMap<>();
    private final Semaphore slots;
    private final ExecutorService executor;
    private final Instant startedAt = Instant.now();
    private volatile boolean active;

    public WorkerNode(JobRepository repository, HandlerRegistry handlers, BackoffPolicy backoff,
                      WorkerRegistry workerRegistry, KafkaListenerEndpointRegistry listeners,
                      TaskFlowProperties props, NodeIdentity node, MeterRegistry meters) {
        this.repository = repository;
        this.handlers = handlers;
        this.backoff = backoff;
        this.workerRegistry = workerRegistry;
        this.listeners = listeners;
        this.config = props.worker();
        this.node = node;
        this.meters = meters;
        this.slots = new Semaphore(config.concurrency());
        AtomicInteger threadNo = new AtomicInteger();
        this.executor = Executors.newFixedThreadPool(config.concurrency(),
                r -> new Thread(r, "job-runner-" + threadNo.incrementAndGet()));
        meters.gauge("taskflow.worker.running", running, Map::size);
    }

    // ---------------------------------------------------------------- intake

    @KafkaListener(id = LISTENER_ID, topics = "${taskflow.topic:taskflow.jobs}", groupId = "taskflow-workers",
            autoStartup = "false", concurrency = "${taskflow.worker.consumer-threads:2}")
    public void onJobMessage(String jobId) throws InterruptedException {
        // Take a slot *before* claiming, so a claimed job never waits for a thread while its lease ticks.
        slots.acquire();
        boolean handedOff = false;
        try {
            Optional<Job> claimed = repository.claim(UUID.fromString(jobId), node.id(), config.leaseDuration());
            if (claimed.isEmpty()) {
                // Duplicate delivery, cancelled, or already taken by another worker.
                meters.counter("taskflow.worker.claims", "result", "skipped").increment();
                return;
            }
            Job job = claimed.get();
            meters.counter("taskflow.worker.claims", "result", "claimed").increment();
            repository.addEvent(job.id(), JobEvent.CLAIMED, job.attempt(), node.id(),
                    job.checkpoint() == null ? null : "Resuming from checkpoint " + job.checkpoint());
            RunningJob run = new RunningJob(job, repository);
            running.put(job.id(), run);
            executor.execute(() -> {
                try {
                    execute(run);
                } finally {
                    running.remove(job.id());
                    slots.release();
                }
            });
            handedOff = true;
        } finally {
            if (!handedOff) {
                slots.release();
            }
        }
    }

    // ---------------------------------------------------------------- execution

    private void execute(RunningJob run) {
        Job job = run.job();
        Optional<JobHandler> handler = handlers.find(job.type());
        if (handler.isEmpty()) {
            finishDead(run, "No handler registered for type '" + job.type() + "'");
            return;
        }
        Timer.Sample sample = Timer.start(meters);
        String outcome;
        try {
            Map<String, Object> result = handler.get().execute(run);
            run.throwIfStopped();
            if (repository.markSucceeded(job.id(), run.fence(), result)) {
                repository.addEvent(job.id(), JobEvent.SUCCEEDED, job.attempt(), node.id(), null);
                outcome = "succeeded";
            } else {
                outcome = leaseLost(run);
            }
        } catch (LeaseLostException e) {
            outcome = leaseLost(run);
        } catch (JobCancelledException e) {
            outcome = stopped(run);
        } catch (NonRetryableJobException e) {
            finishDead(run, "Non-retryable: " + e.getMessage());
            outcome = "dead";
        } catch (Exception e) {
            outcome = failed(run, e);
        }
        sample.stop(meters.timer("taskflow.jobs.duration", "type", job.type(), "outcome", outcome));
        meters.counter("taskflow.jobs.outcome", "type", job.type(), "outcome", outcome).increment();
    }

    private String failed(RunningJob run, Exception e) {
        Job job = run.job();
        String error = e.getClass().getSimpleName() + ": " + e.getMessage();
        if (job.attempt() >= job.maxAttempts()) {
            finishDead(run, "Attempts exhausted. Last error: " + error);
            return "dead";
        }
        Duration delay = backoff.delayAfter(job.attempt());
        if (repository.scheduleRetry(job.id(), run.fence(), delay, error)) {
            repository.addEvent(job.id(), JobEvent.RETRY_SCHEDULED, job.attempt(), node.id(),
                    error + " (retry in " + delay.toMillis() + "ms)");
            return "retry";
        }
        return leaseLost(run);
    }

    private String stopped(RunningJob run) {
        Job job = run.job();
        if (run.stopReason() == RunningJob.StopReason.SHUTDOWN) {
            if (repository.release(job.id(), run.fence())) {
                repository.addEvent(job.id(), JobEvent.RELEASED, job.attempt(), node.id(),
                        "Worker shutting down; job handed back with checkpoint");
            }
            return "released";
        }
        if (repository.markCancelled(job.id(), run.fence())) {
            repository.addEvent(job.id(), JobEvent.CANCELLED, job.attempt(), node.id(), null);
        }
        return "cancelled";
    }

    private String leaseLost(RunningJob run) {
        // Another worker owns the job now (we stalled past our lease). Write nothing else.
        log.warn("Lease lost for job {} (fence {}); abandoning", run.jobId(), run.fence());
        repository.addEvent(run.jobId(), JobEvent.LEASE_LOST, run.attempt(), node.id(), "Stale fence " + run.fence());
        return "lease_lost";
    }

    private void finishDead(RunningJob run, String reason) {
        if (repository.markDead(run.jobId(), run.fence(), reason)) {
            repository.addEvent(run.jobId(), JobEvent.DEAD, run.attempt(), node.id(), reason);
        }
    }

    // ---------------------------------------------------------------- heartbeats

    @Scheduled(fixedDelayString = "${taskflow.worker.heartbeat-interval-ms:5000}")
    public void heartbeat() {
        if (!active) {
            return;
        }
        for (RunningJob run : running.values()) {
            try {
                Optional<Boolean> cancelRequested =
                        repository.heartbeat(run.jobId(), run.fence(), config.leaseDuration());
                if (cancelRequested.isEmpty()) {
                    run.stop(RunningJob.StopReason.LEASE_LOST);
                } else if (cancelRequested.get()) {
                    run.stop(RunningJob.StopReason.CANCEL_REQUESTED);
                }
            } catch (Exception e) {
                // DB unreachable: keep running; if it stays down the lease expires and the job is retried.
                log.warn("Heartbeat failed for job {}: {}", run.jobId(), e.getMessage());
            }
        }
        try {
            workerRegistry.heartbeat(new WorkerInfo(node.id(), node.host(), config.concurrency(), running.size(),
                    startedAt, Instant.now()), config.leaseDuration());
        } catch (Exception e) {
            log.debug("Worker registry heartbeat failed: {}", e.getMessage());
        }
    }

    int runningCount() {
        return running.size();
    }

    // ---------------------------------------------------------------- lifecycle

    @Override
    public void start() {
        if (config.enabled()) {
            listeners.getListenerContainer(LISTENER_ID).start();
            active = true;
            log.info("Worker {} started with {} slots", node.id(), config.concurrency());
        }
    }

    /** Graceful drain: stop intake, ask running jobs to stop at their next checkpoint, release them. */
    @Override
    public void stop() {
        if (!active) {
            return;
        }
        active = false;
        listeners.getListenerContainer(LISTENER_ID).stop();
        running.values().forEach(r -> r.stop(RunningJob.StopReason.SHUTDOWN));
        executor.shutdown();
        try {
            if (!executor.awaitTermination(config.shutdownGracePeriod().toMillis(), TimeUnit.MILLISECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        try {
            workerRegistry.remove(node.id());
        } catch (Exception ignored) {
            // entry expires on its own
        }
        log.info("Worker {} stopped", node.id());
    }

    @Override
    public boolean isRunning() {
        return active;
    }

    @Override
    public int getPhase() {
        // Start after the Kafka infrastructure, stop before it.
        return Integer.MAX_VALUE - 100;
    }
}
