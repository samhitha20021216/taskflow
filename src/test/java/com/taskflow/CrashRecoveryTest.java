package com.taskflow;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.taskflow.job.Job;
import com.taskflow.job.JobEvent;
import com.taskflow.job.JobStatus;
import com.taskflow.worker.handlers.ReportGenerationHandler;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Simulates a worker that claims a job, makes progress, then dies (no more heartbeats).
 * The sweeper must notice the expired lease, the job must be re-dispatched, and a healthy worker must
 * resume from the dead worker's checkpoint — without double-counting any rows.
 */
class CrashRecoveryTest extends IntegrationTest {

    @Autowired
    JdbcClient jdbc;

    @Test
    void jobFromCrashedWorkerIsRecoveredAndResumedFromCheckpoint() {
        Job ghostJob = claimAsGhostWorker(Map.of("rows", 1000, "chunkSize", 100, "msPerChunk", 10));
        long checksumOfFirst400Rows = 400L * 401 / 2;
        assertThat(jobRepository.saveCheckpoint(ghostJob.id(), ghostJob.fence(),
                Map.of("offset", 400, "checksum", checksumOfFirst400Rows), 40)).isTrue();

        // The ghost never heartbeats again. Its 1.5s lease expires and the sweeper takes over.
        Job done = awaitStatus(ghostJob.id(), JobStatus.SUCCEEDED);

        assertThat(done.attempt()).isEqualTo(2);
        assertThat(done.result()).containsEntry("resumedFromOffset", 400);
        assertThat(((Number) done.result().get("checksum")).longValue()).isEqualTo(1000L * 1001 / 2);
        assertThat(eventTypes(ghostJob.id())).containsSubsequence(
                JobEvent.CLAIMED, JobEvent.LEASE_EXPIRED, JobEvent.DISPATCHED, JobEvent.CLAIMED, JobEvent.SUCCEEDED);
    }

    @Test
    void staleWorkerCannotOverwriteStateAfterLosingItsLease() {
        Job ghostJob = claimAsGhostWorker(Map.of("rows", 200, "chunkSize", 100, "msPerChunk", 10));
        long staleFence = ghostJob.fence();

        Job done = awaitStatus(ghostJob.id(), JobStatus.SUCCEEDED);
        assertThat(done.fence()).isGreaterThan(staleFence);

        // The "zombie" wakes up from its GC pause and tries to write: every write must be rejected.
        assertThat(jobRepository.saveCheckpoint(ghostJob.id(), staleFence, Map.of("offset", 0), 0)).isFalse();
        assertThat(jobRepository.markDead(ghostJob.id(), staleFence, "zombie")).isFalse();
        assertThat(jobRepository.heartbeat(ghostJob.id(), staleFence, java.time.Duration.ofSeconds(5))).isEmpty();
        assertThat(jobService.get(ghostJob.id()).status()).isEqualTo(JobStatus.SUCCEEDED);
    }

    @Test
    void jobIsBuriedWhenCrashesExhaustItsAttempts() {
        Job ghostJob = claimAsGhostWorker(Map.of("rows", 100), 1);

        Job dead = awaitStatus(ghostJob.id(), JobStatus.DEAD);

        assertThat(dead.lastError()).contains("ghost-worker");
        assertThat(eventTypes(ghostJob.id())).contains(JobEvent.LEASE_EXPIRED, JobEvent.DEAD);
    }

    @Test
    void queuedJobWhoseMessageWasLostIsRedelivered() {
        UUID id = UUID.randomUUID();
        jobRepository.insert(id, ReportGenerationHandler.TYPE, Map.of("rows", 100, "msPerChunk", 1), 0, null, 3,
                Instant.now().plus(1, ChronoUnit.DAYS));
        // Pretend the dispatcher marked it QUEUED but the Kafka message never arrived.
        jdbc.sql("UPDATE jobs SET status = 'QUEUED', queued_at = now() WHERE id = :id").param("id", id).update();

        awaitStatus(id, JobStatus.SUCCEEDED);

        assertThat(eventTypes(id)).containsSubsequence(JobEvent.REDELIVERED, JobEvent.DISPATCHED, JobEvent.CLAIMED);
    }

    private Job claimAsGhostWorker(Map<String, Object> payload) {
        return claimAsGhostWorker(payload, 5);
    }

    /** Creates a job and claims it directly as a worker that will never heartbeat. */
    private Job claimAsGhostWorker(Map<String, Object> payload, int maxAttempts) {
        UUID id = UUID.randomUUID();
        jobRepository.insert(id, ReportGenerationHandler.TYPE, payload, 0, null, maxAttempts,
                Instant.now().plus(1, ChronoUnit.DAYS));
        jdbc.sql("UPDATE jobs SET status = 'QUEUED', queued_at = now() WHERE id = :id").param("id", id).update();
        Job claimed = jobRepository.claim(id, "ghost-worker", java.time.Duration.ofMillis(1500)).orElseThrow();
        jobRepository.addEvent(id, JobEvent.CLAIMED, claimed.attempt(), "ghost-worker", null);
        return claimed;
    }
}
