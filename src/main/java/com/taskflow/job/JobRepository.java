package com.taskflow.job;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import com.taskflow.config.Json;

/**
 * All job state transitions live here as single conditional UPDATE statements.
 *
 * <p>Every transition performed by a worker is guarded by {@code fence}: the value is incremented on
 * each claim, so if a worker stalls (GC pause, network partition) and its lease is handed to another
 * worker, the stale worker's writes match zero rows and are rejected.
 */
@Repository
public class JobRepository {

    private final JdbcClient jdbc;
    private final Json json;
    private final RowMapper<Job> jobMapper;

    public JobRepository(JdbcClient jdbc, Json json) {
        this.jdbc = jdbc;
        this.json = json;
        this.jobMapper = this::mapJob;
    }

    // ---------------------------------------------------------------- submission & queries

    /** Inserts a job; returns false if a job with the same idempotency key already exists. */
    public boolean insert(UUID id, String type, Map<String, Object> payload, int priority,
                          String idempotencyKey, int maxAttempts, Instant runAt) {
        return jdbc.sql("""
                        INSERT INTO jobs (id, type, payload, status, priority, idempotency_key, max_attempts, next_run_at)
                        VALUES (:id, :type, CAST(:payload AS jsonb), 'SCHEDULED', :priority, :key, :maxAttempts, :runAt)
                        ON CONFLICT (idempotency_key) DO NOTHING
                        """)
                .param("id", id)
                .param("type", type)
                .param("payload", json.write(payload))
                .param("priority", priority)
                .param("key", idempotencyKey)
                .param("maxAttempts", maxAttempts)
                .param("runAt", OffsetDateTime.ofInstant(runAt, java.time.ZoneOffset.UTC))
                .update() == 1;
    }

    public Optional<Job> findById(UUID id) {
        return jdbc.sql("SELECT * FROM jobs WHERE id = :id").param("id", id).query(jobMapper).optional();
    }

    public Optional<Job> findByIdempotencyKey(String key) {
        return jdbc.sql("SELECT * FROM jobs WHERE idempotency_key = :key").param("key", key).query(jobMapper).optional();
    }

    public List<Job> list(JobStatus status, String type, int limit, int offset) {
        return jdbc.sql("""
                        SELECT * FROM jobs
                        WHERE (CAST(:status AS varchar) IS NULL OR status = :status)
                          AND (CAST(:type AS varchar) IS NULL OR type = :type)
                        ORDER BY created_at DESC
                        LIMIT :limit OFFSET :offset
                        """)
                .param("status", status == null ? null : status.name())
                .param("type", type)
                .param("limit", limit)
                .param("offset", offset)
                .query(jobMapper)
                .list();
    }

    public Map<JobStatus, Long> countByStatus() {
        Map<JobStatus, Long> counts = new LinkedHashMap<>();
        for (JobStatus s : JobStatus.values()) {
            counts.put(s, 0L);
        }
        jdbc.sql("SELECT status, count(*) AS n FROM jobs GROUP BY status")
                .query((rs, i) -> counts.put(JobStatus.valueOf(rs.getString("status")), rs.getLong("n")))
                .list();
        return counts;
    }

    // ---------------------------------------------------------------- scheduler side

    public record DispatchedJob(UUID id, String type, int attempt) {
    }

    /**
     * Moves due SCHEDULED jobs to QUEUED. SKIP LOCKED lets several schedulers run concurrently
     * without double-dispatching (leader election is an optimization, not a correctness requirement).
     */
    public List<DispatchedJob> markDueJobsQueued(int limit) {
        return jdbc.sql("""
                        UPDATE jobs SET status = 'QUEUED', queued_at = now(), updated_at = now()
                        WHERE id IN (
                            SELECT id FROM jobs
                            WHERE status = 'SCHEDULED' AND next_run_at <= now()
                            ORDER BY priority DESC, next_run_at
                            LIMIT :limit
                            FOR UPDATE SKIP LOCKED)
                        RETURNING id, type, attempt
                        """)
                .param("limit", limit)
                .query((rs, i) -> new DispatchedJob(rs.getObject("id", UUID.class), rs.getString("type"), rs.getInt("attempt")))
                .list();
    }

    public record RecoveredJob(UUID id, JobStatus newStatus, int attempt, String previousOwner) {
    }

    /** Jobs whose worker stopped heartbeating: reschedule them, or bury them if out of attempts. */
    public List<RecoveredJob> recoverExpiredLeases(int limit) {
        return jdbc.sql("""
                        WITH expired AS (
                            SELECT id, lease_owner FROM jobs
                            WHERE status = 'RUNNING' AND lease_expires_at < now()
                            LIMIT :limit
                            FOR UPDATE SKIP LOCKED)
                        UPDATE jobs j SET
                            status = CASE
                                WHEN j.cancel_requested THEN 'CANCELLED'
                                WHEN j.attempt >= j.max_attempts THEN 'DEAD'
                                ELSE 'SCHEDULED' END,
                            next_run_at = now(),
                            finished_at = CASE
                                WHEN j.cancel_requested OR j.attempt >= j.max_attempts THEN now() END,
                            last_error = 'Lease expired: worker ' || COALESCE(j.lease_owner, '?') || ' stopped heartbeating',
                            lease_owner = NULL,
                            lease_expires_at = NULL,
                            updated_at = now()
                        FROM expired e
                        WHERE j.id = e.id
                        RETURNING j.id, j.status, j.attempt, e.lease_owner AS previous_owner
                        """)
                .param("limit", limit)
                .query((rs, i) -> new RecoveredJob(rs.getObject("id", UUID.class),
                        JobStatus.valueOf(rs.getString("status")), rs.getInt("attempt"), rs.getString("previous_owner")))
                .list();
    }

    /** QUEUED jobs nobody claimed (e.g. the Kafka message was lost): put them back to be re-dispatched. */
    public List<UUID> requeueStuckQueued(Duration olderThan, int limit) {
        return jdbc.sql("""
                        UPDATE jobs SET status = 'SCHEDULED', queued_at = NULL, next_run_at = now(), updated_at = now()
                        WHERE id IN (
                            SELECT id FROM jobs
                            WHERE status = 'QUEUED' AND queued_at < now() - make_interval(secs => :secs)
                            LIMIT :limit
                            FOR UPDATE SKIP LOCKED)
                        RETURNING id
                        """)
                .param("secs", seconds(olderThan))
                .param("limit", limit)
                .query((rs, i) -> rs.getObject("id", UUID.class))
                .list();
    }

    // ---------------------------------------------------------------- worker side (fenced)

    /** Atomically takes ownership of a QUEUED job. Empty if another worker got it first. */
    public Optional<Job> claim(UUID id, String workerId, Duration lease) {
        return jdbc.sql("""
                        UPDATE jobs SET
                            status = 'RUNNING',
                            attempt = attempt + 1,
                            fence = fence + 1,
                            lease_owner = :worker,
                            lease_expires_at = now() + make_interval(secs => :secs),
                            started_at = COALESCE(started_at, now()),
                            updated_at = now()
                        WHERE id = :id AND status = 'QUEUED'
                        RETURNING *
                        """)
                .param("id", id)
                .param("worker", workerId)
                .param("secs", seconds(lease))
                .query(jobMapper)
                .optional();
    }

    /**
     * Extends the lease. Returns empty if the lease was lost, otherwise whether cancellation
     * has been requested for the job.
     */
    public Optional<Boolean> heartbeat(UUID id, long fence, Duration lease) {
        return jdbc.sql("""
                        UPDATE jobs SET lease_expires_at = now() + make_interval(secs => :secs), updated_at = now()
                        WHERE id = :id AND fence = :fence AND status = 'RUNNING'
                        RETURNING cancel_requested
                        """)
                .param("id", id)
                .param("fence", fence)
                .param("secs", seconds(lease))
                .query((rs, i) -> rs.getBoolean("cancel_requested"))
                .optional();
    }

    public boolean saveCheckpoint(UUID id, long fence, Map<String, Object> checkpoint, int progress) {
        return jdbc.sql("""
                        UPDATE jobs SET checkpoint = CAST(:cp AS jsonb), progress = :progress, updated_at = now()
                        WHERE id = :id AND fence = :fence AND status = 'RUNNING'
                        """)
                .param("id", id)
                .param("fence", fence)
                .param("cp", json.write(checkpoint))
                .param("progress", Math.clamp(progress, 0, 100))
                .update() == 1;
    }

    public boolean markSucceeded(UUID id, long fence, Map<String, Object> result) {
        return jdbc.sql("""
                        UPDATE jobs SET status = 'SUCCEEDED', result = CAST(:result AS jsonb), progress = 100,
                            last_error = NULL, finished_at = now(), lease_owner = NULL, lease_expires_at = NULL,
                            updated_at = now()
                        WHERE id = :id AND fence = :fence AND status = 'RUNNING'
                        """)
                .param("id", id)
                .param("fence", fence)
                .param("result", json.write(result))
                .update() == 1;
    }

    public boolean scheduleRetry(UUID id, long fence, Duration delay, String error) {
        return jdbc.sql("""
                        UPDATE jobs SET status = 'SCHEDULED', next_run_at = now() + make_interval(secs => :secs),
                            last_error = :error, queued_at = NULL, lease_owner = NULL, lease_expires_at = NULL,
                            updated_at = now()
                        WHERE id = :id AND fence = :fence AND status = 'RUNNING'
                        """)
                .param("id", id)
                .param("fence", fence)
                .param("secs", seconds(delay))
                .param("error", truncate(error))
                .update() == 1;
    }

    public boolean markDead(UUID id, long fence, String error) {
        return finishFenced(id, fence, JobStatus.DEAD, error);
    }

    public boolean markCancelled(UUID id, long fence) {
        return finishFenced(id, fence, JobStatus.CANCELLED, "Cancelled while running");
    }

    /** Graceful shutdown: hand the job back without consuming an attempt. Checkpoint is kept. */
    public boolean release(UUID id, long fence) {
        return jdbc.sql("""
                        UPDATE jobs SET status = 'SCHEDULED', attempt = GREATEST(attempt - 1, 0), next_run_at = now(),
                            queued_at = NULL, lease_owner = NULL, lease_expires_at = NULL, updated_at = now()
                        WHERE id = :id AND fence = :fence AND status = 'RUNNING'
                        """)
                .param("id", id)
                .param("fence", fence)
                .update() == 1;
    }

    private boolean finishFenced(UUID id, long fence, JobStatus status, String error) {
        return jdbc.sql("""
                        UPDATE jobs SET status = :status, last_error = :error, finished_at = now(),
                            lease_owner = NULL, lease_expires_at = NULL, updated_at = now()
                        WHERE id = :id AND fence = :fence AND status = 'RUNNING'
                        """)
                .param("id", id)
                .param("fence", fence)
                .param("status", status.name())
                .param("error", truncate(error))
                .update() == 1;
    }

    // ---------------------------------------------------------------- operator actions

    /** Cancels a job that has not started yet. */
    public boolean cancelIfNotStarted(UUID id) {
        return jdbc.sql("""
                        UPDATE jobs SET status = 'CANCELLED', finished_at = now(), updated_at = now()
                        WHERE id = :id AND status IN ('SCHEDULED', 'QUEUED')
                        """)
                .param("id", id)
                .update() == 1;
    }

    /** Asks the owning worker to stop; it notices on its next heartbeat. */
    public boolean requestCancel(UUID id) {
        return jdbc.sql("""
                        UPDATE jobs SET cancel_requested = true, updated_at = now()
                        WHERE id = :id AND status = 'RUNNING'
                        """)
                .param("id", id)
                .update() == 1;
    }

    /** Revives a DEAD or CANCELLED job with a fresh attempt budget. The checkpoint is kept, so it resumes. */
    public boolean revive(UUID id) {
        return jdbc.sql("""
                        UPDATE jobs SET status = 'SCHEDULED', attempt = 0, next_run_at = now(), cancel_requested = false,
                            last_error = NULL, finished_at = NULL, queued_at = NULL, updated_at = now()
                        WHERE id = :id AND status IN ('DEAD', 'CANCELLED')
                        """)
                .param("id", id)
                .update() == 1;
    }

    // ---------------------------------------------------------------- events

    public void addEvent(UUID jobId, String type, int attempt, String worker, String message) {
        jdbc.sql("""
                        INSERT INTO job_events (job_id, type, attempt, worker, message)
                        VALUES (:jobId, :type, :attempt, :worker, :message)
                        """)
                .param("jobId", jobId)
                .param("type", type)
                .param("attempt", attempt)
                .param("worker", worker)
                .param("message", truncate(message))
                .update();
    }

    public List<JobEvent> events(UUID jobId) {
        return jdbc.sql("SELECT * FROM job_events WHERE job_id = :jobId ORDER BY id")
                .param("jobId", jobId)
                .query((rs, i) -> new JobEvent(rs.getLong("id"), rs.getObject("job_id", UUID.class),
                        rs.getString("type"), rs.getInt("attempt"), rs.getString("worker"),
                        rs.getString("message"), instant(rs, "created_at")))
                .list();
    }

    // ---------------------------------------------------------------- mapping helpers

    private Job mapJob(ResultSet rs, int rowNum) throws SQLException {
        return new Job(
                rs.getObject("id", UUID.class),
                rs.getString("type"),
                json.readMap(rs.getString("payload")),
                JobStatus.valueOf(rs.getString("status")),
                rs.getInt("priority"),
                rs.getString("idempotency_key"),
                rs.getInt("attempt"),
                rs.getInt("max_attempts"),
                instant(rs, "next_run_at"),
                rs.getString("lease_owner"),
                instant(rs, "lease_expires_at"),
                rs.getLong("fence"),
                rs.getBoolean("cancel_requested"),
                json.readMap(rs.getString("checkpoint")),
                rs.getInt("progress"),
                json.readMap(rs.getString("result")),
                rs.getString("last_error"),
                instant(rs, "created_at"),
                instant(rs, "started_at"),
                instant(rs, "finished_at"),
                instant(rs, "updated_at"));
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        OffsetDateTime value = rs.getObject(column, OffsetDateTime.class);
        return value == null ? null : value.toInstant();
    }

    private static double seconds(Duration d) {
        return d.toMillis() / 1000.0;
    }

    private static String truncate(String s) {
        return s == null || s.length() <= 4000 ? s : s.substring(0, 4000);
    }
}
