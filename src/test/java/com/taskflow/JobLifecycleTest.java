package com.taskflow;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

import com.taskflow.job.Job;
import com.taskflow.job.JobEvent;
import com.taskflow.job.JobStatus;
import com.taskflow.worker.handlers.FlakyHandler;
import com.taskflow.worker.handlers.PoisonHandler;
import com.taskflow.worker.handlers.ReportGenerationHandler;
import com.taskflow.worker.handlers.SleepHandler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class JobLifecycleTest extends IntegrationTest {

    @Test
    void jobRunsToCompletionThroughKafka() {
        Job job = submit(ReportGenerationHandler.TYPE, Map.of("rows", 300, "chunkSize", 100, "msPerChunk", 10));

        Job done = awaitStatus(job.id(), JobStatus.SUCCEEDED);

        assertThat(done.attempt()).isEqualTo(1);
        assertThat(done.progress()).isEqualTo(100);
        assertThat(((Number) done.result().get("checksum")).longValue()).isEqualTo(300L * 301 / 2);
        assertThat(eventTypes(job.id()))
                .containsExactly(JobEvent.SUBMITTED, JobEvent.DISPATCHED, JobEvent.CLAIMED, JobEvent.SUCCEEDED);
    }

    @Test
    void transientFailuresAreRetriedWithBackoff() {
        Job job = submit(FlakyHandler.TYPE, Map.of("succeedOnAttempt", 3));

        Job done = awaitStatus(job.id(), JobStatus.SUCCEEDED);

        assertThat(done.attempt()).isEqualTo(3);
        assertThat(eventTypes(job.id())).filteredOn(JobEvent.RETRY_SCHEDULED::equals).hasSize(2);
    }

    @Test
    void exhaustingAttemptsMovesJobToDead() {
        Job job = submit(FlakyHandler.TYPE, Map.of("succeedOnAttempt", 99), 2);

        Job dead = awaitStatus(job.id(), JobStatus.DEAD);

        assertThat(dead.attempt()).isEqualTo(2);
        assertThat(dead.lastError()).contains("Attempts exhausted");
    }

    @Test
    void nonRetryableFailureSkipsRetries() {
        Job job = submit(PoisonHandler.TYPE, Map.of("bad", true));

        Job dead = awaitStatus(job.id(), JobStatus.DEAD);

        assertThat(dead.attempt()).isEqualTo(1);
        assertThat(dead.lastError()).startsWith("Non-retryable");
    }

    @Test
    void deadJobCanBeRevivedByOperator() {
        Job job = submit(FlakyHandler.TYPE, Map.of("succeedOnAttempt", 99), 1);
        awaitStatus(job.id(), JobStatus.DEAD);

        Job revived = jobService.retry(job.id());

        assertThat(revived.status()).isEqualTo(JobStatus.SCHEDULED);
        assertThat(revived.attempt()).isZero();
        awaitStatus(job.id(), JobStatus.DEAD);
        assertThat(eventTypes(job.id())).contains(JobEvent.MANUAL_RETRY);
    }

    @Test
    void runningJobCanBeCancelled() {
        Job job = submit(SleepHandler.TYPE, Map.of("millis", 30_000));
        awaitStatus(job.id(), JobStatus.RUNNING);

        jobService.cancel(job.id());

        Job cancelled = awaitStatus(job.id(), JobStatus.CANCELLED, Duration.ofSeconds(5));
        assertThat(eventTypes(job.id())).contains(JobEvent.CANCEL_REQUESTED, JobEvent.CANCELLED);
        assertThat(cancelled.finishedAt()).isNotNull();
    }

    /**
     * Regression test for a commit-ordering race: when the dispatcher published to Kafka before committing,
     * fast workers saw the job as SCHEDULED, dropped the message, and jobs looped through redelivery.
     */
    @Test
    void manyConcurrentJobsAreEachExecutedExactlyOnce() {
        List<Job> jobs = IntStream.range(0, 150)
                .mapToObj(i -> submit(SleepHandler.TYPE, Map.of("millis", 5)))
                .toList();

        await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(20))
                .until(() -> jobs.stream().allMatch(j -> jobService.get(j.id()).status() == JobStatus.SUCCEEDED));

        for (Job j : jobs) {
            assertThat(eventTypes(j.id()))
                    .containsExactly(JobEvent.SUBMITTED, JobEvent.DISPATCHED, JobEvent.CLAIMED, JobEvent.SUCCEEDED);
        }
    }
}
