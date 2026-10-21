package com.taskflow;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import com.taskflow.job.Job;
import com.taskflow.job.JobEvent;
import com.taskflow.job.JobRepository;
import com.taskflow.job.JobService;
import com.taskflow.job.JobStatus;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;

import static org.awaitility.Awaitility.await;

/**
 * Boots the whole app (API + scheduler + worker) against a real Postgres and a real Kafka broker,
 * both running in-process, so the suite needs no Docker. All subclasses share one cached context.
 */
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test")
@EmbeddedKafka(partitions = 6, topics = "taskflow.jobs", bootstrapServersProperty = "spring.kafka.bootstrap-servers")
public abstract class IntegrationTest {

    private static final EmbeddedPostgres POSTGRES = startPostgres();

    @Autowired
    protected JobService jobService;

    @Autowired
    protected JobRepository jobRepository;

    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", () -> POSTGRES.getJdbcUrl("postgres", "postgres"));
        registry.add("spring.datasource.username", () -> "postgres");
        registry.add("spring.datasource.password", () -> "postgres");
    }

    private static EmbeddedPostgres startPostgres() {
        try {
            return EmbeddedPostgres.start();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    protected Job submit(String type, Map<String, Object> payload) {
        return submit(type, payload, 5);
    }

    protected Job submit(String type, Map<String, Object> payload, int maxAttempts) {
        return jobService.submit(new JobService.SubmitCommand(type, payload, 0, maxAttempts, 0, null)).job();
    }

    protected Job awaitStatus(UUID id, JobStatus status) {
        return awaitStatus(id, status, Duration.ofSeconds(20));
    }

    protected Job awaitStatus(UUID id, JobStatus status, Duration timeout) {
        await().atMost(timeout).pollInterval(Duration.ofMillis(50)).untilAsserted(() -> {
            Job job = jobService.get(id);
            if (job.status() != status) {
                throw new AssertionError("Expected " + status + " but job is " + job + "\nEvents: " + eventTypes(id));
            }
        });
        return jobService.get(id);
    }

    protected List<String> eventTypes(UUID id) {
        return jobRepository.events(id).stream().map(JobEvent::type).toList();
    }
}
