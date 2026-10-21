package com.taskflow.dispatch;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import com.taskflow.config.NodeIdentity;
import com.taskflow.config.TaskFlowProperties;
import com.taskflow.coordination.LeaderElector;
import com.taskflow.job.JobEvent;
import com.taskflow.job.JobRepository;
import com.taskflow.job.JobRepository.DispatchedJob;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Moves due jobs from SCHEDULED to QUEUED, commits, and only then publishes their ids to Kafka.
 *
 * <p>Publishing must happen after the commit: a fast worker can receive the message before the
 * transaction commits, still see the job as SCHEDULED, and drop the message. With commit-then-publish:
 * if publishing fails, the batch is put back to SCHEDULED immediately; if the process dies between
 * commit and publish, the sweeper re-dispatches jobs that stay QUEUED longer than the queued timeout.
 */
@Component
public class Dispatcher {

    private static final Logger log = LoggerFactory.getLogger(Dispatcher.class);

    private final JobRepository repository;
    private final KafkaTemplate<String, String> kafka;
    private final TransactionTemplate tx;
    private final LeaderElector elector;
    private final TaskFlowProperties props;
    private final NodeIdentity node;
    private final Counter dispatched;

    public Dispatcher(JobRepository repository, KafkaTemplate<String, String> kafka, TransactionTemplate tx,
                      LeaderElector elector, TaskFlowProperties props, NodeIdentity node, MeterRegistry meters) {
        this.repository = repository;
        this.kafka = kafka;
        this.tx = tx;
        this.elector = elector;
        this.props = props;
        this.node = node;
        this.dispatched = Counter.builder("taskflow.jobs.dispatched").register(meters);
    }

    @Scheduled(fixedDelayString = "${taskflow.scheduler.dispatch-interval-ms:500}")
    public void dispatchDueJobs() {
        if (!elector.isLeader()) {
            return;
        }
        int batchSize = props.scheduler().dispatchBatchSize();
        try {
            int count;
            do {
                count = dispatchBatch(batchSize);
            } while (count == batchSize);
        } catch (Exception e) {
            log.warn("Dispatch failed: {}", e.getMessage());
        }
    }

    private int dispatchBatch(int batchSize) {
        List<DispatchedJob> jobs = tx.execute(status -> {
            List<DispatchedJob> due = repository.markDueJobsQueued(batchSize);
            for (DispatchedJob j : due) {
                repository.addEvent(j.id(), JobEvent.DISPATCHED, j.attempt(), node.id(), null);
            }
            return due;
        });
        if (jobs == null || jobs.isEmpty()) {
            return 0;
        }
        try {
            CompletableFuture<?>[] sends = jobs.stream()
                    .map(j -> kafka.send(props.topic(), j.id().toString(), j.id().toString()))
                    .toArray(CompletableFuture[]::new);
            CompletableFuture.allOf(sends).orTimeout(10, TimeUnit.SECONDS).join();
        } catch (RuntimeException e) {
            List<UUID> ids = jobs.stream().map(DispatchedJob::id).toList();
            int reverted = repository.revertQueued(ids);
            log.warn("Publishing {} jobs failed ({}); reverted {} to SCHEDULED", ids.size(), e.getMessage(), reverted);
            return 0;
        }
        dispatched.increment(jobs.size());
        return jobs.size();
    }
}
