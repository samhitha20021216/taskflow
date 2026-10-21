package com.taskflow.dispatch;

import java.util.List;
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
 * Moves due jobs from SCHEDULED to QUEUED and publishes their ids to Kafka, inside one DB transaction.
 *
 * <p>If publishing fails the transaction rolls back and the jobs stay SCHEDULED. If a message is lost after
 * commit, the sweeper re-dispatches the stuck job.
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
            log.warn("Dispatch batch rolled back: {}", e.getMessage());
        }
    }

    private int dispatchBatch(int batchSize) {
        Integer count = tx.execute(status -> {
            List<DispatchedJob> jobs = repository.markDueJobsQueued(batchSize);
            if (jobs.isEmpty()) {
                return 0;
            }
            CompletableFuture<?>[] sends = jobs.stream()
                    .map(j -> kafka.send(props.topic(), j.id().toString(), j.id().toString()))
                    .toArray(CompletableFuture[]::new);
            CompletableFuture.allOf(sends).orTimeout(10, TimeUnit.SECONDS).join();
            for (DispatchedJob j : jobs) {
                repository.addEvent(j.id(), JobEvent.DISPATCHED, j.attempt(), node.id(), null);
            }
            return jobs.size();
        });
        int n = count == null ? 0 : count;
        dispatched.increment(n);
        return n;
    }
}
