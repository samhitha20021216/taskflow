package com.taskflow.dispatch;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.taskflow.config.NodeIdentity;
import com.taskflow.config.TaskFlowProperties;
import com.taskflow.coordination.LeaderElector;
import com.taskflow.job.JobEvent;
import com.taskflow.job.JobRepository;
import com.taskflow.job.JobRepository.RecoveredJob;
import com.taskflow.job.JobStatus;

import io.micrometer.core.instrument.MeterRegistry;

/** Recovers work from crashed workers (expired leases) and lost dispatch messages (stuck QUEUED jobs). */
@Component
public class Sweeper {

    private static final Logger log = LoggerFactory.getLogger(Sweeper.class);
    private static final int BATCH = 500;

    private final JobRepository repository;
    private final LeaderElector elector;
    private final TaskFlowProperties props;
    private final NodeIdentity node;
    private final MeterRegistry meters;

    public Sweeper(JobRepository repository, LeaderElector elector, TaskFlowProperties props,
                   NodeIdentity node, MeterRegistry meters) {
        this.repository = repository;
        this.elector = elector;
        this.props = props;
        this.node = node;
        this.meters = meters;
    }

    @Scheduled(fixedDelayString = "${taskflow.scheduler.sweep-interval-ms:2000}")
    public void sweep() {
        if (!elector.isLeader()) {
            return;
        }
        try {
            for (RecoveredJob r : repository.recoverExpiredLeases(BATCH)) {
                String message = "Worker " + r.previousOwner() + " stopped heartbeating; job -> " + r.newStatus();
                repository.addEvent(r.id(), JobEvent.LEASE_EXPIRED, r.attempt(), node.id(), message);
                if (r.newStatus() == JobStatus.DEAD) {
                    repository.addEvent(r.id(), JobEvent.DEAD, r.attempt(), node.id(), "Attempts exhausted");
                }
                meters.counter("taskflow.jobs.recovered", "reason", "lease_expired").increment();
                log.info("Recovered job {} from {} -> {}", r.id(), r.previousOwner(), r.newStatus());
            }
            for (UUID id : repository.requeueStuckQueued(props.scheduler().queuedTimeout(), BATCH)) {
                repository.addEvent(id, JobEvent.REDELIVERED, 0, node.id(), "Queued but never claimed; re-dispatching");
                meters.counter("taskflow.jobs.recovered", "reason", "stuck_queued").increment();
            }
        } catch (Exception e) {
            log.warn("Sweep failed: {}", e.getMessage());
        }
    }
}
