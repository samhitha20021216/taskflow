package com.taskflow.coordination;

import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.taskflow.config.NodeIdentity;
import com.taskflow.config.TaskFlowProperties;

import jakarta.annotation.PreDestroy;

/**
 * Keeps trying to hold the "scheduler" lock. Leadership only avoids wasted work: the dispatcher and
 * sweeper use row locks and conditional updates, so two overlapping leaders cannot corrupt state.
 */
@Component
public class LeaderElector {

    static final String LOCK_NAME = "scheduler";
    private static final Logger log = LoggerFactory.getLogger(LeaderElector.class);

    private final LeaderLock lock;
    private final NodeIdentity node;
    private final TaskFlowProperties props;
    private volatile long leaderUntilNanos;
    private volatile boolean leading;
    private volatile boolean wasLeader;

    public LeaderElector(LeaderLock lock, NodeIdentity node, TaskFlowProperties props) {
        this.lock = lock;
        this.node = node;
        this.props = props;
    }

    @Scheduled(fixedDelayString = "${taskflow.scheduler.election-interval-ms:3000}", initialDelay = 0)
    public void campaign() {
        if (!props.scheduler().enabled()) {
            return;
        }
        Duration ttl = props.scheduler().leaderTtl();
        long started = System.nanoTime();
        boolean acquired;
        try {
            acquired = lock.tryAcquire(LOCK_NAME, node.id(), ttl);
        } catch (Exception e) {
            log.warn("Leader election failed: {}", e.getMessage());
            acquired = false;
        }
        // Trust our leadership for slightly less than the TTL, measured from before the request.
        leaderUntilNanos = started + ttl.toNanos() * 8 / 10;
        leading = acquired;
        if (acquired != wasLeader) {
            log.info(acquired ? "{} became scheduler leader" : "{} lost scheduler leadership", node.id());
            wasLeader = acquired;
        }
    }

    public boolean isLeader() {
        return props.scheduler().enabled() && leading && System.nanoTime() - leaderUntilNanos < 0;
    }

    @PreDestroy
    void resign() {
        if (wasLeader) {
            try {
                lock.release(LOCK_NAME, node.id());
            } catch (Exception ignored) {
                // lock will expire on its own
            }
        }
    }
}
