package com.taskflow.config;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties(prefix = "taskflow")
public record TaskFlowProperties(
        String nodeId,
        @DefaultValue("taskflow.jobs") String topic,
        @DefaultValue("6") int topicPartitions,
        @DefaultValue("postgres") Coordination coordination,
        @DefaultValue Scheduler scheduler,
        @DefaultValue Worker worker,
        @DefaultValue Retry retry) {

    public enum Coordination { POSTGRES, REDIS }

    /** Dispatcher + sweeper. Any number of nodes may enable it; only the elected leader acts. */
    public record Scheduler(
            @DefaultValue("true") boolean enabled,
            @DefaultValue("200") int dispatchBatchSize,
            @DefaultValue("30s") Duration queuedTimeout,
            @DefaultValue("10s") Duration leaderTtl) {
    }

    public record Worker(
            @DefaultValue("true") boolean enabled,
            @DefaultValue("4") int concurrency,
            @DefaultValue("15s") Duration leaseDuration,
            @DefaultValue("30s") Duration shutdownGracePeriod) {
    }

    public record Retry(
            @DefaultValue("1s") Duration baseDelay,
            @DefaultValue("5m") Duration maxDelay,
            @DefaultValue("2.0") double multiplier,
            @DefaultValue("0.2") double jitter) {
    }
}
