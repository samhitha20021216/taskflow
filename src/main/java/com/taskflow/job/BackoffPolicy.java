package com.taskflow.job;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.taskflow.config.TaskFlowProperties;

/** Exponential backoff with symmetric jitter, capped at maxDelay. */
@Component
public class BackoffPolicy {

    private final TaskFlowProperties.Retry config;
    private final DoubleSupplier random;

    @Autowired
    public BackoffPolicy(TaskFlowProperties props) {
        this(props.retry(), () -> ThreadLocalRandom.current().nextDouble());
    }

    BackoffPolicy(TaskFlowProperties.Retry config, DoubleSupplier random) {
        this.config = config;
        this.random = random;
    }

    /** Delay before the next attempt, given the attempt that just failed (1-based). */
    public Duration delayAfter(int failedAttempt) {
        double base = config.baseDelay().toMillis() * Math.pow(config.multiplier(), Math.max(0, failedAttempt - 1));
        double capped = Math.min(base, config.maxDelay().toMillis());
        double jitterFactor = 1 + config.jitter() * (2 * random.getAsDouble() - 1);
        return Duration.ofMillis(Math.max(0, Math.round(capped * jitterFactor)));
    }
}
