package com.taskflow.job;

import java.time.Duration;

import org.junit.jupiter.api.Test;

import com.taskflow.config.TaskFlowProperties;

import static org.assertj.core.api.Assertions.assertThat;

class BackoffPolicyTest {

    private final TaskFlowProperties.Retry config =
            new TaskFlowProperties.Retry(Duration.ofSeconds(1), Duration.ofSeconds(30), 2.0, 0.2);

    @Test
    void growsExponentiallyWithoutJitter() {
        BackoffPolicy policy = new BackoffPolicy(config, () -> 0.5); // 0.5 => jitter factor 1.0
        assertThat(policy.delayAfter(1)).isEqualTo(Duration.ofSeconds(1));
        assertThat(policy.delayAfter(2)).isEqualTo(Duration.ofSeconds(2));
        assertThat(policy.delayAfter(3)).isEqualTo(Duration.ofSeconds(4));
        assertThat(policy.delayAfter(5)).isEqualTo(Duration.ofSeconds(16));
    }

    @Test
    void isCappedAtMaxDelay() {
        BackoffPolicy policy = new BackoffPolicy(config, () -> 0.5);
        assertThat(policy.delayAfter(20)).isEqualTo(Duration.ofSeconds(30));
    }

    @Test
    void jitterStaysWithinBounds() {
        assertThat(new BackoffPolicy(config, () -> 0.0).delayAfter(3)).isEqualTo(Duration.ofMillis(3200));
        assertThat(new BackoffPolicy(config, () -> 1.0).delayAfter(3)).isEqualTo(Duration.ofMillis(4800));
    }
}
