package com.taskflow;

import java.time.Duration;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.taskflow.coordination.LeaderLock;
import com.taskflow.coordination.PostgresLeaderLock;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class LeaderLockTest extends IntegrationTest {

    @Autowired
    JdbcClient jdbc;

    @Test
    void postgresLockHasLeaderSemantics() {
        LeaderLockContract.verify(new PostgresLeaderLock(jdbc));
    }

    /** Shared contract, also run against Redis in {@link RedisLeaderLockTest}. */
    static final class LeaderLockContract {

        static void verify(LeaderLock lock) {
            String name = "contract-" + System.nanoTime();
            assertThat(lock.tryAcquire(name, "node-a", Duration.ofSeconds(1))).isTrue();
            assertThat(lock.tryAcquire(name, "node-b", Duration.ofSeconds(1))).as("held by a").isFalse();
            assertThat(lock.tryAcquire(name, "node-a", Duration.ofSeconds(1))).as("renewal").isTrue();

            await().atMost(Duration.ofSeconds(5)).until(() -> lock.tryAcquire(name, "node-b", Duration.ofSeconds(30)));
            assertThat(lock.tryAcquire(name, "node-a", Duration.ofSeconds(1))).as("expired, b took over").isFalse();

            lock.release(name, "node-a");
            assertThat(lock.tryAcquire(name, "node-a", Duration.ofSeconds(1))).as("a can't release b's lock").isFalse();
            lock.release(name, "node-b");
            assertThat(lock.tryAcquire(name, "node-a", Duration.ofSeconds(1))).as("free after release").isTrue();
        }
    }
}
