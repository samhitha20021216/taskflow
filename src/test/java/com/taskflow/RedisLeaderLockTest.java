package com.taskflow;

import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.taskflow.coordination.RedisLeaderLock;

/** Runs only where Docker is available (e.g. CI). */
@Testcontainers(disabledWithoutDocker = true)
class RedisLeaderLockTest {

    @Container
    static final GenericContainer<?> REDIS = new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);

    @Test
    void redisLockHasLeaderSemantics() {
        LettuceConnectionFactory factory = new LettuceConnectionFactory(REDIS.getHost(), REDIS.getMappedPort(6379));
        factory.afterPropertiesSet();
        factory.start();
        try {
            StringRedisTemplate template = new StringRedisTemplate(factory);
            LeaderLockTest.LeaderLockContract.verify(new RedisLeaderLock(template));
        } finally {
            factory.destroy();
        }
    }
}
