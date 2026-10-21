package com.taskflow.coordination;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.simple.JdbcClient;

import com.taskflow.config.TaskFlowProperties;

/** Picks Redis or Postgres for leader election and the worker registry ({@code taskflow.coordination}). */
@Configuration
public class CoordinationConfig {

    @Bean
    LeaderLock leaderLock(TaskFlowProperties props, JdbcClient jdbc, ObjectProvider<StringRedisTemplate> redis) {
        return props.coordination() == TaskFlowProperties.Coordination.REDIS
                ? new RedisLeaderLock(redis.getObject())
                : new PostgresLeaderLock(jdbc);
    }

    @Bean
    WorkerRegistry workerRegistry(TaskFlowProperties props, JdbcClient jdbc, ObjectProvider<StringRedisTemplate> redis) {
        return props.coordination() == TaskFlowProperties.Coordination.REDIS
                ? new RedisWorkerRegistry(redis.getObject())
                : new PostgresWorkerRegistry(jdbc);
    }
}
