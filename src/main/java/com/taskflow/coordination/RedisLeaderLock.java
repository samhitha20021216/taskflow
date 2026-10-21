package com.taskflow.coordination;

import java.time.Duration;
import java.util.List;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;

/** SET-NX-PX lock with owner check on renew/release, done atomically in Lua. */
public class RedisLeaderLock implements LeaderLock {

    private static final RedisScript<Long> ACQUIRE = RedisScript.of("""
            local current = redis.call('GET', KEYS[1])
            if current == false then
              redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
              return 1
            elseif current == ARGV[1] then
              redis.call('PEXPIRE', KEYS[1], ARGV[2])
              return 1
            end
            return 0
            """, Long.class);

    private static final RedisScript<Long> RELEASE = RedisScript.of("""
            if redis.call('GET', KEYS[1]) == ARGV[1] then
              return redis.call('DEL', KEYS[1])
            end
            return 0
            """, Long.class);

    private final StringRedisTemplate redis;

    public RedisLeaderLock(StringRedisTemplate redis) {
        this.redis = redis;
    }

    @Override
    public boolean tryAcquire(String name, String owner, Duration ttl) {
        Long result = redis.execute(ACQUIRE, List.of(key(name)), owner, String.valueOf(ttl.toMillis()));
        return result != null && result == 1L;
    }

    @Override
    public void release(String name, String owner) {
        redis.execute(RELEASE, List.of(key(name)), owner);
    }

    private static String key(String name) {
        return "taskflow:leader:" + name;
    }
}
