package com.taskflow.coordination;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.data.redis.core.StringRedisTemplate;

/** One hash per worker with a TTL, plus a set index of worker ids. Expired entries are pruned on read. */
public class RedisWorkerRegistry implements WorkerRegistry {

    private static final String INDEX = "taskflow:workers";

    private final StringRedisTemplate redis;

    public RedisWorkerRegistry(StringRedisTemplate redis) {
        this.redis = redis;
    }

    @Override
    public void heartbeat(WorkerInfo w, Duration ttl) {
        String key = key(w.id());
        redis.opsForHash().putAll(key, Map.of(
                "host", w.host(),
                "capacity", String.valueOf(w.capacity()),
                "activeJobs", String.valueOf(w.activeJobs()),
                "startedAt", w.startedAt().toString(),
                "lastHeartbeat", Instant.now().toString()));
        redis.expire(key, ttl);
        redis.opsForSet().add(INDEX, w.id());
    }

    @Override
    public void remove(String workerId) {
        redis.delete(key(workerId));
        redis.opsForSet().remove(INDEX, workerId);
    }

    @Override
    public List<WorkerInfo> alive(Duration ttl) {
        Set<String> ids = redis.opsForSet().members(INDEX);
        List<WorkerInfo> result = new ArrayList<>();
        if (ids == null) {
            return result;
        }
        for (String id : ids) {
            Map<Object, Object> h = redis.opsForHash().entries(key(id));
            if (h.isEmpty()) {
                redis.opsForSet().remove(INDEX, id);
                continue;
            }
            result.add(new WorkerInfo(id, (String) h.get("host"),
                    Integer.parseInt((String) h.get("capacity")),
                    Integer.parseInt((String) h.get("activeJobs")),
                    Instant.parse((String) h.get("startedAt")),
                    Instant.parse((String) h.get("lastHeartbeat"))));
        }
        result.sort(Comparator.comparing(WorkerInfo::id));
        return result;
    }

    private static String key(String id) {
        return "taskflow:worker:" + id;
    }
}
