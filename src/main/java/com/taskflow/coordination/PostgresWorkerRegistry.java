package com.taskflow.coordination;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;

import org.springframework.jdbc.core.simple.JdbcClient;

public class PostgresWorkerRegistry implements WorkerRegistry {

    private final JdbcClient jdbc;

    public PostgresWorkerRegistry(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public void heartbeat(WorkerInfo w, Duration ttl) {
        jdbc.sql("""
                        INSERT INTO workers (id, host, capacity, active_jobs, started_at, last_heartbeat)
                        VALUES (:id, :host, :capacity, :active, :startedAt, now())
                        ON CONFLICT (id) DO UPDATE SET active_jobs = EXCLUDED.active_jobs, last_heartbeat = now()
                        """)
                .param("id", w.id())
                .param("host", w.host())
                .param("capacity", w.capacity())
                .param("active", w.activeJobs())
                .param("startedAt", OffsetDateTime.ofInstant(w.startedAt(), ZoneOffset.UTC))
                .update();
    }

    @Override
    public void remove(String workerId) {
        jdbc.sql("DELETE FROM workers WHERE id = :id").param("id", workerId).update();
    }

    @Override
    public List<WorkerInfo> alive(Duration ttl) {
        return jdbc.sql("""
                        SELECT * FROM workers WHERE last_heartbeat > now() - make_interval(secs => :secs) ORDER BY id
                        """)
                .param("secs", ttl.toMillis() / 1000.0)
                .query((rs, i) -> new WorkerInfo(rs.getString("id"), rs.getString("host"), rs.getInt("capacity"),
                        rs.getInt("active_jobs"), rs.getObject("started_at", OffsetDateTime.class).toInstant(),
                        rs.getObject("last_heartbeat", OffsetDateTime.class).toInstant()))
                .list();
    }
}
