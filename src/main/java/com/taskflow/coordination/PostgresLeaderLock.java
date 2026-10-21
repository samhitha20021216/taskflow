package com.taskflow.coordination;

import java.time.Duration;

import org.springframework.jdbc.core.simple.JdbcClient;

public class PostgresLeaderLock implements LeaderLock {

    private final JdbcClient jdbc;

    public PostgresLeaderLock(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public boolean tryAcquire(String name, String owner, Duration ttl) {
        return jdbc.sql("""
                        INSERT INTO leader_leases (name, owner, expires_at)
                        VALUES (:name, :owner, now() + make_interval(secs => :secs))
                        ON CONFLICT (name) DO UPDATE SET owner = EXCLUDED.owner, expires_at = EXCLUDED.expires_at
                        WHERE leader_leases.owner = EXCLUDED.owner OR leader_leases.expires_at < now()
                        """)
                .param("name", name)
                .param("owner", owner)
                .param("secs", ttl.toMillis() / 1000.0)
                .update() == 1;
    }

    @Override
    public void release(String name, String owner) {
        jdbc.sql("DELETE FROM leader_leases WHERE name = :name AND owner = :owner")
                .param("name", name)
                .param("owner", owner)
                .update();
    }
}
