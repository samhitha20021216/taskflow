package com.taskflow.coordination;

import java.time.Duration;

/** A named, expiring lock used to elect a single active scheduler. */
public interface LeaderLock {

    /** Acquires the lock, or renews it if {@code owner} already holds it. */
    boolean tryAcquire(String name, String owner, Duration ttl);

    void release(String name, String owner);
}
