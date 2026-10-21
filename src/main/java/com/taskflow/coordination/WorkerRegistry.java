package com.taskflow.coordination;

import java.time.Duration;
import java.util.List;

/** Liveness view of the worker fleet, for operators. Job recovery does not depend on it (leases do). */
public interface WorkerRegistry {

    void heartbeat(WorkerInfo worker, Duration ttl);

    void remove(String workerId);

    List<WorkerInfo> alive(Duration ttl);
}
