package com.taskflow.coordination;

import java.time.Instant;

public record WorkerInfo(String id, String host, int capacity, int activeJobs, Instant startedAt, Instant lastHeartbeat) {
}
