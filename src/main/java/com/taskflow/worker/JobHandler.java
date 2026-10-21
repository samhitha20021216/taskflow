package com.taskflow.worker;

import java.util.Map;

/**
 * Business logic for one job type. Implementations must tolerate being executed more than once
 * (delivery is at-least-once); use {@link JobContext#checkpoint} to make re-execution cheap.
 */
public interface JobHandler {

    String type();

    Map<String, Object> execute(JobContext context) throws Exception;
}
