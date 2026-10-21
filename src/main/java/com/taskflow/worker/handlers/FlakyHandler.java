package com.taskflow.worker.handlers;

import java.util.Map;

import org.springframework.stereotype.Component;

import com.taskflow.worker.JobContext;
import com.taskflow.worker.JobHandler;

/** Fails with a transient error until attempt {@code succeedOnAttempt}; demonstrates retry with backoff. */
@Component
public class FlakyHandler implements JobHandler {

    public static final String TYPE = "demo.flaky";

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public Map<String, Object> execute(JobContext ctx) {
        int succeedOn = ctx.intParam("succeedOnAttempt", 3);
        if (ctx.attempt() < succeedOn) {
            throw new IllegalStateException("Simulated transient failure on attempt " + ctx.attempt());
        }
        return Map.of("succeededOnAttempt", ctx.attempt());
    }
}
