package com.taskflow.worker.handlers;

import java.time.Duration;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.taskflow.worker.JobContext;
import com.taskflow.worker.JobHandler;

/** Sleeps for {@code millis}; handy for load tests and for demonstrating cancellation. */
@Component
public class SleepHandler implements JobHandler {

    public static final String TYPE = "demo.sleep";

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public Map<String, Object> execute(JobContext ctx) {
        int millis = ctx.intParam("millis", 1_000);
        ctx.sleep(Duration.ofMillis(millis));
        return Map.of("sleptMillis", millis);
    }
}
