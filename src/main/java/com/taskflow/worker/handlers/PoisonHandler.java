package com.taskflow.worker.handlers;

import java.util.Map;

import org.springframework.stereotype.Component;

import com.taskflow.worker.JobContext;
import com.taskflow.worker.JobHandler;
import com.taskflow.worker.NonRetryableJobException;

/** Always fails permanently; demonstrates that non-retryable errors skip the retry budget. */
@Component
public class PoisonHandler implements JobHandler {

    public static final String TYPE = "demo.poison";

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public Map<String, Object> execute(JobContext ctx) {
        throw new NonRetryableJobException("Payload rejected: " + ctx.payload());
    }
}
