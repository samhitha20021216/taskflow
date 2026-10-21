package com.taskflow.worker.handlers;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.taskflow.worker.JobContext;
import com.taskflow.worker.JobHandler;

/**
 * Long-running, resumable job: "processes" {@code rows} rows in chunks and checkpoints after each chunk.
 * The checksum is the sum of all row numbers, so if a resumed run double-counted or skipped a chunk the
 * result would not equal rows*(rows+1)/2 — that is what the recovery tests assert.
 */
@Component
public class ReportGenerationHandler implements JobHandler {

    public static final String TYPE = "report.generate";

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public Map<String, Object> execute(JobContext ctx) {
        int rows = ctx.intParam("rows", 1_000);
        int chunkSize = Math.max(1, ctx.intParam("chunkSize", 100));
        int msPerChunk = ctx.intParam("msPerChunk", 200);

        long offset = ((Number) ctx.checkpoint().getOrDefault("offset", 0)).longValue();
        long checksum = ((Number) ctx.checkpoint().getOrDefault("checksum", 0)).longValue();
        long resumedFrom = offset;

        while (offset < rows) {
            long end = Math.min(rows, offset + chunkSize);
            ctx.sleep(Duration.ofMillis(msPerChunk));
            for (long row = offset + 1; row <= end; row++) {
                checksum += row;
            }
            offset = end;
            ctx.checkpoint(Map.of("offset", offset, "checksum", checksum), (int) (offset * 100 / rows));
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("rows", rows);
        result.put("checksum", checksum);
        result.put("resumedFromOffset", resumedFrom);
        result.put("attempt", ctx.attempt());
        return result;
    }
}
