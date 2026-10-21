package com.taskflow.api;

import java.util.List;
import java.util.UUID;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.taskflow.job.Job;
import com.taskflow.job.JobEvent;
import com.taskflow.job.JobService;
import com.taskflow.job.JobService.SubmitCommand;
import com.taskflow.job.JobService.SubmitResult;
import com.taskflow.job.JobStatus;

import jakarta.validation.Valid;

@RestController
@RequestMapping("/api/jobs")
public class JobController {

    private final JobService jobs;

    public JobController(JobService jobs) {
        this.jobs = jobs;
    }

    /** Submit a job. Re-sending with the same Idempotency-Key returns the original job (200, not 201). */
    @PostMapping
    public ResponseEntity<Job> submit(@Valid @RequestBody SubmitJobRequest request,
                                      @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey) {
        SubmitResult result = jobs.submit(new SubmitCommand(
                request.type(),
                request.payload(),
                request.priority() == null ? 0 : request.priority(),
                request.maxAttempts() == null ? 5 : request.maxAttempts(),
                request.delaySeconds() == null ? 0 : request.delaySeconds(),
                idempotencyKey));
        return ResponseEntity.status(result.created() ? HttpStatus.CREATED : HttpStatus.OK).body(result.job());
    }

    @GetMapping("/{id}")
    public Job get(@PathVariable UUID id) {
        return jobs.get(id);
    }

    @GetMapping
    public List<Job> list(@RequestParam(required = false) JobStatus status,
                          @RequestParam(required = false) String type,
                          @RequestParam(defaultValue = "50") int limit,
                          @RequestParam(defaultValue = "0") int offset) {
        return jobs.list(status, type, limit, offset);
    }

    /** Full execution history: every claim, retry, lease expiry and recovery. */
    @GetMapping("/{id}/events")
    public List<JobEvent> events(@PathVariable UUID id) {
        return jobs.events(id);
    }

    @PostMapping("/{id}/cancel")
    public ResponseEntity<Job> cancel(@PathVariable UUID id) {
        return ResponseEntity.accepted().body(jobs.cancel(id));
    }

    @PostMapping("/{id}/retry")
    public Job retry(@PathVariable UUID id) {
        return jobs.retry(id);
    }
}
