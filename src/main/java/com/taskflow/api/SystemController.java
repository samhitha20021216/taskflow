package com.taskflow.api;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.taskflow.config.NodeIdentity;
import com.taskflow.config.TaskFlowProperties;
import com.taskflow.coordination.LeaderElector;
import com.taskflow.coordination.WorkerInfo;
import com.taskflow.coordination.WorkerRegistry;
import com.taskflow.job.JobService;
import com.taskflow.job.JobStatus;
import com.taskflow.worker.HandlerRegistry;

@RestController
@RequestMapping("/api")
public class SystemController {

    public record Stats(Map<JobStatus, Long> jobs, int aliveWorkers, String node, boolean nodeIsLeader) {
    }

    private final JobService jobs;
    private final WorkerRegistry workers;
    private final HandlerRegistry handlers;
    private final LeaderElector elector;
    private final NodeIdentity node;
    private final TaskFlowProperties props;

    public SystemController(JobService jobs, WorkerRegistry workers, HandlerRegistry handlers,
                            LeaderElector elector, NodeIdentity node, TaskFlowProperties props) {
        this.jobs = jobs;
        this.workers = workers;
        this.handlers = handlers;
        this.elector = elector;
        this.node = node;
        this.props = props;
    }

    @GetMapping("/stats")
    public Stats stats() {
        return new Stats(jobs.counts(), aliveWorkers().size(), node.id(), elector.isLeader());
    }

    @GetMapping("/workers")
    public List<WorkerInfo> aliveWorkers() {
        return workers.alive(props.worker().leaseDuration());
    }

    @GetMapping("/job-types")
    public Set<String> jobTypes() {
        return handlers.types();
    }
}
