package com.taskflow.worker;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.springframework.stereotype.Component;

@Component
public class HandlerRegistry {

    private final Map<String, JobHandler> handlers;

    public HandlerRegistry(List<JobHandler> handlers) {
        this.handlers = handlers.stream().collect(Collectors.toMap(JobHandler::type, Function.identity()));
    }

    public Optional<JobHandler> find(String type) {
        return Optional.ofNullable(handlers.get(type));
    }

    public Set<String> types() {
        return new TreeSet<>(handlers.keySet());
    }
}
