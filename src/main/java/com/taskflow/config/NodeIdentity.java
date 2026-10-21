package com.taskflow.config;

import java.net.InetAddress;
import java.util.UUID;

import org.springframework.stereotype.Component;

/** Stable identity of this process; used as lease owner, worker id and leader id. */
@Component
public class NodeIdentity {

    private final String id;
    private final String host;

    public NodeIdentity(TaskFlowProperties props) {
        this.host = resolveHost();
        this.id = props.nodeId() != null && !props.nodeId().isBlank()
                ? props.nodeId()
                : host + "-" + UUID.randomUUID().toString().substring(0, 8);
    }

    public String id() {
        return id;
    }

    public String host() {
        return host;
    }

    private static String resolveHost() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "unknown-host";
        }
    }
}
