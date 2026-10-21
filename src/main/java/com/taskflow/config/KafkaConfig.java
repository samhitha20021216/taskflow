package com.taskflow.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfig {

    @Bean
    NewTopic jobsTopic(TaskFlowProperties props) {
        return TopicBuilder.name(props.topic())
                .partitions(props.topicPartitions())
                .replicas(1)
                .build();
    }
}
