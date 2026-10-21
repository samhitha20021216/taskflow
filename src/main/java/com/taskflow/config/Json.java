package com.taskflow.config;

import java.util.Map;

import org.springframework.stereotype.Component;

import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

/** Small wrapper so repositories don't need to know about Jackson. */
@Component
public class Json {

    private static final TypeReference<Map<String, Object>> MAP = new TypeReference<>() {
    };

    private final JsonMapper mapper;

    public Json(JsonMapper mapper) {
        this.mapper = mapper;
    }

    public String write(Object value) {
        return value == null ? null : mapper.writeValueAsString(value);
    }

    public Map<String, Object> readMap(String json) {
        return json == null ? null : mapper.readValue(json, MAP);
    }
}
