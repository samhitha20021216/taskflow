package com.taskflow;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import com.jayway.jsonpath.JsonPath;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class JobApiTest extends IntegrationTest {

    private static final String BODY = """
            {"type": "demo.sleep", "payload": {"millis": 10}, "maxAttempts": 3}
            """;

    @Autowired
    MockMvc mvc;

    @Test
    void submittingTwiceWithSameIdempotencyKeyReturnsOriginalJob() throws Exception {
        MvcResult first = mvc.perform(post("/api/jobs").contentType(MediaType.APPLICATION_JSON)
                        .header("Idempotency-Key", "order-42-invoice").content(BODY))
                .andExpect(status().isCreated())
                .andReturn();
        String id = JsonPath.read(first.getResponse().getContentAsString(), "$.id");

        mvc.perform(post("/api/jobs").contentType(MediaType.APPLICATION_JSON)
                        .header("Idempotency-Key", "order-42-invoice").content(BODY))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(id));
    }

    @Test
    void unknownJobTypeIsRejected() throws Exception {
        mvc.perform(post("/api/jobs").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"type\": \"does.not.exist\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.detail").value(org.hamcrest.Matchers.containsString("Unknown job type")));
    }

    @Test
    void jobDetailsAndHistoryAreExposed() throws Exception {
        MvcResult created = mvc.perform(post("/api/jobs").contentType(MediaType.APPLICATION_JSON).content(BODY))
                .andExpect(status().isCreated())
                .andReturn();
        String id = JsonPath.read(created.getResponse().getContentAsString(), "$.id");
        awaitStatus(java.util.UUID.fromString(id), com.taskflow.job.JobStatus.SUCCEEDED);

        mvc.perform(get("/api/jobs/{id}", id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("SUCCEEDED"));
        String events = mvc.perform(get("/api/jobs/{id}/events", id))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();
        assertThat(events).contains("SUBMITTED", "CLAIMED", "SUCCEEDED");
    }

    @Test
    void cancellingFinishedJobIsAConflict() throws Exception {
        String id = JsonPath.read(mvc.perform(post("/api/jobs").contentType(MediaType.APPLICATION_JSON).content(BODY))
                .andReturn().getResponse().getContentAsString(), "$.id");
        awaitStatus(java.util.UUID.fromString(id), com.taskflow.job.JobStatus.SUCCEEDED);

        mvc.perform(post("/api/jobs/{id}/cancel", id)).andExpect(status().isConflict());
    }

    @Test
    void missingJobIs404() throws Exception {
        mvc.perform(get("/api/jobs/{id}", java.util.UUID.randomUUID())).andExpect(status().isNotFound());
    }

    @Test
    void statsReportLeaderAndWorkers() throws Exception {
        org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(10)).untilAsserted(() ->
                mvc.perform(get("/api/stats"))
                        .andExpect(status().isOk())
                        .andExpect(jsonPath("$.nodeIsLeader").value(true))
                        .andExpect(jsonPath("$.aliveWorkers").value(1)));
    }
}
