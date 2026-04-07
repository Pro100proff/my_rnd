package com.platform.archive.maintenance.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.List;

public record CheckpointEvent(
        @JsonProperty("index_id") String indexId,
        @JsonProperty("checkpoint_id") long checkpointId,
        List<String> files,
        @JsonProperty("total_bytes") long totalBytes,
        Instant timestamp) {
}
