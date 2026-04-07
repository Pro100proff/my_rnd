package com.platform.archive.maintenance.model;

import com.fasterxml.jackson.databind.JsonNode;

public record RotationRuleConfig(
        String indexId,
        String name,
        RotationRuleType type,
        JsonNode params,
        int executionOrder,
        boolean enabled) {
}
