package com.platform.archive.maintenance.config;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public record JobConfig(
        String kafkaBrokers,
        String mergeEventsTopic,
        String consumerGroupId,
        String pgJdbc,
        String pgUser,
        String pgPassword,
        Duration kafkaPollTimeout,
        int kafkaMaxPollRecords,
        int deferredCleanupEveryCycles,
        int inProgressMinAgeSeconds) {

    public static JobConfig fromArgs(String[] args) {
        Map<String, String> m = parse(args);
        return new JobConfig(
                required(m, "kafka-brokers"),
                m.getOrDefault("merge-events-topic", "merge-events"),
                m.getOrDefault("kafka-group-id", "archive-maintenance-v1"),
                required(m, "pg-jdbc"),
                required(m, "pg-user"),
                required(m, "pg-password"),
                Duration.ofSeconds(Long.parseLong(m.getOrDefault("kafka-poll-seconds", "30"))),
                Integer.parseInt(m.getOrDefault("kafka-max-poll-records", "100")),
                Integer.parseInt(m.getOrDefault("deferred-cleanup-every-cycles", "10")),
                Integer.parseInt(m.getOrDefault("in-progress-min-age-seconds", "120"))
        );
    }

    private static Map<String, String> parse(String[] args) {
        Map<String, String> result = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (!arg.startsWith("--")) continue;
            String key = arg.substring(2);
            String value = i + 1 < args.length ? args[++i] : "";
            result.put(key, value);
        }
        return result;
    }

    private static String required(Map<String, String> m, String key) {
        String v = m.get(key);
        if (v == null || v.isBlank()) {
            throw new IllegalArgumentException("Missing required argument --" + key);
        }
        return v;
    }
}
