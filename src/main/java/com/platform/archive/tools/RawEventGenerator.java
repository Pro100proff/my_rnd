package com.platform.archive.tools;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class RawEventGenerator {
    private static final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
    private static final String[] LEVELS = {"INFO", "WARN", "DEBUG", "ERROR"};
    private static final String[] INDEXES = {"archive-logs-tenant-42", "archive-logs-tenant-99"};

    public static void main(String[] args) throws Exception {
        Map<String, String> cfg = parseArgs(args);
        String brokers = required(cfg, "kafka-brokers");
        String topic = cfg.getOrDefault("topic", "raw-archive-events");
        long intervalMs = Long.parseLong(cfg.getOrDefault("interval-ms", "200"));

        Properties p = new Properties();
        p.setProperty("bootstrap.servers", brokers);
        p.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        p.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        p.setProperty("acks", "1");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(p)) {
            long i = 0;
            while (true) {
                RawEvent event = randomEvent(i++);
                String payload = mapper.writeValueAsString(event);
                producer.send(new ProducerRecord<>(topic, event.indexId, payload));
                if (i % 100 == 0) {
                    producer.flush();
                    System.out.println("generated=" + i);
                }
                Thread.sleep(intervalMs);
            }
        }
    }

    private static RawEvent randomEvent(long seq) {
        ThreadLocalRandom r = ThreadLocalRandom.current();
        RawEvent e = new RawEvent();
        e.indexId = INDEXES[r.nextInt(INDEXES.length)];
        e.level = LEVELS[r.nextInt(LEVELS.length)];
        e.message = "generated-log-" + seq;
        e.eventTs = Instant.now();
        return e;
    }

    public static class RawEvent {
        public String indexId;
        public String level;
        public String message;
        public Instant eventTs;
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> out = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            if (!args[i].startsWith("--")) continue;
            String key = args[i].substring(2);
            String val = i + 1 < args.length ? args[++i] : "";
            out.put(key, val);
        }
        return out;
    }

    private static String required(Map<String, String> m, String key) {
        String v = m.get(key);
        if (v == null || v.isBlank()) throw new IllegalArgumentException("Missing --" + key);
        return v;
    }
}
