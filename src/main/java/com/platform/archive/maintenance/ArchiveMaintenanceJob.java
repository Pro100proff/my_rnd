package com.platform.archive.maintenance;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.platform.archive.maintenance.cleanup.DeferredCleanupService;
import com.platform.archive.maintenance.config.JobConfig;
import com.platform.archive.maintenance.db.PostgresClient;
import com.platform.archive.maintenance.merge.MergeExecutor;
import com.platform.archive.maintenance.merge.MergeResult;
import com.platform.archive.maintenance.model.ArchiveIndexMeta;
import com.platform.archive.maintenance.model.CheckpointEvent;
import com.platform.archive.maintenance.model.RotationRuleConfig;
import com.platform.archive.maintenance.rotation.RotationAction;
import com.platform.archive.maintenance.rotation.RotationEvaluator;
import com.platform.archive.maintenance.rotation.RotationExecutor;
import com.platform.archive.maintenance.util.HdfsFacade;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

public class ArchiveMaintenanceJob {
    private static final Logger log = LoggerFactory.getLogger(ArchiveMaintenanceJob.class);
    private static final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();

    public static void main(String[] args) throws Exception {
        JobConfig cfg = JobConfig.fromArgs(args);
        SparkSession spark = SparkSession.builder()
                .appName("archive-maintenance")
                .config("spark.sql.orc.compression.codec", "zstd")
                .getOrCreate();

        try (PostgresClient pg = new PostgresClient(cfg.pgJdbc(), cfg.pgUser(), cfg.pgPassword());
             KafkaConsumer<String, String> consumer = buildConsumer(cfg)) {
            HdfsFacade hdfs = new HdfsFacade();
            MergeExecutor mergeExecutor = new MergeExecutor(spark, pg, hdfs, Duration.ofSeconds(cfg.inProgressMinAgeSeconds()));
            RotationEvaluator rotationEvaluator = new RotationEvaluator();
            RotationExecutor rotationExecutor = new RotationExecutor(spark, hdfs, pg);
            DeferredCleanupService cleanupService = new DeferredCleanupService(pg, hdfs);

            consumer.subscribe(List.of(cfg.mergeEventsTopic()));
            int cycle = 0;
            while (true) {
                cycle++;
                ConsumerRecords<String, String> records = consumer.poll(cfg.kafkaPollTimeout());
                if (records.isEmpty()) {
                    maybeRunDeferredCleanup(cycle, cfg.deferredCleanupEveryCycles(), cleanupService);
                    continue;
                }

                Map<String, List<CheckpointEvent>> grouped = parseAndGroup(records);
                boolean allIndexesOk = true;

                for (Map.Entry<String, List<CheckpointEvent>> e : grouped.entrySet()) {
                    String indexId = e.getKey();
                    List<CheckpointEvent> events = e.getValue();
                    try {
                        MergeResult mr = mergeExecutor.merge(indexId, events);
                        log.info("merge done index={}, files={}, bytes={}", indexId, mr.filesCount(), mr.bytesWritten());

                        ArchiveIndexMeta meta = pg.getIndexMeta(indexId)
                                .orElseThrow(() -> new IllegalStateException("No index metadata for " + indexId));
                        List<RotationRuleConfig> rules = pg.getEnabledRules(indexId);
                        List<RotationAction> actions = rotationEvaluator.evaluate(meta, rules, hdfs);
                        rotationExecutor.execute(indexId, actions);
                    } catch (Exception ex) {
                        allIndexesOk = false;
                        log.error("Processing failed for index={}, eventCount={}", indexId, events.size(), ex);
                    }
                }

                if (allIndexesOk) {
                    consumer.commitSync();
                } else {
                    log.warn("Kafka offsets are not committed in this cycle due to index processing errors");
                }
                maybeRunDeferredCleanup(cycle, cfg.deferredCleanupEveryCycles(), cleanupService);
            }
        } finally {
            spark.stop();
        }
    }

    private static void maybeRunDeferredCleanup(int cycle, int eachCycle, DeferredCleanupService cleanupService) {
        if (cycle % eachCycle != 0) return;
        try {
            cleanupService.processReadyDeletes();
        } catch (Exception e) {
            log.error("Deferred cleanup cycle failed", e);
        }
    }

    private static Map<String, List<CheckpointEvent>> parseAndGroup(ConsumerRecords<String, String> records) {
        Map<String, List<CheckpointEvent>> grouped = new LinkedHashMap<>();
        for (ConsumerRecord<String, String> rec : records) {
            CheckpointEvent event = parseEvent(rec);
            if (event == null) continue;
            grouped.computeIfAbsent(event.indexId(), k -> new ArrayList<>()).add(event);
        }
        return grouped;
    }

    private static CheckpointEvent parseEvent(ConsumerRecord<String, String> rec) {
        try {
            return mapper.readValue(rec.value(), CheckpointEvent.class);
        } catch (Exception e) {
            log.error("Cannot parse merge event. topic={}, partition={}, offset={}", rec.topic(), rec.partition(), rec.offset(), e);
            return null;
        }
    }

    private static KafkaConsumer<String, String> buildConsumer(JobConfig cfg) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.kafkaBrokers());
        p.put(ConsumerConfig.GROUP_ID_CONFIG, cfg.consumerGroupId());
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Integer.toString(cfg.kafkaMaxPollRecords()));
        return new KafkaConsumer<>(p);
    }
}
