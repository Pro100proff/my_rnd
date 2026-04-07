package com.platform.archive.flink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.platform.archive.maintenance.model.CheckpointEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.apache.orc.storage.ql.exec.vector.BytesColumnVector;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class FlinkArchiveIndexerJob {
    private static final Logger log = LoggerFactory.getLogger(FlinkArchiveIndexerJob.class);
    private static final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();

    public static void main(String[] args) throws Exception {
        Map<String, String> cfg = parseArgs(args);
        String kafkaBrokers = required(cfg, "kafka-brokers");
        String sourceTopic = cfg.getOrDefault("source-topic", "raw-archive-events");
        String mergeTopic = cfg.getOrDefault("merge-events-topic", "merge-events");
        String hdfsBaseTmp = cfg.getOrDefault("hdfs-tmp-base", "hdfs://namenode:8020/tmp");
        long targetBytes = Long.parseLong(cfg.getOrDefault("target-file-size-bytes", "1048576"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(15000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000L);
        env.setParallelism(1);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaBrokers)
                .setTopics(sourceTopic)
                .setGroupId(cfg.getOrDefault("source-group-id", "flink-archive-indexer"))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.latest())
                .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "raw-kafka-source")
                .map(json -> mapper.readValue(json, RawLogEvent.class))
                .returns(TypeInformation.of(new TypeHint<RawLogEvent>() {}))
                .addSink(new RollingOrcSink(kafkaBrokers, mergeTopic, hdfsBaseTmp, targetBytes));

        env.execute("flink-archive-indexer");
    }

    private static class RollingOrcSink extends RichSinkFunction<RawLogEvent> implements CheckpointedFunction, CheckpointListener {
        private static final TypeDescription SCHEMA = TypeDescription.fromString("struct<index_id:string,level:string,message:string,event_ts:string>");

        private final String kafkaBrokers;
        private final String mergeTopic;
        private final String hdfsBaseTmp;
        private final long targetFileSizeBytes;

        private transient Writer writer;
        private transient VectorizedRowBatch batch;
        private transient String currentFile;
        private transient long currentBytes;
        private transient String currentIndexId;

        private transient ListState<PendingCheckpointFiles> pendingState;
        private final Map<Long, PendingCheckpointFiles> pendingByCheckpoint = new HashMap<>();
        private final List<WrittenFile> stagedFiles = new ArrayList<>();

        private transient ExecutorService notifierPool;
        private transient KafkaProducer<String, String> mergeProducer;

        private RollingOrcSink(String kafkaBrokers, String mergeTopic, String hdfsBaseTmp, long targetFileSizeBytes) {
            this.kafkaBrokers = kafkaBrokers;
            this.mergeTopic = mergeTopic;
            this.hdfsBaseTmp = hdfsBaseTmp;
            this.targetFileSizeBytes = targetFileSizeBytes;
        }

        @Override
        public void open(Configuration parameters) {
            Properties p = new Properties();
            p.setProperty("bootstrap.servers", kafkaBrokers);
            p.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            p.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            p.setProperty("acks", "all");
            mergeProducer = new KafkaProducer<>(p);
            notifierPool = Executors.newSingleThreadExecutor();
        }

        @Override
        public void invoke(RawLogEvent value, Context context) throws Exception {
            Instant ts = value.eventTs == null ? Instant.now() : value.eventTs;
            if (writer == null || !Objects.equals(currentIndexId, value.indexId)) {
                closeCurrent();
                rotateFile(value.indexId, ts);
            }

            int row = batch.size++;
            setString((BytesColumnVector) batch.cols[0], row, value.indexId);
            setString((BytesColumnVector) batch.cols[1], row, value.level);
            setString((BytesColumnVector) batch.cols[2], row, value.message);
            setString((BytesColumnVector) batch.cols[3], row, ts.toString());
            currentBytes += approxBytes(value);

            if (batch.size == batch.getMaxSize()) {
                writer.addRowBatch(batch);
                batch.reset();
            }

            if (currentBytes >= targetFileSizeBytes) {
                closeCurrent();
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            closeCurrent();
            PendingCheckpointFiles cp = new PendingCheckpointFiles();
            cp.checkpointId = context.getCheckpointId();
            cp.timestamp = Instant.now().toString();
            cp.files.addAll(stagedFiles);
            stagedFiles.clear();
            pendingByCheckpoint.put(cp.checkpointId, cp);

            pendingState.clear();
            for (PendingCheckpointFiles value : pendingByCheckpoint.values()) {
                pendingState.add(value);
            }
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            pendingState = context.getOperatorStateStore()
                    .getListState(new ListStateDescriptor<>("pending-checkpoints", PendingCheckpointFiles.class));
            if (context.isRestored()) {
                for (PendingCheckpointFiles value : pendingState.get()) {
                    pendingByCheckpoint.put(value.checkpointId, value);
                }
            }
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            PendingCheckpointFiles completed = pendingByCheckpoint.remove(checkpointId);
            if (completed == null || completed.files.isEmpty()) return;

            notifierPool.submit(() -> {
                try {
                    Map<String, List<WrittenFile>> byIndex = completed.files.stream().collect(Collectors.groupingBy(w -> w.indexId));
                    for (Map.Entry<String, List<WrittenFile>> e : byIndex.entrySet()) {
                        String indexId = e.getKey();
                        List<String> files = e.getValue().stream().map(w -> w.path).toList();
                        long bytes = e.getValue().stream().mapToLong(w -> w.bytes).sum();
                        CheckpointEvent event = new CheckpointEvent(indexId, checkpointId, files, bytes, Instant.parse(completed.timestamp));

                        String payload = mapper.writeValueAsString(event);
                        mergeProducer.send(new ProducerRecord<>(mergeTopic, indexId, payload)).get();
                        log.info("merge-event sent index={}, checkpoint={}, files={}", indexId, checkpointId, files.size());
                    }
                } catch (Exception ex) {
                    log.error("Failed async merge-event publishing for checkpoint={}", checkpointId, ex);
                }
            });
        }

        @Override
        public void close() throws Exception {
            closeCurrent();
            if (mergeProducer != null) mergeProducer.close();
            if (notifierPool != null) notifierPool.shutdown();
            super.close();
        }

        private void rotateFile(String indexId, Instant ts) throws Exception {
            String dt = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneOffset.UTC).format(ts);
            String hr = DateTimeFormatter.ofPattern("HH").withZone(ZoneOffset.UTC).format(ts);
            currentFile = hdfsBaseTmp + "/" + indexId + "/dt=" + dt + "/hr=" + hr + "/slot-" + UUID.randomUUID() + ".orc";
            writer = OrcFile.createWriter(new org.apache.hadoop.fs.Path(currentFile),
                    OrcFile.writerOptions(new org.apache.hadoop.conf.Configuration()).setSchema(SCHEMA));
            batch = SCHEMA.createRowBatch();
            currentBytes = 0L;
            currentIndexId = indexId;
        }

        private void closeCurrent() throws Exception {
            if (writer == null) return;
            if (batch.size > 0) {
                writer.addRowBatch(batch);
                batch.reset();
            }
            writer.close();
            stagedFiles.add(new WrittenFile(currentIndexId, currentFile, currentBytes));

            writer = null;
            batch = null;
            currentFile = null;
            currentBytes = 0L;
            currentIndexId = null;
        }

        private void setString(BytesColumnVector vector, int row, String value) {
            String v = value == null ? "" : value;
            byte[] bytes = v.getBytes(StandardCharsets.UTF_8);
            vector.setRef(row, bytes, 0, bytes.length);
        }

        private long approxBytes(RawLogEvent v) {
            return (v.indexId == null ? 0 : v.indexId.length())
                    + (v.level == null ? 0 : v.level.length())
                    + (v.message == null ? 0 : v.message.length())
                    + 32;
        }
    }

    public static class RawLogEvent {
        public String indexId;
        public String level;
        public String message;
        public Instant eventTs;
    }

    public static class WrittenFile {
        public String indexId;
        public String path;
        public long bytes;

        public WrittenFile() {}

        public WrittenFile(String indexId, String path, long bytes) {
            this.indexId = indexId;
            this.path = path;
            this.bytes = bytes;
        }
    }

    public static class PendingCheckpointFiles {
        public long checkpointId;
        public List<WrittenFile> files = new ArrayList<>();
        public String timestamp = Instant.now().toString();
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> out = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            String a = args[i];
            if (!a.startsWith("--")) continue;
            String k = a.substring(2);
            String v = i + 1 < args.length ? args[++i] : "";
            out.put(k, v);
        }
        return out;
    }

    private static String required(Map<String, String> m, String key) {
        String v = m.get(key);
        if (v == null || v.isBlank()) throw new IllegalArgumentException("Missing --" + key);
        return v;
    }
}
