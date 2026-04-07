package com.platform.archive.maintenance.merge;

import com.platform.archive.maintenance.db.PostgresClient;
import com.platform.archive.maintenance.model.ArchiveIndexMeta;
import com.platform.archive.maintenance.model.CheckpointEvent;
import com.platform.archive.maintenance.util.HdfsFacade;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

public class MergeExecutor {
    private static final Logger log = LoggerFactory.getLogger(MergeExecutor.class);

    private final SparkSession spark;
    private final PostgresClient pg;
    private final HdfsFacade hdfs;
    private final Duration minAge;

    public MergeExecutor(SparkSession spark, PostgresClient pg, HdfsFacade hdfs, Duration minAge) {
        this.spark = spark;
        this.pg = pg;
        this.hdfs = hdfs;
        this.minAge = minAge;
    }

    public MergeResult merge(String indexId, List<CheckpointEvent> events) throws Exception {
        ArchiveIndexMeta meta = pg.getIndexMeta(indexId).orElseThrow(() -> new IllegalStateException("No archive index config for " + indexId));
        List<String> allFiles = events.stream().flatMap(e -> e.files().stream()).distinct().toList();
        List<String> stableFiles = hdfs.existingStableFiles(allFiles, minAge);
        if (stableFiles.isEmpty()) {
            return new MergeResult(0, 0L);
        }

        Optional<String> completed = pg.findCompletedBatch(indexId, stableFiles);
        if (completed.isPresent()) {
            log.info("Batch already completed for index={}, batch={}, deleting tmp files only", indexId, completed.get());
            hdfs.deletePaths(stableFiles);
            return new MergeResult(stableFiles.size(), 0L);
        }

        String batchId = pg.registerBatchPending(indexId, stableFiles);
        long totalBytes = events.stream().mapToLong(CheckpointEvent::totalBytes).sum();
        int repartitions = (int) Math.max(1, Math.ceil((double) totalBytes / (double) meta.targetFileSize()));
        String outputPath = meta.activeDirectory() + "/batch-" + batchId;

        Dataset<Row> df = spark.read().orc(stableFiles.toArray(new String[0]));
        df.repartition(repartitions)
                .write()
                .mode("append")
                .format("orc")
                .option("orc.compress", "zstd")
                .save(outputPath);

        hdfs.deletePaths(stableFiles);
        long bytesWritten = hdfs.contentSize(outputPath);
        pg.completeBatch(batchId, bytesWritten);
        pg.updateIndexMergeStats(indexId, bytesWritten);

        return new MergeResult(stableFiles.size(), bytesWritten);
    }
}
