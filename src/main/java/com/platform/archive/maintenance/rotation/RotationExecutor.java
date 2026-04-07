package com.platform.archive.maintenance.rotation;

import com.platform.archive.maintenance.db.PostgresClient;
import com.platform.archive.maintenance.util.HdfsFacade;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.rand;

public class RotationExecutor {
    private final SparkSession spark;
    private final HdfsFacade hdfs;
    private final PostgresClient pg;

    public RotationExecutor(SparkSession spark, HdfsFacade hdfs, PostgresClient pg) {
        this.spark = spark;
        this.hdfs = hdfs;
        this.pg = pg;
    }

    public void execute(String indexId, List<RotationAction> actions) throws Exception {
        for (RotationAction action : actions) {
            if (action instanceof DropPartitionsAction drop) {
                for (String p : drop.partitionPaths()) hdfs.deleteRecursively(p);
            } else if (action instanceof FilterContentAction filter) {
                rewritePartition(filter.partitionPath(), "NOT (" + filter.condition() + ")");
            } else if (action instanceof SampleContentAction sample) {
                String condition = sample.condition();
                Dataset<Row> df = spark.read().orc(sample.partitionPath());
                Dataset<Row> matched = df.filter(expr(condition)).where(rand().lt(sample.keepRatio()));
                Dataset<Row> unmatched = df.filter(expr("NOT (" + condition + ")"));
                Dataset<Row> result = unmatched.unionByName(matched);
                atomicRewrite(sample.partitionPath(), result);
            } else if (action instanceof SwitchDirectoryAction sw) {
                if (!hdfs.exists(sw.newDirectory())) hdfs.mkdirs(sw.newDirectory());
                pg.switchActiveDirectory(indexId, sw.newDirectory());
                pg.addDeferredDelete(sw.oldDirectory(), sw.graceMinutes());
            }
        }
    }

    private void rewritePartition(String partitionPath, String predicate) throws Exception {
        Dataset<Row> df = spark.read().orc(partitionPath).filter(expr(predicate));
        atomicRewrite(partitionPath, df);
    }

    private void atomicRewrite(String partitionPath, Dataset<Row> df) throws Exception {
        String tmp = partitionPath + ".__rewrite_tmp";
        try {
            hdfs.deleteRecursively(tmp);
            df.write().mode("overwrite").format("orc").option("orc.compress", "zstd").save(tmp);
            hdfs.deleteRecursively(partitionPath);
            if (!hdfs.rename(tmp, partitionPath)) throw new IllegalStateException("Rename failed for " + partitionPath);
        } catch (Exception e) {
            hdfs.deleteRecursively(tmp);
            throw e;
        }
    }
}
