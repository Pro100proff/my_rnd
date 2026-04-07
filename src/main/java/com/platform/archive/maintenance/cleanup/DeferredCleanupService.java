package com.platform.archive.maintenance.cleanup;

import com.platform.archive.maintenance.db.PostgresClient;
import com.platform.archive.maintenance.util.HdfsFacade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeferredCleanupService {
    private static final Logger log = LoggerFactory.getLogger(DeferredCleanupService.class);

    private final PostgresClient pg;
    private final HdfsFacade hdfs;

    public DeferredCleanupService(PostgresClient pg, HdfsFacade hdfs) {
        this.pg = pg;
        this.hdfs = hdfs;
    }

    public void processReadyDeletes() throws Exception {
        for (PostgresClient.DeferredDeleteItem item : pg.getReadyDeferredDeletes()) {
            try {
                hdfs.deleteRecursively(item.path());
                pg.markDeferredDeleteCompleted(item.id());
            } catch (Exception e) {
                log.error("Deferred delete failed for {}", item.path(), e);
                pg.markDeferredDeleteFailed(item.id(), e.getMessage());
            }
        }
    }
}
