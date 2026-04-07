package com.platform.archive.maintenance.model;

import java.time.Instant;

public record ArchiveIndexMeta(
        String indexId,
        String activeDirectory,
        String tmpDirectory,
        long targetFileSize,
        Instant dirCreatedAt) {
}
