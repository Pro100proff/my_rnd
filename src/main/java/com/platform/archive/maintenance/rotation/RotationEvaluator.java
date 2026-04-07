package com.platform.archive.maintenance.rotation;

import com.fasterxml.jackson.databind.JsonNode;
import com.platform.archive.maintenance.model.ArchiveIndexMeta;
import com.platform.archive.maintenance.model.RotationRuleConfig;
import com.platform.archive.maintenance.model.RotationRuleType;
import com.platform.archive.maintenance.util.HdfsFacade;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class RotationEvaluator {
    private static final DateTimeFormatter DATE = DateTimeFormatter.ISO_LOCAL_DATE;

    public List<RotationAction> evaluate(ArchiveIndexMeta meta, List<RotationRuleConfig> rules, HdfsFacade hdfs) throws Exception {
        List<RotationAction> actions = new ArrayList<>();
        List<String> partitions = hdfs.listDtPartitionsSorted(meta.activeDirectory());

        for (RotationRuleConfig rule : rules) {
            if (!rule.enabled()) continue;
            JsonNode p = rule.params();
            RotationRuleType t = rule.type();
            if (t == RotationRuleType.max_age_days) {
                int days = p.get("days").asInt();
                LocalDate cutoff = LocalDate.now(ZoneOffset.UTC).minusDays(days);
                List<String> old = partitions.stream().filter(path -> partitionDate(path).isBefore(cutoff)).toList();
                if (!old.isEmpty()) actions.add(new DropPartitionsAction(old));
            } else if (t == RotationRuleType.max_total_size) {
                long maxBytes = (long) (p.get("max_gb").asLong() * 1024d * 1024d * 1024d);
                long current = hdfs.contentSize(meta.activeDirectory());
                List<String> toDrop = new ArrayList<>();
                for (String part : partitions) {
                    if (current <= maxBytes) break;
                    current -= hdfs.contentSize(part);
                    toDrop.add(part);
                }
                if (!toDrop.isEmpty()) actions.add(new DropPartitionsAction(toDrop));
            } else if (t == RotationRuleType.content_filter) {
                int olderThan = p.get("older_than_days").asInt();
                String condition = p.get("condition").asText();
                LocalDate cutoff = LocalDate.now(ZoneOffset.UTC).minusDays(olderThan);
                partitions.stream().filter(path -> partitionDate(path).isBefore(cutoff))
                        .forEach(path -> actions.add(new FilterContentAction(path, condition)));
            } else if (t == RotationRuleType.sampling) {
                int olderThan = p.get("older_than_days").asInt();
                String condition = p.get("condition").asText();
                double keepRatio = p.get("keep_ratio").asDouble();
                LocalDate cutoff = LocalDate.now(ZoneOffset.UTC).minusDays(olderThan);
                partitions.stream().filter(path -> partitionDate(path).isBefore(cutoff))
                        .forEach(path -> actions.add(new SampleContentAction(path, condition, keepRatio)));
            } else if (t == RotationRuleType.directory_switch) {
                int switchEveryDays = p.get("switch_every_days").asInt();
                int grace = p.get("grace_minutes").asInt(30);
                if (meta.dirCreatedAt().plusSeconds(switchEveryDays * 86400L).isBefore(java.time.Instant.now())) {
                    String newDir = meta.activeDirectory() + "-" + LocalDate.now(ZoneOffset.UTC).format(DATE);
                    actions.add(new SwitchDirectoryAction(meta.activeDirectory(), newDir, grace));
                }
            }
        }
        return actions;
    }

    private LocalDate partitionDate(String path) {
        String marker = "dt=";
        int idx = path.lastIndexOf(marker);
        if (idx < 0) return LocalDate.MIN;
        String date = path.substring(idx + marker.length(), Math.min(path.length(), idx + marker.length() + 10));
        return LocalDate.parse(date, DATE);
    }
}
