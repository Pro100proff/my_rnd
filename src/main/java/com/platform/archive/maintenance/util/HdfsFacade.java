package com.platform.archive.maintenance.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class HdfsFacade {
    private final FileSystem fs;

    public HdfsFacade() throws IOException {
        this.fs = FileSystem.get(new Configuration());
    }

    public List<String> existingStableFiles(List<String> files, Duration minAge) throws IOException {
        Instant cutoff = Instant.now().minus(minAge);
        List<String> out = new ArrayList<>();
        for (String file : files) {
            if (file.endsWith(".in-progress")) continue;
            Path p = new Path(file);
            if (!fs.exists(p)) continue;
            FileStatus st = fs.getFileStatus(p);
            if (Instant.ofEpochMilli(st.getModificationTime()).isAfter(cutoff)) continue;
            out.add(file);
        }
        return out;
    }

    public void deletePaths(List<String> paths) throws IOException {
        for (String p : paths) fs.delete(new Path(p), false);
    }

    public boolean exists(String p) throws IOException { return fs.exists(new Path(p)); }
    public void mkdirs(String p) throws IOException { fs.mkdirs(new Path(p)); }
    public void deleteRecursively(String p) throws IOException { fs.delete(new Path(p), true); }
    public boolean rename(String from, String to) throws IOException { return fs.rename(new Path(from), new Path(to)); }

    public long contentSize(String path) throws IOException {
        return fs.getContentSummary(new Path(path)).getLength();
    }

    public List<String> listDtPartitionsSorted(String activeDir) throws IOException {
        List<String> partitions = new ArrayList<>();
        FileStatus[] statuses = fs.listStatus(new Path(activeDir));
        for (FileStatus status : statuses) {
            String name = status.getPath().getName();
            if (status.isDirectory() && name.startsWith("dt=")) partitions.add(status.getPath().toString());
        }
        partitions.sort(Comparator.naturalOrder());
        return partitions;
    }
}
