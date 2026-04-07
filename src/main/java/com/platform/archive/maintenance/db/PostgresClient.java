package com.platform.archive.maintenance.db;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.platform.archive.maintenance.model.ArchiveIndexMeta;
import com.platform.archive.maintenance.model.RotationRuleConfig;
import com.platform.archive.maintenance.model.RotationRuleType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public class PostgresClient implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(PostgresClient.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    private final Connection connection;

    public PostgresClient(String jdbc, String user, String password) throws SQLException {
        this.connection = DriverManager.getConnection(jdbc, user, password);
        this.connection.setAutoCommit(true);
    }

    public Optional<ArchiveIndexMeta> getIndexMeta(String indexId) throws SQLException {
        String sql = "select index_id, active_directory, tmp_directory, target_file_size, dir_created_at from archive_indexes where index_id = ?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, indexId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                Timestamp createdAt = rs.getTimestamp("dir_created_at");
                return Optional.of(new ArchiveIndexMeta(
                        rs.getString("index_id"),
                        rs.getString("active_directory"),
                        rs.getString("tmp_directory"),
                        rs.getLong("target_file_size"),
                        createdAt != null ? createdAt.toInstant() : Instant.EPOCH));
            }
        }
    }

    public Optional<String> findCompletedBatch(String indexId, List<String> files) throws SQLException {
        String sql = "select batch_id from merge_batches where index_id = ? and files = ? and status = 'completed' limit 1";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, indexId);
            ps.setArray(2, connection.createArrayOf("text", files.toArray()));
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? Optional.of(rs.getString("batch_id")) : Optional.empty();
            }
        }
    }

    public String registerBatchPending(String indexId, List<String> files) throws SQLException {
        String batchId = UUID.randomUUID().toString();
        String sql = "insert into merge_batches(batch_id, index_id, files, status, created_at) values (?, ?, ?, 'pending', now())";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, batchId);
            ps.setString(2, indexId);
            ps.setArray(3, connection.createArrayOf("text", files.toArray()));
            ps.executeUpdate();
        }
        return batchId;
    }

    public void completeBatch(String batchId, long bytesWritten) throws SQLException {
        String sql = "update merge_batches set status='completed', bytes_written=?, completed_at=now() where batch_id=?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setLong(1, bytesWritten);
            ps.setString(2, batchId);
            ps.executeUpdate();
        }
    }

    public void updateIndexMergeStats(String indexId, long bytesWritten) throws SQLException {
        String sql = "update archive_indexes set last_merge_at=now(), total_bytes_merged=coalesce(total_bytes_merged,0)+? where index_id=?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setLong(1, bytesWritten);
            ps.setString(2, indexId);
            ps.executeUpdate();
        }
    }

    public List<RotationRuleConfig> getEnabledRules(String indexId) throws Exception {
        String sql = "select index_id,name,type,params,execution_order,enabled from rotation_rules where index_id=? and enabled=true order by execution_order";
        List<RotationRuleConfig> result = new ArrayList<>();
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, indexId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    JsonNode params = mapper.readTree(rs.getString("params"));
                    result.add(new RotationRuleConfig(
                            rs.getString("index_id"),
                            rs.getString("name"),
                            RotationRuleType.valueOf(rs.getString("type")),
                            params,
                            rs.getInt("execution_order"),
                            rs.getBoolean("enabled")
                    ));
                }
            }
        }
        return result;
    }

    public void switchActiveDirectory(String indexId, String newPath) throws SQLException {
        String sql = "update archive_indexes set active_directory=?, dir_created_at=now() where index_id=?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, newPath);
            ps.setString(2, indexId);
            ps.executeUpdate();
        }
    }

    public void addDeferredDelete(String path, int graceMinutes) throws SQLException {
        String sql = "insert into deferred_deletes(path, delete_after, status, created_at) values (?, now() + (? * interval '1 minute'), 'pending', now())";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, path);
            ps.setInt(2, graceMinutes);
            ps.executeUpdate();
        }
    }

    public List<DeferredDeleteItem> getReadyDeferredDeletes() throws SQLException {
        String sql = "select id, path from deferred_deletes where status='pending' and delete_after <= now()";
        List<DeferredDeleteItem> out = new ArrayList<>();
        try (PreparedStatement ps = connection.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) out.add(new DeferredDeleteItem(rs.getLong("id"), rs.getString("path")));
        }
        return out;
    }

    public void markDeferredDeleteCompleted(long id) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement("update deferred_deletes set status='completed', completed_at=now() where id=?")) {
            ps.setLong(1, id);
            ps.executeUpdate();
        }
    }

    public void markDeferredDeleteFailed(long id, String error) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement("update deferred_deletes set status='failed', error_message=? where id=?")) {
            ps.setString(1, error);
            ps.setLong(2, id);
            ps.executeUpdate();
        }
    }

    @Override
    public void close() {
        try {
            connection.close();
        } catch (SQLException e) {
            log.warn("Failed to close postgres connection", e);
        }
    }

    public record DeferredDeleteItem(long id, String path) {}
}
