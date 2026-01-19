package io.ducklake.service.event.store.clickhouse;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.ducklake.service.event.model.AccessEvent;
import io.ducklake.service.event.model.Resource;
import io.ducklake.service.event.model.SearchQuery;
import io.ducklake.service.event.store.EventStore;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * ClickHouse implementation of EventStore (READ-ONLY).
 *
 * WRITES: Handled by OTel Collector with ClickHouse exporter
 * READS:  This class queries the access_log table
 */
@Slf4j
@RequiredArgsConstructor
public class ClickHouseEventStore implements EventStore {

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    private static final String CREATE_TABLE_SQL = """
        CREATE TABLE IF NOT EXISTS access_log (
            event_id String,
            timestamp DateTime64(3),
            trace_id String,
            span_id String,
            user_id String,
            user_email String,
            source_ip String,
            client_info String,
            resource_type LowCardinality(String),
            resource_id String,
            resource_name String,
            resource_path String,
            operation LowCardinality(String),
            status LowCardinality(String),
            rejection_reason String,
            rows_affected UInt64,
            execution_time_ms UInt32
        )
        ENGINE = MergeTree()
        PARTITION BY toYYYYMM(timestamp)
        ORDER BY (user_id, timestamp)
        """;

    @Override
    public void initialize() {
        log.info("Initializing ClickHouse event store (read-only)...");
        jdbcTemplate.execute(CREATE_TABLE_SQL);
        log.info("ClickHouse event store initialized");
    }

    @Override
    public List<AccessEvent> getRecentByUser(String userId, int limit) {
        String sql = """
            SELECT event_id, timestamp, trace_id, span_id,
                   user_id, user_email, source_ip, client_info,
                   resource_type, resource_id, resource_name, resource_path,
                   operation, status, rejection_reason, rows_affected, execution_time_ms
            FROM access_log
            WHERE user_id = ?
                AND (resource_type, resource_id, timestamp) IN (
                    SELECT resource_type, resource_id, max(timestamp)
                    FROM access_log
                    WHERE user_id = ?
                    GROUP BY resource_type, resource_id
                )
            ORDER BY timestamp DESC
            LIMIT ?
            """;
        return jdbcTemplate.query(sql, new AccessEventRowMapper(), userId, userId, limit);
    }

    @Override
    public List<AccessEvent> getRecentByUserAndType(String userId, String resourceType, int limit) {
        String sql = """
            SELECT event_id, timestamp, trace_id, span_id,
                   user_id, user_email, source_ip, client_info,
                   resource_type, resource_id, resource_name, resource_path,
                   operation, status, rejection_reason, rows_affected, execution_time_ms
            FROM access_log
            WHERE user_id = ? AND resource_type = ?
                AND (resource_id, timestamp) IN (
                    SELECT resource_id, max(timestamp)
                    FROM access_log
                    WHERE user_id = ? AND resource_type = ?
                    GROUP BY resource_id
                )
            ORDER BY timestamp DESC
            LIMIT ?
            """;
        return jdbcTemplate.query(sql, new AccessEventRowMapper(),
                userId, resourceType, userId, resourceType, limit);
    }

    @Override
    public List<AccessEvent> search(SearchQuery query) {
        StringBuilder sql = new StringBuilder("""
            SELECT event_id, timestamp, trace_id, span_id,
                   user_id, user_email, source_ip, client_info,
                   resource_type, resource_id, resource_name, resource_path,
                   operation, status, rejection_reason, rows_affected, execution_time_ms
            FROM access_log WHERE 1=1
            """);

        List<Object> params = new ArrayList<>();

        if (query.getUserId() != null) {
            sql.append(" AND user_id = ?");
            params.add(query.getUserId());
        }

        if (query.getText() != null && !query.getText().isBlank()) {
            sql.append(" AND position(lower(resource_name), lower(?)) > 0");
            params.add(query.getText());
        }

        if (query.getResourceTypes() != null && !query.getResourceTypes().isEmpty()) {
            sql.append(" AND resource_type IN (");
            for (int i = 0; i < query.getResourceTypes().size(); i++) {
                sql.append(i > 0 ? ", ?" : "?");
                params.add(query.getResourceTypes().get(i));
            }
            sql.append(")");
        }

        if (query.getOperations() != null && !query.getOperations().isEmpty()) {
            sql.append(" AND operation IN (");
            for (int i = 0; i < query.getOperations().size(); i++) {
                sql.append(i > 0 ? ", ?" : "?");
                params.add(query.getOperations().get(i));
            }
            sql.append(")");
        }

        if (query.getStatuses() != null && !query.getStatuses().isEmpty()) {
            sql.append(" AND status IN (");
            for (int i = 0; i < query.getStatuses().size(); i++) {
                sql.append(i > 0 ? ", ?" : "?");
                params.add(query.getStatuses().get(i));
            }
            sql.append(")");
        }

        if (query.getFromTime() != null) {
            sql.append(" AND timestamp >= ?");
            params.add(Timestamp.from(query.getFromTime()));
        }

        if (query.getToTime() != null) {
            sql.append(" AND timestamp <= ?");
            params.add(Timestamp.from(query.getToTime()));
        }

        sql.append(" ORDER BY timestamp DESC LIMIT ? OFFSET ?");
        params.add(query.getLimit());
        params.add(query.getOffset());

        return jdbcTemplate.query(sql.toString(), new AccessEventRowMapper(), params.toArray());
    }

    @Override
    public List<AccessEvent> getAuditTrail(String resourceType, String resourceId, int limit) {
        String sql = """
            SELECT event_id, timestamp, trace_id, span_id,
                   user_id, user_email, source_ip, client_info,
                   resource_type, resource_id, resource_name, resource_path,
                   operation, status, rejection_reason, rows_affected, execution_time_ms
            FROM access_log
            WHERE resource_type = ? AND resource_id = ?
            ORDER BY timestamp DESC
            LIMIT ?
            """;
        return jdbcTemplate.query(sql, new AccessEventRowMapper(), resourceType, resourceId, limit);
    }

    private Map<String, String> fromJson(String json) {
        if (json == null || json.isBlank() || "{}".equals(json)) return null;
        try {
            return objectMapper.readValue(json, new TypeReference<>() {});
        } catch (JsonProcessingException e) {
            log.warn("Failed to deserialize resource path: {}", e.getMessage());
            return null;
        }
    }

    private String emptyToNull(String s) {
        return (s == null || s.isEmpty()) ? null : s;
    }

    private class AccessEventRowMapper implements RowMapper<AccessEvent> {
        @Override
        public AccessEvent mapRow(ResultSet rs, int rowNum) throws SQLException {
            Resource resource = Resource.builder()
                    .type(rs.getString("resource_type"))
                    .id(rs.getString("resource_id"))
                    .name(emptyToNull(rs.getString("resource_name")))
                    .path(fromJson(rs.getString("resource_path")))
                    .build();

            Timestamp ts = rs.getTimestamp("timestamp");
            long rowsAffected = rs.getLong("rows_affected");
            int execTime = rs.getInt("execution_time_ms");

            return AccessEvent.builder()
                    .eventId(rs.getString("event_id"))
                    .timestamp(ts != null ? ts.toInstant() : null)
                    .traceId(emptyToNull(rs.getString("trace_id")))
                    .spanId(emptyToNull(rs.getString("span_id")))
                    .userId(rs.getString("user_id"))
                    .userEmail(emptyToNull(rs.getString("user_email")))
                    .sourceIp(emptyToNull(rs.getString("source_ip")))
                    .clientInfo(emptyToNull(rs.getString("client_info")))
                    .resource(resource)
                    .operation(emptyToNull(rs.getString("operation")))
                    .status(emptyToNull(rs.getString("status")))
                    .rejectionReason(emptyToNull(rs.getString("rejection_reason")))
                    .rowsAffected(rowsAffected > 0 ? rowsAffected : null)
                    .executionTimeMs(execTime > 0 ? execTime : null)
                    .build();
        }
    }
}
