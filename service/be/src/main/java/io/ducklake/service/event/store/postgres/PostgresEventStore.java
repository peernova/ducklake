package io.ducklake.service.event.store.postgres;

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
 * PostgreSQL implementation of EventStore (READ-ONLY).
 *
 * WRITES: Handled by OTel Collector with PostgreSQL exporter
 * READS:  This class queries the access_log table
 */
@Slf4j
@RequiredArgsConstructor
public class PostgresEventStore implements EventStore {

    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;

    private static final String CREATE_TABLE_SQL = """
        CREATE TABLE IF NOT EXISTS access_log (
            id BIGSERIAL PRIMARY KEY,
            event_id VARCHAR(36) NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            trace_id VARCHAR(32),
            span_id VARCHAR(16),
            user_id VARCHAR(255) NOT NULL,
            user_email VARCHAR(255),
            source_ip VARCHAR(45),
            client_info VARCHAR(500),
            resource_type VARCHAR(50) NOT NULL,
            resource_id VARCHAR(500) NOT NULL,
            resource_name VARCHAR(500),
            resource_path JSONB,
            operation VARCHAR(50),
            status VARCHAR(20),
            rejection_reason VARCHAR(500),
            rows_affected BIGINT,
            execution_time_ms INT
        )
        """;

    private static final String CREATE_INDEXES_SQL = """
        CREATE INDEX IF NOT EXISTS idx_access_log_user_time ON access_log(user_id, timestamp DESC);
        CREATE INDEX IF NOT EXISTS idx_access_log_resource ON access_log(resource_type, resource_id, timestamp DESC);
        CREATE INDEX IF NOT EXISTS idx_access_log_timestamp ON access_log(timestamp DESC);
        """;

    @Override
    public void initialize() {
        log.info("Initializing PostgreSQL event store (read-only)...");
        jdbcTemplate.execute(CREATE_TABLE_SQL);
        for (String sql : CREATE_INDEXES_SQL.split(";")) {
            if (!sql.trim().isEmpty()) {
                try {
                    jdbcTemplate.execute(sql.trim());
                } catch (Exception e) {
                    log.debug("Index may already exist: {}", e.getMessage());
                }
            }
        }
        log.info("PostgreSQL event store initialized");
    }

    @Override
    public List<AccessEvent> getRecentByUser(String userId, int limit) {
        String sql = """
            SELECT DISTINCT ON (resource_type, resource_id)
                   event_id, timestamp, trace_id, span_id,
                   user_id, user_email, source_ip, client_info,
                   resource_type, resource_id, resource_name, resource_path,
                   operation, status, rejection_reason, rows_affected, execution_time_ms
            FROM access_log
            WHERE user_id = ?
            ORDER BY resource_type, resource_id, timestamp DESC
            LIMIT ?
            """;
        return jdbcTemplate.query(sql, new AccessEventRowMapper(), userId, limit);
    }

    @Override
    public List<AccessEvent> getRecentByUserAndType(String userId, String resourceType, int limit) {
        String sql = """
            SELECT DISTINCT ON (resource_id)
                   event_id, timestamp, trace_id, span_id,
                   user_id, user_email, source_ip, client_info,
                   resource_type, resource_id, resource_name, resource_path,
                   operation, status, rejection_reason, rows_affected, execution_time_ms
            FROM access_log
            WHERE user_id = ? AND resource_type = ?
            ORDER BY resource_id, timestamp DESC
            LIMIT ?
            """;
        return jdbcTemplate.query(sql, new AccessEventRowMapper(), userId, resourceType, limit);
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
            sql.append(" AND resource_name ILIKE ?");
            params.add("%" + query.getText() + "%");
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
        if (json == null || json.isBlank()) return null;
        try {
            return objectMapper.readValue(json, new TypeReference<>() {});
        } catch (JsonProcessingException e) {
            log.warn("Failed to deserialize resource path: {}", e.getMessage());
            return null;
        }
    }

    private class AccessEventRowMapper implements RowMapper<AccessEvent> {
        @Override
        public AccessEvent mapRow(ResultSet rs, int rowNum) throws SQLException {
            Resource resource = Resource.builder()
                    .type(rs.getString("resource_type"))
                    .id(rs.getString("resource_id"))
                    .name(rs.getString("resource_name"))
                    .path(fromJson(rs.getString("resource_path")))
                    .build();

            Timestamp ts = rs.getTimestamp("timestamp");

            return AccessEvent.builder()
                    .eventId(rs.getString("event_id"))
                    .timestamp(ts != null ? ts.toInstant() : null)
                    .traceId(rs.getString("trace_id"))
                    .spanId(rs.getString("span_id"))
                    .userId(rs.getString("user_id"))
                    .userEmail(rs.getString("user_email"))
                    .sourceIp(rs.getString("source_ip"))
                    .clientInfo(rs.getString("client_info"))
                    .resource(resource)
                    .operation(rs.getString("operation"))
                    .status(rs.getString("status"))
                    .rejectionReason(rs.getString("rejection_reason"))
                    .rowsAffected(rs.getObject("rows_affected") != null ? rs.getLong("rows_affected") : null)
                    .executionTimeMs(rs.getObject("execution_time_ms") != null ? rs.getInt("execution_time_ms") : null)
                    .build();
        }
    }
}
