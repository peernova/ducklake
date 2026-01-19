package io.ducklake.service.event.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.sql.*;
import java.time.Instant;
import java.util.*;

/**
 * Dashboard analytics endpoints - queries ClickHouse for access log metrics.
 */
@Slf4j
@RestController
@RequestMapping("/api/v1/dashboard")
@RequiredArgsConstructor
@Tag(name = "Dashboard", description = "Access log analytics and dashboard metrics")
public class DashboardController {

    @Value("${clickhouse.url:jdbc:clickhouse://localhost:8123/default?compress=0}")
    private String clickhouseUrl;

    @Value("${clickhouse.enabled:true}")
    private boolean clickhouseEnabled;

    // Helper to build time filter WHERE clause
    private String buildTimeFilter(Long startTime, Long endTime) {
        StringBuilder where = new StringBuilder();
        if (startTime != null) {
            where.append(" AND Timestamp >= fromUnixTimestamp64Milli(").append(startTime).append(")");
        }
        if (endTime != null) {
            where.append(" AND Timestamp <= fromUnixTimestamp64Milli(").append(endTime).append(")");
        }
        return where.toString();
    }

    // ========== Summary Stats ==========

    @GetMapping("/stats")
    @Operation(summary = "Get dashboard stats", description = "Get summary statistics for access logs")
    public ResponseEntity<DashboardStats> getStats(
            @RequestParam(required = false) Long startTime,
            @RequestParam(required = false) Long endTime) {
        if (!clickhouseEnabled) {
            return ResponseEntity.ok(mockStats());
        }

        String timeFilter = buildTimeFilter(startTime, endTime);

        try (Connection conn = DriverManager.getConnection(clickhouseUrl)) {
            DashboardStats stats = new DashboardStats();

            // Total events (in time range)
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT count() FROM otel_logs WHERE 1=1" + timeFilter)) {
                if (rs.next()) stats.setTotalEvents(rs.getLong(1));
            }

            // Unique users (in time range)
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(
                         "SELECT uniq(LogAttributes['user_id']) FROM otel_logs WHERE 1=1" + timeFilter)) {
                if (rs.next()) stats.setUniqueUsers(rs.getLong(1));
            }

            // Events today (still useful as absolute metric)
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(
                         "SELECT count() FROM otel_logs WHERE Timestamp > today()")) {
                if (rs.next()) stats.setEventsToday(rs.getLong(1));
            }

            // Events this hour (still useful as absolute metric)
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(
                         "SELECT count() FROM otel_logs WHERE Timestamp > now() - INTERVAL 1 HOUR")) {
                if (rs.next()) stats.setEventsLastHour(rs.getLong(1));
            }

            return ResponseEntity.ok(stats);
        } catch (SQLException e) {
            log.error("Failed to query ClickHouse stats", e);
            return ResponseEntity.ok(mockStats());
        }
    }

    // ========== Events by User ==========

    @GetMapping("/events-by-user")
    @Operation(summary = "Events per user", description = "Get event counts grouped by user")
    public ResponseEntity<List<UserEventCount>> getEventsByUser(
            @RequestParam(defaultValue = "10") int limit,
            @RequestParam(required = false) Long startTime,
            @RequestParam(required = false) Long endTime) {

        if (!clickhouseEnabled) {
            return ResponseEntity.ok(mockUserEvents());
        }

        String timeFilter = buildTimeFilter(startTime, endTime);

        try (Connection conn = DriverManager.getConnection(clickhouseUrl);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(String.format("""
                SELECT
                    LogAttributes['user_id'] as user_id,
                    count() as event_count,
                    max(Timestamp) as last_activity
                FROM otel_logs
                WHERE LogAttributes['user_id'] != ''%s
                GROUP BY user_id
                ORDER BY event_count DESC
                LIMIT %d
                """, timeFilter, limit))) {

            List<UserEventCount> results = new ArrayList<>();
            while (rs.next()) {
                UserEventCount item = new UserEventCount();
                item.setUserId(rs.getString("user_id"));
                item.setEventCount(rs.getLong("event_count"));
                item.setLastActivity(rs.getTimestamp("last_activity").toInstant());
                results.add(item);
            }
            return ResponseEntity.ok(results);
        } catch (SQLException e) {
            log.error("Failed to query ClickHouse events by user", e);
            return ResponseEntity.ok(mockUserEvents());
        }
    }

    // ========== Events by Resource Type ==========

    @GetMapping("/events-by-resource")
    @Operation(summary = "Events per resource type", description = "Get event counts grouped by resource type")
    public ResponseEntity<List<ResourceEventCount>> getEventsByResource(
            @RequestParam(required = false) Long startTime,
            @RequestParam(required = false) Long endTime) {

        if (!clickhouseEnabled) {
            return ResponseEntity.ok(mockResourceEvents());
        }

        String timeFilter = buildTimeFilter(startTime, endTime);

        try (Connection conn = DriverManager.getConnection(clickhouseUrl);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(String.format("""
                SELECT
                    LogAttributes['resource_type'] as resource_type,
                    count() as event_count,
                    uniq(LogAttributes['user_id']) as unique_users
                FROM otel_logs
                WHERE LogAttributes['resource_type'] != ''%s
                GROUP BY resource_type
                ORDER BY event_count DESC
                """, timeFilter))) {

            List<ResourceEventCount> results = new ArrayList<>();
            while (rs.next()) {
                ResourceEventCount item = new ResourceEventCount();
                item.setResourceType(rs.getString("resource_type"));
                item.setEventCount(rs.getLong("event_count"));
                item.setUniqueUsers(rs.getLong("unique_users"));
                results.add(item);
            }
            return ResponseEntity.ok(results);
        } catch (SQLException e) {
            log.error("Failed to query ClickHouse events by resource", e);
            return ResponseEntity.ok(mockResourceEvents());
        }
    }

    // ========== Events Timeline ==========

    @GetMapping("/timeline")
    @Operation(summary = "Events timeline", description = "Get events over time for charting")
    public ResponseEntity<List<TimelinePoint>> getTimeline(
            @RequestParam(defaultValue = "24") int hours,
            @RequestParam(defaultValue = "hour") String interval) {

        if (!clickhouseEnabled) {
            return ResponseEntity.ok(mockTimeline());
        }

        String timeFunc = "hour".equals(interval) ? "toStartOfHour" : "toStartOfMinute";

        try (Connection conn = DriverManager.getConnection(clickhouseUrl);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(String.format("""
                SELECT
                    %s(Timestamp) as time_bucket,
                    count() as event_count,
                    uniq(LogAttributes['user_id']) as unique_users
                FROM otel_logs
                WHERE Timestamp > now() - INTERVAL %d HOUR
                GROUP BY time_bucket
                ORDER BY time_bucket
                """, timeFunc, hours))) {

            List<TimelinePoint> results = new ArrayList<>();
            while (rs.next()) {
                TimelinePoint point = new TimelinePoint();
                point.setTimestamp(rs.getTimestamp("time_bucket").toInstant());
                point.setEventCount(rs.getLong("event_count"));
                point.setUniqueUsers(rs.getLong("unique_users"));
                results.add(point);
            }
            return ResponseEntity.ok(results);
        } catch (SQLException e) {
            log.error("Failed to query ClickHouse timeline", e);
            return ResponseEntity.ok(mockTimeline());
        }
    }

    // ========== Recent Events ==========

    @GetMapping("/recent")
    @Operation(summary = "Recent events", description = "Get most recent access events with filtering and pagination. " +
            "Supports dynamic attribute filters via query params like ?filter.user_id=alice&filter.catalog_name=demo")
    public ResponseEntity<RecentEventsResponse> getRecentEvents(
            @RequestParam(defaultValue = "20") int limit,
            @RequestParam(defaultValue = "0") int offset,
            @RequestParam(required = false) Long startTime,
            @RequestParam(required = false) Long endTime,
            @RequestParam Map<String, String> allParams) {

        if (!clickhouseEnabled) {
            return ResponseEntity.ok(new RecentEventsResponse(mockRecentEvents(), 0L));
        }

        try (Connection conn = DriverManager.getConnection(clickhouseUrl)) {
            // Build WHERE clause using epoch milliseconds
            StringBuilder where = new StringBuilder("WHERE 1=1");
            if (startTime != null) {
                where.append(" AND Timestamp >= fromUnixTimestamp64Milli(").append(startTime).append(")");
            }
            if (endTime != null) {
                where.append(" AND Timestamp <= fromUnixTimestamp64Milli(").append(endTime).append(")");
            }

            // Dynamic attribute filters: params starting with "filter." e.g. filter.user_id=alice
            for (Map.Entry<String, String> entry : allParams.entrySet()) {
                if (entry.getKey().startsWith("filter.") && entry.getValue() != null && !entry.getValue().isEmpty()) {
                    String attrName = entry.getKey().substring(7); // Remove "filter." prefix
                    String attrValue = entry.getValue().replace("'", "''");
                    where.append(" AND LogAttributes['").append(attrName.replace("'", "''"))
                         .append("'] = '").append(attrValue).append("'");
                }
            }

            // Get total count
            long totalCount = 0;
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT count() FROM otel_logs " + where)) {
                if (rs.next()) totalCount = rs.getLong(1);
            }

            // Get paginated results
            String query = String.format("""
                SELECT
                    Timestamp,
                    LogAttributes['event_id'] as event_id,
                    LogAttributes['user_id'] as user_id,
                    LogAttributes['user_email'] as user_email,
                    LogAttributes['resource_type'] as resource_type,
                    LogAttributes['resource_id'] as resource_id,
                    LogAttributes['operation'] as operation,
                    LogAttributes['status'] as status,
                    TraceId as trace_id,
                    LogAttributes
                FROM otel_logs
                %s
                ORDER BY Timestamp DESC
                LIMIT %d OFFSET %d
                """, where, limit, offset);

            List<RecentEvent> results = new ArrayList<>();
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(query)) {
                while (rs.next()) {
                    RecentEvent event = new RecentEvent();
                    event.setTimestamp(rs.getTimestamp("Timestamp").toInstant());
                    event.setEventId(rs.getString("event_id"));
                    event.setUserId(rs.getString("user_id"));
                    event.setUserEmail(rs.getString("user_email"));
                    event.setResourceType(rs.getString("resource_type"));
                    event.setResourceId(rs.getString("resource_id"));
                    event.setOperation(rs.getString("operation"));
                    event.setStatus(rs.getString("status"));
                    event.setTraceId(rs.getString("trace_id"));
                    // Parse LogAttributes Map
                    @SuppressWarnings("unchecked")
                    Map<String, String> logAttrs = (Map<String, String>) rs.getObject("LogAttributes");
                    event.setLogAttributes(logAttrs != null ? logAttrs : Collections.emptyMap());
                    results.add(event);
                }
            }
            return ResponseEntity.ok(new RecentEventsResponse(results, totalCount));
        } catch (SQLException e) {
            log.error("Failed to query ClickHouse recent events", e);
            return ResponseEntity.ok(new RecentEventsResponse(mockRecentEvents(), 0L));
        }
    }

    // ========== Filter Options ==========

    @GetMapping("/filter-options")
    @Operation(summary = "Get cascading filter options",
               description = "Returns filter options that narrow down based on other selected filters. " +
                           "Pass current selections as filter.attr_name=value to get refined options. " +
                           "Additional attributes are discovered dynamically from the data.")
    public ResponseEntity<Map<String, List<String>>> getFilterOptions(
            @RequestParam Map<String, String> allParams) {
        if (!clickhouseEnabled) {
            return ResponseEntity.ok(Collections.emptyMap());
        }

        try (Connection conn = DriverManager.getConnection(clickhouseUrl)) {
            Map<String, List<String>> filters = new LinkedHashMap<>();

            // Extract current filter selections (params starting with "filter.")
            Map<String, String> currentFilters = new LinkedHashMap<>();
            for (Map.Entry<String, String> entry : allParams.entrySet()) {
                if (entry.getKey().startsWith("filter.") && entry.getValue() != null && !entry.getValue().isEmpty()) {
                    currentFilters.put(entry.getKey().substring(7), entry.getValue());
                }
            }

            // Core filters - always shown
            List<String> coreKeys = List.of("user_id", "resource_type", "status", "operation");
            for (String key : coreKeys) {
                List<String> values = getDistinctValuesWithFilters(conn, key, currentFilters);
                if (!values.isEmpty()) {
                    filters.put(key, values);
                }
            }

            // Dynamic filters - discover additional attributes based on current filters
            Set<String> excludedKeys = Set.of(
                "event_id", "timestamp", "trace_id", "span_id", "trace_flags",
                "execution_time_ms", "rows_affected", "source_ip", "client_info",
                "logback.marker", "event_type", "rejection_reason",
                // Exclude core keys (already shown above)
                "user_id", "resource_type", "status", "operation"
            );

            // Discover additional attributes from data matching current filters
            List<String> additionalKeys = discoverAttributes(conn, currentFilters, excludedKeys);
            for (String key : additionalKeys) {
                List<String> values = getDistinctValuesWithFilters(conn, key, currentFilters);
                if (!values.isEmpty() && values.size() <= 50) { // Only show if reasonable number of options
                    filters.put(key, values);
                }
            }

            return ResponseEntity.ok(filters);
        } catch (SQLException e) {
            log.error("Failed to query filter options", e);
            return ResponseEntity.ok(Collections.emptyMap());
        }
    }

    private List<String> discoverAttributes(Connection conn, Map<String, String> currentFilters, Set<String> excludedKeys) throws SQLException {
        List<String> attributes = new ArrayList<>();

        // Build WHERE clause from current filters
        StringBuilder where = new StringBuilder("WHERE 1=1");
        for (Map.Entry<String, String> filter : currentFilters.entrySet()) {
            where.append(" AND LogAttributes['").append(filter.getKey().replace("'", "''"))
                 .append("'] = '").append(filter.getValue().replace("'", "''")).append("'");
        }

        // Discover all attribute keys in matching rows
        String query = "SELECT DISTINCT arrayJoin(mapKeys(LogAttributes)) as attr_key FROM otel_logs " + where + " ORDER BY attr_key";
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            while (rs.next()) {
                String key = rs.getString("attr_key");
                if (key != null && !key.isEmpty() && !key.startsWith("thread.") && !excludedKeys.contains(key)) {
                    attributes.add(key);
                }
            }
        }
        return attributes;
    }

    private List<String> getDistinctValuesWithFilters(Connection conn, String attrKey, Map<String, String> currentFilters) throws SQLException {
        List<String> values = new ArrayList<>();

        // Build WHERE clause from OTHER filters (exclude the current attribute)
        StringBuilder where = new StringBuilder("WHERE LogAttributes['").append(attrKey.replace("'", "''")).append("'] != ''");
        for (Map.Entry<String, String> filter : currentFilters.entrySet()) {
            if (!filter.getKey().equals(attrKey)) { // Don't filter by self
                where.append(" AND LogAttributes['").append(filter.getKey().replace("'", "''"))
                     .append("'] = '").append(filter.getValue().replace("'", "''")).append("'");
            }
        }

        String query = String.format(
            "SELECT DISTINCT LogAttributes['%s'] as v FROM otel_logs %s ORDER BY v LIMIT 100",
            attrKey.replace("'", "''"), where);

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            while (rs.next()) {
                values.add(rs.getString("v"));
            }
        }
        return values;
    }

    // ========== Operations Breakdown ==========

    @GetMapping("/operations")
    @Operation(summary = "Operations breakdown", description = "Get event counts by operation type")
    public ResponseEntity<List<OperationCount>> getOperations(
            @RequestParam(required = false) Long startTime,
            @RequestParam(required = false) Long endTime) {

        if (!clickhouseEnabled) {
            return ResponseEntity.ok(mockOperations());
        }

        String timeFilter = buildTimeFilter(startTime, endTime);

        try (Connection conn = DriverManager.getConnection(clickhouseUrl);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(String.format("""
                SELECT
                    LogAttributes['operation'] as operation,
                    LogAttributes['status'] as status,
                    count() as event_count
                FROM otel_logs
                WHERE LogAttributes['operation'] != ''%s
                GROUP BY operation, status
                ORDER BY event_count DESC
                """, timeFilter))) {

            List<OperationCount> results = new ArrayList<>();
            while (rs.next()) {
                OperationCount item = new OperationCount();
                item.setOperation(rs.getString("operation"));
                item.setStatus(rs.getString("status"));
                item.setEventCount(rs.getLong("event_count"));
                results.add(item);
            }
            return ResponseEntity.ok(results);
        } catch (SQLException e) {
            log.error("Failed to query ClickHouse operations", e);
            return ResponseEntity.ok(mockOperations());
        }
    }

    // ========== Mock Data for when ClickHouse is unavailable ==========

    private DashboardStats mockStats() {
        DashboardStats stats = new DashboardStats();
        stats.setTotalEvents(0L);
        stats.setUniqueUsers(0L);
        stats.setEventsToday(0L);
        stats.setEventsLastHour(0L);
        return stats;
    }

    private List<UserEventCount> mockUserEvents() {
        return Collections.emptyList();
    }

    private List<ResourceEventCount> mockResourceEvents() {
        return Collections.emptyList();
    }

    private List<TimelinePoint> mockTimeline() {
        return Collections.emptyList();
    }

    private List<RecentEvent> mockRecentEvents() {
        return Collections.emptyList();
    }

    private List<OperationCount> mockOperations() {
        return Collections.emptyList();
    }

    // ========== Response DTOs ==========

    @Data
    public static class DashboardStats {
        private Long totalEvents;
        private Long uniqueUsers;
        private Long eventsToday;
        private Long eventsLastHour;
    }

    @Data
    public static class UserEventCount {
        private String userId;
        private Long eventCount;
        private Instant lastActivity;
    }

    @Data
    public static class ResourceEventCount {
        private String resourceType;
        private Long eventCount;
        private Long uniqueUsers;
    }

    @Data
    public static class TimelinePoint {
        private Instant timestamp;
        private Long eventCount;
        private Long uniqueUsers;
    }

    @Data
    public static class RecentEvent {
        private Instant timestamp;
        private String eventId;
        private String userId;
        private String userEmail;
        private String resourceType;
        private String resourceId;
        private String operation;
        private String status;
        private String traceId;
        private Map<String, String> logAttributes;
    }

    @Data
    public static class OperationCount {
        private String operation;
        private String status;
        private Long eventCount;
    }

    @Data
    public static class RecentEventsResponse {
        private List<RecentEvent> events;
        private Long totalCount;

        public RecentEventsResponse(List<RecentEvent> events, Long totalCount) {
            this.events = events;
            this.totalCount = totalCount;
        }
    }

}
