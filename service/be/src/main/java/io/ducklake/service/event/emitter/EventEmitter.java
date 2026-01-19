package io.ducklake.service.event.emitter;

import io.ducklake.service.event.model.AccessEvent;
import io.ducklake.service.event.model.Resource;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * Emits audit events via SLF4J with AUDIT marker.
 *
 * Best practice: Use OTel Java Agent for log capture.
 * The agent auto-instruments SLF4J and exports to collector.
 *
 * Run with:
 *   java -javaagent:opentelemetry-javaagent.jar \
 *        -Dotel.service.name=ducklake-service \
 *        -Dotel.exporter.otlp.endpoint=http://localhost:4317 \
 *        -jar app.jar
 *
 * Flow:
 *   log.info(AUDIT, ...) → OTel Agent → Collector → ClickHouse/Postgres
 */
@Component
@Slf4j
public class EventEmitter {

    // Standard SLF4J Marker - OTel agent captures this
    private static final Marker AUDIT = MarkerFactory.getMarker("AUDIT");

    /**
     * Emit an audit event.
     */
    public void emit(AccessEvent event) {
        if (event.getEventId() == null) {
            event.setEventId(UUID.randomUUID().toString());
        }
        if (event.getTimestamp() == null) {
            event.setTimestamp(Instant.now());
        }

        try {
            // Set MDC - OTel agent captures these as log attributes
            MDC.put("event_type", "ACCESS_EVENT");
            MDC.put("event_id", event.getEventId());
            MDC.put("timestamp", event.getTimestamp().toString());

            putIfNotNull("user_id", event.getUserId());
            putIfNotNull("user_email", event.getUserEmail());
            putIfNotNull("source_ip", event.getSourceIp());
            putIfNotNull("client_info", event.getClientInfo());
            putIfNotNull("trace_id", event.getTraceId());
            putIfNotNull("span_id", event.getSpanId());
            putIfNotNull("operation", event.getOperation());
            putIfNotNull("status", event.getStatus());
            putIfNotNull("rejection_reason", event.getRejectionReason());

            if (event.getRowsAffected() != null) {
                MDC.put("rows_affected", event.getRowsAffected().toString());
            }
            if (event.getExecutionTimeMs() != null) {
                MDC.put("execution_time_ms", event.getExecutionTimeMs().toString());
            }

            Resource r = event.getResource();
            if (r != null) {
                putIfNotNull("resource_type", r.getType());
                putIfNotNull("resource_id", r.getId());
                putIfNotNull("resource_name", r.getName());
                if (r.getPath() != null && !r.getPath().isEmpty()) {
                    MDC.put("resource_path", toJson(r.getPath()));
                }
            }

            // Log with AUDIT marker - OTel agent captures and exports this
            log.info(AUDIT, "{} | {} | {} | {} | {}",
                    event.getOperation(),
                    r != null ? r.getType() : "-",
                    r != null ? r.getId() : "-",
                    event.getUserId(),
                    event.getStatus());

        } catch (Exception e) {
            log.error("Failed to emit audit event: {}", e.getMessage(), e);
        } finally {
            MDC.clear();
        }
    }

    // ========== Convenience methods ==========

    public void emitView(String userId, Resource resource) {
        emit(AccessEvent.builder()
                .userId(userId)
                .resource(resource)
                .operation("view")
                .status("success")
                .build());
    }

    public void emitQuery(String userId, Resource resource, int executionTimeMs, long rowsReturned) {
        emit(AccessEvent.builder()
                .userId(userId)
                .resource(resource)
                .operation("execute")
                .status("success")
                .executionTimeMs(executionTimeMs)
                .rowsAffected(rowsReturned)
                .build());
    }

    public void emitRejection(String userId, Resource resource, String operation, String reason) {
        emit(AccessEvent.builder()
                .userId(userId)
                .resource(resource)
                .operation(operation)
                .status("rejected")
                .rejectionReason(reason)
                .build());
    }

    public void emitError(String userId, Resource resource, String operation, String error) {
        emit(AccessEvent.builder()
                .userId(userId)
                .resource(resource)
                .operation(operation)
                .status("error")
                .rejectionReason(error)
                .build());
    }

    private void putIfNotNull(String key, String value) {
        if (value != null && !value.isEmpty()) {
            MDC.put(key, value);
        }
    }

    private String toJson(Map<String, String> map) {
        if (map == null || map.isEmpty()) return "{}";
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, String> e : map.entrySet()) {
            if (!first) sb.append(",");
            sb.append("\"").append(escape(e.getKey())).append("\":\"").append(escape(e.getValue())).append("\"");
            first = false;
        }
        sb.append("}");
        return sb.toString();
    }

    private String escape(String s) {
        if (s == null) return "";
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
