package io.ducklake.service.event.model;

import lombok.Builder;
import lombok.Data;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * Represents an access event for auditing and activity tracking.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AccessEvent {

    /**
     * Unique event identifier.
     */
    private String eventId;

    /**
     * When the event occurred.
     */
    private Instant timestamp;

    // ---- OpenTelemetry correlation ----

    /**
     * OpenTelemetry trace ID for distributed tracing.
     */
    private String traceId;

    /**
     * OpenTelemetry span ID.
     */
    private String spanId;

    // ---- Who ----

    /**
     * User identifier.
     */
    private String userId;

    /**
     * User email (denormalized for convenience).
     */
    private String userEmail;

    /**
     * Source IP address.
     */
    private String sourceIp;

    /**
     * Client information (user agent, app name, etc.).
     */
    private String clientInfo;

    // ---- What ----

    /**
     * The resource being accessed.
     */
    private Resource resource;

    /**
     * Operation performed: view, edit, execute, delete, export, etc.
     */
    private String operation;

    // ---- Result ----

    /**
     * Status: success, rejected, error.
     */
    private String status;

    /**
     * Reason for rejection (if status is rejected).
     */
    private String rejectionReason;

    /**
     * Number of rows affected/returned (for queries).
     */
    private Long rowsAffected;

    /**
     * Execution time in milliseconds.
     */
    private Integer executionTimeMs;
}
