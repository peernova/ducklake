package io.ducklake.service.model.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Data;
import lombok.Builder;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.List;

/**
 * gRPC-style error response following google.rpc.Status pattern.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
@Schema(description = "Error response following gRPC Status convention")
public class ErrorResponse {

    /**
     * gRPC status code (0-16).
     */
    @Schema(description = "gRPC status code", example = "3")
    private Integer code;

    /**
     * Error code name for readability.
     */
    @Schema(description = "Error code name", example = "INVALID_ARGUMENT")
    private String status;

    /**
     * A developer-facing error message in English.
     */
    @Schema(description = "Error message", example = "Invalid SQL syntax")
    private String message;

    /**
     * Additional error details.
     */
    @Schema(description = "List of error details")
    private List<ErrorDetail> details;

    /**
     * Timestamp when error occurred.
     */
    @Schema(description = "Timestamp of error occurrence")
    private Instant timestamp;

    /**
     * Request path that caused the error.
     */
    @Schema(description = "Request path", example = "/api/v1/query/execute")
    private String path;

    /**
     * OpenTelemetry trace ID for distributed tracing.
     */
    @Schema(description = "Trace ID for distributed tracing", example = "4bf92f3577b34da6a3ce929d0e0e4736")
    private String traceId;

    /**
     * OpenTelemetry span ID.
     */
    @Schema(description = "Span ID", example = "00f067aa0ba902b7")
    private String spanId;

    /**
     * Create error response from ErrorCode and message.
     */
    public static ErrorResponse of(ErrorCode errorCode, String message) {
        return ErrorResponse.builder()
                .code(errorCode.getGrpcCode())
                .status(errorCode.name())
                .message(message)
                .timestamp(Instant.now())
                .build();
    }

    /**
     * Create error response with details.
     */
    public static ErrorResponse of(ErrorCode errorCode, String message, List<ErrorDetail> details) {
        return ErrorResponse.builder()
                .code(errorCode.getGrpcCode())
                .status(errorCode.name())
                .message(message)
                .details(details)
                .timestamp(Instant.now())
                .build();
    }
}
