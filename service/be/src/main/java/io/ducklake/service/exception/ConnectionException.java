package io.ducklake.service.exception;

import io.ducklake.service.model.dto.ErrorCode;
import io.ducklake.service.model.dto.ErrorDetail;

import java.util.Collections;

/**
 * Exception for DuckDB connection pool errors.
 */
public class ConnectionException extends DuckLakeException {

    public ConnectionException(String message) {
        super(ErrorCode.UNAVAILABLE, message);
    }

    public ConnectionException(String message, Throwable cause) {
        super(ErrorCode.UNAVAILABLE, message, cause);
    }

    public static ConnectionException timeout() {
        return new ConnectionException(ErrorCode.DEADLINE_EXCEEDED,
                "Connection pool timeout",
                Collections.singletonList(
                        ErrorDetail.builder()
                                .type("type.googleapis.com/ducklake.ConnectionError")
                                .description("Timed out waiting for available DuckDB connection")
                                .build()
                ));
    }

    public static ConnectionException poolExhausted() {
        return new ConnectionException(ErrorCode.RESOURCE_EXHAUSTED,
                "Connection pool exhausted",
                Collections.singletonList(
                        ErrorDetail.builder()
                                .type("type.googleapis.com/ducklake.ConnectionError")
                                .description("All connections in the pool are in use")
                                .build()
                ));
    }

    public static ConnectionException notInitialized() {
        return new ConnectionException(ErrorCode.UNAVAILABLE,
                "Service not initialized",
                Collections.singletonList(
                        ErrorDetail.builder()
                                .type("type.googleapis.com/ducklake.ConnectionError")
                                .description("DuckDB connection pool has not been initialized")
                                .build()
                ));
    }

    private ConnectionException(ErrorCode code, String message, java.util.List<ErrorDetail> details) {
        super(code, message, details);
    }
}
