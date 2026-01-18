package io.ducklake.service.exception;

import io.ducklake.service.model.dto.ErrorCode;
import io.ducklake.service.model.dto.ErrorDetail;
import lombok.Getter;

import java.util.List;

/**
 * Base exception for DuckLake service errors.
 */
@Getter
public class DuckLakeException extends RuntimeException {

    private final ErrorCode errorCode;
    private final List<ErrorDetail> details;

    public DuckLakeException(ErrorCode errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
        this.details = null;
    }

    public DuckLakeException(ErrorCode errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
        this.details = null;
    }

    public DuckLakeException(ErrorCode errorCode, String message, List<ErrorDetail> details) {
        super(message);
        this.errorCode = errorCode;
        this.details = details;
    }

    public DuckLakeException(ErrorCode errorCode, String message, List<ErrorDetail> details, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
        this.details = details;
    }
}
