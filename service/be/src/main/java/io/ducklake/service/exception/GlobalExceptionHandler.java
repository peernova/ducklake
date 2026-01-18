package io.ducklake.service.exception;

import io.ducklake.service.model.dto.ErrorCode;
import io.ducklake.service.model.dto.ErrorDetail;
import io.ducklake.service.model.dto.ErrorResponse;
import io.ducklake.service.tracing.TraceContext;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.ConstraintViolationException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.HttpMediaTypeNotSupportedException;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.servlet.NoHandlerFoundException;

import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

@RestControllerAdvice
@Slf4j
@RequiredArgsConstructor
public class GlobalExceptionHandler {

    private final TraceContext traceContext;

    @ExceptionHandler(DuckLakeException.class)
    public ResponseEntity<ErrorResponse> handleDuckLakeException(DuckLakeException ex, HttpServletRequest request) {
        log.error("DuckLake error: {} - {} [traceId={}]", ex.getErrorCode(), ex.getMessage(), traceContext.getTraceId());

        ErrorResponse response = buildResponse(ex.getErrorCode(), ex.getMessage(), ex.getDetails(), request);
        return ResponseEntity.status(ex.getErrorCode().getHttpStatus()).body(response);
    }

    @ExceptionHandler(SQLException.class)
    public ResponseEntity<ErrorResponse> handleSQLException(SQLException ex, HttpServletRequest request) {
        log.error("SQL error: {} - {} [traceId={}]", ex.getSQLState(), ex.getMessage(), traceContext.getTraceId());

        ErrorCode errorCode = mapSqlError(ex);
        ErrorDetail detail = ErrorDetail.builder()
                .type("type.googleapis.com/ducklake.SqlError")
                .sqlState(ex.getSQLState())
                .description(ex.getMessage())
                .build();

        ErrorResponse response = buildResponse(errorCode, sanitize(ex.getMessage()), Collections.singletonList(detail), request);
        return ResponseEntity.status(errorCode.getHttpStatus()).body(response);
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ErrorResponse> handleValidation(MethodArgumentNotValidException ex, HttpServletRequest request) {
        log.warn("Validation error [traceId={}]: {}", traceContext.getTraceId(), ex.getMessage());

        List<ErrorDetail> details = ex.getBindingResult().getFieldErrors().stream()
                .map(e -> ErrorDetail.builder()
                        .type("type.googleapis.com/google.rpc.BadRequest.FieldViolation")
                        .field(e.getField())
                        .description(e.getDefaultMessage())
                        .build())
                .collect(Collectors.toList());

        ErrorResponse response = buildResponse(ErrorCode.INVALID_ARGUMENT, "Validation failed", details, request);
        return ResponseEntity.status(400).body(response);
    }

    @ExceptionHandler(ConstraintViolationException.class)
    public ResponseEntity<ErrorResponse> handleConstraintViolation(ConstraintViolationException ex, HttpServletRequest request) {
        log.warn("Constraint violation [traceId={}]: {}", traceContext.getTraceId(), ex.getMessage());

        List<ErrorDetail> details = ex.getConstraintViolations().stream()
                .map(v -> ErrorDetail.builder()
                        .type("type.googleapis.com/google.rpc.BadRequest.FieldViolation")
                        .field(v.getPropertyPath().toString())
                        .description(v.getMessage())
                        .build())
                .collect(Collectors.toList());

        ErrorResponse response = buildResponse(ErrorCode.INVALID_ARGUMENT, "Constraint violation", details, request);
        return ResponseEntity.status(400).body(response);
    }

    @ExceptionHandler(MissingServletRequestParameterException.class)
    public ResponseEntity<ErrorResponse> handleMissingParam(MissingServletRequestParameterException ex, HttpServletRequest request) {
        log.warn("Missing parameter [traceId={}]: {}", traceContext.getTraceId(), ex.getParameterName());

        ErrorDetail detail = ErrorDetail.builder()
                .type("type.googleapis.com/google.rpc.BadRequest.FieldViolation")
                .field(ex.getParameterName())
                .description("Required parameter '" + ex.getParameterName() + "' is missing")
                .build();

        ErrorResponse response = buildResponse(ErrorCode.INVALID_ARGUMENT, "Missing required parameter", Collections.singletonList(detail), request);
        return ResponseEntity.status(400).body(response);
    }

    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    public ResponseEntity<ErrorResponse> handleTypeMismatch(MethodArgumentTypeMismatchException ex, HttpServletRequest request) {
        log.warn("Type mismatch [traceId={}]: {} for parameter {}", traceContext.getTraceId(), ex.getValue(), ex.getName());

        String expectedType = ex.getRequiredType() != null ? ex.getRequiredType().getSimpleName() : "unknown";
        ErrorDetail detail = ErrorDetail.builder()
                .type("type.googleapis.com/google.rpc.BadRequest.FieldViolation")
                .field(ex.getName())
                .description("Invalid value '" + ex.getValue() + "'. Expected type: " + expectedType)
                .build();

        ErrorResponse response = buildResponse(ErrorCode.INVALID_ARGUMENT, "Invalid parameter type", Collections.singletonList(detail), request);
        return ResponseEntity.status(400).body(response);
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ErrorResponse> handleUnreadable(HttpMessageNotReadableException ex, HttpServletRequest request) {
        log.warn("Unreadable message [traceId={}]: {}", traceContext.getTraceId(), ex.getMessage());

        ErrorDetail detail = ErrorDetail.builder()
                .type("type.googleapis.com/google.rpc.BadRequest")
                .description("Malformed request body. Please check JSON syntax.")
                .build();

        ErrorResponse response = buildResponse(ErrorCode.INVALID_ARGUMENT, "Invalid request body", Collections.singletonList(detail), request);
        return ResponseEntity.status(400).body(response);
    }

    @ExceptionHandler(HttpRequestMethodNotSupportedException.class)
    public ResponseEntity<ErrorResponse> handleMethodNotSupported(HttpRequestMethodNotSupportedException ex, HttpServletRequest request) {
        log.warn("Method not supported [traceId={}]: {} for {}", traceContext.getTraceId(), ex.getMethod(), request.getRequestURI());

        String supported = ex.getSupportedHttpMethods() != null
                ? ex.getSupportedHttpMethods().stream().map(Object::toString).collect(Collectors.joining(", "))
                : "unknown";

        ErrorDetail detail = ErrorDetail.builder()
                .type("type.googleapis.com/google.rpc.ErrorInfo")
                .description("Method " + ex.getMethod() + " not supported. Supported methods: " + supported)
                .build();

        ErrorResponse response = buildResponse(ErrorCode.UNIMPLEMENTED, "Method not allowed", Collections.singletonList(detail), request);
        return ResponseEntity.status(405).body(response);
    }

    @ExceptionHandler(HttpMediaTypeNotSupportedException.class)
    public ResponseEntity<ErrorResponse> handleMediaTypeNotSupported(HttpMediaTypeNotSupportedException ex, HttpServletRequest request) {
        log.warn("Media type not supported [traceId={}]: {}", traceContext.getTraceId(), ex.getContentType());

        ErrorDetail detail = ErrorDetail.builder()
                .type("type.googleapis.com/google.rpc.ErrorInfo")
                .description("Media type " + ex.getContentType() + " not supported. Use application/json.")
                .build();

        ErrorResponse response = buildResponse(ErrorCode.INVALID_ARGUMENT, "Unsupported media type", Collections.singletonList(detail), request);
        return ResponseEntity.status(415).body(response);
    }

    @ExceptionHandler(NoHandlerFoundException.class)
    public ResponseEntity<ErrorResponse> handleNotFound(NoHandlerFoundException ex, HttpServletRequest request) {
        log.warn("Endpoint not found [traceId={}]: {} {}", traceContext.getTraceId(), ex.getHttpMethod(), ex.getRequestURL());

        ErrorResponse response = buildResponse(ErrorCode.NOT_FOUND, "Endpoint not found: " + ex.getRequestURL(), null, request);
        return ResponseEntity.status(404).body(response);
    }

    @ExceptionHandler({TimeoutException.class, SQLTimeoutException.class})
    public ResponseEntity<ErrorResponse> handleTimeout(Exception ex, HttpServletRequest request) {
        log.error("Timeout error [traceId={}]: {}", traceContext.getTraceId(), ex.getMessage());

        ErrorDetail detail = ErrorDetail.builder()
                .type("type.googleapis.com/google.rpc.ErrorInfo")
                .description("Operation timed out. Please try again or reduce query complexity.")
                .build();

        ErrorResponse response = buildResponse(ErrorCode.DEADLINE_EXCEEDED, "Request timeout", Collections.singletonList(detail), request);
        return ResponseEntity.status(504).body(response);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<ErrorResponse> handleIllegalArgument(IllegalArgumentException ex, HttpServletRequest request) {
        log.warn("Illegal argument [traceId={}]: {}", traceContext.getTraceId(), ex.getMessage());

        ErrorResponse response = buildResponse(ErrorCode.INVALID_ARGUMENT, ex.getMessage(), null, request);
        return ResponseEntity.status(400).body(response);
    }

    @ExceptionHandler(IllegalStateException.class)
    public ResponseEntity<ErrorResponse> handleIllegalState(IllegalStateException ex, HttpServletRequest request) {
        log.error("Illegal state [traceId={}]: {}", traceContext.getTraceId(), ex.getMessage());

        ErrorResponse response = buildResponse(ErrorCode.FAILED_PRECONDITION, ex.getMessage(), null, request);
        return ResponseEntity.status(400).body(response);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ErrorResponse> handleGeneric(Exception ex, HttpServletRequest request) {
        log.error("Unexpected error [traceId={}]", traceContext.getTraceId(), ex);

        List<ErrorDetail> details = Collections.singletonList(
                ErrorDetail.builder()
                        .type("type.googleapis.com/google.rpc.DebugInfo")
                        .description(ex.getClass().getSimpleName() + ": " + sanitize(ex.getMessage()))
                        .build());

        ErrorResponse response = buildResponse(ErrorCode.INTERNAL, "Internal error", details, request);
        return ResponseEntity.status(500).body(response);
    }

    private ErrorResponse buildResponse(ErrorCode code, String message, List<ErrorDetail> details, HttpServletRequest request) {
        return ErrorResponse.builder()
                .code(code.getGrpcCode())
                .status(code.name())
                .message(message)
                .details(details)
                .timestamp(Instant.now())
                .path(request.getRequestURI())
                .traceId(traceContext.getTraceId())
                .spanId(traceContext.getSpanId())
                .build();
    }

    private ErrorCode mapSqlError(SQLException ex) {
        String msg = ex.getMessage() != null ? ex.getMessage().toLowerCase() : "";
        if (msg.contains("not found") || msg.contains("does not exist")) return ErrorCode.NOT_FOUND;
        if (msg.contains("already exists") || msg.contains("duplicate")) return ErrorCode.ALREADY_EXISTS;
        if (msg.contains("syntax error") || msg.contains("parser error")) return ErrorCode.INVALID_ARGUMENT;
        if (msg.contains("binder error") || msg.contains("catalog error")) return ErrorCode.INVALID_ARGUMENT;
        if (msg.contains("timeout")) return ErrorCode.DEADLINE_EXCEEDED;
        return ErrorCode.INTERNAL;
    }

    private String sanitize(String msg) {
        if (msg == null) return "Unknown error";
        return msg.length() > 300 ? msg.substring(0, 300) + "..." : msg;
    }
}
