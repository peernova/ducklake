package io.ducklake.service.model.dto;

/**
 * gRPC-style status codes for error responses.
 * Based on google.rpc.Code enumeration.
 */
public enum ErrorCode {
    OK(0, 200),
    CANCELLED(1, 499),
    UNKNOWN(2, 500),
    INVALID_ARGUMENT(3, 400),
    DEADLINE_EXCEEDED(4, 504),
    NOT_FOUND(5, 404),
    ALREADY_EXISTS(6, 409),
    PERMISSION_DENIED(7, 403),
    RESOURCE_EXHAUSTED(8, 429),
    FAILED_PRECONDITION(9, 400),
    ABORTED(10, 409),
    OUT_OF_RANGE(11, 400),
    UNIMPLEMENTED(12, 501),
    INTERNAL(13, 500),
    UNAVAILABLE(14, 503),
    DATA_LOSS(15, 500),
    UNAUTHENTICATED(16, 401);

    private final int grpcCode;
    private final int httpStatus;

    ErrorCode(int grpcCode, int httpStatus) {
        this.grpcCode = grpcCode;
        this.httpStatus = httpStatus;
    }

    public int getGrpcCode() {
        return grpcCode;
    }

    public int getHttpStatus() {
        return httpStatus;
    }
}
