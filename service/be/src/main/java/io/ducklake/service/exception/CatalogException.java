package io.ducklake.service.exception;

import io.ducklake.service.model.dto.ErrorCode;
import io.ducklake.service.model.dto.ErrorDetail;

import java.util.Collections;

/**
 * Exception for catalog-related errors.
 */
public class CatalogException extends DuckLakeException {

    public CatalogException(String message) {
        super(ErrorCode.INVALID_ARGUMENT, message);
    }

    public CatalogException(String message, Throwable cause) {
        super(ErrorCode.INVALID_ARGUMENT, message, cause);
    }

    public static CatalogException notFound(String catalogId) {
        return new CatalogException(ErrorCode.NOT_FOUND,
                "Catalog not found: " + catalogId,
                Collections.singletonList(
                        ErrorDetail.builder()
                                .type("type.googleapis.com/ducklake.CatalogError")
                                .field("catalogId")
                                .description("No catalog exists with ID: " + catalogId)
                                .build()
                ));
    }

    public static CatalogException alreadyExists(String catalogId) {
        return new CatalogException(ErrorCode.ALREADY_EXISTS,
                "Catalog already exists: " + catalogId,
                Collections.singletonList(
                        ErrorDetail.builder()
                                .type("type.googleapis.com/ducklake.CatalogError")
                                .field("catalogId")
                                .description("A catalog with ID '" + catalogId + "' already exists")
                                .build()
                ));
    }

    public static CatalogException attachFailed(String catalogId, String reason) {
        return new CatalogException(ErrorCode.INTERNAL,
                "Failed to attach catalog: " + catalogId,
                Collections.singletonList(
                        ErrorDetail.builder()
                                .type("type.googleapis.com/ducklake.CatalogError")
                                .field("catalogId")
                                .description(reason)
                                .build()
                ));
    }

    public static CatalogException invalidCatalogId(String catalogId, String reason) {
        return new CatalogException(ErrorCode.INVALID_ARGUMENT,
                "Invalid catalog ID: " + reason,
                Collections.singletonList(
                        ErrorDetail.builder()
                                .type("type.googleapis.com/ducklake.CatalogError")
                                .field("catalogId")
                                .description(reason)
                                .build()
                ));
    }

    public static CatalogException invalidArgument(String field, String reason) {
        return new CatalogException(ErrorCode.INVALID_ARGUMENT,
                "Invalid argument: " + reason,
                Collections.singletonList(
                        ErrorDetail.builder()
                                .type("type.googleapis.com/ducklake.CatalogError")
                                .field(field)
                                .description(reason)
                                .build()
                ));
    }

    private CatalogException(ErrorCode code, String message, java.util.List<ErrorDetail> details) {
        super(code, message, details);
    }
}
