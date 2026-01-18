package io.ducklake.service.model.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;
import lombok.Builder;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

/**
 * Represents detailed error information following gRPC's Any type pattern.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ErrorDetail {
    /**
     * Type URL identifying the error detail type.
     * e.g., "type.googleapis.com/google.rpc.BadRequest"
     */
    private String type;

    /**
     * Field that caused the error (for validation errors).
     */
    private String field;

    /**
     * Description of what went wrong.
     */
    private String description;

    /**
     * SQL state code for database errors.
     */
    private String sqlState;

    /**
     * The original SQL query that failed (if applicable).
     */
    private String query;

    /**
     * Position in query where error occurred (if applicable).
     */
    private Integer position;
}
