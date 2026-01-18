package io.ducklake.service.exception;

import io.ducklake.service.model.dto.ErrorCode;
import io.ducklake.service.model.dto.ErrorDetail;
import lombok.Getter;

import java.util.Collections;

/**
 * Exception for SQL query execution errors.
 */
@Getter
public class QueryException extends DuckLakeException {

    private final String query;
    private final String sqlState;

    public QueryException(String message, String query) {
        super(ErrorCode.INVALID_ARGUMENT, message, buildDetails(message, query, null));
        this.query = query;
        this.sqlState = null;
    }

    public QueryException(String message, String query, String sqlState) {
        super(ErrorCode.INVALID_ARGUMENT, message, buildDetails(message, query, sqlState));
        this.query = query;
        this.sqlState = sqlState;
    }

    public QueryException(String message, String query, Throwable cause) {
        super(ErrorCode.INVALID_ARGUMENT, message, buildDetails(message, query, null), cause);
        this.query = query;
        this.sqlState = null;
    }

    private static java.util.List<ErrorDetail> buildDetails(String message, String query, String sqlState) {
        return Collections.singletonList(
                ErrorDetail.builder()
                        .type("type.googleapis.com/ducklake.QueryError")
                        .description(message)
                        .query(truncateQuery(query))
                        .sqlState(sqlState)
                        .build()
        );
    }

    private static String truncateQuery(String query) {
        if (query != null && query.length() > 200) {
            return query.substring(0, 200) + "...";
        }
        return query;
    }
}
