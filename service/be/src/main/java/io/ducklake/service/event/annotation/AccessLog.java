package io.ducklake.service.event.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to configure access logging for controller methods.
 *
 * Example usage:
 * <pre>
 * {@code
 * @AccessLog(resourceType = "table", resourceIdParams = {"catalogId", "schemaName", "tableName"})
 * public ResponseEntity<TableInfo> getTableInfo(...) { }
 *
 * @AccessLog(resourceType = "query", operation = "execute")
 * public ResponseEntity<QueryResponse> executeQuery(...) { }
 *
 * @AccessLog(skip = true)  // Don't log this endpoint
 * public ResponseEntity<Health> healthCheck() { }
 * }
 * </pre>
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface AccessLog {

    /**
     * Resource type being accessed (e.g., "catalog", "branch", "table", "query").
     */
    String resourceType() default "";

    /**
     * Path variable names that form the resource ID, in order.
     * They will be joined with "/" to form the resource ID.
     * Example: {"catalogId", "schemaName", "tableName"} → "mycatalog/myschema/mytable"
     */
    String[] resourceIdParams() default {};

    /**
     * Override the operation name (default: derived from HTTP method).
     * Examples: "view", "execute", "create", "update", "delete", "search"
     */
    String operation() default "";

    /**
     * Path variable name for the resource display name.
     * Example: "tableName" → the table name will be used as display name
     */
    String resourceNameParam() default "";

    /**
     * Skip access logging for this endpoint.
     */
    boolean skip() default false;
}
