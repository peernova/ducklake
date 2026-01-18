package io.ducklake.service.controller;

import io.ducklake.service.exception.QueryException;
import io.ducklake.service.model.dto.ErrorResponse;
import io.ducklake.service.model.dto.ExecuteRequest;
import io.ducklake.service.model.dto.QueryRequest;
import io.ducklake.service.model.dto.QueryResponse;
import io.ducklake.service.model.dto.TableInfo;
import io.ducklake.service.service.QueryService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.sql.SQLException;
import java.util.List;

@RestController
@RequestMapping("/api/v1")
@RequiredArgsConstructor
@Slf4j
@Tag(name = "Query", description = "Query execution and analysis")
public class QueryController {

    private final QueryService queryService;

    @PostMapping("/query")
    @Operation(summary = "Execute SQL query", description = "Execute a SQL query with optional branch context and RLS policies")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Query executed successfully"),
            @ApiResponse(responseCode = "400", description = "Invalid SQL syntax or query error",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class))),
            @ApiResponse(responseCode = "404", description = "Table or catalog not found",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class))),
            @ApiResponse(responseCode = "500", description = "Internal server error",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class)))
    })
    public ResponseEntity<QueryResponse> executeQuery(@Valid @RequestBody QueryRequest request) {
        log.info("Executing query: {}", truncateForLog(request.getSql()));
        try {
            QueryResponse response = queryService.executeQuery(request);
            log.info("Query completed: {} rows in {}ms", response.getRowCount(), response.getExecutionTimeMs());
            return ResponseEntity.ok(response);
        } catch (SQLException e) {
            log.error("Query execution failed", e);
            throw new QueryException(e.getMessage(), request.getSql(), e.getSQLState());
        }
    }

    @PostMapping("/query/analyze")
    @Operation(summary = "Analyze SQL query", description = "Extract table references from a SQL query for permission checking")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Query analyzed successfully"),
            @ApiResponse(responseCode = "400", description = "Invalid SQL syntax",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class))),
            @ApiResponse(responseCode = "500", description = "Internal server error",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class)))
    })
    public ResponseEntity<List<TableInfo>> analyzeQuery(@RequestBody String sql) {
        log.debug("Analyzing query: {}", truncateForLog(sql));
        try {
            List<TableInfo> tables = queryService.analyzeQuery(sql);
            return ResponseEntity.ok(tables);
        } catch (SQLException e) {
            log.error("Query analysis failed", e);
            throw new QueryException(e.getMessage(), sql, e.getSQLState());
        }
    }

    @PostMapping("/catalogs/{catalogId}/execute")
    @Operation(summary = "Execute DDL/DML on branch",
            description = "Execute a DDL or DML statement on a specific branch. The branch context is set atomically with the statement execution.")
    @ApiResponses({
            @ApiResponse(responseCode = "200", description = "Statement executed successfully"),
            @ApiResponse(responseCode = "400", description = "Invalid SQL syntax or statement error",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class))),
            @ApiResponse(responseCode = "403", description = "Permission denied - no write access to branch",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class))),
            @ApiResponse(responseCode = "404", description = "Catalog or branch not found",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class))),
            @ApiResponse(responseCode = "500", description = "Internal server error",
                    content = @Content(schema = @Schema(implementation = ErrorResponse.class)))
    })
    public ResponseEntity<QueryResponse> executeOnBranch(
            @PathVariable String catalogId,
            @Valid @RequestBody ExecuteRequest request) {
        log.info("Executing on catalog {} branch {}: {}", catalogId, request.getBranchName(), truncateForLog(request.getSql()));

        // TODO: Add permission check here
        // Example: permissionService.checkWriteAccess(catalogId, request.getBranchName(), getCurrentUser());
        // Throw PermissionDeniedException if no access

        try {
            QueryResponse response = queryService.executeOnBranch(catalogId, request);
            log.info("Execution completed on branch {}: {} rows affected in {}ms",
                    request.getBranchName(), response.getRowCount(), response.getExecutionTimeMs());
            return ResponseEntity.ok(response);
        } catch (SQLException e) {
            log.error("Execution failed on branch {}", request.getBranchName(), e);
            throw new QueryException(e.getMessage(), request.getSql(), e.getSQLState());
        }
    }

    private String truncateForLog(String sql) {
        if (sql == null) return null;
        return sql.length() > 100 ? sql.substring(0, 100) + "..." : sql;
    }
}
