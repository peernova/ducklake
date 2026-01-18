package io.ducklake.service.service;

import io.ducklake.service.model.dto.*;
import io.ducklake.service.tracing.TraceContext;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.observation.annotation.Observed;
import io.micrometer.tracing.Span;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.*;

@Service
@Slf4j
@RequiredArgsConstructor
public class QueryService {

    private final DuckDBService duckDBService;
    private final CatalogService catalogService;
    private final TraceContext traceContext;
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Analyze a SQL query to extract table references using ducklake_analyze_query.
     */
    @Observed(name = "query.analyze", contextualName = "analyze-query")
    public List<TableInfo> analyzeQuery(String sql) throws SQLException {
        Connection conn = duckDBService.getConnection();
        try {
            List<TableInfo> tables = new ArrayList<>();
            String analyzeSql = "SELECT * FROM ducklake_analyze_query(?, skip_errors := true)";

            try (PreparedStatement stmt = conn.prepareStatement(analyzeSql)) {
                stmt.setString(1, sql);

                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        String tableName = rs.getString("table_name");
                        String refType = rs.getString("reference_type");

                        // Skip error rows (returned when skip_errors is true and binding fails)
                        if ("ERROR".equals(tableName) || "ERROR".equals(refType)) {
                            log.debug("Skipping error row in analyze result");
                            continue;
                        }

                        // Convert column names to ColumnInfo objects
                        List<ColumnInfo> columnInfos = extractArrayColumn(rs, "columns").stream()
                                .map(name -> ColumnInfo.builder().name(name).build())
                                .collect(java.util.stream.Collectors.toList());

                        TableInfo info = TableInfo.builder()
                                .catalogName(rs.getString("catalog_name"))
                                .schemaName(rs.getString("schema_name"))
                                .tableName(tableName)
                                .referenceType(refType)
                                .columns(columnInfos)
                                .build();
                        tables.add(info);
                    }
                }
            }
            return tables;
        } finally {
            duckDBService.releaseConnection(conn);
        }
    }

    /**
     * Extract array column from DuckDB ResultSet.
     */
    private List<String> extractArrayColumn(ResultSet rs, String columnName) {
        try {
            Object obj = rs.getObject(columnName);
            if (obj == null) {
                return Collections.emptyList();
            }

            // Handle DuckDB array types
            if (obj instanceof java.sql.Array) {
                Object[] arr = (Object[]) ((java.sql.Array) obj).getArray();
                List<String> result = new ArrayList<>();
                for (Object item : arr) {
                    if (item != null) {
                        result.add(item.toString());
                    }
                }
                return result;
            }

            // Handle if it comes as a List
            if (obj instanceof List) {
                List<String> result = new ArrayList<>();
                for (Object item : (List<?>) obj) {
                    if (item != null) {
                        result.add(item.toString());
                    }
                }
                return result;
            }

            // Fallback: try to parse as string
            String str = obj.toString();
            return parseColumns(str);
        } catch (SQLException e) {
            log.warn("Failed to extract array column {}: {}", columnName, e.getMessage());
            return Collections.emptyList();
        }
    }

    /**
     * Rewrite a SQL query with branch and RLS policies using ducklake_rewrite_query.
     */
    @Observed(name = "query.rewrite", contextualName = "rewrite-query")
    public String rewriteQuery(String sql, Map<String, TablePolicy> policies) throws SQLException {
        if (policies == null || policies.isEmpty()) {
            return sql;
        }

        Connection conn = duckDBService.getConnection();
        try {
            String policiesJson = buildPoliciesJson(policies);
            String rewriteSql = "SELECT ducklake_rewrite_query(?, ?)";

            try (PreparedStatement stmt = conn.prepareStatement(rewriteSql)) {
                stmt.setString(1, sql);
                stmt.setString(2, policiesJson);

                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        return rs.getString(1);
                    }
                }
            }
            return sql;
        } finally {
            duckDBService.releaseConnection(conn);
        }
    }

    /**
     * Execute DDL/DML statement on a specific branch.
     * This method atomically sets the branch context and executes the statement.
     */
    @Observed(name = "query.executeOnBranch", contextualName = "execute-on-branch")
    public QueryResponse executeOnBranch(String catalogId, ExecuteRequest request) throws SQLException {
        long startTime = System.currentTimeMillis();

        // Get catalog and attach it
        var catalog = catalogService.getCatalog(catalogId)
            .orElseThrow(() -> new SQLException("Catalog not found: " + catalogId));

        Connection conn = duckDBService.getConnection();

        try {
            // Attach the catalog first
            duckDBService.attachCatalog(conn, catalog);

            // Then switch to the specified branch (in same connection/transaction)
            String useBranchSql = String.format(
                "CALL ducklake_use_branch('%s', '%s')",
                catalogId.replace("'", "''"),
                request.getBranchName().replace("'", "''")
            );

            try (Statement stmt = conn.createStatement()) {
                stmt.execute(useBranchSql);
                log.debug("Switched to branch {} on catalog {}", request.getBranchName(), catalogId);
            }

            // Now execute the DDL/DML statement on the same connection
            Span sqlSpan = traceContext.startSpan("duckdb-execute-on-branch");
            try {
                sqlSpan.tag("db.system", "duckdb");
                sqlSpan.tag("db.catalog", catalogId);
                sqlSpan.tag("db.branch", request.getBranchName());
                sqlSpan.tag("db.statement", request.getSql().length() > 100
                    ? request.getSql().substring(0, 100) + "..." : request.getSql());

                try (Statement stmt = conn.createStatement()) {
                    if (request.getTimeoutMs() != null) {
                        stmt.setQueryTimeout((int) (request.getTimeoutMs() / 1000));
                    }

                    boolean hasResultSet = stmt.execute(request.getSql());
                    sqlSpan.tag("db.operation", hasResultSet ? "SELECT" : "DML");

                    if (hasResultSet) {
                        try (ResultSet rs = stmt.getResultSet()) {
                            return buildQueryResponseSimple(rs, startTime, request.getBranchName());
                        }
                    } else {
                        long rowCount = stmt.getUpdateCount();
                        sqlSpan.tag("db.rows_affected", String.valueOf(rowCount));
                        return QueryResponse.builder()
                                .rowCount(rowCount)
                                .executionTimeMs((double) (System.currentTimeMillis() - startTime))
                                .branch(request.getBranchName())
                                .traceId(traceContext.getTraceId())
                                .spanId(traceContext.getSpanId())
                                .build();
                    }
                }
            } catch (Exception e) {
                sqlSpan.error(e);
                throw e;
            } finally {
                sqlSpan.end();
            }
        } finally {
            duckDBService.releaseConnection(conn);
        }
    }

    private QueryResponse buildQueryResponseSimple(ResultSet rs, long startTime, String branch) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        int columnCount = meta.getColumnCount();

        List<ColumnInfo> columns = new ArrayList<>();
        for (int i = 1; i <= columnCount; i++) {
            columns.add(ColumnInfo.builder()
                    .name(meta.getColumnName(i))
                    .type(meta.getColumnTypeName(i))
                    .nullable(meta.isNullable(i) != ResultSetMetaData.columnNoNulls)
                    .build());
        }

        List<List<Object>> rows = new ArrayList<>();
        long rowCount = 0;
        while (rs.next()) {
            List<Object> row = new ArrayList<>();
            for (int i = 1; i <= columnCount; i++) {
                row.add(rs.getObject(i));
            }
            rows.add(row);
            rowCount++;
        }

        return QueryResponse.builder()
                .columns(columns)
                .rows(rows)
                .rowCount(rowCount)
                .executionTimeMs((double) (System.currentTimeMillis() - startTime))
                .branch(branch)
                .traceId(traceContext.getTraceId())
                .spanId(traceContext.getSpanId())
                .build();
    }

    /**
     * Execute a SQL query with optional branch context.
     */
    @Observed(name = "query.execute", contextualName = "execute-query")
    public QueryResponse executeQuery(QueryRequest request) throws SQLException {
        long startTime = System.currentTimeMillis();
        Connection conn = duckDBService.getConnection();

        try {
            String sql = request.getSql();

            // Build policies from branch context and any RLS policies
            Map<String, TablePolicy> policies = buildPolicies(request);
            if (!policies.isEmpty()) {
                sql = rewriteQueryInternal(conn, sql, policies);
                log.debug("Rewritten SQL: {}", sql);
            }

            // Execute the query with manual span
            Span sqlSpan = traceContext.startSpan("duckdb-sql-execute");
            try {
                sqlSpan.tag("db.system", "duckdb");
                sqlSpan.tag("db.statement", sql.length() > 100 ? sql.substring(0, 100) + "..." : sql);
                try (Statement stmt = conn.createStatement()) {
                    if (request.getTimeoutMs() != null) {
                        stmt.setQueryTimeout((int) (request.getTimeoutMs() / 1000));
                    }

                    boolean hasResultSet = stmt.execute(sql);
                    sqlSpan.tag("db.operation", hasResultSet ? "SELECT" : "DML");

                    if (hasResultSet) {
                        try (ResultSet rs = stmt.getResultSet()) {
                            QueryResponse response = buildQueryResponse(rs, startTime, request);
                            sqlSpan.tag("db.rows", String.valueOf(response.getRowCount()));
                            return response;
                        }
                    } else {
                        // DML statement
                        long rowCount = stmt.getUpdateCount();
                        sqlSpan.tag("db.rows_affected", String.valueOf(rowCount));
                        return QueryResponse.builder()
                                .rowCount(rowCount)
                                .executionTimeMs((double) (System.currentTimeMillis() - startTime))
                                .traceId(traceContext.getTraceId())
                                .spanId(traceContext.getSpanId())
                                .build();
                    }
                }
            } catch (Exception e) {
                sqlSpan.error(e);
                throw e;
            } finally {
                sqlSpan.end();
            }
        } finally {
            duckDBService.releaseConnection(conn);
        }
    }

    private String rewriteQueryInternal(Connection conn, String sql, Map<String, TablePolicy> policies) throws SQLException {
        String policiesJson = buildPoliciesJson(policies);
        String rewriteSql = "SELECT ducklake_rewrite_query(?, ?)";

        try (PreparedStatement stmt = conn.prepareStatement(rewriteSql)) {
            stmt.setString(1, sql);
            stmt.setString(2, policiesJson);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getString(1);
                }
            }
        }
        return sql;
    }

    private Map<String, TablePolicy> buildPolicies(QueryRequest request) {
        Map<String, TablePolicy> policies = new HashMap<>();

        // Add branch context as policies
        if (request.getBranchContext() != null) {
            for (Map.Entry<String, String> entry : request.getBranchContext().entrySet()) {
                String tableKey = entry.getKey(); // catalog.schema.table or just table
                policies.put(tableKey, TablePolicy.builder()
                        .branch(entry.getValue())
                        .build());
            }
        }

        return policies;
    }

    private String buildPoliciesJson(Map<String, TablePolicy> policies) {
        try {
            Map<String, Map<String, String>> jsonMap = new HashMap<>();
            for (Map.Entry<String, TablePolicy> entry : policies.entrySet()) {
                Map<String, String> policyMap = new HashMap<>();
                if (entry.getValue().getBranch() != null) {
                    policyMap.put("branch", entry.getValue().getBranch());
                }
                if (entry.getValue().getRls() != null) {
                    policyMap.put("rls", entry.getValue().getRls());
                }
                jsonMap.put(entry.getKey(), policyMap);
            }
            return objectMapper.writeValueAsString(jsonMap);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize policies", e);
            return "{}";
        }
    }

    private QueryResponse buildQueryResponse(ResultSet rs, long startTime, QueryRequest request) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        int columnCount = meta.getColumnCount();

        // Build column info
        List<ColumnInfo> columns = new ArrayList<>();
        for (int i = 1; i <= columnCount; i++) {
            columns.add(ColumnInfo.builder()
                    .name(meta.getColumnName(i))
                    .type(meta.getColumnTypeName(i))
                    .nullable(meta.isNullable(i) != ResultSetMetaData.columnNoNulls)
                    .build());
        }

        // Build rows
        List<List<Object>> rows = new ArrayList<>();
        long rowCount = 0;
        while (rs.next()) {
            List<Object> row = new ArrayList<>();
            for (int i = 1; i <= columnCount; i++) {
                row.add(rs.getObject(i));
            }
            rows.add(row);
            rowCount++;
        }

        // Get branch from context if provided
        String branch = null;
        if (request.getBranchContext() != null && !request.getBranchContext().isEmpty()) {
            branch = request.getBranchContext().values().iterator().next();
        }

        return QueryResponse.builder()
                .columns(columns)
                .rows(rows)
                .rowCount(rowCount)
                .executionTimeMs((double) (System.currentTimeMillis() - startTime))
                .branch(branch)
                .traceId(traceContext.getTraceId())
                .spanId(traceContext.getSpanId())
                .build();
    }

    private List<String> parseColumns(String columnsStr) {
        if (columnsStr == null || columnsStr.isEmpty() || "[]".equals(columnsStr)) {
            return Collections.emptyList();
        }
        // Parse array string like "[col1, col2, col3]"
        String inner = columnsStr.substring(1, columnsStr.length() - 1);
        if (inner.isEmpty()) {
            return Collections.emptyList();
        }
        return Arrays.asList(inner.split(",\\s*"));
    }
}
