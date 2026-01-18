package io.ducklake.service.connector;

import io.ducklake.service.model.Catalog;
import io.ducklake.service.model.dto.ColumnInfo;
import io.ducklake.service.model.dto.QueryResponse;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.*;
import java.util.*;

/**
 * Connector for Databricks Unity Catalog.
 * Uses DuckDB's uc_catalog and delta extensions for read-only access.
 */
@Slf4j
public class DatabricksCatalogConnector implements CatalogConnector {

    private final Catalog catalog;
    private final Connection connection;
    private final HttpClient httpClient;
    private String oauthToken;
    private long tokenExpiry = 0;
    private boolean attached = false;

    public DatabricksCatalogConnector(Catalog catalog, Connection connection) {
        this.catalog = catalog;
        this.connection = connection;
        this.httpClient = HttpClient.newHttpClient();
    }

    public void attach() throws SQLException {
        if (attached) return;

        try {
            // Load required extensions
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("INSTALL delta");
                stmt.execute("LOAD delta");
                stmt.execute("INSTALL uc_catalog FROM core_nightly");
                stmt.execute("LOAD uc_catalog");
            }

            // Get OAuth token
            refreshOAuthToken();

            // Create secret and attach
            try (Statement stmt = connection.createStatement()) {
                // Drop existing secret if any
                try {
                    stmt.execute("DROP SECRET IF EXISTS uc_secret_" + catalog.getCatalogId());
                } catch (SQLException ignored) {}

                // Create new secret
                String createSecretSql = String.format(
                    "CREATE SECRET uc_secret_%s (TYPE UC, TOKEN '%s', ENDPOINT '%s')",
                    catalog.getCatalogId(),
                    oauthToken,
                    catalog.getDatabricksWorkspaceUrl()
                );
                stmt.execute(createSecretSql);

                // Attach catalog
                String attachSql = String.format(
                    "ATTACH '%s' AS \"%s\" (TYPE UC_CATALOG)",
                    catalog.getDatabricksCatalogName(),
                    catalog.getCatalogId()
                );
                stmt.execute(attachSql);
            }

            attached = true;
            log.debug("Attached Databricks catalog: {}", catalog.getCatalogId());
        } catch (Exception e) {
            throw new SQLException("Failed to attach Databricks catalog: " + e.getMessage(), e);
        }
    }

    private void refreshOAuthToken() throws Exception {
        if (System.currentTimeMillis() < tokenExpiry - 60000) {
            return; // Token still valid for at least 1 minute
        }

        String tokenUrl = catalog.getDatabricksWorkspaceUrl() + "/oidc/v1/token";
        String body = String.format(
            "grant_type=client_credentials&client_id=%s&client_secret=%s&scope=all-apis",
            catalog.getDatabricksClientId(),
            catalog.getDatabricksClientSecret()
        );

        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(tokenUrl))
            .header("Content-Type", "application/x-www-form-urlencoded")
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 200) {
            throw new RuntimeException("OAuth token request failed: " + response.body());
        }

        // Parse JSON response (simple parsing without external library)
        String responseBody = response.body();
        int tokenStart = responseBody.indexOf("\"access_token\":\"") + 16;
        int tokenEnd = responseBody.indexOf("\"", tokenStart);
        oauthToken = responseBody.substring(tokenStart, tokenEnd);

        int expiresStart = responseBody.indexOf("\"expires_in\":") + 13;
        int expiresEnd = responseBody.indexOf(",", expiresStart);
        if (expiresEnd == -1) expiresEnd = responseBody.indexOf("}", expiresStart);
        long expiresIn = Long.parseLong(responseBody.substring(expiresStart, expiresEnd).trim());

        tokenExpiry = System.currentTimeMillis() + (expiresIn * 1000);
        log.debug("Refreshed OAuth token for Databricks, expires in {} seconds", expiresIn);
    }

    @Override
    public boolean testConnection() {
        try {
            attach();
            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT 1")) {
                return rs.next();
            }
        } catch (Exception e) {
            log.error("Connection test failed for Databricks catalog: {}", catalog.getCatalogId(), e);
            return false;
        }
    }

    @Override
    public QueryResponse executeQuery(String sql) {
        long startTime = System.currentTimeMillis();
        try {
            attach();

            // Refresh token if needed
            refreshOAuthToken();

            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                return buildQueryResponse(rs, startTime);
            }
        } catch (Exception e) {
            throw new RuntimeException("Query execution failed: " + e.getMessage(), e);
        }
    }

    private QueryResponse buildQueryResponse(ResultSet rs, long startTime) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        int columnCount = meta.getColumnCount();

        List<ColumnInfo> columns = new ArrayList<>();
        for (int i = 1; i <= columnCount; i++) {
            columns.add(ColumnInfo.builder()
                    .name(meta.getColumnName(i))
                    .type(meta.getColumnTypeName(i))
                    .nullable(meta.isNullable(i) == ResultSetMetaData.columnNullable)
                    .build());
        }

        List<List<Object>> rows = new ArrayList<>();
        while (rs.next()) {
            List<Object> row = new ArrayList<>();
            for (int i = 1; i <= columnCount; i++) {
                row.add(rs.getObject(i));
            }
            rows.add(row);
        }

        return QueryResponse.builder()
                .columns(columns)
                .rows(rows)
                .rowCount((long) rows.size())
                .executionTimeMs((double) (System.currentTimeMillis() - startTime))
                .build();
    }

    @Override
    public List<String> listSchemas() {
        try {
            attach();
            List<String> schemas = new ArrayList<>();
            String sql = "SELECT schema_name FROM \"" + catalog.getCatalogId() + "\".information_schema.schemata " +
                        "WHERE schema_name != 'information_schema'";
            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                while (rs.next()) {
                    schemas.add(rs.getString(1));
                }
            }
            return schemas;
        } catch (Exception e) {
            throw new RuntimeException("Failed to list schemas: " + e.getMessage(), e);
        }
    }

    @Override
    public List<String> listTables(String schema) {
        try {
            attach();
            List<String> tables = new ArrayList<>();
            String sql = "SELECT table_name FROM \"" + catalog.getCatalogId() +
                        "\".information_schema.tables WHERE table_schema = '" + schema + "'";
            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                while (rs.next()) {
                    tables.add(rs.getString(1));
                }
            }
            return tables;
        } catch (Exception e) {
            throw new RuntimeException("Failed to list tables: " + e.getMessage(), e);
        }
    }

    @Override
    public List<Map<String, Object>> describeTable(String schema, String table) {
        try {
            attach();
            List<Map<String, Object>> columns = new ArrayList<>();
            String sql = "SELECT column_name, data_type, is_nullable FROM \"" + catalog.getCatalogId() +
                        "\".information_schema.columns WHERE table_schema = '" + schema +
                        "' AND table_name = '" + table + "'";
            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                while (rs.next()) {
                    Map<String, Object> col = new HashMap<>();
                    col.put("name", rs.getString("column_name"));
                    col.put("type", rs.getString("data_type"));
                    col.put("nullable", "YES".equals(rs.getString("is_nullable")));
                    columns.add(col);
                }
            }
            return columns;
        } catch (Exception e) {
            throw new RuntimeException("Failed to describe table: " + e.getMessage(), e);
        }
    }

    @Override
    public boolean supportsBranching() {
        return false;
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }

    @Override
    public Catalog.CatalogType getCatalogType() {
        return Catalog.CatalogType.DATABRICKS;
    }

    @Override
    public void close() {
        if (attached) {
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("DETACH \"" + catalog.getCatalogId() + "\"");
                stmt.execute("DROP SECRET IF EXISTS uc_secret_" + catalog.getCatalogId());
                attached = false;
            } catch (SQLException e) {
                log.warn("Failed to detach Databricks catalog: {}", catalog.getCatalogId(), e);
            }
        }
    }
}
