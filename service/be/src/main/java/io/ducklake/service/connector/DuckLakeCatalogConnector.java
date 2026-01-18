package io.ducklake.service.connector;

import io.ducklake.service.model.Catalog;
import io.ducklake.service.model.dto.ColumnInfo;
import io.ducklake.service.model.dto.QueryResponse;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.util.*;

/**
 * Connector for native DuckLake catalogs.
 * Supports full read/write operations and branching.
 */
@Slf4j
public class DuckLakeCatalogConnector implements CatalogConnector {

    private final Catalog catalog;
    private final Connection connection;
    private boolean attached = false;

    public DuckLakeCatalogConnector(Catalog catalog, Connection connection) {
        this.catalog = catalog;
        this.connection = connection;
    }

    public void attach() throws SQLException {
        if (attached) return;

        String attachSql = buildAttachSql();
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(attachSql);
            attached = true;
            log.debug("Attached DuckLake catalog: {}", catalog.getCatalogId());
        }
    }

    private String buildAttachSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("ATTACH '");

        if (catalog.getMetadataType() == Catalog.MetadataType.postgres) {
            sb.append("metadata=").append(catalog.getMetadataUri());
        } else {
            sb.append(catalog.getMetadataUri());
        }

        sb.append("' AS \"").append(catalog.getCatalogId()).append("\" (TYPE DUCKLAKE");

        if (catalog.getDataPath() != null && !catalog.getDataPath().isEmpty()) {
            sb.append(", DATA_PATH '").append(catalog.getDataPath()).append("'");
        }

        sb.append(")");
        return sb.toString();
    }

    @Override
    public boolean testConnection() {
        try {
            attach();
            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT 1")) {
                return rs.next();
            }
        } catch (SQLException e) {
            log.error("Connection test failed for catalog: {}", catalog.getCatalogId(), e);
            return false;
        }
    }

    @Override
    public QueryResponse executeQuery(String sql) {
        long startTime = System.currentTimeMillis();
        try {
            attach();
            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                return buildQueryResponse(rs, startTime);
            }
        } catch (SQLException e) {
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
            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(
                         "SELECT schema_name FROM \"" + catalog.getCatalogId() + "\".information_schema.schemata")) {
                while (rs.next()) {
                    schemas.add(rs.getString(1));
                }
            }
            return schemas;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to list schemas: " + e.getMessage(), e);
        }
    }

    @Override
    public List<String> listTables(String schema) {
        try {
            attach();
            List<String> tables = new ArrayList<>();
            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(
                         "SELECT table_name FROM \"" + catalog.getCatalogId() +
                         "\".information_schema.tables WHERE table_schema = '" + schema + "'")) {
                while (rs.next()) {
                    tables.add(rs.getString(1));
                }
            }
            return tables;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to list tables: " + e.getMessage(), e);
        }
    }

    @Override
    public List<Map<String, Object>> describeTable(String schema, String table) {
        try {
            attach();
            List<Map<String, Object>> columns = new ArrayList<>();
            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(
                         "SELECT column_name, data_type, is_nullable FROM \"" + catalog.getCatalogId() +
                         "\".information_schema.columns WHERE table_schema = '" + schema +
                         "' AND table_name = '" + table + "'")) {
                while (rs.next()) {
                    Map<String, Object> col = new HashMap<>();
                    col.put("name", rs.getString("column_name"));
                    col.put("type", rs.getString("data_type"));
                    col.put("nullable", "YES".equals(rs.getString("is_nullable")));
                    columns.add(col);
                }
            }
            return columns;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to describe table: " + e.getMessage(), e);
        }
    }

    @Override
    public boolean supportsBranching() {
        return true;
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public Catalog.CatalogType getCatalogType() {
        return Catalog.CatalogType.DUCKLAKE;
    }

    @Override
    public void close() {
        if (attached) {
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("DETACH \"" + catalog.getCatalogId() + "\"");
                attached = false;
            } catch (SQLException e) {
                log.warn("Failed to detach catalog: {}", catalog.getCatalogId(), e);
            }
        }
    }
}
