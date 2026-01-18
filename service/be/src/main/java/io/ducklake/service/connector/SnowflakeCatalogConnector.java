package io.ducklake.service.connector;

import io.ducklake.service.model.Catalog;
import io.ducklake.service.model.dto.QueryResponse;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * Connector for Snowflake catalogs.
 * TODO: Implement using DuckDB's snowflake extension.
 */
@Slf4j
public class SnowflakeCatalogConnector implements CatalogConnector {

    private final Catalog catalog;
    private final Connection connection;

    public SnowflakeCatalogConnector(Catalog catalog, Connection connection) {
        this.catalog = catalog;
        this.connection = connection;
    }

    @Override
    public boolean testConnection() {
        // TODO: Implement Snowflake connection test
        throw new UnsupportedOperationException("Snowflake connector not yet implemented");
    }

    @Override
    public QueryResponse executeQuery(String sql) {
        // TODO: Implement Snowflake query execution
        throw new UnsupportedOperationException("Snowflake connector not yet implemented");
    }

    @Override
    public List<String> listSchemas() {
        // TODO: Implement Snowflake schema listing
        throw new UnsupportedOperationException("Snowflake connector not yet implemented");
    }

    @Override
    public List<String> listTables(String schema) {
        // TODO: Implement Snowflake table listing
        throw new UnsupportedOperationException("Snowflake connector not yet implemented");
    }

    @Override
    public List<Map<String, Object>> describeTable(String schema, String table) {
        // TODO: Implement Snowflake table description
        throw new UnsupportedOperationException("Snowflake connector not yet implemented");
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
        return Catalog.CatalogType.SNOWFLAKE;
    }

    @Override
    public void close() {
        // TODO: Implement cleanup
    }
}
