package io.ducklake.service.connector;

import io.ducklake.service.model.Catalog;
import io.ducklake.service.model.dto.QueryResponse;

import java.util.List;
import java.util.Map;

/**
 * Interface for catalog connectors.
 * Each catalog type (DuckLake, Databricks, Snowflake) implements this interface.
 */
public interface CatalogConnector {

    /**
     * Test the connection to the catalog.
     * @return true if connection is successful
     */
    boolean testConnection();

    /**
     * Execute a SQL query against the catalog.
     * @param sql the SQL query to execute
     * @return QueryResponse with results
     */
    QueryResponse executeQuery(String sql);

    /**
     * List all schemas in the catalog.
     * @return list of schema names
     */
    List<String> listSchemas();

    /**
     * List all tables in a schema.
     * @param schema the schema name
     * @return list of table names
     */
    List<String> listTables(String schema);

    /**
     * Get table columns/schema.
     * @param schema the schema name
     * @param table the table name
     * @return list of column info maps
     */
    List<Map<String, Object>> describeTable(String schema, String table);

    /**
     * Check if this catalog supports branching.
     * @return true if branching is supported (only DuckLake)
     */
    default boolean supportsBranching() {
        return false;
    }

    /**
     * Check if this catalog is read-only.
     * @return true if catalog is read-only (Databricks, Snowflake)
     */
    default boolean isReadOnly() {
        return true;
    }

    /**
     * Get the catalog type.
     * @return the catalog type
     */
    Catalog.CatalogType getCatalogType();

    /**
     * Close any resources held by the connector.
     */
    void close();
}
