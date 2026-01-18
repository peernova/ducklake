package io.ducklake.service.connector;

import io.ducklake.service.model.Catalog;
import io.ducklake.service.service.DuckDBService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Factory for creating catalog connectors based on catalog type.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class CatalogConnectorFactory {

    private final DuckDBService duckDBService;

    /**
     * Create a connector for the given catalog.
     * @param catalog the catalog to connect to
     * @return the appropriate connector
     */
    public CatalogConnector createConnector(Catalog catalog) throws SQLException {
        Connection connection = duckDBService.getConnection();

        Catalog.CatalogType type = catalog.getCatalogType();
        if (type == null) {
            type = Catalog.CatalogType.DUCKLAKE; // Default
        }

        switch (type) {
            case DUCKLAKE:
                return new DuckLakeCatalogConnector(catalog, connection);
            case DATABRICKS:
                return new DatabricksCatalogConnector(catalog, connection);
            case SNOWFLAKE:
                return new SnowflakeCatalogConnector(catalog, connection);
            default:
                throw new IllegalArgumentException("Unknown catalog type: " + type);
        }
    }

    /**
     * Release the connection used by a connector back to the pool.
     * @param connector the connector to release
     */
    public void releaseConnector(CatalogConnector connector) {
        if (connector != null) {
            connector.close();
            // Note: The connection is managed by the connector and should be released
            // back to the pool. This is handled internally by the DuckDBService.
        }
    }
}
