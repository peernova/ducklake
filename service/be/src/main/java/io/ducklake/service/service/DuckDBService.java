package io.ducklake.service.service;

import io.ducklake.service.config.DuckDBConfig;
import io.ducklake.service.model.Catalog;
import io.micrometer.observation.annotation.Observed;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.duckdb.DuckDBConnection;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
@RequiredArgsConstructor
public class DuckDBService {

    private final DuckDBConfig config;

    // Primary connection - used for initialization and as source for duplicates
    private DuckDBConnection primaryConnection;

    // Connection pool using DuckDB's duplicate() for shared database state
    private BlockingQueue<Connection> connectionPool;
    private final List<Connection> allConnections = Collections.synchronizedList(new ArrayList<>());

    private volatile boolean initialized = false;

    // Track attached catalogs (shared across all duplicated connections)
    private final Set<String> attachedCatalogs = ConcurrentHashMap.newKeySet();

    private static final int POOL_SIZE = 10;
    private static final long ACQUIRE_TIMEOUT_MS = 5000;

    @PostConstruct
    public synchronized void init() throws SQLException {
        log.info("Initializing DuckDB connection pool with {} connections", POOL_SIZE);

        // Create primary connection
        primaryConnection = createPrimaryConnection();
        allConnections.add(primaryConnection);

        // Create pool with duplicated connections
        connectionPool = new ArrayBlockingQueue<>(POOL_SIZE);
        for (int i = 0; i < POOL_SIZE; i++) {
            Connection conn = primaryConnection.duplicate();
            connectionPool.offer(conn);
            allConnections.add(conn);
        }

        initialized = true;
        log.info("DuckDB connection pool initialized with {} connections", POOL_SIZE);
    }

    private DuckDBConnection createPrimaryConnection() throws SQLException {
        try {
            Class.forName("org.duckdb.DuckDBDriver");
        } catch (ClassNotFoundException e) {
            throw new SQLException("DuckDB driver not found", e);
        }

        // Single in-memory DuckDB instance
        Properties props = new Properties();
        props.put("allow_unsigned_extensions", "true");

        Connection conn = DriverManager.getConnection("jdbc:duckdb:", props);
        DuckDBConnection duckConn = conn.unwrap(DuckDBConnection.class);

        try (Statement stmt = duckConn.createStatement()) {
            // Configure DuckDB
            stmt.execute("SET threads = " + config.getThreads());
            stmt.execute("SET memory_limit = '" + config.getMemoryLimit() + "'");
            stmt.execute("SET temp_directory = '" + config.getTempDirectory() + "'");

            // Load postgres_scanner for postgres metadata support
            String pgScannerPath = config.getPostgresScannerPath();
            if (pgScannerPath != null && !pgScannerPath.isEmpty()) {
                stmt.execute("LOAD '" + pgScannerPath + "'");
                log.debug("Loaded postgres_scanner extension from {}", pgScannerPath);
            }

            // Load DuckLake extension
            String extPath = config.getExtensionPath();
            stmt.execute("LOAD '" + extPath + "'");
            log.debug("Loaded DuckLake extension from {}", extPath);
        }

        return duckConn;
    }

    @Observed(name = "duckdb.pool.acquire", contextualName = "acquire-connection")
    public Connection getConnection() throws SQLException {
        if (!initialized) {
            throw new SQLException("DuckDB service not initialized");
        }

        try {
            Connection conn = connectionPool.poll(ACQUIRE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            if (conn == null) {
                throw new SQLException("Timeout waiting for connection from pool");
            }

            // Verify connection is still valid
            if (conn.isClosed()) {
                log.warn("Got closed connection from pool, creating new one");
                conn = primaryConnection.duplicate();
                allConnections.add(conn);
            }

            return conn;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SQLException("Interrupted while waiting for connection", e);
        }
    }

    public void releaseConnection(Connection conn) {
        if (conn != null && initialized) {
            try {
                if (!conn.isClosed()) {
                    // Clear any transaction state
                    if (!conn.getAutoCommit()) {
                        conn.rollback();
                        conn.setAutoCommit(true);
                    }
                    connectionPool.offer(conn);
                }
            } catch (SQLException e) {
                log.warn("Error releasing connection", e);
            }
        }
    }

    @Observed(name = "duckdb.catalog.attach", contextualName = "attach-catalog")
    public void attachCatalog(Connection conn, Catalog catalog) throws SQLException {
        String catalogId = catalog.getCatalogId();

        // Check if already attached (catalogs are shared across duplicated connections)
        if (attachedCatalogs.contains(catalogId)) {
            log.debug("Catalog already attached: {}", catalogId);
            return;
        }

        // Attach on primary connection (shared state across all duplicates)
        synchronized (this) {
            if (attachedCatalogs.contains(catalogId)) {
                return;
            }

            String attachSql = buildAttachSql(catalog);
            try (Statement stmt = primaryConnection.createStatement()) {
                stmt.execute(attachSql);
                attachedCatalogs.add(catalogId);
                log.debug("Attached catalog: {}", catalogId);
            }
        }
    }

    public void detachCatalog(Connection conn, String catalogId) throws SQLException {
        // Skip detach - keep catalogs attached for reuse
        // DuckDB duplicated connections share catalog state
    }

    private String buildAttachSql(Catalog catalog) {
        StringBuilder sb = new StringBuilder();
        sb.append("ATTACH '");

        // Build connection string with ducklake: prefix
        if (catalog.getMetadataType() == Catalog.MetadataType.postgres) {
            // Format: ducklake:postgres:dbname=xxx host=localhost port=5433 user=postgres password=postgres
            sb.append("ducklake:").append(catalog.getMetadataUri());
        } else {
            // For duckdb/sqlite metadata
            sb.append(catalog.getMetadataUri());
        }

        sb.append("' AS \"").append(catalog.getCatalogId()).append("\"");

        // Add data path if specified
        if (catalog.getDataPath() != null && !catalog.getDataPath().isEmpty()) {
            sb.append(" (DATA_PATH '").append(catalog.getDataPath()).append("')");
        }

        return sb.toString();
    }

    @PreDestroy
    public synchronized void shutdown() {
        log.info("Shutting down DuckDB connection pool");
        initialized = false;

        // Close all connections
        for (Connection conn : allConnections) {
            try {
                if (conn != null && !conn.isClosed()) {
                    conn.close();
                }
            } catch (SQLException e) {
                log.warn("Error closing connection", e);
            }
        }

        allConnections.clear();
        connectionPool.clear();
        attachedCatalogs.clear();
        log.info("DuckDB connection pool shut down");
    }
}
