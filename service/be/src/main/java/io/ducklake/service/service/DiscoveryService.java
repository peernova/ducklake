package io.ducklake.service.service;

import io.ducklake.service.exception.CatalogException;
import io.ducklake.service.model.Catalog;
import io.ducklake.service.model.dto.*;
import io.micrometer.observation.annotation.Observed;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.time.Instant;
import java.util.*;

@Service
@Slf4j
@RequiredArgsConstructor
public class DiscoveryService {

    private final DuckDBService duckDBService;
    private final CatalogService catalogService;

    @Observed(name = "discovery.schemas", contextualName = "list-schemas")
    public List<SchemaInfo> listSchemas(String catalogId, String branch) throws SQLException {
        Catalog catalog = getCatalogOrThrow(catalogId);
        Connection conn = duckDBService.getConnection();
        try {
            duckDBService.attachCatalog(conn, catalog);

            // Set branch context if specified
            if (branch != null && !branch.isEmpty()) {
                String useBranchSql = String.format("CALL ducklake_use_branch('%s', '%s')",
                        catalogId.replace("'", "''"), branch.replace("'", "''"));
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute(useBranchSql);
                }
            }

            List<SchemaInfo> schemas = new ArrayList<>();

            String sql = "SELECT schema_name FROM information_schema.schemata WHERE catalog_name = ?";
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setString(1, catalogId);
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        schemas.add(SchemaInfo.builder()
                                .schemaName(rs.getString("schema_name"))
                                .build());
                    }
                }
            }
            duckDBService.detachCatalog(conn, catalogId);
            return schemas;
        } finally {
            duckDBService.releaseConnection(conn);
        }
    }

    @Observed(name = "discovery.tables", contextualName = "list-tables")
    public List<TableInfo> listTables(String catalogId, String schemaName, String branch) throws SQLException {
        Catalog catalog = getCatalogOrThrow(catalogId);
        Connection conn = duckDBService.getConnection();
        try {
            duckDBService.attachCatalog(conn, catalog);

            // Set branch context if specified
            if (branch != null && !branch.isEmpty()) {
                String useBranchSql = String.format("CALL ducklake_use_branch('%s', '%s')",
                        catalogId.replace("'", "''"), branch.replace("'", "''"));
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute(useBranchSql);
                }
            }

            // First collect all table names
            List<String> tableNames = new ArrayList<>();
            String sql = "SELECT table_name FROM information_schema.tables WHERE table_catalog = ? AND table_schema = ?";
            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setString(1, catalogId);
                stmt.setString(2, schemaName);
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        tableNames.add(rs.getString("table_name"));
                    }
                }
            }

            // Then fetch columns for each table (after ResultSet is closed)
            List<TableInfo> tables = new ArrayList<>();
            for (String tableName : tableNames) {
                List<ColumnInfo> columns = fetchColumnsForTable(conn, catalogId, schemaName, tableName);
                tables.add(TableInfo.builder()
                        .tableName(tableName)
                        .schemaName(schemaName)
                        .catalogName(catalogId)
                        .columns(columns)
                        .build());
            }

            duckDBService.detachCatalog(conn, catalogId);
            return tables;
        } finally {
            duckDBService.releaseConnection(conn);
        }
    }

    private List<ColumnInfo> fetchColumnsForTable(Connection conn, String catalogId, String schemaName, String tableName) throws SQLException {
        List<ColumnInfo> columns = new ArrayList<>();
        String sql = "SELECT column_name, data_type, is_nullable FROM information_schema.columns " +
                     "WHERE table_catalog = ? AND table_schema = ? AND table_name = ? ORDER BY ordinal_position";
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, catalogId);
            stmt.setString(2, schemaName);
            stmt.setString(3, tableName);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    columns.add(ColumnInfo.builder()
                            .name(rs.getString("column_name"))
                            .type(rs.getString("data_type"))
                            .nullable("YES".equalsIgnoreCase(rs.getString("is_nullable")))
                            .build());
                }
            }
        }
        return columns;
    }

    @Observed(name = "discovery.table-info", contextualName = "get-table-info")
    public TableInfo getTableInfo(String catalogId, String schemaName, String tableName, String branch) throws SQLException {
        Catalog catalog = getCatalogOrThrow(catalogId);
        Connection conn = duckDBService.getConnection();
        try {
            duckDBService.attachCatalog(conn, catalog);

            String sql = "SELECT * FROM ducklake_table_info('" + catalogId + "', '" + schemaName + "', '" + tableName + "')";
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                if (rs.next()) {
                    TableInfo info = TableInfo.builder()
                            .tableName(tableName)
                            .schemaName(schemaName)
                            .catalogName(catalogId)
                            .build();
                    duckDBService.detachCatalog(conn, catalogId);
                    return info;
                }
            }
            duckDBService.detachCatalog(conn, catalogId);
            return null;
        } finally {
            duckDBService.releaseConnection(conn);
        }
    }

    @Observed(name = "catalog.test", contextualName = "test-catalog")
    public CatalogTestResult testCatalog(String catalogId) throws SQLException {
        Catalog catalog = getCatalogOrThrow(catalogId);
        Connection conn = duckDBService.getConnection();

        CatalogTestResult.MetadataBackendStatus metadataStatus;
        CatalogTestResult.DataStorageStatus storageStatus;
        Integer branchCount = null;
        String overallStatus = "healthy";

        try {
            long startTime = System.currentTimeMillis();
            duckDBService.attachCatalog(conn, catalog);
            double latency = System.currentTimeMillis() - startTime;

            metadataStatus = CatalogTestResult.MetadataBackendStatus.builder()
                    .status("connected")
                    .latencyMs(latency)
                    .build();

            // Get branch count
            String sql = "SELECT ducklake_branch_count('" + catalogId + "')";
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                if (rs.next()) {
                    branchCount = rs.getInt(1);
                }
            }

            storageStatus = CatalogTestResult.DataStorageStatus.builder()
                    .status("accessible")
                    .path(catalog.getDataPath())
                    .build();

            duckDBService.detachCatalog(conn, catalogId);
        } catch (SQLException e) {
            overallStatus = "unhealthy";
            metadataStatus = CatalogTestResult.MetadataBackendStatus.builder()
                    .status("error")
                    .error(e.getMessage())
                    .build();
            storageStatus = CatalogTestResult.DataStorageStatus.builder()
                    .status("error")
                    .error(e.getMessage())
                    .build();
        } finally {
            duckDBService.releaseConnection(conn);
        }

        return CatalogTestResult.builder()
                .catalogId(catalogId)
                .overallStatus(overallStatus)
                .metadataBackend(metadataStatus)
                .dataStorage(storageStatus)
                .branchCount(branchCount)
                .testedAt(Instant.now())
                .build();
    }

    @Observed(name = "branch.search", contextualName = "search-branches")
    public List<BranchInfo> searchBranches(String catalogId, SearchBranchesRequest request) throws SQLException {
        Catalog catalog = getCatalogOrThrow(catalogId);
        Connection conn = duckDBService.getConnection();
        try {
            duckDBService.attachCatalog(conn, catalog);
            List<BranchInfo> branches = new ArrayList<>();

            StringBuilder sql = new StringBuilder("SELECT * FROM ducklake_search_branches('" + catalogId + "'");
            if (request.getPattern() != null) {
                sql.append(", pattern := '").append(request.getPattern()).append("'");
            }
            if (request.getStatus() != null) {
                sql.append(", status := '").append(request.getStatus()).append("'");
            }
            sql.append(")");

            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(sql.toString())) {
                while (rs.next()) {
                    branches.add(mapToBranchInfo(rs));
                }
            }
            duckDBService.detachCatalog(conn, catalogId);
            return branches;
        } finally {
            duckDBService.releaseConnection(conn);
        }
    }

    @Observed(name = "branch.activity", contextualName = "branch-activity")
    public List<BranchInfo> getBranchActivity(String catalogId, String orderBy, int limit) throws SQLException {
        Catalog catalog = getCatalogOrThrow(catalogId);
        Connection conn = duckDBService.getConnection();
        try {
            duckDBService.attachCatalog(conn, catalog);
            List<BranchInfo> branches = new ArrayList<>();

            String sql = "SELECT * FROM ducklake_branch_activity('" + catalogId + "', " + limit + ")";
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                while (rs.next()) {
                    branches.add(mapToBranchInfo(rs));
                }
            }
            duckDBService.detachCatalog(conn, catalogId);
            return branches;
        } finally {
            duckDBService.releaseConnection(conn);
        }
    }

    @Observed(name = "branch.by-age", contextualName = "branches-by-age")
    public List<BranchInfo> getBranchesByAge(String catalogId, int days, String status) throws SQLException {
        Catalog catalog = getCatalogOrThrow(catalogId);
        Connection conn = duckDBService.getConnection();
        try {
            duckDBService.attachCatalog(conn, catalog);
            List<BranchInfo> branches = new ArrayList<>();

            String sql = "SELECT * FROM ducklake_branches_by_age('" + catalogId + "', " + days + ")";
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                while (rs.next()) {
                    branches.add(mapToBranchInfo(rs));
                }
            }
            duckDBService.detachCatalog(conn, catalogId);
            return branches;
        } finally {
            duckDBService.releaseConnection(conn);
        }
    }

    @Observed(name = "branch.current", contextualName = "current-branch")
    public BranchInfo getCurrentBranch(String catalogId) throws SQLException {
        Catalog catalog = getCatalogOrThrow(catalogId);
        Connection conn = duckDBService.getConnection();
        try {
            duckDBService.attachCatalog(conn, catalog);

            String sql = "SELECT * FROM ducklake_current_branch('" + catalogId + "')";
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                if (rs.next()) {
                    BranchInfo info = BranchInfo.builder()
                            .branchName(rs.getString("branch_name"))
                            .branchId(rs.getLong("branch_id"))
                            .headSnapshotId(rs.getLong("snapshot_id"))
                            .build();
                    duckDBService.detachCatalog(conn, catalogId);
                    return info;
                }
            }
            duckDBService.detachCatalog(conn, catalogId);
            return null;
        } finally {
            duckDBService.releaseConnection(conn);
        }
    }

    @Observed(name = "branch.use", contextualName = "use-branch")
    public BranchInfo useBranch(String catalogId, String branchName) throws SQLException {
        Catalog catalog = getCatalogOrThrow(catalogId);
        Connection conn = duckDBService.getConnection();
        try {
            duckDBService.attachCatalog(conn, catalog);

            String sql = "CALL ducklake_use_branch('" + catalogId + "', '" + branchName + "')";
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(sql);
            }

            // Return current branch info
            BranchInfo info = getCurrentBranch(catalogId);
            duckDBService.detachCatalog(conn, catalogId);
            return info;
        } finally {
            duckDBService.releaseConnection(conn);
        }
    }

    @Observed(name = "branch.diff", contextualName = "diff-branches")
    public BranchDiffResponse diffBranches(String catalogId, String baseBranch, String compareBranch) throws SQLException {
        log.info("Computing diff: catalog={}, base={}, compare={}", catalogId, baseBranch, compareBranch);
        Catalog catalog = getCatalogOrThrow(catalogId);
        Connection conn = duckDBService.getConnection();
        try {
            duckDBService.attachCatalog(conn, catalog);

            // Get schemas from base branch
            Map<String, List<TableInfo>> baseSchemas = getSchemasWithTables(conn, catalogId, baseBranch);
            log.info("Base branch {} has {} schemas: {}", baseBranch, baseSchemas.size(), baseSchemas.keySet());

            // Get schemas from compare branch
            Map<String, List<TableInfo>> compareSchemas = getSchemasWithTables(conn, catalogId, compareBranch);
            log.info("Compare branch {} has {} schemas: {}", compareBranch, compareSchemas.size(), compareSchemas.keySet());

            List<BranchDiffResponse.SchemaDiff> schemaDiffs = new ArrayList<>();
            int schemasAdded = 0, schemasRemoved = 0, schemasModified = 0;
            int tablesAdded = 0, tablesRemoved = 0, tablesModified = 0;
            int columnsAdded = 0, columnsRemoved = 0, columnsModified = 0;

            // Find all unique schema names
            Set<String> allSchemas = new HashSet<>();
            allSchemas.addAll(baseSchemas.keySet());
            allSchemas.addAll(compareSchemas.keySet());

            for (String schemaName : allSchemas) {
                boolean inBase = baseSchemas.containsKey(schemaName);
                boolean inCompare = compareSchemas.containsKey(schemaName);

                if (!inBase && inCompare) {
                    // Schema added
                    schemasAdded++;
                    List<BranchDiffResponse.TableDiff> tableDiffs = new ArrayList<>();
                    for (TableInfo table : compareSchemas.get(schemaName)) {
                        tablesAdded++;
                        List<BranchDiffResponse.ColumnDiff> colDiffs = new ArrayList<>();
                        if (table.getColumns() != null) {
                            for (ColumnInfo col : table.getColumns()) {
                                columnsAdded++;
                                colDiffs.add(BranchDiffResponse.ColumnDiff.builder()
                                        .columnName(col.getName())
                                        .status("added")
                                        .compareType(col.getType())
                                        .compareNullable(col.getNullable())
                                        .build());
                            }
                        }
                        tableDiffs.add(BranchDiffResponse.TableDiff.builder()
                                .tableName(table.getTableName())
                                .schemaName(schemaName)
                                .status("added")
                                .columns(colDiffs)
                                .build());
                    }
                    schemaDiffs.add(BranchDiffResponse.SchemaDiff.builder()
                            .schemaName(schemaName)
                            .status("added")
                            .tables(tableDiffs)
                            .build());
                } else if (inBase && !inCompare) {
                    // Schema removed
                    schemasRemoved++;
                    List<BranchDiffResponse.TableDiff> tableDiffs = new ArrayList<>();
                    for (TableInfo table : baseSchemas.get(schemaName)) {
                        tablesRemoved++;
                        List<BranchDiffResponse.ColumnDiff> colDiffs = new ArrayList<>();
                        if (table.getColumns() != null) {
                            for (ColumnInfo col : table.getColumns()) {
                                columnsRemoved++;
                                colDiffs.add(BranchDiffResponse.ColumnDiff.builder()
                                        .columnName(col.getName())
                                        .status("removed")
                                        .baseType(col.getType())
                                        .baseNullable(col.getNullable())
                                        .build());
                            }
                        }
                        tableDiffs.add(BranchDiffResponse.TableDiff.builder()
                                .tableName(table.getTableName())
                                .schemaName(schemaName)
                                .status("removed")
                                .columns(colDiffs)
                                .build());
                    }
                    schemaDiffs.add(BranchDiffResponse.SchemaDiff.builder()
                            .schemaName(schemaName)
                            .status("removed")
                            .tables(tableDiffs)
                            .build());
                } else {
                    // Schema exists in both - compare tables
                    List<TableInfo> baseTables = baseSchemas.get(schemaName);
                    List<TableInfo> compareTables = compareSchemas.get(schemaName);
                    Map<String, TableInfo> baseTableMap = new HashMap<>();
                    Map<String, TableInfo> compareTableMap = new HashMap<>();
                    baseTables.forEach(t -> baseTableMap.put(t.getTableName(), t));
                    compareTables.forEach(t -> compareTableMap.put(t.getTableName(), t));

                    List<BranchDiffResponse.TableDiff> tableDiffs = new ArrayList<>();
                    Set<String> allTables = new HashSet<>();
                    allTables.addAll(baseTableMap.keySet());
                    allTables.addAll(compareTableMap.keySet());

                    boolean schemaModified = false;
                    for (String tableName : allTables) {
                        boolean tableInBase = baseTableMap.containsKey(tableName);
                        boolean tableInCompare = compareTableMap.containsKey(tableName);

                        if (!tableInBase && tableInCompare) {
                            // Table added
                            tablesAdded++;
                            schemaModified = true;
                            TableInfo table = compareTableMap.get(tableName);
                            List<BranchDiffResponse.ColumnDiff> colDiffs = new ArrayList<>();
                            if (table.getColumns() != null) {
                                for (ColumnInfo col : table.getColumns()) {
                                    columnsAdded++;
                                    colDiffs.add(BranchDiffResponse.ColumnDiff.builder()
                                            .columnName(col.getName())
                                            .status("added")
                                            .compareType(col.getType())
                                            .compareNullable(col.getNullable())
                                            .build());
                                }
                            }
                            tableDiffs.add(BranchDiffResponse.TableDiff.builder()
                                    .tableName(tableName)
                                    .schemaName(schemaName)
                                    .status("added")
                                    .columns(colDiffs)
                                    .build());
                        } else if (tableInBase && !tableInCompare) {
                            // Table removed
                            tablesRemoved++;
                            schemaModified = true;
                            TableInfo table = baseTableMap.get(tableName);
                            List<BranchDiffResponse.ColumnDiff> colDiffs = new ArrayList<>();
                            if (table.getColumns() != null) {
                                for (ColumnInfo col : table.getColumns()) {
                                    columnsRemoved++;
                                    colDiffs.add(BranchDiffResponse.ColumnDiff.builder()
                                            .columnName(col.getName())
                                            .status("removed")
                                            .baseType(col.getType())
                                            .baseNullable(col.getNullable())
                                            .build());
                                }
                            }
                            tableDiffs.add(BranchDiffResponse.TableDiff.builder()
                                    .tableName(tableName)
                                    .schemaName(schemaName)
                                    .status("removed")
                                    .columns(colDiffs)
                                    .build());
                        } else {
                            // Table in both - compare columns
                            TableInfo baseTable = baseTableMap.get(tableName);
                            TableInfo compareTable = compareTableMap.get(tableName);
                            List<BranchDiffResponse.ColumnDiff> colDiffs = compareColumns(
                                    baseTable.getColumns(), compareTable.getColumns());

                            // Count column changes
                            boolean tableModified = false;
                            for (BranchDiffResponse.ColumnDiff cd : colDiffs) {
                                if ("added".equals(cd.getStatus())) { columnsAdded++; tableModified = true; }
                                if ("removed".equals(cd.getStatus())) { columnsRemoved++; tableModified = true; }
                                if ("modified".equals(cd.getStatus())) { columnsModified++; tableModified = true; }
                            }

                            if (tableModified) {
                                tablesModified++;
                                schemaModified = true;
                                tableDiffs.add(BranchDiffResponse.TableDiff.builder()
                                        .tableName(tableName)
                                        .schemaName(schemaName)
                                        .status("modified")
                                        .columns(colDiffs)
                                        .build());
                            }
                        }
                    }

                    if (schemaModified) {
                        schemasModified++;
                        schemaDiffs.add(BranchDiffResponse.SchemaDiff.builder()
                                .schemaName(schemaName)
                                .status("modified")
                                .tables(tableDiffs)
                                .build());
                    }
                }
            }

            duckDBService.detachCatalog(conn, catalogId);

            return BranchDiffResponse.builder()
                    .baseBranch(baseBranch)
                    .compareBranch(compareBranch)
                    .schemas(schemaDiffs)
                    .summary(BranchDiffResponse.DiffSummary.builder()
                            .schemasAdded(schemasAdded)
                            .schemasRemoved(schemasRemoved)
                            .schemasModified(schemasModified)
                            .tablesAdded(tablesAdded)
                            .tablesRemoved(tablesRemoved)
                            .tablesModified(tablesModified)
                            .columnsAdded(columnsAdded)
                            .columnsRemoved(columnsRemoved)
                            .columnsModified(columnsModified)
                            .build())
                    .build();
        } finally {
            duckDBService.releaseConnection(conn);
        }
    }

    private Map<String, List<TableInfo>> getSchemasWithTables(Connection conn, String catalogId, String branch) throws SQLException {
        Map<String, List<TableInfo>> result = new HashMap<>();

        // Set branch context
        String useBranchSql = String.format("CALL ducklake_use_branch('%s', '%s')",
                catalogId.replace("'", "''"), branch.replace("'", "''"));
        log.debug("Setting branch context for diff: catalog={}, branch={}", catalogId, branch);
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(useBranchSql);
        }

        // Get schemas
        List<String> schemaNames = new ArrayList<>();
        String schemasSql = "SELECT schema_name FROM information_schema.schemata WHERE catalog_name = ?";
        try (PreparedStatement stmt = conn.prepareStatement(schemasSql)) {
            stmt.setString(1, catalogId);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    schemaNames.add(rs.getString("schema_name"));
                }
            }
        }
        log.debug("Found {} schemas for branch {}: {}", schemaNames.size(), branch, schemaNames);

        // Initialize result map
        for (String schemaName : schemaNames) {
            result.put(schemaName, new ArrayList<>());
        }

        // Get tables for each schema - collect table names first, then fetch columns
        for (String schemaName : schemaNames) {
            List<String> tableNames = new ArrayList<>();
            String tablesSql = "SELECT table_name FROM information_schema.tables WHERE table_catalog = ? AND table_schema = ?";
            try (PreparedStatement stmt = conn.prepareStatement(tablesSql)) {
                stmt.setString(1, catalogId);
                stmt.setString(2, schemaName);
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        tableNames.add(rs.getString("table_name"));
                    }
                }
            }

            // Now fetch columns for each table (after ResultSet is closed)
            List<TableInfo> tables = new ArrayList<>();
            for (String tableName : tableNames) {
                List<ColumnInfo> columns = fetchColumnsForTable(conn, catalogId, schemaName, tableName);
                tables.add(TableInfo.builder()
                        .tableName(tableName)
                        .schemaName(schemaName)
                        .catalogName(catalogId)
                        .columns(columns)
                        .build());
            }
            result.put(schemaName, tables);
        }

        return result;
    }

    private List<BranchDiffResponse.ColumnDiff> compareColumns(List<ColumnInfo> baseCols, List<ColumnInfo> compareCols) {
        List<BranchDiffResponse.ColumnDiff> diffs = new ArrayList<>();

        Map<String, ColumnInfo> baseColMap = new HashMap<>();
        Map<String, ColumnInfo> compareColMap = new HashMap<>();
        if (baseCols != null) baseCols.forEach(c -> baseColMap.put(c.getName(), c));
        if (compareCols != null) compareCols.forEach(c -> compareColMap.put(c.getName(), c));

        Set<String> allCols = new HashSet<>();
        allCols.addAll(baseColMap.keySet());
        allCols.addAll(compareColMap.keySet());

        for (String colName : allCols) {
            boolean inBase = baseColMap.containsKey(colName);
            boolean inCompare = compareColMap.containsKey(colName);

            if (!inBase && inCompare) {
                ColumnInfo col = compareColMap.get(colName);
                diffs.add(BranchDiffResponse.ColumnDiff.builder()
                        .columnName(colName)
                        .status("added")
                        .compareType(col.getType())
                        .compareNullable(col.getNullable())
                        .build());
            } else if (inBase && !inCompare) {
                ColumnInfo col = baseColMap.get(colName);
                diffs.add(BranchDiffResponse.ColumnDiff.builder()
                        .columnName(colName)
                        .status("removed")
                        .baseType(col.getType())
                        .baseNullable(col.getNullable())
                        .build());
            } else {
                // Both have column - check for modifications
                ColumnInfo baseCol = baseColMap.get(colName);
                ColumnInfo compareCol = compareColMap.get(colName);
                boolean typeChanged = !Objects.equals(baseCol.getType(), compareCol.getType());
                boolean nullableChanged = !Objects.equals(baseCol.getNullable(), compareCol.getNullable());

                if (typeChanged || nullableChanged) {
                    diffs.add(BranchDiffResponse.ColumnDiff.builder()
                            .columnName(colName)
                            .status("modified")
                            .baseType(baseCol.getType())
                            .compareType(compareCol.getType())
                            .baseNullable(baseCol.getNullable())
                            .compareNullable(compareCol.getNullable())
                            .build());
                }
            }
        }

        return diffs;
    }

    /**
     * Get catalog-wide statistics (physical stats, not branch-specific).
     */
    @Observed(name = "discovery.catalog-stats", contextualName = "get-catalog-stats")
    public CatalogStats getCatalogStats(String catalogId) throws SQLException {
        Catalog catalog = getCatalogOrThrow(catalogId);

        if (catalog.getCatalogType() != Catalog.CatalogType.DUCKLAKE) {
            throw new CatalogException("Catalog stats only available for DuckLake catalogs");
        }

        Connection conn = duckDBService.getConnection();
        try {
            duckDBService.attachCatalog(conn, catalog);

            String sql = "SELECT * FROM ducklake_catalog_stats('" + catalogId.replace("'", "''") + "')";

            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                if (rs.next()) {
                    CatalogStats stats = CatalogStats.builder()
                            .catalogName(rs.getString("catalog_name"))
                            .branchCount(rs.getLong("branch_count"))
                            .activeBranchCount(rs.getLong("active_branch_count"))
                            .schemaCount(rs.getLong("schema_count"))
                            .tableCount(rs.getLong("table_count"))
                            .viewCount(rs.getLong("view_count"))
                            .dataFileCount(rs.getLong("data_file_count"))
                            .deleteFileCount(rs.getLong("delete_file_count"))
                            .totalRows(rs.getLong("total_rows"))
                            .totalSizeBytes(rs.getLong("total_size_bytes"))
                            .snapshotCount(rs.getLong("snapshot_count"))
                            .build();
                    duckDBService.detachCatalog(conn, catalogId);
                    return stats;
                }
            }

            duckDBService.detachCatalog(conn, catalogId);
            throw new CatalogException("Failed to get catalog stats");
        } finally {
            duckDBService.releaseConnection(conn);
        }
    }

    private Catalog getCatalogOrThrow(String catalogId) {
        return catalogService.getCatalog(catalogId)
                .orElseThrow(() -> CatalogException.notFound(catalogId));
    }

    private BranchInfo mapToBranchInfo(ResultSet rs) throws SQLException {
        return BranchInfo.builder()
                .branchId(rs.getLong("branch_id"))
                .branchName(rs.getString("branch_name"))
                .headSnapshotId(rs.getLong("snapshot_id"))
                .isActive(rs.getBoolean("is_active"))
                .createdAt(rs.getTimestamp("created_at") != null ? rs.getTimestamp("created_at").toInstant() : null)
                .build();
    }
}
