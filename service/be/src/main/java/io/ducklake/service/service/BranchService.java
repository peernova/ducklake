package io.ducklake.service.service;

import io.ducklake.service.exception.BranchException;
import io.ducklake.service.exception.CatalogException;
import io.ducklake.service.model.Catalog;
import io.ducklake.service.model.dto.*;
import io.micrometer.observation.annotation.Observed;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.*;

@Service
@Slf4j
@RequiredArgsConstructor
public class BranchService {

    private final DuckDBService duckDBService;
    private final CatalogService catalogService;

    /**
     * List all branches for a catalog.
     */
    @Observed(name = "branch.list", contextualName = "list-branches")
    public List<BranchInfo> listBranches(String catalogId) throws SQLException {
        Catalog catalog = getCatalogOrThrow(catalogId);
        Connection conn = duckDBService.getConnection();

        try {
            duckDBService.attachCatalog(conn, catalog);

            List<BranchInfo> branches = new ArrayList<>();
            // Use ducklake_branches_by_status which includes created_at
            String sql = "SELECT * FROM ducklake_branches_by_status('" + catalogId + "', 'active')";

            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                while (rs.next()) {
                    branches.add(mapToBranchInfoWithTimestamp(rs));
                }
            }

            // Build branchId -> branchName map to resolve parent names
            Map<Long, String> branchIdToName = new HashMap<>();
            for (BranchInfo branch : branches) {
                branchIdToName.put(branch.getBranchId(), branch.getBranchName());
            }

            // Resolve parent branch names
            for (BranchInfo branch : branches) {
                if (branch.getParentBranchId() != null) {
                    String parentName = branchIdToName.get(branch.getParentBranchId());
                    branch.setParentBranchName(parentName);
                }
            }

            duckDBService.detachCatalog(conn, catalogId);
            return branches;
        } finally {
            duckDBService.releaseConnection(conn);
        }
    }

    /**
     * Get branch info by name.
     */
    public Optional<BranchInfo> getBranch(String catalogId, String branchName) throws SQLException {
        Catalog catalog = getCatalogOrThrow(catalogId);
        Connection conn = duckDBService.getConnection();

        try {
            duckDBService.attachCatalog(conn, catalog);

            String sql = "SELECT * FROM ducklake_branches('" + catalogId + "') WHERE branch_name = ?";

            try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                stmt.setString(1, branchName);
                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        BranchInfo info = mapToBranchInfo(rs);
                        duckDBService.detachCatalog(conn, catalogId);
                        return Optional.of(info);
                    }
                }
            }

            duckDBService.detachCatalog(conn, catalogId);
            return Optional.empty();
        } finally {
            duckDBService.releaseConnection(conn);
        }
    }

    /**
     * Create a new branch.
     */
    @Observed(name = "branch.create", contextualName = "create-branch")
    public BranchInfo createBranch(String catalogId, BranchRequest request) throws SQLException {
        Catalog catalog = getCatalogOrThrow(catalogId);
        Connection conn = duckDBService.getConnection();

        try {
            duckDBService.attachCatalog(conn, catalog);

            String sql;
            if (request.getFromSnapshot() != null) {
                sql = String.format(
                    "CALL ducklake_create_branch('%s', '%s', '%s', %d)",
                    catalogId, request.getBranchName(), request.getFromBranch(), request.getFromSnapshot()
                );
            } else {
                sql = String.format(
                    "CALL ducklake_create_branch('%s', '%s', '%s')",
                    catalogId, request.getBranchName(), request.getFromBranch()
                );
            }

            try (Statement stmt = conn.createStatement()) {
                stmt.execute(sql);
            }

            log.info("Created branch {} from {} in catalog {}", request.getBranchName(), request.getFromBranch(), catalogId);

            // Fetch the created branch
            Optional<BranchInfo> created = getBranchInternal(conn, catalogId, request.getBranchName());
            duckDBService.detachCatalog(conn, catalogId);

            return created.orElseThrow(() -> BranchException.notFound(catalogId, request.getBranchName()));
        } finally {
            duckDBService.releaseConnection(conn);
        }
    }

    /**
     * Delete a branch.
     */
    public void deleteBranch(String catalogId, String branchName) throws SQLException {
        Catalog catalog = getCatalogOrThrow(catalogId);
        Connection conn = duckDBService.getConnection();

        try {
            duckDBService.attachCatalog(conn, catalog);

            String sql = String.format("CALL ducklake_delete_branch('%s', '%s')", catalogId, branchName);

            try (Statement stmt = conn.createStatement()) {
                stmt.execute(sql);
            }

            log.info("Deleted branch {} from catalog {}", branchName, catalogId);
            duckDBService.detachCatalog(conn, catalogId);
        } finally {
            duckDBService.releaseConnection(conn);
        }
    }

    /**
     * Get branch statistics.
     */
    public BranchStats getBranchStats(String catalogId, String branchName) throws SQLException {
        Catalog catalog = getCatalogOrThrow(catalogId);
        Connection conn = duckDBService.getConnection();

        try {
            duckDBService.attachCatalog(conn, catalog);

            // Get basic stats
            String sql = "SELECT * FROM ducklake_branch_stats('" + catalogId + "', '" + branchName + "')";
            BranchStats stats = null;

            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                if (rs.next()) {
                    stats = mapToBranchStats(rs);
                }
            }

            if (stats == null) {
                duckDBService.detachCatalog(conn, catalogId);
                throw BranchException.notFound(catalogId, branchName);
            }

            // Get last_modified_at from branch_activity
            String activitySql = "SELECT last_modified_at FROM ducklake_branch_activity('" + catalogId + "') WHERE branch_name = '" + branchName + "'";
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(activitySql)) {
                if (rs.next()) {
                    java.sql.Timestamp lastModified = rs.getTimestamp("last_modified_at");
                    if (lastModified != null) {
                        stats.setLastModifiedAt(lastModified.toInstant());
                    }
                }
            }

            duckDBService.detachCatalog(conn, catalogId);
            return stats;
        } finally {
            duckDBService.releaseConnection(conn);
        }
    }

    /**
     * Get branch lineage (parent chain).
     */
    public List<BranchInfo> getBranchLineage(String catalogId, String branchName) throws SQLException {
        Catalog catalog = getCatalogOrThrow(catalogId);
        Connection conn = duckDBService.getConnection();

        try {
            duckDBService.attachCatalog(conn, catalog);

            List<BranchInfo> lineage = new ArrayList<>();
            String sql = "SELECT * FROM ducklake_branch_lineage('" + catalogId + "', '" + branchName + "')";

            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {
                while (rs.next()) {
                    lineage.add(mapToBranchInfo(rs));
                }
            }

            duckDBService.detachCatalog(conn, catalogId);
            return lineage;
        } finally {
            duckDBService.releaseConnection(conn);
        }
    }

    /**
     * Count branches in a catalog.
     */
    public long countBranches(String catalogId) throws SQLException {
        List<BranchInfo> branches = listBranches(catalogId);
        return branches.stream().filter(b -> Boolean.TRUE.equals(b.getIsActive())).count();
    }

    private Optional<BranchInfo> getBranchInternal(Connection conn, String catalogId, String branchName) throws SQLException {
        String sql = "SELECT * FROM ducklake_branches('" + catalogId + "') WHERE branch_name = ?";

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, branchName);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(mapToBranchInfo(rs));
                }
            }
        }
        return Optional.empty();
    }

    private Catalog getCatalogOrThrow(String catalogId) {
        return catalogService.getCatalog(catalogId)
                .orElseThrow(() -> CatalogException.notFound(catalogId));
    }

    private BranchInfo mapToBranchInfo(ResultSet rs) throws SQLException {
        // ducklake_branches returns: branch_id, branch_name, parent_branch_id, fork_snapshot_id, head_snapshot_id, status
        String status = rs.getString("status");
        return BranchInfo.builder()
                .branchId(rs.getLong("branch_id"))
                .branchName(rs.getString("branch_name"))
                .parentBranchId(rs.getObject("parent_branch_id") != null ? rs.getLong("parent_branch_id") : null)
                .forkSnapshotId(rs.getObject("fork_snapshot_id") != null ? rs.getLong("fork_snapshot_id") : null)
                .headSnapshotId(rs.getLong("head_snapshot_id"))
                .status(status)
                .isActive("active".equals(status))
                .build();
    }

    private BranchInfo mapToBranchInfoWithTimestamp(ResultSet rs) throws SQLException {
        // ducklake_branches_by_status returns: branch_id, branch_name, parent_branch_id, fork_snapshot_id, head_snapshot_id, created_at, status
        String status = rs.getString("status");
        java.sql.Timestamp createdAt = rs.getTimestamp("created_at");
        return BranchInfo.builder()
                .branchId(rs.getLong("branch_id"))
                .branchName(rs.getString("branch_name"))
                .parentBranchId(rs.getObject("parent_branch_id") != null ? rs.getLong("parent_branch_id") : null)
                .forkSnapshotId(rs.getObject("fork_snapshot_id") != null ? rs.getLong("fork_snapshot_id") : null)
                .headSnapshotId(rs.getLong("head_snapshot_id"))
                .createdAt(createdAt != null ? createdAt.toInstant() : null)
                .status(status)
                .isActive("active".equals(status))
                .build();
    }

    private BranchStats mapToBranchStats(ResultSet rs) throws SQLException {
        // ducklake_branch_stats returns: branch_name, table_count, schema_count, view_count,
        // data_file_count, total_rows, total_size_bytes, snapshot_count
        return BranchStats.builder()
                .branchName(rs.getString("branch_name"))
                .tableCount(rs.getLong("table_count"))
                .schemaCount(rs.getLong("schema_count"))
                .viewCount(rs.getLong("view_count"))
                .dataFileCount(rs.getLong("data_file_count"))
                .totalRows(rs.getLong("total_rows"))
                .totalSizeBytes(rs.getLong("total_size_bytes"))
                .snapshotCount(rs.getLong("snapshot_count"))
                .build();
    }
}
