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

    /**
     * Get changes between two branches (simplified version using DuckDB functions).
     * Uses ducklake_common_ancestor and ducklake_branch_changes_summary.
     */
    @Observed(name = "branch.changes.summary", contextualName = "get-branch-changes-summary")
    public BranchChangesResponse getBranchChanges(String catalogId, String baseBranch, String compareBranch,
                                                   int limit, int offset) throws SQLException {
        Catalog catalog = getCatalogOrThrow(catalogId);
        Connection conn = duckDBService.getConnection();

        try {
            duckDBService.attachCatalog(conn, catalog);

            // Step 1: Get common ancestor using DuckDB function
            BranchChangesResponse.CommonAncestor commonAncestor = getCommonAncestor(conn, catalogId, baseBranch, compareBranch);

            // Step 2: Get branch IDs
            long compareBranchId = getBranchId(conn, catalogId, compareBranch);
            long baseBranchId = getBranchId(conn, catalogId, baseBranch);

            // Step 3: Get aggregated summaries for compare branch
            BranchChangesResponse.BranchSummarySet compareSummary = getBranchChangesSummary(
                    conn, catalogId, compareBranch, compareBranchId, commonAncestor.getSnapshotId());

            // Step 4: Get count for base branch
            int baseCount = getChangesCountSince(conn, catalogId, baseBranch, commonAncestor.getSnapshotId());
            BranchChangesResponse.BranchChangeSummary baseSummary = BranchChangesResponse.BranchChangeSummary.builder()
                    .branchName(baseBranch)
                    .branchId(baseBranchId)
                    .totalChanges(baseCount)
                    .build();

            duckDBService.detachCatalog(conn, catalogId);

            return BranchChangesResponse.builder()
                    .commonAncestor(commonAncestor)
                    .compareBranchSummary(compareSummary)
                    .baseBranchSummary(baseSummary)
                    .build();
        } finally {
            duckDBService.releaseConnection(conn);
        }
    }

    /**
     * Get just the counts of changes between two branches.
     */
    public BranchChangesResponse getBranchChangesCount(String catalogId, String baseBranch, String compareBranch) throws SQLException {
        Catalog catalog = getCatalogOrThrow(catalogId);
        Connection conn = duckDBService.getConnection();

        try {
            duckDBService.attachCatalog(conn, catalog);

            // Get common ancestor
            BranchChangesResponse.CommonAncestor commonAncestor = getCommonAncestor(conn, catalogId, baseBranch, compareBranch);

            // Get branch IDs
            long compareBranchId = getBranchId(conn, catalogId, compareBranch);
            long baseBranchId = getBranchId(conn, catalogId, baseBranch);

            // Get counts only
            int compareCount = getChangesCountSince(conn, catalogId, compareBranch, commonAncestor.getSnapshotId());
            int baseCount = getChangesCountSince(conn, catalogId, baseBranch, commonAncestor.getSnapshotId());

            duckDBService.detachCatalog(conn, catalogId);

            return BranchChangesResponse.builder()
                    .commonAncestor(commonAncestor)
                    .compareBranchSummary(BranchChangesResponse.BranchSummarySet.builder()
                            .branchName(compareBranch)
                            .branchId(compareBranchId)
                            .totalChanges(compareCount)
                            .build())
                    .baseBranchSummary(BranchChangesResponse.BranchChangeSummary.builder()
                            .branchName(baseBranch)
                            .branchId(baseBranchId)
                            .totalChanges(baseCount)
                            .build())
                    .build();
        } finally {
            duckDBService.releaseConnection(conn);
        }
    }

    /**
     * Get common ancestor using ducklake_common_ancestor function.
     */
    private BranchChangesResponse.CommonAncestor getCommonAncestor(Connection conn, String catalogId,
                                                                    String branch1, String branch2) throws SQLException {
        String sql = String.format(
                "SELECT * FROM ducklake_common_ancestor('%s', '%s', '%s')",
                catalogId, branch1, branch2);

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                java.sql.Timestamp snapshotTime = rs.getTimestamp("common_snapshot_time");
                return BranchChangesResponse.CommonAncestor.builder()
                        .branchId(rs.getLong("ancestor_branch_id"))
                        .branchName(rs.getString("ancestor_branch_name"))
                        .snapshotId(rs.getLong("common_snapshot_id"))
                        .snapshotTime(snapshotTime != null ? snapshotTime.toInstant() : null)
                        .build();
            }
        }
        throw new SQLException("No common ancestor found between branches: " + branch1 + " and " + branch2);
    }

    /**
     * Get branch ID by name using ducklake_branches function.
     */
    private long getBranchId(Connection conn, String catalogId, String branchName) throws SQLException {
        String sql = String.format(
                "SELECT branch_id FROM ducklake_branches('%s') WHERE branch_name = ? AND status = 'active'",
                catalogId);
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, branchName);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getLong("branch_id");
                }
            }
        }
        throw new SQLException("Branch not found: " + branchName);
    }

    /**
     * Get aggregated changes summary using ducklake_branch_changes_summary function.
     */
    private BranchChangesResponse.BranchSummarySet getBranchChangesSummary(Connection conn, String catalogId,
                                                                            String branchName, long branchId,
                                                                            long sinceSnapshot) throws SQLException {
        List<ChangeSummaryItem> summaries = new ArrayList<>();
        int totalChanges = 0;

        String sql = String.format(
                "SELECT * FROM ducklake_branch_changes_summary('%s', '%s', %d)",
                catalogId, branchName, sinceSnapshot);

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                long count = rs.getLong("change_count");
                totalChanges += count;
                summaries.add(ChangeSummaryItem.builder()
                        .changeType(rs.getString("change_type"))
                        .schemaName(rs.getString("schema_name"))
                        .tableName(rs.getString("table_name"))
                        .changeCount(count)
                        .build());
            }
        }

        return BranchChangesResponse.BranchSummarySet.builder()
                .branchName(branchName)
                .branchId(branchId)
                .totalChanges(totalChanges)
                .summaries(summaries)
                .build();
    }

    /**
     * Get count of changes since a specific snapshot by summing up the summary.
     */
    private int getChangesCountSince(Connection conn, String catalogId, String branchName, long sinceSnapshot) throws SQLException {
        String sql = String.format(
                "SELECT COALESCE(SUM(change_count), 0) as cnt FROM ducklake_branch_changes_summary('%s', '%s', %d)",
                catalogId, branchName, sinceSnapshot);

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                return rs.getInt("cnt");
            }
        }
        return 0;
    }
}
