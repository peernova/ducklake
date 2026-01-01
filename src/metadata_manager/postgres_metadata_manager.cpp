#include "metadata_manager/postgres_metadata_manager.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_transaction.hpp"

namespace duckdb {

PostgresMetadataManager::PostgresMetadataManager(DuckLakeTransaction &transaction)
    : DuckLakeMetadataManager(transaction) {
}

bool PostgresMetadataManager::TypeIsNativelySupported(const LogicalType &type) {
	switch (type.id()) {
	// Unnamed composite types are not supported.
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::MAP:
	case LogicalTypeId::LIST:
		return false;
	default:
		return true;
	}
}

string PostgresMetadataManager::GetLatestSnapshotQuery() const {
	// For per-branch snapshot IDs, get the latest snapshot for the current working branch
	// If the branch has no snapshots yet (newly forked), walk up the lineage to find an ancestor
	// that has the snapshot at fork_snapshot_id
	auto &catalog = transaction.GetCatalog();
	auto working_branch = catalog.GetWorkingBranch();

	// Query that handles:
	// 1. Branches with their own snapshots (use latest own snapshot)
	// 2. Newly forked branches (use ancestor's snapshot via lineage table)
	// The lineage table lets us find the right ancestor even for deeply nested new branches
	// TODO: Performance - LATERAL JOIN walks lineage table which is O(branch_depth).
	// Currently acceptable since branch hierarchies are shallow (typically 1-5 levels).
	// If deep hierarchies become common, consider: caching, or storing direct reference to
	// the ancestor that owns the fork_snapshot_id in ducklake_branch table.
	return StringUtil::Format(R"(
	SELECT * FROM postgres_query({METADATA_CATALOG_NAME_LITERAL},
		'SELECT
		    COALESCE(own.snapshot_id, b.fork_snapshot_id) as snapshot_id,
		    COALESCE(own.schema_version, ancestor_snap.schema_version) as schema_version,
		    COALESCE(own.next_catalog_id, ancestor_snap.next_catalog_id) as next_catalog_id,
		    COALESCE(own.next_file_id, ancestor_snap.next_file_id) as next_file_id
		FROM {METADATA_SCHEMA_ESCAPED}.ducklake_branch b
		LEFT JOIN (
		    SELECT snapshot_id, schema_version, next_catalog_id, next_file_id
		    FROM {METADATA_SCHEMA_ESCAPED}.ducklake_snapshot
		    WHERE branch_id = %d
		    ORDER BY snapshot_id DESC
		    LIMIT 1
		) own ON true
		LEFT JOIN LATERAL (
		    -- Find an ancestor branch that has the fork_snapshot_id
		    -- Walk up lineage to find the branch that actually has the snapshot
		    SELECT s.schema_version, s.next_catalog_id, s.next_file_id
		    FROM {METADATA_SCHEMA_ESCAPED}.ducklake_branch_lineage bl
		    JOIN {METADATA_SCHEMA_ESCAPED}.ducklake_snapshot s
		        ON s.branch_id = bl.ancestor_branch_id AND s.snapshot_id = b.fork_snapshot_id
		    WHERE bl.branch_id = %d AND bl.ancestor_branch_id != %d
		    LIMIT 1
		) ancestor_snap ON true
		WHERE b.branch_id = %d;')
	)", working_branch.index, working_branch.index, working_branch.index, working_branch.index);
}

} // namespace duckdb
