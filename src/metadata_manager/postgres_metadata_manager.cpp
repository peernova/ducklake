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

	// If the branch has no snapshots yet (newly forked), use the fork_snapshot_id from the parent

	auto &catalog = transaction.GetCatalog();

	auto working_branch = catalog.GetWorkingBranch();

 

	return StringUtil::Format(R"(

	SELECT * FROM postgres_query({METADATA_CATALOG_NAME_LITERAL},

		'SELECT

		    COALESCE(own.snapshot_id, b.fork_snapshot_id) as snapshot_id,

		    COALESCE(own.schema_version, parent.schema_version) as schema_version,

		    COALESCE(own.next_catalog_id, parent.next_catalog_id) as next_catalog_id,

		    COALESCE(own.next_file_id, parent.next_file_id) as next_file_id

		FROM {METADATA_SCHEMA_ESCAPED}.ducklake_branch b

		LEFT JOIN (

		    SELECT snapshot_id, schema_version, next_catalog_id, next_file_id

		    FROM {METADATA_SCHEMA_ESCAPED}.ducklake_snapshot

		    WHERE branch_id = %d

		    ORDER BY snapshot_id DESC

		    LIMIT 1

		) own ON true

		LEFT JOIN {METADATA_SCHEMA_ESCAPED}.ducklake_snapshot parent

		    ON parent.branch_id = b.parent_branch_id AND parent.snapshot_id = b.fork_snapshot_id

		WHERE b.branch_id = %d;')

	)", working_branch.index, working_branch.index);
}

} // namespace duckdb
