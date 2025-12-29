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
	auto &catalog = transaction.GetCatalog();
	auto working_branch = catalog.GetWorkingBranch();

	return StringUtil::Format(R"(
	SELECT * FROM postgres_query({METADATA_CATALOG_NAME_LITERAL},
		'SELECT snapshot_id, schema_version, next_catalog_id, next_file_id
		 FROM {METADATA_SCHEMA_ESCAPED}.ducklake_snapshot
		 WHERE branch_id = %d AND snapshot_id = (
		     SELECT MAX(snapshot_id) FROM {METADATA_SCHEMA_ESCAPED}.ducklake_snapshot WHERE branch_id = %d
		 );')
	)", working_branch.index, working_branch.index);
}

} // namespace duckdb
