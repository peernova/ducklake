#include "ducklake_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "storage/ducklake_storage.hpp"
#include "functions/ducklake_table_functions.hpp"
#include "storage/ducklake_secret.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	loader.SetDescription("Adds support for DuckLake, SQL as a Lakehouse Format");

	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	config.storage_extensions["ducklake"] = make_uniq<DuckLakeStorageExtension>();

	config.AddExtensionOption("ducklake_max_retry_count",
	                          "The maximum amount of retry attempts for a ducklake transaction", LogicalType::UBIGINT,
	                          Value::UBIGINT(10), nullptr, SetScope::GLOBAL);
	config.AddExtensionOption("ducklake_retry_wait_ms", "Time between retries", LogicalType::UBIGINT,
	                          Value::UBIGINT(100), nullptr, SetScope::GLOBAL);
	config.AddExtensionOption("ducklake_retry_backoff", "Backoff factor for exponentially increasing retry wait time",
	                          LogicalType::DOUBLE, Value::DOUBLE(1.5), nullptr, SetScope::GLOBAL);

	DuckLakeSnapshotsFunction snapshots;
	loader.RegisterFunction(snapshots);

	DuckLakeTableInfoFunction table_info;
	loader.RegisterFunction(table_info);

	auto table_insertions = DuckLakeTableInsertionsFunction::GetFunctions();
	loader.RegisterFunction(table_insertions);

	auto table_deletions = DuckLakeTableDeletionsFunction::GetFunctions();
	loader.RegisterFunction(table_deletions);

	auto merge_adjacent_files = DuckLakeMergeAdjacentFilesFunction::GetFunctions();
	loader.RegisterFunction(merge_adjacent_files);

	auto rewrite_files = DuckLakeRewriteDataFilesFunction::GetFunctions();
	loader.RegisterFunction(rewrite_files);

	DuckLakeCleanupOldFilesFunction cleanup_old_files;
	loader.RegisterFunction(cleanup_old_files);

	DuckLakeCleanupOrphanedFilesFunction cleanup_orphaned_files;
	loader.RegisterFunction(cleanup_orphaned_files);

	DuckLakeExpireSnapshotsFunction expire_snapshots;
	loader.RegisterFunction(expire_snapshots);

	DuckLakeFlushInlinedDataFunction flush_inlined_data;
	loader.RegisterFunction(flush_inlined_data);

	DuckLakeSetOptionFunction set_options;
	loader.RegisterFunction(set_options);

	DuckLakeOptionsFunction options;
	loader.RegisterFunction(options);

	DuckLakeSetCommitMessage set_commit_message;
	loader.RegisterFunction(set_commit_message);

	auto table_changes = DuckLakeTableInsertionsFunction::GetDuckLakeTableChanges();
	loader.RegisterFunction(*table_changes);

	DuckLakeListFilesFunction list_files;
	loader.RegisterFunction(list_files);

	auto add_files = DuckLakeAddDataFilesFunction::GetFunctions();
	loader.RegisterFunction(add_files);

	DuckLakeCurrentSnapshotFunction current_snapshot;
	loader.RegisterFunction(current_snapshot);

	DuckLakeLastCommittedSnapshotFunction last_committed;
	loader.RegisterFunction(last_committed);

	// Branch functions
	auto list_branches = DuckLakeListBranchesFunction::GetFunction();
	loader.RegisterFunction(list_branches);

	auto create_branch = DuckLakeCreateBranchFunction::GetFunctions();
	loader.RegisterFunction(create_branch);

	auto delete_branch = DuckLakeDeleteBranchFunction::GetFunction();
	loader.RegisterFunction(delete_branch);

	auto branch_lineage = DuckLakeBranchLineageFunction::GetFunction();
	loader.RegisterFunction(branch_lineage);

	auto use_branch = DuckLakeUseBranchFunction::GetFunction();
	loader.RegisterFunction(use_branch);

	auto current_branch = DuckLakeCurrentBranchFunction::GetFunction();
	loader.RegisterFunction(current_branch);

	auto branch_count = DuckLakeBranchCountFunction::GetFunctions();
	loader.RegisterFunction(branch_count);

	auto branch_stats = DuckLakeBranchStatsFunction::GetFunction();
	loader.RegisterFunction(branch_stats);

	auto catalog_stats = DuckLakeCatalogStatsFunction::GetFunction();
	loader.RegisterFunction(catalog_stats);

	auto search_branches = DuckLakeSearchBranchesFunction::GetFunctions();
	loader.RegisterFunction(search_branches);

	auto branches_by_status = DuckLakeBranchesByStatusFunction::GetFunction();
	loader.RegisterFunction(branches_by_status);

	auto branches_by_age = DuckLakeBranchesByAgeFunction::GetFunctions();
	loader.RegisterFunction(branches_by_age);

	auto branch_activity = DuckLakeBranchActivityFunction::GetFunctions();
	loader.RegisterFunction(branch_activity);

	auto common_ancestor = DuckLakeCommonAncestorFunction::GetFunction();
	loader.RegisterFunction(common_ancestor);

	auto branch_changes_summary = DuckLakeBranchChangesSummaryFunction::GetFunction();
	loader.RegisterFunction(branch_changes_summary);

	// Query analysis functions
	auto analyze_query = DuckLakeAnalyzeQueryFunction::GetFunction();
	loader.RegisterFunction(analyze_query);

	auto rewrite_query = DuckLakeRewriteQueryFunction::GetFunctions();
	loader.RegisterFunction(rewrite_query);

	// secrets
	auto secret_type = DuckLakeSecret::GetSecretType();
	loader.RegisterSecretType(secret_type);

	auto ducklake_secret_function = DuckLakeSecret::GetFunction();
	loader.RegisterFunction(ducklake_secret_function);
}

void DucklakeExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string DucklakeExtension::Name() {
	return "ducklake";
}

std::string DucklakeExtension::Version() const {
#ifdef EXT_VERSION_DUCKLAKE
	return EXT_VERSION_DUCKLAKE;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(ducklake, loader) {
	LoadInternal(loader);
}
}
