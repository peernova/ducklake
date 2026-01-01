//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_branch_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/mutex.hpp"
#include "common/ducklake_branch.hpp"
#include "common/ducklake_snapshot.hpp"
#include <unordered_map>

namespace duckdb {

class DuckLakeTransaction;
class DuckLakeMetadataManager;

//! The branch manager handles branch operations and lineage caching
class DuckLakeBranchManager {
public:
	explicit DuckLakeBranchManager(DuckLakeMetadataManager &metadata_manager);
	~DuckLakeBranchManager();

	//! Get branch info by ID
	DuckLakeBranchInfo GetBranch(DuckLakeTransaction &transaction, BranchIndex branch_id);

	//! Get branch info by name
	DuckLakeBranchInfo GetBranchByName(DuckLakeTransaction &transaction, const string &branch_name);

	//! List all active branches
	vector<DuckLakeBranchInfo> ListBranches(DuckLakeTransaction &transaction);

	//! Get the lineage for a branch (all ancestors with their max visible snapshot)
	vector<DuckLakeBranchLineage> GetBranchLineage(DuckLakeTransaction &transaction, BranchIndex branch_id);

	//! Check if a snapshot is visible to a branch
	bool IsSnapshotVisible(DuckLakeTransaction &transaction, BranchIndex branch_id, BranchIndex file_branch_id,
	                       idx_t snapshot_id);

	//! Update branch head after commit
	void UpdateBranchHead(DuckLakeTransaction &transaction, BranchIndex branch_id, idx_t new_head_snapshot);

	//! Record a file deletion on a branch (for compaction of inherited files)
	void RecordFileDeletion(DuckLakeTransaction &transaction, const DuckLakeBranchFileDeletion &deletion);

	//! Record a delete file deletion on a branch (for REWRITE_DELETES)
	void RecordDeleteFileDeletion(DuckLakeTransaction &transaction, const DuckLakeBranchDeleteFileDeletion &deletion);

	//! Check if a file is deleted on a branch
	bool IsFileDeletedOnBranch(DuckLakeTransaction &transaction, BranchIndex branch_id, BranchIndex file_branch_id,
	                           DataFileIndex data_file_id, idx_t snapshot_id);

	//! Get all file deletions for a branch
	vector<DuckLakeBranchFileDeletion> GetFileDeletions(DuckLakeTransaction &transaction, BranchIndex branch_id);

	//! Clear lineage cache (called when branches are created/deleted)
	void InvalidateCache();

	//! Generate SQL WHERE clause for branch-aware file visibility
	string GenerateBranchVisibilitySQL(BranchIndex branch_id, idx_t snapshot_id, const string &file_table_alias);

	//! Generate SQL for getting visible files with branch support
	string GenerateVisibleFilesQuery(BranchIndex branch_id, idx_t snapshot_id, TableIndex table_id);

private:
	//! Load lineage from database if not cached
	void EnsureLineageLoaded(DuckLakeTransaction &transaction, BranchIndex branch_id);

	//! Insert lineage records for a new branch
	void InsertBranchLineage(DuckLakeTransaction &transaction, BranchIndex new_branch_id,
	                         BranchIndex parent_branch_id, idx_t fork_snapshot_id);

private:
	DuckLakeMetadataManager &metadata_manager;

	//! Lineage cache: branch_id -> list of (ancestor_id, max_visible_snapshot)
	mutex lineage_cache_lock;
	unordered_map<idx_t, vector<DuckLakeBranchLineage>> lineage_cache;
};

} // namespace duckdb
