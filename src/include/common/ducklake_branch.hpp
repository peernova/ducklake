//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/ducklake_branch.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "common/index.hpp"

namespace duckdb {

//! Status of a branch
enum class BranchStatus { ACTIVE, MERGED, ARCHIVED, DELETED };

//! Information about a branch
struct DuckLakeBranchInfo {
	BranchIndex branch_id;
	string branch_name;
	BranchIndex parent_branch_id;
	idx_t fork_snapshot_id;
	idx_t head_snapshot_id;
	timestamp_t created_at;
	BranchStatus status;

	DuckLakeBranchInfo()
	    : branch_id(BranchIndex(0)), parent_branch_id(BranchIndex(DConstants::INVALID_INDEX)), fork_snapshot_id(0),
	      head_snapshot_id(0), status(BranchStatus::ACTIVE) {
	}

	bool IsMain() const {
		return branch_id.IsMain();
	}
};

//! Pre-computed lineage entry for a branch
struct DuckLakeBranchLineage {
	BranchIndex branch_id;
	BranchIndex ancestor_branch_id;
	idx_t max_visible_snapshot;

	DuckLakeBranchLineage() : max_visible_snapshot(DuckLakeConstants::MAX_VISIBLE_SNAPSHOT) {
	}

	DuckLakeBranchLineage(BranchIndex branch, BranchIndex ancestor, idx_t max_snap)
	    : branch_id(branch), ancestor_branch_id(ancestor), max_visible_snapshot(max_snap) {
	}
};

//! Branch-scoped file deletion record (for compaction or inherited file deletion)
struct DuckLakeBranchFileDeletion {
	BranchIndex branch_id;          // Branch that performed the deletion
	BranchIndex ancestor_branch_id; // Branch that owns the file
	DataFileIndex data_file_id;
	idx_t deleted_at_snapshot;

	DuckLakeBranchFileDeletion() : deleted_at_snapshot(0) {
	}

	DuckLakeBranchFileDeletion(BranchIndex branch, BranchIndex ancestor, DataFileIndex file_id, idx_t snapshot)
	    : branch_id(branch), ancestor_branch_id(ancestor), data_file_id(file_id), deleted_at_snapshot(snapshot) {
	}
};

//! Branch-scoped delete file deletion record (for REWRITE_DELETES compaction)
struct DuckLakeBranchDeleteFileDeletion {
	BranchIndex branch_id;
	BranchIndex ancestor_branch_id;
	DataFileIndex delete_file_id;
	idx_t deleted_at_snapshot;

	DuckLakeBranchDeleteFileDeletion() : deleted_at_snapshot(0) {
	}

	DuckLakeBranchDeleteFileDeletion(BranchIndex branch, BranchIndex ancestor, DataFileIndex file_id, idx_t snapshot)
	    : branch_id(branch), ancestor_branch_id(ancestor), delete_file_id(file_id), deleted_at_snapshot(snapshot) {
	}
};

//! Input for creating a new branch
struct CreateBranchInput {
	string branch_name;
	BranchIndex parent_branch_id;
	idx_t fork_snapshot_id; // Snapshot on parent to fork from

	CreateBranchInput() : parent_branch_id(BranchIndex(0)), fork_snapshot_id(DConstants::INVALID_INDEX) {
	}

	CreateBranchInput(string name, BranchIndex parent, idx_t fork_snap)
	    : branch_name(std::move(name)), parent_branch_id(parent), fork_snapshot_id(fork_snap) {
	}
};

//! Result of creating a new branch
struct CreateBranchResult {
	BranchIndex branch_id;
	string branch_name;
	idx_t head_snapshot_id;
};

} // namespace duckdb
