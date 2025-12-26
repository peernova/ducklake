//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_branch_manager.cpp
//
//
//===----------------------------------------------------------------------===//

#include "storage/ducklake_branch_manager.hpp"
#include "storage/ducklake_metadata_manager.hpp"
#include "storage/ducklake_transaction.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/operator/cast_operators.hpp"

namespace duckdb {

DuckLakeBranchManager::DuckLakeBranchManager(DuckLakeMetadataManager &metadata_manager)
    : metadata_manager(metadata_manager) {
}

DuckLakeBranchManager::~DuckLakeBranchManager() {
}

CreateBranchResult DuckLakeBranchManager::CreateBranch(DuckLakeTransaction &transaction,
                                                       const CreateBranchInput &input) {
	// 1. Get next branch_id - use explicit BIGINT cast for type consistency
	auto result = transaction.Query(
	    "SELECT COALESCE(MAX(branch_id), CAST(-1 AS BIGINT)) + 1 FROM {METADATA_CATALOG}.ducklake_branch");
	auto chunk = result->Fetch();
	idx_t new_branch_id = NumericCast<idx_t>(chunk->GetValue(0, 0).GetValue<int64_t>());

	// 2. Get fork snapshot (default to parent's head if not specified)
	idx_t fork_snapshot = input.fork_snapshot_id;
	if (fork_snapshot == DConstants::INVALID_INDEX) {
		auto head_result = transaction.Query(
		    StringUtil::Format("SELECT head_snapshot_id FROM {METADATA_CATALOG}.ducklake_branch WHERE branch_id = %lld",
		                       NumericCast<int64_t>(input.parent_branch_id.index)));
		auto head_chunk = head_result->Fetch();
		fork_snapshot = NumericCast<idx_t>(head_chunk->GetValue(0, 0).GetValue<int64_t>());
	}

	// 3. Insert branch record
	transaction.Query(StringUtil::Format(
	    "INSERT INTO {METADATA_CATALOG}.ducklake_branch "
	    "(branch_id, branch_name, parent_branch_id, fork_snapshot_id, head_snapshot_id, created_at, status) "
	    "VALUES (%lld, '%s', %lld, %lld, %lld, NOW(), 'active')",
	    NumericCast<int64_t>(new_branch_id), input.branch_name, NumericCast<int64_t>(input.parent_branch_id.index),
	    NumericCast<int64_t>(fork_snapshot), NumericCast<int64_t>(fork_snapshot)));

	// 4. Insert self-reference in lineage
	transaction.Query(StringUtil::Format("INSERT INTO {METADATA_CATALOG}.ducklake_branch_lineage "
	                                     "(branch_id, ancestor_branch_id, max_visible_snapshot) "
	                                     "VALUES (%lld, %lld, %lld)",
	                                     NumericCast<int64_t>(new_branch_id), NumericCast<int64_t>(new_branch_id),
	                                     DuckLakeConstants::MAX_VISIBLE_SNAPSHOT));

	// 5. Copy parent's lineage with fork point cap
	transaction.Query(
	    StringUtil::Format("INSERT INTO {METADATA_CATALOG}.ducklake_branch_lineage "
	                       "(branch_id, ancestor_branch_id, max_visible_snapshot) "
	                       "SELECT %lld, ancestor_branch_id, "
	                       "       CASE WHEN ancestor_branch_id = %lld THEN %lld ELSE max_visible_snapshot END "
	                       "FROM {METADATA_CATALOG}.ducklake_branch_lineage "
	                       "WHERE branch_id = %lld",
	                       NumericCast<int64_t>(new_branch_id), NumericCast<int64_t>(input.parent_branch_id.index),
	                       NumericCast<int64_t>(fork_snapshot), NumericCast<int64_t>(input.parent_branch_id.index)));

	// 6. Invalidate cache
	InvalidateCache();

	CreateBranchResult result_info;
	result_info.branch_id = BranchIndex(new_branch_id);
	result_info.branch_name = input.branch_name;
	result_info.head_snapshot_id = fork_snapshot;
	return result_info;
}

void DuckLakeBranchManager::DeleteBranch(DuckLakeTransaction &transaction, BranchIndex branch_id) {
	if (branch_id.IsMain()) {
		throw InvalidInputException("Cannot delete the main branch");
	}

	// Check for child branches
	auto children_result =
	    transaction.Query(StringUtil::Format("SELECT COUNT(*) FROM {METADATA_CATALOG}.ducklake_branch "
	                                         "WHERE parent_branch_id = %lld AND status = 'active'",
	                                         NumericCast<int64_t>(branch_id.index)));
	auto chunk = children_result->Fetch();
	if (chunk->GetValue(0, 0).GetValue<int64_t>() > 0) {
		throw InvalidInputException("Cannot delete branch with active child branches");
	}

	// Soft delete
	transaction.Query(StringUtil::Format("UPDATE {METADATA_CATALOG}.ducklake_branch "
	                                     "SET status = 'deleted' "
	                                     "WHERE branch_id = %lld",
	                                     NumericCast<int64_t>(branch_id.index)));

	InvalidateCache();
}

DuckLakeBranchInfo DuckLakeBranchManager::GetBranch(DuckLakeTransaction &transaction, BranchIndex branch_id) {
	fprintf(stderr, "[DEBUG GetBranch] Entering with branch_id: %llu\n",
	        static_cast<unsigned long long>(branch_id.index));
	fflush(stderr);

	auto result = transaction.Query(StringUtil::Format(
	    "SELECT branch_id, branch_name, parent_branch_id, fork_snapshot_id, head_snapshot_id, created_at, status "
	    "FROM {METADATA_CATALOG}.ducklake_branch WHERE branch_id = %lld",
	    NumericCast<int64_t>(branch_id.index)));

	fprintf(stderr, "[DEBUG GetBranch] Query executed, fetching chunk\n");
	fflush(stderr);

	auto chunk = result->Fetch();
	if (!chunk || chunk->size() == 0) {
		throw InvalidInputException("Branch not found: %lld", NumericCast<int64_t>(branch_id.index));
	}

	fprintf(stderr, "[DEBUG GetBranch] Got chunk with %llu rows\n", static_cast<unsigned long long>(chunk->size()));
	fflush(stderr);

	DuckLakeBranchInfo info;

	// Log each column type before extraction
	for (idx_t i = 0; i < chunk->ColumnCount(); i++) {
		auto val = chunk->GetValue(i, 0);
		fprintf(stderr, "[DEBUG GetBranch] Column %llu type: %s, value: %s\n", static_cast<unsigned long long>(i),
		        val.type().ToString().c_str(), val.ToString().c_str());
	}
	fflush(stderr);

	fprintf(stderr, "[DEBUG GetBranch] Extracting branch_id (col 0)\n");
	fflush(stderr);
	info.branch_id = BranchIndex(NumericCast<idx_t>(chunk->GetValue(0, 0).GetValue<int64_t>()));

	fprintf(stderr, "[DEBUG GetBranch] Extracting branch_name (col 1)\n");
	fflush(stderr);
	info.branch_name = chunk->GetValue(1, 0).ToString();

	fprintf(stderr, "[DEBUG GetBranch] Extracting parent_branch_id (col 2)\n");
	fflush(stderr);
	auto parent_val = chunk->GetValue(2, 0);
	info.parent_branch_id = parent_val.IsNull() ? BranchIndex(DConstants::INVALID_INDEX)
	                                            : BranchIndex(NumericCast<idx_t>(parent_val.GetValue<int64_t>()));

	fprintf(stderr, "[DEBUG GetBranch] Extracting fork_snapshot_id (col 3)\n");
	fflush(stderr);
	auto fork_val = chunk->GetValue(3, 0);
	info.fork_snapshot_id = fork_val.IsNull() ? 0 : NumericCast<idx_t>(fork_val.GetValue<int64_t>());

	fprintf(stderr, "[DEBUG GetBranch] Extracting head_snapshot_id (col 4)\n");
	fflush(stderr);
	info.head_snapshot_id = NumericCast<idx_t>(chunk->GetValue(4, 0).GetValue<int64_t>());

	fprintf(stderr, "[DEBUG GetBranch] Extracting created_at (col 5) - type: %s\n",
	        chunk->GetValue(5, 0).type().ToString().c_str());
	fflush(stderr);
	// The column is TIMESTAMP WITH TIME ZONE, but created_at is timestamp_t
	// We need to cast to TIMESTAMP first, or extract as timestamp_tz_t and convert
	auto created_at_val = chunk->GetValue(5, 0);
	// Cast TIMESTAMP WITH TIME ZONE to TIMESTAMP before extracting
	info.created_at = created_at_val.DefaultCastAs(LogicalType::TIMESTAMP).GetValue<timestamp_t>();

	fprintf(stderr, "[DEBUG GetBranch] Extracting status (col 6)\n");
	fflush(stderr);
	string status_str = chunk->GetValue(6, 0).ToString();
	if (status_str == "active") {
		info.status = BranchStatus::ACTIVE;
	} else if (status_str == "merged") {
		info.status = BranchStatus::MERGED;
	} else if (status_str == "archived") {
		info.status = BranchStatus::ARCHIVED;
	} else {
		info.status = BranchStatus::DELETED;
	}

	fprintf(stderr, "[DEBUG GetBranch] Successfully extracted all fields\n");
	fflush(stderr);
	return info;
}

DuckLakeBranchInfo DuckLakeBranchManager::GetBranchByName(DuckLakeTransaction &transaction, const string &branch_name) {
	// DEBUG: Log GetBranchByName entry
	fprintf(stderr, "[DEBUG GetBranchByName] Looking for branch: '%s'\n", branch_name.c_str());
	fflush(stderr);

	auto result = transaction.Query(StringUtil::Format("SELECT branch_id FROM {METADATA_CATALOG}.ducklake_branch "
	                                                   "WHERE branch_name = '%s' AND status = 'active'",
	                                                   branch_name));

	auto chunk = result->Fetch();
	if (!chunk || chunk->size() == 0) {
		fprintf(stderr, "[DEBUG GetBranchByName] Branch not found: '%s'\n", branch_name.c_str());
		fflush(stderr);
		throw InvalidInputException("Branch not found: %s", branch_name);
	}

	auto branch_id_val = chunk->GetValue(0, 0);
	fprintf(stderr, "[DEBUG GetBranchByName] Found branch, branch_id value type: %s, value: %s\n",
	        branch_id_val.type().ToString().c_str(), branch_id_val.ToString().c_str());
	fflush(stderr);

	auto branch_id = BranchIndex(NumericCast<idx_t>(branch_id_val.GetValue<int64_t>()));
	fprintf(stderr, "[DEBUG GetBranchByName] Calling GetBranch with branch_id: %llu\n",
	        static_cast<unsigned long long>(branch_id.index));
	fflush(stderr);

	return GetBranch(transaction, branch_id);
}

vector<DuckLakeBranchInfo> DuckLakeBranchManager::ListBranches(DuckLakeTransaction &transaction) {
	auto result = transaction.Query(
	    "SELECT branch_id, branch_name, parent_branch_id, fork_snapshot_id, head_snapshot_id, created_at, status "
	    "FROM {METADATA_CATALOG}.ducklake_branch WHERE status = 'active' ORDER BY branch_id");

	vector<DuckLakeBranchInfo> branches;
	while (true) {
		auto chunk = result->Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}
		for (idx_t i = 0; i < chunk->size(); i++) {
			DuckLakeBranchInfo info;
			info.branch_id = BranchIndex(NumericCast<idx_t>(chunk->GetValue(0, i).GetValue<int64_t>()));
			info.branch_name = chunk->GetValue(1, i).ToString();
			auto parent_val = chunk->GetValue(2, i);
			info.parent_branch_id = parent_val.IsNull()
			                            ? BranchIndex(DConstants::INVALID_INDEX)
			                            : BranchIndex(NumericCast<idx_t>(parent_val.GetValue<int64_t>()));
			auto fork_val = chunk->GetValue(3, i);
			info.fork_snapshot_id = fork_val.IsNull() ? 0 : NumericCast<idx_t>(fork_val.GetValue<int64_t>());
			info.head_snapshot_id = NumericCast<idx_t>(chunk->GetValue(4, i).GetValue<int64_t>());
			info.status = BranchStatus::ACTIVE;
			branches.push_back(info);
		}
	}
	return branches;
}

vector<DuckLakeBranchLineage> DuckLakeBranchManager::GetBranchLineage(DuckLakeTransaction &transaction,
                                                                      BranchIndex branch_id) {
	// Check cache first
	{
		lock_guard<mutex> guard(lineage_cache_lock);
		auto it = lineage_cache.find(branch_id.index);
		if (it != lineage_cache.end()) {
			return it->second;
		}
	}

	// Load from database
	auto result =
	    transaction.Query(StringUtil::Format("SELECT branch_id, ancestor_branch_id, max_visible_snapshot "
	                                         "FROM {METADATA_CATALOG}.ducklake_branch_lineage WHERE branch_id = %lld",
	                                         NumericCast<int64_t>(branch_id.index)));

	vector<DuckLakeBranchLineage> lineage;
	while (true) {
		auto chunk = result->Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}
		for (idx_t i = 0; i < chunk->size(); i++) {
			DuckLakeBranchLineage entry;
			entry.branch_id = BranchIndex(NumericCast<idx_t>(chunk->GetValue(0, i).GetValue<int64_t>()));
			entry.ancestor_branch_id = BranchIndex(NumericCast<idx_t>(chunk->GetValue(1, i).GetValue<int64_t>()));
			entry.max_visible_snapshot = NumericCast<idx_t>(chunk->GetValue(2, i).GetValue<int64_t>());
			lineage.push_back(entry);
		}
	}

	// Cache it
	{
		lock_guard<mutex> guard(lineage_cache_lock);
		lineage_cache[branch_id.index] = lineage;
	}

	return lineage;
}

void DuckLakeBranchManager::UpdateBranchHead(DuckLakeTransaction &transaction, BranchIndex branch_id,
                                             idx_t new_head_snapshot) {
	transaction.Query(StringUtil::Format(
	    "UPDATE {METADATA_CATALOG}.ducklake_branch SET head_snapshot_id = %lld WHERE branch_id = %lld",
	    NumericCast<int64_t>(new_head_snapshot), NumericCast<int64_t>(branch_id.index)));
}

void DuckLakeBranchManager::RecordFileDeletion(DuckLakeTransaction &transaction,
                                               const DuckLakeBranchFileDeletion &deletion) {
	transaction.Query(StringUtil::Format(
	    "INSERT INTO {METADATA_CATALOG}.ducklake_branch_file_deletion "
	    "(branch_id, ancestor_branch_id, data_file_id, deleted_at_snapshot) "
	    "VALUES (%lld, %lld, %lld, %lld) "
	    "ON CONFLICT (branch_id, ancestor_branch_id, data_file_id) DO NOTHING",
	    NumericCast<int64_t>(deletion.branch_id.index), NumericCast<int64_t>(deletion.ancestor_branch_id.index),
	    NumericCast<int64_t>(deletion.data_file_id.index), NumericCast<int64_t>(deletion.deleted_at_snapshot)));
}

void DuckLakeBranchManager::RecordDeleteFileDeletion(DuckLakeTransaction &transaction,
                                                     const DuckLakeBranchDeleteFileDeletion &deletion) {
	transaction.Query(StringUtil::Format(
	    "INSERT INTO {METADATA_CATALOG}.ducklake_branch_delete_file_deletion "
	    "(branch_id, ancestor_branch_id, delete_file_id, deleted_at_snapshot) "
	    "VALUES (%lld, %lld, %lld, %lld) "
	    "ON CONFLICT (branch_id, ancestor_branch_id, delete_file_id) DO NOTHING",
	    NumericCast<int64_t>(deletion.branch_id.index), NumericCast<int64_t>(deletion.ancestor_branch_id.index),
	    NumericCast<int64_t>(deletion.delete_file_id.index), NumericCast<int64_t>(deletion.deleted_at_snapshot)));
}

bool DuckLakeBranchManager::IsFileDeletedOnBranch(DuckLakeTransaction &transaction, BranchIndex branch_id,
                                                  BranchIndex file_branch_id, DataFileIndex data_file_id,
                                                  idx_t snapshot_id) {
	auto lineage = GetBranchLineage(transaction, branch_id);

	// Check if any ancestor has deleted this file
	auto result = transaction.Query(StringUtil::Format(
	    "SELECT 1 FROM {METADATA_CATALOG}.ducklake_branch_file_deletion del "
	    "JOIN {METADATA_CATALOG}.ducklake_branch_lineage bl ON del.branch_id = bl.ancestor_branch_id "
	    "WHERE bl.branch_id = %lld "
	    "AND del.ancestor_branch_id = %lld "
	    "AND del.data_file_id = %lld "
	    "AND del.deleted_at_snapshot <= bl.max_visible_snapshot "
	    "LIMIT 1",
	    NumericCast<int64_t>(branch_id.index), NumericCast<int64_t>(file_branch_id.index),
	    NumericCast<int64_t>(data_file_id.index)));

	auto chunk = result->Fetch();
	return chunk && chunk->size() > 0;
}

vector<DuckLakeBranchFileDeletion> DuckLakeBranchManager::GetFileDeletions(DuckLakeTransaction &transaction,
                                                                           BranchIndex branch_id) {
	auto result = transaction.Query(
	    StringUtil::Format("SELECT branch_id, ancestor_branch_id, data_file_id, deleted_at_snapshot "
	                       "FROM {METADATA_CATALOG}.ducklake_branch_file_deletion WHERE branch_id = %lld",
	                       NumericCast<int64_t>(branch_id.index)));

	vector<DuckLakeBranchFileDeletion> deletions;
	while (true) {
		auto chunk = result->Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}
		for (idx_t i = 0; i < chunk->size(); i++) {
			DuckLakeBranchFileDeletion deletion;
			deletion.branch_id = BranchIndex(NumericCast<idx_t>(chunk->GetValue(0, i).GetValue<int64_t>()));
			deletion.ancestor_branch_id = BranchIndex(NumericCast<idx_t>(chunk->GetValue(1, i).GetValue<int64_t>()));
			deletion.data_file_id = DataFileIndex(NumericCast<idx_t>(chunk->GetValue(2, i).GetValue<int64_t>()));
			deletion.deleted_at_snapshot = NumericCast<idx_t>(chunk->GetValue(3, i).GetValue<int64_t>());
			deletions.push_back(deletion);
		}
	}
	return deletions;
}

void DuckLakeBranchManager::InvalidateCache() {
	lock_guard<mutex> guard(lineage_cache_lock);
	lineage_cache.clear();
}

string DuckLakeBranchManager::GenerateVisibleFilesQuery(BranchIndex branch_id, idx_t snapshot_id, TableIndex table_id) {
	return StringUtil::Format(R"(
SELECT df.*
FROM {METADATA_CATALOG}.ducklake_data_file df
JOIN {METADATA_CATALOG}.ducklake_branch_lineage bl 
    ON df.branch_id = bl.ancestor_branch_id
WHERE bl.branch_id = %lld
  AND df.table_id = %lld
  AND df.begin_snapshot <= CASE 
      WHEN df.branch_id = %lld THEN %lld
      ELSE bl.max_visible_snapshot
  END
  AND (df.end_snapshot IS NULL 
       OR df.end_snapshot > CASE 
           WHEN df.branch_id = %lld THEN %lld
           ELSE bl.max_visible_snapshot
       END)
  AND NOT EXISTS (
      SELECT 1 FROM {METADATA_CATALOG}.ducklake_branch_file_deletion del
      JOIN {METADATA_CATALOG}.ducklake_branch_lineage del_bl 
          ON del.branch_id = del_bl.ancestor_branch_id
      WHERE del_bl.branch_id = %lld
        AND del.ancestor_branch_id = df.branch_id
        AND del.data_file_id = df.data_file_id
        AND del.deleted_at_snapshot <= del_bl.max_visible_snapshot
  )
)",
	                          NumericCast<int64_t>(branch_id.index), NumericCast<int64_t>(table_id.index),
	                          NumericCast<int64_t>(branch_id.index), NumericCast<int64_t>(snapshot_id),
	                          NumericCast<int64_t>(branch_id.index), NumericCast<int64_t>(snapshot_id),
	                          NumericCast<int64_t>(branch_id.index));
}

} // namespace duckdb
