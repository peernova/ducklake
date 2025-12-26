//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/ducklake_snapshot.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "common/index.hpp"

namespace duckdb {

struct DuckLakeSnapshot {
	DuckLakeSnapshot(idx_t snapshot_id, idx_t schema_version, idx_t next_catalog_id, idx_t next_file_id,
	                 BranchIndex branch_id = BranchIndex(0))
	    : snapshot_id(snapshot_id), schema_version(schema_version), next_catalog_id(next_catalog_id),
	      next_file_id(next_file_id), branch_id(branch_id) {
	}

	DuckLakeSnapshot()
	    : snapshot_id(DConstants::INVALID_INDEX), schema_version(DConstants::INVALID_INDEX),
	      next_catalog_id(DConstants::INVALID_INDEX), next_file_id(DConstants::INVALID_INDEX),
	      branch_id(BranchIndex(0)) {
	}

	idx_t snapshot_id;
	idx_t schema_version;
	idx_t next_catalog_id;
	idx_t next_file_id;
	BranchIndex branch_id;

	bool IsValid() const {
		return snapshot_id != DConstants::INVALID_INDEX;
	}
};

} // namespace duckdb
