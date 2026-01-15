//===----------------------------------------------------------------------===//
//                         DuckDB
//
// functions/ducklake_branch_functions.cpp
//
//
//===----------------------------------------------------------------------===//

#include "functions/ducklake_table_functions.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_branch_manager.hpp"
#include "duckdb/common/operator/cast_operators.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// List Branches
//===--------------------------------------------------------------------===//
struct ListBranchesBindData : public TableFunctionData {
	string catalog_name;
};

struct DuckLakeListBranchesData : public GlobalTableFunctionState {
	DuckLakeListBranchesData() : done(false) {
	}

	bool done;
};

static unique_ptr<FunctionData> ListBranchesBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("branch_id");
	return_types.emplace_back(LogicalType::BIGINT);
	
	names.emplace_back("branch_name");
	return_types.emplace_back(LogicalType::VARCHAR);
	
	names.emplace_back("parent_branch_id");
	return_types.emplace_back(LogicalType::BIGINT);
	
	names.emplace_back("fork_snapshot_id");
	return_types.emplace_back(LogicalType::BIGINT);
	
	names.emplace_back("head_snapshot_id");
	return_types.emplace_back(LogicalType::BIGINT);
	
	names.emplace_back("status");
	return_types.emplace_back(LogicalType::VARCHAR);

	auto result = make_uniq<ListBranchesBindData>();
	result->catalog_name = input.inputs[0].ToString();
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> ListBranchesInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<DuckLakeListBranchesData>();
}

static void ListBranchesFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<ListBranchesBindData>();
	auto &data = data_p.global_state->Cast<DuckLakeListBranchesData>();
	
	if (data.done) {
		output.SetCardinality(0);
		return;
	}
	
	auto &catalog = BaseMetadataFunction::GetCatalog(context, Value(bind_data.catalog_name));
	auto &transaction = DuckLakeTransaction::Get(context, catalog);

	// Query branches directly
	auto result = transaction.Query(
	    "SELECT branch_id, branch_name, parent_branch_id, fork_snapshot_id, head_snapshot_id, status "
	    "FROM {METADATA_CATALOG}.ducklake_branch WHERE status = 'active' ORDER BY branch_id");

	idx_t count = 0;
	while (count < STANDARD_VECTOR_SIZE) {
		auto chunk = result->Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}
		for (idx_t i = 0; i < chunk->size() && count < STANDARD_VECTOR_SIZE; i++) {
			output.SetValue(0, count, chunk->GetValue(0, i));  // branch_id
			output.SetValue(1, count, chunk->GetValue(1, i));  // branch_name
			output.SetValue(2, count, chunk->GetValue(2, i));  // parent_branch_id
			output.SetValue(3, count, chunk->GetValue(3, i));  // fork_snapshot_id
			output.SetValue(4, count, chunk->GetValue(4, i));  // head_snapshot_id
			output.SetValue(5, count, chunk->GetValue(5, i));  // status
			count++;
		}
	}
	output.SetCardinality(count);
	data.done = true;
}

TableFunction DuckLakeListBranchesFunction::GetFunction() {
	TableFunction func("ducklake_branches", {LogicalType::VARCHAR}, ListBranchesFunction, 
	                   ListBranchesBind, ListBranchesInit);
	return func;
}

//===--------------------------------------------------------------------===//
// Create Branch
//===--------------------------------------------------------------------===//
struct CreateBranchBindData : public TableFunctionData {
	string catalog_name;
	string branch_name;
	string parent_name;
	optional_idx fork_snapshot_id;
};

struct CreateBranchState : public GlobalTableFunctionState {
	CreateBranchState() : done(false) {}
	bool done;
};

static unique_ptr<FunctionData> CreateBranchBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("branch_id");
	return_types.emplace_back(LogicalType::BIGINT);
	
	names.emplace_back("branch_name");
	return_types.emplace_back(LogicalType::VARCHAR);
	
	names.emplace_back("head_snapshot_id");
	return_types.emplace_back(LogicalType::BIGINT);

	auto result = make_uniq<CreateBranchBindData>();
	result->catalog_name = input.inputs[0].ToString();
	result->branch_name = input.inputs[1].ToString();
	
	// Optional parent branch name - default to current working branch (not 'main')
	// This allows nested branching: when on dev_branch, creating a branch defaults to forking from dev_branch
	if (input.inputs.size() > 2 && !input.inputs[2].IsNull()) {
		result->parent_name = input.inputs[2].ToString();
	} else {
		// Get current working branch name as default parent
		auto &catalog = BaseMetadataFunction::GetCatalog(context, Value(result->catalog_name));
		auto &ducklake_catalog = catalog.Cast<DuckLakeCatalog>();
		BranchIndex working_branch = ducklake_catalog.GetWorkingBranch();
		
		// Query branch name from working branch id
		auto &transaction = DuckLakeTransaction::Get(context, catalog);
		auto branch_result = transaction.Query(StringUtil::Format(
		    "SELECT branch_name FROM {METADATA_CATALOG}.ducklake_branch "
		    "WHERE branch_id = %lld AND status = 'active'",
		    NumericCast<int64_t>(working_branch.index)));
		auto branch_chunk = branch_result->Fetch();
		if (branch_chunk && branch_chunk->size() > 0) {
			result->parent_name = branch_chunk->GetValue(0, 0).ToString();
		} else {
			result->parent_name = "main";  // Fallback to main if current branch not found
		}
	}
	
	// Optional fork snapshot
	if (input.inputs.size() > 3 && !input.inputs[3].IsNull()) {
		result->fork_snapshot_id = input.inputs[3].GetValue<idx_t>();
	}
	
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> CreateBranchInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<CreateBranchState>();
}

static void CreateBranchFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<CreateBranchBindData>();
	auto &state = data_p.global_state->Cast<CreateBranchState>();
	
	if (state.done) {
		output.SetCardinality(0);
		return;
	}
	
	auto &catalog = BaseMetadataFunction::GetCatalog(context, Value(bind_data.catalog_name));
	auto &transaction = DuckLakeTransaction::Get(context, catalog);

	// Get parent branch id
	auto parent_result = transaction.Query(StringUtil::Format(
	    "SELECT branch_id, head_snapshot_id FROM {METADATA_CATALOG}.ducklake_branch "
	    "WHERE branch_name = '%s' AND status = 'active'",
	    bind_data.parent_name));
	auto parent_chunk = parent_result->Fetch();
	if (!parent_chunk || parent_chunk->size() == 0) {
		throw InvalidInputException("Parent branch not found: %s", bind_data.parent_name);
	}
	int64_t parent_branch_id = parent_chunk->GetValue(0, 0).GetValue<int64_t>();
	int64_t parent_head = parent_chunk->GetValue(1, 0).GetValue<int64_t>();

	// Get fork snapshot
	int64_t fork_snapshot = bind_data.fork_snapshot_id.IsValid() 
	                        ? NumericCast<int64_t>(bind_data.fork_snapshot_id.GetIndex())
	                        : parent_head;

	// Check if branch name already exists
	auto dup_result = transaction.Query(StringUtil::Format(
	    "SELECT COUNT(*) FROM {METADATA_CATALOG}.ducklake_branch "
	    "WHERE branch_name = '%s' AND status = 'active'",
	    bind_data.branch_name));
	auto dup_chunk = dup_result->Fetch();
	if (dup_chunk && dup_chunk->GetValue(0, 0).GetValue<int64_t>() > 0) {
		throw InvalidInputException("Branch '%s' already exists", bind_data.branch_name);
	}

	// Get next branch id - use explicit BIGINT cast for -1 to ensure consistent types
	auto max_result = transaction.Query("SELECT COALESCE(MAX(branch_id), CAST(-1 AS BIGINT)) + 1 FROM {METADATA_CATALOG}.ducklake_branch");
	auto max_chunk = max_result->Fetch();
	int64_t new_branch_id = max_chunk->GetValue(0, 0).GetValue<int64_t>();

	// Insert branch record
	transaction.Query(StringUtil::Format(
	    "INSERT INTO {METADATA_CATALOG}.ducklake_branch "
	    "(branch_id, branch_name, parent_branch_id, fork_snapshot_id, head_snapshot_id, status) "
	    "VALUES (%lld, '%s', %lld, %lld, %lld, 'active')",
	    new_branch_id, bind_data.branch_name, parent_branch_id, fork_snapshot, fork_snapshot));

	// Insert self-reference in lineage
	transaction.Query(StringUtil::Format(
	    "INSERT INTO {METADATA_CATALOG}.ducklake_branch_lineage "
	    "(branch_id, ancestor_branch_id, max_visible_snapshot) "
	    "VALUES (%lld, %lld, 9223372036854775807)",
	    new_branch_id, new_branch_id));

	// Copy parent's lineage with fork point cap
	transaction.Query(StringUtil::Format(
	    "INSERT INTO {METADATA_CATALOG}.ducklake_branch_lineage "
	    "(branch_id, ancestor_branch_id, max_visible_snapshot) "
	    "SELECT %lld, ancestor_branch_id, "
	    "       CASE WHEN ancestor_branch_id = %lld THEN %lld ELSE max_visible_snapshot END "
	    "FROM {METADATA_CATALOG}.ducklake_branch_lineage "
	    "WHERE branch_id = %lld",
	    new_branch_id, parent_branch_id, fork_snapshot, parent_branch_id));

	// Copy parent's file deletions (so child inherits deleted files from ancestors)
	transaction.Query(StringUtil::Format(
	    "INSERT INTO {METADATA_CATALOG}.ducklake_branch_file_deletion "
	    "(branch_id, ancestor_branch_id, data_file_id, deleted_at_snapshot) "
	    "SELECT %lld, ancestor_branch_id, data_file_id, deleted_at_snapshot "
	    "FROM {METADATA_CATALOG}.ducklake_branch_file_deletion "
	    "WHERE branch_id = %lld",
	    new_branch_id, parent_branch_id));

	// Copy parent's delete file deletions
	transaction.Query(StringUtil::Format(
	    "INSERT INTO {METADATA_CATALOG}.ducklake_branch_delete_file_deletion "
	    "(branch_id, ancestor_branch_id, delete_file_id, deleted_at_snapshot) "
	    "SELECT %lld, ancestor_branch_id, delete_file_id, deleted_at_snapshot "
	    "FROM {METADATA_CATALOG}.ducklake_branch_delete_file_deletion "
	    "WHERE branch_id = %lld",
	    new_branch_id, parent_branch_id));

	// Copy parent's partition deletions
	transaction.Query(StringUtil::Format(
	    "INSERT INTO {METADATA_CATALOG}.ducklake_branch_partition_deletion "
	    "(branch_id, ancestor_branch_id, partition_id, table_id, deleted_at_snapshot) "
	    "SELECT %lld, ancestor_branch_id, partition_id, table_id, deleted_at_snapshot "
	    "FROM {METADATA_CATALOG}.ducklake_branch_partition_deletion "
	    "WHERE branch_id = %lld",
	    new_branch_id, parent_branch_id));

	// Copy parent's table deletions (so child inherits deleted tables from ancestors)
	transaction.Query(StringUtil::Format(
	    "INSERT INTO {METADATA_CATALOG}.ducklake_branch_table_deletion "
	    "(branch_id, ancestor_branch_id, table_id, deleted_at_snapshot) "
	    "SELECT %lld, ancestor_branch_id, table_id, deleted_at_snapshot "
	    "FROM {METADATA_CATALOG}.ducklake_branch_table_deletion "
	    "WHERE branch_id = %lld",
	    new_branch_id, parent_branch_id));

	// Copy parent's schema deletions (so child inherits deleted schemas from ancestors)
	transaction.Query(StringUtil::Format(
	    "INSERT INTO {METADATA_CATALOG}.ducklake_branch_schema_deletion "
	    "(branch_id, ancestor_branch_id, schema_id, deleted_at_snapshot) "
	    "SELECT %lld, ancestor_branch_id, schema_id, deleted_at_snapshot "
	    "FROM {METADATA_CATALOG}.ducklake_branch_schema_deletion "
	    "WHERE branch_id = %lld",
	    new_branch_id, parent_branch_id));

	// Copy parent's view deletions (so child inherits deleted views from ancestors)
	transaction.Query(StringUtil::Format(
	    "INSERT INTO {METADATA_CATALOG}.ducklake_branch_view_deletion "
	    "(branch_id, ancestor_branch_id, view_id, deleted_at_snapshot) "
	    "SELECT %lld, ancestor_branch_id, view_id, deleted_at_snapshot "
	    "FROM {METADATA_CATALOG}.ducklake_branch_view_deletion "
	    "WHERE branch_id = %lld",
	    new_branch_id, parent_branch_id));

	// Copy parent's macro deletions (so child inherits deleted macros from ancestors)
	transaction.Query(StringUtil::Format(
	    "INSERT INTO {METADATA_CATALOG}.ducklake_branch_macro_deletion "
	    "(branch_id, ancestor_branch_id, macro_id, deleted_at_snapshot) "
	    "SELECT %lld, ancestor_branch_id, macro_id, deleted_at_snapshot "
	    "FROM {METADATA_CATALOG}.ducklake_branch_macro_deletion "
	    "WHERE branch_id = %lld",
	    new_branch_id, parent_branch_id));

	// Copy parent's column deletions (so child inherits dropped columns from ancestors)
	transaction.Query(StringUtil::Format(
	    "INSERT INTO {METADATA_CATALOG}.ducklake_branch_column_deletion "
	    "(branch_id, ancestor_branch_id, table_id, column_id, deleted_at_snapshot) "
	    "SELECT %lld, ancestor_branch_id, table_id, column_id, deleted_at_snapshot "
	    "FROM {METADATA_CATALOG}.ducklake_branch_column_deletion "
	    "WHERE branch_id = %lld",
	    new_branch_id, parent_branch_id));

	// Copy parent's table stats (so child starts with same counts as parent at fork point)
	//
	// DESIGN NOTE: When branching from an old snapshot (fork_snapshot_id < parent_head),
	// we still copy parent's CURRENT stats, not stats as of fork_snapshot_id.
	// This is consistent with time travel query behavior - time travel queries also use
	// current branch stats for cardinality estimation, not snapshot-specific stats.
	//
	// Rationale:
	// - Table-level stats (record_count, min/max) are optimizer hints only
	// - They do NOT affect query correctness, only query plan selection
	// - File-level stats (in ducklake_file_column_stats) ARE used for filter pushdown
	//   and those are immutable per-file, so filter pushdown remains accurate
	// - Stats will naturally improve as new data is inserted on the child branch
	// - Recomputing accurate stats from visible files would be expensive and complex
	//
	// See docs/use_of_stats_and_filter_push_down.md for detailed explanation.
	transaction.Query(StringUtil::Format(
	    "INSERT INTO {METADATA_CATALOG}.ducklake_table_stats "
	    "(branch_id, table_id, record_count, next_row_id, file_size_bytes) "
	    "SELECT %lld, table_id, record_count, next_row_id, file_size_bytes "
	    "FROM {METADATA_CATALOG}.ducklake_table_stats "
	    "WHERE branch_id = %lld",
	    new_branch_id, parent_branch_id));

	// Copy parent's table column stats (same design note as above applies)
	transaction.Query(StringUtil::Format(
	    "INSERT INTO {METADATA_CATALOG}.ducklake_table_column_stats "
	    "(branch_id, table_id, column_id, contains_null, contains_nan, min_value, max_value, extra_stats) "
	    "SELECT %lld, table_id, column_id, contains_null, contains_nan, min_value, max_value, extra_stats "
	    "FROM {METADATA_CATALOG}.ducklake_table_column_stats "
	    "WHERE branch_id = %lld",
	    new_branch_id, parent_branch_id));

	output.SetValue(0, 0, Value::BIGINT(new_branch_id));
	output.SetValue(1, 0, Value(bind_data.branch_name));
	output.SetValue(2, 0, Value::BIGINT(fork_snapshot));
	output.SetCardinality(1);
	state.done = true;
}

TableFunctionSet DuckLakeCreateBranchFunction::GetFunctions() {
	TableFunctionSet set("ducklake_create_branch");
	
	// ducklake_create_branch(catalog, branch_name)
	TableFunction func2({LogicalType::VARCHAR, LogicalType::VARCHAR}, CreateBranchFunction, CreateBranchBind, CreateBranchInit);
	set.AddFunction(func2);
	
	// ducklake_create_branch(catalog, branch_name, parent_branch_name)
	TableFunction func3({LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, 
	                    CreateBranchFunction, CreateBranchBind, CreateBranchInit);
	set.AddFunction(func3);
	
	// ducklake_create_branch(catalog, branch_name, parent_branch_name, fork_snapshot_id)
	TableFunction func4({LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT}, 
	                    CreateBranchFunction, CreateBranchBind, CreateBranchInit);
	set.AddFunction(func4);
	
	return set;
}

//===--------------------------------------------------------------------===//
// Delete Branch
//===--------------------------------------------------------------------===//
struct DeleteBranchBindData : public TableFunctionData {
	string catalog_name;
	string branch_name;
};

struct DeleteBranchState : public GlobalTableFunctionState {
	DeleteBranchState() : done(false) {}
	bool done;
};

static unique_ptr<FunctionData> DeleteBranchBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("success");
	return_types.emplace_back(LogicalType::BOOLEAN);

	auto result = make_uniq<DeleteBranchBindData>();
	result->catalog_name = input.inputs[0].ToString();
	result->branch_name = input.inputs[1].ToString();
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> DeleteBranchInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<DeleteBranchState>();
}

static void DeleteBranchFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<DeleteBranchBindData>();
	auto &state = data_p.global_state->Cast<DeleteBranchState>();
	
	if (state.done) {
		output.SetCardinality(0);
		return;
	}
	
	auto &catalog = BaseMetadataFunction::GetCatalog(context, Value(bind_data.catalog_name));
	auto &transaction = DuckLakeTransaction::Get(context, catalog);

	// Get branch id
	auto branch_result = transaction.Query(StringUtil::Format(
	    "SELECT branch_id FROM {METADATA_CATALOG}.ducklake_branch "
	    "WHERE branch_name = '%s' AND status = 'active'",
	    bind_data.branch_name));
	auto branch_chunk = branch_result->Fetch();
	if (!branch_chunk || branch_chunk->size() == 0) {
		throw InvalidInputException("Branch not found: %s", bind_data.branch_name);
	}
	int64_t branch_id = branch_chunk->GetValue(0, 0).GetValue<int64_t>();

	if (branch_id == 0) {
		throw InvalidInputException("Cannot delete the main branch");
	}

	// Check for child branches
	auto children_result = transaction.Query(StringUtil::Format(
	    "SELECT COUNT(*) FROM {METADATA_CATALOG}.ducklake_branch "
	    "WHERE parent_branch_id = %lld AND status = 'active'",
	    branch_id));
	auto children_chunk = children_result->Fetch();
	if (children_chunk->GetValue(0, 0).GetValue<int64_t>() > 0) {
		throw InvalidInputException("Cannot delete branch with active child branches");
	}

	// Soft delete
	transaction.Query(StringUtil::Format(
	    "UPDATE {METADATA_CATALOG}.ducklake_branch SET status = 'deleted' WHERE branch_id = %lld",
	    branch_id));

	output.SetValue(0, 0, Value::BOOLEAN(true));
	output.SetCardinality(1);
	state.done = true;
}

TableFunction DuckLakeDeleteBranchFunction::GetFunction() {
	TableFunction func("ducklake_delete_branch", {LogicalType::VARCHAR, LogicalType::VARCHAR}, 
	                   DeleteBranchFunction, DeleteBranchBind, DeleteBranchInit);
	return func;
}

//===--------------------------------------------------------------------===//
// Get Branch Lineage
//===--------------------------------------------------------------------===//
struct BranchLineageBindData : public TableFunctionData {
	string catalog_name;
	string branch_name;
};

struct DuckLakeBranchLineageData : public GlobalTableFunctionState {
	DuckLakeBranchLineageData() : done(false) {
	}
	bool done;
};

static unique_ptr<FunctionData> BranchLineageBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("branch_id");
	return_types.emplace_back(LogicalType::BIGINT);
	
	names.emplace_back("ancestor_branch_id");
	return_types.emplace_back(LogicalType::BIGINT);
	
	names.emplace_back("max_visible_snapshot");
	return_types.emplace_back(LogicalType::BIGINT);

	auto result = make_uniq<BranchLineageBindData>();
	result->catalog_name = input.inputs[0].ToString();
	result->branch_name = input.inputs[1].ToString();
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> BranchLineageInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<DuckLakeBranchLineageData>();
}

static void BranchLineageFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<BranchLineageBindData>();
	auto &data = data_p.global_state->Cast<DuckLakeBranchLineageData>();
	
	if (data.done) {
		output.SetCardinality(0);
		return;
	}
	
	auto &catalog = BaseMetadataFunction::GetCatalog(context, Value(bind_data.catalog_name));
	auto &transaction = DuckLakeTransaction::Get(context, catalog);

	// Get branch id first
	auto branch_result = transaction.Query(StringUtil::Format(
	    "SELECT branch_id FROM {METADATA_CATALOG}.ducklake_branch "
	    "WHERE branch_name = '%s' AND status = 'active'",
	    bind_data.branch_name));
	auto branch_chunk = branch_result->Fetch();
	if (!branch_chunk || branch_chunk->size() == 0) {
		throw InvalidInputException("Branch not found: %s", bind_data.branch_name);
	}
	int64_t branch_id = branch_chunk->GetValue(0, 0).GetValue<int64_t>();

	// Get lineage
	auto result = transaction.Query(StringUtil::Format(
	    "SELECT branch_id, ancestor_branch_id, max_visible_snapshot "
	    "FROM {METADATA_CATALOG}.ducklake_branch_lineage WHERE branch_id = %lld",
	    branch_id));

	idx_t count = 0;
	while (count < STANDARD_VECTOR_SIZE) {
		auto chunk = result->Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}
		for (idx_t i = 0; i < chunk->size() && count < STANDARD_VECTOR_SIZE; i++) {
			output.SetValue(0, count, chunk->GetValue(0, i));
			output.SetValue(1, count, chunk->GetValue(1, i));
			output.SetValue(2, count, chunk->GetValue(2, i));
			count++;
		}
	}
	output.SetCardinality(count);
	data.done = true;
}

TableFunction DuckLakeBranchLineageFunction::GetFunction() {
	TableFunction func("ducklake_branch_lineage", {LogicalType::VARCHAR, LogicalType::VARCHAR}, 
	                   BranchLineageFunction, BranchLineageBind, BranchLineageInit);
	return func;
}

//===--------------------------------------------------------------------===//
// Use Branch (Switch Working Branch for Writes)
//===--------------------------------------------------------------------===//
struct UseBranchBindData : public TableFunctionData {
	string catalog_name;
	string branch_name;
};

struct UseBranchState : public GlobalTableFunctionState {
	UseBranchState() : done(false) {}
	bool done;
};

static unique_ptr<FunctionData> UseBranchBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("catalog");
	return_types.emplace_back(LogicalType::VARCHAR);
	
	names.emplace_back("branch_name");
	return_types.emplace_back(LogicalType::VARCHAR);
	
	names.emplace_back("branch_id");
	return_types.emplace_back(LogicalType::BIGINT);
	
	names.emplace_back("head_snapshot_id");
	return_types.emplace_back(LogicalType::BIGINT);

	auto result = make_uniq<UseBranchBindData>();
	result->catalog_name = input.inputs[0].ToString();
	result->branch_name = input.inputs[1].ToString();
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> UseBranchInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<UseBranchState>();
}

static void UseBranchFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<UseBranchBindData>();
	auto &state = data_p.global_state->Cast<UseBranchState>();
	
	if (state.done) {
		output.SetCardinality(0);
		return;
	}
	
	auto &catalog = BaseMetadataFunction::GetCatalog(context, Value(bind_data.catalog_name));
	auto &transaction = DuckLakeTransaction::Get(context, catalog);

	// Get branch info
	auto branch_result = transaction.Query(StringUtil::Format(
	    "SELECT branch_id, head_snapshot_id FROM {METADATA_CATALOG}.ducklake_branch "
	    "WHERE branch_name = '%s' AND status = 'active'",
	    bind_data.branch_name));
	auto branch_chunk = branch_result->Fetch();
	if (!branch_chunk || branch_chunk->size() == 0) {
		throw InvalidInputException("Branch not found: %s", bind_data.branch_name);
	}
	int64_t branch_id = branch_chunk->GetValue(0, 0).GetValue<int64_t>();
	int64_t head_snapshot_id = branch_chunk->GetValue(1, 0).GetValue<int64_t>();

	// Set the working branch in the catalog
	auto &ducklake_catalog = catalog.Cast<DuckLakeCatalog>();
	ducklake_catalog.SetWorkingBranch(BranchIndex(NumericCast<idx_t>(branch_id)));

	// Invalidate cached snapshot so subsequent queries use the new branch's snapshot
	transaction.InvalidateSnapshot();

	output.SetValue(0, 0, Value(bind_data.catalog_name));
	output.SetValue(1, 0, Value(bind_data.branch_name));
	output.SetValue(2, 0, Value::BIGINT(branch_id));
	output.SetValue(3, 0, Value::BIGINT(head_snapshot_id));
	output.SetCardinality(1);
	state.done = true;
}

TableFunction DuckLakeUseBranchFunction::GetFunction() {
	TableFunction func("ducklake_use_branch", {LogicalType::VARCHAR, LogicalType::VARCHAR}, 
	                   UseBranchFunction, UseBranchBind, UseBranchInit);
	return func;
}

//===--------------------------------------------------------------------===//
// Current Branch (Get the current working branch)
//===--------------------------------------------------------------------===//
struct CurrentBranchBindData : public TableFunctionData {
	string catalog_name;
};

struct CurrentBranchState : public GlobalTableFunctionState {
	CurrentBranchState() : done(false) {}
	bool done;
};

static unique_ptr<FunctionData> CurrentBranchBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("catalog");
	return_types.emplace_back(LogicalType::VARCHAR);
	
	names.emplace_back("branch_name");
	return_types.emplace_back(LogicalType::VARCHAR);
	
	names.emplace_back("branch_id");
	return_types.emplace_back(LogicalType::BIGINT);
	
	names.emplace_back("head_snapshot_id");
	return_types.emplace_back(LogicalType::BIGINT);

	auto result = make_uniq<CurrentBranchBindData>();
	result->catalog_name = input.inputs[0].ToString();
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> CurrentBranchInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<CurrentBranchState>();
}

static void CurrentBranchFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<CurrentBranchBindData>();
	auto &state = data_p.global_state->Cast<CurrentBranchState>();
	
	if (state.done) {
		output.SetCardinality(0);
		return;
	}
	
	auto &catalog = BaseMetadataFunction::GetCatalog(context, Value(bind_data.catalog_name));
	auto &transaction = DuckLakeTransaction::Get(context, catalog);

	// Get the current working branch from the catalog
	auto &ducklake_catalog = catalog.Cast<DuckLakeCatalog>();
	BranchIndex working_branch = ducklake_catalog.GetWorkingBranch();

	// Get branch info
	auto branch_result = transaction.Query(StringUtil::Format(
	    "SELECT branch_name, head_snapshot_id FROM {METADATA_CATALOG}.ducklake_branch "
	    "WHERE branch_id = %lld AND status = 'active'",
	    NumericCast<int64_t>(working_branch.index)));
	auto branch_chunk = branch_result->Fetch();
	if (!branch_chunk || branch_chunk->size() == 0) {
		throw InvalidInputException("Current working branch not found (id: %lld)", 
		                           NumericCast<int64_t>(working_branch.index));
	}
	string branch_name = branch_chunk->GetValue(0, 0).ToString();
	int64_t head_snapshot_id = branch_chunk->GetValue(1, 0).GetValue<int64_t>();

	output.SetValue(0, 0, Value(bind_data.catalog_name));
	output.SetValue(1, 0, Value(branch_name));
	output.SetValue(2, 0, Value::BIGINT(NumericCast<int64_t>(working_branch.index)));
	output.SetValue(3, 0, Value::BIGINT(head_snapshot_id));
	output.SetCardinality(1);
	state.done = true;
}

TableFunction DuckLakeCurrentBranchFunction::GetFunction() {
	TableFunction func("ducklake_current_branch", {LogicalType::VARCHAR},
	                   CurrentBranchFunction, CurrentBranchBind, CurrentBranchInit);
	return func;
}

//===--------------------------------------------------------------------===//
// Branch Count
//===--------------------------------------------------------------------===//
struct BranchCountBindData : public TableFunctionData {
	string catalog_name;
	string status_filter;  // empty means all
};

struct BranchCountState : public GlobalTableFunctionState {
	BranchCountState() : done(false) {}
	bool done;
};

static unique_ptr<FunctionData> BranchCountBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("count");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("status_filter");
	return_types.emplace_back(LogicalType::VARCHAR);

	auto result = make_uniq<BranchCountBindData>();
	result->catalog_name = input.inputs[0].ToString();

	if (input.inputs.size() > 1 && !input.inputs[1].IsNull()) {
		result->status_filter = input.inputs[1].ToString();
	}

	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> BranchCountInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<BranchCountState>();
}

static void BranchCountFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<BranchCountBindData>();
	auto &state = data_p.global_state->Cast<BranchCountState>();

	if (state.done) {
		output.SetCardinality(0);
		return;
	}

	auto &catalog = BaseMetadataFunction::GetCatalog(context, Value(bind_data.catalog_name));
	auto &transaction = DuckLakeTransaction::Get(context, catalog);

	string query;
	if (bind_data.status_filter.empty()) {
		query = "SELECT COUNT(*) FROM {METADATA_CATALOG}.ducklake_branch";
	} else {
		query = StringUtil::Format(
		    "SELECT COUNT(*) FROM {METADATA_CATALOG}.ducklake_branch WHERE status = '%s'",
		    bind_data.status_filter);
	}

	auto result = transaction.Query(query);
	auto chunk = result->Fetch();

	int64_t count = chunk ? chunk->GetValue(0, 0).GetValue<int64_t>() : 0;

	output.SetValue(0, 0, Value::BIGINT(count));
	output.SetValue(1, 0, bind_data.status_filter.empty() ? Value("all") : Value(bind_data.status_filter));
	output.SetCardinality(1);
	state.done = true;
}

TableFunctionSet DuckLakeBranchCountFunction::GetFunctions() {
	TableFunctionSet set("ducklake_branch_count");

	// ducklake_branch_count(catalog)
	TableFunction func1({LogicalType::VARCHAR}, BranchCountFunction, BranchCountBind, BranchCountInit);
	set.AddFunction(func1);

	// ducklake_branch_count(catalog, status)
	TableFunction func2({LogicalType::VARCHAR, LogicalType::VARCHAR}, BranchCountFunction, BranchCountBind, BranchCountInit);
	set.AddFunction(func2);

	return set;
}

//===--------------------------------------------------------------------===//
// Branch Stats
//===--------------------------------------------------------------------===//
struct BranchStatsBindData : public TableFunctionData {
	string catalog_name;
	string branch_name;
};

struct BranchStatsState : public GlobalTableFunctionState {
	BranchStatsState() : done(false) {}
	bool done;
};

static unique_ptr<FunctionData> BranchStatsBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("branch_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("table_count");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("schema_count");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("view_count");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("data_file_count");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("total_rows");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("total_size_bytes");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("snapshot_count");
	return_types.emplace_back(LogicalType::BIGINT);

	auto result = make_uniq<BranchStatsBindData>();
	result->catalog_name = input.inputs[0].ToString();
	result->branch_name = input.inputs[1].ToString();
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> BranchStatsInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<BranchStatsState>();
}

static void BranchStatsFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<BranchStatsBindData>();
	auto &state = data_p.global_state->Cast<BranchStatsState>();

	if (state.done) {
		output.SetCardinality(0);
		return;
	}

	auto &catalog = BaseMetadataFunction::GetCatalog(context, Value(bind_data.catalog_name));
	auto &transaction = DuckLakeTransaction::Get(context, catalog);

	// Get branch info
	auto branch_result = transaction.Query(StringUtil::Format(
	    "SELECT branch_id, head_snapshot_id FROM {METADATA_CATALOG}.ducklake_branch "
	    "WHERE branch_name = '%s' AND status = 'active'",
	    bind_data.branch_name));
	auto branch_chunk = branch_result->Fetch();
	if (!branch_chunk || branch_chunk->size() == 0) {
		throw InvalidInputException("Branch not found: %s", bind_data.branch_name);
	}
	int64_t branch_id = branch_chunk->GetValue(0, 0).GetValue<int64_t>();
	int64_t head_snapshot = branch_chunk->GetValue(1, 0).GetValue<int64_t>();

	// Build lineage visibility CTE for complex queries
	string lineage_cte = StringUtil::Format(R"(
		WITH branch_visibility AS (
			SELECT ancestor_branch_id, max_visible_snapshot
			FROM {METADATA_CATALOG}.ducklake_branch_lineage
			WHERE branch_id = %lld
		)
	)", branch_id);

	// Table count - count visible tables not deleted on this branch
	auto table_result = transaction.Query(lineage_cte + StringUtil::Format(R"(
		SELECT COUNT(DISTINCT t.table_id)
		FROM {METADATA_CATALOG}.ducklake_table t
		JOIN branch_visibility bv ON t.branch_id = bv.ancestor_branch_id
		WHERE t.begin_snapshot <= bv.max_visible_snapshot
		  AND (t.end_snapshot IS NULL OR t.end_snapshot > bv.max_visible_snapshot)
		  AND NOT EXISTS (
		      SELECT 1 FROM {METADATA_CATALOG}.ducklake_branch_table_deletion td
		      WHERE td.branch_id = %lld AND td.table_id = t.table_id
		  )
	)", branch_id));
	auto table_chunk = table_result->Fetch();
	int64_t table_count = table_chunk ? table_chunk->GetValue(0, 0).GetValue<int64_t>() : 0;

	// Schema count
	auto schema_result = transaction.Query(lineage_cte + StringUtil::Format(R"(
		SELECT COUNT(DISTINCT s.schema_id)
		FROM {METADATA_CATALOG}.ducklake_schema s
		JOIN branch_visibility bv ON s.branch_id = bv.ancestor_branch_id
		WHERE s.begin_snapshot <= bv.max_visible_snapshot
		  AND (s.end_snapshot IS NULL OR s.end_snapshot > bv.max_visible_snapshot)
		  AND NOT EXISTS (
		      SELECT 1 FROM {METADATA_CATALOG}.ducklake_branch_schema_deletion sd
		      WHERE sd.branch_id = %lld AND sd.schema_id = s.schema_id
		  )
	)", branch_id));
	auto schema_chunk = schema_result->Fetch();
	int64_t schema_count = schema_chunk ? schema_chunk->GetValue(0, 0).GetValue<int64_t>() : 0;

	// View count
	auto view_result = transaction.Query(lineage_cte + StringUtil::Format(R"(
		SELECT COUNT(DISTINCT v.view_id)
		FROM {METADATA_CATALOG}.ducklake_view v
		JOIN branch_visibility bv ON v.branch_id = bv.ancestor_branch_id
		WHERE v.begin_snapshot <= bv.max_visible_snapshot
		  AND (v.end_snapshot IS NULL OR v.end_snapshot > bv.max_visible_snapshot)
		  AND NOT EXISTS (
		      SELECT 1 FROM {METADATA_CATALOG}.ducklake_branch_view_deletion vd
		      WHERE vd.branch_id = %lld AND vd.view_id = v.view_id
		  )
	)", branch_id));
	auto view_chunk = view_result->Fetch();
	int64_t view_count = view_chunk ? view_chunk->GetValue(0, 0).GetValue<int64_t>() : 0;

	// Data file count - visible files not deleted
	auto file_result = transaction.Query(lineage_cte + StringUtil::Format(R"(
		SELECT COUNT(*)
		FROM {METADATA_CATALOG}.ducklake_data_file df
		JOIN branch_visibility bv ON df.branch_id = bv.ancestor_branch_id
		WHERE df.begin_snapshot <= bv.max_visible_snapshot
		  AND (df.end_snapshot IS NULL OR df.end_snapshot > bv.max_visible_snapshot)
		  AND NOT EXISTS (
		      SELECT 1 FROM {METADATA_CATALOG}.ducklake_branch_file_deletion fd
		      WHERE fd.branch_id = %lld AND fd.data_file_id = df.data_file_id
		  )
	)", branch_id));
	auto file_chunk = file_result->Fetch();
	int64_t file_count = file_chunk ? file_chunk->GetValue(0, 0).GetValue<int64_t>() : 0;

	// Total rows and size from table stats
	auto stats_result = transaction.Query(StringUtil::Format(
	    "SELECT COALESCE(SUM(record_count), 0), COALESCE(SUM(file_size_bytes), 0) "
	    "FROM {METADATA_CATALOG}.ducklake_table_stats WHERE branch_id = %lld",
	    branch_id));
	auto stats_chunk = stats_result->Fetch();
	int64_t total_rows = stats_chunk ? stats_chunk->GetValue(0, 0).GetValue<int64_t>() : 0;
	int64_t total_size = stats_chunk ? stats_chunk->GetValue(1, 0).GetValue<int64_t>() : 0;

	// Snapshot count
	auto snapshot_result = transaction.Query(StringUtil::Format(
	    "SELECT COUNT(*) FROM {METADATA_CATALOG}.ducklake_snapshot WHERE branch_id = %lld",
	    branch_id));
	auto snapshot_chunk = snapshot_result->Fetch();
	int64_t snapshot_count = snapshot_chunk ? snapshot_chunk->GetValue(0, 0).GetValue<int64_t>() : 0;

	output.SetValue(0, 0, Value(bind_data.branch_name));
	output.SetValue(1, 0, Value::BIGINT(table_count));
	output.SetValue(2, 0, Value::BIGINT(schema_count));
	output.SetValue(3, 0, Value::BIGINT(view_count));
	output.SetValue(4, 0, Value::BIGINT(file_count));
	output.SetValue(5, 0, Value::BIGINT(total_rows));
	output.SetValue(6, 0, Value::BIGINT(total_size));
	output.SetValue(7, 0, Value::BIGINT(snapshot_count));
	output.SetCardinality(1);
	state.done = true;
}

TableFunction DuckLakeBranchStatsFunction::GetFunction() {
	TableFunction func("ducklake_branch_stats", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                   BranchStatsFunction, BranchStatsBind, BranchStatsInit);
	return func;
}

//===--------------------------------------------------------------------===//
// Search Branches
//===--------------------------------------------------------------------===//
struct SearchBranchesBindData : public TableFunctionData {
	string catalog_name;
	string pattern;         // LIKE pattern, empty = no filter
	string status;          // empty = no filter
	Value created_after;    // NULL = no filter
	Value created_before;   // NULL = no filter
	string parent_name;     // empty = no filter
};

struct SearchBranchesState : public GlobalTableFunctionState {
	SearchBranchesState() : done(false) {}
	bool done;
};

static unique_ptr<FunctionData> SearchBranchesBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("branch_id");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("branch_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("parent_branch_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("created_at");
	return_types.emplace_back(LogicalType::TIMESTAMP_TZ);

	names.emplace_back("status");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("head_snapshot_id");
	return_types.emplace_back(LogicalType::BIGINT);

	auto result = make_uniq<SearchBranchesBindData>();
	result->catalog_name = input.inputs[0].ToString();

	// All filters are optional
	if (input.inputs.size() > 1 && !input.inputs[1].IsNull()) {
		result->pattern = input.inputs[1].ToString();
	}
	if (input.inputs.size() > 2 && !input.inputs[2].IsNull()) {
		result->status = input.inputs[2].ToString();
	}
	if (input.inputs.size() > 3 && !input.inputs[3].IsNull()) {
		result->created_after = input.inputs[3];
	}
	if (input.inputs.size() > 4 && !input.inputs[4].IsNull()) {
		result->created_before = input.inputs[4];
	}
	if (input.inputs.size() > 5 && !input.inputs[5].IsNull()) {
		result->parent_name = input.inputs[5].ToString();
	}

	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> SearchBranchesInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<SearchBranchesState>();
}

static void SearchBranchesFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<SearchBranchesBindData>();
	auto &state = data_p.global_state->Cast<SearchBranchesState>();

	if (state.done) {
		output.SetCardinality(0);
		return;
	}

	auto &catalog = BaseMetadataFunction::GetCatalog(context, Value(bind_data.catalog_name));
	auto &transaction = DuckLakeTransaction::Get(context, catalog);

	// Build dynamic query
	string query = R"(
		SELECT b.branch_id, b.branch_name, p.branch_name as parent_branch_name,
		       b.created_at, b.status, b.head_snapshot_id
		FROM {METADATA_CATALOG}.ducklake_branch b
		LEFT JOIN {METADATA_CATALOG}.ducklake_branch p ON b.parent_branch_id = p.branch_id
		WHERE 1=1
	)";

	if (!bind_data.pattern.empty()) {
		query += StringUtil::Format(" AND b.branch_name LIKE '%s'", bind_data.pattern);
	}
	if (!bind_data.status.empty()) {
		query += StringUtil::Format(" AND b.status = '%s'", bind_data.status);
	}
	if (!bind_data.created_after.IsNull()) {
		query += StringUtil::Format(" AND b.created_at >= '%s'", bind_data.created_after.ToString());
	}
	if (!bind_data.created_before.IsNull()) {
		query += StringUtil::Format(" AND b.created_at <= '%s'", bind_data.created_before.ToString());
	}
	if (!bind_data.parent_name.empty()) {
		// Resolve parent name to id
		auto parent_result = transaction.Query(StringUtil::Format(
		    "SELECT branch_id FROM {METADATA_CATALOG}.ducklake_branch WHERE branch_name = '%s'",
		    bind_data.parent_name));
		auto parent_chunk = parent_result->Fetch();
		if (parent_chunk && parent_chunk->size() > 0) {
			int64_t parent_id = parent_chunk->GetValue(0, 0).GetValue<int64_t>();
			query += StringUtil::Format(" AND b.parent_branch_id = %lld", parent_id);
		} else {
			// Parent not found, return empty
			output.SetCardinality(0);
			state.done = true;
			return;
		}
	}

	query += " ORDER BY b.created_at DESC";

	auto result = transaction.Query(query);

	idx_t count = 0;
	while (count < STANDARD_VECTOR_SIZE) {
		auto chunk = result->Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}
		for (idx_t i = 0; i < chunk->size() && count < STANDARD_VECTOR_SIZE; i++) {
			output.SetValue(0, count, chunk->GetValue(0, i));  // branch_id
			output.SetValue(1, count, chunk->GetValue(1, i));  // branch_name
			output.SetValue(2, count, chunk->GetValue(2, i));  // parent_branch_name
			output.SetValue(3, count, chunk->GetValue(3, i));  // created_at
			output.SetValue(4, count, chunk->GetValue(4, i));  // status
			output.SetValue(5, count, chunk->GetValue(5, i));  // head_snapshot_id
			count++;
		}
	}
	output.SetCardinality(count);
	state.done = true;
}

TableFunctionSet DuckLakeSearchBranchesFunction::GetFunctions() {
	TableFunctionSet set("ducklake_search_branches");

	// ducklake_search_branches(catalog) - list all
	TableFunction func1({LogicalType::VARCHAR}, SearchBranchesFunction, SearchBranchesBind, SearchBranchesInit);
	set.AddFunction(func1);

	// ducklake_search_branches(catalog, pattern)
	TableFunction func2({LogicalType::VARCHAR, LogicalType::VARCHAR},
	                    SearchBranchesFunction, SearchBranchesBind, SearchBranchesInit);
	set.AddFunction(func2);

	// ducklake_search_branches(catalog, pattern, status)
	TableFunction func3({LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                    SearchBranchesFunction, SearchBranchesBind, SearchBranchesInit);
	set.AddFunction(func3);

	// ducklake_search_branches(catalog, pattern, status, created_after)
	TableFunction func4({LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::TIMESTAMP_TZ},
	                    SearchBranchesFunction, SearchBranchesBind, SearchBranchesInit);
	set.AddFunction(func4);

	// ducklake_search_branches(catalog, pattern, status, created_after, created_before)
	TableFunction func5({LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                     LogicalType::TIMESTAMP_TZ, LogicalType::TIMESTAMP_TZ},
	                    SearchBranchesFunction, SearchBranchesBind, SearchBranchesInit);
	set.AddFunction(func5);

	// ducklake_search_branches(catalog, pattern, status, created_after, created_before, parent_name)
	TableFunction func6({LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                     LogicalType::TIMESTAMP_TZ, LogicalType::TIMESTAMP_TZ, LogicalType::VARCHAR},
	                    SearchBranchesFunction, SearchBranchesBind, SearchBranchesInit);
	set.AddFunction(func6);

	return set;
}

//===--------------------------------------------------------------------===//
// Branches By Status
//===--------------------------------------------------------------------===//
struct BranchesByStatusBindData : public TableFunctionData {
	string catalog_name;
	string status;
};

struct BranchesByStatusState : public GlobalTableFunctionState {
	BranchesByStatusState() : done(false) {}
	bool done;
};

static unique_ptr<FunctionData> BranchesByStatusBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("branch_id");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("branch_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("parent_branch_id");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("fork_snapshot_id");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("head_snapshot_id");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("created_at");
	return_types.emplace_back(LogicalType::TIMESTAMP_TZ);

	names.emplace_back("status");
	return_types.emplace_back(LogicalType::VARCHAR);

	auto result = make_uniq<BranchesByStatusBindData>();
	result->catalog_name = input.inputs[0].ToString();
	result->status = input.inputs[1].ToString();

	// Validate status
	string status_lower = StringUtil::Lower(result->status);
	if (status_lower != "active" && status_lower != "merged" &&
	    status_lower != "archived" && status_lower != "deleted") {
		throw InvalidInputException("Invalid status '%s'. Must be one of: active, merged, archived, deleted",
		                           result->status);
	}

	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> BranchesByStatusInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<BranchesByStatusState>();
}

static void BranchesByStatusFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<BranchesByStatusBindData>();
	auto &state = data_p.global_state->Cast<BranchesByStatusState>();

	if (state.done) {
		output.SetCardinality(0);
		return;
	}

	auto &catalog = BaseMetadataFunction::GetCatalog(context, Value(bind_data.catalog_name));
	auto &transaction = DuckLakeTransaction::Get(context, catalog);

	auto result = transaction.Query(StringUtil::Format(
	    "SELECT branch_id, branch_name, parent_branch_id, fork_snapshot_id, "
	    "       head_snapshot_id, created_at, status "
	    "FROM {METADATA_CATALOG}.ducklake_branch WHERE status = '%s' ORDER BY created_at DESC",
	    bind_data.status));

	idx_t count = 0;
	while (count < STANDARD_VECTOR_SIZE) {
		auto chunk = result->Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}
		for (idx_t i = 0; i < chunk->size() && count < STANDARD_VECTOR_SIZE; i++) {
			output.SetValue(0, count, chunk->GetValue(0, i));  // branch_id
			output.SetValue(1, count, chunk->GetValue(1, i));  // branch_name
			output.SetValue(2, count, chunk->GetValue(2, i));  // parent_branch_id
			output.SetValue(3, count, chunk->GetValue(3, i));  // fork_snapshot_id
			output.SetValue(4, count, chunk->GetValue(4, i));  // head_snapshot_id
			output.SetValue(5, count, chunk->GetValue(5, i));  // created_at
			output.SetValue(6, count, chunk->GetValue(6, i));  // status
			count++;
		}
	}
	output.SetCardinality(count);
	state.done = true;
}

TableFunction DuckLakeBranchesByStatusFunction::GetFunction() {
	TableFunction func("ducklake_branches_by_status", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                   BranchesByStatusFunction, BranchesByStatusBind, BranchesByStatusInit);
	return func;
}

//===--------------------------------------------------------------------===//
// Branches By Age
//===--------------------------------------------------------------------===//
struct BranchesByAgeBindData : public TableFunctionData {
	string catalog_name;
	int64_t days;           // positive = older than, negative = newer than
	string status_filter;   // optional
};

struct BranchesByAgeState : public GlobalTableFunctionState {
	BranchesByAgeState() : done(false) {}
	bool done;
};

static unique_ptr<FunctionData> BranchesByAgeBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("branch_id");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("branch_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("parent_branch_id");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("fork_snapshot_id");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("head_snapshot_id");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("created_at");
	return_types.emplace_back(LogicalType::TIMESTAMP_TZ);

	names.emplace_back("status");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("age_days");
	return_types.emplace_back(LogicalType::DOUBLE);

	auto result = make_uniq<BranchesByAgeBindData>();
	result->catalog_name = input.inputs[0].ToString();
	result->days = input.inputs[1].GetValue<int64_t>();

	if (input.inputs.size() > 2 && !input.inputs[2].IsNull()) {
		result->status_filter = input.inputs[2].ToString();
	}

	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> BranchesByAgeInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<BranchesByAgeState>();
}

static void BranchesByAgeFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<BranchesByAgeBindData>();
	auto &state = data_p.global_state->Cast<BranchesByAgeState>();

	if (state.done) {
		output.SetCardinality(0);
		return;
	}

	auto &catalog = BaseMetadataFunction::GetCatalog(context, Value(bind_data.catalog_name));
	auto &transaction = DuckLakeTransaction::Get(context, catalog);

	string query = R"(
		SELECT branch_id, branch_name, parent_branch_id, fork_snapshot_id,
		       head_snapshot_id, created_at, status,
		       EXTRACT(EPOCH FROM (NOW() - created_at)) / 86400.0 as age_days
		FROM {METADATA_CATALOG}.ducklake_branch
		WHERE 1=1
	)";

	int64_t abs_days = bind_data.days < 0 ? -bind_data.days : bind_data.days;

	if (bind_data.days > 0) {
		// Older than N days
		query += StringUtil::Format(" AND created_at <= NOW() - INTERVAL '%lld days'", abs_days);
	} else if (bind_data.days < 0) {
		// Newer than N days
		query += StringUtil::Format(" AND created_at >= NOW() - INTERVAL '%lld days'", abs_days);
	}

	if (!bind_data.status_filter.empty()) {
		query += StringUtil::Format(" AND status = '%s'", bind_data.status_filter);
	}

	// Order: oldest first for "older than", newest first for "newer than"
	if (bind_data.days > 0) {
		query += " ORDER BY created_at ASC";
	} else {
		query += " ORDER BY created_at DESC";
	}

	auto result = transaction.Query(query);

	idx_t count = 0;
	while (count < STANDARD_VECTOR_SIZE) {
		auto chunk = result->Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}
		for (idx_t i = 0; i < chunk->size() && count < STANDARD_VECTOR_SIZE; i++) {
			output.SetValue(0, count, chunk->GetValue(0, i));  // branch_id
			output.SetValue(1, count, chunk->GetValue(1, i));  // branch_name
			output.SetValue(2, count, chunk->GetValue(2, i));  // parent_branch_id
			output.SetValue(3, count, chunk->GetValue(3, i));  // fork_snapshot_id
			output.SetValue(4, count, chunk->GetValue(4, i));  // head_snapshot_id
			output.SetValue(5, count, chunk->GetValue(5, i));  // created_at
			output.SetValue(6, count, chunk->GetValue(6, i));  // status
			output.SetValue(7, count, chunk->GetValue(7, i));  // age_days
			count++;
		}
	}
	output.SetCardinality(count);
	state.done = true;
}

TableFunctionSet DuckLakeBranchesByAgeFunction::GetFunctions() {
	TableFunctionSet set("ducklake_branches_by_age");

	// ducklake_branches_by_age(catalog, days)
	TableFunction func2({LogicalType::VARCHAR, LogicalType::BIGINT},
	                    BranchesByAgeFunction, BranchesByAgeBind, BranchesByAgeInit);
	set.AddFunction(func2);

	// ducklake_branches_by_age(catalog, days, status)
	TableFunction func3({LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::VARCHAR},
	                    BranchesByAgeFunction, BranchesByAgeBind, BranchesByAgeInit);
	set.AddFunction(func3);

	return set;
}

//===--------------------------------------------------------------------===//
// Branch Activity
//===--------------------------------------------------------------------===//
struct BranchActivityBindData : public TableFunctionData {
	string catalog_name;
	string order_by;   // 'last_modified', 'created', 'snapshot_count'
	int64_t limit;     // 0 = no limit
};

struct BranchActivityState : public GlobalTableFunctionState {
	BranchActivityState() : done(false) {}
	bool done;
};

static unique_ptr<FunctionData> BranchActivityBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("branch_id");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("branch_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("created_at");
	return_types.emplace_back(LogicalType::TIMESTAMP_TZ);

	names.emplace_back("last_modified_at");
	return_types.emplace_back(LogicalType::TIMESTAMP_TZ);

	names.emplace_back("snapshot_count");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("head_snapshot_id");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("status");
	return_types.emplace_back(LogicalType::VARCHAR);

	auto result = make_uniq<BranchActivityBindData>();
	result->catalog_name = input.inputs[0].ToString();
	result->order_by = "last_modified";  // default
	result->limit = 0;  // no limit

	if (input.inputs.size() > 1 && !input.inputs[1].IsNull()) {
		result->order_by = input.inputs[1].ToString();
		// Validate
		string ob = StringUtil::Lower(result->order_by);
		if (ob != "last_modified" && ob != "created" && ob != "snapshot_count") {
			throw InvalidInputException("Invalid order_by '%s'. Must be: last_modified, created, or snapshot_count",
			                           result->order_by);
		}
	}
	if (input.inputs.size() > 2 && !input.inputs[2].IsNull()) {
		result->limit = input.inputs[2].GetValue<int64_t>();
	}

	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> BranchActivityInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<BranchActivityState>();
}

static void BranchActivityFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<BranchActivityBindData>();
	auto &state = data_p.global_state->Cast<BranchActivityState>();

	if (state.done) {
		output.SetCardinality(0);
		return;
	}

	auto &catalog = BaseMetadataFunction::GetCatalog(context, Value(bind_data.catalog_name));
	auto &transaction = DuckLakeTransaction::Get(context, catalog);

	// Use snapshot table to derive activity
	string query = R"(
		SELECT b.branch_id, b.branch_name, b.created_at,
		       MAX(s.snapshot_time) as last_modified_at,
		       COUNT(s.snapshot_id) as snapshot_count,
		       b.head_snapshot_id, b.status
		FROM {METADATA_CATALOG}.ducklake_branch b
		LEFT JOIN {METADATA_CATALOG}.ducklake_snapshot s ON b.branch_id = s.branch_id
		WHERE b.status = 'active'
		GROUP BY b.branch_id, b.branch_name, b.created_at, b.head_snapshot_id, b.status
	)";

	string ob = StringUtil::Lower(bind_data.order_by);
	if (ob == "last_modified") {
		query += " ORDER BY last_modified_at DESC NULLS LAST";
	} else if (ob == "created") {
		query += " ORDER BY b.created_at DESC";
	} else if (ob == "snapshot_count") {
		query += " ORDER BY snapshot_count DESC";
	}

	if (bind_data.limit > 0) {
		query += StringUtil::Format(" LIMIT %lld", bind_data.limit);
	}

	auto result = transaction.Query(query);

	idx_t count = 0;
	while (count < STANDARD_VECTOR_SIZE) {
		auto chunk = result->Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}
		for (idx_t i = 0; i < chunk->size() && count < STANDARD_VECTOR_SIZE; i++) {
			output.SetValue(0, count, chunk->GetValue(0, i));  // branch_id
			output.SetValue(1, count, chunk->GetValue(1, i));  // branch_name
			output.SetValue(2, count, chunk->GetValue(2, i));  // created_at
			output.SetValue(3, count, chunk->GetValue(3, i));  // last_modified_at
			output.SetValue(4, count, chunk->GetValue(4, i));  // snapshot_count
			output.SetValue(5, count, chunk->GetValue(5, i));  // head_snapshot_id
			output.SetValue(6, count, chunk->GetValue(6, i));  // status
			count++;
		}
	}
	output.SetCardinality(count);
	state.done = true;
}

TableFunctionSet DuckLakeBranchActivityFunction::GetFunctions() {
	TableFunctionSet set("ducklake_branch_activity");

	// ducklake_branch_activity(catalog)
	TableFunction func1({LogicalType::VARCHAR}, BranchActivityFunction, BranchActivityBind, BranchActivityInit);
	set.AddFunction(func1);

	// ducklake_branch_activity(catalog, order_by)
	TableFunction func2({LogicalType::VARCHAR, LogicalType::VARCHAR},
	                    BranchActivityFunction, BranchActivityBind, BranchActivityInit);
	set.AddFunction(func2);

	// ducklake_branch_activity(catalog, order_by, limit)
	TableFunction func3({LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT},
	                    BranchActivityFunction, BranchActivityBind, BranchActivityInit);
	set.AddFunction(func3);

	return set;
}

} // namespace duckdb
