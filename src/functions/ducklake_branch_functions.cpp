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

} // namespace duckdb
