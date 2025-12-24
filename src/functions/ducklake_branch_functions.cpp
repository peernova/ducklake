#include "functions/ducklake_table_functions.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_catalog.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/common/optional_idx.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// ducklake_branches() - List all branches
//===--------------------------------------------------------------------===//

static unique_ptr<FunctionData> DuckLakeBranchesBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
	auto &catalog = BaseMetadataFunction::GetCatalog(context, input.inputs[0]);
	auto &transaction = DuckLakeTransaction::Get(context, catalog);

	auto &metadata_manager = transaction.GetMetadataManager();
	auto branches = metadata_manager.GetAllBranches();
	auto result = make_uniq<MetadataBindData>();

	for (auto &branch : branches) {
		vector<Value> row_values;
		row_values.push_back(Value(branch.name));
		row_values.push_back(Value::BIGINT(NumericCast<int64_t>(branch.snapshot_id)));
		row_values.push_back(Value::TIMESTAMPTZ(branch.created_at));
		row_values.push_back(branch.created_by);
		row_values.push_back(branch.description);
		result->rows.push_back(std::move(row_values));
	}

	names.emplace_back("branch_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("snapshot_id");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("created_at");
	return_types.emplace_back(LogicalType::TIMESTAMP_TZ);

	names.emplace_back("created_by");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("description");
	return_types.emplace_back(LogicalType::VARCHAR);

	return std::move(result);
}

DuckLakeBranchesFunction::DuckLakeBranchesFunction()
    : BaseMetadataFunction("ducklake_branches", DuckLakeBranchesBind) {
}

//===--------------------------------------------------------------------===//
// ducklake_current_branch() - Get the current branch
//===--------------------------------------------------------------------===//

static unique_ptr<FunctionData> DuckLakeCurrentBranchBind(ClientContext &context, TableFunctionBindInput &input,
                                                           vector<LogicalType> &return_types, vector<string> &names) {
	auto &catalog = BaseMetadataFunction::GetCatalog(context, input.inputs[0]);
	auto &transaction = DuckLakeTransaction::Get(context, catalog);

	auto &metadata_manager = transaction.GetMetadataManager();
	auto current_branch = metadata_manager.GetCurrentBranch();
	auto result = make_uniq<MetadataBindData>();

	vector<Value> row_values;
	row_values.push_back(Value(current_branch));
	result->rows.push_back(std::move(row_values));

	names.emplace_back("current_branch");
	return_types.emplace_back(LogicalType::VARCHAR);

	return std::move(result);
}

DuckLakeCurrentBranchFunction::DuckLakeCurrentBranchFunction()
    : BaseMetadataFunction("ducklake_current_branch", DuckLakeCurrentBranchBind) {
}

//===--------------------------------------------------------------------===//
// ducklake_create_branch() - Create a new branch
//===--------------------------------------------------------------------===//

struct CreateBranchBindData : public TableFunctionData {
	string catalog_name;
	string branch_name;
	optional_idx snapshot_id;
	Value created_by;
	Value description;
};

struct CreateBranchState : public GlobalTableFunctionState {
	bool finished = false;
};

static unique_ptr<FunctionData> DuckLakeCreateBranchBind(ClientContext &context, TableFunctionBindInput &input,
                                                          vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<CreateBranchBindData>();
	result->catalog_name = input.inputs[0].GetValue<string>();
	result->branch_name = input.inputs[1].GetValue<string>();

	// Get named parameters
	for (auto &kv : input.named_parameters) {
		if (kv.first == "snapshot_id") {
			result->snapshot_id = kv.second.GetValue<idx_t>();
		} else if (kv.first == "created_by") {
			result->created_by = kv.second;
		} else if (kv.first == "description") {
			result->description = kv.second;
		}
	}

	// If snapshot_id not specified, use the latest snapshot
	if (!result->snapshot_id.IsValid()) {
		auto &catalog = BaseMetadataFunction::GetCatalog(context, input.inputs[0]);
		auto &transaction = DuckLakeTransaction::Get(context, catalog);
		auto snapshot = transaction.GetSnapshot();
		result->snapshot_id = snapshot.snapshot_id;
	}

	names.emplace_back("success");
	return_types.emplace_back(LogicalType::BOOLEAN);

	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> DuckLakeCreateBranchInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<CreateBranchState>();
}

static void DuckLakeCreateBranchExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<CreateBranchState>();
	if (state.finished) {
		output.SetCardinality(0);
		return;
	}

	auto &bind_data = data_p.bind_data->CastNoConst<CreateBranchBindData>();
	auto &catalog = Catalog::GetCatalog(context, bind_data.catalog_name);
	auto &transaction = DuckLakeTransaction::Get(context, catalog);

	auto &metadata_manager = transaction.GetMetadataManager();
	metadata_manager.CreateBranch(bind_data.branch_name, bind_data.snapshot_id.GetIndex(), bind_data.created_by,
	                              bind_data.description);

	output.SetCardinality(1);
	output.SetValue(0, 0, Value::BOOLEAN(true));
	state.finished = true;
}

TableFunctionSet DuckLakeCreateBranchFunction::GetFunctions() {
	TableFunctionSet set("ducklake_create_branch");

	TableFunction func({LogicalType::VARCHAR, LogicalType::VARCHAR}, DuckLakeCreateBranchExecute,
	                   DuckLakeCreateBranchBind, DuckLakeCreateBranchInit);
	func.named_parameters["snapshot_id"] = LogicalType::BIGINT;
	func.named_parameters["created_by"] = LogicalType::VARCHAR;
	func.named_parameters["description"] = LogicalType::VARCHAR;
	set.AddFunction(func);

	return set;
}

//===--------------------------------------------------------------------===//
// ducklake_drop_branch() - Drop a branch
//===--------------------------------------------------------------------===//

struct DropBranchBindData : public TableFunctionData {
	string catalog_name;
	string branch_name;
	bool if_exists = false;
};

struct DropBranchState : public GlobalTableFunctionState {
	bool finished = false;
};

static unique_ptr<FunctionData> DuckLakeDropBranchBind(ClientContext &context, TableFunctionBindInput &input,
                                                        vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<DropBranchBindData>();
	result->catalog_name = input.inputs[0].GetValue<string>();
	result->branch_name = input.inputs[1].GetValue<string>();

	for (auto &kv : input.named_parameters) {
		if (kv.first == "if_exists") {
			result->if_exists = kv.second.GetValue<bool>();
		}
	}

	names.emplace_back("success");
	return_types.emplace_back(LogicalType::BOOLEAN);

	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> DuckLakeDropBranchInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<DropBranchState>();
}

static void DuckLakeDropBranchExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<DropBranchState>();
	if (state.finished) {
		output.SetCardinality(0);
		return;
	}

	auto &bind_data = data_p.bind_data->CastNoConst<DropBranchBindData>();
	auto &catalog = Catalog::GetCatalog(context, bind_data.catalog_name);
	auto &transaction = DuckLakeTransaction::Get(context, catalog);

	auto &metadata_manager = transaction.GetMetadataManager();

	// Check if branch exists when if_exists is true
	if (bind_data.if_exists) {
		auto existing = metadata_manager.GetBranch(bind_data.branch_name);
		if (!existing) {
			output.SetCardinality(1);
			output.SetValue(0, 0, Value::BOOLEAN(false));
			state.finished = true;
			return;
		}
	}

	metadata_manager.DropBranch(bind_data.branch_name);

	output.SetCardinality(1);
	output.SetValue(0, 0, Value::BOOLEAN(true));
	state.finished = true;
}

TableFunctionSet DuckLakeDropBranchFunction::GetFunctions() {
	TableFunctionSet set("ducklake_drop_branch");

	TableFunction func({LogicalType::VARCHAR, LogicalType::VARCHAR}, DuckLakeDropBranchExecute,
	                   DuckLakeDropBranchBind, DuckLakeDropBranchInit);
	func.named_parameters["if_exists"] = LogicalType::BOOLEAN;
	set.AddFunction(func);

	return set;
}

//===--------------------------------------------------------------------===//
// ducklake_use_branch() - Switch to a different branch
//===--------------------------------------------------------------------===//

struct UseBranchBindData : public TableFunctionData {
	string catalog_name;
	string branch_name;
};

struct UseBranchState : public GlobalTableFunctionState {
	bool finished = false;
};

static unique_ptr<FunctionData> DuckLakeUseBranchBind(ClientContext &context, TableFunctionBindInput &input,
                                                       vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<UseBranchBindData>();
	result->catalog_name = input.inputs[0].GetValue<string>();
	result->branch_name = input.inputs[1].GetValue<string>();

	names.emplace_back("success");
	return_types.emplace_back(LogicalType::BOOLEAN);

	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> DuckLakeUseBranchInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<UseBranchState>();
}

static void DuckLakeUseBranchExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<UseBranchState>();
	if (state.finished) {
		output.SetCardinality(0);
		return;
	}

	auto &bind_data = data_p.bind_data->CastNoConst<UseBranchBindData>();
	auto &catalog = Catalog::GetCatalog(context, bind_data.catalog_name);
	auto &transaction = DuckLakeTransaction::Get(context, catalog);

	auto &metadata_manager = transaction.GetMetadataManager();
	metadata_manager.SetCurrentBranch(bind_data.branch_name);

	// Commit the metadata changes so subsequent transactions see the new branch
	transaction.CommitMetadataChanges();

	// Invalidate the cached snapshot so the next query uses the new branch's snapshot
	transaction.InvalidateCachedSnapshot();

	output.SetCardinality(1);
	output.SetValue(0, 0, Value::BOOLEAN(true));
	state.finished = true;
}

TableFunctionSet DuckLakeUseBranchFunction::GetFunctions() {
	TableFunctionSet set("ducklake_use_branch");

	TableFunction func({LogicalType::VARCHAR, LogicalType::VARCHAR}, DuckLakeUseBranchExecute,
	                   DuckLakeUseBranchBind, DuckLakeUseBranchInit);
	set.AddFunction(func);

	return set;
}

//===--------------------------------------------------------------------===//
// ducklake_merge_branch() - Merge one branch into another
//===--------------------------------------------------------------------===//

struct MergeBranchBindData : public TableFunctionData {
	string catalog_name;
	string source_branch;
	string target_branch;
	bool dry_run = false;
};

struct MergeBranchState : public GlobalTableFunctionState {
	bool finished = false;
};

static unique_ptr<FunctionData> DuckLakeMergeBranchBind(ClientContext &context, TableFunctionBindInput &input,
                                                         vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<MergeBranchBindData>();
	result->catalog_name = input.inputs[0].GetValue<string>();
	result->source_branch = input.inputs[1].GetValue<string>();

	for (auto &kv : input.named_parameters) {
		if (kv.first == "target") {
			result->target_branch = kv.second.GetValue<string>();
		} else if (kv.first == "dry_run") {
			result->dry_run = kv.second.GetValue<bool>();
		}
	}

	// Default target branch is 'main'
	if (result->target_branch.empty()) {
		result->target_branch = "main";
	}

	names.emplace_back("merged");
	return_types.emplace_back(LogicalType::BOOLEAN);

	names.emplace_back("source_snapshot");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("target_snapshot");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("conflicts");
	return_types.emplace_back(LogicalType::VARCHAR);

	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> DuckLakeMergeBranchInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<MergeBranchState>();
}

static void DuckLakeMergeBranchExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<MergeBranchState>();
	if (state.finished) {
		output.SetCardinality(0);
		return;
	}

	auto &bind_data = data_p.bind_data->CastNoConst<MergeBranchBindData>();
	auto &catalog = Catalog::GetCatalog(context, bind_data.catalog_name);
	auto &transaction = DuckLakeTransaction::Get(context, catalog);

	auto &metadata_manager = transaction.GetMetadataManager();

	// Get source and target branches
	// Note: We must copy the data because GetBranch uses a single cache that gets overwritten
	auto source_ptr = metadata_manager.GetBranch(bind_data.source_branch);
	if (!source_ptr) {
		throw InvalidInputException("Source branch '%s' does not exist", bind_data.source_branch);
	}
	DuckLakeBranchInfo source = *source_ptr;

	auto target_ptr = metadata_manager.GetBranch(bind_data.target_branch);
	if (!target_ptr) {
		throw InvalidInputException("Target branch '%s' does not exist", bind_data.target_branch);
	}
	DuckLakeBranchInfo target = *target_ptr;

	// For now, implement fast-forward merge only
	// A fast-forward merge is possible when the target branch's snapshot is an ancestor of the source branch's snapshot
	// This means source_snapshot >= target_snapshot

	output.SetCardinality(1);
	state.finished = true;

	if (source.snapshot_id < target.snapshot_id) {
		// Source is behind target - nothing to merge (or needs rebase)
		output.SetValue(0, 0, Value::BOOLEAN(false));
		output.SetValue(1, 0, Value::BIGINT(NumericCast<int64_t>(source.snapshot_id)));
		output.SetValue(2, 0, Value::BIGINT(NumericCast<int64_t>(target.snapshot_id)));
		output.SetValue(3, 0, Value("Source branch is behind target branch. Consider rebasing."));
		return;
	}

	if (source.snapshot_id == target.snapshot_id) {
		// Already at the same snapshot - no merge needed
		output.SetValue(0, 0, Value::BOOLEAN(true));
		output.SetValue(1, 0, Value::BIGINT(NumericCast<int64_t>(source.snapshot_id)));
		output.SetValue(2, 0, Value::BIGINT(NumericCast<int64_t>(target.snapshot_id)));
		output.SetValue(3, 0, Value());
		return;
	}

	// Perform fast-forward merge (update target branch to source's snapshot)
	if (!bind_data.dry_run) {
		metadata_manager.UpdateBranch(bind_data.target_branch, source.snapshot_id);
	}

	output.SetValue(0, 0, Value::BOOLEAN(true));
	output.SetValue(1, 0, Value::BIGINT(NumericCast<int64_t>(source.snapshot_id)));
	output.SetValue(2, 0, Value::BIGINT(NumericCast<int64_t>(target.snapshot_id)));
	output.SetValue(3, 0, Value());
}

TableFunctionSet DuckLakeMergeBranchFunction::GetFunctions() {
	TableFunctionSet set("ducklake_merge_branch");

	TableFunction func({LogicalType::VARCHAR, LogicalType::VARCHAR}, DuckLakeMergeBranchExecute,
	                   DuckLakeMergeBranchBind, DuckLakeMergeBranchInit);
	func.named_parameters["target"] = LogicalType::VARCHAR;
	func.named_parameters["dry_run"] = LogicalType::BOOLEAN;
	set.AddFunction(func);

	return set;
}

} // namespace duckdb
