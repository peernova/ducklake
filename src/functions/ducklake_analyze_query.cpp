#include "functions/ducklake_table_functions.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/connection.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Analyze Query Bind Data
//===--------------------------------------------------------------------===//
struct AnalyzeQueryBindData : public TableFunctionData {
	vector<vector<Value>> rows;
};

struct AnalyzeQueryGlobalState : public GlobalTableFunctionState {
	AnalyzeQueryGlobalState() : offset(0) {
	}
	idx_t offset;
};

//===--------------------------------------------------------------------===//
// Helper to extract table info from logical operators
//===--------------------------------------------------------------------===//
struct TableReferenceInfo {
	string catalog_name;
	string schema_name;
	string table_name;
	string reference_type;
	vector<string> columns;
};

static void ExtractTableReferences(LogicalOperator &op, vector<TableReferenceInfo> &tables,
                                   const string &default_type = "SELECT") {
	// Check if this is a LogicalGet (table scan)
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op.Cast<LogicalGet>();
		auto table_entry = get.GetTable();
		if (table_entry) {
			TableReferenceInfo info;
			info.catalog_name = table_entry->catalog.GetName();
			info.schema_name = table_entry->schema.name;
			info.table_name = table_entry->name;
			info.reference_type = default_type;
			// Get projected column names
			auto &col_ids = get.GetColumnIds();
			for (auto &col_id : col_ids) {
				if (col_id.GetPrimaryIndex() < get.names.size()) {
					info.columns.push_back(get.names[col_id.GetPrimaryIndex()]);
				}
			}
			tables.push_back(std::move(info));
		} else {
			// Table function or view - get from function name
			TableReferenceInfo info;
			info.catalog_name = "";
			info.schema_name = "";
			info.table_name = get.function.name;
			info.reference_type = "FUNCTION";
			for (auto &name : get.names) {
				info.columns.push_back(name);
			}
			tables.push_back(std::move(info));
		}
	}
	// Check for INSERT
	else if (op.type == LogicalOperatorType::LOGICAL_INSERT) {
		auto &insert = op.Cast<LogicalInsert>();
		TableReferenceInfo info;
		info.catalog_name = insert.table.catalog.GetName();
		info.schema_name = insert.table.schema.name;
		info.table_name = insert.table.name;
		info.reference_type = "INSERT";
		tables.push_back(std::move(info));
	}
	// Check for UPDATE
	else if (op.type == LogicalOperatorType::LOGICAL_UPDATE) {
		auto &update = op.Cast<LogicalUpdate>();
		TableReferenceInfo info;
		info.catalog_name = update.table.catalog.GetName();
		info.schema_name = update.table.schema.name;
		info.table_name = update.table.name;
		info.reference_type = "UPDATE";
		tables.push_back(std::move(info));
	}
	// Check for DELETE
	else if (op.type == LogicalOperatorType::LOGICAL_DELETE) {
		auto &del = op.Cast<LogicalDelete>();
		TableReferenceInfo info;
		info.catalog_name = del.table.catalog.GetName();
		info.schema_name = del.table.schema.name;
		info.table_name = del.table.name;
		info.reference_type = "DELETE";
		tables.push_back(std::move(info));
	}

	// Recursively process children
	for (auto &child : op.children) {
		ExtractTableReferences(*child, tables, default_type);
	}
}

//===--------------------------------------------------------------------===//
// Bind Function
//===--------------------------------------------------------------------===//
static unique_ptr<FunctionData> AnalyzeQueryBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<AnalyzeQueryBindData>();

	// Get the SQL query from input
	if (input.inputs.empty() || input.inputs[0].IsNull()) {
		throw BinderException("SQL query cannot be NULL");
	}
	auto sql = input.inputs[0].GetValue<string>();

	try {
		// Parse the SQL
		Parser parser;
		parser.ParseQuery(sql);

		// Process each statement
		for (auto &statement : parser.statements) {
			auto stmt = std::move(statement);

			// Create plan using planner
			Planner planner(context);
			planner.CreatePlan(std::move(stmt));

			if (!planner.plan) {
				continue;
			}

			// Extract table references from the plan
			vector<TableReferenceInfo> tables;
			ExtractTableReferences(*planner.plan, tables);

			// Convert to result rows
			for (auto &table_info : tables) {
				vector<Value> row;
				row.push_back(table_info.catalog_name.empty() ? Value() : Value(table_info.catalog_name));
				row.push_back(table_info.schema_name.empty() ? Value() : Value(table_info.schema_name));
				row.push_back(Value(table_info.table_name));
				row.push_back(Value(table_info.reference_type));

				// Convert columns to a list
				vector<Value> col_values;
				for (auto &col : table_info.columns) {
					col_values.push_back(Value(col));
				}
				row.push_back(Value::LIST(LogicalType::VARCHAR, std::move(col_values)));

				result->rows.push_back(std::move(row));
			}
		}
	} catch (std::exception &ex) {
		// On error, return an error row
		vector<Value> row;
		row.push_back(Value()); // catalog
		row.push_back(Value()); // schema
		row.push_back(Value("ERROR")); // table
		row.push_back(Value("ERROR")); // type
		row.push_back(Value::LIST(LogicalType::VARCHAR, {Value(ex.what())})); // columns contains error message
		result->rows.push_back(std::move(row));
	}

	// Define return columns
	names.emplace_back("catalog_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("schema_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("table_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("reference_type");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("columns");
	return_types.emplace_back(LogicalType::LIST(LogicalType::VARCHAR));

	return std::move(result);
}

//===--------------------------------------------------------------------===//
// Init Function
//===--------------------------------------------------------------------===//
static unique_ptr<GlobalTableFunctionState> AnalyzeQueryInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<AnalyzeQueryGlobalState>();
}

//===--------------------------------------------------------------------===//
// Execute Function
//===--------------------------------------------------------------------===//
static void AnalyzeQueryExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->Cast<AnalyzeQueryBindData>();
	auto &state = data_p.global_state->Cast<AnalyzeQueryGlobalState>();

	if (state.offset >= data.rows.size()) {
		return;
	}

	idx_t count = 0;
	while (state.offset < data.rows.size() && count < STANDARD_VECTOR_SIZE) {
		auto &row = data.rows[state.offset++];
		for (idx_t c = 0; c < row.size(); c++) {
			output.SetValue(c, count, row[c]);
		}
		count++;
	}
	output.SetCardinality(count);
}

//===--------------------------------------------------------------------===//
// Register Function
//===--------------------------------------------------------------------===//
TableFunction DuckLakeAnalyzeQueryFunction::GetFunction() {
	TableFunction func("ducklake_analyze_query", {LogicalType::VARCHAR}, AnalyzeQueryExecute, AnalyzeQueryBind,
	                   AnalyzeQueryInit);
	func.named_parameters["skip_errors"] = LogicalType::BOOLEAN;
	return func;
}

} // namespace duckdb
