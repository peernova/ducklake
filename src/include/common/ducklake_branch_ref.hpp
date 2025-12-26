//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/ducklake_branch_ref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

//! Parsed reference from table@branch or table@branch:version syntax
struct DuckLakeBranchRef {
	//! The actual table name (without branch suffix)
	string table_name;
	//! Branch name (empty = current/main branch)
	string branch_name;
	//! Optional version within branch
	optional_idx version;

	DuckLakeBranchRef() = default;

	explicit DuckLakeBranchRef(string table_name_p) : table_name(std::move(table_name_p)) {
	}

	DuckLakeBranchRef(string table_name_p, string branch_name_p)
	    : table_name(std::move(table_name_p)), branch_name(std::move(branch_name_p)) {
	}

	DuckLakeBranchRef(string table_name_p, string branch_name_p, idx_t version_p)
	    : table_name(std::move(table_name_p)), branch_name(std::move(branch_name_p)), version(version_p) {
	}

	//! Check if this reference includes a branch
	bool HasBranch() const {
		return !branch_name.empty();
	}

	//! Check if this reference includes a version
	bool HasVersion() const {
		return version.IsValid();
	}

	//! Check if this is a plain table reference (no branch, no version)
	bool IsPlainReference() const {
		return !HasBranch() && !HasVersion();
	}

	//! Parse a table reference that may contain @branch or @branch:version
	//! Examples:
	//!   "users"           -> table=users, branch="", version=invalid
	//!   "users@dev"       -> table=users, branch=dev, version=invalid
	//!   "users@dev:5"     -> table=users, branch=dev, version=5
	//!   "users@:5"        -> table=users, branch="", version=5 (current branch, version 5)
	static DuckLakeBranchRef Parse(const string &table_ref) {
		DuckLakeBranchRef result;

		auto at_pos = table_ref.find('@');
		if (at_pos == string::npos) {
			// No @ symbol - plain table reference
			result.table_name = table_ref;
			return result;
		}

		// Split on @
		result.table_name = table_ref.substr(0, at_pos);
		string branch_part = table_ref.substr(at_pos + 1);

		// Check for :version suffix
		auto colon_pos = branch_part.find(':');
		if (colon_pos == string::npos) {
			// No version, just branch
			result.branch_name = branch_part;
		} else {
			// branch:version
			result.branch_name = branch_part.substr(0, colon_pos);
			string version_str = branch_part.substr(colon_pos + 1);
			if (!version_str.empty()) {
				try {
					result.version = std::stoull(version_str);
				} catch (...) {
					throw InvalidInputException("Invalid version number in table reference: %s", version_str);
				}
			}
		}

		return result;
	}

	//! Convert back to string representation
	string ToString() const {
		string result = table_name;
		if (HasBranch() || HasVersion()) {
			result += "@";
			result += branch_name;
			if (HasVersion()) {
				result += ":" + std::to_string(version.GetIndex());
			}
		}
		return result;
	}

	//! Get a display string for error messages
	string ToDisplayString() const {
		if (IsPlainReference()) {
			return table_name;
		}
		string result = table_name;
		if (HasBranch()) {
			result += " on branch '" + branch_name + "'";
		}
		if (HasVersion()) {
			result += " at version " + std::to_string(version.GetIndex());
		}
		return result;
	}
};

} // namespace duckdb
