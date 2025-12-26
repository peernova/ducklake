# DuckLake Branching Tests

This directory contains tests for DuckLake's Git-like branching functionality.

## ⚠️ Important: Branches are READ-ONLY Snapshots

Currently, branches work as **read-only snapshots**:

- **All DML (INSERT/UPDATE/DELETE) goes to the `main` branch**
- Use `AT (BRANCH => 'name')` in **SELECT only** to read from branch snapshots
- Branches capture the state of data at the time they were created
- Changes on main after a branch is created are NOT visible to that branch

## Syntax

```sql
-- Create a branch (snapshot at current main state)
SELECT * FROM ducklake_create_branch('catalog', 'branch_name');

-- Create branch from another branch
SELECT * FROM ducklake_create_branch('catalog', 'child_branch', 'parent_branch');

-- Query a branch (READ-ONLY)
SELECT * FROM products AT (BRANCH => 'branch_name');

-- Query at specific version
SELECT * FROM products AT (VERSION => 3);

-- List branches
SELECT * FROM ducklake_branches('catalog');

-- Delete branch
SELECT * FROM ducklake_delete_branch('catalog', 'branch_name');

-- Get branch lineage
SELECT * FROM ducklake_branch_lineage('catalog', 'branch_name');
```

## Test Files

| File | Description |
|------|-------------|
| `test_branch_basic.test` | Basic branching functionality |
| `test_at_clause_branch.test` | AT (BRANCH => 'name') clause tests |
| `test_at_syntax.test` | Extended AT syntax testing |
| `test_branch_isolation.test` | Data isolation between branches |
| `test_nested_branches.test` | Nested branch hierarchies |
| `test_cross_branch_queries.test` | Cross-branch JOINs, UNION, etc. |
| `test_branch_schema_evolution.test` | Schema changes and branch visibility |
| `test_branch_time_travel.test` | Version queries with branches |
| `test_branch_edge_cases.test` | Edge cases and error handling |
| `test_branch_compaction.test` | Compaction behavior with branches |
| `test_interactive_branching.sql` | Interactive SQL test suite |
| `test_branching.sql` | Manual SQL test suite |

## Running Tests

### Build and Run

```bash
cd /path/to/ducklake
make GEN=ninja release

# Run interactive test
./build/release/duckdb -unsigned < test/sql/branching/test_interactive_branching.sql

# Run all tests
chmod +x test/sql/branching/run_all_branch_tests.sh
./test/sql/branching/run_all_branch_tests.sh
```

### Quick Manual Test

```bash
rm -rf test.ducklake test.ducklake.files
./build/release/duckdb -unsigned -c "
LOAD 'build/release/extension/ducklake/ducklake.duckdb_extension';
ATTACH 'ducklake:test.ducklake' AS dl;
USE dl;

-- Create table and initial data
CREATE TABLE products (id INT, name VARCHAR);
INSERT INTO products VALUES (1, 'Widget');

-- Create snapshot branch
SELECT * FROM ducklake_create_branch('dl', 'v1');

-- Add more to main
INSERT INTO products VALUES (2, 'Gadget');

-- Compare: main has 2, v1 has 1
SELECT 'main', COUNT(*) FROM products AT (BRANCH => 'main')
UNION ALL 
SELECT 'v1', COUNT(*) FROM products AT (BRANCH => 'v1');
"
```

## Example: Creating Release Snapshots

```sql
-- Initial data
CREATE TABLE config (key VARCHAR, value VARCHAR);
INSERT INTO config VALUES ('version', '1.0.0');

-- Create v1.0.0 release snapshot
SELECT * FROM ducklake_create_branch('dl', 'release_1_0_0');

-- Continue development on main
UPDATE config SET value = '1.1.0' WHERE key = 'version';
INSERT INTO config VALUES ('new_feature', 'enabled');

-- Create v1.1.0 release snapshot
SELECT * FROM ducklake_create_branch('dl', 'release_1_1_0');

-- Query different releases
SELECT * FROM config AT (BRANCH => 'release_1_0_0');  -- version=1.0.0
SELECT * FROM config AT (BRANCH => 'release_1_1_0');  -- version=1.1.0
SELECT * FROM config AT (BRANCH => 'main');           -- current dev
```

## Branch Hierarchy

Branches can be created from other branches:

```
main
├── release_1_0 (forked from main)
├── release_1_1 (forked from main, later)
└── develop (forked from main)
    └── feature_x (forked from develop)
```

## Limitations

1. **Read-only branches**: Cannot INSERT/UPDATE/DELETE on branches
2. **No merge**: Cannot merge branches back to main (yet)
3. **Cannot delete non-leaf branches**: Must delete child branches first
