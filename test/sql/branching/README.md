# DuckLake Branching Tests

This directory contains tests for DuckLake's Git-like branching feature.

## @ Syntax for Branches

DuckLake uses `@` syntax to specify branches, similar to Git's branch notation:

```sql
-- Select from a specific branch
SELECT * FROM table@branch;

-- Select from main branch
SELECT * FROM users@main;

-- Select from dev branch  
SELECT * FROM users@dev;

-- Insert on a specific branch
INSERT INTO users@dev VALUES (1, 'Alice');

-- Update on a branch
UPDATE products@staging SET price = 19.99 WHERE id = 1;

-- Delete on a branch
DELETE FROM users@dev WHERE id = 5;

-- Query at a specific version (main branch implied)
SELECT * FROM users@:5;

-- Query a branch at a specific version
SELECT * FROM users@dev:10;

-- Join across branches
SELECT o.*, u.name 
FROM orders@dev o 
JOIN users@dev u ON o.user_id = u.id;
```

## Test Files

| File | Description |
|------|-------------|
| `test_branch_basic.test` | Basic branching operations with @ syntax |
| `test_at_syntax.test` | Comprehensive @ syntax tests |
| `test_branch_compaction.test` | Branch-aware compaction tests |
| `test_branching.sql` | Manual SQL tests (runnable with duckdb) |
| `run_branch_tests.sh` | Shell script to run all tests |

## Running Tests

### Using the test runner script:
```bash
./run_branch_tests.sh
```

### Using DuckDB directly:
```bash
# From the ducklake build directory
./build/release/duckdb < test/sql/branching/test_branching.sql
```

### Using DuckDB test framework:
```bash
# From the ducklake root directory
./build/release/test/unittest test/sql/branching/test_branch_basic.test
```

## Branch Management Functions

```sql
-- List all branches
SELECT * FROM ducklake_branches('catalog_name');

-- Create a branch from main
SELECT * FROM ducklake_create_branch('catalog_name', 'dev');

-- Create a branch from another branch
SELECT * FROM ducklake_create_branch('catalog_name', 'feature', 'dev');

-- Create a branch at a specific snapshot
SELECT * FROM ducklake_create_branch('catalog_name', 'hotfix', 'main', 5);

-- View branch lineage
SELECT * FROM ducklake_branch_lineage('catalog_name', 'dev');

-- Delete a branch
SELECT * FROM ducklake_delete_branch('catalog_name', 'feature');
```

## How Branching Works

1. **Branch Creation**: When you create a branch, it records:
   - Parent branch ID
   - Fork snapshot ID (the snapshot on parent where we forked)
   - Branch lineage (which ancestor snapshots are visible)

2. **Data Isolation**: 
   - Each branch has its own head snapshot
   - Inserts/updates/deletes on a branch only affect that branch
   - Parent branches don't see child branch changes
   - Child branches don't see changes made to parent after fork

3. **File Visibility**:
   - Files from ancestor branches are visible up to the fork point
   - New files written on a branch are only visible to that branch
   - Branch-local file deletions don't affect other branches

4. **@ Syntax Resolution**:
   - `table@branch` → looks up table, resolves branch, uses branch's head snapshot
   - `table@branch:version` → uses specific version on branch
   - `table@:version` → uses specific version on current/main branch

## Schema

The branching feature uses these metadata tables:

- `ducklake_branch` - Branch definitions
- `ducklake_branch_lineage` - Pre-computed ancestry for fast lookups
- `ducklake_branch_file_deletion` - Files deleted on specific branches
- `ducklake_branch_delete_file_deletion` - Delete files removed by compaction
