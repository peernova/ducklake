# DuckLake Debugging Skill

## Approach: Add Logging During Operations, Not After

When debugging DuckLake branching issues, **always add debug logging to trace the operation as it happens**, rather than trying to analyze the result after the fact.

## Key Debug Points for Branching Operations

### 1. UPDATE Operations (`ducklake_update.cpp`)
UPDATE in DuckLake is implemented as DELETE + INSERT. Key logging points:
- `DuckLakeUpdate::Sink()` - Log incoming data, chunk sizes, row values
- `DuckLakeUpdate::Finalize()` - Log rows being inserted after delete
- `DuckLakeUpdate::GetDataInternal()` - Log final update count

### 2. Branch Switching (`ducklake_branch_functions.cpp`)
- `UseBranchFunction()` - Log branch_id, head_snapshot_id when switching
- After `SetWorkingBranch()` - Verify the branch was set correctly

### 3. Snapshot Management (`ducklake_transaction.cpp`)
- `GetSnapshot()` - Log which snapshot is being used for queries
- `FlushChanges()` - Log before/after commit, verify snapshot invalidation
- After commit: **MUST invalidate cached snapshot** so next query sees new data

### 4. File Visibility (`ducklake_metadata_manager.cpp`)
- `GetFilesForTable()` - Log which files are returned for the current branch
- Branch lineage query - Log which ancestor branches are visible

### 5. Schema/Table Lookup (`ducklake_catalog.cpp`)
- `GetSchemaForSnapshot()` - Log cache hits/misses, branch_id, schema_version
- `LookupEntry()` - Log whether entry was found

## Debug Logging Pattern

```cpp
fprintf(stderr, "[DEBUG FunctionName] message: var1=%llu, var2=%s\n",
        static_cast<unsigned long long>(numeric_var),
        string_var.c_str());
fflush(stderr);
```

Always use `fflush(stderr)` to ensure logs appear immediately.

## Common Issues

1. **Stale Snapshot Cache**: After `FlushChanges()`, the transaction's cached snapshot must be invalidated
2. **Schema Cache by Branch**: Schema cache key must include branch_id, not just schema_version
3. **Table ID Collisions**: Different branches can have different tables with same table_id - visibility queries must filter by branch lineage
4. **Row ID Corruption**: If row IDs appear wrong (e.g., +256 offset), check multi-file reader column mapping

## Test Strategy

1. Create a **minimal isolated test** to verify the operation works in isolation
2. Run the **comprehensive test** to see if it fails in context
3. If isolated passes but comprehensive fails: **state pollution from earlier tests**
4. Add logging to trace the operation step-by-step to find where it diverges
