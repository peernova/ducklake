# DuckLake Branching

DuckLake supports git-like branching for data, allowing you to create named pointers to specific snapshots and work on different versions of your data independently.

## Overview

Branches in DuckLake are lightweight pointers to snapshots. Each branch stores:
- `branch_name`: The unique name of the branch
- `snapshot_id`: The snapshot this branch points to
- `created_at`: When the branch was created
- `created_by`: Who created the branch (optional)
- `description`: A description of the branch's purpose (optional)

When you make changes (INSERT, UPDATE, DELETE, DDL) while on a branch, a new snapshot is created and the branch pointer is automatically updated to point to this new snapshot.

## SQL Functions

### List Branches

```sql
SELECT * FROM ducklake_branches('catalog_name');
```

Returns all branches with their metadata:
| Column | Type | Description |
|--------|------|-------------|
| branch_name | VARCHAR | Name of the branch |
| snapshot_id | BIGINT | Snapshot ID the branch points to |
| created_at | TIMESTAMP WITH TIME ZONE | Creation timestamp |
| created_by | VARCHAR | Creator (optional) |
| description | VARCHAR | Branch description (optional) |

### Get Current Branch

```sql
SELECT * FROM ducklake_current_branch('catalog_name');
```

Returns the name of the currently active branch.

### Create Branch

```sql
SELECT * FROM ducklake_create_branch('catalog_name', 'branch_name',
    snapshot_id := 123,           -- Optional: defaults to current snapshot
    created_by := 'user',         -- Optional
    description := 'description'  -- Optional
);
```

Creates a new branch pointing to the specified snapshot (or current snapshot if not specified).

### Switch Branch

```sql
SELECT * FROM ducklake_use_branch('catalog_name', 'branch_name');
```

Switches to the specified branch. Subsequent queries will see data as of that branch's snapshot.

### Drop Branch

```sql
SELECT * FROM ducklake_drop_branch('catalog_name', 'branch_name',
    if_exists := true  -- Optional: don't error if branch doesn't exist
);
```

Deletes a branch. Cannot drop the `main` branch.

### Merge Branch

```sql
SELECT * FROM ducklake_merge_branch('catalog_name', 'source_branch',
    target := 'target_branch',  -- Optional: defaults to 'main'
    dry_run := true             -- Optional: check if merge is possible without executing
);
```

Performs a fast-forward merge, updating the target branch's pointer to match the source branch's snapshot.

Returns:
| Column | Type | Description |
|--------|------|-------------|
| merged | BOOLEAN | Whether merge was successful |
| source_snapshot | BIGINT | Source branch's snapshot ID |
| target_snapshot | BIGINT | Target branch's snapshot ID (before merge) |
| conflicts | VARCHAR | Conflict description if merge failed |

## Usage Examples

### Basic Workflow

```sql
-- Attach a DuckLake catalog
ATTACH 'ducklake:metadata.db' AS lake (DATA_PATH 'data/');

-- Create some initial data
CREATE TABLE lake.products(id INT, name VARCHAR);
INSERT INTO lake.products VALUES (1, 'Widget');

-- Create a feature branch for experimentation
SELECT * FROM ducklake_create_branch('lake', 'experiment',
    description := 'Testing new product catalog');

-- Switch to the feature branch
SELECT * FROM ducklake_use_branch('lake', 'experiment');

-- Make changes on the feature branch
INSERT INTO lake.products VALUES (2, 'Gadget');
INSERT INTO lake.products VALUES (3, 'Gizmo');

-- These changes are only visible on the experiment branch
SELECT * FROM lake.products;
-- Returns: Widget, Gadget, Gizmo

-- Switch back to main
SELECT * FROM ducklake_use_branch('lake', 'main');

-- Main still has the original data
SELECT * FROM lake.products;
-- Returns: Widget

-- Merge the experiment into main
SELECT * FROM ducklake_merge_branch('lake', 'experiment', target := 'main');

-- Now main sees all the data
SELECT * FROM lake.products;
-- Returns: Widget, Gadget, Gizmo

-- Clean up the feature branch
SELECT * FROM ducklake_drop_branch('lake', 'experiment');
```

### Time Travel with Branches

Branches can be used to bookmark specific points in time:

```sql
-- Create a branch to mark a release
SELECT * FROM ducklake_create_branch('lake', 'v1.0-release',
    description := 'Production release v1.0');

-- Continue development on main
INSERT INTO lake.products VALUES (4, 'New Product');

-- Query data as it was at the v1.0 release
SELECT * FROM ducklake_use_branch('lake', 'v1.0-release');
SELECT * FROM lake.products;  -- Shows data at release time

-- Return to current development
SELECT * FROM ducklake_use_branch('lake', 'main');
```

## Architecture

### How It Works

1. **Branch Storage**: Branches are stored in the `ducklake_branch` metadata table with their snapshot pointers.

2. **Current Branch**: The active branch is stored in `ducklake_metadata` with key `current_branch`. Defaults to `main`.

3. **Snapshot Selection**: When you query data, DuckLake looks up the current branch's snapshot_id and filters data files to only include those with `begin_snapshot <= branch_snapshot_id`.

4. **Branch Updates**: When you commit changes (INSERT, UPDATE, DELETE), a new snapshot is created and the current branch's pointer is updated to this new snapshot.

5. **Fast-Forward Merge**: Merging simply updates the target branch's snapshot pointer to match the source branch's snapshot. No data copying occurs.

### Data File Filtering

Data files in DuckLake have `begin_snapshot` and `end_snapshot` columns:
- `begin_snapshot`: The snapshot when this file was created
- `end_snapshot`: The snapshot when this file was superseded (NULL if current)

When querying on a branch at snapshot N, only files where `begin_snapshot <= N` and (`end_snapshot > N` or `end_snapshot IS NULL`) are visible.

## Characteristics

### Lightweight Branches
- Creating a branch is instantaneous (just stores a pointer)
- No data is copied when creating branches
- Branches have minimal storage overhead

### Consistent Views
- Each branch provides a consistent view of data at its snapshot
- Changes on one branch don't affect queries on other branches (until merged)

### Fast-Forward Merging
- Merging is instantaneous (just updates a pointer)
- Works when the source branch is "ahead" of the target branch
- No data reconciliation or conflict resolution

## Limitations

### Point-in-Time Isolation (Not Full Git-Style Isolation)

DuckLake branches provide **point-in-time isolation**, not full git-style parallel development isolation.

**What this means:**
- Each branch points to a specific snapshot
- Data created AFTER that snapshot is not visible when querying on that branch
- However, once a branch advances past another branch's snapshot, it will see that data

**Example of the limitation:**

```sql
-- Initial state: main at snapshot 2
INSERT INTO products VALUES (1, 'A');

-- Create feature branch at snapshot 2
SELECT * FROM ducklake_create_branch('lake', 'feature');

-- Switch to feature, insert data (feature now at snapshot 3)
SELECT * FROM ducklake_use_branch('lake', 'feature');
INSERT INTO products VALUES (2, 'B');

-- Switch to main (still at snapshot 2)
SELECT * FROM ducklake_use_branch('lake', 'main');
SELECT * FROM products;  -- Returns only 'A' (correct!)

-- Insert on main (main now at snapshot 4)
INSERT INTO products VALUES (3, 'C');

-- Now main at snapshot 4 sees ALL data with begin_snapshot <= 4
SELECT * FROM products;  -- Returns 'A', 'B', 'C' (includes feature's data!)
```

This happens because the snapshot namespace is global. Once main advances to snapshot 4, it sees data from snapshot 3 (which was created on feature).

**True git-style isolation** (where parallel branches never see each other's uncommitted work) would require tracking which branch created each snapshot and filtering based on branch lineage. This is not yet implemented.

### Merge Limitations

- Only fast-forward merges are supported
- Cannot merge if the source branch is "behind" the target branch
- No three-way merge or conflict resolution
- No rebasing support

### Schema Changes

- Schema changes (ALTER TABLE, etc.) create new snapshots
- All branches share the same schema evolution history
- Cannot have different schemas on different branches

### No Branch-Specific Garbage Collection

- Deleted/superseded data files are tracked globally
- Cannot garbage collect files that are only referenced by deleted branches
- Files remain until no branch references them

## Best Practices

1. **Use branches for experimentation**: Create a branch before making experimental changes, then merge or discard.

2. **Create release bookmarks**: Use branches to mark stable release points for easy rollback.

3. **Keep branches short-lived**: Merge or delete branches promptly to avoid confusion about which branch is "current".

4. **Avoid parallel development on same tables**: Due to the point-in-time isolation model, avoid making changes to the same tables on multiple branches simultaneously.

5. **Use descriptive names and descriptions**: Help team members understand the purpose of each branch.

## Future Enhancements

Potential future improvements:

- **True branch isolation**: Track branch lineage to provide git-style isolation
- **Three-way merge**: Support merging diverged branches with conflict detection
- **Rebasing**: Allow rebasing one branch onto another
- **Branch-aware garbage collection**: Clean up files only referenced by deleted branches
- **Branch permissions**: Control who can modify specific branches
