-- ============================================================================
-- DuckLake Branching Schema Migration (Per-Branch Snapshot IDs)
-- Run this to add branching support to existing DuckLake metadata database
-- ============================================================================

-- ============================================================================
-- NEW TABLES
-- ============================================================================

-- Branch metadata
CREATE TABLE IF NOT EXISTS ducklake_branch (
    branch_id BIGINT PRIMARY KEY,
    branch_name VARCHAR NOT NULL,
    parent_branch_id BIGINT REFERENCES ducklake_branch(branch_id),
    fork_snapshot_id BIGINT,           -- parent branch's snapshot when forked
    head_snapshot_id BIGINT NOT NULL DEFAULT 0,  -- this branch's latest snapshot (per-branch: 0, 1, 2...)
    next_file_id BIGINT DEFAULT 0,     -- per-branch file ID counter
    created_at TIMESTAMPTZ DEFAULT NOW(),
    status VARCHAR DEFAULT 'active'
);

-- Partial unique index: only active branches must have unique names
CREATE UNIQUE INDEX IF NOT EXISTS idx_branch_name_active 
ON ducklake_branch(branch_name) 
WHERE status = 'active';

-- Pre-computed lineage for visibility
CREATE TABLE IF NOT EXISTS ducklake_branch_lineage (
    branch_id BIGINT NOT NULL,
    ancestor_branch_id BIGINT NOT NULL,
    max_visible_snapshot BIGINT NOT NULL,  -- max snapshot visible from ancestor (per-branch ID)
    PRIMARY KEY (branch_id, ancestor_branch_id)
);

-- Branch-scoped data file deletions (for files inherited from ancestors)
CREATE TABLE IF NOT EXISTS ducklake_branch_file_deletion (
    branch_id BIGINT NOT NULL,
    ancestor_branch_id BIGINT NOT NULL,
    data_file_id BIGINT NOT NULL,
    deleted_at_snapshot BIGINT NOT NULL,   -- per-branch snapshot ID
    PRIMARY KEY (branch_id, ancestor_branch_id, data_file_id)
);

-- Branch-scoped delete file deletions
CREATE TABLE IF NOT EXISTS ducklake_branch_delete_file_deletion (
    branch_id BIGINT NOT NULL,
    ancestor_branch_id BIGINT NOT NULL,
    delete_file_id BIGINT NOT NULL,
    deleted_at_snapshot BIGINT NOT NULL,   -- per-branch snapshot ID
    PRIMARY KEY (branch_id, ancestor_branch_id, delete_file_id)
);

-- ============================================================================
-- MODIFY EXISTING TABLES (add branch_id, change snapshot semantics)
-- ============================================================================

-- Snapshot table: now keyed by (branch_id, snapshot_id)
-- snapshot_id is per-branch: 0, 1, 2, 3...
ALTER TABLE ducklake_snapshot ADD COLUMN IF NOT EXISTS branch_id BIGINT NOT NULL DEFAULT 0;

-- Snapshot changes: also per-branch
ALTER TABLE ducklake_snapshot_changes ADD COLUMN IF NOT EXISTS branch_id BIGINT NOT NULL DEFAULT 0;

-- Data files: belong to a branch, snapshots are branch-local
ALTER TABLE ducklake_data_file ADD COLUMN IF NOT EXISTS branch_id BIGINT NOT NULL DEFAULT 0;
-- begin_snapshot and end_snapshot are now per-branch snapshot IDs

-- Delete files: belong to a branch
ALTER TABLE ducklake_delete_file ADD COLUMN IF NOT EXISTS branch_id BIGINT NOT NULL DEFAULT 0;
ALTER TABLE ducklake_delete_file ADD COLUMN IF NOT EXISTS data_file_branch_id BIGINT DEFAULT 0;
-- Note: data_file_branch_id tracks which branch owns the data file being deleted

-- Schema: branch-scoped
ALTER TABLE ducklake_schema ADD COLUMN IF NOT EXISTS branch_id BIGINT NOT NULL DEFAULT 0;

-- Table: branch-scoped
ALTER TABLE ducklake_table ADD COLUMN IF NOT EXISTS branch_id BIGINT NOT NULL DEFAULT 0;

-- Column: branch-scoped
ALTER TABLE ducklake_column ADD COLUMN IF NOT EXISTS branch_id BIGINT NOT NULL DEFAULT 0;

-- View: branch-scoped
ALTER TABLE ducklake_view ADD COLUMN IF NOT EXISTS branch_id BIGINT NOT NULL DEFAULT 0;

-- File column stats: keyed by branch
ALTER TABLE ducklake_file_column_stats ADD COLUMN IF NOT EXISTS branch_id BIGINT NOT NULL DEFAULT 0;

-- File partition values: keyed by branch
ALTER TABLE ducklake_file_partition_value ADD COLUMN IF NOT EXISTS branch_id BIGINT NOT NULL DEFAULT 0;

-- Partition info: branch-scoped
ALTER TABLE ducklake_partition_info ADD COLUMN IF NOT EXISTS branch_id BIGINT NOT NULL DEFAULT 0;

-- Partition column: branch-scoped
ALTER TABLE ducklake_partition_column ADD COLUMN IF NOT EXISTS branch_id BIGINT NOT NULL DEFAULT 0;

-- Table stats: per-branch
ALTER TABLE ducklake_table_stats ADD COLUMN IF NOT EXISTS branch_id BIGINT NOT NULL DEFAULT 0;

-- Table column stats: per-branch
ALTER TABLE ducklake_table_column_stats ADD COLUMN IF NOT EXISTS branch_id BIGINT NOT NULL DEFAULT 0;

-- Tags: branch-scoped
ALTER TABLE ducklake_tag ADD COLUMN IF NOT EXISTS branch_id BIGINT NOT NULL DEFAULT 0;

-- Column tags: branch-scoped
ALTER TABLE ducklake_column_tag ADD COLUMN IF NOT EXISTS branch_id BIGINT NOT NULL DEFAULT 0;

-- Column mapping: branch-scoped
ALTER TABLE ducklake_column_mapping ADD COLUMN IF NOT EXISTS branch_id BIGINT NOT NULL DEFAULT 0;

-- Name mapping: branch-scoped
ALTER TABLE ducklake_name_mapping ADD COLUMN IF NOT EXISTS branch_id BIGINT NOT NULL DEFAULT 0;

-- Inlined data tables: branch-scoped
ALTER TABLE ducklake_inlined_data_tables ADD COLUMN IF NOT EXISTS branch_id BIGINT NOT NULL DEFAULT 0;

-- Files scheduled for deletion: branch-scoped
ALTER TABLE ducklake_files_scheduled_for_deletion ADD COLUMN IF NOT EXISTS branch_id BIGINT NOT NULL DEFAULT 0;

-- Schema versions: branch-scoped
ALTER TABLE ducklake_schema_versions ADD COLUMN IF NOT EXISTS branch_id BIGINT NOT NULL DEFAULT 0;

-- Macro: branch-scoped
ALTER TABLE ducklake_macro ADD COLUMN IF NOT EXISTS branch_id BIGINT NOT NULL DEFAULT 0;

-- Macro impl: branch-scoped
ALTER TABLE ducklake_macro_impl ADD COLUMN IF NOT EXISTS branch_id BIGINT NOT NULL DEFAULT 0;

-- Macro parameters: branch-scoped
ALTER TABLE ducklake_macro_parameters ADD COLUMN IF NOT EXISTS branch_id BIGINT NOT NULL DEFAULT 0;

-- ============================================================================
-- INDEXES
-- ============================================================================

-- Branch lookups
CREATE INDEX IF NOT EXISTS idx_branch_parent ON ducklake_branch(parent_branch_id);
CREATE INDEX IF NOT EXISTS idx_branch_status ON ducklake_branch(status);

-- Lineage lookups
CREATE INDEX IF NOT EXISTS idx_lineage_branch ON ducklake_branch_lineage(branch_id);
CREATE INDEX IF NOT EXISTS idx_lineage_ancestor ON ducklake_branch_lineage(ancestor_branch_id);

-- Snapshot lookups with branch (compound)
CREATE INDEX IF NOT EXISTS idx_snapshot_branch ON ducklake_snapshot(branch_id, snapshot_id);

-- Data file lookups with branch
CREATE INDEX IF NOT EXISTS idx_datafile_branch_table ON ducklake_data_file(branch_id, table_id, begin_snapshot);
CREATE INDEX IF NOT EXISTS idx_datafile_branch_id ON ducklake_data_file(branch_id, data_file_id);

-- Delete file lookups with branch
CREATE INDEX IF NOT EXISTS idx_deletefile_branch ON ducklake_delete_file(branch_id, table_id);
CREATE INDEX IF NOT EXISTS idx_deletefile_datafile ON ducklake_delete_file(data_file_branch_id, data_file_id);

-- Branch file deletion lookups
CREATE INDEX IF NOT EXISTS idx_branch_deletion ON ducklake_branch_file_deletion(branch_id, deleted_at_snapshot);
CREATE INDEX IF NOT EXISTS idx_branch_deletion_file ON ducklake_branch_file_deletion(ancestor_branch_id, data_file_id);

-- Schema/Table/Column lookups with branch
CREATE INDEX IF NOT EXISTS idx_schema_branch ON ducklake_schema(branch_id, begin_snapshot);
CREATE INDEX IF NOT EXISTS idx_table_branch ON ducklake_table(branch_id, table_id, begin_snapshot);
CREATE INDEX IF NOT EXISTS idx_column_branch ON ducklake_column(branch_id, table_id, begin_snapshot);

-- ============================================================================
-- INITIALIZE MAIN BRANCH (if not exists)
-- ============================================================================

-- Get current max snapshot and file IDs for main branch initialization
INSERT INTO ducklake_branch (branch_id, branch_name, parent_branch_id, fork_snapshot_id, head_snapshot_id, next_file_id, status)
SELECT 0, 'main', NULL, NULL, 
       COALESCE(MAX(snapshot_id), 0),
       COALESCE((SELECT MAX(data_file_id) + 1 FROM ducklake_data_file WHERE branch_id = 0), 0),
       'active'
FROM ducklake_snapshot
WHERE branch_id = 0
  AND NOT EXISTS (SELECT 1 FROM ducklake_branch WHERE branch_id = 0);

-- Main branch lineage (sees only itself, all snapshots)
INSERT INTO ducklake_branch_lineage (branch_id, ancestor_branch_id, max_visible_snapshot)
SELECT 0, 0, 9223372036854775807
WHERE NOT EXISTS (SELECT 1 FROM ducklake_branch_lineage WHERE branch_id = 0);

-- ============================================================================
-- UPDATE METADATA VERSION
-- ============================================================================

UPDATE ducklake_metadata SET value = '0.5' WHERE key = 'version';
INSERT INTO ducklake_metadata (key, value) 
SELECT 'branching_enabled', 'true'
WHERE NOT EXISTS (SELECT 1 FROM ducklake_metadata WHERE key = 'branching_enabled');

INSERT INTO ducklake_metadata (key, value)
SELECT 'branching_mode', 'per_branch_snapshots'
WHERE NOT EXISTS (SELECT 1 FROM ducklake_metadata WHERE key = 'branching_mode');

-- ============================================================================
-- VERIFICATION
-- ============================================================================

SELECT 'Migration complete. Verifying...';

SELECT 'Branches:' AS info, COUNT(*) AS count FROM ducklake_branch;
SELECT 'Lineage entries:' AS info, COUNT(*) AS count FROM ducklake_branch_lineage;
SELECT 'Main branch head:' AS info, head_snapshot_id FROM ducklake_branch WHERE branch_id = 0;
