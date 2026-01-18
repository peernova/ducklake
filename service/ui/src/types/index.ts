// ============================================================================
// Common Types
// ============================================================================

export interface Page {
  size: number;
  page_number: number;
  total_number_of_elements: number;
}

// ============================================================================
// Catalog Types
// ============================================================================

export type MetadataType = 'postgres' | 'duckdb' | 'sqlite';
export type StorageType = 'local' | 's3' | 'gcs' | 'azure';

export interface Catalog {
  catalog_id: string;
  display_name?: string;
  description?: string;
  metadata_uri: string;
  data_path: string;
  metadata_type: MetadataType;
  storage_type: StorageType;
  branch_count?: number;
  table_count?: number;
  total_size_bytes?: number;
  enabled: boolean;
  tags?: string[];
  options?: Record<string, string>;
  created_at: string;
  created_by?: string;
  updated_at?: string;
  last_accessed_at?: string;
}

export interface RegisterCatalogRequest {
  catalog_id: string;
  display_name?: string;
  description?: string;
  metadata_uri: string;
  data_path: string;
  options?: Record<string, string>;
  secret_name?: string;
  tags?: string[];
}

export interface UpdateCatalogRequest {
  display_name?: string;
  description?: string;
  metadata_uri?: string;
  data_path?: string;
  options?: Record<string, string>;
  secret_name?: string;
  tags?: string[];
  enabled?: boolean;
}

export interface CatalogTestResult {
  catalog_id: string;
  overall_status: 'healthy' | 'degraded' | 'unhealthy';
  metadata_backend: {
    status: 'connected' | 'error';
    latency_ms?: number;
    version?: string;
    error?: string;
  };
  data_storage: {
    status: 'accessible' | 'error';
    latency_ms?: number;
    path?: string;
    error?: string;
  };
  branch_count?: number;
  table_count?: number;
  tested_at: string;
}

export interface ListCatalogsResponse {
  catalogs: Catalog[];
  page: Page;
}

// ============================================================================
// Schema & Table Types
// ============================================================================

export interface SchemaInfo {
  schema_id: number;
  schema_name: string;
  path?: string;
  table_count?: number;
  view_count?: number;
  created_at?: string;
}

export interface ListSchemasResponse {
  schemas: SchemaInfo[];
  page: Page;
}

export interface ColumnInfo {
  name: string;
  type: string;
  nullable?: boolean;
}

export interface TableInfo {
  table_id: number;
  table_name: string;
  schema_name: string;
  path?: string;
  columns?: ColumnInfo[];
  row_count?: number;
  file_count?: number;
  total_size_bytes?: number;
  partition_columns?: string[];
  created_at?: string;
  last_modified_at?: string;
}

export interface ListTablesResponse {
  tables: TableInfo[];
  page: Page;
}

// ============================================================================
// Query Types
// ============================================================================

export interface QueryRequest {
  sql: string;
  branch_context?: Record<string, string>;
  snapshot_context?: Record<string, number>;
  parameters?: Record<string, unknown>;
  timeout_ms?: number;
}

export interface QueryResponse {
  columns: ColumnInfo[];
  rows: Array<Array<string | number | boolean | null>>;
  row_count: number;
  execution_time_ms: number;
  branch?: string;
  snapshot_id?: number;
}

// ============================================================================
// Branch Types
// ============================================================================

export type BranchStatus = 'active' | 'merged' | 'archived' | 'deleted';

export interface Branch {
  branch_id: number;
  branch_name: string;
  parent_branch_id?: number | null;
  parent_branch_name?: string | null;
  fork_snapshot_id?: number | null;
  head_snapshot_id?: number;
  created_at?: string;
  status: BranchStatus;
}

export interface CreateBranchRequest {
  branch_name: string;
  from_branch?: string;
  from_snapshot?: number;
}

export interface SearchBranchesRequest {
  pattern?: string;
  status?: BranchStatus;
  created_after?: string;
  created_before?: string;
  parent_branch?: string;
}

export interface ListBranchesResponse {
  branches: Branch[];
  page: Page;
}

export interface UseBranchResponse {
  catalog: string;
  branch_name: string;
  branch_id: number;
  head_snapshot_id: number;
}

export interface CurrentBranchResponse {
  catalog: string;
  branch_name: string;
  branch_id: number;
  head_snapshot_id: number;
}

// ============================================================================
// Branch Stats Types
// ============================================================================

export interface BranchCountResponse {
  count: number;
  status_filter: string;
}

export interface BranchStats {
  branch_name: string;
  table_count: number;
  schema_count: number;
  view_count: number;
  data_file_count: number;
  total_rows?: number;
  total_size_bytes?: number;
  snapshot_count: number;
  last_modified_at?: string;
  last_accessed_at?: string;  // TODO: Track via query logs
}

export interface CatalogStats {
  catalog_name: string;
  branch_count: number;
  active_branch_count: number;
  schema_count: number;
  table_count: number;
  view_count: number;
  data_file_count: number;
  delete_file_count: number;
  total_rows: number;
  total_size_bytes: number;
  snapshot_count: number;
}

export interface LineageEntry {
  branch_id: number;
  ancestor_branch_id: number;
  max_visible_snapshot: number;
}

export interface BranchLineageResponse {
  branch_name: string;
  lineage: LineageEntry[];
}

export interface BranchActivity {
  branch_id: number;
  branch_name: string;
  created_at: string;
  last_modified_at?: string | null;
  snapshot_count: number;
  head_snapshot_id: number;
  status: BranchStatus;
}

export interface BranchActivityResponse {
  branches: BranchActivity[];
}

// ============================================================================
// Error Types (gRPC-style)
// ============================================================================

export interface FieldViolation {
  field: string;
  description: string;
}

export interface BadRequestDetail {
  '@type': 'type.googleapis.com/google.rpc.BadRequest';
  field_violations: FieldViolation[];
}

export interface ResourceInfo {
  '@type': 'type.googleapis.com/google.rpc.ResourceInfo';
  resource_type: string;
  resource_name: string;
  owner: string;
  description: string;
}

export interface ErrorInfo {
  '@type': 'type.googleapis.com/google.rpc.ErrorInfo';
  reason: string;
  domain: string;
  metadata?: Record<string, string>;
}

export type ErrorDetail = BadRequestDetail | ResourceInfo | ErrorInfo;

export interface Status {
  code: number;
  message: string;
  details?: ErrorDetail[];
}

// ============================================================================
// Branch Diff/Compare Types (TODO: Add to Swagger/OpenAPI spec)
// ============================================================================
// These APIs are needed for the branch comparison feature in the UI

export type DiffStatus = 'added' | 'removed' | 'modified' | 'unchanged';

export interface ColumnDiff {
  column_name: string;
  status: DiffStatus;
  base_type?: string;      // Type in base branch (null if added)
  compare_type?: string;   // Type in compare branch (null if removed)
  base_nullable?: boolean;
  compare_nullable?: boolean;
}

export interface TableDiff {
  table_name: string;
  schema_name: string;
  status: DiffStatus;
  base_row_count?: number;
  compare_row_count?: number;
  columns: ColumnDiff[];
}

export interface SchemaDiff {
  schema_name: string;
  status: DiffStatus;
  tables: TableDiff[];
}

export interface BranchDiffRequest {
  base_branch: string;           // e.g., "main"
  compare_branch: string;        // e.g., "feature_new_model"
  base_snapshot_id?: number;     // Optional: compare at specific snapshots
  compare_snapshot_id?: number;
  include_unchanged?: boolean;   // Whether to include unchanged items
}

export interface BranchDiffResponse {
  base_branch: string;
  base_snapshot_id: number;
  compare_branch: string;
  compare_snapshot_id: number;
  schemas: SchemaDiff[];
  summary: {
    schemas_added: number;
    schemas_removed: number;
    schemas_modified: number;
    tables_added: number;
    tables_removed: number;
    tables_modified: number;
    columns_added: number;
    columns_removed: number;
    columns_modified: number;
  };
}

// ============================================================================
// SQL Parser/Formatter Types (TODO: Add to Swagger/OpenAPI spec)
// ============================================================================
// Backend SQL parsing provides: security validation, syntax checking, formatting

export interface SQLParseRequest {
  sql: string;
  dialect?: 'duckdb' | 'postgres' | 'standard';  // DuckDB-specific syntax
}

export interface SQLParseResponse {
  valid: boolean;
  errors?: Array<{
    message: string;
    line: number;
    column: number;
    code?: string;
  }>;
  warnings?: Array<{
    message: string;
    line: number;
    column: number;
  }>;
  statement_type?: 'SELECT' | 'INSERT' | 'UPDATE' | 'DELETE' | 'CREATE' | 'ALTER' | 'DROP' | 'OTHER';
  tables_referenced?: string[];    // Useful for permission checking
  columns_referenced?: string[];
}

export interface SQLFormatRequest {
  sql: string;
  options?: {
    indent_width?: number;         // Default: 2
    uppercase_keywords?: boolean;  // Default: true
    line_width?: number;           // Default: 80
    inline_simple_case?: boolean;
  };
}

export interface SQLFormatResponse {
  formatted_sql: string;
}

// ============================================================================
// SQL Autocomplete Types (TODO: Add to Swagger/OpenAPI spec)
// ============================================================================
// Server-side completions are more accurate than client-side for:
// - Large schemas (1000s of tables)
// - Dynamic/computed columns
// - Permission-aware suggestions
// - Cross-catalog references

export type CompletionContext =
  | 'table'      // After FROM, JOIN
  | 'column'     // After SELECT, WHERE, ORDER BY
  | 'schema'     // After schema.
  | 'function'   // After function name or in expression
  | 'keyword'    // SQL keywords
  | 'alias'      // Table aliases in scope
  | 'any';       // General context

export interface SQLCompletionRequest {
  sql: string;
  cursor_position: number;        // Character offset where cursor is
  branch?: string;                // Branch context for schema lookup
  schema?: string;                // Current default schema
  limit?: number;                 // Max suggestions (default: 50)
}

export interface CompletionItem {
  label: string;                  // Display text
  kind: 'table' | 'column' | 'schema' | 'function' | 'keyword' | 'snippet';
  detail?: string;                // Type info, e.g., "VARCHAR", "INTEGER"
  documentation?: string;         // Extended description
  insert_text: string;            // Text to insert
  insert_text_format?: 'plain' | 'snippet';  // Snippet = has $1, $2 placeholders
  sort_priority?: number;         // Lower = higher priority
  filter_text?: string;           // Text to match against (if different from label)
  table_name?: string;            // For columns: which table
  schema_name?: string;           // For tables/columns: which schema
}

export interface SQLCompletionResponse {
  completions: CompletionItem[];
  context: CompletionContext;     // What type of completion was detected
  incomplete?: boolean;           // True if results truncated
}

// ============================================================================
// UI State Types
// ============================================================================

export interface SortOption {
  field: string;
  direction: 'asc' | 'desc';
}

export interface FilterOption {
  field: string;
  value: string;
  operator: 'eq' | 'contains' | 'gt' | 'lt' | 'gte' | 'lte';
}
