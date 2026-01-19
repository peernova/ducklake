// =============================================================================
// API Services - Type-safe API calls matching OpenAPI spec
// =============================================================================

import { api } from './client';
import type {
  Catalog,
  CatalogStats,
  ListCatalogsResponse,
  RegisterCatalogRequest,
  UpdateCatalogRequest,
  CatalogTestResult,
  Branch,
  ListBranchesResponse,
  CreateBranchRequest,
  BranchStats,
  BranchDiffResponse,
  ListSchemasResponse,
  TableInfo,
  ListTablesResponse,
  QueryRequest,
  QueryResponse,
  SQLParseRequest,
  SQLParseResponse,
  SQLFormatRequest,
  SQLFormatResponse,
  SQLCompletionRequest,
  SQLCompletionResponse,
} from '../types';

// =============================================================================
// Catalogs
// =============================================================================

export const catalogsApi = {
  list: (params?: { page_size?: number; page_number?: number; enabled?: boolean }) =>
    api.get<ListCatalogsResponse>('/catalogs', params),

  get: (catalogId: string) =>
    api.get<Catalog>(`/catalogs/${catalogId}`),

  create: (data: RegisterCatalogRequest) =>
    api.post<Catalog>('/catalogs', data),

  update: (catalogId: string, data: UpdateCatalogRequest) =>
    api.patch<Catalog>(`/catalogs/${catalogId}`, data),

  delete: (catalogId: string) =>
    api.delete(`/catalogs/${catalogId}`),

  test: (catalogId: string) =>
    api.post<CatalogTestResult>(`/catalogs/${catalogId}/test`),

  stats: (catalogId: string) =>
    api.get<CatalogStats>(`/catalogs/${catalogId}/stats`),
};

// =============================================================================
// Branches
// =============================================================================

export const branchesApi = {
  list: (catalogId: string, params?: { status?: string; pattern?: string }) =>
    api.get<ListBranchesResponse>(`/catalogs/${catalogId}/branches`, params),

  get: (catalogId: string, branchName: string) =>
    api.get<Branch>(`/catalogs/${catalogId}/branches/${encodeURIComponent(branchName)}`),

  create: (catalogId: string, data: CreateBranchRequest) =>
    api.post<Branch>(`/catalogs/${catalogId}/branches`, data),

  delete: (catalogId: string, branchName: string) =>
    api.delete(`/catalogs/${catalogId}/branches/${encodeURIComponent(branchName)}`),

  stats: (catalogId: string, branchName: string) =>
    api.get<BranchStats>(`/catalogs/${catalogId}/branches/${encodeURIComponent(branchName)}/stats`),

  diff: (catalogId: string, baseBranch: string, compareBranch: string, options?: { include_unchanged?: boolean }) =>
    api.get<BranchDiffResponse>(`/catalogs/${catalogId}/branches/diff`, {
      base_branch: baseBranch,
      compare_branch: compareBranch,
      ...options,
    }),
};

// =============================================================================
// Schemas
// =============================================================================

export const schemasApi = {
  list: (catalogId: string, branchName?: string) =>
    api.get<ListSchemasResponse>(`/catalogs/${catalogId}/schemas`, branchName ? { branch: branchName } : undefined),

  listTables: (catalogId: string, schemaName: string, branchName?: string) =>
    api.get<ListTablesResponse>(
      `/catalogs/${catalogId}/schemas/${schemaName}/tables`,
      branchName ? { branch: branchName } : undefined
    ),

  getTable: (catalogId: string, schemaName: string, tableName: string, branchName?: string) =>
    api.get<TableInfo>(
      `/catalogs/${catalogId}/schemas/${schemaName}/tables/${tableName}`,
      branchName ? { branch: branchName } : undefined
    ),
};

// =============================================================================
// Query
// =============================================================================

export const queryApi = {
  /** Execute a SELECT query with branch context */
  execute: (data: QueryRequest) =>
    api.post<QueryResponse>('/query', data),

  /** Execute DDL/DML on a specific branch */
  executeOnBranch: (catalogId: string, branchName: string, sql: string) =>
    api.post<QueryResponse>(`/catalogs/${catalogId}/execute`, { branchName, sql }),
};

// =============================================================================
// SQL Tools
// =============================================================================

export const sqlApi = {
  parse: (data: SQLParseRequest) =>
    api.post<SQLParseResponse>('/sql/parse', data),

  format: (data: SQLFormatRequest) =>
    api.post<SQLFormatResponse>('/sql/format', data),

  completions: (data: SQLCompletionRequest) =>
    api.post<SQLCompletionResponse>('/sql/completions', data),
};

// Re-export everything
export * from './client';
