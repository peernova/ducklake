import type {
  Catalog,
  RegisterCatalogRequest,
  UpdateCatalogRequest,
  CatalogTestResult,
  ListCatalogsResponse,
  ListSchemasResponse,
  ListTablesResponse,
  TableInfo,
  QueryRequest,
  QueryResponse,
  Branch,
  CreateBranchRequest,
  SearchBranchesRequest,
  ListBranchesResponse,
  UseBranchResponse,
  CurrentBranchResponse,
  BranchCountResponse,
  BranchStats,
  BranchLineageResponse,
  BranchActivityResponse,
  BranchStatus,
  Status,
} from '../types';

const API_BASE = '/api/v1';

// ============================================================================
// Fetch Wrapper
// ============================================================================

class ApiError extends Error {
  status: Status;
  httpStatus: number;

  constructor(message: string, status: Status, httpStatus: number) {
    super(message);
    this.status = status;
    this.httpStatus = httpStatus;
    this.name = 'ApiError';
  }
}

async function fetchJson<T>(url: string, options?: RequestInit): Promise<T> {
  const response = await fetch(url, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      ...options?.headers,
    },
  });

  if (!response.ok) {
    let status: Status;
    try {
      status = await response.json();
    } catch {
      status = {
        code: 13,
        message: `HTTP error: ${response.status} ${response.statusText}`,
      };
    }
    throw new ApiError(status.message, status, response.status);
  }

  // Handle 204 No Content
  if (response.status === 204) {
    return undefined as T;
  }

  return response.json();
}

function buildQueryString(params: Record<string, string | number | undefined>): string {
  const filtered = Object.entries(params).filter(([, v]) => v !== undefined);
  if (filtered.length === 0) return '';
  return '?' + filtered.map(([k, v]) => `${k}=${encodeURIComponent(String(v))}`).join('&');
}

// ============================================================================
// Query API
// ============================================================================

export const queryApi = {
  execute: (request: QueryRequest): Promise<QueryResponse> =>
    fetchJson<QueryResponse>(`${API_BASE}/query`, {
      method: 'POST',
      body: JSON.stringify(request),
    }),
};

// ============================================================================
// Catalogs API
// ============================================================================

export const catalogsApi = {
  list: (params?: {
    page_size?: number;
    page_number?: number;
    metadata_type?: string;
    storage_type?: string;
  }): Promise<ListCatalogsResponse> =>
    fetchJson<ListCatalogsResponse>(`${API_BASE}/catalogs${buildQueryString(params || {})}`),

  get: (catalogId: string): Promise<Catalog> =>
    fetchJson<Catalog>(`${API_BASE}/catalogs/${catalogId}`),

  register: (request: RegisterCatalogRequest): Promise<Catalog> =>
    fetchJson<Catalog>(`${API_BASE}/catalogs`, {
      method: 'POST',
      body: JSON.stringify(request),
    }),

  update: (catalogId: string, request: UpdateCatalogRequest): Promise<Catalog> =>
    fetchJson<Catalog>(`${API_BASE}/catalogs/${catalogId}`, {
      method: 'PATCH',
      body: JSON.stringify(request),
    }),

  unregister: (catalogId: string): Promise<void> =>
    fetchJson<void>(`${API_BASE}/catalogs/${catalogId}`, {
      method: 'DELETE',
    }),

  test: (catalogId: string): Promise<CatalogTestResult> =>
    fetchJson<CatalogTestResult>(`${API_BASE}/catalogs/${catalogId}:test`, {
      method: 'POST',
    }),

  // Schema & Table discovery
  listSchemas: (catalogId: string, branch?: string): Promise<ListSchemasResponse> =>
    fetchJson<ListSchemasResponse>(
      `${API_BASE}/catalogs/${catalogId}/schemas${buildQueryString({ branch })}`
    ),

  listTables: (
    catalogId: string,
    schemaName: string,
    branch?: string
  ): Promise<ListTablesResponse> =>
    fetchJson<ListTablesResponse>(
      `${API_BASE}/catalogs/${catalogId}/schemas/${schemaName}/tables${buildQueryString({ branch })}`
    ),

  getTable: (
    catalogId: string,
    schemaName: string,
    tableName: string,
    branch?: string
  ): Promise<TableInfo> =>
    fetchJson<TableInfo>(
      `${API_BASE}/catalogs/${catalogId}/schemas/${schemaName}/tables/${tableName}${buildQueryString({ branch })}`
    ),
};

// ============================================================================
// Branches API
// ============================================================================

export const branchesApi = {
  list: (
    catalogId: string,
    params?: {
      page_size?: number;
      page_number?: number;
      status?: BranchStatus;
    }
  ): Promise<ListBranchesResponse> =>
    fetchJson<ListBranchesResponse>(
      `${API_BASE}/catalogs/${catalogId}/branches${buildQueryString(params || {})}`
    ),

  search: (
    catalogId: string,
    request: SearchBranchesRequest,
    params?: { page_size?: number; page_number?: number }
  ): Promise<ListBranchesResponse> =>
    fetchJson<ListBranchesResponse>(
      `${API_BASE}/catalogs/${catalogId}/branches:search${buildQueryString(params || {})}`,
      {
        method: 'POST',
        body: JSON.stringify(request),
      }
    ),

  get: (catalogId: string, branchName: string): Promise<Branch> =>
    fetchJson<Branch>(`${API_BASE}/catalogs/${catalogId}/branches/${branchName}`),

  create: (catalogId: string, request: CreateBranchRequest): Promise<Branch> =>
    fetchJson<Branch>(`${API_BASE}/catalogs/${catalogId}/branches`, {
      method: 'POST',
      body: JSON.stringify(request),
    }),

  delete: (catalogId: string, branchName: string): Promise<void> =>
    fetchJson<void>(`${API_BASE}/catalogs/${catalogId}/branches/${branchName}`, {
      method: 'DELETE',
    }),

  use: (catalogId: string, branchName: string): Promise<UseBranchResponse> =>
    fetchJson<UseBranchResponse>(
      `${API_BASE}/catalogs/${catalogId}/branches/${branchName}:use`,
      { method: 'POST' }
    ),

  getCurrent: (catalogId: string): Promise<CurrentBranchResponse> =>
    fetchJson<CurrentBranchResponse>(`${API_BASE}/catalogs/${catalogId}/current-branch`),

  // Stats & Activity
  count: (catalogId: string, status?: BranchStatus): Promise<BranchCountResponse> =>
    fetchJson<BranchCountResponse>(
      `${API_BASE}/catalogs/${catalogId}/branches:count${buildQueryString({ status })}`
    ),

  getStats: (catalogId: string, branchName: string): Promise<BranchStats> =>
    fetchJson<BranchStats>(`${API_BASE}/catalogs/${catalogId}/branches/${branchName}/stats`),

  getLineage: (catalogId: string, branchName: string): Promise<BranchLineageResponse> =>
    fetchJson<BranchLineageResponse>(
      `${API_BASE}/catalogs/${catalogId}/branches/${branchName}/lineage`
    ),

  getActivity: (
    catalogId: string,
    orderBy?: 'last_modified' | 'created' | 'snapshot_count',
    limit?: number
  ): Promise<BranchActivityResponse> =>
    fetchJson<BranchActivityResponse>(
      `${API_BASE}/catalogs/${catalogId}/branches:activity${buildQueryString({
        order_by: orderBy,
        limit,
      })}`
    ),

  getByAge: (
    catalogId: string,
    days: number,
    status?: BranchStatus
  ): Promise<ListBranchesResponse> =>
    fetchJson<ListBranchesResponse>(
      `${API_BASE}/catalogs/${catalogId}/branches:by-age${buildQueryString({ days, status })}`
    ),
};

export { ApiError };
