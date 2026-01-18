import React, { useState, useCallback, useMemo, useRef, useEffect } from 'react';
import {
  GitCompare,
  GitBranch,
  RefreshCw,
  Folder,
  Table,
  Columns,
  ChevronRight,
  ChevronDown,
  X,
  ArrowRight,
  Plus,
  Minus,
  Edit3,
  FolderOpen,
  Search,
} from 'lucide-react';
import { AgGridReact } from 'ag-grid-react';
import type { ColDef, ICellRendererParams } from 'ag-grid-community';
import { branchesApi } from '../api';
import type { Branch, BranchDiffResponse } from '../types';

interface BranchDiffTabProps {
  catalogId: string;
  branches: Branch[];
  currentBranch: string;
  compact?: boolean;  // When true, used in embedded/split view mode
}

type SchemaDiff = BranchDiffResponse['schemas'][0];
type TableDiff = SchemaDiff['tables'][0];
type ColumnDiff = TableDiff['columns'][0];

interface DetailModalProps {
  type: 'schema' | 'table';
  title: string;
  baseBranch: string;
  compareBranch: string;
  items: Array<{
    name: string;
    status: string;
    baseInfo?: string;
    compareInfo?: string;
    children?: Array<{
      name: string;
      status: string;
      baseInfo?: string;
      compareInfo?: string;
    }>;
  }>;
  onClose: () => void;
}

// Status cell renderer for AG Grid - now takes branch context
function StatusCellRenderer(props: ICellRendererParams & { baseBranch?: string; compareBranch?: string }) {
  const status = props.value;
  const { baseBranch, compareBranch } = props;

  const colors: Record<string, { bg: string; text: string }> = {
    added: { bg: 'rgba(34, 197, 94, 0.15)', text: '#22c55e' },
    removed: { bg: 'rgba(239, 68, 68, 0.15)', text: '#ef4444' },
    modified: { bg: 'rgba(245, 158, 11, 0.15)', text: '#f59e0b' },
    unchanged: { bg: 'var(--bg-tertiary)', text: 'var(--text-muted)' },
  };
  const c = colors[status] || colors.unchanged;

  const icon = status === 'added' ? <Plus size={12} /> :
               status === 'removed' ? <Plus size={12} /> :
               status === 'modified' ? <Edit3 size={12} /> : null;

  // Show contextual label - which branch the item exists in
  let label = status;
  if (status === 'added' && compareBranch) {
    label = compareBranch;  // exists only in compare branch
  } else if (status === 'removed' && baseBranch) {
    label = baseBranch;  // exists only in base branch
  }

  return (
    <span style={{
      display: 'inline-flex',
      alignItems: 'center',
      gap: '4px',
      fontSize: '10px',
      padding: '2px 8px',
      borderRadius: '4px',
      background: c.bg,
      color: c.text,
      fontWeight: 500,
    }}>
      {icon}
      {label}
    </span>
  );
}

// Cell renderer for base/compare values with appropriate styling
function ValueCellRenderer(props: ICellRendererParams & { isBase?: boolean }) {
  const { value, data, isBase } = props;
  if (!value || value === '-') {
    return <span style={{ color: 'var(--text-muted)', fontStyle: 'italic' }}>-</span>;
  }

  const status = data?.status;
  let color = 'var(--text-primary)';
  if (isBase && status === 'removed') color = '#ef4444';
  if (!isBase && status === 'added') color = '#22c55e';

  return <span style={{ color, fontFamily: 'monospace', fontSize: '12px' }}>{value}</span>;
}

function DetailModal({ type, title, baseBranch, compareBranch, items, onClose }: DetailModalProps) {
  // Flatten items for AG Grid (include children as separate rows with indentation)
  const rowData = useMemo(() => {
    const rows: Array<{
      id: string;
      name: string;
      status: string;
      baseValue: string;
      compareValue: string;
      isChild: boolean;
      parentName?: string;
    }> = [];

    items.forEach(item => {
      rows.push({
        id: item.name,
        name: item.name,
        status: item.status,
        baseValue: item.status !== 'added' ? (item.baseInfo || item.name) : '-',
        compareValue: item.status !== 'removed' ? (item.compareInfo || item.name) : '-',
        isChild: false,
      });

      if (item.children) {
        item.children.forEach(child => {
          rows.push({
            id: `${item.name}.${child.name}`,
            name: child.name,
            status: child.status,
            baseValue: child.status !== 'added' ? (child.baseInfo || '-') : '-',
            compareValue: child.status !== 'removed' ? (child.compareInfo || '-') : '-',
            isChild: true,
            parentName: item.name,
          });
        });
      }
    });

    return rows;
  }, [items]);

  const columnDefs: ColDef[] = useMemo(() => [
    {
      headerName: 'Name',
      field: 'name',
      flex: 1.5,
      minWidth: 150,
      cellRenderer: (params: ICellRendererParams) => {
        const { value, data } = params;
        const indent = data?.isChild ? 24 : 0;
        const icon = data?.isChild ?
          <Columns size={12} style={{ color: 'var(--text-muted)' }} /> :
          <Table size={14} style={{ color: '#8b5cf6' }} />;
        return (
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px', paddingLeft: indent }}>
            {icon}
            <span style={{ fontWeight: data?.isChild ? 400 : 500 }}>{value}</span>
          </div>
        );
      },
    },
    {
      headerName: `${baseBranch} (base)`,
      field: 'baseValue',
      flex: 1,
      minWidth: 120,
      headerClass: 'base-header',
      cellRenderer: (params: ICellRendererParams) => <ValueCellRenderer {...params} isBase={true} />,
    },
    {
      headerName: 'Status',
      field: 'status',
      width: 140,
      cellRenderer: (params: ICellRendererParams) => (
        <StatusCellRenderer {...params} baseBranch={baseBranch} compareBranch={compareBranch} />
      ),
      cellStyle: { display: 'flex', alignItems: 'center', justifyContent: 'center' },
    },
    {
      headerName: `${compareBranch} (compare)`,
      field: 'compareValue',
      flex: 1,
      minWidth: 120,
      headerClass: 'compare-header',
      cellRenderer: (params: ICellRendererParams) => <ValueCellRenderer {...params} isBase={false} />,
    },
  ], [baseBranch, compareBranch]);

  const getRowStyle = (params: { data: { status: string; isChild: boolean } }) => {
    const status = params.data?.status;
    const isChild = params.data?.isChild;
    let bg = 'transparent';

    if (status === 'added') bg = 'rgba(34, 197, 94, 0.06)';
    else if (status === 'removed') bg = 'rgba(239, 68, 68, 0.06)';
    else if (status === 'modified') bg = 'rgba(245, 158, 11, 0.06)';

    if (isChild) {
      return { background: bg, fontSize: '12px' };
    }
    return { background: bg, fontWeight: 500 };
  };

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div
        className="modal"
        style={{ maxWidth: '1000px', width: '90vw', maxHeight: '85vh', display: 'flex', flexDirection: 'column' }}
        onClick={e => e.stopPropagation()}
      >
        <div className="modal-header" style={{ borderBottom: '1px solid var(--border-color)' }}>
          <h3 className="modal-title" style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
            {type === 'schema' ? <Folder size={18} style={{ color: '#0ea5e9' }} /> : <Table size={18} style={{ color: '#8b5cf6' }} />}
            {title}
          </h3>
          <button className="modal-close" onClick={onClose}>
            <X size={20} />
          </button>
        </div>

        <div className="modal-body" style={{ flex: 1, overflow: 'hidden', padding: '16px' }}>
          <div
            className="ag-theme-alpine-dark"
            style={{
              height: Math.min(400, rowData.length * 42 + 48),
              width: '100%',
            }}
          >
            <AgGridReact
              rowData={rowData}
              columnDefs={columnDefs}
              getRowStyle={getRowStyle}
              headerHeight={40}
              rowHeight={38}
              suppressCellFocus={true}
              animateRows={false}
              getRowId={(params) => params.data.id}
            />
          </div>

          {/* Legend */}
          <div style={{
            marginTop: '16px',
            padding: '12px 16px',
            background: 'var(--bg-tertiary)',
            borderRadius: '6px',
            display: 'flex',
            gap: '24px',
            justifyContent: 'center',
            fontSize: '12px',
          }}>
            <span style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
              <span style={{
                display: 'inline-flex', alignItems: 'center', gap: '3px',
                padding: '2px 8px', borderRadius: '4px',
                background: 'rgba(34, 197, 94, 0.15)', color: '#22c55e', fontSize: '10px', fontWeight: 500
              }}>
                <Plus size={10} /> {compareBranch}
              </span>
              <span style={{ color: 'var(--text-muted)' }}>Only in compare branch</span>
            </span>
            <span style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
              <span style={{
                display: 'inline-flex', alignItems: 'center', gap: '3px',
                padding: '2px 8px', borderRadius: '4px',
                background: 'rgba(239, 68, 68, 0.15)', color: '#ef4444', fontSize: '10px', fontWeight: 500
              }}>
                <Plus size={10} /> {baseBranch}
              </span>
              <span style={{ color: 'var(--text-muted)' }}>Only in base branch</span>
            </span>
            <span style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
              <span style={{
                display: 'inline-flex', alignItems: 'center', gap: '3px',
                padding: '2px 8px', borderRadius: '4px',
                background: 'rgba(245, 158, 11, 0.15)', color: '#f59e0b', fontSize: '10px', fontWeight: 500
              }}>
                <Edit3 size={10} /> modified
              </span>
              <span style={{ color: 'var(--text-muted)' }}>Changed between branches</span>
            </span>
          </div>
        </div>

        <div className="modal-footer" style={{ borderTop: '1px solid var(--border-color)' }}>
          <button className="btn btn-secondary" onClick={onClose}>Close</button>
        </div>
      </div>
    </div>
  );
}

export function BranchDiffTab({ catalogId, branches, currentBranch, compact = false }: BranchDiffTabProps) {
  const [baseBranch, setBaseBranch] = useState('main');
  const [compareBranch, setCompareBranch] = useState(currentBranch !== 'main' ? currentBranch : '');
  const [diff, setDiff] = useState<BranchDiffResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Track which branches the current diff is for
  const [diffForBranches, setDiffForBranches] = useState<{ base: string; compare: string } | null>(null);

  // Expanded state for tree
  const [expandedSchemas, setExpandedSchemas] = useState<Set<string>>(new Set());
  const [expandedTables, setExpandedTables] = useState<Set<string>>(new Set());

  // Modal state
  const [modalData, setModalData] = useState<{
    type: 'schema' | 'table';
    title: string;
    items: DetailModalProps['items'];
  } | null>(null);

  // Search state
  const [searchQuery, setSearchQuery] = useState('');
  const [showSearchResults, setShowSearchResults] = useState(false);
  const [highlightedItem, setHighlightedItem] = useState<string | null>(null);
  const searchRef = useRef<HTMLDivElement>(null);
  const treeContainerRef = useRef<HTMLDivElement>(null);

  // Branch selector dropdown state
  const [baseDropdownOpen, setBaseDropdownOpen] = useState(false);
  const [compareDropdownOpen, setCompareDropdownOpen] = useState(false);
  const [baseBranchSearch, setBaseBranchSearch] = useState('');
  const [compareBranchSearch, setCompareBranchSearch] = useState('');
  const baseDropdownRef = useRef<HTMLDivElement>(null);
  const compareDropdownRef = useRef<HTMLDivElement>(null);

  // Close dropdowns when clicking outside
  useEffect(() => {
    const handleClickOutside = (e: MouseEvent) => {
      if (searchRef.current && !searchRef.current.contains(e.target as Node)) {
        setShowSearchResults(false);
      }
      if (baseDropdownRef.current && !baseDropdownRef.current.contains(e.target as Node)) {
        setBaseDropdownOpen(false);
      }
      if (compareDropdownRef.current && !compareDropdownRef.current.contains(e.target as Node)) {
        setCompareDropdownOpen(false);
      }
    };
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  // Search results
  const searchResults = useMemo(() => {
    if (!diff || !searchQuery.trim()) return { schemas: [], tables: [], columns: [] };

    const query = searchQuery.toLowerCase();
    const schemas: Array<{ name: string; status: string }> = [];
    const tables: Array<{ schema: string; name: string; status: string }> = [];
    const columns: Array<{ schema: string; table: string; name: string; status: string; type?: string }> = [];

    diff.schemas.forEach(schema => {
      if (schema.schema_name.toLowerCase().includes(query)) {
        schemas.push({ name: schema.schema_name, status: schema.status });
      }
      schema.tables.forEach(table => {
        if (table.table_name.toLowerCase().includes(query)) {
          tables.push({ schema: schema.schema_name, name: table.table_name, status: table.status });
        }
        table.columns.forEach(col => {
          if (col.column_name.toLowerCase().includes(query)) {
            columns.push({
              schema: schema.schema_name,
              table: table.table_name,
              name: col.column_name,
              status: col.status,
              type: col.compare_type || col.base_type,
            });
          }
        });
      });
    });

    return { schemas, tables, columns };
  }, [diff, searchQuery]);

  const hasSearchResults = searchResults.schemas.length > 0 || searchResults.tables.length > 0 || searchResults.columns.length > 0;

  // Navigate to item from search
  const navigateToSchema = (schemaName: string) => {
    setExpandedSchemas(prev => new Set([...prev, schemaName]));
    setHighlightedItem(`schema:${schemaName}`);
    setShowSearchResults(false);
    setSearchQuery('');
    setTimeout(() => {
      const el = document.getElementById(`schema-${schemaName}`);
      el?.scrollIntoView({ behavior: 'smooth', block: 'center' });
      setTimeout(() => setHighlightedItem(null), 2000);
    }, 100);
  };

  const navigateToTable = (schemaName: string, tableName: string) => {
    setExpandedSchemas(prev => new Set([...prev, schemaName]));
    setHighlightedItem(`table:${schemaName}.${tableName}`);
    setShowSearchResults(false);
    setSearchQuery('');
    setTimeout(() => {
      const el = document.getElementById(`table-${schemaName}-${tableName}`);
      el?.scrollIntoView({ behavior: 'smooth', block: 'center' });
      setTimeout(() => setHighlightedItem(null), 2000);
    }, 100);
  };

  const navigateToColumn = (schemaName: string, tableName: string, columnName: string) => {
    setExpandedSchemas(prev => new Set([...prev, schemaName]));
    setExpandedTables(prev => new Set([...prev, `${schemaName}.${tableName}`]));
    setHighlightedItem(`column:${schemaName}.${tableName}.${columnName}`);
    setShowSearchResults(false);
    setSearchQuery('');
    setTimeout(() => {
      const el = document.getElementById(`column-${schemaName}-${tableName}-${columnName}`);
      el?.scrollIntoView({ behavior: 'smooth', block: 'center' });
      setTimeout(() => setHighlightedItem(null), 2000);
    }, 100);
  };

  const fetchDiff = useCallback(async () => {
    if (!baseBranch || !compareBranch || baseBranch === compareBranch) return;

    setLoading(true);
    setError(null);
    try {
      const result = await branchesApi.diff(catalogId, baseBranch, compareBranch);
      setDiff(result);
      setDiffForBranches({ base: baseBranch, compare: compareBranch });
      // Auto-expand schemas with changes
      setExpandedSchemas(new Set(result.schemas.filter(s => s.status !== 'unchanged').map(s => s.schema_name)));
      setExpandedTables(new Set());
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to fetch diff');
      setDiff(null);
      setDiffForBranches(null);
    } finally {
      setLoading(false);
    }
  }, [catalogId, baseBranch, compareBranch]);

  const toggleSchema = (schemaName: string) => {
    setExpandedSchemas(prev => {
      const next = new Set(prev);
      if (next.has(schemaName)) next.delete(schemaName);
      else next.add(schemaName);
      return next;
    });
  };

  const toggleTable = (key: string) => {
    setExpandedTables(prev => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  };

  const expandAll = () => {
    if (!diff) return;
    setExpandedSchemas(new Set(diff.schemas.map(s => s.schema_name)));
    const allTables = diff.schemas.flatMap(s => s.tables.map(t => `${s.schema_name}.${t.table_name}`));
    setExpandedTables(new Set(allTables));
  };

  const collapseAll = () => {
    setExpandedSchemas(new Set());
    setExpandedTables(new Set());
  };

  const openSchemaModal = (schema: SchemaDiff) => {
    setModalData({
      type: 'schema',
      title: `Schema: ${schema.schema_name}`,
      items: schema.tables.map(t => ({
        name: t.table_name,
        status: t.status,
        baseInfo: t.status !== 'added' ? `${t.columns.length} columns` : undefined,
        compareInfo: t.status !== 'removed' ? `${t.columns.length} columns` : undefined,
        children: t.columns.map(c => ({
          name: c.column_name,
          status: c.status,
          baseInfo: c.base_type,
          compareInfo: c.compare_type,
        })),
      })),
    });
  };

  const openTableModal = (schema: SchemaDiff, table: TableDiff) => {
    setModalData({
      type: 'table',
      title: `Table: ${schema.schema_name}.${table.table_name}`,
      items: table.columns.map(c => ({
        name: c.column_name,
        status: c.status,
        baseInfo: c.base_type ? `${c.base_type}${c.base_nullable === false ? ' NOT NULL' : ''}` : undefined,
        compareInfo: c.compare_type ? `${c.compare_type}${c.compare_nullable === false ? ' NOT NULL' : ''}` : undefined,
      })),
    });
  };

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'added': return '#22c55e';
      case 'removed': return '#ef4444';
      case 'modified': return '#f59e0b';
      default: return 'var(--text-muted)';
    }
  };

  const getStatusBg = (status: string) => {
    switch (status) {
      case 'added': return 'rgba(34, 197, 94, 0.06)';
      case 'removed': return 'rgba(239, 68, 68, 0.06)';
      case 'modified': return 'rgba(245, 158, 11, 0.06)';
      default: return 'transparent';
    }
  };

  const getStatusBadge = (status: string) => {
    const colors: Record<string, { bg: string; text: string }> = {
      added: { bg: 'rgba(34, 197, 94, 0.15)', text: '#22c55e' },
      removed: { bg: 'rgba(239, 68, 68, 0.15)', text: '#ef4444' },
      modified: { bg: 'rgba(245, 158, 11, 0.15)', text: '#f59e0b' },
    };
    const c = colors[status] || { bg: 'var(--bg-tertiary)', text: 'var(--text-muted)' };

    // Show contextual label - which branch the item exists in
    let label = status;
    let icon = null;
    if (status === 'added') {
      label = compareBranch;  // exists only in compare branch
      icon = <Plus size={10} />;
    } else if (status === 'removed') {
      label = baseBranch;  // exists only in base branch
      icon = <Plus size={10} />;
    } else if (status === 'modified') {
      label = 'modified';
      icon = <Edit3 size={10} />;
    }

    return (
      <span style={{
        display: 'inline-flex',
        alignItems: 'center',
        gap: '3px',
        fontSize: '10px',
        padding: '2px 6px',
        borderRadius: '4px',
        background: c.bg,
        color: c.text,
        fontWeight: 500,
      }}>
        {icon}
        {label}
      </span>
    );
  };

  const activeBranches = branches.filter(b => b.status === 'active');

  // Calculate total items for performance indicator
  const totalItems = useMemo(() => {
    if (!diff) return 0;
    let count = diff.schemas.length;
    for (const s of diff.schemas) {
      count += s.tables.length;
      for (const t of s.tables) {
        count += t.columns.filter(c => c.status !== 'unchanged').length;
      }
    }
    return count;
  }, [diff]);

  return (
    <div style={{ height: compact ? '100%' : 'auto', display: 'flex', flexDirection: 'column' }}>
      {/* Header - hide in compact mode */}
      {!compact && (
        <div className="tab-header">
          <div className="tab-header-left">
            <h3 className="section-title" style={{ margin: 0 }}>Branch Diff</h3>
            <span className="text-muted text-sm">Compare schemas between branches</span>
          </div>
        </div>
      )}

      {/* Branch Selectors */}
      <div style={{
        display: 'flex',
        gap: compact ? '8px' : '16px',
        alignItems: 'flex-end',
        marginBottom: compact ? '0' : '16px',
        padding: compact ? '12px' : '16px',
        background: 'var(--bg-secondary)',
        borderRadius: compact ? '0' : '8px',
        borderBottom: compact ? '1px solid var(--border-color)' : 'none',
      }}>
        {/* Base Branch Selector */}
        <div style={{ flex: 1 }}>
          <label style={{ fontSize: '11px', color: 'var(--text-muted)', display: 'block', marginBottom: '6px', textTransform: 'uppercase', fontWeight: 600 }}>
            Base Branch
          </label>
          <div ref={baseDropdownRef} style={{ position: 'relative' }}>
            <div
              onClick={() => { setBaseDropdownOpen(!baseDropdownOpen); setCompareDropdownOpen(false); }}
              style={{
                display: 'flex',
                alignItems: 'center',
                gap: '8px',
                padding: '8px 10px',
                background: 'var(--bg-primary)',
                border: '1px solid var(--border-color)',
                borderRadius: '6px',
                cursor: 'pointer',
                transition: 'border-color 0.15s ease',
              }}
              onMouseEnter={(e) => e.currentTarget.style.borderColor = 'var(--accent-primary)'}
              onMouseLeave={(e) => e.currentTarget.style.borderColor = 'var(--border-color)'}
            >
              <GitBranch size={14} style={{ color: 'var(--accent-primary)', flexShrink: 0 }} />
              <span style={{ flex: 1, fontSize: '13px', fontWeight: 500, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                {baseBranch}
              </span>
              <ChevronDown size={14} style={{ color: 'var(--text-muted)', flexShrink: 0, transform: baseDropdownOpen ? 'rotate(180deg)' : 'none', transition: 'transform 0.15s ease' }} />
            </div>

            {baseDropdownOpen && (
              <div style={{
                position: 'absolute',
                top: '100%',
                left: 0,
                right: 0,
                marginTop: '4px',
                background: 'var(--bg-primary)',
                border: '1px solid var(--border-color)',
                borderRadius: '6px',
                boxShadow: '0 4px 12px rgba(0,0,0,0.15)',
                zIndex: 100,
                maxHeight: '240px',
                display: 'flex',
                flexDirection: 'column',
              }}>
                <div style={{ padding: '8px', borderBottom: '1px solid var(--border-color)' }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: '6px', padding: '6px 8px', background: 'var(--bg-secondary)', borderRadius: '4px' }}>
                    <Search size={12} style={{ color: 'var(--text-muted)' }} />
                    <input
                      type="text"
                      placeholder="Search..."
                      value={baseBranchSearch}
                      onChange={(e) => setBaseBranchSearch(e.target.value)}
                      onClick={(e) => e.stopPropagation()}
                      autoFocus
                      style={{ flex: 1, background: 'transparent', border: 'none', outline: 'none', color: 'var(--text-primary)', fontSize: '12px' }}
                    />
                    {baseBranchSearch && (
                      <button onClick={(e) => { e.stopPropagation(); setBaseBranchSearch(''); }} style={{ background: 'none', border: 'none', padding: '2px', cursor: 'pointer', color: 'var(--text-muted)' }}>
                        <X size={10} />
                      </button>
                    )}
                  </div>
                </div>
                <div style={{ overflowY: 'auto', flex: 1 }}>
                  {activeBranches.filter(b => b.branch_name.toLowerCase().includes(baseBranchSearch.toLowerCase())).map(b => (
                    <div
                      key={b.branch_id}
                      onClick={() => { setBaseBranch(b.branch_name); setBaseDropdownOpen(false); setBaseBranchSearch(''); }}
                      style={{
                        display: 'flex', alignItems: 'center', gap: '8px', padding: '8px 12px', cursor: 'pointer', fontSize: '12px',
                        background: b.branch_name === baseBranch ? 'var(--accent-secondary)' : 'transparent',
                        borderLeft: b.branch_name === baseBranch ? '2px solid var(--accent-primary)' : '2px solid transparent',
                      }}
                      onMouseEnter={(e) => { if (b.branch_name !== baseBranch) e.currentTarget.style.background = 'var(--bg-secondary)'; }}
                      onMouseLeave={(e) => { if (b.branch_name !== baseBranch) e.currentTarget.style.background = 'transparent'; }}
                    >
                      <GitBranch size={12} style={{ color: b.branch_name === 'main' ? 'var(--accent-primary)' : 'var(--text-muted)' }} />
                      <span style={{ flex: 1, fontWeight: b.branch_name === baseBranch ? 500 : 400 }}>{b.branch_name}</span>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>
        </div>

        <div style={{ padding: '8px', color: 'var(--text-muted)' }}>
          <GitCompare size={20} />
        </div>

        {/* Compare Branch Selector */}
        <div style={{ flex: 1 }}>
          <label style={{ fontSize: '11px', color: 'var(--text-muted)', display: 'block', marginBottom: '6px', textTransform: 'uppercase', fontWeight: 600 }}>
            Compare Branch
          </label>
          <div ref={compareDropdownRef} style={{ position: 'relative' }}>
            <div
              onClick={() => { setCompareDropdownOpen(!compareDropdownOpen); setBaseDropdownOpen(false); }}
              style={{
                display: 'flex',
                alignItems: 'center',
                gap: '8px',
                padding: '8px 10px',
                background: 'var(--bg-primary)',
                border: '1px solid var(--border-color)',
                borderRadius: '6px',
                cursor: 'pointer',
                transition: 'border-color 0.15s ease',
              }}
              onMouseEnter={(e) => e.currentTarget.style.borderColor = 'var(--accent-primary)'}
              onMouseLeave={(e) => e.currentTarget.style.borderColor = 'var(--border-color)'}
            >
              <GitBranch size={14} style={{ color: compareBranch ? 'var(--accent-primary)' : 'var(--text-muted)', flexShrink: 0 }} />
              <span style={{ flex: 1, fontSize: '13px', fontWeight: 500, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', color: compareBranch ? 'var(--text-primary)' : 'var(--text-muted)' }}>
                {compareBranch || 'Select branch...'}
              </span>
              <ChevronDown size={14} style={{ color: 'var(--text-muted)', flexShrink: 0, transform: compareDropdownOpen ? 'rotate(180deg)' : 'none', transition: 'transform 0.15s ease' }} />
            </div>

            {compareDropdownOpen && (
              <div style={{
                position: 'absolute',
                top: '100%',
                left: 0,
                right: 0,
                marginTop: '4px',
                background: 'var(--bg-primary)',
                border: '1px solid var(--border-color)',
                borderRadius: '6px',
                boxShadow: '0 4px 12px rgba(0,0,0,0.15)',
                zIndex: 100,
                maxHeight: '240px',
                display: 'flex',
                flexDirection: 'column',
              }}>
                <div style={{ padding: '8px', borderBottom: '1px solid var(--border-color)' }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: '6px', padding: '6px 8px', background: 'var(--bg-secondary)', borderRadius: '4px' }}>
                    <Search size={12} style={{ color: 'var(--text-muted)' }} />
                    <input
                      type="text"
                      placeholder="Search..."
                      value={compareBranchSearch}
                      onChange={(e) => setCompareBranchSearch(e.target.value)}
                      onClick={(e) => e.stopPropagation()}
                      autoFocus
                      style={{ flex: 1, background: 'transparent', border: 'none', outline: 'none', color: 'var(--text-primary)', fontSize: '12px' }}
                    />
                    {compareBranchSearch && (
                      <button onClick={(e) => { e.stopPropagation(); setCompareBranchSearch(''); }} style={{ background: 'none', border: 'none', padding: '2px', cursor: 'pointer', color: 'var(--text-muted)' }}>
                        <X size={10} />
                      </button>
                    )}
                  </div>
                </div>
                <div style={{ overflowY: 'auto', flex: 1 }}>
                  {activeBranches.filter(b => b.branch_name !== baseBranch && b.branch_name.toLowerCase().includes(compareBranchSearch.toLowerCase())).map(b => (
                    <div
                      key={b.branch_id}
                      onClick={() => { setCompareBranch(b.branch_name); setCompareDropdownOpen(false); setCompareBranchSearch(''); }}
                      style={{
                        display: 'flex', alignItems: 'center', gap: '8px', padding: '8px 12px', cursor: 'pointer', fontSize: '12px',
                        background: b.branch_name === compareBranch ? 'var(--accent-secondary)' : 'transparent',
                        borderLeft: b.branch_name === compareBranch ? '2px solid var(--accent-primary)' : '2px solid transparent',
                      }}
                      onMouseEnter={(e) => { if (b.branch_name !== compareBranch) e.currentTarget.style.background = 'var(--bg-secondary)'; }}
                      onMouseLeave={(e) => { if (b.branch_name !== compareBranch) e.currentTarget.style.background = 'transparent'; }}
                    >
                      <GitBranch size={12} style={{ color: b.branch_name === 'main' ? 'var(--accent-primary)' : 'var(--text-muted)' }} />
                      <span style={{ flex: 1, fontWeight: b.branch_name === compareBranch ? 500 : 400 }}>{b.branch_name}</span>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>
        </div>

        <button
          className="btn btn-primary"
          onClick={fetchDiff}
          disabled={loading || !compareBranch || baseBranch === compareBranch || (diffForBranches?.base === baseBranch && diffForBranches?.compare === compareBranch)}
          style={{ minWidth: compact ? '90px' : '120px' }}
        >
          {loading ? <RefreshCw size={16} className="spin" /> : <GitCompare size={16} />}
          {compact ? '' : 'Compare'}
        </button>
      </div>

      {/* Error */}
      {error && (
        <div style={{
          padding: '12px 16px',
          background: 'rgba(239, 68, 68, 0.1)',
          border: '1px solid rgba(239, 68, 68, 0.3)',
          borderRadius: '8px',
          color: '#ef4444',
          marginBottom: '16px',
        }}>
          {error}
        </div>
      )}

      {/* Summary & Controls */}
      {diff && (
        <div style={{
          display: 'flex',
          flexDirection: 'column',
          gap: compact ? '8px' : '12px',
          marginBottom: compact ? '0' : '12px',
          padding: compact ? '8px 12px' : '0',
          borderBottom: compact ? '1px solid var(--border-color)' : 'none',
        }}>
          {/* Summary row */}
          <div style={{
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'center',
            flexWrap: 'wrap',
            gap: '12px',
          }}>
            <div style={{ display: 'flex', gap: '16px', flexWrap: 'wrap' }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: '6px', fontSize: '13px' }}>
                <Folder size={14} style={{ color: '#0ea5e9' }} />
                <span style={{ fontWeight: 500 }}>Schemas:</span>
                {diff.summary.schemas_added > 0 && <span style={{ color: '#22c55e' }}>+{diff.summary.schemas_added}</span>}
                {diff.summary.schemas_removed > 0 && <span style={{ color: '#ef4444' }}>-{diff.summary.schemas_removed}</span>}
                {diff.summary.schemas_modified > 0 && <span style={{ color: '#f59e0b' }}>~{diff.summary.schemas_modified}</span>}
              </div>
              <div style={{ display: 'flex', alignItems: 'center', gap: '6px', fontSize: '13px' }}>
                <Table size={14} style={{ color: '#8b5cf6' }} />
                <span style={{ fontWeight: 500 }}>Tables:</span>
                {diff.summary.tables_added > 0 && <span style={{ color: '#22c55e' }}>+{diff.summary.tables_added}</span>}
                {diff.summary.tables_removed > 0 && <span style={{ color: '#ef4444' }}>-{diff.summary.tables_removed}</span>}
                {diff.summary.tables_modified > 0 && <span style={{ color: '#f59e0b' }}>~{diff.summary.tables_modified}</span>}
              </div>
              <div style={{ display: 'flex', alignItems: 'center', gap: '6px', fontSize: '13px' }}>
                <Columns size={14} style={{ color: '#64748b' }} />
                <span style={{ fontWeight: 500 }}>Columns:</span>
                {diff.summary.columns_added > 0 && <span style={{ color: '#22c55e' }}>+{diff.summary.columns_added}</span>}
                {diff.summary.columns_removed > 0 && <span style={{ color: '#ef4444' }}>-{diff.summary.columns_removed}</span>}
                {diff.summary.columns_modified > 0 && <span style={{ color: '#f59e0b' }}>~{diff.summary.columns_modified}</span>}
              </div>
              <span style={{ fontSize: '11px', color: 'var(--text-muted)' }}>
                ({totalItems} items)
              </span>
            </div>

            <div style={{ display: 'flex', gap: '8px' }}>
              <button className="btn btn-ghost btn-sm" onClick={expandAll}>
                <ChevronDown size={14} /> Expand All
              </button>
              <button className="btn btn-ghost btn-sm" onClick={collapseAll}>
                <ChevronRight size={14} /> Collapse All
              </button>
            </div>
          </div>

          {/* Search bar */}
          <div ref={searchRef} style={{ position: 'relative' }}>
            <div style={{
              display: 'flex',
              alignItems: 'center',
              gap: '8px',
              padding: '8px 12px',
              background: 'var(--bg-secondary)',
              border: '1px solid var(--border-color)',
              borderRadius: '6px',
            }}>
              <Search size={16} style={{ color: 'var(--text-muted)' }} />
              <input
                type="text"
                placeholder="Search schemas, tables, columns..."
                value={searchQuery}
                onChange={(e) => {
                  setSearchQuery(e.target.value);
                  setShowSearchResults(true);
                }}
                onFocus={() => setShowSearchResults(true)}
                style={{
                  flex: 1,
                  background: 'transparent',
                  border: 'none',
                  outline: 'none',
                  color: 'var(--text-primary)',
                  fontSize: '13px',
                }}
              />
              {searchQuery && (
                <button
                  onClick={() => { setSearchQuery(''); setShowSearchResults(false); }}
                  style={{
                    background: 'none',
                    border: 'none',
                    padding: '2px',
                    cursor: 'pointer',
                    color: 'var(--text-muted)',
                  }}
                >
                  <X size={14} />
                </button>
              )}
            </div>

            {/* Search Results Dropdown */}
            {showSearchResults && searchQuery && (
              <div style={{
                position: 'absolute',
                top: '100%',
                left: 0,
                right: 0,
                marginTop: '4px',
                background: 'var(--bg-primary)',
                border: '1px solid var(--border-color)',
                borderRadius: '6px',
                boxShadow: '0 4px 12px rgba(0,0,0,0.3)',
                maxHeight: '320px',
                overflowY: 'auto',
                zIndex: 100,
              }}>
                {!hasSearchResults ? (
                  <div style={{ padding: '16px', textAlign: 'center', color: 'var(--text-muted)', fontSize: '13px' }}>
                    No results found for "{searchQuery}"
                  </div>
                ) : (
                  <>
                    {/* Schemas */}
                    {searchResults.schemas.length > 0 && (
                      <div>
                        <div style={{
                          padding: '8px 12px',
                          background: 'var(--bg-tertiary)',
                          fontSize: '11px',
                          fontWeight: 600,
                          color: 'var(--text-muted)',
                          textTransform: 'uppercase',
                          display: 'flex',
                          alignItems: 'center',
                          gap: '6px',
                        }}>
                          <Folder size={12} style={{ color: '#0ea5e9' }} />
                          Schemas ({searchResults.schemas.length})
                        </div>
                        {searchResults.schemas.map(s => (
                          <div
                            key={s.name}
                            onClick={() => navigateToSchema(s.name)}
                            style={{
                              padding: '8px 12px',
                              cursor: 'pointer',
                              display: 'flex',
                              alignItems: 'center',
                              gap: '8px',
                              borderBottom: '1px solid var(--border-light)',
                            }}
                            onMouseEnter={(e) => e.currentTarget.style.background = 'var(--bg-secondary)'}
                            onMouseLeave={(e) => e.currentTarget.style.background = 'transparent'}
                          >
                            <Folder size={14} style={{ color: '#0ea5e9' }} />
                            <span style={{ flex: 1 }}>{s.name}</span>
                            {getStatusBadge(s.status)}
                          </div>
                        ))}
                      </div>
                    )}

                    {/* Tables */}
                    {searchResults.tables.length > 0 && (
                      <div>
                        <div style={{
                          padding: '8px 12px',
                          background: 'var(--bg-tertiary)',
                          fontSize: '11px',
                          fontWeight: 600,
                          color: 'var(--text-muted)',
                          textTransform: 'uppercase',
                          display: 'flex',
                          alignItems: 'center',
                          gap: '6px',
                        }}>
                          <Table size={12} style={{ color: '#8b5cf6' }} />
                          Tables ({searchResults.tables.length})
                        </div>
                        {searchResults.tables.map(t => (
                          <div
                            key={`${t.schema}.${t.name}`}
                            onClick={() => navigateToTable(t.schema, t.name)}
                            style={{
                              padding: '8px 12px',
                              cursor: 'pointer',
                              display: 'flex',
                              alignItems: 'center',
                              gap: '8px',
                              borderBottom: '1px solid var(--border-light)',
                            }}
                            onMouseEnter={(e) => e.currentTarget.style.background = 'var(--bg-secondary)'}
                            onMouseLeave={(e) => e.currentTarget.style.background = 'transparent'}
                          >
                            <Table size={14} style={{ color: '#8b5cf6' }} />
                            <span style={{ flex: 1 }}>
                              <span style={{ color: 'var(--text-muted)' }}>{t.schema}.</span>
                              {t.name}
                            </span>
                            {getStatusBadge(t.status)}
                          </div>
                        ))}
                      </div>
                    )}

                    {/* Columns */}
                    {searchResults.columns.length > 0 && (
                      <div>
                        <div style={{
                          padding: '8px 12px',
                          background: 'var(--bg-tertiary)',
                          fontSize: '11px',
                          fontWeight: 600,
                          color: 'var(--text-muted)',
                          textTransform: 'uppercase',
                          display: 'flex',
                          alignItems: 'center',
                          gap: '6px',
                        }}>
                          <Columns size={12} style={{ color: '#64748b' }} />
                          Columns ({searchResults.columns.length})
                        </div>
                        {searchResults.columns.map(c => (
                          <div
                            key={`${c.schema}.${c.table}.${c.name}`}
                            onClick={() => navigateToColumn(c.schema, c.table, c.name)}
                            style={{
                              padding: '8px 12px',
                              cursor: 'pointer',
                              display: 'flex',
                              alignItems: 'center',
                              gap: '8px',
                              borderBottom: '1px solid var(--border-light)',
                            }}
                            onMouseEnter={(e) => e.currentTarget.style.background = 'var(--bg-secondary)'}
                            onMouseLeave={(e) => e.currentTarget.style.background = 'transparent'}
                          >
                            <Columns size={14} style={{ color: '#64748b' }} />
                            <span style={{ flex: 1 }}>
                              <span style={{ color: 'var(--text-muted)' }}>{c.schema}.{c.table}.</span>
                              {c.name}
                              {c.type && <span style={{ marginLeft: '6px', fontSize: '11px', color: 'var(--text-muted)', fontFamily: 'monospace' }}>{c.type}</span>}
                            </span>
                            {getStatusBadge(c.status)}
                          </div>
                        ))}
                      </div>
                    )}
                  </>
                )}
              </div>
            )}
          </div>
        </div>
      )}

      {/* Diff Tree */}
      {loading ? (
        <div style={{
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'center',
          justifyContent: 'center',
          padding: compact ? '40px' : '60px',
          color: 'var(--text-muted)',
          flex: compact ? 1 : 'none',
        }}>
          <RefreshCw size={compact ? 24 : 32} className="spin" />
          <p style={{ marginTop: '12px', fontSize: compact ? '12px' : '14px' }}>Loading diff...</p>
        </div>
      ) : diff ? (
        <div
          ref={treeContainerRef}
          style={{
            border: compact ? 'none' : '1px solid var(--border-color)',
            borderRadius: compact ? '0' : '8px',
            overflow: 'hidden',
            flex: compact ? 1 : 'none',
            height: compact ? 'auto' : '400px',
            overflowY: 'auto',
          }}
        >
          {diff.schemas.length === 0 ? (
            <div style={{ padding: '40px', textAlign: 'center', color: 'var(--text-muted)' }}>
              No differences found between branches
            </div>
          ) : (
            diff.schemas.map(schema => {
              const isSchemaExpanded = expandedSchemas.has(schema.schema_name);
              const isSchemaHighlighted = highlightedItem === `schema:${schema.schema_name}`;

              return (
                <div key={schema.schema_name}>
                  {/* Schema row */}
                  <div
                    id={`schema-${schema.schema_name}`}
                    style={{
                      display: 'flex',
                      alignItems: 'center',
                      padding: '10px 12px',
                      background: isSchemaHighlighted ? 'rgba(14, 165, 233, 0.2)' : getStatusBg(schema.status),
                      borderBottom: '1px solid var(--border-color)',
                      cursor: 'pointer',
                      userSelect: 'none',
                      transition: 'background 0.3s ease',
                      boxShadow: isSchemaHighlighted ? 'inset 0 0 0 2px #0ea5e9' : 'none',
                    }}
                    onClick={() => toggleSchema(schema.schema_name)}
                    onDoubleClick={(e) => { e.stopPropagation(); openSchemaModal(schema); }}
                  >
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px', flex: 1 }}>
                      {isSchemaExpanded ? (
                        <ChevronDown size={16} style={{ color: 'var(--text-muted)' }} />
                      ) : (
                        <ChevronRight size={16} style={{ color: 'var(--text-muted)' }} />
                      )}
                      {isSchemaExpanded ? (
                        <FolderOpen size={16} style={{ color: '#0ea5e9' }} />
                      ) : (
                        <Folder size={16} style={{ color: '#0ea5e9' }} />
                      )}
                      <span style={{ fontWeight: 600 }}>{schema.schema_name}</span>
                      {getStatusBadge(schema.status)}
                      <span style={{ fontSize: '12px', color: 'var(--text-muted)' }}>
                        ({schema.tables.length} tables)
                      </span>
                    </div>
                    <button
                      className="btn btn-ghost btn-sm"
                      onClick={(e) => { e.stopPropagation(); openSchemaModal(schema); }}
                      style={{ opacity: 0.6 }}
                    >
                      View Details
                    </button>
                  </div>

                  {/* Tables */}
                  {isSchemaExpanded && (
                    <div style={{ background: 'var(--bg-secondary)' }}>
                      {schema.tables.map(table => {
                        const tableKey = `${schema.schema_name}.${table.table_name}`;
                        const isTableExpanded = expandedTables.has(tableKey);
                        const changedCols = table.columns.filter(c => c.status !== 'unchanged');
                        const isTableHighlighted = highlightedItem === `table:${tableKey}`;

                        return (
                          <div key={table.table_name}>
                            {/* Table row */}
                            <div
                              id={`table-${schema.schema_name}-${table.table_name}`}
                              style={{
                                display: 'flex',
                                alignItems: 'center',
                                padding: '8px 12px 8px 36px',
                                background: isTableHighlighted ? 'rgba(139, 92, 246, 0.2)' : getStatusBg(table.status),
                                borderBottom: '1px solid var(--border-light)',
                                cursor: changedCols.length > 0 ? 'pointer' : 'default',
                                userSelect: 'none',
                                transition: 'background 0.3s ease',
                                boxShadow: isTableHighlighted ? 'inset 0 0 0 2px #8b5cf6' : 'none',
                              }}
                              onClick={() => changedCols.length > 0 && toggleTable(tableKey)}
                              onDoubleClick={(e) => { e.stopPropagation(); openTableModal(schema, table); }}
                            >
                              <div style={{ display: 'flex', alignItems: 'center', gap: '8px', flex: 1 }}>
                                {changedCols.length > 0 ? (
                                  isTableExpanded ? (
                                    <ChevronDown size={14} style={{ color: 'var(--text-muted)' }} />
                                  ) : (
                                    <ChevronRight size={14} style={{ color: 'var(--text-muted)' }} />
                                  )
                                ) : (
                                  <span style={{ width: 14 }} />
                                )}
                                <Table size={14} style={{ color: '#8b5cf6' }} />
                                <span style={{ fontWeight: 500 }}>{table.table_name}</span>
                                {getStatusBadge(table.status)}
                                {changedCols.length > 0 && (
                                  <span style={{ fontSize: '11px', color: 'var(--text-muted)' }}>
                                    ({changedCols.length} column changes)
                                  </span>
                                )}
                              </div>
                            </div>

                            {/* Columns */}
                            {isTableExpanded && changedCols.length > 0 && (
                              <div style={{ background: 'var(--bg-primary)' }}>
                                {changedCols.map(col => {
                                  const isColHighlighted = highlightedItem === `column:${schema.schema_name}.${table.table_name}.${col.column_name}`;
                                  return (
                                    <div
                                      key={col.column_name}
                                      id={`column-${schema.schema_name}-${table.table_name}-${col.column_name}`}
                                      style={{
                                        display: 'flex',
                                        alignItems: 'center',
                                        padding: '6px 12px 6px 72px',
                                        background: isColHighlighted ? 'rgba(100, 116, 139, 0.3)' : getStatusBg(col.status),
                                        borderBottom: '1px solid var(--border-light)',
                                        fontSize: '12px',
                                        transition: 'background 0.3s ease',
                                        boxShadow: isColHighlighted ? 'inset 0 0 0 2px #64748b' : 'none',
                                      }}
                                    >
                                      <div style={{ display: 'flex', alignItems: 'center', gap: '8px', flex: 1 }}>
                                        <Columns size={12} style={{ color: 'var(--text-muted)' }} />
                                        <span style={{
                                          fontWeight: 500,
                                          color: getStatusColor(col.status),
                                        }}>
                                          {col.column_name}
                                        </span>
                                        <span style={{
                                          fontFamily: 'monospace',
                                          fontSize: '11px',
                                          color: 'var(--text-muted)',
                                        }}>
                                          {col.status === 'modified' ? (
                                            <>{col.base_type} <ArrowRight size={10} style={{ verticalAlign: 'middle' }} /> {col.compare_type}</>
                                          ) : col.status === 'added' ? (
                                            col.compare_type
                                          ) : (
                                            col.base_type
                                          )}
                                        </span>
                                      </div>
                                      {getStatusBadge(col.status)}
                                    </div>
                                  );
                                })}
                              </div>
                            )}
                          </div>
                        );
                      })}
                    </div>
                  )}
                </div>
              );
            })
          )}
        </div>
      ) : (
        <div style={{
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'center',
          justifyContent: 'center',
          padding: compact ? '40px' : '60px',
          color: 'var(--text-muted)',
          background: 'var(--bg-secondary)',
          borderRadius: compact ? '0' : '8px',
          flex: compact ? 1 : 'none',
        }}>
          <GitCompare size={compact ? 32 : 48} style={{ opacity: 0.3 }} />
          <p style={{ marginTop: '12px', fontSize: compact ? '12px' : '14px' }}>Select two branches and click Compare</p>
        </div>
      )}

      {/* Hint - hide in compact mode */}
      {!compact && diff && diff.schemas.length > 0 && (
        <div style={{
          marginTop: '8px',
          fontSize: '11px',
          color: 'var(--text-muted)',
          textAlign: 'center',
        }}>
          Double-click on a schema or table to see side-by-side comparison
        </div>
      )}

      {/* Detail Modal */}
      {modalData && (
        <DetailModal
          type={modalData.type}
          title={modalData.title}
          baseBranch={baseBranch}
          compareBranch={compareBranch}
          items={modalData.items}
          onClose={() => setModalData(null)}
        />
      )}
    </div>
  );
}
