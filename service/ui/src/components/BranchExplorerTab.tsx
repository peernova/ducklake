import React, { useState, useCallback, useMemo, useEffect } from 'react';
import {
  GitBranch,
  Folder,
  Table,
  Columns,
  ChevronRight,
  ChevronDown,
  Search,
  X,
  FolderOpen,
  RefreshCw,
  Database,
} from 'lucide-react';
import { schemasApi } from '../api';
import { BranchDiffTab } from './BranchDiffTab';
import type { Branch, SchemaInfo, TableInfo } from '../types';

const CONTAINER_HEIGHT = 500;

interface BranchExplorerTabProps {
  catalogId: string;
  branches: Branch[];
  currentBranch: string;
}

// ============================================================================
// Schema Browser Component (Left Side)
// ============================================================================

interface SchemaBrowserPanelProps {
  catalogId: string;
  branch: string;
  branches: Branch[];
  onBranchChange: (branch: string) => void;
}

interface SchemaWithTables extends SchemaInfo {
  tables?: TableInfo[];
}

function SchemaBrowserPanel({ catalogId, branch, branches, onBranchChange }: SchemaBrowserPanelProps) {
  const [schemas, setSchemas] = useState<SchemaWithTables[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [searchQuery, setSearchQuery] = useState('');
  const [expandedSchemas, setExpandedSchemas] = useState<Set<string>>(new Set());
  const [expandedTables, setExpandedTables] = useState<Set<string>>(new Set());

  // Branch selector state
  const [branchDropdownOpen, setBranchDropdownOpen] = useState(false);
  const [branchSearch, setBranchSearch] = useState('');
  const branchDropdownRef = React.useRef<HTMLDivElement>(null);

  // Close branch dropdown when clicking outside
  useEffect(() => {
    const handleClickOutside = (e: MouseEvent) => {
      if (branchDropdownRef.current && !branchDropdownRef.current.contains(e.target as Node)) {
        setBranchDropdownOpen(false);
      }
    };
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  const fetchSchemas = useCallback(async () => {
    if (!branch) return;
    setLoading(true);
    setError(null);
    try {
      const schemasResult = await schemasApi.list(catalogId, branch);
      // Handle both response formats: array or { schemas: [...] }
      const schemasList = schemasResult.schemas || schemasResult;
      const schemasArray = Array.isArray(schemasList) ? schemasList : [];
      const schemasWithTables: SchemaWithTables[] = [];

      for (const schema of schemasArray) {
        try {
          const tablesResult = await schemasApi.listTables(catalogId, schema.schema_name, branch);
          // Handle both response formats: array or { tables: [...] }
          const tablesList = tablesResult.tables || tablesResult;
          const tablesArray = Array.isArray(tablesList) ? tablesList : [];
          schemasWithTables.push({
            ...schema,
            tables: tablesArray,
          });
        } catch {
          schemasWithTables.push({ ...schema, tables: [] });
        }
      }

      setSchemas(schemasWithTables);
      if (schemasWithTables.length > 0) {
        setExpandedSchemas(new Set([schemasWithTables[0].schema_name]));
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to fetch schemas');
    } finally {
      setLoading(false);
    }
  }, [catalogId, branch]);

  useEffect(() => {
    fetchSchemas();
  }, [fetchSchemas]);

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

  const filteredSchemas = useMemo(() => {
    if (!searchQuery.trim()) return schemas;
    const query = searchQuery.toLowerCase();
    return schemas
      .map(schema => {
        const schemaMatches = schema.schema_name.toLowerCase().includes(query);
        const filteredTables = (schema.tables || [])
          .map(table => {
            const tableMatches = table.table_name.toLowerCase().includes(query);
            const filteredColumns = (table.columns || []).filter(col =>
              col.name.toLowerCase().includes(query)
            );
            if (tableMatches || filteredColumns.length > 0) {
              return { ...table, columns: tableMatches ? table.columns : filteredColumns };
            }
            return null;
          })
          .filter(Boolean) as TableInfo[];

        if (schemaMatches || filteredTables.length > 0) {
          return { ...schema, tables: schemaMatches ? schema.tables : filteredTables };
        }
        return null;
      })
      .filter(Boolean) as SchemaWithTables[];
  }, [schemas, searchQuery]);

  const activeBranches = branches.filter(b => b.status === 'active');
  const filteredBranches = activeBranches.filter(b =>
    b.branch_name.toLowerCase().includes(branchSearch.toLowerCase())
  );

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: CONTAINER_HEIGHT }}>
      {/* Branch Selector */}
      <div style={{
        padding: '12px',
        borderBottom: '1px solid var(--border-color)',
        background: 'var(--bg-secondary)',
      }}>
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '8px' }}>
          <label style={{ fontSize: '11px', color: 'var(--text-muted)', textTransform: 'uppercase', fontWeight: 600 }}>
            Browse Branch
          </label>
          <button
            className="btn btn-ghost btn-sm"
            onClick={fetchSchemas}
            disabled={loading}
            title="Refresh"
            style={{ padding: '4px' }}
          >
            <RefreshCw size={14} className={loading ? 'spin' : ''} />
          </button>
        </div>

        {/* Custom searchable dropdown */}
        <div ref={branchDropdownRef} style={{ position: 'relative' }}>
          <div
            onClick={() => setBranchDropdownOpen(!branchDropdownOpen)}
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
              {branch}
            </span>
            <ChevronDown size={14} style={{ color: 'var(--text-muted)', flexShrink: 0, transform: branchDropdownOpen ? 'rotate(180deg)' : 'none', transition: 'transform 0.15s ease' }} />
          </div>

          {/* Dropdown */}
          {branchDropdownOpen && (
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
              maxHeight: '280px',
              display: 'flex',
              flexDirection: 'column',
            }}>
              {/* Search input */}
              <div style={{ padding: '8px', borderBottom: '1px solid var(--border-color)' }}>
                <div style={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: '6px',
                  padding: '6px 8px',
                  background: 'var(--bg-secondary)',
                  borderRadius: '4px',
                }}>
                  <Search size={12} style={{ color: 'var(--text-muted)' }} />
                  <input
                    type="text"
                    placeholder="Search branches..."
                    value={branchSearch}
                    onChange={(e) => setBranchSearch(e.target.value)}
                    onClick={(e) => e.stopPropagation()}
                    autoFocus
                    style={{
                      flex: 1,
                      background: 'transparent',
                      border: 'none',
                      outline: 'none',
                      color: 'var(--text-primary)',
                      fontSize: '12px',
                    }}
                  />
                  {branchSearch && (
                    <button
                      onClick={(e) => { e.stopPropagation(); setBranchSearch(''); }}
                      style={{ background: 'none', border: 'none', padding: '2px', cursor: 'pointer', color: 'var(--text-muted)' }}
                    >
                      <X size={10} />
                    </button>
                  )}
                </div>
              </div>

              {/* Branch list */}
              <div style={{ overflowY: 'auto', flex: 1 }}>
                {filteredBranches.length === 0 ? (
                  <div style={{ padding: '12px', textAlign: 'center', color: 'var(--text-muted)', fontSize: '12px' }}>
                    No branches found
                  </div>
                ) : (
                  filteredBranches.map(b => (
                    <div
                      key={b.branch_id}
                      onClick={() => {
                        onBranchChange(b.branch_name);
                        setBranchDropdownOpen(false);
                        setBranchSearch('');
                      }}
                      style={{
                        display: 'flex',
                        alignItems: 'center',
                        gap: '8px',
                        padding: '8px 12px',
                        cursor: 'pointer',
                        fontSize: '12px',
                        background: b.branch_name === branch ? 'var(--accent-secondary)' : 'transparent',
                        borderLeft: b.branch_name === branch ? '2px solid var(--accent-primary)' : '2px solid transparent',
                      }}
                      onMouseEnter={(e) => {
                        if (b.branch_name !== branch) e.currentTarget.style.background = 'var(--bg-secondary)';
                      }}
                      onMouseLeave={(e) => {
                        if (b.branch_name !== branch) e.currentTarget.style.background = 'transparent';
                      }}
                    >
                      <GitBranch size={12} style={{ color: b.branch_name === 'main' ? 'var(--accent-primary)' : 'var(--text-muted)' }} />
                      <span style={{ flex: 1, fontWeight: b.branch_name === branch ? 500 : 400 }}>{b.branch_name}</span>
                      {b.branch_name === branch && (
                        <span style={{ fontSize: '10px', color: 'var(--accent-primary)' }}>current</span>
                      )}
                    </div>
                  ))
                )}
              </div>

              {/* Footer with count */}
              <div style={{
                padding: '6px 12px',
                borderTop: '1px solid var(--border-color)',
                background: 'var(--bg-secondary)',
                fontSize: '10px',
                color: 'var(--text-muted)',
              }}>
                {filteredBranches.length} of {activeBranches.length} branches
              </div>
            </div>
          )}
        </div>
      </div>

      {/* Search */}
      <div style={{ padding: '12px', borderBottom: '1px solid var(--border-color)' }}>
        <div style={{
          display: 'flex',
          alignItems: 'center',
          gap: '8px',
          padding: '8px 10px',
          background: 'var(--bg-primary)',
          border: '1px solid var(--border-color)',
          borderRadius: '6px',
        }}>
          <Search size={14} style={{ color: 'var(--text-muted)' }} />
          <input
            type="text"
            placeholder="Search schemas, tables, columns..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            style={{
              flex: 1,
              background: 'transparent',
              border: 'none',
              outline: 'none',
              color: 'var(--text-primary)',
              fontSize: '12px',
            }}
          />
          {searchQuery && (
            <button
              onClick={() => setSearchQuery('')}
              style={{ background: 'none', border: 'none', padding: '2px', cursor: 'pointer', color: 'var(--text-muted)' }}
            >
              <X size={12} />
            </button>
          )}
        </div>
      </div>

      {/* Error */}
      {error && (
        <div style={{ padding: '12px', color: '#ef4444', fontSize: '12px', background: 'rgba(239, 68, 68, 0.1)' }}>
          {error}
        </div>
      )}

      {/* Schema Tree */}
      <div style={{ flex: 1, overflowY: 'auto', padding: '8px 0' }}>
        {loading ? (
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', padding: '40px', color: 'var(--text-muted)' }}>
            <RefreshCw size={20} className="spin" />
          </div>
        ) : filteredSchemas.length === 0 ? (
          <div style={{ padding: '20px', textAlign: 'center', color: 'var(--text-muted)', fontSize: '12px' }}>
            {searchQuery ? 'No results found' : 'No schemas found'}
          </div>
        ) : (
          filteredSchemas.map(schema => {
            const isExpanded = expandedSchemas.has(schema.schema_name);
            return (
              <div key={schema.schema_name}>
                <div
                  onClick={() => toggleSchema(schema.schema_name)}
                  style={{
                    display: 'flex',
                    alignItems: 'center',
                    gap: '6px',
                    padding: '6px 12px',
                    cursor: 'pointer',
                    fontSize: '12px',
                    userSelect: 'none',
                  }}
                  onMouseEnter={(e) => e.currentTarget.style.background = 'var(--bg-secondary)'}
                  onMouseLeave={(e) => e.currentTarget.style.background = 'transparent'}
                >
                  {isExpanded ? <ChevronDown size={12} /> : <ChevronRight size={12} />}
                  {isExpanded ? <FolderOpen size={14} style={{ color: '#0ea5e9' }} /> : <Folder size={14} style={{ color: '#0ea5e9' }} />}
                  <span style={{ fontWeight: 500 }}>{schema.schema_name}</span>
                  <span style={{ color: 'var(--text-muted)', fontSize: '10px' }}>
                    ({schema.tables?.length || 0})
                  </span>
                </div>

                {isExpanded && schema.tables?.map(table => {
                  const tableKey = `${schema.schema_name}.${table.table_name}`;
                  const isTableExpanded = expandedTables.has(tableKey);
                  return (
                    <div key={table.table_name}>
                      <div
                        onClick={() => toggleTable(tableKey)}
                        style={{
                          display: 'flex',
                          alignItems: 'center',
                          gap: '6px',
                          padding: '5px 12px 5px 32px',
                          cursor: 'pointer',
                          fontSize: '12px',
                          userSelect: 'none',
                        }}
                        onMouseEnter={(e) => e.currentTarget.style.background = 'var(--bg-secondary)'}
                        onMouseLeave={(e) => e.currentTarget.style.background = 'transparent'}
                      >
                        {(table.columns?.length || 0) > 0 ? (
                          isTableExpanded ? <ChevronDown size={10} /> : <ChevronRight size={10} />
                        ) : <span style={{ width: 10 }} />}
                        <Table size={12} style={{ color: '#8b5cf6' }} />
                        <span>{table.table_name}</span>
                        {table.columns && (
                          <span style={{ color: 'var(--text-muted)', fontSize: '10px' }}>
                            ({table.columns.length} cols)
                          </span>
                        )}
                      </div>

                      {isTableExpanded && table.columns?.map(col => (
                        <div
                          key={col.name}
                          style={{
                            display: 'flex',
                            alignItems: 'center',
                            gap: '6px',
                            padding: '4px 12px 4px 56px',
                            fontSize: '11px',
                            color: 'var(--text-secondary)',
                          }}
                        >
                          <Columns size={10} style={{ color: 'var(--text-muted)' }} />
                          <span>{col.name}</span>
                          <span style={{ color: 'var(--text-muted)', fontFamily: 'monospace', fontSize: '10px' }}>
                            {col.type}
                          </span>
                        </div>
                      ))}
                    </div>
                  );
                })}
              </div>
            );
          })
        )}
      </div>

      {/* Footer Stats */}
      <div style={{
        padding: '8px 12px',
        borderTop: '1px solid var(--border-color)',
        background: 'var(--bg-secondary)',
        fontSize: '11px',
        color: 'var(--text-muted)',
        display: 'flex',
        gap: '12px',
      }}>
        <span><Database size={10} style={{ verticalAlign: 'middle' }} /> {schemas.length} schemas</span>
        <span><Table size={10} style={{ verticalAlign: 'middle' }} /> {schemas.reduce((acc, s) => acc + (s.tables?.length || 0), 0)} tables</span>
      </div>
    </div>
  );
}

// ============================================================================
// Main Component - Split View
// ============================================================================

export function BranchExplorerTab({ catalogId, branches, currentBranch }: BranchExplorerTabProps) {
  const [browseBranch, setBrowseBranch] = useState(currentBranch);
  const [leftPanelWidth, setLeftPanelWidth] = useState(25); // percentage
  const [isDragging, setIsDragging] = useState(false);
  const containerRef = React.useRef<HTMLDivElement>(null);

  // Handle drag events
  const handleMouseDown = (e: React.MouseEvent) => {
    e.preventDefault();
    setIsDragging(true);
  };

  React.useEffect(() => {
    if (!isDragging) return;

    const handleMouseMove = (e: MouseEvent) => {
      if (!containerRef.current) return;
      const rect = containerRef.current.getBoundingClientRect();
      const newWidth = ((e.clientX - rect.left) / rect.width) * 100;
      // Clamp between 20% and 80%
      setLeftPanelWidth(Math.min(80, Math.max(20, newWidth)));
    };

    const handleMouseUp = () => {
      setIsDragging(false);
    };

    document.addEventListener('mousemove', handleMouseMove);
    document.addEventListener('mouseup', handleMouseUp);
    return () => {
      document.removeEventListener('mousemove', handleMouseMove);
      document.removeEventListener('mouseup', handleMouseUp);
    };
  }, [isDragging]);

  return (
    <div style={{ display: 'flex', flexDirection: 'column' }}>
      {/* Header */}
      <div className="tab-header">
        <div className="tab-header-left">
          <h3 className="section-title" style={{ margin: 0 }}>Schema Explorer</h3>
          <span className="text-muted text-sm">Browse branch schemas and compare differences</span>
        </div>
      </div>

      {/* Split View */}
      <div
        ref={containerRef}
        style={{
          display: 'flex',
          border: '1px solid var(--border-color)',
          borderRadius: '8px',
          overflow: 'hidden',
          height: CONTAINER_HEIGHT,
          cursor: isDragging ? 'col-resize' : 'default',
          userSelect: isDragging ? 'none' : 'auto',
        }}
      >
        {/* Left Panel - Schema Browser */}
        <div style={{
          width: `${leftPanelWidth}%`,
          background: 'var(--bg-primary)',
          overflow: 'auto',
          flexShrink: 0,
        }}>
          <SchemaBrowserPanel
            catalogId={catalogId}
            branch={browseBranch}
            branches={branches}
            onBranchChange={setBrowseBranch}
          />
        </div>

        {/* Draggable Divider */}
        <div
          onMouseDown={handleMouseDown}
          style={{
            width: '6px',
            background: isDragging ? 'var(--accent-primary)' : 'var(--border-color)',
            cursor: 'col-resize',
            flexShrink: 0,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            transition: 'background 0.15s ease',
          }}
          onMouseEnter={(e) => !isDragging && (e.currentTarget.style.background = 'var(--accent-secondary)')}
          onMouseLeave={(e) => !isDragging && (e.currentTarget.style.background = 'var(--border-color)')}
        >
          <div style={{
            width: '2px',
            height: '32px',
            background: isDragging ? '#fff' : 'var(--text-muted)',
            borderRadius: '1px',
            opacity: 0.5,
          }} />
        </div>

        {/* Right Panel - Diff (embedded BranchDiffTab) */}
        <div style={{
          flex: 1,
          background: 'var(--bg-primary)',
          overflow: 'auto',
          height: CONTAINER_HEIGHT,
          minWidth: 0,
        }}>
          <BranchDiffTab
            catalogId={catalogId}
            branches={branches}
            currentBranch={currentBranch}
            compact={true}
          />
        </div>
      </div>
    </div>
  );
}
