import React, { useState, useCallback, useRef, useEffect } from 'react';
import {
  Search,
  GitCompare,
  X,
  ChevronsUpDown,
  ChevronsDownUp,
  RefreshCw,
} from 'lucide-react';
import { SchemaTree } from './SchemaTree';
import type { SchemaTreeRef, DiffHighlight } from './SchemaTree';
import type { Branch, BranchDiffResponse, DiffStatus } from '../types';
import { branchesApi } from '../api';

// =============================================================================
// Types
// =============================================================================

interface SchemaBrowserProps {
  catalogId: string;
  branches: Branch[];
  currentBranch: string;
  height?: number;
  onSelectTable?: (schemaName: string, tableName: string) => void;
  onSelectColumn?: (schemaName: string, tableName: string, columnName: string) => void;
}

// =============================================================================
// Schema Browser Component
// =============================================================================

export function SchemaBrowser({
  catalogId,
  branches,
  currentBranch,
  height = 500,
  onSelectTable,
  onSelectColumn,
}: SchemaBrowserProps) {
  // State
  const [searchTerm, setSearchTerm] = useState('');
  const [baseBranch, setBaseBranch] = useState(currentBranch);
  const [compareBranch, setCompareBranch] = useState<string | null>(null);
  const [highlightDiff, setHighlightDiff] = useState(true);
  const [showDiffOnly, setShowDiffOnly] = useState(false);
  const [diffData, setDiffData] = useState<BranchDiffResponse | null>(null);
  const [diffLoading, setDiffLoading] = useState(false);
  const [diffHighlight, setDiffHighlight] = useState<DiffHighlight | null>(null);

  // Refs for tree instances
  const leftTreeRef = useRef<SchemaTreeRef | null>(null);
  const rightTreeRef = useRef<SchemaTreeRef | null>(null);

  // Update base branch when current branch changes
  useEffect(() => {
    setBaseBranch(currentBranch);
  }, [currentBranch]);

  // Fetch diff when compare branch changes
  useEffect(() => {
    if (compareBranch && compareBranch !== baseBranch) {
      fetchDiff();
    } else {
      setDiffData(null);
      setDiffHighlight(null);
    }
  }, [baseBranch, compareBranch]);

  // Build diff highlight map from diff data
  useEffect(() => {
    if (!diffData) {
      setDiffHighlight(null);
      return;
    }

    const nodeStatus = new Map<string, DiffStatus>();

    for (const schema of diffData.schemas) {
      // Schema level
      nodeStatus.set(`schema:${schema.schema_name}`, schema.status);

      for (const table of schema.tables) {
        // Table level
        nodeStatus.set(`table:${schema.schema_name}.${table.table_name}`, table.status);

        for (const column of table.columns) {
          // Column level
          nodeStatus.set(
            `column:${schema.schema_name}.${table.table_name}.${column.column_name}`,
            column.status
          );
        }
      }
    }

    setDiffHighlight({ nodeStatus });
  }, [diffData]);

  const fetchDiff = async () => {
    if (!compareBranch || compareBranch === baseBranch) return;

    setDiffLoading(true);
    try {
      const diff = await branchesApi.diff(catalogId, baseBranch, compareBranch);
      setDiffData(diff);
    } catch (err) {
      console.error('Failed to fetch diff:', err);
      setDiffData(null);
    } finally {
      setDiffLoading(false);
    }
  };

  const handleExpandAll = useCallback(() => {
    leftTreeRef.current?.openAll();
    if (compareBranch) {
      rightTreeRef.current?.openAll();
    }
  }, [compareBranch]);

  const handleCollapseAll = useCallback(() => {
    leftTreeRef.current?.closeAll();
    if (compareBranch) {
      rightTreeRef.current?.closeAll();
    }
  }, [compareBranch]);

  const handleClearCompare = () => {
    setCompareBranch(null);
    setDiffData(null);
    setDiffHighlight(null);
  };

  const isCompareMode = compareBranch !== null && compareBranch !== baseBranch;
  const treeHeight = height - 140; // Account for header and footer

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
      {/* Header Controls */}
      <div style={{
        padding: '12px',
        borderBottom: '1px solid var(--border)',
        backgroundColor: 'var(--bg-secondary)',
        display: 'flex',
        flexDirection: 'column',
        gap: 10,
      }}>
        {/* Search Bar */}
        <div style={{ position: 'relative' }}>
          <Search
            size={14}
            style={{
              position: 'absolute',
              left: 10,
              top: '50%',
              transform: 'translateY(-50%)',
              color: 'var(--text-muted)'
            }}
          />
          <input
            type="text"
            className="form-input"
            placeholder="Search schemas, tables, columns..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            style={{
              paddingLeft: 32,
              width: '100%',
              height: 32,
              fontSize: 12,
              backgroundColor: 'var(--bg)',
              border: '1px solid var(--border)',
              borderRadius: 6,
            }}
          />
        </div>

        {/* Branch Selectors */}
        <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
            <label style={{ fontSize: 12, color: 'var(--text-muted)' }}>Base:</label>
            <select
              value={baseBranch}
              onChange={(e) => setBaseBranch(e.target.value)}
              style={{
                padding: '4px 8px',
                fontSize: 12,
                borderRadius: 4,
                border: '1px solid var(--border)',
                backgroundColor: 'var(--bg)',
                color: 'var(--text)',
                minWidth: 120,
              }}
            >
              {branches.map(b => (
                <option key={b.branch_name} value={b.branch_name}>
                  {b.branch_name}
                </option>
              ))}
            </select>
          </div>

          <GitCompare size={16} style={{ color: 'var(--text-muted)' }} />

          <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
            <label style={{ fontSize: 12, color: 'var(--text-muted)' }}>Compare:</label>
            <select
              value={compareBranch || ''}
              onChange={(e) => setCompareBranch(e.target.value || null)}
              style={{
                padding: '4px 8px',
                fontSize: 12,
                borderRadius: 4,
                border: '1px solid var(--border)',
                backgroundColor: 'var(--bg)',
                color: 'var(--text)',
                minWidth: 120,
              }}
            >
              <option value="">Select branch...</option>
              {branches.filter(b => b.branch_name !== baseBranch).map(b => (
                <option key={b.branch_name} value={b.branch_name}>
                  {b.branch_name}
                </option>
              ))}
            </select>
            {isCompareMode && (
              <button
                onClick={handleClearCompare}
                title="Clear comparison"
                style={{
                  padding: '4px',
                  border: 'none',
                  background: 'none',
                  cursor: 'pointer',
                  color: 'var(--text-muted)',
                  display: 'flex',
                  alignItems: 'center',
                }}
              >
                <X size={14} />
              </button>
            )}
          </div>

          {diffLoading && (
            <RefreshCw size={14} className="icon-spin" style={{ color: 'var(--text-muted)' }} />
          )}
        </div>

        {/* Options and Actions */}
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 16 }}>
            {isCompareMode && (
              <>
                <label style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 12, cursor: 'pointer' }}>
                  <input
                    type="checkbox"
                    checked={highlightDiff}
                    onChange={(e) => setHighlightDiff(e.target.checked)}
                  />
                  Highlight differences
                </label>
                <label style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 12, cursor: 'pointer' }}>
                  <input
                    type="checkbox"
                    checked={showDiffOnly}
                    onChange={(e) => setShowDiffOnly(e.target.checked)}
                  />
                  Show diff only
                </label>
              </>
            )}
          </div>

          <div style={{ display: 'flex', gap: 4 }}>
            <button
              onClick={handleExpandAll}
              title="Expand All"
              style={{
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                width: 28,
                height: 28,
                border: '1px solid var(--border)',
                borderRadius: 4,
                backgroundColor: 'var(--bg)',
                cursor: 'pointer',
                color: 'var(--text-muted)',
              }}
            >
              <ChevronsUpDown size={14} />
            </button>
            <button
              onClick={handleCollapseAll}
              title="Collapse All"
              style={{
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                width: 28,
                height: 28,
                border: '1px solid var(--border)',
                borderRadius: 4,
                backgroundColor: 'var(--bg)',
                cursor: 'pointer',
                color: 'var(--text-muted)',
              }}
            >
              <ChevronsDownUp size={14} />
            </button>
          </div>
        </div>
      </div>

      {/* Tree View(s) */}
      <div style={{
        flex: 1,
        display: 'flex',
        overflow: 'hidden',
      }}>
        {/* Left Tree (Base Branch) */}
        <div style={{
          flex: 1,
          borderRight: isCompareMode ? '1px solid var(--border)' : 'none',
          display: 'flex',
          flexDirection: 'column',
        }}>
          {isCompareMode && (
            <div style={{
              padding: '6px 12px',
              backgroundColor: 'var(--bg-secondary)',
              borderBottom: '1px solid var(--border)',
              fontSize: 12,
              fontWeight: 500,
              color: 'var(--text)',
            }}>
              {baseBranch}
            </div>
          )}
          <SchemaTree
            ref={leftTreeRef}
            catalogId={catalogId}
            branch={baseBranch}
            height={treeHeight}
            searchTerm={searchTerm}
            diffHighlight={highlightDiff && isCompareMode ? diffHighlight : null}
            showDiffOnly={showDiffOnly && isCompareMode}
            diffSide="base"
            onSelectTable={onSelectTable}
            onSelectColumn={onSelectColumn}
            hideControls
          />
        </div>

        {/* Right Tree (Compare Branch) */}
        {isCompareMode && (
          <div style={{
            flex: 1,
            display: 'flex',
            flexDirection: 'column',
          }}>
            <div style={{
              padding: '6px 12px',
              backgroundColor: 'var(--bg-secondary)',
              borderBottom: '1px solid var(--border)',
              fontSize: 12,
              fontWeight: 500,
              color: 'var(--text)',
            }}>
              {compareBranch}
            </div>
            <SchemaTree
              ref={rightTreeRef}
              catalogId={catalogId}
              branch={compareBranch}
              height={treeHeight}
              searchTerm={searchTerm}
              diffHighlight={highlightDiff ? diffHighlight : null}
              showDiffOnly={showDiffOnly}
              diffSide="compare"
              onSelectTable={onSelectTable}
              onSelectColumn={onSelectColumn}
              hideControls
            />
          </div>
        )}
      </div>

      {/* Footer Stats */}
      <div style={{
        padding: '8px 12px',
        borderTop: '1px solid var(--border)',
        backgroundColor: 'var(--bg-secondary)',
        fontSize: 11,
        color: 'var(--text-muted)',
        display: 'flex',
        gap: 12,
        flexWrap: 'wrap',
      }}>
        {isCompareMode && diffData ? (
          <>
            {diffData.summary.schemas_added > 0 && (
              <span style={{ color: '#22c55e' }}>+{diffData.summary.schemas_added} schemas</span>
            )}
            {diffData.summary.schemas_removed > 0 && (
              <span style={{ color: '#ef4444' }}>-{diffData.summary.schemas_removed} schemas</span>
            )}
            {diffData.summary.tables_added > 0 && (
              <span style={{ color: '#22c55e' }}>+{diffData.summary.tables_added} tables</span>
            )}
            {diffData.summary.tables_removed > 0 && (
              <span style={{ color: '#ef4444' }}>-{diffData.summary.tables_removed} tables</span>
            )}
            {diffData.summary.tables_modified > 0 && (
              <span style={{ color: '#f59e0b' }}>~{diffData.summary.tables_modified} tables</span>
            )}
            {diffData.summary.columns_added > 0 && (
              <span style={{ color: '#22c55e' }}>+{diffData.summary.columns_added} cols</span>
            )}
            {diffData.summary.columns_removed > 0 && (
              <span style={{ color: '#ef4444' }}>-{diffData.summary.columns_removed} cols</span>
            )}
            {Object.values(diffData.summary).every(v => v === 0) && (
              <span>No differences</span>
            )}
          </>
        ) : (
          <span>Select a branch to compare</span>
        )}
      </div>
    </div>
  );
}

export default SchemaBrowser;
