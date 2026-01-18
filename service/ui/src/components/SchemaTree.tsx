import React, { useState, useCallback, useRef, useEffect, forwardRef, useImperativeHandle, createContext, useContext } from 'react';
import { Tree } from 'react-arborist';
import type { NodeRendererProps, TreeApi } from 'react-arborist';
import {
  Folder,
  FolderOpen,
  Table,
  Key,
  Type,
  ChevronRight,
  ChevronDown,
  Search,
  Database,
  Loader2,
  ChevronsUpDown,
  ChevronsDownUp,
  Plus,
  Minus,
  PenLine,
} from 'lucide-react';
import { schemasApi } from '../api';
import type { SchemaInfo, TableInfo, ColumnInfo, DiffStatus } from '../types';

// =============================================================================
// Types
// =============================================================================

type NodeType = 'schema' | 'table' | 'column';

interface TreeNodeData {
  id: string;
  name: string;
  nodeType: NodeType;
  // Schema-specific
  schemaName?: string;
  // Table-specific
  tableName?: string;
  columnCount?: number;
  // Column-specific
  columnType?: string;
  nullable?: boolean;
  isPrimaryKey?: boolean;
  // Children (lazy loaded)
  children?: TreeNodeData[];
  // Loading state
  isLoading?: boolean;
  isLoaded?: boolean;
  // Diff status
  diffStatus?: DiffStatus;
}

export interface DiffHighlight {
  nodeStatus: Map<string, DiffStatus>;
}

interface SchemaTreeProps {
  catalogId: string;
  branch?: string;
  height?: number;
  searchTerm?: string; // Controlled from parent
  diffHighlight?: DiffHighlight | null;
  showDiffOnly?: boolean;
  diffSide?: 'base' | 'compare';
  hideControls?: boolean;
  onSelectTable?: (schemaName: string, tableName: string) => void;
  onSelectColumn?: (schemaName: string, tableName: string, columnName: string) => void;
}

export interface SchemaTreeRef {
  openAll: () => void;
  closeAll: () => void;
  loadAllTables: () => Promise<void>;
}

// =============================================================================
// Diff Context
// =============================================================================

interface DiffContextValue {
  diffHighlight: DiffHighlight | null;
  diffSide: 'base' | 'compare' | undefined;
}

const DiffContext = createContext<DiffContextValue>({ diffHighlight: null, diffSide: undefined });

// =============================================================================
// Node Renderer
// =============================================================================

function Node({ node, style, dragHandle }: NodeRendererProps<TreeNodeData>) {
  const data = node.data;
  const indent = node.level * 20;
  const { diffHighlight, diffSide } = useContext(DiffContext);

  // Get diff status for this node
  const diffStatus = diffHighlight?.nodeStatus.get(data.id);

  // Debug logging for columns
  if (data.nodeType === 'column' && diffHighlight && data.name === 'risk_score') {
    console.log('risk_score lookup:', {
      nodeId: data.id,
      diffStatus,
      diffSide,
      mapKeys: Array.from(diffHighlight.nodeStatus.keys()).filter(k => k.includes('risk_score'))
    });
  }

  const getDiffColor = () => {
    if (!diffStatus || diffStatus === 'unchanged') return null;
    // For base side: removed items show red, added items don't exist
    // For compare side: added items show green, removed items don't exist
    if (diffSide === 'base' && diffStatus === 'added') return null;
    if (diffSide === 'compare' && diffStatus === 'removed') return null;

    switch (diffStatus) {
      case 'added': return '#22c55e';
      case 'removed': return '#ef4444';
      case 'modified': return '#f59e0b';
      default: return null;
    }
  };

  const getDiffBgColor = () => {
    const color = getDiffColor();
    if (!color) return 'transparent';
    return `${color}15`; // 15 = ~9% opacity in hex
  };

  const getDiffIcon = () => {
    if (!diffStatus || diffStatus === 'unchanged') return null;
    if (diffSide === 'base' && diffStatus === 'added') return null;
    if (diffSide === 'compare' && diffStatus === 'removed') return null;

    switch (diffStatus) {
      case 'added': return <Plus size={12} style={{ color: '#22c55e' }} />;
      case 'removed': return <Minus size={12} style={{ color: '#ef4444' }} />;
      case 'modified': return <PenLine size={12} style={{ color: '#f59e0b' }} />;
      default: return null;
    }
  };

  const getIcon = () => {
    if (data.isLoading) {
      return <Loader2 size={16} className="icon-spin" style={{ color: 'var(--text-muted)' }} />;
    }

    switch (data.nodeType) {
      case 'schema':
        return node.isOpen ? (
          <FolderOpen size={16} style={{ color: '#f59e0b' }} />
        ) : (
          <Folder size={16} style={{ color: '#f59e0b' }} />
        );
      case 'table':
        return <Table size={16} style={{ color: '#3b82f6' }} />;
      case 'column':
        return data.isPrimaryKey ? (
          <Key size={16} style={{ color: '#eab308' }} />
        ) : (
          <Type size={16} style={{ color: '#6b7280' }} />
        );
      default:
        return <Database size={16} />;
    }
  };

  const getChevron = () => {
    if (data.nodeType === 'column') return null;
    if (!node.children || node.children.length === 0) {
      if (data.isLoaded) return <span style={{ width: 16 }} />;
      return <ChevronRight size={16} style={{ color: 'var(--text-muted)' }} />;
    }
    return node.isOpen ? (
      <ChevronDown size={16} style={{ color: 'var(--text-muted)' }} />
    ) : (
      <ChevronRight size={16} style={{ color: 'var(--text-muted)' }} />
    );
  };

  const handleClick = (e: React.MouseEvent) => {
    e.stopPropagation();
    if (data.nodeType !== 'column') {
      node.toggle();
    }
    node.select();
  };

  const diffColor = getDiffColor();

  return (
    <div
      ref={dragHandle}
      style={{
        ...style,
        paddingLeft: indent,
        display: 'flex',
        alignItems: 'center',
        gap: 6,
        height: 32,
        cursor: 'pointer',
        backgroundColor: node.isSelected ? 'rgba(59, 130, 246, 0.1)' : getDiffBgColor(),
        borderRadius: 4,
        padding: '4px 8px',
        marginLeft: indent,
        borderLeft: diffColor ? `3px solid ${diffColor}` : 'none',
      }}
      onClick={handleClick}
      onDoubleClick={() => data.nodeType !== 'column' && node.toggle()}
    >
      <span style={{ display: 'flex', alignItems: 'center', width: 16 }}>
        {getChevron()}
      </span>
      <span style={{ display: 'flex', alignItems: 'center' }}>
        {getIcon()}
      </span>
      <span style={{
        fontWeight: data.nodeType === 'schema' ? 600 : 400,
        color: diffColor || (node.isSelected ? 'var(--primary)' : 'var(--text)'),
        fontSize: data.nodeType === 'column' ? 12 : 13,
      }}>
        {data.name}
      </span>
      {getDiffIcon()}
      {data.nodeType === 'column' && data.columnType && (
        <span style={{
          color: 'var(--text-muted)',
          fontSize: 11,
          marginLeft: 4,
          padding: '1px 6px',
          backgroundColor: 'var(--bg-secondary)',
          borderRadius: 4,
        }}>
          {data.columnType}
        </span>
      )}
      {data.nodeType === 'column' && data.nullable && (
        <span style={{
          color: 'var(--text-muted)',
          fontSize: 10,
          marginLeft: 4,
        }}>
          NULL
        </span>
      )}
      {data.nodeType === 'table' && data.columnCount !== undefined && (
        <span style={{
          color: 'var(--text-muted)',
          fontSize: 11,
          marginLeft: 'auto',
        }}>
          {data.columnCount} cols
        </span>
      )}
    </div>
  );
}

// =============================================================================
// Schema Tree Component
// =============================================================================

export const SchemaTree = forwardRef<SchemaTreeRef, SchemaTreeProps>(function SchemaTree({
  catalogId,
  branch,
  height = 500,
  searchTerm: controlledSearchTerm,
  diffHighlight = null,
  showDiffOnly = false,
  diffSide,
  hideControls = false,
  onSelectTable,
  onSelectColumn
}, ref) {
  const [treeData, setTreeData] = useState<TreeNodeData[]>([]);
  const [internalSearchTerm, setInternalSearchTerm] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [initialLoadDone, setInitialLoadDone] = useState(false);
  const [allExpanded, setAllExpanded] = useState(false);
  const treeRef = useRef<TreeApi<TreeNodeData> | null>(null);

  // Use controlled search term if provided, otherwise use internal state
  const searchTerm = controlledSearchTerm !== undefined ? controlledSearchTerm : internalSearchTerm;

  // Load schemas on mount
  useEffect(() => {
    loadSchemas();
    setAllExpanded(false);
  }, [catalogId, branch]);

  const loadSchemas = async () => {
    setIsLoading(true);
    setError(null);
    try {
      const response = await schemasApi.list(catalogId, branch);
      const schemas = response.schemas || response;
      const schemaNodes: TreeNodeData[] = (Array.isArray(schemas) ? schemas : []).map((schema: SchemaInfo) => ({
        id: `schema:${schema.schema_name}`,
        name: schema.schema_name,
        nodeType: 'schema' as NodeType,
        schemaName: schema.schema_name,
        children: [], // Will be lazy loaded
        isLoaded: false,
      }));
      setTreeData(schemaNodes);
      setInitialLoadDone(true);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load schemas');
    } finally {
      setIsLoading(false);
    }
  };

  const loadTablesForSchema = async (schemaName: string): Promise<TreeNodeData[]> => {
    try {
      const response = await schemasApi.listTables(catalogId, schemaName, branch);
      const tables = response.tables || response;
      return (Array.isArray(tables) ? tables : []).map((table: TableInfo) => ({
        id: `table:${schemaName}.${table.table_name}`,
        name: table.table_name,
        nodeType: 'table' as NodeType,
        schemaName: schemaName,
        tableName: table.table_name,
        columnCount: table.columns?.length || 0,
        children: table.columns?.map((col: ColumnInfo) => ({
          id: `column:${schemaName}.${table.table_name}.${col.name}`,
          name: col.name,
          nodeType: 'column' as NodeType,
          schemaName: schemaName,
          tableName: table.table_name,
          columnType: col.type,
          nullable: col.nullable,
          isPrimaryKey: col.name === 'id', // Simple heuristic
        })) || [],
        isLoaded: true,
      }));
    } catch (err) {
      console.error('Failed to load tables for schema:', schemaName, err);
      return [];
    }
  };

  // Handle node toggle (lazy load)
  const handleToggle = useCallback(async (id: string) => {
    const parts = id.split(':');
    if (parts[0] !== 'schema') return;

    const schemaName = parts[1];

    // Check if already loaded
    const schemaNode = treeData.find(n => n.id === id);
    if (schemaNode?.isLoaded) return;

    // Mark as loading
    setTreeData(prev => prev.map(node =>
      node.id === id ? { ...node, isLoading: true } : node
    ));

    // Load tables
    const tableNodes = await loadTablesForSchema(schemaName);

    // Update tree with loaded tables
    setTreeData(prev => prev.map(node =>
      node.id === id
        ? { ...node, children: tableNodes, isLoaded: true, isLoading: false }
        : node
    ));
  }, [catalogId, branch, treeData]);

  // Load all tables for all schemas (for auto-expand)
  const loadAllTables = useCallback(async () => {
    const updatedNodes = await Promise.all(
      treeData.map(async (schemaNode) => {
        if (schemaNode.isLoaded) return schemaNode;
        const tableNodes = await loadTablesForSchema(schemaNode.schemaName!);
        return { ...schemaNode, children: tableNodes, isLoaded: true };
      })
    );
    setTreeData(updatedNodes);
    return updatedNodes;
  }, [treeData]);

  // Auto-expand all if total tables < 200
  useEffect(() => {
    const autoExpandAll = async () => {
      if (!initialLoadDone || treeData.length === 0 || allExpanded) return;

      // First, load all tables to count them
      const allLoaded = treeData.every(s => s.isLoaded);
      let totalTables = 0;

      if (allLoaded) {
        totalTables = treeData.reduce((acc, s) => acc + (s.children?.length || 0), 0);
      } else {
        // Load all schemas to count tables
        const loadedNodes = await loadAllTables();
        totalTables = loadedNodes.reduce((acc, s) => acc + (s.children?.length || 0), 0);
      }

      // If total tables < 200, expand all
      if (totalTables < 200) {
        setTimeout(() => {
          treeRef.current?.openAll();
          setAllExpanded(true);
        }, 100);
      }
    };

    autoExpandAll();
  }, [initialLoadDone, treeData.length]);

  // Expand all handler
  const handleExpandAll = useCallback(async () => {
    // First ensure all tables are loaded
    await loadAllTables();
    setTimeout(() => {
      treeRef.current?.openAll();
      setAllExpanded(true);
    }, 100);
  }, [loadAllTables]);

  // Collapse all handler
  const handleCollapseAll = useCallback(() => {
    treeRef.current?.closeAll();
    setAllExpanded(false);
  }, []);

  // Expose methods via ref
  useImperativeHandle(ref, () => ({
    openAll: () => {
      handleExpandAll();
    },
    closeAll: () => {
      handleCollapseAll();
    },
    loadAllTables: async () => {
      await loadAllTables();
    },
  }), [handleExpandAll, handleCollapseAll, loadAllTables]);

  // Filter data for diff-only mode
  const filteredTreeData = React.useMemo(() => {
    if (!showDiffOnly || !diffHighlight) return treeData;

    // Filter to only show nodes that have changes
    const filterNode = (node: TreeNodeData): TreeNodeData | null => {
      const status = diffHighlight.nodeStatus.get(node.id);
      const hasChange = status && status !== 'unchanged';

      // For schemas and tables, check if any children have changes
      if (node.children && node.children.length > 0) {
        const filteredChildren = node.children
          .map(filterNode)
          .filter((n): n is TreeNodeData => n !== null);

        if (filteredChildren.length > 0 || hasChange) {
          return { ...node, children: filteredChildren };
        }
      }

      // Leaf node or no children with changes
      if (hasChange) {
        return node;
      }

      return null;
    };

    return treeData
      .map(filterNode)
      .filter((n): n is TreeNodeData => n !== null);
  }, [treeData, showDiffOnly, diffHighlight]);

  // Search match function
  const searchMatch = useCallback((node: { data: TreeNodeData }, term: string) => {
    const lowerTerm = term.toLowerCase();
    const data = node.data;

    // Match on name
    if (data.name.toLowerCase().includes(lowerTerm)) return true;

    // Match on column type
    if (data.columnType?.toLowerCase().includes(lowerTerm)) return true;

    return false;
  }, []);

  // Handle selection
  const handleSelect = useCallback((nodes: { data: TreeNodeData }[]) => {
    if (nodes.length === 0) return;
    const data = nodes[0].data;

    if (data.nodeType === 'table' && data.schemaName && data.tableName) {
      onSelectTable?.(data.schemaName, data.tableName);
    } else if (data.nodeType === 'column' && data.schemaName && data.tableName) {
      onSelectColumn?.(data.schemaName, data.tableName, data.name);
    }
  }, [onSelectTable, onSelectColumn]);

  if (error) {
    return (
      <div style={{ padding: 16, color: 'var(--danger)' }}>
        Error: {error}
      </div>
    );
  }

  const treeHeight = hideControls ? height : height - 90;

  return (
    <DiffContext.Provider value={{ diffHighlight, diffSide }}>
      <div style={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
        {/* Search Bar and Controls - Only show if not hidden */}
        {!hideControls && (
          <div style={{
            padding: '8px 12px',
            borderBottom: '1px solid var(--border)',
            backgroundColor: 'var(--bg-secondary)',
          }}>
            <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
              <div style={{ position: 'relative', flex: 1 }}>
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
                  onChange={(e) => setInternalSearchTerm(e.target.value)}
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
              <button
                onClick={handleExpandAll}
                title="Expand All"
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  width: 32,
                  height: 32,
                  border: '1px solid var(--border)',
                  borderRadius: 6,
                  backgroundColor: 'var(--bg)',
                  cursor: 'pointer',
                  color: 'var(--text-muted)',
                }}
              >
                <ChevronsUpDown size={16} />
              </button>
              <button
                onClick={handleCollapseAll}
                title="Collapse All"
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  width: 32,
                  height: 32,
                  border: '1px solid var(--border)',
                  borderRadius: 6,
                  backgroundColor: 'var(--bg)',
                  cursor: 'pointer',
                  color: 'var(--text-muted)',
                }}
              >
                <ChevronsDownUp size={16} />
              </button>
            </div>
          </div>
        )}

        {/* Tree View */}
        <div style={{ flex: 1, overflow: 'hidden' }}>
          {isLoading && !initialLoadDone ? (
            <div style={{
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              height: 200,
              color: 'var(--text-muted)',
              gap: 8,
            }}>
              <Loader2 size={20} className="icon-spin" />
              Loading schemas...
            </div>
          ) : filteredTreeData.length === 0 ? (
            <div style={{
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              height: 200,
              color: 'var(--text-muted)',
            }}>
              {showDiffOnly ? 'No differences found' : 'No schemas found'}
            </div>
          ) : (
            <Tree<TreeNodeData>
              ref={treeRef}
              data={filteredTreeData}
              width="100%"
              height={treeHeight}
              rowHeight={32}
              overscanCount={10}
              searchTerm={searchTerm}
              searchMatch={searchMatch}
              onToggle={handleToggle}
              onSelect={handleSelect}
              openByDefault={false}
              padding={8}
            >
              {Node}
            </Tree>
          )}
        </div>

        {/* Footer Stats - Only show if not hidden */}
        {!hideControls && (
          <div style={{
            padding: '8px 12px',
            borderTop: '1px solid var(--border)',
            backgroundColor: 'var(--bg-secondary)',
            fontSize: 11,
            color: 'var(--text-muted)',
            display: 'flex',
            gap: 16,
          }}>
            <span>{filteredTreeData.length} schemas</span>
            <span>
              {filteredTreeData.reduce((acc, s) => acc + (s.children?.length || 0), 0)} tables loaded
            </span>
          </div>
        )}
      </div>
    </DiffContext.Provider>
  );
});

export default SchemaTree;
