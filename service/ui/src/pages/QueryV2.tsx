import { useState, useCallback, useRef, useEffect, useMemo } from 'react';
import Editor, { type OnMount } from '@monaco-editor/react';
import { AgGridReact } from 'ag-grid-react';
import { type ColDef, themeQuartz, type ICellRendererParams } from 'ag-grid-community';
import {
  Play,
  Database,
  GitBranch,
  Download,
  Copy,
  Check,
  ChevronDown,
  ChevronRight,
  PanelLeftClose,
  PanelLeft,
  AlertTriangle,
  X,
  Search,
  Clock,
  Table2,
  Columns,
  Info,
  Shield,
  Activity,
} from 'lucide-react';
import type { QueryResponse, Branch, Catalog, TableInfo } from '../types';
import { SchemaTree } from '../components/SchemaTree';
import { catalogsApi, branchesApi, queryApi, schemasApi } from '../api';

// Schema metadata for autocomplete
interface SchemaMetadata {
  schemas: string[];
  tables: Record<string, string[]>; // schema -> table names
  columns: Record<string, string[]>; // schema.table -> column names
}

const defaultQuery = `-- Select from your tables
SELECT * FROM main.positions LIMIT 100;`;

// Table References Modal with AG Grid and expandable rows
interface TableRefRow {
  id: string;
  name: string;
  catalog?: string;
  branch?: string;
  schema?: string;
  refType?: string;
  columnCount?: number;
  isChild: boolean;
  parentId?: string;
  isExpanded?: boolean;
}

interface TableReferencesModalProps {
  tableRefs: Array<{
    catalog_name?: string;
    schema_name?: string;
    table_name: string;
    branch_name?: string;
    reference_type?: string;
    columns?: string[];
  }>;
  onClose: () => void;
}

function TableReferencesModal({ tableRefs, onClose }: TableReferencesModalProps) {
  const [expandedTables, setExpandedTables] = useState<Set<string>>(new Set());

  // Build row data with expandable tables and column children
  const rowData = useMemo(() => {
    const rows: TableRefRow[] = [];

    tableRefs.forEach((ref, idx) => {
      const tableId = `table-${idx}`;
      const isExpanded = expandedTables.has(tableId);
      const columns = ref.columns || [];

      // Table row (parent)
      rows.push({
        id: tableId,
        name: ref.table_name,
        catalog: ref.catalog_name,
        branch: ref.branch_name,
        schema: ref.schema_name,
        refType: ref.reference_type,
        columnCount: columns.length,
        isChild: false,
        isExpanded,
      });

      // Column rows (children) - only if expanded
      if (isExpanded) {
        columns.forEach((col, colIdx) => {
          rows.push({
            id: `${tableId}-col-${colIdx}`,
            name: col,
            isChild: true,
            parentId: tableId,
          });
        });
      }
    });

    return rows;
  }, [tableRefs, expandedTables]);

  // Toggle table expansion
  const toggleTable = (tableId: string) => {
    setExpandedTables(prev => {
      const next = new Set(prev);
      if (next.has(tableId)) {
        next.delete(tableId);
      } else {
        next.add(tableId);
      }
      return next;
    });
  };

  // Column definitions for AG Grid
  const columnDefs: ColDef[] = useMemo(() => [
    {
      headerName: 'Table / Column',
      field: 'name',
      flex: 2,
      minWidth: 200,
      cellRenderer: (params: ICellRendererParams) => {
        const { data } = params;
        if (!data) return null;

        if (data.isChild) {
          // Column row (child)
          return (
            <div style={{ display: 'flex', alignItems: 'center', gap: 8, paddingLeft: 32 }}>
              <Columns size={12} style={{ color: 'var(--text-muted)' }} />
              <span style={{ fontSize: 12 }}>{data.name}</span>
            </div>
          );
        }

        // Table row (parent)
        const hasColumns = (data.columnCount || 0) > 0;
        return (
          <div
            style={{
              display: 'flex',
              alignItems: 'center',
              gap: 8,
              cursor: hasColumns ? 'pointer' : 'default',
            }}
            onClick={() => hasColumns && toggleTable(data.id)}
          >
            {hasColumns ? (
              data.isExpanded ? (
                <ChevronDown size={14} style={{ color: 'var(--text-muted)' }} />
              ) : (
                <ChevronRight size={14} style={{ color: 'var(--text-muted)' }} />
              )
            ) : (
              <span style={{ width: 14 }} />
            )}
            <Table2 size={14} style={{ color: '#8b5cf6' }} />
            <span style={{ fontWeight: 500 }}>{data.name}</span>
            {hasColumns && (
              <span style={{
                fontSize: 10,
                color: 'var(--text-muted)',
                background: 'var(--bg-tertiary)',
                padding: '2px 6px',
                borderRadius: 4,
              }}>
                {data.columnCount} col{data.columnCount !== 1 ? 's' : ''}
              </span>
            )}
          </div>
        );
      },
    },
    {
      headerName: 'Catalog',
      field: 'catalog',
      flex: 1,
      minWidth: 100,
      cellRenderer: (params: ICellRendererParams) => {
        const { data } = params;
        if (!data || data.isChild) return null;
        return (
          <div style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 12 }}>
            <Database size={12} style={{ color: 'var(--text-muted)' }} />
            {data.catalog || '-'}
          </div>
        );
      },
    },
    {
      headerName: 'Branch',
      field: 'branch',
      flex: 1,
      minWidth: 100,
      cellRenderer: (params: ICellRendererParams) => {
        const { data } = params;
        if (!data || data.isChild) return null;
        return (
          <div style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 12, color: 'var(--accent-primary)' }}>
            <GitBranch size={12} />
            {data.branch || '-'}
          </div>
        );
      },
    },
    {
      headerName: 'Schema',
      field: 'schema',
      flex: 1,
      minWidth: 100,
      cellRenderer: (params: ICellRendererParams) => {
        const { data } = params;
        if (!data || data.isChild) return null;
        return <span style={{ fontSize: 12 }}>{data.schema || '-'}</span>;
      },
    },
    {
      headerName: 'Access',
      width: 100,
      cellRenderer: (params: ICellRendererParams) => {
        const { data } = params;
        if (!data || data.isChild) return null;
        return (
          <span style={{
            display: 'inline-flex',
            alignItems: 'center',
            gap: 4,
            padding: '3px 8px',
            borderRadius: 4,
            background: 'rgba(34, 197, 94, 0.1)',
            color: '#22c55e',
            fontSize: 10,
            fontWeight: 500,
          }}>
            <Check size={10} /> Verified
          </span>
        );
      },
      cellStyle: { display: 'flex', alignItems: 'center' },
    },
  ], [expandedTables]);

  // Row styling - use any to avoid AG Grid's strict typing
  const getRowStyle = (params: any) => {
    if (!params.data) return undefined;
    if (params.data.isChild) {
      return { background: 'var(--bg-secondary)', fontSize: '12px' };
    }
    return { fontWeight: 500 };
  };

  // Calculate total columns across all tables
  const totalColumns = tableRefs.reduce((sum, ref) => sum + (ref.columns?.length || 0), 0);

  return (
    <div
      style={{
        position: 'fixed',
        top: 0,
        left: 0,
        right: 0,
        bottom: 0,
        background: 'rgba(0,0,0,0.5)',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        zIndex: 1000,
      }}
      onClick={onClose}
    >
      <div
        style={{
          background: 'var(--bg-primary)',
          borderRadius: 8,
          boxShadow: '0 8px 32px rgba(0,0,0,0.3)',
          maxWidth: 900,
          width: '90%',
          maxHeight: '80vh',
          overflow: 'hidden',
          display: 'flex',
          flexDirection: 'column',
        }}
        onClick={(e) => e.stopPropagation()}
      >
        {/* Modal Header */}
        <div style={{
          padding: '12px 16px',
          borderBottom: '1px solid var(--border-color)',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
        }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            <Table2 size={16} style={{ color: 'var(--accent-primary)' }} />
            <span style={{ fontWeight: 600, fontSize: 14 }}>Table References</span>
            <span style={{ fontSize: 12, color: 'var(--text-muted)' }}>
              ({tableRefs.length} table{tableRefs.length !== 1 ? 's' : ''}, {totalColumns} column{totalColumns !== 1 ? 's' : ''})
            </span>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            <button
              onClick={() => {
                if (expandedTables.size > 0) {
                  setExpandedTables(new Set());
                } else {
                  setExpandedTables(new Set(tableRefs.map((_, i) => `table-${i}`)));
                }
              }}
              className="btn btn-ghost btn-sm"
              style={{ fontSize: 11, padding: '4px 8px' }}
            >
              {expandedTables.size > 0 ? (
                <><ChevronRight size={12} /> Collapse All</>
              ) : (
                <><ChevronDown size={12} /> Expand All</>
              )}
            </button>
            <button
              onClick={onClose}
              style={{
                background: 'none',
                border: 'none',
                cursor: 'pointer',
                padding: 4,
                color: 'var(--text-muted)',
              }}
            >
              <X size={18} />
            </button>
          </div>
        </div>

        {/* Modal Body with AG Grid */}
        <div style={{ flex: 1, overflow: 'hidden', padding: 16 }}>
          <div
            className="ag-theme-alpine-dark"
            style={{
              height: Math.min(500, rowData.length * 40 + 48),
              width: '100%',
            }}
          >
            <AgGridReact
              rowData={rowData}
              columnDefs={columnDefs}
              getRowStyle={getRowStyle as any}
              headerHeight={40}
              rowHeight={38}
              suppressCellFocus={true}
              animateRows={false}
              getRowId={(params) => params.data?.id || ''}
            />
          </div>
        </div>

        {/* Modal Footer */}
        <div style={{
          padding: '12px 16px',
          borderTop: '1px solid var(--border-color)',
          display: 'flex',
          justifyContent: 'flex-end',
        }}>
          <button className="btn btn-secondary" onClick={onClose}>
            Close
          </button>
        </div>
      </div>
    </div>
  );
}

export default function QueryV2() {
  // Refs
  const editorRef = useRef<any>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const schemaTreeContainerRef = useRef<HTMLDivElement>(null);

  // Editor state
  const [query, setQuery] = useState(defaultQuery);
  const [editorHeight, setEditorHeight] = useState(280);
  const [isResizingEditor, setIsResizingEditor] = useState(false);
  const [executing, setExecuting] = useState(false);
  const [result, setResult] = useState<QueryResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [hasSelection, setHasSelection] = useState(false);
  const [copied, setCopied] = useState(false);
  const [showTableRefs, setShowTableRefs] = useState(false);

  // Sidebar state
  const [sidebarOpen, setSidebarOpen] = useState(true);
  const [sidebarWidth, setSidebarWidth] = useState(300);
  const [schemaTreeHeight, setSchemaTreeHeight] = useState(400);

  // Catalog/Branch state
  const [catalogs, setCatalogs] = useState<Catalog[]>([]);
  const [branches, setBranches] = useState<Branch[]>([]);
  const [selectedCatalog, setSelectedCatalog] = useState('');
  const [selectedBranch, setSelectedBranch] = useState('main');
  const [catalogsLoading, setCatalogsLoading] = useState(true);
  const [branchesLoading, setBranchesLoading] = useState(false);

  // Catalogs to USE for query execution (checked by default)
  const [useCatalogs, setUseCatalogs] = useState<Set<string>>(new Set());

  // Catalog dropdown
  const [catalogDropdownOpen, setCatalogDropdownOpen] = useState(false);
  const [catalogSearch, setCatalogSearch] = useState('');
  const catalogDropdownRef = useRef<HTMLDivElement>(null);

  // Branch dropdown
  const [branchDropdownOpen, setBranchDropdownOpen] = useState(false);
  const [branchSearch, setBranchSearch] = useState('');
  const branchDropdownRef = useRef<HTMLDivElement>(null);

  // Catalog overflow dropdown
  const [catalogOverflowOpen, setCatalogOverflowOpen] = useState(false);
  const catalogOverflowRef = useRef<HTMLDivElement>(null);
  const MAX_VISIBLE_CATALOGS = 3;

  // Schema metadata for autocomplete
  const [schemaMetadata, setSchemaMetadata] = useState<SchemaMetadata>({ schemas: [], tables: {}, columns: {} });
  const completionProviderRef = useRef<any>(null);
  const executeQueryRef = useRef<() => void>(() => {});

  // Load catalogs
  useEffect(() => {
    catalogsApi.list().then((data) => {
      const list = Array.isArray(data) ? data : (data as any).catalogs || [];
      setCatalogs(list);
      if (list.length > 0) {
        setSelectedCatalog(list[0].catalog_id);
        // Check all catalogs by default for query execution
        setUseCatalogs(new Set(list.map((c: Catalog) => c.catalog_id)));
      }
      setCatalogsLoading(false);
    }).catch(() => setCatalogsLoading(false));
  }, []);

  // Load branches when catalog changes
  useEffect(() => {
    if (!selectedCatalog) return;
    setBranchesLoading(true);
    branchesApi.list(selectedCatalog).then((data) => {
      const list = Array.isArray(data) ? data : (data as any).branches || [];
      setBranches(list);
      const main = list.find((b: Branch) => b.branch_name === 'main');
      setSelectedBranch(main?.branch_name || list[0]?.branch_name || 'main');
      setBranchesLoading(false);
    }).catch(() => setBranchesLoading(false));
  }, [selectedCatalog]);

  // Load schema metadata for autocomplete
  useEffect(() => {
    if (!selectedCatalog || !selectedBranch) return;

    const loadMetadata = async () => {
      try {
        const metadata: SchemaMetadata = { schemas: [], tables: {}, columns: {} };

        // Fetch schemas
        const schemasData = await schemasApi.list(selectedCatalog, selectedBranch);
        const schemaList = Array.isArray(schemasData) ? schemasData : (schemasData as any).schemas || [];
        metadata.schemas = schemaList.map((s: any) => s.schema_name || s);

        // Fetch tables for each schema (in parallel)
        await Promise.all(metadata.schemas.map(async (schemaName) => {
          try {
            const tablesData = await schemasApi.listTables(selectedCatalog, schemaName, selectedBranch);
            const tableList = Array.isArray(tablesData) ? tablesData : (tablesData as any).tables || [];
            metadata.tables[schemaName] = tableList.map((t: any) => t.table_name || t.name || t);

            // Fetch columns for each table
            await Promise.all(metadata.tables[schemaName].map(async (tableName) => {
              try {
                const tableInfo = await schemasApi.getTable(selectedCatalog, schemaName, tableName, selectedBranch) as TableInfo;
                if (tableInfo?.columns) {
                  metadata.columns[`${schemaName}.${tableName}`] = tableInfo.columns.map((c: any) => c.column_name || c.name || c);
                }
              } catch { /* ignore column fetch errors */ }
            }));
          } catch { /* ignore table fetch errors */ }
        }));

        setSchemaMetadata(metadata);
      } catch (err) {
        console.error('Failed to load schema metadata:', err);
      }
    };

    loadMetadata();
  }, [selectedCatalog, selectedBranch]);

  // Close dropdowns on outside click
  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (catalogDropdownRef.current && !catalogDropdownRef.current.contains(e.target as Node)) {
        setCatalogDropdownOpen(false);
      }
      if (branchDropdownRef.current && !branchDropdownRef.current.contains(e.target as Node)) {
        setBranchDropdownOpen(false);
      }
      if (catalogOverflowRef.current && !catalogOverflowRef.current.contains(e.target as Node)) {
        setCatalogOverflowOpen(false);
      }
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, []);

  // Filtered catalogs
  const filteredCatalogs = catalogs.filter(c =>
    (c.display_name || c.catalog_id).toLowerCase().includes(catalogSearch.toLowerCase())
  );

  // Filtered branches
  const activeBranches = branches.filter(b => b.status === 'active');
  const filteredBranches = activeBranches.filter(b =>
    b.branch_name.toLowerCase().includes(branchSearch.toLowerCase())
  );

  // Trigger Monaco layout when sidebar width changes
  useEffect(() => {
    if (editorRef.current) {
      editorRef.current.layout();
    }
  }, [sidebarWidth]);

  // Measure schema tree container height
  useEffect(() => {
    const container = schemaTreeContainerRef.current;
    if (!container) return;

    const resizeObserver = new ResizeObserver((entries) => {
      for (const entry of entries) {
        const height = entry.contentRect.height;
        if (height > 0) {
          setSchemaTreeHeight(height);
        }
      }
    });

    resizeObserver.observe(container);
    return () => resizeObserver.disconnect();
  }, []);

  // Sidebar resize - direct update for instant feedback
  const handleDividerMouseDown = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    const startX = e.clientX;
    const startWidth = sidebarWidth;

    const onMouseMove = (moveEvent: MouseEvent) => {
      const delta = moveEvent.clientX - startX;
      const newWidth = Math.min(500, Math.max(200, startWidth + delta));
      setSidebarWidth(newWidth);
    };

    const onMouseUp = () => {
      document.removeEventListener('mousemove', onMouseMove);
      document.removeEventListener('mouseup', onMouseUp);
      document.body.style.cursor = '';
      document.body.style.userSelect = '';
    };

    document.body.style.cursor = 'col-resize';
    document.body.style.userSelect = 'none';
    document.addEventListener('mousemove', onMouseMove);
    document.addEventListener('mouseup', onMouseUp);
  }, [sidebarWidth]);

  // Editor vertical resize
  const handleEditorResizeStart = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    setIsResizingEditor(true);
    document.body.style.cursor = 'row-resize';
    document.body.style.userSelect = 'none';

    const startY = e.clientY;
    const startHeight = editorHeight;

    const onMouseMove = (moveEvent: MouseEvent) => {
      const delta = moveEvent.clientX ? moveEvent.clientY - startY : 0;
      const newHeight = Math.min(600, Math.max(150, startHeight + delta));
      setEditorHeight(newHeight);
    };

    const onMouseUp = () => {
      setIsResizingEditor(false);
      document.body.style.cursor = '';
      document.body.style.userSelect = '';
      document.removeEventListener('mousemove', onMouseMove);
      document.removeEventListener('mouseup', onMouseUp);
    };

    document.addEventListener('mousemove', onMouseMove);
    document.addEventListener('mouseup', onMouseUp);
  }, [editorHeight]);

  // Insert text into editor
  const insertText = useCallback((text: string) => {
    if (!editorRef.current) return;
    const editor = editorRef.current;
    const selection = editor.getSelection();
    const id = { major: 1, minor: 1 };
    const op = { identifier: id, range: selection, text: text + ' ', forceMoveMarkers: true };
    editor.executeEdits('insert', [op]);
    editor.focus();
  }, []);

  // Helper to detect DML/DDL statements
  const isDmlOrDdl = (sql: string): boolean => {
    const trimmed = sql.trim().toUpperCase();
    // Remove comments and get first keyword
    const withoutComments = trimmed.replace(/--.*$/gm, '').replace(/\/\*[\s\S]*?\*\//g, '').trim();
    const firstWord = withoutComments.split(/\s+/)[0];
    return ['INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER', 'TRUNCATE', 'MERGE'].includes(firstWord);
  };

  // Execute query using real API
  const executeQuery = useCallback(async () => {
    if (useCatalogs.size === 0) {
      setError('Please select at least one catalog');
      return;
    }

    setExecuting(true);
    setError(null);
    try {
      let sql = query;
      if (hasSelection && editorRef.current) {
        const sel = editorRef.current.getSelection();
        const text = editorRef.current.getModel()?.getValueInRange(sel);
        if (text?.trim()) sql = text;
      }

      // Check if this is a DML/DDL statement
      if (isDmlOrDdl(sql)) {
        // DML/DDL requires a single catalog - use the browsed catalog
        if (!selectedCatalog) {
          setError('Please select a catalog for DML/DDL statements');
          return;
        }

        const res = await queryApi.executeOnBranch(selectedCatalog, selectedBranch, sql);

        // Execute returns QueryResponse for SELECT, or {row_count, execution_time_ms} for DML
        if ('columns' in res && res.columns) {
          // SELECT result - use as-is
          setResult(res as unknown as QueryResponse);
        } else {
          // DML result - convert to display format
          const rowCount = (res as any).row_count ?? 0;
          setResult({
            columns: [{ name: 'result', type: 'VARCHAR' }],
            rows: [[`Success: ${rowCount} row${rowCount !== 1 ? 's' : ''} affected`]],
            row_count: 1,
            execution_time_ms: (res as any).execution_time_ms ?? 0,
          });
        }
      } else {
        // SELECT query - use query API with branch context
        const branchContext: Record<string, string> = {};
        useCatalogs.forEach(catalogId => {
          // Use selectedBranch for browsed catalog, 'main' for others
          branchContext[catalogId] = catalogId === selectedCatalog ? selectedBranch : 'main';
        });

        const res = await queryApi.execute({
          sql,
          branch_context: branchContext,
        });
        setResult(res);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Query failed');
      setResult(null);
    } finally {
      setExecuting(false);
    }
  }, [query, hasSelection, useCatalogs, selectedCatalog, selectedBranch]);

  // Keep executeQueryRef up to date
  useEffect(() => {
    executeQueryRef.current = executeQuery;
  }, [executeQuery]);

  // Monaco setup with custom theme and keyboard shortcuts
  const handleEditorMount: OnMount = (editor, monaco) => {
    editorRef.current = editor;

    // Define custom modern dark theme (slate/blue tones)
    monaco.editor.defineTheme('ducklake-dark', {
      base: 'vs-dark',
      inherit: true,
      rules: [
        { token: 'keyword', foreground: '7dd3fc', fontStyle: 'bold' }, // sky-300
        { token: 'keyword.sql', foreground: '7dd3fc', fontStyle: 'bold' },
        { token: 'string', foreground: 'fda4af' }, // rose-300
        { token: 'string.sql', foreground: 'fda4af' },
        { token: 'number', foreground: 'a5f3fc' }, // cyan-200
        { token: 'comment', foreground: '64748b', fontStyle: 'italic' }, // slate-500
        { token: 'operator', foreground: 'e2e8f0' }, // slate-200
        { token: 'identifier', foreground: 'c4b5fd' }, // violet-300
        { token: 'type', foreground: '6ee7b7' }, // emerald-300
        { token: 'predefined', foreground: 'fbbf24' }, // amber-400
      ],
      colors: {
        'editor.background': '#1e293b', // slate-800
        'editor.foreground': '#e2e8f0', // slate-200
        'editor.lineHighlightBackground': '#334155', // slate-700
        'editor.selectionBackground': '#0ea5e966', // sky-500/40
        'editor.inactiveSelectionBackground': '#0ea5e933',
        'editorLineNumber.foreground': '#64748b', // slate-500
        'editorLineNumber.activeForeground': '#f1f5f9', // slate-100
        'editorCursor.foreground': '#38bdf8', // sky-400
        'editor.selectionHighlightBackground': '#0ea5e933',
        'editorBracketMatch.background': '#0ea5e944',
        'editorBracketMatch.border': '#38bdf8',
        'editorIndentGuide.background1': '#334155', // slate-700
        'editorIndentGuide.activeBackground1': '#475569', // slate-600
        'editorGutter.background': '#1e293b', // slate-800
        'scrollbarSlider.background': '#47556955',
        'scrollbarSlider.hoverBackground': '#47556988',
        'scrollbarSlider.activeBackground': '#475569bb',
        'editorWidget.background': '#1e293b',
        'editorSuggestWidget.background': '#1e293b',
        'editorSuggestWidget.border': '#334155',
        'editorSuggestWidget.selectedBackground': '#334155',
      },
    });
    monaco.editor.setTheme('ducklake-dark');

    // Track selection changes
    editor.onDidChangeCursorSelection((e) => {
      const sel = e.selection;
      setHasSelection(!sel.isEmpty());
    });

    // Add Cmd/Ctrl+Enter to run query
    editor.addAction({
      id: 'run-query',
      label: 'Run Query',
      keybindings: [monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter],
      run: () => {
        executeQueryRef.current();
      },
    });

    // Add Cmd/Ctrl+Shift+F to format
    editor.addAction({
      id: 'format-query',
      label: 'Format SQL',
      keybindings: [monaco.KeyMod.CtrlCmd | monaco.KeyMod.Shift | monaco.KeyCode.KeyF],
      run: () => {
        editor.getAction('editor.action.formatDocument')?.run();
      },
    });

  };

  // Register SQL completion provider with schema metadata (updates when metadata changes)
  useEffect(() => {
    // Need monaco instance - get it from window
    const monaco = (window as any).monaco;
    if (!monaco) return;

    // Dispose previous provider
    if (completionProviderRef.current) {
      completionProviderRef.current.dispose();
    }

    const provider = monaco.languages.registerCompletionItemProvider('sql', {
      triggerCharacters: ['.', ' '],
      provideCompletionItems: (model: any, position: any) => {
        const word = model.getWordUntilPosition(position);
        const range = {
          startLineNumber: position.lineNumber,
          endLineNumber: position.lineNumber,
          startColumn: word.startColumn,
          endColumn: word.endColumn,
        };

        // Get text before cursor to detect context (e.g., "schema." or "schema.table.")
        const textUntilPosition = model.getValueInRange({
          startLineNumber: 1,
          startColumn: 1,
          endLineNumber: position.lineNumber,
          endColumn: position.column,
        });
        const lastLine = textUntilPosition.split('\n').pop() || '';
        const beforeCursor = lastLine.substring(0, position.column - 1);

        const suggestions: any[] = [];

        // Check if we're after "schema." - suggest tables
        const schemaMatch = beforeCursor.match(/(\w+)\.$/);
        if (schemaMatch) {
          const schemaName = schemaMatch[1];
          const tables = schemaMetadata.tables[schemaName] || [];
          tables.forEach(tableName => {
            suggestions.push({
              label: tableName,
              kind: monaco.languages.CompletionItemKind.Class,
              insertText: tableName,
              detail: `Table in ${schemaName}`,
              range,
              sortText: '0' + tableName, // Sort tables first
            });
          });
          return { suggestions };
        }

        // Check if we're after "schema.table." - suggest columns
        const tableMatch = beforeCursor.match(/(\w+)\.(\w+)\.$/);
        if (tableMatch) {
          const schemaName = tableMatch[1];
          const tableName = tableMatch[2];
          const columns = schemaMetadata.columns[`${schemaName}.${tableName}`] || [];
          columns.forEach(colName => {
            suggestions.push({
              label: colName,
              kind: monaco.languages.CompletionItemKind.Field,
              insertText: colName,
              detail: `Column in ${schemaName}.${tableName}`,
              range,
            });
          });
          return { suggestions };
        }

        // Default: suggest schemas, keywords, and functions

        // Schemas
        schemaMetadata.schemas.forEach(schemaName => {
          suggestions.push({
            label: schemaName,
            kind: monaco.languages.CompletionItemKind.Module,
            insertText: schemaName,
            detail: 'Schema',
            range,
            sortText: '0' + schemaName,
          });
        });

        // Also suggest schema.table combinations for convenience
        Object.entries(schemaMetadata.tables).forEach(([schemaName, tables]) => {
          tables.forEach(tableName => {
            suggestions.push({
              label: `${schemaName}.${tableName}`,
              kind: monaco.languages.CompletionItemKind.Class,
              insertText: `${schemaName}.${tableName}`,
              detail: 'Table',
              range,
              sortText: '1' + schemaName + tableName,
            });
          });
        });

        // SQL Keywords
        const keywords = [
          'SELECT', 'FROM', 'WHERE', 'JOIN', 'LEFT', 'RIGHT', 'INNER', 'OUTER', 'ON',
          'AND', 'OR', 'NOT', 'IN', 'BETWEEN', 'LIKE', 'IS', 'NULL', 'AS', 'DISTINCT',
          'GROUP BY', 'ORDER BY', 'HAVING', 'LIMIT', 'OFFSET', 'UNION', 'INTERSECT',
          'EXCEPT', 'INSERT', 'INTO', 'VALUES', 'UPDATE', 'SET', 'DELETE', 'CREATE',
          'TABLE', 'VIEW', 'INDEX', 'DROP', 'ALTER', 'ADD', 'COLUMN', 'PRIMARY KEY',
          'FOREIGN KEY', 'REFERENCES', 'CASCADE', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END',
          'ASC', 'DESC', 'NULLS FIRST', 'NULLS LAST', 'WITH', 'RECURSIVE', 'OVER',
          'PARTITION BY', 'ROWS', 'RANGE', 'UNBOUNDED', 'PRECEDING', 'FOLLOWING', 'CURRENT ROW',
        ];

        keywords.forEach(kw => {
          suggestions.push({
            label: kw,
            kind: monaco.languages.CompletionItemKind.Keyword,
            insertText: kw,
            range,
            sortText: '2' + kw,
          });
        });

        // DuckDB/SQL Functions
        const functions = [
          { name: 'COUNT', detail: 'COUNT(expr) - Count rows' },
          { name: 'SUM', detail: 'SUM(expr) - Sum values' },
          { name: 'AVG', detail: 'AVG(expr) - Average value' },
          { name: 'MIN', detail: 'MIN(expr) - Minimum value' },
          { name: 'MAX', detail: 'MAX(expr) - Maximum value' },
          { name: 'COALESCE', detail: 'COALESCE(expr, ...) - First non-null' },
          { name: 'NULLIF', detail: 'NULLIF(a, b) - NULL if a = b' },
          { name: 'CAST', detail: 'CAST(expr AS type) - Type conversion' },
          { name: 'CONCAT', detail: 'CONCAT(str, ...) - Concatenate strings' },
          { name: 'SUBSTRING', detail: 'SUBSTRING(str, start, len)' },
          { name: 'TRIM', detail: 'TRIM(str) - Remove whitespace' },
          { name: 'UPPER', detail: 'UPPER(str) - Uppercase' },
          { name: 'LOWER', detail: 'LOWER(str) - Lowercase' },
          { name: 'LENGTH', detail: 'LENGTH(str) - String length' },
          { name: 'REPLACE', detail: 'REPLACE(str, from, to)' },
          { name: 'ROUND', detail: 'ROUND(num, decimals)' },
          { name: 'FLOOR', detail: 'FLOOR(num) - Round down' },
          { name: 'CEIL', detail: 'CEIL(num) - Round up' },
          { name: 'ABS', detail: 'ABS(num) - Absolute value' },
          { name: 'NOW', detail: 'NOW() - Current timestamp' },
          { name: 'CURRENT_DATE', detail: 'CURRENT_DATE - Today' },
          { name: 'DATE_TRUNC', detail: 'DATE_TRUNC(part, date)' },
          { name: 'DATE_PART', detail: 'DATE_PART(part, date)' },
          { name: 'EXTRACT', detail: 'EXTRACT(part FROM date)' },
          { name: 'ROW_NUMBER', detail: 'ROW_NUMBER() OVER(...)' },
          { name: 'RANK', detail: 'RANK() OVER(...)' },
          { name: 'DENSE_RANK', detail: 'DENSE_RANK() OVER(...)' },
          { name: 'LAG', detail: 'LAG(expr, offset) OVER(...)' },
          { name: 'LEAD', detail: 'LEAD(expr, offset) OVER(...)' },
          { name: 'FIRST_VALUE', detail: 'FIRST_VALUE(expr) OVER(...)' },
          { name: 'LAST_VALUE', detail: 'LAST_VALUE(expr) OVER(...)' },
          { name: 'LIST_AGG', detail: 'LIST_AGG(expr) - Aggregate to list' },
          { name: 'STRING_AGG', detail: 'STRING_AGG(expr, sep)' },
          { name: 'ARRAY_AGG', detail: 'ARRAY_AGG(expr) - Aggregate to array' },
        ];

        functions.forEach(fn => {
          suggestions.push({
            label: fn.name,
            kind: monaco.languages.CompletionItemKind.Function,
            insertText: fn.name + '($0)',
            insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet,
            detail: fn.detail,
            range,
            sortText: '3' + fn.name,
          });
        });

        return { suggestions };
      },
    });

    completionProviderRef.current = provider;

    return () => {
      provider.dispose();
    };
  }, [schemaMetadata]);

  // AG Grid custom theme - clean modern styling
  const gridTheme = themeQuartz.withParams({
    fontFamily: "'SF Mono', Consolas, monospace",
    fontSize: 13,
    headerFontSize: 12,
    headerFontWeight: 600,
    cellHorizontalPadding: 12,
    rowBorder: true,
    wrapperBorderRadius: 0,
  });

  // AG Grid columns - sized to content, not stretched
  const columnDefs: ColDef[] = (result?.columns.map((col) => ({
    field: col.name,
    headerName: col.name.toUpperCase(),
    minWidth: 100,
    cellStyle: (params: any) => {
      // Right-align numbers
      if (typeof params.value === 'number') {
        return { textAlign: 'right', fontFamily: "'SF Mono', monospace" };
      }
      // Dim null values
      if (params.value === null) {
        return { color: '#64748b', fontStyle: 'italic' };
      }
      return null;
    },
    valueFormatter: (params: any) => {
      if (params.value === null) return 'NULL';
      if (typeof params.value === 'number') {
        // Format numbers with commas
        return params.value.toLocaleString();
      }
      return String(params.value);
    },
  })) || []) as ColDef[];

  const rowData = result?.rows.map((row, i) => {
    const obj: Record<string, unknown> = { _id: i };
    result.columns.forEach((col, j) => { obj[col.name] = row[j]; });
    return obj;
  }) || [];

  // AG Grid default column config
  const defaultColDef: ColDef = {
    sortable: true,
    filter: true,
    resizable: true,
    suppressMovable: false,
  };

  // Copy results
  const copyResults = () => {
    if (!result) return;
    const header = result.columns.map(c => c.name).join('\t');
    const rows = result.rows.map(r => r.join('\t')).join('\n');
    navigator.clipboard.writeText(`${header}\n${rows}`);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  // Download CSV
  const downloadCSV = () => {
    if (!result) return;
    const header = result.columns.map(c => c.name).join(',');
    const rows = result.rows.map(r => r.map(cell => {
      if (cell === null) return '';
      if (typeof cell === 'string' && cell.includes(',')) return `"${cell.replace(/"/g, '""')}"`;
      return String(cell);
    }).join(',')).join('\n');
    const blob = new Blob([`${header}\n${rows}`], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'query_results.csv';
    a.click();
    URL.revokeObjectURL(url);
  };

  return (
    <div style={{ display: 'flex', height: '100%', width: '100%', overflow: 'hidden' }} className="no-transition">
      {/* Sidebar */}
      <div style={{
        width: sidebarOpen ? sidebarWidth : 40,
        minWidth: sidebarOpen ? 200 : 40,
        background: 'var(--bg-secondary)',
        display: 'flex',
        flexDirection: 'column',
        flexShrink: 0,
        overflow: 'hidden',
        transition: 'none',
      }}>
        {/* Sidebar Header */}
        <div style={{
          padding: '12px',
          borderBottom: '1px solid var(--border-color)',
          display: 'flex',
          alignItems: 'center',
          justifyContent: sidebarOpen ? 'space-between' : 'center',
        }}>
          {sidebarOpen && (
            <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
              <Database size={16} style={{ color: 'var(--text-muted)' }} />
              <span style={{ fontWeight: 600, fontSize: 13 }}>Schema Browser</span>
            </div>
          )}
          <button
            onClick={() => setSidebarOpen(!sidebarOpen)}
            style={{
              background: 'none',
              border: 'none',
              cursor: 'pointer',
              padding: 4,
              color: 'var(--text-muted)',
            }}
          >
            {sidebarOpen ? <PanelLeftClose size={16} /> : <PanelLeft size={16} />}
          </button>
        </div>

        {sidebarOpen && (
          <>
            {/* Catalog Select (Searchable) */}
            <div style={{ padding: '8px 12px', borderBottom: '1px solid var(--border-color)' }}>
              <label style={{ fontSize: 11, color: 'var(--text-muted)', textTransform: 'uppercase', fontWeight: 600, display: 'block', marginBottom: 4 }}>
                Catalog
              </label>
              <div ref={catalogDropdownRef} style={{ position: 'relative' }}>
                <div
                  onClick={() => !catalogsLoading && setCatalogDropdownOpen(!catalogDropdownOpen)}
                  style={{
                    display: 'flex',
                    alignItems: 'center',
                    gap: 8,
                    padding: '6px 10px',
                    border: '1px solid var(--border-color)',
                    borderRadius: 6,
                    background: 'var(--bg-primary)',
                    cursor: catalogsLoading ? 'wait' : 'pointer',
                  }}
                >
                  <Database size={14} style={{ color: 'var(--accent-primary)' }} />
                  <span style={{ flex: 1, fontSize: 12, fontWeight: 500 }}>
                    {catalogsLoading ? 'Loading...' : (catalogs.find(c => c.catalog_id === selectedCatalog)?.display_name || selectedCatalog || 'Select catalog')}
                  </span>
                  <ChevronDown size={14} style={{ color: 'var(--text-muted)', transform: catalogDropdownOpen ? 'rotate(180deg)' : 'none' }} />
                </div>

                {catalogDropdownOpen && (
                  <div style={{
                    position: 'absolute',
                    top: '100%',
                    left: 0,
                    right: 0,
                    marginTop: 4,
                    background: 'var(--bg-primary)',
                    border: '1px solid var(--border-color)',
                    borderRadius: 6,
                    boxShadow: '0 4px 12px rgba(0,0,0,0.15)',
                    zIndex: 100,
                    maxHeight: 280,
                    display: 'flex',
                    flexDirection: 'column',
                  }}>
                    {/* Search */}
                    <div style={{ padding: 8, borderBottom: '1px solid var(--border-color)' }}>
                      <div style={{ display: 'flex', alignItems: 'center', gap: 6, padding: '6px 8px', background: 'var(--bg-secondary)', borderRadius: 4 }}>
                        <Search size={12} style={{ color: 'var(--text-muted)' }} />
                        <input
                          type="text"
                          placeholder="Search catalogs..."
                          value={catalogSearch}
                          onChange={(e) => setCatalogSearch(e.target.value)}
                          autoFocus
                          style={{ flex: 1, background: 'none', border: 'none', outline: 'none', color: 'var(--text-primary)', fontSize: 12 }}
                        />
                        {catalogSearch && (
                          <button onClick={() => setCatalogSearch('')} style={{ background: 'none', border: 'none', cursor: 'pointer', color: 'var(--text-muted)', padding: 0 }}>
                            <X size={10} />
                          </button>
                        )}
                      </div>
                    </div>
                    {/* List */}
                    <div style={{ flex: 1, overflowY: 'auto' }}>
                      {filteredCatalogs.length === 0 ? (
                        <div style={{ padding: 12, textAlign: 'center', color: 'var(--text-muted)', fontSize: 12 }}>No catalogs found</div>
                      ) : filteredCatalogs.map(c => (
                        <div
                          key={c.catalog_id}
                          onClick={() => {
                            setSelectedCatalog(c.catalog_id);
                            // Auto-check the catalog in toolbar
                            setUseCatalogs(prev => new Set(prev).add(c.catalog_id));
                            setCatalogDropdownOpen(false);
                            setCatalogSearch('');
                          }}
                          style={{
                            display: 'flex',
                            alignItems: 'center',
                            gap: 8,
                            padding: '8px 12px',
                            cursor: 'pointer',
                            fontSize: 12,
                            background: c.catalog_id === selectedCatalog ? 'var(--accent-secondary)' : 'transparent',
                          }}
                          onMouseEnter={(e) => c.catalog_id !== selectedCatalog && (e.currentTarget.style.background = 'var(--bg-secondary)')}
                          onMouseLeave={(e) => c.catalog_id !== selectedCatalog && (e.currentTarget.style.background = 'transparent')}
                        >
                          <Database size={12} style={{ color: c.catalog_id === selectedCatalog ? 'var(--accent-primary)' : 'var(--text-muted)' }} />
                          <span style={{ flex: 1 }}>{c.display_name || c.catalog_id}</span>
                          {c.catalog_id === selectedCatalog && <Check size={12} style={{ color: 'var(--accent-primary)' }} />}
                        </div>
                      ))}
                    </div>
                    <div style={{ padding: '6px 12px', borderTop: '1px solid var(--border-color)', fontSize: 10, color: 'var(--text-muted)', background: 'var(--bg-secondary)' }}>
                      {filteredCatalogs.length} of {catalogs.length} catalogs
                    </div>
                  </div>
                )}
              </div>
            </div>

            {/* Branch Select (Searchable) */}
            <div style={{ padding: '8px 12px', borderBottom: '1px solid var(--border-color)' }}>
              <label style={{ fontSize: 11, color: 'var(--text-muted)', textTransform: 'uppercase', fontWeight: 600, display: 'block', marginBottom: 4 }}>
                Branch
              </label>
              <div ref={branchDropdownRef} style={{ position: 'relative' }}>
                <div
                  onClick={() => !branchesLoading && setBranchDropdownOpen(!branchDropdownOpen)}
                  style={{
                    display: 'flex',
                    alignItems: 'center',
                    gap: 8,
                    padding: '6px 10px',
                    border: '1px solid var(--border-color)',
                    borderRadius: 6,
                    background: 'var(--bg-primary)',
                    cursor: branchesLoading ? 'wait' : 'pointer',
                  }}
                >
                  <GitBranch size={14} style={{ color: 'var(--accent-primary)' }} />
                  <span style={{ flex: 1, fontSize: 12, fontWeight: 500 }}>
                    {branchesLoading ? 'Loading...' : selectedBranch}
                  </span>
                  <ChevronDown size={14} style={{ color: 'var(--text-muted)', transform: branchDropdownOpen ? 'rotate(180deg)' : 'none' }} />
                </div>

                {branchDropdownOpen && (
                  <div style={{
                    position: 'absolute',
                    top: '100%',
                    left: 0,
                    right: 0,
                    marginTop: 4,
                    background: 'var(--bg-primary)',
                    border: '1px solid var(--border-color)',
                    borderRadius: 6,
                    boxShadow: '0 4px 12px rgba(0,0,0,0.15)',
                    zIndex: 100,
                    maxHeight: 280,
                    display: 'flex',
                    flexDirection: 'column',
                  }}>
                    {/* Search */}
                    <div style={{ padding: 8, borderBottom: '1px solid var(--border-color)' }}>
                      <div style={{ display: 'flex', alignItems: 'center', gap: 6, padding: '6px 8px', background: 'var(--bg-secondary)', borderRadius: 4 }}>
                        <Search size={12} style={{ color: 'var(--text-muted)' }} />
                        <input
                          type="text"
                          placeholder="Search branches..."
                          value={branchSearch}
                          onChange={(e) => setBranchSearch(e.target.value)}
                          autoFocus
                          style={{ flex: 1, background: 'none', border: 'none', outline: 'none', color: 'var(--text-primary)', fontSize: 12 }}
                        />
                        {branchSearch && (
                          <button onClick={() => setBranchSearch('')} style={{ background: 'none', border: 'none', cursor: 'pointer', color: 'var(--text-muted)', padding: 0 }}>
                            <X size={10} />
                          </button>
                        )}
                      </div>
                    </div>
                    {/* List */}
                    <div style={{ flex: 1, overflowY: 'auto' }}>
                      {filteredBranches.length === 0 ? (
                        <div style={{ padding: 12, textAlign: 'center', color: 'var(--text-muted)', fontSize: 12 }}>No branches found</div>
                      ) : filteredBranches.map(b => (
                        <div
                          key={b.branch_id}
                          onClick={() => { setSelectedBranch(b.branch_name); setBranchDropdownOpen(false); setBranchSearch(''); }}
                          style={{
                            display: 'flex',
                            alignItems: 'center',
                            gap: 8,
                            padding: '8px 12px',
                            cursor: 'pointer',
                            fontSize: 12,
                            background: b.branch_name === selectedBranch ? 'var(--accent-secondary)' : 'transparent',
                          }}
                          onMouseEnter={(e) => b.branch_name !== selectedBranch && (e.currentTarget.style.background = 'var(--bg-secondary)')}
                          onMouseLeave={(e) => b.branch_name !== selectedBranch && (e.currentTarget.style.background = 'transparent')}
                        >
                          <GitBranch size={12} style={{ color: b.branch_name === 'main' ? 'var(--accent-primary)' : 'var(--text-muted)' }} />
                          <span style={{ flex: 1 }}>{b.branch_name}</span>
                          {b.branch_name === selectedBranch && <Check size={12} style={{ color: 'var(--accent-primary)' }} />}
                        </div>
                      ))}
                    </div>
                    <div style={{ padding: '6px 12px', borderTop: '1px solid var(--border-color)', fontSize: 10, color: 'var(--text-muted)', background: 'var(--bg-secondary)' }}>
                      {filteredBranches.length} of {activeBranches.length} branches
                    </div>
                  </div>
                )}
              </div>
            </div>

            {/* Schema Tree - takes remaining height with proper flex growth */}
            <div
              ref={schemaTreeContainerRef}
              style={{ flex: 1, minHeight: 0, overflow: 'hidden' }}
            >
              {selectedCatalog ? (
                <SchemaTree
                  catalogId={selectedCatalog}
                  branch={selectedBranch}
                  height={schemaTreeHeight}
                  onSelectTable={(schema, table) => insertText(`${schema}.${table}`)}
                  onSelectColumn={(_, __, col) => insertText(col)}
                />
              ) : (
                <div style={{ padding: 20, textAlign: 'center', color: 'var(--text-muted)', fontSize: 12 }}>
                  Select a catalog
                </div>
              )}
            </div>
          </>
        )}
      </div>

      {/* Divider */}
      {sidebarOpen && (
        <div
          onMouseDown={handleDividerMouseDown}
          style={{
            width: 6,
            background: 'var(--border-color)',
            cursor: 'col-resize',
            flexShrink: 0,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
          }}
          onMouseEnter={(e) => e.currentTarget.style.background = 'var(--accent-secondary)'}
          onMouseLeave={(e) => e.currentTarget.style.background = 'var(--border-color)'}
        >
          <div style={{ width: 2, height: 40, background: 'var(--text-muted)', borderRadius: 2, opacity: 0.5 }} />
        </div>
      )}

      {/* Main Area - explicit width calculation so Monaco/AG Grid resize properly */}
      <div ref={containerRef} style={{
        width: sidebarOpen ? `calc(100% - ${sidebarWidth}px - 6px)` : 'calc(100% - 40px)',
        display: 'flex',
        flexDirection: 'column',
        overflow: 'hidden',
        transition: 'none',
      }}>
        {/* Pinned Toolbar - always visible */}
        <div style={{
          padding: '10px 16px',
          borderBottom: '1px solid var(--border-color)',
          display: 'flex',
          alignItems: 'center',
          gap: 12,
          background: 'var(--bg-secondary)',
          flexShrink: 0,
          position: 'sticky',
          top: 0,
          zIndex: 10,
        }}>
          {/* Catalog Checkboxes - Limited Display */}
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, maxWidth: 500 }}>
            {/* Show selected catalog first, then others up to MAX_VISIBLE_CATALOGS */}
            {(() => {
              const selectedCat = catalogs.find(c => c.catalog_id === selectedCatalog);
              const otherCats = catalogs.filter(c => c.catalog_id !== selectedCatalog);
              const visibleCats = selectedCat
                ? [selectedCat, ...otherCats.slice(0, MAX_VISIBLE_CATALOGS - 1)]
                : catalogs.slice(0, MAX_VISIBLE_CATALOGS);
              return visibleCats;
            })().map(c => {
              const isChecked = useCatalogs.has(c.catalog_id);
              const isBrowsed = c.catalog_id === selectedCatalog;
              return (
                <label
                  key={c.catalog_id}
                  style={{
                    display: 'flex',
                    alignItems: 'center',
                    gap: 6,
                    padding: '6px 10px',
                    background: 'var(--bg-primary)',
                    borderRadius: 6,
                    fontSize: 12,
                    cursor: 'pointer',
                    border: '1px solid var(--border-color)',
                    opacity: isChecked ? 1 : 0.5,
                    whiteSpace: 'nowrap',
                    position: 'relative',
                  }}
                >
                  <input
                    type="checkbox"
                    checked={isChecked}
                    onChange={(e) => {
                      const newSet = new Set(useCatalogs);
                      if (e.target.checked) {
                        newSet.add(c.catalog_id);
                      } else {
                        newSet.delete(c.catalog_id);
                      }
                      setUseCatalogs(newSet);
                    }}
                    style={{
                      margin: 0,
                      cursor: 'pointer',
                      accentColor: 'var(--accent-primary)',
                      flexShrink: 0,
                    }}
                  />
                  <Database size={12} style={{ color: 'var(--text-muted)', flexShrink: 0 }} />
                  <span
                    className="branch-tooltip"
                    data-tooltip={c.display_name || c.catalog_id}
                    style={{ overflow: 'hidden', textOverflow: 'ellipsis', position: 'relative' }}
                  >
                    {(c.display_name || c.catalog_id).length > 12
                      ? (c.display_name || c.catalog_id).slice(0, 12) + '…'
                      : (c.display_name || c.catalog_id)}
                  </span>
                  {isBrowsed && (
                    <span
                      className="branch-tooltip"
                      data-tooltip={selectedBranch}
                      style={{ display: 'flex', alignItems: 'center', gap: 4, marginLeft: 4, color: 'var(--text-muted)', flexShrink: 0, cursor: 'default', position: 'relative' }}
                    >
                      <span>@</span>
                      <GitBranch size={10} />
                      <span style={{ fontSize: 11, minWidth: 40, maxWidth: 70, overflow: 'hidden', textOverflow: 'ellipsis' }}>
                        {selectedBranch.length > 10 ? selectedBranch.slice(0, 10) + '…' : selectedBranch}
                      </span>
                    </span>
                  )}
                </label>
              );
            })}

            {/* Overflow dropdown for additional catalogs */}
            {catalogs.length > MAX_VISIBLE_CATALOGS && (
              <div ref={catalogOverflowRef} style={{ position: 'relative' }}>
                <button
                  onClick={() => setCatalogOverflowOpen(!catalogOverflowOpen)}
                  style={{
                    display: 'flex',
                    alignItems: 'center',
                    gap: 4,
                    padding: '6px 10px',
                    background: 'var(--bg-primary)',
                    borderRadius: 6,
                    fontSize: 12,
                    cursor: 'pointer',
                    border: '1px solid var(--border-color)',
                    color: 'var(--text-secondary)',
                  }}
                >
                  +{catalogs.length - MAX_VISIBLE_CATALOGS} more
                  <ChevronDown size={12} style={{ transform: catalogOverflowOpen ? 'rotate(180deg)' : 'none' }} />
                </button>

                {catalogOverflowOpen && (
                  <div style={{
                    position: 'absolute',
                    top: '100%',
                    left: 0,
                    marginTop: 4,
                    background: 'var(--bg-secondary)',
                    border: '1px solid var(--border-color)',
                    borderRadius: 6,
                    boxShadow: '0 4px 16px rgba(0,0,0,0.25)',
                    zIndex: 100,
                    minWidth: 220,
                    maxHeight: 300,
                    overflowY: 'auto',
                  }}>
                    <div style={{ padding: '8px 12px', borderBottom: '1px solid var(--border-color)', fontSize: 11, color: 'var(--text-muted)', fontWeight: 600, background: 'var(--bg-tertiary)' }}>
                      All Catalogs ({catalogs.length})
                    </div>
                    {catalogs.map(c => {
                      const isChecked = useCatalogs.has(c.catalog_id);
                      const isBrowsed = c.catalog_id === selectedCatalog;
                      return (
                        <label
                          key={c.catalog_id}
                          style={{
                            display: 'flex',
                            alignItems: 'center',
                            gap: 8,
                            padding: '8px 12px',
                            cursor: 'pointer',
                            fontSize: 12,
                            background: isChecked ? 'rgba(99, 102, 241, 0.1)' : 'transparent',
                          }}
                          onMouseEnter={(e) => (e.currentTarget.style.background = isChecked ? 'rgba(99, 102, 241, 0.15)' : 'var(--bg-tertiary)')}
                          onMouseLeave={(e) => (e.currentTarget.style.background = isChecked ? 'rgba(99, 102, 241, 0.1)' : 'transparent')}
                        >
                          <input
                            type="checkbox"
                            checked={isChecked}
                            onChange={(e) => {
                              const newSet = new Set(useCatalogs);
                              if (e.target.checked) {
                                newSet.add(c.catalog_id);
                              } else {
                                newSet.delete(c.catalog_id);
                              }
                              setUseCatalogs(newSet);
                            }}
                            style={{
                              margin: 0,
                              cursor: 'pointer',
                              accentColor: 'var(--accent-primary)',
                            }}
                          />
                          <Database size={12} style={{ color: isChecked ? 'var(--text-secondary)' : 'var(--text-muted)', opacity: 0.7 }} />
                          <span style={{ flex: 1, color: isChecked ? 'var(--text-primary)' : 'var(--text-secondary)' }}>{c.display_name || c.catalog_id}</span>
                          {isBrowsed && (
                            <span
                              className="branch-tooltip"
                              data-tooltip={selectedBranch}
                              style={{ display: 'flex', alignItems: 'center', gap: 2, color: 'var(--text-muted)', fontSize: 10, cursor: 'default', position: 'relative' }}
                            >
                              <GitBranch size={10} />
                              {selectedBranch.length > 10 ? selectedBranch.slice(0, 10) + '…' : selectedBranch}
                            </span>
                          )}
                        </label>
                      );
                    })}
                    <div style={{ padding: '6px 12px', borderTop: '1px solid var(--border-color)', display: 'flex', gap: 8, background: 'var(--bg-tertiary)' }}>
                      <button
                        onClick={() => setUseCatalogs(new Set(catalogs.map(c => c.catalog_id)))}
                        style={{ flex: 1, padding: '4px 8px', fontSize: 11, background: 'transparent', border: '1px solid var(--border-color)', borderRadius: 4, cursor: 'pointer', color: 'var(--text-muted)' }}
                      >
                        Select All
                      </button>
                      <button
                        onClick={() => setUseCatalogs(new Set())}
                        style={{ flex: 1, padding: '4px 8px', fontSize: 11, background: 'transparent', border: '1px solid var(--border-color)', borderRadius: 4, cursor: 'pointer', color: 'var(--text-muted)' }}
                      >
                        Clear All
                      </button>
                    </div>
                  </div>
                )}
              </div>
            )}

            {/* Summary badge */}
            <span style={{ fontSize: 11, color: 'var(--text-muted)', padding: '4px 8px', background: 'var(--bg-tertiary)', borderRadius: 4 }}>
              {useCatalogs.size}/{catalogs.length} selected
            </span>
          </div>

          <div style={{ flex: 1 }} />

          {/* Run Button */}
          <span style={{ fontSize: 11, color: 'var(--text-muted)', opacity: 0.7 }}>⌘↵</span>
          <button
            onClick={executeQuery}
            disabled={executing}
            className="btn btn-primary"
            style={{
              display: 'flex',
              alignItems: 'center',
              gap: 6,
              padding: '8px 16px',
              background: executing ? 'var(--text-muted)' : 'var(--accent-primary)',
              color: 'white',
              border: 'none',
              borderRadius: 6,
              cursor: executing ? 'wait' : 'pointer',
              fontSize: 13,
              fontWeight: 600,
              minWidth: 100,
              justifyContent: 'center',
            }}
          >
            {executing ? (
              <div className="spinner" style={{ width: 14, height: 14, borderWidth: 2 }} />
            ) : (
              <Play size={14} />
            )}
            {hasSelection ? 'Run Selected' : 'Run'}
          </button>
        </div>

        {/* Editor */}
        <div style={{ height: editorHeight, flexShrink: 0, minWidth: 0, overflow: 'hidden' }}>
          <Editor
            height="100%"
            defaultLanguage="sql"
            value={query}
            onChange={(v) => setQuery(v || '')}
            onMount={handleEditorMount}
            options={{
              minimap: { enabled: false },
              fontSize: 14,
              fontFamily: "'JetBrains Mono', 'Fira Code', 'SF Mono', Consolas, monospace",
              fontLigatures: true,
              lineNumbers: 'on',
              lineNumbersMinChars: 3,
              scrollBeyondLastLine: false,
              tabSize: 2,
              wordWrap: 'on',
              padding: { top: 12, bottom: 12 },
              suggestOnTriggerCharacters: true,
              quickSuggestions: true,
              snippetSuggestions: 'inline',
              acceptSuggestionOnEnter: 'on',
              tabCompletion: 'on',
              bracketPairColorization: { enabled: true },
              autoClosingBrackets: 'always',
              autoClosingQuotes: 'always',
              autoIndent: 'full',
              formatOnPaste: true,
              renderWhitespace: 'selection',
              smoothScrolling: false,
              cursorBlinking: 'blink',
              cursorSmoothCaretAnimation: 'off',
              mouseWheelZoom: true,
              folding: true,
              foldingStrategy: 'indentation',
              showFoldingControls: 'mouseover',
              matchBrackets: 'always',
              selectionHighlight: true,
              occurrencesHighlight: 'singleFile',
              renderLineHighlight: 'all',
              scrollbar: {
                verticalScrollbarSize: 10,
                horizontalScrollbarSize: 10,
              },
            }}
          />
        </div>

        {/* Editor/Results Resize Handle */}
        <div
          onMouseDown={handleEditorResizeStart}
          style={{
            height: 6,
            background: isResizingEditor ? 'var(--accent-primary)' : 'var(--border-color)',
            cursor: 'row-resize',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            flexShrink: 0,
            transition: isResizingEditor ? 'none' : 'background 0.15s ease',
          }}
          onMouseEnter={(e) => !isResizingEditor && (e.currentTarget.style.background = 'var(--accent-secondary)')}
          onMouseLeave={(e) => !isResizingEditor && (e.currentTarget.style.background = 'var(--border-color)')}
        >
          <div style={{
            width: 40,
            height: 3,
            borderRadius: 2,
            background: isResizingEditor ? '#fff' : 'var(--text-muted)',
            opacity: 0.5,
          }} />
        </div>

        {/* Results */}
        <div style={{ flex: '1 1 0', display: 'flex', flexDirection: 'column', minWidth: 0, overflow: 'hidden' }}>
          {/* Results Header */}
          <div style={{
            padding: '8px 16px',
            borderBottom: '1px solid var(--border-color)',
            display: 'flex',
            alignItems: 'center',
            gap: 12,
            background: 'var(--bg-secondary)',
          }}>
            <span style={{ fontSize: 13, fontWeight: 500 }}>Results</span>
            {result && (
              <>
                <span style={{ fontSize: 12, color: 'var(--text-secondary)' }}>
                  <strong>{result.row_count}</strong> rows
                </span>
                <span style={{ fontSize: 12, color: 'var(--text-secondary)', display: 'flex', alignItems: 'center', gap: 4 }}>
                  <Clock size={12} /> {result.execution_time_ms.toFixed(1)} ms
                </span>
                {/* Query Tools */}
                <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
                  {/* Table References Button with Security Shield */}
                  {result.table_references && result.table_references.length > 0 && (
                    <button
                      onClick={() => setShowTableRefs(true)}
                      style={{
                        display: 'flex',
                        alignItems: 'center',
                        gap: 4,
                        padding: '4px 8px',
                        background: 'var(--bg-primary)',
                        border: '1px solid var(--border-color)',
                        borderRadius: 4,
                        cursor: 'pointer',
                        fontSize: 11,
                        color: 'var(--text-secondary)',
                      }}
                      title="View table references and access verification"
                    >
                      <Shield size={12} style={{ color: '#22c55e' }} />
                      <Table2 size={12} />
                      {result.table_references.length} table{result.table_references.length > 1 ? 's' : ''}
                    </button>
                  )}
                  {/* Profiler Placeholder */}
                  <button
                    onClick={() => alert('Query profiler coming soon!')}
                    style={{
                      display: 'flex',
                      alignItems: 'center',
                      gap: 4,
                      padding: '4px 8px',
                      background: 'var(--bg-primary)',
                      border: '1px solid var(--border-color)',
                      borderRadius: 4,
                      cursor: 'pointer',
                      fontSize: 11,
                      color: 'var(--text-secondary)',
                      opacity: 0.7,
                    }}
                    title="Query profiler (coming soon)"
                  >
                    <Activity size={12} style={{ color: '#f59e0b' }} />
                  </button>
                </div>
              </>
            )}
            {!result && !error && (
              <span style={{ fontSize: 12, color: 'var(--text-muted)' }}>Run a query to see results</span>
            )}
            <div style={{ flex: 1 }} />
            {result && (
              <div style={{ display: 'flex', gap: 8 }}>
                <button
                  onClick={copyResults}
                  className="btn btn-ghost btn-sm"
                  style={{ display: 'flex', alignItems: 'center', gap: 4, fontSize: 12, padding: '4px 8px' }}
                >
                  {copied ? <Check size={14} /> : <Copy size={14} />}
                  {copied ? 'Copied!' : 'Copy'}
                </button>
                <button
                  onClick={downloadCSV}
                  className="btn btn-ghost btn-sm"
                  style={{ display: 'flex', alignItems: 'center', gap: 4, fontSize: 12, padding: '4px 8px' }}
                >
                  <Download size={14} /> CSV
                </button>
              </div>
            )}
          </div>

          {/* Table References Modal with AG Grid */}
          {showTableRefs && result?.table_references && (
            <TableReferencesModal
              tableRefs={result.table_references}
              onClose={() => setShowTableRefs(false)}
            />
          )}

          {/* Results Grid - full height container for AG Grid */}
          <div style={{ flex: '1 1 0', overflow: 'hidden', minWidth: 0 }}>
            {error ? (
              <div style={{ padding: 20, color: 'var(--error)', display: 'flex', alignItems: 'center', gap: 8 }}>
                <AlertTriangle size={16} /> {error}
              </div>
            ) : result ? (
              <AgGridReact
                theme={gridTheme}
                columnDefs={columnDefs}
                rowData={rowData}
                defaultColDef={defaultColDef}
                rowHeight={36}
                headerHeight={40}
                animateRows={false}
                rowSelection="multiple"
                suppressRowClickSelection={true}
                enableCellTextSelection={true}
                ensureDomOrder={true}
                suppressCellFocus={false}
                pagination={result.row_count > 100}
                paginationPageSize={100}
                paginationPageSizeSelector={[50, 100, 200, 500]}
                autoSizeStrategy={{ type: 'fitCellContents' }}
              />
            ) : (
              <div style={{ padding: 40, textAlign: 'center', color: 'var(--text-muted)' }}>
                Run a query to see results
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
