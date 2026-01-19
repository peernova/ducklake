import React, { useState, useCallback, useRef, useEffect } from 'react';
import Editor, { type OnMount } from '@monaco-editor/react';
import type * as Monaco from 'monaco-editor';
import { AgGridReact } from 'ag-grid-react';
import { type ColDef, themeQuartz } from 'ag-grid-community';
import {
  Play,
  Clock,
  Database,
  GitBranch,
  Download,
  Copy,
  Check,
  ChevronDown,
  PanelLeftClose,
  PanelLeft,
  AlertTriangle,
  X,
  Search,
} from 'lucide-react';
import type { QueryResponse, ColumnInfo, Branch, Catalog } from '../types';
import { SchemaTree, type SchemaTreeRef } from '../components/SchemaTree';
import { catalogsApi, branchesApi } from '../api';


// Mock query execution
function mockExecuteQuery(sql: string): Promise<QueryResponse> {
  return new Promise((resolve) => {
    setTimeout(() => {
      // Simulate different results based on query
      if (sql.toLowerCase().includes('select')) {
        resolve({
          columns: [
            { name: 'id', type: 'INTEGER', nullable: false },
            { name: 'name', type: 'VARCHAR', nullable: true },
            { name: 'price', type: 'DECIMAL(10,2)', nullable: true },
            { name: 'created_at', type: 'TIMESTAMP', nullable: false },
          ],
          rows: [
            [1, 'Product A', 29.99, '2024-01-15T10:30:00Z'],
            [2, 'Product B', 49.99, '2024-01-15T11:00:00Z'],
            [3, 'Product C', 19.99, '2024-01-15T12:00:00Z'],
            [4, 'Product D', 99.99, '2024-01-15T13:00:00Z'],
            [5, 'Product E', 14.99, '2024-01-15T14:00:00Z'],
          ],
          row_count: 5,
          execution_time_ms: 45.2,
          branch: 'main',
          snapshot_id: 34,
        });
      } else {
        resolve({
          columns: [{ name: 'result', type: 'VARCHAR', nullable: true }],
          rows: [['Query executed successfully']],
          row_count: 1,
          execution_time_ms: 12.5,
        });
      }
    }, 500);
  });
}

const defaultQuery = `-- DuckLake SQL Query
-- Default schema: use table names directly
-- Other schemas: prefix with schema name (e.g., staging.temp_data)

SELECT p.*, s.status
FROM positions p                    -- uses default schema
JOIN staging.audit_log s            -- explicit schema
  ON p.trade_id = s.trade_id
WHERE p.trade_date >= '2024-01-01'
ORDER BY p.notional DESC
LIMIT 100;`;

// =============================================================================
// TODO: Missing Backend APIs (Add to Swagger/OpenAPI spec)
// =============================================================================
//
// 1. BRANCH DIFF API - GET /catalogs/{catalog_id}/branches/diff
//    Request: { base_branch, compare_branch, include_unchanged? }
//    Response: { schemas: SchemaDiff[], summary: { added, removed, modified } }
//    - Currently using client-side diff with mock data
//    - Backend can diff at snapshot level for accuracy
//    - See types: BranchDiffRequest, BranchDiffResponse
//
// 2. SQL PARSER/VALIDATOR API - POST /sql/parse
//    Request: { sql, dialect? }
//    Response: { valid, errors[], statement_type, tables_referenced[] }
//    - Security: validate queries before execution
//    - Permission checking based on tables_referenced
//    - See types: SQLParseRequest, SQLParseResponse
//
// 3. SQL FORMATTER API - POST /sql/format
//    Request: { sql, options? }
//    Response: { formatted_sql }
//    - DuckDB-aware formatting
//    - Currently no formatting in UI
//    - See types: SQLFormatRequest, SQLFormatResponse
//
// 4. SQL AUTOCOMPLETE API - POST /sql/completions
//    Request: { sql, cursor_position, branch?, schema? }
//    Response: { completions[], context }
//    - Server-side is better for large schemas
//    - Permission-aware suggestions
//    - Currently using client-side with mock schema data
//    - See types: SQLCompletionRequest, SQLCompletionResponse
//
// 5. SCHEMA BROWSER API - GET /catalogs/{catalog_id}/branches/{branch}/schemas
//    - Need branch-aware schema listing (currently faking with mock data)
//    - Should return full hierarchy: schemas -> tables -> columns
//
// =============================================================================
// TODO: Query History Feature
// =============================================================================
// - Store executed queries in localStorage with timestamp, duration, row count
// - Add "History" panel/drawer accessible from toolbar
// - Allow re-running queries from history
// - Support pinning/favoriting queries
// - Add search/filter for history entries
// - Consider syncing history across sessions via API
//
// =============================================================================
// TODO: Branch Comparison UI Improvements
// =============================================================================
// - Wire up to real BranchDiff API when available
// - Add data diff preview (sample rows that changed)
// - Export diff as migration script
// - Show diff statistics in branch list view

export default function Query() {
  const editorRef = useRef<any>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const schemaTreeRef = useRef<SchemaTreeRef>(null);
  const [hasSelection, setHasSelection] = useState(false);
  const [query, setQuery] = useState(defaultQuery);
  const [editorHeight, setEditorHeight] = useState(280);
  const [isResizing, setIsResizing] = useState(false);
  const [executing, setExecuting] = useState(false);
  const [result, setResult] = useState<QueryResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [selectedCatalog, setSelectedCatalog] = useState('');
  const [selectedBranch, setSelectedBranch] = useState('main');
  const [useCatalogContext, setUseCatalogContext] = useState(true);
  const [selectedSchema, setSelectedSchema] = useState('main');

  // Schema browser state - independent from query context
  const [sidebarOpen, setSidebarOpen] = useState(true);
  const [sidebarWidth, setSidebarWidth] = useState(280);
  const [isResizingSidebar, setIsResizingSidebar] = useState(false);
  const [browserCatalog, setBrowserCatalog] = useState('');
  const [browserBranch, setBrowserBranch] = useState('main');

  // Real data from API
  const [catalogs, setCatalogs] = useState<Catalog[]>([]);
  const [branches, setBranches] = useState<Branch[]>([]);
  const [catalogsLoading, setCatalogsLoading] = useState(true);
  const [branchesLoading, setBranchesLoading] = useState(false);

  // Branch dropdown state
  const [branchDropdownOpen, setBranchDropdownOpen] = useState(false);
  const [branchSearch, setBranchSearch] = useState('');
  const branchDropdownRef = useRef<HTMLDivElement>(null);

  // Load catalogs on mount
  useEffect(() => {
    const loadCatalogs = async () => {
      try {
        const data = await catalogsApi.list();
        const catalogList = Array.isArray(data) ? data : (data as { catalogs: Catalog[] }).catalogs || [];
        setCatalogs(catalogList);
        if (catalogList.length > 0) {
          const firstCatalog = catalogList[0].catalog_id;
          setSelectedCatalog(firstCatalog);
          setBrowserCatalog(firstCatalog);
        }
      } catch (err) {
        console.error('Failed to load catalogs:', err);
      } finally {
        setCatalogsLoading(false);
      }
    };
    loadCatalogs();
  }, []);

  // Load branches when catalog changes
  useEffect(() => {
    if (!browserCatalog) return;
    const loadBranches = async () => {
      setBranchesLoading(true);
      try {
        const data = await branchesApi.list(browserCatalog);
        const branchList = Array.isArray(data) ? data : (data as { branches: Branch[] }).branches || [];
        setBranches(branchList);
        // Set default branch to main if exists, otherwise first branch
        const mainBranch = branchList.find(b => b.branch_name === 'main');
        const defaultBranch = mainBranch?.branch_name || branchList[0]?.branch_name || 'main';
        setBrowserBranch(defaultBranch);
        if (!selectedBranch || selectedBranch === 'main') {
          setSelectedBranch(defaultBranch);
        }
      } catch (err) {
        console.error('Failed to load branches:', err);
      } finally {
        setBranchesLoading(false);
      }
    };
    loadBranches();
  }, [browserCatalog]);

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

  // Sidebar resize - using refs to avoid closure issues
  const isResizingRef = useRef(false);

  const handleSidebarMouseDown = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    isResizingRef.current = true;
    setIsResizingSidebar(true);
    document.body.style.cursor = 'col-resize';
    document.body.style.userSelect = 'none';

    const handleMouseMove = (moveEvent: MouseEvent) => {
      if (!isResizingRef.current) return;
      const newWidth = Math.min(500, Math.max(200, moveEvent.clientX));
      setSidebarWidth(newWidth);
    };

    const handleMouseUp = () => {
      isResizingRef.current = false;
      setIsResizingSidebar(false);
      document.body.style.cursor = '';
      document.body.style.userSelect = '';
      document.removeEventListener('mousemove', handleMouseMove);
      document.removeEventListener('mouseup', handleMouseUp);
    };

    document.addEventListener('mousemove', handleMouseMove);
    document.addEventListener('mouseup', handleMouseUp);
  }, []);

  // Filter active branches for dropdown
  const activeBranches = branches.filter(b => b.status === 'active');
  const filteredBranches = activeBranches.filter(b =>
    b.branch_name.toLowerCase().includes(branchSearch.toLowerCase())
  );

  // Check if browser context differs from query context
  const contextMismatch = useCatalogContext && (
    browserCatalog !== selectedCatalog || browserBranch !== selectedBranch
  );

  // Sync browser to query context
  const syncBrowserToQuery = () => {
    setBrowserCatalog(selectedCatalog);
    setBrowserBranch(selectedBranch);
  };

  const insertText = (text: string) => {
    if (editorRef.current) {
      const editor = editorRef.current;
      const selection = editor.getSelection();
      const op = { range: selection, text, forceMoveMarkers: true };
      editor.executeEdits('insert', [op]);
      editor.focus();
    }
  };

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
      const selection = e.selection;
      const hasText = !selection.isEmpty();
      setHasSelection(hasText);
    });

    // Add Cmd/Ctrl+Enter to run query (smart - runs selected if there's a selection)
    editor.addAction({
      id: 'run-query',
      label: 'Run Query',
      keybindings: [monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter],
      run: () => {
        const selection = editor.getSelection();
        const hasSelectedText = selection ? !selection.isEmpty() : false;
        executeQuery(hasSelectedText);
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

    // Register SQL completion provider with schema awareness
    const completionProvider = monaco.languages.registerCompletionItemProvider('sql', {
      provideCompletionItems: (model: Monaco.editor.ITextModel, position: Monaco.Position) => {
        const word = model.getWordUntilPosition(position);
        const range = {
          startLineNumber: position.lineNumber,
          endLineNumber: position.lineNumber,
          startColumn: word.startColumn,
          endColumn: word.endColumn,
        };

        const suggestions: Monaco.languages.CompletionItem[] = [];

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
          });
        });

        // Note: Schema/table/column suggestions can be added via a backend API
        // For now, use the Schema Browser sidebar to click-to-insert

        return { suggestions };
      },
    });

    // Cleanup on unmount
    return () => {
      completionProvider.dispose();
    };
  };

  const executeQuery = useCallback(async (selectedOnly = false) => {
    setExecuting(true);
    setError(null);
    try {
      let queryToRun = query;

      // If running selected, get the selected text from editor
      if (selectedOnly && editorRef.current) {
        const selection = editorRef.current.getSelection();
        const selectedText = editorRef.current.getModel()?.getValueInRange(selection);
        if (selectedText && selectedText.trim()) {
          queryToRun = selectedText;
        }
      }

      const response = await mockExecuteQuery(queryToRun);
      setResult(response);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Query execution failed');
      setResult(null);
    } finally {
      setExecuting(false);
    }
  }, [query]);

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') {
        e.preventDefault();
        executeQuery();
      }
    },
    [executeQuery]
  );

  // Resize handlers
  const handleResizeStart = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    setIsResizing(true);
  }, []);

  const handleResizeMove = useCallback((e: MouseEvent) => {
    if (!isResizing || !containerRef.current) return;

    const containerRect = containerRef.current.getBoundingClientRect();
    const containerHeight = containerRect.height;
    const maxHeight = containerHeight * 0.5; // Max 50%
    const minHeight = 150;

    const newHeight = e.clientY - containerRect.top - 56; // 56 = toolbar height
    setEditorHeight(Math.max(minHeight, Math.min(maxHeight, newHeight)));
  }, [isResizing]);

  const handleResizeEnd = useCallback(() => {
    setIsResizing(false);
  }, []);

  // Global mouse event listeners for resize
  useEffect(() => {
    if (isResizing) {
      document.addEventListener('mousemove', handleResizeMove);
      document.addEventListener('mouseup', handleResizeEnd);
      document.body.style.cursor = 'row-resize';
      document.body.style.userSelect = 'none';
    }
    return () => {
      document.removeEventListener('mousemove', handleResizeMove);
      document.removeEventListener('mouseup', handleResizeEnd);
      document.body.style.cursor = '';
      document.body.style.userSelect = '';
    };
  }, [isResizing, handleResizeMove, handleResizeEnd]);

  // Convert result columns to AG Grid column definitions
  const columnDefs: ColDef[] = result
    ? result.columns.map((col: ColumnInfo) => ({
        field: col.name,
        headerName: col.name,
        sortable: true,
        filter: true,
        resizable: true,
        minWidth: 100,
      }))
    : [];

  // Convert result rows to AG Grid row data
  const rowData = result
    ? result.rows.map((row, index) => {
        const obj: Record<string, unknown> = { _id: index };
        result.columns.forEach((col, colIndex) => {
          obj[col.name] = row[colIndex];
        });
        return obj;
      })
    : [];

  const copyResults = () => {
    if (!result) return;
    const header = result.columns.map((c) => c.name).join('\t');
    const rows = result.rows.map((row) => row.join('\t')).join('\n');
    navigator.clipboard.writeText(`${header}\n${rows}`);
  };

  const downloadCSV = () => {
    if (!result) return;
    const header = result.columns.map((c) => c.name).join(',');
    const rows = result.rows
      .map((row) =>
        row
          .map((cell) => {
            if (cell === null) return '';
            if (typeof cell === 'string' && cell.includes(',')) {
              return `"${cell.replace(/"/g, '""')}"`;
            }
            return String(cell);
          })
          .join(',')
      )
      .join('\n');
    const csv = `${header}\n${rows}`;
    const blob = new Blob([csv], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'query_results.csv';
    a.click();
    URL.revokeObjectURL(url);
  };

  // Handle table/column selection from SchemaTree for insertion
  const handleSelectTable = useCallback((schemaName: string, tableName: string) => {
    const text = schemaName === selectedSchema ? tableName : `${schemaName}.${tableName}`;
    insertText(text);
  }, [selectedSchema, insertText]);

  const handleSelectColumn = useCallback((_schemaName: string, _tableName: string, columnName: string) => {
    insertText(columnName);
  }, [insertText]);

  return (
    <div
      className="query-container"
      onKeyDown={handleKeyDown}
      style={{ cursor: isResizingSidebar ? 'col-resize' : undefined }}
    >
      {/* Schema Browser Sidebar */}
      <div
        className="schema-browser"
        style={{
          width: sidebarOpen ? `${sidebarWidth}px` : '40px',
          minWidth: sidebarOpen ? '200px' : '40px',
          background: 'var(--bg-secondary)',
          display: 'flex',
          flexDirection: 'column',
          overflow: 'hidden',
          flexShrink: 0,
        }}
      >
        <div
          style={{
            padding: '12px',
            borderBottom: '1px solid var(--border-light)',
            display: 'flex',
            alignItems: 'center',
            justifyContent: sidebarOpen ? 'space-between' : 'center',
          }}
        >
          {sidebarOpen && (
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <Database size={16} className="text-muted" />
              <span style={{ fontWeight: 600, fontSize: '13px' }}>Schema Browser</span>
            </div>
          )}
          <button
            className="btn btn-ghost btn-icon btn-sm"
            onClick={() => setSidebarOpen(!sidebarOpen)}
            title={sidebarOpen ? 'Collapse sidebar' : 'Expand sidebar'}
          >
            {sidebarOpen ? <PanelLeftClose size={16} /> : <PanelLeft size={16} />}
          </button>
        </div>

        {sidebarOpen && (
          <>
            {/* Catalog selector */}
            <div style={{ padding: '8px 12px', borderBottom: '1px solid var(--border-light)' }}>
              <label style={{ fontSize: '11px', color: 'var(--text-muted)', textTransform: 'uppercase', fontWeight: 600, marginBottom: '4px', display: 'block' }}>
                Catalog
              </label>
              <select
                className="form-input form-select"
                style={{ width: '100%', padding: '6px 10px', fontSize: '12px' }}
                value={browserCatalog}
                onChange={(e) => setBrowserCatalog(e.target.value)}
                disabled={catalogsLoading}
              >
                {catalogsLoading ? (
                  <option>Loading...</option>
                ) : catalogs.length === 0 ? (
                  <option>No catalogs</option>
                ) : (
                  catalogs.map(c => (
                    <option key={c.catalog_id} value={c.catalog_id}>
                      {c.display_name || c.catalog_id}
                    </option>
                  ))
                )}
              </select>
            </div>

            {/* Searchable branch selector */}
            <div style={{ padding: '8px 12px', borderBottom: '1px solid var(--border-light)' }}>
              <label style={{ fontSize: '11px', color: 'var(--text-muted)', textTransform: 'uppercase', fontWeight: 600, marginBottom: '4px', display: 'block' }}>
                Branch
              </label>
              <div ref={branchDropdownRef} style={{ position: 'relative' }}>
                <div
                  onClick={() => setBranchDropdownOpen(!branchDropdownOpen)}
                  style={{
                    display: 'flex',
                    alignItems: 'center',
                    gap: '8px',
                    padding: '6px 10px',
                    background: 'var(--bg-primary)',
                    border: '1px solid var(--border-color)',
                    borderRadius: '6px',
                    cursor: branchesLoading ? 'wait' : 'pointer',
                    transition: 'border-color 0.15s ease',
                  }}
                  onMouseEnter={(e) => e.currentTarget.style.borderColor = 'var(--accent-primary)'}
                  onMouseLeave={(e) => e.currentTarget.style.borderColor = 'var(--border-color)'}
                >
                  <GitBranch size={14} style={{ color: 'var(--accent-primary)', flexShrink: 0 }} />
                  <span style={{ flex: 1, fontSize: '12px', fontWeight: 500, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                    {branchesLoading ? 'Loading...' : browserBranch}
                  </span>
                  <ChevronDown size={14} style={{ color: 'var(--text-muted)', flexShrink: 0, transform: branchDropdownOpen ? 'rotate(180deg)' : 'none', transition: 'transform 0.15s ease' }} />
                </div>

                {/* Dropdown */}
                {branchDropdownOpen && !branchesLoading && (
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
                              setBrowserBranch(b.branch_name);
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
                              background: b.branch_name === browserBranch ? 'var(--accent-secondary)' : 'transparent',
                              borderLeft: b.branch_name === browserBranch ? '2px solid var(--accent-primary)' : '2px solid transparent',
                            }}
                            onMouseEnter={(e) => {
                              if (b.branch_name !== browserBranch) e.currentTarget.style.background = 'var(--bg-secondary)';
                            }}
                            onMouseLeave={(e) => {
                              if (b.branch_name !== browserBranch) e.currentTarget.style.background = 'transparent';
                            }}
                          >
                            <GitBranch size={12} style={{ color: b.branch_name === 'main' ? 'var(--accent-primary)' : 'var(--text-muted)' }} />
                            <span style={{ flex: 1, fontWeight: b.branch_name === browserBranch ? 500 : 400 }}>{b.branch_name}</span>
                            {b.branch_name === browserBranch && (
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

            {/* Warning when browser context differs from query context */}
            {contextMismatch && (
              <div
                style={{
                  padding: '6px 12px',
                  background: 'var(--warning-bg)',
                  display: 'flex',
                  alignItems: 'center',
                  gap: '6px',
                  fontSize: '11px',
                  color: '#92400e',
                }}
              >
                <AlertTriangle size={12} />
                <span>Viewing different context</span>
                <button
                  onClick={syncBrowserToQuery}
                  style={{
                    marginLeft: 'auto',
                    padding: '2px 6px',
                    fontSize: '10px',
                    background: 'white',
                    border: '1px solid #d97706',
                    borderRadius: '3px',
                    cursor: 'pointer',
                    color: '#92400e',
                  }}
                >
                  Sync
                </button>
              </div>
            )}

            {/* SchemaTree - with click to insert */}
            <div style={{ flex: 1, overflow: 'hidden' }}>
              {browserCatalog ? (
                <SchemaTree
                  ref={schemaTreeRef}
                  catalogId={browserCatalog}
                  branch={browserBranch}
                  height={500}
                  onSelectTable={handleSelectTable}
                  onSelectColumn={handleSelectColumn}
                />
              ) : (
                <div style={{ padding: '20px', textAlign: 'center', color: 'var(--text-muted)', fontSize: '12px' }}>
                  Select a catalog to browse schemas
                </div>
              )}
            </div>
          </>
        )}

      </div>

      {/* Draggable Divider - outside sidebar for better event handling */}
      {sidebarOpen && (
        <div
          onMouseDown={handleSidebarMouseDown}
          className="sidebar-divider"
          style={{
            width: '6px',
            cursor: 'col-resize',
            background: isResizingSidebar ? 'var(--accent-primary)' : 'var(--border-light)',
            flexShrink: 0,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            transition: isResizingSidebar ? 'none' : 'background 0.15s ease',
          }}
          onMouseEnter={(e) => {
            if (!isResizingSidebar) e.currentTarget.style.background = 'var(--accent-secondary)';
          }}
          onMouseLeave={(e) => {
            if (!isResizingSidebar) e.currentTarget.style.background = 'var(--border-light)';
          }}
        >
          <div style={{
            width: '2px',
            height: '40px',
            background: isResizingSidebar ? '#fff' : 'var(--text-muted)',
            borderRadius: '2px',
            opacity: 0.6,
          }} />
        </div>
      )}

      {/* Main Query Area */}
      <div ref={containerRef} style={{ flex: 1, display: 'flex', flexDirection: 'column', minWidth: 0 }}>
      <div className="query-editor-wrapper" style={{ flexShrink: 0 }}>
        <div className="query-toolbar">
          <div className="query-toolbar-left">
            <label
              className="context-toggle"
              style={{
                display: 'flex',
                alignItems: 'center',
                gap: '8px',
                cursor: 'pointer',
                padding: '6px 12px',
                borderRadius: 'var(--radius-md)',
                background: useCatalogContext ? 'rgba(14, 165, 233, 0.1)' : 'transparent',
                border: `1px solid ${useCatalogContext ? 'var(--accent-secondary)' : 'var(--border-light)'}`,
                transition: 'all var(--transition-fast)',
              }}
            >
              <div
                style={{
                  width: '16px',
                  height: '16px',
                  borderRadius: '3px',
                  border: `2px solid ${useCatalogContext ? 'var(--accent-secondary)' : 'var(--border-medium)'}`,
                  background: useCatalogContext ? 'var(--accent-secondary)' : 'transparent',
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  transition: 'all var(--transition-fast)',
                }}
              >
                {useCatalogContext && <Check size={12} color="white" strokeWidth={3} />}
              </div>
              <input
                type="checkbox"
                checked={useCatalogContext}
                onChange={(e) => setUseCatalogContext(e.target.checked)}
                style={{ display: 'none' }}
              />
              <span style={{ fontSize: '13px', fontWeight: 500 }}>Use Catalog</span>
            </label>

            <div className="flex items-center gap-2" style={{ opacity: useCatalogContext ? 1 : 0.5 }}>
              <Database size={16} className="text-muted" />
              <select
                className="form-input form-select"
                style={{ width: '140px', padding: '6px 12px' }}
                value={selectedCatalog}
                onChange={(e) => setSelectedCatalog(e.target.value)}
                disabled={!useCatalogContext || catalogsLoading}
              >
                {catalogs.map(c => (
                  <option key={c.catalog_id} value={c.catalog_id}>
                    {c.display_name || c.catalog_id}
                  </option>
                ))}
              </select>
            </div>

            <div className="flex items-center gap-2" style={{ opacity: useCatalogContext ? 1 : 0.5 }}>
              <span className="text-muted" style={{ fontSize: '11px' }}>default:</span>
              <select
                className="form-input form-select"
                style={{ width: '100px', padding: '6px 12px' }}
                value={selectedSchema}
                onChange={(e) => setSelectedSchema(e.target.value)}
                disabled={!useCatalogContext}
                title="Default schema - other schemas can be accessed with schema.table syntax"
              >
                <option value="main">main</option>
                <option value="staging">staging</option>
                <option value="archive">archive</option>
              </select>
            </div>

            <div className="flex items-center gap-2" style={{ opacity: useCatalogContext ? 1 : 0.5 }}>
              <GitBranch size={16} className="text-muted" />
              <select
                className="form-input form-select"
                style={{ width: '150px', padding: '6px 12px' }}
                value={selectedBranch}
                onChange={(e) => setSelectedBranch(e.target.value)}
                disabled={!useCatalogContext || branchesLoading}
              >
                {activeBranches.map(b => (
                  <option key={b.branch_id} value={b.branch_name}>
                    {b.branch_name}
                  </option>
                ))}
              </select>
            </div>

            {useCatalogContext && (
              <div
                className="font-mono text-sm"
                style={{
                  padding: '6px 10px',
                  background: 'var(--bg-tertiary)',
                  borderRadius: 'var(--radius-md)',
                  color: 'var(--text-secondary)',
                }}
              >
                {selectedCatalog}.{selectedSchema} @ <span style={{ color: 'var(--accent-secondary)' }}>{selectedBranch}</span>
              </div>
            )}
          </div>
          <div className="query-toolbar-right">
            <span className="text-sm text-muted" style={{ fontSize: '11px', opacity: 0.7 }}>
              ⌘↵
            </span>
            <button
              className="btn btn-primary"
              onClick={() => executeQuery(hasSelection)}
              disabled={executing}
              title={hasSelection ? "Run selected (Cmd/Ctrl+Enter)" : "Run query (Cmd/Ctrl+Enter)"}
            >
              {executing ? (
                <div className="spinner" style={{ width: '16px', height: '16px', borderWidth: '2px' }} />
              ) : (
                <Play size={16} />
              )}
              {hasSelection ? 'Run Selected' : 'Run'}
            </button>
          </div>
        </div>
        <Editor
          height={`${editorHeight}px`}
          defaultLanguage="sql"
          value={query}
          onChange={(value) => setQuery(value || '')}
          onMount={handleEditorMount}
          options={{
            minimap: { enabled: false },
            fontSize: 14,
            fontFamily: "'JetBrains Mono', 'Fira Code', 'SF Mono', Consolas, monospace",
            fontLigatures: true,
            lineNumbers: 'on',
            lineNumbersMinChars: 3,
            scrollBeyondLastLine: false,
            automaticLayout: true,
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
            smoothScrolling: true,
            cursorBlinking: 'smooth',
            cursorSmoothCaretAnimation: 'on',
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
        {/* Resize Handle */}
        <div
          onMouseDown={handleResizeStart}
          style={{
            height: '6px',
            background: isResizing ? 'var(--accent-secondary)' : 'var(--border-light)',
            cursor: 'row-resize',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            transition: 'background 0.15s',
          }}
          onMouseEnter={(e) => (e.currentTarget.style.background = 'var(--accent-secondary)')}
          onMouseLeave={(e) => !isResizing && (e.currentTarget.style.background = 'var(--border-light)')}
        >
          <div style={{
            width: '40px',
            height: '3px',
            borderRadius: '2px',
            background: isResizing ? 'white' : 'var(--text-muted)',
            opacity: 0.5,
          }} />
        </div>
      </div>

      <div className="query-results">
        <div className="query-results-header">
          <div className="query-results-stats">
            {result && (
              <>
                <span>
                  <strong>{result.row_count}</strong> rows
                </span>
                <span className="flex items-center gap-1">
                  <Clock size={14} />
                  {result.execution_time_ms.toFixed(1)} ms
                </span>
                {result.branch && (
                  <span className="flex items-center gap-1">
                    <GitBranch size={14} />
                    {result.branch}
                  </span>
                )}
              </>
            )}
            {error && <span style={{ color: 'var(--error)' }}>{error}</span>}
            {!result && !error && <span>Run a query to see results</span>}
          </div>
          {result && (
            <div className="flex gap-2">
              <button className="btn btn-ghost btn-sm" onClick={copyResults}>
                <Copy size={14} />
                Copy
              </button>
              <button className="btn btn-ghost btn-sm" onClick={downloadCSV}>
                <Download size={14} />
                CSV
              </button>
            </div>
          )}
        </div>
        <div className="query-results-body">
          {result && (
            <AgGridReact
              theme={themeQuartz}
              columnDefs={columnDefs}
              rowData={rowData}
              defaultColDef={{
                sortable: true,
                filter: true,
                resizable: true,
              }}
              animateRows={true}
              rowSelection="multiple"
              suppressRowClickSelection={true}
            />
          )}
        </div>
      </div>
      </div> {/* End Main Query Area */}
    </div>
  );
}
