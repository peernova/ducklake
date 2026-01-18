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
  ChevronRight,
  ChevronDown,
  Table,
  FolderOpen,
  Folder,
  Type,
  PanelLeftClose,
  PanelLeft,
  GitCompare,
  AlertTriangle,
  X,
  Plus,
  Minus,
  Search,
  Filter,
  RefreshCw,
  Columns,
  Rows,
} from 'lucide-react';
import type { QueryResponse, ColumnInfo } from '../types';

// Schema browser types
interface TableColumn {
  name: string;
  type: string;
}

interface TableDef {
  name: string;
  columns: TableColumn[];
}

interface SchemaDef {
  name: string;
  tables: TableDef[];
}

// Mock schema data - keyed by catalog:branch
const mockSchemaData: Record<string, SchemaDef[]> = {
  'xva_desk:main': [
    {
      name: 'main',
      tables: [
        { name: 'positions', columns: [
          { name: 'trade_id', type: 'VARCHAR' },
          { name: 'trade_date', type: 'DATE' },
          { name: 'instrument', type: 'VARCHAR' },
          { name: 'notional', type: 'DECIMAL(18,2)' },
          { name: 'counterparty', type: 'VARCHAR' },
        ]},
        { name: 'trades', columns: [
          { name: 'id', type: 'BIGINT' },
          { name: 'trade_date', type: 'DATE' },
          { name: 'product_type', type: 'VARCHAR' },
          { name: 'quantity', type: 'INTEGER' },
        ]},
        { name: 'counterparties', columns: [
          { name: 'id', type: 'BIGINT' },
          { name: 'name', type: 'VARCHAR' },
          { name: 'rating', type: 'VARCHAR' },
        ]},
      ],
    },
    {
      name: 'staging',
      tables: [
        { name: 'audit_log', columns: [
          { name: 'trade_id', type: 'VARCHAR' },
          { name: 'status', type: 'VARCHAR' },
          { name: 'updated_at', type: 'TIMESTAMP' },
        ]},
        { name: 'temp_imports', columns: [
          { name: 'row_id', type: 'BIGINT' },
          { name: 'data', type: 'JSON' },
        ]},
      ],
    },
  ],
  'xva_desk:feature_new_model': [
    {
      name: 'main',
      tables: [
        { name: 'positions', columns: [
          { name: 'trade_id', type: 'VARCHAR' },
          { name: 'trade_date', type: 'DATE' },
          { name: 'instrument', type: 'VARCHAR' },
          { name: 'notional', type: 'DECIMAL(18,2)' },
          { name: 'counterparty', type: 'VARCHAR' },
          { name: 'model_version', type: 'VARCHAR' },  // New column in this branch
        ]},
        { name: 'trades', columns: [
          { name: 'id', type: 'BIGINT' },
          { name: 'trade_date', type: 'DATE' },
          { name: 'product_type', type: 'VARCHAR' },
          { name: 'quantity', type: 'INTEGER' },
        ]},
        { name: 'model_params', columns: [  // New table in this branch
          { name: 'param_id', type: 'BIGINT' },
          { name: 'param_name', type: 'VARCHAR' },
          { name: 'param_value', type: 'DOUBLE' },
        ]},
      ],
    },
    {
      name: 'staging',
      tables: [
        { name: 'audit_log', columns: [
          { name: 'trade_id', type: 'VARCHAR' },
          { name: 'status', type: 'VARCHAR' },
          { name: 'updated_at', type: 'TIMESTAMP' },
        ]},
      ],
    },
  ],
  'market_data:main': [
    {
      name: 'main',
      tables: [
        { name: 'prices', columns: [
          { name: 'instrument_id', type: 'VARCHAR' },
          { name: 'price_date', type: 'DATE' },
          { name: 'price', type: 'DECIMAL(18,6)' },
          { name: 'currency', type: 'VARCHAR' },
        ]},
        { name: 'instruments', columns: [
          { name: 'id', type: 'VARCHAR' },
          { name: 'name', type: 'VARCHAR' },
          { name: 'asset_class', type: 'VARCHAR' },
        ]},
      ],
    },
  ],
};

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
  const [hasSelection, setHasSelection] = useState(false);
  const [query, setQuery] = useState(defaultQuery);
  const [editorHeight, setEditorHeight] = useState(280);
  const [isResizing, setIsResizing] = useState(false);
  const [executing, setExecuting] = useState(false);
  const [result, setResult] = useState<QueryResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [selectedCatalog, setSelectedCatalog] = useState('xva_desk');
  const [selectedBranch, setSelectedBranch] = useState('main');
  const [useCatalogContext, setUseCatalogContext] = useState(true);
  const [selectedSchema, setSelectedSchema] = useState('main');

  // Schema browser state - independent from query context
  const [sidebarOpen, setSidebarOpen] = useState(true);
  const [browserCatalog, setBrowserCatalog] = useState('xva_desk');
  const [browserBranch, setBrowserBranch] = useState('main');
  const [expandedSchemas, setExpandedSchemas] = useState<Set<string>>(new Set(['main']));
  const [expandedTables, setExpandedTables] = useState<Set<string>>(new Set());

  // Compare mode state
  const [showCompareModal, setShowCompareModal] = useState(false);
  const [compareCatalog1, setCompareCatalog1] = useState('xva_desk');
  const [compareBranch1, setCompareBranch1] = useState('main');
  const [compareCatalog2, setCompareCatalog2] = useState('xva_desk');
  const [compareBranch2, setCompareBranch2] = useState('feature_new_model');
  const [compareSearch, setCompareSearch] = useState('');
  const [compareFilterLevel, setCompareFilterLevel] = useState<'all' | 'schemas' | 'tables' | 'columns'>('all');
  const [compareShowOnlyDiffs, setCompareShowOnlyDiffs] = useState(false);
  const [compareSplitView, setCompareSplitView] = useState(true);
  const [compareExpandedSchemas, setCompareExpandedSchemas] = useState<Set<string>>(new Set());
  const [compareExpandedTables, setCompareExpandedTables] = useState<Set<string>>(new Set());

  // Get schemas for compare mode (needed for computeDiff)
  const compareKey1 = `${compareCatalog1}:${compareBranch1}`;
  const compareKey2 = `${compareCatalog2}:${compareBranch2}`;
  const compareSchemas1 = mockSchemaData[compareKey1] || mockSchemaData[`${compareCatalog1}:main`] || [];
  const compareSchemas2 = mockSchemaData[compareKey2] || mockSchemaData[`${compareCatalog2}:main`] || [];

  // Compute unified diff structure
  const computeDiff = () => {
    const allSchemaNames = new Set([
      ...compareSchemas1.map(s => s.name),
      ...compareSchemas2.map(s => s.name),
    ]);

    const diff: Array<{
      schemaName: string;
      status: 'same' | 'left-only' | 'right-only' | 'modified';
      tables: Array<{
        tableName: string;
        status: 'same' | 'left-only' | 'right-only' | 'modified';
        leftCols: TableColumn[];
        rightCols: TableColumn[];
        columns: Array<{
          colName: string;
          colType: string;
          status: 'same' | 'left-only' | 'right-only' | 'type-changed';
          leftType?: string;
          rightType?: string;
        }>;
      }>;
    }> = [];

    allSchemaNames.forEach(schemaName => {
      const schema1 = compareSchemas1.find(s => s.name === schemaName);
      const schema2 = compareSchemas2.find(s => s.name === schemaName);

      const schemaStatus = !schema1 ? 'right-only' : !schema2 ? 'left-only' : 'same';

      const allTableNames = new Set([
        ...(schema1?.tables.map(t => t.name) || []),
        ...(schema2?.tables.map(t => t.name) || []),
      ]);

      const tables: typeof diff[0]['tables'] = [];
      let hasTableDiff = false;

      allTableNames.forEach(tableName => {
        const table1 = schema1?.tables.find(t => t.name === tableName);
        const table2 = schema2?.tables.find(t => t.name === tableName);

        const allColNames = new Set([
          ...(table1?.columns.map(c => c.name) || []),
          ...(table2?.columns.map(c => c.name) || []),
        ]);

        const columns: typeof tables[0]['columns'] = [];
        let hasColDiff = false;

        allColNames.forEach(colName => {
          const col1 = table1?.columns.find(c => c.name === colName);
          const col2 = table2?.columns.find(c => c.name === colName);

          let colStatus: 'same' | 'left-only' | 'right-only' | 'type-changed' = 'same';
          if (!col1) colStatus = 'right-only';
          else if (!col2) colStatus = 'left-only';
          else if (col1.type !== col2.type) colStatus = 'type-changed';

          if (colStatus !== 'same') hasColDiff = true;

          columns.push({
            colName,
            colType: col1?.type || col2?.type || '',
            status: colStatus,
            leftType: col1?.type,
            rightType: col2?.type,
          });
        });

        let tableStatus: 'same' | 'left-only' | 'right-only' | 'modified' = 'same';
        if (!table1) tableStatus = 'right-only';
        else if (!table2) tableStatus = 'left-only';
        else if (hasColDiff) tableStatus = 'modified';

        if (tableStatus !== 'same') hasTableDiff = true;

        tables.push({
          tableName,
          status: tableStatus,
          leftCols: table1?.columns || [],
          rightCols: table2?.columns || [],
          columns: columns.sort((a, b) => a.colName.localeCompare(b.colName)),
        });
      });

      diff.push({
        schemaName,
        status: schemaStatus === 'same' && hasTableDiff ? 'modified' : schemaStatus,
        tables: tables.sort((a, b) => a.tableName.localeCompare(b.tableName)),
      });
    });

    return diff.sort((a, b) => a.schemaName.localeCompare(b.schemaName));
  };

  const diffData = showCompareModal ? computeDiff() : [];

  // Filter diff based on search and level
  const filteredDiff = diffData.filter(schema => {
    const searchLower = compareSearch.toLowerCase();
    if (!searchLower) {
      return compareShowOnlyDiffs ? schema.status !== 'same' : true;
    }

    if (compareFilterLevel === 'schemas' || compareFilterLevel === 'all') {
      if (schema.schemaName.toLowerCase().includes(searchLower)) {
        return compareShowOnlyDiffs ? schema.status !== 'same' : true;
      }
    }
    if (compareFilterLevel === 'tables' || compareFilterLevel === 'all') {
      if (schema.tables.some(t => t.tableName.toLowerCase().includes(searchLower))) {
        return true;
      }
    }
    if (compareFilterLevel === 'columns' || compareFilterLevel === 'all') {
      if (schema.tables.some(t => t.columns.some(c => c.colName.toLowerCase().includes(searchLower)))) {
        return true;
      }
    }
    return false;
  }).map(schema => {
    if (!compareSearch) return schema;
    const searchLower = compareSearch.toLowerCase();

    // Filter tables based on search
    const filteredTables = schema.tables.filter(table => {
      if (compareFilterLevel === 'schemas') return true;
      if (compareFilterLevel === 'tables' || compareFilterLevel === 'all') {
        if (table.tableName.toLowerCase().includes(searchLower)) return true;
      }
      if (compareFilterLevel === 'columns' || compareFilterLevel === 'all') {
        if (table.columns.some(c => c.colName.toLowerCase().includes(searchLower))) return true;
      }
      return false;
    });

    return { ...schema, tables: filteredTables };
  });

  // Check if browser context differs from query context
  const contextMismatch = useCatalogContext && (
    browserCatalog !== selectedCatalog || browserBranch !== selectedBranch
  );

  // Get schemas for browser's catalog:branch
  const browserKey = `${browserCatalog}:${browserBranch}`;
  const schemas = mockSchemaData[browserKey] || mockSchemaData[`${browserCatalog}:main`] || [];

  // Sync browser to query context
  const syncBrowserToQuery = () => {
    setBrowserCatalog(selectedCatalog);
    setBrowserBranch(selectedBranch);
  };

  const toggleSchema = (schemaName: string) => {
    setExpandedSchemas(prev => {
      const next = new Set(prev);
      if (next.has(schemaName)) {
        next.delete(schemaName);
      } else {
        next.add(schemaName);
      }
      return next;
    });
  };

  const toggleTable = (tableKey: string) => {
    setExpandedTables(prev => {
      const next = new Set(prev);
      if (next.has(tableKey)) {
        next.delete(tableKey);
      } else {
        next.add(tableKey);
      }
      return next;
    });
  };

  // Compare modal toggle functions
  const toggleCompareSchema = (schemaName: string) => {
    setCompareExpandedSchemas(prev => {
      const next = new Set(prev);
      if (next.has(schemaName)) {
        next.delete(schemaName);
      } else {
        next.add(schemaName);
      }
      return next;
    });
  };

  const toggleCompareTable = (tableKey: string) => {
    setCompareExpandedTables(prev => {
      const next = new Set(prev);
      if (next.has(tableKey)) {
        next.delete(tableKey);
      } else {
        next.add(tableKey);
      }
      return next;
    });
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

        // Add tables and columns from current schema
        const browserKey = `${browserCatalog}:${browserBranch}`;
        const currentSchemas = mockSchemaData[browserKey] || mockSchemaData[`${browserCatalog}:main`] || [];

        currentSchemas.forEach(schema => {
          // Add schema name
          suggestions.push({
            label: schema.name,
            kind: monaco.languages.CompletionItemKind.Module,
            insertText: schema.name,
            detail: `Schema (${schema.tables.length} tables)`,
            range,
          });

          schema.tables.forEach(table => {
            // Add table name
            suggestions.push({
              label: table.name,
              kind: monaco.languages.CompletionItemKind.Class,
              insertText: table.name,
              detail: `Table in ${schema.name} (${table.columns.length} columns)`,
              range,
            });

            // Add fully qualified table name
            suggestions.push({
              label: `${schema.name}.${table.name}`,
              kind: monaco.languages.CompletionItemKind.Class,
              insertText: `${schema.name}.${table.name}`,
              detail: `${table.columns.length} columns`,
              range,
            });

            // Add columns
            table.columns.forEach(col => {
              suggestions.push({
                label: col.name,
                kind: monaco.languages.CompletionItemKind.Field,
                insertText: col.name,
                detail: `${col.type} (${table.name})`,
                range,
              });
            });
          });
        });

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

  return (
    <div className="query-container" onKeyDown={handleKeyDown}>
      {/* Schema Browser Sidebar */}
      <div
        className="schema-browser"
        style={{
          width: sidebarOpen ? '260px' : '40px',
          minWidth: sidebarOpen ? '260px' : '40px',
          background: 'var(--bg-secondary)',
          borderRight: '1px solid var(--border-light)',
          display: 'flex',
          flexDirection: 'column',
          transition: 'all var(--transition-fast)',
          overflow: 'hidden',
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
            {/* Browser's own catalog/branch selectors */}
            <div style={{ padding: '8px 12px', borderBottom: '1px solid var(--border-light)' }}>
              <div style={{ display: 'flex', gap: '6px', marginBottom: '6px' }}>
                <select
                  className="form-input form-select"
                  style={{ flex: 1, padding: '4px 8px', fontSize: '12px' }}
                  value={browserCatalog}
                  onChange={(e) => setBrowserCatalog(e.target.value)}
                >
                  <option value="xva_desk">xva_desk</option>
                  <option value="market_data">market_data</option>
                  <option value="analytics">analytics</option>
                </select>
              </div>
              <div style={{ display: 'flex', gap: '6px', alignItems: 'center' }}>
                <GitBranch size={12} className="text-muted" />
                <select
                  className="form-input form-select"
                  style={{ flex: 1, padding: '4px 8px', fontSize: '12px' }}
                  value={browserBranch}
                  onChange={(e) => setBrowserBranch(e.target.value)}
                >
                  <option value="main">main</option>
                  <option value="feature_new_model">feature_new_model</option>
                  <option value="hotfix_pricing">hotfix_pricing</option>
                </select>
                <button
                  className="btn btn-ghost btn-icon btn-sm"
                  onClick={() => setShowCompareModal(true)}
                  title="Compare branches"
                  style={{ padding: '4px' }}
                >
                  <GitCompare size={14} />
                </button>
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

            <div style={{ flex: 1, overflow: 'auto', padding: '8px 0' }}>
              {schemas.length === 0 ? (
                <div style={{ padding: '20px', textAlign: 'center', color: 'var(--text-muted)', fontSize: '13px' }}>
                  No schemas found
                </div>
              ) : (
                schemas.map((schema) => (
                  <div key={schema.name}>
                    {/* Schema row */}
                    <div
                      onClick={() => toggleSchema(schema.name)}
                      style={{
                        padding: '6px 12px',
                        display: 'flex',
                        alignItems: 'center',
                        gap: '6px',
                        cursor: 'pointer',
                        fontSize: '13px',
                        fontWeight: 500,
                        background: schema.name === selectedSchema ? 'rgba(14, 165, 233, 0.08)' : 'transparent',
                      }}
                      className="hover-bg"
                    >
                      {expandedSchemas.has(schema.name) ? (
                        <ChevronDown size={14} className="text-muted" />
                      ) : (
                        <ChevronRight size={14} className="text-muted" />
                      )}
                      {expandedSchemas.has(schema.name) ? (
                        <FolderOpen size={14} style={{ color: 'var(--accent-primary)' }} />
                      ) : (
                        <Folder size={14} style={{ color: 'var(--accent-primary)' }} />
                      )}
                      <span>{schema.name}</span>
                      <span style={{ marginLeft: 'auto', fontSize: '11px', color: 'var(--text-muted)' }}>
                        {schema.tables.length}
                      </span>
                    </div>

                    {/* Tables */}
                    {expandedSchemas.has(schema.name) && (
                      <div style={{ marginLeft: '12px' }}>
                        {schema.tables.map((table) => {
                          const tableKey = `${schema.name}.${table.name}`;
                          const isExpanded = expandedTables.has(tableKey);
                          return (
                            <div key={table.name}>
                              {/* Table row */}
                              <div
                                style={{
                                  padding: '4px 12px',
                                  display: 'flex',
                                  alignItems: 'center',
                                  gap: '6px',
                                  cursor: 'pointer',
                                  fontSize: '12px',
                                }}
                                className="hover-bg"
                              >
                                <span
                                  onClick={() => toggleTable(tableKey)}
                                  style={{ display: 'flex', alignItems: 'center' }}
                                >
                                  {isExpanded ? (
                                    <ChevronDown size={12} className="text-muted" />
                                  ) : (
                                    <ChevronRight size={12} className="text-muted" />
                                  )}
                                </span>
                                <Table size={13} style={{ color: 'var(--accent-secondary)' }} />
                                <span
                                  onClick={() => insertText(schema.name === selectedSchema ? table.name : `${schema.name}.${table.name}`)}
                                  style={{ cursor: 'pointer' }}
                                  title="Click to insert table name"
                                >
                                  {table.name}
                                </span>
                                <span style={{ marginLeft: 'auto', fontSize: '10px', color: 'var(--text-muted)' }}>
                                  {table.columns.length} cols
                                </span>
                              </div>

                              {/* Columns */}
                              {isExpanded && (
                                <div style={{ marginLeft: '24px' }}>
                                  {table.columns.map((col) => (
                                    <div
                                      key={col.name}
                                      onClick={() => insertText(col.name)}
                                      style={{
                                        padding: '3px 12px',
                                        display: 'flex',
                                        alignItems: 'center',
                                        gap: '6px',
                                        cursor: 'pointer',
                                        fontSize: '11px',
                                      }}
                                      className="hover-bg"
                                      title="Click to insert column name"
                                    >
                                      <Type size={11} className="text-muted" />
                                      <span style={{ color: 'var(--text-secondary)' }}>{col.name}</span>
                                      <span style={{ marginLeft: 'auto', fontSize: '10px', color: 'var(--text-muted)', fontFamily: 'monospace' }}>
                                        {col.type}
                                      </span>
                                    </div>
                                  ))}
                                </div>
                              )}
                            </div>
                          );
                        })}
                      </div>
                    )}
                  </div>
                ))
              )}
            </div>
          </>
        )}
      </div>

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
                disabled={!useCatalogContext}
              >
                <option value="xva_desk">xva_desk</option>
                <option value="market_data">market_data</option>
                <option value="analytics">analytics</option>
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
                disabled={!useCatalogContext}
              >
                <option value="main">main</option>
                <option value="feature_new_model">feature_new_model</option>
                <option value="hotfix_pricing">hotfix_pricing</option>
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

      {/* Compare Modal */}
      {showCompareModal && (
        <div className="modal-overlay" onClick={() => setShowCompareModal(false)}>
          <div className="modal modal-lg" onClick={(e) => e.stopPropagation()} style={{ maxWidth: '950px', height: '85vh' }}>
            <div className="modal-header" style={{ padding: '16px 20px' }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
                <GitCompare size={20} />
                <h3 className="modal-title">Compare Schemas</h3>
              </div>
              <button className="modal-close" onClick={() => setShowCompareModal(false)}>
                <X size={20} />
              </button>
            </div>

            {/* Compare Header - Source Selection */}
            <div style={{ padding: '12px 20px', background: 'var(--bg-tertiary)', borderBottom: '1px solid var(--border-light)', display: 'flex', gap: '24px', alignItems: 'center' }}>
              {/* Left Source */}
              <div style={{ flex: 1, display: 'flex', alignItems: 'center', gap: '8px' }}>
                <span style={{ fontSize: '11px', color: 'var(--text-muted)', textTransform: 'uppercase', fontWeight: 600 }}>Base</span>
                <select
                  className="form-input form-select"
                  style={{ width: '120px', padding: '5px 8px', fontSize: '12px' }}
                  value={compareCatalog1}
                  onChange={(e) => setCompareCatalog1(e.target.value)}
                >
                  <option value="xva_desk">xva_desk</option>
                  <option value="market_data">market_data</option>
                </select>
                <GitBranch size={14} className="text-muted" />
                <select
                  className="form-input form-select"
                  style={{ width: '140px', padding: '5px 8px', fontSize: '12px' }}
                  value={compareBranch1}
                  onChange={(e) => setCompareBranch1(e.target.value)}
                >
                  <option value="main">main</option>
                  <option value="feature_new_model">feature_new_model</option>
                  <option value="hotfix_pricing">hotfix_pricing</option>
                </select>
              </div>

              <RefreshCw size={16} className="text-muted" />

              {/* Right Source */}
              <div style={{ flex: 1, display: 'flex', alignItems: 'center', gap: '8px' }}>
                <span style={{ fontSize: '11px', color: 'var(--text-muted)', textTransform: 'uppercase', fontWeight: 600 }}>Compare</span>
                <select
                  className="form-input form-select"
                  style={{ width: '120px', padding: '5px 8px', fontSize: '12px' }}
                  value={compareCatalog2}
                  onChange={(e) => setCompareCatalog2(e.target.value)}
                >
                  <option value="xva_desk">xva_desk</option>
                  <option value="market_data">market_data</option>
                </select>
                <GitBranch size={14} className="text-muted" />
                <select
                  className="form-input form-select"
                  style={{ width: '140px', padding: '5px 8px', fontSize: '12px' }}
                  value={compareBranch2}
                  onChange={(e) => setCompareBranch2(e.target.value)}
                >
                  <option value="main">main</option>
                  <option value="feature_new_model">feature_new_model</option>
                  <option value="hotfix_pricing">hotfix_pricing</option>
                </select>
              </div>
            </div>

            {/* Search & Filter Bar */}
            <div style={{ padding: '12px 20px', borderBottom: '1px solid var(--border-light)', display: 'flex', gap: '12px', alignItems: 'center' }}>
              <div style={{ flex: 1, position: 'relative' }}>
                <Search size={14} style={{ position: 'absolute', left: '10px', top: '50%', transform: 'translateY(-50%)', color: 'var(--text-muted)' }} />
                <input
                  type="text"
                  className="form-input"
                  placeholder="Search schemas, tables, or columns..."
                  style={{ paddingLeft: '32px', fontSize: '13px' }}
                  value={compareSearch}
                  onChange={(e) => setCompareSearch(e.target.value)}
                />
              </div>
              <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
                <Filter size={14} className="text-muted" />
                <select
                  className="form-input form-select"
                  style={{ width: '100px', padding: '6px 8px', fontSize: '12px' }}
                  value={compareFilterLevel}
                  onChange={(e) => setCompareFilterLevel(e.target.value as typeof compareFilterLevel)}
                >
                  <option value="all">All</option>
                  <option value="schemas">Schemas</option>
                  <option value="tables">Tables</option>
                  <option value="columns">Columns</option>
                </select>
              </div>
              <label style={{ display: 'flex', alignItems: 'center', gap: '6px', cursor: 'pointer', fontSize: '12px', color: 'var(--text-secondary)' }}>
                <input
                  type="checkbox"
                  checked={compareShowOnlyDiffs}
                  onChange={(e) => setCompareShowOnlyDiffs(e.target.checked)}
                />
                Only differences
              </label>
              <div style={{ borderLeft: '1px solid var(--border-light)', height: '20px', margin: '0 4px' }} />
              <button
                className={`btn btn-sm ${compareSplitView ? 'btn-primary' : 'btn-ghost'}`}
                onClick={() => setCompareSplitView(!compareSplitView)}
                title={compareSplitView ? 'Unified view' : 'Split view'}
                style={{ padding: '4px 8px', display: 'flex', alignItems: 'center', gap: '4px' }}
              >
                {compareSplitView ? <Rows size={14} /> : <Columns size={14} />}
                <span style={{ fontSize: '11px' }}>{compareSplitView ? 'Unified' : 'Split'}</span>
              </button>
            </div>

            {/* Diff Legend */}
            <div style={{ padding: '8px 20px', borderBottom: '1px solid var(--border-light)', display: 'flex', gap: '20px', fontSize: '12px' }}>
              <span style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
                <span style={{ color: 'var(--error)', fontWeight: 700 }}>−</span>
                <span>Only in Base</span>
              </span>
              <span style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
                <span style={{ color: 'var(--success)', fontWeight: 700 }}>+</span>
                <span>Only in Compare</span>
              </span>
              <span style={{ display: 'flex', alignItems: 'center', gap: '6px', padding: '2px 8px', background: 'var(--warning-bg)', borderRadius: '4px' }}>
                <AlertTriangle size={12} style={{ color: 'var(--warning)' }} />
                <span>Modified</span>
              </span>
            </div>

            {/* Hierarchical Diff View */}
            <div style={{ flex: 1, overflow: 'auto', padding: '12px 20px' }}>
              {filteredDiff.length === 0 ? (
                <div style={{ textAlign: 'center', padding: '40px', color: 'var(--text-muted)' }}>
                  {compareSearch ? 'No results found' : 'Select sources to compare'}
                </div>
              ) : compareSplitView ? (
                /* Split View - Side by Side with Diff Indicators */
                (() => {
                  const searchLower = compareSearch.toLowerCase();

                  // Helper to check if item matches search
                  const matchesSearch = (name: string, level: 'schemas' | 'tables' | 'columns') => {
                    if (!searchLower) return true;
                    if (compareFilterLevel === 'all' || compareFilterLevel === level) {
                      return name.toLowerCase().includes(searchLower);
                    }
                    return false;
                  };

                  // Filter base schemas
                  const filteredBaseSchemas = compareSchemas1.filter(schema => {
                    const schemaInCompare = compareSchemas2.find(s => s.name === schema.name);
                    const isDiff = !schemaInCompare || schema.tables.some(t => {
                      const tInCompare = schemaInCompare?.tables.find(ct => ct.name === t.name);
                      return !tInCompare || t.columns.length !== tInCompare.columns.length ||
                        t.columns.some(c => !tInCompare.columns.find(cc => cc.name === c.name && cc.type === c.type));
                    });
                    if (compareShowOnlyDiffs && !isDiff) return false;
                    if (!searchLower) return true;
                    if (matchesSearch(schema.name, 'schemas')) return true;
                    if (schema.tables.some(t => matchesSearch(t.name, 'tables'))) return true;
                    if (schema.tables.some(t => t.columns.some(c => matchesSearch(c.name, 'columns')))) return true;
                    return false;
                  });

                  // Filter compare schemas
                  const filteredCompareSchemas = compareSchemas2.filter(schema => {
                    const schemaInBase = compareSchemas1.find(s => s.name === schema.name);
                    const isDiff = !schemaInBase || schema.tables.some(t => {
                      const tInBase = schemaInBase?.tables.find(ct => ct.name === t.name);
                      return !tInBase || t.columns.length !== tInBase.columns.length ||
                        t.columns.some(c => !tInBase.columns.find(cc => cc.name === c.name && cc.type === c.type));
                    });
                    if (compareShowOnlyDiffs && !isDiff) return false;
                    if (!searchLower) return true;
                    if (matchesSearch(schema.name, 'schemas')) return true;
                    if (schema.tables.some(t => matchesSearch(t.name, 'tables'))) return true;
                    if (schema.tables.some(t => t.columns.some(c => matchesSearch(c.name, 'columns')))) return true;
                    return false;
                  });

                  return (
                <div style={{ display: 'flex', gap: '16px', height: '100%' }}>
                  {/* Base Side */}
                  <div style={{ flex: 1, overflow: 'auto', borderRight: '1px solid var(--border-light)', paddingRight: '16px' }}>
                    <div style={{ fontSize: '11px', fontWeight: 600, color: 'var(--text-muted)', textTransform: 'uppercase', marginBottom: '12px', position: 'sticky', top: 0, background: 'var(--bg-primary)', padding: '4px 0' }}>
                      Base: {compareCatalog1}:{compareBranch1}
                    </div>
                    {filteredBaseSchemas.map((schema) => {
                      const isSchemaExpanded = compareExpandedSchemas.has(schema.name);
                      const schemaInCompare = compareSchemas2.find(s => s.name === schema.name);
                      // Check for table-level differences
                      const hasTableDiff = schemaInCompare && (
                        schema.tables.length !== schemaInCompare.tables.length ||
                        schema.tables.some(t => !schemaInCompare.tables.find(ct => ct.name === t.name)) ||
                        schema.tables.some(t => {
                          const ct = schemaInCompare.tables.find(ct => ct.name === t.name);
                          return ct && (t.columns.length !== ct.columns.length ||
                            t.columns.some(c => !ct.columns.find(cc => cc.name === c.name && cc.type === c.type)));
                        })
                      );
                      const schemaStatus = !schemaInCompare ? 'removed' : hasTableDiff ? 'modified' : 'same';

                      return (
                        <div key={schema.name} style={{ marginBottom: '8px' }}>
                          <div
                            className="hover-bg"
                            onClick={() => toggleCompareSchema(schema.name)}
                            style={{
                              padding: '6px 8px',
                              borderRadius: 'var(--radius-sm)',
                              display: 'flex',
                              alignItems: 'center',
                              gap: '6px',
                              cursor: 'pointer',
                              background: schemaStatus === 'modified' ? 'var(--warning-bg)' : undefined,
                            }}
                          >
                            {isSchemaExpanded ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
                            {schemaStatus === 'removed' && <span style={{ color: 'var(--error)', fontWeight: 700 }}>−</span>}
                            {schemaStatus === 'modified' && <AlertTriangle size={12} style={{ color: 'var(--warning)' }} />}
                            <FolderOpen size={14} style={{ color: 'var(--accent-primary)' }} />
                            <span style={{ fontWeight: 500, fontSize: '13px', color: schemaStatus === 'removed' ? 'var(--error)' : undefined }}>{schema.name}</span>
                            <span style={{ marginLeft: 'auto', fontSize: '10px', color: 'var(--text-muted)' }}>{schema.tables.length}</span>
                          </div>
                          {isSchemaExpanded && (
                            <div style={{ marginLeft: '20px', borderLeft: '1px solid var(--border-light)', paddingLeft: '8px' }}>
                              {schema.tables.map((table) => {
                                const isTableExpanded = compareExpandedTables.has(`${schema.name}.${table.name}`);
                                const tableInCompare = schemaInCompare?.tables.find(t => t.name === table.name);
                                const hasColDiff = tableInCompare && (
                                  table.columns.length !== tableInCompare.columns.length ||
                                  table.columns.some(c => {
                                    const cmpCol = tableInCompare.columns.find(tc => tc.name === c.name);
                                    return !cmpCol || cmpCol.type !== c.type;
                                  })
                                );
                                const tableStatus = !tableInCompare ? 'removed' : hasColDiff ? 'modified' : 'same';

                                return (
                                  <div key={table.name} style={{ marginBottom: '4px' }}>
                                    <div
                                      className="hover-bg"
                                      onClick={() => toggleCompareTable(`${schema.name}.${table.name}`)}
                                      style={{
                                        padding: '4px 6px',
                                        borderRadius: 'var(--radius-sm)',
                                        display: 'flex',
                                        alignItems: 'center',
                                        gap: '6px',
                                        cursor: 'pointer',
                                      }}
                                    >
                                      {isTableExpanded ? <ChevronDown size={12} /> : <ChevronRight size={12} />}
                                      {tableStatus === 'removed' && <span style={{ color: 'var(--error)', fontWeight: 700, fontSize: '14px' }}>−</span>}
                                      {tableStatus === 'modified' && <AlertTriangle size={10} style={{ color: 'var(--warning)' }} />}
                                      <Table size={12} style={{ color: 'var(--accent-secondary)' }} />
                                      <span style={{ fontSize: '12px', color: tableStatus === 'removed' ? 'var(--error)' : undefined }}>{table.name}</span>
                                      <span style={{ marginLeft: 'auto', fontSize: '9px', color: 'var(--text-muted)' }}>{table.columns.length}</span>
                                    </div>
                                    {isTableExpanded && (
                                      <div style={{ marginLeft: '18px', marginTop: '2px' }}>
                                        {table.columns.map((col) => {
                                          const colInCompare = tableInCompare?.columns.find(c => c.name === col.name);
                                          const colStatus = !colInCompare ? 'removed' : colInCompare.type !== col.type ? 'modified' : 'same';
                                          return (
                                            <div key={col.name} style={{
                                              padding: '2px 6px',
                                              fontSize: '11px',
                                              display: 'flex',
                                              alignItems: 'center',
                                              gap: '4px',
                                              background: colStatus === 'removed' ? 'var(--error-bg)' : colStatus === 'modified' ? 'var(--warning-bg)' : undefined,
                                              borderRadius: '2px',
                                            }}>
                                              {colStatus === 'removed' && <span style={{ color: 'var(--error)', fontWeight: 700 }}>−</span>}
                                              {colStatus === 'modified' && <AlertTriangle size={9} style={{ color: 'var(--warning)' }} />}
                                              <Type size={10} className="text-muted" />
                                              <span style={{ color: colStatus === 'removed' ? 'var(--error)' : undefined }}>{col.name}</span>
                                              <span style={{ marginLeft: 'auto', fontFamily: 'monospace', fontSize: '9px', color: 'var(--text-muted)' }}>{col.type}</span>
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
                    })}
                  </div>
                  {/* Compare Side */}
                  <div style={{ flex: 1, overflow: 'auto' }}>
                    <div style={{ fontSize: '11px', fontWeight: 600, color: 'var(--text-muted)', textTransform: 'uppercase', marginBottom: '12px', position: 'sticky', top: 0, background: 'var(--bg-primary)', padding: '4px 0' }}>
                      Compare: {compareCatalog2}:{compareBranch2}
                    </div>
                    {filteredCompareSchemas.map((schema) => {
                      const isSchemaExpanded = compareExpandedSchemas.has(schema.name);
                      const schemaInBase = compareSchemas1.find(s => s.name === schema.name);
                      // Check for table-level differences
                      const hasTableDiff = schemaInBase && (
                        schema.tables.length !== schemaInBase.tables.length ||
                        schema.tables.some(t => !schemaInBase.tables.find(ct => ct.name === t.name)) ||
                        schema.tables.some(t => {
                          const ct = schemaInBase.tables.find(ct => ct.name === t.name);
                          return ct && (t.columns.length !== ct.columns.length ||
                            t.columns.some(c => !ct.columns.find(cc => cc.name === c.name && cc.type === c.type)));
                        })
                      );
                      const schemaStatus = !schemaInBase ? 'added' : hasTableDiff ? 'modified' : 'same';

                      return (
                        <div key={schema.name} style={{ marginBottom: '8px' }}>
                          <div
                            className="hover-bg"
                            onClick={() => toggleCompareSchema(schema.name)}
                            style={{
                              padding: '6px 8px',
                              borderRadius: 'var(--radius-sm)',
                              display: 'flex',
                              alignItems: 'center',
                              gap: '6px',
                              cursor: 'pointer',
                              background: schemaStatus === 'modified' ? 'var(--warning-bg)' : undefined,
                            }}
                          >
                            {isSchemaExpanded ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
                            {schemaStatus === 'added' && <span style={{ color: 'var(--success)', fontWeight: 700 }}>+</span>}
                            {schemaStatus === 'modified' && <AlertTriangle size={12} style={{ color: 'var(--warning)' }} />}
                            <FolderOpen size={14} style={{ color: 'var(--accent-primary)' }} />
                            <span style={{ fontWeight: 500, fontSize: '13px', color: schemaStatus === 'added' ? 'var(--success)' : undefined }}>{schema.name}</span>
                            <span style={{ marginLeft: 'auto', fontSize: '10px', color: 'var(--text-muted)' }}>{schema.tables.length}</span>
                          </div>
                          {isSchemaExpanded && (
                            <div style={{ marginLeft: '20px', borderLeft: '1px solid var(--border-light)', paddingLeft: '8px' }}>
                              {schema.tables.map((table) => {
                                const isTableExpanded = compareExpandedTables.has(`${schema.name}.${table.name}`);
                                const tableInBase = schemaInBase?.tables.find(t => t.name === table.name);
                                const hasColDiff = tableInBase && (
                                  table.columns.length !== tableInBase.columns.length ||
                                  table.columns.some(c => {
                                    const baseCol = tableInBase.columns.find(tc => tc.name === c.name);
                                    return !baseCol || baseCol.type !== c.type;
                                  })
                                );
                                const tableStatus = !tableInBase ? 'added' : hasColDiff ? 'modified' : 'same';

                                return (
                                  <div key={table.name} style={{ marginBottom: '4px' }}>
                                    <div
                                      className="hover-bg"
                                      onClick={() => toggleCompareTable(`${schema.name}.${table.name}`)}
                                      style={{
                                        padding: '4px 6px',
                                        borderRadius: 'var(--radius-sm)',
                                        display: 'flex',
                                        alignItems: 'center',
                                        gap: '6px',
                                        cursor: 'pointer',
                                      }}
                                    >
                                      {isTableExpanded ? <ChevronDown size={12} /> : <ChevronRight size={12} />}
                                      {tableStatus === 'added' && <span style={{ color: 'var(--success)', fontWeight: 700 }}>+</span>}
                                      {tableStatus === 'modified' && <AlertTriangle size={10} style={{ color: 'var(--warning)' }} />}
                                      <Table size={12} style={{ color: 'var(--accent-secondary)' }} />
                                      <span style={{ fontSize: '12px', color: tableStatus === 'added' ? 'var(--success)' : undefined }}>{table.name}</span>
                                      <span style={{ marginLeft: 'auto', fontSize: '9px', color: 'var(--text-muted)' }}>{table.columns.length}</span>
                                    </div>
                                    {isTableExpanded && (
                                      <div style={{ marginLeft: '18px', marginTop: '2px' }}>
                                        {table.columns.map((col) => {
                                          const colInBase = tableInBase?.columns.find(c => c.name === col.name);
                                          const colStatus = !colInBase ? 'added' : colInBase.type !== col.type ? 'modified' : 'same';
                                          return (
                                            <div key={col.name} style={{
                                              padding: '2px 6px',
                                              fontSize: '11px',
                                              display: 'flex',
                                              alignItems: 'center',
                                              gap: '4px',
                                              background: colStatus === 'added' ? 'var(--success-bg)' : colStatus === 'modified' ? 'var(--warning-bg)' : undefined,
                                              borderRadius: '2px',
                                            }}>
                                              {colStatus === 'added' && <span style={{ color: 'var(--success)', fontWeight: 700 }}>+</span>}
                                              {colStatus === 'modified' && <AlertTriangle size={9} style={{ color: 'var(--warning)' }} />}
                                              <Type size={10} className="text-muted" />
                                              <span style={{ color: colStatus === 'added' ? 'var(--success)' : undefined }}>{col.name}</span>
                                              <span style={{ marginLeft: 'auto', fontFamily: 'monospace', fontSize: '9px', color: 'var(--text-muted)' }}>{col.type}</span>
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
                    })}
                  </div>
                </div>
                  );
                })()
              ) : (
                /* Unified Diff View */
                filteredDiff.map((schema) => {
                  const schemaStatusStyle = {
                    'left-only': { bg: 'var(--error-bg)', icon: <Minus size={12} style={{ color: 'var(--error)' }} /> },
                    'right-only': { bg: 'var(--success-bg)', icon: <Plus size={12} style={{ color: 'var(--success)' }} /> },
                    'modified': { bg: 'var(--warning-bg)', icon: <AlertTriangle size={12} style={{ color: 'var(--warning)' }} /> },
                    'same': { bg: 'transparent', icon: null },
                  }[schema.status];

                  if (compareShowOnlyDiffs && schema.status === 'same') return null;

                  const isSchemaExpanded = compareExpandedSchemas.has(schema.schemaName);

                  return (
                    <div key={schema.schemaName} style={{ marginBottom: '8px' }}>
                      {/* Schema Row */}
                      <div
                        className="hover-bg"
                        onClick={() => toggleCompareSchema(schema.schemaName)}
                        style={{
                          padding: '8px 12px',
                          background: schemaStatusStyle.bg,
                          borderRadius: 'var(--radius-md)',
                          display: 'flex',
                          alignItems: 'center',
                          gap: '8px',
                          fontWeight: 600,
                          fontSize: '14px',
                          cursor: 'pointer',
                        }}
                      >
                        {isSchemaExpanded ? <ChevronDown size={16} /> : <ChevronRight size={16} />}
                        {schemaStatusStyle.icon}
                        <FolderOpen size={16} style={{ color: 'var(--accent-primary)' }} />
                        <span>{schema.schemaName}</span>
                        <span style={{ marginLeft: 'auto', fontSize: '11px', color: 'var(--text-muted)', fontWeight: 400 }}>
                          {schema.tables.length} tables
                        </span>
                      </div>

                      {/* Tables */}
                      {isSchemaExpanded && (
                        <div style={{ marginLeft: '20px', borderLeft: '2px solid var(--border-light)', paddingLeft: '12px', marginTop: '4px' }}>
                          {schema.tables.map((table) => {
                            const tableStatusStyle = {
                              'left-only': { bg: 'var(--error-bg)', icon: <Minus size={11} style={{ color: 'var(--error)' }} /> },
                              'right-only': { bg: 'var(--success-bg)', icon: <Plus size={11} style={{ color: 'var(--success)' }} /> },
                              'modified': { bg: 'var(--warning-bg)', icon: <AlertTriangle size={11} style={{ color: 'var(--warning)' }} /> },
                              'same': { bg: 'transparent', icon: null },
                            }[table.status];

                            if (compareShowOnlyDiffs && table.status === 'same') return null;

                            const tableKey = `${schema.schemaName}.${table.tableName}`;
                            const isTableExpanded = compareExpandedTables.has(tableKey);

                            return (
                              <div key={table.tableName} style={{ marginBottom: '4px' }}>
                                {/* Table Row */}
                                <div
                                  className="hover-bg"
                                  onClick={() => toggleCompareTable(tableKey)}
                                  style={{
                                    padding: '6px 10px',
                                    background: tableStatusStyle.bg,
                                    borderRadius: 'var(--radius-sm)',
                                    display: 'flex',
                                    alignItems: 'center',
                                    gap: '6px',
                                    fontSize: '13px',
                                    cursor: 'pointer',
                                  }}
                                >
                                  {isTableExpanded ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
                                  {tableStatusStyle.icon}
                                  <Table size={14} style={{ color: 'var(--accent-secondary)' }} />
                                  <span style={{ fontWeight: 500 }}>{table.tableName}</span>
                                  <span style={{ marginLeft: 'auto', fontSize: '10px', color: 'var(--text-muted)' }}>
                                    {table.leftCols.length === table.rightCols.length
                                      ? `${table.leftCols.length} cols`
                                      : `${table.leftCols.length} → ${table.rightCols.length} cols`}
                                  </span>
                                </div>

                                {/* Columns (show when table is expanded) */}
                                {isTableExpanded && (
                                  <div style={{ marginLeft: '24px', marginTop: '4px' }}>
                                    {table.columns.map((col) => {
                                      const colStatusStyle = {
                                        'left-only': { bg: 'var(--error-bg)', icon: <Minus size={10} style={{ color: 'var(--error)' }} /> },
                                        'right-only': { bg: 'var(--success-bg)', icon: <Plus size={10} style={{ color: 'var(--success)' }} /> },
                                        'type-changed': { bg: 'var(--warning-bg)', icon: <AlertTriangle size={10} style={{ color: 'var(--warning)' }} /> },
                                        'same': { bg: 'transparent', icon: null },
                                      }[col.status];

                                      if (compareShowOnlyDiffs && col.status === 'same') return null;

                                      return (
                                        <div key={col.colName} style={{
                                          padding: '3px 8px',
                                          background: colStatusStyle.bg,
                                          borderRadius: '2px',
                                          display: 'flex',
                                          alignItems: 'center',
                                          gap: '6px',
                                          fontSize: '11px',
                                          marginBottom: '2px',
                                        }}>
                                          {colStatusStyle.icon}
                                          <Type size={10} className="text-muted" />
                                          <span>{col.colName}</span>
                                          <span style={{ marginLeft: 'auto', fontFamily: 'monospace', fontSize: '10px', color: 'var(--text-muted)' }}>
                                            {col.status === 'type-changed' ? (
                                              <><span style={{ textDecoration: 'line-through' }}>{col.leftType}</span> → {col.rightType}</>
                                            ) : (
                                              col.colType
                                            )}
                                          </span>
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
          </div>
        </div>
      )}
    </div>
  );
}
