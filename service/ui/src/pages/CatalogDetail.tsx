import React, { useEffect, useState, useCallback, useRef, useMemo } from 'react';
import ReactDOM from 'react-dom';
import { useParams, Link, useNavigate } from 'react-router-dom';
import {
  Database,
  GitBranch,
  Table,
  HardDrive,
  Plus,
  RefreshCw,
  Trash2,
  CheckCircle,
  XCircle,
  ChevronRight,
  ChevronDown,
  Folder,
  FileCode,
  Settings,
  GitCompare,
  AlertTriangle,
  Layers,
  X,
  Edit3,
  Save,
  List,
  Network,
  Search,
  GitGraph,
  Eye,
  Info,
  History,
  MoreVertical,
  FileText,
  Rows3,
} from 'lucide-react';
import mermaid from 'mermaid';
import { AgGridReact } from 'ag-grid-react';
import { type ColDef, themeQuartz } from 'ag-grid-community';
import { catalogsApi, branchesApi, schemasApi } from '../api';
import { SchemaBrowser } from '../components/SchemaBrowser';
import { BranchDiffTab } from '../components/BranchDiffTab';
import { BranchExplorerTab } from '../components/BranchExplorerTab';
import type { Catalog, Branch, SchemaInfo, CatalogTestResult, BranchStats, BranchDiffResponse } from '../types';

// Custom AG Grid theme - light mode with clean styling
const customGridTheme = themeQuartz
  .withParams({
    backgroundColor: '#ffffff',
    foregroundColor: '#1e293b',
    borderColor: '#e2e8f0',
    headerBackgroundColor: '#f8fafc',
    headerTextColor: '#64748b',
    oddRowBackgroundColor: '#f8fafc',
    rowHoverColor: 'rgba(14, 165, 233, 0.08)',
    selectedRowBackgroundColor: 'rgba(14, 165, 233, 0.12)',
    accentColor: '#0ea5e9',
    borderRadius: 6,
    fontSize: 13,
    headerFontSize: 12,
    headerFontWeight: 600,
    rowHeight: 40,
    headerHeight: 42,
    cellHorizontalPadding: 12,
    spacing: 6,
  });

// =============================================================================
// Helpers
// =============================================================================

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
}

function formatNumber(num: number): string {
  if (num >= 1000000) return (num / 1000000).toFixed(1) + 'M';
  if (num >= 1000) return (num / 1000).toFixed(1) + 'K';
  return num.toString();
}

function timeAgo(dateString: string): string {
  const date = new Date(dateString);
  const now = new Date();
  const seconds = Math.floor((now.getTime() - date.getTime()) / 1000);

  if (seconds < 60) return 'just now';
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
  if (seconds < 604800) return `${Math.floor(seconds / 86400)}d ago`;
  return date.toLocaleDateString();
}

// Branch actions cell renderer component (for AG Grid)
interface BranchActionsCellProps {
  data: Branch;
  onInfo: (branch: Branch) => void;
  onCompare: (branchName: string) => void;
  onDelete: (branchName: string) => void;
}

function BranchActionsCell({ data: branch, onInfo, onCompare, onDelete }: BranchActionsCellProps) {
  const [isOpen, setIsOpen] = React.useState(false);
  const isMain = branch.branch_name === 'main';
  const buttonRef = React.useRef<HTMLButtonElement>(null);
  const [menuPosition, setMenuPosition] = React.useState({ top: 0, left: 0 });

  // Close menu when clicking outside
  React.useEffect(() => {
    if (!isOpen) return;
    const handleClickOutside = () => setIsOpen(false);
    document.addEventListener('click', handleClickOutside);
    return () => document.removeEventListener('click', handleClickOutside);
  }, [isOpen]);

  const handleOpen = (e: React.MouseEvent) => {
    e.stopPropagation();
    if (buttonRef.current) {
      const rect = buttonRef.current.getBoundingClientRect();
      setMenuPosition({ top: rect.bottom + 4, left: rect.left });
    }
    setIsOpen(!isOpen);
  };

  return (
    <div style={{ display: 'flex', alignItems: 'center', height: '100%' }}>
      <button
        ref={buttonRef}
        className="btn btn-ghost btn-icon btn-sm"
        onClick={handleOpen}
      >
        <MoreVertical size={16} />
      </button>
      {isOpen && ReactDOM.createPortal(
        <div
          style={{
            position: 'fixed',
            top: menuPosition.top,
            left: menuPosition.left,
            zIndex: 9999,
            background: 'var(--bg-primary)',
            border: '1px solid var(--border-color)',
            borderRadius: '8px',
            boxShadow: 'var(--shadow-lg)',
            minWidth: '150px',
            padding: '4px 0',
          }}
          onClick={(e) => e.stopPropagation()}
        >
          <button
            style={{
              display: 'flex',
              alignItems: 'center',
              gap: '8px',
              width: '100%',
              padding: '8px 12px',
              border: 'none',
              background: 'none',
              cursor: 'pointer',
              fontSize: '13px',
              color: 'var(--text-primary)',
              textAlign: 'left',
            }}
            onMouseEnter={(e) => (e.currentTarget.style.background = 'var(--bg-secondary)')}
            onMouseLeave={(e) => (e.currentTarget.style.background = 'none')}
            onClick={() => { onInfo(branch); setIsOpen(false); }}
          >
            <Info size={14} /> Info
          </button>
          <button
            style={{
              display: 'flex',
              alignItems: 'center',
              gap: '8px',
              width: '100%',
              padding: '8px 12px',
              border: 'none',
              background: 'none',
              cursor: 'not-allowed',
              fontSize: '13px',
              color: 'var(--text-muted)',
              opacity: 0.5,
              textAlign: 'left',
            }}
            disabled
          >
            <History size={14} /> Access Log
          </button>
          <button
            style={{
              display: 'flex',
              alignItems: 'center',
              gap: '8px',
              width: '100%',
              padding: '8px 12px',
              border: 'none',
              background: 'none',
              cursor: isMain ? 'not-allowed' : 'pointer',
              fontSize: '13px',
              color: isMain ? 'var(--text-muted)' : 'var(--text-primary)',
              opacity: isMain ? 0.5 : 1,
              textAlign: 'left',
            }}
            disabled={isMain}
            onMouseEnter={(e) => !isMain && (e.currentTarget.style.background = 'var(--bg-secondary)')}
            onMouseLeave={(e) => (e.currentTarget.style.background = 'none')}
            onClick={() => { if (!isMain) { onCompare(branch.branch_name); setIsOpen(false); } }}
          >
            <GitCompare size={14} /> Compare
          </button>
          <div style={{ height: '1px', background: 'var(--border-color)', margin: '4px 0' }} />
          <button
            style={{
              display: 'flex',
              alignItems: 'center',
              gap: '8px',
              width: '100%',
              padding: '8px 12px',
              border: 'none',
              background: 'none',
              cursor: isMain ? 'not-allowed' : 'pointer',
              fontSize: '13px',
              color: isMain ? 'var(--text-muted)' : '#ef4444',
              opacity: isMain ? 0.5 : 1,
              textAlign: 'left',
            }}
            disabled={isMain}
            onMouseEnter={(e) => !isMain && (e.currentTarget.style.background = 'var(--bg-secondary)')}
            onMouseLeave={(e) => (e.currentTarget.style.background = 'none')}
            onClick={() => { if (!isMain) { onDelete(branch.branch_name); setIsOpen(false); } }}
          >
            <Trash2 size={14} /> Delete
          </button>
        </div>,
        document.body
      )}
    </div>
  );
}

// Branch tree node for hierarchical display
interface BranchTreeNode {
  branch: Branch;
  children: BranchTreeNode[];
  depth: number;
}

function buildBranchTree(branches: Branch[]): BranchTreeNode[] {
  // Create a map for quick lookup
  const branchMap = new Map<string, Branch>();
  branches.forEach(b => branchMap.set(b.branch_name, b));

  // Build tree structure
  const nodeMap = new Map<string, BranchTreeNode>();
  const roots: BranchTreeNode[] = [];

  // First pass: create nodes
  branches.forEach(branch => {
    nodeMap.set(branch.branch_name, { branch, children: [], depth: 0 });
  });

  // Second pass: link children to parents
  branches.forEach(branch => {
    const node = nodeMap.get(branch.branch_name)!;
    if (branch.parent_branch_name && nodeMap.has(branch.parent_branch_name)) {
      const parentNode = nodeMap.get(branch.parent_branch_name)!;
      parentNode.children.push(node);
    } else {
      roots.push(node);
    }
  });

  // Third pass: calculate depth and sort children
  function setDepth(node: BranchTreeNode, depth: number) {
    node.depth = depth;
    node.children.sort((a, b) => a.branch.branch_name.localeCompare(b.branch.branch_name));
    node.children.forEach(child => setDepth(child, depth + 1));
  }
  roots.forEach(root => setDepth(root, 0));

  return roots;
}

// Get ancestry path from a branch back to main
function getAncestryPath(branches: Branch[], branchName: string): Set<string> {
  const path = new Set<string>();
  const branchMap = new Map<string, Branch>();
  branches.forEach(b => branchMap.set(b.branch_name, b));

  let current = branchMap.get(branchName);
  while (current) {
    path.add(current.branch_name);
    if (current.parent_branch_name) {
      current = branchMap.get(current.parent_branch_name);
    } else {
      break;
    }
  }
  return path;
}

// Get all descendants of a branch
function getDescendants(branches: Branch[], branchName: string): Set<string> {
  const descendants = new Set<string>();
  const childrenMap = new Map<string, string[]>();

  branches.forEach(b => {
    if (b.parent_branch_name) {
      const children = childrenMap.get(b.parent_branch_name) || [];
      children.push(b.branch_name);
      childrenMap.set(b.parent_branch_name, children);
    }
  });

  function addDescendants(name: string) {
    const children = childrenMap.get(name) || [];
    children.forEach(child => {
      descendants.add(child);
      addDescendants(child);
    });
  }
  addDescendants(branchName);
  return descendants;
}

function flattenBranchTree(
  nodes: BranchTreeNode[],
  collapsedSet: Set<string>,
  searchTerm: string,
  focusedBranch?: string | null,
  allBranches?: Branch[]
): BranchTreeNode[] {
  const result: BranchTreeNode[] = [];
  const search = searchTerm.toLowerCase().trim();

  // If focused on a branch, show only its lineage (ancestors + descendants)
  let focusedNodes: Set<string> | null = null;
  if (focusedBranch && allBranches) {
    focusedNodes = new Set<string>();
    const ancestors = getAncestryPath(allBranches, focusedBranch);
    const descendants = getDescendants(allBranches, focusedBranch);
    ancestors.forEach(n => focusedNodes!.add(n));
    descendants.forEach(n => focusedNodes!.add(n));
  }

  // If searching, find all matching nodes and their ancestors
  const matchingNodes = new Set<string>();
  if (search) {
    function findMatches(node: BranchTreeNode): boolean {
      const selfMatches = node.branch.branch_name.toLowerCase().includes(search);
      const childMatches = node.children.some(child => findMatches(child));
      if (selfMatches || childMatches) {
        matchingNodes.add(node.branch.branch_name);
        return true;
      }
      return false;
    }
    nodes.forEach(findMatches);
  }

  function traverse(node: BranchTreeNode) {
    // If focused, only include nodes in lineage
    if (focusedNodes && !focusedNodes.has(node.branch.branch_name)) {
      return;
    }
    // If searching, only include matching nodes and their ancestors
    if (search && !matchingNodes.has(node.branch.branch_name)) {
      return;
    }
    result.push(node);
    // Don't traverse children if collapsed (unless searching - always expand when searching)
    // Note: Focus mode should still respect collapse state
    if (!search && collapsedSet.has(node.branch.branch_name)) {
      return;
    }
    node.children.forEach(traverse);
  }
  nodes.forEach(traverse);
  return result;
}

// =============================================================================
// Component
// =============================================================================

type TabId = 'branches' | 'schemas' | 'explorer' | 'diff' | 'settings';

export default function CatalogDetail() {
  const { catalogId } = useParams<{ catalogId: string }>();
  const navigate = useNavigate();

  // Core state
  const [catalog, setCatalog] = useState<Catalog | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState<TabId>('branches');

  // Test connectivity state
  const [testing, setTesting] = useState(false);
  const [testResult, setTestResult] = useState<CatalogTestResult | null>(null);

  // Branches state
  const [branches, setBranches] = useState<Branch[]>([]);
  const [branchesLoading, setBranchesLoading] = useState(false);
  const [selectedBranch, setSelectedBranch] = useState<string>('main');
  const [showCreateBranch, setShowCreateBranch] = useState(false);
  const [newBranchName, setNewBranchName] = useState('');
  const [newBranchParent, setNewBranchParent] = useState('main');

  // Schemas state (used for stats display)
  const [schemas, setSchemas] = useState<SchemaInfo[]>([]);

  // Catalog stats (from selected branch for DuckLake catalogs)
  const [catalogStats, setCatalogStats] = useState<BranchStats | null>(null);
  const [catalogStatsLoading, setCatalogStatsLoading] = useState(false);
  const [statsBranch, setStatsBranch] = useState<string>('main');

  // Settings state
  const [editMode, setEditMode] = useState(false);
  const [editedCatalog, setEditedCatalog] = useState<Partial<Catalog>>({});
  const [saving, setSaving] = useState(false);

  // Branch view mode (tree, table, or diagram)
  const [branchViewMode, setBranchViewMode] = useState<'tree' | 'table' | 'diagram'>('tree');
  const [branchSearch, setBranchSearch] = useState('');
  const [branchSearchOpen, setBranchSearchOpen] = useState(false);
  const [branchSearchHighlight, setBranchSearchHighlight] = useState(0);
  const [collapsedBranches, setCollapsedBranches] = useState<Set<string>>(new Set());
  const [focusedBranch, setFocusedBranch] = useState<string | null>(null); // For double-click filter
  const [hoveredBranch, setHoveredBranch] = useState<string | null>(null);
  const [mousePos, setMousePos] = useState<{ x: number; y: number }>({ x: 0, y: 0 });
  const mermaidRef = React.useRef<HTMLDivElement>(null);
  const branchTreeRef = useRef<HTMLDivElement>(null);
  const branchSearchRef = useRef<HTMLDivElement>(null);

  // Branch info modal state
  const [showBranchInfo, setShowBranchInfo] = useState(false);
  const [branchInfoData, setBranchInfoData] = useState<{ branch: Branch; stats: BranchStats | null } | null>(null);
  const [branchInfoLoading, setBranchInfoLoading] = useState(false);

  // Compare modal state
  const [showCompareModal, setShowCompareModal] = useState(false);
  const [compareBaseBranch, setCompareBaseBranch] = useState('main');
  const [compareTargetBranch, setCompareTargetBranch] = useState('');
  const [compareDiff, setCompareDiff] = useState<BranchDiffResponse | null>(null);
  const [compareLoading, setCompareLoading] = useState(false);
  const [compareExpandedSchemas, setCompareExpandedSchemas] = useState<Set<string>>(new Set());

  // =============================================================================
  // Data Fetching
  // =============================================================================

  const fetchCatalog = useCallback(async () => {
    if (!catalogId) return;
    try {
      setLoading(true);
      const data = await catalogsApi.get(catalogId);
      setCatalog(data);
      setEditedCatalog(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load catalog');
    } finally {
      setLoading(false);
    }
  }, [catalogId]);

  const fetchBranches = useCallback(async () => {
    if (!catalogId) return;
    try {
      setBranchesLoading(true);
      const data = await branchesApi.list(catalogId);
      console.log('Branches API response:', data);
      const branchList = Array.isArray(data) ? data : data.branches || [];
      console.log('Branch list:', branchList);
      console.log('First non-main branch parent_branch_name:', branchList.find(b => b.branch_name !== 'main')?.parent_branch_name);
      setBranches(branchList);
    } catch (err) {
      console.error('Failed to fetch branches:', err);
    } finally {
      setBranchesLoading(false);
    }
  }, [catalogId]);

  const fetchSchemas = useCallback(async (branchName: string) => {
    if (!catalogId) return;
    try {
      const data = await schemasApi.list(catalogId, branchName);
      setSchemas(Array.isArray(data) ? data : data.schemas || []);
    } catch (err) {
      console.error('Failed to fetch schemas:', err);
    }
  }, [catalogId]);

  const fetchCatalogStats = useCallback(async () => {
    if (!catalogId || !catalog || catalog.catalog_type !== 'DUCKLAKE') return;
    setCatalogStatsLoading(true);
    try {
      const stats = await branchesApi.stats(catalogId, 'main');
      setCatalogStats(stats);
    } catch (err) {
      console.error('Failed to fetch catalog stats:', err);
    } finally {
      setCatalogStatsLoading(false);
    }
  }, [catalogId, catalog]);

  // Initial load
  useEffect(() => {
    fetchCatalog();
    fetchBranches();
  }, [fetchCatalog, fetchBranches]);

  // Fetch catalog stats when catalog is loaded (for DuckLake catalogs)
  useEffect(() => {
    if (catalog?.catalog_type === 'DUCKLAKE') {
      fetchCatalogStats();
    }
  }, [catalog, fetchCatalogStats]);

  // Load branch-specific data when branch changes
  useEffect(() => {
    if (selectedBranch) {
      fetchSchemas(selectedBranch);
    }
  }, [selectedBranch, fetchSchemas]);

  // Filtered branches for type-ahead dropdown
  const filteredBranches = useMemo(() => {
    if (!branchSearch.trim()) return branches;
    const search = branchSearch.toLowerCase();
    return branches.filter(b => b.branch_name.toLowerCase().includes(search));
  }, [branches, branchSearch]);

  // Reset highlight when filtered results change
  useEffect(() => {
    setBranchSearchHighlight(0);
  }, [filteredBranches.length]);

  // Click outside handler for branch search dropdown
  useEffect(() => {
    if (!branchSearchOpen) return;
    const handleClickOutside = (e: MouseEvent) => {
      if (branchSearchRef.current && !branchSearchRef.current.contains(e.target as Node)) {
        setBranchSearchOpen(false);
      }
    };
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, [branchSearchOpen]);

  // Keyboard handler for branch tree (arrow keys to collapse/expand)
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      // Only handle if tree view is active and we have a selected branch
      if (branchViewMode !== 'tree' || !selectedBranch) return;
      // Don't handle if focus is in an input
      if ((e.target as HTMLElement).tagName === 'INPUT') return;

      if (e.key === 'ArrowLeft') {
        e.preventDefault();
        // Collapse the selected branch
        setCollapsedBranches(prev => {
          const next = new Set(prev);
          next.add(selectedBranch);
          return next;
        });
      } else if (e.key === 'ArrowRight') {
        e.preventDefault();
        // Expand the selected branch
        setCollapsedBranches(prev => {
          const next = new Set(prev);
          next.delete(selectedBranch);
          return next;
        });
      } else if (e.key === 'ArrowUp' || e.key === 'ArrowDown') {
        e.preventDefault();
        // Navigate between branches (don't use branchSearch - it's only for type-ahead)
        const flatNodes = flattenBranchTree(buildBranchTree(branches), collapsedBranches, '', focusedBranch, branches);
        const currentIndex = flatNodes.findIndex(n => n.branch.branch_name === selectedBranch);
        if (currentIndex === -1) return;
        const nextIndex = e.key === 'ArrowUp'
          ? Math.max(0, currentIndex - 1)
          : Math.min(flatNodes.length - 1, currentIndex + 1);
        setSelectedBranch(flatNodes[nextIndex].branch.branch_name);
        // Scroll into view
        const nodeEl = branchTreeRef.current?.querySelector(`[data-branch="${flatNodes[nextIndex].branch.branch_name}"]`);
        nodeEl?.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
      }
    };
    document.addEventListener('keydown', handleKeyDown);
    return () => document.removeEventListener('keydown', handleKeyDown);
  }, [branchViewMode, selectedBranch, collapsedBranches, focusedBranch, branches]);

  // Handle branch selection from type-ahead
  const handleBranchTypeaheadSelect = (branchName: string) => {
    setFocusedBranch(branchName);
    setSelectedBranch(branchName);
    setBranchSearchOpen(false);
    // Expand the path to this branch
    const ancestry = getAncestryPath(branches, branchName);
    setCollapsedBranches(prev => {
      const next = new Set(prev);
      ancestry.forEach(name => next.delete(name));
      return next;
    });
  };

  // =============================================================================
  // Actions
  // =============================================================================

  const handleTest = async () => {
    if (!catalogId) return;
    setTesting(true);
    setTestResult(null);
    try {
      const result = await catalogsApi.test(catalogId);
      setTestResult(result);
    } catch (err) {
      console.error('Test failed:', err);
    } finally {
      setTesting(false);
    }
  };

  const handleCreateBranch = async () => {
    if (!catalogId || !newBranchName.trim()) return;
    try {
      await branchesApi.create(catalogId, {
        branch_name: newBranchName.trim(),
        from_branch: newBranchParent,
      });
      setShowCreateBranch(false);
      setNewBranchName('');
      fetchBranches();
    } catch (err) {
      console.error('Failed to create branch:', err);
    }
  };

  const handleDeleteBranch = async (branchName: string) => {
    if (!catalogId || branchName === 'main') return;
    if (!confirm(`Delete branch "${branchName}"?`)) return;
    try {
      await branchesApi.delete(catalogId, branchName);
      fetchBranches();
    } catch (err) {
      console.error('Failed to delete branch:', err);
    }
  };

  const handleShowBranchInfo = async (branch: Branch) => {
    if (!catalogId) return;
    setShowBranchInfo(true);
    setBranchInfoData({ branch, stats: null });
    setBranchInfoLoading(true);
    try {
      const stats = await branchesApi.stats(catalogId, branch.branch_name);
      setBranchInfoData({ branch, stats });
    } catch (err) {
      console.error('Failed to fetch branch stats:', err);
    } finally {
      setBranchInfoLoading(false);
    }
  };

  const fetchCompareDiff = async (baseBranch?: string, targetBranch?: string) => {
    const base = baseBranch ?? compareBaseBranch;
    const target = targetBranch ?? compareTargetBranch;
    if (!catalogId || !base || !target) return;
    setCompareLoading(true);
    try {
      const diff = await branchesApi.diff(catalogId, base, target);
      setCompareDiff(diff);
      // Auto-expand schemas with changes
      const schemasWithChanges = new Set(
        diff.schemas.filter(s => s.status !== 'unchanged').map(s => s.schema_name)
      );
      setCompareExpandedSchemas(schemasWithChanges);
    } catch (err) {
      console.error('Failed to fetch diff:', err);
    } finally {
      setCompareLoading(false);
    }
  };

  const handleCompareBranch = (branchName: string) => {
    setCompareTargetBranch(branchName);
    setCompareBaseBranch('main');
    setCompareDiff(null);
    setCompareExpandedSchemas(new Set());
    setShowCompareModal(true);
    // Auto-fetch diff with main vs the selected branch
    fetchCompareDiff('main', branchName);
  };

  const handleSaveSettings = async () => {
    if (!catalogId) return;
    setSaving(true);
    try {
      const updated = await catalogsApi.update(catalogId, {
        display_name: editedCatalog.display_name,
        description: editedCatalog.description,
        tags: editedCatalog.tags,
      });
      setCatalog(updated);
      setEditMode(false);
    } catch (err) {
      console.error('Failed to save:', err);
    } finally {
      setSaving(false);
    }
  };

  const handleDeleteCatalog = async () => {
    if (!catalogId) return;
    if (!confirm('Are you sure you want to unregister this catalog? This cannot be undone.')) return;
    try {
      await catalogsApi.delete(catalogId);
      navigate('/catalogs');
    } catch (err) {
      console.error('Failed to delete catalog:', err);
    }
  };

  // Generate mermaid gitGraph code from branches
  const generateMermaidGraph = useCallback(() => {
    if (branches.length === 0) return '';

    const lines: string[] = ['gitGraph'];

    // Track which branches we've created
    const createdBranches = new Set<string>();

    // Sort branches by created_at if available, otherwise by branch_id
    const sortedBranches = [...branches].sort((a, b) => {
      if (a.created_at && b.created_at) {
        return new Date(a.created_at).getTime() - new Date(b.created_at).getTime();
      }
      return (a.branch_id || 0) - (b.branch_id || 0);
    });

    // Find main branch
    const mainBranch = sortedBranches.find(b => b.branch_name === 'main');
    if (mainBranch) {
      lines.push(`  commit id: "init"`);
      createdBranches.add('main');
    }

    // Process branches in chronological order
    sortedBranches.forEach(branch => {
      if (branch.branch_name === 'main') return;

      const parentName = branch.parent_branch_name || 'main';
      const safeName = branch.branch_name.replace(/[^a-zA-Z0-9]/g, '_');
      const safeParent = parentName.replace(/[^a-zA-Z0-9]/g, '_');

      // Checkout parent if needed
      if (parentName !== 'main' && createdBranches.has(parentName)) {
        lines.push(`  checkout ${safeParent}`);
      } else if (parentName === 'main') {
        lines.push(`  checkout main`);
      }

      // Create branch and commit
      lines.push(`  branch ${safeName}`);
      lines.push(`  commit id: "${branch.branch_name.split('/').pop()}"`);
      createdBranches.add(branch.branch_name);

      // If merged, show merge back
      if (branch.status === 'merged') {
        lines.push(`  checkout ${safeParent === 'main' ? 'main' : safeParent}`);
        lines.push(`  merge ${safeName}`);
      }
    });

    return lines.join('\n');
  }, [branches]);

  // Render mermaid diagram when in diagram mode
  useEffect(() => {
    if (branchViewMode === 'diagram' && mermaidRef.current && branches.length > 0) {
      const graphCode = generateMermaidGraph();
      if (graphCode) {
        mermaid.initialize({
          startOnLoad: false,
          theme: 'base',
          themeVariables: {
            primaryColor: '#0ea5e9',
            primaryTextColor: '#0f172a',
            primaryBorderColor: '#cbd5e1',
            lineColor: '#94a3b8',
            secondaryColor: '#f59e0b',
            tertiaryColor: '#f1f5f9',
            git0: '#0ea5e9',
            git1: '#f59e0b',
            git2: '#22c55e',
            git3: '#8b5cf6',
            git4: '#ec4899',
            git5: '#14b8a6',
            git6: '#f97316',
            git7: '#6366f1',
            gitBranchLabel0: '#ffffff',
            gitBranchLabel1: '#ffffff',
            gitBranchLabel2: '#ffffff',
            gitBranchLabel3: '#ffffff',
          },
          gitGraph: {
            showBranches: true,
            showCommitLabel: true,
            mainBranchName: 'main',
          },
        });

        mermaidRef.current.innerHTML = graphCode;
        mermaid.run({ nodes: [mermaidRef.current] });
      }
    }
  }, [branchViewMode, branches, generateMermaidGraph]);

  // =============================================================================
  // Render
  // =============================================================================

  if (loading || !catalog) {
    return (
      <div className="loading">
        <div className="spinner" />
      </div>
    );
  }

  if (error) {
    return (
      <div className="empty-state">
        <AlertTriangle className="empty-state-icon" style={{ color: 'var(--error)' }} />
        <div className="empty-state-title">Error Loading Catalog</div>
        <div className="empty-state-description">{error}</div>
        <button className="btn btn-primary" onClick={fetchCatalog}>
          <RefreshCw size={16} />
          Retry
        </button>
      </div>
    );
  }

  const tabs: { id: TabId; label: string; icon: React.ReactNode }[] = [
    { id: 'branches', label: 'Branches', icon: <GitBranch size={16} /> },
    { id: 'schemas', label: 'Schemas', icon: <Layers size={16} /> },
    { id: 'explorer', label: 'Explorer', icon: <Search size={16} /> },
    { id: 'diff', label: 'Branch Diff', icon: <GitCompare size={16} /> },
    { id: 'settings', label: 'Settings', icon: <Settings size={16} /> },
  ];

  return (
    <div className="catalog-detail">
      {/* Header */}
      <div className="page-header">
        <div className="breadcrumb mb-3">
          <Link to="/catalogs" className="breadcrumb-link">Catalogs</Link>
          <ChevronRight size={14} className="breadcrumb-separator" />
          <span>{catalog.display_name || catalog.catalog_id}</span>
        </div>

        <div className="page-header-row">
          <div style={{ flex: 1 }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
              <h1 className="page-title" style={{ marginBottom: 0 }}>
                {catalog.display_name || catalog.catalog_id}
              </h1>
              <span className={`badge badge-${catalog.enabled ? 'active' : 'archived'}`}>
                {catalog.enabled ? 'Active' : 'Disabled'}
              </span>
            </div>
            {catalog.description && (
              <p className="page-description" style={{ marginTop: '8px' }}>{catalog.description}</p>
            )}
          </div>

          <div className="page-actions">
            <button
              className="btn btn-secondary"
              onClick={handleTest}
              disabled={testing}
            >
              {testing ? (
                <RefreshCw size={16} className="spin" />
              ) : testResult?.overall_status === 'healthy' ? (
                <CheckCircle size={16} style={{ color: 'var(--success)' }} />
              ) : testResult?.overall_status === 'unhealthy' ? (
                <XCircle size={16} style={{ color: 'var(--error)' }} />
              ) : (
                <RefreshCw size={16} />
              )}
              Test Connection
            </button>
            <button
              className="btn btn-primary"
              onClick={() => navigate(`/query?catalog=${catalogId}`)}
            >
              <FileCode size={16} />
              Query
            </button>
          </div>
        </div>
      </div>

      {/* Stats Bar */}
      <div style={{
        display: 'flex',
        flexWrap: 'wrap',
        gap: '16px',
        marginBottom: '20px',
        padding: '12px 16px',
        background: 'var(--bg-secondary)',
        borderRadius: '8px',
        fontSize: '13px',
        alignItems: 'center',
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
          <GitBranch size={14} style={{ color: 'var(--text-muted)' }} />
          <span style={{ fontWeight: 500 }}>{catalog.branch_count || branches.length}</span>
          <span style={{ color: 'var(--text-muted)' }}>branches</span>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
          <Database size={14} style={{ color: 'var(--text-muted)' }} />
          <span style={{ fontWeight: 500 }}>{catalogStats?.schema_count ?? schemas.length}</span>
          <span style={{ color: 'var(--text-muted)' }}>schemas</span>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
          <Table size={14} style={{ color: 'var(--text-muted)' }} />
          <span style={{ fontWeight: 500 }}>{catalogStats?.table_count ?? catalog.table_count ?? 0}</span>
          <span style={{ color: 'var(--text-muted)' }}>tables</span>
        </div>
        {catalog.catalog_type === 'DUCKLAKE' && (
          <>
            <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
              <Eye size={14} style={{ color: 'var(--text-muted)' }} />
              <span style={{ fontWeight: 500 }}>{catalogStats?.view_count ?? 0}</span>
              <span style={{ color: 'var(--text-muted)' }}>views</span>
            </div>
            <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
              <FileText size={14} style={{ color: 'var(--text-muted)' }} />
              <span style={{ fontWeight: 500 }}>{catalogStats?.data_file_count?.toLocaleString() ?? 0}</span>
              <span style={{ color: 'var(--text-muted)' }}>files</span>
            </div>
            <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
              <Rows3 size={14} style={{ color: 'var(--text-muted)' }} />
              <span style={{ fontWeight: 500 }}>{catalogStats?.total_rows?.toLocaleString() ?? 0}</span>
              <span style={{ color: 'var(--text-muted)' }}>rows</span>
            </div>
          </>
        )}
        <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
          <HardDrive size={14} style={{ color: 'var(--text-muted)' }} />
          <span style={{ fontWeight: 500 }}>{formatBytes(catalogStats?.total_size_bytes ?? catalog.total_size_bytes ?? 0)}</span>
        </div>
        {catalog.catalog_type === 'DUCKLAKE' && catalogStats?.snapshot_count !== undefined && (
          <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
            <History size={14} style={{ color: 'var(--text-muted)' }} />
            <span style={{ fontWeight: 500 }}>{catalogStats.snapshot_count}</span>
            <span style={{ color: 'var(--text-muted)' }}>snapshots</span>
          </div>
        )}
        <div style={{ marginLeft: 'auto', display: 'flex', alignItems: 'center', gap: '16px', color: 'var(--text-muted)' }}>
          {catalog.catalog_type === 'DUCKLAKE' && (
            <button
              className="btn btn-ghost btn-sm"
              onClick={fetchCatalogStats}
              disabled={catalogStatsLoading}
              title="Refresh stats"
              style={{ padding: '4px' }}
            >
              <RefreshCw size={14} className={catalogStatsLoading ? 'spin' : ''} />
            </button>
          )}
          <span>Created: <span style={{ color: 'var(--text-primary)' }}>{new Date(catalog.created_at).toLocaleDateString()}</span></span>
          {catalog.updated_at && (
            <span>Updated: <span style={{ color: 'var(--text-primary)' }}>{new Date(catalog.updated_at).toLocaleDateString()}</span></span>
          )}
          {catalog.last_accessed_at && (
            <span>Accessed: <span style={{ color: 'var(--text-primary)' }}>{timeAgo(catalog.last_accessed_at)}</span></span>
          )}
        </div>
      </div>

      {/* Tabs */}
      <div className="card">
        <div className="tabs-modern">
          {tabs.map((tab) => (
            <button
              key={tab.id}
              className={`tab-modern ${activeTab === tab.id ? 'active' : ''}`}
              onClick={() => setActiveTab(tab.id)}
            >
              {tab.icon}
              <span>{tab.label}</span>
              {tab.id === 'branches' && branches.length > 0 && (
                <span className="tab-badge">{branches.length}</span>
              )}
            </button>
          ))}
        </div>

        <div className="tab-content">
          {/* Branches Tab */}
          {activeTab === 'branches' && (
            <div>
              <div className="tab-header">
                <div className="tab-header-left">
                  <h3 className="section-title" style={{ margin: 0 }}>
                    {branchViewMode === 'tree' ? 'Branch Tree' : 'Branches'}
                  </h3>
                  <span className="text-muted text-sm">{branches.length} branches</span>
                </div>
                <div className="tab-header-right">
                  {(branchViewMode === 'tree' || branchViewMode === 'table') && (
                    <div ref={branchSearchRef} style={{ position: 'relative' }}>
                      <div style={{ position: 'relative' }}>
                        <Search size={14} style={{ position: 'absolute', left: '10px', top: '50%', transform: 'translateY(-50%)', color: 'var(--text-muted)', zIndex: 1 }} />
                        <input
                          type="text"
                          className="form-input"
                          placeholder="Search branches..."
                          value={branchSearch}
                          onChange={(e) => {
                            setBranchSearch(e.target.value);
                            setBranchSearchOpen(true);
                          }}
                          onFocus={() => setBranchSearchOpen(true)}
                          onKeyDown={(e) => {
                            if (e.key === 'ArrowDown') {
                              e.preventDefault();
                              setBranchSearchHighlight(prev => Math.min(prev + 1, filteredBranches.length - 1));
                            } else if (e.key === 'ArrowUp') {
                              e.preventDefault();
                              setBranchSearchHighlight(prev => Math.max(prev - 1, 0));
                            } else if (e.key === 'Enter' && filteredBranches.length > 0) {
                              e.preventDefault();
                              handleBranchTypeaheadSelect(filteredBranches[branchSearchHighlight].branch_name);
                            } else if (e.key === 'Escape') {
                              setBranchSearchOpen(false);
                            }
                          }}
                          style={{ paddingLeft: '32px', width: '240px', height: '32px', fontSize: '12px' }}
                        />
                      </div>
                      {branchSearchOpen && branchSearch.trim() && filteredBranches.length > 0 && (
                        <div style={{
                          position: 'absolute',
                          top: '100%',
                          left: 0,
                          right: 0,
                          marginTop: '4px',
                          background: 'var(--bg-primary)',
                          border: '1px solid var(--border-color)',
                          borderRadius: '8px',
                          boxShadow: 'var(--shadow-lg)',
                          maxHeight: '300px',
                          overflowY: 'auto',
                          zIndex: 100,
                        }}>
                          {filteredBranches.slice(0, 20).map((branch, idx) => (
                            <div
                              key={branch.branch_id}
                              onClick={() => handleBranchTypeaheadSelect(branch.branch_name)}
                              style={{
                                padding: '8px 12px',
                                cursor: 'pointer',
                                fontSize: '12px',
                                display: 'flex',
                                alignItems: 'center',
                                gap: '8px',
                                background: idx === branchSearchHighlight ? 'var(--bg-secondary)' : 'transparent',
                                borderBottom: idx < filteredBranches.length - 1 ? '1px solid var(--border-light)' : 'none',
                              }}
                              onMouseEnter={() => setBranchSearchHighlight(idx)}
                            >
                              <GitBranch size={12} style={{ color: 'var(--text-muted)', flexShrink: 0 }} />
                              <span style={{ flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                                {branch.branch_name}
                              </span>
                              <span className={`badge badge-${branch.status}`} style={{ fontSize: '9px', padding: '1px 4px' }}>
                                {branch.status}
                              </span>
                            </div>
                          ))}
                          {filteredBranches.length > 20 && (
                            <div style={{ padding: '6px 12px', fontSize: '11px', color: 'var(--text-muted)', textAlign: 'center' }}>
                              +{filteredBranches.length - 20} more results
                            </div>
                          )}
                        </div>
                      )}
                      {branchSearchOpen && branchSearch.trim() && filteredBranches.length === 0 && (
                        <div style={{
                          position: 'absolute',
                          top: '100%',
                          left: 0,
                          right: 0,
                          marginTop: '4px',
                          background: 'var(--bg-primary)',
                          border: '1px solid var(--border-color)',
                          borderRadius: '8px',
                          boxShadow: 'var(--shadow-lg)',
                          padding: '12px',
                          fontSize: '12px',
                          color: 'var(--text-muted)',
                          textAlign: 'center',
                          zIndex: 100,
                        }}>
                          No branches found
                        </div>
                      )}
                    </div>
                  )}
                  <button
                    className="btn btn-ghost btn-sm"
                    onClick={fetchBranches}
                    disabled={branchesLoading}
                    title="Refresh branches"
                  >
                    <RefreshCw size={14} className={branchesLoading ? 'spin' : ''} />
                  </button>
                  <div className="btn-group">
                    <button
                      className={`btn btn-sm ${branchViewMode === 'tree' ? 'btn-secondary' : 'btn-ghost'}`}
                      onClick={() => setBranchViewMode('tree')}
                      title="Tree View"
                    >
                      <Network size={14} />
                    </button>
                    <button
                      className={`btn btn-sm ${branchViewMode === 'table' ? 'btn-secondary' : 'btn-ghost'}`}
                      onClick={() => setBranchViewMode('table')}
                      title="Table View"
                    >
                      <List size={14} />
                    </button>
                    <button
                      className={`btn btn-sm ${branchViewMode === 'diagram' ? 'btn-secondary' : 'btn-ghost'}`}
                      onClick={() => setBranchViewMode('diagram')}
                      title="Timeline View"
                    >
                      <GitGraph size={14} />
                    </button>
                  </div>
                  <button
                    className="btn btn-primary btn-sm"
                    onClick={() => setShowCreateBranch(true)}
                  >
                    <Plus size={14} />
                    Create Branch
                  </button>
                </div>
              </div>

              {/* Focus mode indicator */}
              {focusedBranch && (
                <div className="focus-indicator" style={{
                  padding: '8px 16px',
                  background: 'var(--info-bg)',
                  borderRadius: 'var(--radius-md)',
                  marginBottom: '12px',
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'space-between',
                }}>
                  <span style={{ fontSize: '12px', color: 'var(--info)' }}>
                    Showing lineage for: <strong>{focusedBranch}</strong>
                  </span>
                  <button
                    className="btn btn-ghost btn-sm"
                    onClick={() => setFocusedBranch(null)}
                    style={{ padding: '4px 8px' }}
                  >
                    <X size={14} /> Clear
                  </button>
                </div>
              )}

              {/* Keyboard shortcuts hint */}
              {branchViewMode === 'tree' && !focusedBranch && (
                <div style={{
                  padding: '6px 12px',
                  marginBottom: '8px',
                  fontSize: '11px',
                  color: 'var(--text-muted)',
                  display: 'flex',
                  gap: '16px',
                }}>
                  <span><kbd style={{ background: 'var(--bg-tertiary)', padding: '1px 4px', borderRadius: '3px', fontSize: '10px' }}>↑↓</kbd> Navigate</span>
                  <span><kbd style={{ background: 'var(--bg-tertiary)', padding: '1px 4px', borderRadius: '3px', fontSize: '10px' }}>←→</kbd> Collapse/Expand</span>
                  <span><kbd style={{ background: 'var(--bg-tertiary)', padding: '1px 4px', borderRadius: '3px', fontSize: '10px' }}>Double-click</kbd> Focus lineage</span>
                </div>
              )}

              {branchesLoading ? (
                <div className="loading-inline">
                  <div className="spinner" />
                </div>
              ) : branchViewMode === 'tree' ? (
                <div ref={branchTreeRef} className="branch-tree-v2" style={{ maxHeight: '700px', overflowY: 'auto', padding: '8px 0' }}>
                  {(() => {
                    const ancestryPath = selectedBranch ? getAncestryPath(branches, selectedBranch) : new Set<string>();
                    // Don't pass branchSearch to tree - it's only for the type-ahead dropdown
                    // This allows collapse/expand to work while searching
                    const flatNodes = flattenBranchTree(buildBranchTree(branches), collapsedBranches, '', focusedBranch, branches);

                    return flatNodes.map((node, index, arr) => {
                      const { branch, depth, children } = node;
                      const isMain = branch.branch_name === 'main';
                      const isSelected = branch.branch_name === selectedBranch;
                      const isInPath = ancestryPath.has(branch.branch_name);
                      const isHovered = hoveredBranch === branch.branch_name;
                      const hasChildren = children.length > 0;
                      const isCollapsed = collapsedBranches.has(branch.branch_name);

                      // eslint-disable-next-line @typescript-eslint/no-explicit-any
                      const extBranch = branch as any;
                      const createdDate = branch.created_at ? new Date(branch.created_at) : null;

                      // Colors based on depth for visual hierarchy
                      const depthColors = ['#0ea5e9', '#f59e0b', '#22c55e', '#8b5cf6', '#ec4899'];
                      const lineColor = isInPath ? depthColors[depth % depthColors.length] : 'var(--border-light)';

                      // Check if last child
                      const isLastChild = index > 0 && (() => {
                        for (let i = index + 1; i < arr.length; i++) {
                          if (arr[i].depth < depth) return true;
                          if (arr[i].depth === depth) return false;
                        }
                        return true;
                      })();

                      const searchMatch = branchSearch && branch.branch_name.toLowerCase().includes(branchSearch.toLowerCase());

                      return (
                        <div
                          key={branch.branch_id}
                          data-branch={branch.branch_name}
                          className={`branch-node ${isSelected ? 'selected' : ''} ${isInPath ? 'in-path' : ''} ${isHovered ? 'hovered' : ''}`}
                          style={{
                            display: 'flex',
                            alignItems: 'stretch',
                            marginLeft: `${depth * 32}px`,
                            position: 'relative',
                          }}
                          onMouseEnter={(e) => {
                            setHoveredBranch(branch.branch_name);
                            setMousePos({ x: e.clientX, y: e.clientY });
                          }}
                          onMouseMove={(e) => setMousePos({ x: e.clientX, y: e.clientY })}
                          onMouseLeave={() => setHoveredBranch(null)}
                        >
                          {/* Vertical line connector */}
                          {depth > 0 && (
                            <svg
                              width="32"
                              height="100%"
                              style={{
                                position: 'absolute',
                                left: '-32px',
                                top: 0,
                                height: '100%',
                                minHeight: '56px',
                              }}
                            >
                              {/* Curved connector from parent */}
                              <path
                                d={`M 16 0 Q 16 28, 32 28`}
                                fill="none"
                                stroke={lineColor}
                                strokeWidth={isInPath ? 2.5 : 1.5}
                                strokeLinecap="round"
                              />
                              {/* Vertical continuation line */}
                              {!isLastChild && (
                                <line
                                  x1="16" y1="0" x2="16" y2="100%"
                                  stroke={lineColor}
                                  strokeWidth={isInPath ? 2.5 : 1.5}
                                />
                              )}
                            </svg>
                          )}

                          {/* Node content */}
                          <div
                            className="branch-node-card"
                            onClick={() => setSelectedBranch(branch.branch_name)}
                            onDoubleClick={() => setFocusedBranch(focusedBranch === branch.branch_name ? null : branch.branch_name)}
                            style={{
                              flex: 1,
                              display: 'flex',
                              alignItems: 'center',
                              gap: '12px',
                              padding: '10px 16px',
                              marginBottom: '4px',
                              background: isSelected ? 'var(--accent-secondary)' : isInPath ? 'rgba(14, 165, 233, 0.08)' : 'var(--bg-secondary)',
                              border: `1px solid ${isSelected ? 'var(--accent-secondary)' : isInPath ? 'rgba(14, 165, 233, 0.3)' : 'var(--border-light)'}`,
                              borderRadius: '8px',
                              cursor: 'pointer',
                              transition: 'all 0.15s ease',
                              boxShadow: isHovered ? 'var(--shadow-md)' : 'var(--shadow-sm)',
                            }}
                          >
                            {/* Node circle indicator */}
                            <div style={{
                              width: '12px',
                              height: '12px',
                              borderRadius: '50%',
                              background: isMain ? 'var(--accent-primary)' : isSelected ? '#fff' : depthColors[depth % depthColors.length],
                              border: `2px solid ${isMain ? 'var(--accent-primary)' : depthColors[depth % depthColors.length]}`,
                              flexShrink: 0,
                            }} />

                            {/* Collapse toggle */}
                            {hasChildren && (
                              <button
                                className="btn btn-ghost btn-icon"
                                style={{ padding: '2px', marginLeft: '-8px' }}
                                onClick={(e) => {
                                  e.stopPropagation();
                                  setCollapsedBranches(prev => {
                                    const next = new Set(prev);
                                    next.has(branch.branch_name) ? next.delete(branch.branch_name) : next.add(branch.branch_name);
                                    return next;
                                  });
                                }}
                              >
                                {isCollapsed ? <ChevronRight size={14} /> : <ChevronDown size={14} />}
                              </button>
                            )}

                            {/* Branch info */}
                            <div style={{ flex: 1, minWidth: 0 }}>
                              <div style={{ display: 'flex', alignItems: 'center', gap: '8px', flexWrap: 'wrap' }}>
                                <span style={{
                                  fontWeight: isMain ? 600 : 500,
                                  color: isSelected ? '#fff' : 'var(--text-primary)',
                                  background: searchMatch ? 'rgba(245, 158, 11, 0.3)' : undefined,
                                  padding: searchMatch ? '0 4px' : undefined,
                                  borderRadius: '2px',
                                }}>
                                  {branch.branch_name}
                                </span>
                                <span className={`badge badge-${branch.status}`} style={{ fontSize: '10px' }}>{branch.status}</span>
                                {hasChildren && isCollapsed && (
                                  <span style={{ fontSize: '11px', color: 'var(--text-muted)' }}>+{children.length}</span>
                                )}
                              </div>
                              {/* Snapshot & time info */}
                              <div style={{
                                display: 'flex',
                                alignItems: 'center',
                                gap: '12px',
                                marginTop: '4px',
                                fontSize: '11px',
                                color: isSelected ? 'rgba(255,255,255,0.8)' : 'var(--text-muted)',
                              }}>
                                {branch.fork_snapshot_id != null && (
                                  <span title="Parent snapshot (fork point)">Fork: #{branch.fork_snapshot_id}</span>
                                )}
                                <span style={{ fontWeight: 500 }} title="Current head snapshot">Head: #{branch.head_snapshot_id}</span>
                                {createdDate && <span title="Branch created">Created: {createdDate.toLocaleDateString()}</span>}
                                {extBranch.last_accessed_at && (
                                  <span style={{ display: 'flex', alignItems: 'center', gap: '2px' }}>
                                    <Eye size={10} /> {timeAgo(extBranch.last_accessed_at)}
                                  </span>
                                )}
                              </div>
                            </div>

                            {/* Actions */}
                            <div style={{ display: 'flex', gap: '4px', opacity: isHovered ? 1 : 0, transition: 'opacity 0.15s' }}>
                              <button
                                className="btn btn-ghost btn-icon btn-sm"
                                title="Info"
                                onClick={(e) => { e.stopPropagation(); handleShowBranchInfo(branch); }}
                              >
                                <Info size={14} />
                              </button>
                              <button
                                className="btn btn-ghost btn-icon btn-sm"
                                title="Access Log"
                                disabled
                                style={{ opacity: 0.4, cursor: 'not-allowed' }}
                              >
                                <History size={14} />
                              </button>
                              {!isMain && (
                                <>
                                  <button
                                    className="btn btn-ghost btn-icon btn-sm"
                                    title="Compare"
                                    onClick={(e) => { e.stopPropagation(); handleCompareBranch(branch.branch_name); }}
                                  >
                                    <GitCompare size={14} />
                                  </button>
                                  <button
                                    className="btn btn-ghost btn-icon btn-sm"
                                    title="Delete"
                                    onClick={(e) => { e.stopPropagation(); handleDeleteBranch(branch.branch_name); }}
                                  >
                                    <Trash2 size={14} />
                                  </button>
                                </>
                              )}
                            </div>
                          </div>

                          {/* Hover tooltip */}
                          {isHovered && (
                            <div className="branch-tooltip" style={{
                              position: 'fixed',
                              left: mousePos.x + 15,
                              top: mousePos.y + 10,
                              width: '200px',
                              padding: '12px',
                              background: 'var(--bg-sidebar)',
                              color: 'var(--text-sidebar)',
                              borderRadius: '8px',
                              fontSize: '11px',
                              boxShadow: 'var(--shadow-lg)',
                              zIndex: 1000,
                              pointerEvents: 'none',
                            }}>
                              <div style={{ fontWeight: 600, marginBottom: '8px' }}>Snapshot Details</div>
                              <div style={{ display: 'grid', gap: '4px' }}>
                                <div>Fork: <span style={{ color: '#fff' }}>#{branch.fork_snapshot_id ?? 'N/A'}</span></div>
                                <div>Current: <span style={{ color: '#fff' }}>#{branch.head_snapshot_id}</span></div>
                                {createdDate && <div>Created: <span style={{ color: '#fff' }}>{createdDate.toLocaleString()}</span></div>}
                                {extBranch.last_modified_at && (
                                  <div>Modified: <span style={{ color: '#fff' }}>{new Date(extBranch.last_modified_at).toLocaleString()}</span></div>
                                )}
                                {extBranch.last_accessed_at && (
                                  <div>Accessed: <span style={{ color: '#fff' }}>{new Date(extBranch.last_accessed_at).toLocaleString()}</span></div>
                                )}
                                {extBranch.created_by && (
                                  <div>By: <span style={{ color: '#fff' }}>{extBranch.created_by}</span></div>
                                )}
                              </div>
                              <div style={{ marginTop: '8px', paddingTop: '8px', borderTop: '1px solid rgba(255,255,255,0.1)', fontSize: '10px', color: 'var(--text-muted)' }}>
                                Double-click to focus lineage
                              </div>
                            </div>
                          )}
                        </div>
                      );
                    });
                  })()}
                </div>
              ) : branchViewMode === 'table' ? (
                <div style={{ height: '520px', width: '100%', padding: '8px' }}>
                  <AgGridReact
                    theme={customGridTheme}
                    rowData={branches
                      .filter((b) => !branchSearch || b.branch_name.toLowerCase().includes(branchSearch.toLowerCase()))
                      .map((b) => ({
                        ...b,
                        // eslint-disable-next-line @typescript-eslint/no-explicit-any
                        description: (b as any).description || '',
                        // eslint-disable-next-line @typescript-eslint/no-explicit-any
                        last_modified_at: (b as any).last_modified_at || b.created_at,
                        // eslint-disable-next-line @typescript-eslint/no-explicit-any
                        last_accessed_at: (b as any).last_accessed_at || b.created_at,
                        // eslint-disable-next-line @typescript-eslint/no-explicit-any
                        created_by: (b as any).created_by || 'unknown',
                      }))}
                    columnDefs={[
                      {
                        field: 'branch_name',
                        headerName: 'Name',
                        flex: 2,
                        minWidth: 200,
                        cellRenderer: (params: { value: string }) => (
                          <span style={{ fontWeight: params.value === 'main' ? 600 : 400 }}>{params.value}</span>
                        ),
                      },
                      {
                        field: 'parent_branch_name',
                        headerName: 'Parent',
                        flex: 1,
                        minWidth: 150,
                        valueFormatter: (params: { value: string | null }) => params.value || '—',
                      },
                      {
                        field: 'fork_snapshot_id',
                        headerName: 'Fork Snapshot',
                        width: 120,
                        type: 'numericColumn',
                        valueFormatter: (params: { value: number | null }) => params.value != null ? `#${params.value}` : '—',
                      },
                      {
                        field: 'head_snapshot_id',
                        headerName: 'Current Snapshot',
                        width: 140,
                        type: 'numericColumn',
                        valueFormatter: (params: { value: number }) => `#${params.value}`,
                      },
                      {
                        field: 'status',
                        headerName: 'Status',
                        width: 90,
                        cellRenderer: (params: { value: string }) => (
                          <span className={`badge badge-${params.value}`} style={{ fontSize: '10px', padding: '2px 6px' }}>{params.value}</span>
                        ),
                      },
                      {
                        field: 'created_by',
                        headerName: 'Created By',
                        flex: 1,
                        minWidth: 120,
                      },
                      {
                        field: 'created_at',
                        headerName: 'Created',
                        width: 130,
                        valueFormatter: (params: { value: string }) => timeAgo(params.value),
                      },
                      {
                        field: 'last_modified_at',
                        headerName: 'Last Modified',
                        width: 130,
                        valueFormatter: (params: { value: string }) => timeAgo(params.value),
                      },
                      {
                        field: 'last_accessed_at',
                        headerName: 'Last Accessed',
                        width: 130,
                        valueFormatter: (params: { value: string }) => timeAgo(params.value),
                      },
                      {
                        field: 'description',
                        headerName: 'Description',
                        flex: 2,
                        minWidth: 200,
                        tooltipField: 'description',
                      },
                      {
                        headerName: '',
                        width: 50,
                        pinned: 'left',
                        sortable: false,
                        filter: false,
                        cellRenderer: (params: { data: Branch }) => (
                          <BranchActionsCell
                            data={params.data}
                            onInfo={handleShowBranchInfo}
                            onCompare={handleCompareBranch}
                            onDelete={handleDeleteBranch}
                          />
                        ),
                      },
                    ] as ColDef[]}
                    defaultColDef={{
                      sortable: true,
                      filter: true,
                      resizable: true,
                    }}
                    animateRows={true}
                    rowSelection={{ mode: 'singleRow', checkboxes: false }}
                    onRowClicked={(event) => {
                      if (event.data?.branch_name) {
                        setSelectedBranch(event.data.branch_name);
                      }
                    }}
                  />
                </div>
              ) : (
                /* Diagram View - Mermaid gitGraph */
                <div className="mermaid-container" style={{ padding: '20px', minHeight: '400px', overflow: 'auto' }}>
                  <div
                    ref={mermaidRef}
                    className="mermaid"
                    style={{ display: 'flex', justifyContent: 'center' }}
                  />
                </div>
              )}

            </div>
          )}

          {/* Schemas Tab */}
          {activeTab === 'schemas' && (
            <div>
              <div className="tab-header">
                <div className="tab-header-left">
                  <h3 className="section-title" style={{ margin: 0 }}>Schema Browser</h3>
                </div>
              </div>

              <div className="schema-tree-container" style={{ marginTop: 16, border: '1px solid var(--border)', borderRadius: 8, overflow: 'hidden' }}>
                <SchemaBrowser
                  catalogId={catalogId!}
                  branches={branches}
                  currentBranch={selectedBranch}
                  height={520}
                  onSelectTable={(schemaName, tableName) => {
                    console.log('Selected table:', schemaName, tableName);
                  }}
                  onSelectColumn={(schemaName, tableName, columnName) => {
                    console.log('Selected column:', schemaName, tableName, columnName);
                  }}
                />
              </div>
            </div>
          )}

          {/* Schema Explorer Tab */}
          {activeTab === 'explorer' && (
            <BranchExplorerTab
              catalogId={catalogId!}
              branches={branches}
              currentBranch={selectedBranch}
            />
          )}

          {/* Branch Diff Tab */}
          {activeTab === 'diff' && (
            <BranchDiffTab
              catalogId={catalogId!}
              branches={branches}
              currentBranch={selectedBranch}
            />
          )}

          {/* Settings Tab */}
          {activeTab === 'settings' && (
            <div className="settings-panel">
              <div className="tab-header">
                <div className="tab-header-left">
                  <h3 className="section-title" style={{ margin: 0 }}>Catalog Settings</h3>
                </div>
                <div className="tab-header-right">
                  {editMode ? (
                    <>
                      <button
                        className="btn btn-ghost btn-sm"
                        onClick={() => {
                          setEditMode(false);
                          setEditedCatalog(catalog);
                        }}
                      >
                        Cancel
                      </button>
                      <button
                        className="btn btn-primary btn-sm"
                        onClick={handleSaveSettings}
                        disabled={saving}
                      >
                        {saving ? <RefreshCw size={14} className="spin" /> : <Save size={14} />}
                        Save Changes
                      </button>
                    </>
                  ) : (
                    <button
                      className="btn btn-secondary btn-sm"
                      onClick={() => setEditMode(true)}
                    >
                      <Edit3 size={14} />
                      Edit
                    </button>
                  )}
                </div>
              </div>

              <div className="settings-form">
                <div className="form-group">
                  <label className="form-label">Catalog ID</label>
                  <input
                    type="text"
                    className="form-input font-mono"
                    value={catalog.catalog_id}
                    disabled
                  />
                  <div className="form-hint">Cannot be changed after creation</div>
                </div>

                <div className="form-group">
                  <label className="form-label">Display Name</label>
                  <input
                    type="text"
                    className="form-input"
                    value={editedCatalog.display_name || ''}
                    onChange={(e) => setEditedCatalog({ ...editedCatalog, display_name: e.target.value })}
                    disabled={!editMode}
                  />
                </div>

                <div className="form-group">
                  <label className="form-label">Description</label>
                  <textarea
                    className="form-input form-textarea"
                    value={editedCatalog.description || ''}
                    onChange={(e) => setEditedCatalog({ ...editedCatalog, description: e.target.value })}
                    disabled={!editMode}
                    rows={3}
                  />
                </div>

                <div className="form-group">
                  <label className="form-label">Metadata URI</label>
                  <input
                    type="text"
                    className="form-input font-mono"
                    value={catalog.metadata_uri}
                    disabled
                  />
                  <div className="form-hint">Connection string changes require re-registration</div>
                </div>

                <div className="form-group">
                  <label className="form-label">Data Path</label>
                  <input
                    type="text"
                    className="form-input font-mono"
                    value={catalog.data_path}
                    disabled
                  />
                </div>

                <div className="form-group">
                  <label className="form-label">Tags</label>
                  <input
                    type="text"
                    className="form-input"
                    value={editedCatalog.tags?.join(', ') || ''}
                    onChange={(e) => setEditedCatalog({
                      ...editedCatalog,
                      tags: e.target.value.split(',').map((t) => t.trim()).filter(Boolean),
                    })}
                    disabled={!editMode}
                    placeholder="tag1, tag2, tag3"
                  />
                  <div className="form-hint">Comma-separated list of tags</div>
                </div>

                <div className="settings-danger-zone">
                  <h4>Danger Zone</h4>
                  <p>Unregistering this catalog will remove it from DuckLake. The underlying data will not be deleted.</p>
                  <button
                    className="btn btn-danger"
                    onClick={handleDeleteCatalog}
                  >
                    <Trash2 size={16} />
                    Unregister Catalog
                  </button>
                </div>
              </div>
            </div>
          )}
        </div>
      </div>

      {/* Create Branch Modal */}
      {showCreateBranch && (
        <div className="modal-overlay" onClick={() => setShowCreateBranch(false)}>
          <div className="modal" onClick={(e) => e.stopPropagation()}>
            <div className="modal-header">
              <h3 className="modal-title">Create Branch</h3>
              <button className="modal-close" onClick={() => setShowCreateBranch(false)}>
                <X size={20} />
              </button>
            </div>
            <div className="modal-body">
              <div className="form-group">
                <label className="form-label">Branch Name</label>
                <input
                  type="text"
                  className="form-input"
                  value={newBranchName}
                  onChange={(e) => setNewBranchName(e.target.value)}
                  placeholder="feature/my-branch"
                  autoFocus
                />
              </div>
              <div className="form-group">
                <label className="form-label">Parent Branch</label>
                <select
                  className="form-input form-select"
                  value={newBranchParent}
                  onChange={(e) => setNewBranchParent(e.target.value)}
                >
                  {branches.filter((b) => b.status === 'active').map((b) => (
                    <option key={b.branch_id} value={b.branch_name}>
                      {b.branch_name}
                    </option>
                  ))}
                </select>
              </div>
            </div>
            <div className="modal-footer">
              <button className="btn btn-secondary" onClick={() => setShowCreateBranch(false)}>
                Cancel
              </button>
              <button
                className="btn btn-primary"
                onClick={handleCreateBranch}
                disabled={!newBranchName.trim()}
              >
                Create Branch
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Branch Info Modal */}
      {showBranchInfo && branchInfoData && (
        <div className="modal-overlay" onClick={() => setShowBranchInfo(false)}>
          <div className="modal" style={{ maxWidth: '500px' }} onClick={(e) => e.stopPropagation()}>
            <div className="modal-header">
              <h3 className="modal-title">Branch Info: {branchInfoData.branch.branch_name}</h3>
              <button className="modal-close" onClick={() => setShowBranchInfo(false)}>
                <X size={20} />
              </button>
            </div>
            <div className="modal-body">
              {branchInfoLoading ? (
                <div style={{ textAlign: 'center', padding: '24px' }}>
                  <RefreshCw size={24} className="spin" style={{ color: 'var(--text-muted)' }} />
                  <p style={{ marginTop: '8px', color: 'var(--text-muted)' }}>Loading branch info...</p>
                </div>
              ) : (
                <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
                  {/* Branch Details */}
                  <div>
                    <h4 style={{ fontSize: '12px', fontWeight: 600, color: 'var(--text-muted)', marginBottom: '8px', textTransform: 'uppercase' }}>Branch Details</h4>
                    <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '8px', fontSize: '13px' }}>
                      <div>
                        <span style={{ color: 'var(--text-muted)' }}>Status:</span>{' '}
                        <span className={`badge badge-${branchInfoData.branch.status}`}>{branchInfoData.branch.status}</span>
                      </div>
                      <div>
                        <span style={{ color: 'var(--text-muted)' }}>Parent:</span>{' '}
                        <span>{branchInfoData.branch.parent_branch_name || '—'}</span>
                      </div>
                      <div>
                        <span style={{ color: 'var(--text-muted)' }}>Fork Snapshot:</span>{' '}
                        <span>#{branchInfoData.branch.fork_snapshot_id ?? 'N/A'}</span>
                      </div>
                      <div>
                        <span style={{ color: 'var(--text-muted)' }}>Head Snapshot:</span>{' '}
                        <span>#{branchInfoData.branch.head_snapshot_id}</span>
                      </div>
                      {branchInfoData.branch.created_at && (
                        <div>
                          <span style={{ color: 'var(--text-muted)' }}>Created:</span>{' '}
                          <span>{new Date(branchInfoData.branch.created_at).toLocaleString()}</span>
                        </div>
                      )}
                      {branchInfoData.stats && (
                        <div>
                          <span style={{ color: 'var(--text-muted)' }}>Modified:</span>{' '}
                          <span>{branchInfoData.stats.last_modified_at
                            ? new Date(branchInfoData.stats.last_modified_at).toLocaleString()
                            : 'Never'}</span>
                        </div>
                      )}
                    </div>
                  </div>

                  {/* Statistics */}
                  {branchInfoData.stats && (
                    <div>
                      <h4 style={{ fontSize: '12px', fontWeight: 600, color: 'var(--text-muted)', marginBottom: '8px', textTransform: 'uppercase' }}>Statistics</h4>
                      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: '12px' }}>
                        <div style={{ background: 'var(--bg-tertiary)', padding: '12px', borderRadius: '8px', textAlign: 'center' }}>
                          <div style={{ fontSize: '20px', fontWeight: 600 }}>{branchInfoData.stats.schema_count}</div>
                          <div style={{ fontSize: '11px', color: 'var(--text-muted)' }}>Schemas</div>
                        </div>
                        <div style={{ background: 'var(--bg-tertiary)', padding: '12px', borderRadius: '8px', textAlign: 'center' }}>
                          <div style={{ fontSize: '20px', fontWeight: 600 }}>{branchInfoData.stats.table_count}</div>
                          <div style={{ fontSize: '11px', color: 'var(--text-muted)' }}>Tables</div>
                        </div>
                        <div style={{ background: 'var(--bg-tertiary)', padding: '12px', borderRadius: '8px', textAlign: 'center' }}>
                          <div style={{ fontSize: '20px', fontWeight: 600 }}>{branchInfoData.stats.view_count}</div>
                          <div style={{ fontSize: '11px', color: 'var(--text-muted)' }}>Views</div>
                        </div>
                        <div style={{ background: 'var(--bg-tertiary)', padding: '12px', borderRadius: '8px', textAlign: 'center' }}>
                          <div style={{ fontSize: '20px', fontWeight: 600 }}>{branchInfoData.stats.snapshot_count}</div>
                          <div style={{ fontSize: '11px', color: 'var(--text-muted)' }}>Snapshots</div>
                        </div>
                        <div style={{ background: 'var(--bg-tertiary)', padding: '12px', borderRadius: '8px', textAlign: 'center' }}>
                          <div style={{ fontSize: '20px', fontWeight: 600 }}>{branchInfoData.stats.data_file_count}</div>
                          <div style={{ fontSize: '11px', color: 'var(--text-muted)' }}>Data Files</div>
                        </div>
                        <div style={{ background: 'var(--bg-tertiary)', padding: '12px', borderRadius: '8px', textAlign: 'center' }}>
                          <div style={{ fontSize: '20px', fontWeight: 600 }}>{branchInfoData.stats.total_rows != null ? formatNumber(branchInfoData.stats.total_rows) : '—'}</div>
                          <div style={{ fontSize: '11px', color: 'var(--text-muted)' }}>Total Rows</div>
                        </div>
                      </div>
                      {branchInfoData.stats.total_size_bytes != null && (
                        <div style={{ marginTop: '12px', fontSize: '13px', color: 'var(--text-muted)' }}>
                          Total Size: <span style={{ color: 'var(--text-primary)', fontWeight: 500 }}>{formatBytes(branchInfoData.stats.total_size_bytes)}</span>
                        </div>
                      )}
                    </div>
                  )}
                </div>
              )}
            </div>
            <div className="modal-footer">
              <button className="btn btn-secondary" onClick={() => setShowBranchInfo(false)}>
                Close
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Compare Branches Modal */}
      {showCompareModal && (
        <div className="modal-overlay" onClick={() => setShowCompareModal(false)}>
          <div className="modal" style={{ maxWidth: '700px', maxHeight: '80vh' }} onClick={(e) => e.stopPropagation()}>
            <div className="modal-header">
              <h3 className="modal-title">Compare Branches</h3>
              <button className="modal-close" onClick={() => setShowCompareModal(false)}>
                <X size={20} />
              </button>
            </div>
            <div className="modal-body" style={{ overflow: 'auto' }}>
              {/* Branch selectors */}
              <div style={{ display: 'flex', gap: '16px', marginBottom: '16px', alignItems: 'center' }}>
                <div style={{ flex: 1 }}>
                  <label style={{ fontSize: '12px', color: 'var(--text-muted)', display: 'block', marginBottom: '4px' }}>Base Branch</label>
                  <select
                    className="form-input form-select"
                    value={compareBaseBranch}
                    onChange={(e) => setCompareBaseBranch(e.target.value)}
                    style={{ width: '100%' }}
                  >
                    {branches.filter(b => b.status === 'active').map(b => (
                      <option key={b.branch_id} value={b.branch_name}>{b.branch_name}</option>
                    ))}
                  </select>
                </div>
                <div style={{ paddingTop: '20px', color: 'var(--text-muted)' }}>vs</div>
                <div style={{ flex: 1 }}>
                  <label style={{ fontSize: '12px', color: 'var(--text-muted)', display: 'block', marginBottom: '4px' }}>Compare Branch</label>
                  <select
                    className="form-input form-select"
                    value={compareTargetBranch}
                    onChange={(e) => setCompareTargetBranch(e.target.value)}
                    style={{ width: '100%' }}
                  >
                    {branches.filter(b => b.status === 'active').map(b => (
                      <option key={b.branch_id} value={b.branch_name}>{b.branch_name}</option>
                    ))}
                  </select>
                </div>
                <div style={{ paddingTop: '20px' }}>
                  <button
                    className="btn btn-primary"
                    onClick={() => fetchCompareDiff()}
                    disabled={compareLoading || compareBaseBranch === compareTargetBranch}
                  >
                    {compareLoading ? <RefreshCw size={14} className="spin" /> : 'Compare'}
                  </button>
                </div>
              </div>

              {/* Diff results */}
              {compareLoading ? (
                <div style={{ textAlign: 'center', padding: '40px' }}>
                  <RefreshCw size={24} className="spin" style={{ color: 'var(--text-muted)' }} />
                  <p style={{ marginTop: '8px', color: 'var(--text-muted)' }}>Loading diff...</p>
                </div>
              ) : compareDiff ? (
                <div>
                  {/* Summary */}
                  <div style={{ display: 'flex', gap: '8px', marginBottom: '16px', flexWrap: 'wrap' }}>
                    {compareDiff.summary.schemas_added > 0 && (
                      <span className="badge" style={{ background: 'rgba(34, 197, 94, 0.2)', color: '#22c55e' }}>
                        +{compareDiff.summary.schemas_added} schemas
                      </span>
                    )}
                    {compareDiff.summary.schemas_removed > 0 && (
                      <span className="badge" style={{ background: 'rgba(239, 68, 68, 0.2)', color: '#ef4444' }}>
                        -{compareDiff.summary.schemas_removed} schemas
                      </span>
                    )}
                    {compareDiff.summary.tables_added > 0 && (
                      <span className="badge" style={{ background: 'rgba(34, 197, 94, 0.2)', color: '#22c55e' }}>
                        +{compareDiff.summary.tables_added} tables
                      </span>
                    )}
                    {compareDiff.summary.tables_removed > 0 && (
                      <span className="badge" style={{ background: 'rgba(239, 68, 68, 0.2)', color: '#ef4444' }}>
                        -{compareDiff.summary.tables_removed} tables
                      </span>
                    )}
                    {compareDiff.summary.tables_modified > 0 && (
                      <span className="badge" style={{ background: 'rgba(245, 158, 11, 0.2)', color: '#f59e0b' }}>
                        ~{compareDiff.summary.tables_modified} tables modified
                      </span>
                    )}
                    {compareDiff.summary.columns_added > 0 && (
                      <span className="badge" style={{ background: 'rgba(34, 197, 94, 0.2)', color: '#22c55e' }}>
                        +{compareDiff.summary.columns_added} columns
                      </span>
                    )}
                    {compareDiff.summary.columns_removed > 0 && (
                      <span className="badge" style={{ background: 'rgba(239, 68, 68, 0.2)', color: '#ef4444' }}>
                        -{compareDiff.summary.columns_removed} columns
                      </span>
                    )}
                  </div>

                  {/* Schema tree */}
                  <div style={{ border: '1px solid var(--border-color)', borderRadius: '8px', overflow: 'hidden' }}>
                    {compareDiff.schemas.length === 0 ? (
                      <div style={{ padding: '24px', textAlign: 'center', color: 'var(--text-muted)' }}>
                        No differences found
                      </div>
                    ) : (
                      compareDiff.schemas.map(schema => (
                        <div key={schema.schema_name}>
                          <div
                            style={{
                              padding: '8px 12px',
                              background: 'var(--bg-secondary)',
                              borderBottom: '1px solid var(--border-color)',
                              display: 'flex',
                              alignItems: 'center',
                              gap: '8px',
                              cursor: 'pointer',
                            }}
                            onClick={() => {
                              const next = new Set(compareExpandedSchemas);
                              if (next.has(schema.schema_name)) next.delete(schema.schema_name);
                              else next.add(schema.schema_name);
                              setCompareExpandedSchemas(next);
                            }}
                          >
                            {compareExpandedSchemas.has(schema.schema_name) ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
                            <Folder size={14} style={{ color: 'var(--accent-primary)' }} />
                            <span style={{ fontWeight: 500 }}>{schema.schema_name}</span>
                            {schema.status === 'added' && <span style={{ color: '#22c55e', fontSize: '11px' }}>added</span>}
                            {schema.status === 'removed' && <span style={{ color: '#ef4444', fontSize: '11px' }}>removed</span>}
                            {schema.status === 'modified' && <span style={{ color: '#f59e0b', fontSize: '11px' }}>modified</span>}
                          </div>
                          {compareExpandedSchemas.has(schema.schema_name) && (
                            <div style={{ paddingLeft: '24px' }}>
                              {schema.tables.map(table => (
                                <div
                                  key={table.table_name}
                                  style={{
                                    padding: '6px 12px',
                                    borderBottom: '1px solid var(--border-color)',
                                    fontSize: '13px',
                                  }}
                                >
                                  <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                                    <Table size={12} style={{ color: 'var(--text-muted)' }} />
                                    <span>{table.table_name}</span>
                                    {table.status === 'added' && <span style={{ color: '#22c55e', fontSize: '11px' }}>+added</span>}
                                    {table.status === 'removed' && <span style={{ color: '#ef4444', fontSize: '11px' }}>-removed</span>}
                                    {table.status === 'modified' && <span style={{ color: '#f59e0b', fontSize: '11px' }}>~modified</span>}
                                  </div>
                                  {table.status === 'modified' && table.columns.length > 0 && (
                                    <div style={{ paddingLeft: '20px', marginTop: '4px', fontSize: '12px' }}>
                                      {table.columns.filter(c => c.status !== 'unchanged').map(col => (
                                        <div key={col.column_name} style={{ display: 'flex', gap: '8px', padding: '2px 0' }}>
                                          <span style={{
                                            color: col.status === 'added' ? '#22c55e' : col.status === 'removed' ? '#ef4444' : '#f59e0b'
                                          }}>
                                            {col.status === 'added' ? '+' : col.status === 'removed' ? '-' : '~'}
                                          </span>
                                          <span>{col.column_name}</span>
                                          <span style={{ color: 'var(--text-muted)' }}>
                                            {col.status === 'modified'
                                              ? `${col.base_type} → ${col.compare_type}`
                                              : col.base_type || col.compare_type
                                            }
                                          </span>
                                        </div>
                                      ))}
                                    </div>
                                  )}
                                </div>
                              ))}
                            </div>
                          )}
                        </div>
                      ))
                    )}
                  </div>
                </div>
              ) : (
                <div style={{ textAlign: 'center', padding: '40px', color: 'var(--text-muted)' }}>
                  Select branches and click Compare to see differences
                </div>
              )}
            </div>
            <div className="modal-footer">
              <button className="btn btn-secondary" onClick={() => setShowCompareModal(false)}>
                Close
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
