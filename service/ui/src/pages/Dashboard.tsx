import { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import {
  Database,
  GitBranch,
  Table,
  HardDrive,
  ArrowRight,
  Activity,
} from 'lucide-react';
import type { Catalog, BranchActivity } from '../types';

// Mock data for now - will be replaced with API calls
const mockCatalogs: Catalog[] = [
  {
    catalog_id: 'xva_desk',
    display_name: 'XVA Trading Desk',
    description: 'Production XVA risk calculations and trade data',
    metadata_uri: 'postgres:dbname=xva host=localhost',
    data_path: 's3://xva-bucket/data',
    metadata_type: 'postgres',
    storage_type: 's3',
    branch_count: 5,
    table_count: 24,
    total_size_bytes: 1073741824,
    enabled: true,
    created_at: '2024-01-01T00:00:00Z',
  },
  {
    catalog_id: 'market_data',
    display_name: 'Market Data',
    description: 'Real-time and historical market data feeds',
    metadata_uri: 'postgres:dbname=market host=localhost',
    data_path: 's3://market-bucket/data',
    metadata_type: 'postgres',
    storage_type: 's3',
    branch_count: 3,
    table_count: 12,
    total_size_bytes: 5368709120,
    enabled: true,
    created_at: '2024-01-15T00:00:00Z',
  },
];

const mockActivity: BranchActivity[] = [
  {
    branch_id: 1,
    branch_name: 'feature_new_model',
    created_at: '2024-01-15T10:30:00Z',
    last_modified_at: '2024-01-15T14:30:00Z',
    snapshot_count: 5,
    head_snapshot_id: 15,
    status: 'active',
  },
  {
    branch_id: 0,
    branch_name: 'main',
    created_at: '2024-01-01T00:00:00Z',
    last_modified_at: '2024-01-15T12:00:00Z',
    snapshot_count: 34,
    head_snapshot_id: 34,
    status: 'active',
  },
];

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
}

function formatRelativeTime(date: string): string {
  const now = new Date();
  const d = new Date(date);
  const diffMs = now.getTime() - d.getTime();
  const diffMins = Math.floor(diffMs / 60000);
  const diffHours = Math.floor(diffMs / 3600000);
  const diffDays = Math.floor(diffMs / 86400000);

  if (diffMins < 1) return 'just now';
  if (diffMins < 60) return `${diffMins}m ago`;
  if (diffHours < 24) return `${diffHours}h ago`;
  if (diffDays < 7) return `${diffDays}d ago`;
  return d.toLocaleDateString();
}

export default function Dashboard() {
  const [catalogs, setCatalogs] = useState<Catalog[]>([]);
  const [activity, setActivity] = useState<BranchActivity[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    // Simulate API call
    setTimeout(() => {
      setCatalogs(mockCatalogs);
      setActivity(mockActivity);
      setLoading(false);
    }, 500);
  }, []);

  const totalBranches = catalogs.reduce((sum, c) => sum + (c.branch_count || 0), 0);
  const totalTables = catalogs.reduce((sum, c) => sum + (c.table_count || 0), 0);
  const totalSize = catalogs.reduce((sum, c) => sum + (c.total_size_bytes || 0), 0);

  if (loading) {
    return (
      <div className="loading">
        <div className="spinner" />
      </div>
    );
  }

  return (
    <div>
      <div className="page-header">
        <div className="page-header-row">
          <div>
            <h1 className="page-title">Dashboard</h1>
            <p className="page-description">Overview of your DuckLake data catalogs</p>
          </div>
        </div>
      </div>

      <div className="stats-grid">
        <div className="stat-card">
          <div className="stat-icon stat-icon-blue">
            <Database />
          </div>
          <div className="stat-content">
            <div className="stat-value">{catalogs.length}</div>
            <div className="stat-label">Catalogs</div>
          </div>
        </div>

        <div className="stat-card">
          <div className="stat-icon stat-icon-purple">
            <GitBranch />
          </div>
          <div className="stat-content">
            <div className="stat-value">{totalBranches}</div>
            <div className="stat-label">Total Branches</div>
          </div>
        </div>

        <div className="stat-card">
          <div className="stat-icon stat-icon-amber">
            <Table />
          </div>
          <div className="stat-content">
            <div className="stat-value">{totalTables}</div>
            <div className="stat-label">Tables</div>
          </div>
        </div>

        <div className="stat-card">
          <div className="stat-icon stat-icon-green">
            <HardDrive />
          </div>
          <div className="stat-content">
            <div className="stat-value">{formatBytes(totalSize)}</div>
            <div className="stat-label">Total Data</div>
          </div>
        </div>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: '2fr 1fr', gap: '24px' }}>
        <div className="card">
          <div className="card-header">
            <h3 className="card-title">Catalogs</h3>
            <Link to="/catalogs" className="btn btn-ghost btn-sm">
              View All <ArrowRight size={14} />
            </Link>
          </div>
          <div className="card-body" style={{ padding: 0 }}>
            <table className="table">
              <thead>
                <tr>
                  <th>Name</th>
                  <th>Branches</th>
                  <th>Tables</th>
                  <th>Size</th>
                  <th>Storage</th>
                </tr>
              </thead>
              <tbody>
                {catalogs.map((catalog) => (
                  <tr key={catalog.catalog_id}>
                    <td>
                      <Link to={`/catalogs/${catalog.catalog_id}`} className="table-link">
                        {catalog.display_name || catalog.catalog_id}
                      </Link>
                    </td>
                    <td>{catalog.branch_count}</td>
                    <td>{catalog.table_count}</td>
                    <td>{formatBytes(catalog.total_size_bytes || 0)}</td>
                    <td>
                      <span className="tag">{catalog.storage_type}</span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        <div className="card">
          <div className="card-header">
            <h3 className="card-title">Recent Activity</h3>
            <Activity size={18} className="text-muted" />
          </div>
          <div className="card-body" style={{ padding: 0 }}>
            {activity.map((item) => (
              <div
                key={`${item.branch_id}-${item.branch_name}`}
                style={{
                  padding: '12px 20px',
                  borderBottom: '1px solid var(--border-light)',
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'space-between',
                }}
              >
                <div>
                  <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <GitBranch size={14} className="text-muted" />
                    <span className={`branch-tree-name ${item.branch_name === 'main' ? 'main' : ''}`}>
                      {item.branch_name}
                    </span>
                  </div>
                  <div className="text-sm text-muted" style={{ marginTop: '4px' }}>
                    {item.snapshot_count} snapshots
                  </div>
                </div>
                <div className="text-sm text-muted">
                  {formatRelativeTime(item.last_modified_at || item.created_at)}
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
