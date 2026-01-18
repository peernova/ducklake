import { useEffect, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  Database,
  GitBranch,
  Table,
  HardDrive,
  Plus,
  Search,
  X,
} from 'lucide-react';
import { catalogsApi } from '../api';
import type { Catalog, RegisterCatalogRequest } from '../types';

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
}

export default function Catalogs() {
  const navigate = useNavigate();
  const [catalogs, setCatalogs] = useState<Catalog[]>([]);
  const [loading, setLoading] = useState(true);
  const [searchQuery, setSearchQuery] = useState('');
  const [showModal, setShowModal] = useState(false);
  const [formData, setFormData] = useState<RegisterCatalogRequest>({
    catalog_id: '',
    display_name: '',
    description: '',
    metadata_uri: '',
    data_path: '',
  });

  useEffect(() => {
    const fetchCatalogs = async () => {
      try {
        const response = await catalogsApi.list();
        // Handle both array response and wrapped response
        const data = Array.isArray(response) ? response : response.catalogs || [];
        setCatalogs(data);
      } catch (error) {
        console.error('Failed to fetch catalogs:', error);
        setCatalogs([]);
      } finally {
        setLoading(false);
      }
    };
    fetchCatalogs();
  }, []);

  const filteredCatalogs = catalogs.filter(
    (c) =>
      c.catalog_id.toLowerCase().includes(searchQuery.toLowerCase()) ||
      c.display_name?.toLowerCase().includes(searchQuery.toLowerCase()) ||
      c.description?.toLowerCase().includes(searchQuery.toLowerCase())
  );

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    // TODO: API call to register catalog
    console.log('Register catalog:', formData);
    setShowModal(false);
    setFormData({
      catalog_id: '',
      display_name: '',
      description: '',
      metadata_uri: '',
      data_path: '',
    });
  };

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
            <h1 className="page-title">Catalogs</h1>
            <p className="page-description">Manage your DuckLake data catalogs</p>
          </div>
          <div className="page-actions">
            <button className="btn btn-primary" onClick={() => setShowModal(true)}>
              <Plus size={16} />
              Register Catalog
            </button>
          </div>
        </div>
      </div>

      <div className="card mb-4">
        <div className="card-body" style={{ padding: '12px 16px' }}>
          <div className="search-box">
            <div className="search-icon">
              <Search />
            </div>
            <input
              type="text"
              className="form-input"
              placeholder="Search catalogs..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
            />
          </div>
        </div>
      </div>

      <div className="catalog-cards">
        {filteredCatalogs.map((catalog) => (
          <div
            key={catalog.catalog_id}
            className="catalog-card"
            onClick={() => navigate(`/catalogs/${catalog.catalog_id}`)}
          >
            <div className="catalog-card-header">
              <div>
                <div className="catalog-card-title">
                  {catalog.display_name || catalog.catalog_id}
                </div>
                <div className="text-sm text-muted font-mono">{catalog.catalog_id}</div>
              </div>
              <span className={`badge badge-${catalog.enabled ? 'active' : 'archived'}`}>
                {catalog.enabled ? 'Active' : 'Disabled'}
              </span>
            </div>

            {catalog.description && (
              <div className="catalog-card-description">{catalog.description}</div>
            )}

            <div className="catalog-card-meta">
              <div className="catalog-card-meta-item">
                <GitBranch />
                <span>{catalog.branch_count} branches</span>
              </div>
              <div className="catalog-card-meta-item">
                <Table />
                <span>{catalog.table_count} tables</span>
              </div>
              <div className="catalog-card-meta-item">
                <HardDrive />
                <span>{formatBytes(catalog.total_size_bytes || 0)}</span>
              </div>
              <div className="catalog-card-meta-item">
                <Database />
                <span>{catalog.metadata_type}</span>
              </div>
            </div>

            {catalog.tags && catalog.tags.length > 0 && (
              <div style={{ marginTop: '12px', display: 'flex', gap: '6px', flexWrap: 'wrap' }}>
                {catalog.tags.map((tag) => (
                  <span key={tag} className="tag">{tag}</span>
                ))}
              </div>
            )}
          </div>
        ))}
      </div>

      {filteredCatalogs.length === 0 && (
        <div className="empty-state">
          <Database className="empty-state-icon" />
          <div className="empty-state-title">No catalogs found</div>
          <div className="empty-state-description">
            {searchQuery
              ? 'Try adjusting your search query'
              : 'Get started by registering your first catalog'}
          </div>
          {!searchQuery && (
            <button className="btn btn-primary" onClick={() => setShowModal(true)}>
              <Plus size={16} />
              Register Catalog
            </button>
          )}
        </div>
      )}

      {showModal && (
        <div className="modal-overlay" onClick={() => setShowModal(false)}>
          <div className="modal" onClick={(e) => e.stopPropagation()}>
            <div className="modal-header">
              <h3 className="modal-title">Register Catalog</h3>
              <button className="modal-close" onClick={() => setShowModal(false)}>
                <X size={20} />
              </button>
            </div>
            <form onSubmit={handleSubmit}>
              <div className="modal-body">
                <div className="form-group">
                  <label className="form-label">Catalog ID</label>
                  <input
                    type="text"
                    className="form-input"
                    placeholder="my_catalog"
                    value={formData.catalog_id}
                    onChange={(e) => setFormData({ ...formData, catalog_id: e.target.value })}
                    required
                  />
                  <div className="form-hint">Unique identifier (lowercase, underscores allowed)</div>
                </div>

                <div className="form-group">
                  <label className="form-label">Display Name</label>
                  <input
                    type="text"
                    className="form-input"
                    placeholder="My Catalog"
                    value={formData.display_name}
                    onChange={(e) => setFormData({ ...formData, display_name: e.target.value })}
                  />
                </div>

                <div className="form-group">
                  <label className="form-label">Description</label>
                  <textarea
                    className="form-input form-textarea"
                    placeholder="Description of this catalog..."
                    value={formData.description}
                    onChange={(e) => setFormData({ ...formData, description: e.target.value })}
                  />
                </div>

                <div className="form-group">
                  <label className="form-label">Metadata URI</label>
                  <input
                    type="text"
                    className="form-input"
                    placeholder="postgres:dbname=mydb host=localhost port=5432"
                    value={formData.metadata_uri}
                    onChange={(e) => setFormData({ ...formData, metadata_uri: e.target.value })}
                    required
                  />
                  <div className="form-hint">PostgreSQL, DuckDB, or SQLite connection string</div>
                </div>

                <div className="form-group">
                  <label className="form-label">Data Path</label>
                  <input
                    type="text"
                    className="form-input"
                    placeholder="s3://my-bucket/data or /local/path"
                    value={formData.data_path}
                    onChange={(e) => setFormData({ ...formData, data_path: e.target.value })}
                    required
                  />
                  <div className="form-hint">S3, GCS, Azure, or local filesystem path</div>
                </div>
              </div>
              <div className="modal-footer">
                <button type="button" className="btn btn-secondary" onClick={() => setShowModal(false)}>
                  Cancel
                </button>
                <button type="submit" className="btn btn-primary">
                  Register
                </button>
              </div>
            </form>
          </div>
        </div>
      )}
    </div>
  );
}
