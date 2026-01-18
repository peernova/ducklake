import { NavLink, Outlet } from 'react-router-dom';
import {
  Database,
  LayoutDashboard,
  Search,
  Settings,
  Terminal,
  FileCode,
} from 'lucide-react';

export default function Layout() {

  const navItems = [
    { path: '/dashboard', icon: LayoutDashboard, label: 'Dashboard' },
    { path: '/catalogs', icon: Database, label: 'Catalogs' },
    { path: '/query', icon: Terminal, label: 'Query' },
    { path: '/api-docs', icon: FileCode, label: 'API Docs' },
  ];

  return (
    <div className="app-layout">
      <aside className="sidebar">
        <div className="sidebar-header">
          <img src="/duck.svg" alt="DuckLake" className="sidebar-logo" />
          <span className="sidebar-title">DuckLake</span>
        </div>

        <nav className="sidebar-nav">
          <div className="nav-section">
            <div className="nav-section-title">Main</div>
            {navItems.map((item) => (
              <NavLink
                key={item.path}
                to={item.path}
                className={({ isActive }) =>
                  `nav-item ${isActive ? 'active' : ''}`
                }
              >
                <item.icon />
                <span>{item.label}</span>
              </NavLink>
            ))}
          </div>

          <div className="nav-section">
            <div className="nav-section-title">Quick Access</div>
            <NavLink
              to="/catalogs"
              state={{ filter: 'recent' }}
              className="nav-item"
            >
              <Database />
              <span>Recent Catalogs</span>
            </NavLink>
            <NavLink
              to="/query"
              state={{ history: true }}
              className="nav-item"
            >
              <Search />
              <span>Query History</span>
            </NavLink>
          </div>
        </nav>

        <div className="sidebar-nav" style={{ borderTop: '1px solid rgba(255,255,255,0.1)', paddingTop: '16px' }}>
          <NavLink to="/settings" className="nav-item">
            <Settings />
            <span>Settings</span>
          </NavLink>
        </div>
      </aside>

      <main className="main-content">
        <div className="page-content">
          <Outlet />
        </div>
      </main>
    </div>
  );
}
