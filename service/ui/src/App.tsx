import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import Layout from './components/Layout';
import Dashboard from './pages/Dashboard';
import Catalogs from './pages/Catalogs';
import CatalogDetail from './pages/CatalogDetail';
import AccessLogs from './pages/AccessLogs';
import Query from './pages/Query';
import QueryV2 from './pages/QueryV2';
import ApiDocs from './pages/ApiDocs';

function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<Layout />}>
          <Route index element={<Navigate to="/dashboard" replace />} />
          <Route path="dashboard" element={<Dashboard />} />
          <Route path="catalogs" element={<Catalogs />} />
          <Route path="catalogs/:catalogId" element={<CatalogDetail />} />
          <Route path="resource-access-log" element={<AccessLogs />} />
          <Route path="query" element={<QueryV2 />} />
          <Route path="query-old" element={<Query />} />
          <Route path="api-docs" element={<ApiDocs />} />
          <Route path="settings" element={<div className="page-content"><h1>Settings</h1><p>Coming soon...</p></div>} />
        </Route>
      </Routes>
    </BrowserRouter>
  );
}

export default App;
