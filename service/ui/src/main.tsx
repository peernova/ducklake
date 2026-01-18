import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { ModuleRegistry, AllCommunityModule } from 'ag-grid-community';
import './index.css';
import App from './App';

// Register AG Grid modules
ModuleRegistry.registerModules([AllCommunityModule]);

// Render app - connects to real Java backend via proxy
createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <App />
  </StrictMode>
);
