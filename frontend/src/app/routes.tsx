
import { createBrowserRouter, Navigate } from 'react-router-dom';
import AppLayout from '@/components/AppLayout';
import LoginPage from '@/pages/LoginPage';
import DashboardPage from '@/pages/DashboardPage';
import ProductsPage from '@/pages/Products/ProductsPage';
import SyncRunsPage from '@/pages/Products/SyncRunsPage';
import SchedulesPage from '@/pages/Schedules/SchedulePage';

import FreightResultsPage from '@/pages/Freight/FreightResultsPage';  
import FreightCalParamConfig from '@/pages/Freight/FreightCalParamConfig';  

import KoganTemplateDataDownload from '@/pages/download/KoganTemplateDataDownload';
import ShopifyJobsPage from '@/pages/ShopifyJobs/ShopifyJobsPage';



export const router = createBrowserRouter([
  // 登录页单独路由（不走应用布局）
  { path: '/', element: <Navigate to="/dashboard" replace /> },
  { path: '/login', element: <LoginPage /> },

  // 应用布局
  {
    path: '/',
    element: <AppLayout />,
    children: [
      { path: 'dashboard', element: <DashboardPage /> },

      { path: 'products', element: <ProductsPage /> },
      { path: 'sync-runs', element: <SyncRunsPage /> },

      { path: 'schedules', element: <SchedulesPage /> },
      { path: 'freight-results', element: <FreightResultsPage /> },
      { path: 'freight-config', element: <FreightCalParamConfig /> },
      
      { path: 'download/kogan-template', element: <KoganTemplateDataDownload /> },

      { path: 'shopify-jobs', element: <ShopifyJobsPage /> },
    ],
  },

]);
