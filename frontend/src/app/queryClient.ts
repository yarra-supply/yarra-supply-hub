import { QueryClient, keepPreviousData } from '@tanstack/react-query';


// 它的作用是全局管理前端应用中的数据请求、缓存、自动刷新等功能。
// 具体功能说明：
// QueryClient 是 React Query 的核心对象，负责管理所有的查询（query）和变更（mutation）。
// defaultOptions 设置了全局的查询行为：
// refetchOnWindowFocus: false：窗口聚焦时不自动重新请求数据。
// placeholderData: keepPreviousData：切换查询参数时保留上一次的数据，避免界面闪烁。
// retry: 1：请求失败时最多重试 1 次。
// 通常这个 queryClient 会在应用的根组件用 <QueryClientProvider client={queryClient}> 包裹，给全局提供数据请求和缓存能力。


export const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      refetchOnWindowFocus: false,
      placeholderData: keepPreviousData,
      retry: 1,
    },
  },
});
