
import { useEffect, useState } from "react";
// import '@/App.css'
import { RouterProvider } from 'react-router-dom';
import { router } from './routes';
import { QueryClientProvider } from '@tanstack/react-query';
import { queryClient } from './queryClient';
import 'antd/dist/reset.css';
import { me } from "../api/auth"; // 新增：用于检测是否已登录
import { Spin } from "antd";


export default function App() {

  const [checked, setChecked] = useState(false);
  const [isAuthed, setIsAuthed] = useState(false);

  useEffect(() => {
    // 检查当前是否已登录
    me()
      .then(() => setIsAuthed(true))
      .catch(() => setIsAuthed(false))
      .finally(() => setChecked(true));
  }, []);

  if (!checked) {
    return (
      <div
        style={{
          height: "100vh",
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
        }}
      >
        <Spin tip="Loading user..." size="large" />
      </div>
    );
  }

  // 若用户未登录且当前不在 /login，则强制跳转登录
  if (!isAuthed && window.location.pathname !== "/login") {
    window.location.href = "/login";
    return null;
  }

  // 若用户已登录且在 /login，则自动跳首页
  if (isAuthed && window.location.pathname === "/login") {
    window.location.href = "/";
    return null;
  }

  return (
    <QueryClientProvider client={queryClient}>
      <RouterProvider router={router} />
    </QueryClientProvider>
  );
}

