
import axios from 'axios';

// 同时支持“同源代理模式”(默认) 与 “直连独立域名模式”（线上可选）
// - 默认（VITE_USE_PROXY 不设或为 'true'）：baseURL='/api/v1'，走代理
// - 如线上不走代理：设置 VITE_USE_PROXY=false + VITE_API_BASE=https://api.xxx.com/api/v1

const useProxy = (import.meta.env.VITE_USE_PROXY ?? "true") !== "false";  

const baseURL = useProxy ? "/api/v1" : (import.meta.env.VITE_API_BASE ?? "/api/v1");


// 同时提供命名导出与默认导出，避免导入方式不一致踩坑
export const http = axios.create({
  baseURL,                 
  withCredentials: true,   // Cookie 登录要带上
  timeout: 30000,
});


// 可选：如果未来用“Header JWT”，暴露设置方法
// export function setAuthToken(token: string | null) {
//   if (token) {
//     http.defaults.headers.common.Authorization = `Bearer ${token}`;
//   } else {
//     delete http.defaults.headers.common.Authorization;
//   }
// }

export default http;


