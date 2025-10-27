

import axios from 'axios'

export const api = axios.create({
  baseURL: '/api/v1',       // 只写相对路径！交给 Vite 代理去转发
  withCredentials: true,    // Cookie 登录建议打开；JWT 也不受影响
})

// 如果是 JWT，也可以加个拦截器（可选）
let token: string | null = null
export function setToken(t: string | null) { token = t }
api.interceptors.request.use((config) => {
  if (token) config.headers.Authorization = `Bearer ${token}`
  return config
})
