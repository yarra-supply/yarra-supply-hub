
import { http } from "./http";

export type LoginPayload = { username: string; password: string }; 

export interface User {
  id: number;
  username: string;
  full_name?: string | null;
  is_superuser: boolean;
}


// Cookie 登录：后端会 Set-Cookie，前端同源自动带上
export async function login(payload: LoginPayload) {
  const res = await http.post("/auth/login", payload);
  return res.data;
}


export async function logout() {
  await http.post("/auth/logout");
}


// 获取当前用户（用于守卫页面）
export async function me() {
  const res = await http.get("/auth/me");
  return res.data;
}
