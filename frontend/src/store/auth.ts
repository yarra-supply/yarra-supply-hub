
import { create } from 'zustand';


// 这段代码是用 zustand 创建的前端全局认证（登录）状态管理。

// 功能说明：

// 定义了 AuthState 类型，包含 token（存储登录令牌）、setToken（设置 token）、isAuthed（判断是否已登录）三个字段。
// useAuth 是一个自定义 hook，可以在 React 组件中调用，获取和设置 token，以及判断用户是否已认证。
// 如果用 Bearer token 登录，token 会存到状态里；如果用 cookie 登录，可以不用存 token，isAuthed 可以改为请求后端接口判断。
// 作用：让前端任意组件都能方便地获取和修改登录状态，实现登录、登出、鉴权等功能。


type AuthState = {
  token?: string; // 如果你用 Bearer；若用 cookie 可以不存
  setToken: (t?: string) => void;
  isAuthed: () => boolean;
};

export const useAuth = create<AuthState>((set, get) => ({
  token: undefined,
  setToken: (t) => set({ token: t }),
  isAuthed: () => !!get().token, // cookie 场景可改为检查后端 /me
}));

