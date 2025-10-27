# 依赖基底
FROM node:20-alpine AS base
WORKDIR /app
COPY package*.json ./
RUN npm ci || npm i

# 开发阶段（vite dev server）
FROM base AS dev
COPY . .
EXPOSE 5173
CMD ["npm","run","dev","--","--host"]

# 构建阶段（用于 S3/CloudFront 或 nginx 静态托管）
FROM base AS builder
COPY . .
# 提示：生产构建会读取 frontend/.env.production（或用命令注入 VITE_API_BASE）
RUN npm run build
# 产物在 /app/dist
