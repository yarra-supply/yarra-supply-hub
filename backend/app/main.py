
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from app.core.config import settings
from app.api.v1 import api_v1

app = FastAPI(title=settings.PROJECT_NAME)

# 从环境读取前端白名单（逗号分隔）。本地可配：
# BACKEND_CORS_ORIGINS=http://localhost:5173,https://app.local.test:5173
origins = [o.strip() for o in settings.BACKEND_CORS_ORIGINS.split(",") if o.strip()]


app.add_middleware(
    CORSMiddleware, 
    allow_origins=origins,     #配成明确白名单（本地 http://localhost:5173，线上是前端域名）
    allow_credentials=True,    # Access-Control-Allow-Credentials: true
    allow_methods=["GET","POST","PUT","PATCH","DELETE","OPTIONS"],
    allow_headers=["*"],
    )


# Origin 校验（仅对改数据方法）。放行第三方服务器回调（如 Shopify Webhook）
TRUSTED = {o.strip() for o in settings.BACKEND_CORS_ORIGINS.split(",") if o.strip()}
WEBHOOK_PATH_PREFIXES = (
    "/api/v1/webhooks/shopify",   # 你实际的 webhook 路径
    "/api/v1/shopify/callback",   # 如果用到 OAuth 回调等
)

@app.middleware("http")
async def origin_check(request: Request, call_next):
    p = request.url.path

    if p.startswith(WEBHOOK_PATH_PREFIXES):
        # 服务器回调不在浏览器上下文，不适用 Origin 校验
        return await call_next(request)

    if request.method in {"POST", "PUT", "PATCH", "DELETE"}:
        origin = request.headers.get("origin")
        # 没有 Origin（如 curl/健康检查）则放行
        if not origin:
            return await call_next(request)
        # 有 Origin 但不在白名单里，才拒绝
        if origin not in TRUSTED:
            raise HTTPException(status_code=403, detail="Bad Origin")
        
    return await call_next(request)


app.include_router(api_v1, prefix=settings.API_PREFIX)

# 根路径健康探活（方便测试或 Docker 健康检查）
@app.get("/")
def root():
    return {
        "app": settings.PROJECT_NAME,
        "env": settings.ENVIRONMENT,
        "ok": True
    }