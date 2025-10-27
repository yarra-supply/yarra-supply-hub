
from fastapi import APIRouter, Depends
from app.services.auth_service import get_current_user

# origin的
# from .routes_ops import router as ops_router
# from app.api.v1.dsz_ops import router as dsz_router
# from app.api.v1.shopify_bulk_ops import router as shopify_router


# 非受保护路由
from .routes_health import router as health_router
from .auth import router as auth_router


# 需要登录的受保护路由
from .freight import router as freight_router
from .freight_config import router as freight_config_router
from .kogan_template_download import router as kogan_template_router
from .product import router as product_router
from .routes_ops import router as ops_router
from .scheduler import router as scheduler_router
from .shopify_bulk_ops import router as shopify_router
# from .shopify_task import router as shopify_task_router
from .webhooks_shopify import router as webhooks_router
from .dsz_ops import router as dsz_router


# todo ?
# origin
# api_router = APIRouter()
# api_router.include_router(dsz_router)
# api_router.include_router(shopify_router)
# api_router.include_router(ops_router)



api_v1 = APIRouter()
api_v1.include_router(health_router)      # /health 不需要登录
api_v1.include_router(auth_router)        # /auth 登录相关

# --- 需要登录的接口 ---
protected = APIRouter(dependencies=[Depends(get_current_user)])

protected.include_router(freight_router)
protected.include_router(freight_config_router)
protected.include_router(kogan_template_router)
protected.include_router(product_router)
protected.include_router(ops_router)
protected.include_router(scheduler_router)
protected.include_router(shopify_router)
# protected.include_router(shopify_task_router)
protected.include_router(webhooks_router)
protected.include_router(dsz_router)

# 把受保护路由注册进主路由
api_v1.include_router(protected)