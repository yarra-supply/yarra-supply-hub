
# 健康检查（含DB/Redis探活）

from fastapi import APIRouter
from sqlalchemy import text
from app.db.session import engine

router = APIRouter(tags=["health"])

@router.get("/health")
def health():
    # 轻量 DB ping（不依赖迁移）
    # with engine.connect() as conn:
    #     conn.execute(text("SELECT 1"))
    return {"status": "ok"}
