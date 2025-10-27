
from __future__ import annotations
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, Field, field_validator
from typing import List, Dict, Any
from app.integrations.dsz.dsz_products import get_products_by_skus_with_stats, get_products_by_skus
from app.services.auth_service import get_current_user



router = APIRouter(
    prefix="/dsz", 
    tags=["dsz"],
    dependencies=[Depends(get_current_user)], 
)


#todo 运维方法重写

# class DSZQueryBody(BaseModel):
#     skus: List[str] = Field(..., description="DSZ SKU 列表，建议 ≤ 50/次（与上游批大小一致）")
#     normalized: bool = Field(True, description="是否返回归一化结果")
#     @field_validator("skus")
#     @classmethod
#     def _non_empty_unique(cls, v: List[str]) -> List[str]:
#         v = [s.strip() for s in v if s and s.strip()]
#         if not v:
#             raise ValueError("skus 不能为空")
#         # 去重并保持顺序
#         return list(dict.fromkeys(v))
    

# @router.post("/products/by-skus")
# def dsz_products_by_skus(body: DSZQueryBody) -> Dict[str, Any]:
#     try:
#         rows = fetch_dsz_by_codes(body.skus)
#         items = [normalize_dsz_product(r) for r in rows] if body.normalized else rows
#         return {"count": len(items), "items": items}
#     except Exception as e:
#         # 收敛为 502，前端友好提示
#         raise HTTPException(status_code=502, detail=f"DSZ 查询失败: {e}")