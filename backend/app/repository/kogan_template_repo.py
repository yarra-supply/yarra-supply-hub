
from __future__ import annotations
from typing import Dict, Iterable, Iterator, List, Optional, Tuple

from sqlalchemy import select, and_
from sqlalchemy.orm import Session

from app.db.model.freight import SkuFreightFee
from app.db.model.kogan_au_template import KoganTemplate



"""
分页迭代待导出的 运费结果表中本次更新/新增的运费结果：

    以批次形式迭代返回“需要导出的 SKU 列表”。
    - 当 only_dirty=True：WHERE kogan_dirty=true
    - 当提供 freight_run_id：WHERE last_changed_run_id=...
    - 两者都提供时，取交集条件（更严格）
    """
def iter_changed_skus(
    db: Session,
    batch_size: int = 5000,
) -> Iterator[List[str]]:
    
    # 分页迭代待导出的 SKU（固定：WHERE kogan_dirty=true）
    q = (
        db.query(SkuFreightFee.sku_code)
        .filter(SkuFreightFee.kogan_dirty.is_(True))
        .order_by(SkuFreightFee.sku_code.asc())
    )

    # 用 offset/limit 分页；4 万级别可接受。如需更大规模可改为 keyset 分页。
    # 默认一批 5000 todo 配置修改？
    offset = 0
    while True:
        batch = q.offset(offset).limit(batch_size).all()
        if not batch:
            break
        skus = [r.sku_code for r in batch]
        yield skus
        offset += batch_size


# 读取 KoganTemplate 表的历史基线，返回 {sku: ORM对象}，供 service 做列级 diff 使用
def load_kogan_baseline_map(db: Session, country_type: str, skus: List[str]) -> Dict[str, KoganTemplate]:

    if not skus:
        return {}

    rows: List[KoganTemplate] = (
        db.query(KoganTemplate)
        .filter(
            KoganTemplate.country_type == country_type,
            KoganTemplate.sku.in_(skus),
        )
        .all()
    )
    return {r.sku: r for r in rows}


# todo 怎么用？
def _chunker(items: List[str], size: int) -> Iterator[List[str]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]

