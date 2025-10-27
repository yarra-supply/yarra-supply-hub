

from __future__ import annotations
from typing import Dict, Any
from sqlalchemy.orm import Session
from app.repository.freight_cal_config_repo import get_or_create_config, to_dict, DEFAULTS


"""
统一从 freight_cal_config_repo 读取/创建一份配置行，
转 dict，并用 DEFAULTS 做兜底，返回扁平的配置字典
"""
def load_freight_calc_config(db: Session) -> Dict[str, Any]:
    row = get_or_create_config(db)
    data = to_dict(row)
    # 兜底：若缺某列（迁移不一致等），用默认值
    for k, v in DEFAULTS.items():
        data.setdefault(k, v)
    return data
