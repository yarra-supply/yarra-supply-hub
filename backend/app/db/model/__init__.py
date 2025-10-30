
# 聚合导入所有模型，供 Alembic 发现

from .product import (
    SkuInfo,
    ProductSyncRun,
    ProductSyncCandidate,
    ProductSyncChunk,   
)

from .freight import (
    SkuFreightFee,
    FreightRun,
)

from .freight_cal_config import FreightCalcConfig
from .kogan_au_template import KoganTemplate
from .kogan_export_job import KoganExportJob, KoganExportJobSku
from .schedule import Schedule
from .user import User

__all__ = [
    # product
    "SkuInfo", "ProductSyncRun", "ProductSyncCandidate", "ProductSyncChunk",
    # freight
    "SkuFreightFee", "FreightRun",
    # others
    "FreightCalcConfig", "KoganTemplate", "KoganExportJob", "KoganExportJobSku", "Schedule", "User",
]
