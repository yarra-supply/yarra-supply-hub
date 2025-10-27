
from __future__ import annotations
from typing import Dict, Iterable, Iterator, List, Optional, Tuple
from datetime import datetime, timezone
import csv
import io


from sqlalchemy.orm import Session

from app.repository.product_repo import load_products_map
from app.repository.freight_repo import load_freight_map
from app.repository.kogan_template_repo import (
    iter_changed_skus,
    load_kogan_baseline_map,
)


# batch size 默认常量
DEFAULT_BATCH_SIZE = 5000
MIN_BATCH_SIZE = 1000
MAX_BATCH_SIZE = 10000


# ===== Kogan 模版 CSV 头（按模板精确确认；如要“第二个 SKU 列”，解除下面注释） =====
HEADERS: List[str] = [
    "SKU",
    "Price",
    "RRP",
    "Kogan First Price",
    "Handling Days",
    "Barcode",
    "Stock",
    "Shipping",
    "Weight",
    "Brand",
    "Title",
    "Description",
    "Subtitle",
    "What's in the Box",
    # "SKU 2",  # 如模板强制需要“第二个 SKU 列”，打开此行
    "Category",
]

# ===== 列名映射：CSV -> KoganTemplate ORM 字段（用于与基线比较） =====
CSV_TO_MODEL_COL = {
    "SKU": "sku",
    "Price": "price",
    "RRP": "rrp",
    "Kogan First Price": "kogan_first_price",
    "Handling Days": "handling_days",
    "Barcode": "barcode",
    "Stock": "stock",
    "Shipping": "shipping",
    "Weight": "weight",
    "Brand": "brand",
    "Title": "title",
    "Description": "description",
    "Subtitle": "subtitle",
    "What's in the Box": "whats_in_the_box",
    # "SKU 2": "sku2",
    "Category": "category",
}





"""
获取kogan template数据方法
    - 只按 kogan_dirty=true 取待导出的 SKU；
    - 分批 + 流式返回（迭代器, 文件名）。
"""
def stream_kogan_diff_csv(
    db: Session,
    *, 
    source: str | None = None,
    country_type: str,
) -> Tuple[Iterator[str], str]:
    
    # 在内部决定 batch_size，并做钳制
    batch_size = DEFAULT_BATCH_SIZE
    if batch_size < MIN_BATCH_SIZE:
        batch_size = MIN_BATCH_SIZE
    if batch_size > MAX_BATCH_SIZE:
        batch_size = MAX_BATCH_SIZE
    
    # 1) 查询运费计算结果表，获取需要导出的sku: 拿到需要导出的 SKU（固定按 kogan_dirty=true）
    skus_iter = iter_changed_skus(
        db=db,
        batch_size=batch_size,
    )

    filename = f'kogan_diff_{country_type}_{datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")}.csv'
    
    # 2) 返回一个迭代器 (边生成边 yield）和一个带时间戳的文件名
    return _csv_iter(db, country_type, skus_iter, batch_size), filename



"""
方法: 具体的流式生成器
    - 边查边比对边生成 CSV 行（字符串分块）。
    - 只输出“发生变更”的行；且该行只有发生变更的列有值，其余列留空。
"""
def _csv_iter(
    db: Session,
    country_type: str,
    skus_iter: Iterable[List[str]],
    batch_size: int,
) -> Iterator[str]:
    
    # 1) 写入 header
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(HEADERS)
    yield buf.getvalue()
    buf.seek(0); buf.truncate(0)


    # todo 修改AU 和 NZ 模版不一样？根据type对应不同模版
    # 2) 对每个 SKU 批次执行
    for skus in skus_iter:
        if not skus:
            continue

        # 3) 获取产品信息
        prod_map = load_products_map(db, skus)      # {sku: {字段:值}}
        # 4) 获取运费结果信息
        fr_map = load_freight_map(db, skus)         # {sku: {字段:值}}
        # 5) 获取历史kogan信息
        base_map = load_kogan_baseline_map(db, country_type, skus)  # {sku: ORM}

        for sku in skus:
            # 6) 把“产品 + 运费”映射成完整的 CSV 行
            csv_full = _map_to_kogan_csv_row(
                country_type=country_type,
                sku=sku,
                product_row=prod_map.get(sku, {}),
                freight_row=fr_map.get(sku, {}),
            )

            # 7) 与基线做列级比较，得到只含变化的列
            sparse = _diff_against_baseline(csv_full, base_map.get(sku))

            if not sparse:
                continue  # 没变化则不输出该行

            # 只填变化的列，其他列写空
            # todo 写法？
            row_values = [sparse.get(h, "") for h in HEADERS]
            writer.writerow(row_values)

        # flush chunk
        yield buf.getvalue()
        buf.seek(0); buf.truncate(0)



"""
方法: 与基线做列级比较，只含变化的列, 始终包含 SKU，保证行能定位到具体商品
    - 返回“仅变更列”的稀疏行：key 为 CSV 列名，value 为新值；
    - 未变化的列不出现在返回里。
    - baseline_row 为 None（首次）时，认为所有填充值都为变化。
"""
def _diff_against_baseline(
    csv_row: Dict[str, object],
    baseline_row: Optional[object],  # ORM
) -> Dict[str, object]:
    
    sparse: Dict[str, object] = {}

    for csv_col in HEADERS:
        # 始终输出 SKU（不参与变更比较）
        if csv_col == "SKU":
            sparse["SKU"] = csv_row.get("SKU")
            continue
        model_col = CSV_TO_MODEL_COL.get(csv_col)
        if not model_col:
            continue

        # 1) 先做规整（字符串裁空、空值统一、浮点保留精度）
        new_val = _normalize(csv_row.get(csv_col))
        old_val = None if baseline_row is None else _normalize(getattr(baseline_row, model_col, None))

        # 比较（空值等价、浮点容差）
        if isinstance(new_val, (int, float)) and isinstance(old_val, (int, float)):
            if abs(float(new_val) - float(old_val or 0)) >= 0.005:
                sparse[csv_col] = new_val
        else:
            if new_val != old_val:
                sparse[csv_col] = new_val

    # 返回“只包含变化列”的 dict
    return sparse



#============= 工具类 ===============
# ====== 业务映射：把产品/运费行 -> CSV 行（这里只是默认策略，可按实际完善） ======
"""
给定一个 SKU，把产品信息 + 运费结果 映射为一整行 Kogan CSV 字段。
    - Shipping：默认用 shipping_ave（如你要按州/偏远定价，可自行改造）
    - Price/RRP：默认用产品 price / rrp（若没有 rrp，可 None）
    - Weight：优先产品 weight；无则用运费计算里的 cubic_weight（或 None）
"""
def _map_to_kogan_csv_row(
    country_type: str,
    sku: str,
    product_row: Dict[str, object],
    freight_row: Dict[str, object],
) -> Dict[str, object]:
   
    # 安全取值
    def g(d: Dict[str, object], key: str):
        return d.get(key) if d else None

    shipping_val = g(freight_row, "shipping_ave")
    weight_val = g(product_row, "weight") or g(freight_row, "cubic_weight")

    # todo 字段修改？
    row = {
        "SKU": sku,
        "Price": g(product_row, "price"),
        "RRP": g(product_row, "rrp"),
        "Kogan First Price": g(product_row, "kogan_first_price"),
        "Handling Days": g(product_row, "handling_days"),
        "Barcode": g(product_row, "barcode"),
        "Stock": g(product_row, "stock"),
        "Shipping": shipping_val,
        "Weight": weight_val,
        "Brand": g(product_row, "brand"),
        "Title": g(product_row, "title"),
        "Description": g(product_row, "description"),
        "Subtitle": g(product_row, "subtitle"),
        "What's in the Box": g(product_row, "whats_in_the_box"),
        # "SKU 2": None,  # 如需要“第二个 SKU 列”，在此填充
        "Category": g(product_row, "category"),
    }
    return row


def _normalize(v: object) -> object:
    """统一比较策略：去除多余空白、控制精度、空值统一。"""
    if v is None:
        return None
    if isinstance(v, str):
        s = v.strip()
        return s if s != "" else None
    if isinstance(v, float):
        # 价格保留 2 位；重量保留 3 位（按需调整）
        return round(v, 3)
    return v