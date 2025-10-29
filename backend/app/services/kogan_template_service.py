
from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Tuple
from datetime import datetime, timezone
from decimal import Decimal
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


@dataclass(frozen=True)
class ColumnSpec:
    header: str
    logical_key: str
    model_col: Optional[str]
    always_include: bool = False


COUNTRY_COLUMN_SPECS: Dict[str, List[ColumnSpec]] = {
    "AU": [
        ColumnSpec(header="SKU", logical_key="SKU", model_col="sku", always_include=True),
        ColumnSpec(header="Price", logical_key="Price", model_col="price"),
        ColumnSpec(header="RRP", logical_key="RRP", model_col="rrp"),
        ColumnSpec(header="Kogan First Price", logical_key="Kogan First Price", model_col="kogan_first_price"),
        ColumnSpec(header="Handling Days", logical_key="Handling Days", model_col="handling_days"),
        ColumnSpec(header="Barcode", logical_key="Barcode", model_col="barcode"),
        ColumnSpec(header="Stock", logical_key="Stock", model_col="stock"),
        ColumnSpec(header="Shipping", logical_key="Shipping", model_col="shipping"),
        ColumnSpec(header="Weight", logical_key="Weight", model_col="weight"),
        ColumnSpec(header="Brand", logical_key="Brand", model_col="brand"),
        ColumnSpec(header="Title", logical_key="Title", model_col="title"),
        ColumnSpec(header="Description", logical_key="Description", model_col="description"),
        ColumnSpec(header="Subtitle", logical_key="Subtitle", model_col="subtitle"),
        ColumnSpec(header="What's in the Box", logical_key="What's in the Box", model_col="whats_in_the_box"),
        ColumnSpec(header="SKU", logical_key="SKU_2", model_col="sku2"),
        ColumnSpec(header="Category", logical_key="Category", model_col="category"),
    ],
    "NZ": [
        ColumnSpec(header="SKU", logical_key="SKU", model_col="sku", always_include=True),
        ColumnSpec(header="Price", logical_key="Price", model_col="price"),
        ColumnSpec(header="RRP", logical_key="RRP", model_col="rrp"),
        ColumnSpec(header="Kogan First Price", logical_key="Kogan First Price", model_col="kogan_first_price"),
        ColumnSpec(header="Shipping", logical_key="Shipping", model_col="shipping"),
        ColumnSpec(header="Handling Days", logical_key="Handling Days", model_col="handling_days"),
    ],
}


def _get_column_specs(country_type: str) -> List[ColumnSpec]:
    try:
        return COUNTRY_COLUMN_SPECS[country_type]
    except KeyError as exc:
        raise ValueError(f"Unsupported country_type: {country_type}") from exc





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

    # 2) 准备列规格 & 文件名
    column_specs = _get_column_specs(country_type)
    filename = f'kogan_diff_{country_type}_{datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")}.csv'
    
    # 3) 返回一个迭代器 (边生成边 yield）和一个带时间戳的文件名
    csv_iter = _csv_iter(
        db=db,
        country_type=country_type,
        column_specs=column_specs,
        skus_iter=skus_iter,
    )
    return csv_iter, filename



"""
方法: 具体的流式生成器
    - 边查边比对边生成 CSV 行（字符串分块）。
    - 只输出“发生变更”的行；且该行只有发生变更的列有值，其余列留空。
"""
def _csv_iter(
    db: Session,
    country_type: str,
    column_specs: Sequence[ColumnSpec],
    skus_iter: Iterable[List[str]],
) -> Iterator[str]:
    
    headers = [col.header for col in column_specs]

    # 1) 写入 header
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(headers)
    yield buf.getvalue()
    buf.seek(0); buf.truncate(0)


    # 2) 对每个 SKU 批次执行, AU 和 NZ 模版不一样, 根据type对应不同模版
    for skus in skus_iter:
        if not skus:
            continue

        # 3) 获取产品信息: sku, rrp, ean_code, stock_qty, brand, sku2? 
        prod_map = load_products_map(db, skus) 

        # 4) 获取运费结果信息: sku, kogan_au_price, kogan first price, shipping_type, weight(update后的)
        fr_map = load_freight_map(db, skus)   

        # 5) 获取历史kogan信息
        base_map = load_kogan_baseline_map(db, country_type, skus) 

        for sku in skus:
            # 6) 把“产品 + 运费”映射成完整的 CSV 行:
            #  sku, rrp, ean_code, stock_qty, brand, sku2? kogan_au_price, kogan first price, shipping_type, weight(update后的)
            csv_full = _map_to_kogan_csv_row(
                country_type=country_type,
                sku=sku,
                column_specs=column_specs,
                product_row=prod_map.get(sku, {}),
                freight_row=fr_map.get(sku, {}),
            )

            # 7) 与基线做列级比较，得到只含变化的列
            sparse = _diff_against_baseline(
                csv_row=csv_full,
                baseline_row=base_map.get(sku),
                columns=column_specs,
            )

            if not sparse:
                continue  # 没变化则不输出该行

            # 8) todo 写法？只填变化的列，其他列写空
            row_values = [sparse.get(col.logical_key, "") for col in column_specs]
            writer.writerow(row_values)

            # 9) todo 本次 kogan_template 变化的字段需要更新到DB 


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
    *,
    columns: Sequence[ColumnSpec],
) -> Dict[str, object]:
    
    sparse: Dict[str, object] = {}
    has_diff = False

    for col in columns:
        key = col.logical_key
        model_col = col.model_col

        if col.always_include:
            sparse[key] = csv_row.get(key)
            continue

        if not model_col:
            continue

        new_val = _normalize(csv_row.get(key))
        old_val = None if baseline_row is None else _normalize(getattr(baseline_row, model_col, None))

        if _values_different(new_val, old_val):
            sparse[key] = csv_row.get(key)
            has_diff = True

    if not has_diff:
        return {}

    return sparse



#============= 工具类 ===============
_NUMERIC_TYPES = (int, float, Decimal)


# ====== 业务映射：把产品/运费行 -> CSV 行（这里只是默认策略，可按实际完善） ======
"""
给定一个 SKU, 把产品信息 + 运费结果 映射为一整行 Kogan CSV 字段。
    - Price: 优先使用运费结果表里的 Kogan 价格（AU/NZ），否则退回到 sku_info.price；
    - Shipping: AU 中 shipping_ave 为 0 时写 0，否则写 "variable"；NZ 固定写 0；
    - Weight: 优先运费结果里的 weight，其次 sku_info.weight，最后 cubic_weight；
    - return : sku, kogan_au_price, rrp, kogan first price, handing days, ean_code, stock_qty, shipping_type, weight(update后的), brand, sku2? 
"""
def _map_to_kogan_csv_row(
    country_type: str,
    sku: str,
    column_specs: Sequence[ColumnSpec],
    product_row: Dict[str, object],
    freight_row: Dict[str, object],
) -> Dict[str, object]:
   
    # 安全取值
    def g(d: Dict[str, object], key: str):
        return d.get(key) if d else None
    
    #1 price
    price_key = "kogan_au_price" if country_type == "AU" else "kogan_nz_price"
    price_val = g(freight_row, price_key) or g(product_row, "price")

    #2 shipping 
    shipping_raw = g(freight_row, "shipping_ave")
    if country_type == "AU":
        if shipping_raw is None:
            shipping_val = None
        elif _is_zero(shipping_raw):
            shipping_val = _zero_like(shipping_raw)
        else:
            shipping_val = "variable"
    else:  # NZ
        shipping_val = Decimal("0")

    # shipping_raw = g(freight_row, "shipping_ave")
    # if country_type == "AU":
    #     if shipping_raw is None:
    #         shipping_val = None
    #     elif _is_zero(shipping_raw):
    #         shipping_val = _zero_like(shipping_raw)
    #     else:
    #         shipping_val = shipping_raw
    # else:  # NZ
    #     shipping_val = Decimal("0")


    row = {
        "SKU": sku,
        "Price": price_val,
        "RRP": g(product_row, "rrp"),
        "Kogan First Price": g(freight_row, "kogan_k1_price"),
        "Handling Days": 3,
        "Barcode": g(product_row, "barcode"),
        "Stock": g(product_row, "stock"),
        "Shipping": shipping_val,
        "Weight": g(freight_row, "weight"),
        "Brand": g(product_row, "brand"),
        # "Title": g(product_row, "title"),
        # "Description": g(product_row, "description"),
        # "Subtitle": g(product_row, "subtitle"),
        # "What's in the Box": g(product_row, "whats_in_the_box"),
        # "SKU_2": sku if country_type == "AU" else None,
        # "Category": g(product_row, "category"),
    }
    return {spec.logical_key: row.get(spec.logical_key) for spec in column_specs}




def _is_zero(value: object) -> bool:
    if isinstance(value, Decimal):
        return value == Decimal("0")
    if isinstance(value, (int, float)):
        return abs(float(value)) < 0.0005
    return False


def _zero_like(value: object) -> object:
    if isinstance(value, Decimal):
        return Decimal("0")
    if isinstance(value, float):
        return 0.0
    if isinstance(value, int):
        return 0
    return 0


def _values_different(new_val: object, old_val: object) -> bool:
    if new_val is None and old_val is None:
        return False
    if new_val is None or old_val is None:
        return True
    if isinstance(new_val, _NUMERIC_TYPES) and isinstance(old_val, _NUMERIC_TYPES):
        return abs(float(new_val) - float(old_val)) >= 0.005
    return new_val != old_val


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
