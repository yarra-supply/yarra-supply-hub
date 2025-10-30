
from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence
from datetime import datetime, timezone
from decimal import Decimal
import csv
import io


from sqlalchemy.orm import Session

from app.repository.product_repo import load_products_map
from app.repository.freight_repo import load_freight_map
from app.db.model.kogan_export_job import ExportJobStatus, KoganExportJob
from app.repository.kogan_template_repo import (
    apply_kogan_template_updates,
    clear_kogan_dirty_flags,
    create_export_job as repo_create_export_job,
    get_export_job,
    iter_changed_skus,
    load_kogan_baseline_map,
    mark_job_status,
)


# batch size 默认常量
DEFAULT_BATCH_SIZE = 5000
MIN_BATCH_SIZE = 1000
MAX_BATCH_SIZE = 10000

def _resolve_batch_size() -> int:
    size = DEFAULT_BATCH_SIZE
    if size < MIN_BATCH_SIZE:
        size = MIN_BATCH_SIZE
    if size > MAX_BATCH_SIZE:
        size = MAX_BATCH_SIZE
    return size


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


class NoDirtySkuError(RuntimeError):
    """没有待导出的 SKU。"""


class ExportJobNotFoundError(RuntimeError):
    """指定的导出任务不存在。"""


@dataclass(frozen=True)
class ExportJobSkuRecord:
    sku: str
    template_payload: Dict[str, object]
    changed_columns: List[str]


@dataclass(frozen=True)
class ExportJobBuild:
    file_name: str
    file_bytes: bytes
    row_count: int
    sku_records: List[ExportJobSkuRecord]
    skus: List[str]





"""创建导出任务：先生成完整 CSV，再写入数据库，最后返回 job + 文件字节"""
def create_kogan_export_job(
    db: Session,
    *,
    country_type: str,
    created_by: Optional[int],
) -> KoganExportJob:
    
    column_specs = _get_column_specs(country_type)

    # 1 - 构建导出数据集
    build = _build_export_dataset(db, country_type, column_specs)
    if build.row_count == 0:
        raise NoDirtySkuError("没有可导出的 kogan 数据")
    
    # 2 - 写入导出任务记录
    job = repo_create_export_job(
        db,
        country_type=country_type,
        file_name=build.file_name,
        file_bytes=build.file_bytes,
        row_count=build.row_count,
        created_by=created_by,
        sku_records=[
            {
                "sku": record.sku,
                "template_payload": record.template_payload,
                "changed_columns": record.changed_columns,
            }
            for record in build.sku_records
        ],
    )

    return job




# 获取导出任务及其文件内容；找不到则抛错
def get_export_job_file(db: Session, job_id: str) -> KoganExportJob:
    job = get_export_job(db, job_id)
    
    if job is None:
        raise ExportJobNotFoundError(f"未找到导出任务: {job_id}")
    return job



def apply_export_job(
    db: Session,
    *,
    job_id: str,
    applied_by: Optional[int],
) -> KoganExportJob:
    job = get_export_job(db, job_id)
    if job is None:
        raise ExportJobNotFoundError(f"未找到导出任务: {job_id}")
    if job.status != ExportJobStatus.EXPORTED:
        raise RuntimeError(f"当前状态不允许回写: {job.status}")

    updates = []
    for sku_row in job.skus:
        template_values = _decode_template_payload(sku_row.template_payload)
        if not template_values:
            continue
        updates.append({
            "sku": sku_row.sku,
            "values": template_values,
        })

    apply_kogan_template_updates(
        db,
        country_type=job.country_type,
        updates=updates,
    )
    clear_kogan_dirty_flags(db, [row.sku for row in job.skus])
    db.flush()
    mark_job_status(
        db,
        job,
        status=ExportJobStatus.APPLIED,
        applied_by=applied_by,
    )
    return job



def _build_export_dataset(
    db: Session,
    country_type: str,
    column_specs: Sequence[ColumnSpec],
) -> ExportJobBuild:
    
    headers = [col.header for col in column_specs]
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(headers)

    sku_records: List[ExportJobSkuRecord] = []
    all_skus: List[str] = []
    row_count = 0

    batch_size = _resolve_batch_size()
    for skus in iter_changed_skus(db=db, batch_size=batch_size):
        if not skus:
            continue

        # 1 - query productdata
        product_map = load_products_map(db, skus)

        # 2 - query freight data
        freight_map = load_freight_map(db, skus)

        # 3 - load history kogan template data
        baseline_map = load_kogan_baseline_map(db, country_type, skus)

        for sku in skus:

            # 4 - build full csv row
            csv_full = _map_to_kogan_csv_row(
                country_type=country_type,
                sku=sku,
                column_specs=column_specs,
                product_row=product_map.get(sku, {}),
                freight_row=freight_map.get(sku, {}),
            )

            # 5 - diff against baseline
            sparse = _diff_against_baseline(
                csv_row=csv_full,
                baseline_row=baseline_map.get(sku),
                columns=column_specs,
            )

            if not sparse:
                continue

            # 6 - write csv row
            row = [sparse.get(col.logical_key, "") for col in column_specs]
            writer.writerow(row)
            row_count += 1
            all_skus.append(sku)

            template_payload, changed_columns = _build_template_payload(
                column_specs,
                csv_full,
                sparse,
            )

            # 7 - record sku change
            sku_records.append(
                ExportJobSkuRecord(
                    sku=sku,
                    template_payload=template_payload,
                    changed_columns=changed_columns,
                )
            )

    csv_bytes = buf.getvalue().encode("utf-8")
    filename = f'kogan_diff_{country_type}_{datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")}.csv'

    return ExportJobBuild(
        file_name=filename,
        file_bytes=csv_bytes,
        row_count=row_count,
        sku_records=sku_records,
        skus=all_skus,
    )



# 构建“仅变化列”的模板负载 + 变化列列表
def _build_template_payload(
    column_specs: Sequence[ColumnSpec],
    csv_full: Dict[str, object],
    sparse: Dict[str, object],
) -> tuple[Dict[str, object], List[str]]:
    
    payload: Dict[str, object] = {}
    changed: List[str] = []
    for col in column_specs:
        if col.always_include or not col.model_col:
            continue
        if col.logical_key not in sparse:
            continue
        value = csv_full.get(col.logical_key)
        payload[col.model_col] = _jsonify_value(value)
        changed.append(col.model_col)
    return payload, changed



DECIMAL_MODEL_COLUMNS = {"price", "rrp", "kogan_first_price", "weight"}
INT_MODEL_COLUMNS = {"stock", "handling_days"}


def _decode_template_payload(payload: Dict[str, object]) -> Dict[str, object]:
    decoded: Dict[str, object] = {}
    for key, raw in payload.items():
        if raw is None:
            decoded[key] = None
            continue
        if key in DECIMAL_MODEL_COLUMNS:
            decoded[key] = Decimal(str(raw))
        elif key in INT_MODEL_COLUMNS:
            decoded[key] = int(raw)
        else:
            decoded[key] = str(raw)
    return decoded


def _jsonify_value(value: object) -> object:
    if isinstance(value, Decimal):
        return str(value)
    return value



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
    - Shipping: 运费类型为 Extra3/4/5 → 填 "variable"；其余类型填 "0"；
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
    shipping_type = g(freight_row, "shipping_type")
    shipping_type_normalized = shipping_type.lower() if isinstance(shipping_type, str) else ""
    if shipping_type_normalized in {"extra3", "extra4", "extra5"}:
        shipping_val = "variable"
    else:
        shipping_val = "0"


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
