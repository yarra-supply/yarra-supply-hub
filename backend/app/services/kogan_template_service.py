
from __future__ import annotations
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Set
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
import csv
import io
import os
from pathlib import Path


from sqlalchemy.orm import Session

from app.repository.product_repo import load_products_map
from app.repository.freight_repo import load_freight_map
from app.db.model.kogan_export_job import ExportJobStatus, KoganExportJob
import logging
from app.repository.kogan_template_repo import (
    apply_kogan_template_updates,
    clear_kogan_dirty_flags,
    create_export_job as repo_create_export_job,
    fetch_latest_export_job,
    get_export_job,
    iter_changed_skus,
    load_kogan_baseline_map,
    mark_job_status,
    KoganTemplateModel,
)
from app.core.logging import configure_logging

configure_logging()
logger = logging.getLogger(__name__)

# batch size 默认常量
DEFAULT_BATCH_SIZE = 5000
MIN_BATCH_SIZE = 1000
MAX_BATCH_SIZE = 10000
KOGAN_OVERRIDE_K1_SKUS_FILE_ENV = "KOGAN_OVERRIDE_K1_SKUS_FILE"
KOGAN_OVERRIDE_PRICE_SKUS_FILE_ENV = "KOGAN_OVERRIDE_PRICE_SKUS_FILE"
KOGAN_OVERRIDE_RRP_SKUS_FILE_ENV = "KOGAN_OVERRIDE_RRP_SKUS_FILE"
DEFAULT_KOGAN_OVERRIDE_K1_SKUS_FILE = (
    Path(__file__).resolve().parents[2] / "data" / "kogan_override_k1_skus.txt"
)
DEFAULT_KOGAN_OVERRIDE_PRICE_SKUS_FILE = (
    Path(__file__).resolve().parents[2] / "data" / "kogan_override_price_skus.txt"
)
DEFAULT_KOGAN_OVERRIDE_RRP_SKUS_FILE = (
    Path(__file__).resolve().parents[2] / "data" / "kogan_override_rrp_skus.txt"
)
_OVERRIDE_SKU_CACHES: Dict[str, Set[str]] = {}


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

HEADER_ONLY_COLUMNS = {"Stock", "Barcode"}

def _get_column_specs(country_type: str) -> List[ColumnSpec]:
    try:
        return COUNTRY_COLUMN_SPECS[country_type]
    except KeyError as exc:
        raise ValueError(f"Unsupported country_type: {country_type}") from exc


class NoDirtySkuError(RuntimeError):
    """没有待导出的 SKU。"""

    def __init__(self, message: str, last_job: Optional["KoganExportJob"] = None):
        super().__init__(message)
        self.last_job = last_job


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
    skipped_dirty_skus: List[str]




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
        if build.skipped_dirty_skus:
            clear_kogan_dirty_flags(db, build.skipped_dirty_skus, country_type=country_type)
            db.commit()
        last_job = fetch_latest_export_job(db, country_type)
        raise NoDirtySkuError("没有可导出的 kogan 数据", last_job=last_job)
    
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
    if build.skipped_dirty_skus:
        clear_kogan_dirty_flags(db, build.skipped_dirty_skus, country_type=country_type)
        db.commit()

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
) -> tuple[KoganExportJob, Dict[str, Set[str]]]:
    job = get_export_job(db, job_id)
    if job is None:
        raise ExportJobNotFoundError(f"未找到导出任务: {job_id}")
    if job.status != ExportJobStatus.EXPORTED:
        raise RuntimeError(f"当前状态不允许回写: {job.status}")

    updates = []
    price_override_skus: Set[str] = set()
    k1_override_skus: Set[str] = set()

    for sku_row in job.skus:
        template_values = _decode_template_payload(sku_row.template_payload)
        if not template_values:
            continue
        updates.append({
            "sku": sku_row.sku,
            "values": template_values,
        })
        changed_cols = set(sku_row.changed_columns or [])
        if "price" in changed_cols:
            price_override_skus.add(sku_row.sku)
        if "kogan_first_price" in changed_cols:
            k1_override_skus.add(sku_row.sku)

    apply_kogan_template_updates(
        db,
        country_type=job.country_type,
        updates=updates,
    )
    clear_kogan_dirty_flags(db, [row.sku for row in job.skus], country_type=job.country_type)
    db.flush()
    mark_job_status(
        db,
        job,
        status=ExportJobStatus.APPLIED,
        applied_by=applied_by,
    )
    override_updates = {
        "price": price_override_skus,
        "k1": k1_override_skus,
    }
    return job, override_updates



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
    exported_skus: List[str] = []
    row_count = 0
    exported_set: Set[str] = set()
    dirty_order: List[str] = []
    dirty_seen: Set[str] = set()

    # 获取历史上必须更新 k1/price/rrp 的 sku 列表
    kogan_override_price_skus: Set[str] = _load_kogan_override_price_skus()
    # kogan_override_rrp_skus: Set[str] = _load_kogan_override_rrp_skus()
    kogan_override_k1_skus: Set[str] = _load_kogan_override_k1_skus()


    batch_size = _resolve_batch_size()
    for skus in iter_changed_skus(db=db, country_type=country_type, batch_size=batch_size):
        if not skus:
            continue

        # 1 - query productdata
        product_map = load_products_map(db, skus)

        # 2 - query freight data
        freight_map = load_freight_map(db, skus)

        # 3 - load history kogan template data
        baseline_map = load_kogan_baseline_map(db, country_type, skus)

        for sku in skus:
            if sku not in dirty_seen:
                dirty_seen.add(sku)
                dirty_order.append(sku)
            product_row = product_map.get(sku, {})
            freight_row = freight_map.get(sku, {})

            # sku is NZ but no tags, continue
            if country_type == "NZ" and not _has_product_tag(product_row.get("product_tags"), "Kogan NZ"):
                continue

            # get sku price/rrp/k1 flag
            kogan_override_price_flag = sku in kogan_override_price_skus
            # kogan_override_rrp_flag = sku in kogan_override_rrp_skus
            kogan_override_k1_flag = sku in kogan_override_k1_skus

            # 4 - build full csv row: 计算出完整的price/rrp/k1 正确值
            csv_full = _map_to_kogan_csv_row(
                country_type=country_type,
                sku=sku,
                column_specs=column_specs,
                product_row=product_row,
                freight_row=freight_row,
                baseline_row=baseline_map.get(sku),
            )

            # 5 - diff against baseline
            sparse = _diff_against_baseline(
                csv_row=csv_full,
                baseline_row=baseline_map.get(sku),
                columns=column_specs,
            )

            if not sparse:
                continue

            # 6 - 在导出 CSV 前统一判断是否写入 Price/RRP/K1：NZ 通道保留原逻辑
            price_decimal = _to_decimal(csv_full.get("Price"))
            freight_shopify_price = _to_decimal(freight_row.get("shopify_price"))
            sparse = _apply_override_column_rules(
                sparse,
                country_type=country_type,
                price_decimal=price_decimal,
                freight_shopify_price=freight_shopify_price,
                override_price=kogan_override_price_flag,
                override_k1=kogan_override_k1_flag,
            )
            #对于新增sku的处理, apply流程才add到txt

            if not sparse:
                continue

            # 更严格的“非SKU变更”校验 
            if not _has_non_key_diff(sparse, column_specs):  # [CHANGED] 新增保护：仅 SKU 不导出
                continue

            template_payload, changed_columns = _build_template_payload(
                column_specs,
                csv_full,
                sparse,
            )

            if not changed_columns:
                # 没有实际字段变更，跳过该 SKU
                continue

            # 6 - write csv row
            row = [sparse.get(col.logical_key, "") for col in column_specs]
            writer.writerow(row)
            row_count += 1
            exported_skus.append(sku)
            exported_set.add(sku)

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

    skipped_dirty_skus = [sku for sku in dirty_order if sku not in exported_set]
    print("skipped_dirty_skus:", skipped_dirty_skus)

    return ExportJobBuild(
        file_name=filename,
        file_bytes=csv_bytes,
        row_count=row_count,
        sku_records=sku_records,
        skus=exported_skus,
        skipped_dirty_skus=skipped_dirty_skus,
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


def serialize_export_job(job: Optional[KoganExportJob]) -> Optional[Dict[str, object]]:
    if job is None:
        return None
    return {
        "job_id": job.id,
        "file_name": job.file_name,
        "row_count": job.row_count,
        "country_type": job.country_type,
        "status": job.status,
        "exported_at": job.exported_at.isoformat() if job.exported_at else None,
        "applied_at": job.applied_at.isoformat() if job.applied_at else None,
        "created_by": job.created_by,
        "applied_by": job.applied_by,
    }



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
        if key in HEADER_ONLY_COLUMNS:
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
    *,
    baseline_row: Optional[KoganTemplateModel],
    # force_first_price: bool = False,
) -> Dict[str, object]:
   
    shipping_val = _resolve_shipping(country_type, freight_row)
    weight_val = _resolve_weight(product_row, freight_row)
    # au price / nz price
    kogan_price_val = _resolve_price(country_type, product_row, freight_row)
    rrp_val = _resolve_rrp_price(country_type, kogan_price_val, product_row, freight_row)
    kogan_first_price_val = _resolve_first_price(country_type, kogan_price_val, freight_row)

    # AU且满足条件才不更新，NZ都更新
    # if (
    #     kogan_price_val is not None
    #     and kogan_price_val > Decimal("67")
    #     and country_type == "AU"
    #     and not force_first_price
    # ):
    #     # logger.info("Kogan first price cleared for SKU %s (country=%s): price=%s > 67 and force_first_price=%s",
    #     # sku, country_type, price_val, force_first_price,)
    #     kogan_first_price_val = None


    row = {
        "SKU": sku,
        "Price": kogan_price_val,
        "RRP": rrp_val,
        "Kogan First Price": kogan_first_price_val,
        "Handling Days": 3,
        "Shipping": shipping_val,
        "Weight": weight_val,
        "Brand": _get_value(product_row, "brand"),

        # "Stock": _get_value(product_row, "stock"),         # stock/barcode现在不导出
        # "Barcode": _get_value(product_row, "barcode"),
        # "Title": _get_value(product_row, "title"),
        # "Description": _get_value(product_row, "description"),
        # "Subtitle": _get_value(product_row, "subtitle"),
        # "What's in the Box": _get_value(product_row, "whats_in_the_box"),
        # "SKU_2": sku if country_type == "AU" else None,
        # "Category": _get_value(product_row, "category"),
    }

    # populate columns we do not currently compute with baseline fallback
    if baseline_row is not None:
        # check 表头
        for spec in column_specs:
            if spec.always_include:
                continue
            if spec.model_col:
                current_val = row.get(spec.logical_key)
                if current_val is None:
                    row[spec.logical_key] = getattr(baseline_row, spec.model_col, None)
                else:
                    # If current value is effectively empty (e.g. blank string), also fall back to baseline
                    normalized = _normalize(current_val)
                    if normalized is None:
                        row[spec.logical_key] = getattr(baseline_row, spec.model_col, None)
            else:
                row.setdefault(spec.logical_key, None)
    else:
        for spec in column_specs:
            row.setdefault(spec.logical_key, None)

    return {spec.logical_key: row.get(spec.logical_key) for spec in column_specs}




#============= 工具类 ===============
_NUMERIC_TYPES = (int, float, Decimal)


# ====== 业务映射：把产品/运费行 -> CSV 行（这里只是默认策略，可按实际完善） ======
def _get_value(row: Optional[Dict[str, object]], key: str) -> Optional[object]:
    if not row:
        return None
    return row.get(key)


def _resolve_price(country_type: str, product_row: Optional[Dict[str, object]], 
                   freight_row: Optional[Dict[str, object]]) -> Optional[object]:
    price_key = "kogan_au_price" if country_type == "AU" else "kogan_nz_price"
    return _get_value(freight_row, price_key)


def _resolve_shipping(country_type: str, freight_row: Optional[Dict[str, object]]) -> str:
    if country_type == "NZ":
        return "0"
    else:
        shipping_type = _get_value(freight_row, "shipping_type")
        if isinstance(shipping_type, str) and shipping_type.strip().lower() in {"extra3", "extra4", "extra5"}:
            return "variable"
        return "0"



def _resolve_weight(
    product_row: Optional[Dict[str, object]],
    freight_row: Optional[Dict[str, object]],
) -> Optional[object]:
    shipping_type = _get_value(freight_row, "shipping_type")
    freight_weight_raw = _get_value(freight_row, "weight")
    product_weight_raw = _get_value(product_row, "weight")

    if isinstance(shipping_type, str) and shipping_type.strip().lower() in {"extra3", "extra4", "extra5"}:
        freight_weight = _to_decimal(freight_weight_raw)
        product_weight = _to_decimal(product_weight_raw)

        # IFERROR wrapper：任何计算异常或缺失都返回空字符串
        if freight_weight is None or product_weight is None or product_weight == 0:
            return ""

        # IF 逻辑：freight_weight < 5 或两者相差不到 10% 都返回空字符串
        if freight_weight < Decimal("5"):
            return ""

        try:
            relative_gap = (product_weight - freight_weight) / product_weight
        except (InvalidOperation, ZeroDivisionError):
            return ""

        if abs(relative_gap) < Decimal("0.1"):
            return ""

        try:
            return freight_weight.quantize(Decimal("0.1"), rounding=ROUND_HALF_UP)
        except InvalidOperation:
            return ""
    return None



def _calc_rrp_from_price(price_decimal: Optional[Decimal]) -> Optional[Decimal]:
    if price_decimal is None:
        return None
    try:
        return (price_decimal * Decimal("1.5")).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    except (InvalidOperation, TypeError):
        return None


def _resolve_rrp_price(
    country_type: str,
    kogan_price_val: Optional[object],
    product_row: Optional[Dict[str, object]],
    freight_row: Optional[Dict[str, object]],
) -> Optional[object]:
    
    kogan_price_decimal = _to_decimal(kogan_price_val)
    origin_au_rrp_price = _to_decimal(_get_value(product_row, "rrp_price"))

    if kogan_price_decimal is None:
        return None
    
    # 1-au
    if country_type == "AU":
        if origin_au_rrp_price is None:
            return _calc_rrp_from_price(kogan_price_decimal)
        if origin_au_rrp_price > kogan_price_decimal:
            return _calc_rrp_from_price(kogan_price_decimal)
        return origin_au_rrp_price
        
    # 2-nz 直接使用
    return _calc_rrp_from_price(kogan_price_decimal)



def _resolve_first_price(
    country_type: str,
    kogan_price_val: Optional[object],
    freight_row: Optional[Dict[str, object]],
) -> Optional[object]:
    if country_type == "AU":
        return _get_value(freight_row, "kogan_k1_price")

    kogan_price_decimal = _to_decimal(kogan_price_val)
    if kogan_price_decimal is None:
        return None
    return _calculate_nz_first_price(kogan_price_decimal)


def _calculate_nz_first_price(price_decimal: Decimal) -> Decimal:
    if price_decimal > Decimal("66.7"):
        return (price_decimal * Decimal("0.969")).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return price_decimal - Decimal("2.01")






# ====================== 辅助函数 =======================
"""
    根据国家与 override 标记过滤需要写入 CSV 的列。
    - NZ：不做额外限制，5 个核心字段按照 diff 结果更新；
    - AU：Price 对新增 SKU（override=false）需要与 Shopify price 比较方可导出；若导出则强制 RRP=Price*1.5；K1 使用原有限制。
"""
def _apply_override_column_rules(
    sparse: Dict[str, object],
    *,
    country_type: str,
    price_decimal: Optional[Decimal],
    freight_shopify_price: Optional[Decimal],
    override_price: bool,
    override_k1: bool,
) -> Dict[str, object]:
    
    if not sparse or country_type != "AU":
        return sparse
    
    # 把 sparse 字典浅拷贝一份，存成 filtered
    filtered = dict(sparse)

    # 1-Price：新增 SKU 根据 Shopify price 判断是否导出
    price_kept = "Price" in filtered
    if price_kept and not override_price:
        if (
            price_decimal is None
            or freight_shopify_price is None
            or price_decimal <= freight_shopify_price
        ):
            filtered.pop("Price")
            price_kept = False


    # 2-RRP：仅当 Price 导出时才写入，并强制等于 Price*1.5
    if price_kept:
        rrp_override = _calc_rrp_from_price(price_decimal)
        if rrp_override is not None:
            filtered["RRP"] = rrp_override
        else:
            filtered.pop("RRP", None)
    else:
        filtered.pop("RRP", None)


    # 3-Kogan First Price: 如果有变化 & kogan_override_k1_flag =true，不管price <= 67 还是 >67, k1都更新
    # 如果有变化 & kogan_override_k1_flag =false, price <= 67，k1才写到csv，price >67, 不写到csv
    if "Kogan First Price" in filtered:
        if not override_k1:
            if price_decimal is None or price_decimal > Decimal("67"):
                filtered.pop("Kogan First Price")

    return filtered



# 用于更严谨地判定“是否有非SKU变更
def _has_non_key_diff(sparse: Dict[str, object], columns: Sequence[ColumnSpec]) -> bool:
    """若 sparse 里包含任意一个非 always_include 列，视为存在真正变更（可导出）。"""
    for col in columns:
        if not col.always_include and col.logical_key in sparse:
            return True
    return False


def _has_product_tag(tags: Optional[object], target: str) -> bool:
    """判断 product_tags 是否包含指定标签（大小写不敏感）。"""
    if not tags or not target:
        return False
    target_norm = target.strip().lower()
    if not target_norm:
        return False

    values: List[str]
    if isinstance(tags, str):
        values = [tags]
    elif isinstance(tags, (list, tuple, set)):
        values = [str(t) for t in tags if t is not None]
    else:
        return False

    for val in values:
        if isinstance(val, str) and val.strip().lower() == target_norm:
            return True
    return False




def _resolve_override_file_path(env_var: str, default_file: Path) -> Path:
    configured_path = os.getenv(env_var)
    if configured_path:
        return Path(configured_path).expanduser()
    return default_file


# 从txt文件load sku
def _load_override_skus(
    cache_key: str,
    *,
    env_var: str,
    default_file: Path,
    label: str,
) -> Set[str]:
    cached = _OVERRIDE_SKU_CACHES.get(cache_key)
    if cached is not None:
        return cached

    path = _resolve_override_file_path(env_var, default_file)
    skus: Set[str] = set()
    if not path.exists():
        logger.debug("%s file not found at %s; skipping override.", label, path)
        _OVERRIDE_SKU_CACHES[cache_key] = skus
        return skus

    try:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                sku = line.strip()
                if sku:
                    skus.add(sku)
    except OSError as exc:
        logger.warning("Failed to read %s file %s: %s", label, path, exc)

    _OVERRIDE_SKU_CACHES[cache_key] = skus
    return skus


def _load_kogan_override_k1_skus() -> Set[str]:
    return _load_override_skus(
        "kogan_k1",
        env_var=KOGAN_OVERRIDE_K1_SKUS_FILE_ENV,
        default_file=DEFAULT_KOGAN_OVERRIDE_K1_SKUS_FILE,
        label="Kogan-Override K1 SKU",
    )


def _load_kogan_override_price_skus() -> Set[str]:
    return _load_override_skus(
        "kogan_price",
        env_var=KOGAN_OVERRIDE_PRICE_SKUS_FILE_ENV,
        default_file=DEFAULT_KOGAN_OVERRIDE_PRICE_SKUS_FILE,
        label="Kogan-Override Price SKU",
    )


def _load_kogan_override_rrp_skus() -> Set[str]:
    return _load_override_skus(
        "kogan_rrp",
        env_var=KOGAN_OVERRIDE_RRP_SKUS_FILE_ENV,
        default_file=DEFAULT_KOGAN_OVERRIDE_RRP_SKUS_FILE,
        label="Kogan-Override RRP SKU",
    )


# update kogan_override_price_skus.txt / kogan_override_k1_skus.txt
def update_override_files(price_skus: Set[str], k1_skus: Set[str]) -> None:
    _update_override_files(price_skus=price_skus, k1_skus=k1_skus)


def _update_override_files(*, price_skus: Set[str], k1_skus: Set[str]) -> None:
    _append_override_skus(
        cache_key="kogan_price",
        env_var=KOGAN_OVERRIDE_PRICE_SKUS_FILE_ENV,
        default_file=DEFAULT_KOGAN_OVERRIDE_PRICE_SKUS_FILE,
        label="Kogan-Override Price SKU",
        new_skus=price_skus,
    )
    _append_override_skus(
        cache_key="kogan_k1",
        env_var=KOGAN_OVERRIDE_K1_SKUS_FILE_ENV,
        default_file=DEFAULT_KOGAN_OVERRIDE_K1_SKUS_FILE,
        label="Kogan-Override K1 SKU",
        new_skus=k1_skus,
    )


def _append_override_skus(
    *,
    cache_key: str,
    env_var: str,
    default_file: Path,
    label: str,
    new_skus: Set[str],
) -> None:
    if not new_skus:
        return
    
    existing = _OVERRIDE_SKU_CACHES.get(cache_key)
    if existing is None:
        existing = _load_override_skus(
            cache_key,
            env_var=env_var,
            default_file=default_file,
            label=label,
        )

    # copy to avoid modifying while iterating? but set fine
    write_skus = [sku for sku in sorted(new_skus) if sku not in existing]
    if not write_skus:
        return
    
    path = _resolve_override_file_path(env_var, default_file)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            for sku in write_skus:
                handle.write(f"{sku}\n")
    except OSError as exc:
        logger.warning("Failed to append %s entries to %s: %s", label, path, exc)
        return
    if existing is None:
        existing = set()
    existing.update(write_skus)
    _OVERRIDE_SKU_CACHES[cache_key] = existing



def _to_decimal(value: object) -> Optional[Decimal]:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, float)):
        try:
            return Decimal(str(value))
        except (InvalidOperation, ValueError):
            return None
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            return Decimal(stripped)
        except (InvalidOperation, ValueError):
            return None
    return None



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
