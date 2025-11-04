# """Integration tests for product repository bulk operations."""

# from __future__ import annotations

# from typing import List, Dict

# import pytest
# from sqlalchemy.orm import Session

# from app.db.session import SessionLocal
# from app.db.model.product import SkuInfo
# from app.repository.product_repo import bulk_upsert_sku_info


# @pytest.fixture
# def db_session() -> Session:
#     """Yield a real database session backed by the configured PostgreSQL."""
#     session = SessionLocal()
#     try:
#         yield session
#     finally:
#         session.close()


# def _make_rows(count: int) -> List[Dict[str, object]]:
#     rows: List[Dict[str, object]] = []
#     for idx in range(count):
#         suffix = f"{idx:03d}"
#         rows.append(
#             {
#                 "sku_code": f"TEST-SKU-{suffix}",
#                 "brand": "TestBrand",
#                 "stock_qty": idx,
#                 "price": 10 + idx,
#                 "rrp_price": 12 + idx,
#                 "special_price": None,
#                 "special_price_end_date": None,
#                 "shopify_price": None,
#                 "shopify_variant_id": f"gid://shopify/ProductVariant/{idx}",
#                 "weight": 1.0,
#                 "length": 2.0,
#                 "width": 3.0,
#                 "height": 4.0,
#                 "cbm": 0.5,
#                 "product_tags": ["tag"],
#                 "freight_act": None,
#                 "freight_nsw_m": None,
#                 "freight_nsw_r": None,
#                 "freight_nt_m": None,
#                 "freight_nt_r": None,
#                 "freight_qld_m": None,
#                 "freight_qld_r": None,
#                 "remote": None,
#                 "freight_sa_m": None,
#                 "freight_sa_r": None,
#                 "freight_tas_m": None,
#                 "freight_tas_r": None,
#                 "freight_vic_m": None,
#                 "freight_vic_r": None,
#                 "freight_wa_m": None,
#                 "freight_wa_r": None,
#                 "freight_nz": None,
#                 "attrs_hash_current": f"hash-{suffix}",
#             }
#         )
#     return rows


# @pytest.mark.integration
# def test_bulk_upsert_sku_info_inserts_into_real_table(db_session: Session) -> None:
#     rows = _make_rows(10)
#     sku_codes = [row["sku_code"] for row in rows]

#     try:
#         bulk_upsert_sku_info(db_session, rows)
#         db_session.commit()

#         stored = (
#             db_session.query(SkuInfo)
#             .filter(SkuInfo.sku_code.in_(sku_codes))
#             .order_by(SkuInfo.sku_code)
#             .all()
#         )
#         assert len(stored) == len(rows)
#         assert stored[0].sku_code == "TEST-SKU-000"
#         assert stored[-1].sku_code == "TEST-SKU-009"
#     finally:
#         db_session.query(SkuInfo).filter(SkuInfo.sku_code.in_(sku_codes)).delete(
#             synchronize_session=False
#         )
#         db_session.commit()
