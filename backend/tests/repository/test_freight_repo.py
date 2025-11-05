# # /backend/tests/db/test_freight_repo.py
# from __future__ import annotations

# from typing import List, Dict
# import pytest
# from sqlalchemy.orm import Session

# from app.db.session import SessionLocal
# from app.db.model.freight import SkuFreightFee
# from app.repository.freight_repo import update_changed_prices


# @pytest.fixture
# def db_session() -> Session:
#     """Yield a real database session backed by the configured PostgreSQL."""
#     session = SessionLocal()
#     try:
#         yield session
#     finally:
#         session.close()


# def _make_changed_rows(count: int) -> List[Dict[str, object]]:
#     """
#     构造传给 update_changed_prices 的 changed 数据，每条包含 sku_code 和要更新的列。
#     """
#     rows: List[Dict[str, object]] = []
#     for idx in range(count):
#         suffix = f"{idx:03d}"
#         rows.append(
#             {
#                 "sku_code": f"TEST-FREIGHT-{suffix}",
#                 "fields": {
#                     "shipping_type": "FreeShipping" if idx % 2 == 0 else "Kogan",
#                     "weight": 1.0 + idx * 0.1,
#                     "kogan_au_price": 11.0 + idx,
#                     "kogan_nz_price": 100.0 + idx,
#                 },
#             }
#         )
#     return rows


# @pytest.mark.integration
# def test_update_changed_prices_batch_write(db_session: Session) -> None:
#     """测试 update_changed_prices 可批量写入 10 条记录到真实表。"""
#     changed = _make_changed_rows(10)
#     sku_codes = [row["sku_code"] for row in changed]

#     try:
#         # 先确保表中没有旧数据
#         # db_session.query(SkuFreightFee).filter(
#         #     SkuFreightFee.sku_code.in_(sku_codes)
#         # ).delete(synchronize_session=False)
#         # db_session.commit()

#         # 调用要测试的函数
#         update_changed_prices(
#             db_session,
#             [(row["sku_code"], row["fields"]) for row in changed],
#             source="test_case",
#             run_id="test_run_001",
#         )
#         db_session.commit()

#         # 校验数据库中确实写入了 10 条
#         stored = (
#             db_session.query(SkuFreightFee)
#             .filter(SkuFreightFee.sku_code.in_(sku_codes))
#             .order_by(SkuFreightFee.sku_code)
#             .all()
#         )
#         assert len(stored) == len(changed)
#         assert stored[0].sku_code == "TEST-FREIGHT-000"
#         assert stored[-1].sku_code == "TEST-FREIGHT-009"

#         # 抽样验证更新字段
#         first = stored[0]
#         assert first.shipping_type in ("FreeShipping", "Kogan")
#         assert first.weight is not None
#         assert first.kogan_au_price is not None
#         assert first.kogan_nz_price is not None

#     finally:
#         # 清理测试数据
#         print("Cleaning up test data...")
#         # db_session.query(SkuFreightFee).filter(
#         #     SkuFreightFee.sku_code.in_(sku_codes)
#         # ).delete(synchronize_session=False)
#         # db_session.commit()
