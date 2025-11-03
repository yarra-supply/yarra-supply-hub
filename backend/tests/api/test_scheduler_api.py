


# import datetime as dt
# from types import SimpleNamespace
# from typing import Any, Dict, List

# import pytest
# from fastapi import FastAPI
# from fastapi.testclient import TestClient

# # === 被测路由 ===
# # 注意：按照项目的模块路径来引入。如果文件就在 app/api/v1/scheduler.py，
# # 就保持下面的 import；若路径不同，请按实际路径调整。
# from app.api.v1.scheduler import router as schedules_router, ScheduleUpsert
# from app.api.v1 import scheduler as scheduler_module

# # === 依赖函数（用于覆盖） ===
# from app.services.auth_service import get_current_user
# from app.db.session import get_db


# # ---------- 测试用的“哑Session”和依赖覆盖 ----------
# class DummySession:
#     """一个什么都不做的假 Session，用来占位 get_db。"""
#     def __init__(self) -> None:
#         self.calls: Dict[str, Any] = {}


# def override_get_current_user():
#     # 返回一个假的用户上下文，绕过鉴权
#     return {"sub": "test-user"}


# def override_get_db():
#     # FastAPI 支持依赖是生成器；这里返回一个可用的 dummy 对象
#     db = DummySession()
#     try:
#         yield db
#     finally:
#         pass


# @pytest.fixture()
# def app_client(monkeypatch) -> TestClient:
#     """
#     搭建一个只包含 /schedules 路由的 FastAPI 应用；
#     覆盖鉴权与 DB 依赖；同时让测试可 monkeypatch 仓储函数。
#     """
#     app = FastAPI()
#     app.include_router(schedules_router)

#     # 覆盖依赖：鉴权 + DB
#     app.dependency_overrides[get_current_user] = override_get_current_user
#     app.dependency_overrides[get_db] = override_get_db

#     client = TestClient(app)
#     return client


# # ---------- 工具：构造“行对象”，模拟 ORM 返回 ----------
# def make_row(
#     key: str,
#     enabled: bool,
#     day_of_week: str,
#     hour: int,
#     minute: int,
#     every_2_weeks: bool,
#     timezone: str,
#     updated_at: dt.datetime | None = None,
# ):
#     return SimpleNamespace(
#         key=key,
#         enabled=enabled,
#         day_of_week=day_of_week,
#         hour=hour,
#         minute=minute,
#         every_2_weeks=every_2_weeks,
#         timezone=timezone,
#         updated_at=updated_at,
#     )


# # ---------- 用例 1：GET /schedules 返回（默认 + existing），不写库 ----------
# def test_list_schedules_returns_defaults(monkeypatch, app_client: TestClient):
#     captured_args: Dict[str, Any] = {}

#     def fake_list_all_with_defaults(db, defaults_map):
#         # 记录一下默认值参数是否传入正确
#         captured_args["defaults_keys"] = sorted(list(defaults_map.keys()))
#         # 返回两条“看起来像真实 ORM 行”的对象
#         now = dt.datetime(2025, 1, 1, 12, 0, 0)
#         return [
#             make_row(
#                 key="price_reset",
#                 enabled=False,
#                 day_of_week="WED",
#                 hour=20,
#                 minute=0,
#                 every_2_weeks=True,
#                 timezone="Australia/Sydney",
#                 updated_at=now,
#             ),
#             make_row(
#                 key="product_full_sync",
#                 enabled=False,
#                 day_of_week="THU",
#                 hour=8,
#                 minute=10,
#                 every_2_weeks=True,
#                 timezone="Australia/Sydney",
#                 updated_at=None,
#             ),
#         ]

#     # 注意：monkeypatch 目标是“被测模块里绑定的名字”
#     monkeypatch.setattr(scheduler_module, "list_all_with_defaults", fake_list_all_with_defaults)

#     res = app_client.get("/schedules")
#     assert res.status_code == 200

#     data = res.json()
#     assert isinstance(data, list) and len(data) == 2

#     # 校验 keys
#     got_keys = sorted([row["key"] for row in data])
#     assert got_keys == ["price_reset", "product_full_sync"]

#     # 校验默认 map 确实传入（包含这两个 key）
#     assert captured_args["defaults_keys"] == ["price_reset", "product_full_sync"]

#     # 校验时间序列化：一个是 ISO，另一个是 None
#     by_key = {row["key"]: row for row in data}
#     assert by_key["price_reset"]["updated_at"].startswith("2025-01-01T12:00:00")
#     assert by_key["product_full_sync"]["updated_at"] is None


# # ---------- 用例 2：PUT /schedules/{key} 成功 upsert ----------
# def test_upsert_schedule_success(monkeypatch, app_client: TestClient):
#     def fake_upsert(db, key, dto):
#         # 直接把 dto 回写成“行对象”，模拟更新后的行
#         assert key == "price_reset"
#         # 也可以断言 dto 字段
#         assert dto.enabled is True
#         assert dto.day_of_week == "WED"
#         assert dto.hour == 21
#         assert dto.minute == 30
#         assert dto.every_2_weeks is False
#         assert dto.timezone == "Australia/Melbourne"
#         return make_row(
#             key=key,
#             enabled=dto.enabled,
#             day_of_week=dto.day_of_week,
#             hour=dto.hour,
#             minute=dto.minute,
#             every_2_weeks=dto.every_2_weeks,
#             timezone=dto.timezone,
#             updated_at=dt.datetime(2025, 1, 2, 9, 0, 0),
#         )

#     monkeypatch.setattr(scheduler_module, "upsert", fake_upsert)

#     body = {
#         "enabled": True,
#         "day_of_week": "WED",
#         "hour": 21,
#         "minute": 30,
#         "every_2_weeks": False,
#         "timezone": "Australia/Melbourne",
#     }
#     res = app_client.put("/schedules/price_reset", json=body)
#     assert res.status_code == 200

#     data = res.json()
#     assert data["key"] == "price_reset"
#     assert data["enabled"] is True
#     assert data["day_of_week"] == "WED"
#     assert data["hour"] == 21 and data["minute"] == 30
#     assert data["every_2_weeks"] is False
#     assert data["timezone"] == "Australia/Melbourne"
#     assert data["updated_at"].startswith("2025-01-02T09:00:00")


# # ---------- 用例 3：仓储抛 ValueError -> 422 ----------
# def test_upsert_schedule_repo_value_error_maps_to_422(monkeypatch, app_client: TestClient):
#     def fake_upsert_raise(db, key, dto):
#         raise ValueError("time window conflicts")

#     monkeypatch.setattr(scheduler_module, "upsert", fake_upsert_raise)

#     body = {
#         "enabled": True,
#         "day_of_week": "THU",
#         "hour": 8,
#         "minute": 10,
#         "every_2_weeks": True,
#         "timezone": "Australia/Sydney",
#     }
#     res = app_client.put("/schedules/product_full_sync", json=body)
#     assert res.status_code == 422
#     assert res.json()["detail"] == "time window conflicts"


# # ---------- 用例 4：入参不合法（Pydantic 约束） -> 422 ----------
# @pytest.mark.parametrize(
#     "payload_patch",
#     [
#         {"hour": 24},      # 超上限
#         {"minute": 60},    # 超上限
#         {"day_of_week": "XXX"},  # 非法枚举
#     ],
# )
# def test_upsert_schedule_validation_error_422(monkeypatch, app_client: TestClient, payload_patch):
#     # 成功时的 upsert 不会被调用；但为了安全，先挂一个不会触发的 fake
#     def fake_upsert(db, key, dto):
#         raise AssertionError("should not be called when validation fails")

#     monkeypatch.setattr(scheduler_module, "upsert", fake_upsert)

#     base = {
#         "enabled": True,
#         "day_of_week": "WED",
#         "hour": 21,
#         "minute": 30,
#         "every_2_weeks": True,
#         "timezone": "Australia/Sydney",
#     }
#     base.update(payload_patch)

#     res = app_client.put("/schedules/price_reset", json=base)
#     assert res.status_code == 422
