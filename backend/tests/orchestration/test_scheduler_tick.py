# import datetime as dt

# import pytest, pytz
# from sqlalchemy import create_engine
# from sqlalchemy.orm import sessionmaker

# from app.db.base import Base
# from app.db.model.schedule import Schedule
# from app.orchestration import scheduler_tick

# from app.db.session import SessionLocal


# # record： test done ✅ 


# # @pytest.fixture
# # def testing_session(monkeypatch):
# #     """Attach an in-memory SQLite session factory to scheduler_tick.SessionLocal."""
# #     engine = create_engine("sqlite:///:memory:", future=True)
# #     TestingSessionLocal = sessionmaker(
# #         bind=engine,
# #         autoflush=False,
# #         autocommit=False,
# #         expire_on_commit=False,
# #     )
# #     Base.metadata.create_all(engine)
# #     monkeypatch.setattr(scheduler_tick, "SessionLocal", TestingSessionLocal)
# #     yield TestingSessionLocal
# #     Base.metadata.drop_all(engine)
# #     engine.dispose()


# # @pytest.fixture
# # def fixed_datetime(monkeypatch):
# #     """
# #     Provide a helper to freeze scheduler_tick.dt.datetime.now to a deterministic moment.
# #     """

# #     def _freeze(target: dt.datetime):
# #         if target.tzinfo is not None:
# #             target_naive = target.astimezone(dt.timezone.utc).replace(tzinfo=None)
# #         else:
# #             target_naive = target

# #         class _FixedDateTime(dt.datetime):
# #             @classmethod
# #             def now(cls, tz=None):  # type: ignore[override]
# #                 if tz is None:
# #                     return target_naive
# #                 if hasattr(tz, "localize"):
# #                     # pytz style timezone
# #                     return tz.localize(target_naive)
# #                 return target_naive.replace(tzinfo=tz)

# #         monkeypatch.setattr(scheduler_tick.dt, "datetime", _FixedDateTime)

# #     return _freeze


# # def _insert_schedule(
# #     SessionFactory,
# #     *,
# #     key: str,
# #     enabled: bool,
# #     day_of_week: str,
# #     hour: int,
# #     minute: int,
# #     every_2_weeks: bool = False,
# #     timezone: str = "UTC",
# #     last_run_at: dt.datetime | None = None,
# # ):
# #     now = dt.datetime.now(dt.timezone.utc)
# #     with SessionFactory() as session:
# #         sched = Schedule(
# #             key=key,
# #             enabled=enabled,
# #             day_of_week=day_of_week,
# #             hour=hour,
# #             minute=minute,
# #             every_2_weeks=every_2_weeks,
# #             timezone=timezone,
# #             last_run_at=last_run_at,
# #             created_at=now,
# #             updated_at=now,
# #         )
# #         session.add(sched)
# #         session.commit()


# # def test_tick_product_full_sync_no_schedule(monkeypatch, testing_session, fixed_datetime):
# #     monkeypatch.setattr(scheduler_tick.settings, "CELERY_TIMEZONE", "UTC", raising=False)
# #     fixed_datetime(dt.datetime(2025, 1, 1, 20, 5))

# #     result = scheduler_tick.tick_product_full_sync.run()
# #     assert result == {"status": "no-schedule-item"}


# # def test_tick_product_full_sync_disabled(monkeypatch, testing_session, fixed_datetime):
# #     monkeypatch.setattr(scheduler_tick.settings, "CELERY_TIMEZONE", "UTC", raising=False)
# #     fixed_datetime(dt.datetime(2025, 1, 1, 20, 5))
# #     _insert_schedule(
# #         testing_session,
# #         key="product_full_sync",
# #         enabled=False,
# #         day_of_week="WED",
# #         hour=20,
# #         minute=0,
# #         every_2_weeks=False,
# #     )

# #     result = scheduler_tick.tick_product_full_sync.run()
# #     assert result == {"status": "disabled"}


# # def test_tick_product_full_sync_triggers_and_updates_last_run(monkeypatch, testing_session, fixed_datetime):
# #     monkeypatch.setattr(scheduler_tick.settings, "CELERY_TIMEZONE", "UTC", raising=False)
# #     fixed_datetime(dt.datetime(2025, 1, 1, 20, 5))
# #     _insert_schedule(
# #         testing_session,
# #         key="product_full_sync",
# #         enabled=True,
# #         day_of_week="WED",
# #         hour=20,
# #         minute=0,
# #         every_2_weeks=False,
# #     )

# #     result = scheduler_tick.tick_product_full_sync.run()
# #     assert result == {"status": "fired"}

# #     with testing_session() as session:
# #         row = session.get(Schedule, "product_full_sync")
# #         assert row is not None
# #         assert row.last_run_at is not None
# #         # Should be patched time with UTC tzinfo
# #         assert row.last_run_at.replace(tzinfo=None) == dt.datetime(2025, 1, 1, 20, 5)
# #         assert row.last_run_at.tzinfo is not None




# def test_tick_product_full_sync_real_db():

#     # # 测试product full 直接调用任务
#     # result = scheduler_tick.tick_product_full_sync()
#     # assert isinstance(result, dict)  # 防止测试红

#     # # 再查 DB 看 last_run_at 是否更新
#     # with SessionLocal() as session:
#     #     updated = session.get(Schedule, 'product_full_sync')
#     #     assert updated.last_run_at is not None

#     # 直接调用任务
#     result = scheduler_tick.tick_price_reset()
#     assert isinstance(result, dict)  # 防止测试红

#     # 再查 DB 看 last_run_at 是否更新
#     with SessionLocal() as session:
#         updated = session.get(Schedule, 'price_reset')
#         assert updated.last_run_at is not None

