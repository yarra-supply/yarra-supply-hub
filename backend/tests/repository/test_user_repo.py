# import pytest
# from sqlalchemy.exc import OperationalError

# from app.db.model.user import User
# from app.db.session import SessionLocal
# from app.repository import user_repo
# from app.core.security import verify_password


# pytestmark = pytest.mark.integration


# def test_create_default_user_persists_and_is_idempotent():
#     """Integration check: the default admin user is created (or reused) in the real DB."""
#     try:
#         with SessionLocal() as db:
#             # 保证测试起点干净，便于断言插入逻辑
#             db.query(User).filter(User.username == user_repo.DEFAULT_USERNAME).delete()
#             db.commit()

#             created = user_repo.create_default_user(db)
#             assert created.id is not None, "Default user should have a primary key after insert"
#             assert created.username == user_repo.DEFAULT_USERNAME
#             assert created.is_superuser is True
#             assert created.is_active is True
#             assert created.full_name == user_repo.DEFAULT_FULL_NAME
#             assert created.hashed_password != user_repo.DEFAULT_PASSWORD
#             assert verify_password(user_repo.DEFAULT_PASSWORD, created.hashed_password)

#             # 第二次调用不应重新插入记录，应返回现有用户
#             again = user_repo.create_default_user(db)
#             assert again.id == created.id

#             # 再次从数据库读取，确认数据真实存在
#             fetched = user_repo.get_by_username(db, user_repo.DEFAULT_USERNAME)
#             assert fetched is not None and fetched.id == created.id
#     except OperationalError as exc:
#         pytest.skip(f"Database not reachable for integration test: {exc}")
