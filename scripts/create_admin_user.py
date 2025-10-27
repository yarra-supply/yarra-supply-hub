

from sqlalchemy.orm import Session
from app.db.session import SessionLocal
from app.repository.user_repo import get_by_username, create_user
from app.core.security import get_password_hash


# 在容器里运行一次：python -m scripts.create_admin_user
# （确保 PYTHONPATH 指向 app 上级目录，或把脚本放在合适的位置）。

def main():
    db: Session = SessionLocal()
    username = "admin"
    password = "admin123"  # 改成你自己的
    if get_by_username(db, username):
        print("User exists")
        return
    create_user(db, username, get_password_hash(password), full_name="Admin", is_superuser=True)
    print("Admin created")

if __name__ == "__main__":
    main()
