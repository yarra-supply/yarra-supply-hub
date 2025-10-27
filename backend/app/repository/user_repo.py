

from sqlalchemy.orm import Session
from typing import Optional
from app.db.model.user import User


def get_by_username(db: Session, username: str) -> Optional[User]:
    return db.query(User).filter(User.username == username).first()


def create_user(db: Session, username: str, hashed_password: str,
                full_name: str | None = None, is_superuser: bool = False) -> User:
    user = User(
        username=username,
        hashed_password=hashed_password,
        full_name=full_name,
        is_superuser=is_superuser,
        is_active=True,
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    return user
