

from sqlalchemy.orm import Session
from typing import Optional
from app.db.model.user import User
from app.core.security import get_password_hash


DEFAULT_USERNAME = "yarrasupply"
DEFAULT_PASSWORD = "yarrasupply2025"
DEFAULT_FULL_NAME = "Yarra Supply Admin"


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


def create_default_user(db: Session) -> User:
    """Create the default admin user if it does not exist; return the user."""
    existing = get_by_username(db, DEFAULT_USERNAME)
    if existing:
        return existing

    hashed_password = get_password_hash(DEFAULT_PASSWORD)
    return create_user(
        db,
        username=DEFAULT_USERNAME,
        hashed_password=hashed_password,
        full_name=DEFAULT_FULL_NAME,
        is_superuser=True,
    )
