

from fastapi import APIRouter, Depends, Response
from pydantic import BaseModel
from sqlalchemy.orm import Session
from app.db.session import get_db
from app.services.auth_service import (
    clear_cookie, get_current_user, login_user,
)


router = APIRouter(prefix="/auth", tags=["auth"])


class LoginInput(BaseModel):
    username: str
    password: str


class UserOut(BaseModel):
    id: int
    username: str
    full_name: str | None = None
    is_superuser: bool


@router.post("/login", response_model=UserOut)
def login(data: LoginInput, response: Response, db: Session = Depends(get_db)):
    user = login_user(response, db, data.username, data.password)
    return UserOut(
        id=user.id,
        username=user.username,
        full_name=user.full_name,
        is_superuser=user.is_superuser,
    )


@router.post("/logout")
def logout(response: Response):
    clear_cookie(response)
    return {"ok": True}


@router.get("/me", response_model=UserOut)
def me(current=Depends(get_current_user)):
    return UserOut(
        id=current.id,
        username=current.username,
        full_name=current.full_name,
        is_superuser=current.is_superuser,
    )
