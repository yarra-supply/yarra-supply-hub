

from fastapi import APIRouter, Depends, Response, HTTPException, status
from pydantic import BaseModel
from sqlalchemy.orm import Session
from app.db.session import get_db
from app.services.auth_service import (
    clear_cookie, get_current_user, login_user,
)
from app.core.security import create_access_token


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
    # mock test 使用
    return login_user(response, data.username, data.password)

    # user = authenticate_user(db, data.username, data.password)
    # if not user:
    #     raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid username or password")
    # token = create_access_token({"user_id": user.id, "username": user.username})
    # issue_cookie(response, token)
    # return UserOut(
    #     id=user.id, username=user.username, full_name=user.full_name, is_superuser=user.is_superuser
    # )


@router.post("/logout")
def logout(response: Response):
    clear_cookie(response)
    return {"ok": True}


@router.get("/me", response_model=UserOut)
def me(current=Depends(get_current_user)):
    # mock test
    # current 是 dict（FAKE_USER），用下标取
    return UserOut(
        id=current["id"],
        username=current["username"],
        full_name=current["full_name"],
        is_superuser=current["is_superuser"],
    )

    # return UserOut(
    #     id=current.id, username=current.username, 
    #     full_name=current.full_name, is_superuser=current.is_superuser
    # )
