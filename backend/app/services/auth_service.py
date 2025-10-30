
from fastapi import Depends, HTTPException, status, Request, Response
from sqlalchemy.orm import Session
from app.db.session import get_db
from app.core.security import verify_password, create_access_token, decode_token
from app.core.config import settings
from app.db.model.user import User
from app.repository.user_repo import get_by_username


COOKIE_NAME = settings.COOKIE_NAME

# 统一 Cookie 策略：
# - 线上/云环境：Secure=True，SameSite="Strict"（如需第三方跳转回调可用 "Lax"）
# - 本地 http 开发（不走 HTTPS）可以降级 Secure=False
ENV = getattr(settings, "ENVIRONMENT", "dev")
COOKIE_SECURE_DEFAULT = False if ENV in ("local", "dev") else True
COOKIE_DOMAIN = getattr(settings, "COOKIE_DOMAIN", None) or None
COOKIE_SAMESITE = getattr(settings, "COOKIE_SAMESITE", "Strict")  # 可在 .env 调整 Strict/Lax


'''
设置了一个 Cookie
    - 只有一枚 Cookie，且过期取决于 Access 的 TTL
    - max_age 用的是 ACCESS_TOKEN_EXPIRE_MINUTES（典型的短期 access）。无第二枚 refresh_token
    - 只负责发 HttpOnly '登录票据' 的 Cookie。
    - max_age_sec 要与 JWT 的过期时间一致（比如 8 小时 = 28800 秒）。
'''
def set_auth_cookie(resp: Response, token: str, max_age: int):
    resp.set_cookie(
        key=COOKIE_NAME,
        value=token, 
        max_age=max_age,
        httponly=True, 
        secure=COOKIE_SECURE_DEFAULT,
        samesite=COOKIE_SAMESITE,       # Strict 或 Lax
        domain=COOKIE_DOMAIN, 
        path="/",
    )


def clear_cookie(response):
    response.delete_cookie(key=COOKIE_NAME, domain=COOKIE_DOMAIN, path="/")
    


'''
登录：
    - 登录主逻辑：把有效期、两枚 Cookie（票据 + csrf）一次性写好
    登录成功后：
    1) 签发 Access Token（JWT）并写入 HttpOnly Cookie（Secure + SameSite=None）
    2) 生成可读的 csrf_token Cookie（非 HttpOnly），用于双提交校验
    3) 有效期：默认 8 小时（可通过 settings.ACCESS_TOKEN_EXPIRE_MINUTES 配置为 480/720）
'''
def login_user(response: Response, db: Session, username: str, password: str) -> User:
    user = authenticate_user(db, username, password)
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")
    
    # 1) 设定有效期（分钟）
    expires_minutes = int(getattr(settings, "ACCESS_TOKEN_EXPIRE_MINUTES", 480) or 480)
    max_age = expires_minutes * 60

    # 2) 签发 JWT（把有效期显式传入，确保与 Cookie 的 max_age 对齐）
    token = create_access_token(
        {"user_id": user.id, "username": user.username}, 
        expires_minutes=expires_minutes
    )

    # 3) 写入两枚 Cookie：票据（HttpOnly）+ csrf（非 HttpOnly）
    set_auth_cookie(response, token, max_age)

    # 4) 返回前端用的用户概要
    return user


def authenticate_user(db: Session, username: str, password: str) -> User | None:
    user = get_by_username(db, username)
    if not user or not user.is_active:
        return None
    if not verify_password(password, user.hashed_password):
        return None
    return user


'''
获取当前登录用户
    - 从 Cookie 里拿到 token → decode_token(...) 
    - 读出 user_id，没有去 Redis/DB 用 sessionId 回表找用户会话
'''
def get_current_user(request: Request, db: Session = Depends(get_db)) -> User:
    """从 Cookie 取出 JWT 并校验"""
    raw = request.cookies.get(COOKIE_NAME)
    if not raw:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Not authenticated")
    payload = decode_token(raw)
    if not payload or "user_id" not in payload:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token")
    
    user = db.get(User, payload["user_id"])
    if not user or not user.is_active:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User disabled")
    return user
