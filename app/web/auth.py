from __future__ import annotations

import base64
import hashlib
import hmac
import os
import re
import time
from urllib.parse import quote, urlparse

from fastapi import Depends, Request
from fastapi.responses import RedirectResponse, Response
from sqlalchemy.orm import Session

from app.config import settings
from app.db.models import AppUser
from app.db.session import get_db

AUTH_COOKIE_NAME = "mt_session"
_PASSWORD_SCHEME = "pbkdf2_sha256"
_PASSWORD_ITERATIONS = 260_000
_EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")


def normalize_email(email: str) -> str:
    return email.strip().lower()


def is_valid_email(email: str) -> bool:
    return bool(_EMAIL_RE.fullmatch(email))


def hash_password(password: str) -> str:
    salt = os.urandom(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, _PASSWORD_ITERATIONS)
    return f"{_PASSWORD_SCHEME}${_PASSWORD_ITERATIONS}${salt.hex()}${digest.hex()}"


def verify_password(password: str, password_hash: str) -> bool:
    try:
        scheme, iterations_s, salt_hex, digest_hex = password_hash.split("$", 3)
        if scheme != _PASSWORD_SCHEME:
            return False
        iterations = int(iterations_s)
        salt = bytes.fromhex(salt_hex)
        expected = bytes.fromhex(digest_hex)
    except Exception:
        return False
    actual = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return hmac.compare_digest(actual, expected)


def _secret_bytes() -> bytes:
    secret = (settings.AUTH_SECRET_KEY or "").strip()
    if not secret:
        secret = "dev-only-change-me"
    return secret.encode("utf-8")


def _b64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("utf-8").rstrip("=")


def _b64url_decode(raw: str) -> bytes:
    return base64.urlsafe_b64decode(raw + ("=" * (-len(raw) % 4)))


def create_session_token(user_id: int) -> str:
    exp_ts = int(time.time()) + int(settings.AUTH_SESSION_MAX_AGE_SECONDS)
    payload = f"{int(user_id)}:{exp_ts}".encode("utf-8")
    payload_b64 = _b64url_encode(payload)
    sig = hmac.new(_secret_bytes(), payload_b64.encode("utf-8"), hashlib.sha256).hexdigest()
    return f"{payload_b64}.{sig}"


def parse_session_user_id(token: str | None) -> int | None:
    if not token or "." not in token:
        return None
    payload_b64, sig = token.split(".", 1)
    expected_sig = hmac.new(_secret_bytes(), payload_b64.encode("utf-8"), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(sig, expected_sig):
        return None
    try:
        payload = _b64url_decode(payload_b64).decode("utf-8")
        user_id_s, exp_ts_s = payload.split(":", 1)
        user_id = int(user_id_s)
        exp_ts = int(exp_ts_s)
    except Exception:
        return None
    if int(time.time()) > exp_ts:
        return None
    return user_id


def set_login_cookie(response: Response, user_id: int) -> None:
    token = create_session_token(user_id)
    response.set_cookie(
        key=AUTH_COOKIE_NAME,
        value=token,
        max_age=int(settings.AUTH_SESSION_MAX_AGE_SECONDS),
        httponly=True,
        samesite="lax",
        secure=False,
        path="/",
    )


def clear_login_cookie(response: Response) -> None:
    response.delete_cookie(key=AUTH_COOKIE_NAME, path="/")


def get_current_user(request: Request, db: Session = Depends(get_db)) -> AppUser | None:
    user_id = parse_session_user_id(request.cookies.get(AUTH_COOKIE_NAME))
    if user_id is None:
        return None
    user = db.query(AppUser).filter(AppUser.id == user_id).first()
    if user is None or not user.is_active:
        return None
    return user


def safe_next_path(next_path: str | None, *, fallback: str = "/") -> str:
    if not next_path:
        return fallback
    parsed = urlparse(next_path)
    if parsed.scheme or parsed.netloc:
        return fallback
    if not next_path.startswith("/"):
        return fallback
    return next_path


def build_login_redirect(request: Request, *, next_path: str | None = None) -> RedirectResponse:
    path = next_path
    if not path:
        path = request.url.path
        if request.url.query:
            path = f"{path}?{request.url.query}"
    path = safe_next_path(path, fallback="/")
    base = (request.scope.get("root_path") or "").rstrip("/")
    return RedirectResponse(url=f"{base}/login?next={quote(path, safe='')}", status_code=303)
