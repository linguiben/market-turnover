from __future__ import annotations

import ipaddress
import logging
from typing import Any

from fastapi import FastAPI, Request, Response

from app.db.models import UserVisitLog
from app.db.session import SessionLocal
from app.web.activity_counter import increment_activity_counter
from app.web.auth import AUTH_COOKIE_NAME, parse_session_user_id

logger = logging.getLogger(__name__)


_EXCLUDE_PATH_PREFIXES = (
    "/healthz",
    "/docs",
    "/openapi.json",
    "/redoc",
    "/test/",
)


def _should_skip(request: Request) -> bool:
    path = request.url.path or ""
    for prefix in _EXCLUDE_PATH_PREFIXES:
        if prefix.endswith("/"):
            if path.startswith(prefix):
                return True
        else:
            if path == prefix or path.startswith(prefix + "/"):
                return True
    return False


def _client_ip(request: Request) -> str | None:
    # Prefer reverse-proxy forwarded IP if present.
    xff = request.headers.get("x-forwarded-for")
    if xff:
        first = xff.split(",", 1)[0].strip()
        if first:
            return first

    real_ip = request.headers.get("x-real-ip")
    if real_ip:
        return real_ip.strip()

    if request.client and request.client.host:
        return request.client.host
    return None


def _safe_headers(headers: Any) -> dict[str, str]:
    # Avoid persisting secrets by default.
    deny = {"authorization", "cookie", "set-cookie"}
    out: dict[str, str] = {}
    for k, v in headers.items():
        lk = k.lower()
        if lk in deny:
            out[k] = "<redacted>"
        else:
            out[k] = v
    return out


def add_visit_logging(app: FastAPI) -> None:
    @app.middleware("http")
    async def _visit_logger(request: Request, call_next):
        if _should_skip(request):
            return await call_next(request)

        response: Response | None = None
        try:
            response = await call_next(request)
            return response
        finally:
            try:
                ip_str = _client_ip(request)
                if not ip_str:
                    return

                # validate/normalize (Postgres INET will also validate)
                try:
                    ipaddress.ip_address(ip_str)
                except ValueError:
                    return

                db = SessionLocal()
                try:
                    row = UserVisitLog(
                        user_id=parse_session_user_id(request.cookies.get(AUTH_COOKIE_NAME)),
                        ip_address=ip_str,
                        session_id=request.cookies.get("session_id") or request.cookies.get("session"),
                        action_type="visit",
                        user_agent=request.headers.get("user-agent"),
                        browser_family=None,
                        os_family=None,
                        device_type=None,
                        request_url=str(request.url),
                        referer_url=request.headers.get("referer"),
                        request_headers=_safe_headers(request.headers),
                    )
                    db.add(row)
                    increment_activity_counter(db, event="visit")
                    db.commit()
                finally:
                    db.close()
            except Exception:
                # Never break user traffic for logging.
                logger.exception("failed to write user visit log")
