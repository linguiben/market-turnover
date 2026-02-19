from __future__ import annotations

import ipaddress
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from fastapi import FastAPI, Request, Response

from app.db.models import UserVisitLog
from app.db.session import SessionLocal
from app.web.activity_counter import increment_activity_counter
from app.web.auth import AUTH_COOKIE_NAME, parse_session_user_id

logger = logging.getLogger(__name__)

VISIT_TRACKING_MAX_AGE = 7 * 24 * 3600

# Limit background analytics concurrency to avoid exhausting DB connections.
_visit_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="visitlog")


_EXCLUDE_PATH_PREFIXES = (
    "/healthz",
    "/docs",
    "/openapi.json",
    "/redoc",
    "/test/",
    "/favicon.ico",
    "/static/",
)


def _should_skip(request: Request) -> bool:
    path = request.url.path or ""
    for prefix in _EXCLUDE_PATH_PREFIXES:
        if path.startswith(prefix):
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


def _persist_visit_log_async(payload: dict[str, Any], should_increment: bool = True) -> None:
    """Persist visit log in a background thread.

    IMPORTANT: never block the request path for analytics/logging.
    """

    try:
        db = SessionLocal()
        try:
            row = UserVisitLog(**payload)
            db.add(row)
            if should_increment:
                increment_activity_counter(db, event="visit")
            db.commit()
        finally:
            db.close()
    except Exception:
        logger.exception("failed to write user visit log")


def add_visit_logging(app: FastAPI) -> None:
    @app.middleware("http")
    async def _visit_logger(request: Request, call_next):
        if _should_skip(request):
            return await call_next(request)

        # Check for deduplication: cookie or refresh param
        is_refresh = request.query_params.get("refresh") == "1"
        has_cookie = request.cookies.get("v_tracked") == "1"
        should_increment = not (is_refresh or has_cookie)

        # Let the request proceed
        response: Response = await call_next(request)

        try:
            ip_str = _client_ip(request)
            if ip_str:
                payload = {
                    "user_id": parse_session_user_id(request.cookies.get(AUTH_COOKIE_NAME)),
                    "ip_address": ip_str,
                    "session_id": request.cookies.get("session_id") or request.cookies.get("session"),
                    "action_type": "visit",
                    "user_agent": request.headers.get("user-agent"),
                    "browser_family": None,
                    "os_family": None,
                    "device_type": None,
                    "request_url": str(request.url),
                    "referer_url": request.headers.get("referer"),
                    "request_headers": _safe_headers(request.headers),
                }
                _visit_executor.submit(_persist_visit_log_async, payload, should_increment)

            # Always set/refresh the cookie if we want to track this user
            # but only if it's missing or we just incremented.
            if should_increment:
                response.set_cookie(
                    key="v_tracked",
                    value="1",
                    max_age=VISIT_TRACKING_MAX_AGE,
                    path="/",
                    httponly=True,
                    samesite="lax",
                )
        except Exception:
            logger.exception("failed to process user visit log")

        return response
