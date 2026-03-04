import os
import secrets
import time
from datetime import datetime, timedelta, timezone
from base64 import urlsafe_b64decode, urlsafe_b64encode
from hashlib import sha256
from hmac import new as hmac_new
from urllib.parse import urlparse

from bson import ObjectId
from bson.errors import InvalidId
from fastapi import Request, Response
from pymongo import ReturnDocument
from app.database import get_database
from app.models import Visitor
from app.realtime import admin_realtime_hub

ADMIN_SESSION_COOKIE = "admin_session"
ADMIN_SESSION_MAX_AGE = 60 * 60 * 12


def validate_admin_credentials(username: str, password: str) -> bool:
    expected_username = str(os.getenv("ADMIN_USERNAME", "")).strip()
    expected_password = str(os.getenv("ADMIN_PASSWORD", ""))
    # Never raise from login validation to avoid turning auth failures
    # into 500 responses.
    if not expected_username or not expected_password:
        return False
    if expected_password.lower() == "change-me":
        return False
    is_valid_username = secrets.compare_digest(username, expected_username)
    is_valid_password = secrets.compare_digest(password, expected_password)
    return is_valid_username and is_valid_password


def _admin_session_secret() -> str:
    secret = str(os.getenv("ADMIN_SESSION_SECRET", "")).strip()
    if len(secret) < 32 or secret.lower() == "change-me":
        raise RuntimeError(
            "ADMIN_SESSION_SECRET must be configured and at least 32 characters long."
        )
    return secret


def get_request_ip(request: Request) -> str:
    trust_proxy_headers = str(os.getenv("TRUST_PROXY_HEADERS", "")).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    if trust_proxy_headers:
        forwarded_for = request.headers.get("x-forwarded-for")
        if isinstance(forwarded_for, str) and forwarded_for.strip():
            first_ip = forwarded_for.split(",")[0].strip()
            if first_ip:
                return first_ip
        real_ip = request.headers.get("x-real-ip")
        if isinstance(real_ip, str) and real_ip.strip():
            return real_ip.strip()
    client_host = request.client.host if request.client is not None else ""
    return client_host or "unknown"


async def allow_rate_limit(bucket: str, limit: int, window_seconds: int) -> bool:
    now = datetime.now(timezone.utc)
    window_epoch = int(time.time()) // window_seconds
    key = f"{bucket}:{window_epoch}"
    expires_at = now + timedelta(seconds=window_seconds + 30)
    collection = get_database()["rate_limits"]
    doc = await collection.find_one_and_update(
        {"_id": key},
        {
            "$inc": {"count": 1},
            "$setOnInsert": {"expires_at": expires_at},
        },
        upsert=True,
        return_document=ReturnDocument.AFTER,
    )
    if not isinstance(doc, dict):
        return True
    count = int(doc.get("count", 0))
    return count <= limit


def has_same_origin(request: Request) -> bool:
    request_origin = request.headers.get("origin")
    request_referer = request.headers.get("referer")
    host = request.headers.get("host")
    if not host:
        return False

    allowed_host = host.lower()
    candidates = [value for value in (request_origin, request_referer) if isinstance(value, str) and value.strip()]
    if not candidates:
        return False

    for candidate in candidates:
        parsed = urlparse(candidate)
        candidate_host = parsed.netloc.lower()
        if candidate_host == allowed_host:
            return True
    return False


def create_admin_session_token(username: str) -> str:
    issued_at = str(int(time.time()))
    payload = f"{username}:{issued_at}"
    signature = hmac_new(
        _admin_session_secret().encode("utf-8"),
        payload.encode("utf-8"),
        sha256,
    ).hexdigest()
    token = f"{payload}:{signature}".encode("utf-8")
    return urlsafe_b64encode(token).decode("utf-8")


def get_admin_user_from_request(request: Request) -> str | None:
    token = request.cookies.get(ADMIN_SESSION_COOKIE)
    return get_admin_user_from_token(token)


def get_admin_user_from_token(token: str | None) -> str | None:
    if not token:
        return None

    try:
        decoded = urlsafe_b64decode(token.encode("utf-8")).decode("utf-8")
        username, issued_at_raw, signature = decoded.split(":", 2)
    except Exception:
        return None

    payload = f"{username}:{issued_at_raw}"
    expected_signature = hmac_new(
        _admin_session_secret().encode("utf-8"),
        payload.encode("utf-8"),
        sha256,
    ).hexdigest()

    if not secrets.compare_digest(signature, expected_signature):
        return None

    try:
        issued_at = int(issued_at_raw)
    except ValueError:
        return None

    if int(time.time()) - issued_at > ADMIN_SESSION_MAX_AGE:
        return None

    return username


async def get_or_create_id(request: Request, response: Response) -> tuple[str, str]:
    collection = get_database()["visitors"]
    metrics_collection = get_database()["visitor_metrics"]
    cookie_id = request.cookies.get("id")
    should_count_visit = request.method.upper() == "GET" and not request.url.path.startswith("/visitors/")
    now_utc = datetime.now(timezone.utc)

    async def track_metrics(is_new_visitor: bool) -> None:
        inc_payload: dict[str, int] = {}
        if should_count_visit:
            inc_payload["total_visits"] = 1
        if is_new_visitor:
            inc_payload["unique_visitors"] = 1
        if not inc_payload:
            return

        await metrics_collection.update_one(
            {"_id": "global"},
            {
                "$inc": inc_payload,
                "$set": {"updated_at": now_utc},
                "$setOnInsert": {"created_at": now_utc},
            },
            upsert=True,
        )

    async def create_new_visitor() -> tuple[str, str]:
        visitor = Visitor(id=ObjectId())
        await collection.insert_one(visitor.model_dump(by_alias=True))
        visitor_id = str(visitor.id)
        response.set_cookie(
            key="id",
            value=visitor_id,
            httponly=True,
            samesite="lax",
            secure=request.url.scheme == "https",
            max_age=31536000,
        )
        await track_metrics(is_new_visitor=True)
        await admin_realtime_hub.broadcast("visitor.created", visitor_id=visitor_id)
        return visitor_id, "new"

    if not cookie_id:
        return await create_new_visitor()

    try:
        object_id = ObjectId(cookie_id)
    except InvalidId:
        return await create_new_visitor()

    existing = await collection.find_one({"_id": object_id})
    if existing is None:
        visitor = Visitor(id=object_id)
        await collection.insert_one(visitor.model_dump(by_alias=True))
        await track_metrics(is_new_visitor=True)
        visitor_id = str(visitor.id)
        await admin_realtime_hub.broadcast("visitor.created", visitor_id=visitor_id)
        return visitor_id, "new"

    update_doc: dict[str, dict[str, object]] = {
        "$set": {"last_activity": now_utc},
    }
    if isinstance(existing, dict) and "archived_at" in existing:
        update_doc["$unset"] = {
            "archived_at": "",
            "archive_reason": "",
        }

    await collection.update_one({"_id": object_id}, update_doc)
    await track_metrics(is_new_visitor=False)
    return str(object_id), "returning"
