import os
from typing import Iterable

from bson import ObjectId

try:
    import redis.asyncio as redis
except Exception:  # pragma: no cover
    redis = None  # type: ignore[assignment]


class PresenceState:
    client = None
    enabled = False


presence_state = PresenceState()

ONLINE_KEY_PREFIX = "presence:online:"
PAGE_KEY_PREFIX = "presence:page:"
DEFAULT_TTL_SECONDS = 20


def _normalize_visitor_id(visitor_id: str) -> str:
    try:
        return str(ObjectId(visitor_id))
    except Exception:
        return str(visitor_id).strip()


def _online_key(visitor_id: str) -> str:
    return f"{ONLINE_KEY_PREFIX}{_normalize_visitor_id(visitor_id)}"


def _page_key(visitor_id: str) -> str:
    return f"{PAGE_KEY_PREFIX}{_normalize_visitor_id(visitor_id)}"


async def start_presence() -> None:
    redis_url = str(os.getenv("REDIS_URL", "")).strip()
    if not redis_url or redis is None:
        presence_state.enabled = False
        presence_state.client = None
        return
    presence_state.client = redis.from_url(redis_url, decode_responses=True)
    presence_state.enabled = True


async def stop_presence() -> None:
    if presence_state.client is not None:
        try:
            await presence_state.client.close()
        except Exception:
            pass
    presence_state.client = None
    presence_state.enabled = False


async def mark_online(visitor_id: str, current_page: str | None = None, ttl_seconds: int = DEFAULT_TTL_SECONDS) -> None:
    if not presence_state.enabled or presence_state.client is None:
        return
    vid = _normalize_visitor_id(visitor_id)
    if not vid:
        return
    online_key = _online_key(vid)
    page_key = _page_key(vid)
    try:
        pipe = presence_state.client.pipeline()
        pipe.set(online_key, "1", ex=max(1, int(ttl_seconds)))
        if isinstance(current_page, str) and current_page.strip():
            pipe.set(page_key, current_page.strip(), ex=max(1, int(ttl_seconds)))
        else:
            pipe.expire(page_key, max(1, int(ttl_seconds)))
        await pipe.execute()
    except Exception:
        return


async def update_current_page(visitor_id: str, current_page: str, ttl_seconds: int = DEFAULT_TTL_SECONDS) -> None:
    if not presence_state.enabled or presence_state.client is None:
        return
    vid = _normalize_visitor_id(visitor_id)
    if not vid:
        return
    try:
        await presence_state.client.set(
            _page_key(vid),
            str(current_page).strip(),
            ex=max(1, int(ttl_seconds)),
        )
    except Exception:
        return


async def mark_offline(visitor_id: str) -> None:
    if not presence_state.enabled or presence_state.client is None:
        return
    vid = _normalize_visitor_id(visitor_id)
    if not vid:
        return
    try:
        await presence_state.client.delete(_online_key(vid), _page_key(vid))
    except Exception:
        return


async def is_online(visitor_id: str) -> bool:
    if not presence_state.enabled or presence_state.client is None:
        return False
    vid = _normalize_visitor_id(visitor_id)
    if not vid:
        return False
    try:
        return bool(await presence_state.client.exists(_online_key(vid)))
    except Exception:
        return False


async def get_online_statuses(visitor_ids: Iterable[str]) -> dict[str, bool]:
    ids = [v for v in (_normalize_visitor_id(item) for item in visitor_ids) if v]
    if not ids:
        return {}
    if not presence_state.enabled or presence_state.client is None:
        return {visitor_id: False for visitor_id in ids}
    keys = [_online_key(visitor_id) for visitor_id in ids]
    try:
        values = await presence_state.client.mget(keys)
    except Exception:
        return {visitor_id: False for visitor_id in ids}
    return {
        visitor_id: bool(value)
        for visitor_id, value in zip(ids, values)
    }


async def get_current_pages(visitor_ids: Iterable[str]) -> dict[str, str]:
    ids = [v for v in (_normalize_visitor_id(item) for item in visitor_ids) if v]
    if not ids:
        return {}
    if not presence_state.enabled or presence_state.client is None:
        return {}
    keys = [_page_key(visitor_id) for visitor_id in ids]
    try:
        values = await presence_state.client.mget(keys)
    except Exception:
        return {}
    result: dict[str, str] = {}
    for visitor_id, value in zip(ids, values):
        if isinstance(value, str) and value.strip():
            result[visitor_id] = value.strip()
    return result


async def count_online() -> int:
    if not presence_state.enabled or presence_state.client is None:
        return 0
    count = 0
    try:
        async for _key in presence_state.client.scan_iter(match=f"{ONLINE_KEY_PREFIX}*"):
            count += 1
    except Exception:
        return 0
    return count


async def list_online_visitor_ids() -> set[str]:
    if not presence_state.enabled or presence_state.client is None:
        return set()
    result: set[str] = set()
    try:
        async for key in presence_state.client.scan_iter(match=f"{ONLINE_KEY_PREFIX}*"):
            if not isinstance(key, str):
                continue
            visitor_id = key.removeprefix(ONLINE_KEY_PREFIX).strip()
            if visitor_id:
                result.add(visitor_id)
    except Exception:
        return set()
    return result
