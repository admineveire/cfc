import asyncio
import json
import os
from typing import Any
from uuid import uuid4

from fastapi import WebSocket

try:
    import redis.asyncio as redis
except Exception:  # pragma: no cover
    redis = None  # type: ignore[assignment]


class AdminRealtimeHub:
    def __init__(self) -> None:
        self._connections: set[WebSocket] = set()
        self._lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        async with self._lock:
            self._connections.add(websocket)

    async def disconnect(self, websocket: WebSocket) -> None:
        async with self._lock:
            self._connections.discard(websocket)

    async def _send_payload(self, payload: dict[str, Any]) -> None:
        async with self._lock:
            targets = list(self._connections)

        stale: list[WebSocket] = []
        for connection in targets:
            try:
                await connection.send_json(payload)
            except Exception:
                stale.append(connection)

        if stale:
            async with self._lock:
                for connection in stale:
                    self._connections.discard(connection)

    async def broadcast(
        self,
        event: str,
        visitor_id: str | None = None,
        meta: dict[str, Any] | None = None,
    ) -> None:
        payload: dict[str, Any] = {"event": event}
        if visitor_id:
            payload["visitor_id"] = visitor_id
        if isinstance(meta, dict) and meta:
            payload["meta"] = meta

        await self._send_payload(payload)
        await realtime_bus.publish_admin(payload)

    async def receive_bus_payload(self, payload: dict[str, Any]) -> None:
        await self._send_payload(payload)


admin_realtime_hub = AdminRealtimeHub()


class VisitorRealtimeHub:
    def __init__(self) -> None:
        self._connections_by_visitor: dict[str, set[WebSocket]] = {}
        self._lock = asyncio.Lock()

    async def connect(self, visitor_id: str, websocket: WebSocket) -> None:
        await websocket.accept()
        async with self._lock:
            bucket = self._connections_by_visitor.setdefault(visitor_id, set())
            bucket.add(websocket)

    async def disconnect(self, visitor_id: str, websocket: WebSocket) -> None:
        async with self._lock:
            bucket = self._connections_by_visitor.get(visitor_id)
            if not bucket:
                return
            bucket.discard(websocket)
            if not bucket:
                self._connections_by_visitor.pop(visitor_id, None)

    async def _send_to_visitor_local(
        self,
        visitor_id: str,
        payload: dict[str, Any],
    ) -> None:
        async with self._lock:
            targets = list(self._connections_by_visitor.get(visitor_id, set()))

        stale: list[WebSocket] = []
        for connection in targets:
            try:
                await connection.send_json(payload)
            except Exception:
                stale.append(connection)

        if stale:
            async with self._lock:
                bucket = self._connections_by_visitor.get(visitor_id)
                if not bucket:
                    return
                for connection in stale:
                    bucket.discard(connection)
                if not bucket:
                    self._connections_by_visitor.pop(visitor_id, None)

    async def send_to_visitor(
        self,
        visitor_id: str,
        event: str,
        meta: dict[str, Any] | None = None,
    ) -> None:
        payload: dict[str, Any] = {"event": event, "visitor_id": visitor_id}
        if isinstance(meta, dict) and meta:
            payload["meta"] = meta

        await self._send_to_visitor_local(visitor_id, payload)
        await realtime_bus.publish_visitor(visitor_id, payload)

    async def receive_bus_payload(self, visitor_id: str, payload: dict[str, Any]) -> None:
        await self._send_to_visitor_local(visitor_id, payload)


visitor_realtime_hub = VisitorRealtimeHub()


class RedisRealtimeBus:
    def __init__(self) -> None:
        self._redis = None
        self._pubsub = None
        self._listener_task: asyncio.Task[None] | None = None
        self._channel = str(os.getenv("REDIS_REALTIME_CHANNEL", "fastapi_realtime_events")).strip() or "fastapi_realtime_events"
        self._instance_id = uuid4().hex
        self._enabled = False

    async def start(self) -> None:
        redis_url = str(os.getenv("REDIS_URL", "")).strip()
        if not redis_url or redis is None:
            self._enabled = False
            return

        self._redis = redis.from_url(redis_url, decode_responses=True)
        self._pubsub = self._redis.pubsub()
        await self._pubsub.subscribe(self._channel)
        self._enabled = True
        self._listener_task = asyncio.create_task(self._listen_loop())

    async def stop(self) -> None:
        self._enabled = False
        if self._listener_task:
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass
            self._listener_task = None

        if self._pubsub is not None:
            try:
                await self._pubsub.unsubscribe(self._channel)
            except Exception:
                pass
            try:
                await self._pubsub.close()
            except Exception:
                pass
            self._pubsub = None

        if self._redis is not None:
            try:
                await self._redis.close()
            except Exception:
                pass
            self._redis = None

    async def publish_admin(self, payload: dict[str, Any]) -> None:
        await self._publish(scope="admin", payload=payload)

    async def publish_visitor(self, visitor_id: str, payload: dict[str, Any]) -> None:
        await self._publish(scope="visitor", payload=payload, visitor_id=visitor_id)

    async def _publish(
        self,
        scope: str,
        payload: dict[str, Any],
        visitor_id: str | None = None,
    ) -> None:
        if not self._enabled or self._redis is None:
            return

        envelope = {
            "source": self._instance_id,
            "scope": scope,
            "payload": payload,
        }
        if visitor_id:
            envelope["visitor_id"] = visitor_id

        try:
            await self._redis.publish(self._channel, json.dumps(envelope, ensure_ascii=True))
        except Exception:
            # Redis is an optimization path. Local delivery already happened.
            return

    async def _listen_loop(self) -> None:
        if self._pubsub is None:
            return
        try:
            async for message in self._pubsub.listen():
                if not isinstance(message, dict):
                    continue
                if message.get("type") != "message":
                    continue

                raw_data = message.get("data")
                if not isinstance(raw_data, str):
                    continue

                try:
                    envelope = json.loads(raw_data)
                except Exception:
                    continue
                if not isinstance(envelope, dict):
                    continue

                source = envelope.get("source")
                if isinstance(source, str) and source == self._instance_id:
                    continue

                scope = str(envelope.get("scope", "")).strip().lower()
                payload = envelope.get("payload")
                if not isinstance(payload, dict):
                    continue

                if scope == "admin":
                    await admin_realtime_hub.receive_bus_payload(payload)
                    continue

                if scope == "visitor":
                    visitor_id = str(envelope.get("visitor_id", "")).strip()
                    if not visitor_id:
                        visitor_id = str(payload.get("visitor_id", "")).strip()
                    if not visitor_id:
                        continue
                    await visitor_realtime_hub.receive_bus_payload(visitor_id, payload)
        except asyncio.CancelledError:
            return
        except Exception:
            return


realtime_bus = RedisRealtimeBus()


async def start_realtime_bus() -> None:
    await realtime_bus.start()


async def stop_realtime_bus() -> None:
    await realtime_bus.stop()
