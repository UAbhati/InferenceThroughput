"""Coordination between gateway replicas.

Without Redis a gateway is a single replica: it owns the whole limit and treats every other owner found in the
database as dead. With Redis, replicas:
  * heartbeat (`replica:<id>` with a TTL), so everyone knows who is alive and who died;
  * split every model limit evenly across live replicas, so the sum of their usage cannot exceed the limit;
  * share limit changes: the current limits live in one hash and every change is published to all replicas.

A replica that joins waits `join_delay_s` before it starts dispatching, so the others have time to shrink their share
first; otherwise the combined rate would briefly exceed the limit.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from typing import Awaitable, Callable

log = logging.getLogger("gateway.coordination")

LimitsCallback = Callable[[str, int, int], None]


class LocalCoordinator:
    shared = False

    def __init__(self, replica_id: str | None = None):
        self.replica_id = replica_id or uuid.uuid4().hex[:12]
        self.replicas = [self.replica_id]
        self.dispatch_after = 0.0

    async def start(self, on_limits: LimitsCallback, on_membership: Callable[[], None]) -> None: ...
    async def stop(self) -> None: ...

    def share(self, total: int) -> int:
        return total

    async def get_limits(self) -> dict[str, tuple[int, int]]:
        return {}

    async def publish_limits(self, model: str, rpm: int, tpm: int) -> None: ...

    async def live_replicas(self) -> set[str]:
        return {self.replica_id}


class RedisCoordinator:
    shared = True

    def __init__(self, url: str, replica_id: str | None = None, prefix: str = "inf", ttl_s: float = 6.0,
                 refresh_s: float = 1.0, join_delay_s: float | None = None):
        self.url, self.prefix, self.ttl_s, self.refresh_s = url, prefix, ttl_s, refresh_s
        self.join_delay_s = 2.5 * refresh_s if join_delay_s is None else join_delay_s
        self.replica_id = replica_id or uuid.uuid4().hex[:12]
        self.replicas = [self.replica_id]
        self.dispatch_after = 0.0
        self._redis = None
        self._tasks: list[asyncio.Task] = []
        self._on_limits: LimitsCallback = lambda *a: None
        self._on_membership: Callable[[], None] = lambda: None

    # keys
    def _hb(self, rid: str) -> str:
        return f"{self.prefix}:replica:{rid}"

    @property
    def _limits_key(self) -> str:
        return f"{self.prefix}:limits"

    @property
    def _channel(self) -> str:
        return f"{self.prefix}:limits-changed"

    async def start(self, on_limits: LimitsCallback, on_membership: Callable[[], None]) -> None:
        import redis.asyncio as aioredis  # optional dependency
        self._redis = aioredis.from_url(self.url, decode_responses=True)
        self._on_limits, self._on_membership = on_limits, on_membership
        await self._heartbeat()
        await self._refresh_members(notify=False)
        self.dispatch_after = time.monotonic() + self.join_delay_s
        loop = asyncio.get_running_loop()
        self._tasks = [loop.create_task(self._heartbeat_loop()), loop.create_task(self._listen())]

    async def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        if self._redis:
            try:
                await self._redis.delete(self._hb(self.replica_id))  # leave at once, do not wait for the TTL
            except Exception:
                pass
            await self._redis.aclose()

    async def _heartbeat(self) -> None:
        await self._redis.set(self._hb(self.replica_id), str(time.time()), px=int(self.ttl_s * 1000))

    async def _refresh_members(self, notify: bool = True) -> None:
        prefix = self._hb("")
        keys = [k async for k in self._redis.scan_iter(match=prefix + "*", count=100)]
        ids = sorted(k[len(prefix):] for k in keys)
        if self.replica_id not in ids:
            ids = sorted(ids + [self.replica_id])
        if ids != self.replicas:
            log.info("replicas now %s", ids)
            self.replicas = ids
            if notify:
                self._on_membership()

    async def _heartbeat_loop(self) -> None:
        while True:
            try:
                await self._heartbeat()
                await self._refresh_members()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("redis heartbeat failed: %s", e)
            await asyncio.sleep(self.refresh_s)

    async def _listen(self) -> None:
        while True:
            try:
                pubsub = self._redis.pubsub()
                await pubsub.subscribe(self._channel)
                while True:
                    msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                    if msg and msg.get("type") == "message":
                        d = json.loads(msg["data"])
                        self._on_limits(d["model"], d["rpm"], d["tpm"])
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("redis subscription lost (%s); reconnecting", e)
                await asyncio.sleep(1.0)

    def share(self, total: int) -> int:
        n = len(self.replicas)
        rank = self.replicas.index(self.replica_id)
        return max(total // n + (1 if rank < total % n else 0), 1)

    async def get_limits(self) -> dict[str, tuple[int, int]]:
        raw = await self._redis.hgetall(self._limits_key)
        return {m: (json.loads(v)["rpm"], json.loads(v)["tpm"]) for m, v in raw.items()}

    async def publish_limits(self, model: str, rpm: int, tpm: int) -> None:
        payload = json.dumps({"model": model, "rpm": rpm, "tpm": tpm})
        await self._redis.hset(self._limits_key, model, payload)
        await self._redis.publish(self._channel, payload)

    async def live_replicas(self) -> set[str]:
        await self._refresh_members(notify=False)
        return set(self.replicas)
