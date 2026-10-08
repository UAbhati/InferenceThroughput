"""Needs Postgres and Redis (docker compose up -d). Skipped when they are not reachable."""
import asyncio
import os
import socket
import time
import uuid
from urllib.parse import urlparse

import httpx
import pytest
from fastapi import FastAPI, Request

from common.config import EngineConfig, GatewayConfig, ModelsConfig, ModelSpec
from gateway.app import create_app
from gateway.coordination import RedisCoordinator
from gateway.http_provider import HttpxCaller
from gateway.persistence import Persistence
from provider_sim.app import create_app as create_provider
from provider_sim.simulator import ProviderSimulator

PG = os.environ.get("TEST_DATABASE_URL", "postgresql://inference:inference@127.0.0.1:55432/inference")
RD = os.environ.get("TEST_REDIS_URL", "redis://127.0.0.1:56379/0")


def reachable(url: str) -> bool:
    u = urlparse(url)
    try:
        with socket.create_connection((u.hostname, u.port), timeout=0.5):
            return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(not (reachable(PG) and reachable(RD)), reason="Postgres/Redis not running (docker compose up -d)")


def specs(rpm=600_000, latency_ms=5):
    base = dict(rpm=rpm, tpm=10**10, latency_ms_median=latency_ms, latency_sigma=0)
    return {"a": ModelSpec(**base), "b": ModelSpec(**base)}


class World:
    """One provider, one callback receiver, and any number of gateways sharing Postgres (and Redis)."""

    def __init__(self, models, engine=None, redis=False):
        self.models, self.use_redis = models, redis
        self.engine = engine or EngineConfig(queue_ttl_s=60)
        self.prefix = "t" + uuid.uuid4().hex[:8]
        self.sim = ProviderSimulator(models)
        self.provider_app = create_provider(self.sim)
        self.callbacks: list[dict] = []
        sink = FastAPI()

        @sink.post("/cb")
        async def cb(request: Request):
            self.callbacks.append(await request.json())
            return {"ok": True}

        self.sink_app = sink
        self.gateways: list = []

    async def gateway(self, **kw):
        cb_client = httpx.AsyncClient(transport=httpx.ASGITransport(app=self.sink_app), base_url="http://sink")
        pv = HttpxCaller(httpx.AsyncClient(transport=httpx.ASGITransport(app=self.provider_app), base_url="http://provider"))
        coord = RedisCoordinator(RD, prefix=self.prefix, ttl_s=1.0, refresh_s=0.2) if self.use_redis else None
        cfg = ModelsConfig(models=self.models, engine=self.engine,
                           gateway=GatewayConfig(callback_backoff_base_s=0.05, callback_backoff_cap_s=0.2, **kw))
        app = create_app(cfg, pv, cb_client, persistence=Persistence(PG, flush_interval_s=0.05), coordinator=coord)
        gw = app.state.gw
        await gw.start()
        client = httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://gw")
        self.gateways.append((gw, client))
        return gw, client

    async def close(self):
        for gw, _ in self.gateways:
            if not getattr(gw, "_crashed", False):
                await gw.stop()


async def crash(gw):
    """Like kill -9: stop everything without flushing writes or releasing the heartbeat."""
    gw._crashed = True
    tasks = list(gw._tasks) + list(gw._callback_tasks) + list(gw.provider._tasks)
    if getattr(gw.persistence, "_task", None):
        tasks.append(gw.persistence._task)
    tasks += list(getattr(gw.coordinator, "_tasks", []))
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await gw.persistence._pool.close()
    if getattr(gw.coordinator, "_redis", None):
        await gw.coordinator._redis.aclose()


async def wait(cond, timeout=30):
    end = time.monotonic() + timeout
    while not cond():
        assert time.monotonic() < end, "timed out"
        await asyncio.sleep(0.05)


@pytest.fixture(autouse=True)
async def clean_db():
    p = Persistence(PG)
    await p.start()
    await p.truncate()
    async with p._pool.acquire() as c:
        await c.execute("TRUNCATE model_limits")
    await p.stop()


async def test_batch_is_stored_before_it_is_acknowledged():
    w = World(specs())
    try:
        gw, client = await w.gateway()
        items = [{"request_id": f"d{i}", "model": "a"} for i in range(500)]
        r = await client.post("/v1/batches", json={"requests": items})
        assert r.status_code == 202
        # no waiting for a flush: the rows must already be there
        async with gw.persistence._pool.acquire() as c:
            assert await c.fetchval("SELECT count(*) FROM requests WHERE batch_id = $1", r.json()["batch_id"]) == 500
            assert await c.fetchval("SELECT total FROM batches WHERE id = $1", r.json()["batch_id"]) == 500
        dup = await client.post("/v1/batches", json={"requests": [{"request_id": "d1", "model": "a"}]})
        assert dup.status_code == 409  # id already in the database
    finally:
        await w.close()


async def test_crash_midway_is_recovered_exactly_once_and_callback_delivered():
    w = World(specs(rpm=12_000))  # 200/s per model: the batch takes a few seconds, long enough to crash in the middle
    try:
        gw1, c1 = await w.gateway()
        items = [{"request_id": f"c{i}", "model": "ab"[i % 2]} for i in range(1200)]
        r = await c1.post("/v1/batches", json={"requests": items, "callback_url": "http://sink/cb"})
        bid = r.json()["batch_id"]
        await wait(lambda: gw1.batches[bid].succeeded + gw1.batches[bid].failed >= 250)
        done_before = gw1.batches[bid].succeeded
        assert 250 <= done_before < 1200
        await crash(gw1)

        gw2, c2 = await w.gateway()   # a new replica, same database
        await wait(lambda: bid in gw2.batches, 10)
        await wait(lambda: w.callbacks, 60)
        assert len(w.callbacks) == 1 and w.callbacks[0]["batch_id"] == bid
        cb = w.callbacks[0]
        assert cb["total"] == 1200 and cb["succeeded"] == 1200 and cb["status"] == "completed"
        ids, offset = [], 0
        while offset is not None:
            page = (await c2.get(f"/v1/batches/{bid}/results", params={"offset": offset, "limit": 500})).json()
            ids += [x["request_id"] for x in page["results"]]
            assert all(x["state"] == "succeeded" for x in page["results"])
            offset = page["next_offset"]
        assert sorted(ids) == sorted(i["request_id"] for i in items)
        await wait(lambda: gw2.batches[bid].callback["status"] == "delivered")
    finally:
        await w.close()


async def test_finished_work_is_served_from_the_database_after_restart():
    w = World(specs())
    try:
        gw1, c1 = await w.gateway()
        await c1.post("/v1/requests", json={"request_id": "single-1", "model": "a", "estimated_tokens": 700})
        r = await c1.post("/v1/batches", json={"requests": [{"request_id": f"h{i}", "model": "b"} for i in range(50)]})
        bid = r.json()["batch_id"]
        await wait(lambda: gw1.is_idle())
        await gw1.stop()

        gw2, c2 = await w.gateway()
        assert "single-1" not in gw2.registry.by_id and bid not in gw2.batches  # nothing in memory
        v = (await c2.get("/v1/requests/single-1")).json()
        assert v["state"] == "succeeded" and v["estimated_tokens"] == 700
        b = (await c2.get(f"/v1/batches/{bid}")).json()
        assert b["status"] == "completed" and b["succeeded"] == 50 and b["source"] == "database"
        page = (await c2.get(f"/v1/batches/{bid}/results", params={"limit": 20})).json()
        assert len(page["results"]) == 20 and page["next_offset"] == 20 and page["results"][0]["request_id"] == "h0"
        # re-submitting a finished id is idempotent across the restart: nothing new is queued
        again = await c2.post("/v1/requests", json={"request_id": "single-1", "model": "a"})
        assert again.status_code == 200 and again.json()["state"] == "succeeded"
        states = (await c2.post("/v1/requests/status", json={"ids": ["single-1", "h3", "nope"]})).json()["states"]
        assert states == {"single-1": "succeeded", "h3": "succeeded", "nope": "unknown"}
        assert (await c2.get("/v1/requests/nope")).status_code == 404
    finally:
        await w.close()


async def test_open_single_requests_survive_a_crash():
    w = World(specs(rpm=600))  # 10/s: most of 60 requests are still queued when we crash
    try:
        gw1, c1 = await w.gateway()
        for i in range(60):
            assert (await c1.post("/v1/requests", json={"request_id": f"s{i}", "model": "a"})).status_code == 202
        await gw1.persistence.flush()
        await crash(gw1)
        gw2, c2 = await w.gateway()
        await wait(lambda: gw2.is_idle() and gw2.sink.series["a"]["completed"].sum() > 0, 30)
        states = (await c2.post("/v1/requests/status", json={"ids": [f"s{i}" for i in range(60)]})).json()["states"]
        assert set(states.values()) <= {"succeeded", "expired"} and "unknown" not in states.values()
    finally:
        await w.close()


async def test_runtime_limit_changes_survive_a_restart():
    w = World(specs(rpm=30_000))
    try:
        gw1, c1 = await w.gateway()
        assert (await c1.put("/admin/models/a/limits", json={"rpm": 5_000})).status_code == 200
        await gw1.stop()
        gw2, c2 = await w.gateway()
        m = (await c2.get("/admin/models")).json()
        assert m["a"]["limits"]["rpm"] == 5_000 and m["b"]["limits"]["rpm"] == 30_000
        assert gw2.engine.limiters["a"].rpm == 5_000  # (headroom 1.0 in this config)
    finally:
        await w.close()


async def test_two_replicas_split_the_limit_and_share_changes_through_redis():
    w = World(specs(rpm=30_000), redis=True)
    try:
        gw1, c1 = await w.gateway()
        gw2, c2 = await w.gateway()
        await wait(lambda: len(gw1.coordinator.replicas) == 2 and len(gw2.coordinator.replicas) == 2, 10)
        gw1._apply_limits(); gw2._apply_limits()
        assert gw1.engine.limiters["a"].rpm + gw2.engine.limiters["a"].rpm == 30_000
        # a change made through one replica reaches the other, and each applies its own share
        await c1.put("/admin/models/a/limits", json={"rpm": 5_001})
        await wait(lambda: gw2.specs["a"].rpm == 5_001, 5)
        assert gw1.engine.limiters["a"].rpm + gw2.engine.limiters["a"].rpm == 5_001
        assert (await c2.get("/admin/models")).json()["a"]["this_replica_share"]["rpm"] in (2500, 2501)
        # when one replica leaves, the other takes over the whole limit
        await gw2.stop()
        await wait(lambda: len(gw1.coordinator.replicas) == 1, 10)
        assert gw1.engine.limiters["a"].rpm == 5_001
    finally:
        await w.close()


async def test_two_replicas_together_never_exceed_the_provider_limit():
    w = World(specs(rpm=1_200), engine=EngineConfig(queue_ttl_s=60, burst_s=0.25))
    w.use_redis = True
    try:
        gw1, c1 = await w.gateway()
        gw2, c2 = await w.gateway()
        await wait(lambda: len(gw1.coordinator.replicas) == 2 and len(gw2.coordinator.replicas) == 2, 10)
        await asyncio.sleep(1.0)  # both past their join delay
        for c in (c1, c2):
            await c.post("/v1/batches", json={"requests": [{"model": "a"} for _ in range(400)]})
        await asyncio.sleep(12)
        counts = w.sim.counts["a"]
        assert counts["rate_limited"] == 0, counts   # the provider never had to push back
        assert w.sim.audit.violations("a", 1_200, 10**10)["rpm_ok"]
        assert gw1.sink.series["a"]["completed"].sum() > 50 and gw2.sink.series["a"]["completed"].sum() > 50  # both worked
    finally:
        await w.close()


async def test_survivor_adopts_work_of_a_replica_that_died():
    w = World(specs(rpm=6_000), redis=True)
    try:
        gw1, c1 = await w.gateway()
        gw2, c2 = await w.gateway()
        await wait(lambda: len(gw1.coordinator.replicas) == 2, 10)
        await asyncio.sleep(1.0)
        items = [{"request_id": f"x{i}", "model": "a"} for i in range(400)]
        r = await c1.post("/v1/batches", json={"requests": items, "callback_url": "http://sink/cb"})
        bid = r.json()["batch_id"]
        await wait(lambda: gw1.batches[bid].succeeded >= 30, 20)
        await crash(gw1)   # heartbeat key expires after ~1 s, then the janitor of gw2 adopts the batch
        await wait(lambda: bid in gw2.batches, 20)
        await wait(lambda: w.callbacks, 90)
        assert w.callbacks[0]["total"] == 400 and w.callbacks[0]["succeeded"] == 400
        ids = [x["request_id"] for x in (await c2.get(f"/v1/batches/{bid}/results", params={"limit": 1000})).json()["results"]]
        assert sorted(ids) == sorted(i["request_id"] for i in items)
    finally:
        await w.close()
