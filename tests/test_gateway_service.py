import asyncio
import time

import httpx
import pytest
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from common.config import EngineConfig, GatewayConfig, ModelsConfig, ModelSpec
from gateway.app import create_app
from provider_sim.app import create_app as create_provider
from provider_sim.simulator import ProviderSimulator


def specs(**kw):
    base = dict(rpm=600_000, tpm=10**10, latency_ms_median=5, latency_sigma=0)
    return {"a": ModelSpec(**{**base, **kw}), "b": ModelSpec(**{**base, **kw})}


class Rig:
    """Gateway + provider (+ optional callback receiver) wired together in-process."""

    def __init__(self, models, engine=None, gateway=None, reject_first=0):
        self.sim = ProviderSimulator(models)
        provider_client = httpx.AsyncClient(transport=httpx.ASGITransport(app=create_provider(self.sim)), base_url="http://provider")
        self.received, self.attempts, self.reject_first = [], 0, reject_first
        sink = FastAPI()

        @sink.post("/cb")
        async def cb(request: Request):
            self.attempts += 1
            body = await request.json()
            if self.attempts <= self.reject_first:
                return JSONResponse({"no": 1}, status_code=503)
            self.received.append((time.monotonic(), body))
            return {"ok": True}

        cb_client = httpx.AsyncClient(transport=httpx.ASGITransport(app=sink), base_url="http://sink")
        cfg = ModelsConfig(models=models, engine=engine or EngineConfig(),
                           gateway=gateway or GatewayConfig(callback_backoff_base_s=0.05, callback_backoff_cap_s=0.2))
        self.app = create_app(cfg, provider_client, cb_client)
        self.gw = self.app.state.gw
        self.client = httpx.AsyncClient(transport=httpx.ASGITransport(app=self.app), base_url="http://gw")

    async def __aenter__(self):
        self.gw.start()
        return self

    async def __aexit__(self, *a):
        await self.gw.stop()

    async def wait(self, cond, timeout=20):
        end = time.monotonic() + timeout
        while not cond():
            assert time.monotonic() < end, "timed out"
            await asyncio.sleep(0.02)


async def test_single_request_lifecycle():
    async with Rig(specs()) as rig:
        r = await rig.client.post("/v1/requests", json={"request_id": "x1", "model": "a", "estimated_tokens": 500, "payload": {"p": 1}})
        assert r.status_code == 202 and r.json()["state"] == "queued"
        await rig.wait(lambda: rig.gw.registry.by_id["x1"].state == "succeeded")
        v = (await rig.client.get("/v1/requests/x1")).json()
        assert v["state"] == "succeeded" and v["attempts"] == 1 and v["latency_s"] > 0
        # idempotent resubmit returns the existing record
        again = await rig.client.post("/v1/requests", json={"request_id": "x1", "model": "a"})
        assert again.status_code == 200
        assert (await rig.client.post("/v1/requests", json={"model": "zzz"})).status_code == 404
        assert (await rig.client.get("/v1/requests/none")).status_code == 404


async def test_full_queue_rejects_with_429_and_everything_is_accounted():
    # limit of 60 rpm => at most ~58 admitted at once; queue of 20 holds the rest, the rest is rejected
    async with Rig(specs(rpm=60), engine=EngineConfig(queue_max=20, queue_ttl_s=1.0)) as rig:
        codes = [(await rig.client.post("/v1/requests", json={"request_id": f"r{i}", "model": "a"})).status_code for i in range(200)]
        assert codes.count(429) > 100 and codes.count(202) > 0
        await rig.wait(lambda: rig.gw.is_idle(), 30)
        states = (await rig.client.post("/v1/requests/status", json={"ids": [f"r{i}" for i in range(200)]})).json()["states"]
        assert set(states.values()) <= {"succeeded", "failed", "expired", "rejected"}
        assert sum(v == "rejected" for v in states.values()) == codes.count(429)
        assert len(states) == 200
        assert rig.sim.audit.violations("a", 60, 10**10)["rpm_ok"]


async def test_batch_callback_survives_rejections_and_reports_exactly_once():
    models = specs(transient_failure_rate=0.2, permanent_failure_rate=0.1)
    async with Rig(models, reject_first=2) as rig:
        items = [{"request_id": f"id{i}", "model": "ab"[i % 2], "estimated_tokens": 1000} for i in range(2000)]
        t = time.monotonic()
        r = await rig.client.post("/v1/batches", json={"requests": items, "callback_url": "http://sink/cb"})
        assert r.status_code == 202 and time.monotonic() - t < 1.0 and r.json()["total"] == 2000
        bid = r.json()["batch_id"]
        assert (await rig.client.get(f"/v1/batches/{bid}")).json()["status"] == "processing"
        await rig.wait(lambda: rig.received, 30)
        _, cb = rig.received[0]
        assert rig.attempts == 3  # two rejected, third accepted
        batch = rig.gw.batches[bid]
        assert batch.remaining == 0  # callback only after every request was final
        status = (await rig.client.get(f"/v1/batches/{bid}")).json()
        assert {k: cb[k] for k in ("batch_id", "status", "total", "succeeded", "failed", "expired")} == \
               {k: status[k] for k in ("batch_id", "status", "total", "succeeded", "failed", "expired")}
        assert cb["total"] == cb["succeeded"] + cb["failed"] == 2000 and cb["failed"] > 0
        assert cb["status"] == "completed_with_failures"
        await rig.wait(lambda: batch.callback["status"] == "delivered")
        ids, offset = [], 0
        while offset is not None:
            page = (await rig.client.get(f"/v1/batches/{bid}/results", params={"offset": offset, "limit": 700})).json()
            ids += [x["request_id"] for x in page["results"]]
            assert all(x["state"] in ("succeeded", "failed", "expired") for x in page["results"])
            offset = page["next_offset"]
        assert sorted(ids) == sorted(f"id{i}" for i in range(2000))  # every id exactly once


async def test_callback_gives_up_after_max_attempts_but_status_stays_queryable():
    gw_cfg = GatewayConfig(callback_max_attempts=3, callback_backoff_base_s=0.01, callback_backoff_cap_s=0.02)
    async with Rig(specs(), gateway=gw_cfg, reject_first=99) as rig:
        r = await rig.client.post("/v1/batches", json={"requests": [{"model": "a"}] * 5, "callback_url": "http://sink/cb"})
        bid = r.json()["batch_id"]
        await rig.wait(lambda: rig.gw.batches[bid].callback["status"] == "failed")
        v = (await rig.client.get(f"/v1/batches/{bid}")).json()
        assert v["status"] == "completed" and v["callback"]["attempts"] == 3


async def test_batch_validation():
    async with Rig(specs()) as rig:
        dup = await rig.client.post("/v1/batches", json={"requests": [{"request_id": "d", "model": "a"}] * 2})
        assert dup.status_code == 409
        bad = await rig.client.post("/v1/batches", json={"requests": [{"model": "nope"}]})
        assert bad.status_code == 404
        assert (await rig.client.post("/v1/batches", json={"requests": []})).status_code == 422


async def test_limits_change_at_runtime_through_api_and_models_are_independent():
    base = dict(tpm=10**10, latency_ms_median=5, latency_sigma=0)
    models = {"a": ModelSpec(rpm=3000, **base), "b": ModelSpec(rpm=600_000, **base)}
    async with Rig(models, engine=EngineConfig(queue_max=50_000, queue_ttl_s=120)) as rig:
        items = [{"model": m} for _ in range(4000) for m in "ab"]
        await rig.client.post("/v1/batches", json={"requests": items})
        await rig.wait(lambda: rig.gw.engine.limiters["a"].used_req >= 2500, 30)
        r = await rig.client.put("/admin/models/a/limits", json={"rpm": 300})
        assert r.json()["rpm"] == 300
        assert (await rig.client.get("/admin/models")).json()["a"]["limits"]["rpm"] == 300
        assert (await rig.client.put("/admin/models/a/limits", json={})).status_code == 422
        a_before = rig.sim.audit.max_60s("a")[0]
        # b is untouched and finishes all its work while a is held back by the reduced limit
        await rig.wait(lambda: rig.gw.sink.series["b"]["completed"].sum() == 4000, 30)
        await asyncio.sleep(0.5)
        assert rig.sim.audit.max_60s("a")[0] == a_before  # nothing more admitted for a: window still holds old traffic
        assert rig.gw.status()["limit_changes"][0]["model"] == "a"
