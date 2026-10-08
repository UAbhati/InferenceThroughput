"""Demo console: one command starts the provider and the gateway and serves a single page to drive and watch them.

    python -m loadgen.console [--models config/models.yaml] [--port 8080]

The page shows live throughput against each model's limit and lets you start/stop load, change limits while it
runs, send a batch with a callback, make the callback receiver refuse its first attempts, inject provider failures,
and (with DATABASE_URL set) kill and restart the gateway. Swagger for both services is linked from the page.
The console is a separate process, so none of this touches the gateway's own code path.
"""
from __future__ import annotations

import argparse
import asyncio
import random
import socket
import tempfile
import threading
import time
import webbrowser
from collections import deque
from contextlib import asynccontextmanager
from pathlib import Path

import aiohttp
import httpx
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from common.config import ModelsConfig
from common.scenario import Scenario
from loadgen.callback_sink import create_sink
from loadgen.http_run import Stack, auth_headers, free_port


class LoadIn(BaseModel):
    rate_per_s: float = Field(gt=0, le=20000)
    token_size: int = Field(default=1000, gt=0)
    mix: dict[str, float]


class LimitsIn(BaseModel):
    model: str
    rpm: int | None = Field(default=None, gt=0)
    tpm: int | None = Field(default=None, gt=0)


class BatchIn(BaseModel):
    total: int = Field(default=2000, gt=0, le=100_000)
    token_size: int = 1000
    mix: dict[str, float]
    reject_first_callbacks: int = Field(default=0, ge=0)


class BehaviorIn(BaseModel):
    model: str
    transient_failure_rate: float | None = Field(default=None, ge=0, le=1)
    permanent_failure_rate: float | None = Field(default=None, ge=0, le=1)
    latency_ms_median: float | None = Field(default=None, gt=0)


def port_free(port: int) -> bool:
    with socket.socket() as s:
        return s.connect_ex(("127.0.0.1", port)) != 0


class LiveLoad:
    """Open-loop load whose rate can be changed while it runs."""

    def __init__(self, session: aiohttp.ClientSession):
        self.session = session
        self.running = False
        self.rate = 0.0
        self.token_size = 1000
        self.mix: dict[str, float] = {}
        self.sent = self.accepted = self.rejected = self.errors = 0
        self.ack: deque[float] = deque(maxlen=2000)
        self._task: asyncio.Task | None = None
        self._tasks: set[asyncio.Task] = set()
        self._run_id = 0
        self._rng = random.Random()

    def start(self, rate: float, token_size: int, mix: dict[str, float]) -> None:
        self.rate, self.token_size, self.mix = rate, token_size, mix
        if not self.running:
            self.running = True
            self._run_id += 1
            self._task = asyncio.get_running_loop().create_task(self._loop())

    async def stop(self) -> None:
        self.running = False
        if self._task:
            await asyncio.gather(self._task, return_exceptions=True)

    async def _loop(self) -> None:
        n, acc, last = 0, 0.0, time.perf_counter()
        while self.running:
            now = time.perf_counter()
            acc += self.rate * (now - last)
            last = now
            models, weights = list(self.mix), list(self.mix.values())
            for _ in range(int(acc)):
                body = {"request_id": f"ui{self._run_id}-{n}", "model": self._rng.choices(models, weights)[0], "estimated_tokens": self.token_size}
                n += 1
                t = asyncio.get_running_loop().create_task(self._send(body))
                self._tasks.add(t)
                t.add_done_callback(self._tasks.discard)
            acc -= int(acc)
            await asyncio.sleep(0.01)

    async def _send(self, body: dict) -> None:
        self.sent += 1
        t = time.perf_counter()
        try:
            async with self.session.post("/v1/requests", json=body) as r:
                await r.read()
                self.ack.append(time.perf_counter() - t)
                if r.status in (200, 202):
                    self.accepted += 1
                elif r.status == 429:
                    self.rejected += 1
                else:
                    self.errors += 1
        except (aiohttp.ClientError, asyncio.TimeoutError, OSError):
            self.errors += 1

    def view(self) -> dict:
        ack = sorted(self.ack)
        q = lambda p: round(ack[min(int(len(ack) * p), len(ack) - 1)] * 1000, 1) if ack else None
        return {"running": self.running, "rate_per_s": self.rate, "token_size": self.token_size, "mix": self.mix,
                "sent": self.sent, "accepted": self.accepted, "rejected_429": self.rejected, "errors": self.errors,
                "ack_ms_p50": q(0.5), "ack_ms_p99": q(0.99)}


def create_console(models_path: str, port: int, out: Path | None = None) -> FastAPI:
    cfg = ModelsConfig.load(models_path)
    out = out or Path(tempfile.mkdtemp(prefix="console-"))
    scn = Scenario(name="console", duration_s=1, models=cfg.models, engine=cfg.engine, gateway=cfg.gateway)
    sink = create_sink(0)
    ctx: dict = {"limits": {m: (s.rpm, s.tpm) for m, s in cfg.models.items()},
                 "behavior": {m: {"transient_failure_rate": s.transient_failure_rate, "permanent_failure_rate": s.permanent_failure_rate,
                                  "latency_ms_median": s.latency_ms_median} for m, s in cfg.models.items()},
                 "batches": [], "gateway_up": True, "batch_seq": 0}

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        stack = Stack(scn, out, gateway_port=8000 if port_free(8000) else None, provider_port=8001 if port_free(8001) else None)
        await asyncio.to_thread(stack.start)
        ctx["stack"] = stack
        gw_url, pv_url = stack.urls
        ctx["gw"] = httpx.AsyncClient(base_url=gw_url, timeout=30, headers=auth_headers())
        ctx["pv"] = httpx.AsyncClient(base_url=pv_url, timeout=30)
        ctx["session"] = aiohttp.ClientSession(gw_url, timeout=aiohttp.ClientTimeout(total=30), connector=aiohttp.TCPConnector(limit=2000))
        ctx["load"] = LiveLoad(ctx["session"])
        print(f"\n  Console:          http://127.0.0.1:{port}\n  Gateway Swagger:  {gw_url}/docs\n  Provider Swagger: {pv_url}/docs\n")
        yield
        await ctx["load"].stop()
        await ctx["session"].close()
        await ctx["gw"].aclose()
        await ctx["pv"].aclose()
        await asyncio.to_thread(stack.stop)

    app = FastAPI(title="Demo console", lifespan=lifespan)
    app.mount("/callback-app", sink)

    @app.get("/", response_class=HTMLResponse, include_in_schema=False)
    async def page():
        return (Path(__file__).parent / "console.html").read_text()

    @app.get("/api/state")
    async def state():
        stack: Stack = ctx["stack"]
        gw_url, pv_url = stack.urls
        out = {"links": {"gateway_docs": f"{gw_url}/docs", "provider_docs": f"{pv_url}/docs"}, "load": ctx["load"].view(),
               "limits": ctx["limits"], "behavior": ctx["behavior"], "db": bool(__import__("os").environ.get("DATABASE_URL")),
               "redis": bool(__import__("os").environ.get("REDIS_URL")), "gateway_up": False, "now": time.time()}
        try:
            st = (await ctx["gw"].get("/admin/status")).json()
            out.update(gateway_up=True, replica=st["replica_id"], replicas=st["replicas"], models=st["models"],
                       persistence=st["persistence"])
            audit = (await ctx["pv"].get("/admin/audit")).json()
            for m, a in audit.items():
                out["models"][m]["provider"] = {"counts": a["counts"], "max_rpm_60s": a["max_rpm_60s"], "limit_rpm": a["limits"]["rpm"]}
            batches = []
            for b in ctx["batches"][-6:]:
                v = (await ctx["gw"].get(f"/v1/batches/{b['id']}")).json()
                batches.append({**b, "status": v["status"], "total": v["total"], "succeeded": v["succeeded"], "failed": v["failed"],
                                "expired": v["expired"], "pending": v["pending"], "callback": v["callback"]})
            out["batches"] = batches
        except (httpx.HTTPError, KeyError, ValueError):
            out["gateway_up"] = False
            out["batches"] = ctx["batches"][-6:]
        out["callbacks"] = [{"t": a["t"], "batch_id": a["batch_id"], "status_code": a["status_code"],
                             "total": a["body"].get("total"), "succeeded": a["body"].get("succeeded"), "failed": a["body"].get("failed")}
                            for a in sink.state.attempts[-12:]]
        out["callback_rejects_pending"] = max(sink.state.reject_first - len(sink.state.attempts), 0)
        return out

    @app.post("/api/load/start")
    async def load_start(body: LoadIn):
        ctx["load"].start(body.rate_per_s, body.token_size, body.mix)
        return ctx["load"].view()

    @app.post("/api/load/stop")
    async def load_stop():
        await ctx["load"].stop()
        return ctx["load"].view()

    @app.put("/api/limits")
    async def limits(body: LimitsIn):
        if body.model not in ctx["limits"]:
            raise HTTPException(404, "unknown model")
        cur_rpm, cur_tpm = ctx["limits"][body.model]
        rpm, tpm = body.rpm or cur_rpm, body.tpm or cur_tpm
        payload = {"rpm": rpm, "tpm": tpm}
        increase = rpm > cur_rpm or tpm > cur_tpm
        # raise the provider first, lower the gateway first: the gateway never gets more than the provider accepts
        for client in ((ctx["pv"], ctx["gw"]) if increase else (ctx["gw"], ctx["pv"])):
            try:
                r = await client.put(f"/admin/models/{body.model}/limits", json=payload)
                r.raise_for_status()
            except httpx.HTTPError as e:
                raise HTTPException(502, f"could not apply limit: {e}")
        ctx["limits"][body.model] = (rpm, tpm)
        return {"model": body.model, **payload}

    @app.put("/api/provider/behavior")
    async def behavior(body: BehaviorIn):
        r = await ctx["pv"].put(f"/admin/models/{body.model}/behavior", json=body.model_dump(exclude={"model"}))
        r.raise_for_status()
        ctx["behavior"][body.model].update({k: v for k, v in body.model_dump(exclude={"model"}).items() if v is not None})
        return ctx["behavior"][body.model]

    @app.post("/api/batch")
    async def batch(body: BatchIn):
        ctx["batch_seq"] += 1
        tag = f"b{ctx['batch_seq']}-{int(time.time()) % 100000}"
        rng, models, weights = random.Random(), list(body.mix), list(body.mix.values())
        items = [{"request_id": f"{tag}-{i}", "model": rng.choices(models, weights)[0], "estimated_tokens": body.token_size} for i in range(body.total)]
        if body.reject_first_callbacks:
            sink.state.reject_first = len(sink.state.attempts) + body.reject_first_callbacks
        t = time.perf_counter()
        r = await ctx["gw"].post("/v1/batches", json={"requests": items, "callback_url": f"http://127.0.0.1:{port}/callback-app/callback"})
        ack_ms = round((time.perf_counter() - t) * 1000)
        if r.status_code != 202:
            raise HTTPException(r.status_code, r.text)
        info = {"id": r.json()["batch_id"], "ack_ms": ack_ms, "sent_at": time.time()}
        ctx["batches"].append(info)
        return info

    @app.post("/api/callback/reject")
    async def callback_reject(count: int = 2):
        sink.state.reject_first = len(sink.state.attempts) + count
        return {"next_attempts_refused": count}

    @app.post("/api/gateway/kill")
    async def gateway_kill():
        if not ctx["stack"].gateway or ctx["stack"].gateway.poll() is not None:
            raise HTTPException(409, "gateway is not running")
        await ctx["load"].stop()
        await asyncio.to_thread(ctx["stack"].kill_gateway)
        return {"killed": True}

    @app.post("/api/gateway/restart")
    async def gateway_restart():
        if ctx["stack"].gateway and ctx["stack"].gateway.poll() is None:
            raise HTTPException(409, "gateway is already running")
        await asyncio.to_thread(ctx["stack"].start_gateway)
        return {"restarted": True}

    @app.post("/api/reset")
    async def reset():
        await ctx["load"].stop()
        r = await ctx["gw"].post("/admin/reset")
        if r.status_code == 409:
            raise HTTPException(409, "work is still in progress; stop the load and wait until queues are empty")
        await ctx["pv"].post("/admin/reset")
        ctx["batches"].clear()
        sink.state.attempts.clear()
        sink.state.reject_first = 0
        ctx["load"].sent = ctx["load"].accepted = ctx["load"].rejected = ctx["load"].errors = 0
        return {"ok": True}

    return app


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--models", default="config/models.yaml")
    ap.add_argument("--port", type=int, default=8080)
    ap.add_argument("--no-browser", action="store_true")
    args = ap.parse_args()
    app = create_console(args.models, args.port)
    if not args.no_browser:
        threading.Timer(2.5, lambda: webbrowser.open(f"http://127.0.0.1:{args.port}")).start()
    uvicorn.run(app, host="127.0.0.1", port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
