"""HTTP API of the gateway.

    uvicorn gateway.app:app --port 8000
    env: GATEWAY_MODELS (default config/models.yaml), PROVIDER_URL (default http://127.0.0.1:8001),
         GATEWAY_PUBLIC_URL (used in results_url),
         DATABASE_URL (optional: Postgres, durable batches/requests/limits + crash recovery),
         REDIS_URL (optional: coordination between several gateway replicas)
"""
from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import AnyHttpUrl, BaseModel, Field

from common.config import ModelsConfig
from gateway.coordination import LocalCoordinator, RedisCoordinator
from gateway.http_provider import AiohttpCaller
from gateway.persistence import NullPersistence, Persistence
from gateway.records import REJECTED
from gateway.service import Conflict, Gateway, Unavailable


class RequestIn(BaseModel):
    request_id: str | None = Field(default=None, max_length=128)
    model: str
    payload: Any = None
    estimated_tokens: int = Field(default=1000, gt=0)


class BatchIn(BaseModel):
    requests: list[RequestIn] = Field(min_length=1)
    callback_url: AnyHttpUrl | None = None


class LimitsIn(BaseModel):
    rpm: int | None = Field(default=None, gt=0)
    tpm: int | None = Field(default=None, gt=0)


class IdsIn(BaseModel):
    ids: list[str]


def create_app(cfg: ModelsConfig | None = None, provider_caller=None,
               callback_client: httpx.AsyncClient | None = None, persistence=None, coordinator=None) -> FastAPI:
    cfg = cfg or ModelsConfig.load(os.environ.get("GATEWAY_MODELS", "config/models.yaml"))
    if provider_caller is None:
        provider_caller = AiohttpCaller(os.environ.get("PROVIDER_URL", "http://127.0.0.1:8001"), cfg.gateway.provider_timeout_s)
    if persistence is None:
        persistence = Persistence(os.environ["DATABASE_URL"]) if os.environ.get("DATABASE_URL") else NullPersistence()
    if coordinator is None:
        coordinator = RedisCoordinator(os.environ["REDIS_URL"]) if os.environ.get("REDIS_URL") else LocalCoordinator()
    gw = Gateway(cfg, provider_caller, callback_client, os.environ.get("GATEWAY_PUBLIC_URL", "http://127.0.0.1:8000"),
                 persistence, coordinator)

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        await gw.start()
        yield
        await gw.stop()

    app = FastAPI(title="Inference gateway", lifespan=lifespan)
    app.state.gw = gw

    def need_model(model: str) -> None:
        if model not in gw.specs:
            raise HTTPException(404, f"unknown model '{model}'")

    # ---- requests ----
    @app.post("/v1/requests", status_code=202)
    async def submit(body: RequestIn):
        need_model(body.model)
        rid, state, created = await gw.submit_single(body.model, body.estimated_tokens, body.payload, body.request_id)
        view = {"request_id": rid, "state": state}
        if state == REJECTED:
            return JSONResponse({**view, "error": "queue_full"}, status_code=429, headers={"Retry-After": "1"})
        return JSONResponse(view, status_code=202 if created else 200)

    @app.get("/v1/requests/{request_id}")
    async def get_request(request_id: str):
        view = await gw.request_view(request_id)
        if view is None:
            raise HTTPException(404, "unknown request_id")
        return view

    @app.post("/v1/requests/status")
    async def request_states(body: IdsIn):
        """Look up many ids at once (used to verify that every submitted request is accounted for)."""
        return {"states": await gw.request_states(body.ids)}

    # ---- batches ----
    @app.post("/v1/batches", status_code=202)
    async def submit_batch(body: BatchIn):
        if len(body.requests) > cfg.gateway.batch_max_requests:
            raise HTTPException(413, f"batch larger than {cfg.gateway.batch_max_requests} requests")
        for r in body.requests:
            need_model(r.model)
        try:
            batch = await gw.create_batch([r.model_dump() for r in body.requests], str(body.callback_url) if body.callback_url else None)
        except Conflict as e:
            raise HTTPException(409, str(e))
        except Unavailable as e:
            raise HTTPException(503, str(e))
        return {"batch_id": batch.id, "status": batch.status, "total": batch.total,
                "status_url": f"/v1/batches/{batch.id}", "results_url": f"/v1/batches/{batch.id}/results"}

    @app.get("/v1/batches/{batch_id}")
    async def get_batch(batch_id: str):
        view = await gw.get_batch_view(batch_id)
        if view is None:
            raise HTTPException(404, "unknown batch_id")
        return view

    @app.get("/v1/batches/{batch_id}/results")
    async def batch_results(batch_id: str, offset: int = Query(0, ge=0), limit: int = Query(1000, ge=1, le=50_000)):
        page = await gw.get_batch_results(batch_id, offset, limit)
        if page is None:
            raise HTTPException(404, "unknown batch_id")
        return page

    # ---- admin ----
    @app.get("/admin/models")
    async def models():
        return gw.status()["models"]

    @app.put("/admin/models/{model}/limits")
    async def set_limits(model: str, body: LimitsIn):
        need_model(model)
        if body.rpm is None and body.tpm is None:
            raise HTTPException(422, "give rpm and/or tpm")
        return await gw.set_limits(model, body.rpm, body.tpm)

    @app.get("/admin/status")
    async def status():
        return gw.status()

    @app.get("/admin/stats")
    async def stats():
        return gw.stats()

    @app.post("/admin/reset")
    async def reset():
        try:
            await gw.reset()
        except Conflict as e:
            raise HTTPException(409, str(e))
        return {"ok": True}

    @app.get("/healthz")
    async def healthz():
        return {"ok": True}

    return app


app = create_app() if os.environ.get("GATEWAY_AUTOSTART", "1") == "1" and os.path.exists(os.environ.get("GATEWAY_MODELS", "config/models.yaml")) else None
