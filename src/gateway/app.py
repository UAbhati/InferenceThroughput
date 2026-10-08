"""HTTP API of the gateway.

    uvicorn gateway.app:app --port 8000
    env: GATEWAY_MODELS (default config/models.yaml), PROVIDER_URL (default http://127.0.0.1:8001),
         GATEWAY_PUBLIC_URL (used in results_url),
         DATABASE_URL (optional: Postgres, durable batches/requests/limits + crash recovery),
         REDIS_URL (optional: coordination between several gateway replicas),
         ADMIN_TOKEN (optional: when set, /admin/* needs "Authorization: Bearer <token>")
"""
from __future__ import annotations

import hashlib
import hmac
import json
import os
from contextlib import asynccontextmanager
from typing import Any
from urllib.parse import urlparse

import httpx
from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import AnyHttpUrl, BaseModel, Field

from common.config import ModelsConfig
from gateway.coordination import LocalCoordinator, RedisCoordinator
from gateway.http_provider import AiohttpCaller
from gateway.persistence import NullPersistence, Persistence
from gateway.records import REJECTED
from gateway.service import Conflict, Gateway, IdempotencyMismatch, TooLarge, Unavailable


class RequestIn(BaseModel):
    request_id: str | None = Field(default=None, max_length=128, pattern=r"^[A-Za-z0-9._:@/-]+$",
                                   description="your own id (idempotent); generated if omitted")
    model: str = Field(examples=["model-a"])
    payload: Any = Field(default=None, examples=[{"prompt": "Say hello"}])
    estimated_tokens: int = Field(default=1000, gt=0, le=10_000_000, description="used for the tokens-per-minute limit")


class BatchIn(BaseModel):
    requests: list[RequestIn] = Field(min_length=1)
    callback_url: AnyHttpUrl | None = Field(default=None, description="called once every request in the batch is final",
                                            examples=["http://127.0.0.1:8082/callback-app/callback"])

    model_config = {"json_schema_extra": {"examples": [{
        "requests": [{"request_id": "demo-1", "model": "model-a", "estimated_tokens": 1000, "payload": {"prompt": "hi"}},
                     {"request_id": "demo-2", "model": "model-b", "estimated_tokens": 1500}],
        "callback_url": "http://127.0.0.1:8082/callback-app/callback"}]}}


class LimitsIn(BaseModel):
    rpm: int | None = Field(default=None, gt=0, examples=[5000], description="requests per minute (total, across replicas)")
    tpm: int | None = Field(default=None, gt=0, examples=[10000000], description="tokens per minute (total, across replicas)")


class IdsIn(BaseModel):
    ids: list[str] = Field(max_length=10_000)


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
    admin_token = os.environ.get("ADMIN_TOKEN")

    async def require_admin(authorization: str | None = Header(default=None, include_in_schema=False)) -> None:
        if admin_token and not hmac.compare_digest((authorization or "").encode(), f"Bearer {admin_token}".encode()):
            raise HTTPException(401, "admin endpoints need 'Authorization: Bearer <ADMIN_TOKEN>'")
    admin = [Depends(require_admin)]

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        await gw.start()
        yield
        await gw.stop()

    app = FastAPI(
        title="Inference gateway",
        description="Accepts single and batch inference requests and sends them to the provider as fast as each model's "
                    "RPM/TPM limit allows, never faster. Excess work waits in a bounded per-model queue, expires after a TTL, "
                    "or is rejected with 429. Limits can be changed while running.",
        openapi_tags=[{"name": "requests", "description": "Submit one request and look at its state."},
                      {"name": "batches", "description": "Submit many at once; get a callback when all are final."},
                      {"name": "admin", "description": "Limits, queues, statistics."}],
        lifespan=lifespan)
    app.state.gw = gw

    def need_model(model: str) -> None:
        if model not in gw.specs:
            raise HTTPException(404, f"unknown model '{model}'")

    def check_payload(r: RequestIn) -> None:
        if r.payload is not None and len(json.dumps(r.payload)) > cfg.gateway.max_payload_bytes:
            raise HTTPException(422, f"payload larger than {cfg.gateway.max_payload_bytes:,} bytes")

    def too_large(e: TooLarge) -> HTTPException:
        return HTTPException(422, {"error": "request_too_large", "message": str(e), "request_id": e.request_id, "model": e.model,
                                   "estimated_tokens": e.tokens, "tokens_per_minute_limit": e.limit})

    # ---- requests ----
    @app.post("/v1/requests", status_code=202, tags=["requests"], summary="Submit one request (202 queued, 200 existing id, 429 queue full)")
    async def submit(body: RequestIn):
        need_model(body.model)
        check_payload(body)
        try:
            rid, state, created = await gw.submit_single(body.model, body.estimated_tokens, body.payload, body.request_id)
        except TooLarge as e:
            raise too_large(e)
        view = {"request_id": rid, "state": state}
        if state == REJECTED:
            return JSONResponse({**view, "error": "queue_full"}, status_code=429, headers={"Retry-After": "1"})
        return JSONResponse(view, status_code=202 if created else 200)

    @app.get("/v1/requests/{request_id}", tags=["requests"], summary="State of one request")
    async def get_request(request_id: str):
        view = await gw.request_view(request_id)
        if view is None:
            raise HTTPException(404, "unknown request_id")
        return view

    @app.post("/v1/requests/status", tags=["requests"], summary="State of many ids at once")
    async def request_states(body: IdsIn):
        """Look up many ids at once (used to verify that every submitted request is accounted for)."""
        return {"states": await gw.request_states(body.ids)}

    # ---- batches ----
    @app.post("/v1/batches", status_code=202, tags=["batches"], summary="Submit a batch; returns at once with a batch_id")
    async def submit_batch(body: BatchIn, idempotency_key: str | None = Header(default=None, max_length=128,
                           description="repeat the call with the same key (and body) to get the same batch back instead of a second one")):
        if len(body.requests) > cfg.gateway.batch_max_requests:
            raise HTTPException(413, f"batch larger than {cfg.gateway.batch_max_requests} requests")
        for r in body.requests:
            need_model(r.model)
            check_payload(r)
        url = str(body.callback_url) if body.callback_url else None
        allowed = cfg.gateway.callback_allowed_hosts
        if url and allowed is not None and urlparse(url).hostname not in allowed:
            raise HTTPException(422, f"callback host not allowed; allowed hosts: {allowed}")
        items = [r.model_dump() for r in body.requests]
        digest = hashlib.sha256(json.dumps([items, url], sort_keys=True, default=str).encode()).hexdigest() if idempotency_key else None
        try:
            batch_id, created = await gw.create_batch(items, url, idempotency_key, digest)
        except TooLarge as e:
            raise too_large(e)
        except (Conflict, IdempotencyMismatch) as e:
            raise HTTPException(409 if isinstance(e, Conflict) else 422, str(e))
        except Unavailable as e:
            raise HTTPException(503, str(e))
        view = await gw.get_batch_view(batch_id)
        return JSONResponse({"batch_id": batch_id, "status": view["status"], "total": view["total"], "created": created,
                             "status_url": f"/v1/batches/{batch_id}", "results_url": f"/v1/batches/{batch_id}/results"},
                            status_code=202 if created else 200)

    @app.get("/v1/batches/{batch_id}", tags=["batches"], summary="Batch status, counts and callback delivery state")
    async def get_batch(batch_id: str):
        view = await gw.get_batch_view(batch_id)
        if view is None:
            raise HTTPException(404, "unknown batch_id")
        return view

    @app.get("/v1/batches/{batch_id}/results", tags=["batches"], summary="Per-request results of a batch, paged")
    async def batch_results(batch_id: str, offset: int = Query(0, ge=0), limit: int = Query(1000, ge=1, le=50_000)):
        page = await gw.get_batch_results(batch_id, offset, limit)
        if page is None:
            raise HTTPException(404, "unknown batch_id")
        return page

    # ---- admin ----
    @app.get("/admin/models", tags=["admin"], dependencies=admin, summary="Limits, queue depth and last-60 s usage per model")
    async def models():
        return gw.status()["models"]

    @app.put("/admin/models/{model}/limits", tags=["admin"], dependencies=admin, summary="Change a model's limits while running")
    async def set_limits(model: str, body: LimitsIn):
        need_model(model)
        if body.rpm is None and body.tpm is None:
            raise HTTPException(422, "give rpm and/or tpm")
        return await gw.set_limits(model, body.rpm, body.tpm)

    @app.get("/admin/status", tags=["admin"], dependencies=admin, summary="Everything: replicas, models, requests by state")
    async def status():
        return gw.status()

    @app.get("/admin/stats", tags=["admin"], dependencies=admin, summary="Per-second statistics and latency histogram")
    async def stats():
        return gw.stats()

    @app.post("/admin/reset", tags=["admin"], dependencies=admin, summary="Forget all requests and statistics (only when idle)")
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
