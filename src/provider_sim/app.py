"""HTTP face of the provider simulator.

    uvicorn provider_sim.app:app --port 8001        (config via PROVIDER_MODELS, default config/models.yaml)
"""
from __future__ import annotations

import asyncio
import os
from typing import Any

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from common.config import ModelsConfig
from provider_sim.simulator import Outcome, ProviderSimulator

HTTP_STATUS = {Outcome.SUCCESS: 200, Outcome.TRANSIENT_FAILURE: 503,
               Outcome.PERMANENT_FAILURE: 422, Outcome.RATE_LIMITED: 429}


class InferenceIn(BaseModel):
    request_id: str
    model: str
    estimated_tokens: int = 1000
    payload: Any = None


class LimitsIn(BaseModel):
    rpm: int | None = Field(default=None, gt=0, examples=[5000])
    tpm: int | None = Field(default=None, gt=0, examples=[10000000])


class BehaviorIn(BaseModel):
    latency_ms_median: float | None = Field(default=None, gt=0)
    latency_sigma: float | None = Field(default=None, ge=0)
    transient_failure_rate: float | None = Field(default=None, ge=0, le=1, description="share of calls answered 503 (retryable)")
    permanent_failure_rate: float | None = Field(default=None, ge=0, le=1, description="share of calls answered 422 (not retryable)")


def create_app(sim: ProviderSimulator | None = None) -> FastAPI:
    if sim is None:
        cfg = ModelsConfig.load(os.environ.get("PROVIDER_MODELS", "config/models.yaml"))
        sim = ProviderSimulator(cfg.models, seed=int(os.environ.get("PROVIDER_SEED", "0")))
    app = FastAPI(
        title="Provider simulator",
        description="A fake inference provider with its own per-model RPM/TPM limits, latency and failure behaviour. "
                    "It counts every call it accepts in an independent audit, which is how the benchmarks prove limits were respected.",
        openapi_tags=[{"name": "inference", "description": "What the gateway calls."},
                      {"name": "admin", "description": "Change limits and behaviour while running; read the audit."}])
    app.state.sim = sim

    @app.post("/inference", tags=["inference"], summary="Run one simulated inference call (200 / 503 / 422 / 429)")
    async def inference(body: InferenceIn):
        if body.model not in sim.specs:
            return JSONResponse({"error": "unknown_model"}, status_code=404)
        result = sim.call(body.model, body.estimated_tokens)
        if result.outcome is Outcome.RATE_LIMITED:
            return JSONResponse({"error": "rate_limited"}, status_code=429,
                                headers={"Retry-After": f"{result.retry_after_s:.2f}"})
        await asyncio.sleep(result.latency_s)
        return JSONResponse({"request_id": body.request_id, "outcome": result.outcome.value},
                            status_code=HTTP_STATUS[result.outcome])

    @app.put("/admin/models/{model}/limits", tags=["admin"], summary="Change a model's RPM/TPM limit now")
    async def set_limits(model: str, body: LimitsIn):
        if model not in sim.specs:
            return JSONResponse({"error": "unknown_model"}, status_code=404)
        return sim.set_limits(model, body.rpm, body.tpm)

    @app.get("/admin/audit", tags=["admin"], summary="Per model: limits, outcome counts, max usage over any 60 s")
    async def audit():
        out = {}
        for m, spec in sim.specs.items():
            out[m] = {"limits": {"rpm": spec.rpm, "tpm": spec.tpm}, "counts": sim.counts[m],
                      **sim.audit.violations(m, spec.rpm, spec.tpm)}
        return out

    @app.put("/admin/models/{model}/behavior", tags=["admin"], summary="Change latency or failure rates now")
    async def set_behavior(model: str, body: BehaviorIn):
        if model not in sim.specs:
            return JSONResponse({"error": "unknown_model"}, status_code=404)
        return sim.set_behavior(model, **body.model_dump())

    @app.post("/admin/reset", tags=["admin"], summary="Clear audit, counters and limiter windows")
    async def reset():
        sim.reset()
        return {"ok": True}

    @app.get("/admin/audit/raw", tags=["admin"], summary="Accepted requests/tokens per 10 ms bucket")
    async def audit_raw():
        """Accepted requests/tokens per 10ms bucket (index 0 = last reset), for external verification."""
        return {m: {"bucket_s": 0.01, "requests": sim.audit._req[m].tolist(), "tokens": sim.audit._tok[m].tolist()}
                for m in sim.audit._req}

    @app.get("/healthz")
    async def healthz():
        return {"ok": True}

    @app.get("/admin/audit/{model}/timeline", tags=["admin"], summary="Per-second accepted requests and trailing-60 s totals")
    async def timeline(model: str, step_s: float = 1.0):
        return {"limit_changes": sim.audit.limit_changes.get(model, []),
                "timeline": sim.audit.timeline(model, step_s)}

    return app


app = create_app() if os.environ.get("PROVIDER_AUTOSTART", "1") == "1" and os.path.exists(os.environ.get("PROVIDER_MODELS", "config/models.yaml")) else None
