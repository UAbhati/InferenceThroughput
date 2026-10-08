"""HTTP face of the provider simulator.

    uvicorn provider_sim.app:app --port 8001        (config via PROVIDER_MODELS, default config/models.yaml)
"""
from __future__ import annotations

import asyncio
import os
from typing import Any

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel

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
    rpm: int | None = None
    tpm: int | None = None


def create_app(sim: ProviderSimulator | None = None) -> FastAPI:
    if sim is None:
        cfg = ModelsConfig.load(os.environ.get("PROVIDER_MODELS", "config/models.yaml"))
        sim = ProviderSimulator(cfg.models, seed=int(os.environ.get("PROVIDER_SEED", "0")))
    app = FastAPI(title="Provider simulator")
    app.state.sim = sim

    @app.post("/inference")
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

    @app.put("/admin/models/{model}/limits")
    async def set_limits(model: str, body: LimitsIn):
        if model not in sim.specs:
            return JSONResponse({"error": "unknown_model"}, status_code=404)
        return sim.set_limits(model, body.rpm, body.tpm)

    @app.get("/admin/audit")
    async def audit():
        out = {}
        for m, spec in sim.specs.items():
            out[m] = {"limits": {"rpm": spec.rpm, "tpm": spec.tpm}, "counts": sim.counts[m],
                      **sim.audit.violations(m, spec.rpm, spec.tpm)}
        return out

    @app.post("/admin/reset")
    async def reset():
        sim.reset()
        return {"ok": True}

    @app.get("/admin/audit/raw")
    async def audit_raw():
        """Accepted requests/tokens per 10ms bucket (index 0 = last reset), for external verification."""
        return {m: {"bucket_s": 0.01, "requests": sim.audit._req[m].tolist(), "tokens": sim.audit._tok[m].tolist()}
                for m in sim.audit._req}

    @app.get("/healthz")
    async def healthz():
        return {"ok": True}

    @app.get("/admin/audit/{model}/timeline")
    async def timeline(model: str, step_s: float = 1.0):
        return {"limit_changes": sim.audit.limit_changes.get(model, []),
                "timeline": sim.audit.timeline(model, step_s)}

    return app


app = create_app() if os.environ.get("PROVIDER_AUTOSTART", "1") == "1" and os.path.exists(os.environ.get("PROVIDER_MODELS", "config/models.yaml")) else None
