"""Callback receiver for tests: records every delivery attempt and can refuse the first N."""
from __future__ import annotations

import time

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse


def create_sink(reject_first: int = 0) -> FastAPI:
    app = FastAPI()
    app.state.attempts = []  # {t_epoch, batch_id, status_code, body}
    app.state.reject_first = reject_first

    @app.post("/callback")
    async def callback(request: Request):
        body = await request.json()
        n = len(app.state.attempts) + 1
        code = 503 if n <= app.state.reject_first else 200
        app.state.attempts.append({"t": time.time(), "batch_id": body.get("batch_id"), "status_code": code, "body": body,
                                   "idempotency_key": request.headers.get("idempotency-key")})
        return JSONResponse({"accepted": code == 200}, status_code=code)

    return app
