"""Engine provider backend that calls the provider over HTTP."""
from __future__ import annotations

import asyncio
import time

import aiohttp
import httpx
import numpy as np

from gateway.engine import OK, PERMANENT, RATE_LIMITED, TRANSIENT
from gateway.records import IN_FLIGHT, Registry

STATUS_TO_CODE = {200: OK, 429: RATE_LIMITED, 503: TRANSIENT, 422: PERMANENT}


class AiohttpCaller:
    """POST /inference with aiohttp. (httpx's connection pool does work proportional to
    connections x waiting requests per call, which stalls the event loop at a few thousand
    concurrent calls; aiohttp's does not.)"""

    def __init__(self, base_url: str, timeout_s: float = 60.0, max_connections: int = 4000):
        self.base_url, self.timeout_s, self.max_connections = base_url.rstrip("/"), timeout_s, max_connections
        self._session: aiohttp.ClientSession | None = None

    async def __call__(self, body: dict) -> int:
        if self._session is None:
            self._session = aiohttp.ClientSession(
                self.base_url, timeout=aiohttp.ClientTimeout(total=self.timeout_s),
                connector=aiohttp.TCPConnector(limit=self.max_connections, keepalive_timeout=30))
        async with self._session.post("/inference", json=body) as r:
            await r.read()
            return r.status

    async def close(self) -> None:
        if self._session:
            await self._session.close()


class HttpxCaller:
    """Same interface over an httpx client (used in tests with an in-process ASGI provider)."""

    def __init__(self, client: httpx.AsyncClient):
        self.client = client

    async def __call__(self, body: dict) -> int:
        return (await self.client.post("/inference", json=body)).status_code

    async def close(self) -> None:
        await self.client.aclose()


class HttpProvider:
    def __init__(self, caller, registry: Registry):
        self.caller, self.registry = caller, registry
        self._done: list[tuple] = []
        self._tasks: set[asyncio.Task] = set()

    def submit(self, model, now, chunk) -> int:
        seq, tok, enq, att = chunk
        for i in range(len(seq)):
            rec = self.registry.by_seq[int(seq[i])]
            rec.state = IN_FLIGHT
            task = asyncio.get_running_loop().create_task(self._call(model, rec, int(seq[i]), int(tok[i]), float(enq[i]), int(att[i])))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)
        return len(seq)

    async def _call(self, model, rec, seq, tokens, enq, att) -> None:
        try:
            status = await self.caller({"request_id": rec.id, "model": model, "estimated_tokens": tokens,
                                        "payload": rec.payload})
            code = STATUS_TO_CODE.get(status, TRANSIENT if status >= 500 else PERMANENT)
        except (aiohttp.ClientError, httpx.HTTPError, asyncio.TimeoutError, OSError):
            code = TRANSIENT  # network trouble is worth retrying
        self._done.append((model, seq, tokens, enq, att, code, time.monotonic()))

    def poll(self, now):
        if not self._done:
            return []
        done, self._done = self._done, []
        by_model: dict[str, list] = {}
        for row in done:
            by_model.setdefault(row[0], []).append(row[1:])
        out = []
        for model, rows in by_model.items():
            seq, tok, enq, att, codes, fin = (np.array(c) for c in zip(*rows))
            out.append((model, seq.astype(np.int64), tok.astype(np.int64), enq.astype(float),
                        att.astype(np.int8), codes.astype(np.int8), fin.astype(float)))
        return out

    def idle(self) -> bool:
        return not self._tasks and not self._done
