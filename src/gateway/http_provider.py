"""Engine provider backend that calls the provider over HTTP."""
from __future__ import annotations

import asyncio
import time

import httpx
import numpy as np

from gateway.engine import OK, PERMANENT, RATE_LIMITED, TRANSIENT
from gateway.records import IN_FLIGHT, Registry

STATUS_TO_CODE = {200: OK, 429: RATE_LIMITED, 503: TRANSIENT, 422: PERMANENT}


class HttpProvider:
    def __init__(self, client: httpx.AsyncClient, registry: Registry):
        self.client, self.registry = client, registry
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
            r = await self.client.post("/inference", json={"request_id": rec.id, "model": model,
                                                           "estimated_tokens": tokens, "payload": rec.payload})
            code = STATUS_TO_CODE.get(r.status_code, TRANSIENT if r.status_code >= 500 else PERMANENT)
        except (httpx.HTTPError, OSError):
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
