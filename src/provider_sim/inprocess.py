"""In-process provider backend for the engine (no network).

Calls are decided immediately by the simulator, but completions are held until their
simulated latency has elapsed on the (wall) clock, then released by `poll()`. Pending
completions sit in 10ms slots so thousands of requests are scheduled per numpy op.
"""
from __future__ import annotations

import heapq

import numpy as np

from provider_sim.simulator import ProviderSimulator

SLOT_S = 0.01


class InProcessProvider:
    def __init__(self, sim: ProviderSimulator):
        self.sim = sim
        self._pending: dict[int, list] = {}
        self._slots: list[int] = []

    def submit(self, model, now, chunk) -> int:
        seq, tok, enq, att = chunk
        k, codes, lat = self.sim.call_chunk(model, tok)
        if k == 0:
            return 0
        done = now + lat
        slots = (done / SLOT_S).astype(np.int64)
        order = np.argsort(slots, kind="stable")
        slots = slots[order]
        cols = tuple(a[:k][order] for a in (seq, tok, enq, att)) + (codes[order], done[order])
        uniq, start = np.unique(slots, return_index=True)
        bounds = start.tolist() + [k]
        for i, s in enumerate(uniq.tolist()):
            lo, hi = bounds[i], bounds[i + 1]
            bucket = self._pending.get(s)
            if bucket is None:
                self._pending[s] = bucket = []
                heapq.heappush(self._slots, s)
            bucket.append((model,) + tuple(c[lo:hi] for c in cols))
        return k

    def poll(self, now):
        limit = int(now / SLOT_S)
        by_model: dict[str, list] = {}
        while self._slots and self._slots[0] <= limit:
            for piece in self._pending.pop(heapq.heappop(self._slots)):
                by_model.setdefault(piece[0], []).append(piece[1:])
        out = []
        for model, parts in by_model.items():
            cols = parts[0] if len(parts) == 1 else tuple(np.concatenate(c) for c in zip(*parts))
            out.append((model,) + tuple(cols))
        return out

    def pending(self) -> int:
        return sum(len(p[1]) for ps in self._pending.values() for p in ps)
