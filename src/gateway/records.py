"""Per-request and per-batch records for the HTTP service, and the sink that keeps them current."""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable

import numpy as np

from gateway.engine import OK, PERMANENT
from gateway.stats import StatsSink

QUEUED, IN_FLIGHT, SUCCEEDED, FAILED, EXPIRED, REJECTED = (
    "queued", "in_flight", "succeeded", "failed", "expired", "rejected")
FINAL_STATES = {SUCCEEDED, FAILED, EXPIRED, REJECTED}


@dataclass(slots=True, eq=False)
class Rec:
    id: str
    seq: int
    model: str
    tokens: int
    payload: Any
    t_accept: float  # monotonic
    state: str = QUEUED
    attempts: int = 0
    t_done: float | None = None
    error: str | None = None
    batch: "Batch | None" = None


@dataclass(eq=False)
class Batch:
    id: str
    recs: list[Rec]
    callback_url: str | None
    t_created: float
    pending: dict[str, deque] = field(default_factory=dict)  # not yet handed to the engine, per model
    remaining: int = 0  # requests not yet in a final state
    succeeded: int = 0
    failed: int = 0  # includes expired
    expired: int = 0
    t_completed: float | None = None
    callback: dict = field(default_factory=lambda: {"status": "none", "attempts": 0, "last_error": None, "delivered_at": None})

    @property
    def total(self) -> int:
        return len(self.recs)

    @property
    def status(self) -> str:
        if self.remaining:
            return "processing"
        return "completed" if not self.failed else "completed_with_failures"

    def summary(self) -> dict:
        return {"batch_id": self.id, "status": self.status, "total": self.total,
                "succeeded": self.succeeded, "failed": self.failed, "expired": self.expired}


class Registry:
    def __init__(self) -> None:
        self.by_id: dict[str, Rec] = {}
        self.by_seq: dict[int, Rec] = {}
        self._seq = 0

    def add(self, rec_id: str, model: str, tokens: int, payload: Any, now: float, batch: Batch | None = None) -> Rec:
        self._seq += 1
        rec = Rec(rec_id, self._seq, model, tokens, payload, now, batch=batch)
        self.by_id[rec_id] = rec
        self.by_seq[rec.seq] = rec
        return rec


class RecordSink(StatsSink):
    """StatsSink that also updates request records and notifies when one reaches a final state."""

    def __init__(self, models, t0: float, horizon_s: float, registry: Registry, on_done: Callable[[Rec], None]):
        super().__init__(models, t0, horizon_s)
        self.registry, self.on_done = registry, on_done

    def expired(self, model, now, seq, enq):
        super().expired(model, now, seq, enq)
        for s in seq.tolist():
            rec = self.registry.by_seq.pop(s)
            rec.state, rec.t_done, rec.error = EXPIRED, now, "expired_in_queue"
            self.on_done(rec)

    def retried(self, model, now, seq):
        super().retried(model, now, seq)
        for s in seq.tolist():
            self.registry.by_seq[s].state = QUEUED

    def final(self, model, now, seq, tokens, enq, attempt, codes, done):
        super().final(model, now, seq, tokens, enq, attempt, codes, done)
        for s, a, c, d in zip(seq.tolist(), attempt.tolist(), codes.tolist(), done.tolist()):
            rec = self.registry.by_seq.pop(s)
            rec.attempts, rec.t_done = a + 1, d
            if c == OK:
                rec.state = SUCCEEDED
            else:
                rec.state = FAILED
                rec.error = "permanent_failure" if c == PERMANENT else "transient_failure_retries_exhausted"
            self.on_done(rec)
