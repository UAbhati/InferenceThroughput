"""The gateway service: engine + records + batches + callbacks, independent of HTTP framing.

Request flow:  submit -> queued -> in_flight -> succeeded | failed   (or expired / rejected)
Batch flow:    accept (ack at once) -> items fed to the engine as queue room allows ->
               when every item is final: build summary -> POST it to callback_url, retrying with backoff.
"""
from __future__ import annotations

import asyncio
import random
import time
import uuid
from collections import deque
from datetime import datetime, timezone

import httpx
import numpy as np

from common.config import ModelsConfig
from gateway.engine import Engine
from gateway.http_provider import HttpProvider
from gateway.records import (FINAL_STATES, FAILED, EXPIRED, QUEUED, REJECTED, SUCCEEDED, Batch, Rec, RecordSink, Registry)

STATS_HORIZON_S = 4 * 3600


class Conflict(Exception):
    pass


class Gateway:
    def __init__(self, cfg: ModelsConfig, provider_client: httpx.AsyncClient, callback_client: httpx.AsyncClient | None = None,
                 public_url: str = "http://127.0.0.1:8000"):
        self.cfg, self.public_url = cfg, public_url.rstrip("/")
        self.provider_client = provider_client
        self.callback_client = callback_client or httpx.AsyncClient(timeout=cfg.gateway.callback_timeout_s)
        self.specs = dict(cfg.models)
        self.limit_log: list[dict] = []
        self.batches: dict[str, Batch] = {}
        self._active: deque[Batch] = deque()  # batches that still have items to hand to the engine
        self._callback_tasks: set[asyncio.Task] = set()
        self._runner: asyncio.Task | None = None
        self._build()

    def _build(self) -> None:
        self.mono0, self.wall0 = time.monotonic(), time.time()
        self.registry = Registry()
        self.sink = RecordSink(self.specs, self.mono0, STATS_HORIZON_S, self.registry, self._on_done)
        self.provider = HttpProvider(self.provider_client, self.registry)
        self.engine = Engine(self.specs, self.provider, self.sink, self.cfg.engine)
        self.batches.clear()
        self._active.clear()
        self.limit_log.clear()

    def epoch(self, mono: float | None) -> float | None:
        return None if mono is None else self.wall0 + (mono - self.mono0)

    def iso(self, mono: float | None) -> str | None:
        e = self.epoch(mono)
        return None if e is None else datetime.fromtimestamp(e, timezone.utc).isoformat(timespec="milliseconds")

    # ---- lifecycle -------------------------------------------------------------------------------------------
    def start(self) -> None:
        self._runner = asyncio.get_running_loop().create_task(self._run())

    async def stop(self) -> None:
        if self._runner:
            self._runner.cancel()
            await asyncio.gather(self._runner, return_exceptions=True)
        for t in list(self._callback_tasks):
            t.cancel()
        await asyncio.gather(*self._callback_tasks, return_exceptions=True)

    async def _run(self) -> None:
        tick = self.cfg.engine.tick_s
        while True:
            now = time.monotonic()
            self._feed(now)
            self.engine.step(now)
            await asyncio.sleep(max(tick - (time.monotonic() - now), 0.0005))

    # ---- admin -----------------------------------------------------------------------------------------------
    def set_limits(self, model: str, rpm: int | None, tpm: int | None) -> dict:
        spec = self.specs[model]
        self.specs[model] = spec = spec.model_copy(update={k: v for k, v in (("rpm", rpm), ("tpm", tpm)) if v})
        self.engine.set_limits(model, rpm, tpm)
        entry = {"t": time.time(), "model": model, "rpm": spec.rpm, "tpm": spec.tpm}
        self.limit_log.append(entry)
        return entry

    def is_idle(self) -> bool:
        return (self.engine.total_queued() == 0 and self.engine.total_in_flight() == 0 and not self._active
                and self.provider.idle() and all(b.remaining == 0 for b in self.batches.values()))

    def reset(self) -> None:
        """Forget all records and statistics and start with empty limiter windows. Only when idle."""
        if not self.is_idle():
            raise Conflict("gateway is not idle")
        self._build()

    # ---- single requests -------------------------------------------------------------------------------------
    def submit_single(self, model: str, tokens: int, payload, request_id: str | None) -> tuple[Rec, bool]:
        """Return (record, created). Re-submitting a known id returns its record, unless it was rejected."""
        rid = request_id or uuid.uuid4().hex
        existing = self.registry.by_id.get(rid)
        if existing is not None and existing.state != REJECTED:
            return existing, False
        now = time.monotonic()
        rec = self.registry.add(rid, model, tokens, payload, now)
        if self.engine.submit(model, now, np.array([rec.seq]), np.array([tokens])) == 0:
            rec.state, rec.t_done, rec.error = REJECTED, now, "queue_full"
            self.registry.by_seq.pop(rec.seq, None)
        return rec, True

    # ---- batches ---------------------------------------------------------------------------------------------
    def create_batch(self, items: list[dict], callback_url: str | None) -> Batch:
        """Register a batch and return at once; items are fed to the engine in the background."""
        ids = [it["request_id"] or uuid.uuid4().hex for it in items]
        if len(set(ids)) != len(ids):
            raise Conflict("duplicate request_id within the batch")
        clash = next((i for i in ids if i in self.registry.by_id), None)
        if clash:
            raise Conflict(f"request_id already exists: {clash}")
        now = time.monotonic()
        batch = Batch(uuid.uuid4().hex, [], callback_url, now)
        for rid, it in zip(ids, items):
            rec = self.registry.add(rid, it["model"], it["estimated_tokens"], it.get("payload"), now, batch)
            batch.recs.append(rec)
            batch.pending.setdefault(rec.model, deque()).append(rec)
        batch.remaining = len(batch.recs)
        if callback_url:
            batch.callback["status"] = "pending"
        self.batches[batch.id] = batch
        self._active.append(batch)
        return batch

    def _feed_target(self, model: str) -> int:
        """How deep batch traffic may fill a model queue: bounded by the queue itself and by what the
        model can actually clear within a fraction of the TTL (so batch items do not expire waiting)."""
        c, g = self.cfg.engine, self.cfg.gateway
        by_queue = c.queue_max * g.batch_queue_fraction
        by_capacity = self.specs[model].rpm * c.headroom / 60 * c.queue_ttl_s * g.batch_target_ttl_fraction
        return int(max(min(by_queue, by_capacity), 100))

    def _feed(self, now: float) -> None:
        if not self._active:
            return
        room = {m: self._feed_target(m) - self.engine.queued(m) for m in self.specs}
        for batch in list(self._active):
            for model, dq in batch.pending.items():
                n = min(len(dq), room[model])
                if n <= 0:
                    continue
                recs = [dq.popleft() for _ in range(n)]
                k = self.engine.submit(model, now, np.array([r.seq for r in recs]), np.array([r.tokens for r in recs]))
                if k < n:
                    dq.extendleft(reversed(recs[k:]))
                room[model] -= k
            if not any(batch.pending.values()):
                self._active.remove(batch)

    def _on_done(self, rec: Rec) -> None:
        b = rec.batch
        if b is None:
            return
        b.remaining -= 1
        if rec.state == SUCCEEDED:
            b.succeeded += 1
        else:
            b.failed += 1
            b.expired += rec.state == EXPIRED
        if b.remaining == 0:
            b.t_completed = time.monotonic()
            if b.callback_url:
                t = asyncio.get_running_loop().create_task(self._deliver(b))
                self._callback_tasks.add(t)
                t.add_done_callback(self._callback_tasks.discard)

    # ---- views -----------------------------------------------------------------------------------------------
    def rec_view(self, r: Rec) -> dict:
        return {"request_id": r.id, "model": r.model, "state": r.state, "attempts": r.attempts, "error": r.error,
                "batch_id": r.batch.id if r.batch else None, "estimated_tokens": r.tokens,
                "accepted_at": self.iso(r.t_accept), "finished_at": self.iso(r.t_done),
                "latency_s": None if r.t_done is None else round(r.t_done - r.t_accept, 4)}

    def result_view(self, r: Rec) -> dict:
        return {k: v for k, v in self.rec_view(r).items() if k in ("request_id", "model", "state", "attempts", "error", "latency_s")}

    def batch_view(self, b: Batch) -> dict:
        fed = b.total - sum(len(d) for d in b.pending.values())
        counts = {s: 0 for s in ("queued", "in_flight")}
        if b.remaining:
            for r in b.recs:
                if r.state in counts:
                    counts[r.state] += 1
        counts["not_yet_submitted"] = b.total - fed
        counts["queued"] -= counts["not_yet_submitted"]
        return {**b.summary(), "pending": counts, "created_at": self.iso(b.t_created), "completed_at": self.iso(b.t_completed),
                "callback_url": b.callback_url, "callback": dict(b.callback),
                "results_url": f"{self.public_url}/v1/batches/{b.id}/results"}

    # ---- callbacks -------------------------------------------------------------------------------------------
    def _callback_payload(self, b: Batch) -> dict:
        payload = {"event": "batch.completed", **b.summary(), "completed_at": self.iso(b.t_completed),
                   "results_url": f"{self.public_url}/v1/batches/{b.id}/results"}
        if b.total <= self.cfg.gateway.callback_inline_results_max:
            payload["results"] = [self.result_view(r) for r in b.recs]
        return payload

    async def _deliver(self, b: Batch) -> None:
        """POST the completion summary; retry with exponential backoff + jitter until a 2xx or attempts run out."""
        g, payload = self.cfg.gateway, self._callback_payload(b)
        for attempt in range(1, g.callback_max_attempts + 1):
            b.callback.update(status="retrying" if attempt > 1 else "delivering", attempts=attempt)
            try:
                r = await self.callback_client.post(b.callback_url, json=payload,
                                                    headers={"Idempotency-Key": b.id, "X-Batch-Id": b.id})
                if r.is_success:
                    b.callback.update(status="delivered", last_error=None, delivered_at=self.iso(time.monotonic()))
                    return
                b.callback["last_error"] = f"HTTP {r.status_code}"
            except (httpx.HTTPError, OSError) as e:
                b.callback["last_error"] = f"{type(e).__name__}: {e}"
            if attempt < g.callback_max_attempts:
                delay = min(g.callback_backoff_base_s * 2 ** (attempt - 1), g.callback_backoff_cap_s)
                await asyncio.sleep(delay * random.uniform(0.8, 1.2))
        b.callback["status"] = "failed"

    # ---- stats -----------------------------------------------------------------------------------------------
    def totals(self) -> dict:
        out = {m: {"submitted": 0} for m in self.specs}
        for m, s in self.sink.series.items():
            out[m] = {k: int(v.sum()) for k, v in s.items()}
        return out

    def status(self) -> dict:
        models, totals = {}, self.totals()
        for m, spec in self.specs.items():
            lim = self.engine.limiters[m]
            lim.remaining(time.monotonic())  # expire old buckets so usage reflects the current window
            models[m] = {"limits": {"rpm": spec.rpm, "tpm": spec.tpm}, "queued": self.engine.queued(m),
                         "in_flight": self.engine.in_flight[m],
                         "window_usage": {"requests": lim.used_req, "tokens": lim.used_tok},
                         "totals": totals[m]}
        by_state: dict[str, int] = {}
        for r in self.registry.by_id.values():
            by_state[r.state] = by_state.get(r.state, 0) + 1
        return {"models": models, "requests_by_state": by_state, "records": len(self.registry.by_id),
                "batches": len(self.batches), "active_batches": sum(1 for b in self.batches.values() if b.remaining),
                "limit_changes": self.limit_log}

    def stats(self) -> dict:
        n = int(time.monotonic() - self.mono0) + 2
        return {"t0_epoch": self.wall0, "series": {m: {k: v[:n].tolist() for k, v in s.items()} for m, s in self.sink.series.items()},
                "hist": {m: h.tolist() for m, h in self.sink.hist.items()},
                "queued": {m: self.engine.queued(m) for m in self.specs},
                "in_flight": dict(self.engine.in_flight),
                "backlog": {m: sum(len(b.pending.get(m, ())) for b in self._active) for m in self.specs}}
