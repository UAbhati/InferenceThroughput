"""The gateway service: engine + records + batches + callbacks, independent of HTTP framing.

Request flow:  submit -> queued -> in_flight -> succeeded | failed   (or expired / rejected)
Batch flow:    accept (durable, then ack) -> items fed to the engine as queue room allows ->
               when every item is final: build summary -> POST it to callback_url, retrying with backoff.

Persistence and coordination are optional (see gateway.persistence / gateway.coordination). Without them the
service is a single in-memory replica.
"""
from __future__ import annotations

import asyncio
import json
import logging
import random
import time
import uuid
from collections import deque
from datetime import datetime, timezone

import httpx
import numpy as np

from common.config import ModelsConfig
from gateway.coordination import LocalCoordinator
from gateway.engine import Engine
from gateway.http_provider import HttpProvider
from gateway.persistence import DuplicateId, NullPersistence
from gateway.records import (EXPIRED, FAILED, FINAL_STATES, QUEUED, REJECTED, SUCCEEDED, Batch, Rec, RecordSink, Registry)

log = logging.getLogger("gateway")
STATS_HORIZON_S = 4 * 3600
JANITOR_INTERVAL_S = 2.0


class Conflict(Exception):
    pass


class Unavailable(Exception):
    pass


def iso_epoch(e: float | None) -> str | None:
    return None if e is None else datetime.fromtimestamp(e, timezone.utc).isoformat(timespec="milliseconds")


class Gateway:
    def __init__(self, cfg: ModelsConfig, provider_caller, callback_client: httpx.AsyncClient | None = None,
                 public_url: str = "http://127.0.0.1:8000", persistence=None, coordinator=None):
        self.cfg, self.public_url = cfg, public_url.rstrip("/")
        self.provider_caller = provider_caller
        self.callback_client = callback_client or httpx.AsyncClient(timeout=cfg.gateway.callback_timeout_s)
        self.persistence = persistence or NullPersistence()
        self.coordinator = coordinator or LocalCoordinator()
        self.replica_id = self.coordinator.replica_id
        self.specs = dict(cfg.models)  # the *total* limits; the engine runs on this replica's share of them
        self.limit_log: list[dict] = []
        self.batches: dict[str, Batch] = {}
        self._active: deque[Batch] = deque()  # batches that still have items to hand to the engine
        self._callback_tasks: set[asyncio.Task] = set()
        self._tasks: list[asyncio.Task] = []
        self._build()

    def _build(self) -> None:
        self.mono0, self.wall0 = time.monotonic(), time.time()
        self.registry = Registry()
        self.sink = RecordSink(self.specs, self.mono0, STATS_HORIZON_S, self.registry, self._on_done)
        self.provider = HttpProvider(self.provider_caller, self.registry)
        shares = {m: s.model_copy(update={"rpm": self.coordinator.share(s.rpm), "tpm": self.coordinator.share(s.tpm)})
                  for m, s in self.specs.items()}
        self.engine = Engine(shares, self.provider, self.sink, self.cfg.engine)
        self.batches.clear()
        self._active.clear()
        self.limit_log.clear()

    # ---- time ------------------------------------------------------------------------------------------------
    def epoch(self, mono: float | None) -> float | None:
        return None if mono is None else self.wall0 + (mono - self.mono0)

    def mono(self, epoch: float | None) -> float | None:
        return None if epoch is None else self.mono0 + (epoch - self.wall0)

    def iso(self, mono: float | None) -> str | None:
        return iso_epoch(self.epoch(mono))

    # ---- lifecycle -------------------------------------------------------------------------------------------
    async def start(self) -> None:
        await self.persistence.start()
        await self.coordinator.start(self._on_remote_limits, self._apply_limits)
        await self._load_limits()
        self._apply_limits()
        try:
            await self.recover()
        except Exception:
            log.exception("recovery failed at startup")
        loop = asyncio.get_running_loop()
        self._tasks = [loop.create_task(self._run())]
        if self.coordinator.shared:
            self._tasks.append(loop.create_task(self._janitor()))

    async def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        for t in list(self._callback_tasks):
            t.cancel()
        await asyncio.gather(*self._callback_tasks, return_exceptions=True)
        await self.provider_caller.close()
        await self.callback_client.aclose()
        await self.coordinator.stop()
        await self.persistence.stop()

    async def _run(self) -> None:
        tick = self.cfg.engine.tick_s
        while True:
            now = time.monotonic()
            admit = now >= self.coordinator.dispatch_after
            if admit:
                self._feed(now)
            self.engine.step(now, admit)
            await asyncio.sleep(max(tick - (time.monotonic() - now), 0.0005))

    async def _janitor(self) -> None:
        """Adopt the unfinished work of replicas that died while we are running."""
        while True:
            await asyncio.sleep(JANITOR_INTERVAL_S)
            try:
                await self.recover()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("janitor recovery failed")

    # ---- limits ----------------------------------------------------------------------------------------------
    async def _load_limits(self) -> None:
        """Runtime limit changes survive restarts: shared store first, then the database, then the config file."""
        stored = await self.coordinator.get_limits()
        from_db = False
        if not stored:
            stored, from_db = await self.persistence.limits(), True
        for m, (rpm, tpm) in stored.items():
            if m in self.specs:
                self.specs[m] = self.specs[m].model_copy(update={"rpm": rpm, "tpm": tpm})
                if from_db:
                    await self.coordinator.publish_limits(m, rpm, tpm)

    def _apply_limits(self) -> None:
        """Give the engine this replica's share of every model's current total limit."""
        for m, s in self.specs.items():
            self.engine.set_limits(m, self.coordinator.share(s.rpm), self.coordinator.share(s.tpm))

    def _on_remote_limits(self, model: str, rpm: int, tpm: int) -> None:
        if model in self.specs:
            self.specs[model] = self.specs[model].model_copy(update={"rpm": rpm, "tpm": tpm})
            self._apply_limits()

    async def set_limits(self, model: str, rpm: int | None, tpm: int | None) -> dict:
        spec = self.specs[model]
        self.specs[model] = spec = spec.model_copy(update={k: v for k, v in (("rpm", rpm), ("tpm", tpm)) if v})
        self._apply_limits()
        await self.persistence.save_limits(model, spec.rpm, spec.tpm)
        await self.coordinator.publish_limits(model, spec.rpm, spec.tpm)
        entry = {"t": time.time(), "model": model, "rpm": spec.rpm, "tpm": spec.tpm}
        self.limit_log.append(entry)
        return entry

    def is_idle(self) -> bool:
        return (self.engine.total_queued() == 0 and self.engine.total_in_flight() == 0 and not self._active
                and self.provider.idle() and all(b.remaining == 0 for b in self.batches.values()))

    async def reset(self) -> None:
        """Forget all records and statistics, empty the limiter windows and clear stored requests. Only when idle."""
        if not self.is_idle():
            raise Conflict("gateway is not idle")
        await self.persistence.truncate()
        self._build()

    # ---- persistence rows ------------------------------------------------------------------------------------
    def _rec_row(self, r: Rec) -> tuple:
        return (r.id, self.replica_id, r.batch.id if r.batch else None, r.idx, r.model, r.tokens, json.dumps(r.payload),
                r.state, r.attempts, r.error, self.epoch(r.t_accept), self.epoch(r.t_done))

    def _batch_row(self, b: Batch) -> tuple:
        cb = b.callback
        delivered = None if cb["delivered_at"] is None else datetime.fromisoformat(cb["delivered_at"]).timestamp()
        return (b.id, self.replica_id, b.callback_url, b.status, b.total, b.succeeded, b.failed, b.expired,
                self.epoch(b.t_created), self.epoch(b.t_completed), cb["status"], cb["attempts"], cb["last_error"], delivered)

    # ---- single requests -------------------------------------------------------------------------------------
    async def submit_single(self, model: str, tokens: int, payload, request_id: str | None) -> tuple[str, str, bool]:
        """Return (request_id, state, created). Re-submitting a known id returns its state, unless it was rejected."""
        rid = request_id or uuid.uuid4().hex
        existing = self.registry.by_id.get(rid)
        if existing is not None and existing.state != REJECTED:
            return rid, existing.state, False
        if existing is None and request_id and self.persistence.enabled:
            row = await self.persistence.get_request(rid)  # idempotency must also hold across restarts
            if row is not None:
                return rid, row["state"], False
        now = time.monotonic()
        rec = self.registry.add(rid, model, tokens, payload, now)
        if self.engine.submit(model, now, np.array([rec.seq]), np.array([tokens])) == 0:
            rec.state, rec.t_done, rec.error = REJECTED, now, "queue_full"
            self.registry.by_seq.pop(rec.seq, None)
        else:
            self.persistence.put_request(self._rec_row(rec))
        return rid, rec.state, True

    # ---- batches ---------------------------------------------------------------------------------------------
    async def create_batch(self, items: list[dict], callback_url: str | None) -> Batch:
        """Register a batch, save it durably, and return; items are fed to the engine in the background."""
        ids = [it["request_id"] or uuid.uuid4().hex for it in items]
        if len(set(ids)) != len(ids):
            raise Conflict("duplicate request_id within the batch")
        clash = next((i for i in ids if i in self.registry.by_id), None)
        if clash:
            raise Conflict(f"request_id already exists: {clash}")
        now = time.monotonic()
        batch = Batch(uuid.uuid4().hex, [], callback_url, now)
        for idx, (rid, it) in enumerate(zip(ids, items)):
            rec = self.registry.add(rid, it["model"], it["estimated_tokens"], it.get("payload"), now, batch, idx)
            batch.recs.append(rec)
        batch.remaining = len(batch.recs)
        if callback_url:
            batch.callback["status"] = "pending"
        if self.persistence.enabled:
            try:
                await self.persistence.insert_batch(self._batch_row(batch), [self._rec_row(r) for r in batch.recs])
            except Exception as e:
                for r in batch.recs:
                    self.registry.by_id.pop(r.id, None)
                    self.registry.by_seq.pop(r.seq, None)
                if isinstance(e, DuplicateId):
                    raise Conflict(str(e))
                log.exception("could not save batch")
                raise Unavailable("could not save the batch, nothing was accepted")
        for rec in batch.recs:
            batch.pending.setdefault(rec.model, deque()).append(rec)
        self.batches[batch.id] = batch
        self._active.append(batch)
        return batch

    def _feed_target(self, model: str) -> int:
        """How deep batch traffic may fill a model queue: bounded by the queue itself and by what this replica can
        clear within a fraction of the TTL (so batch items do not expire waiting behind their own batch)."""
        c, g = self.cfg.engine, self.cfg.gateway
        by_queue = c.queue_max * g.batch_queue_fraction
        by_capacity = self.engine.limiters[model].rpm / 60 * c.queue_ttl_s * g.batch_target_ttl_fraction
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
        self.persistence.put_request(self._rec_row(rec))
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
            self._batch_completed(b)

    def _batch_completed(self, b: Batch) -> None:
        b.t_completed = time.monotonic()
        self.persistence.put_batch(self._batch_row(b))
        self._start_delivery(b)

    def _start_delivery(self, b: Batch) -> None:
        if b.callback_url:
            t = asyncio.get_running_loop().create_task(self._deliver(b))
            self._callback_tasks.add(t)
            t.add_done_callback(self._callback_tasks.discard)

    # ---- recovery --------------------------------------------------------------------------------------------
    async def recover(self) -> dict:
        """Adopt unfinished work of dead replicas: re-queue open requests, resume batches, re-send undelivered callbacks.

        Requests that were in flight when their replica died are run again (at-least-once toward the provider).
        """
        if not self.persistence.enabled:
            return {}
        live = await self.coordinator.live_replicas()
        dead = [o for o in await self.persistence.owners_with_open_work() if o not in live and o != self.replica_id]
        if not dead:
            return {}
        batches, singles = await self.persistence.claim(self.replica_id, dead)
        now = time.monotonic()
        n_batch_reqs = 0
        for brow, rrows in batches:
            b = Batch(brow["id"], [], brow["callback_url"], self.mono(brow["created_at"]))
            b.callback.update(status=brow["cb_status"], last_error=brow["cb_last_error"], attempts=0,
                              delivered_at=iso_epoch(brow["cb_delivered_at"]))
            for rr in rrows:
                rec = self.registry.add(rr["id"], rr["model"], rr["tokens"], json.loads(rr["payload"]) if rr["payload"] else None,
                                        self.mono(rr["accepted_at"]), b, rr["idx"])
                b.recs.append(rec)
                if rr["state"] in FINAL_STATES:
                    rec.state, rec.attempts, rec.error, rec.t_done = rr["state"], rr["attempts"], rr["error"], self.mono(rr["finished_at"])
                    self.registry.by_seq.pop(rec.seq, None)
                    b.succeeded += rec.state == SUCCEEDED
                    b.failed += rec.state != SUCCEEDED
                    b.expired += rec.state == EXPIRED
                elif rec.model in self.specs:
                    b.pending.setdefault(rec.model, deque()).append(rec)
                    n_batch_reqs += 1
                else:  # its model no longer exists: it can never run
                    rec.state, rec.error, rec.t_done = FAILED, "unknown_model", now
                    self.registry.by_seq.pop(rec.seq, None)
                    b.failed += 1
            b.remaining = b.total - b.succeeded - b.failed
            self.batches[b.id] = b
            if b.remaining:
                self._active.append(b)
                self.persistence.put_batch(self._batch_row(b))
            else:
                b.t_completed = self.mono(brow["completed_at"]) or now
                b.callback["attempts"] = 0
                self.persistence.put_batch(self._batch_row(b))
                self._start_delivery(b)
        for rr in singles:
            rec = self.registry.add(rr["id"], rr["model"], rr["tokens"], json.loads(rr["payload"]) if rr["payload"] else None,
                                    self.mono(rr["accepted_at"]))
            if rec.model not in self.specs or self.engine.submit(rec.model, now, np.array([rec.seq]), np.array([rec.tokens])) == 0:
                rec.state, rec.error, rec.t_done = FAILED, "unknown_model" if rec.model not in self.specs else "queue_full_on_recovery", now
                self.registry.by_seq.pop(rec.seq, None)
                self.persistence.put_request(self._rec_row(rec))
        summary = {"from_replicas": dead, "batches": len(batches), "batch_requests_requeued": n_batch_reqs, "single_requests": len(singles)}
        log.info("recovered %s", summary)
        return summary

    # ---- views -----------------------------------------------------------------------------------------------
    def rec_view(self, r: Rec) -> dict:
        return {"request_id": r.id, "model": r.model, "state": r.state, "attempts": r.attempts, "error": r.error,
                "batch_id": r.batch.id if r.batch else None, "estimated_tokens": r.tokens,
                "accepted_at": self.iso(r.t_accept), "finished_at": self.iso(r.t_done),
                "latency_s": None if r.t_done is None else round(r.t_done - r.t_accept, 4)}

    def result_view(self, r: Rec) -> dict:
        return {k: v for k, v in self.rec_view(r).items() if k in ("request_id", "model", "state", "attempts", "error", "latency_s")}

    @staticmethod
    def _row_rec_view(row: dict) -> dict:
        fin = row["finished_at"]
        return {"request_id": row["id"], "model": row["model"], "state": row["state"], "attempts": row["attempts"], "error": row["error"],
                "batch_id": row["batch_id"], "estimated_tokens": row["tokens"], "accepted_at": iso_epoch(row["accepted_at"]),
                "finished_at": iso_epoch(fin), "latency_s": None if fin is None else round(fin - row["accepted_at"], 4)}

    async def request_view(self, rid: str) -> dict | None:
        rec = self.registry.by_id.get(rid)
        if rec is not None:
            return self.rec_view(rec)
        row = await self.persistence.get_request(rid)
        return None if row is None else self._row_rec_view(row)

    async def request_states(self, ids: list[str]) -> dict[str, str]:
        by_id = self.registry.by_id
        out = {i: (by_id[i].state if i in by_id else "unknown") for i in ids}
        missing = [i for i, s in out.items() if s == "unknown"]
        if missing and self.persistence.enabled:
            for i in missing:
                row = await self.persistence.get_request(i)
                if row:
                    out[i] = row["state"]
        return out

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

    def _row_batch_view(self, row: dict) -> dict:
        open_n = sum(n for s, n in row["states"].items() if s in ("queued", "in_flight"))
        remaining = row["total"] - row["succeeded"] - row["failed"]
        status = "processing" if remaining else ("completed" if not row["failed"] else "completed_with_failures")
        return {"batch_id": row["id"], "status": status, "total": row["total"], "succeeded": row["succeeded"], "failed": row["failed"],
                "expired": row["expired"], "pending": {"queued": open_n, "in_flight": 0, "not_yet_submitted": 0},
                "created_at": iso_epoch(row["created_at"]), "completed_at": iso_epoch(row["completed_at"]),
                "callback_url": row["callback_url"],
                "callback": {"status": row["cb_status"], "attempts": row["cb_attempts"], "last_error": row["cb_last_error"],
                             "delivered_at": iso_epoch(row["cb_delivered_at"])},
                "results_url": f"{self.public_url}/v1/batches/{row['id']}/results", "source": "database"}

    async def get_batch_view(self, bid: str) -> dict | None:
        b = self.batches.get(bid)
        if b is not None:
            return self.batch_view(b)
        row = await self.persistence.get_batch(bid)
        return None if row is None else self._row_batch_view(row)

    async def get_batch_results(self, bid: str, offset: int, limit: int) -> dict | None:
        b = self.batches.get(bid)
        if b is not None:
            page = b.recs[offset:offset + limit]
            return {"batch_id": b.id, "status": b.status, "total": b.total, "offset": offset,
                    "next_offset": offset + len(page) if offset + len(page) < b.total else None,
                    "results": [self.result_view(r) for r in page]}
        view = await self.get_batch_view(bid)
        if view is None:
            return None
        rows = await self.persistence.batch_results(bid, offset, limit)
        return {"batch_id": bid, "status": view["status"], "total": view["total"], "offset": offset,
                "next_offset": offset + len(rows) if offset + len(rows) < view["total"] else None,
                "results": [{k: v for k, v in self._row_rec_view(r).items() if k in ("request_id", "model", "state", "attempts", "error", "latency_s")}
                            for r in rows]}

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
            self.persistence.put_batch(self._batch_row(b))
            try:
                r = await self.callback_client.post(b.callback_url, json=payload,
                                                    headers={"Idempotency-Key": b.id, "X-Batch-Id": b.id})
                if r.is_success:
                    b.callback.update(status="delivered", last_error=None, delivered_at=self.iso(time.monotonic()))
                    self.persistence.put_batch(self._batch_row(b))
                    return
                b.callback["last_error"] = f"HTTP {r.status_code}"
            except (httpx.HTTPError, OSError) as e:
                b.callback["last_error"] = f"{type(e).__name__}: {e}"
            if attempt < g.callback_max_attempts:
                delay = min(g.callback_backoff_base_s * 2 ** (attempt - 1), g.callback_backoff_cap_s)
                await asyncio.sleep(delay * random.uniform(0.8, 1.2))
        b.callback["status"] = "failed"
        self.persistence.put_batch(self._batch_row(b))

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
            models[m] = {"limits": {"rpm": spec.rpm, "tpm": spec.tpm},
                         "this_replica_share": {"rpm": self.coordinator.share(spec.rpm), "tpm": self.coordinator.share(spec.tpm)},
                         "queued": self.engine.queued(m), "in_flight": self.engine.in_flight[m],
                         "window_usage": {"requests": lim.used_req, "tokens": lim.used_tok}, "totals": totals[m]}
        by_state: dict[str, int] = {}
        for r in self.registry.by_id.values():
            by_state[r.state] = by_state.get(r.state, 0) + 1
        return {"replica_id": self.replica_id, "replicas": list(self.coordinator.replicas), "models": models,
                "requests_by_state": by_state, "records": len(self.registry.by_id), "batches": len(self.batches),
                "active_batches": sum(1 for b in self.batches.values() if b.remaining), "limit_changes": self.limit_log,
                "persistence": {"enabled": self.persistence.enabled,
                                "pending_writes": len(getattr(self.persistence, "_req", ())) + len(getattr(self.persistence, "_batch", ())),
                                "flush_errors": getattr(self.persistence, "flush_errors", 0)}}

    def stats(self) -> dict:
        n = int(time.monotonic() - self.mono0) + 2
        return {"t0_epoch": self.wall0, "series": {m: {k: v[:n].tolist() for k, v in s.items()} for m, s in self.sink.series.items()},
                "hist": {m: h.tolist() for m, h in self.sink.hist.items()},
                "queued": {m: self.engine.queued(m) for m in self.specs},
                "in_flight": dict(self.engine.in_flight),
                "backlog": {m: sum(len(b.pending.get(m, ())) for b in self._active) for m in self.specs}}
