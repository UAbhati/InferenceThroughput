"""Gateway engine: per-model bounded queues -> rate limiter -> provider -> final states.

    submit()  ->  queued --(limiter admits)--> in flight --> succeeded | failed
                     |                              |-- transient failure -> wait (backoff + jitter) -> queued again (up to max_attempts)
                     |                              `-- provider 429 -> queued again, that model pauses briefly
                     |-- larger than the model's whole token budget -> failed at once (it could never be sent)
                     `-- ttl exceeded -> expired
    submit() into a full queue -> rejected (returned to the caller, never queued)

The engine is clock-agnostic (`now` is passed in) and transport-agnostic: a `Provider`
executes admitted chunks and hands finished ones back through `poll()`. The same
engine therefore drives the in-process bulk benchmark and (via an HTTP provider) the
real service. Each model has its own queue and limiter, so one constrained model
never slows another.
"""
from __future__ import annotations

import heapq
import random
from typing import Protocol

import numpy as np

from common.config import EngineConfig, ModelSpec
from gateway.limiter import SlidingWindowLimiter
from gateway.queueing import Chunk, ChunkQueue

OK, TRANSIENT, PERMANENT, RATE_LIMITED, TOO_LARGE = 0, 1, 2, 3, 4  # outcome codes (TOO_LARGE is decided by the gateway)


class Provider(Protocol):
    def submit(self, model: str, now: float, chunk: Chunk) -> int:
        """Start the chunk's calls; return how many the provider accepted (a prefix)."""

    def poll(self, now: float) -> list[tuple[str, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]]:
        """Finished calls as (model, seq, tokens, enqueued_at, attempt, outcome_codes, finished_at)."""


class Sink(Protocol):
    def submitted(self, model: str, now: float, accepted: int, rejected: int, tokens: int) -> None: ...
    def expired(self, model: str, now: float, seq: np.ndarray, enq: np.ndarray) -> None: ...
    def final(self, model: str, now: float, seq: np.ndarray, tokens: np.ndarray, enq: np.ndarray,
              attempt: np.ndarray, codes: np.ndarray, done: np.ndarray) -> None: ...
    def retried(self, model: str, now: float, seq: np.ndarray) -> None:
        """Requests going back to the queue (transient failure, or the provider pushed back)."""


class Engine:
    def __init__(self, specs: dict[str, ModelSpec], provider: Provider, sink: Sink, cfg: EngineConfig | None = None):
        self.cfg = cfg or EngineConfig()
        self.provider, self.sink = provider, sink
        self.queues = {m: ChunkQueue() for m in specs}
        self.limiters = {m: SlidingWindowLimiter(self._eff(s.rpm), self._eff(s.tpm)) for m, s in specs.items()}
        self.in_flight = {m: 0 for m in specs}
        self._credit = {m: self._credit_cap(m) for m in specs}  # smoothing: requests we may send right now
        self._credit_at: float | None = None
        self._delayed: list[tuple] = []  # retries waiting out their backoff: (ready_at, tiebreak, model, chunk)
        self._delayed_n = {m: 0 for m in specs}
        self._tiebreak = 0
        self._pause_until = {m: 0.0 for m in specs}
        self._rng = random.Random(7)

    def _eff(self, limit: int) -> int:
        return max(int(limit * self.cfg.headroom), 1)

    def _rate(self, model: str) -> float:
        return self.limiters[model].rpm / 60.0

    def _credit_cap(self, model: str) -> float:
        return max(self._rate(model) * self.cfg.burst_s, 2.0)

    def set_limits(self, model: str, rpm: int | None = None, tpm: int | None = None) -> None:
        self.limiters[model].set_limits(self._eff(rpm) if rpm else None, self._eff(tpm) if tpm else None)

    def submit(self, model: str, now: float, seq: np.ndarray, tokens: np.ndarray) -> int:
        """Queue as many as fit; return how many were accepted (the rest are rejected)."""
        q = self.queues[model]
        k = min(self.cfg.queue_max - q.n, len(seq))
        if k > 0:
            q.push((seq[:k], tokens[:k], np.full(k, now), np.zeros(k, np.int8)))
        k = max(k, 0)
        self.sink.submitted(model, now, k, len(seq) - k, int(tokens.sum()))
        return k

    def _finish(self, model: str, now: float, chunk: Chunk, code: int) -> None:
        n = len(chunk[0])
        self.sink.final(model, now, chunk[0], chunk[1], chunk[2], chunk[3], np.full(n, code, np.int8), np.full(n, now))

    def _schedule_retry(self, model: str, now: float, chunk: Chunk, attempt: int) -> None:
        delay = min(self.cfg.retry_backoff_s * 2 ** (attempt - 1), self.cfg.retry_backoff_cap_s) * self._rng.uniform(0.5, 1.5)
        self._tiebreak += 1
        heapq.heappush(self._delayed, (now + delay, self._tiebreak, model, chunk))
        self._delayed_n[model] += len(chunk[0])

    def step(self, now: float, admit: bool = True) -> None:
        dt = 0.0 if self._credit_at is None else max(now - self._credit_at, 0.0)
        self._credit_at = now
        while self._delayed and self._delayed[0][0] <= now:  # retries whose backoff is over rejoin the front of their queue
            _, _, model, chunk = heapq.heappop(self._delayed)
            self._delayed_n[model] -= len(chunk[0])
            self.queues[model].push_front(chunk)
        for model, q in self.queues.items():
            self._credit[model] = min(self._credit[model] + self._rate(model) * dt, self._credit_cap(model))
            if q.n:
                for c in q.expire(now - self.cfg.queue_ttl_s):
                    self.sink.expired(model, now, c[0], c[2])
            if q.n and admit and now >= self._pause_until[model]:
                lim = self.limiters[model]
                rem_req, _ = lim.remaining(now)
                chunk = q.pop(min(rem_req, int(self._credit[model])))
                if chunk is not None:
                    big = chunk[1] > lim.tpm  # can never fit the token budget (e.g. the limit was lowered): fail it,
                    if big.any():             # do not let it sit at the head and block everything behind it
                        self._finish(model, now, tuple(a[big] for a in chunk), TOO_LARGE)  # type: ignore[arg-type]
                        chunk = tuple(a[~big] for a in chunk)  # type: ignore[assignment]
                if chunk is not None and len(chunk[0]):
                    k = lim.admit_chunk(now, chunk[1])
                    self._credit[model] -= k
                    if k < len(chunk[0]):
                        q.push_front(tuple(a[k:] for a in chunk))  # type: ignore[arg-type]
                    if k:
                        sent = tuple(a[:k] for a in chunk)
                        accepted = self.provider.submit(model, now, sent)  # type: ignore[arg-type]
                        if accepted < k:  # provider pushed back (its own 429): try again later
                            q.push_front(tuple(a[accepted:] for a in sent))  # type: ignore[arg-type]
                        self.in_flight[model] += accepted
        for model, seq, tok, enq, att, codes, done in self.provider.poll(now):
            self.in_flight[model] -= len(seq)
            limited = codes == RATE_LIMITED
            retry = ((codes == TRANSIENT) & (att + 1 < self.cfg.max_attempts)) | limited
            if retry.any():
                idx = np.nonzero(retry)[0]
                idx = idx[np.argsort(enq[idx], kind="stable")]  # queue expiry relies on enqueue order
                r_seq, r_tok, r_enq, r_att, r_lim = seq[idx], tok[idx], enq[idx], att[idx], limited[idx]
                self.sink.retried(model, now, r_seq)
                if r_lim.any():
                    # a provider 429 means "not yet", not a failed attempt: no attempt used, and stop sending for a moment
                    self.queues[model].push_front((r_seq[r_lim], r_tok[r_lim], r_enq[r_lim], r_att[r_lim]))
                    self._pause_until[model] = now + self.cfg.rate_limited_pause_s
                tr = ~r_lim
                if tr.any():
                    nxt = (r_att[tr] + 1).astype(np.int8)
                    for a in np.unique(nxt):  # one backoff per attempt number, so a retry storm spreads out
                        m = nxt == a
                        self._schedule_retry(model, now, (r_seq[tr][m], r_tok[tr][m], r_enq[tr][m], nxt[m]), int(a))
                keep = ~retry
                seq, tok, enq, att, codes, done = (a[keep] for a in (seq, tok, enq, att, codes, done))
            if len(seq):
                self.sink.final(model, now, seq, tok, enq, att, codes, done)

    def queued(self, model: str) -> int:
        return self.queues[model].n + self._delayed_n[model]  # waiting retries are still queued work

    def total_in_flight(self) -> int:
        return sum(self.in_flight.values())

    def total_queued(self) -> int:
        return sum(q.n for q in self.queues.values()) + sum(self._delayed_n.values())
