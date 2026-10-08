"""Core of the provider simulation: limits, latency and outcomes per model.

Transport-agnostic. The HTTP app (provider_sim.app) and the in-process bulk
benchmark both use it. The provider enforces its own RPM/TPM limits, so a
gateway that oversends gets 429s rather than silently passing.
"""
from __future__ import annotations

import enum
import random
import time
from dataclasses import dataclass
from typing import Callable

import numpy as np

from common.config import ModelSpec
from gateway.limiter import SlidingWindowLimiter
from provider_sim.audit import UsageAudit


class Outcome(str, enum.Enum):
    SUCCESS = "success"
    TRANSIENT_FAILURE = "transient_failure"  # retryable (HTTP 503)
    PERMANENT_FAILURE = "permanent_failure"  # not retryable (HTTP 422)
    RATE_LIMITED = "rate_limited"  # provider-side limit hit (HTTP 429)


@dataclass(slots=True)
class CallResult:
    outcome: Outcome
    latency_s: float
    retry_after_s: float = 0.0


class ProviderSimulator:
    def __init__(self, models: dict[str, ModelSpec], seed: int = 0,
                 clock: Callable[[], float] = time.monotonic):
        self.clock = clock
        self.specs = dict(models)
        # The provider's limiter is the source of truth for what it accepts.
        self._limiters = {m: SlidingWindowLimiter(s.rpm, s.tpm) for m, s in models.items()}
        self.audit = UsageAudit(clock())
        self._rng = random.Random(seed)
        self._np_rng = np.random.default_rng(seed)
        self.counts: dict[str, dict[str, int]] = {m: {o.value: 0 for o in Outcome} for m in models}
        for m, s in models.items():
            self.audit.record_limits(m, clock(), s.rpm, s.tpm)

    def reset(self) -> None:
        """Fresh limiter windows, audit and counters; limits and behaviour are kept."""
        self._limiters = {m: SlidingWindowLimiter(s.rpm, s.tpm) for m, s in self.specs.items()}
        self.audit = UsageAudit(self.clock())
        self.counts = {m: {o.value: 0 for o in Outcome} for m in self.specs}
        for m, s in self.specs.items():
            self.audit.record_limits(m, self.clock(), s.rpm, s.tpm)

    def set_limits(self, model: str, rpm: int | None = None, tpm: int | None = None) -> ModelSpec:
        spec = self.specs[model]
        updated = spec.model_copy(update={k: v for k, v in (("rpm", rpm), ("tpm", tpm)) if v})
        self.specs[model] = updated
        self._limiters[model].set_limits(updated.rpm, updated.tpm)
        self.audit.record_limits(model, self.clock(), updated.rpm, updated.tpm)
        return updated

    def _latency(self, spec: ModelSpec) -> float:
        median = spec.latency_ms_median / 1000
        if spec.latency_sigma <= 0:
            return median
        return self._rng.lognormvariate(np.log(median), spec.latency_sigma)

    def call(self, model: str, tokens: int) -> CallResult:
        """Decide the outcome of one call at the current clock time.

        Latency is returned, not slept; the caller decides how to wait for it.
        """
        spec, now = self.specs[model], self.clock()
        limiter = self._limiters[model]
        if not limiter.try_acquire(now, tokens):
            self.counts[model][Outcome.RATE_LIMITED.value] += 1
            return CallResult(Outcome.RATE_LIMITED, 0.0, limiter.retry_after(now))
        self.audit.record(model, now, 1, tokens)
        roll = self._rng.random()
        if roll < spec.permanent_failure_rate:
            outcome = Outcome.PERMANENT_FAILURE
        elif roll < spec.permanent_failure_rate + spec.transient_failure_rate:
            outcome = Outcome.TRANSIENT_FAILURE
        else:
            outcome = Outcome.SUCCESS
        self.counts[model][outcome.value] += 1
        return CallResult(outcome, self._latency(spec))

    def call_chunk(self, model: str, tokens: np.ndarray):
        """Vectorised call for the bulk benchmark.

        Returns (accepted, outcome_codes, latencies_s): the first `accepted` requests
        passed the provider's limiter; the rest were rate limited. Codes index `Outcome`
        in definition order (0=success, 1=transient, 2=permanent).
        """
        spec, now = self.specs[model], self.clock()
        k = self._limiters[model].admit_chunk(now, tokens)
        rejected = len(tokens) - k
        if rejected:
            self.counts[model][Outcome.RATE_LIMITED.value] += rejected
        if k == 0:
            return 0, np.empty(0, np.int8), np.empty(0)
        self.audit.record(model, now, k, int(tokens[:k].sum()))
        roll = self._np_rng.random(k)
        codes = np.zeros(k, np.int8)
        codes[roll < spec.permanent_failure_rate + spec.transient_failure_rate] = 1
        codes[roll < spec.permanent_failure_rate] = 2
        for code, outcome in enumerate((Outcome.SUCCESS, Outcome.TRANSIENT_FAILURE, Outcome.PERMANENT_FAILURE)):
            self.counts[model][outcome.value] += int((codes == code).sum())
        if spec.latency_sigma > 0:
            lat = self._np_rng.lognormal(np.log(spec.latency_ms_median / 1000), spec.latency_sigma, k)
        else:
            lat = np.full(k, spec.latency_ms_median / 1000)
        return k, codes, lat
