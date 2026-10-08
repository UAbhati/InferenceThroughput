"""Run statistics: per-model totals, per-second series and a latency histogram.

Latency is end-to-end: from entering the gateway queue until the provider call
reached a final state. Histograms use geometric bins (1% wide), so percentiles
from merged per-process histograms are accurate to about 1%.
"""
from __future__ import annotations

import math

import numpy as np

LOG_MIN = math.log(1e-4)
BIN_W = math.log(1.01)
NBINS = int((math.log(1e3) - LOG_MIN) / BIN_W) + 1  # 100us .. 1000s

SERIES = ("submitted", "rejected", "completed", "succeeded", "failed", "expired", "retried", "completed_tokens")


def latency_bins(lat: np.ndarray) -> np.ndarray:
    idx = ((np.log(np.maximum(lat, 1e-4)) - LOG_MIN) / BIN_W).astype(np.int64)
    return np.clip(idx, 0, NBINS - 1)


def percentile_from_hist(hist: np.ndarray, q: float) -> float:
    total = int(hist.sum())
    if total == 0:
        return float("nan")
    idx = int(np.searchsorted(np.cumsum(hist), q * total, side="left"))
    return math.exp(LOG_MIN + (min(idx, NBINS - 1) + 0.5) * BIN_W)


class StatsSink:
    def __init__(self, models, t0: float, horizon_s: float):
        self.t0 = t0
        self.n = int(horizon_s) + 2
        self.series = {m: {k: np.zeros(self.n, np.int64) for k in SERIES} for m in models}
        self.hist = {m: np.zeros(NBINS, np.int64) for m in models}

    def _i(self, now: float) -> int:
        return min(max(int(now - self.t0), 0), self.n - 1)

    def submitted(self, model, now, accepted, rejected, tokens):
        s, i = self.series[model], self._i(now)
        s["submitted"][i] += accepted + rejected
        s["rejected"][i] += rejected

    def expired(self, model, now, seq, enq):
        self.series[model]["expired"][self._i(now)] += len(seq)

    def retried(self, model, now, seq):
        self.series[model]["retried"][self._i(now)] += len(seq)

    def final(self, model, now, seq, tokens, enq, attempt, codes, done):
        s, i = self.series[model], self._i(now)
        n_ok = int((codes == 0).sum())
        s["completed"][i] += len(seq)
        s["succeeded"][i] += n_ok
        s["failed"][i] += len(seq) - n_ok
        s["completed_tokens"][i] += int(tokens.sum())
        self.hist[model] += np.bincount(latency_bins(done - enq), minlength=NBINS)
