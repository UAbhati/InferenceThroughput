"""Independent usage audit for the provider simulator.

Counts every call the provider accepted in 10ms buckets and computes the maximum
usage over any 60s span. It shares no code with the gateway's limiter, so it can
catch the gateway (or the simulator itself) exceeding a limit.
"""
from __future__ import annotations

import numpy as np

BUCKET_S = 0.01
WINDOW_BUCKETS = int(60 / BUCKET_S)


class UsageAudit:
    def __init__(self, t0: float):
        self.t0 = t0
        self._req: dict[str, np.ndarray] = {}
        self._tok: dict[str, np.ndarray] = {}
        self.limit_changes: dict[str, list[tuple[float, int, int]]] = {}

    def _ensure(self, model: str, size: int) -> None:
        cur = self._req.get(model)
        if cur is None:
            self._req[model] = np.zeros(max(size, 8192), dtype=np.int64)
            self._tok[model] = np.zeros(max(size, 8192), dtype=np.int64)
        elif size > len(cur):
            new = max(size, len(cur) * 2)
            self._req[model] = np.pad(cur, (0, new - len(cur)))
            self._tok[model] = np.pad(self._tok[model], (0, new - len(cur)))

    def record(self, model: str, now: float, requests: int, tokens: int) -> None:
        idx = max(int((now - self.t0) / BUCKET_S), 0)
        self._ensure(model, idx + 1)
        self._req[model][idx] += requests
        self._tok[model][idx] += tokens

    def record_limits(self, model: str, now: float, rpm: int, tpm: int) -> None:
        self.limit_changes.setdefault(model, []).append((now - self.t0, rpm, tpm))

    def _rolling(self, arr: np.ndarray) -> np.ndarray:
        c = np.concatenate(([0], np.cumsum(arr)))
        end = np.arange(1, len(c))
        return c[end] - c[np.maximum(end - WINDOW_BUCKETS, 0)]

    def max_60s(self, model: str) -> tuple[int, int]:
        if model not in self._req:
            return 0, 0
        return int(self._rolling(self._req[model]).max()), int(self._rolling(self._tok[model]).max())

    def timeline(self, model: str, step_s: float = 1.0) -> list[dict]:
        """Per-step accepted requests/tokens plus the trailing-60s totals at each step end."""
        if model not in self._req:
            return []
        roll_r, roll_t = self._rolling(self._req[model]), self._rolling(self._tok[model])
        per = int(step_s / BUCKET_S)
        out = []
        for end in range(per, len(roll_r) + 1, per):
            lo = end - per
            out.append({
                "t": round(end * BUCKET_S, 3),
                "requests": int(self._req[model][lo:end].sum()),
                "tokens": int(self._tok[model][lo:end].sum()),
                "rpm_trailing_60s": int(roll_r[end - 1]),
                "tpm_trailing_60s": int(roll_t[end - 1]),
            })
        return out

    def violations(self, model: str, rpm: int, tpm: int) -> dict:
        r, t = self.max_60s(model)
        return {"max_rpm_60s": r, "max_tpm_60s": t, "rpm_ok": r <= rpm, "tpm_ok": t <= tpm}
