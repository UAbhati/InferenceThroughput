"""Sliding-window RPM/TPM limiter.

Usage is tracked in small time buckets. The window is one bucket longer than the
nominal 60s so that *any* real 60s interval, which can touch window/bucket + 1
buckets, is covered. That makes the guarantee "no 60s period exceeds the limit"
strict, at the cost of ~bucket/window (0.17% at the defaults) lost capacity.

The limiter takes `now` as an argument, so it runs on wall-clock time in the
service and on any clock in tests. It is not thread-safe: one instance belongs
to one event loop / process.
"""
from __future__ import annotations

import numpy as np


class SlidingWindowLimiter:
    def __init__(self, rpm: int, tpm: int, window_s: float = 60.0, bucket_s: float = 0.1):
        self.rpm = rpm
        self.tpm = tpm
        self.window_s = window_s
        self.bucket_s = bucket_s
        self._n = int(round(window_s / bucket_s)) + 1
        self._req = [0] * self._n
        self._tok = [0] * self._n
        self._cur: int | None = None  # absolute index of the newest bucket
        self.used_req = 0
        self.used_tok = 0

    def set_limits(self, rpm: int | None = None, tpm: int | None = None) -> None:
        """Takes effect immediately. If usage already exceeds a reduced limit,
        nothing is admitted until the window drains below it."""
        if rpm is not None:
            self.rpm = rpm
        if tpm is not None:
            self.tpm = tpm

    def _advance(self, now: float) -> None:
        idx = int(now / self.bucket_s)
        if self._cur is None:
            self._cur = idx
            return
        if idx <= self._cur:  # same bucket, or clock went backwards: stay put
            return
        for i in range(1, min(idx - self._cur, self._n) + 1):
            slot = (self._cur + i) % self._n
            self.used_req -= self._req[slot]
            self.used_tok -= self._tok[slot]
            self._req[slot] = 0
            self._tok[slot] = 0
        self._cur = idx

    def remaining(self, now: float) -> tuple[int, int]:
        self._advance(now)
        return max(self.rpm - self.used_req, 0), max(self.tpm - self.used_tok, 0)

    def try_acquire(self, now: float, tokens: int) -> bool:
        self._advance(now)
        if self.used_req + 1 > self.rpm or self.used_tok + tokens > self.tpm:
            return False
        slot = self._cur % self._n
        self._req[slot] += 1
        self._tok[slot] += tokens
        self.used_req += 1
        self.used_tok += tokens
        return True

    def admit_chunk(self, now: float, tokens: np.ndarray) -> int:
        """Admit the longest prefix of `tokens` that fits both limits; return its length.

        FIFO: a large request is never skipped in favour of a later small one.
        """
        self._advance(now)
        rem_req, rem_tok = max(self.rpm - self.used_req, 0), max(self.tpm - self.used_tok, 0)
        if len(tokens) == 0 or rem_req == 0 or rem_tok == 0:
            return 0
        head = tokens[:rem_req]
        k = int(np.searchsorted(np.cumsum(head, dtype=np.int64), rem_tok, side="right"))
        if k:
            slot = self._cur % self._n
            t = int(head[:k].sum())
            self._req[slot] += k
            self._tok[slot] += t
            self.used_req += k
            self.used_tok += t
        return k

    def retry_after(self, now: float) -> float:
        """Seconds until the oldest used bucket leaves the window (a hint for 429s)."""
        self._advance(now)
        for age in range(self._n - 1, -1, -1):  # oldest bucket first
            slot = (self._cur - age) % self._n
            if self._req[slot]:
                return max((self._cur - age + self._n) * self.bucket_s - now, self.bucket_s)
        return 0.0
