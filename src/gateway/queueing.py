"""FIFO queue of request chunks (struct-of-arrays), so thousands of requests move per operation.

A chunk is the tuple (seq, tokens, enqueued_at, attempt), four equal-length numpy arrays.
"""
from __future__ import annotations

from collections import deque

import numpy as np

Chunk = tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]


def _concat(parts: list[Chunk]) -> Chunk:
    if len(parts) == 1:
        return parts[0]
    return tuple(np.concatenate(cols) for cols in zip(*parts))  # type: ignore[return-value]


class ChunkQueue:
    def __init__(self) -> None:
        self._chunks: deque[Chunk] = deque()
        self.n = 0

    def push(self, chunk: Chunk) -> None:
        if len(chunk[0]):
            self._chunks.append(chunk)
            self.n += len(chunk[0])

    def push_front(self, chunk: Chunk) -> None:
        if len(chunk[0]):
            self._chunks.appendleft(chunk)
            self.n += len(chunk[0])

    def pop(self, max_n: int) -> Chunk | None:
        if max_n <= 0 or self.n == 0:
            return None
        parts, need = [], max_n
        while need and self._chunks:
            c = self._chunks.popleft()
            m = len(c[0])
            if m <= need:
                parts.append(c)
                need -= m
            else:
                parts.append(tuple(a[:need] for a in c))  # type: ignore[arg-type]
                self._chunks.appendleft(tuple(a[need:] for a in c))  # type: ignore[arg-type]
                need = 0
        self.n -= max_n - need
        return _concat(parts)

    def expire(self, cutoff: float) -> list[Chunk]:
        """Remove and return everything enqueued before `cutoff` (oldest are at the head)."""
        out: list[Chunk] = []
        while self._chunks:
            c = self._chunks[0]
            enq = c[2]
            if enq[0] >= cutoff:
                break
            if enq[-1] < cutoff:
                out.append(self._chunks.popleft())
                self.n -= len(enq)
            else:
                j = int(np.searchsorted(enq, cutoff, side="left"))
                out.append(tuple(a[:j] for a in c))  # type: ignore[arg-type]
                self._chunks[0] = tuple(a[j:] for a in c)  # type: ignore[assignment]
                self.n -= j
                break
        return out
