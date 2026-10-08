"""One shard of the bulk (in-process) benchmark.

Every shard runs a full gateway engine and provider simulator with 1/N of each
model's limits and 1/N of the offered load, on the wall clock. Shards share nothing
while running; the master merges their results.
"""
from __future__ import annotations

import time

import numpy as np

from common.scenario import Scenario
from gateway.engine import Engine
from gateway.stats import StatsSink
from provider_sim.inprocess import InProcessProvider
from provider_sim.simulator import ProviderSimulator


def share(total: int, idx: int, n: int) -> int:
    """Split `total` into n integer shares that sum exactly to total."""
    return max(total // n + (1 if idx < total % n else 0), 1)


class Generator:
    """Open-loop load: emits rate*dt requests per tick regardless of how the system keeps up."""

    def __init__(self, scn: Scenario, idx: int, n: int, t_start: float):
        self.rate = scn.load.rate_per_s / n
        weights = np.array([scn.load.mix[m] for m in scn.load.mix], dtype=float)
        self.models = list(scn.load.mix)
        self.weights = weights / weights.sum()
        self.base, self.jit = scn.load.token_size, scn.load.token_jitter
        self.rng = np.random.default_rng(1000 + idx)
        self.acc = {m: 0.0 for m in self.models}
        self.last = t_start
        self.next_seq = idx << 48

    def emit(self, now: float, engine: Engine) -> None:
        dt, self.last = now - self.last, now
        for m, w in zip(self.models, self.weights):
            self.acc[m] += self.rate * w * dt
            n = int(self.acc[m])
            if n <= 0:
                continue
            self.acc[m] -= n
            if self.jit:
                tok = self.rng.integers(int(self.base * (1 - self.jit)), int(self.base * (1 + self.jit)) + 1, n)
            else:
                tok = np.full(n, self.base, dtype=np.int64)
            seq = np.arange(self.next_seq, self.next_seq + n, dtype=np.int64)
            self.next_seq += n
            engine.submit(m, now, seq, tok)


def worker_main(idx: int, n: int, scn_json: str, start_epoch: float, conn) -> None:
    scn = Scenario.model_validate_json(scn_json)
    t0 = time.monotonic() + (start_epoch - time.time())  # common start across processes
    specs = {m: s.model_copy(update={"rpm": share(s.rpm, idx, n), "tpm": share(s.tpm, idx, n)})
             for m, s in scn.models.items()}
    cur = [0.0]
    sim = ProviderSimulator(specs, seed=idx + 1, clock=lambda: cur[0])
    sim.audit.t0 = t0
    sink = StatsSink(specs, t0, scn.duration_s + scn.drain_s)
    engine = Engine(specs, InProcessProvider(sim), sink, scn.engine)
    gen = Generator(scn, idx, n, t0)
    end, hard_end, tick = t0 + scn.duration_s, t0 + scn.duration_s + scn.drain_s, scn.engine.tick_s
    conn.send(("ready", idx))
    while True:
        now = time.monotonic()
        while conn.poll():
            kind, model, rpm, tpm = conn.recv()
            cur[0] = now
            sim.set_limits(model, share(rpm, idx, n) if rpm else None, share(tpm, idx, n) if tpm else None)
            engine.set_limits(model, share(rpm, idx, n) if rpm else None, share(tpm, idx, n) if tpm else None)
        if now < t0:
            time.sleep(min(t0 - now, 0.05))
            continue
        cur[0] = now
        if now < end:
            gen.emit(now, engine)
        engine.step(now)
        if now >= end and (engine.total_in_flight() == 0 and (engine.total_queued() == 0 or now >= hard_end - 1e-9)):
            break
        if now >= hard_end + 5:  # safety net
            break
        spare = tick - (time.monotonic() - now)
        if spare > 0:
            time.sleep(spare)
    conn.send(("result", {
        "series": sink.series, "hist": sink.hist,
        "queued": {m: engine.queued(m) for m in specs},
        "in_flight": dict(engine.in_flight),
        "audit_req": {m: a for m, a in sim.audit._req.items()},
        "audit_tok": {m: a for m, a in sim.audit._tok.items()},
        "provider_counts": sim.counts,
    }))
    conn.close()
