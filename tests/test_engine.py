import numpy as np

from common.config import EngineConfig, ModelSpec
from gateway.engine import Engine
from gateway.stats import StatsSink
from loadgen.report import judge_windows
from provider_sim.inprocess import InProcessProvider
from provider_sim.simulator import ProviderSimulator


def build(spec, cfg, seed=1):
    now = [0.0]
    sim = ProviderSimulator({"m": spec}, seed=seed, clock=lambda: now[0])
    sink = StatsSink(["m"], 0.0, 400)
    return Engine({"m": spec}, InProcessProvider(sim), sink, cfg), sim, sink, now


def run(engine, now, offered_per_s, seconds, tokens=1000, dt=0.01, seq0=0):
    seq, carry = seq0, 0.0
    for i in range(int(seconds / dt)):
        now[0] = i * dt
        carry += offered_per_s * dt
        n, carry = int(carry), carry - int(carry)
        if n:
            engine.submit("m", now[0], np.arange(seq, seq + n), np.full(n, tokens))
            seq += n
        engine.step(now[0])
    return seq


def drain(engine, now, dt=0.01, upto=60):
    t = now[0]
    while (engine.total_in_flight() or engine.total_queued()) and t < now[0] + upto:
        t += dt
        engine.step(t)


def accounted(sink, engine, submitted):
    s = sink.series["m"]
    done = sum(int(s[k].sum()) for k in ("completed", "expired", "rejected"))
    return done + engine.queued("m") + engine.in_flight["m"] == submitted


def test_over_capacity_respects_limits_and_accounts_for_everything():
    spec = ModelSpec(rpm=3000, tpm=10**9, latency_ms_median=100, latency_sigma=0.3)
    engine, sim, sink, now = build(spec, EngineConfig(queue_max=500, queue_ttl_s=20))
    n = run(engine, now, offered_per_s=100, seconds=150)  # 6000 rpm offered vs 3000 allowed
    drain(engine, now)
    s = sink.series["m"]
    assert s["rejected"].sum() > 0 and s["expired"].sum() > 0  # bounded queue + ttl both exercised
    assert accounted(sink, engine, n)
    assert sim.audit.violations("m", 3000, 10**9)["rpm_ok"]
    done_after_warmup = s["completed"][60:150].sum()
    assert done_after_warmup >= 0.9 * 3000 * 90 / 60


def test_tpm_binds_for_large_requests():
    spec = ModelSpec(rpm=10**6, tpm=600_000, latency_ms_median=50, latency_sigma=0)
    engine, sim, sink, now = build(spec, EngineConfig(queue_max=10_000))
    run(engine, now, offered_per_s=50, seconds=130, tokens=2000)
    assert sim.audit.violations("m", 10**6, 600_000)["tpm_ok"]
    assert sim.audit.max_60s("m")[1] >= 0.9 * 600_000


def test_transient_failures_are_retried_then_fail():
    spec = ModelSpec(rpm=10**6, tpm=10**9, latency_ms_median=10, latency_sigma=0,
                     transient_failure_rate=0.5, permanent_failure_rate=0.1)
    engine, sim, sink, now = build(spec, EngineConfig(max_attempts=3))
    n = run(engine, now, offered_per_s=1000, seconds=5)
    now[0] += 5
    drain(engine, now)
    s = sink.series["m"]
    assert s["retried"].sum() > 0 and s["failed"].sum() > 0 and s["succeeded"].sum() > 0
    assert accounted(sink, engine, n) and engine.queued("m") == 0
    # P(success) = 0.4 + 0.5*0.4 + 0.25*0.4 = 0.7 with 3 attempts
    assert abs(s["succeeded"].sum() / n - 0.7) < 0.05


def test_limit_reduction_applies_without_restart_and_drains_correctly():
    spec = ModelSpec(rpm=6000, tpm=10**9, latency_ms_median=20, latency_sigma=0)
    engine, sim, sink, now = build(spec, EngineConfig(queue_max=100_000, queue_ttl_s=300))
    seq = run(engine, now, 200, 90)  # saturate at 6000 rpm
    for e in (engine, sim):  # reduce to 600 rpm at t=90 and keep going
        e.set_limits("m", rpm=600)
    seq = run_from(engine, now, 200, 90, 150, seq)
    arr = np.zeros(int(160 / 0.01) + 10, np.int64)
    arr[: len(sim.audit._req["m"])] = sim.audit._req["m"][: len(arr)]
    assert judge_windows(arr, 6000, [(90.0, 600)])["ok"]
    assert sim.audit._req["m"][int(150 / 0.01):int(160 / 0.01)].sum() <= 600 // 6 + 50  # new rate after drain


def run_from(engine, now, rate, t_from, t_to, seq, dt=0.01):
    carry = 0.0
    for i in range(int(t_from / dt), int(t_to / dt)):
        now[0] = i * dt
        carry += rate * dt
        n, carry = int(carry), carry - int(carry)
        if n:
            engine.submit("m", now[0], np.arange(seq, seq + n), np.full(n, 1000))
            seq += n
        engine.step(now[0])
    return seq


# ---- gotchas: oversized requests, retry storms, provider pushback ------------------------------------------------

class ScriptedProvider:
    """Answers every call with a fixed outcome 10 ms later and remembers when it was called."""

    def __init__(self, code):
        self.code, self.calls, self._pending = code, [], []

    def submit(self, model, now, chunk):
        self.calls.append((now, chunk[0].tolist()))
        self._pending.append((now + 0.01, chunk))
        return len(chunk[0])

    def poll(self, now):
        out, self._pending = [(t, c) for t, c in self._pending if t <= now], [(t, c) for t, c in self._pending if t > now]
        res = []
        for t, (seq, tok, enq, att) in out:
            n = len(seq)
            res.append(("m", seq, tok, enq, att, np.full(n, self.code, np.int8), np.full(n, t)))
        return res


class Collect(StatsSink):
    def __init__(self):
        super().__init__(["m"], 0.0, 100)
        self.finals = {}

    def final(self, model, now, seq, tokens, enq, attempt, codes, done):
        super().final(model, now, seq, tokens, enq, attempt, codes, done)
        for s, c in zip(seq.tolist(), codes.tolist()):
            self.finals[s] = c


def test_request_bigger_than_the_token_budget_fails_at_once_and_does_not_block_the_queue():
    spec = ModelSpec(rpm=60_000, tpm=5_000, latency_ms_median=10, latency_sigma=0)
    engine, sim, sink, now = build(spec, EngineConfig(queue_ttl_s=60, burst_s=60))
    engine.submit("m", 0.0, np.array([0, 1, 2]), np.array([50_000, 500, 500]))  # the first can never fit 5,000 tokens/min
    for i in range(300):
        now[0] = i * 0.01
        engine.step(now[0])
    s = sink.series["m"]
    assert s["failed"].sum() == 1 and s["succeeded"].sum() == 2 and s["expired"].sum() == 0


def test_lowering_the_token_limit_fails_queued_requests_that_no_longer_fit_instead_of_wedging():
    spec = ModelSpec(rpm=60_000, tpm=100_000, latency_ms_median=10, latency_sigma=0)
    engine, sim, sink, now = build(spec, EngineConfig(queue_ttl_s=60, burst_s=60))
    engine.set_limits("m", tpm=1_000)  # while 2,000-token requests are waiting
    engine.submit("m", 0.0, np.arange(5), np.full(5, 2_000))
    for i in range(100):
        now[0] = i * 0.01
        engine.step(now[0])
    assert sink.series["m"]["failed"].sum() == 5 and engine.queued("m") == 0


def test_transient_failures_are_retried_with_backoff_not_immediately():
    prov, sink = ScriptedProvider(1), Collect()  # TRANSIENT every time
    engine = Engine({"m": ModelSpec(rpm=60_000, tpm=10**9)}, prov, sink, EngineConfig(max_attempts=4, retry_backoff_s=0.25, burst_s=60))
    engine.submit("m", 0.0, np.array([7]), np.array([100]))
    for i in range(1000):
        engine.step(i * 0.01)
    times = [t for t, _ in prov.calls]
    assert len(times) == 4 and sink.finals[7] == 1  # four attempts, then it gives up
    gaps = [b - a for a, b in zip(times, times[1:])]
    assert gaps[0] >= 0.125 and gaps[1] >= 0.25 and gaps[2] >= 0.5  # 0.25 s doubling, jitter 0.5x-1.5x, plus the 10 ms call
    assert gaps[2] > gaps[0]


def test_provider_429_pauses_the_model_instead_of_hammering_and_uses_no_attempt():
    prov, sink = ScriptedProvider(3), Collect()  # RATE_LIMITED every time
    engine = Engine({"m": ModelSpec(rpm=60_000, tpm=10**9)}, prov, sink, EngineConfig(rate_limited_pause_s=0.5, max_attempts=1, burst_s=60))
    engine.submit("m", 0.0, np.array([1]), np.array([100]))
    for i in range(300):  # 3 simulated seconds
        engine.step(i * 0.01)
    assert 4 <= len(prov.calls) <= 8       # about one try per pause, not one per tick (that would be ~300)
    assert 1 not in sink.finals            # still waiting: a 429 never counts as a failed attempt
    assert engine.queued("m") == 1
