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
