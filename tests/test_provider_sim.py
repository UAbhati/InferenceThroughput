import numpy as np
from fastapi.testclient import TestClient

from common.config import ModelSpec
from provider_sim.app import create_app
from provider_sim.simulator import Outcome, ProviderSimulator


class Clock:
    t = 100.0

    def __call__(self):
        return self.t


def make(clock=None, **kw):
    clock = clock or Clock()
    spec = ModelSpec(rpm=kw.pop("rpm", 600), tpm=kw.pop("tpm", 10**9), latency_sigma=0, **kw)
    return ProviderSimulator({"m": spec}, clock=clock), clock


def test_rate_limits_above_rpm():
    sim, _ = make(rpm=10)
    results = [sim.call("m", 100).outcome for _ in range(15)]
    assert results.count(Outcome.RATE_LIMITED) == 5


def test_audit_agrees_with_limits():
    sim, clock = make(rpm=1000)
    for _ in range(300):
        clock.t += 0.1
        sim.call_chunk("m", np.full(200, 1000))
    v = sim.audit.violations("m", 1000, 10**9)
    assert v["rpm_ok"] and v["max_rpm_60s"] >= 990


def test_failure_rates_roughly_match():
    sim, _ = make(rpm=10**6, transient_failure_rate=0.2, permanent_failure_rate=0.1)
    k, codes, lat = sim.call_chunk("m", np.full(100_000, 10))
    assert k == 100_000
    assert abs((codes == 1).mean() - 0.2) < 0.01 and abs((codes == 2).mean() - 0.1) < 0.01


def test_http_inference_and_admin():
    sim, _ = make(rpm=2)
    c = TestClient(create_app(sim))
    body = {"request_id": "r", "model": "m", "estimated_tokens": 10}
    assert c.post("/inference", json=body).status_code == 200
    assert c.post("/inference", json=body).status_code == 200
    r = c.post("/inference", json=body)
    assert r.status_code == 429 and "retry-after" in r.headers
    assert c.put("/admin/models/m/limits", json={"rpm": 50}).json()["rpm"] == 50
    assert c.post("/inference", json={**body, "model": "nope"}).status_code == 404
    assert c.get("/admin/audit").json()["m"]["limits"]["rpm"] == 50
