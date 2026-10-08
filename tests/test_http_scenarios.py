"""End to end: real gateway + provider processes, real HTTP, small and fast."""
import asyncio

from common.scenario import Scenario
from loadgen.http_run import run_http


def test_batch_scenario_end_to_end(tmp_path):
    scn = Scenario.model_validate({
        "name": "mini-batch", "mode": "http", "kind": "batch", "duration_s": 60, "drain_s": 10,
        "models": {"a": {"rpm": 30000, "tpm": 10**9, "latency_ms_median": 20, "transient_failure_rate": 0.2, "permanent_failure_rate": 0.05},
                   "b": {"rpm": 30000, "tpm": 10**9, "latency_ms_median": 20}},
        "batch_job": {"total": 600, "mix": {"a": 0.5, "b": 0.5}, "callback_reject_first": 2},
        "gateway": {"callback_backoff_base_s": 0.1, "callback_backoff_cap_s": 0.3},
        "engine": {"queue_ttl_s": 30, "headroom": 0.98},
    })
    rep = asyncio.run(run_http(scn, tmp_path))
    failed = {k: v["detail"] for k, v in rep["checks"].items() if not v["ok"]}
    assert not failed, failed


def test_rate_scenario_with_limit_change_end_to_end(tmp_path):
    scn = Scenario.model_validate({
        "name": "mini-rate", "mode": "http", "kind": "generic", "duration_s": 12, "drain_s": 10,
        "models": {"a": {"rpm": 3000, "tpm": 10**9, "latency_ms_median": 20}},
        "load": {"rate_per_s": 100, "mix": {"a": 1.0}},
        "changes": [{"at_s": 5, "model": "a", "rpm": 600}],
        "engine": {"queue_max": 500, "queue_ttl_s": 5, "headroom": 0.98},
    })
    rep = asyncio.run(run_http(scn, tmp_path))
    assert rep["checks"]["every_request_accounted"]["ok"], rep["checks"]["every_request_accounted"]["detail"]
    assert rep["checks"]["no_60s_window_over_limit"]["ok"]
    assert [c["rpm"] for c in rep["limit_changes"]] == [600]
