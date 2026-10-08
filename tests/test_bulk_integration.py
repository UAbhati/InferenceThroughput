from common.scenario import Scenario
from loadgen.bulk import run_scenario


def test_sharded_run_with_runtime_limit_change():
    scn = Scenario.model_validate({
        "name": "t", "duration_s": 8, "drain_s": 6,
        "models": {"a": {"rpm": 6000, "tpm": 10**9, "latency_ms_median": 50},
                   "b": {"rpm": 6000, "tpm": 10**9, "latency_ms_median": 50}},
        "load": {"rate_per_s": 400, "mix": {"a": 0.5, "b": 0.5}},
        "changes": [{"at_s": 4, "model": "a", "rpm": 600}, {"at_s": 6, "model": "a", "rpm": 6000}],
        "engine": {"queue_max": 5000, "queue_ttl_s": 5},
    })
    rep = run_scenario(scn, procs=2)
    assert rep["overall"]["all_requests_accounted"]
    assert rep["overall"]["all_limits_respected"]
    assert len(rep["limit_changes"]) == 2
    assert rep["models"]["b"]["totals"]["completed"] > 0
