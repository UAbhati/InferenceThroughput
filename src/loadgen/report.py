"""Merge shard results into one report: measurements only, no projections."""
from __future__ import annotations

import os
import platform
import sys

import numpy as np

from common.scenario import Scenario
from gateway.stats import SERIES, percentile_from_hist

BUCKET_S = 0.01
WINDOW = int(60 / BUCKET_S)
SETTLE_S = 0.05  # shards apply a limit change within a tick or two of the master sending it


def _merge(arrs: list[np.ndarray]) -> np.ndarray:
    n = max((len(a) for a in arrs), default=1)
    out = np.zeros(n, np.int64)
    for a in arrs:
        out[: len(a)] += a
    return out


def rolling_60s(arr: np.ndarray) -> np.ndarray:
    c = np.concatenate(([0], np.cumsum(arr)))
    end = np.arange(1, len(c))
    return c[end] - c[np.maximum(end - WINDOW, 0)]


def judge_windows(arr: np.ndarray, initial: int, changes: list[tuple[float, int]]) -> dict:
    """Check every trailing-60s window against the limit in effect at its end.

    After a limit *reduction*, windows ending within the next 60s still contain traffic
    admitted under the old limit. Those windows are allowed up to max(new limit, old
    traffic still in the window): the system may not admit anything new on top of it
    while it drains. Returns the worst window and whether any window broke the rule.
    """
    cum = np.concatenate(([0], np.cumsum(arr)))
    end = np.arange(1, len(cum))
    win = cum[end] - cum[np.maximum(end - WINDOW, 0)]
    t_end = end * BUCKET_S
    times = np.array([c[0] for c in changes])
    limits = np.array([initial] + [c[1] for c in changes], dtype=float)
    allowed = limits[np.searchsorted(times, t_end, side="right")] if len(changes) else np.full(len(end), float(initial))
    prev = initial
    for t_c, new in changes:
        if new < prev:
            i0, i1 = int(t_c / BUCKET_S), int((t_c + SETTLE_S) / BUCKET_S)
            sel = (end >= i0) & (end < i1 + WINDOW)
            old_rem = cum[min(i1, len(cum) - 1)] - cum[np.maximum(end[sel] - WINDOW, 0)]
            allowed[sel] = np.maximum(allowed[sel], old_rem)
        prev = new
    ratio = win / allowed
    worst = int(np.argmax(ratio))
    return {"max_window": int(win.max()), "worst_ratio": float(ratio[worst]),
            "worst_at_s": float(t_end[worst]), "ok": bool(ratio.max() <= 1.0)}


def build_report(scn: Scenario, results: list[dict], changes_log: list[dict], procs: int, wall_s: float,
                 mode: str = "bulk (in-process simulated provider, wall clock)") -> dict:
    models = list(scn.models)
    steady_lo, steady_hi = int(scn.warmup_s), int(scn.duration_s)
    steady_s = max(steady_hi - steady_lo, 1)
    rep: dict = {"scenario": scn.name, "description": scn.description, "mode": mode,
                 "environment": {"python": sys.version.split()[0], "platform": platform.platform(),
                                 "cpu_count": os.cpu_count(), "shards": procs, "wall_s": round(wall_s, 1)},
                 "config": {"duration_s": scn.duration_s, "warmup_s": scn.warmup_s, "offered_rate_per_s": scn.load.rate_per_s if scn.load else None,
                            "mix": scn.load.mix if scn.load else None, "token_size": scn.load.token_size if scn.load else None, "engine": scn.engine.model_dump(),
                            "models": {m: s.model_dump() for m, s in scn.models.items()}},
                 "limit_changes": changes_log, "models": {}}
    tot = {k: 0 for k in SERIES}
    all_hist = None
    steady_completed = 0
    for m in models:
        series = {k: _merge([r["series"][m][k] for r in results]) for k in SERIES}
        hist = sum(r["hist"][m] for r in results)
        all_hist = hist if all_hist is None else all_hist + hist
        totals = {k: int(series[k].sum()) for k in SERIES}
        queued = sum(r["queued"][m] for r in results)
        inflight = sum(r["in_flight"][m] for r in results)
        accounted = totals["rejected"] + totals["expired"] + totals["completed"] + queued + inflight
        for k in SERIES:
            tot[k] += totals[k]
        spec = scn.models[m]
        ch = sorted((c["at_s"], c) for c in changes_log if c["model"] == m)
        rpm_changes = [(t, c["rpm"]) for t, c in ch if c.get("rpm")]
        tpm_changes = [(t, c["tpm"]) for t, c in ch if c.get("tpm")]
        areq, atok = _merge([r["audit_req"][m] for r in results if m in r["audit_req"]]), _merge([r["audit_tok"][m] for r in results if m in r["audit_tok"]])
        done_steady = int(series["completed"][steady_lo:steady_hi].sum())
        steady_completed += done_steady
        tok_steady = int(series["completed_tokens"][steady_lo:steady_hi].sum())
        has_change = bool(ch)
        rep["models"][m] = {
            "configured": {"rpm": spec.rpm, "tpm": spec.tpm},
            "totals": {**totals, "waiting_at_end": queued, "in_flight_at_end": inflight, "unaccounted": totals["submitted"] - accounted},
            "steady_state": {"window_s": [steady_lo, steady_hi], "completed": done_steady,
                             "observed_rpm": round(done_steady / steady_s * 60), "observed_tpm": round(tok_steady / steady_s * 60),
                             "rpm_utilisation": None if has_change else round(done_steady / steady_s * 60 / spec.rpm, 4),
                             "tpm_utilisation": None if has_change else round(tok_steady / steady_s * 60 / spec.tpm, 4)},
            "latency_s": {f"p{int(q * 100)}": round(percentile_from_hist(hist, q), 4) for q in (0.5, 0.95, 0.99)},
            "limit_audit": {"rpm": judge_windows(areq, spec.rpm, rpm_changes), "tpm": judge_windows(atok, spec.tpm, tpm_changes)},
            "timeline": [{"t": t + 1, "completed_per_s": int(series["completed"][t]), "rejected_per_s": int(series["rejected"][t]),
                          "expired_per_s": int(series["expired"][t]), "provider_rpm_trailing_60s": int(rolling_60s(areq)[min((t + 1) * 100 - 1, len(areq) - 1)]) if len(areq) else 0}
                         for t in range(int(scn.duration_s + scn.drain_s)) if t < len(series["completed"])],
        }
    rep["overall"] = {"totals": tot, "steady_completed_per_s": round(steady_completed / steady_s),
                      "latency_s": {f"p{int(q * 100)}": round(percentile_from_hist(all_hist, q), 4) for q in (0.5, 0.95, 0.99)},
                      "all_requests_accounted": all(v["totals"]["unaccounted"] == 0 for v in rep["models"].values()),
                      "all_limits_respected": all(v["limit_audit"][k]["ok"] for v in rep["models"].values() for k in ("rpm", "tpm"))}
    return rep


def render_markdown(rep: dict) -> str:
    o, env = rep["overall"], rep["environment"]
    t = o["totals"]
    lines = [f"# {rep['scenario']}", "", rep["description"], "",
             f"Mode: {rep['mode']}. {env['shards']} process(es) for the engine on {env['cpu_count']} CPUs, Python {env['python']}, {env['platform']}.", "",
             "## Overall (measured)", "",
             f"- Offered: {rep['config']['offered_rate_per_s'] or 0:,.0f} req/s for {rep['config']['duration_s']}s (warm-up {rep['config']['warmup_s']}s excluded from steady state)",
             f"- **Completed per second (steady state): {o['steady_completed_per_s']:,}**",
             f"- Submitted {t['submitted']:,}; completed {t['completed']:,} (succeeded {t['succeeded']:,}, failed {t['failed']:,}); "
             f"expired {t['expired']:,}; rejected {t['rejected']:,}; retried attempts {t['retried']:,}",
             f"- Latency p50/p95/p99: {o['latency_s']['p50']}s / {o['latency_s']['p95']}s / {o['latency_s']['p99']}s",
             f"- Every request accounted for: **{o['all_requests_accounted']}**; all 60s windows within limits: **{o['all_limits_respected']}**", ""]
    for m, v in rep["models"].items():
        s, a = v["steady_state"], v["limit_audit"]
        lines += [f"## {m}", "",
                  f"- Configured: {v['configured']['rpm']:,} RPM / {v['configured']['tpm']:,} TPM",
                  f"- Observed (steady): {s['observed_rpm']:,} RPM / {s['observed_tpm']:,} TPM"
                  + (f" ({s['rpm_utilisation']:.1%} / {s['tpm_utilisation']:.1%} of limit)" if s["rpm_utilisation"] is not None else " (limits changed during run; see timeline)"),
                  f"- Max 60s window at provider: {a['rpm']['max_window']:,} requests (worst ratio to limit in effect {a['rpm']['worst_ratio']:.4f}), "
                  f"{a['tpm']['max_window']:,} tokens (worst ratio {a['tpm']['worst_ratio']:.4f})",
                  f"- Latency p50/p95/p99: {v['latency_s']['p50']}s / {v['latency_s']['p95']}s / {v['latency_s']['p99']}s",
                  f"- Totals: {v['totals']}", ""]
    if rep.get("client"):
        c = rep["client"]
        lines += ["## Load generator (client side)", ""] + [f"- {k}: {v}" for k, v in c.items()] + [""]
    if rep.get("batch"):
        lines += ["## Batch and callback", ""] + [f"- {k}: {v}" for k, v in rep["batch"].items()] + [""]
    if rep.get("checks"):
        lines += ["## Pass criteria", ""] + [f"- [{'x' if v['ok'] else ' '}] {k}: {v['detail']}" for k, v in rep["checks"].items()] + [""]
    if rep["limit_changes"]:
        lines += ["## Limit changes applied while running", ""] + [f"- t={c['at_s']:.1f}s {c['model']}: rpm={c.get('rpm')} tpm={c.get('tpm')}" for c in rep["limit_changes"]] + [""]
    return "\n".join(lines)
