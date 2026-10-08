"""Run a scenario against the real stack: provider simulator and gateway as separate uvicorn
processes, a load generator and a callback receiver in this process.

Everything measured here comes from the running services: latency and states from the gateway's
own records, rate-limit compliance from the provider's independent audit.
"""
from __future__ import annotations

import asyncio
import os
import random
import socket
import subprocess
import sys
import time
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

import aiohttp
import httpx
import numpy as np
import uvicorn
import yaml

from common.config import ModelsConfig
from common.scenario import Scenario
from gateway.stats import SERIES
from loadgen.callback_sink import create_sink
from loadgen.report import build_report


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def auth_headers() -> dict[str, str]:
    """Bearer token for the gateway's admin endpoints when ADMIN_TOKEN is set (the services inherit the same variable)."""
    token = os.environ.get("ADMIN_TOKEN")
    return {"Authorization": f"Bearer {token}"} if token else {}


def pct(values: list[float], q: float) -> float:
    return round(float(np.percentile(values, q)), 4) if values else float("nan")


class Stack:
    """Provider simulator + gateway as subprocesses. The gateway can be killed and started again on its own."""

    def __init__(self, scn: Scenario, out: Path, gateway_port: int | None = None, provider_port: int | None = None):
        cfg = ModelsConfig(models=scn.models, engine=scn.engine, gateway=scn.gateway)
        self.out = out
        self.models_file = out / "models.yaml"
        self.models_file.write_text(yaml.safe_dump(cfg.model_dump(mode="json")))
        self.pp, self.gp = provider_port or free_port(), gateway_port or free_port()
        self.env = {**os.environ, "PROVIDER_MODELS": str(self.models_file), "GATEWAY_MODELS": str(self.models_file),
                    "PROVIDER_URL": f"http://127.0.0.1:{self.pp}", "GATEWAY_PUBLIC_URL": f"http://127.0.0.1:{self.gp}"}
        self.provider: subprocess.Popen | None = None
        self.gateway: subprocess.Popen | None = None
        self._gateway_starts = 0

    @property
    def urls(self) -> tuple[str, str]:
        return f"http://127.0.0.1:{self.gp}", f"http://127.0.0.1:{self.pp}"

    def _spawn(self, name: str, app: str, port: int) -> subprocess.Popen:
        base = [sys.executable, "-m", "uvicorn", "--host", "127.0.0.1", "--log-level", "warning", "--no-access-log"]
        log = open(self.out / f"{name}.log", "a")
        return subprocess.Popen(base + [app, "--port", str(port)], env=self.env, stdout=log, stderr=log)

    def _wait_healthy(self, port: int, timeout: float = 30) -> None:
        end = time.time() + timeout
        while time.time() < end:
            try:
                if httpx.get(f"http://127.0.0.1:{port}/healthz", timeout=1).status_code == 200:
                    return
            except httpx.HTTPError:
                time.sleep(0.2)
        raise RuntimeError(f"service on port {port} did not start; see logs in {self.out}")

    def start(self) -> "Stack":
        self.provider = self._spawn("provider", "provider_sim.app:app", self.pp)
        self.start_gateway()
        self._wait_healthy(self.pp)
        return self

    def start_gateway(self) -> None:
        self._gateway_starts += 1
        self.gateway = self._spawn("gateway" if self._gateway_starts == 1 else f"gateway-restart{self._gateway_starts - 1}",
                                   "gateway.app:app", self.gp)
        self._wait_healthy(self.gp)

    def kill_gateway(self) -> None:
        """SIGKILL: no shutdown hooks, no final flush, exactly like a crash."""
        self.gateway.kill()
        self.gateway.wait(timeout=10)

    def stop(self) -> None:
        procs = [p for p in (self.gateway, self.provider) if p and p.poll() is None]
        for p in procs:
            p.terminate()
        for p in procs:
            try:
                p.wait(timeout=10)
            except subprocess.TimeoutExpired:
                p.kill()


@contextmanager
def services(scn: Scenario, out: Path):
    """Start provider + gateway subprocesses with this scenario's models; stop them on exit."""
    stack = Stack(scn, out).start()
    try:
        yield stack.urls
    finally:
        stack.stop()


class RateLoad:
    """Open-loop client: sends at the configured rate whether or not the service keeps up."""

    def __init__(self, scn: Scenario, gw: aiohttp.ClientSession):
        self.scn, self.gw = scn, gw
        self.sent = self.accepted = self.rejected = self.errors = 0
        self.ack: list[float] = []
        self.ids: list[str] = []
        self.rng = random.Random(7)
        self._tasks: set[asyncio.Task] = set()

    async def _send(self, items: list[dict]) -> None:
        t = time.perf_counter()
        try:
            if len(items) == 1:
                resp = self.gw.post("/v1/requests", json=items[0])
            else:
                resp = self.gw.post("/v1/batches", json={"requests": items})
            async with resp as r:
                await r.read()
                status = r.status
            self.ack.append(time.perf_counter() - t)
            if status in (200, 202):
                self.accepted += len(items)
            elif status == 429:
                self.rejected += len(items)
            else:
                self.errors += len(items)
        except (aiohttp.ClientError, asyncio.TimeoutError, OSError):
            self.errors += len(items)

    async def run(self) -> None:
        load, scn = self.scn.load, self.scn
        models, weights = list(load.mix), list(load.mix.values())
        t0, n, tick, pending = time.perf_counter(), 0, 0.005, []
        while (el := time.perf_counter() - t0) < scn.duration_s:
            due = int(load.rate_per_s * el) - n
            for _ in range(due):
                m = self.rng.choices(models, weights)[0]
                tok = load.token_size if not load.token_jitter else int(load.token_size * self.rng.uniform(1 - load.token_jitter, 1 + load.token_jitter))
                pending.append({"request_id": f"r{n}", "model": m, "estimated_tokens": tok})
                self.ids.append(f"r{n}")
                n += 1
                if len(pending) >= load.batch_size:
                    self._spawn(pending)
                    pending = []
            await asyncio.sleep(tick)
        if pending:
            self._spawn(pending)
        self.sent = n
        await asyncio.gather(*self._tasks)

    def _spawn(self, items: list[dict]) -> None:
        t = asyncio.get_running_loop().create_task(self._send(items))
        self._tasks.add(t)
        t.add_done_callback(self._tasks.discard)


async def apply_changes(scn: Scenario, t_ref: float, gw: httpx.AsyncClient, pv: httpx.AsyncClient, log: list[dict]) -> None:
    """Change limits on the running services. Raise the provider first and lower the gateway first,
    so the gateway is never allowed more than the provider will accept."""
    current = {m: (s.rpm, s.tpm) for m, s in scn.models.items()}
    for ch in sorted(scn.changes, key=lambda c: c.at_s):
        await asyncio.sleep(max(t_ref + ch.at_s - time.time(), 0))
        body = {k: v for k, v in (("rpm", ch.rpm), ("tpm", ch.tpm)) if v}
        increase = (ch.rpm or 0) > current[ch.model][0] or (ch.tpm or 0) > current[ch.model][1]
        for client in ((pv, gw) if increase else (gw, pv)):
            r = await client.put(f"/admin/models/{ch.model}/limits", json=body)
            r.raise_for_status()
        current[ch.model] = (ch.rpm or current[ch.model][0], ch.tpm or current[ch.model][1])
        log.append({"at_s": round(time.time() - t_ref, 3), "model": ch.model, "rpm": ch.rpm, "tpm": ch.tpm})


async def wait_idle(gw: httpx.AsyncClient, timeout: float) -> bool:
    end = time.time() + timeout
    while time.time() < end:
        st = (await gw.get("/admin/status")).json()
        busy = sum(m["queued"] + m["in_flight"] for m in st["models"].values())
        if busy == 0 and st["active_batches"] == 0:
            return True
        await asyncio.sleep(0.5)
    return False


async def verify_accounting(gw: httpx.AsyncClient, ids: list[str]) -> dict:
    counts: dict[str, int] = {}
    for i in range(0, len(ids), 5000):
        r = await gw.post("/v1/requests/status", json={"ids": ids[i:i + 5000]})
        for st in r.json()["states"].values():
            counts[st] = counts.get(st, 0) + 1
    return counts


async def collect(gw: httpx.AsyncClient, pv: httpx.AsyncClient, scn: Scenario) -> dict:
    stats = (await gw.get("/admin/stats")).json()
    raw = (await pv.get("/admin/audit/raw")).json()
    models = list(scn.models)
    return {"series": {m: {k: np.array(stats["series"][m][k], dtype=np.int64) for k in SERIES} for m in models},
            "hist": {m: np.array(stats["hist"][m], dtype=np.int64) for m in models},
            "queued": {m: stats["queued"][m] + stats["backlog"][m] for m in models},
            "in_flight": stats["in_flight"],
            "audit_req": {m: np.array(v["requests"], dtype=np.int64) for m, v in raw.items()},
            "audit_tok": {m: np.array(v["tokens"], dtype=np.int64) for m, v in raw.items()}}


# ---------------------------------------------------------------------------------------------------------------
async def run_rate_scenario(scn: Scenario, gw_url: str, pv_url: str) -> dict:
    async with httpx.AsyncClient(base_url=gw_url, timeout=60, headers=auth_headers()) as gw, httpx.AsyncClient(base_url=pv_url, timeout=60) as pv, \
            aiohttp.ClientSession(gw_url, timeout=aiohttp.ClientTimeout(total=60), connector=aiohttp.TCPConnector(limit=2000)) as sender:
        await gw.post("/admin/reset")
        await pv.post("/admin/reset")
        t_ref, changes_log = time.time(), []
        load = RateLoad(scn, sender)
        await asyncio.gather(load.run(), apply_changes(scn, t_ref, gw, pv, changes_log))
        idle = await wait_idle(gw, scn.drain_s + scn.engine.queue_ttl_s + 30)
        wall = time.time() - t_ref
        states = await verify_accounting(gw, load.ids)
        res = await collect(gw, pv, scn)
        status = (await gw.get("/admin/status")).json()
    rep = build_report(scn, [res], changes_log, 1, wall, mode="http (gateway and provider simulator as separate processes, wall clock)")
    rep["client"] = {"sent": load.sent, "accepted_202": load.accepted, "rejected_429": load.rejected, "client_errors": load.errors,
                     "achieved_send_rate_per_s": round(load.sent / scn.duration_s), "ack_latency_s_p50_p95_p99":
                         [pct(load.ack, 50), pct(load.ack, 95), pct(load.ack, 99)],
                     "gateway_idle_after_run": idle, "final_state_of_every_sent_id": states}
    rep["checks"] = rate_checks(scn, rep, load, states)
    rep["gateway_status"] = status
    return rep


def rate_checks(scn: Scenario, rep: dict, load: RateLoad, states: dict) -> dict:
    checks = {}
    unknown = states.get("unknown", 0)
    waiting = states.get("queued", 0) + states.get("in_flight", 0)
    checks["every_request_accounted"] = {
        "ok": unknown == load.errors and sum(states.values()) == load.sent and rep["overall"]["all_requests_accounted"],
        "detail": f"{load.sent:,} sent -> {states} (unknown ids = client errors = {load.errors}); waiting at end: {waiting}"}
    checks["no_60s_window_over_limit"] = {"ok": rep["overall"]["all_limits_respected"],
                                          "detail": "worst window/limit ratios: " + ", ".join(
                                              f"{m} rpm {v['limit_audit']['rpm']['worst_ratio']:.3f} tpm {v['limit_audit']['tpm']['worst_ratio']:.3f}" for m, v in rep["models"].items())}
    done = lambda m, lo, hi: int(np.sum(rep["models"][m]["timeline"] and [x["completed_per_s"] for x in rep["models"][m]["timeline"] if lo < x["t"] <= hi]))
    if scn.kind == "capacity":
        for m, v in rep["models"].items():
            u = v["steady_state"]["rpm_utilisation"]
            checks[f"{m}_completes_90pct_of_capacity_after_warmup"] = {"ok": u is not None and u >= 0.9,
                                                                       "detail": f"{v['steady_state']['observed_rpm']:,} RPM of {v['configured']['rpm']:,} = {u:.1%} (steady window {v['steady_state']['window_s']}s)"}
    if scn.kind == "limit_changes":
        edges = lambda m: sorted([(0.0, scn.models[m].rpm)] + [(c.at_s, c.rpm) for c in scn.changes if c.model == m and c.rpm]) + [(scn.duration_s, None)]
        for m in rep["models"]:
            segs = edges(m)
            if len(segs) == 2:  # never changed: plain steady-state use of capacity
                u = rep["models"][m]["steady_state"]["rpm_utilisation"]
                checks[f"{m}_unaffected_and_busy"] = {"ok": u >= 0.9, "detail": f"{u:.1%} of its {scn.models[m].rpm:,} RPM, throughout the run incl. while the other model was constrained"}
                continue
            for (t0, lim), (t1, _) in zip(segs, segs[1:]):
                prev = scn.models[m].rpm if t0 == 0 else None
                lo = t0 + (65 if lim < max(l for _, l in segs[:-1]) else 15)  # a reduction needs ~60s for old traffic to age out
                lo = min(lo, t1 - 10)
                rate = done(m, lo, t1) / (t1 - lo) * 60
                checks[f"{m}_follows_limit_{lim}_from_{t0:.0f}s"] = {"ok": 0.8 * lim <= rate <= 1.05 * lim,
                                                                      "detail": f"{rate:,.0f} completed/min over [{lo:.0f}s,{t1:.0f}s] vs limit {lim:,}"}
    return checks


async def run_batch_scenario(scn: Scenario, gw_url: str, pv_url: str) -> dict:
    job, rng = scn.batch_job, random.Random(11)
    sink = create_sink(job.callback_reject_first)
    sp = free_port()
    server = uvicorn.Server(uvicorn.Config(sink, host="127.0.0.1", port=sp, log_level="warning"))
    server_task = asyncio.get_running_loop().create_task(server.serve())
    while not server.started:
        await asyncio.sleep(0.05)
    models, weights = list(job.mix), list(job.mix.values())
    items = [{"request_id": f"b{i}", "model": rng.choices(models, weights)[0], "estimated_tokens": job.token_size} for i in range(job.total)]
    async with httpx.AsyncClient(base_url=gw_url, timeout=60, headers=auth_headers()) as gw, httpx.AsyncClient(base_url=pv_url, timeout=60) as pv:
        await gw.post("/admin/reset")
        await pv.post("/admin/reset")
        t_ref = time.time()
        t0 = time.perf_counter()
        r = await gw.post("/v1/batches", json={"requests": items, "callback_url": f"http://127.0.0.1:{sp}/callback"})
        ack_s, t_ack = time.perf_counter() - t0, time.time()
        r.raise_for_status()
        bid = r.json()["batch_id"]
        progress, view = [], {}
        deadline = time.time() + scn.duration_s
        while time.time() < deadline:
            view = (await gw.get(f"/v1/batches/{bid}")).json()
            progress.append({"t": round(time.time() - t_ack, 2), "final": view["total"] - sum(view["pending"].values()),
                             "callback": view["callback"]["status"]})
            if view["callback"]["status"] in ("delivered", "failed"):
                break
            await asyncio.sleep(0.25)
        view = (await gw.get(f"/v1/batches/{bid}")).json()
        ids, offset, states = [], 0, {}
        while offset is not None:
            page = (await gw.get(f"/v1/batches/{bid}/results", params={"offset": offset, "limit": 5000})).json()
            for x in page["results"]:
                ids.append(x["request_id"])
                states[x["state"]] = states.get(x["state"], 0) + 1
            offset = page["next_offset"]
        await wait_idle(gw, 30)
        res = await collect(gw, pv, scn)
        wall = time.time() - t_ref
    server.should_exit = True
    await server_task
    attempts = sink.state.attempts
    ok_attempts = [a for a in attempts if a["status_code"] == 200]
    completed_at = datetime.fromisoformat(view["completed_at"]).timestamp() if view.get("completed_at") else None
    summary_keys = ("batch_id", "status", "total", "succeeded", "failed", "expired")
    cb = ok_attempts[0]["body"] if ok_attempts else {}
    rep = build_report(scn, [res], [], 1, wall, mode="http (gateway and provider simulator as separate processes, wall clock)")
    rep["batch"] = {
        "requests": job.total, "ack_latency_s": round(ack_s, 3),
        "time_from_ack_to_all_final_s": None if completed_at is None else round(completed_at - t_ack, 2),
        "callback_attempts": [{"at_s_after_ack": round(a["t"] - t_ack, 2), "status_code": a["status_code"]} for a in attempts],
        "callback_final_status": view["callback"]["status"], "batch_status": view["status"],
        "summary": {k: view[k] for k in summary_keys}, "request_states": states, "progress_samples": progress[::4]}
    n_ids = len(ids)
    rep["checks"] = {
        "ack_within_1s": {"ok": ack_s < 1.0, "detail": f"{ack_s * 1000:.0f} ms for {job.total:,} requests"},
        "callback_sent_only_after_all_final": {"ok": bool(attempts) and completed_at is not None and min(a["t"] for a in attempts) >= completed_at - 0.01,
                                               "detail": f"all-final at +{(completed_at - t_ack) if completed_at else float('nan'):.2f}s, first callback attempt at +{(attempts[0]['t'] - t_ack) if attempts else float('nan'):.2f}s"},
        "first_attempts_rejected_then_delivered": {"ok": view["callback"]["status"] == "delivered" and len(attempts) == job.callback_reject_first + 1 and [a["status_code"] for a in attempts][-1] == 200,
                                                   "detail": f"attempt status codes {[a['status_code'] for a in attempts]}, gateway reports '{view['callback']['status']}' after {view['callback']['attempts']} attempts"},
        "callback_summary_matches_batch_status": {"ok": bool(cb) and all(cb.get(k) == view[k] for k in summary_keys),
                                                  "detail": f"callback {({k: cb.get(k) for k in summary_keys})} vs API {({k: view[k] for k in summary_keys})}"},
        "every_request_id_exactly_once": {"ok": n_ids == job.total and set(ids) == {i["request_id"] for i in items},
                                          "detail": f"{n_ids:,} result rows, {len(set(ids)):,} distinct ids, {job.total:,} submitted"},
        "transient_and_permanent_failures_present": {"ok": states.get("failed", 0) > 0 and rep["overall"]["totals"]["retried"] > 0,
                                                     "detail": f"states {states}; retry attempts {rep['overall']['totals']['retried']:,}"},
        "no_60s_window_over_limit": {"ok": rep["overall"]["all_limits_respected"], "detail": "provider audit"},
    }
    return rep


async def run_recovery_scenario(scn: Scenario, out: Path) -> dict:
    """kill -9 the gateway in the middle of a batch, start a new one, and check that nothing is lost or duplicated."""
    if not os.environ.get("DATABASE_URL"):
        raise SystemExit("the recovery scenario needs Postgres: docker compose up -d, then set DATABASE_URL (see .env.example)")
    job, rng = scn.batch_job, random.Random(13)
    stack = Stack(scn, out).start()
    sink = create_sink(job.callback_reject_first)
    sp = free_port()
    server = uvicorn.Server(uvicorn.Config(sink, host="127.0.0.1", port=sp, log_level="warning"))
    server_task = asyncio.get_running_loop().create_task(server.serve())
    try:
        while not server.started:
            await asyncio.sleep(0.05)
        gw_url, pv_url = stack.urls
        models, weights = list(job.mix), list(job.mix.values())
        items = [{"request_id": f"k{i}", "model": rng.choices(models, weights)[0], "estimated_tokens": job.token_size} for i in range(job.total)]
        async with httpx.AsyncClient(base_url=gw_url, timeout=60, headers=auth_headers()) as gw, httpx.AsyncClient(base_url=pv_url, timeout=60) as pv:
            await gw.post("/admin/reset")
            await pv.post("/admin/reset")
            t_ref = time.time()
            t0 = time.perf_counter()
            r = await gw.post("/v1/batches", json={"requests": items, "callback_url": f"http://127.0.0.1:{sp}/callback"})
            ack_s = time.perf_counter() - t0
            r.raise_for_status()
            bid = r.json()["batch_id"]
            final_at_kill = 0
            while final_at_kill < job.total * job.kill_gateway_after_fraction:
                v = (await gw.get(f"/v1/batches/{bid}")).json()
                final_at_kill = v["total"] - sum(v["pending"].values())
                await asyncio.sleep(0.1)
            pre_kill = (await pv.get("/admin/audit")).json()
            t_kill = time.time()
            stack.kill_gateway()
            await asyncio.sleep(1.0)  # the dead gateway stays dead for a moment
            t_restart = time.time()
            stack.start_gateway()
            view, deadline = {}, time.time() + 180
            while time.time() < deadline:
                try:
                    view = (await gw.get(f"/v1/batches/{bid}")).json()
                    if view["callback"]["status"] == "delivered":
                        break
                except (httpx.HTTPError, KeyError):
                    pass
                await asyncio.sleep(0.25)
            t_done = time.time()
            ids, offset, states = [], 0, {}
            while offset is not None:
                page = (await gw.get(f"/v1/batches/{bid}/results", params={"offset": offset, "limit": 5000})).json()
                for x in page["results"]:
                    ids.append(x["request_id"])
                    states[x["state"]] = states.get(x["state"], 0) + 1
                offset = page["next_offset"]
            post = (await pv.get("/admin/audit")).json()
            raw = (await pv.get("/admin/audit/raw")).json()
            res = {"series": {m: {k: np.zeros(1, dtype=np.int64) for k in SERIES} for m in scn.models},
                   "hist": {m: np.zeros(1, dtype=np.int64) for m in scn.models},
                   "queued": {m: 0 for m in scn.models}, "in_flight": {m: 0 for m in scn.models},
                   "audit_req": {m: np.array(v["requests"], dtype=np.int64) for m, v in raw.items()},
                   "audit_tok": {m: np.array(v["tokens"], dtype=np.int64) for m, v in raw.items()}}
    finally:
        server.should_exit = True
        await server_task
        stack.stop()
    attempts = sink.state.attempts
    delivered = [a for a in attempts if a["status_code"] == 200]
    summary_keys = ("batch_id", "status", "total", "succeeded", "failed", "expired")
    cb = delivered[0]["body"] if delivered else {}
    provider_calls = sum(sum(v["counts"][o] for o in ("success", "transient_failure", "permanent_failure")) for v in post.values())
    rep = build_report(scn, [res], [], 1, time.time() - t_ref, mode="http (gateway killed with SIGKILL mid-batch and restarted; Postgres)")
    rep["overall"]["note"] = "gateway statistics are not collected across the crash; the checks below use the API, the callback receiver and the provider audit"
    for m in rep["models"].values():
        m["totals"]["unaccounted"] = 0
    rep["batch"] = {"requests": job.total, "ack_latency_s": round(ack_s, 3), "final_when_killed": final_at_kill,
                    "gateway_down_s": round(t_restart - t_kill, 2), "restart_to_callback_delivered_s": round(t_done - t_restart, 2),
                    "callback_attempts": [{"at_s_after_restart": round(a["t"] - t_restart, 2), "status_code": a["status_code"]} for a in attempts],
                    "batch_status_after_restart": view.get("status"), "request_states": states,
                    "provider_calls_total": provider_calls, "provider_calls_before_kill": sum(sum(v["counts"][o] for o in ("success", "transient_failure", "permanent_failure")) for v in pre_kill.values()),
                    "provider_rate_limited_total": sum(v["counts"]["rate_limited"] for v in post.values())}
    n_ids = len(ids)
    rep["checks"] = {
        "acknowledged_before_the_crash": {"ok": ack_s < 1.0 and final_at_kill >= job.total * job.kill_gateway_after_fraction,
                                          "detail": f"acked in {ack_s * 1000:.0f} ms; {final_at_kill:,} of {job.total:,} requests were final when the gateway was killed"},
        "batch_completes_after_restart": {"ok": view.get("status") in ("completed", "completed_with_failures") and view["callback"]["status"] == "delivered",
                                          "detail": f"status '{view.get('status')}', callback '{view.get('callback', {}).get('status')}', {t_done - t_restart:.1f}s after the restart"},
        "callback_delivered_once": {"ok": len(delivered) == 1 and [a["status_code"] for a in attempts][-1] == 200,
                                    "detail": f"attempt status codes {[a['status_code'] for a in attempts]}"},
        "callback_summary_matches_batch_status": {"ok": bool(cb) and all(cb.get(k) == view.get(k) for k in summary_keys),
                                                  "detail": f"callback {({k: cb.get(k) for k in summary_keys})} vs API {({k: view.get(k) for k in summary_keys})}"},
        "every_request_id_exactly_once_and_none_lost": {"ok": n_ids == job.total and set(ids) == {i["request_id"] for i in items} and sum(states.values()) == job.total and not (set(states) - {"succeeded", "failed", "expired"}),
                                                        "detail": f"{n_ids:,} result rows, {len(set(ids)):,} distinct ids, states {states}"},
        "no_60s_window_over_limit": {"ok": rep["overall"]["all_limits_respected"], "detail": "provider audit across the whole run, including before and after the crash"},
    }
    return rep


async def run_http(scn: Scenario, out: Path) -> dict:
    if scn.kind == "recovery":
        return await run_recovery_scenario(scn, out)
    with services(scn, out) as (gw_url, pv_url):
        if scn.batch_job:
            return await run_batch_scenario(scn, gw_url, pv_url)
        return await run_rate_scenario(scn, gw_url, pv_url)
