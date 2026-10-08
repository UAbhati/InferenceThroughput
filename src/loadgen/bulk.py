"""Run a scenario on the in-process bulk path, sharded over several processes.

    python -m loadgen.bulk scenarios/scale_300k.yaml [--procs N] [--out runs]
"""
from __future__ import annotations

import argparse
import json
import math
import multiprocessing as mp
import os
import time
from pathlib import Path

from common.scenario import Scenario
from loadgen.bulk_worker import worker_main
from loadgen.report import build_report, render_markdown


def pick_procs(scn: Scenario, requested: int | None) -> int:
    if requested or scn.procs:
        return max(requested or scn.procs, 1)
    # one shard per ~40k offered req/s, never more than the CPUs available
    return max(1, min(os.cpu_count() or 1, math.ceil(scn.load.rate_per_s / 40_000)))


def run_scenario(scn: Scenario, procs: int | None = None) -> dict:
    n = pick_procs(scn, procs)
    ctx = mp.get_context("spawn")  # same behaviour on macOS, Linux and Windows
    start_epoch = time.time() + 1.5 + 0.15 * n
    conns, workers = [], []
    for i in range(n):
        parent, child = ctx.Pipe()
        p = ctx.Process(target=worker_main, args=(i, n, scn.model_dump_json(), start_epoch, child), daemon=True)
        p.start()
        conns.append(parent)
        workers.append(p)
    for c in conns:
        c.recv()  # "ready"
    changes_log = []
    for ch in sorted(scn.changes, key=lambda c: c.at_s):
        time.sleep(max(start_epoch + ch.at_s - time.time(), 0))
        for c in conns:
            c.send(("limits", ch.model, ch.rpm, ch.tpm))
        changes_log.append({"at_s": round(time.time() - start_epoch, 3), "model": ch.model, "rpm": ch.rpm, "tpm": ch.tpm})
    results = []
    deadline = start_epoch + scn.duration_s + scn.drain_s + 60
    for c in conns:
        if not c.poll(max(deadline - time.time(), 1)):
            raise RuntimeError("a shard did not report back in time")
        kind, payload = c.recv()
        results.append(payload)
    for p in workers:
        p.join(timeout=10)
    return build_report(scn, results, changes_log, n, time.time() - start_epoch)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("scenario")
    ap.add_argument("--procs", type=int)
    ap.add_argument("--out", default="runs")
    args = ap.parse_args()
    scn = Scenario.load_file(args.scenario)
    rep = run_scenario(scn, args.procs)
    out = Path(args.out) / f"{scn.name}-{time.strftime('%Y%m%d-%H%M%S')}"
    out.mkdir(parents=True, exist_ok=True)
    (out / "report.json").write_text(json.dumps(rep, indent=1))
    md = render_markdown(rep)
    (out / "report.md").write_text(md)
    print(md)
    print(f"\nWritten to {out}/")


if __name__ == "__main__":
    main()
