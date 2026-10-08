"""Run any scenario file.

    python -m loadgen.run scenarios/s1_capacity.yaml [--out runs]

`mode: bulk` scenarios run the in-process sharded benchmark, `mode: http` scenarios start the
provider and gateway services and drive them over HTTP.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import time
from pathlib import Path

from common.scenario import Scenario
from loadgen.bulk import run_scenario
from loadgen.http_run import run_http
from loadgen.plots import plot_limits
from loadgen.report import render_markdown


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("scenario")
    ap.add_argument("--procs", type=int, help="bulk mode: number of shard processes (default: from CPUs and rate)")
    ap.add_argument("--out", default="runs")
    args = ap.parse_args()
    scn = Scenario.load_file(args.scenario)
    out = Path(args.out) / f"{scn.name}-{time.strftime('%Y%m%d-%H%M%S')}"
    out.mkdir(parents=True, exist_ok=True)
    rep = run_scenario(scn, args.procs) if scn.mode == "bulk" else asyncio.run(run_http(scn, out))
    chart = plot_limits(rep, out) if rep["limit_changes"] else None
    (out / "report.json").write_text(json.dumps(rep, indent=1, default=str))
    md = render_markdown(rep)
    if chart:
        md += f"\n![limits over time]({chart.name})\n"
    (out / "report.md").write_text(md)
    print(md)
    print(f"\nWritten to {out}/")
    checks = rep.get("checks")
    if checks and not all(c["ok"] for c in checks.values()):
        raise SystemExit("some pass criteria FAILED")


if __name__ == "__main__":
    main()
