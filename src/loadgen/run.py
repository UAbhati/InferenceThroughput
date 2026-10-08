"""Run any scenario file.

    python -m loadgen.run scenarios/s1_capacity.yaml [--out runs]
    python -m loadgen.run scenarios/s1_capacity.yaml --rate 2000 --duration 90 --mix model-a=0.7,model-b=0.3 --token-size 500 --batch-size 50

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


def apply_overrides(scn: Scenario, args) -> Scenario:
    load_flags = {k: v for k, v in (("rate_per_s", args.rate), ("token_size", args.token_size), ("batch_size", args.batch_size)) if v}
    if args.mix:
        try:
            mix = {k.strip(): float(v) for k, v in (kv.split("=") for kv in args.mix.split(","))}
        except ValueError:
            raise SystemExit("--mix looks like: model-a=0.7,model-b=0.3")
        unknown = set(mix) - set(scn.models)
        if unknown:
            raise SystemExit(f"--mix names unknown model(s) {sorted(unknown)}; this scenario has {sorted(scn.models)}")
        load_flags["mix"] = mix
    if not load_flags and not (args.duration or args.warmup):
        return scn
    if load_flags and scn.load is None:
        raise SystemExit("this scenario has no steady load to override (it is a batch scenario)")
    update: dict = {"name": scn.name + "-custom"}
    if load_flags:
        update["load"] = scn.load.model_copy(update=load_flags)
    if args.duration:
        update["duration_s"] = args.duration
        update["warmup_s"] = min(scn.warmup_s, args.duration / 2)
    if args.warmup is not None:
        update["warmup_s"] = args.warmup
    # the pass criteria are written for the scenario as shipped, so a changed run is reported without a verdict
    update["kind"] = "generic"
    print(f"Overrides applied ({', '.join(f'{k}={v}' for k, v in {**load_flags, **{k: v for k, v in (('duration', args.duration), ('warmup', args.warmup)) if v}}.items())}); "
          "scenario-specific pass criteria are off for a customised run (accounting and limit checks still run).")
    if scn.mode == "bulk" and load_flags.get("batch_size", 1) > 1:
        print("note: --batch-size has no effect in bulk mode (the bulk path moves requests in internal chunks)")
    return scn.model_copy(update=update)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("scenario")
    ap.add_argument("--procs", type=int, help="bulk mode: number of shard processes (default: from CPUs and rate)")
    ap.add_argument("--out", default="runs")
    ap.add_argument("--rate", type=float, help="offered requests per second (all models together)")
    ap.add_argument("--duration", type=float, help="seconds of load")
    ap.add_argument("--warmup", type=float, help="seconds excluded from steady-state numbers")
    ap.add_argument("--mix", help="traffic share per model, e.g. model-a=0.7,model-b=0.3")
    ap.add_argument("--token-size", type=int, help="approximate tokens per request")
    ap.add_argument("--batch-size", type=int, help="http mode: send requests in batches of this size (1 = single requests)")
    args = ap.parse_args()
    scn = apply_overrides(Scenario.load_file(args.scenario), args)
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
