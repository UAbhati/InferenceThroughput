#!/usr/bin/env bash
# Reproduce every result. Usage: scripts/run_all.sh [quick]
#   full  (~25 min): tests, both scale benchmarks, scenarios 1-3
#   quick (~4 min) : tests, both scale benchmarks, scenario 3
# Needs the virtualenv from the README to be active (or set PYTHON=/path/to/python).
set -euo pipefail
cd "$(dirname "$0")/.."
PY="${PYTHON:-python}"

echo "== tests";                      "$PY" -m pytest -q
echo "== required: 300k req/s";       "$PY" -m loadgen.run scenarios/scale_300k.yaml
echo "== stretch: 1M req/s";          "$PY" -m loadgen.run scenarios/scale_1m.yaml
echo "== scenario 3: batch+callback"; "$PY" -m loadgen.run scenarios/s3_batch_callback.yaml
if [ "${1:-full}" != "quick" ]; then
  echo "== scenario 1: provider capacity (6 min)"; "$PY" -m loadgen.run scenarios/s1_capacity.yaml
  echo "== scenario 2: changing limits (5 min)";   "$PY" -m loadgen.run scenarios/s2_changing_limits.yaml
fi
echo "Done. Reports are under runs/<scenario>-<timestamp>/ (report.md, report.json, charts)."
