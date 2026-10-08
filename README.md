# High-throughput inference under provider limits

A Python system that pushes inference requests to rate-limited providers as fast as the limits allow, and never faster.
It contains the **gateway** (queues, RPM/TPM limiting, retries, asynchronous batches with callbacks), a **provider
simulator**, and a **load generator**. Everything is reproducible from the scenario files in `scenarios/`.

Results are in [reports/BENCHMARK.md](reports/BENCHMARK.md). Headline numbers (measured, Apple M4 Pro, 12 cores):

| Goal | Result |
|---|---|
| 50,000 RPM / 100M TPM provider capacity | 97.8% of the RPM limit completed over a 5-minute steady state; no 60s window above the limit |
| Different models + changing limits | limits cut and restored at runtime with no restart; each model stayed within its current limit |
| Async batch + callback | 10,000 requests acknowledged in 26 ms (188 ms when saved durably first), callback delivered after two rejections, every id exactly once |
| Crash recovery (optional, needs Postgres) | gateway killed with `kill -9` mid-batch; the restarted gateway finished it, delivered the callback once, no request lost or duplicated |
| **300,000 simulated requests/s** (required) | **329,947 completed/s** |
| **1,000,000 simulated requests/s** (stretch) | **1,099,978 completed/s** |
| 10 billion requests/minute (planning) | design only, see [BENCHMARK.md](reports/BENCHMARK.md#9-projection-10-billion-requests-per-minute-not-measured) |

## Try it interactively (demo console + Swagger)
One command starts the provider simulator and the gateway and opens a control page:

```bash
python -m loadgen.console            # http://127.0.0.1:8080   (add DATABASE_URL / REDIS_URL to include them)
```

- **Console** (`:8080`): live per-model throughput against the limit line, queue depths and counters; start/stop load and change the rate while it runs;
  change a model's limits while running (with a "cut to 1/6" button); send a batch with a callback and make the receiver refuse its first N attempts;
  inject provider failures; and, with Postgres on, kill the gateway with SIGKILL and restart it to watch the batch resume.
- **Swagger** for the gateway (`http://127.0.0.1:8000/docs`) and the provider simulator (`http://127.0.0.1:8001/docs`), both linked from the console: every endpoint
  below can be called from the browser. A ready-made batch example is prefilled under `POST /v1/batches`.

The console is a separate process that talks to the services over HTTP; it is a way to drive and watch the system, not part of it. The automated runs
(`python -m loadgen.run ...`) are what the benchmark report is based on.

## Contents
1. [Setup](#setup) 2. [Run the pieces](#run-the-pieces) 3. [API](#api) 4. [Configure models and change limits](#configure-models-and-change-limits)
5. [Durability and several replicas](#durability-and-several-replicas) 6. [Run the validation scenarios](#run-the-validation-scenarios)
7. [Design](#design-decisions-and-tradeoffs) 8. [Simulation assumptions](#simulation-assumptions) 9. [Known gaps](#known-gaps)

## Setup
Python 3.11+ (developed on 3.14). **No external services are needed** for the benchmarks and Scenarios 1-3; Postgres and Redis are optional
(see [Durability and several replicas](#durability-and-several-replicas)).

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev,report,persistence]"   # report = matplotlib chart; persistence = asyncpg + redis clients (both optional)
pytest -q                                    # 33 tests, ~75 s; the 8 persistence tests are skipped unless `docker compose up -d` is running
```

Everything runs on localhost; processes are started with `spawn`, so it behaves the same on macOS, Linux and Windows.
The bulk benchmark picks its number of worker processes from `os.cpu_count()` (override with `--procs`).

## Run the pieces
Three processes, started separately (the scenario runner does this for you):

```bash
# 1. provider simulator  (models, limits, latency, failures come from the YAML)
PROVIDER_MODELS=config/models.yaml uvicorn provider_sim.app:app --port 8001

# 2. gateway
GATEWAY_MODELS=config/models.yaml PROVIDER_URL=http://127.0.0.1:8001 uvicorn gateway.app:app --port 8000

# 3. load generator + scenario runner (starts 1 and 2 itself, runs the load, writes a report)
python -m loadgen.run scenarios/s3_batch_callback.yaml
```

Try it by hand:

```bash
curl -s localhost:8000/v1/requests -H 'content-type: application/json' \
     -d '{"request_id":"demo-1","model":"model-a","payload":{"prompt":"hi"},"estimated_tokens":1000}'
curl -s localhost:8000/v1/requests/demo-1
curl -s localhost:8000/admin/status | python -m json.tool
curl -s localhost:8001/admin/audit | python -m json.tool      # what the provider actually received, per model
```

## API
Gateway (`uvicorn gateway.app:app`):

| Endpoint | Purpose |
|---|---|
| `POST /v1/requests` `{request_id?, model, payload, estimated_tokens}` | Submit one request. `202` queued, `200` if the id already exists (idempotent), `429` + `Retry-After` if the model's queue is full, `404` unknown model. |
| `GET /v1/requests/{id}` | State: `queued`, `in_flight`, `succeeded`, `failed`, `expired`, `rejected`; attempts, latency, error. |
| `POST /v1/requests/status` `{ids:[...]}` | State of many ids at once (used to verify nothing is lost). |
| `POST /v1/batches` `{requests:[...], callback_url?}` | Returns `202` with a `batch_id` immediately; work happens in the background. `409` on duplicate ids. |
| `GET /v1/batches/{id}` | `status` (`processing`, `completed`, `completed_with_failures`), counts, pending breakdown, callback delivery state. |
| `GET /v1/batches/{id}/results?offset=&limit=` | Per-request results, paged, in submission order. |
| `PUT /admin/models/{model}/limits` `{rpm?, tpm?}` | Change limits while running. |
| `GET /admin/models`, `/admin/status`, `/admin/stats` | Limits, queue depths, window usage, per-second statistics. |

With `DATABASE_URL` set, reads fall back to Postgres for anything not in memory (finished work after a restart, work owned by another replica),
and `POST /v1/requests` with a client-supplied `request_id` is idempotent across restarts. A batch that cannot be saved is refused with `503`
and nothing is accepted.

**Callback** (`POST callback_url`, sent once every request in the batch is final): `{event, batch_id, status, total, succeeded,
failed, expired, completed_at, results_url, results?}`. `failed` includes `expired`. Results are inlined for batches of up to
100 requests, otherwise fetch `results_url`. Headers `Idempotency-Key` and `X-Batch-Id` carry the batch id so receivers can
de-duplicate. Any non-2xx or network error is retried with exponential backoff and jitter (default: 8 attempts, 0.5s doubling
to a 15s cap). The same summary is available at `GET /v1/batches/{id}`.

Provider simulator (`uvicorn provider_sim.app:app`): `POST /inference` (`200` success, `503` transient failure, `422`
permanent failure, `429` over its own limit), `PUT /admin/models/{m}/limits`, `GET /admin/audit`, `GET /admin/audit/raw`.

## Configure models and change limits
Models live in a YAML file (`config/models.yaml` is the default; scenario files embed the same structure):

```yaml
models:
  model-a:
    rpm: 30000                 # requests per minute
    tpm: 60000000              # tokens per minute
    latency_ms_median: 300     # simulated provider latency (lognormal)
    latency_sigma: 0.4
    transient_failure_rate: 0.0   # simulated 503s, retried by the gateway
    permanent_failure_rate: 0.0   # simulated 422s, not retried
engine:                        # behaviour when demand exceeds capacity
  queue_max: 100000            # per model; a full queue rejects new requests (429)
  queue_ttl_s: 30              # a request waiting longer than this expires
  max_attempts: 3              # first try + retries of transient failures
  headroom: 1.0                # fraction of the provider limit the gateway uses (scenarios use 0.98)
  burst_s: 0.25                # never send more than this many seconds of capacity at once
gateway:                       # batches and callbacks
  callback_max_attempts: 8
  callback_backoff_base_s: 0.5
  callback_backoff_cap_s: 15
  callback_inline_results_max: 100
```

Change a limit **while the system runs**; the new limit applies to the next admission decision:

```bash
curl -X PUT localhost:8000/admin/models/model-a/limits -H 'content-type: application/json' -d '{"rpm": 5000}'
curl -X PUT localhost:8001/admin/models/model-a/limits -H 'content-type: application/json' -d '{"rpm": 5000}'   # simulated provider
```

(In the scenarios the provider is changed too, because the point is that the provider's capacity changed.
Raise the provider first and lower the gateway first, so the gateway never exceeds what the provider accepts.)
Scenario files can schedule changes with `changes: [{at_s, model, rpm, tpm}]`.

## Durability and several replicas
Both are optional and independent. Start the backing services and point the gateway at them (copy `.env.example`):

```bash
docker compose up -d                      # Postgres on :55432, Redis on :56379 (throwaway data)
export DATABASE_URL=postgresql://inference:inference@127.0.0.1:55432/inference
export REDIS_URL=redis://127.0.0.1:56379/0
uvicorn gateway.app:app --port 8000       # a second replica: the same command with --port 8002
```

**With `DATABASE_URL` (Postgres):**
- A batch and all its requests are saved **before the 202**, so an acknowledged batch survives a crash. Single requests and every state change are
  written behind in groups (default every 100 ms, one statement per table), off the request path.
- Current model limits are stored, so a runtime limit change survives a restart.
- **Recovery:** on start, and every 2 s when Redis is used, a replica adopts the unfinished work of replicas that are no longer alive: open requests
  are queued again, batches resume where they stopped, callbacks that were not delivered are sent. Work claimed this way is claimed atomically, so two
  replicas never adopt the same batch. Requests that were in flight at the crash run again, so delivery to the provider is **at-least-once**.
- Finished requests and batches stay readable after a restart.

**With `REDIS_URL`:** several gateway replicas can run side by side (put any load balancer in front).
- Every replica heartbeats; each model's limit is **split evenly across the live replicas**, so their combined usage cannot exceed the limit.
- A limit changed through any replica is stored once and pushed to all of them.
- A replica that joins waits 2.5 s before dispatching so the existing replicas can shrink their share first; a replica that leaves or dies
  hands its share back within a few seconds.
- Heartbeats also tell the survivors which replica died, so they know whose work to adopt.

Without `REDIS_URL` a gateway is a single replica and treats every other owner found in the database as dead, so run only one gateway per database in that mode.
Neither service is on the 300k / 1M per second benchmark path: that path is in memory by design.

## Run the validation scenarios
Each command starts the provider and gateway, drives load, applies any limit changes, then writes
`runs/<name>-<timestamp>/{report.md,report.json}` (plus a chart for Scenario 2) and **exits non-zero if a pass criterion fails**.

| Scenario | Command | Time | Passes when |
|---|---|---|---|
| 1. Reach provider capacity | `python -m loadgen.run scenarios/s1_capacity.yaml` | ~7 min | ≥90% of allowed capacity completed after warm-up; no 60s window above either limit; every request accounted for |
| 2. Models and changing limits | `python -m loadgen.run scenarios/s2_changing_limits.yaml` | ~6 min | each model within its current limit; Model A follows both changes; Model B unaffected; report shows limits and throughput over time |
| 3. Async batch completion | `python -m loadgen.run scenarios/s3_batch_callback.yaml` | ~1 min | ack < 1 s; callback only after all final; delivered after the destination recovers; summary equals API; each id exactly once |
| 4. Crash recovery (needs `DATABASE_URL`) | `python -m loadgen.run scenarios/s4_crash_recovery.yaml` | ~1 min | gateway killed with SIGKILL at 30% of a 10,000-request batch; the restarted gateway finishes it; callback delivered once; every id exactly once, none lost |
| Required scale benchmark | `python -m loadgen.run scenarios/scale_300k.yaml` | ~1 min | see report |
| Stretch scale benchmark | `python -m loadgen.run scenarios/scale_1m.yaml` | ~1 min | see report |

`scripts/run_all.sh` runs the tests plus all of the above (`scripts/run_all.sh quick` skips Scenarios 1 and 2); it includes Scenario 4 when `DATABASE_URL` is set.
Scenarios 1-3 also run with `DATABASE_URL` and `REDIS_URL` set, exercising the durable path end to end.
Edit a scenario file to choose the request rate, duration, model mix, batch size (`load.batch_size`) and token size (`load.token_size`, `token_jitter`).

How the numbers are measured: latency and request states come from the gateway's own records; rate-limit compliance comes from
the **provider simulator's independent audit**, which counts every call it accepted in 10 ms buckets and reports the maximum
over any 60-second span. It shares no code with the gateway's limiter.

## Design decisions and tradeoffs
**One engine, two ways to drive it.** The same `Engine` (per-model queue, limiter, retry logic) runs behind the real HTTP
service and the high-rate benchmark. The HTTP path (Scenarios 1-3) calls the simulator over the network. The bulk path
(300k / 1M per second) runs the engine and an in-process simulator inside N worker processes, moving requests in numpy chunks
of thousands instead of one at a time. Python is too slow for a per-request loop at 1M/s but fast enough when each operation
covers a chunk.

**Strict limiter.** Usage is counted in 100 ms buckets over a window one bucket longer than 60 s, so *any* real 60 s interval is
covered. This guarantees the "no 60 s period exceeds the limit" criterion at a cost of about 0.17% of capacity. A second,
smoothing credit bucket (`burst_s`) stops the gateway from emitting a whole minute of capacity in one instant (this
saturated the event loop in an early run).

**Per-model isolation.** Each model has its own queue and limiter, so a constrained model cannot slow another one.

**Behaviour above capacity.** Per-model FIFO queue with a bound and a TTL. A full queue rejects new work immediately
(`429`, state `rejected`); work that waits longer than the TTL becomes `expired`. Every request ends in exactly one of
`succeeded`, `failed`, `expired`, `rejected`, or is still `queued`/`in_flight`; the reports check that the counts add up.

**Changing limits.** A change is applied to the running limiter immediately. After a *reduction* the previous 60 s of traffic is
still inside the window, so nothing new is admitted until it drains below the new limit (Scenario 2: about 50 s). The audit
judges each window against the limit in effect and allows the not-yet-drained old traffic after a reduction; the drop and drain
are visible in the chart.

**Retries.** Transient failures are retried up to `max_attempts` and consume capacity each time. A provider `429` means "not
yet" and re-queues the request without using an attempt.

**Batches.** A batch is registered and acknowledged at once. Its items are fed to the engine only as fast as a model can clear
them (about half a TTL's worth of capacity), so they do not expire while waiting behind their own batch, and singles can still
use the queue. The callback is sent after the last item is final and retried with backoff; delivery is at-least-once with an
idempotency key.

**Durability, and what is deliberately not durable.** The promise is: *an acknowledged batch is never lost*. Batches are saved before the ack. A single
request is acknowledged before it is written, so one accepted in the last ~100 ms before a crash can be lost; making it durable first would put a database
round trip on every request. Per-attempt transitions and rejected requests are not stored (a rejected request was answered with 429). Recovery re-runs
requests that were in flight, which is why delivery is at-least-once; the idempotency key on the callback lets receivers de-duplicate.

**Replicas split limits instead of sharing a counter.** Each replica enforces its share locally with the same sliding-window limiter, so there is no
shared-state round trip per request and the limit is respected by construction. The cost: a replica cannot use an idle neighbour's share, and a
crashed replica's share is unused for a few seconds until its heartbeat expires. A restarted replica's limiter window starts empty (usage is not persisted);
the provider's own 429s, which the gateway re-queues without using an attempt, are the backstop for that first minute.

**Bulk sharding.** Each of N worker processes owns 1/N of every model's limits and 1/N of the offered load. This is exact and has
no coordination on the hot path; the price is that a shard cannot borrow an idle neighbour's share (fine for symmetric load,
see the 10B plan for the real answer).

**Why not Go (or Rust)?** The measured results do not need it: the scale benchmarks exceed the stretch goal and Scenarios 1-3 are
limit-bound at about 800 requests/s, while a single Python gateway process handled 6,000 requests/s with the limits removed. A
second language would add a toolchain for graders to install and split the code for no gain on the required goals. It becomes
worthwhile at the 10B/min scale, where per-core efficiency is a cost driver (see the report).

**aiohttp for the hot client.** httpx's connection pool does work proportional to connections × waiting requests on every call,
which stalled the gateway's event loop for seconds under load, so the gateway and load generator use aiohttp. httpx is kept
for callbacks (low rate) and in-process tests.

## Simulation assumptions
- The provider is a simulator with lognormal latency (median and shape per model), random transient/permanent failures, and its
  own sliding-window RPM/TPM limits. No real LLM is called.
- **HTTP scenarios (1-3):** gateway, provider and load generator are separate processes on one machine, wall-clock time, real HTTP.
- **Scale benchmarks:** the provider is *in-process* (no network hop, no serialization), requests are generated in-process, and
  limits are set far above the offered load (the assignment allows higher limits for the high-throughput benchmarks).
  Completion still means a simulated provider call reached a final state after its latency elapsed in real time.
- Request counts at 300k+/s are tracked as counters and a latency histogram (1% bins), not as per-request records.
- Token counts are the `estimated_tokens` supplied by the client; there is no separate token estimator.

## Known gaps
- Single requests are acknowledged before they are written (up to ~100 ms of exposure); only batches are durable at the ack. Rejected requests and per-attempt transitions are not stored.
- A restarted replica starts with an empty limiter window; the provider's 429s protect the provider during that first minute (the gateway re-queues them without using an attempt).
- If Redis is down the replicas keep the last known membership and cannot receive limit changes pushed through it; if Postgres is down writes queue in memory and are retried (batches are refused with 503).
- Records are never evicted from memory (fine for the benchmark durations); with Postgres, memory could be trimmed to open work only.
- Scale numbers are from one machine; the true ceiling was not found because the shards kept up with the offered load.
- In Scenarios 1 and 2 the offered load exceeds capacity by design, so latency is mostly queue wait and many requests are rejected or expire; that is the specified over-capacity behaviour, not a defect.
- The 300k / 1M per second benchmark runs entirely in memory; durability costs are measured separately (see the benchmark report).

## Layout
```
src/common/       config + scenario schemas
src/gateway/      limiter, queues, engine, HTTP service, batches/callbacks, persistence (Postgres), coordination (Redis)
src/provider_sim/ provider simulator, HTTP app, independent usage audit, in-process backend
src/loadgen/      scenario runner (HTTP and bulk), report builder, callback receiver, charts, demo console (console.py + console.html)
scenarios/        one YAML per scenario / benchmark
docker-compose.yml  optional Postgres + Redis
scripts/          run_all.sh
reports/          BENCHMARK.md and the result files it cites
tests/            33 tests (limiter, engine, service, callbacks, bulk, end-to-end, persistence/recovery/replicas)
```
