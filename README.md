# High-throughput inference under provider limits

Inference providers cap how many requests and tokens you can send per minute. This repo is a gateway that sends work to
such a provider as fast as the caps allow and never faster, plus a provider simulator and a load generator to prove it.

It handles single requests and batches (with a callback when a batch finishes), per-model limits that can change while it
runs, and what happens when demand is higher than capacity. Postgres and Redis are optional: with them the gateway survives
crashes and several copies can run side by side; without them it is a single in-memory process.

The numbers below are from one MacBook Pro (M4 Pro, 12 cores). The full write-up is in
[reports/BENCHMARK.md](reports/BENCHMARK.md).

| Goal | Result |
|---|---|
| Use the 50,000 RPM / 100M TPM capacity | 97.8% of the RPM limit over a 5 minute steady state; the busiest 60 s window was 98.0% of the limit |
| Different models, limits changed while running | Model A cut from 30,000 to 5,000 RPM and back with no restart; both models stayed inside their current limits |
| Async batch with callback | 10,000 requests acknowledged in 28 ms, callback refused twice then delivered, every id exactly once |
| Crash recovery (needs Postgres) | gateway killed with `kill -9` at 3,018 of 10,000; the restarted one finished the batch, callback delivered once |
| 300,000 simulated requests/s (required) | 330,020 completed per second |
| 1,000,000 simulated requests/s (stretch) | 1,100,012 completed per second |
| 10 billion requests/minute (planning) | design only, [section 9 of the report](reports/BENCHMARK.md#9-projection-10-billion-requests-per-minute-not-measured) |

The two scale numbers use an in-process simulated provider and limits set far above the load (the assignment allows that).
See [simulation assumptions](#assumptions-and-simulation-notes) before comparing them to anything.

## Contents
[Setup](#setup) · [Try it](#try-it) · [API](#api) · [Configuration and changing limits](#configuration-and-changing-limits) ·
[Postgres and Redis](#postgres-and-redis-optional) · [Validation scenarios](#validation-scenarios) · [How it works](#how-it-works) ·
[Gotchas I looked at](#gotchas-i-looked-at) · [Assumptions and simulation notes](#assumptions-and-simulation-notes) ·
[Known gaps](#known-gaps) · [Layout](#layout)

## Setup
Python 3.11 or newer (developed on 3.14). Nothing else is needed for the benchmarks and Scenarios 1 to 3.

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev,report,persistence]"   # report = matplotlib for the chart, persistence = Postgres/Redis clients
pytest -q                                    # 46 tests, about 80 s
```

The 12 tests that need Postgres and Redis skip themselves unless `docker compose up -d` is running. Everything runs on
localhost. Worker processes are started with `spawn`, so it behaves the same on macOS, Linux and Windows. The bulk benchmark
sizes itself from `os.cpu_count()` (override with `--procs`).

## Try it
The quickest way to see it work is the demo console. From the **repository root**, after completing Setup and activating
the virtual environment, run:

```bash
source .venv/bin/activate          # if not activated, if already activated then ignore this
python -m loadgen.console          # then open http://127.0.0.1:8082
```

From the page you can start and stop load, change a model's limits while it runs, send a batch with a callback (and make the
receiver refuse the first few attempts), inject provider failures, and, if Postgres is on, kill the gateway and restart it.
It plots throughput against the limit live. It links to the Swagger pages of both services
(`http://127.0.0.1:8000/docs` and `http://127.0.0.1:8001/docs`), where every endpoint below can be called from the browser.
The console is a separate process that talks to the services over HTTP, so it is a viewer and not part of the system.

To run the pieces yourself:

```bash
PROVIDER_MODELS=config/models.yaml uvicorn provider_sim.app:app --port 8001
GATEWAY_MODELS=config/models.yaml PROVIDER_URL=http://127.0.0.1:8001 uvicorn gateway.app:app --port 8000

curl -s localhost:8000/v1/requests -H 'content-type: application/json' \
     -d '{"request_id":"demo-1","model":"model-a","payload":{"prompt":"hi"},"estimated_tokens":1000}'
curl -s localhost:8000/v1/requests/demo-1
curl -s localhost:8000/admin/status | python -m json.tool
curl -s localhost:8001/admin/audit | python -m json.tool     # what the provider really received
```

## API
Gateway:

| Endpoint | What it does |
|---|---|
| `POST /v1/requests` | Submit one request: `{request_id?, model, payload, estimated_tokens}`. `202` queued, `200` if the id is already known, `429` (with `Retry-After`) if the model's queue is full. |
| `GET /v1/requests/{id}` | State (`queued`, `in_flight`, `succeeded`, `failed`, `expired`, `rejected`), attempts, latency, error. |
| `POST /v1/requests/status` | States for up to 10,000 ids at once. |
| `POST /v1/batches` | `{requests: [...], callback_url?}`. Returns `202` with a `batch_id` straight away; the work happens in the background. Optional `Idempotency-Key` header. |
| `GET /v1/batches/{id}` | `processing`, `completed` or `completed_with_failures`, counts, what is still pending, callback delivery state. |
| `GET /v1/batches/{id}/results?offset=&limit=` | Per-request results in submission order. |
| `PUT /admin/models/{model}/limits` | Change `rpm` and/or `tpm` now. |
| `GET /admin/models`, `/admin/status`, `/admin/stats` | Limits, queue depths, usage in the last 60 s, per-second statistics. |

Errors: `404` unknown model or id, `409` duplicate ids in a batch, `413` batch over 100,000 requests, `422` bad input
(see below), `429` queue full, `503` a batch could not be saved.

Input rules: `request_id` is 1 to 128 characters from `A-Za-z0-9._:@/-`; `estimated_tokens` is 1 to 10,000,000; a payload
is at most 64 KB as JSON. A request that needs more tokens than the model's whole per-minute budget is refused with `422`
(`request_too_large`), because it could never be sent. In a batch that refuses the whole batch.

**Retrying safely.** Sending the same `request_id` again returns the existing request instead of creating a second one, even
if the first call is still in progress and even after a restart (with Postgres). For batches, send an `Idempotency-Key`
header: repeating the call with the same key and body returns the same batch (`200`, `created: false`), the same key with a
different body is a `422`.

**Callback.** When every request in a batch is final, the gateway POSTs to `callback_url`:
`{event, batch_id, status, total, succeeded, failed, expired, completed_at, results_url, results?}`. `failed` includes
`expired`. Results are inlined for batches of up to 100 requests, otherwise fetch `results_url`. The headers `Idempotency-Key`
and `X-Batch-Id` carry the batch id so a receiver can ignore duplicates. Anything other than a 2xx, a timeout or a connection
error is retried with exponential backoff and jitter (8 attempts by default, 0.5 s doubling up to 15 s). The same summary is
always available from `GET /v1/batches/{id}`, whether or not the callback worked.

Provider simulator: `POST /inference` answers `200`, `503` (transient failure), `422` (permanent failure) or `429` (over its own
limit). Admin endpoints change limits and behaviour (`PUT /admin/models/{m}/limits`, `/behavior`) and read the audit
(`GET /admin/audit`, `/admin/audit/raw`).

## Configuration and changing limits
Models and behaviour live in one YAML file. `config/models.yaml` is the default, and each scenario file embeds the same structure:

```yaml
models:
  model-a:
    rpm: 30000                    # requests per minute
    tpm: 60000000                 # tokens per minute
    latency_ms_median: 300        # simulated provider latency (lognormal)
    latency_sigma: 0.4
    transient_failure_rate: 0.0   # share of calls answered 503, retried by the gateway
    permanent_failure_rate: 0.0   # share answered 422, not retried
engine:                           # what happens when demand exceeds capacity
  queue_max: 100000               # per model; a full queue rejects with 429
  queue_ttl_s: 30                 # queued longer than this and the request expires
  max_attempts: 3                 # first try plus retries of transient failures
  retry_backoff_s: 0.25           # first retry waits about this long, doubling, with jitter
  retry_backoff_cap_s: 5
  rate_limited_pause_s: 0.5       # a provider 429 pauses that model for this long
  headroom: 1.0                   # share of the provider's limit the gateway uses (the scenarios use 0.98)
  burst_s: 0.25                   # never send more than this many seconds of capacity at once
gateway:
  batch_max_requests: 100000
  max_payload_bytes: 65536
  callback_max_attempts: 8
  callback_backoff_base_s: 0.5
  callback_backoff_cap_s: 15
  callback_timeout_s: 5
  callback_inline_results_max: 100
  callback_allowed_hosts: null    # e.g. [hooks.example.com]; null allows any host
```

Environment variables: `GATEWAY_MODELS`, `PROVIDER_MODELS` (YAML paths), `PROVIDER_URL`, `GATEWAY_PUBLIC_URL` (used in
`results_url`), `DATABASE_URL`, `REDIS_URL`, `ADMIN_TOKEN` (when set, `/admin/*` needs `Authorization: Bearer <token>`).
Models are read at startup, so adding a model means a restart; changing a model's limits does not.

Change a limit while everything is running. It applies to the next admission decision:

```bash
curl -X PUT localhost:8000/admin/models/model-a/limits -H 'content-type: application/json' -d '{"rpm": 5000}'
curl -X PUT localhost:8001/admin/models/model-a/limits -H 'content-type: application/json' -d '{"rpm": 5000}'   # the simulated provider
```

In the scenarios the provider is changed as well, because the point is that the provider's capacity changed. Raise the
provider first and lower the gateway first, so the gateway is never allowed more than the provider accepts. A scenario file
can schedule changes with `changes: [{at_s, model, rpm, tpm}]`.

## Postgres and Redis (optional)
```bash
docker compose up -d                       # Postgres on :55432, Redis on :56379 (throwaway data)
export DATABASE_URL=postgresql://inference:inference@127.0.0.1:55432/inference
export REDIS_URL=redis://127.0.0.1:56379/0
uvicorn gateway.app:app --port 8000        # a second replica is the same command on another port
```

**Postgres** makes the gateway durable. A batch and all of its requests are written before the `202`, so an acknowledged
batch survives a crash. Single requests and the final state of every request are written behind, in groups every 100 ms,
off the request path. Runtime limit changes are stored too. When a gateway starts (and every 2 s when Redis is on), it
adopts the unfinished work of gateways that are gone: queued requests go back in the queue, batches carry on from where they
stopped, undelivered callbacks are sent. Adoption is atomic, so two replicas never take the same batch. Requests that were
in flight at the moment of the crash run again, so delivery to the provider is at-least-once. Finished work stays readable after a restart.

**Redis** lets several gateways run together. Each one sends a heartbeat. Every model's limit is split evenly across the
live gateways, so their combined usage cannot go over it. A limit changed through any gateway is stored once and pushed to
all of them. A gateway that joins waits 2.5 s before it starts sending, so the others have time to give up part of their
share; one that leaves or dies hands its share back within a few seconds. The heartbeats are also how survivors learn that
a gateway died and its work needs adopting.

Without Redis a gateway assumes it is the only one and treats every other owner in the database as dead, so run one
gateway per database in that mode. Neither service is involved in the 300k and 1M benchmarks, which run in memory.

## Validation scenarios
Each command starts the provider and gateway, drives load, applies any scheduled limit changes and writes
`runs/<name>-<timestamp>/` with `report.md`, `report.json` and, for limit changes, a chart. It exits non-zero if a pass
criterion fails.

| Scenario | Command | Time | Passes when |
|---|---|---|---|
| 1. Reach provider capacity | `python -m loadgen.run scenarios/s1_capacity.yaml` | 7 min | at least 90% of allowed capacity completed after warm-up; no 60 s window over either limit; every request accounted for |
| 2. Models and changing limits | `python -m loadgen.run scenarios/s2_changing_limits.yaml` | 6 min | each model within its current limit; Model A follows both changes; Model B unaffected; limits and throughput over time in the report |
| 3. Async batch | `python -m loadgen.run scenarios/s3_batch_callback.yaml` | 1 min | ack under 1 s; callback only after all requests are final; delivered after the receiver recovers; callback equals the batch API; each id once |
| 4. Crash recovery | `python -m loadgen.run scenarios/s4_crash_recovery.yaml` | 1 min | needs `DATABASE_URL`. Gateway killed with SIGKILL at 30% of a batch; the restarted one finishes it; callback once; each id once |
| Required scale | `python -m loadgen.run scenarios/scale_300k.yaml` | 1 min | see the report |
| Stretch scale | `python -m loadgen.run scenarios/scale_1m.yaml` | 1 min | see the report |

`scripts/run_all.sh` runs the tests and everything above (`scripts/run_all.sh quick` skips Scenarios 1 and 2).

**Choosing the load.** The load generator takes its settings from the scenario file, and any of them can be overridden on the command line:

```bash
python -m loadgen.run scenarios/s1_capacity.yaml --rate 2000 --duration 90 --mix model-a=0.7,model-b=0.3 --token-size 500 --batch-size 50
```

`--batch-size` above 1 sends requests through `POST /v1/batches` in groups of that size (HTTP scenarios only). A customised run
drops the scenario's own pass criteria but still checks accounting and limits. Each report lists submitted, completed,
succeeded and failed requests, the completion rate, p50/p95/p99 latency, configured and observed RPM and TPM per model, and for
batch scenarios the batch and callback timing.

**How the numbers are checked.** Request states and latency come from the gateway's own records. Compliance with the limits
comes from the provider simulator's audit, which counts every call it accepted in 10 ms buckets and reports the maximum over
any 60 s span. It shares no code with the gateway's limiter, so it can catch the gateway being wrong.

## How it works
**One engine, two drivers.** The `Engine` (per-model queue, limiter, retries) runs behind the HTTP service and behind the
high-rate benchmark. The HTTP path calls the simulator over the network and is what Scenarios 1 to 4 use. The bulk path runs the engine and
an in-process simulator inside several worker processes and moves requests in numpy arrays of thousands at a time. Python is
far too slow to loop per request at a million a second, but is fine when each operation covers a whole chunk.

**The limiter.** Usage is counted in 100 ms buckets over a window one bucket longer than 60 s. That way any real 60 s
interval is covered, which is what makes "no 60 s period goes over the limit" true instead of probable. It costs about 0.17% of
capacity. A second small bucket (`burst_s`) stops the gateway from sending a whole minute of capacity in one instant; an early
version did that and froze its own event loop with 10,000 simultaneous calls.

**Over capacity.** Each model has its own FIFO queue with a size bound and a time limit. A full queue answers `429`, and a request that
waits longer than `queue_ttl_s` becomes `expired`. Because the queues and limiters are per model, a constrained model cannot hold up another.
Every request ends in exactly one state, and the reports check that the counts add up.

**Changing limits.** A change reaches the running limiter at once. After a reduction, the last 60 s of traffic is still inside
the window, so nothing new is sent until it drains below the new limit. In Scenario 2 that is about 50 seconds with no
completions for Model A, visible in the chart. The audit judges each 60 s window against the limit in effect at its end and
lets the old, not yet drained traffic stand after a reduction; it would flag any new traffic sent on top of it.

**Retries.** A transient failure is retried after a delay that doubles each time, with jitter, up to `max_attempts`. Each
attempt uses capacity again. A provider `429` means "not yet" rather than "failed": the request goes back in the queue
without using an attempt and that model pauses briefly.

**Batches.** A batch is registered and acknowledged at once. Its requests are handed to the engine only as fast as the
model can clear them (about half a TTL of capacity), so they don't expire waiting behind the rest of their own batch and
single requests can still get through. The callback is sent after the last request is final.

**Replicas** split each limit instead of sharing a counter. Every replica enforces its own share with the same limiter, so
there is no shared state on the request path. The price is that a replica can't borrow a quiet neighbour's share, and a crashed one's share
sits unused for a few seconds. Bulk worker processes work the same way: each owns 1/N of every limit and 1/N of the load.

**Why only Python.** The scale benchmarks already pass the stretch goal and Scenarios 1 to 4 are limited by the rate limits (about 800
requests a second), not by the CPU. With the limits removed, one gateway process handled 6,000 requests a second. A second language would
add a toolchain for whoever runs this and wouldn't change a required result. It would start to matter at the 10B/min scale, where
cores cost real money (see the report).

**Why aiohttp inside the gateway.** httpx's connection pool does work proportional to connections times waiting requests on every call.
With a few thousand calls in flight it blocked the gateway's event loop for seconds (found by profiling), so the gateway and the load
generator use aiohttp. httpx stays for callbacks and tests.

## Gotchas I looked at
The requirement is easy to meet in the happy case. These are the places it breaks. Each row says what the system does; the
tests are in `tests/` and several of these were found by probing the running system and then fixed.

| Gotcha | What happens here |
|---|---|
| The provider may use a fixed minute, a sliding window or a token bucket | The gateway uses the strictest reading (any 60 s span), which satisfies the others |
| Sending a minute's worth at the start of a window | Admission is smoothed with a small burst allowance on top of the window |
| Requests and tokens are separate limits | Both are enforced together; whichever is tighter binds (tested with large requests) |
| A request needs more tokens than the model's whole budget | `422` at submit. If the limit is lowered later, queued requests that no longer fit fail at once. Before this was fixed, one such request blocked everything behind it |
| Token counts are estimates | The gateway trusts `estimated_tokens` and keeps 2% headroom. If the provider counts more, it answers `429`, which the gateway absorbs. It does not read real usage back, see known gaps |
| Gateway counts at send time, provider at arrival | Network delay shifts calls across bucket edges; the 2% headroom covers it. The provider simulator audits independently |
| Limits change while requests are queued or in flight | Applied immediately; reductions drain as described above; the provider is raised before and lowered after the gateway |
| Retries use capacity and can stampede a struggling provider | Exponential backoff with jitter, and a provider `429` pauses that model instead of being retried in a loop |
| A client retries after a timeout | Same `request_id` returns the existing request, also when the two calls overlap and after a restart. `Idempotency-Key` does the same for batches; a reused key with a different body is refused |
| A request must appear exactly once in a batch result | Ids are unique per batch and across the gateway; results come from one ordered list; checked in Scenarios 3 and 4 |
| Callback receiver is down, slow or answers an error | Retried with backoff and jitter, timeout 5 s, gives up after 8 attempts but the results stay available through the API |
| Callback sent twice, or before the batch is done | Sent only after the last request is final. Duplicates are possible after a crash, so the batch id is sent as an idempotency key |
| Callback URL pointing somewhere private | Optional `callback_allowed_hosts`. Off by default because the demo calls back to localhost, so turn it on when exposing the gateway |
| The gateway or the machine dies mid-batch | Batches are saved before the ack and adopted by another gateway. Requests in flight run again (at-least-once) |
| The new gateway forgets what it sent a second ago | Its window starts empty. The provider's own `429` is the safety net for the first minute and is not charged as a failed attempt |
| Two gateways would double the rate | With Redis each gets a share of the limit; a joining gateway waits before sending; a dead one is detected by its heartbeat |
| Bad data wedging the database writer | Inputs are validated (id characters, token range, payload size). Payloads are stored as text. If the database still rejects a row, only that row is dropped and logged. Before this was fixed, one oversized number stalled every later write |
| Unbounded memory | Bounded queues, a 100,000 request batch cap, a 64 KB payload cap, batches fed to the queue gradually. Finished records are not evicted, see known gaps |
| Unprotected admin endpoints | Optional `ADMIN_TOKEN`. The request API has no authentication at all, which a real deployment needs in front of it |
| Clock changes | Limits and timeouts use a monotonic clock; wall-clock time is only for timestamps |
| A provider call times out but may have run | Treated as a transient failure and retried, so the provider may see it twice. Same at-least-once trade as above |

## Assumptions and simulation notes
- There is one provider account per model, and its limits apply to that model alone. A request counts against both limits when it is sent, and a retry counts again.
- A request's token cost is `estimated_tokens`, as given by the client.
- The provider is a simulator: lognormal latency (median and shape per model), random transient and permanent failures, and its own sliding-window limits. No real model is called.
- Scenarios 1 to 4 run the gateway, the provider and the load generator as separate processes on one machine over real HTTP, on the wall clock.
- The 300k and 1M runs use an in-process provider (no network, no serialisation) and limits far above the load, as the assignment permits. A request still only
  counts as completed once its simulated latency has really elapsed. At that rate requests are tracked as counters and a latency histogram, not as records.
- Latency in Scenarios 1 and 2 is mostly queue wait, because the offered load is above capacity on purpose.
- All numbers come from one laptop. They show what the design does, not what any other machine will do.

## Known gaps
- Single requests are acknowledged before they are written, so one accepted in the last ~100 ms before a crash can be lost. Only batches are durable at the ack. Rejected requests and per-attempt states are not stored.
- The gateway does not read the provider's real token usage back, so it cannot correct an estimate that was wrong. A provider that counts more than the estimate will push back with `429`; one that counts less leaves capacity unused.
- A restarted gateway starts with an empty limiter window.
- If Redis is down, replicas keep the last membership they saw and stop receiving pushed limit changes. If Postgres is down, writes queue in memory and retry, and new batches are refused with `503`.
- Records are never evicted from memory. Fine for these runs, not for months of uptime. A limit cannot be set to zero to pause a model.
- The request API has no authentication, no TLS and no per-client quotas.
- Throughput of several replicas at scale and behaviour during a Postgres or Redis outage were not measured. The scale ceiling on this laptop was not found, because the workers kept up with the load offered.

## Layout
```
src/common/         config and scenario schemas
src/gateway/        limiter, queues, engine, HTTP service, batches and callbacks, persistence, coordination
src/provider_sim/   provider simulator, its HTTP app, the independent audit, the in-process backend
src/loadgen/        scenario runner (HTTP and bulk), reports, callback receiver, charts, demo console
scenarios/          one YAML per scenario and benchmark
scripts/            run_all.sh
reports/            BENCHMARK.md and the result files it cites
tests/              46 tests: limiter, engine, service, callbacks, bulk, end to end, persistence, console
docker-compose.yml  optional Postgres and Redis
```
