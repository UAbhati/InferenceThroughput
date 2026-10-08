# Benchmark report

Sections 1 to 8 are measurements taken with the scripts in this repository. Section 9 is a projection and is kept apart on
purpose. The run reports (markdown, plus JSON with per-second timelines) are in [`results/`](results/).

## 1. Summary

| # | Goal | Pass criteria | Result | |
|---|---|---|---|---|
| S1 | Use the 50k RPM / 100M TPM capacity | at least 90% of allowed capacity after warm-up, no 60 s window over a limit, every request accounted for | 97.8% of the RPM limit over a 300 s steady state; worst 60 s window 98.0% of the limit; 395,995 sent, all accounted for | pass |
| S2 | Different models, changing limits | each model inside its current limits, A follows both changes, B unaffected, report over time | A: 29,362, then 4,903, then 29,379 completed/min against limits of 30,000, 5,000, 30,000. B at 97.8% throughout. No window over its limit | pass |
| S3 | Async batch | ack under 1 s, callback only after all final, delivered after recovery, summary equals API, each id once | ack 28 ms; callback sent after all 10,000 were final; attempts 503, 503, 200; summary identical to the API; 10,000 distinct ids | pass |
| S4 | Crash recovery (extra, needs Postgres) | batch finishes after `kill -9`, callback once, each id once, none lost | killed with 3,018 of 10,000 final; the restarted gateway finished it, callback delivered once, 10,000 distinct ids | pass |
| Required | 300,000 simulated requests/s | complete 300k/s | 330,020 completed/s (offered 330,000) | pass |
| Stretch | 1,000,000 simulated requests/s | complete 1M/s | 1,100,012 completed/s (offered 1,100,000) | pass |
| Planning | 10 billion requests/min | explain the extension | section 9 | design only |

Every scenario and benchmark was run twice, once before and once after the last round of changes (section 8). The
repeats agree to within 0.1% (for example 329,947 and 330,020 per second for the 300k run). The one visible difference is S3, which went
from 15.5 s to 16.5 s to finish because retries now back off instead of being resent at once.

## 2. Environment and method

- **Machine:** MacBook Pro, Apple M4 Pro, 12 cores (8 performance, 4 efficiency), 24 GB RAM, macOS, Python 3.14.7. Other machines
  will give other absolute numbers. The number of bulk worker processes follows `os.cpu_count()`.
- **Scenarios 1 to 4:** the provider simulator, the gateway and the load generator are separate processes on this machine
  talking HTTP over localhost, on the wall clock. The gateway is one `uvicorn` process.
- **Scale benchmarks:** the engine and the provider simulator run in the same process (no network), in N worker processes (9 for 300k,
  12 for 1M). Each worker owns 1/N of every limit and of the load. Limits are set far above the offered load, as the assignment allows.
- **What counts as completed:** a request counts only once the simulated provider call has reached a final state, after its
  simulated latency has really elapsed. Being accepted by the API does not count.
- **Where the numbers come from:** request states and latency from the gateway's own records; compliance with the limits from the
  provider simulator's audit, which counts every call it accepted in 10 ms buckets and takes the maximum over any 60 s span and shares no code
  with the gateway's limiter; client-side counts from the load generator. For S1 and S2 the load generator also asks the gateway for the final
  state of every id it sent.
- **Steady state** is the part of the run after warm-up. For S1 and S2 that is after 60 s, so the limiter's window is full. For the scale runs it is after 10 s.
- **Latency** is measured inside the system, from joining the gateway queue to a final state. Where the offered load is above capacity (S1, S2)
  it is mostly queue wait, capped by the TTL, so it says nothing about how fast the provider is.

Each run's configuration is the scenario file of the same name in [`../scenarios/`](../scenarios/). S1 to S4 use `queue_max` 20,000
(S3 and S4: 100,000), `queue_ttl_s` 30 (S3 and S4: 60), `max_attempts` 3, and `headroom` 0.98. The headroom means the gateway uses 98% of the
provider's limit, so network jitter cannot push the provider over it.

## 3. Scale benchmarks

| | Required (300k) | Stretch (1M) |
|---|---|---|
| Offered load | 330,000 req/s | 1,100,000 req/s |
| Worker processes | 9 | 12 (all cores) |
| **Completed per second, steady state** | **330,020** | **1,100,012** |
| Requests in the 30 s run | 9.9 million | 33.0 million |
| Succeeded / failed / expired / rejected | all succeeded, none of the others | all succeeded, none of the others |
| Latency p50 / p95 / p99 | 0.266 s / 0.589 s / 0.794 s | 0.266 s / 0.589 s / 0.794 s |
| Every request accounted for | yes | yes |
| Any 60 s window over a limit | no | no |
| Per worker | about 37k/s | about 92k/s |

Two models with simulated median latency of 200 ms and 350 ms (lognormal, shape 0.4), 50/50 traffic, 1,000 tokens give or take 20%. The configured
limits (12M and 40M RPM per model) were not binding, so these runs measure the system's own throughput. Limit handling is covered by S1 to S4
and the unit tests. Latency here is almost entirely the simulated provider latency, so the engine adds very little queueing.

Files: [scale-300k.md](results/scale-300k.md), [scale-1m.md](results/scale-1m.md).

The workers kept up in both runs (completions equalled the offered rate), so the ceiling of this design on this machine was not found.
1.1M/s is a lower bound for a 12 core laptop. These runs also leave out the network and serialisation cost that a real provider
call has.

## 4. Scenario 1: reach the available capacity

One model with 50,000 RPM and 100M TPM. 1,000 token requests offered at 1,100 per second (66,000 RPM, 32% over capacity) for 360 s. The first 60 s are warm-up.

| Metric | Value |
|---|---|
| Steady state (60 to 360 s) | 48,921 RPM, 97.8% of the 50,000 limit (the missing 2% is the configured headroom) |
| Tokens | 48.9M TPM, 48.9% of the TPM limit (1,000 token requests hit the RPM limit first) |
| Worst 60 s window at the provider | 49,000 requests, 0.980 of the limit; 49.0M tokens, 0.490 |
| Sent / accepted (202) / rejected (429) | 395,995 / 313,780 / 82,215 |
| Final state of every sent id | 313,780 succeeded and 82,215 rejected; none unknown, none still waiting |
| Failed / expired | 0 / 0. The 20,000 request queue fills in about 24 s, before the 30 s TTL, so the excess is rejected rather than expired |
| Client ack latency p50 / p95 / p99 | 0.8 ms / 1.6 ms / 2.8 ms |
| Latency inside the system p50 / p95 / p99 | 24.8 s / 25.1 s / 25.3 s, queue wait |

Files: [s1-capacity.md](results/s1-capacity.md), [s1-capacity.json](results/s1-capacity.json).

## 5. Scenario 2: different models and changing limits

Model A has 30,000 RPM / 60M TPM and Model B has 20,000 RPM / 40M TPM. 1,100 requests per second are offered (A about 650, B about 450), both above their
limits. At 100 s Model A is cut to 5,000 RPM and at 200 s it is restored. Nothing is restarted. The change goes through the admin API, provider first
when raising and gateway first when lowering.

![Limits and throughput over time](results/s2-limits-over-time.png)

| Segment | Limit in effect | Completed per minute | |
|---|---|---|---|
| A, 15 to 100 s | 30,000 | 29,362 | 98% of the limit |
| A, 165 to 200 s (after the drain) | 5,000 | 4,903 | 98% of the limit |
| A, 215 to 300 s (after the restore) | 30,000 | 29,379 | back at full rate |
| B, 60 to 300 s (never changed) | 20,000 | 19,568 | 97.8%, unaffected |

- **Drop and drain.** The cut applies at 100 s, but the trailing 60 s still holds about 29,400 of A's old requests, so nothing new is sent for A until
  they age out: 49 seconds in a row (102 to 150 s) with no completions for A. After that A runs at the new rate. The provider's trailing 60 s count
  falls to 832 at its lowest and rises again. After the restore at 200 s A is back at full rate straight away, because the queue still holds work.
- **Compliance.** Each 60 s window was checked against the limit in effect at its end, allowing the old, not yet drained traffic after the cut.
  No window failed. The worst ratio for A is 1.000 (exactly the drain allowance) and 0.980 for B.
- **Over capacity.** A completed 116,778 and expired 78,406. B completed 107,710 and expired 27,104. None were rejected, because the TTL fires before the
  queue fills. All 329,998 sent ids ended up succeeded (224,488) or expired (105,510).

Files: [s2-changing-limits.md](results/s2-changing-limits.md), [s2-changing-limits.json](results/s2-changing-limits.json) (per-second timeline for both models).

## 6. Scenario 3: asynchronous batch

One batch of 10,000 requests, 60% to Model A and 40% to Model B, 1,000 tokens each. Simulated failures: A 15% transient and 3% permanent, B 10% transient and 2% permanent.
The callback receiver answers 503 to the first two attempts.

| Check | Result |
|---|---|
| Acknowledgement | 28 ms (limit 1 s) |
| All requests final | 16.5 s after the ack. This is capacity: about 11,500 provider calls including retries at about 800 per second |
| First callback attempt | at +16.56 s, after every request was final |
| Callback attempts | 503 at +16.56 s, 503 at +17.07 s, 200 at +17.95 s (backoff of 0.5 s and then 1 s, with jitter) |
| Callback summary vs `GET /v1/batches/{id}` | identical: `completed_with_failures`, total 10,000, succeeded 9,674, failed 326, expired 0 |
| Ids in the final result | 10,000 rows, 10,000 distinct, the same set that was submitted |
| Failures exercised | 1,530 retry attempts; 326 requests failed (permanent failures, plus transient ones that used up 3 attempts) |
| Any 60 s window over a limit | no |

Files: [s3-batch-callback.md](results/s3-batch-callback.md), [s3-batch-callback.json](results/s3-batch-callback.json).

## 7. Durability, recovery and replicas

Sections 3 to 6 ran in memory. This section is the optional mode with Postgres and Redis (see the README).

**Scenario 4: killing the gateway in the middle of a batch.** A 10,000 request batch with a callback is acknowledged. When 3,018 requests are final the
gateway process is killed with `SIGKILL` (no shutdown, no final flush). It stays dead for 1 s and then a new gateway starts on the same database.

| Check | Result |
|---|---|
| Ack (rows saved before the 202) | 192 ms |
| Batch after the restart | `completed_with_failures`: 9,760 succeeded, 240 failed (permanent failures and exhausted retries), none lost |
| Callback | delivered once, 11.4 s after the restart, summary identical to `GET /v1/batches/{id}` |
| Ids in the final result | 10,000 rows, 10,000 distinct, equal to the submitted set |
| Provider calls in total | 11,512: the 10,000 requests, their retries, and the re-run of requests that were in flight at the crash. No provider 429s |
| Any 60 s window over a limit | no, checked across the whole run including before and after the crash |

Files: [s4-crash-recovery.md](results/s4-crash-recovery.md), [s4-crash-recovery.json](results/s4-crash-recovery.json).

**What persistence costs** (one gateway, same machine):

| Measure | In memory | Postgres and Redis |
|---|---|---|
| S3 ack for 10,000 requests | 28 ms | 185 ms (the rows are saved before the 202) |
| S3 time until all requests are final | 16.5 s | 18.4 s (2.5 s of that is the replica join delay below) |
| Single request ack, p50 / p99 | 0.8 ms / 2.6 ms | 2.2 ms / 7.3 ms (a lookup per client-supplied id, to keep retries idempotent) |
| Steady-state completions (70 s check at 1,100/s) | 815/s, 97.8% of the limit | 816/s, 97.9% of the limit |
| Requests rejected in that check | 0 | 1,652 |

Throughput at the limit is the same because the database writes are off the dispatch path. The cost is at the edges. The rejections come from the startup
delay: a gateway that joins through Redis waits 2.5 s before sending so the others can shrink their share first, and during that time the 20,000 request queue
fills. I kept the delay because starting at once could briefly push two gateways over the limit. The 70 s runs are short checks
([persistence-overhead-off.md](results/persistence-overhead-off.md), [persistence-overhead-on.md](results/persistence-overhead-on.md)), not repeats of the 6 minute S1.
The S3 run with both services is [s3-batch-callback-with-postgres-redis.md](results/s3-batch-callback-with-postgres-redis.md).

**Tests against real Postgres and Redis** (12 in `tests/test_persistence.py`): a batch is in the database before it is acknowledged; a crash in the middle of a
batch is recovered with every id once and one callback; finished work and idempotent retries survive a restart; open single requests survive a crash; limit
changes survive a restart; two replicas split a limit, share changes and hand the share back when one leaves; two replicas together never made the
provider push back; a survivor adopts the batch of a replica that died; one row the database rejects does not block the others; odd payloads (a NUL
character, emoji) are stored; concurrent retries of one id send one provider call; an idempotency key holds across a restart and between two replicas.

Not measured: throughput of several replicas at scale, behaviour with Postgres or Redis down, database growth over long runs.

## 8. Problems found along the way, and current limits

The first version met the headline goals. Probing it for edge cases found these problems, which are fixed and covered by tests:

| Found | What was wrong | Now |
|---|---|---|
| httpx connection pool | With a few thousand calls in flight it did work proportional to connections times waiting requests on every call (48 million `is_idle` calls in a 25 s profile) and froze the gateway's event loop for up to 3.7 s, which showed as bursty completions | aiohttp for the gateway-to-provider path and the load generator. S3 went from 34 s to 15.5 s |
| Burst at the start | The sliding window alone lets a whole minute out at once: about 10,000 simultaneous calls in one tick | a small credit bucket (`burst_s`); the window is still the hard limit |
| Audit at a limit change | An early checker judged a window ending exactly at a change against the new limit | the drain allowance described in section 5 |
| A request bigger than the model's token budget | A 50,000 token request to a 5,000 TPM model was accepted, sat at the head of the queue and held up the five normal requests behind it | `422` at submit; if a limit is lowered later, queued requests that no longer fit fail at once |
| Concurrent retries of the same id | Five simultaneous submissions of one new id were all accepted and produced 11 provider calls | one request, one call; the later ones get `200` |
| One bad row in the database writer | A 5 billion token value overflowed Postgres's integer and the writer retried it 58 times while a normal request behind it never got written. A NUL character in a payload made a batch fail with a misleading 503 | inputs are validated; payloads are stored as text; if a row is still rejected only that row is dropped and logged |
| Retrying a batch POST | Created a second batch | `Idempotency-Key` returns the same batch; a reused key with a different body is refused |
| Retries sent immediately | A failing provider would get the retries straight back, on top of the normal load | exponential backoff with jitter; a provider `429` pauses that model instead of being retried in a loop |
| Open admin and callback endpoints | Anyone could change limits; a callback URL could point at an internal address | optional `ADMIN_TOKEN` and `callback_allowed_hosts` |

**Current limits of the implementation**
- **HTTP gateway:** one asyncio process holding a Python object per request. With the rate limits removed (`scenarios/probe_http_ceiling.yaml`, 6,000 req/s offered
  for 20 s) it completed 6,000 per second with no errors and p99 latency of 92 ms ([http-ceiling-probe.md](results/http-ceiling-probe.md)). That is
  about 7 times what S1 to S4 need. It is a floor and not a ceiling: the load generator or the gateway may have been the limit, and I did not look further.
- **Bulk path:** about 92k requests per second per worker, sustained without saturating. A stripped-down benchmark of only the limiter and the simulator reached about
  1.2M per second in one process. It scales by adding processes, one per core.
- **Estimates:** the gateway trusts `estimated_tokens` and does not read the provider's real usage back.
- **Memory:** finished records stay in memory.

## 9. Projection: 10 billion requests per minute (not measured)

10 billion a minute is about 167 million requests per second, roughly 150 times the stretch benchmark. Nothing in this section was measured. It is arithmetic from
the numbers above plus design reasoning.

**Compute.** At the 92k requests per second per Python worker that was demonstrated, the admission and completion path alone needs about 1,800 cores, around 150 machines like
this one. A native (Go or Rust) hot path at an assumed 1M requests per second per core, which I have not measured, would need about 170 cores. That is where a second language starts to pay
off, as a cost saving rather than a requirement.

**Ingress.** 167 million separate HTTP calls per second is not realistic. Clients would have to submit batches (for example 1,000 requests per call is about 167,000 calls a second, or about
28 gateway processes at the 6,000 calls per second floor measured above) or stream over a persistent connection. Single-request calls stay for low-rate traffic.

**Design, most important first**
1. *Cells.* Split by provider account, model and region into independent cells, each a small group of gateway workers with its own queues. Failures and load stay local; more capacity means more cells.
2. *Hierarchical quota.* A small control plane owns each model's global limit and leases slices of it to cells in short epochs (say 1 s), moving quota towards cells that have queued work. Cells enforce their lease locally with
   the same sliding-window limiter, so the shared store is touched cells times once a second, never per request. This replaces today's fixed 1/N split, which wastes the share of an idle worker. Leases must add up to no more than the limit in every window.
   On a reduction the control plane stops issuing leases first and waits for the drain, as the single-process system does now.
3. *Durability without a database row per request.* Keep batch metadata and outcomes in a partitioned append-only log, keep per-request state in memory and rebuild it from the log, and put results in columnar or object storage with a retention period. At 167M/s even 100 bytes
   per request is 16 GB/s, so per-request rows in a relational database are out; Postgres would hold batches, quotas and configuration. The mode built here (write-behind rows, claim-based recovery) is the same idea at about 1,000 requests a second per gateway, not at that scale.
4. *Backpressure.* Bounded per-cell queues with a TTL, as now, plus admission control at the edge that looks at queue depth and lease headroom, so overload turns into fast 429s instead of long waits.
5. *Callbacks* from a separate delivery service that reads batch-completion events: retries with backoff and jitter, per-destination rate limits and circuit breakers, and idempotency keys (already in this design).
6. *Providers.* 10B requests a minute implies provider capacity far beyond the 50k RPM / 100M TPM used here (at 1,000 tokens per request, about 10 trillion tokens a minute), so many provider accounts and regions. The router has to weigh models, accounts, regional limits and failover.
   Limits reported by providers (headers, 429s) should feed the control plane so the gateway learns about changes itself instead of being told.
7. *Observability and safety.* Per-cell limiter and lease metrics, an independent audit stream (the equivalent of the simulator's audit here) to prove limits are respected, load shedding, and a kill switch per cell.

**What to check first:** per-core throughput of the real networked provider path (lower than the in-process simulation), lease epoch length against burstiness, memory per in-flight request, and callback fan-out when a large batch completes.

## 10. Reproducing

```bash
python3 -m venv .venv && source .venv/bin/activate && pip install -e ".[dev,report,persistence]"
scripts/run_all.sh            # tests and every benchmark and scenario, about 25 minutes
scripts/run_all.sh quick      # skips S1 and S2, about 5 minutes
docker compose up -d && export DATABASE_URL=postgresql://inference:inference@127.0.0.1:55432/inference REDIS_URL=redis://127.0.0.1:56379/0
python -m loadgen.run scenarios/s4_crash_recovery.yaml        # crash recovery, section 7
python -m loadgen.run scenarios/probe_http_ceiling.yaml       # the HTTP ceiling probe, section 8
```

Each run writes `runs/<name>-<timestamp>/` (git-ignored) with `report.md`, `report.json`, the service logs and, for limit changes, a chart. The files in `reports/results/` are copies of the runs cited here.
