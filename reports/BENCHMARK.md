# Benchmark report

Everything in sections 1-6 is **measured** by the scripts in this repository. Section 7 is a **projection**, kept separate on purpose.
Raw per-run reports (markdown, and JSON with per-second timelines) are in [`reports/results/`](results/).

## 1. Summary

| # | Goal | Pass criteria | Result | Verdict |
|---|---|---|---|---|
| S1 | Reach 50k RPM / 100M TPM | ≥90% of allowed capacity after warm-up; no 60 s window over a limit; every request accounted for | **97.8%** of RPM over 300 s steady state; worst 60 s window **98.0%** of the limit; 395,996 sent, all accounted for | pass |
| S2 | Different models, changing limits | each model within current limits; A follows both changes; B unaffected; report over time | A: 29,361 → 4,905 → 29,388 completed/min against limits 30,000 → 5,000 → 30,000; B at 97.8% throughout; no window over its limit | pass |
| S3 | Async batch completion | ack < 1 s; callback after all final; delivered after recovery; summary = API; ids exactly once | ack **26 ms**; callback after all 10,000 final; attempts 503, 503, 200; summary identical to API; 10,000 distinct ids | pass |
| Required | 300,000 simulated req/s | complete 300k/s | **329,947 completed/s** (offered 330,000) | pass |
| Stretch | 1,000,000 simulated req/s | complete 1M/s | **1,099,978 completed/s** (offered 1,100,000) | pass |
| Planning | 10B req/min | explain the extension | section 7 | design only |

## 2. Test environment and method

- **Machine:** MacBook Pro, Apple M4 Pro, 12 cores (8 performance + 4 efficiency), 24 GB RAM, macOS, Python 3.14.7. A different machine will
  give different absolute numbers; the number of bulk worker processes adapts to `os.cpu_count()`.
- **HTTP scenarios (S1-S3):** provider simulator, gateway and load generator are three separate OS processes on this machine, talking over HTTP on
  localhost, wall-clock time. The gateway is one `uvicorn` process.
- **Scale benchmarks:** the gateway engine and the provider simulator run *in the same process* (no network), inside N worker processes (9 for 300k,
  12 for 1M). Each worker owns 1/N of every limit and 1/N of the offered load. Limits are set far above the offered load, as the assignment allows.
- **What "completed" means:** a request counts only when the simulated provider call reached a final state (success or failure) after its simulated
  latency elapsed in real time. Accepting a request at the API does not count.
- **Where the numbers come from:** request states, counts and latency from the gateway's own records; rate-limit compliance from the provider
  simulator's **independent audit** (every accepted call counted in 10 ms buckets, maximum taken over any 60 s span; no shared code with the gateway limiter);
  client-side counts from the load generator. For S1/S2 the load generator additionally asks the gateway for the final state of every id it sent.
- **Steady state:** throughput is taken over the window after warm-up (S1/S2: after 60 s so the limiter window is full; scale runs: after 10 s).
- **Latency** is end to end inside the system: from entering the gateway queue to a final state. Where offered load exceeds capacity (S1, S2) it is
  dominated by queue wait and is capped by the TTL, so it is not a measure of provider speed.

Configuration of each run is the scenario file of the same name in [`scenarios/`](../scenarios/). Gateway settings used by S1-S3: `queue_max` 20,000 (S3: 100,000),
`queue_ttl_s` 30 (S3: 60), `max_attempts` 3, `headroom` 0.98 (the gateway uses 98% of the provider limit so network jitter cannot push the provider over), `burst_s` 0.25.

## 3. Scale benchmarks

| | Required (300k) | Stretch (1M) |
|---|---|---|
| Offered load | 330,000 req/s | 1,100,000 req/s |
| Worker processes | 9 | 12 (all cores) |
| **Completed per second, steady state** | **329,947** | **1,099,978** |
| Requests in the run (30 s) | 9.9 million | 33.0 million |
| Succeeded / failed / expired / rejected | all succeeded / 0 / 0 / 0 | all succeeded / 0 / 0 / 0 |
| Latency p50 / p95 / p99 | 0.263 s / 0.589 s / 0.794 s | 0.266 s / 0.589 s / 0.794 s |
| Every request accounted for | yes | yes |
| Any 60 s window above a limit | no | no |
| Per-worker rate | about 37k/s | about 92k/s |

Models: two, with simulated median latency 200 ms and 350 ms (lognormal, shape 0.4), 50/50 mix, 1,000 tokens ±20%. The configured limits (12M RPM and 40M RPM per model)
were deliberately not binding, so these runs measure the system's own throughput, not limit handling; limit handling is covered in S1-S3 and in the unit tests.
Latency here is almost entirely the simulated provider latency, which shows the engine adds little queueing.

Files: [scale-300k.md](results/scale-300k.md), [scale-1m.md](results/scale-1m.md).

**Caveat.** In both runs completions equalled the offered rate, so the workers were keeping up and the ceiling of this design on this machine was *not* found; 1.1M/s is
a lower bound for the 12-core machine. It also excludes network and serialization costs, which the in-process provider does not have.

## 4. Scenario 1: reach the available provider capacity

One model, 50,000 RPM and 100M TPM; 1,000-token requests offered at 1,100/s (66,000 RPM, 32% above capacity) for 360 s; the first 60 s excluded as warm-up.

| Metric | Value |
|---|---|
| Observed steady-state rate (60-360 s) | **48,921 RPM = 97.8%** of the 50,000 limit (the 2% gap is the configured 0.98 headroom) |
| Observed tokens | 48.9M TPM = 48.9% of the TPM limit (1,000-token requests exhaust RPM first) |
| Worst 60 s window at the provider | **49,000 requests = 0.980 of the limit**; 49.0M tokens = 0.490 |
| Sent / accepted (202) / rejected (429) | 395,996 / 313,791 / 82,205 |
| Final state of every sent id | 313,791 succeeded + 82,205 rejected = 395,996; 0 unknown, 0 still waiting |
| Failed / expired | 0 / 0 (the 20,000-request queue fills in about 24 s, before the 30 s TTL, so excess is rejected rather than expired) |
| Client ack latency p50 / p95 / p99 | 0.8 ms / 1.5 ms / 2.3 ms |
| Latency inside the system p50 / p95 / p99 | 24.8 s / 25.1 s / 25.3 s (queue wait, as expected when saturated) |

Files: [s1-capacity.md](results/s1-capacity.md), [s1-capacity.json](results/s1-capacity.json).

## 5. Scenario 2: different models and changing limits

Model A 30,000 RPM / 60M TPM, Model B 20,000 RPM / 40M TPM, 1,100 req/s offered (A about 650/s, B about 450/s, both above their limits). At t=100 s Model A is cut to
5,000 RPM, at t=200 s it is restored to 30,000. The services keep running; limits are changed through the admin API (provider raised first / gateway lowered first).

![Limits and throughput over time](results/s2-limits-over-time.png)

| Segment | Limit in effect | Observed completed/min | Result |
|---|---|---|---|
| A, 15-100 s | 30,000 | 29,361 | within limit, 98% |
| A, 165-200 s (after drain) | 5,000 | 4,905 | within limit, 98% |
| A, 215-300 s (after restore) | 30,000 | 29,388 | recovered to full rate |
| B, 60-300 s (never changed) | 20,000 | 19,568 (97.8%) | unaffected while A was constrained |

- **Drop time and drain time.** At t=100 s the reduction applies instantly, but the trailing 60 s still holds about 29,400 of A's old requests, so the gateway admits nothing
  for A until they age out: **49 consecutive seconds (102-150 s) with zero completions for A**, then A runs at the new rate. The provider's trailing-60 s count falls to a minimum of 832
  and rises again. After the restore at t=200 s, A returns to full rate immediately (the queue still holds work).
- **Compliance.** Every 60 s window was checked against the limit *in effect* at its end, allowing the not-yet-drained pre-reduction traffic after the cut. No window failed.
  The worst ratio for A is 1.000 (exactly at the drain allowance), for B 0.980.
- **Over-capacity behaviour:** A completed 116,777 and expired 78,407; B completed 107,711 and expired 27,103; none rejected (the queue limit was not reached before the TTL). All 329,998 sent ids ended
  as succeeded (224,488) or expired (105,510); none lost.

Files: [s2-changing-limits.md](results/s2-changing-limits.md), [s2-changing-limits.json](results/s2-changing-limits.json) (per-second timeline for both models).

## 6. Scenario 3: asynchronous batch completion

One batch of 10,000 requests, 60% Model A / 40% Model B, 1,000 tokens each. Simulated failures: A 15% transient + 3% permanent, B 10% transient + 2% permanent. The callback receiver answers 503 to the first two attempts.

| Check | Result |
|---|---|
| Acknowledgement | **26 ms** (limit 1 s) |
| All requests final | +15.5 s after the acknowledgement (capacity-bound: about 11,500 provider calls including retries at about 800/s) |
| First callback attempt | +15.50 s, i.e. after every request was final |
| Callback attempts | 503 (+15.50 s), 503 (+15.92 s), **200 (+16.76 s)**; backoff 0.5 s then 1 s, with jitter |
| Callback summary vs `GET /v1/batches/{id}` | identical: status `completed_with_failures`, total 10,000, succeeded 9,657, failed 343, expired 0 |
| Request ids in the final result | 10,000 rows, 10,000 distinct ids, equal to the submitted set |
| Failures exercised | 1,527 retry attempts (A 1,066, B 461); 343 requests failed (permanent failures plus transient failures that exhausted 3 attempts) |
| Any 60 s window above a limit | no |

Files: [s3-batch-callback.md](results/s3-batch-callback.md), [s3-batch-callback.json](results/s3-batch-callback.json).

## 7. Bottlenecks and what was found

**Found and fixed during development**
1. *httpx connection pool.* With a few thousand concurrent provider calls, httpx's pool did work proportional to connections × waiting requests on every call (48M `is_idle` calls in a 25 s profile) and stalled the
   gateway's event loop for up to 3.7 s, which showed up as bursty completions. Replaced with aiohttp for the gateway→provider path and the load generator. S3 then ran at capacity (15.5 s instead of 34 s).
2. *Startup burst.* The window limiter alone lets a whole minute of capacity out at once, which fired about 10,000 concurrent calls in one tick. Added a small smoothing credit (`burst_s`, 0.25 s of capacity); the sliding
   window remains the hard guarantee.
3. *Audit boundary.* An early version of the checker judged a window ending exactly at a limit change against the new limit; fixed with the drain allowance described in section 5.

**Current limits of this implementation**
- **HTTP gateway:** one asyncio process holding per-request Python objects. With the rate limits removed (`scenarios/probe_http_ceiling.yaml`, 6,000 req/s offered for 20 s) it completed **6,003 req/s with no errors, p99 inside-system latency 93 ms**
  ([http-ceiling-probe.md](results/http-ceiling-probe.md)). That is about 7× what S1-S3 need. It is a *floor*, not a ceiling: the load generator or the gateway may have been the limit and this was not investigated further.
- **Bulk path:** about 92k requests/s per worker process was sustained without saturating (a stripped micro-benchmark of just the limiter and simulator reached about 1.2M/s per process). Scaling is by processes, one per core.
- **S1/S2 latency** is queue wait by construction, bounded by the TTL.
- **No durability.** State is in memory; the throughput numbers do not include any storage cost.

## 8. Projection: 10 billion requests per minute (not measured)

10 billion per minute is about **167 million requests/s**, about 150× the stretch benchmark. Nothing below was measured; it is arithmetic from the measurements above plus design reasoning.

**Compute.** At the demonstrated 92k requests/s per Python worker, the admission/completion path alone would need about 1,800 cores, roughly 150 machines of this class. A native (Go/Rust) hot path at an
*assumed* 1M requests/s per core, which is not measured here, would need about 170 cores. This is where a second language starts to pay off, as a cost reduction, not as a requirement.

**Ingress.** 167M individual HTTP calls per second is not realistic. Clients must submit batches (for example 1,000 requests per call, about 167k calls/s, or about 28 gateway processes at the 6k calls/s floor measured above) or stream
over a persistent protocol; single-request calls remain for low-rate traffic.

**Design, in order of importance**
1. **Cells.** Partition by (provider account, model, region) into independent cells, each a small group of gateway workers with its own queues. Failure and load stay local; adding capacity means adding cells.
2. **Hierarchical quota.** A small control plane owns the global limit per model and *leases* slices of it to cells in short epochs (for example 1 s), rebalancing toward cells with queued work. Cells enforce their lease locally with the same sliding-window
   limiter, so the shared store is touched cells × 1 Hz, never per request. This replaces today's static 1/N split, which wastes the share of an idle shard. Leases must sum to at most the limit in every window; on a limit reduction the control plane stops issuing leases
   first and waits for the drain, exactly as the single-process system does.
3. **Durability without a per-request database write.** Persist batch metadata and outcomes in a partitioned, append-only log; keep per-request state in memory and recover from the log; store results columnar/object storage with a retention period.
   At 167M/s even 100 bytes per request is 16 GB/s, so per-request rows in a relational database are out; Postgres holds batches, quotas and configuration only.
4. **Backpressure.** Per-cell bounded queues with TTL (as now) plus admission control at the edge using the cells' queue depth and lease headroom, so overload shows up as fast 429s rather than latency.
5. **Callbacks** from a separate delivery service reading batch-completion events: retries with backoff and jitter, per-destination rate limits and circuit breakers, idempotency keys (already in this design).
6. **Providers.** 10B requests/min implies provider capacity far beyond the 50k RPM / 100M TPM used here (at 1,000 tokens per request, about 10 trillion tokens/min), i.e. many provider accounts and regions; the router must weigh models, accounts, regional
   limits and failover. Provider-reported limits (headers, 429s) should feed the control plane so the gateway learns of limit changes itself rather than being told.
7. **Observability and safety.** Per-cell limiter windows and lease usage exported as metrics; an independent audit stream (the equivalent of the simulator audit here) to prove limits are respected; load-shedding and kill switches per cell.

**Risks to validate first:** per-core throughput of the real (networked) provider path, which is lower than the in-process simulation; lease epoch length against burstiness; memory per in-flight request; callback fan-out at batch completion.

## 9. Reproducing

```bash
python3 -m venv .venv && source .venv/bin/activate && pip install -e ".[dev,report]"
scripts/run_all.sh            # tests + all benchmarks and scenarios, about 25 minutes
scripts/run_all.sh quick      # skips S1 and S2, about 4 minutes
python -m loadgen.run scenarios/probe_http_ceiling.yaml     # the HTTP ceiling probe from section 7
```

Each run writes `runs/<name>-<timestamp>/` (git-ignored) with `report.md`, `report.json`, service logs and, for limit changes, a chart; the files in `reports/results/` are copies of the runs cited above.
