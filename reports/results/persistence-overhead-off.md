# s1-capacity

Scenario 1. One model at the available provider capacity (50,000 RPM / 100M TPM), 1,000-token requests offered at 66,000 RPM (32% above capacity) for 6 minutes. The first 60s (window fill) is excluded from the steady state.


Mode: http (gateway and provider simulator as separate processes, wall clock). 1 process(es) for the engine on 12 CPUs, Python 3.14.7, macOS-27.0.1-arm64-arm-64bit-Mach-O.

## Overall (measured)

- Offered: 1,100 req/s for 70.0s (warm-up 20.0s excluded from steady state)
- **Completed per second (steady state): 815**
- Submitted 76,995; completed 76,995 (succeeded 76,995, failed 0); expired 0; rejected 0; retried attempts 0
- Latency p50/p95/p99: 12.2579s / 23.1732s / 24.1141s
- Every request accounted for: **True**; all 60s windows within limits: **True**

## model-a

- Configured: 50,000 RPM / 100,000,000 TPM
- Observed (steady): 48,917 RPM / 48,916,800 TPM (97.8% / 48.9% of limit)
- Max 60s window at provider: 49,000 requests (worst ratio to limit in effect 0.9800), 49,000,000 tokens (worst ratio 0.4900)
- Latency p50/p95/p99: 12.2579s / 23.1732s / 24.1141s
- Totals: {'submitted': 76995, 'rejected': 0, 'completed': 76995, 'succeeded': 76995, 'failed': 0, 'expired': 0, 'retried': 0, 'completed_tokens': 76995000, 'waiting_at_end': 0, 'in_flight_at_end': 0, 'unaccounted': 0}

## Load generator (client side)

- sent: 76995
- accepted_202: 76995
- rejected_429: 0
- client_errors: 0
- achieved_send_rate_per_s: 1100
- ack_latency_s_p50_p95_p99: [0.0008, 0.0014, 0.0022]
- gateway_idle_after_run: True
- final_state_of_every_sent_id: {'succeeded': 76995}

## Pass criteria

- [x] every_request_accounted: 76,995 sent -> {'succeeded': 76995} (unknown ids = client errors = 0); waiting at end: 0
- [x] no_60s_window_over_limit: worst window/limit ratios: model-a rpm 0.980 tpm 0.490
- [x] model-a_completes_90pct_of_capacity_after_warmup: 48,917 RPM of 50,000 = 97.8% (steady window [20, 70]s)
