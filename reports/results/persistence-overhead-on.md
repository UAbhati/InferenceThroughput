# s1-capacity

Scenario 1. One model at the available provider capacity (50,000 RPM / 100M TPM), 1,000-token requests offered at 66,000 RPM (32% above capacity) for 6 minutes. The first 60s (window fill) is excluded from the steady state.


Mode: http (gateway and provider simulator as separate processes, wall clock). 1 process(es) for the engine on 12 CPUs, Python 3.14.7, macOS-27.0.1-arm64-arm-64bit-Mach-O.

## Overall (measured)

- Offered: 1,100 req/s for 70.0s (warm-up 20.0s excluded from steady state)
- **Completed per second (steady state): 815**
- Submitted 76,997; completed 75,310 (succeeded 75,310, failed 0); expired 0; rejected 1,687; retried attempts 0
- Latency p50/p95/p99: 14.3734s / 24.5988s / 24.8448s
- Every request accounted for: **True**; all 60s windows within limits: **True**

## model-a

- Configured: 50,000 RPM / 100,000,000 TPM
- Observed (steady): 48,929 RPM / 48,928,800 TPM (97.9% / 48.9% of limit)
- Max 60s window at provider: 49,000 requests (worst ratio to limit in effect 0.9800), 49,000,000 tokens (worst ratio 0.4900)
- Latency p50/p95/p99: 14.3734s / 24.5988s / 24.8448s
- Totals: {'submitted': 76997, 'rejected': 1687, 'completed': 75310, 'succeeded': 75310, 'failed': 0, 'expired': 0, 'retried': 0, 'completed_tokens': 75310000, 'waiting_at_end': 0, 'in_flight_at_end': 0, 'unaccounted': 0}

## Load generator (client side)

- sent: 76997
- accepted_202: 75310
- rejected_429: 1687
- client_errors: 0
- achieved_send_rate_per_s: 1100
- ack_latency_s_p50_p95_p99: [0.0023, 0.0045, 0.0066]
- gateway_idle_after_run: True
- final_state_of_every_sent_id: {'succeeded': 75310, 'rejected': 1687}

## Pass criteria

- [x] every_request_accounted: 76,997 sent -> {'succeeded': 75310, 'rejected': 1687} (unknown ids = client errors = 0); waiting at end: 0
- [x] no_60s_window_over_limit: worst window/limit ratios: model-a rpm 0.980 tpm 0.490
- [x] model-a_completes_90pct_of_capacity_after_warmup: 48,929 RPM of 50,000 = 97.9% (steady window [20, 70]s)
