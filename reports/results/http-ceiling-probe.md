# http-ceiling-probe



Mode: http (gateway and provider simulator as separate processes, wall clock). 1 process(es) for the engine on 12 CPUs, Python 3.14.7, macOS-27.0.1-arm64-arm-64bit-Mach-O.

## Overall (measured)

- Offered: 6,000 req/s for 20.0s (warm-up 8.0s excluded from steady state)
- **Completed per second (steady state): 6,003**
- Submitted 119,971; completed 119,971 (succeeded 119,971, failed 0); expired 0; rejected 0; retried attempts 0
- Latency p50/p95/p99: 0.0552s / 0.0774s / 0.0926s
- Every request accounted for: **True**; all 60s windows within limits: **True**

## model-a

- Configured: 100,000,000 RPM / 100,000,000,000 TPM
- Observed (steady): 360,195 RPM / 360,195,000 TPM (0.4% / 0.4% of limit)
- Max 60s window at provider: 119,971 requests (worst ratio to limit in effect 0.0012), 119,971,000 tokens (worst ratio 0.0012)
- Latency p50/p95/p99: 0.0552s / 0.0774s / 0.0926s
- Totals: {'submitted': 119971, 'rejected': 0, 'completed': 119971, 'succeeded': 119971, 'failed': 0, 'expired': 0, 'retried': 0, 'completed_tokens': 119971000, 'waiting_at_end': 0, 'in_flight_at_end': 0, 'unaccounted': 0}

## Load generator (client side)

- sent: 119971
- accepted_202: 119971
- rejected_429: 0
- client_errors: 0
- achieved_send_rate_per_s: 5999
- ack_latency_s_p50_p95_p99: [0.0013, 0.0036, 0.0222]
- gateway_idle_after_run: True
- final_state_of_every_sent_id: {'succeeded': 119971}

## Pass criteria

- [x] every_request_accounted: 119,971 sent -> {'succeeded': 119971} (unknown ids = client errors = 0); waiting at end: 0
- [x] no_60s_window_over_limit: worst window/limit ratios: model-a rpm 0.001 tpm 0.001
