# http-ceiling-probe



Mode: http (gateway and provider simulator as separate processes, wall clock). 1 process(es) for the engine on 12 CPUs, Python 3.14.7, macOS-27.0.1-arm64-arm-64bit-Mach-O.

## Overall (measured)

- Offered: 6,000 req/s for 20.0s (warm-up 8.0s excluded from steady state)
- **Completed per second (steady state): 6,000**
- Submitted 119,997; completed 119,997 (succeeded 119,997, failed 0); expired 0; rejected 0; retried attempts 0
- Latency p50/p95/p99: 0.0557s / 0.0774s / 0.0917s
- Every request accounted for: **True**; all 60s windows within limits: **True**

## model-a

- Configured: 100,000,000 RPM / 100,000,000,000 TPM
- Observed (steady): 360,010 RPM / 360,010,000 TPM (0.4% / 0.4% of limit)
- Max 60s window at provider: 119,997 requests (worst ratio to limit in effect 0.0012), 119,997,000 tokens (worst ratio 0.0012)
- Latency p50/p95/p99: 0.0557s / 0.0774s / 0.0917s
- Totals: {'submitted': 119997, 'rejected': 0, 'completed': 119997, 'succeeded': 119997, 'failed': 0, 'expired': 0, 'retried': 0, 'completed_tokens': 119997000, 'waiting_at_end': 0, 'in_flight_at_end': 0, 'unaccounted': 0}

## Load generator (client side)

- sent: 119997
- accepted_202: 119997
- rejected_429: 0
- client_errors: 0
- achieved_send_rate_per_s: 6000
- ack_latency_s_p50_p95_p99: [0.0014, 0.0042, 0.0224]
- gateway_idle_after_run: True
- final_state_of_every_sent_id: {'succeeded': 119997}

## Pass criteria

- [x] every_request_accounted: 119,997 sent -> {'succeeded': 119997} (unknown ids = client errors = 0); waiting at end: 0
- [x] no_60s_window_over_limit: worst window/limit ratios: model-a rpm 0.001 tpm 0.001
