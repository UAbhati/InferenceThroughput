# s2-changing-limits

Scenario 2. Model A (30,000 RPM / 60M TPM) and Model B (20,000 RPM / 40M TPM) both offered more than they allow. Model A is cut to 5,000 RPM at t=100s and restored at t=200s while the services keep running; Model B is untouched.


Mode: http (gateway and provider simulator as separate processes, wall clock). 1 process(es) for the engine on 12 CPUs, Python 3.14.7, macOS-27.0.1-arm64-arm-64bit-Mach-O.

## Overall (measured)

- Offered: 1,100 req/s for 300.0s (warm-up 60.0s excluded from steady state)
- **Completed per second (steady state): 629**
- Submitted 329,998; completed 224,488 (succeeded 224,488, failed 0); expired 105,510; rejected 0; retried attempts 0
- Latency p50/p95/p99: 30.3154s / 30.6185s / 30.9247s
- Every request accounted for: **True**; all 60s windows within limits: **True**

## model-a

- Configured: 30,000 RPM / 60,000,000 TPM
- Observed (steady): 18,158 RPM / 18,158,250 TPM (limits changed during run; see timeline)
- Max 60s window at provider: 29,400 requests (worst ratio to limit in effect 1.0000), 29,400,000 tokens (worst ratio 0.4900)
- Latency p50/p95/p99: 30.3154s / 30.6185s / 30.6185s
- Totals: {'submitted': 195184, 'rejected': 0, 'completed': 116778, 'succeeded': 116778, 'failed': 0, 'expired': 78406, 'retried': 0, 'completed_tokens': 116778000, 'waiting_at_end': 0, 'in_flight_at_end': 0, 'unaccounted': 0}

## model-b

- Configured: 20,000 RPM / 40,000,000 TPM
- Observed (steady): 19,568 RPM / 19,568,250 TPM (97.8% / 48.9% of limit)
- Max 60s window at provider: 19,600 requests (worst ratio to limit in effect 0.9800), 19,600,000 tokens (worst ratio 0.4900)
- Latency p50/p95/p99: 30.3154s / 30.9247s / 31.2339s
- Totals: {'submitted': 134814, 'rejected': 0, 'completed': 107710, 'succeeded': 107710, 'failed': 0, 'expired': 27104, 'retried': 0, 'completed_tokens': 107710000, 'waiting_at_end': 0, 'in_flight_at_end': 0, 'unaccounted': 0}

## Load generator (client side)

- sent: 329998
- accepted_202: 329998
- rejected_429: 0
- client_errors: 0
- achieved_send_rate_per_s: 1100
- ack_latency_s_p50_p95_p99: [0.0008, 0.0018, 0.0042]
- gateway_idle_after_run: True
- final_state_of_every_sent_id: {'succeeded': 224488, 'expired': 105510}

## Pass criteria

- [x] every_request_accounted: 329,998 sent -> {'succeeded': 224488, 'expired': 105510} (unknown ids = client errors = 0); waiting at end: 0
- [x] no_60s_window_over_limit: worst window/limit ratios: model-a rpm 1.000 tpm 0.490, model-b rpm 0.980 tpm 0.490
- [x] model-a_follows_limit_30000_from_0s: 29,362 completed/min over [15s,100s] vs limit 30,000
- [x] model-a_follows_limit_5000_from_100s: 4,903 completed/min over [165s,200s] vs limit 5,000
- [x] model-a_follows_limit_30000_from_200s: 29,379 completed/min over [215s,300s] vs limit 30,000
- [x] model-b_unaffected_and_busy: 97.8% of its 20,000 RPM, throughout the run incl. while the other model was constrained

## Limit changes applied while running

- t=100.0s model-a: rpm=5000 tpm=None
- t=200.0s model-a: rpm=30000 tpm=None

![limits over time](s2-limits-over-time.png)
