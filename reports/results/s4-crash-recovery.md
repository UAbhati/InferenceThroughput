# s4-crash-recovery

Durability check (needs Postgres, see README). A batch of 10,000 requests with a callback is accepted; once 30% of it is final the gateway process is killed with SIGKILL, then started again. The new gateway must finish the batch, deliver the callback once, and report every request id exactly once.


Mode: http (gateway killed with SIGKILL mid-batch and restarted; Postgres). 1 process(es) for the engine on 12 CPUs, Python 3.14.7, macOS-27.0.1-arm64-arm-64bit-Mach-O.

## Overall (measured)

- Offered: 0 req/s for 180.0s (warm-up 0.0s excluded from steady state)
- **Completed per second (steady state): 0**
- Submitted 0; completed 0 (succeeded 0, failed 0); expired 0; rejected 0; retried attempts 0
- Latency p50/p95/p99: nans / nans / nans
- Every request accounted for: **True**; all 60s windows within limits: **True**

## model-a

- Configured: 30,000 RPM / 60,000,000 TPM
- Observed (steady): 0 RPM / 0 TPM (0.0% / 0.0% of limit)
- Max 60s window at provider: 6,933 requests (worst ratio to limit in effect 0.2311), 6,933,000 tokens (worst ratio 0.1155)
- Latency p50/p95/p99: nans / nans / nans
- Totals: {'submitted': 0, 'rejected': 0, 'completed': 0, 'succeeded': 0, 'failed': 0, 'expired': 0, 'retried': 0, 'completed_tokens': 0, 'waiting_at_end': 0, 'in_flight_at_end': 0, 'unaccounted': 0}

## model-b

- Configured: 20,000 RPM / 40,000,000 TPM
- Observed (steady): 0 RPM / 0 TPM (0.0% / 0.0% of limit)
- Max 60s window at provider: 4,579 requests (worst ratio to limit in effect 0.2289), 4,579,000 tokens (worst ratio 0.1145)
- Latency p50/p95/p99: nans / nans / nans
- Totals: {'submitted': 0, 'rejected': 0, 'completed': 0, 'succeeded': 0, 'failed': 0, 'expired': 0, 'retried': 0, 'completed_tokens': 0, 'waiting_at_end': 0, 'in_flight_at_end': 0, 'unaccounted': 0}

## Batch and callback

- requests: 10000
- ack_latency_s: 0.192
- final_when_killed: 3018
- gateway_down_s: 1.02
- restart_to_callback_delivered_s: 11.4
- callback_attempts: [{'at_s_after_restart': 11.37, 'status_code': 200}]
- batch_status_after_restart: completed_with_failures
- request_states: {'succeeded': 9760, 'failed': 240}
- provider_calls_total: 11512
- provider_calls_before_kill: 3793
- provider_rate_limited_total: 0

## Pass criteria

- [x] acknowledged_before_the_crash: acked in 192 ms; 3,018 of 10,000 requests were final when the gateway was killed
- [x] batch_completes_after_restart: status 'completed_with_failures', callback 'delivered', 11.4s after the restart
- [x] callback_delivered_once: attempt status codes [200]
- [x] callback_summary_matches_batch_status: callback {'batch_id': '2c9543934ad9400bb344f0ddf8afe665', 'status': 'completed_with_failures', 'total': 10000, 'succeeded': 9760, 'failed': 240, 'expired': 0} vs API {'batch_id': '2c9543934ad9400bb344f0ddf8afe665', 'status': 'completed_with_failures', 'total': 10000, 'succeeded': 9760, 'failed': 240, 'expired': 0}
- [x] every_request_id_exactly_once_and_none_lost: 10,000 result rows, 10,000 distinct ids, states {'succeeded': 9760, 'failed': 240}
- [x] no_60s_window_over_limit: provider audit across the whole run, including before and after the crash
