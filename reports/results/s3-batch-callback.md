# s3-batch-callback

Scenario 3. One asynchronous batch of 10,000 requests across two models, with simulated transient and permanent failures. The callback receiver refuses the first two delivery attempts.


Mode: http (gateway and provider simulator as separate processes, wall clock). 1 process(es) for the engine on 12 CPUs, Python 3.14.7, macOS-27.0.1-arm64-arm-64bit-Mach-O.

## Overall (measured)

- Offered: 0 req/s for 120.0s (warm-up 0.0s excluded from steady state)
- **Completed per second (steady state): 83**
- Submitted 10,000; completed 10,000 (succeeded 9,674, failed 326); expired 0; rejected 0; retried attempts 1,530
- Latency p50/p95/p99: 7.2341s / 13.5404s / 14.3734s
- Every request accounted for: **True**; all 60s windows within limits: **True**

## model-a

- Configured: 30,000 RPM / 60,000,000 TPM
- Observed (steady): 3,037 RPM / 3,037,000 TPM (10.1% / 5.1% of limit)
- Max 60s window at provider: 7,129 requests (worst ratio to limit in effect 0.2376), 7,129,000 tokens (worst ratio 0.1188)
- Latency p50/p95/p99: 7.3795s / 13.9507s / 14.5171s
- Totals: {'submitted': 6074, 'rejected': 0, 'completed': 6074, 'succeeded': 5834, 'failed': 240, 'expired': 0, 'retried': 1055, 'completed_tokens': 6074000, 'waiting_at_end': 0, 'in_flight_at_end': 0, 'unaccounted': 0}

## model-b

- Configured: 20,000 RPM / 40,000,000 TPM
- Observed (steady): 1,963 RPM / 1,963,000 TPM (9.8% / 4.9% of limit)
- Max 60s window at provider: 4,356 requests (worst ratio to limit in effect 0.2178), 4,356,000 tokens (worst ratio 0.1089)
- Latency p50/p95/p99: 7.0916s / 13.1422s / 13.6758s
- Totals: {'submitted': 3926, 'rejected': 0, 'completed': 3926, 'succeeded': 3840, 'failed': 86, 'expired': 0, 'retried': 475, 'completed_tokens': 3926000, 'waiting_at_end': 0, 'in_flight_at_end': 0, 'unaccounted': 0}

## Batch and callback

- requests: 10000
- ack_latency_s: 0.028
- time_from_ack_to_all_final_s: 16.54
- callback_attempts: [{'at_s_after_ack': 16.56, 'status_code': 503}, {'at_s_after_ack': 17.07, 'status_code': 503}, {'at_s_after_ack': 17.95, 'status_code': 200}]
- callback_final_status: delivered
- batch_status: completed_with_failures
- summary: {'batch_id': '57b320b7291a4b049a1152423e9c6da6', 'status': 'completed_with_failures', 'total': 10000, 'succeeded': 9674, 'failed': 326, 'expired': 0}
- request_states: {'succeeded': 9674, 'failed': 326}
- progress_samples: [{'t': 0.01, 'final': 0, 'callback': 'pending'}, {'t': 1.03, 'final': 562, 'callback': 'pending'}, {'t': 2.05, 'final': 1268, 'callback': 'pending'}, {'t': 3.07, 'final': 1996, 'callback': 'pending'}, {'t': 4.09, 'final': 2739, 'callback': 'pending'}, {'t': 5.11, 'final': 3464, 'callback': 'pending'}, {'t': 6.14, 'final': 4194, 'callback': 'pending'}, {'t': 7.16, 'final': 4924, 'callback': 'pending'}, {'t': 8.18, 'final': 5643, 'callback': 'pending'}, {'t': 9.2, 'final': 6370, 'callback': 'pending'}, {'t': 10.22, 'final': 7100, 'callback': 'pending'}, {'t': 11.24, 'final': 7846, 'callback': 'pending'}, {'t': 12.26, 'final': 8547, 'callback': 'pending'}, {'t': 13.28, 'final': 9304, 'callback': 'pending'}, {'t': 14.3, 'final': 9859, 'callback': 'pending'}, {'t': 15.32, 'final': 9987, 'callback': 'pending'}, {'t': 16.34, 'final': 9999, 'callback': 'pending'}, {'t': 17.36, 'final': 10000, 'callback': 'retrying'}]

## Pass criteria

- [x] ack_within_1s: 28 ms for 10,000 requests
- [x] callback_sent_only_after_all_final: all-final at +16.54s, first callback attempt at +16.56s
- [x] first_attempts_rejected_then_delivered: attempt status codes [503, 503, 200], gateway reports 'delivered' after 3 attempts
- [x] callback_summary_matches_batch_status: callback {'batch_id': '57b320b7291a4b049a1152423e9c6da6', 'status': 'completed_with_failures', 'total': 10000, 'succeeded': 9674, 'failed': 326, 'expired': 0} vs API {'batch_id': '57b320b7291a4b049a1152423e9c6da6', 'status': 'completed_with_failures', 'total': 10000, 'succeeded': 9674, 'failed': 326, 'expired': 0}
- [x] every_request_id_exactly_once: 10,000 result rows, 10,000 distinct ids, 10,000 submitted
- [x] transient_and_permanent_failures_present: states {'succeeded': 9674, 'failed': 326}; retry attempts 1,530
- [x] no_60s_window_over_limit: provider audit
